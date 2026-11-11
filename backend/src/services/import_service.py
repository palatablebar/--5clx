"""导入 / 采样 → Drain 挖掘预览 → 应用模板到事件 → 槽位指标重算。

设计要点：
- 挖掘是「真算」：把上传的日志行灌进 DrainEngine，先匹配库内已知模板，剩余行自建模板；
- 覆盖率 = 被「样本 >= 2 的模板」覆盖的行 / 总行（_noise 与 tentative 不计入分子）；
- 应用时按 line_hash 幂等入库，并给 template_id 为空的 pending 事件回填模板；
- 结构化管道：每条事件提取命名槽值（写回 fields["slots"]），并按 (模板, 时间桶, 槽位, 槽值)
  重算 slot_metric；受影响桶先删后建，重复导入不新增事件、不重复计数；
- 迟到记录归入新模板时按事件自身时间桶重算，历史走势随之修正；
- 导入批次按全文指纹（content_fingerprint）幂等：同一批重复导入只更新 last_run_at；
- 样本文件由 backend/scripts/fixtures.py 确定性生成，seed 里的 pending 行与样本逐字一致，
  因此重复导入不会产生重复事件（自然键 = source + 原始行）。
"""
from __future__ import annotations

import hashlib
import time
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.constants import error_codes as codes
from src.constants.enums import EventLevel, TemplateStatus
from src.constants.limits import MAX_UPLOAD_LINES, MAX_UPLOAD_MB_DEFAULT
from src.config.settings import settings
from src.models.base import now_local
from src.models.import_batch import ImportBatch
from src.models.log_event import LogEvent
from src.models.log_source import LogSource
from src.models.log_template import LogTemplate
from src.services import drain_engine, slot_pipeline
from src.utils.errors import AppError
from src.utils.logger import get_logger
from src.utils.text import line_hash, normalize_line, sha1_text, truncate
from src.utils.time_buckets import bucket_start, parse_log_time

logger = get_logger("logmine.import")

MAX_SAMPLE_LINES = MAX_UPLOAD_LINES


def _known_templates(db: Session) -> list[dict]:
    rows = db.execute(
        select(
            LogTemplate.id,
            LogTemplate.template_str,
            LogTemplate.slot_types,
            LogTemplate.status,
            LogTemplate.sample_line,
            LogTemplate.frequency,
            LogTemplate.service,
            LogTemplate.level_hint,
            LogTemplate.signature,
        ).where(LogTemplate.is_noise.is_(False))
    ).all()
    return [
        {
            "template_id": row[0],
            "template_str": row[1],
            "slot_types": row[2],
            "status": row[3],
            "sample_line": row[4],
            "frequency": row[5],
            "service": row[6],
            "level_hint": row[7],
            "signature": row[8],
        }
        for row in rows
    ]


def _split_lines(text: str) -> list[str]:
    if text is None:
        raise AppError(codes.IMPORT_EMPTY, "上传内容为空", "text is None")
    raw = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = raw.split("\n")
    # 去掉文件末尾换行产生的空行（保留中间空行，它们是真实噪声样本）
    while lines and lines[-1] == "":
        lines.pop()
    if not lines:
        raise AppError(codes.IMPORT_EMPTY, "上传内容为空", "no line")
    if len(lines) > MAX_SAMPLE_LINES:
        raise AppError(codes.IMPORT_TOO_LARGE, f"一次最多导入 {MAX_SAMPLE_LINES} 行", str(len(lines)))
    return lines


def _source_resolver(sources: list[LogSource]):
    by_name = {source.name: source for source in sources}
    by_service = {source.service: source for source in sources}

    def resolve(service_hint: str) -> LogSource | None:
        if not service_hint:
            return None
        candidates = [service_hint, service_hint.replace(".v2", ""), service_hint.split(".")[0]]
        for candidate in candidates:
            if candidate in by_service:
                return by_service[candidate]
            if candidate in by_name:
                return by_name[candidate]
        for name, source in by_name.items():
            if service_hint.startswith(source.service) or service_hint.startswith(name):
                return source
        return None

    return resolve


def mine_preview(db: Session, text: str, *, persist: bool = False) -> dict:
    """挖掘预览：不落模板，只返回模板列表 / 覆盖率 / 噪声统计。"""
    started = time.perf_counter()
    lines = _split_lines(text)
    known = _known_templates(db)
    engine = drain_engine.DrainEngine()
    engine.load_known(known)
    result = engine.mine(lines)
    sources = list(db.execute(select(LogSource).order_by(LogSource.id)).scalars().all())
    resolve = _source_resolver(sources)

    preview_templates: list[dict] = []
    for index, template in enumerate(result.templates):
        source = resolve(template.service)
        item = template.to_dict()
        item["is_new"] = True
        item["suggested_source_id"] = source.id if source else None
        item["suggested_source_name"] = source.name if source else ""
        item["template_index"] = index
        preview_templates.append(item)

    payload = {
        "lines_total": result.lines_total,
        "covered_lines": result.covered_lines,
        "noise_lines": result.noise_lines,
        "tentative_lines": result.tentative_lines,
        "coverage": result.coverage,
        "coverage_percent": result.coverage_percent,
        "new_templates": result.new_templates,
        "templates": preview_templates,
        "known_hits": result.known_hits,
        "known_templates_used": result.known_templates_used,
        "elapsed_ms": int((time.perf_counter() - started) * 1000),
        "persisted": False,
        "line_signatures": result.line_signatures,
    }
    if persist:
        payload.update(apply_preview(db, lines, result, resolve))
    return payload


def apply_preview(db: Session, lines: list[str], result, resolve) -> dict:
    """把挖掘结果落库：新建模板 + 回填 pending 事件 + 幂等入库新行 + 重算槽位指标。"""
    created_templates = 0
    assigned = 0
    inserted = 0
    skipped = 0
    affected_pairs: set[tuple[int, datetime]] = set()
    template_by_signature: dict[str, LogTemplate] = {}
    existing = {
        template.signature: template
        for template in db.execute(select(LogTemplate)).scalars().all()
    }

    mined_templates = [item for item in result.templates if item.status == TemplateStatus.MINED.value]
    for template in mined_templates:
        template_row = existing.get(template.signature)
        if template_row is None:
            source = resolve(template.service)
            template_row = LogTemplate(
                signature=template.signature,
                template_str=template.template_str,
                token_count=template.token_count,
                slot_types=template.slot_types,
                slot_names=template.slot_names,
                source_id=source.id if source else None,
                service=template.service,
                sample_line=truncate(template.sample_line, 400),
                level_hint=template.level_hint,
                status=TemplateStatus.MINED.value,
                is_noise=False,
                frequency=template.sample_count,
                first_seen_at=now_local(),
                last_seen_at=now_local(),
                origin="import",
                confirmed_by="",
            )
            db.add(template_row)
            db.flush()
            existing[template.signature] = template_row
            created_templates += 1
        template_by_signature[template.signature] = template_row

    # 命中「库内已有模板」的行也要能拿到模板行：否则重复导入时这些行既不回填也不进指标，
    # 还会被误当新行插入（重复计数）。把全部已知模板并入签名映射。
    for signature, template_row in existing.items():
        template_by_signature.setdefault(signature, template_row)

    # 逐行回填：signature → 模板 id；同时把还没入库的行（噪声/新增行）按 line_hash 幂等写入
    sources = {source.id: source for source in db.execute(select(LogSource).order_by(LogSource.id)).scalars().all()}
    default_source = next(iter(sources.values()), None)
    line_signatures = result.line_signatures if len(result.line_signatures) == len(lines) else []
    pending_hashes = {
        row[0]: row[1]
        for row in db.execute(
            select(LogEvent.line_hash, LogEvent.id)
            .where(LogEvent.template_id.is_(None))
            .limit(5000)
        ).all()
    }

    fallback_time = db.execute(select(func.max(LogEvent.event_time))).scalar_one_or_none()
    if fallback_time is None:
        from src.utils.time_buckets import parse_anchor

        fallback_time = parse_anchor(settings.seed_anchor)

    seen_hashes: set[str] = set()
    for index, raw in enumerate(lines):
        signature = line_signatures[index] if index < len(line_signatures) else None
        template_row = template_by_signature.get(signature) if signature else None
        normalized = normalize_line(raw)
        source_key = template_row.service if template_row else (default_source.name if default_source else "import")
        resolved_source = resolve(source_key) if template_row else default_source
        hash_key = line_hash(resolved_source.name if resolved_source else "import", normalized)
        if hash_key in seen_hashes:
            skipped += 1
            continue
        seen_hashes.add(hash_key)
        level = drain_engine.detect_level(normalized) if normalized else EventLevel.INFO.value
        event = db.execute(select(LogEvent).where(LogEvent.line_hash == hash_key)).scalar_one_or_none()
        if event is None:
            event_time = fallback_time
            tokens = drain_engine.tokenize(normalized)
            if tokens:
                parsed = parse_log_time(tokens[0])
                if parsed is not None:
                    event_time = parsed
            named_slots = (
                slot_pipeline.named_slots_for_event(template_row, normalized) if template_row else {}
            )
            event = LogEvent(
                source_id=resolved_source.id if resolved_source else (default_source.id if default_source else 0),
                template_id=template_row.id if template_row else None,
                raw_line=normalized,
                line_hash=hash_key,
                fields={"tokens": len(tokens), "import_batch": "sample", "slots": named_slots},
                level=level,
                event_time=event_time,
                bucket_start=bucket_start(event_time),
                origin="import",
                service=resolved_source.service if resolved_source else "",
                ingest_batch=sha1_text(f"{len(lines)}:{index}")[:12],
            )
            db.add(event)
            db.flush()
            inserted += 1
            if template_row is not None:
                affected_pairs.add((template_row.id, event.bucket_start))
        else:
            # 行已在库中（pending 回填或此前已归类）：不新增事件，计入重复跳过
            skipped += 1
            if template_row is not None and event.template_id is None:
                event.template_id = template_row.id
                event.bucket_start = bucket_start(event.event_time)
                fields = dict(event.fields or {})
                fields["slots"] = slot_pipeline.named_slots_for_event(template_row, event.raw_line)
                event.fields = fields
                assigned += 1
                affected_pairs.add((template_row.id, event.bucket_start))
    # pending 回填：上面按 line_hash 命中已存在的 pending 事件；再补一次「已存在的 pending 行按签名匹配」
    for hash_key, event_id in pending_hashes.items():
        event = db.get(LogEvent, event_id)
        if event is None or event.template_id is not None:
            continue
        tokens = drain_engine.tokenize(event.raw_line)
        matched = _match_signature(tokens, mined_templates)
        if matched and matched in template_by_signature:
            matched_template = template_by_signature[matched]
            event.template_id = matched_template.id
            event.bucket_start = bucket_start(event.event_time)
            fields = dict(event.fields or {})
            fields["slots"] = slot_pipeline.named_slots_for_event(matched_template, event.raw_line)
            event.fields = fields
            assigned += 1
            affected_pairs.add((matched_template.id, event.bucket_start))

    # 模板频次 / 首末出现时间：按库里真实计数刷新（不是前端传入的数字）
    for template_row in template_by_signature.values():
        stats_row = db.execute(
            select(func.count(), func.min(LogEvent.event_time), func.max(LogEvent.event_time)).where(
                LogEvent.template_id == template_row.id
            )
        ).first()
        if stats_row is None or not stats_row[0]:
            continue
        template_row.frequency = int(stats_row[0] or 0)
        template_row.first_seen_at = stats_row[1]
        template_row.last_seen_at = stats_row[2]

    # 结构化管道：受影响的 (模板, 时间桶) 先删后建，重复导入/迟到回填都按原桶重算
    recompute = slot_pipeline.recompute_metrics(db, affected_pairs)
    db.flush()
    touched_template_ids = {template_id for template_id, _ in affected_pairs}
    logger.info(
        "drain.run lines=%s known=%s new_templates=%s coverage=%s recompute_buckets=%s metric_rows=%s",
        len(lines),
        result.known_hits,
        len(mined_templates),
        result.coverage,
        recompute["recomputed_buckets"],
        recompute["metric_rows"],
    )
    return {
        "created_templates": created_templates,
        "assigned_events": assigned,
        "inserted_events": inserted,
        "skipped_duplicates": skipped,
        "recomputed_buckets": recompute["recomputed_buckets"],
        "metric_rows": recompute["metric_rows"],
        "persisted": True,
        "template_ids": {
            template.template_str: template.id
            for template in template_by_signature.values()
            if template.id in touched_template_ids
        },
    }


def _match_signature(tokens: list[str], mined_templates: list) -> str | None:
    """按 token 序列在挖掘结果里找最相似模板（用于补回填）。"""
    best_signature: str | None = None
    best_similarity = 0.0
    for template in mined_templates:
        template_tokens = template.template_str.split(" ")
        if len(template_tokens) != len(tokens):
            continue
        mismatches = sum(
            1 for existing, incoming in zip(template_tokens, tokens) if existing != drain_engine.WILDCARD and existing != incoming
        )
        similarity = 1 - mismatches / len(template_tokens)
        if similarity > best_similarity:
            best_similarity = similarity
            best_signature = template.signature
    if best_similarity >= drain_engine.DEFAULT_SIMILARITY_THRESHOLD:
        return best_signature
    return None


def apply_text(db: Session, text: str, *, actor: str) -> dict:
    """重新挖掘并落库（前端先预览、确认后再应用；两次挖掘确定性一致）。

    批次幂等：content_fingerprint = 全文 sha256。同一批重复导入走同一条 import_batch，
    事件靠 line_hash 去重不新增、指标靠先删后建不重复计数，只刷新 last_run_at 与口径。
    """
    lines = _split_lines(text)
    fingerprint = hashlib.sha256(text.encode("utf-8")).hexdigest()
    known = _known_templates(db)
    engine = drain_engine.DrainEngine()
    engine.load_known(known)
    result = engine.mine(lines)
    sources = list(db.execute(select(LogSource).order_by(LogSource.id)).scalars().all())
    resolve = _source_resolver(sources)
    applied = apply_preview(db, lines, result, resolve)

    noise = result.noise_lines
    batch = db.execute(
        select(ImportBatch).where(ImportBatch.content_fingerprint == fingerprint)
    ).scalar_one_or_none()
    duplicate_import = batch is not None
    if batch is None:
        batch = ImportBatch(content_fingerprint=fingerprint, status="applied")
        db.add(batch)
    batch.rows_total = result.lines_total
    batch.inserted_events = applied["inserted_events"]
    batch.skipped_duplicates = applied["skipped_duplicates"]
    batch.noise_lines = noise
    batch.created_templates = applied["created_templates"]
    batch.recomputed_buckets = applied["recomputed_buckets"]
    batch.metric_rows = applied["metric_rows"]
    batch.actor = actor
    batch.last_run_at = now_local()
    db.flush()
    applied["batch_id"] = batch.id
    applied["duplicate_import"] = duplicate_import
    return {
        "lines_total": result.lines_total,
        "covered_lines": result.covered_lines,
        "noise_lines": result.noise_lines,
        "coverage": result.coverage,
        "coverage_percent": result.coverage_percent,
        "new_templates": result.new_templates,
        "known_hits": result.known_hits,
        "elapsed_ms": result.elapsed_ms,
        **applied,
    }


def sample_text() -> str:
    """返回确定性样本（249 行 = 240 新格式 + 9 噪声），与 seed 的 pending 事件逐字一致。"""
    from scripts import fixtures

    return fixtures.build_import_sample_text(settings.seed_anchor)


def list_batches(db: Session, *, limit: int = 50) -> list[dict]:
    """导入批次历史：同一指纹只保留一条，last_run_at 反映最近一次重复导入。"""
    rows = (
        db.execute(select(ImportBatch).order_by(ImportBatch.last_run_at.desc()).limit(limit))
        .scalars()
        .all()
    )
    return [
        {
            "id": row.id,
            "content_fingerprint": row.content_fingerprint[:12],
            "rows_total": row.rows_total,
            "inserted_events": row.inserted_events,
            "skipped_duplicates": row.skipped_duplicates,
            "noise_lines": row.noise_lines,
            "created_templates": row.created_templates,
            "recomputed_buckets": row.recomputed_buckets,
            "metric_rows": row.metric_rows,
            "status": row.status,
            "actor": row.actor,
            "last_run_at": row.last_run_at.isoformat(sep=" ") if row.last_run_at else None,
            "created_at": row.created_at.isoformat(sep=" ") if row.created_at else None,
        }
        for row in rows
    ]


def recompute_all(db: Session, *, actor: str) -> dict:
    """全量重算槽位指标（页面「重新计算」按钮）：先清空再按全部已归类事件重建。"""
    result = slot_pipeline.recompute_all_metrics(db)
    violations = slot_pipeline.verify_invariants(db)
    result["violations"] = len(violations)
    result["actor"] = actor
    logger.info("slot_metric.manual_recompute actor=%s result=%s", actor, result)
    return result


def sample_meta() -> dict:
    from scripts import fixtures

    return fixtures.summarize_plans()


def max_upload_bytes() -> int:
    return settings.max_upload_mb * 1024 * 1024 if settings.max_upload_mb else MAX_UPLOAD_MB_DEFAULT * 1024 * 1024
