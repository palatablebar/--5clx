"""结构化指标管道：事件 → 命名槽值 → (模板, 时间桶, 槽位, 槽值) 指标。

口径（与需求逐条对应）：
- 每条已归类事件按模板通配位提取命名槽值（drain_engine.extract_slot_values），
  并把 {槽名: 槽值} 写回 LogEvent.fields["slots"]，让每条日志都带结构化槽值；
- 缺值 / 空值 / 无法归类的 token 统一计入未识别分组（UNIDENTIFIED_SLOT_VALUE）；
- 指标表 slot_metric 按 (template_id, bucket_start, slot_index, slot_value) 唯一，
  重算走「先删后建」，天然幂等，重复导入不会重复计数；
- 迟到记录归入新模板后，按事件自身 event_time 对齐的原时间桶重算（bucket_start），
  而不是写入当前桶，保证历史走势被修正；
- 不变量：任一 (template_id, bucket_start) 内，每个槽位的分组数量之和 == 该桶事件数
  （未识别分组兜底），所有槽位分组数量之和 == 桶事件数 × 槽位数。
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from src.constants.enums import SlotType, UNIDENTIFIED_SLOT_VALUE
from src.models.log_event import LogEvent
from src.models.log_template import LogTemplate
from src.models.slot_metric import SlotMetric
from src.services import drain_engine
from src.utils.logger import get_logger
from src.utils.text import truncate
from src.utils.time_buckets import BUCKET_MINUTES, bucket_start, last_n_buckets

logger = get_logger("logmine.slot_pipeline")

SLOT_VALUE_MAX = 250


def _wildcard_indexes(template: LogTemplate) -> list[int]:
    return [index for index, token in enumerate(template.template_str.split(" ")) if token == drain_engine.WILDCARD]


def _slot_name(template: LogTemplate, index: int, fallback: str = "") -> str:
    names = template.slot_names or []
    if index < len(names) and names[index]:
        return str(names[index])
    return fallback or f"slot_{index}"


def _slot_type(template: LogTemplate, index: int, fallback: str = SlotType.UNKNOWN.value) -> str:
    types = template.slot_types or []
    if index < len(types) and types[index] and types[index] != "static":
        return str(types[index])
    return fallback


def named_slots_for_event(template: LogTemplate, raw_line: str) -> dict[str, str]:
    """提取一条事件的命名槽值映射 {槽名: 值}；缺值/未识别写未识别占位。"""
    extracted = drain_engine.extract_slot_values(raw_line, template.template_str, template.slot_names)
    if not extracted:
        return {
            _slot_name(template, index): UNIDENTIFIED_SLOT_VALUE for index in _wildcard_indexes(template)
        }
    result: dict[str, str] = {}
    for item in extracted:
        name = _slot_name(template, item["index"], item["name"])
        result[name] = UNIDENTIFIED_SLOT_VALUE if item["is_unidentified"] else item["value"]
    return result


def _bump(acc: dict, template_id: int, bucket: datetime, index: int, name: str, slot_type: str, item: dict) -> None:
    value = UNIDENTIFIED_SLOT_VALUE if item["is_unidentified"] else truncate(item["value"], SLOT_VALUE_MAX)
    key = (template_id, bucket, index, value)
    group = acc.get(key)
    if group is None:
        acc[key] = {
            "template_id": template_id,
            "bucket_start": bucket,
            "slot_index": index,
            "slot_name": name,
            "slot_type": slot_type if not item["is_unidentified"] else SlotType.UNKNOWN.value,
            "slot_value": value,
            "event_count": 1,
            "unidentified_count": 1 if item["is_unidentified"] else 0,
        }
    else:
        group["event_count"] += 1
        if item["is_unidentified"]:
            group["unidentified_count"] += 1


def _unidentified_item(index: int) -> dict:
    return {"index": index, "value": "", "type": SlotType.UNKNOWN.value, "is_unidentified": True}


def recompute_metrics(db: Session, pairs: set[tuple[int, datetime]]) -> dict:
    """重算受影响的 (模板, 时间桶) 集合：逐桶先删后建，幂等。

    同时把涉及事件的 fields["slots"] 刷新为命名槽值。返回统计口径。
    """
    if not pairs:
        return {"recomputed_buckets": 0, "metric_rows": 0, "events_scanned": 0}

    unique_pairs = {(template_id, bucket_start(bucket)) for template_id, bucket in pairs}
    templates = {
        row.id: row
        for row in db.execute(
            select(LogTemplate).where(LogTemplate.id.in_({template_id for template_id, _ in unique_pairs}))
        ).scalars()
    }

    events_scanned = 0
    rows_upserted = 0
    for template_id, bucket in sorted(unique_pairs, key=lambda item: (item[0], item[1])):
        template = templates.get(template_id)
        if template is None:
            continue
        db.execute(
            delete(SlotMetric).where(SlotMetric.template_id == template_id, SlotMetric.bucket_start == bucket)
        )
        events = (
            db.execute(
                select(LogEvent).where(LogEvent.template_id == template_id, LogEvent.bucket_start == bucket)
            )
            .scalars()
            .all()
        )
        wildcard_indexes = _wildcard_indexes(template)
        acc: dict[tuple, dict] = {}
        for event in events:
            events_scanned += 1
            extracted = drain_engine.extract_slot_values(event.raw_line, template.template_str, template.slot_names)
            if not extracted:
                extracted = [_unidentified_item(index) for index in wildcard_indexes]
            slots_payload: dict[str, str] = {}
            for item in extracted:
                index = item["index"]
                name = _slot_name(template, index, item.get("name", ""))
                slot_type = _slot_type(template, index, item["type"])
                _bump(acc, template_id, bucket, index, name, slot_type, item)
                slots_payload[name] = (
                    UNIDENTIFIED_SLOT_VALUE if item["is_unidentified"] else truncate(item["value"], SLOT_VALUE_MAX)
                )
            fields = dict(event.fields or {})
            if fields.get("slots") != slots_payload:
                fields["slots"] = slots_payload
                event.fields = fields
        for payload in acc.values():
            db.add(SlotMetric(**payload))
        rows_upserted += len(acc)
    db.flush()
    logger.info(
        "slot_metric.recompute pairs=%s events=%s rows=%s", len(unique_pairs), events_scanned, rows_upserted
    )
    return {
        "recomputed_buckets": len(unique_pairs),
        "metric_rows": rows_upserted,
        "events_scanned": events_scanned,
    }


def recompute_all_metrics(db: Session) -> dict:
    """全量重算：清空指标表后扫描全部已归类事件，一次性重建（seed / 兜底修复用）。"""
    db.execute(delete(SlotMetric))
    templates = {row.id: row for row in db.execute(select(LogTemplate)).scalars()}
    acc: dict[tuple, dict] = {}
    events_scanned = 0
    events = db.execute(
        select(LogEvent).where(LogEvent.template_id.is_not(None), LogEvent.bucket_start.is_not(None))
    ).scalars()
    for event in events:
        template = templates.get(event.template_id)
        if template is None:
            continue
        events_scanned += 1
        bucket = bucket_start(event.bucket_start or event.event_time)
        extracted = drain_engine.extract_slot_values(event.raw_line, template.template_str, template.slot_names)
        if not extracted:
            extracted = [_unidentified_item(index) for index in _wildcard_indexes(template)]
        slots_payload: dict[str, str] = {}
        for item in extracted:
            index = item["index"]
            name = _slot_name(template, index, item.get("name", ""))
            slot_type = _slot_type(template, index, item["type"])
            _bump(acc, template.id, bucket, index, name, slot_type, item)
            slots_payload[name] = (
                UNIDENTIFIED_SLOT_VALUE if item["is_unidentified"] else truncate(item["value"], SLOT_VALUE_MAX)
            )
        fields = dict(event.fields or {})
        if fields.get("slots") != slots_payload:
            fields["slots"] = slots_payload
            event.fields = fields
    db.add_all([SlotMetric(**payload) for payload in acc.values()])
    db.flush()
    logger.info("slot_metric.recompute_all events=%s rows=%s", events_scanned, len(acc))
    return {"recomputed_buckets": 0, "metric_rows": len(acc), "events_scanned": events_scanned, "full": True}


def verify_invariants(db: Session, *, template_id: int | None = None) -> list[dict]:
    """校验不变量：模板桶内各槽位分组数量之和必须对得上模板事件数。

    返回违例列表（空列表 = 全部对得上）。每条违例给出事件数、指标合计、差额。
    """
    event_rows = db.execute(
        select(LogEvent.template_id, LogEvent.bucket_start, func.count(LogEvent.id))
        .where(LogEvent.template_id.is_not(None), LogEvent.bucket_start.is_not(None))
        .group_by(LogEvent.template_id, LogEvent.bucket_start)
    ).all()
    metric_rows = db.execute(
        select(
            SlotMetric.template_id,
            SlotMetric.bucket_start,
            func.sum(SlotMetric.event_count),
        ).group_by(SlotMetric.template_id, SlotMetric.bucket_start)
    ).all()
    templates = {row.id: row for row in db.execute(select(LogTemplate)).scalars()}
    metric_index = {(row[0], bucket_start(row[1])): int(row[2] or 0) for row in metric_rows}
    violations: list[dict] = []
    for event_template_id, bucket, event_count in event_rows:
        if template_id is not None and event_template_id != template_id:
            continue
        template = templates.get(event_template_id)
        slot_count = len(_wildcard_indexes(template)) if template else 0
        key = (event_template_id, bucket_start(bucket))
        metric_sum = metric_index.get(key, 0)
        expected = int(event_count) * max(slot_count, 0)
        if metric_sum != expected:
            violations.append(
                {
                    "template_id": event_template_id,
                    "bucket_start": bucket_start(bucket).isoformat(sep=" "),
                    "event_count": int(event_count),
                    "slot_count": slot_count,
                    "metric_sum": metric_sum,
                    "expected_sum": expected,
                    "diff": metric_sum - expected,
                }
            )
    return violations


def slot_metric_series(
    db: Session,
    *,
    template_id: int,
    buckets: int = 24,
    slot_index: int | None = None,
    top_values: int = 8,
) -> dict:
    """查一个模板的槽位分组走势：按时间桶返回各槽位的槽值分组计数。"""
    template = db.get(LogTemplate, template_id)
    if template is None:
        return {"template_id": template_id, "buckets": [], "slots": [], "unidentified_total": 0}
    anchor_row = db.execute(
        select(func.max(LogEvent.bucket_start)).where(LogEvent.template_id == template_id)
    ).scalar_one_or_none()
    if anchor_row is None:
        return {"template_id": template_id, "buckets": [], "slots": [], "unidentified_total": 0}
    bucket_list = last_n_buckets(bucket_start(anchor_row), max(4, min(buckets, 288)))
    window_start = bucket_list[0]

    conditions = [
        SlotMetric.template_id == template_id,
        SlotMetric.bucket_start >= window_start,
    ]
    if slot_index is not None:
        conditions.append(SlotMetric.slot_index == slot_index)
    rows = db.execute(
        select(
            SlotMetric.slot_index,
            SlotMetric.slot_name,
            SlotMetric.slot_type,
            SlotMetric.slot_value,
            SlotMetric.bucket_start,
            SlotMetric.event_count,
            SlotMetric.unidentified_count,
        ).where(*conditions)
    ).all()

    by_slot: dict[int, dict] = {}
    unidentified_total = 0
    for s_index, name, s_type, value, bucket, count, unid in rows:
        slot = by_slot.setdefault(
            s_index,
            {
                "slot_index": s_index,
                "slot_name": name,
                "slot_type": s_type,
                "totals": defaultdict(int),
                "unidentified": 0,
                "groups": {},
            },
        )
        slot["totals"][value] += int(count or 0)
        slot["unidentified"] += int(unid or 0)
        unidentified_total += int(unid or 0)
        group = slot["groups"].setdefault(value, {item: 0 for item in bucket_list})
        group[bucket_start(bucket)] = int(count or 0)

    slots: list[dict] = []
    for s_index in sorted(by_slot):
        slot = by_slot[s_index]
        ranked = sorted(slot["totals"].items(), key=lambda item: (-item[1], item[0]))
        # 未识别分组必须保留（缺值可见性要求），其余按总量取 Top-N
        top: list[tuple[str, int]] = [item for item in ranked if item[0] == UNIDENTIFIED_SLOT_VALUE]
        for item in ranked:
            if len(top) >= top_values:
                break
            if item[0] != UNIDENTIFIED_SLOT_VALUE:
                top.append(item)
        top_values_set = {value for value, _ in top}
        groups = []
        for value, total in top:
            groups.append(
                {
                    "slot_value": value,
                    "total": total,
                    "is_unidentified": value == UNIDENTIFIED_SLOT_VALUE,
                    "series": [slot["groups"][value][item] for item in bucket_list],
                }
            )
        slots.append(
            {
                "slot_index": slot["slot_index"],
                "slot_name": slot["slot_name"],
                "slot_type": slot["slot_type"],
                "unidentified_count": slot["unidentified"],
                "groups": groups,
                "group_count": len(slot["totals"]),
                "shown_groups": len(groups),
                "top_values": list(top_values_set),
            }
        )
    return {
        "template_id": template_id,
        "template_str": template.template_str,
        "bucket_minutes": BUCKET_MINUTES,
        "anchor": bucket_list[-1].isoformat(sep=" "),
        "buckets": [item.isoformat(sep=" ") for item in bucket_list],
        "slots": slots,
        "unidentified_total": unidentified_total,
    }
