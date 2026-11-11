"""导入 / 采样 / 挖掘预览 / 应用 / 批次历史 / 指标重算。"""
from __future__ import annotations

from fastapi import Request
from sqlalchemy.orm import Session

from src.constants.messages import MSG_IMPORT_APPLIED, MSG_IMPORT_PREVIEW, MSG_SLOT_RECOMPUTED
from src.middlewares.auth import current_ip
from src.models.user import User
from src.services import audit_service, import_service


def get_sample() -> str:
    return import_service.sample_text()


def sample_meta() -> dict:
    return import_service.sample_meta()


def list_batches(db: Session) -> dict:
    items = import_service.list_batches(db)
    return {"items": items, "total": len(items)}


def recompute(db: Session, user: User, request: Request) -> dict:
    result = import_service.recompute_all(db, actor=user.email)
    audit_service.record(
        db,
        user=user,
        action="events.import_recompute",
        entity="slot_metric",
        entity_id=str(result.get("metric_rows", 0)),
        detail={
            "events_scanned": result.get("events_scanned"),
            "metric_rows": result.get("metric_rows"),
            "violations": result.get("violations"),
        },
        client_ip=current_ip(request),
    )
    db.commit()
    result["message"] = MSG_SLOT_RECOMPUTED
    return result


def mine(db: Session, text: str, apply: bool, user: User, request: Request) -> dict:
    if apply:
        result = import_service.apply_text(db, text, actor=user.email)
        audit_service.record(
            db,
            user=user,
            action="events.import_apply",
            entity="log_template",
            entity_id=str(result.get("created_templates", 0)),
            detail={
                "lines": result.get("lines_total"),
                "new_templates": result.get("new_templates"),
                "coverage": result.get("coverage"),
                "assigned_events": result.get("assigned_events"),
                "inserted_events": result.get("inserted_events"),
                "recomputed_buckets": result.get("recomputed_buckets"),
                "metric_rows": result.get("metric_rows"),
                "duplicate_import": result.get("duplicate_import"),
            },
            client_ip=current_ip(request),
        )
        db.commit()
        result["message"] = MSG_IMPORT_APPLIED
        return result
    result = import_service.mine_preview(db, text)
    result["message"] = MSG_IMPORT_PREVIEW
    return result
