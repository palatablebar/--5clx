"""事件检索与趋势路由。"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.config.database import get_db
from src.controllers import event_controller
from src.middlewares.rate_limit import guard

router = APIRouter(prefix="/events", tags=["events"])


@router.get("", dependencies=[guard("read")])
def search(
    template_id: int | None = None,
    source_id: int | None = None,
    level: str | None = None,
    search: str | None = None,
    pending_only: bool = False,
    start: str | None = None,
    end: str | None = None,
    page: int = 1,
    page_size: int = 50,
    db: Session = Depends(get_db),
) -> dict:
    return event_controller.search(
        db,
        template_id=template_id,
        source_id=source_id,
        level=level,
        search=search,
        pending_only=pending_only,
        start=start,
        end=end,
        page=page,
        page_size=page_size,
    )


@router.get("/trend", dependencies=[guard("read")])
def trend(buckets: int = 36, template_id: int | None = None, db: Session = Depends(get_db)) -> dict:
    return event_controller.trend(db, buckets=buckets, template_id=template_id)


@router.get("/aggregate", dependencies=[guard("read")])
def aggregate(top: int = 12, db: Session = Depends(get_db)) -> dict:
    return event_controller.aggregate(db, top=top)


@router.get("/levels", dependencies=[guard("read")])
def levels(template_id: int | None = None, db: Session = Depends(get_db)) -> dict:
    return event_controller.levels(db, template_id=template_id)


@router.get("/slots", dependencies=[guard("read")])
def slots(
    template_id: int,
    buckets: int = 24,
    slot_index: int | None = None,
    top_values: int = 8,
    db: Session = Depends(get_db),
) -> dict:
    return event_controller.slots(
        db, template_id=template_id, buckets=buckets, slot_index=slot_index, top_values=top_values
    )


@router.get("/slots/invariants", dependencies=[guard("read")])
def slot_invariants(template_id: int | None = None, db: Session = Depends(get_db)) -> dict:
    return event_controller.slot_invariants(db, template_id=template_id)
