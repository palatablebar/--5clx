from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base, TimestampMixin, now_local


class SlotMetric(Base, TimestampMixin):
    """槽位分组指标：按 (模板, 时间桶, 槽位, 槽值) 计数。

    - 一条已归类事件在每个通配槽位上各产生一行分组（缺值/空值记为未识别分组）；
    - 同一 (template_id, bucket_start, slot_index, slot_value) 唯一，重算走先删后建；
    - 不变量：任一 (template_id, bucket_start) 下所有槽位分组 count 之和
      == 该模板该桶的事件数（每条事件 × 槽位数）。
    """

    __tablename__ = "slot_metric"
    __table_args__ = (
        Index(
            "ux_slot_metric_group",
            "template_id",
            "bucket_start",
            "slot_index",
            "slot_value",
            unique=True,
        ),
        Index("ix_slot_metric_template_bucket", "template_id", "bucket_start"),
        Index("ix_slot_metric_bucket", "bucket_start"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    template_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("log_template.id", ondelete="CASCADE"), nullable=False
    )
    bucket_start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    slot_index: Mapped[int] = mapped_column(Integer, nullable=False)
    slot_name: Mapped[str] = mapped_column(String(80), nullable=False, default="")
    slot_type: Mapped[str] = mapped_column(String(16), nullable=False, default="unknown")
    slot_value: Mapped[str] = mapped_column(String(255), nullable=False)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unidentified_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=now_local, onupdate=now_local, nullable=False
    )
