from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base, TimestampMixin, now_local


class ImportBatch(Base, TimestampMixin):
    """导入批次：一次「应用」一行记录。

    自然键 = content_fingerprint（上传全文的 sha256），同一批重复导入只更新 last_run_at，
    不新增事件、不重复计数；rows_total/inserted/skipped 记录最近一次管道运行的口径。
    """

    __tablename__ = "import_batch"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    content_fingerprint: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    rows_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    inserted_events: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    skipped_duplicates: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    noise_lines: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_templates: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    recomputed_buckets: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    metric_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="applied", index=True)
    actor: Mapped[str] = mapped_column(String(180), nullable=False, default="")
    last_run_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local)
    note: Mapped[str] = mapped_column(Text, nullable=False, default="")
