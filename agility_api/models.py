from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class MirrorSyncMetadataMixin:
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime, index=True)
    synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    sync_batch_id: Mapped[str | None] = mapped_column(String(64), index=True)
    row_fingerprint: Mapped[str | None] = mapped_column(String(64))
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, index=True)


class ERPSyncState(Base):
    __tablename__ = "erp_sync_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    worker_name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    worker_mode: Mapped[str] = mapped_column(String(50), default="pi")
    source_mode: Mapped[str] = mapped_column(String(50), default="local_sql")
    target_mode: Mapped[str] = mapped_column(String(50), default="mirror")
    interval_seconds: Mapped[int] = mapped_column(Integer, default=5)
    change_monitoring: Mapped[bool] = mapped_column(Boolean, default=True)
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_status: Mapped[str] = mapped_column(String(50), default="starting")
    last_error: Mapped[str | None] = mapped_column(Text)
    last_change_token: Mapped[str | None] = mapped_column(String(128))
    last_payload_hash: Mapped[str | None] = mapped_column(String(128))
    last_push_reason: Mapped[str | None] = mapped_column(String(128))
    last_counts_json: Mapped[str | None] = mapped_column(Text)


class ERPSyncBatch(Base):
    __tablename__ = "erp_sync_batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    worker_name: Mapped[str] = mapped_column(String(128), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String(32), default="running")
    family: Mapped[str | None] = mapped_column(String(32), index=True)
    table_count: Mapped[int] = mapped_column(Integer, default=0)
    rows_extracted: Mapped[int] = mapped_column(Integer, default=0)
    rows_staged: Mapped[int] = mapped_column(Integer, default=0)
    rows_upserted: Mapped[int] = mapped_column(Integer, default=0)
    rows_deleted: Mapped[int] = mapped_column(Integer, default=0)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)


class ERPSyncTableState(Base):
    __tablename__ = "erp_sync_table_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    table_name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    family: Mapped[str] = mapped_column(String(32), index=True)
    strategy: Mapped[str] = mapped_column(String(32))
    last_batch_id: Mapped[str | None] = mapped_column(String(64), index=True)
    last_status: Mapped[str] = mapped_column(String(32), default="pending")
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_error: Mapped[str | None] = mapped_column(Text)
    last_source_updated_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_row_count: Mapped[int] = mapped_column(Integer, default=0)
    last_duration_ms: Mapped[int] = mapped_column(Integer, default=0)


class ERPMirrorCustomer(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_cust"
    __table_args__ = (UniqueConstraint("cust_key", name="uq_erp_mirror_cust_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cust_key: Mapped[str] = mapped_column(String(64), index=True)
    cust_code: Mapped[str] = mapped_column(String(64), index=True)
    cust_name: Mapped[str | None] = mapped_column(String(255))
    phone: Mapped[str | None] = mapped_column(String(64))
    email: Mapped[str | None] = mapped_column(String(255))
    balance: Mapped[float | None] = mapped_column(Numeric(18, 2))
    credit_limit: Mapped[float | None] = mapped_column(Numeric(18, 2))
    credit_account: Mapped[bool | None] = mapped_column(Boolean)
    cust_type: Mapped[str | None] = mapped_column(String(32))
    branch_code: Mapped[str | None] = mapped_column(String(32), index=True)


class ERPMirrorSalesOrderHeader(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_so_header"
    __table_args__ = (UniqueConstraint("system_id", "so_id", name="uq_erp_mirror_so_header_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str] = mapped_column(String(32), index=True)
    so_id: Mapped[str] = mapped_column(String(64), index=True)
    so_status: Mapped[str | None] = mapped_column(String(16), index=True)
    sale_type: Mapped[str | None] = mapped_column(String(32))
    cust_key: Mapped[str | None] = mapped_column(String(64), index=True)
    shipto_seq_num: Mapped[str | None] = mapped_column(String(32))
    reference: Mapped[str | None] = mapped_column(String(255))
    expect_date: Mapped[datetime | None] = mapped_column(DateTime)
    created_date: Mapped[datetime | None] = mapped_column(DateTime)
    invoice_date: Mapped[datetime | None] = mapped_column(DateTime)
    ship_date: Mapped[datetime | None] = mapped_column(DateTime)
    promise_date: Mapped[datetime | None] = mapped_column(DateTime)
    ship_via: Mapped[str | None] = mapped_column(String(128))
    terms: Mapped[str | None] = mapped_column(String(64))
    salesperson: Mapped[str | None] = mapped_column(String(64))
    po_number: Mapped[str | None] = mapped_column(String(128))
    branch_code: Mapped[str | None] = mapped_column(String(32), index=True)


class ERPMirrorArOpen(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_aropen"
    __table_args__ = (UniqueConstraint("ref_num", name="uq_erp_mirror_aropen_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ref_num: Mapped[str] = mapped_column(String(64), index=True)
    cust_key: Mapped[str | None] = mapped_column(String(64), index=True)
    ref_date: Mapped[datetime | None] = mapped_column(DateTime)
    update_date: Mapped[datetime | None] = mapped_column(DateTime)
    amount: Mapped[float | None] = mapped_column(Numeric(18, 2))
    open_amt: Mapped[float | None] = mapped_column(Numeric(18, 2))
    ref_type: Mapped[str | None] = mapped_column(String(16))
    shipto_seq: Mapped[str | None] = mapped_column(String(32))
    statement_id: Mapped[str | None] = mapped_column(String(64))
    discount_amt: Mapped[float | None] = mapped_column(Numeric(18, 2))
    discount_taken: Mapped[float | None] = mapped_column(Numeric(18, 2))
    ref_num_sysid: Mapped[str | None] = mapped_column(String(32), index=True)
    paid_in_full_date: Mapped[datetime | None] = mapped_column(DateTime)
    open_flag: Mapped[bool | None] = mapped_column(Boolean)
