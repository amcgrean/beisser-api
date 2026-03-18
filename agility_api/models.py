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
    __table_args__ = (UniqueConstraint("system_id", "cust_key", name="uq_erp_mirror_cust_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
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


class ERPMirrorCustomerShipTo(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_cust_shipto"
    __table_args__ = (UniqueConstraint("system_id", "cust_key", "seq_num", name="uq_erp_mirror_cust_shipto_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
    cust_key: Mapped[str] = mapped_column(String(64), index=True)
    seq_num: Mapped[int] = mapped_column(Integer)
    shipto_name: Mapped[str | None] = mapped_column(String(255))
    address_1: Mapped[str | None] = mapped_column(String(255))
    address_2: Mapped[str | None] = mapped_column(String(255))
    address_3: Mapped[str | None] = mapped_column(String(255))
    city: Mapped[str | None] = mapped_column(String(128))
    state: Mapped[str | None] = mapped_column(String(32))
    zip: Mapped[str | None] = mapped_column(String(32))
    phone: Mapped[str | None] = mapped_column(String(64))
    branch_code: Mapped[str | None] = mapped_column(String(32), index=True)


class ERPMirrorItem(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_item"
    __table_args__ = (UniqueConstraint("system_id", "item_ptr", name="uq_erp_mirror_item_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
    item_ptr: Mapped[int] = mapped_column(Integer, index=True)
    item: Mapped[str] = mapped_column(String(128), index=True)
    description: Mapped[str | None] = mapped_column(String(255))
    size_: Mapped[str | None] = mapped_column(String(64))
    type: Mapped[str | None] = mapped_column(String(64))
    stocking_uom: Mapped[str | None] = mapped_column(String(32))
    temporary_: Mapped[bool | None] = mapped_column(Boolean)


class ERPMirrorItemBranch(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_item_branch"
    __table_args__ = (UniqueConstraint("system_id", "item_ptr", name="uq_erp_mirror_item_branch_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
    item_ptr: Mapped[int] = mapped_column(Integer, index=True)
    active_flag: Mapped[bool | None] = mapped_column(Boolean)
    contentcode: Mapped[str | None] = mapped_column(String(64))
    buyer_id: Mapped[str | None] = mapped_column(String(64))
    handling_code: Mapped[str | None] = mapped_column(String(64))


class ERPMirrorItemUomConv(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_item_uomconv"
    __table_args__ = (UniqueConstraint("system_id", "item_ptr", "uom_ptr", name="uq_erp_mirror_item_uomconv_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
    item_ptr: Mapped[int] = mapped_column(Integer, index=True)
    uom_ptr: Mapped[str] = mapped_column(String(64))
    created_by: Mapped[str | None] = mapped_column(String(64))
    created_date: Mapped[datetime | None] = mapped_column(DateTime)
    update_by: Mapped[str | None] = mapped_column(String(64))
    update_date: Mapped[datetime | None] = mapped_column(DateTime)
    update_time: Mapped[str | None] = mapped_column(String(32))
    created_time: Mapped[str | None] = mapped_column(String(32))
    conv_factor_from_stocking: Mapped[float | None] = mapped_column(Numeric(18, 6))


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


class ERPMirrorSalesOrderDetail(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_so_detail"
    __table_args__ = (UniqueConstraint("system_id", "so_id", "sequence", name="uq_erp_mirror_so_detail_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
    so_id: Mapped[int] = mapped_column(Integer, index=True)
    sequence: Mapped[int] = mapped_column(Integer)
    item_ptr: Mapped[int | None] = mapped_column(Integer, index=True)
    qty_ordered: Mapped[float | None] = mapped_column(Numeric(18, 4))
    size_: Mapped[str | None] = mapped_column(String(64))
    so_desc: Mapped[str | None] = mapped_column(String(255))
    price: Mapped[float | None] = mapped_column(Numeric(18, 4))
    price_uom_ptr: Mapped[str | None] = mapped_column(String(64))
    bo: Mapped[float | None] = mapped_column(Numeric(18, 4))


class ERPMirrorShipmentHeader(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_shipments_header"
    __table_args__ = (UniqueConstraint("system_id", "so_id", "shipment_num", name="uq_erp_mirror_shipments_header_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
    so_id: Mapped[int] = mapped_column(Integer, index=True)
    shipment_num: Mapped[int] = mapped_column(Integer)
    ship_date: Mapped[datetime | None] = mapped_column(DateTime)
    billed_flag: Mapped[str | None] = mapped_column(String(16))
    status_flag: Mapped[str | None] = mapped_column(String(16))
    route_id_char: Mapped[str | None] = mapped_column(String(64))
    print_status: Mapped[str | None] = mapped_column(String(64))
    invoice_date: Mapped[datetime | None] = mapped_column(DateTime)
    expect_date: Mapped[datetime | None] = mapped_column(DateTime)
    loaded_date: Mapped[datetime | None] = mapped_column(DateTime)
    loaded_time: Mapped[str | None] = mapped_column(String(32))
    driver: Mapped[str | None] = mapped_column(String(128))
    status_flag_delivery: Mapped[str | None] = mapped_column(String(16))
    ship_via: Mapped[str | None] = mapped_column(String(128))


class ERPMirrorShipmentDetail(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_shipments_detail"
    __table_args__ = (UniqueConstraint("system_id", "so_id", "shipment_num", "sequence", name="uq_erp_mirror_shipments_detail_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
    so_id: Mapped[int] = mapped_column(Integer, index=True)
    shipment_num: Mapped[int] = mapped_column(Integer)
    sequence: Mapped[int] = mapped_column(Integer)
    qty: Mapped[float | None] = mapped_column(Numeric(18, 4))
    price: Mapped[float | None] = mapped_column(Numeric(18, 4))
    item_ptr: Mapped[int | None] = mapped_column(Integer, index=True)


class ERPMirrorArOpen(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_aropen"
    __table_args__ = (UniqueConstraint("system_id", "ref_num", "ref_num_seq", name="uq_erp_mirror_aropen_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
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
    ref_num_seq: Mapped[int | None] = mapped_column(Integer)
    paid_in_full_date: Mapped[datetime | None] = mapped_column(DateTime)
    open_flag: Mapped[bool | None] = mapped_column(Boolean)


class ERPMirrorArOpenDetail(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_aropendt"
    __table_args__ = (UniqueConstraint("system_id", "ref_num", "ref_num_seq", name="uq_erp_mirror_aropendt_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
    ref_num: Mapped[str] = mapped_column(String(64), index=True)
    ref_num_seq: Mapped[int] = mapped_column(Integer)
    cust_key: Mapped[str | None] = mapped_column(String(64), index=True)
    ref_type: Mapped[str | None] = mapped_column(String(16))
    amount: Mapped[float | None] = mapped_column(Numeric(18, 2))
    open_amt: Mapped[float | None] = mapped_column(Numeric(18, 2))
    due_date: Mapped[datetime | None] = mapped_column(DateTime)
    merch_amt: Mapped[float | None] = mapped_column(Numeric(18, 2))
    open_merch_amt: Mapped[float | None] = mapped_column(Numeric(18, 2))
    sales_tax_amt: Mapped[float | None] = mapped_column(Numeric(18, 2))
    sales_tax_open_amt: Mapped[float | None] = mapped_column(Numeric(18, 2))
    fin_charge_amt: Mapped[float | None] = mapped_column(Numeric(18, 2))
    fin_charge_open: Mapped[float | None] = mapped_column(Numeric(18, 2))
    discount_date: Mapped[datetime | None] = mapped_column(DateTime)
    discount_amount: Mapped[float | None] = mapped_column(Numeric(18, 2))
    discount_amount_open: Mapped[float | None] = mapped_column(Numeric(18, 2))
    write_off_amt: Mapped[float | None] = mapped_column(Numeric(18, 2))
    write_off_reason: Mapped[str | None] = mapped_column(String(128))
    shipto_seq: Mapped[int | None] = mapped_column(Integer)
    shipment_num: Mapped[int | None] = mapped_column(Integer)
    tran_id: Mapped[int | None] = mapped_column(Integer, index=True)
    open_flag: Mapped[bool | None] = mapped_column(Boolean)


class ERPMirrorPrintTransaction(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_print_transaction"
    __table_args__ = (
        UniqueConstraint("system_id", "tran_id", "tran_type", "seq_num", name="uq_erp_mirror_print_transaction_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
    tran_id: Mapped[str] = mapped_column(String(64), index=True)
    tran_type: Mapped[str] = mapped_column(String(32))
    seq_num: Mapped[int] = mapped_column(Integer)
    rpt_job: Mapped[int | None] = mapped_column(Integer)
    rpt_key: Mapped[int | None] = mapped_column(Integer)
    created_by: Mapped[str | None] = mapped_column(String(64))
    created_date: Mapped[datetime | None] = mapped_column(DateTime)
    update_by: Mapped[str | None] = mapped_column(String(64))
    update_date: Mapped[datetime | None] = mapped_column(DateTime)
    update_time: Mapped[str | None] = mapped_column(String(32))
    processed: Mapped[bool | None] = mapped_column(Boolean)
    branch: Mapped[str | None] = mapped_column(String(32), index=True)
    last_print_record: Mapped[bool | None] = mapped_column(Boolean)
    cust_key: Mapped[str | None] = mapped_column(String(64), index=True)
    invoice_date: Mapped[datetime | None] = mapped_column(DateTime)
    original_print_date: Mapped[datetime | None] = mapped_column(DateTime)
    prev_print_status: Mapped[str | None] = mapped_column(String(32))
    tran_seq: Mapped[int | None] = mapped_column(Integer)
    shipment_num: Mapped[int | None] = mapped_column(Integer)
    num_labels: Mapped[int | None] = mapped_column(Integer)
    pick_id: Mapped[int | None] = mapped_column(Integer)
    pick_seq: Mapped[int | None] = mapped_column(Integer)
    print_file_name: Mapped[str | None] = mapped_column(String(255))


class ERPMirrorPrintTransactionDetail(Base, MirrorSyncMetadataMixin):
    __tablename__ = "erp_mirror_print_transaction_detail"
    __table_args__ = (
        UniqueConstraint(
            "system_id",
            "tran_id",
            "printer_id",
            "printer_destination",
            name="uq_erp_mirror_print_transaction_detail_key",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    system_id: Mapped[str | None] = mapped_column(String(32), index=True)
    tran_id: Mapped[str] = mapped_column(String(64), index=True)
    printer_id: Mapped[str] = mapped_column(String(64))
    printer_destination: Mapped[str] = mapped_column(String(128))
    created_by: Mapped[str | None] = mapped_column(String(64))
    created_time: Mapped[str | None] = mapped_column(String(32))
    update_by: Mapped[str | None] = mapped_column(String(64))
    update_time: Mapped[str | None] = mapped_column(String(32))
    update_date: Mapped[datetime | None] = mapped_column(DateTime)
    created_date: Mapped[datetime | None] = mapped_column(DateTime)
    rpt_job: Mapped[int | None] = mapped_column(Integer)
    rpt_key: Mapped[int | None] = mapped_column(Integer)
    num_copies: Mapped[int | None] = mapped_column(Integer)
    form_footer: Mapped[str | None] = mapped_column(String(255))
