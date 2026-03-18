from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .mirror_framework import SyncFamily
from .models import (
    ERPMirrorArOpen,
    ERPMirrorArOpenDetail,
    ERPMirrorCustomer,
    ERPMirrorCustomerShipTo,
    ERPMirrorItem,
    ERPMirrorItemBranch,
    ERPMirrorItemUomConv,
    ERPMirrorPrintTransaction,
    ERPMirrorPrintTransactionDetail,
    ERPMirrorSalesOrderDetail,
    ERPMirrorSalesOrderHeader,
    ERPMirrorShipmentDetail,
    ERPMirrorShipmentHeader,
)


@dataclass(slots=True)
class ExtractorDefinition:
    name: str
    source_table: str
    target_table: str
    model: type
    family: SyncFamily
    cadence_seconds: int
    natural_keys: tuple[str, ...]
    column_map: dict[str, str]
    watermark_columns: tuple[str, ...]
    default_order_by: tuple[str, ...] | None = None


MASTER_DEFINITIONS = [
    ExtractorDefinition(
        name="cust",
        source_table="dbo.cust",
        target_table="erp_mirror_cust",
        model=ERPMirrorCustomer,
        family=SyncFamily.MASTER,
        cadence_seconds=300,
        natural_keys=("system_id", "cust_key"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "cust_key"),
        column_map={
            "system_id": "system_id",
            "cust_key": "cust_key",
            "cust_code": "cust_code",
            "cust_name": "cust_name",
            "phone": "phone",
            "email_address": "email",
            "current_balance": "balance",
            "credit_limit_amt": "credit_limit",
            "customer_class": "cust_type",
            "home_branch": "branch_code",
        },
    ),
    ExtractorDefinition(
        name="cust_shipto",
        source_table="dbo.cust_shipto",
        target_table="erp_mirror_cust_shipto",
        model=ERPMirrorCustomerShipTo,
        family=SyncFamily.MASTER,
        cadence_seconds=300,
        natural_keys=("system_id", "cust_key", "seq_num"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "cust_key", "seq_num"),
        column_map={
            "system_id": "system_id",
            "cust_key": "cust_key",
            "seq_num": "seq_num",
            "shipto_name": "shipto_name",
            "address_1": "address_1",
            "address_2": "address_2",
            "address_3": "address_3",
            "city": "city",
            "state": "state",
            "zip": "zip",
            "phone": "phone",
        },
    ),
    ExtractorDefinition(
        name="item",
        source_table="dbo.item",
        target_table="erp_mirror_item",
        model=ERPMirrorItem,
        family=SyncFamily.MASTER,
        cadence_seconds=300,
        natural_keys=("system_id", "item_ptr"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "item_ptr"),
        column_map={
            "system_id": "system_id",
            "item_ptr": "item_ptr",
            "item": "item",
            "description": "description",
            "size_": "size_",
            "type": "type",
            "stocking_uom": "stocking_uom",
            "temporary_": "temporary_",
        },
    ),
    ExtractorDefinition(
        name="item_branch",
        source_table="dbo.item_branch",
        target_table="erp_mirror_item_branch",
        model=ERPMirrorItemBranch,
        family=SyncFamily.MASTER,
        cadence_seconds=300,
        natural_keys=("system_id", "item_ptr"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "item_ptr"),
        column_map={
            "system_id": "system_id",
            "item_ptr": "item_ptr",
            "active_flag": "active_flag",
            "contentcode": "contentcode",
            "buyer_id": "buyer_id",
            "handling_code": "handling_code",
        },
    ),
    ExtractorDefinition(
        name="item_uomconv",
        source_table="dbo.item_uomconv",
        target_table="erp_mirror_item_uomconv",
        model=ERPMirrorItemUomConv,
        family=SyncFamily.MASTER,
        cadence_seconds=300,
        natural_keys=("system_id", "item_ptr", "uom_ptr"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "item_ptr", "uom_ptr"),
        column_map={
            "system_id": "system_id",
            "item_ptr": "item_ptr",
            "uom_ptr": "uom_ptr",
            "created_by": "created_by",
            "created_date": "created_date",
            "update_by": "update_by",
            "update_date": "update_date",
            "update_time": "update_time",
            "created_time": "created_time",
            "conv_factor_from_stocking": "conv_factor_from_stocking",
        },
    ),
]


OPERATIONAL_DEFINITIONS = [
    ExtractorDefinition(
        name="so_header",
        source_table="dbo.so_header",
        target_table="erp_mirror_so_header",
        model=ERPMirrorSalesOrderHeader,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("system_id", "so_id"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "so_id"),
        column_map={
            "system_id": "system_id",
            "so_id": "so_id",
            "so_status": "so_status",
            "sale_type": "sale_type",
            "cust_key": "cust_key",
            "shipto_seq_num": "shipto_seq_num",
            "reference": "reference",
            "expect_date": "expect_date",
            "created_date": "created_date",
            "ship_via": "ship_via",
            "route_id_char": "branch_code",
            "driver": "salesperson",
            "cust_po": "po_number",
        },
    ),
    ExtractorDefinition(
        name="so_detail",
        source_table="dbo.so_detail",
        target_table="erp_mirror_so_detail",
        model=ERPMirrorSalesOrderDetail,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("system_id", "so_id", "sequence"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "so_id", "sequence"),
        column_map={
            "system_id": "system_id",
            "so_id": "so_id",
            "sequence": "sequence",
            "item_ptr": "item_ptr",
            "qty_ordered": "qty_ordered",
            "size_": "size_",
            "so_desc": "so_desc",
            "price": "price",
            "price_uom_ptr": "price_uom_ptr",
            "bo": "bo",
        },
    ),
    ExtractorDefinition(
        name="shipments_header",
        source_table="dbo.shipments_header",
        target_table="erp_mirror_shipments_header",
        model=ERPMirrorShipmentHeader,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("system_id", "so_id", "shipment_num"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "so_id", "shipment_num"),
        column_map={
            "system_id": "system_id",
            "so_id": "so_id",
            "shipment_num": "shipment_num",
            "ship_date": "ship_date",
            "billed_flag": "billed_flag",
            "status_flag": "status_flag",
            "route_id_char": "route_id_char",
            "print_status": "print_status",
            "invoice_date": "invoice_date",
            "expect_date": "expect_date",
            "loaded_date": "loaded_date",
            "loaded_time": "loaded_time",
            "driver": "driver",
            "status_flag_delivery": "status_flag_delivery",
            "ship_via": "ship_via",
        },
    ),
    ExtractorDefinition(
        name="shipments_detail",
        source_table="dbo.shipments_detail",
        target_table="erp_mirror_shipments_detail",
        model=ERPMirrorShipmentDetail,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("system_id", "so_id", "shipment_num", "sequence"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "so_id", "shipment_num", "sequence"),
        column_map={
            "system_id": "system_id",
            "so_id": "so_id",
            "shipment_num": "shipment_num",
            "sequence": "sequence",
            "qty": "qty",
            "price": "price",
            "item_ptr": "item_ptr",
        },
    ),
]


AR_DEFINITIONS = [
    ExtractorDefinition(
        name="aropen",
        source_table="dbo.aropen",
        target_table="erp_mirror_aropen",
        model=ERPMirrorArOpen,
        family=SyncFamily.AR,
        cadence_seconds=300,
        natural_keys=("system_id", "ref_num", "ref_num_seq"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "ref_num", "ref_num_seq"),
        column_map={
            "system_id": "system_id",
            "ref_num": "ref_num",
            "ref_num_seq": "ref_num_seq",
            "cust_key": "cust_key",
            "ref_type": "ref_type",
            "ref_date": "ref_date",
            "update_date": "update_date",
            "amount": "amount",
            "open_amt": "open_amt",
            "shipto_seq": "shipto_seq",
            "statement_id": "statement_id",
            "discount_amt": "discount_amt",
            "discount_taken": "discount_taken",
            "ref_num_sysid": "ref_num_sysid",
            "paid_in_full_date": "paid_in_full_date",
            "open_flag": "open_flag",
        },
    ),
    ExtractorDefinition(
        name="aropendt",
        source_table="dbo.aropendt",
        target_table="erp_mirror_aropendt",
        model=ERPMirrorArOpenDetail,
        family=SyncFamily.AR,
        cadence_seconds=300,
        natural_keys=("system_id", "ref_num", "ref_num_seq"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "ref_num", "ref_num_seq"),
        column_map={
            "system_id": "system_id",
            "ref_num": "ref_num",
            "ref_num_seq": "ref_num_seq",
            "cust_key": "cust_key",
            "ref_type": "ref_type",
            "amount": "amount",
            "open_amt": "open_amt",
            "due_date": "due_date",
            "merch_amt": "merch_amt",
            "open_merch_amt": "open_merch_amt",
            "sales_tax_amt": "sales_tax_amt",
            "sales_tax_open_amt": "sales_tax_open_amt",
            "fin_charge_amt": "fin_charge_amt",
            "fin_charge_open": "fin_charge_open",
            "discount_date": "discount_date",
            "discount_amount": "discount_amount",
            "discount_amount_open": "discount_amount_open",
            "write_off_amt": "write_off_amt",
            "write_off_reason": "write_off_reason",
            "shipto_seq": "shipto_seq",
            "shipment_num": "shipment_num",
            "tran_id": "tran_id",
            "open_flag": "open_flag",
        },
    ),
]


DOCUMENT_DEFINITIONS = [
    ExtractorDefinition(
        name="print_transaction",
        source_table="dbo.print_transaction",
        target_table="erp_mirror_print_transaction",
        model=ERPMirrorPrintTransaction,
        family=SyncFamily.DOCUMENT,
        cadence_seconds=300,
        natural_keys=("system_id", "tran_id", "tran_type", "seq_num"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "tran_id", "tran_type", "seq_num"),
        column_map={
            "system_id": "system_id",
            "tran_id": "tran_id",
            "tran_type": "tran_type",
            "seq_num": "seq_num",
            "rpt_job": "rpt_job",
            "rpt_key": "rpt_key",
            "created_by": "created_by",
            "created_date": "created_date",
            "update_by": "update_by",
            "update_date": "update_date",
            "update_time": "update_time",
            "processed": "processed",
            "branch": "branch",
            "last_print_record": "last_print_record",
            "cust_key": "cust_key",
            "invoice_date": "invoice_date",
            "original_print_date": "original_print_date",
            "prev_print_status": "prev_print_status",
            "tran_seq": "tran_seq",
            "shipment_num": "shipment_num",
            "num_labels": "num_labels",
            "pick_id": "pick_id",
            "pick_seq": "pick_seq",
            "print_file_name": "print_file_name",
        },
    ),
    ExtractorDefinition(
        name="print_transaction_detail",
        source_table="dbo.print_transaction_detail",
        target_table="erp_mirror_print_transaction_detail",
        model=ERPMirrorPrintTransactionDetail,
        family=SyncFamily.DOCUMENT,
        cadence_seconds=300,
        natural_keys=("system_id", "tran_id", "printer_id", "printer_destination"),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("system_id", "tran_id", "printer_id", "printer_destination"),
        column_map={
            "system_id": "system_id",
            "tran_id": "tran_id",
            "printer_id": "printer_id",
            "printer_destination": "printer_destination",
            "created_by": "created_by",
            "created_time": "created_time",
            "update_by": "update_by",
            "update_time": "update_time",
            "update_date": "update_date",
            "created_date": "created_date",
            "rpt_job": "rpt_job",
            "rpt_key": "rpt_key",
            "num_copies": "num_copies",
            "form_footer": "form_footer",
        },
    ),
]


FIRST_SYNC_DEFINITIONS = MASTER_DEFINITIONS + OPERATIONAL_DEFINITIONS + AR_DEFINITIONS + DOCUMENT_DEFINITIONS


def current_utc_batch_values() -> dict[str, Any]:
    return {}


def definitions_for_family(family_name: str) -> list[ExtractorDefinition]:
    return [definition for definition in FIRST_SYNC_DEFINITIONS if definition.family.value == family_name]


def definitions_for_names(names: list[str]) -> list[ExtractorDefinition]:
    wanted = set(names)
    return [definition for definition in FIRST_SYNC_DEFINITIONS if definition.name in wanted]
