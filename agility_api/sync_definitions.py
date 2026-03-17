from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .mirror_framework import SyncFamily
from .models import (
    ERPMirrorCustomer,
    ERPMirrorCustomerShipTo,
    ERPMirrorItem,
    ERPMirrorItemBranch,
    ERPMirrorItemUomConv,
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
        column_map={
            "system_id": "system_id",
            "cust_key": "cust_key",
            "cust_code": "cust_code",
            "cust_name": "cust_name",
            "phone": "phone",
            "email": "email",
            "balance": "balance",
            "credit_limit": "credit_limit",
            "credit_hold": "credit_account",
            "cust_type": "cust_type",
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


FIRST_SYNC_DEFINITIONS = MASTER_DEFINITIONS + OPERATIONAL_DEFINITIONS


def current_utc_batch_values() -> dict[str, Any]:
    return {}
