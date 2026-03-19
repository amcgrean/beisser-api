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
    ERPMirrorPickDetail,
    ERPMirrorPickHeader,
    ERPMirrorPrintTransaction,
    ERPMirrorPrintTransactionDetail,
    ERPMirrorPurchaseOrderDetail,
    ERPMirrorPurchaseOrderHeader,
    ERPMirrorReceivingDetail,
    ERPMirrorReceivingHeader,
    ERPMirrorSalesOrderDetail,
    ERPMirrorSalesOrderHeader,
    ERPMirrorShipmentDetail,
    ERPMirrorShipmentHeader,
    ERPMirrorWorkOrderHeader,
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
        source_table="""
            (
                SELECT
                    i.system_id,
                    i.item_ptr,
                    i.item,
                    i.description,
                    i.short_des,
                    i.ext_description,
                    i.customer_description,
                    i.size_,
                    i.type,
                    i.stocking_uom,
                    i.costing_uom,
                    i.tally_uom,
                    i.default_uom_conv_factor,
                    i.direct_only,
                    i.temporary_,
                    i.pg_ptr,
                    i.link_product_group,
                    i.keyword_string,
                    i.pro2modified,
                    i.update_date
                FROM dbo.item i
                WHERE
                    i.item IS NULL
                    OR UPPER(i.item) NOT LIKE 'Z%'
                    OR COALESCE(i.created_date, i.update_date, i.pro2modified) >= DATEADD(year, -3, GETDATE())
                    OR EXISTS (
                        SELECT 1
                        FROM dbo.so_detail sod
                        JOIN dbo.so_header soh
                            ON soh.system_id = sod.system_id
                           AND soh.so_id = sod.so_id
                        WHERE sod.system_id = i.system_id
                          AND sod.item_ptr = i.item_ptr
                          AND COALESCE(
                              soh.order_date,
                              soh.created_date,
                              soh.expect_date,
                              soh.update_date,
                              soh.pro2modified
                          ) >= DATEADD(year, -3, GETDATE())
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM dbo.shipments_detail sd
                        JOIN dbo.shipments_header sh
                            ON sh.system_id = sd.system_id
                           AND sh.so_id = sd.so_id
                           AND sh.shipment_num = sd.shipment_num
                        WHERE sd.system_id = i.system_id
                          AND sd.item_ptr = i.item_ptr
                          AND COALESCE(
                              sh.invoice_date,
                              sh.ship_date,
                              sh.update_date,
                              sh.pro2modified
                          ) >= DATEADD(year, -3, GETDATE())
                    )
            ) item_src
        """,
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
            "short_des": "short_des",
            "ext_description": "ext_description",
            "customer_description": "customer_description",
            "size_": "size_",
            "type": "type",
            "stocking_uom": "stocking_uom",
            "costing_uom": "costing_uom",
            "tally_uom": "tally_uom",
            "default_uom_conv_factor": "default_uom_conv_factor",
            "direct_only": "direct_only",
            "temporary_": "temporary_",
            "pg_ptr": "pg_ptr",
            "link_product_group": "link_product_group",
            "keyword_string": "keyword_string",
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
            "item": "item",
            "active_flag": "active_flag",
            "stock": "stock",
            "contentcode": "contentcode",
            "buyer_id": "buyer_id",
            "handling_code": "handling_code",
            "display_uom": "display_uom",
            "picking_uom": "picking_uom",
            "weight": "weight",
            "weight_uom": "weight_uom",
            "keyword_string": "keyword_string",
            "discontinued_item": "discontinued_item",
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
    ExtractorDefinition(
        name="po_header",
        source_table="dbo.po_header",
        target_table="erp_mirror_po_header",
        model=ERPMirrorPurchaseOrderHeader,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("system_id", "po_id"),
        watermark_columns=("pro2modified", "update_date", "created_date"),
        default_order_by=("system_id", "po_id"),
        column_map={
            "system_id": "system_id",
            "po_id": "po_id",
            "purchase_type": "purchase_type",
            "supplier_key": "supplier_key",
            "shipfrom_seq": "shipfrom_seq",
            "order_date": "order_date",
            "expect_date": "expect_date",
            "due_date": "due_date",
            "buyer": "buyer",
            "reference": "reference",
            "ship_via": "ship_via",
            "current_receive_no": "current_receive_no",
            "po_status": "po_status",
            "canceled": "canceled",
            "wms_status": "wms_status",
            "received_manually": "received_manually",
            "mwt_recv_complete": "mwt_recv_complete",
            "mwt_recv_complete_datetime": "mwt_recv_complete_datetime",
            "created_date": "created_date",
            "update_date": "update_date",
        },
    ),
    ExtractorDefinition(
        name="po_detail",
        source_table="dbo.po_detail",
        target_table="erp_mirror_po_detail",
        model=ERPMirrorPurchaseOrderDetail,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("system_id", "po_id", "sequence"),
        watermark_columns=("pro2modified", "update_date", "created_date"),
        default_order_by=("system_id", "po_id", "sequence"),
        column_map={
            "system_id": "system_id",
            "po_id": "po_id",
            "sequence": "sequence",
            "item_ptr": "item_ptr",
            "size_": "size_",
            "po_desc": "po_desc",
            "qty_ordered": "qty_ordered",
            "uom": "uom",
            "cost": "cost",
            "disp_cost_conv": "disp_cost_conv",
            "display_cost_uom": "display_cost_uom",
            "po_status": "po_status",
            "canceled": "canceled",
            "due_date": "due_date",
            "expect_date": "expect_date",
            "exp_rcpt_date": "exp_rcpt_date",
            "exp_ship_date": "exp_ship_date",
            "wo_id": "wo_id",
            "created_date": "created_date",
            "update_date": "update_date",
        },
    ),
    ExtractorDefinition(
        name="receiving_header",
        source_table="dbo.receiving_header",
        target_table="erp_mirror_receiving_header",
        model=ERPMirrorReceivingHeader,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("system_id", "po_id", "receive_num"),
        watermark_columns=("pro2modified", "update_date", "created_date"),
        default_order_by=("system_id", "po_id", "receive_num"),
        column_map={
            "system_id": "system_id",
            "po_id": "po_id",
            "receive_num": "receive_num",
            "receive_date": "receive_date",
            "recv_status": "recv_status",
            "packing_slip": "packing_slip",
            "wms_user": "wms_user",
            "wms_dispatch_id": "wms_dispatch_id",
            "recv_comment": "recv_comment",
            "created_date": "created_date",
            "update_date": "update_date",
        },
    ),
    ExtractorDefinition(
        name="receiving_detail",
        source_table="dbo.receiving_detail",
        target_table="erp_mirror_receiving_detail",
        model=ERPMirrorReceivingDetail,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("system_id", "receive_num", "po_id", "sequence"),
        watermark_columns=("pro2modified", "update_date", "created_date"),
        default_order_by=("system_id", "receive_num", "po_id", "sequence"),
        column_map={
            "system_id": "system_id",
            "receive_num": "receive_num",
            "po_id": "po_id",
            "sequence": "sequence",
            "item_ptr": "item_ptr",
            "qty": "qty",
            "uom_ptr": "uom_ptr",
            "cost": "cost",
            "recv_status": "recv_status",
            "receive_date": "receive_date",
            "display_cost_conv": "display_cost_conv",
            "display_cost_uom": "display_cost_uom",
            "created_date": "created_date",
            "update_date": "update_date",
        },
    ),
    ExtractorDefinition(
        name="wo_header",
        source_table="""
            (
                SELECT
                    wo_id,
                    source,
                    source_id,
                    source_seq,
                    wo_status,
                    department,
                    wo_rule,
                    pro2modified,
                    update_date
                FROM dbo.wo_header
                WHERE NOT (
                    ISNULL(canceled, 0) = 1
                    AND COALESCE(created_date, order_date, update_date, pro2modified) < '2025-01-01'
                )
            ) wo_header_src
        """,
        target_table="erp_mirror_wo_header",
        model=ERPMirrorWorkOrderHeader,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("wo_id",),
        watermark_columns=("pro2modified", "update_date"),
        default_order_by=("wo_id",),
        column_map={
            "wo_id": "wo_id",
            "source": "source",
            "source_id": "source_id",
            "source_seq": "source_seq",
            "wo_status": "wo_status",
            "department": "department",
            "wo_rule": "wo_rule",
        },
    ),
    ExtractorDefinition(
        name="pick_header",
        source_table="""
            (
                SELECT system_id, pick_id, created_date, created_time, print_status, pro2modified, update_date
                FROM (
                    SELECT
                        system_id,
                        pick_id,
                        created_date,
                        created_time,
                        print_status,
                        pro2modified,
                        update_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY system_id, pick_id
                            ORDER BY COALESCE(pro2modified, update_date, created_date) DESC, created_time DESC
                        ) AS rn
                    FROM dbo.pick_header
                ) ranked
                WHERE rn = 1
            ) pick_header_src
        """,
        target_table="erp_mirror_pick_header",
        model=ERPMirrorPickHeader,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("pick_id", "system_id"),
        watermark_columns=("pro2modified", "update_date", "created_date"),
        default_order_by=("system_id", "pick_id"),
        column_map={
            "system_id": "system_id",
            "pick_id": "pick_id",
            "created_date": "created_date",
            "created_time": "created_time",
            "print_status": "print_status",
        },
    ),
    ExtractorDefinition(
        name="pick_detail",
        source_table="""
            (
                SELECT system_id, pick_id, tran_type, tran_id, tran_seq, pro2modified, update_date, created_date
                FROM (
                    SELECT
                        system_id,
                        pick_id,
                        tran_type,
                        tran_id,
                        tran_seq,
                        pro2modified,
                        update_date,
                        created_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY system_id, pick_id, tran_type, tran_id, tran_seq
                            ORDER BY COALESCE(pro2modified, update_date, created_date) DESC, pick_seq DESC
                        ) AS rn
                    FROM dbo.pick_detail
                ) ranked
                WHERE rn = 1
            ) pick_detail_src
        """,
        target_table="erp_mirror_pick_detail",
        model=ERPMirrorPickDetail,
        family=SyncFamily.OPERATIONAL,
        cadence_seconds=5,
        natural_keys=("pick_id", "system_id", "tran_type", "tran_id", "sequence"),
        watermark_columns=("pro2modified", "update_date", "created_date"),
        default_order_by=("system_id", "pick_id", "tran_type", "tran_id", "tran_seq"),
        column_map={
            "system_id": "system_id",
            "pick_id": "pick_id",
            "tran_type": "tran_type",
            "tran_id": "tran_id",
            "tran_seq": "sequence",
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
