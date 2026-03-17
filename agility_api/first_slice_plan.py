from __future__ import annotations

from .mirror_framework import SyncFamily, SyncStrategy, TableSyncConfig


FIRST_SLICE_CONFIGS = [
    TableSyncConfig(
        table_name="erp_mirror_cust",
        staging_table_name="erp_mirror_cust_staging",
        family=SyncFamily.MASTER,
        strategy=SyncStrategy.INCREMENTAL,
        natural_key_columns=("system_id", "cust_key"),
        source_query="""
            SELECT *
            FROM dbo.cust
            WHERE pro2modified >= ? OR update_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=300,
    ),
    TableSyncConfig(
        table_name="erp_mirror_cust_shipto",
        staging_table_name="erp_mirror_cust_shipto_staging",
        family=SyncFamily.MASTER,
        strategy=SyncStrategy.INCREMENTAL,
        natural_key_columns=("system_id", "cust_key", "seq_num"),
        source_query="""
            SELECT *
            FROM dbo.cust_shipto
            WHERE pro2modified >= ? OR update_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=300,
    ),
    TableSyncConfig(
        table_name="erp_mirror_item",
        staging_table_name="erp_mirror_item_staging",
        family=SyncFamily.MASTER,
        strategy=SyncStrategy.INCREMENTAL,
        natural_key_columns=("system_id", "item_ptr"),
        source_query="""
            SELECT *
            FROM dbo.item
            WHERE pro2modified >= ? OR update_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=300,
    ),
    TableSyncConfig(
        table_name="erp_mirror_item_branch",
        staging_table_name="erp_mirror_item_branch_staging",
        family=SyncFamily.MASTER,
        strategy=SyncStrategy.INCREMENTAL,
        natural_key_columns=("item_ptr", "system_id"),
        source_query="""
            SELECT *
            FROM dbo.item_branch
            WHERE pro2modified >= ? OR update_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=300,
    ),
    TableSyncConfig(
        table_name="erp_mirror_item_uomconv",
        staging_table_name="erp_mirror_item_uomconv_staging",
        family=SyncFamily.MASTER,
        strategy=SyncStrategy.INCREMENTAL,
        natural_key_columns=("system_id", "item_ptr", "uom_ptr"),
        source_query="""
            SELECT *
            FROM dbo.item_uomconv
            WHERE pro2modified >= ? OR update_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=300,
    ),
    TableSyncConfig(
        table_name="erp_mirror_so_header",
        staging_table_name="erp_mirror_so_header_staging",
        family=SyncFamily.OPERATIONAL,
        strategy=SyncStrategy.INCREMENTAL,
        natural_key_columns=("system_id", "so_id"),
        source_query="""
            SELECT *
            FROM dbo.so_header
            WHERE pro2modified >= ? OR update_date >= ? OR expect_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=5,
    ),
    TableSyncConfig(
        table_name="erp_mirror_so_detail",
        staging_table_name="erp_mirror_so_detail_staging",
        family=SyncFamily.OPERATIONAL,
        strategy=SyncStrategy.INCREMENTAL,
        natural_key_columns=("system_id", "so_id", "sequence"),
        source_query="""
            SELECT *
            FROM dbo.so_detail
            WHERE pro2modified >= ? OR update_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=5,
    ),
    TableSyncConfig(
        table_name="erp_mirror_shipments_header",
        staging_table_name="erp_mirror_shipments_header_staging",
        family=SyncFamily.OPERATIONAL,
        strategy=SyncStrategy.INCREMENTAL,
        natural_key_columns=("system_id", "so_id", "shipment_num"),
        source_query="""
            SELECT *
            FROM dbo.shipments_header
            WHERE pro2modified >= ? OR update_date >= ? OR ship_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=5,
    ),
    TableSyncConfig(
        table_name="erp_mirror_shipments_detail",
        staging_table_name="erp_mirror_shipments_detail_staging",
        family=SyncFamily.OPERATIONAL,
        strategy=SyncStrategy.INCREMENTAL,
        natural_key_columns=("system_id", "so_id", "shipment_num", "sequence"),
        source_query="""
            SELECT *
            FROM dbo.shipments_detail
            WHERE pro2modified >= ? OR update_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=5,
    ),
    TableSyncConfig(
        table_name="erp_mirror_wo_header",
        staging_table_name="erp_mirror_wo_header_staging",
        family=SyncFamily.OPERATIONAL,
        strategy=SyncStrategy.INCREMENTAL,
        natural_key_columns=("system_id", "wo_id"),
        source_query="""
            SELECT *
            FROM dbo.wo_header
            WHERE pro2modified >= ? OR update_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=5,
    ),
    TableSyncConfig(
        table_name="erp_mirror_pick_header",
        staging_table_name="erp_mirror_pick_header_staging",
        family=SyncFamily.OPERATIONAL,
        strategy=SyncStrategy.WINDOWED,
        natural_key_columns=("system_id", "pick_id", "pick_seq"),
        source_query="""
            SELECT *
            FROM dbo.pick_header
            WHERE pro2modified >= ? OR update_date >= ? OR created_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=5,
    ),
    TableSyncConfig(
        table_name="erp_mirror_pick_detail",
        staging_table_name="erp_mirror_pick_detail_staging",
        family=SyncFamily.OPERATIONAL,
        strategy=SyncStrategy.WINDOWED,
        natural_key_columns=("system_id", "pick_id", "pick_seq", "tran_id", "tran_seq"),
        source_query="""
            SELECT *
            FROM dbo.pick_detail
            WHERE pro2modified >= ? OR update_date >= ? OR created_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=5,
    ),
    TableSyncConfig(
        table_name="erp_mirror_aropen",
        staging_table_name="erp_mirror_aropen_staging",
        family=SyncFamily.AR,
        strategy=SyncStrategy.WINDOWED,
        natural_key_columns=("system_id", "ref_num", "ref_num_seq"),
        source_query="""
            SELECT *
            FROM dbo.aropen
            WHERE pro2modified >= ? OR update_date >= ? OR ref_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=300,
    ),
    TableSyncConfig(
        table_name="erp_mirror_aropendt",
        staging_table_name="erp_mirror_aropendt_staging",
        family=SyncFamily.AR,
        strategy=SyncStrategy.WINDOWED,
        natural_key_columns=("system_id", "ref_num", "ref_num_seq"),
        source_query="""
            SELECT *
            FROM dbo.aropendt
            WHERE pro2modified >= ? OR update_date >= ? OR due_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=300,
    ),
    TableSyncConfig(
        table_name="erp_mirror_print_transaction",
        staging_table_name="erp_mirror_print_transaction_staging",
        family=SyncFamily.DOCUMENT,
        strategy=SyncStrategy.WINDOWED,
        natural_key_columns=("system_id", "tran_id", "tran_type", "seq_num"),
        source_query="""
            SELECT *
            FROM dbo.print_transaction
            WHERE pro2modified >= ? OR update_date >= ? OR created_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=300,
    ),
    TableSyncConfig(
        table_name="erp_mirror_print_transaction_detail",
        staging_table_name="erp_mirror_print_transaction_detail_staging",
        family=SyncFamily.DOCUMENT,
        strategy=SyncStrategy.WINDOWED,
        natural_key_columns=("system_id", "tran_id", "printer_id", "printer_destination"),
        source_query="""
            SELECT *
            FROM dbo.print_transaction_detail
            WHERE pro2modified >= ? OR update_date >= ? OR created_date >= ?
        """,
        source_updated_column="pro2modified",
        cadence_seconds=300,
    ),
]
