from __future__ import annotations

MIRROR_TARGET_TABLES = [
    "cust",
    "cust_shipto",
    "item",
    "item_branch",
    "item_uomconv",
    "so_header",
    "so_detail",
    "shipments_header",
    "shipments_detail",
    "wo_header",
    "pick_header",
    "pick_detail",
    "aropen",
    "aropendt",
    "print_transaction",
    "print_transaction_detail",
]


GENERAL_DISCOVERY_QUERIES = {
    "schemas": """
        SELECT s.schema_id, s.name AS schema_name, p.name AS owned_by
        FROM sys.schemas s
        JOIN sys.database_principals p ON s.principal_id = p.principal_id
        WHERE s.name NOT IN (
            'sys','INFORMATION_SCHEMA','guest','db_owner','db_accessadmin',
            'db_securityadmin','db_ddladmin','db_backupoperator',
            'db_datareader','db_datawriter','db_denydatareader','db_denydatawriter'
        )
        ORDER BY s.name;
    """,
    "tables_rowcounts": """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            p.rows AS row_count,
            t.create_date AS table_created,
            t.modify_date AS table_modified,
            STATS_DATE(t.object_id, 1) AS stats_last_updated
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
        ORDER BY p.rows DESC, s.name, t.name;
    """,
    "views": """
        SELECT s.name AS schema_name, v.name AS view_name, v.create_date, v.modify_date
        FROM sys.views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        ORDER BY s.name, v.name;
    """,
    "primary_keys": """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            kc.name AS pk_name,
            c.name AS pk_column,
            ic.key_ordinal AS key_order
        FROM sys.key_constraints kc
        JOIN sys.tables t ON kc.parent_object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id
            AND kc.unique_index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
        WHERE kc.type = 'PK'
        ORDER BY s.name, t.name, ic.key_ordinal;
    """,
    "foreign_keys": """
        SELECT
            s.name AS schema_name,
            tp.name AS parent_table,
            cp.name AS parent_column,
            tr.name AS referenced_table,
            cr.name AS referenced_column,
            fk.name AS fk_constraint_name
        FROM sys.foreign_keys fk
        JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
        JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
        JOIN sys.schemas s ON tp.schema_id = s.schema_id
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        ORDER BY s.name, tp.name;
    """,
    "columns_full": """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            c.column_id,
            c.name AS column_name,
            tp.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.types tp ON c.user_type_id = tp.user_type_id
        ORDER BY s.name, t.name, c.column_id;
    """,
    "stored_procedures": """
        SELECT
            s.name AS schema_name,
            p.name AS procedure_name,
            p.create_date,
            p.modify_date
        FROM sys.procedures p
        JOIN sys.schemas s ON p.schema_id = s.schema_id
        ORDER BY s.name, p.name;
    """,
}


def target_table_inventory_query() -> str:
    quoted = ", ".join(f"'{name}'" for name in MIRROR_TARGET_TABLES)
    return f"""
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            p.rows AS row_count,
            t.create_date AS table_created,
            t.modify_date AS table_modified
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
        WHERE LOWER(t.name) IN ({quoted})
        ORDER BY t.name;
    """


def target_column_inventory_query() -> str:
    quoted = ", ".join(f"'{name}'" for name in MIRROR_TARGET_TABLES)
    return f"""
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            c.column_id,
            c.name AS column_name,
            tp.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.types tp ON c.user_type_id = tp.user_type_id
        WHERE LOWER(t.name) IN ({quoted})
        ORDER BY t.name, c.column_id;
    """


def target_change_tracking_query() -> str:
    quoted = ", ".join(f"'{name}'" for name in MIRROR_TARGET_TABLES)
    return f"""
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            c.name AS column_name,
            tp.name AS data_type
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.types tp ON c.user_type_id = tp.user_type_id
        WHERE LOWER(t.name) IN ({quoted})
          AND (
              LOWER(c.name) LIKE '%update%'
              OR LOWER(c.name) LIKE '%modified%'
              OR LOWER(c.name) LIKE '%change%'
              OR LOWER(c.name) LIKE '%timestamp%'
              OR LOWER(c.name) LIKE '%rowversion%'
              OR LOWER(c.name) LIKE '%date%'
          )
        ORDER BY t.name, c.name;
    """


def target_procedure_search_query() -> str:
    predicates = " OR ".join(f"LOWER(p.name) LIKE '%{name.replace('_', '')}%'" for name in MIRROR_TARGET_TABLES)
    return f"""
        SELECT
            s.name AS schema_name,
            p.name AS procedure_name,
            p.create_date,
            p.modify_date
        FROM sys.procedures p
        JOIN sys.schemas s ON p.schema_id = s.schema_id
        WHERE {predicates}
           OR LOWER(p.name) LIKE '%customer%'
           OR LOWER(p.name) LIKE '%invoice%'
           OR LOWER(p.name) LIKE '%ar%'
           OR LOWER(p.name) LIKE '%pick%'
           OR LOWER(p.name) LIKE '%ship%'
           OR LOWER(p.name) LIKE '%workorder%'
        ORDER BY p.name;
    """


TARGET_DISCOVERY_QUERIES = {
    "mirror_target_tables": target_table_inventory_query(),
    "mirror_target_columns": target_column_inventory_query(),
    "mirror_change_tracking_candidates": target_change_tracking_query(),
    "mirror_related_procedures": target_procedure_search_query(),
}
