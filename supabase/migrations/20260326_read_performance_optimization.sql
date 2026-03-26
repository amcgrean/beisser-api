-- ==========================================================================
-- Performance Optimization: WH-Tracker + PO App
-- Migration-safe, additive-only (no drops, no breaking changes)
--
-- CONTRACT GUARANTEES:
--   - All existing views (app_po_search, app_po_header, app_po_detail,
--     app_po_receiving_summary) remain unchanged
--   - All existing tables remain unchanged
--   - New RPC functions are OPTIONAL — apps can adopt at their own pace
--   - Soft-delete (is_deleted=false) semantics preserved everywhere
--   - system_id is case-sensitive (uppercase branch codes)
--   - Numeric po_id lookup path preserved
--   - Submissions write semantics untouched
--
-- Rollback: See bottom of file for commented DROP statements.
--
-- NOTE: Uses plain CREATE INDEX (not CONCURRENTLY) so this can run inside
-- Supabase SQL Editor's implicit transaction. For large production tables,
-- you can run the index statements individually with CONCURRENTLY outside
-- a transaction if needed.
-- ==========================================================================


-- **************************************************************************
-- PART A — WH-Tracker Order Board (/warehouse/board/orders)
-- **************************************************************************

-- -------------------------------------------------------------------------
-- A1. INDEXES — Cover the 5-table join columns and filter predicates
-- -------------------------------------------------------------------------

-- so_header: partial index on open orders (is_deleted=false filtered out)
CREATE INDEX IF NOT EXISTS
    idx_so_header_open_orders
    ON erp_mirror_so_header (system_id, so_status)
    WHERE is_deleted = false;

-- so_detail: join on (system_id, so_id), covers item_ptr + filter cols
CREATE INDEX IF NOT EXISTS
    idx_so_detail_so_lookup
    ON erp_mirror_so_detail (system_id, so_id)
    INCLUDE (item_ptr, sequence, bo);

-- item_branch: join on (system_id, item_ptr), covers handling_code
CREATE INDEX IF NOT EXISTS
    idx_item_branch_item_lookup
    ON erp_mirror_item_branch (system_id, item_ptr)
    INCLUDE (handling_code);

-- cust: join on (system_id, cust_key), covers cust_name
CREATE INDEX IF NOT EXISTS
    idx_cust_key_lookup
    ON erp_mirror_cust (system_id, cust_key)
    INCLUDE (cust_name);

-- cust_shipto: join on (system_id, cust_key, seq_num), covers address cols
CREATE INDEX IF NOT EXISTS
    idx_cust_shipto_key_lookup
    ON erp_mirror_cust_shipto (system_id, cust_key, seq_num)
    INCLUDE (address_1, city);


-- -------------------------------------------------------------------------
-- A2. VIEW — Flat view for the order board
-- -------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_board_open_orders AS
SELECT
    soh.so_id,
    c.cust_name,
    cs.address_1,
    cs.city,
    soh.reference,
    ib.handling_code,
    COUNT(sod.sequence) AS line_count
FROM erp_mirror_so_detail  sod
JOIN erp_mirror_so_header   soh
    ON  soh.system_id = sod.system_id
    AND soh.so_id     = sod.so_id
LEFT JOIN erp_mirror_item_branch ib
    ON  ib.system_id = sod.system_id
    AND ib.item_ptr  = sod.item_ptr
LEFT JOIN erp_mirror_cust c
    ON  c.system_id = soh.system_id
    AND c.cust_key  = soh.cust_key
LEFT JOIN erp_mirror_cust_shipto cs
    ON  cs.system_id = soh.system_id
    AND cs.cust_key  = soh.cust_key
    AND cs.seq_num   = soh.shipto_seq_num
WHERE soh.is_deleted = false
  AND soh.so_status  = 'K'
  AND COALESCE(sod.bo, 0) = 0
GROUP BY
    soh.so_id,
    c.cust_name,
    cs.address_1,
    cs.city,
    soh.reference,
    ib.handling_code
ORDER BY
    ib.handling_code,
    soh.so_id;


-- -------------------------------------------------------------------------
-- A3. RPC — supabase.rpc('get_board_open_orders')
-- -------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_board_open_orders()
RETURNS SETOF vw_board_open_orders
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
    SELECT * FROM vw_board_open_orders;
$$;

GRANT EXECUTE ON FUNCTION get_board_open_orders() TO anon, authenticated;


-- **************************************************************************
-- PART B — PO App Read Optimization
-- **************************************************************************

-- -------------------------------------------------------------------------
-- B1. INDEXES on erp_mirror_po_header
--     Covers: branch open-PO list, PO detail lookup, search backing
-- -------------------------------------------------------------------------

-- Branch open-PO list: filter (system_id, is_deleted), sort (expect_date, order_date)
-- system_id is case-sensitive (uppercase branch codes like 'AUS', 'DAL')
CREATE INDEX IF NOT EXISTS
    idx_po_header_branch_open
    ON erp_mirror_po_header (system_id, expect_date ASC NULLS LAST, order_date DESC NULLS LAST)
    INCLUDE (po_id, supplier_key, purchase_type, po_status, wms_status, reference, synced_at)
    WHERE is_deleted = false;

-- PO detail lookup by po_id (numeric path: cast-safe text match)
CREATE INDEX IF NOT EXISTS
    idx_po_header_po_id
    ON erp_mirror_po_header (po_id);

-- PO detail lookup by po_number (non-numeric path)
CREATE INDEX IF NOT EXISTS
    idx_po_header_po_number
    ON erp_mirror_po_header (po_number);

-- Ordering on synced_at for search results
CREATE INDEX IF NOT EXISTS
    idx_po_header_synced_at
    ON erp_mirror_po_header (synced_at DESC NULLS LAST);


-- -------------------------------------------------------------------------
-- B2. INDEXES on erp_mirror_po_detail (backs app_po_detail view)
-- -------------------------------------------------------------------------

-- Lookup by po_id with line_number for ORDER BY
CREATE INDEX IF NOT EXISTS
    idx_po_detail_po_id
    ON erp_mirror_po_detail (po_id)
    INCLUDE (line_number);

-- Lookup by po_number (non-numeric detail path)
CREATE INDEX IF NOT EXISTS
    idx_po_detail_po_number
    ON erp_mirror_po_detail (po_number)
    INCLUDE (line_number);

-- Composite join path if views join on system_id + po_id
CREATE INDEX IF NOT EXISTS
    idx_po_detail_system_po
    ON erp_mirror_po_detail (system_id, po_id);


-- -------------------------------------------------------------------------
-- B3. INDEXES on app_po_receiving backing tables
--     (Indexes on the view's underlying table — adjust table name if
--      the receiving summary view references a different base table)
-- -------------------------------------------------------------------------

-- If app_po_receiving_summary is backed by erp_mirror_po_receiving or similar:
-- CREATE INDEX IF NOT EXISTS
--     idx_po_receiving_po_id
--     ON erp_mirror_po_receiving (po_id);
-- CREATE INDEX IF NOT EXISTS
--     idx_po_receiving_po_number
--     ON erp_mirror_po_receiving (po_number);
--
-- ^^^ UNCOMMENT after confirming the backing table name.


-- -------------------------------------------------------------------------
-- B4. TRIGRAM INDEXES for ILIKE search on app_po_search
--     Accelerates: or(po_number.ilike.%q%, supplier_name.ilike.%q%, reference.ilike.%q%)
-- -------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- NOTE: These target erp_mirror_po_header assuming app_po_search is a view
-- over it. If app_po_search has a different backing table, move these indexes.

CREATE INDEX IF NOT EXISTS
    idx_po_header_po_number_trgm
    ON erp_mirror_po_header USING gin (po_number gin_trgm_ops);

CREATE INDEX IF NOT EXISTS
    idx_po_header_supplier_name_trgm
    ON erp_mirror_po_header USING gin (supplier_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS
    idx_po_header_reference_trgm
    ON erp_mirror_po_header USING gin (reference gin_trgm_ops);


-- -------------------------------------------------------------------------
-- B5. INDEXES on submissions table
--     Covers: summary per PO list row (IN query), detail page list
--     WRITE SEMANTICS UNCHANGED — these are read-only indexes
-- -------------------------------------------------------------------------

-- Submission lookup by po_number + created_at ordering
CREATE INDEX IF NOT EXISTS
    idx_submissions_po_number
    ON submissions (po_number, created_at DESC);

-- Branch-scoped submission lookup (optional eq(branch, :branch))
CREATE INDEX IF NOT EXISTS
    idx_submissions_po_branch
    ON submissions (po_number, branch, created_at DESC);


-- -------------------------------------------------------------------------
-- B6. RPC — supabase.rpc('get_branch_open_pos', { branch_id, row_limit })
--
--     OPTIONAL replacement for the current direct-table query.
--     Returns the SAME columns in the SAME order as the current select().
--
--     IMPORTANT: Does NOT filter by po_status server-side.
--     The app currently does status filtering client-side and the exact
--     excluded statuses are app logic. This RPC preserves that contract —
--     it only pushes the is_deleted + branch filter + sort into a function
--     for plan caching. The app continues to filter statuses as before.
-- -------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_branch_open_pos(
    branch_id  text,            -- uppercase system_id, e.g. 'AUS'
    row_limit  integer DEFAULT 250
)
RETURNS TABLE (
    po_id          text,
    system_id      text,
    supplier_key   text,
    purchase_type  text,
    order_date     date,
    expect_date    date,
    po_status      text,
    wms_status     text,
    reference      text,
    synced_at      timestamptz
)
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
    SELECT
        h.po_id,
        h.system_id,
        h.supplier_key,
        h.purchase_type,
        h.order_date,
        h.expect_date,
        h.po_status,
        h.wms_status,
        h.reference,
        h.synced_at
    FROM erp_mirror_po_header h
    WHERE h.system_id  = branch_id
      AND h.is_deleted = false
    ORDER BY
        h.expect_date ASC NULLS LAST,
        h.order_date  DESC NULLS LAST
    LIMIT row_limit;
$$;

GRANT EXECUTE ON FUNCTION get_branch_open_pos(text, integer) TO anon, authenticated;


-- -------------------------------------------------------------------------
-- B7. RPC — supabase.rpc('get_po_detail', { filter_col, filter_val })
--
--     OPTIONAL single-round-trip replacement for the 3 parallel queries.
--     Returns { header, lines, receiving_summary } as JSONB.
--
--     CONTRACT:
--       - header: full row from app_po_header (null if not found)
--       - lines: array from app_po_detail ordered by line_number ASC
--                (empty array if none)
--       - receiving_summary: full row from app_po_receiving_summary
--                            (null if not found)
--       - filter_col must be 'po_id' or 'po_number'
--       - po_id path: numeric string lookup (e.g. '12345')
--       - po_number path: alphanumeric lookup (e.g. 'PO-12345')
--       - Nullability: header/receiving_summary may be JSON null;
--                      lines is always an array (possibly empty)
-- -------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_po_detail(
    filter_col  text,    -- 'po_id' or 'po_number'
    filter_val  text
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $$
DECLARE
    result jsonb;
    hdr   jsonb;
    lines jsonb;
    recv  jsonb;
BEGIN
    -- Whitelist filter_col to prevent injection
    IF filter_col NOT IN ('po_id', 'po_number') THEN
        RAISE EXCEPTION 'Invalid filter column: %. Must be po_id or po_number.', filter_col;
    END IF;

    IF filter_col = 'po_id' THEN
        SELECT to_jsonb(h) INTO hdr
          FROM app_po_header h
         WHERE h.po_id::text = filter_val
         LIMIT 1;

        SELECT COALESCE(jsonb_agg(d ORDER BY d.line_number), '[]'::jsonb) INTO lines
          FROM app_po_detail d
         WHERE d.po_id::text = filter_val;

        SELECT to_jsonb(r) INTO recv
          FROM app_po_receiving_summary r
         WHERE r.po_id::text = filter_val
         LIMIT 1;
    ELSE
        SELECT to_jsonb(h) INTO hdr
          FROM app_po_header h
         WHERE h.po_number = filter_val
         LIMIT 1;

        SELECT COALESCE(jsonb_agg(d ORDER BY d.line_number), '[]'::jsonb) INTO lines
          FROM app_po_detail d
         WHERE d.po_number = filter_val;

        SELECT to_jsonb(r) INTO recv
          FROM app_po_receiving_summary r
         WHERE r.po_number = filter_val
         LIMIT 1;
    END IF;

    result := jsonb_build_object(
        'header',            COALESCE(hdr,   'null'::jsonb),
        'lines',             COALESCE(lines, '[]'::jsonb),
        'receiving_summary', COALESCE(recv,  'null'::jsonb)
    );

    RETURN result;
END;
$$;

GRANT EXECUTE ON FUNCTION get_po_detail(text, text) TO anon, authenticated;


-- =========================================================================
-- ROLLBACK PLAN (run manually if needed)
-- =========================================================================
-- -- Part A (WH-Tracker)
-- DROP FUNCTION IF EXISTS get_board_open_orders();
-- DROP VIEW IF EXISTS vw_board_open_orders;
-- DROP INDEX IF EXISTS idx_so_header_open_orders;
-- DROP INDEX IF EXISTS idx_so_detail_so_lookup;
-- DROP INDEX IF EXISTS idx_item_branch_item_lookup;
-- DROP INDEX IF EXISTS idx_cust_key_lookup;
-- DROP INDEX IF EXISTS idx_cust_shipto_key_lookup;
--
-- -- Part B (PO App)
-- DROP FUNCTION IF EXISTS get_po_detail(text, text);
-- DROP FUNCTION IF EXISTS get_branch_open_pos(text, integer);
-- DROP INDEX IF EXISTS idx_po_header_branch_open;
-- DROP INDEX IF EXISTS idx_po_header_po_id;
-- DROP INDEX IF EXISTS idx_po_header_po_number;
-- DROP INDEX IF EXISTS idx_po_header_synced_at;
-- DROP INDEX IF EXISTS idx_po_detail_po_id;
-- DROP INDEX IF EXISTS idx_po_detail_po_number;
-- DROP INDEX IF EXISTS idx_po_detail_system_po;
-- DROP INDEX IF EXISTS idx_po_header_po_number_trgm;
-- DROP INDEX IF EXISTS idx_po_header_supplier_name_trgm;
-- DROP INDEX IF EXISTS idx_po_header_reference_trgm;
-- DROP INDEX IF EXISTS idx_submissions_po_number;
-- DROP INDEX IF EXISTS idx_submissions_po_branch;
-- DROP EXTENSION IF EXISTS pg_trgm;
