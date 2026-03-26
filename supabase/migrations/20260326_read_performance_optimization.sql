-- ==========================================================================
-- Performance Optimization: WH-Tracker + PO App
-- Migration-safe, additive-only (no drops, no breaking changes)
--
-- Rollback: DROP each index/function created here by name.
--           Views are CREATE OR REPLACE so the old definition is gone;
--           re-deploy the previous migration to restore.
-- ==========================================================================


-- **************************************************************************
-- PART A — WH-Tracker Order Board (/warehouse/board/orders)
-- **************************************************************************

-- -------------------------------------------------------------------------
-- A1. INDEXES — Cover the 5-table join columns and filter predicates
-- -------------------------------------------------------------------------

-- so_header: partial index on open orders (is_deleted=false filtered out)
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_so_header_open_orders
    ON erp_mirror_so_header (system_id, so_status)
    WHERE is_deleted = false;

-- so_detail: join on (system_id, so_id), covers item_ptr + filter cols
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_so_detail_so_lookup
    ON erp_mirror_so_detail (system_id, so_id)
    INCLUDE (item_ptr, sequence, backordered_qty);

-- item_branch: join on (system_id, item_ptr), covers handling_code
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_item_branch_item_lookup
    ON erp_mirror_item_branch (system_id, item_ptr)
    INCLUDE (handling_code);

-- cust: join on (system_id, cust_key), covers cust_name
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_cust_key_lookup
    ON erp_mirror_cust (system_id, cust_key)
    INCLUDE (cust_name);

-- cust_shipto: join on (system_id, cust_key, seq_num), covers address cols
CREATE INDEX CONCURRENTLY IF NOT EXISTS
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
  AND COALESCE(sod.backordered_qty, 0) = 0
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
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_po_header_branch_open
    ON erp_mirror_po_header (system_id, is_deleted, expect_date ASC NULLS LAST, order_date DESC NULLS LAST)
    INCLUDE (po_id, supplier_key, purchase_type, po_status, wms_status, reference, synced_at)
    WHERE is_deleted = false;

-- PO detail lookup by po_id (used by app_po_header, app_po_detail, app_po_receiving_summary views)
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_po_header_po_id
    ON erp_mirror_po_header (po_id);

-- Ordering on synced_at for search results
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_po_header_synced_at
    ON erp_mirror_po_header (synced_at DESC NULLS LAST);


-- -------------------------------------------------------------------------
-- B2. INDEXES on erp_mirror_po_detail (backs app_po_detail view)
-- -------------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_po_detail_po_id
    ON erp_mirror_po_detail (po_id)
    INCLUDE (line_number);

-- If the view joins on system_id + po_id:
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_po_detail_system_po
    ON erp_mirror_po_detail (system_id, po_id);


-- -------------------------------------------------------------------------
-- B3. TRIGRAM INDEXES for ILIKE search on app_po_search
--     Requires: CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- -------------------------------------------------------------------------

-- Enable trigram extension (idempotent)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- These indexes accelerate the ILIKE %query% pattern on the search view.
-- If app_po_search is a view over erp_mirror_po_header, these go on that
-- table. If it is a materialized view or separate table, adjust the target.
--
-- NOTE: If these columns live on a different underlying table than
-- erp_mirror_po_header, change the table name below accordingly.

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_po_header_po_number_trgm
    ON erp_mirror_po_header USING gin (po_number gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_po_header_supplier_name_trgm
    ON erp_mirror_po_header USING gin (supplier_name gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_po_header_reference_trgm
    ON erp_mirror_po_header USING gin (reference gin_trgm_ops);


-- -------------------------------------------------------------------------
-- B4. INDEXES on submissions table
--     Covers: summary per PO list row (IN query), detail page list
-- -------------------------------------------------------------------------

-- Submission lookup by po_number + created_at ordering
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_submissions_po_number
    ON submissions (po_number, created_at DESC);

-- Branch-scoped submission lookup
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_submissions_po_branch
    ON submissions (po_number, branch, created_at DESC);


-- -------------------------------------------------------------------------
-- B5. RPC — supabase.rpc('get_branch_open_pos', { branch_id, row_limit })
--     Replaces the client-side query + app-side status filtering.
--     Pushes the status exclusion into the database so fewer rows travel
--     over the wire and Supabase doesn't scan rows the app will discard.
-- -------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_branch_open_pos(
    branch_id  text,
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
      -- Exclude closed / cancelled / fully-received statuses server-side
      -- so fewer rows cross the wire.  Adjust these values to match
      -- the exact statuses your app currently filters out.
      AND h.po_status NOT IN ('C', 'X', 'R')
    ORDER BY
        h.expect_date ASC NULLS LAST,
        h.order_date  DESC NULLS LAST
    LIMIT row_limit;
$$;

GRANT EXECUTE ON FUNCTION get_branch_open_pos(text, integer) TO anon, authenticated;


-- -------------------------------------------------------------------------
-- B6. RPC — supabase.rpc('get_po_detail', { filter_col, filter_val })
--     Runs the three parallel detail queries in a single round-trip.
--     Returns a JSON object with { header, lines, receiving_summary }.
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
    -- Validate filter_col to prevent SQL injection
    IF filter_col NOT IN ('po_id', 'po_number') THEN
        RAISE EXCEPTION 'Invalid filter column: %', filter_col;
    END IF;

    -- Header
    IF filter_col = 'po_id' THEN
        SELECT to_jsonb(h) INTO hdr
        FROM app_po_header h WHERE h.po_id = filter_val LIMIT 1;

        SELECT COALESCE(jsonb_agg(d ORDER BY d.line_number), '[]'::jsonb) INTO lines
        FROM app_po_detail d WHERE d.po_id = filter_val;

        SELECT to_jsonb(r) INTO recv
        FROM app_po_receiving_summary r WHERE r.po_id = filter_val LIMIT 1;
    ELSE
        SELECT to_jsonb(h) INTO hdr
        FROM app_po_header h WHERE h.po_number = filter_val LIMIT 1;

        SELECT COALESCE(jsonb_agg(d ORDER BY d.line_number), '[]'::jsonb) INTO lines
        FROM app_po_detail d WHERE d.po_number = filter_val;

        SELECT to_jsonb(r) INTO recv
        FROM app_po_receiving_summary r WHERE r.po_number = filter_val LIMIT 1;
    END IF;

    result := jsonb_build_object(
        'header',            COALESCE(hdr, 'null'::jsonb),
        'lines',             COALESCE(lines, '[]'::jsonb),
        'receiving_summary', COALESCE(recv, 'null'::jsonb)
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
-- DROP INDEX CONCURRENTLY IF EXISTS idx_so_header_open_orders;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_so_detail_so_lookup;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_item_branch_item_lookup;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_cust_key_lookup;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_cust_shipto_key_lookup;
--
-- -- Part B (PO App)
-- DROP FUNCTION IF EXISTS get_po_detail(text, text);
-- DROP FUNCTION IF EXISTS get_branch_open_pos(text, integer);
-- DROP INDEX CONCURRENTLY IF EXISTS idx_po_header_branch_open;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_po_header_po_id;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_po_header_synced_at;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_po_detail_po_id;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_po_detail_system_po;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_po_header_po_number_trgm;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_po_header_supplier_name_trgm;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_po_header_reference_trgm;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_submissions_po_number;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_submissions_po_branch;
-- DROP EXTENSION IF EXISTS pg_trgm;
