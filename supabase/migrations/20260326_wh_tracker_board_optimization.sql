-- ==========================================================================
-- WH-Tracker Order Board Optimization
-- Target: /warehouse/board/orders (5-table join bottleneck)
-- ==========================================================================

-- -------------------------------------------------------------------------
-- 1. INDEXES — Cover the join columns and filter predicates
-- -------------------------------------------------------------------------

-- so_header: the WHERE clause filters on (is_deleted, so_status)
-- and joins on (system_id, so_id), (system_id, cust_key), (system_id, cust_key, shipto_seq_num)
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_so_header_open_orders
    ON erp_mirror_so_header (system_id, so_status)
    WHERE is_deleted = false;

-- so_detail: joins on (system_id, so_id), filters on backordered_qty
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_so_detail_so_lookup
    ON erp_mirror_so_detail (system_id, so_id)
    INCLUDE (item_ptr, sequence, backordered_qty);

-- item_branch: joins on (system_id, item_ptr)
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_item_branch_item_lookup
    ON erp_mirror_item_branch (system_id, item_ptr)
    INCLUDE (handling_code);

-- cust: joins on (system_id, cust_key)
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_cust_key_lookup
    ON erp_mirror_cust (system_id, cust_key)
    INCLUDE (cust_name);

-- cust_shipto: joins on (system_id, cust_key, seq_num)
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_cust_shipto_key_lookup
    ON erp_mirror_cust_shipto (system_id, cust_key, seq_num)
    INCLUDE (address_1, city);


-- -------------------------------------------------------------------------
-- 2. VIEW — Pre-defined join so the frontend queries a single flat view
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
-- 3. RPC FUNCTION — Returns the board data as JSON for /api/board/orders
--    Callable via supabase.rpc('get_board_open_orders')
-- -------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_board_open_orders()
RETURNS SETOF vw_board_open_orders
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
    SELECT * FROM vw_board_open_orders;
$$;

-- Grant execute to the roles Supabase uses for API calls
GRANT EXECUTE ON FUNCTION get_board_open_orders() TO anon, authenticated;
