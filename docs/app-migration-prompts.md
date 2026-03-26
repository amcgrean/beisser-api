# App Migration Prompts

> Copy-paste these prompts into Claude Code sessions in the respective app repos
> AFTER the Supabase migration has been applied.

---

## Prompt 1: PO App

```
## Task: Optimize PO App for new Supabase RPC functions

The Supabase database now has two new RPC functions that reduce round-trips
and leverage server-side indexes. Adopt them while preserving all existing
behavior.

### What changed on Supabase

1. `get_branch_open_pos(branch_id text, row_limit integer DEFAULT 250)`
   - Replaces: direct query on `erp_mirror_po_header` with system_id + is_deleted filters
   - Returns: same 10 columns (po_id, system_id, supplier_key, purchase_type,
     order_date, expect_date, po_status, wms_status, reference, synced_at)
   - Sorted: expect_date ASC NULLS LAST, order_date DESC NULLS LAST
   - DOES NOT filter by po_status — your app-side status filtering must remain

2. `get_po_detail(filter_col text, filter_val text)`
   - Replaces: 3 parallel queries to app_po_header, app_po_detail, app_po_receiving_summary
   - filter_col must be 'po_id' (numeric) or 'po_number' (non-numeric)
   - Returns JSONB: { header: object|null, lines: array, receiving_summary: object|null }
   - header = full app_po_header row or null (same as .maybeSingle())
   - lines = array ordered by line_number ASC (same as current, empty [] if none)
   - receiving_summary = full app_po_receiving_summary row or null

3. New trigram indexes on erp_mirror_po_header (po_number, supplier_name, reference)
   - Your ILIKE search on app_po_search is now index-accelerated automatically
   - No code change needed for search

4. New composite indexes on submissions (po_number, created_at DESC) and
   (po_number, branch, created_at DESC)
   - Your IN() and eq() queries on submissions are now index-accelerated
   - No code change needed

### Changes to make

1. **Branch open POs (supervisor open-pos list)**
   Find where you query `erp_mirror_po_header` with `.eq('system_id', branch).eq('is_deleted', false)`.
   Replace with:
   ```js
   const { data, error } = await supabase.rpc('get_branch_open_pos', {
     branch_id: branch,      // uppercase, e.g. 'AUS'
     row_limit: limit * 5
   })
   ```
   Keep your existing app-side status filtering logic exactly as-is.

2. **PO detail page (/api/po/[poNumber])**
   Find where you run 3 parallel queries (app_po_header, app_po_detail,
   app_po_receiving_summary). Replace with:
   ```js
   const filterCol = /^\d+$/.test(poNumber) ? 'po_id' : 'po_number'
   const { data, error } = await supabase.rpc('get_po_detail', {
     filter_col: filterCol,
     filter_val: poNumber
   })
   const header = data?.header ?? null
   const lines = data?.lines ?? []
   const receivingSummary = data?.receiving_summary ?? null
   ```
   The response payload to the frontend must remain identical.

### Do NOT change
- PO search (/api/po/search) — already optimized via indexes, no code change
- Submissions queries — already optimized via indexes, no code change
- Any write operations (POST /api/submissions, PATCH /api/submissions/[id], /api/setup)
- Auth/authorization behavior
- Response payload shapes to the frontend
```

---

## Prompt 2: WH-Tracker

```
## Task: Optimize WH-Tracker Order Board for new Supabase view + RPC

The Supabase database now has a pre-built view and RPC function for the
order board. This replaces the 5-table join currently built client-side.

### What changed on Supabase

1. `vw_board_open_orders` — a new view that encapsulates the full 5-table join:
   - Joins: erp_mirror_so_detail → so_header → item_branch → cust → cust_shipto
   - Filters: is_deleted=false, so_status='K', backordered_qty=0
   - Groups by: so_id, cust_name, address_1, city, reference, handling_code
   - Orders by: handling_code, so_id
   - Returns columns: so_id, cust_name, address_1, city, reference,
     handling_code, line_count

2. `get_board_open_orders()` — RPC that returns the same data:
   ```js
   supabase.rpc('get_board_open_orders')
   ```

3. All join columns have covering indexes, so the view executes via
   index-only scans.

### Changes to make

Find the code that builds the order board query (the 5-table join with
so_header, so_detail, item_branch, cust, cust_shipto). It likely runs
in /warehouse/board/orders or a similar route/page.

**Option A — Use the view directly:**
```js
const { data, error } = await supabase
  .from('vw_board_open_orders')
  .select('*')
```

**Option B — Use the RPC:**
```js
const { data, error } = await supabase.rpc('get_board_open_orders')
```

Both return identical data. Use Option A if you want to add .eq()/.order()
filters from the client. Use Option B for the simple "get everything" case.

### Response shape (per row)
| Field | Type | Nullable | Notes |
|-------|------|----------|-------|
| so_id | text | no | |
| cust_name | text | yes | null if customer not found |
| address_1 | text | yes | null if ship-to not found |
| city | text | yes | null if ship-to not found |
| reference | text | yes | |
| handling_code | text | yes | null if item_branch not found |
| line_count | bigint | no | always >= 1 |

### Map your current fields
Your current 5-table join query returns the same columns listed above.
Map your frontend state/props to these field names. If your frontend
currently uses different aliases (e.g. `customer_name` vs `cust_name`),
add the mapping in your data layer — do not change the view.

### Do NOT change
- Any other queries on the board page (if there are 4 other queries beyond
  the main join, those are unchanged unless they also hit the same tables
  with now-indexed columns — in which case they're already faster)
- Write operations
- Auth/authorization behavior
- Soft-delete semantics (the view handles is_deleted=false internally)
```
