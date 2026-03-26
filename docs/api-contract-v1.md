# API Contract v1 — PO App + WH-Tracker ↔ Supabase

> This document defines the read contracts that the database optimization
> migration must preserve. Any drift from these contracts is a breaking change.

---

## PO App Contracts

### 1. PO Search — `app_po_search`

**Current client call:**
```js
supabase.from('app_po_search')
  .select('*')
  .or(`po_number.ilike.%${q}%,supplier_name.ilike.%${q}%,reference.ilike.%${q}%`)
  .order('synced_at', { ascending: false, nullsFirst: false })
  .limit(limit)
```

**Response shape (row):**
All columns of `app_po_search` view. Must include at minimum:
| Field | Type | Nullable | Notes |
|-------|------|----------|-------|
| po_number | text | no | |
| supplier_name | text | yes | |
| reference | text | yes | |
| synced_at | timestamptz | yes | sort column, NULLS LAST |

**Optimization:** Trigram GIN indexes on `po_number`, `supplier_name`, `reference`.
**Contract impact:** None. View untouched. Indexes are transparent.

---

### 2. Branch Open POs — `erp_mirror_po_header`

**Current client call:**
```js
supabase.from('erp_mirror_po_header')
  .select('po_id,system_id,supplier_key,purchase_type,order_date,expect_date,po_status,wms_status,reference,synced_at')
  .eq('system_id', branch)          // uppercase, e.g. 'AUS'
  .eq('is_deleted', false)
  .order('expect_date', { ascending: true, nullsFirst: false })
  .order('order_date', { ascending: false, nullsFirst: false })
  .limit(limit * 5)
```

**Response shape (row):**
| Field | Type | Nullable | Notes |
|-------|------|----------|-------|
| po_id | text | no | numeric string |
| system_id | text | no | uppercase branch code |
| supplier_key | text | yes | |
| purchase_type | text | yes | |
| order_date | date | yes | |
| expect_date | date | yes | NULLS LAST in sort |
| po_status | text | yes | app filters client-side |
| wms_status | text | yes | |
| reference | text | yes | |
| synced_at | timestamptz | yes | |

**App-side behavior:** After fetch, the app excludes closed/cancelled/received
statuses. The exact status codes are app logic, NOT database logic.

**New RPC (optional adoption):**
```js
supabase.rpc('get_branch_open_pos', { branch_id: 'AUS', row_limit: 250 })
```
Returns identical columns. Does NOT filter by po_status — preserves app-side
filtering contract.

---

### 3. PO Detail — `app_po_header` / `app_po_detail` / `app_po_receiving_summary`

**Current client calls (3 parallel):**
```js
// Routing: if poNumber is numeric string → filter by po_id; else → po_number
const filterCol = /^\d+$/.test(poNumber) ? 'po_id' : 'po_number'

// 3a. Header
supabase.from('app_po_header').select('*')
  .eq(filterCol, filterVal).limit(1).maybeSingle()

// 3b. Lines
supabase.from('app_po_detail').select('*')
  .eq(filterCol, filterVal).order('line_number', { ascending: true })

// 3c. Receiving summary
supabase.from('app_po_receiving_summary').select('*')
  .eq(filterCol, filterVal).limit(1).maybeSingle()
```

**Response shapes:**
- `header`: single row object or `null`
- `lines`: array of row objects, ordered by `line_number` ASC; empty array if none
- `receiving_summary`: single row object or `null`

**New RPC (optional adoption):**
```js
const { data } = await supabase.rpc('get_po_detail', {
  filter_col: 'po_id',    // or 'po_number'
  filter_val: '12345'
})
// data = { header: {...}|null, lines: [...], receiving_summary: {...}|null }
```

**Field mapping (old → new):**
| Old path | New path | Change |
|----------|----------|--------|
| header response | `data.header` | Wrapped in object |
| lines response | `data.lines` | Wrapped in object |
| receiving response | `data.receiving_summary` | Wrapped in object |

Nullability: `header` and `receiving_summary` are JSON `null` when not found
(matches `.maybeSingle()` behavior). `lines` is always `[]` when empty
(matches `.select()` empty result).

---

### 4. Submissions Summary — `submissions`

**Current client call:**
```js
supabase.from('submissions')
  .select('id,po_number,image_url,created_at')
  .in('po_number', poNumbers)
  .order('created_at', { ascending: false })
```

**Optimization:** Composite index `(po_number, created_at DESC)`.
**Contract impact:** None. Table and query untouched.

---

### 5. Submissions Detail — `submissions`

**Current client call:**
```js
supabase.from('submissions')
  .select('id,po_number,image_url,image_urls,submitted_username,branch,status,notes,reviewer_notes,created_at')
  .eq('po_number', poNumber)
  .eq('branch', branch)           // optional
  .order('created_at', { ascending: false })
```

**Optimization:** Composite index `(po_number, branch, created_at DESC)`.
**Contract impact:** None. Table and query untouched.

---

## WH-Tracker Contracts

### Board Open Orders — `vw_board_open_orders` (NEW)

**New client call:**
```js
supabase.from('vw_board_open_orders').select('*')
// or
supabase.rpc('get_board_open_orders')
```

**Response shape (row):**
| Field | Type | Nullable | Notes |
|-------|------|----------|-------|
| so_id | text | no | |
| cust_name | text | yes | LEFT JOIN → nullable |
| address_1 | text | yes | LEFT JOIN → nullable |
| city | text | yes | LEFT JOIN → nullable |
| reference | text | yes | |
| handling_code | text | yes | LEFT JOIN → nullable |
| line_count | bigint | no | COUNT() always returns value |

---

## Write Operations — UNCHANGED

These are **not touched** by the migration:

| Operation | Table | Status |
|-----------|-------|--------|
| POST /api/submissions | submissions INSERT | No change |
| PATCH /api/submissions/[id] | submissions UPDATE | No change |
| /api/setup user mgmt | auth + profiles | No change |

---

## Status Semantics

- `is_deleted = false`: Soft-delete filter. Applied in ALL read queries.
  Never changed by this migration.
- `so_status = 'K'`: Open SO status for WH-tracker board.
- `po_status`: Passed through as-is. App-side filtering preserved.
  The RPC does NOT interpret or filter by status.
