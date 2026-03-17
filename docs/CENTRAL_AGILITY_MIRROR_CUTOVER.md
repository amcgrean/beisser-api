# Central Agility Mirror Cutover

## Purpose

`agility-api` is the on-prem Raspberry Pi worker that mirrors Agility SQL Server into normalized Postgres tables for all downstream cloud apps.

## Responsibilities

- local read access to Agility SQL Server
- cloud write access to Supabase/Postgres only
- normalized ERP-like mirror tables
- sync metadata, worker heartbeat, batch history, and per-table state

## First vertical slice

- `cust`
- `cust_shipto`
- `item`
- `item_branch`
- `item_uomconv`
- `so_header`
- `so_detail`
- `shipments_header`
- `shipments_detail`
- `wo_header`
- `pick_header`
- `pick_detail`
- `aropen`
- `aropendt`
- `print_transaction`
- `print_transaction_detail`

Each table includes:

- natural ERP keys
- `source_updated_at`
- `synced_at`
- `sync_batch_id`
- `row_fingerprint`
- `is_deleted`

Worker/system tables:

- `erp_sync_state`
- `erp_sync_batches`
- `erp_sync_table_state`

## Consumers

- WH-Tracker
- ToolBxAPI
- PO-Pics / receiving
- bids
- BI

Apps query this mirror and do not reach into the local ERP network.
