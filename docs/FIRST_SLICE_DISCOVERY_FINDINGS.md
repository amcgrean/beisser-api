# First Slice Discovery Findings

Generated from the `2026-03-17 12:34:10` discovery run.

## High-confidence findings

- All 16 target mirror tables exist under the exact names we expected.
- All target tables are in schema `dbo`.
- The physical SQL primary key on every target table is `prrowid`.
- That means business/natural keys must be defined by us for Postgres merge logic.
- `pro2modified` and `update_date` exist on every target table in the first slice.
- This is enough to build an incremental sync strategy without relying on `prrowid` alone.

## Table volumes

Largest first-slice tables:

- `shipments_detail`: 4,397,953
- `so_detail`: 4,526,236
- `print_transaction_detail`: 3,878,102
- `print_transaction`: 2,722,200
- `item_branch`: 1,479,650
- `aropen`: 1,320,934
- `aropendt`: 1,320,933

This supports a staged-upsert design and argues against naive full-table polling for hot families.

## Natural key candidates

Recommended merge keys from discovery:

- `cust`: `system_id`, `cust_key`
- `cust_shipto`: `system_id`, `cust_key`, `seq_num`
- `item`: `system_id`, `item_ptr`
- `item_branch`: `item_ptr`, `system_id`
- `item_uomconv`: `system_id`, `item_ptr`, `uom_ptr`
- `so_header`: `system_id`, `so_id`
- `so_detail`: `system_id`, `so_id`, `sequence`
- `shipments_header`: `system_id`, `so_id`, `shipment_num`
- `shipments_detail`: `system_id`, `so_id`, `shipment_num`, `sequence`
- `wo_header`: `system_id`, `wo_id`
- `pick_header`: `system_id`, `pick_id`, `pick_seq`
- `pick_detail`: `system_id`, `pick_id`, `pick_seq`, `tran_id`, `tran_seq`
- `aropen`: `system_id`, `ref_num`, `ref_num_seq`
- `aropendt`: `system_id`, `ref_num`, `ref_num_seq`
- `print_transaction`: `system_id`, `tran_id`, `tran_type`, `seq_num`
- `print_transaction_detail`: `system_id`, `tran_id`, `printer_id`, `printer_destination`

## Change tracking columns

Strong candidates:

- `pro2modified`
- `update_date`
- `created_date`

Additional date-window helpers:

- `so_header.expect_date`
- `shipments_header.ship_date`
- `shipments_header.invoice_date`
- `aropen.ref_date`
- `aropendt.due_date`

## Stored procedure findings

Relevant procedures found:

- `GetARDetail`
- `usp_GetCustomersForPython`
- `usp_GetPOItems`

Notably absent from this targeted procedure search:

- no obvious `GetSO...`, `GetShipment...`, `GetWorkOrder...`, or `GetPick...` procedures surfaced as required dependencies

That makes direct table extraction the preferred starting path for the mirror.

## Recommended implementation order

1. Build direct extractor SQL for `cust`, `cust_shipto`, `item`, `item_branch`, `item_uomconv`
2. Build direct extractor SQL for `so_header`, `so_detail`, `shipments_header`, `shipments_detail`
3. Build direct extractor SQL for `wo_header`, `pick_header`, `pick_detail`
4. Build AR/doc parity validation against `GetARDetail` and `usp_GetCustomersForPython`
5. Only preserve stored-procedure behavior where discovery plus parity tests show a missing business filter
