alter table public.erp_mirror_po_header
add column if not exists supplier_code text null;
