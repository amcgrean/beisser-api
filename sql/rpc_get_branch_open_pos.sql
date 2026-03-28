-- RPC: get_branch_open_pos
-- Returns open POs for a given branch (system_id).
-- Captured from live Supabase 2026-03-27.

CREATE OR REPLACE FUNCTION public.get_branch_open_pos(
    branch_id text,
    row_limit integer DEFAULT 500
)
RETURNS TABLE(
    po_id bigint,
    system_id text,
    supplier_code text,
    supplier_name text,
    shipfrom_seq integer,
    purchase_type text,
    order_date text,
    expect_date text,
    po_status text,
    wms_status text,
    reference text,
    synced_at text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $function$
  select
    h.po_id::bigint,
    h.system_id::text,
    h.supplier_code::text,
    h.supplier_name::text,
    h.shipfrom_seq,
    h.purchase_type::text,
    h.order_date::text,
    h.expect_date::text,
    h.po_status::text,
    h.wms_status::text,
    h.reference::text,
    h.synced_at::text
  from public.erp_mirror_po_header h
  where h.system_id = branch_id
    and coalesce(h.is_deleted, false) = false
    and (
      h.po_status is null
      or (
        h.po_status not ilike '%closed%'
        and h.po_status not ilike '%complete%'
        and h.po_status not ilike '%cancel%'
        and h.po_status not ilike '%void%'
        and h.po_status not ilike '%received%'
      )
    )
    and (
      h.wms_status is null
      or (
        h.wms_status not ilike '%closed%'
        and h.wms_status not ilike '%complete%'
        and h.wms_status not ilike '%cancel%'
        and h.wms_status not ilike '%void%'
        and h.wms_status not ilike '%received%'
      )
    )
  order by h.expect_date asc nulls last,
           h.order_date desc nulls last
  limit row_limit;
$function$;
