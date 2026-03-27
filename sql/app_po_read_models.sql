-- App-facing PO read models for the shared agility-api Supabase database.
-- This file is intentionally not applied automatically. Review before running.
--
-- Design goals:
-- - leave raw erp_mirror_* tables untouched
-- - expose stable app_* views for PO check-in and PO pics
-- - keep joins and computed totals centralized in the database

create or replace view public.app_po_header as
with po_totals as (
    select
        d.system_id,
        d.po_id,
        sum(
            coalesce(d.qty_ordered, 0)
            * (
                coalesce(d.cost, 0)
                / nullif(coalesce(d.disp_cost_conv, 1), 0)
            )
        )::numeric(18, 2) as po_total,
        count(*)::int as line_count
    from public.erp_mirror_po_detail d
    where coalesce(d.is_deleted, false) = false
    group by d.system_id, d.po_id
),
receiving_summary as (
    select
        rh.system_id,
        rh.po_id,
        count(distinct rh.receive_num)::int as receipt_count,
        min(rh.receive_date) as first_receive_date,
        max(rh.receive_date) as last_receive_date
    from public.erp_mirror_receiving_header rh
    where coalesce(rh.is_deleted, false) = false
    group by rh.system_id, rh.po_id
),
receiving_qty as (
    select
        rd.system_id,
        rd.po_id,
        sum(coalesce(rd.qty, 0))::numeric(18, 4) as qty_received_total
    from public.erp_mirror_receiving_detail rd
    where coalesce(rd.is_deleted, false) = false
    group by rd.system_id, rd.po_id
)
select
    h.system_id,
    h.po_id,
    h.po_id::text as po_number,
    h.purchase_type,
    h.supplier_code,
    h.shipfrom_seq,
    h.supplier_name,
    null::text as supplier_city,
    null::text as supplier_state,
    null::text as supplier_branch_code,
    h.order_date,
    h.expect_date,
    h.due_date,
    h.buyer,
    h.reference,
    h.ship_via,
    h.current_receive_no,
    h.po_status,
    h.wms_status,
    h.received_manually,
    h.mwt_recv_complete,
    h.mwt_recv_complete_datetime,
    coalesce(pt.po_total, 0)::numeric(18, 2) as po_total,
    coalesce(pt.line_count, 0) as line_count,
    coalesce(rs.receipt_count, 0) as receipt_count,
    rs.first_receive_date,
    rs.last_receive_date,
    coalesce(rq.qty_received_total, 0)::numeric(18, 4) as qty_received_total,
    h.created_date,
    h.update_date,
    h.source_updated_at,
    h.synced_at
from public.erp_mirror_po_header h
left join po_totals pt
    on pt.system_id = h.system_id
   and pt.po_id = h.po_id
left join receiving_summary rs
    on rs.system_id = h.system_id
   and rs.po_id = h.po_id
left join receiving_qty rq
    on rq.system_id = h.system_id
   and rq.po_id = h.po_id
where coalesce(h.is_deleted, false) = false;


create or replace view public.app_po_detail as
select
    d.system_id,
    d.po_id,
    d.po_id::text as po_number,
    d.sequence as line_number,
    d.item_ptr,
    i.item as item_code,
    coalesce(d.po_desc, i.description) as description,
    d.size_,
    d.qty_ordered,
    d.uom,
    d.cost,
    d.disp_cost_conv,
    d.display_cost_uom,
    (
        coalesce(d.qty_ordered, 0)
        * (
            coalesce(d.cost, 0)
            / nullif(coalesce(d.disp_cost_conv, 1), 0)
        )
    )::numeric(18, 2) as extended_total,
    d.po_status,
    d.canceled,
    d.due_date,
    d.expect_date,
    d.exp_rcpt_date,
    d.exp_ship_date,
    d.wo_id,
    wo.wo_status,
    wo.department as wo_department,
    wo.branch_code as wo_branch_code,
    d.created_date,
    d.update_date,
    d.source_updated_at,
    d.synced_at
from public.erp_mirror_po_detail d
left join public.erp_mirror_item i
    on i.system_id = d.system_id
   and i.item_ptr = d.item_ptr
   and coalesce(i.is_deleted, false) = false
left join public.erp_mirror_wo_header wo
    on wo.wo_id = d.wo_id
   and coalesce(wo.is_deleted, false) = false
where coalesce(d.is_deleted, false) = false;


create or replace view public.app_po_receiving_summary as
select
    h.system_id,
    h.po_id,
    h.po_id::text as po_number,
    count(distinct rh.receive_num)::int as receipt_count,
    min(rh.receive_date) as first_receive_date,
    max(rh.receive_date) as last_receive_date,
    sum(coalesce(rd.qty, 0))::numeric(18, 4) as qty_received_total,
    max(case when coalesce(rh.recv_status, '') <> '' then rh.recv_status end) as latest_recv_status
from public.erp_mirror_po_header h
left join public.erp_mirror_receiving_header rh
    on rh.system_id = h.system_id
   and rh.po_id = h.po_id
   and coalesce(rh.is_deleted, false) = false
left join public.erp_mirror_receiving_detail rd
    on rd.system_id = rh.system_id
   and rd.po_id = rh.po_id
   and rd.receive_num = rh.receive_num
   and coalesce(rd.is_deleted, false) = false
where coalesce(h.is_deleted, false) = false
group by h.system_id, h.po_id;


create or replace view public.app_po_search as
select
    h.system_id,
    h.po_id,
    h.po_number,
    h.purchase_type,
    h.supplier_code,
    h.supplier_name,
    h.supplier_city,
    h.supplier_state,
    h.supplier_branch_code as branch_code,
    h.order_date,
    h.expect_date,
    h.due_date,
    h.buyer,
    h.reference,
    h.ship_via,
    h.po_status,
    h.wms_status,
    h.po_total,
    h.line_count,
    h.receipt_count,
    h.last_receive_date,
    h.qty_received_total,
    h.synced_at
from public.app_po_header h;


comment on view public.app_po_header is
'App-facing PO header read model built from ERP mirror header, detail, receiving, and supplier ship-to tables.';

comment on view public.app_po_detail is
'App-facing PO line read model with item and work-order context.';

comment on view public.app_po_receiving_summary is
'App-facing PO receiving summary built from ERP mirror receiving tables.';

comment on view public.app_po_search is
'Thin PO search view intended for worker and supervisor search screens.';
