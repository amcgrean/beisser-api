-- Normalize so_id from integer to text across so_detail, shipments_header,
-- and shipments_detail tables. Normalize seq_num from integer to text in
-- cust_shipto. This aligns all join columns with so_header.so_id (text)
-- and so_header.shipto_seq_num (text).

-- so_detail.so_id: integer -> text
ALTER TABLE public.erp_mirror_so_detail
    ALTER COLUMN so_id TYPE text USING so_id::text;

-- shipments_header.so_id: integer -> text
ALTER TABLE public.erp_mirror_shipments_header
    ALTER COLUMN so_id TYPE text USING so_id::text;

-- shipments_detail.so_id: integer -> text
ALTER TABLE public.erp_mirror_shipments_detail
    ALTER COLUMN so_id TYPE text USING so_id::text;

-- cust_shipto.seq_num: integer -> text
ALTER TABLE public.erp_mirror_cust_shipto
    ALTER COLUMN seq_num TYPE text USING seq_num::text;
