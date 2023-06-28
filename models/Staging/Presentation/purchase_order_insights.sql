select 
b.brand_name,
vendor_id,
c.platform_name,
c.store_name,
product_id,
product_name,
order_date,
currency_code,
sum(netcost_amount) netcost_amount,
sum(listprice_amount) listprice_amount,
sum(ordered_quantity_amount) ordered_quantity_amount,
sum(open_po_accepted_quantity_amount) open_po_accepted_quantity_amount,
sum(closed_po_accepted_quantity_amount) closed_po_accepted_quantity_amount,
sum(open_po_rejected_quantity_amount) open_po_rejected_quantity_amount,
sum(closed_po_rejected_quantity_amount) closed_po_rejected_quantity_amount,
sum(open_po_received_quantity_amount) open_po_received_quantity_amount,
sum(closed_po_received_quantity_amount) closed_po_received_quantity_amount
from {{ ref('fact_purchase_orders') }} a
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform') }} c
on a.platform_key = c.platform_key
left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} where status = 'Active') d
on a.product_key = d.product_key
left join {{ ref('dim_orders') }} e
on a.order_key = e.order_key
left join {{ ref('dim_vendor') }} f
on a.vendor_key = f.vendor_key
group by 1,2,3,4,5,6,7,8