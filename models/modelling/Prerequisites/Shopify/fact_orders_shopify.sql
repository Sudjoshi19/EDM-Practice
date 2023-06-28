select
ord.order_id,
ord.brand,
'Shopify' as platform_name,
{{store_name('store')}},
currency,
exchange_currency_code,
exchange_currency_rate,
date(created_at) as date,
'Order' as transaction_type, 
false as is_cancelled,
sum(order_quantity) quantity,
sum(cast(order_price AS numeric) + ifnull(CAST(total_tax AS numeric),0)) total_price,
sum(cast(order_price AS numeric)) subtotal_price,
sum(cast(total_tax AS numeric)) total_tax, 
sum(cast(order_shipping_price as numeric)) as shipping_price, 
cast(null as numeric) as giftwrap_price, 
sum(cast(total_discounts AS numeric)) as order_discount,
sum(cast(null as numeric)) as shipping_discount,
email
from (
select *
from {{ ref('ShopifyOrders') }}) ord
left join (select order_id,brand,sum(cast(line_items_quantity as numeric)) order_quantity, sum(CAST(line_items_price AS numeric)) order_price from {{ref('ShopifyOrdersLineItems')}} group by 1,2) ord_ln_itms
on ord.order_id=ord_ln_itms.order_id and ord.brand = ord_ln_itms.brand
left join (select order_id,brand,sum(cast(shipping_lines_price as numeric)) order_shipping_price from {{ref('ShopifyOrdersShippingLines')}} group by 1,2) ord_shp_lns
on ord.order_id=ord_shp_lns.order_id and ord.brand = ord_shp_lns.brand
group by 1,2,3,4,5,6,7,8,9,10,19

UNION ALL

select 
cast(rfnd_tran.refund_id as string) as order_id,
brand,
'Shopify' as platform_name,
{{store_name('store')}},
rfnd_tran.transactions_currency as currency,
rfnd_tran.exchange_currency_code,
rfnd_tran.exchange_currency_rate,
date(rfnd_tran.created_at) as date,
'Return' as transaction_type, 
false as is_cancelled,
sum(cast(return_quantity as numeric)) quantity,
sum(cast(transactions_amount as numeric)) total_price,
sum(cast(refund_price as numeric)) as subtotal_price, 
sum(cast(refund_tax as numeric)) total_tax,
cast(null as numeric) as shipping_price, 
cast(null as numeric) as giftwrap_price, 
sum(cast(refund_discount as numeric)) order_discount,
cast(null as numeric) as shipping_discount, 
'' as email
from {{ ref('ShopifyRefundsTransactions')}} rfnd_tran
left join (select cast(refund_id as string) as order_id, sum(cast(refund_line_items_quantity as numeric)) return_quantity, sum(refund_line_items_subtotal) refund_price, sum(refund_line_items_total_tax) refund_tax from {{ref('ShopifyRefundsRefundLineItems')}} group by 1) rfnd_lns
on cast(rfnd_tran.refund_id as string)=rfnd_lns.order_id
left join (select cast(refund_id as string) as order_id, sum(cast(line_item_total_discount as numeric)) refund_discount from {{ref('ShopifyRefundsLineItems')}} group by 1) ord_rfnd_ln_itms
on cast(rfnd_tran.refund_id as string)=ord_rfnd_ln_itms.order_id
where created_at is not null
group by 1,2,3,4,5,6,7,8,9,10,19
