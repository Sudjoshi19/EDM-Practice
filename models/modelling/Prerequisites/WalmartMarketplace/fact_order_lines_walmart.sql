select
purchaseOrderId as order_id,
brand,
'Walmart' as platform_name,
{{store_name('store')}},
'' as product_id, 
sku,
chargeAmount_currency as currency,
exchange_currency_code,
exchange_currency_rate,
orderDate as date,
cast(null as string) as subscription_id,
'Order' as transaction_type,
false as is_cancelled,
'' as reason,
customerEmailId as email,
sum(cast(orderLineQuantity_amount as numeric)) quantity,
sum(CAST(chargeAmount_amount as numeric) + ifnull(tax_amount,0)) total_price,
sum(CAST(chargeAmount_amount as numeric)) subtotal_price,
sum(tax_amount) as total_tax, 
null as shipping_price, 
null as giftwrap_price,
null as item_discount,
null as shipping_discount
from {{ref('WalmartOrders')}}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

UNION ALL

select
cast(purchaseOrderId as string) order_id,
brand,
'Walmart' as platform_name,
{{store_name('store')}},
'' as product_id, 
sku,
totalRefundAmount_currencyUnit as currency,
exchange_currency_code,
exchange_currency_rate,
date(returnOrderDate) as date,
cast(null as string) as subscription_id,
'Return' as transaction_type,
false as is_cancelled,
returnReason as reason,
customerEmailId as email,
sum(refundedQty) as quantity,
sum(totalRefundAmount_currencyAmount) as total_price,
null as subtotal_price,
null as total_tax, 
null as shipping_price, 
null as giftwrap_price,
null as item_discount,
null as shipping_discount
from {{ref('WalmartReturnsOrderLines')}}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15