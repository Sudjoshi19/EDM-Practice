select
purchaseOrderId as order_id,
brand,
'Walmart' as platform_name,
{{store_name('chargeAmount_currency')}},
chargeAmount_currency as currency,
exchange_currency_code,
exchange_currency_rate,
orderDate as date,
'Order' as transaction_type,
false as is_cancelled,
sum(cast(orderLineQuantity_amount as numeric)) quantity,
sum(CAST(chargeAmount_amount as numeric) + ifnull(tax_amount,0)) total_price,
sum(CAST(chargeAmount_amount as numeric)) subtotal_price,
sum(tax_amount) as total_tax, 
cast(null as numeric) as shipping_price,
cast(null as numeric) as giftwrap_price,
cast(null as numeric) as order_discount,
cast(null as numeric) as shipping_discount,
customerEmailId as email
from {{ref('WalmartOrders')}}
group by 1,2,3,4,5,6,7,8,9,10,19

UNION ALL

select
cast(purchaseOrderId as string) order_id,
brand,
'Walmart' as platform_name,
{{store_name('totalRefundAmount_currencyUnit')}},
totalRefundAmount_currencyUnit as currency,
exchange_currency_code,
exchange_currency_rate,
date(returnOrderDate) as date,
'Return' as transaction_type,
false as is_cancelled,
sum(refundedQty) as quantity,
sum(totalRefundAmount_currencyAmount) as total_price,
cast(null as numeric) as subtotal_price,
cast(null as numeric) as total_tax,
cast(null as numeric) as shipping_price,
cast(null as numeric) as giftwrap_price,
cast(null as numeric) as order_discount,
cast(null as numeric) as shipping_discount,
customerEmailId as email
from {{ref('WalmartReturnsOrderLines')}}
group by 1,2,3,4,5,6,7,8,9,10,19