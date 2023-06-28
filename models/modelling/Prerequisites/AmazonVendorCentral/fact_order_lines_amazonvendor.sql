select
cast(null as string) as order_id,
brand,
'Amazon Vendor Central' as platform_name,
marketplaceName as store_name,
asin as product_id, 
cast(null as string) as sku,
currencyCode as currency,
exchange_currency_code,
exchange_currency_rate,
startDate as date,
cast(null as string) as subscription_id,
'Order' as transaction_type, 
false as is_cancelled,
'' as reason,
'' as email,
sum(shippedUnits) quantity,
sum(shippedcogs) total_price,
cast(null as numeric) as subtotal_price,
cast(null as numeric) as total_tax,
cast(null as numeric) as shipping_price,
cast(null as numeric) as giftwrap_price,
cast(null as numeric) as item_discount,
cast(null as numeric) as shipping_discount
from {{ ref('VendorSalesReportBySourcing') }} 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

UNION ALL

select
cast(null as string) as order_id,
brand,
'Amazon Vendor Central' as platform_name,
marketplaceName as store_name,
asin as product_id, 
cast(null as string) as sku,
currencyCode as currency,
exchange_currency_code,
exchange_currency_rate,
startDate as date,
cast(null as string) as subscription_id,
'Return' as transaction_type, 
false as is_cancelled,
'' as reason,
'' as email,
sum(customerReturns) quantity,
sum(shippedRevenue) total_price,
cast(null as numeric) as subtotal_price,
cast(null as numeric) as total_tax,
cast(null as numeric) as shipping_price,
cast(null as numeric) as giftwrap_price,
cast(null as numeric) as item_discount,
cast(null as numeric) as shipping_discount
from {{ ref('VendorSalesReportBySourcing') }} 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    