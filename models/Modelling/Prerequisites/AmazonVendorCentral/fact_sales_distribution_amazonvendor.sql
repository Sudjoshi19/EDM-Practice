
select
brand,
'Amazon Vendor Central' as platform_name,
'Sourcing' as distribution_view,
marketplaceName as store_name,
asin as product_id, 
cast(null as string) as sku,
currencyCode as currency,
exchange_currency_code,
exchange_currency_rate,
startdate as order_date,
sum(shippedUnits) shipped_units,
sum(shippedcogs) shippedcogs,
sum(shippedRevenue) shipped_revenue,
sum(cast(null as numeric)) as ordered_revenue,
sum(customerReturns) customer_returns
from {{ ref('VendorSalesReportBySourcing') }}
group by 1,2,3,4,5,6,7,8,9,10

union all

select
brand,
'Amazon Vendor Central' as platform_name,
'Manufacturing' as distribution_view,
marketplaceName as store_name,
asin as product_id, 
cast(null as string) as sku,
currencyCode as currency,
exchange_currency_code,
exchange_currency_rate,
startdate as order_date,
sum(shippedUnits) quantity,
sum(shippedcogs) shippedcogs,
sum(shippedRevenue) shipped_revenue,
sum(orderedRevenue) as ordered_revenue,
sum(customerReturns) customer_returns 
from {{ ref('VendorSalesReportByManufacturing') }}
group by 1,2,3,4,5,6,7,8,9,10