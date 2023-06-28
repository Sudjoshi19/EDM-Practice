select
  cast(null as string) as order_id,
  brand,
  'Amazon Vendor Central' as platform_name,
  marketplaceName as store_name,
  currencyCode as currency,
  exchange_currency_code,
  exchange_currency_rate,
  startDate as date,
  'Order' as transaction_type, 
  false as is_cancelled,
  coalesce(sum(shippedUnits),cast(null as numeric)) quantity,
  coalesce(sum(shippedcogs),cast(null as numeric)) total_price,
  cast(null as numeric) as subtotal_price,
  cast(null as numeric) as total_tax,
  cast(null as numeric) as shipping_price,
  cast(null as numeric) as giftwrap_price,
  cast(null as numeric) as order_discount,
  cast(null as numeric) as shipping_discount,
  '' as email
  from {{ ref('VendorSalesReportBySourcing') }} 
  group by 1,2,3,4,5,6,7,8,9,10,19

  UNION ALL
  
  select
  cast(null as string) as order_id,
  brand,
  'Amazon Vendor Central' as platform_name,
  marketplaceName as store_name,
  currencyCode as currency,
  exchange_currency_code,
  exchange_currency_rate,
  startDate as date,
  'Return' as transaction_type, 
  false as is_cancelled,
  coalesce(sum(customerReturns),cast(null as numeric)) quantity,
  coalesce(sum(shippedRevenue),cast(null as numeric)) total_price,
  cast(null as numeric) as subtotal_price,
  cast(null as numeric) as total_tax,
  cast(null as numeric) as shipping_price,
  cast(null as numeric) as giftwrap_price,
  cast(null as numeric) as order_discount,
  cast(null as numeric) as shipping_discount,
  '' as email
  from {{ ref('VendorSalesReportBySourcing') }} 
  group by 1,2,3,4,5,6,7,8,9,10,19