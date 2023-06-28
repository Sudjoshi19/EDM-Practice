select
  order_id,
  brand,
  'TargetPlus' as platform_name,
  {{store_name('store')}},
  'USD' as currency,
  'USD' as exchange_currency_code,
  1 as exchange_currency_rate,
  cast(Date_Placed as DATE) as date,
  'Order' as transaction_type, 
  false as is_cancelled,
  coalesce(sum(cast(quantity as numeric)),cast(null as numeric)) quantity,
  coalesce(sum(Unit_Price * quantity),cast(null as numeric)) total_price,
  cast(null as numeric) as subtotal_price,
  cast(null as numeric) as total_tax,
  cast(null as numeric) as shipping_price,
  cast(null as numeric) as giftwrap_price,
  coalesce(sum(cast(Red_Card_Discount as numeric)),cast(null as numeric)) order_discount,
  cast(null as numeric) as shipping_discount,
  null as email
  from {{ ref('TargetPlus_Orders') }}
  group by 1,2,3,4,5,6,7,8,9,10,19
