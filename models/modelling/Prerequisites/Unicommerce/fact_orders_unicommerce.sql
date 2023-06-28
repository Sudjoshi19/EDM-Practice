  select
  concat(DisplayOrderCode,SaleOrderItemCode) as order_id,
  brand,
  ChannelName as platform_name,
  {{store_name('store')}},
  currency,
  exchange_currency_code,
  exchange_currency_rate,
  CAST(order_date as date) as date,
  'Order' as transaction_type,
  false as is_cancelled,
  sum(cast(null as numeric)) quantity,
  sum(CAST(REGEXP_REPLACE(totalprice,'','0') as numeric)) total_price,
  sum(CAST(REGEXP_REPLACE(subtotal,'','0') as numeric)) subtotal_price,
  sum(CAST(REGEXP_REPLACE(cgst,'','0') AS numeric) + cast(REGEXP_REPLACE(sgst,'','0') as numeric) + cast(REGEXP_REPLACE(igst,'','0') as numeric)) total_tax, 
  sum(cast(REGEXP_REPLACE(ShippingCharges,'','0') as numeric) + cast(REGEXP_REPLACE(ShippingMethodCharges,'','0') as numeric) + cast(REGEXP_REPLACE(CODServiceCharges,'','0') as numeric)) as shipping_price, 
  sum(cast(REGEXP_REPLACE(GiftWrapCharges,'','0') as numeric)) as  giftwrap_price,
  sum(cast(REGEXP_REPLACE(Discount,'','0') as numeric)) order_discount,
  cast(null as numeric) as shipping_discount,
  NotificationEmail as email
  from {{ref('UnicommerceSaleOrders')}}
  group by 1,2,3,4,5,6,7,8,9,10,19

  UNION ALL 

  select
  concat(DisplayOrderCode,SaleOrderItemCode) as order_id,
  brand,
  ChannelName as platform_name,
  {{store_name('store')}},
  currency,
  exchange_currency_code,
  exchange_currency_rate,
  cast(ReturnDate as date) date,
  'Return' as transaction_type,
  false as is_cancelled,
  null as units_returned,
  sum(return_amount) as total_price,
  null as subtotal_price,
  null as total_tax,
  null as shipping_price, 
  null as giftwrap_price, 
  null as order_discount,
  null as shipping_discount,
  NotificationEmail as email
  from (
      SELECT * {{exclude()}}(ReturnDate),
      REGEXP_REPLACE(ReturnDate,'',null) ReturnDate,
      case when ReversePickupCode is not null then cast(REGEXP_REPLACE(subtotal,'','0') as numeric) else 0 end as return_amount
      from {{ ref('UnicommerceSaleOrders') }})
  group by 1,2,3,4,5,6,7,8,9,10,19