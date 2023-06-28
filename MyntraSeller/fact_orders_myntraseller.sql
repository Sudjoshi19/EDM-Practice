select
  cast(Order_Id_FK as string) as order_id,
  brand,
  'Myntra' as platform_name,
  {{store_name('store')}},
  'INR' as currency,
  {% if var('currency_conversion_flag') %}
  case when b.value is null then 1 else b.value end as exchange_currency_rate,
  case when b.from_currency_code is null then 'INR' else b.from_currency_code end as exchange_currency_code,
  {% else %}
  cast(1 as decimal) as exchange_currency_rate,
  'INR' as exchange_currency_code, 
  {% endif %}
  Created_On_Date as date,
  'Order' as transaction_type, 
  false as is_cancelled,
  sum(cast(null as numeric)) quantity,
  coalesce(sum(cast(Final_Amount as numeric)),cast(null as numeric)) total_price,
  sum(CAST(null as numeric)) subtotal_price,
  sum(CAST(null as numeric)) total_tax, 
  coalesce(sum(cast(Shipping_Charge as numeric)),cast(null as numeric)) shipping_price,
  coalesce(sum(cast(Gift_Charge as numeric)),cast(null as numeric)) giftwrap_price,
  coalesce(sum(cast(Discount as numeric)),cast(null as numeric)) order_discount,
  cast(null as numeric) as shipping_discount,
  '' as email
  from {{ ref('MyntraSeller') }} a 
  {% if var('currency_conversion_flag') %}
    left join {{ ref('ExchangeRates') }} b on Created_On_Date = b.date and 'INR' = b.to_currency_code
  {% endif %}
  group by 1,2,3,4,5,6,7,8,9,10,19
