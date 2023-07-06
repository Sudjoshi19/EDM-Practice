select
  '' as order_id,
  brand,
  'Walmart Retail' as platform_name,
  STORE_NAME as store_name,
  currency,
  {% if var('currency_conversion_flag') %}
    case when b.value is null then 1 else b.value end as exchange_currency_rate,
    case when b.from_currency_code is null then a.currency else b.from_currency_code end as exchange_currency_code,
  {% else %}
    cast(1 as decimal) as exchange_currency_rate,
    cast(a.currency as string) as exchange_currency_code, 
  {% endif %}
  DAILY_REPORT_DATE as date,
  'Order' as transaction_type,
  false as is_cancelled,
  sum(cast(POS_QTY as numeric)) quantity,
  sum(CAST(regexp_replace(POS_SALES,'[^0-9]+','') as numeric)) total_price,
  sum(CAST(regexp_replace(POS_SALES,'[^0-9]+','') as numeric)) subtotal_price,
  cast(null as numeric) as total_tax,
  cast(null as numeric) as shipping_price,
  cast(null as numeric) as giftwrap_price,
  cast(null as numeric) as order_discount,
  cast(null as numeric) as shipping_discount,
  '' as email
  from (
  select *,
  'USD' as currency
  from {{ref('WalmartRetailSalesReport')}}) a
  {% if var('currency_conversion_flag') %}
  left join {{ ref('ExchangeRates')}} b on date(a.DAILY_REPORT_DATE) = b.date and a.currency = b.to_currency_code  
  {% endif %}
  group by 1,2,3,4,5,6,7,8,9,10,19
