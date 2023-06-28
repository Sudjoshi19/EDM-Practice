select
cast(Order_Id_FK as string) as order_id,
brand,
'Myntra' as platform_name,
{{store_name('store')}},
'' as product_id, 
Myntra_SKU_Code as sku,
'INR' as currency,
{% if var('currency_conversion_flag') %}
case when b.value is null then 1 else b.value end as exchange_currency_rate,
case when b.from_currency_code is null then 'INR' else b.from_currency_code end as exchange_currency_code,
{% else %}
cast(1 as decimal) as exchange_currency_rate,
'INR' as exchange_currency_code, 
{% endif %}
Created_On_Date as date,
cast(null as string) as subscription_id,
'Order' as transaction_type,
case when Cancellation_Reason is null then false else true end as is_cancelled,
Cancellation_Reason as reason,
'' as email,
cast(null as integer) quantity,
sum(CAST(Final_Amount as numeric)) total_price,
sum(CAST(null as numeric)) subtotal_price,
sum(CAST(null as numeric)) total_tax, 
sum(cast(Shipping_Charge as numeric)) as shipping_price, 
sum(cast(Gift_Charge as numeric)) as  giftwrap_price,
sum(cast(Discount as numeric)) item_discount,
null as shipping_discount
from {{ref('MyntraSeller')}} a
{% if var('currency_conversion_flag') %}
left join {{ ref('ExchangeRates') }} b on Created_On_Date = b.date and 'INR' = b.to_currency_code
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
