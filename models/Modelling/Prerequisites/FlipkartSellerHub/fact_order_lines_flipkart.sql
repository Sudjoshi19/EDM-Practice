select
cast(Order_ID as string) as order_id,
brand,
'Flipkart' as platform_name,
{{store_name('store')}},
coalesce(FSN,'') product_id,
coalesce(SKU,'') sku,
'INR' as currency,
{% if var('currency_conversion_flag') %}
case when b.value is null then 1 else b.value end as exchange_currency_rate,
case when b.from_currency_code is null then 'INR' else b.from_currency_code end as exchange_currency_code,
{% else %}
cast(1 as decimal) as exchange_currency_rate,
'INR' as exchange_currency_code, 
{% endif %}
Buyer_Invoice_Date as date,
'' as subscription_id,
'Order' as transaction_type, 
false as is_cancelled,
'' as reason,
'' as email,
sum(Item_Quantity) as quantity,
sum(Final_Invoice_Amount__Price_after_discount_Shipping_Charges_) as total_price,
sum(Taxable_Value__Final_Invoice_Amount__Taxes_) subtotal_price,
sum(CAST(IGST_Amount AS numeric) + CAST(CGST_Rate AS numeric) + CAST(VAT_Amount AS numeric) + CAST(TCS_CGST_Amount AS numeric)) total_tax, 
sum(cast(Shipping_Charges as numeric)) as shipping_price, 
sum(cast(null as numeric)) as giftwrap_price, 
sum(Total_Discount) as item_discount,
sum(cast(null as numeric)) as shipping_discount
from {{ref('FlipkartSalesReport')}} ord
{% if var('currency_conversion_flag') %}
left join {{ ref('ExchangeRates') }} b on Buyer_Invoice_Date = b.date and 'INR' = b.to_currency_code
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

UNION ALL

select 
cast(ret.return_id as string) as order_id,
brand,
'Flipkart' as platform_name,
{{store_name('store')}},
cast(ret.order_item_id as string) as product_id, 
ret.sku,
'INR' as currency,
{% if var('currency_conversion_flag') %}
case when b.value is null then 1 else b.value end as exchange_currency_rate,
case when b.from_currency_code is null then 'INR' else b.from_currency_code end as exchange_currency_code,
{% else %}
cast(1 as decimal) as exchange_currency_rate,
'INR' as exchange_currency_code, 
{% endif %}
date(ret.return_approval_date) as date,
'' as subscription_id,
'Return' as transaction_type, 
false as is_cancelled,
ret.return_reason as reason,
'' as email,
sum(quantity) as quantity,
sum(cast(null as numeric)) as total_price,
sum(cast(null as numeric)) as subtotal_price,
sum(cast(null as numeric)) as total_tax,
sum(cast(null as numeric)) as shipping_price, 
sum(cast(null as numeric)) as giftwrap_price, 
sum(cast(null as numeric)) as item_discount,
sum(cast(null as numeric)) as shipping_discount
from {{ ref('FlipkartFulfillmentReturnsReport')}} ret
{% if var('currency_conversion_flag') %}
left join {{ ref('ExchangeRates') }} b on ret.return_approval_date = b.date and 'INR' = b.to_currency_code
{% endif %}
where ret.return_approval_date is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

