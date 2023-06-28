select
concat(DisplayOrderCode,SaleOrderItemCode) as order_id,
brand,
ChannelName as platform_name,
{{store_name('store')}},
cast(ChannelProductId as string) as product_id, 
ItemSKUCode as sku,
currency,
exchange_currency_code,
exchange_currency_rate,
Cast(order_date as date) as date,
cast(null as string) as subscription_id,
'Order' as transaction_type,
false as is_cancelled,
'' as reason,
NotificationEmail as email,
sum(cast(null as numeric)) quantity,
sum(CAST(totalprice as numeric)) total_price,
sum(CAST(REGEXP_REPLACE(subtotal,'','0') as numeric)) subtotal_price,
sum(CAST(REGEXP_REPLACE(cgst,'','0') AS numeric) + cast(REGEXP_REPLACE(sgst,'','0') as numeric) + cast(REGEXP_REPLACE(igst,'','0') as numeric)) total_tax, 
sum(cast(ShippingCharges as numeric) + cast(ShippingMethodCharges as numeric) + cast(CODServiceCharges as numeric)) as shipping_price, 
sum(cast(GiftWrapCharges as numeric)) as  giftwrap_price,
sum(cast(Discount as numeric)) item_discount,
null as shipping_discount
from {{ref('UnicommerceSaleOrders')}}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

UNION ALL 

select
concat(DisplayOrderCode,SaleOrderItemCode) as order_id,
brand,
ChannelName as platform_name,
{{store_name('store')}},
cast(ChannelProductId as string) as product_id, 
ItemSKUCode as sku,
currency,
exchange_currency_code,
exchange_currency_rate,
cast(ReturnDate as date) date,
cast(null as string) as subscription_id,
'Return' as transaction_type,
false as is_cancelled,
ReturnReason as reason,
NotificationEmail as email,
null as units_returned,
sum(return_amount) as total_price,
null as subtotal_price,
null as total_tax,
null as shipping_price, 
null as giftwrap_price, 
null as item_discount,
null as shipping_discount
from (
    SELECT * {{exclude()}}(ReturnDate),
    REGEXP_REPLACE(ReturnDate,'',null) ReturnDate,
    case when ReversePickupCode is not null then cast(REGEXP_REPLACE(subtotal,'','0') as numeric) else 0 end as return_amount
    from {{ ref('UnicommerceSaleOrders') }})
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15