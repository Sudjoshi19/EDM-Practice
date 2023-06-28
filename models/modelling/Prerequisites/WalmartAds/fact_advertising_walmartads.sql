select 
brand,
{{store_name('store')}},
Campaign_Id as campaign_id,
Campaign_Name as campaign_name,
cast(null as string) as flow_id,
cast(null as string) as flow_name,
Ad_Group_Id as adgroup_id, 
Ad_Group_Name as adgroup_name,
cast(null as string) as ad_id, 
Item_Id as product_id,
cast(null as string) as sku,
date(a.Date) as date,
{% if var('currency_conversion_flag') %}
case when b.value is null then 1 else b.value end as exchange_currency_rate,
case when b.from_currency_code is null then a.currency else b.from_currency_code end as exchange_currency_code,
{% else %}
cast(1 as decimal) as exchange_currency_rate,
cast(a.currency as string) as exchange_currency_code, 
{% endif %}
'Walmart' as platform_name,
'Walmart' as ad_channel,
'Walmart' as campaign_type,
cast(null as string) as flow_type,
sum(cast(Clicks as numeric)) clicks,
sum(cast(Impressions as numeric)) impressions,
sum(cast(Conversion_Rate__Orders_Based_ as numeric)) conversions,
sum(cast(Conversion_Rate__Units_Sold_Based_ as numeric)) quantity,
sum(cast(Ad_Spend as numeric)) as spend,
sum(cast(Total_Attributed_Sales as numeric)) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from (
select *,
{{currency_code('store')}} 
from {{ ref('WalmartAds') }} ) a
{% if var('currency_conversion_flag') %}
left join {{ ref('ExchangeRates') }} b on date(a.Date) = b.date and a.currency = b.to_currency_code
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18