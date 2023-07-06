select 
brand,
'India' as store_name,
Campaign_ID as campaign_id,
cast(null as string) as campaign_name,
cast(null as string) as flow_id,
cast(null as string) as flow_name,
cast(null as string) as adgroup_id, 
AdGroup_Name as adgroup_name,
Listing_ID as ad_id, 
cast(null as string) as product_id,
cast(null as string) as sku,
date(a.Date) as date,
{% if var('currency_conversion_flag') %}
case when b.value is null then 1 else b.value end as exchange_currency_rate,
case when b.from_currency_code is null then a.currency else b.from_currency_code end as exchange_currency_code,
{% else %}
cast(1 as decimal) as exchange_currency_rate,
cast(a.currency as string) as exchange_currency_code, 
{% endif %}
'Flipkart' as platform_name,
'Flipkart' as ad_channel,
cast(null as string) as campaign_type,
cast(null as string) as flow_type,
sum(cast(Clicks as numeric)) clicks,
sum(views) as impressions,
sum(cast(Total_converted_units as numeric)) conversions,
sum(cast(Direct_Units_Sold as numeric) + cast(Indirect_Units_Sold as numeric)) quantity,
sum(cast(Ad_Spend as numeric)) as spend,
sum(cast(Total_Revenue__Rs_ as numeric)) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from (
select *,
'INR' as currency    
from {{ ref('FlipkartAdsCampaign') }} ) a
{% if var('currency_conversion_flag') %}
left join {{ ref('ExchangeRates') }} b on date(a.Date) = b.date and a.currency = b.to_currency_code
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18