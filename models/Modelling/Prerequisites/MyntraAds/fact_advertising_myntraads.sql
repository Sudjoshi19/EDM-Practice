select 
brand,
{{store_name('store')}},
cast(null as string) as campaign_id,
cast(null as string) as campaign_name,
cast(null as string) as flow_id,
cast(null as string) as flow_name,
cast(null as string) as adgroup_id, 
cast(null as string) as adgroup_name,
Adid as ad_id, 
Style_Id as product_id,
cast(null as string) as sku,
date(Style_Id_dt) as date,
{% if var('currency_conversion_flag') %}
case when b.value is null then 1 else b.value end as exchange_currency_rate,
case when b.from_currency_code is null then a.currency else b.from_currency_code end as exchange_currency_code,
{% else %}
cast(1 as decimal) as exchange_currency_rate,
cast(a.currency as string) as exchange_currency_code, 
{% endif %}
'Myntra' as platform_name,
'Myntra' as ad_channel,
'Myntra' as campaign_type,
cast(null as string) as flow_type,
sum(cast(Clicks as int)) clicks,
sum(cast(Impressions as int)) as impressions,
sum(cast(null as int)) conversions,
sum(cast(Quantity as int)) quantity,
sum(cast(null as numeric)) as spend,
sum(cast(Revenue as numeric)) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from (
select *,
{{currency_code('store')}} 
from {{ ref('MyntraAds') }} ) a
{% if var('currency_conversion_flag') %}
left join {{ ref('ExchangeRates') }} b on date(a.Style_Id_dt) = b.date and a.currency = b.to_currency_code
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18