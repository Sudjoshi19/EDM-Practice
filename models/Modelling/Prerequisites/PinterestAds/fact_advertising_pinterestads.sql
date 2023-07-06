select 
brand,
{{store_name('store')}},
CAMPAIGN_ID as campaign_id,
CAMPAIGN_NAME as campaign_name,
cast(null as string) as flow_id,
cast(null as string) as flow_name,
AD_GROUP_ID as adgroup_id, 
cast(null as string) as adgroup_name, 
AD_ID as ad_id,
PIN_ID as product_id,
cast(null as string) as sku,
date(a.DATE) as date,
{% if var('currency_conversion_flag') %}
case when b.value is null then 1 else b.value end as exchange_currency_rate,
case when b.from_currency_code is null then a.currency else b.from_currency_code end as exchange_currency_code,
{% else %}
cast(1 as decimal) as exchange_currency_rate,
cast(a.currency as string) as exchange_currency_code, 
{% endif %}
'Shopify' as platform_name,
'Pinterest' as ad_channel,
'Pinterest' as campaign_type,
cast(null as string) as flow_type,
sum(cast(TOTAL_CLICKTHROUGH_in as int)) clicks,
sum(IMPRESSION_1_in + IMPRESSION_2_in) impressions,
sum(cast(TOTAL_CONVERSIONS_in as int)) conversions,
sum(cast(null as int)) as quantity,
sum(round((cast(SPEND_IN_MICRO_DOLLAR_in as numeric)/1000000),2)) as spend,
sum(round((cast(TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR_in as numeric)/1000000),2)) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from (
select *,
{{currency_code('store')}} 
from {{ ref('PinterestAdsAnalytics') }} 
where CONVERSION_REPORT_TIME = 'TIME_OF_AD_ACTION') a
{% if var('currency_conversion_flag') %}
left join {{ ref('ExchangeRates') }} b on date(a.DATE) = b.date and a.currency = b.to_currency_code
{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
