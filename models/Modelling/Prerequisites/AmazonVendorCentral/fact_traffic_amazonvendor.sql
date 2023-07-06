select
brand,
marketplaceName as store_name,
'Amazon Vendor Central' as platform_name,
asin as product_id,
'' as sku,
'' as event_name,
'' as source,
'' as medium,
'' as campaign,
'' as keyword,
'' as content,
'' as landing_page_path,
startDate as date,
1 as exchange_currency_rate,
cast(null as string) as exchange_currency_code,
sum(cast(null as numeric)) mobile_sessions,
sum(cast(null as numeric)) browser_sessions,
sum(cast(null as numeric)) tablet_sessions,
sum(cast(null as numeric)) as sessions,
sum(cast(null as numeric)) mobile_pageviews,
sum(cast(null as numeric)) browser_pageviews,
sum(cast(null as numeric)) tablet_pageviews,
sum(cast(null as numeric)) as pageviews,
sum(cast(glanceViews as numeric)) as glance_views,
avg(cast(null as numeric)) as buybox_percentage,
sum(cast(null as numeric)) as quantity,
sum(cast(null as numeric)) as product_sales
from {{ ref('VendorTrafficReport') }}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

