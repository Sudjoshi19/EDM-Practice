select 
brand,
order_platform,
store_name,
cast (null as string) as fulfillment_center_id,
'' as fulfillment_channel,
product_id, 
sku, 
date(ReportDateTime) as date,
'Customer Shipment' as event,
'' as type,
sum(value) value
from (
select *,
{{store_name('store')}},
cast(AvailToSell_Quantity as int) value,
'' as product_id, 
'Walmart' as order_platform
from {{ ref('WalmartInventoryReportOnRequest')}}) 
group by 1,2,3,4,5,6,7,8