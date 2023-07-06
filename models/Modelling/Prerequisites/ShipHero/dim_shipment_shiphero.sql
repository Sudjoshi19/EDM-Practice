select 
distinct
'Shopify' as platform_name,
a.order_id,
b.sku,
b.product_id,
cast(null as string) as shipment_id,
cast(null as string) as shipment_item_id,
'Shiphero' as fulfillment_channel,
b.warehouse_id as fulfillment_center_id,
a.created_date as shipment_date,
cast(null as string) as carrier,
cast(null as date) as estimated_delivery_date,
cast(null as date) as delivered_at,
cast(null as string) as tracking_number,
a._daton_batch_runtime 
from {{ ref('ShipHeroShipments') }} a
left join  {{ ref('ShipHeroWarehouseProducts')}} b 
on a.id = b.id