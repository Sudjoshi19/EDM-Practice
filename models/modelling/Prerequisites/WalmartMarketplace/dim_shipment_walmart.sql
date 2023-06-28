select
'Walmart' as platform_name,
purchaseOrderId as order_id,
sku as sku,
cast(null as string) as product_id,
shipmentNo as shipment_id,
shipmentLineNo as shipment_item_id,
fulfillmentOption as fulfillment_channel,
cast(null as string) fulfillment_center_id,
date(cast(pickUpDateTime as timestamp)) shipment_date,
carrier as carrier,
cast(null as date) estimated_delivery_date,
cast(null as date) delivered_at,
trackingNumber as tracking_number,
_daton_batch_runtime
from {{ ref('WalmartOrders') }} 

