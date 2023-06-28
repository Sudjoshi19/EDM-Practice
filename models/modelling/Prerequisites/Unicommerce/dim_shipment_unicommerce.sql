select
ChannelName as platform_name,
concat(DisplayOrderCode,SaleOrderItemCode) as order_id,
ItemSKUCode as sku,
ChannelProductId as product_id,
ShippingPackageCode as shipment_id,
ShippingPackageCode as shipment_item_id,
ShippingArrangedBy as fulfillment_channel,
cast(null as string) fulfillment_center_id,
date(cast(DispatchDate as timestamp)) shipment_date,
Shippingprovider as carrier,
cast(null as date) estimated_delivery_date,
date(cast(DeliveryTime as timestamp)) delivered_at,
TrackingNumber as tracking_number,
_daton_batch_runtime
from {{ ref('UnicommerceSaleOrders') }} 


