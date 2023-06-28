select
'Myntra' as platform_name,
Seller_Order_Id as order_id,
Myntra_SKU_Code as sku,
cast(null as string) as product_id,
cast(Packet_Id as string) as shipment_id,
cast(Packet_Id as string) as shipment_item_id,
cast(null as string) as fulfillment_channel,
cast(Warehouse_Id as string) fulfillment_center_id,
CAST(SUBSTRING(Shipped_On, 1, 10) AS DATE) shipment_date,
Courier_Code as carrier,
cast(null as date) estimated_delivery_date,
CAST(SUBSTRING(Delivered_On, 1, 10) AS DATE) delivered_at,
Order_Tracking_Number as tracking_number,
_daton_batch_runtime
from {{ ref('MyntraSeller') }} 


