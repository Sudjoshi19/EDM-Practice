with daily_inventory as (
    select
    brand,
    store,
    'Shopify' as order_platform,
    warehouse_id,
    date(created_at) as date,
    product_id,
    sku,
    'daily_inventory' as event,
    'inventory' as type,
    sum(available) as value
    from {{ ref('ShipHeroWarehouseProducts')}}
    group by 1,2,3,4,5,6,7,8,9
),

doh as (select
    brand,
    store,
    'Shopify' as order_platform,
    warehouse_id,
    date(created_at) as date,
    product_id,
    sku,
    'days_on_hand' as event,
    'inventory' as type,
    sum(on_hand) as value
    from {{ ref('ShipHeroWarehouseProducts')}}
    group by 1,2,3,4,5,6,7,8,9
),

avg_days_of_shipment as (select
    a.brand,
    a.store,
    'Shopify' as order_platform,
    b.warehouse_id,
    date(a.created_at) as date,
    b.product_id,
    b.sku,
    'avg_days_of_shipment' as event,
    'fulfillment' as type,
    avg(date_diff(date(a.date_closed), date(a.created_at), day)) as value
    from {{ ref('ShipHeroPurchaseOrders')}} a
    left join {{ ref('ShipHeroWarehouseProducts')}} b on a.id=b.id
    group by 1,2,3,4,5,6,7,8,9
)

select 
brand,
order_platform,
{{store_name('store')}},
cast (warehouse_id as string) fulfillment_center_id,
'Shiphero' as fulfillment_channel,
product_id, 
sku, 
date,
event,
type,
value
from 
((select * from daily_inventory) union all
(select * from doh) union all
(select * from avg_days_of_shipment)) 