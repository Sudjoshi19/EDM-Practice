select * {{exclude()}} (row_num) from 
    (
    select
    'Shopify' as  platform_name,
    warehouse_id as fulfillment_center_id,
    warehouse_identifier as name,
    'Active' as status,
    cast(null as string) msa,
    cast(null as string) address_1,
    cast(null as string) address_2,
    cast(null as string) city,
    cast(null as string) district,
    cast(null as string) state,
    cast(null as string) country,
    cast(null as string) zip,
    cast(null as string) geo_location,
    date(updated_at) as last_updated_date,
    row_number() over(partition by warehouse_id order by _daton_batch_runtime desc) row_num
    from {{ref('ShipHeroWarehouseProducts')}} 
    ) orders
where row_num = 1