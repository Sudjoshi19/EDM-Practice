select * {{exclude()}} (row_num) from 
    (
    select
    'TargetPlus' as platform_name,
    Order_ID as order_id,
    cast(null as string) as payment_mode,
    'Online Marketplace' as order_channel,
    cast(null as date) as last_updated_date,
    row_number() over(partition by Order_ID order by _daton_batch_runtime desc) row_num
    from {{ref('TargetPlus_Orders')}} 
    ) orders
where row_num = 1

