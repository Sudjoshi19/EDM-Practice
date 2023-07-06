select * {{exclude()}} (row_num) from 
    (
    select
    'Myntra' as platform_name,
    cast(Order_Id_FK as string) as order_id,
    cast(null as string) as payment_mode,
    'Online Marketplace' as order_channel,
    cast(null as date) as last_updated_date,
    row_number() over(partition by Order_Id_FK order by _daton_batch_runtime desc) row_num
    from {{ref('MyntraSeller')}} 
    ) orders
where row_num = 1
