select * {{exclude()}} (row_num) from 
    (
    select
    'Walmart' as platform_name,
    purchaseOrderId as order_id,
    paymentMethod as payment_mode,
    'Online Marketplace' as order_channel,
    date(statusDate) as last_updated_date,
    row_number() over(partition by purchaseOrderId order by _daton_batch_runtime desc) row_num
    from {{ref('WalmartOrders')}} 
    ) orders
where row_num = 1

