select * {{exclude()}} (row_num) from 
    (
    select
    'Flipkart' as platform_name,
    order_id,
    cast(null as string) as payment_mode,
    'Flipkart Direct' as order_channel,
    cast(null as date) as last_updated_date,
    row_number() over(partition by order_id order by _daton_batch_runtime desc) row_num
    from {{ref('FlipkartSalesReport')}} 
    ) orders
where row_num = 1