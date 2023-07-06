select * {{exclude()}} (row_num) from 
    (
    select
    ChannelName as platform_name,
    SaleOrderCode as order_id,
    PaymentInstrument as payment_mode,
    '' as order_channel,
    date(Updated) as last_updated_date,
    row_number() over(partition by SaleOrderCode order by _daton_batch_runtime desc) row_num
    from {{ref('UnicommerceSaleOrders')}} 
    ) orders
where row_num = 1

