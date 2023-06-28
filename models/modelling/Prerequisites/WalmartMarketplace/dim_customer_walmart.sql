select 
        date(orderDate) as order_date,
        purchaseOrderId as order_id,
        _daton_batch_runtime,
        coalesce(customerEmailId,'') as email,
        'Walmart' as acquisition_channel,
        cast(null as boolean) as accepts_marketing
        from {{ ref('WalmartOrders') }} 