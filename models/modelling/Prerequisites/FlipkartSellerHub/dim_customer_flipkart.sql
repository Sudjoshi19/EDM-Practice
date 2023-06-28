select 
date(cast(Order_Date as timestamp)) as order_date,
order_id,
_daton_batch_runtime,
'' as email,
'Flipkart' as acquisition_channel
from {{ ref('FlipkartSalesReport') }} 