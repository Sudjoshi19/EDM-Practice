select 
date(order_date) as order_date,
concat(DisplayOrderCode,SaleOrderItemCode) as order_id,
_daton_batch_runtime,
coalesce(NotificationEmail,'') as email,
'Unicommerce' as acquisition_channel,
cast(null as boolean) as accepts_marketing
from {{ ref('UnicommerceSaleOrders') }}