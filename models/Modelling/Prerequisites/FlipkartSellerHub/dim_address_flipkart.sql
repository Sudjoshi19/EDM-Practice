select 
'' as full_name,
cast(null as string) as address_type,
cast(null as string) as addr_line_1,
cast(null as string) as addr_line_2,
cast(null as string) as city,
coalesce(customer_s_delivery_state,'') as state,
'India' as country, 
cast(customer_s_delivery_pincode as string) as postal_code,
cast(null as string) as phone,
_daton_batch_runtime,
'' as email
from {{ ref('FlipkartSalesReport') }}   

