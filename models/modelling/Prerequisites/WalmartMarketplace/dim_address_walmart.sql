select 
concat('', '') as full_name,
isSellerOwnedShipment as address_type,
ship_address1 as addr_line_1,
ship_address1 as addr_line_2,
ship_city as city,
ship_state as state,
ship_country as country, 
ship_postal_code as postal_code,
phone,
_daton_batch_runtime,
coalesce(customerEmailId,'') as email
from {{ ref('WalmartOrders') }}  