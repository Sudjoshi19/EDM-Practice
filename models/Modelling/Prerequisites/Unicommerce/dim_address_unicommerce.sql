select 
concat('', '') as full_name,
ShippingAddressName as address_type,
ShippingAddressLine1 as addr_line_1,
ShippingAddressLine2 as addr_line_2,
ShippingAddressCity as city,
ShippingAddressState as state,
ShippingAddressCountry as country, 
ShippingAddressPincode as postal_code,
ShippingAddressPhone as phone,
_daton_batch_runtime,
coalesce(NotificationEmail,'') as email
from {{ ref('UnicommerceSaleOrders') }} 
        