
{% if var('product_details_gs_flag') %}
-- depends_on: {{ ref('ProductDetails') }}
{% endif %}

select distinct prod.*,
{% if var('product_details_gs_flag') %}
description, 
category, 
sub_category, 
cast(mrp as numeric) mrp, 
cast(cogs as numeric) cogs, 
currency_code,
cast(start_date as date) date, 
cast(end_date as  date) date
{% else %}
cast(null as string) as description, 
cast(null as string) as category, 
cast(null as string) as sub_category, 
cast(null as numeric) as mrp, 
cast(null as numeric) as cogs, 
cast(null as string) as currency_code,
cast(null as date) as start_date, 
cast(null as date) as end_date 
{% endif %} 

from (
select 
'Amazon Vendor Central' as platform_name,
coalesce(asin,'') as product_id,
'' as sku,
cast(null as string) as product_name,
cast(null as string) as color, 
cast(null as string) as seller,
cast(null as string) as size,
cast(null as string) product_category,
cast(null as string) as ASINstatus,
cast(null as numeric) as buybox_landed_price,
cast(null as numeric) as buybox_listing_price,
cast(null as string) as buybox_seller_id,
_daton_batch_runtime 
from {{ ref('VendorTrafficReport') }}

union all

select 
'Amazon Vendor Central' as platform_name,
coalesce(buyerProductIdentifier,'') as product_id,
'' as sku,
cast(null as string) as product_name,
cast(null as string) as color, 
cast(null as string) as seller,
cast(null as string) as size,
cast(null as string) product_category,
cast(null as string) as ASINstatus,
cast(null as numeric) as buybox_landed_price,
cast(null as numeric) as buybox_listing_price,
cast(null as string) as buybox_seller_id,
_daton_batch_runtime 
from {{ ref('RetailProcurementOrdersStatus') }}) prod

{% if var('product_details_gs_flag') %}
left join (
  select sku, description,	category, sub_category, mrp, cogs, currency_code, start_date, end_date 
  from {{ ref('ProductDetails') }} 
  where lower(platform_name) = 'amazon vendor central') prod_gs
on prod.sku = prod_gs.sku
{% endif %}


