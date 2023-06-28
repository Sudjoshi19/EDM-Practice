
{% if var('product_details_gs_flag') %}
-- depends_on: {{ ref('ProductDetails') }}
{% endif %}

select prod.*,
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
select distinct
'TargetPlus' as platform_name,
coalesce(cast(TCIN as string),'') product_id,
coalesce(sku,'') sku,
cast(null as string) product_name, 
cast(null as string) as color, 
cast(null as string) seller,
cast(null as string) size,
cast(null as string) product_category,
cast(null as string) as ASINstatus,
cast(null as numeric) as buybox_landed_price,
cast(null as numeric) as buybox_listing_price,
cast(null as string) as buybox_seller_id,
_daton_batch_runtime
from {{ ref('TargetPlus_Orders') }}) prod
{% if var('product_details_gs_flag') %}
left join (
  select sku, description,	category, sub_category, mrp, cogs, currency_code, start_date, end_date 
  from {{ ref('ProductDetails') }} 
  where lower(platform_name) = 'targetplus') prod_gs
on prod.sku = prod_gs.sku
{% endif %}



