

select 
date,
brand_name,
platform_name,
store_name,
product_id,
sku,
product_name,
sum(inbound_quantity) inbound_quantity,
sum(available_quantity) available_quantity,
sum(inv_age_0_to_90_days) inv_age_0_to_90_days,
sum(inv_age_91_to_180_days) inv_age_91_to_180_days,
sum(inv_age_181_to_270_days) inv_age_181_to_270_days,
sum(inv_age_271_to_365_days) inv_age_271_to_365_days,
sum(units_shipped_t7) units_shipped_t7,
sum(units_shipped_t30) units_shipped_t30,
sum(units_shipped_t60) units_shipped_t60,
sum(units_shipped_t90) units_shipped_t90
from (
select *,
case when a.type = 'inbound_quantity' then value else 0 end as inbound_quantity,
case when a.type = 'available' then value else 0 end as available_quantity,
case when a.type = 'inv_age_0_to_90_days' then value else 0 end as inv_age_0_to_90_days,
case when a.type = 'inv_age_91_to_180_days' then value else 0 end as inv_age_91_to_180_days,
case when a.type = 'inv_age_181_to_270_days' then value else 0 end as inv_age_181_to_270_days,
case when a.type = 'inv_age_271_to_365_days' then value else 0 end as inv_age_271_to_365_days,
case when a.type = 'units_shipped_t7' then value else 0 end as units_shipped_t7,
case when a.type = 'units_shipped_t30' then value else 0 end as units_shipped_t30,
case when a.type = 'units_shipped_t60' then value else 0 end as units_shipped_t60,
case when a.type = 'units_shipped_t90' then value else 0 end as units_shipped_t90
from {{ ref('fact_inventory')}} a 
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform')}} c
on a.platform_key = c.platform_key 
left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} where status = 'Active') d
on a.product_key = d.product_key
where product_id is not null
) group by 1,2,3,4,5,6,7