with month as (
    select 
    brand_name, 
    platform_name, 
    store_name, 
    product_id, 
    product_name, 
    sku, 
    sum(this_month_item_total_price) this_month_item_total_price, 
    sum(last_year_item_total_price) as last_year_this_month_item_total_price, 
    sum(this_month_item_total_price - last_year_item_total_price) as month_item_total_price_difference
    FROM `edm-saras.edm_presentation.date_browser_monthly` 
    WHERE month_start_date in (select max(month_start_date) from `edm-saras.edm_presentation.date_browser_monthly` )
    group by 1,2,3,4,5,6
),

year as (
    select
    brand_name, 
    platform_name, 
    store_name, 
    product_id, 
    product_name, 
    sku, 
    sum(this_year_item_total_price) this_year_item_total_price, 
    sum(last_year_item_total_price) as last_year_item_total_price, 
    sum(this_year_item_total_price - last_year_item_total_price) as year_item_total_price_difference
    FROM `edm-saras.edm_presentation.date_browser_yearly` 
    WHERE year_start_date in (select max(year_start_date) from `edm-saras.edm_presentation.date_browser_yearly` )
    group by 1,2,3,4,5,6
    ),

week as (
    select
    brand_name, 
    platform_name, 
    store_name, 
    product_id, 
    product_name, 
    sku, 
    sum(this_week_item_total_price) this_week_item_total_price, 
    sum(last_year_item_total_price) as last_year_this_week_item_total_price, 
    sum(this_week_item_total_price - last_year_item_total_price) as week_item_total_price_difference
    FROM `edm-saras.edm_presentation.date_browser_weekly` 
    WHERE week_start_date in (select max(week_start_date) from `edm-saras.edm_presentation.date_browser_weekly` )
    group by 1,2,3,4,5,6
),

day as (
    select
    brand_name, 
    platform_name, 
    store_name, 
    product_id, 
    product_name, 
    sku, 
    sum(this_day_item_total_price) this_day_item_total_price, 
    sum(last_year_item_total_price) as last_year_this_day_item_total_price, 
    sum(this_day_item_total_price - last_year_item_total_price) as day_item_total_price_difference
    FROM `edm-saras.edm_presentation.date_browser_daily` 
    WHERE date in (select max(date) from `edm-saras.edm_presentation.date_browser_daily` )
    group by 1,2,3,4,5,6
)

select 
coalesce(month.brand_name,year.brand_name, week.brand_name, day.brand_name) brand_name,
coalesce(month.platform_name,year.platform_name, week.platform_name, day.platform_name) platform_name,
coalesce(month.store_name,year.store_name, week.store_name, day.store_name) store_name,
coalesce(month.product_id,year.product_id, week.product_id, day.product_id) product_id,
coalesce(month.product_name,year.product_name, week.product_name, day.product_name) product_name,
coalesce(month.sku,year.sku, week.sku, day.sku) sku,
this_year_item_total_price,
last_year_item_total_price,
year_item_total_price_difference,
this_month_item_total_price,
last_year_this_month_item_total_price,
month_item_total_price_difference,
this_week_item_total_price,
last_year_this_week_item_total_price,
week_item_total_price_difference,
this_day_item_total_price,
last_year_this_day_item_total_price,
day_item_total_price_difference
from year
full outer join month 
on year.platform_name = month.platform_name and year.store_name = month.store_name and year.brand_name = month.brand_name 
and year.product_id = month.product_id and year.product_name = month.product_name and year.sku = month.sku
full outer join week 
on year.platform_name = week.platform_name and year.store_name = week.store_name and year.brand_name = week.brand_name 
and year.product_id = week.product_id and year.product_name = week.product_name and year.sku = week.sku
full outer join day 
on year.platform_name = day.platform_name and year.store_name = day.store_name and year.brand_name = day.brand_name 
and year.product_id = day.product_id and year.product_name = day.product_name and year.sku = day.sku
