with orders as (
    select 
    brand_name,
    c.platform_name,
    extract(month from date) month,
    extract(year from date) year,
    sum(subtotal_price) subtotal_price
    from {{ ref('fact_orders') }} a
    left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
    on a.brand_key = b.brand_key
    left join {{ ref('dim_platform')}} c
    on a.platform_key = c.platform_key
    where transaction_type = 'Order'
    group by 1,2,3,4
),

advertising as (
    select 
    brand_name,
    c.platform_name,
    extract(month from date) month,
    extract(year from date) year,
    sum(sales) adsales 
    from {{ ref('fact_advertising') }} a
    left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
    on a.brand_key = b.brand_key
    left join {{ ref('dim_platform')}} c
    on a.platform_key = c.platform_key
    group by 1,2,3,4
),

commissions as (
    select 
    daton_brand_name as brand_name,
    platform_name,
    start_date,
    case when end_date is null then date(current_timestamp()) else cast(end_date as date) end as end_date,
    commission_type,	
    currency_code,	
    flat_rate,	
    revenue_min,	
    revenue_max,	
    commission_rate
    from {{ ref('dim_commissions')}}
)

select
a.brand_name,
a.platform_name,
month,
year,
commission_type,	
currency_code,	
flat_rate,	
revenue_min,	
revenue_max,	
commission_rate,
subtotal_price
from orders a
left join {{ ref('dim_commissions')}} b
on a.brand_name = b.daton_brand_name 
and a.month between extract(month from cast(b.start_date as date)) and extract(month from b.end_date)
and a.year between extract(year from cast(b.start_date as date)) and extract(year from b.end_date)