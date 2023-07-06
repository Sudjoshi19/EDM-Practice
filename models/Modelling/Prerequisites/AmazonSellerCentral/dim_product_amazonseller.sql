
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
select 
'Amazon Seller Central' as platform_name,
coalesce(product_id.asin,'') product_id,
coalesce(product_id.sku,'') sku,
coalesce(itemName,'') product_name, 
coalesce(colorName,'') color,
coalesce(manufacturer,'') seller,
coalesce(sizeName,'') size,
product_details.summaries_websiteDisplayGroupName as product_category,
alllistings.status as ASINstatus,
listing_offers.LandedPrice_Amount as buybox_landed_price,
listing_offers.ListingPrice_Amount as buybox_listing_price,
listing_offers.BuyBoxPrices_sellerId as buybox_seller_id,
product_details._daton_batch_runtime
from 
(
  select distinct asin,sku from 
    (
    select 
    asin, 
    sku 
    from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}

    union all 

    select 
    ReferenceASIN, 
    cast(null as string) sku 
    from {{ ref('CatalogItems') }}

    union all

    select 
    childAsin, 
    cast(null as string) sku 
    from {{ ref('SalesAndTrafficReportByChildASIN') }}
    )
) product_id
left join {{ ref('CatalogItems') }} product_details
on product_id.asin = product_details.ReferenceASIN

left join (select distinct asin1,status from {{ ref('AllListingsReport') }}) alllistings
on product_id.asin = alllistings.asin1

left join (select distinct ASIN,LandedPrice_Amount,ListingPrice_Amount,BuyBoxPrices_sellerId from{{ ref('ListingOffersForASIN') }}) listing_offers
on product_id.asin = listing_offers.ASIN ) prod

{% if var('product_details_gs_flag') %}
left join (
  select 
  sku, 
  description,	
  category, 
  sub_category, 
  mrp, 
  cogs,
  currency_code, 
  start_date, 
  end_date 
  from {{ ref('ProductDetails') }} 
  where lower(platform_name) = 'amazon') prod_gs
on prod.sku = prod_gs.sku
{% endif %}

