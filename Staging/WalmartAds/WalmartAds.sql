
{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

with unnested_table as(
{% set table_name_query %}
{{set_table_name('%walmartads%')}}    
{% endset %}  


{% set results = run_query(table_name_query) %}
{% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
    {% set results_list = [] %}
    {% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
        {% if var('get_brandname_from_tablename_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
        {% else %}
            {% set brand = var('default_brandname') %}
        {% endif %}

        {% if var('get_storename_from_tablename_flag') %}
            {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
        {% else %}
            {% set store = var('default_storename') %}
        {% endif %}

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

    SELECT * 
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        CTR,
        Orders,
        Total_Attributed_Brand_Click_Sales,
        Units_Sold,
        Attributed_Brand_Click_Sales__Shipping_,
        Total_Attributed_Direct_Click_Sales,
        Attributed_Direct_View_Sales,
        Report_Name_Arg,
        Total_Attributed_Sales__Shipping_,
        Ad_Group_Id,
        Total_Attributed_Related_Click_Sales,
        Item_Id,
        Attributed_Brand_View_Sales,
        Attributed_Brand_Click_Sales__Curbside_pickup_and_delivery_,
        Attributed_Related_Click_Sales__Curbside_pickup_and_delivery_,
        Attribution_Window_Arg,
        Bid,
        Average_CPC,
        Ad_Spend,
        Total_Attributed_Sales__Curbside_pickup_and_delivery_,
        Conversion_Rate__Orders_Based_,
        Impressions,
        Report_Start_Date_Arg,
        RoAS,
        Attributed_Direct_Click_Sales__Curbside_pickup_and_delivery_,
        Attributed_Direct_Click_Sales__Shipping_,
        Ad_Group_Name,
        Group_By_Arg,
        Item_Name,
        Date,
        Campaign_Id,
        Total_Attributed_Sales,
        Attributed_Related_Click_Sales__Shipping_,
        Clicks,
        Report_End_Date_Arg,
        Conversion_Rate__Units_Sold_Based_,
        Campaign_Name,
        FileName,
        CTR_in,
        Average_CPC_in,
        Ad_Spend_in,
        Total_Attributed_Brand_Click_Sales_nu,
        Attributed_Brand_Click_Sales__Shipping__nu,
        Total_Attributed_Sales__Shipping__nu,
        Conversion_Rate__Orders_Based__nu,
        RoAS_nu,
        Total_Attributed_Sales_nu,
        Conversion_Rate__Units_Sold_Based__nu,
        Total_Attributed_Direct_Click_Sales_nu,
        Total_Attributed_Related_Click_Sales_nu,
        Attributed_Direct_Click_Sales__Shipping__nu,
        Attributed_Related_Click_Sales__Shipping__nu,
        Item_Id_dt,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        FROM  {{i}} 
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

dedup as (
select *,
DENSE_RANK() OVER (PARTITION BY Ad_Group_Id,Item_ID,Report_Start_Date_Arg order by _daton_batch_runtime desc) row_num
from unnested_table 
)

SELECT * {{exclude()}}(row_num)
from dedup 
where row_num = 1

