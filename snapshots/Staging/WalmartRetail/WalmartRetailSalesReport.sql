
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

{% set table_name_query %}
{{set_table_name('%walmartretail%sales%')}}    
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

    SELECT * {{exclude()}}(row_num)
    From ( select 
        '{{brand}}' as brand,
        STORE_NBR,
        STORE_NAME,
        STORE_TYPE_DESCR,
        CURR_WHSE_SS_ORDER_QTY,
        WEEKLY_COMP_STORE_SALES,
        CURR_STR_ON_HAND_QTY,
        CURR_WHSE_ON_HAND_QTY,
        POS_QTY,
        HIST_ON_HAND_QTY,
        CURR_STR_IN_WHSE_QTY,
        UPC,
        WHSE_NBR,
        STORE_WEEKS_SUPPLY,
        CURR_STR_ON_ORDER_QTY,
        CURR_INSTOCK__,
        POS_COST,
        TRAIT_FLAG,
        WEEKLY_COMP_STORE_QTY,
        REGION,
        DISTRICT,
        POS_SALES,
        FILENAME,
        WHSE_NAME,
        PHONE_NBR,
        cast(Daily as date) DAILY_REPORT_DATE,
        ZIP_CODE,
        WHSE_STREET_ADDRESS,
        STREET_ADDRESS,
        WHSE_ZIP_CODE,
        STORE_PROTOTYPE_NBR,
        DISTRIBUTION_WAREHOUSE,
        BUILDING_ADDRESS,
        CITY,
        WHSE_BUILDING_ADDRESS,
        WHSE_CITY,
        SUBDIV,
        WHSE_STATE,
        STATE,
        STORE_PROTOTYPE_NBR_ST,
        STORE_PROTOTYPE_NBR_NU,
        POS_QTY_ST,
        WEEKLY_COMP_STORE_QTY_ST,
        STORE_WEEKS_SUPPLY_ST,
        CURR_STR_ON_HAND_QTY_ST,
        HIST_ON_HAND_QTY_ST,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY STORE_NAME,STORE_NBR,UPC,DATE(Daily) order by {{daton_batch_runtime()}} desc,LEFT(filename, 29) DESC) row_num
        from {{i}} a
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}    
        )
        where row_num=1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
