{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX({{to_epoch_milliseconds('last_updated')}}) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name_modelling('fact_inventory%')}}
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% for i in results_list %}
    select 
    {{ dbt_utils.surrogate_key(['brand']) }} AS brand_key,
    {{ dbt_utils.surrogate_key(['order_platform','store_name']) }} AS platform_key,
    {{ dbt_utils.surrogate_key(['product_id','sku','order_platform']) }} AS product_key,
    {{ dbt_utils.surrogate_key(['fulfillment_center_id','order_platform']) }} AS fulfillment_center_key,
    fulfillment_channel,
    date,
    event,
    type,
    value,
    current_timestamp() as last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE {{to_epoch_milliseconds('current_timestamp()')}}  >= {{max_loaded}}
        {% endif %}
{% if not loop.last %} union all {% endif %}
{% endfor %}

