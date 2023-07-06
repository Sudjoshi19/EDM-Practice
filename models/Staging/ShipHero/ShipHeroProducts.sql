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
{{set_table_name('%shiphero_products%')}}    
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

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    SELECT * {{exclude()}} (row_num)
    From (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        a.id,
        a.legacy_id,
        account_id,
        name,
        sku,
        barcode,
        country_of_manufacture,
        tariff_code,
        kit,
        kit_build,
        no_air,
        final_sale,
        customs_value,
        not_owned,
        dropship,
        needs_serial_number,
        thumbnail,
        large_thumbnail,
        created_at,
        updated_at,
        product_note,
        virtual,
        ignore_on_invoice,
        ignore_on_customs,
        active,
        needs_lot_tracking,
        customs_description,
        {% if target.type=='snowflake' %}
        dimensions.VALUE:weight::VARCHAR as dimensions_weight,
        dimensions.VALUE:height::VARCHAR as dimensions_height,
        dimensions.VALUE:width::VARCHAR as dimensions_width,
        dimensions.VALUE:length::VARCHAR as dimensions_length,
        warehouse_products.VALUE:id::VARCHAR as warehouse_products_id,
        {% else %}
        dimensions.weight as dimensions_weight,
        dimensions.height as dimensions_height,
        dimensions.width as dimensions_width,
        dimensions.length as dimensions_length,
        warehouse_products.id as warehouse_products_id,
        {% endif %}
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY a.id, a.legacy_id, account_id, sku order by {{daton_batch_runtime()}} desc) row_num
        from {{i}} a 
        {{unnesting("dimensions")}}
        {{unnesting("warehouse_products")}}
                    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}        
    )
    where row_num = 1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
