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
{{set_table_name('%shiphero_orders%')}}    
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
        order_number,
        partner_order_id,
        shop_name,
        fulfillment_status,
        order_date,
        total_tax,
        subtotal,
        total_discounts,
        total_price,
        box_name,
        auto_print_return_label,
        account_id,
        updated_at,
        email,
        profile,
        required_ship_date,
        tags,
        flagged,
        priority_flag,
        allocation_priority,
        source,
        alcohol,
        expected_weight_in_oz,
        insurance,
        insurance_amount,
        currency,
        has_dry_ice,
        allow_split,
        {% if target.type=='snowflake' %}
        shipping_lines.VALUE:title::VARCHAR as shipping_lines_title,
        shipping_lines.VALUE:carrier::VARCHAR as shipping_lines_carrier,
        shipping_lines.VALUE:method::VARCHAR as shipping_lines_method,
        authorizations.VALUE:transaction_id::VARCHAR as authorizations_transaction_id,
        authorizations.VALUE:authorized_amount::VARCHAR as authorizations_authorized_amount,
        authorizations.VALUE:postauthed_amount::VARCHAR as authorizations_postauthed_amount,
        authorizations.VALUE:refunded_amount::VARCHAR as authorizations_refunded_amount,
        authorizations.VALUE:card_type::VARCHAR as authorizations_card_type,
        authorizations.VALUE:date::VARCHAR as authorizations_date,
        shipments.VALUE:id::VARCHAR as shipments_id,
        shipments.VALUE:legacy_id::VARCHAR as shipments_legacy_id,
        {% else %}
        shipping_lines.title as shipping_lines_title,
        shipping_lines.carrier as shipping_lines_carrier,
        shipping_lines.method as shipping_lines_method,
        authorizations.transaction_id as authorizations_transaction_id,
        authorizations.authorized_amount as authorizations_authorized_amount,
        authorizations.postauthed_amount as authorizations_postauthed_amount,
        authorizations.refunded_amount as authorizations_refunded_amount,
        authorizations.card_type as authorizations_card_type,
        authorizations.date as authorizations_date,
        shipments.id as shipments_id,
        shipments.legacy_id as shipments_legacy_id,
        {% endif %}
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY a.id, a.legacy_id, order_number, partner_order_id order by {{daton_batch_runtime()}} desc) row_num
        from {{i}} a
        {{unnesting("shipping_lines")}}
        {{unnesting("authorizations")}}
        {{unnesting("shipments")}}
                    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}        
    )
    where row_num = 1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
