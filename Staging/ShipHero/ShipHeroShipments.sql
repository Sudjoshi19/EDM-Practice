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
{{set_table_name('%shiphero%shipments%')}}    
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
        a.order_id,
        user_id,
        a.warehouse_id,
        a.profile,
        dropshipment,
        shipped_off_shiphero,
        completed,
        a.created_date,
        a.warehouse,
        total_packages,
        pending_shipment_id,
        {% if target.type=='snowflake' %}
        shipping_labels.VALUE:id::VARCHAR as shipping_labels_id,
        shipping_labels.VALUE:legacy_id::VARCHAR as shipping_labels_legacy_id,
        shipping_labels.VALUE:account_id::VARCHAR as shipping_labels_account_id,
        shipping_labels.VALUE:shipment_id::VARCHAR as shipping_labels_shipment_id,
        shipping_labels.VALUE:order_id::VARCHAR as shipping_labels_order_id,
        shipping_labels.VALUE:box_id::VARCHAR as shipping_labels_legacy_box_id,
        shipping_labels.VALUE:box_name::VARCHAR as shipping_labels_warehouse_box_name,
        shipping_labels.VALUE:status::VARCHAR as shipping_labels_status,
        shipping_labels.VALUE:tracking_number::VARCHAR as shipping_labels_tracking_number,
        shipping_labels.VALUE:order_number::VARCHAR as shipping_labels_order_number,
        shipping_labels.VALUE:order_account_id::VARCHAR as shipping_labels_order_account_id,
        shipping_labels.VALUE:carrier::VARCHAR as shipping_labels_carrier,
        shipping_labels.VALUE:shipping_name::VARCHAR as shipping_labels_shipping_name,
        shipping_labels.VALUE:shipping_method::VARCHAR as shipping_labels_shipping_method,
        shipping_labels.VALUE:box_code::VARCHAR as shipping_labels_box_code,
        shipping_labels.VALUE:device_id::VARCHAR as shipping_labels_device_id,
        shipping_labels.VALUE:profile::VARCHAR as shipping_labels_profile,
        shipping_labels.VALUE:partner_fulfillment_id::VARCHAR as shipping_labels_partner_fulfillment_id,
        shipping_labels.VALUE:full_size_to_print::VARCHAR as shipping_labels_full_size_to_print,
        shipping_labels.VALUE:packing_slip::VARCHAR as shipping_labels_packing_slip,
        shipping_labels.VALUE:warehouse::VARCHAR as shipping_labels_warehouse,
        shipping_labels.VALUE:warehouse_id::VARCHAR as shipping_labels_warehouse_id,
        shipping_labels.VALUE:insurance_amount::VARCHAR as shipping_labels_insurance_amount,
        shipping_labels.VALUE:carrier_account_id::VARCHAR as shipping_labels_carrier_account_id,
        shipping_labels.VALUE:source::VARCHAR as shipping_labels_source,
        shipping_labels.VALUE:created_date::VARCHAR as shipping_labels_created_date,
        shipping_labels.VALUE:tracking_url::VARCHAR as shipping_labels_tracking_url,
        shipping_labels.VALUE:package_number::VARCHAR as shipping_labels_package_number,
        {% else %}
        shipping_labels.id as shipping_labels_id,
        shipping_labels.legacy_id as shipping_labels_legacy_id,
        shipping_labels.account_id as shipping_labels_account_id,
        shipping_labels.shipment_id as shipping_labels_shipment_id,
        shipping_labels.order_id as shipping_labels_order_id,
        shipping_labels.box_id as shipping_labels_box_id,
        shipping_labels.box_name as shipping_labels_box_name,
        shipping_labels.status as shipping_labels_status,
        shipping_labels.tracking_number as shipping_labels_tracking_number,
        shipping_labels.order_number as shipping_labels_order_number,
        shipping_labels.order_account_id as shipping_labels_order_account_id,
        shipping_labels.carrier as shipping_labels_carrier,
        shipping_labels.shipping_name as shipping_labels_shipping_name,
        shipping_labels.shipping_method as shipping_labels_shipping_method,
        shipping_labels.box_code as shipping_labels_box_code,
        shipping_labels.device_id as shipping_labels_device_id,
        shipping_labels.profile as shipping_labels_profile,
        shipping_labels.partner_fulfillment_id as shipping_labels_partner_fulfillment_id,
        shipping_labels.full_size_to_print as shipping_labels_full_size_to_print,
        shipping_labels.packing_slip as shipping_labels_packing_slip,
        shipping_labels.warehouse as shipping_labels_warehouse,
        shipping_labels.warehouse_id as shipping_labels_warehouse_id,
        shipping_labels.insurance_amount as shipping_labels_insurance_amount,
        shipping_labels.carrier_account_id as shipping_labels_carrier_account_id,
        shipping_labels.source as shipping_labels_source,
        shipping_labels.created_date as shipping_labels_created_date,
        shipping_labels.tracking_url as shipping_labels_tracking_url,
        shipping_labels.package_number as shipping_labels_package_number,
        {% endif %}
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY a.id, a.legacy_id, a.order_id, user_id, a.warehouse_id order by {{daton_batch_runtime()}} desc) row_num
        from {{i}} a
        {{unnesting("shipping_labels")}}
                    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}        
    )
    where row_num = 1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
