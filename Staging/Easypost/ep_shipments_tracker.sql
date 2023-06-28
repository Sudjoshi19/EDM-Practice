{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

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

with unnested_customers as(
{% set table_name_query %}
{{set_table_name('%easypost%shipments')}}    
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

    SELECT * 
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        a.created_at,
        is_return,
        a.mode,
        options.currency as option_currency,
        payment.type as options_payment_type,
        options.date_advance as options_date_advance,
        options.print_custom as options_print_custom,
        options.invoice_number as options_invoice_number,
        options.print_custom_1 as options_print_custom_1,
        options.print_custom_2 as options_print_custom_2,
        options.label_format as options_label_format,
        a.status,
        a.tracking_code,
        a.updated_at,
        batch_id,
        batch_status,
        customs_info,
        from_address,
        parcel,
        postage_label,
        rates,
        scan_form,
        selected_rate,
        tracker.id as tracker_id,
        tracker.object as tracker_object,
        tracker.mode as tracker_mode,
        tracker.tracking_code as tracker_tracking_code,
        tracker.status as tracker_status,
        tracker.status_detail as tracker_status_detail,
        tracker.created_at as tracker_created_at,
        tracker.updated_at as tracker_updated_at,
        tracker.est_delivery_date as tracker_est_delivery_date,
        tracker.shipment_id as tracker_shipment_id,
        tracker.carrier as tracker_carrier,
        tracker.tracking_details as tracker_tracking_details,
        tracker.carrier_detail as tracker_carrier_detail,
        tracker.public_url as tracker_public_url,
        tracker.signed_by as tracker_signed_by,
        tracker.weight as tracker_weight,
        to_address,
        usps_zone,
        return_address,
        buyer_address,
        fees,
        a.id,
        a.object,
        messages,
        reference,
        refund_status,
        forms,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then options.currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            options.currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
            {{unnesting("options")}}
            {{unnesting("tracker")}}
            {{multi_unnesting("options","payment")}}
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and options.currency = c.to_currency_code
            {% endif %}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}


),

dedup as (
select *,
ROW_NUMBER() OVER (PARTITION BY id,tracker_id order by _daton_batch_runtime desc) row_num
from unnested_customers a

)

select * {{exclude()}} (row_num)
from dedup 
where row_num = 1
