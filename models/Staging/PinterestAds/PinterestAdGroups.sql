
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

with unnested as (
{% set table_name_query %}
{{set_table_name('%pinterest%ad_groups')}}    
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
    From (
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        name,
        status,
        budget_in_micro_currency,
        bid_in_micro_currency,
        budget_type,
        start_time,
        end_time,
        targeting_spec,
        lifetime_frequency_cap,
        {% if target.type=='snowflake' %} 
        TRACKING_URLS.VALUE:impression::VARCHAR as impression,
        TRACKING_URLS.VALUE:click::VARCHAR as click,
        TRACKING_URLS.VALUE:engagement::VARCHAR as engagement,
        TRACKING_URLS.VALUE:buyable_button::VARCHAR as buyable_button,
        TRACKING_URLS.VALUE:audience_verification::VARCHAR as audience_verification,
        {% else %}
        TRACKING_URLS.impression,
        TRACKING_URLS.click,
        TRACKING_URLS.engagement,
        TRACKING_URLS.buyable_button,
        TRACKING_URLS.audience_verification,
        {% endif %}
        auto_targeting_enabled,
        placement_group,
        pacing_delivery_type,
        conversion_learning_mode_type,
        summary_status,
        feed_profile_id,
        campaign_id,
        billable_event,
        id AS ad_group_id,
        type,
        ad_account_id,
        {% if target.type=='snowflake' %} 
            CAST(to_timestamp_ntz(cast(created_time as int)) as timestamp) as created_time,
        {% else %}
            CAST(TIMESTAMP_MILLIS(cast(created_time as int)) as timestamp) as created_time,
        {% endif %}
        updated_time,
        {% if target.type=='snowflake' %} 
        --     OPTIMIZATION_GOAL_METADATA.VALUE:conversion_tag_v3_goal_metadata,
        {% else %}
            OPTIMIZATION_GOAL_METADATA,
        {% endif %}
        bid_strategy_type,
        CONVERSION_REPORT_TIME,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY id,created_time order by {{daton_batch_runtime()}} desc) row_num
        from {{i}}
            {{unnesting("TRACKING_URLS")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    
        
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
)

select * {{exclude()}}(row_num)
from unnested 
where row_num =1

