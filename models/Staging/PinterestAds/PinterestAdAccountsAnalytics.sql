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

{% set table_name_query %}
{{set_table_name('%pinterest%ad_accounts_analytics')}}    
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
        AD_ACCOUNT_ID,
        CHECKOUT_ROAS,
        CLICKTHROUGH_1,
        CLICKTHROUGH_1_GROSS,
        CLICKTHROUGH_2,
        CPC_IN_MICRO_DOLLAR,
        CPM_IN_DOLLAR,
        CPM_IN_MICRO_DOLLAR,
        CTR,
        CTR_2,
        ECPCV_IN_DOLLAR,
        ECPCV_P95_IN_DOLLAR,
        ECPC_IN_DOLLAR,
        ECPC_IN_MICRO_DOLLAR,
        ECPE_IN_DOLLAR,
        ECPM_IN_MICRO_DOLLAR,
        ECPV_IN_DOLLAR,
        ECTR,
        EENGAGEMENT_RATE,
        ENGAGEMENT_1,
        ENGAGEMENT_2,
        ENGAGEMENT_RATE,
        IDEA_PIN_PRODUCT_TAG_VISIT_1,
        IDEA_PIN_PRODUCT_TAG_VISIT_2,
        IMPRESSION_1,
        IMPRESSION_1_GROSS,
        IMPRESSION_2,
        INAPP_CHECKOUT_COST_PER_ACTION,
        OUTBOUND_CLICK_1,
        OUTBOUND_CLICK_2,
        PAGE_VISIT_COST_PER_ACTION,
        PAGE_VISIT_ROAS,
        PAID_IMPRESSION,
        PIN_ID,
        REPIN_1,
        REPIN_2,
        REPIN_RATE,
        SPEND_IN_DOLLAR,
        SPEND_IN_MICRO_DOLLAR,
        TOTAL_CHECKOUT,
        TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR,
        TOTAL_CLICKTHROUGH,
        TOTAL_CLICK_CHECKOUT,
        TOTAL_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR,
        TOTAL_CLICK_SIGNUP,
        TOTAL_CLICK_SIGNUP_VALUE_IN_MICRO_DOLLAR,
        TOTAL_CONVERSIONS,
        TOTAL_CUSTOM,
        TOTAL_ENGAGEMENT,
        TOTAL_ENGAGEMENT_CHECKOUT,
        TOTAL_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR,
        TOTAL_ENGAGEMENT_SIGNUP,
        TOTAL_ENGAGEMENT_SIGNUP_VALUE_IN_MICRO_DOLLAR,
        TOTAL_IDEA_PIN_PRODUCT_TAG_VISIT,
        TOTAL_IMPRESSION_FREQUENCY,
        TOTAL_IMPRESSION_USER,
        TOTAL_LEAD,
        TOTAL_PAGE_VISIT,
        TOTAL_REPIN_RATE,
        TOTAL_SIGNUP,
        TOTAL_SIGNUP_VALUE_IN_MICRO_DOLLAR,
        TOTAL_VIDEO_3SEC_VIEWS,
        TOTAL_VIDEO_AVG_WATCHTIME_IN_SECOND,
        TOTAL_VIDEO_MRC_VIEWS,
        TOTAL_VIDEO_P0_COMBINED,
        TOTAL_VIDEO_P100_COMPLETE,
        TOTAL_VIDEO_P25_COMBINED,
        TOTAL_VIDEO_P50_COMBINED,
        TOTAL_VIDEO_P75_COMBINED,
        TOTAL_VIDEO_P95_COMBINED,
        TOTAL_VIEW_CHECKOUT,
        TOTAL_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR,
        TOTAL_VIEW_SIGNUP,
        TOTAL_VIEW_SIGNUP_VALUE_IN_MICRO_DOLLAR,
        TOTAL_WEB_CHECKOUT,
        TOTAL_WEB_CHECKOUT_VALUE_IN_MICRO_DOLLAR,
        TOTAL_WEB_CLICK_CHECKOUT,
        TOTAL_WEB_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR,
        TOTAL_WEB_ENGAGEMENT_CHECKOUT,
        TOTAL_WEB_ENGAGEMENT_CHECKOUT_VALUE_IN_MICRO_DOLLAR,
        TOTAL_WEB_VIEW_CHECKOUT,
        TOTAL_WEB_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR,
        VIDEO_3SEC_VIEWS_2,
        VIDEO_LENGTH,
        VIDEO_MRC_VIEWS_2,
        VIDEO_P0_COMBINED_2,
        VIDEO_P100_COMPLETE_2,
        VIDEO_P25_COMBINED_2,
        VIDEO_P50_COMBINED_2,
        VIDEO_P75_COMBINED_2,
        VIDEO_P95_COMBINED_2,
        WEB_CHECKOUT_COST_PER_ACTION,
        WEB_CHECKOUT_ROAS,
        DATE,
        CONVERSION_REPORT_TIME,
        IMPRESSION_1_in,
        IMPRESSION_1_GROSS_in,
        PAID_IMPRESSION_in,
        SPEND_IN_MICRO_DOLLAR_in,
        TOTAL_IMPRESSION_USER_in,
        TOTAL_VIDEO_3SEC_VIEWS_in,
        TOTAL_VIDEO_MRC_VIEWS_in,
        TOTAL_VIDEO_P0_COMBINED_in,
        TOTAL_VIDEO_P100_COMPLETE_in,
        TOTAL_VIDEO_P25_COMBINED_in,
        TOTAL_VIDEO_P50_COMBINED_in,
        TOTAL_VIDEO_P75_COMBINED_in,
        TOTAL_VIDEO_P95_COMBINED_in,
        CLICKTHROUGH_1_in,
        CLICKTHROUGH_1_GROSS_in,
        ENGAGEMENT_1_in,
        OUTBOUND_CLICK_1_in,
        TOTAL_CLICKTHROUGH_in,
        TOTAL_CONVERSIONS_in,
        TOTAL_ENGAGEMENT_in,
        TOTAL_PAGE_VISIT_in,
        REPIN_1_in,
        IMPRESSION_2_in,
        TOTAL_CHECKOUT_in,
        TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR_in,
        TOTAL_VIEW_CHECKOUT_in,
        TOTAL_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR_in,
        TOTAL_WEB_CHECKOUT_in,
        TOTAL_WEB_CHECKOUT_VALUE_IN_MICRO_DOLLAR_in,
        TOTAL_WEB_VIEW_CHECKOUT_in,
        TOTAL_WEB_VIEW_CHECKOUT_VALUE_IN_MICRO_DOLLAR_in,
        ENGAGEMENT_2_in,
        TOTAL_CLICK_CHECKOUT_in,
        TOTAL_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR_in,
        TOTAL_WEB_CLICK_CHECKOUT_in,
        TOTAL_WEB_CLICK_CHECKOUT_VALUE_IN_MICRO_DOLLAR_in,
        VIDEO_3SEC_VIEWS_2_in,
        VIDEO_MRC_VIEWS_2_in,
        VIDEO_P0_COMBINED_2_in,
        VIDEO_P25_COMBINED_2_in,
        VIDEO_P50_COMBINED_2_in,
        VIDEO_P75_COMBINED_2_in,
        OUTBOUND_CLICK_2_in,
        VIDEO_P100_COMPLETE_2_in,
        VIDEO_P95_COMBINED_2_in,
        CLICKTHROUGH_2_in,
        REPIN_2_in,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY AD_ACCOUNT_ID,DATE,CONVERSION_REPORT_TIME order by {{daton_batch_runtime()}} desc) row_num
        from {{i}} 
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
    )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}