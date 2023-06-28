{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

    {{config( 
        materialized='incremental', 
        incremental_strategy='merge', 
        partition_by = { 'field': 'created_at', 'data_type': 'dbt.type_timestamp()' },
        cluster_by = ['order_id'], 
        unique_key = ['id','order_id','state','outcome','line_item_id']
    )}}

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
    {{set_table_name('%loopreturns%return%detail')}}    
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

        SELECT * {{exclude()}} (row_num)
        From (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            id,		
            created_at,
            order_id,			
            order_name,			
            order_number,			
            state,			
            outcome,			
            currency,			
            refund,			
            gift_card,			
            exchange,			
            return_total,	
            line_items.line_item_id ,		
            line_items.provider_line_item_id,			
            line_items.sku as line_items_sku ,			
            line_items.title as line_items_title,			
            line_items.returned_at as line_items_returned_at,						
            line_items.product_id as line_items_product_id ,			
            line_items.variant_id as line_items_variant_id,			
            line_items.price as line_items_price,			
            line_items.exchange_variant as line_items_exchange_variant	,		
            line_items.reason as line_items_reason,			
            line_items.parent_reason as line_items_parent_return_reason,		
            line_items.barcode as line_items_barcode,		
            customer_email,			
            carrier,			
            label_status,			
            label_updated_at,			
            label_rate,			
            label_url,			
            destination_id,			
            status_page_url,			
            tracking_number,
            return_method,					
            package_reference,				
            refund_st,			
            gift_card_st,			
            created_at_st,			
            exchange_order_name,			
            exchange_order_id,			
            gift_card_order_name,			
            gift_card_order_id,	
            {% if var('currency_conversion_flag') %}
                case when c.value is null then 1 else c.value end as exchange_currency_rate,
                case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
            {% else %}
                cast(1 as decimal) as exchange_currency_rate,
                a.currency as exchange_currency_code,
            {% endif %}        
	        a.{{daton_user_id()}} as _daton_user_id,
            a.{{daton_batch_runtime()}} as _daton_batch_runtime,
            a.{{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            ROW_NUMBER() OVER (PARTITION BY id,order_id,state,outcome,line_item_id order by a.{{daton_batch_runtime()}} desc) row_num
            from {{i}} a
            {{unnesting("line_items")}}
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}         
        )
        where row_num = 1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}