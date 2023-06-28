
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
{{set_table_name('%myntraseller%')}}    
{% endset %}  


{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
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

SELECT * {{exclude()}}(row_num)
FROM (
    select 
    '{{brand}}' as brand,
    '{{store}}' as store,
    Final_Amount,
    Shipped_On,
    Discount,
    Shipping_Charge,
    Size,
    Seller_Order_Id,
    Seller_SKU_Code,
    Courier_Code,
    Tax_Recovery,
    Article_Type_Id,
    SKU_Id,
    Packed_On,
    Zipcode,
    Order_Status,
    Seller_Warehouse_Id,
    Article_Type,
    Coupon_Discount,
    Created_On,
    FMPU_date,
    Order_Release_Id,
    Style_ID,
    Vendor_Article_Number,
    Delivered_On,
    Inscanned_On,
    Order_Line_Id,
    Seller_Id,
    Warehouse_Id,
    Packet_Id,
    City,
    a.Brand as Seller_Brand,
    Myntra_SKU_Code,
    Store_Order_Id,
    Order_Id_FK,
    State,
    Style_Name,
    Gift_Charge,
    Order_Tracking_Number,
    Total_Mrp,
    FileName,
    Style_ID_dt,
    Cancellation_Reason,
    Cancelled_On,
    Cancellation_Reason_Id_Fk,
    SKU_Id_dt,
    {% if target.type=='snowflake' %} 
    cast(cast(split_part(Created_On,'.',0) as timestamp) as DATE) as Created_On_Date,
    {% else %}
    cast(timestamp(split(Created_On,'.')[safe_ordinal(1)]) as DATE) as Created_On_Date,
    {% endif %}
	a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
    DENSE_RANK() OVER (PARTITION BY Seller_Order_Id order by a.{{daton_batch_runtime()}} desc) row_num
	    from {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
