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
{{set_table_name('%flipkart%salesreport%')}}    
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

SELECT * {{exclude()}}(row_num)
FROM (
    select 
    '{{brand}}' as brand,
    '{{store}}' as store,
    CGST_Amount,
    CST_Amount,
    Total_TCS_Deducted,
    Product_Title_Description,
    Price_before_discount,
    Price_after_discount__Price_before_discount_Total_discount_,
    TCS_IGST_Rate,
    TDS_Rate,
    Shipping_Charges,
    TCS_CGST_Rate,
    Type_of_tax,
    TCS_IGST_Amount,
    CAST(cast(Buyer_Invoice_Date as timestamp) as DATE) Buyer_Invoice_Date,
    Is_Shopsy_Order_,
    Seller_Share,
    Taxable_Value__Final_Invoice_Amount__Taxes_,
    HSN_Code,
    Luxury_Cess_Amount,
    FSN,
    Customer_s_Billing_State,
    Order_ID,
    Fulfilment_Type,
    Customer_s_Delivery_State,
    Total_Discount,
    Usual_Price,
    Event_Type,
    IGST_Amount,
    CGST_Rate,
    VAT_Amount,
    TCS_CGST_Amount,
    Buyer_Invoice_Amount,
    Customer_s_Billing_Pincode,
    Order_Shipped_From__State_,
    Bank_Offer_Share,
    Item_Quantity,
    SGST_Rate__or_UTGST_as_applicable_,
    Order_Date,
    Buyer_Invoice_ID,
    IGST_Rate,
    Customer_s_Delivery_Pincode,
    Final_Invoice_Amount__Price_after_discount_Shipping_Charges_,
    TCS_SGST_Amount,
    SGST_Amount__Or_UTGST_as_applicable_,
    Order_Item_ID,
    Seller_GSTIN,
    CST_Rate,
    Luxury_Cess_Rate,
    Order_Type,
    VAT_Rate,
    TCS_SGST_Rate,
    Order_Approval_Date,
    TDS_Amount,
    SKU,
    Event_Sub_Type,
    FileName,
    Usual_Price_st,
    Customer_s_Delivery_Pincode_st,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
    DENSE_RANK() OVER (PARTITION BY buyer_invoice_id order by a.{{daton_batch_runtime()}} desc,LEFT(filename, 54) DESC) row_num
	    from {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
