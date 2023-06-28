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
{{set_table_name('%flipkart%settledtransactions%')}}    
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
    My_Share,
    Sale_Amount,
    My_share__Rs_,
    Fixed_Fee___Rs_,
    Uninstallation___Packaging_Fee__Rs_,
    Invoice_ID,
    Total_Offer_Amount,
    Order_item_ID,
    Dead_Weight__kgs_,
    Collection_Fee__Rs_,
    Pick_And_Pack_Fee__Rs_,
    Shopsy_Marketing_Fee__Rs_,
    Invoice_Date,
    Payment_Date,
    Product_Sub_Category,
    Sale_Amount__Rs_,
    Commission__Rs_,
    Order_Date,
    Bank_Settlement_Value__Rs____SUM_J_Q_,
    Shipping_Zone,
    Marketplace_Fee__Rs___SUM__U_AH_,
    Tech_Visit_Fee__Rs_,
    COALESCE(Seller_SKU,'') as Seller_SKU,
    Tier,
    Order_ID,
    Return_Type,
    Fulfilment_Type,
    Quantity,
    Installation_Fee__Rs_,
    Product_Cancellation_Fee__Rs_,
    Refund__Rs_,
    Customer_Add_ons_Amount__Rs_,
    Franchise_Fee__Rs_,
    Length_Breadth_Height,
    Commission_Rate____,
    NEFT_ID,
    Total_Offer_Amount__Rs_,
    Reverse_Shipping_Fee__Rs_,
    Taxes__Rs_,
    Dispatch_Date,
    Protection_Fund__Rs_,
    No_Cost_Emi_Fee_Reimbursement_Rs_,
    Chargeable_Weight_Type,
    Chargeable_Wt_Slab__In_Kgs_,
    Neft_Type,
    Shipping_Fee__Rs_,
    Volumetric_Weight__kgs_,
    FileName,
    Commission__Rs__nu,
    Franchise_Fee__Rs__in,
    Additional_Information,
    My_share__Rs__nu,
    Item_Return_Status,
    My_Share_nu,
    Fixed_Fee___Rs__in,
    Collection_Fee__Rs__in,
    Bank_Settlement_Value__Rs____SUM_J_Q__in,
    Marketplace_Fee__Rs___SUM__U_AH__in,
    Taxes__Rs__in,
    Shipping_Fee__Rs__in,
    Pick_And_Pack_Fee__Rs__nu,
    Refund__Rs__nu,
    Reverse_Shipping_Fee__Rs__nu,
    Total_Offer_Amount__Rs__nu,
    Protection_Fund__Rs__nu,
    Commission_Rate_____nu,
    Dead_Weight__kgs__in,
    Shopsy_Order,
    Sale_Amount__Rs__nu,
    Shopsy_Marketing_Fee__Rs__nu,
    Sale_Amount_nu,
    Product_Cancellation_Fee__Rs__nu,
    Payment_Date_st,
    Invoice_Date_st,
    Order_Date_st,
    Dispatch_Date_st,
    Volumetric_Weight__kgs__in,
    Chargeable_Weight_Source,
    GST_on_MP_Fees__Rs_,
    Column10,
    Column11,
    Income_Tax_Credits__Rs__TDS_,
    TCS__Rs_,
    Column8,
    Column9,
    TDS__Rs_,
    Input_GST___TCS_Credits__Rs__GST_TCS_,
    Customer_Add_ons_Amount_Recovery__Rs_,
    Dead_Weight__kgs__st,
    Volumetric_Weight__kgs__st,
    GST_on_MP_Fees__Rs__nu,
    Income_Tax_Credits__Rs__TDS__nu,
    TCS__Rs__nu,
    TDS__Rs__nu,
    Input_GST___TCS_Credits__Rs__GST_TCS__nu,
    Customer_Add_ons_Amount_Recovery__Rs__nu,
    No_Cost_Emi_Fee_Reimbursement_Rs__nu,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
    ROW_NUMBER() OVER (PARTITION BY Order_ID,Seller_SKU,NEFT_ID,Length_Breadth_Height order by a.{{daton_batch_runtime()}} desc,LEFT(filename, 37) DESC) row_num
	    from {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
