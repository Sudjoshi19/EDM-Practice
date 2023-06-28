-- Myntra Forward Item Price
    select 
    brand,
    date(Created_On_Date) as date,
    'Revenue' as amount_type,
    'Order' as transaction_type,
    'Item Price' as charge_type,
    Order_Id_FK as order_id,
    {{store_name('store')}},
    '' as product_id,
    Myntra_SKU_Code as sku,
    {% if var('currency_conversion_flag') %}
    case when exchange_rates.value is null then 1 else exchange_rates.value end as exchange_currency_rate,
    case when exchange_rates.from_currency_code is null then 'INR' else exchange_rates.from_currency_code end as exchange_currency_code,
    {% else %}
    cast(1 as decimal) as exchange_currency_rate,
    'INR' as exchange_currency_code, 
    {% endif %}
    'Myntra' as platform_name,
    sum(cast(Final_Amount as numeric)) amount
    from {{ ref('MyntraSeller')}} sales
    {% if var('currency_conversion_flag') %}
    left join {{ ref('ExchangeRates') }} exchange_rates 
    on Created_On_Date = exchange_rates.date and 'INR' = exchange_rates.to_currency_code
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10,11

    UNION ALL

    -- Myntra Forward Shipping Promotions
    select 
    brand,
    Created_On_Date as date,
    'Promotion' as amount_type,
    'Order' as transaction_type,
    'Shipping Promotional Discount' as charge_type,
    Order_Id_FK as order_id,
    {{store_name('store')}},
    '' product_id,
    Myntra_SKU_Code as sku,
    {% if var('currency_conversion_flag') %}
    case when exchange_rates.value is null then 1 else exchange_rates.value end as exchange_currency_rate,
    case when exchange_rates.from_currency_code is null then 'INR' else exchange_rates.from_currency_code end as exchange_currency_code,
    {% else %}
    cast(1 as decimal) as exchange_currency_rate,
    'INR' as exchange_currency_code, 
    {% endif %}
    'Myntra' as platform_name,
    sum(cast(Discount as numeric)) amount
    from {{ ref('MyntraSeller')}} sales
    {% if var('currency_conversion_flag') %}
    left join {{ ref('ExchangeRates') }} exchange_rates on Created_On_Date = exchange_rates.date and 'INR' = exchange_rates.to_currency_code
    {% endif %}
    group by 1,2,3,4,5,6,7,8,9,10,11
