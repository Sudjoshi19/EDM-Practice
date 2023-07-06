
select
platform_name,
brand_name,
daton_brand_name,
start_date,
end_date,
commission_type,
currency_code,
cast(flat_rate as numeric) flat_rate,
cast(revenue_min as numeric) revenue_min,
cast(revenue_max as numeric) revenue_max,
cast(commission_rate as numeric) commission_rate,
_daton_batch_runtime
from {{ref('PartnerCommissions')}}
        




