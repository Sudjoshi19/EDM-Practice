select 
'Walmart Retail' as platform_name,
cast(null as string) type,
STORE_NAME as store_name,
cast(null as string) description,
cast(null as string) status,
_daton_batch_runtime
from {{ ref('WalmartRetailSalesReport') }}