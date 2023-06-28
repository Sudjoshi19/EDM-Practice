select 
'Amazon Vendor Central' as platform_name,
cast(null as string) type,
marketplaceName as store_name,
cast(null as string) description,
cast(null as string) status,
_daton_batch_runtime
from {{ ref('VendorSalesReportByManufacturing') }}