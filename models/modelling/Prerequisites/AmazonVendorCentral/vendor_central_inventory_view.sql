

Select 
brand,
marketplaceName,
asin, 
cast(null as string) sku,
startdate as snapshot_date,
openPurchaseOrderUnits,
netReceivedInventoryUnits,
sellableOnHandInventoryUnits,
unsellableOnHandInventoryUnits,
aged90PlusDaysSellableInventoryUnits,
unhealthyInventoryUnits
from {{ ref('VendorInventoryReportByManufacturing') }}

