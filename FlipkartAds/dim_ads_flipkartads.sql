select 
Listing_ID as ad_id,
'Flipkart' as ad_channel,
cast(null as string)as ad_name,
'Flipkart' as ad_type,
cast(null as string) as adgroup_id,
AdGroup_Name as adgroup_name,
_daton_batch_runtime 
from {{ ref('FlipkartAdsCampaign') }}