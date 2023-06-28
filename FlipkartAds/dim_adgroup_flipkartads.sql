select 
cast(null as string) as adgroup_id,
AdGroup_Name as adgroup_name,
'Flipkart' as ad_channel,
Campaign_ID as campaign_id, 
'Flipkart' as campaign_type, 
cast(null as string) as campaign_name, 
_daton_batch_runtime 
from {{ ref('FlipkartAdsCampaign') }}