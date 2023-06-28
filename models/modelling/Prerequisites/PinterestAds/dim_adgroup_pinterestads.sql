select 
(cast(AD_GROUP_ID as string)) as adgroup_id,
'' as adgroup_name,
'Pinterest' as ad_channel,
cast(CAMPAIGN_ID as string) as campaign_id, 
'Pinterest' as campaign_type, 
campaign_name,
_daton_batch_runtime 
from {{ ref('PinterestAdsAnalytics') }}