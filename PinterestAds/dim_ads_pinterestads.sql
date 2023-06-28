select  
AD_ID as ad_id,
'Pinterest' as ad_channel,
cast(null as string) as ad_name,
'Pinterest' as ad_type,
(cast(AD_GROUP_ID as string)) as adgroup_id,
'' as adgroup_name,
_daton_batch_runtime 
from {{ ref('PinterestAdsAnalytics') }}