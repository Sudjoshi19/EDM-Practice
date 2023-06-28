select 
(cast(Ad_Group_Id as string)) as adgroup_id,
Ad_Group_Name as adgroup_name,
'Walmart' as ad_channel,
cast(Campaign_Id as string) as campaign_id, 
'Walmart' as campaign_type, 
Campaign_Name as campaign_name, 
_daton_batch_runtime 
from {{ ref('WalmartAds') }}