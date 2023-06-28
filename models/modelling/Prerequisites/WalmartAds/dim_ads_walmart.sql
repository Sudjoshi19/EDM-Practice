select 
'' as ad_id,
'Walmart' as ad_channel,
cast(null as string) as ad_name ,
'Walmart' as ad_type,
(cast(Ad_Group_Id as string)) as adgroup_id,
Ad_Group_Name as adgroup_name,
_daton_batch_runtime 
from {{ ref('WalmartAds') }}