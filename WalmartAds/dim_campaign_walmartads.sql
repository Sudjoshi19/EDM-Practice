select 
cast(Campaign_Id as string) as campaign_id, 
'Walmart' as campaign_type, 
Campaign_Name as campaign_name, 
'' as portfolio_id,
'' as portfolio_name, 
'Walmart' as ad_channel, 
'' as status,
cast(null as numeric) as budget, 
cast(null as string) as budget_type, 
'' as campaign_placement,
cast(null as decimal) as bidding_amount, 
cast(null as string) as bidding_strategy_type,
_daton_batch_runtime
from {{ ref('WalmartAds') }}