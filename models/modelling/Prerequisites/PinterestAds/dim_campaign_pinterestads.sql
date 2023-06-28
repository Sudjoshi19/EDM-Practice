select 
cast(CAMPAIGN_ID as string) as campaign_id, 
'Pinterest' as campaign_type, 
campaign_name,
'' as portfolio_id,
'' as portfolio_name, 
'Pinterest' as ad_channel, 
COALESCE(CAST(CAMPAIGN_ENTITY_STATUS as string),cast(null as string)) as status,
cast(null as numeric) as budget, 
cast(null as string) as budget_type, 
cast(null as string) as campaign_placement,
cast(null as decimal) as bidding_amount, 
cast(null as string) as bidding_strategy_type,
_daton_batch_runtime
from {{ ref('PinterestAdsAnalytics') }}