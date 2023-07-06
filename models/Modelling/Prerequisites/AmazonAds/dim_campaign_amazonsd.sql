select 
prodads.campaignId as campaign_id, 
'Sponsored Display' as campaign_type, 
campaignName as campaign_name, 
coalesce(portfolioId,'') as portfolio_id,
coalesce(name,'') as portfolio_name,
'Amazon' as ad_channel, 
cast(null as string) as status, 
cast(null as decimal) as budget, 
cast(null as string) as budget_type, 
cast(null as string) as campaign_placement,
cast(null as decimal) as bidding_amount, 
cast(null as string) as bidding_strategy_type,
_daton_batch_runtime
from {{ ref('SDProductAdsReport') }} prodads
left join 
    (
    select distinct 
    campaign.portfolioId, 
    portfolio.name, 
    campaign.campaignId
    from {{ ref('SDCampaign') }} campaign
    left join {{ ref('SDPortfolio') }} portfolio
    on campaign.portfolioId = portfolio.portfolioId
    ) portfolio_map
on prodads.campaignId = portfolio_map.campaignId