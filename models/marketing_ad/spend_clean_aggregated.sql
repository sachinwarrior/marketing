{{ config (
    materialized="table"
)}}
SELECT 
    date,
    affiliate_name, 
    affiliate_id,
    campaign_name, 
    string_agg(cast(offer_id as string)) as offer_id, 
    sum(clicks) as clicks, 
    sum(conversions) as conversions, 
    sum(payout) as payout 
FROM {{ref('spend_clean')}}
group by date, affiliate_name, affiliate_id, campaign_name 
order by date desc