{{ config (
    materialized="table"
)}}
with jb_woo as (select 
        date,
        mktg_custom_1,
        funnel, 
        sum(sales) as sales,
        sum(trials) as trials, 
        sum(sales_count) as sales_count, 
        sum(cogs) as cogs 
    from {{ref('jb_woo_aggregated')}}
    group by date, funnel, mktg_custom_1), 

spend as (select 
        date as ad_date, 
        affiliate_name, affiliate_id, 
        funnel_campaign,
        string_agg(campaign_name,',') as campaign_name,
        sum(clicks) as clicks , 
        sum(conversions) as conversions, 
        string_agg(offer_id,',') as offer_id, 
       sum(payout) as payout
    from (select *,  case when campaign_name = '3' or campaign_name ='25' then 'abs'
        when campaign_name = '27' then 'keto carbs'
        when campaign_name = '14' then 'keto sweet'
        when campaign_name = '17' then 'go2protein'
        end funnel_campaign,
        from {{ref('spend_clean_aggregated')}} )
    group by date, affiliate_name, affiliate_id, 
        funnel_campaign),

hasoffer_spent as (select *
from spend
left join jb_woo 
on jb_woo.date = spend.ad_date and jb_woo.mktg_custom_1 = spend.affiliate_name and jb_woo.funnel= spend.funnel_campaign )

select * except(date, mktg_custom_1, funnel, sales, trials, sales_count, cogs), 
  case when sales is null then 0 else sales end sales, 
  case when trials is null then 0 else trials end trials,
  case when sales_count is null then 0 else sales_count end sales_count, 
  case when cogs is null then 0 else cogs end cogs,
from hasoffer_spent
where affiliate_name is not null and payout != 0
