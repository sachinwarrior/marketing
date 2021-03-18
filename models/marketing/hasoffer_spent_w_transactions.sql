{{ config (
    materialized="table"
)}}

with jb_woo as (select 
        date,
        mktg_custom_1,
        sum(sales) as sales,
        sum(trials) as trials, 
        sum(sales_count) as sales_count, 
        sum(cogs) as cogs 
    from {{ref('jb_woo_aggregated')}}
    group by date, mktg_custom_1), 
spend as (select 
        date as ad_date, 
        affiliate_name, affiliate_id, 
        sum(clicks) as clicks, 
        sum(conversions) as conversions, 
        string_agg(campaign_name,',') as campaign_name, 
        string_agg(offer_id,',') as offer_id, 
        sum(payout) as payout 
    from {{ref('spend_clean_aggregated')}}
    group by date, affiliate_name, affiliate_id)

select *
from spend
left join jb_woo 
on jb_woo.date = spend.ad_date and jb_woo.mktg_custom_1 = spend.affiliate_name 
where mktg_custom_1 is not null