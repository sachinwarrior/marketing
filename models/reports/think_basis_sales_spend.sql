{{ config (
    materialized="table"
)}}
with thinkbasis_da_1 as (SELECT ad_name,ad_id as fb_ad_id, account_id, account_name, campaign_name, adset_name, cast(date_start as date) date_start, cast(date_stop as date) as date_stop, spend, impressions, ctr, clicks  
    FROM thinkbasis_abs_1.ads_insights), 

think_basis as (select * 
from (select * except(date_start), cast(date_start as date) as date_start from thinkbasis_da_1) thinkbasis_da_1
full outer join (select * from dbt_marketing.fb_sales_90d_agg_v3 where ad_id is not null) fb_sales
on fb_sales.order_date = thinkbasis_da_1.date_start and fb_sales.ad_id = thinkbasis_da_1.fb_ad_id)

select * from think_basis where fb_ad_id is not null and ad_id is not null order by date_start desc