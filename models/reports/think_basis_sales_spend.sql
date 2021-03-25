with thinkbasis_da_1 as (SELECT ad_name,ad_id as fb_ad_id, account_id, account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks  
    FROM thinkbasis_abs_1.ads_insights)

select * 
from (select * except(date_start), cast(date_start as date) as date_start from thinkbasis_da_1) thinkbasis_da_1
full outer join (select * from {{ref('fb_sales_90d_agg_v3')}} where ad_id is not null) fb_sales
on fb_sales.order_date = thinkbasis_da_1.date_start and fb_sales.ad_id = thinkbasis_da_1.fb_ad_id
