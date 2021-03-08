{{ config (
    materialized="table"
)}}
select * 
from (select * except(date_start), cast(date_start as date) as date_start from {{ref('fb_ads')}}) facebook_ads
full outer join (select * from {{ref('fb_sales_90d_agg_v3')}} where ad_id is not null) fb_sales
on fb_sales.order_date = facebook_ads.date_start and fb_sales.ad_id = facebook_ads.fb_ad_id
