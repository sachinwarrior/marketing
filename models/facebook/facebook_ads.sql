{{ config (
    materialized="table"
)}}
with fb_digital_abs_1 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks  FROM `wm-gcp-data.facebook_digital_abs_1.ads_insights`),

 fb_digital_abs_3 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr , clicks   FROM `wm-gcp-data.facebook_digital_abs_3.ads_insights`),
 
  fb_digital_abs_4 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_digital_abs_4.ads_insights`),
  
   fb_go2protein_1 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_go2protein_1.ads_insights`), 
   
    fb_keto_carbs_1 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_wm_keto_carbs_1.ads_insights`),
    
    fb_keto_carbs_2 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_2.ads_insights`), 
    
    fb_keto_carbs_3 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks   FROM `wm-gcp-data.facebook_keto_carbs_3.ads_insights`), 
    
    fb_keto_carbs_4 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks    FROM `wm-gcp-data.facebook_keto_carbs_4.ads_insights`), 
    
    fb_keto_carbs_6 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr , clicks  FROM `wm-gcp-data.facebook_keto_carbs_6.ads_insights`),
    
    fb_keto_carbs_7 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr , clicks    FROM `wm-gcp-data.facebook_keto_carbs_7.ads_insights`),
    
    fb_keto_carbs_8 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_8.ads_insights`), 
    
    fb_keto_carbs_9 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_9.ads_insights`),
    
    fb_keto_carbs_10 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_10.ads_insights`), 
    
    fb_keto_carbs_11 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_11.ads_insights`), 
    
    fb_keto_carbs_12 as (SELECT account_name, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_12.ads_insights`)
    
     
  select * 
  from fb_digital_abs_1
UNION ALL 
  select * 
  from fb_digital_abs_3
UNION ALL 
  select * 
  from fb_digital_abs_4
UNION ALL 
  select * 
  from fb_go2protein_1

UNION ALL 
  select * 
  from fb_keto_carbs_2
UNION ALL
  select * 
  from fb_keto_carbs_3 
UNION ALL 
  select * 
  from fb_keto_carbs_4
UNION ALL 
  select * 
  from fb_keto_carbs_6
UNION ALL 
  select * 
  from fb_keto_carbs_7
UNION ALL 
  select * 
  from fb_keto_carbs_8
UNION ALL 
  select * 
  from fb_keto_carbs_9 
UNION ALL 
  select *
  from fb_keto_carbs_10 
UNION ALL 
  select * 
  from fb_keto_carbs_11
UNION ALL 
  select * 
  from fb_keto_carbs_12 
