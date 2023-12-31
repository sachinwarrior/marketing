{{ config (
    materialized="table"
)}}
with fb_digital_abs_1 as (SELECT ad_name,ad_id as fb_ad_id, account_id, account_name, campaign_id, adset_id,campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks  FROM `wm-gcp-data.facebook_digital_abs_1.ads_insights`),

 fb_digital_abs_3 as (SELECT ad_name,ad_id as fb_ad_id,account_id, account_name, campaign_id, adset_id,campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr , clicks   FROM `wm-gcp-data.facebook_digital_abs_3.ads_insights`),
 
  fb_digital_abs_4 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_digital_abs_4.ads_insights`),
  
   fb_go2protein_1 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name, campaign_id, adset_id,campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_go2protein_1.ads_insights`), 
   
    fb_keto_carbs_1 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_wm_keto_carbs_1.ads_insights`),
    
    fb_keto_carbs_2 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_2.ads_insights`), 
    
    fb_keto_carbs_3 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks   FROM `wm-gcp-data.facebook_keto_carbs_3.ads_insights`), 
    
    fb_keto_carbs_4 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks    FROM `wm-gcp-data.facebook_keto_carbs_4.ads_insights`), 
    
    fb_keto_carbs_6 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr , clicks  FROM `wm-gcp-data.facebook_keto_carbs_6.ads_insights`),
    
    fb_keto_carbs_7 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr , clicks    FROM `wm-gcp-data.facebook_keto_carbs_7.ads_insights`),
    
    fb_keto_carbs_8 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_8.ads_insights`), 
    
    fb_keto_carbs_9 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name, campaign_id, adset_id,campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_9.ads_insights`),
    
    fb_keto_carbs_10 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_10.ads_insights`), 
    
    fb_keto_carbs_11 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_11.ads_insights`), 
    
    fb_keto_carbs_12 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.facebook_keto_carbs_12.ads_insights`),

    fb_acct_id_1102834396733832 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_1102834396733832.ads_insights`),

    fb_acct_id_141973277118416 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_141973277118416.ads_insights`),

    fb_acct_id_436184693398179 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_436184693398179.ads_insights`),
    
    fb_acct_id_436885659994749 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_436885659994749.ads_insights`),

    fb_acct_id_437257933290855 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_437257933290855.ads_insights`),

    fb_acct_id_439433613073287 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_439433613073287.ads_insights`),

    fb_acct_id_548205569382922 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_548205569382922.ads_insights`),

    fb_acct_id_710672379706157 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_710672379706157.ads_insights`),

    fb_acct_id_251974135815477 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_251974135815477.ads_insights`),
    
    fb_acct_id_656781831530701 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_656781831530701.ads_insights`),
    
    fb_acct_id_429656990717616 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_429656990717616.ads_insights`),

    fb_acct_id_439432626406719 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_439432626406719.ads_insights`),

    fb_acct_id_905812309850040 as (SELECT ad_name,ad_id as fb_ad_id, account_id,account_name,campaign_id, adset_id, campaign_name, adset_name, date_start, date_stop, spend, impressions, ctr, clicks     FROM `wm-gcp-data.fb_acct_id_905812309850040.ads_insights`),

  fb_ads as (select * 
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
  from fb_keto_carbs_1
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
UNION ALL 
  select * 
  from fb_acct_id_1102834396733832
UNION ALL 
  select * 
  from fb_acct_id_141973277118416
UNION ALL 
  SELECT *
  from fb_acct_id_436184693398179
UNION ALL
  SELECT * 
  FROM fb_acct_id_436885659994749
UNION ALL 
  SELECT * 
  FROM fb_acct_id_437257933290855
UNION ALL 
  SELECT * 
  FROM fb_acct_id_439433613073287
UNION ALL 
  SELECT * 
  FROM fb_acct_id_548205569382922
UNION ALL 
  SELECT * 
  FROM fb_acct_id_710672379706157
UNION ALL 
  SELECT * 
  FROM fb_acct_id_905812309850040
UNION ALL 
  SELECT * 
  FROM fb_acct_id_251974135815477
UNION ALL 
  SELECT * 
  FROM fb_acct_id_429656990717616
UNION ALL 
  SELECT * 
  FROM fb_acct_id_656781831530701
UNION ALL 
  SELECT * 
  FROM fb_acct_id_439432626406719)
  
select * from fb_ads
