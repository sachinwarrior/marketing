{{ config (
    materialized="table"
)}}

with
    mp_campaigns AS (
    SELECT
      concat('https://cloud.maropost.com/accounts/1281/campaigns/','', cast(id as string)) as campaign_url,
      id,   
      name,  
      subject,
      status,
      from_name,
      from_email,
      campaign_type,
      CAST(SUBSTR(sent_at,0,10) AS DATE) sent_at,
      SUBSTR(updated_at,0,10) updated_at,
      total_sent,
      total_received,
      total_opens,
      total_unique_opens,
      total_clicks,
      total_unique_clicks,
      total_unsubscribes
    FROM
      (select id, REPLACE(lower(name),'group','') as name, subject, status, from_name, from_email, campaign_type, sent_at, updated_at, total_sent, total_received,
        total_opens, total_unique_opens, total_clicks, total_unique_clicks, total_unsubscribes
        from maropost.campaigns
        where name like '%wm-%'
        UNION ALL 
        select * 
        from maropost.campaigns
        where name not like '%wm-%')),
        
recurly_email as (select 
  sub_aff_id as recurly_sub_aff_id, 
  aff_sub as recurly_aff_sub,
  sum(sales) as recurly_sales,
  sum(sales_count) as recurly_sales_count, 
  sum(trials) as recurly_trials, sum(cogs) AS recurly_cogs
from wm_data_recurly.recurly_pubsub_transactions_summary 
where aff_id = '2'
group by sub_aff_id, aff_sub), 

woocommerce_email as (
  select
    sub_aff_id as woo_sub_aff_id, aff_sub as woo_aff_sub, 
    sum(total_amount) as woo_sales, 
    sum(sales_count) as woo_sales_count, 
    sum(trials) as woo_trials, 
    sum(cogs) as woo_cogs
    FROM 
  (SELECT 
  mktg_custom_1 as sub_aff_id , 
  REPLACE(mktg_custom_2,' ','') AS aff_sub, 
  (CAST(_order_total AS NUMERIC)) AS total_amount,
  case when products_purchased not like '%u1.%' or products_purchased not like '%u2.%' then 1 
  end sales_count,
  (CAST(_wc_cog_order_total_cost AS NUMERIC)) AS cogs,
  CAST(trial_take AS INT64) as trials
FROM {{ref('transactions_dbt')}}
WHERE mktg_custom_2 IS NOT NULL and post_date >= '2020-11-01' and mktg_affiliate = '2' and post_status in ('wc-completed','wc-processing'))
  group by woo_sub_aff_id, woo_aff_sub), 

all_txn as (
  select sub_aff_id, aff_sub, 
   sum(sales) as sales, 
    sum(sales_count) as sales_count, 
    sum(trials) as trials, 
    sum(cogs) as cogs
  from (select woo_sub_aff_id as sub_aff_id, woo_aff_sub as aff_sub, woo_sales as sales, woo_sales_count as sales_count, woo_trials as trials, woo_cogs as cogs
    from woocommerce_email 
    UNION ALL
    SELECT  recurly_sub_aff_id, recurly_aff_sub, 
    recurly_sales, recurly_sales_count, recurly_trials, recurly_cogs 
    FROM recurly_email)
    group by sub_aff_id, aff_sub),
    
mp_sales as (SELECT * except(woo_aff_sub, woo_sub_aff_id,recurly_aff_sub, recurly_sub_aff_id, recurly_sales, woo_sales, recurly_trials, woo_trials, recurly_sales_count, woo_sales_count, recurly_cogs, woo_cogs), 

case when recurly_sales is null then 0 else recurly_sales end recurly_sales,
case when woo_sales is null then 0 else woo_sales end woo_sales, 

case when recurly_trials is null then 0 else recurly_trials end recurly_trials, 
case when woo_trials is null then 0 else woo_trials end woo_trials, 

case when recurly_sales_count is null then 0 else recurly_sales_count end recurly_sales_count, 
case when woo_sales_count is null then 0 else woo_sales_count end woo_sales_count, 

case when recurly_cogs is null then 0 else recurly_cogs end recurly_cogs, 
case when woo_cogs is null then 0 else woo_cogs end woo_cogs

FROM mp_campaigns
LEFT JOIN all_txn
ON mp_campaigns.name = all_txn.aff_sub  -- joinning based on campaign's name 
left join recurly_email 
on recurly_email.recurly_aff_sub = mp_campaigns.name
left join woocommerce_email 
on woocommerce_email.woo_aff_sub = mp_campaigns.name 
order by sent_at desc) 

select distinct * from mp_sales 


