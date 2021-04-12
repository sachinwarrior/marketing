{{ config (
    materialized="table"
)}}

-- join the customer_id and the corresponding post_parent id to get the aff_id and mktg_custom
with aff_id_table as ( 
select * except(_customer_user, id, post_parent) 
from (select _customer_user as user_id, id, mktg_affiliate, mktg_custom_1, mktg_custom_2 from dbt_marketing.transactions_dbt where cast(_paid_date as timestamp) >= '2020-10-25' and post_status in ('wc-completed','wc-processing')) transactions 
inner join (select _customer_user, post_parent from funnels_processed.subscription_pivot where _sdc_deleted_at is null) subscription 
on transactions.user_id = subscription._customer_user and subscription.post_parent = transactions.id),

-- getting member who successfully buy the subscription 
member_trans as (select * 
from (select _customer_user, cast(_order_total as numeric) as order_total, CAST(CAST(_paid_date AS DATETIME) AS DATE) AS order_date
      from {{ref('transactions_dbt')}}
    where cast(_paid_date as timestamp) >= '2020-10-25' and post_status in ('wc-completed','wc-processing') and products_purchased like '%Warrior Made Tribe%') transactions
left join aff_id_table 
on aff_id_table.user_id = transactions._customer_user 
where user_id is not null
order by order_date desc), 

-- grouping the total revenue of subscription per day
membership_revenue as (select order_date as order_date_trans,  mktg_affiliate as mktg_affiliate_trans, 
    mktg_custom_1 as mktg_custom_1a, mktg_custom_2 as mktg_custom_2a, sum(order_total) as order_total
from member_trans group by order_date, mktg_affiliate, mktg_custom_1, mktg_custom_2), 

-- getting the number of trials per day
trials_table as ((select CAST(CAST(_paid_date AS DATETIME) AS DATE) AS order_date_jb, mktg_affiliate as affiliate, mktg_custom_1 as mktg_custom_1b, mktg_custom_2 as mktg_custom_2b, 
        COUNT(CAST(trial_take AS INT64)) AS trials, 
        from {{ref('transactions_dbt')}} where  cast(_paid_date as timestamp) >= '2020-11-01' and post_status in ('wc-completed','wc-processing') 
        group by order_date_jb, affiliate, mktg_custom_1,mktg_custom_2)),

woo_member_rev as (select 
 case when order_date_trans is null and order_date_jb is not null then order_date_jb
 when order_date_trans is not null and order_date_jb is null then order_date_trans
 else order_date_trans
 end order_date, 
 case when mktg_affiliate_trans is not null and affiliate is null then mktg_affiliate_trans
 when mktg_affiliate_trans is  null and affiliate is not null then affiliate
 else mktg_affiliate_trans
 end mktg_affiliate,
 case when mktg_custom_1a is not null and mktg_custom_1b is null then mktg_custom_1a
 when mktg_custom_1a is  null and mktg_custom_1b is not null then mktg_custom_1b
 else mktg_custom_1a
 end mktg_custom_1,
 case when mktg_custom_2a is not null and mktg_custom_2b is null then mktg_custom_2a
 when mktg_custom_2a is  null and mktg_custom_2b is not null then mktg_custom_2b
 else mktg_custom_2a
 end mktg_custom_2,
 case when order_total is not null then order_total
 else 0 
 end order_total,
 case when trials is not null then trials 
 else 0 
 end trials
from  membership_revenue 
full outer join  trials_table
on trials_table.order_date_jb=membership_revenue.order_date_trans and trials_table.affiliate=membership_revenue.mktg_affiliate_trans and trials_table.mktg_custom_1b= membership_revenue.mktg_custom_1a and trials_table.mktg_custom_2b= membership_revenue.mktg_custom_2a
order by order_date desc)
select order_date, sum(trials) as trials, sum(order_total) as order_total from woo_member_rev group by order_date order by order_date desc





