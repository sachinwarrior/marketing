{{ config (
    materialized="table"
)}}

-- join the customer_id and the corresponding post_parent id to get the aff_id and mktg_custom
with aff_id_table as (select * except(_customer_user,  id, post_parent) 
from (select _customer_user as user_id, id, products_purchased, mktg_affiliate, mktg_custom_1, mktg_custom_2 from {{ref('transactions_dbt')}} where cast(_paid_date as timestamp) >= '2020-10-25' and post_status in ('wc-completed','wc-processing')) transactions 
inner join (select _customer_user, post_parent from funnels_processed.subscription_pivot where _sdc_deleted_at is null) subscription 
on transactions.user_id = subscription._customer_user and subscription.post_parent = transactions.id),

-- getting the number of trials per day
membership as (select * except (_customer_user) 
from (select distinct * from aff_id_table) aff_id_table
left join (select _customer_user, trial_take from {{ref('transactions_dbt')}} where trial_take is not null and post_status in ('wc-completed','wc-processing')) trials_table
on aff_id_table.user_id = trials_table._customer_user), 

-- getting member who successfully buy the subscription 
member_trans as (select * 
from membership
left join (select _customer_user, cast(_order_total as numeric) as order_total, CAST(CAST(_paid_date AS DATETIME) AS DATE) AS order_rebill_date
      from {{ref('transactions_dbt')}}
    where cast(_paid_date as timestamp) >= '2020-10-25' and post_status in ('wc-completed','wc-processing') and products_purchased like '%Warrior Made Tribe%') transactions 
on membership.user_id = transactions._customer_user 
where user_id is not null 
order by order_rebill_date desc)


select * except (_customer_user,trial_take), 
case when trial_take is null and order_total is not null then '1'
else trial_take 
end trial_take 
from member_trans where trial_take is null






