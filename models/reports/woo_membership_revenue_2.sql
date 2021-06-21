{{ config (
    materialized="table"
)}}
with member_transaction as (select cast(cast(_paid_date as datetime) as date) as order_date, 
   case  when utm_campaign like '%keto-carbs%'  then 'keto carbs'
  when utm_campaign like '%abs%' then 'abs' 
    when utm_campaign like '%go2protein%' or utm_campaign like 'go2protien' then 'go2protein'
    when utm_campaign like '%keto-sweet%' then 'keto sweet'
    end as funnel,
    mktg_affiliate, mktg_custom_1, mktg_custom_2, sum(cast(_order_total as numeric)) as member_revenue, 
    from {{ref('transactions_dbt')}}
    where post_status in ('wc-completed','wc-processing') and lower(products_purchased) like '%warrior%'
    group by order_date, funnel, mktg_affiliate, mktg_custom_1, mktg_custom_2)


select * from member_transaction 