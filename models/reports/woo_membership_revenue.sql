{{ config (
    materialized="table"
)}}

with transactions as (select order_date_trans, mktg_affiliate as mktg_affiliate_trans, mktg_custom_1 as mktg_custom_1a, mktg_custom_2 as mktg_custom_2a, 
    sum(cast(_order_total as numeric)) as order_total, 
    from (select *, CAST(CAST(_paid_date AS DATETIME) AS DATE) AS order_date_trans from {{ref('transactions_dbt')}}) 
    where cast(_paid_date as timestamp) >= '2020-11-01' and post_status in ('wc-completed','wc-processing') and products_purchased like '%Warrior Made Tribe%'
    group by order_date_trans, mktg_affiliate, mktg_custom_1, mktg_custom_2 order by order_date_trans desc),

trials_table as ((select CAST(CAST(_paid_date AS DATETIME) AS DATE) AS order_date_jb, mktg_affiliate as affiliate, mktg_custom_1 as mktg_custom_1b, mktg_custom_2 as mktg_custom_2b, 
        COUNT(CAST(trial_take AS INT64)) AS trials, 
        from {{ref('transactions_dbt')}} where  cast(_paid_date as timestamp) >= '2020-11-01' and post_status in ('wc-completed','wc-processing') 
        group by order_date_jb, affiliate, mktg_custom_1,mktg_custom_2))

select 
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
 order_total, trials
from  transactions 
full outer join  trials_table

on trials_table.order_date_jb=transactions.order_date_trans and trials_table.affiliate=transactions.mktg_affiliate_trans
order by order_date





