{{ config (
    materialized="table"
)}}

-- Grouping jb_woo with affiliate = 1
SELECT 
    order_date as date, 
    mktg_custom_1, utm_campaign, 
    funnel, 
    sum(sales) as sales, 
    sum(trials) as trials, 
    sum(cogs) as cogs, 
    sum(sales_count) as sales_count 
FROM {{ ref('jb_woo') }} 
where affiliate='1' 
group by order_date, mktg_custom_1, utm_campaign, funnel