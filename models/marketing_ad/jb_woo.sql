-- grouping woo transactions data to create revenue data grouping by aff_id and utm_campaign and funnel 
{{ config (
    materialized="table"
)}}
SELECT
    CAST(CAST(post_date AS DATETIME) AS DATE) AS order_date,
    mktg_custom_1 AS mktg_custom_1,
    mktg_custom_2 AS mktg_custom_2,
    COUNT(CAST(trial_take AS INT64)) AS trials,
    COUNT(CAST(frontend_purchase AS INT64)) AS sales_count,
    ROUND(SUM(CAST(_order_total AS FLOAT64)),2) AS sales,
    ROUND(SUM(CAST(_wc_cog_order_total_cost AS FLOAT64)),2) AS cogs,
    mktg_affiliate AS affiliate,
    products_purchased AS products,
    utm_campaign,
    case when lower(utm_campaign) like '%keto-carbs%'  then 'keto carbs'
    when lower(utm_campaign) like '%abs%' then 'abs' 
    when lower(utm_campaign) like '%go2protein%' or utm_campaign like 'go2protien' then 'go2protein'
    when lower(utm_campaign) like '%keto-sweet%' then 'keto sweet'
    end as funnel, 
    case when _shipping_country is not null then _shipping_country
    else _billing_country 
    end as country
FROM
        {{ref('transactions_dbt')}}
WHERE
    CAST(post_date AS TIMESTAMP) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24*90 HOUR) and post_status in ('wc-completed','wc-processing') 
GROUP BY
    order_date,
    affiliate,
    mktg_custom_1,
    mktg_custom_2,
    products, utm_campaign, country 
ORDER BY
    order_date DESC 