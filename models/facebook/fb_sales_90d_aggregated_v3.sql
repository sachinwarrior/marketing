{{ config (
    materialized="table"
)}}

SELECT
    CAST(CAST(_paid_date AS DATETIME) AS DATE) AS order_date --USING HACKY WORKAROUND FOR THIS BECAUSE SOURCE DATA IS INPROPERLY LABELED AS UTC. THIS "SHOULD" CONTINUE TO WORK AFTER SOURCE FIELD IS FIXED.
    ,
    mktg_ad_id AS ad_id,
    COUNT(CAST(trial_take AS INT64)) AS trials,
    COUNT(DISTINCT id) AS sales_count,
    ROUND(SUM(CAST(_order_total AS FLOAT64)),2) AS sales,
    ROUND(SUM(CAST(_wc_cog_order_total_cost AS FLOAT64)),2) AS cogs 
FROM
    {{ ref('transactions_dbt_test') }}
WHERE
    CAST(_paid_date AS TIMESTAMP) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24*90 HOUR)
GROUP BY
    order_date,
    ad_id
ORDER BY
    order_date DESC
