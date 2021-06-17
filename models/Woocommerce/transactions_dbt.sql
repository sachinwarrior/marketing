{{ config (
    materialized="table"
)}}
-- this is the same table as the funnels_processed.transactions
WITH
  transactions_pivot AS (
    SELECT
      *, 
      case when utm_campaign like '%keto-carbs%'  then 'keto carbs'
      when utm_campaign like '%abs%' then 'abs' 
      when utm_campaign like '%go2protein%' or utm_campaign like 'go2protien' then 'go2protein'
      when utm_campaign like '%keto-sweet%' then 'keto sweet'
      end as funnel,
    FROM
      funnels_processed.transactions_pivot
    ORDER BY
      id ),
  transaction_products AS (
    SELECT
      order_id,
      order_item_type,
      STRING_AGG(order_item_name) AS products_purchased,
    FROM
      `wm-gcp-data.funnels.wp_RsTs_woocommerce_order_items`
    WHERE
      order_item_type = 'line_item'
    GROUP BY
      order_id,
      order_item_type ),
  join_table AS (
    SELECT
      *
    FROM
      transactions_pivot
    LEFT JOIN
      transaction_products
    ON
      transactions_pivot.id = transaction_products.order_id )

SELECT
  *
FROM
  join_table
ORDER BY
  id 

