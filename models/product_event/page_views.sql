{{ config (
    materialized="table"
)}}
SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.view') as view,
    JSON_EXTRACT_SCALAR(event_properties, '$.section') as section,
    JSON_EXTRACT_SCALAR(user_properties, '$.email') as email,
  FROM test_events.page_views
)