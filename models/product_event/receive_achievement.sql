{{ config (
    materialized="table"
)}}
SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.name') as name,
    JSON_EXTRACT_SCALAR(event_properties, '$.share_method') as share_method,
  FROM test_events.receive_achievement
)