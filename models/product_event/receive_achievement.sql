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
    JSON_EXTRACT_SCALAR(user_properties, '$.email') as email,
  FROM test_events.receive_achievement
)