{{ config (
    materialized="table"
)}}
SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.login_method') as login_method,
    JSON_EXTRACT_SCALAR(user_properties, '$.email') as email,
  FROM test_events.login
)