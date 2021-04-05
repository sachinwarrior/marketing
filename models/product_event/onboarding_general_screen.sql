{{ config (
    materialized="table"
)}}
SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.skip_page') as skip_page,
    JSON_EXTRACT_SCALAR(user_properties, '$.email') as email,
    ROW_NUMBER() OVER() as event_id 
  FROM test_events.onboarding_general_screen
)