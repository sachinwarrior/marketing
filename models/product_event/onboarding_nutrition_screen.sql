{{ config (
    materialized="table"
)}}
SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.screen_sequence') as screen_sequence,
    JSON_EXTRACT_SCALAR(event_properties, '$.time') as time,
    JSON_EXTRACT_SCALAR(event_properties, '$.action') as action, 
    JSON_EXTRACT_SCALAR(user_properties, '$.email') as email,
    ROW_NUMBER() OVER() as event_id 
  FROM test_events.onboarding_nutrition_screen
)