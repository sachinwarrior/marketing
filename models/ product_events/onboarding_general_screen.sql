SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.skip_page') as skip_page,
  FROM test_events.onboarding_general_screen
)