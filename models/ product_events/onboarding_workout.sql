SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.status') as status,
    JSON_EXTRACT_SCALAR(event_properties, '$.complete_time') as complete_time
  FROM test_events.onboarding_workout
)