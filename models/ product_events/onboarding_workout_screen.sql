sSELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.screen_sequence') as screen_sequence,
    JSON_EXTRACT_SCALAR(event_properties, '$.time') as time,
    JSON_EXTRACT_SCALAR(event_properties, '$.action') as action
  FROM test_events.onboarding_workout_screen
)