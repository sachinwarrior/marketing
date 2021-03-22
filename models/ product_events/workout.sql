SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.status') as status,
  FROM test_events.workout
)