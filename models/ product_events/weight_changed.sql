SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.new_weight.weight') as new_weight,
  FROM test_events.weight_changed
)
