SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.recipe_id') as recipe_id,
    JSON_EXTRACT_SCALAR(event_properties, '$.action') as action, 
    JSON_EXTRACT_SCALAR(event_properties, '$.value') as value, 
  FROM test_events.recipe_interactions
)