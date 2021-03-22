SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.old_recipe_id') as old_recipe_id,
    JSON_EXTRACT_SCALAR(event_properties, '$.new_recipe_id') as new_recipe_id, 
  FROM test_events.swap_recipe
)