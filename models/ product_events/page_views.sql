SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.view') as view,
    JSON_EXTRACT_SCALAR(event_properties, '$.section') as section,
  FROM test_events.page_views
)