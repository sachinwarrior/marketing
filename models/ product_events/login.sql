SELECT
  * 
FROM(
  SELECT 
    * except(event_properties),
    JSON_EXTRACT_SCALAR(event_properties, '$.login_method') as login_method,
  FROM test_events.login
)