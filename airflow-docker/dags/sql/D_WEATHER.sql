CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_WEATHER` AS
SELECT DISTINCT
  CAST(dt) YEAR,
  City CITY,
  Country COUNTRY,
  Latitude LATITUDE,
  Longitude LONGITUDE,
  CAST(AverageTemperature AS FLOAT) AVG_TEMPERATURE
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.temp_by_city`
WHERE Country = 'United States'
