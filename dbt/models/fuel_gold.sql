{{ config(materialized='table') }}

SELECT
    state,
    CEIL(SUM(diesel_gallons)) AS total
FROM fuel_data 
GROUP BY state 
ORDER BY state 