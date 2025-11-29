{{ config(
    materialized='table'
) }}


select mg."State", mg.mileage as total_mileage, fg.total as total_fuel
from {{ ref('mileage_gold') }} mg
inner join analytics.fuel_gold fg
on mg."State" = fg.state
