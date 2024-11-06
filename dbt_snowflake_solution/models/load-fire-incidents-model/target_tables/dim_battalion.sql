
WITH battalion_data AS (
    SELECT DISTINCT
        battalion AS battalion_key,
        station_area,
        supervisor_district AS district,
        neighborhood_district
    FROM {{ ref('stg_fire_incidents') }}
    WHERE battalion IS NOT NULL
)

SELECT * FROM battalion_data;
