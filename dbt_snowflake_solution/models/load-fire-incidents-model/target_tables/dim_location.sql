
WITH locations AS (
    SELECT DISTINCT
        address AS location_key,
        city,
        zipcode,
        battalion,
        station_area,
        supervisor_district,
        neighborhood_district,
        box
    FROM {{ ref('stg_fire_incidents') }}
)

SELECT * FROM locations;
