
WITH units AS (
    SELECT DISTINCT
        incident_number,
        suppression_units,
        suppression_personnel,
        ems_units,
        ems_personnel,
        other_units,
        other_personnel
    FROM {{ ref('stg_fire_incidents') }}
)

SELECT * FROM units;
