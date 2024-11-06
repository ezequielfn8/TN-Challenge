
WITH damage_outcome AS (
    SELECT DISTINCT
        incident_number,
        estimated_property_loss,
        estimated_contents_loss,
        fire_fatalities,
        fire_injuries,
        civilian_fatalities,
        civilian_injuries
    FROM {{ ref('stg_fire_incidents') }}
)

SELECT * FROM damage_outcome;
