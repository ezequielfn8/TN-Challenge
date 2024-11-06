
WITH incident_types AS (
    SELECT DISTINCT
        call_type AS incident_type_key,
        primary_situation,
        mutual_aid
    FROM {{ ref('stg_fire_incidents') }}
)

SELECT * FROM incident_types;
