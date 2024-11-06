
WITH incident_base AS (
    SELECT
        incident_number,
        CAST(CAST(incident_date AS DATE) AS STRING) AS date_key,
        address AS location_key,
        call_type AS incident_type_key,
        data_as_of,
        data_loaded_at
    FROM {{ ref('stg_fire_incidents') }}
)

SELECT * FROM incident_base;

