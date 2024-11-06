
WITH dates AS (
    SELECT DISTINCT
        CAST(CAST(incident_date AS DATE) AS STRING) AS date_key, 
        CAST(incident_date AS DATE) AS full_date,
        YEAR(incident_date) AS year,
        QUARTER(incident_date) AS quarter,
        MONTH(incident_date) AS month,
        DAY(incident_date) AS day,
        DAYOFWEEK(incident_date) AS day_of_week,
        CASE 
            WHEN DAYOFWEEK(incident_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type
    FROM {{ ref('stg_fire_incidents') }}
)

SELECT * FROM dates;
