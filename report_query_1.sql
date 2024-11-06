-- Average Response Time Analysis by Battalion and Time of Day

WITH response_time AS (
    SELECT
        f.location_key,
        d.year,
        d.month,
        d.day_of_week,
        d.hour AS incident_hour,
        l.battalion,
        AVG(f.response_time_seconds) AS avg_response_time_seconds,
        COUNT(f.incident_number) AS total_incidents
    FROM FACT_FIRE_INCIDENTS f
    JOIN DIM_DATE d ON f.date_key = d.date_key
    JOIN DIM_LOCATION l ON f.location_key = l.location_key
    WHERE f.response_time_seconds IS NOT NULL
    GROUP BY
        f.location_key, d.year, d.month, d.day_of_week, d.hour, l.battalion
)

SELECT
    battalion,
    incident_hour,
    day_of_week,
    AVG(avg_response_time_seconds) AS avg_response_time_seconds,
    SUM(total_incidents) AS total_incidents,
    CASE
        WHEN AVG(avg_response_time_seconds) < 300 THEN 'Excellent'
        WHEN AVG(avg_response_time_seconds) BETWEEN 300 AND 600 THEN 'Good'
        ELSE 'Needs Improvement'
    END AS performance_category
FROM response_time
GROUP BY battalion, incident_hour, day_of_week
ORDER BY battalion, incident_hour, day_of_week;
