-- Average Response Time Analysis by Battalion and Time of Day

WITH response_time AS (
    SELECT
        f.district_key,
        d.year,
        d.month,
        d.day_of_week,
        d.hour AS incident_hour,
        b.battalion,  -- Get battalion information from dim_battalion
        AVG(f.response_time_seconds) AS avg_response_time_seconds,
        COUNT(f.incident_number) AS total_incidents
    FROM FACT_FIRE_INCIDENTS f
    JOIN DIM_DATE d ON f.date_key = d.date_key
    JOIN DIM_DISTRICT l ON f.district_key = l.district_key
    JOIN DIM_BATTALION b ON f.battalion_key = b.battalion_key  -- Join with battalion dimension
    WHERE f.response_time_seconds IS NOT NULL
    GROUP BY
        f.district_key, d.year, d.month, d.day_of_week, d.hour, b.battalion
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
ORDER BY
