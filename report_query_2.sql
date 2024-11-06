-- Resource Utilization by Incident Type and Damage Outcome

WITH resource_usage AS (
    SELECT
        f.incident_type_key,
        t.primary_situation,
        o.damage_outcome_key,
        d.year,
        SUM(f.suppression_units) AS total_suppression_units,
        SUM(f.suppression_personnel) AS total_suppression_personnel,
        SUM(f.ems_units) AS total_ems_units,
        SUM(f.ems_personnel) AS total_ems_personnel,
        SUM(f.other_units) AS total_other_units,
        SUM(f.other_personnel) AS total_other_personnel,
        AVG(f.estimated_property_loss) AS avg_property_loss,
        AVG(f.estimated_contents_loss) AS avg_contents_loss,
        SUM(f.fire_fatalities + f.civilian_fatalities) AS total_fatalities
    FROM FACT_FIRE_INCIDENTS f
    JOIN DIM_INCIDENT_TYPE t ON f.incident_type_key = t.incident_type_key
    JOIN DIM_DAMAGE_OUTCOME o ON f.damage_outcome_key = o.damage_outcome_key
    JOIN DIM_DATE d ON f.date_key = d.date_key
    GROUP BY f.incident_type_key, t.primary_situation, o.damage_outcome_key, d.year
)

SELECT
    primary_situation AS incident_type,
    year,
    total_suppression_units,
    total_suppression_personnel,
    total_ems_units,
    total_ems_personnel,
    total_other_units,
    total_other_personnel,
    avg_property_loss,
    avg_contents_loss,
    total_fatalities,
    CASE
        WHEN avg_property_loss > 500000 THEN 'High Loss'
        WHEN avg_property_loss BETWEEN 100000 AND 500000 THEN 'Medium Loss'
        ELSE 'Low Loss'
    END AS loss_category
FROM resource_usage
ORDER BY incident_type, year, loss_category DESC;
