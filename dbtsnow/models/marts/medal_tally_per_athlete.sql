WITH data_table AS 
    (
        SELECT * FROM {{ref('stg_athlete_medals')}}
)

SELECT 
    ATHLETE_FULL_NAME, 
    SUM(MEDAL_WEIGHT_321) AS collective_points
FROM data_table
GROUP BY ATHLETE_FULL_NAME
ORDER BY collective_points DESC