WITH data_table AS 
    (
        SELECT * FROM {{ref('stg_athlete_medals')}}
)

SELECT 
    country, 
    SUM(MEDAL_WEIGHT_321) AS collective_points
FROM TF_DEMO.STAGE.STG_ATHLETE_MEDALS
GROUP BY country
ORDER BY collective_points DESC