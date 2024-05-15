WITH data_table AS 
    (
        SELECT * FROM {{ref('stg_athlete_medals')}}
)

SELECT 
    event_name_and_gender, COUNT(DISTINCT olympic_year) as present_at_olympics_for_years 
FROM TF_DEMO.STAGE.STG_ATHLETE_MEDALS
GROUP BY event_name_and_gender
ORDER BY present_at_olympics_for_years DESC