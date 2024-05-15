WITH data_table AS 
    (
        SELECT * FROM {{ref('stg_athlete_medals')}}
)

SELECT 
    *
FROM TF_DEMO.STAGE.STG_ATHLETE_MEDALS
WHERE country = '' or last_name = 'awarded' or FIRST_NAME = '' OR LAST_NAME = '' or medal not in ('Gold', 'Silver', 'Bronze') or OLYMPIC_CITY = '' or olympic_year = '' or sport = ''