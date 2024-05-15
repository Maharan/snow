WITH data_table AS 
    (
        SELECT * FROM {{ref('stg_athlete_medals')}}
)

SELECT 
    country, 
    olympic_year, 
    SUM(medals_won_country_year) OVER (PARTITION BY country ORDER BY olympic_year) cumulative_medals_won_by_country
FROM (
    SELECT 
        country, 
        olympic_year, 
        SUM(medal_weight_321) as medals_won_country_year
    FROM data_table
    GROUP BY COUNTRY, olympic_year
    ORDER BY country, olympic_year
) subq
ORDER BY country, olympic_year