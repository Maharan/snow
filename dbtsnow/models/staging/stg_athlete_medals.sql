WITH raw_data as (
    select * from {{source('demo','raw_fact')}}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['athlete_full_name', 'event']) }} as athlete_medal_id,
    yr::INTEGER as olympic_year,
    city as olympic_city,
    season as olympic_type,
    sport,
    CASE SUBSTR(event, LENGTH(event), 1)
        WHEN 'M' THEN SUBSTR(event, 0, LENGTH(event) - 2)
        WHEN 'W' THEN SUBSTR(event, 0, LENGTH(event) - 2)
        ELSE SUBSTR(event, 0, LENGTH(event) - 1)
    END as event_name,
    event as event_name_and_gender,
    CASE SUBSTR(event, LENGTH(event), 1)
        WHEN 'M' THEN 'Male'
        WHEN 'W' THEN 'Female'
        ELSE 'Unisex'
    END AS Gender,
    country_code as country,
    athlete_full_name,
    SPLIT_PART(athlete_full_name, ' ', 0) as first_name,
    SPLIT_PART(athlete_full_name, ' ', -1) as last_name,
    medal,
    CASE medal
        WHEN 'Gold' THEN 3
        WHEN 'Silver' THEN 2
        WHEN 'Bronze' THEN 1
    END AS Medal_Weight_321,
    TIME_ADDED::TIMESTAMP_NTZ as time_added_into_sf_dwh
    
FROM raw_data