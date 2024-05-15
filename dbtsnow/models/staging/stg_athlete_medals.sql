{{
    config (
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}

WITH raw_data as (
    select * from {{source('demo','raw_fact')}}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['athlete_full_name', 'event', 'yr']) }} as athlete_event_year_id,
    yr::INTEGER as olympic_year,
    city as olympic_city,
    season as olympic_type,
    sport,
    CASE SUBSTR(TRIM(event), LENGTH(TRIM(event)), 1)
        WHEN 'M' THEN CONCAT(SPORT, ' - ', SUBSTR(TRIM(event), 0, LENGTH(TRIM(event)) - 2))
        WHEN 'W' THEN CONCAT(SPORT, ' - ', SUBSTR(TRIM(event), 0, LENGTH(TRIM(event)) - 2))
        ELSE CONCAT(SPORT, ' - ', SUBSTR(TRIM(event), 0, LENGTH(TRIM(event)) - 1))
    END as event_name,
    CONCAT(SPORT, ' - ', SUBSTR(TRIM(event), 0, LENGTH(TRIM(event)))) as event_name_and_gender,
    CASE SUBSTR(TRIM(event), LENGTH(TRIM(event)), 1)
        WHEN 'M' THEN 'Male'
        WHEN 'W' THEN 'Female'
        ELSE 'Unisex'
    END AS Gender,
    country_code as country,
    TRIM(athlete_full_name) as athlete_full_name,
    SPLIT_PART(TRIM(athlete_full_name), ' ', 0) as first_name,
    SPLIT_PART(TRIM(athlete_full_name), ' ', -1) as last_name,
    medal,
    CASE medal
        WHEN 'Gold' THEN 3
        WHEN 'Silver' THEN 2
        WHEN 'Bronze' THEN 1
    END AS Medal_Weight_321,
    TIME_ADDED::TIMESTAMP_NTZ as time_added_into_sf_dwh

FROM raw_data

{% if is_incremental() %}
    WHERE TIME_ADDED::TIMESTAMP_NTZ > (select max(time_added_into_sf_dwh) from {{ this }})
{% endif %}