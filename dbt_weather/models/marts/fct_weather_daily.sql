-- fct_weather_daily.sql
-- Daily weather aggregation by city
-- Source: stg_weather (staging model)

WITH daily_stats AS (

    SELECT
        city,
        DATE(ingested_at)                          AS observation_date,

        -- Temperature aggregates (Celsius)
        ROUND(AVG(temperature_celsius), 2)         AS avg_temperature,
        ROUND(MIN(temperature_celsius), 2)         AS min_temperature,
        ROUND(MAX(temperature_celsius), 2)         AS max_temperature,

        -- Humidity aggregate
        ROUND(AVG(humidity_pct), 2)                AS avg_humidity,

        -- Wind speed aggregate (km/h)
        ROUND(AVG(wind_speed_kmh), 2)              AS avg_wind_speed,

        -- Row count for the day
        COUNT(*)                                   AS record_count

    FROM {{ ref('stg_weather') }}
    GROUP BY city, DATE(ingested_at)

)

SELECT * FROM daily_stats
ORDER BY city, observation_date
