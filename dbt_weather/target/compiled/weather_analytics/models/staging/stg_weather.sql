-- stg_weather.sql
-- Staging model: cleans, trims, and converts imperial units to metric
-- Source: WEATHER_DB.ANALYTICS.WEATHER_FLATTENED

WITH source AS (

    SELECT * FROM WEATHER_DB.ANALYTICS.weather_flattened

),

cleaned AS (

    SELECT
        -- Normalise city name casing
        INITCAP(TRIM(city))                          AS city,

        -- Convert Fahrenheit → Celsius
        ROUND((temperature - 32) * 5.0 / 9.0, 2)    AS temperature_celsius,

        -- Humidity stays as-is (already a percentage)
        ROUND(humidity, 2)                            AS humidity_pct,

        -- Convert mph → km/h
        ROUND(wind_speed * 1.60934, 2)               AS wind_speed_kmh,

        -- Standardise weather condition text
        LOWER(TRIM(weather_condition))                AS weather_condition,

        -- Pass through ingestion timestamp
        ingested_at

    FROM source

)

SELECT * FROM cleaned