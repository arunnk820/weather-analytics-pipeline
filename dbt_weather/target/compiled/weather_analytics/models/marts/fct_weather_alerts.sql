-- fct_weather_alerts.sql
-- Flags weather observations that exceed safety thresholds
-- Source: stg_weather (staging model)
--
-- Thresholds:
--   Temperature  > 35 °C
--   Wind speed   > 50 km/h
--   Humidity     > 90 %

WITH alerts AS (

    SELECT
        city,
        temperature_celsius,
        humidity_pct,
        wind_speed_kmh,
        weather_condition,
        ingested_at,

        -- Individual alert flags
        CASE WHEN temperature_celsius > 35  THEN TRUE ELSE FALSE END   AS is_high_temp,
        CASE WHEN wind_speed_kmh      > 50  THEN TRUE ELSE FALSE END   AS is_high_wind,
        CASE WHEN humidity_pct         > 90  THEN TRUE ELSE FALSE END   AS is_high_humidity,

        -- Combined alert flag
        CASE
            WHEN temperature_celsius > 35
              OR wind_speed_kmh      > 50
              OR humidity_pct         > 90
            THEN TRUE
            ELSE FALSE
        END                                                             AS has_alert

    FROM WEATHER_DB.ANALYTICS.stg_weather

)

SELECT * FROM alerts
WHERE has_alert = TRUE
ORDER BY ingested_at DESC