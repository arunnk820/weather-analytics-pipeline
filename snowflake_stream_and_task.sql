-- ============================================================
-- Snowflake Stream + Task: Auto-flatten raw JSON weather data
-- Source : WEATHER_DB.RAW.RAW_WEATHER_DATA  (data VARIANT, ingested_at TIMESTAMP_NTZ)
-- Target : WEATHER_DB.ANALYTICS.WEATHER_FLATTENED
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- 1. Create the target schema (if it doesn't exist)
CREATE SCHEMA IF NOT EXISTS WEATHER_DB.ANALYTICS;

-- 2. Create the target flattened table
CREATE TABLE IF NOT EXISTS WEATHER_DB.ANALYTICS.WEATHER_FLATTENED (
    city               STRING,
    temperature        FLOAT,
    humidity           FLOAT,
    wind_speed         FLOAT,
    weather_condition  STRING,
    ingested_at        TIMESTAMP_NTZ
);

-- 3. Create a Stream on the raw table to capture new inserts
CREATE OR REPLACE STREAM WEATHER_DB.RAW.RAW_WEATHER_STREAM
    ON TABLE WEATHER_DB.RAW.RAW_WEATHER_DATA
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new rows inserted into RAW_WEATHER_DATA';

-- 4. Create a Task that runs every minute and flattens new data
CREATE OR REPLACE TASK WEATHER_DB.RAW.FLATTEN_WEATHER_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Flattens new JSON weather data from the stream into ANALYTICS.WEATHER_FLATTENED'
WHEN
    SYSTEM$STREAM_HAS_DATA('WEATHER_DB.RAW.RAW_WEATHER_STREAM')
AS
    INSERT INTO WEATHER_DB.ANALYTICS.WEATHER_FLATTENED (
        city,
        temperature,
        humidity,
        wind_speed,
        weather_condition,
        ingested_at
    )
    SELECT
        data:name::STRING                       AS city,
        data:main.temp::FLOAT                   AS temperature,
        data:main.humidity::FLOAT               AS humidity,
        data:wind.speed::FLOAT                  AS wind_speed,
        data:weather[0].description::STRING     AS weather_condition,
        ingested_at                             AS ingested_at
    FROM WEATHER_DB.RAW.RAW_WEATHER_STREAM;

-- 5. Resume the task (tasks are created in a suspended state by default)
ALTER TASK WEATHER_DB.RAW.FLATTEN_WEATHER_TASK RESUME;

-- ============================================================
-- Verification queries (run these after data flows in)
-- ============================================================

-- Check stream status
-- SELECT SYSTEM$STREAM_HAS_DATA('WEATHER_DB.RAW.RAW_WEATHER_STREAM');

-- Preview flattened data
-- SELECT * FROM WEATHER_DB.ANALYTICS.WEATHER_FLATTENED ORDER BY ingested_at DESC LIMIT 10;

-- Check task run history
-- SELECT *
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'FLATTEN_WEATHER_TASK'))
--   ORDER BY SCHEDULED_TIME DESC
--   LIMIT 10;
