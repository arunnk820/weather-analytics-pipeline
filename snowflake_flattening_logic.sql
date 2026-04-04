-- Flattening JSON weather data into tabular format
INSERT INTO WEATHER_DB.ANALYTICS.STG_WEATHER
SELECT
    DATA:location:name::STRING as city,
    DATA:current:temp_c::FLOAT as temp_c,
    DATA:current:condition:text::STRING as condition,
    INGESTED_AT
FROM WEATHER_DB.RAW.RAW_WEATHER_DATA_STREAM;
