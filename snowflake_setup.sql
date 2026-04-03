-- Create the raw landing table for JSON weather data
CREATE OR REPLACE TABLE WEATHER_DB.RAW.RAW_WEATHER_DATA (
    DATA VARIANT,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create an external stage for Azure (to be configured in Airflow)
CREATE OR REPLACE STAGE WEATHER_DB.RAW.WEATHER_AZURE_STAGE
    FILE_FORMAT = (TYPE = 'JSON');
