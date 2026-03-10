-- 1. Create schema
USE DATABASE MARKET_INSIGHTS_DB;
USE SCHEMA RAW_DATA_SCHEMA;

-- 2. Create the Window to S3 (The Stage)
CREATE OR REPLACE STAGE market_data_stage
  URL = 's3://market-insights-data-lake/raw-data/'
  STORAGE_INTEGRATION = s3_market_data_integration;

-- 3. THE FINAL LIST COMMAND

LIST @market_data_stage;
