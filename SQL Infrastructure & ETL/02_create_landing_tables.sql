-- 1. Use project home
USE DATABASE MARKET_INSIGHTS_DB;
USE SCHEMA RAW_DATA_SCHEMA;

-- 2. Sniff the headers
-
SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION=>'@market_data_stage/NASDAQ_Stock.csv',
    FILE_FORMAT=>'global_stock_csv'
  )

);
