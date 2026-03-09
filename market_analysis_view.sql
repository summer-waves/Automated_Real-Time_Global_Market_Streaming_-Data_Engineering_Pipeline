CREATE OR REPLACE VIEW global_market_analysis AS
SELECT 
    ticker,
    company_name,
    -- This logic converts 'K', 'M', and 'B' into actual math-ready numbers
    CASE 
        WHEN last_price ILIKE '%K' THEN CAST(REPLACE(REPLACE(last_price, 'K', ''), ',', '') AS FLOAT) * 1000
        WHEN last_price ILIKE '%M' THEN CAST(REPLACE(REPLACE(last_price, 'M', ''), ',', '') AS FLOAT) * 1000000
        WHEN last_price ILIKE '%B' THEN CAST(REPLACE(REPLACE(last_price, 'B', ''), ',', '') AS FLOAT) * 1000000000
        ELSE TRY_CAST(REPLACE(last_price, ',', '') AS FLOAT)
    END AS price_numeric
FROM global_market_data;

-- Check your new math-ready data
SELECT * FROM global_market_analysis LIMIT 10;