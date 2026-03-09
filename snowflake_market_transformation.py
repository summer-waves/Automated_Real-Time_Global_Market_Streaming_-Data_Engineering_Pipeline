import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, replace, cast, when, regexp_replace, avg, nullif, lit

def main(session: snowpark.Session):
    # 1. Grab your raw data
    df = session.table("MARKET_INSIGHTS_DB.RAW_DATA_SCHEMA.GLOBAL_MARKET_DATA")
    
    # 2. Advanced Cleaning Logic
    # Scrubs whitespace, handles K/M/B, and converts empty strings to NULL
    df_clean = df.with_column("PRICE_NUMERIC", 
        when(col("LAST_PRICE").like("%K"), cast(nullif(regexp_replace(replace(col("LAST_PRICE"), "K", ""), r'[^0-9.]', ''), lit('')), "FLOAT") * 1000)
        .when(col("LAST_PRICE").like("%M"), cast(nullif(regexp_replace(replace(col("LAST_PRICE"), "M", ""), r'[^0-9.]', ''), lit('')), "FLOAT") * 1000000)
        .when(col("LAST_PRICE").like("%B"), cast(nullif(regexp_replace(replace(col("LAST_PRICE"), "B", ""), r'[^0-9.]', ''), lit('')), "FLOAT") * 1000000000)
        .otherwise(cast(nullif(regexp_replace(col("LAST_PRICE"), r'[^0-9.]', ''), lit('')), "FLOAT"))
    )

    # 3. Calculate the Final Statistic
    # We return this as a DataFrame so the worksheet can display it
    final_result_df = df_clean.select(
        avg(col("PRICE_NUMERIC")).alias("AVERAGE_GLOBAL_STOCK_PRICE")
    )
    
    return final_result_df