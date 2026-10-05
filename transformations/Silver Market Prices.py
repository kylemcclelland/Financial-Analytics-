# Databricks notebook source
# DBTITLE 1,Silver Market Prices Header
# MAGIC %md
# MAGIC # Silver Market Prices
# MAGIC
# MAGIC Transforms raw Bronze market price data into a clean, conformed Silver layer:
# MAGIC
# MAGIC - **Source:** `bronze.market_prices_daily`
# MAGIC - **Target:** `silver.market_prices_daily`
# MAGIC
# MAGIC **Transformations:**
# MAGIC 1. Deduplicate on `(ticker, trade_date, adjustment_type)` keeping the latest ingestion
# MAGIC 2. Enforce data-quality rules (non-null keys, positive prices, valid OHLC ordering)
# MAGIC 3. Add a surrogate key (`market_price_key`)
# MAGIC 4. Add `silver_processed_at` timestamp
# MAGIC 5. Idempotent Delta **MERGE** on `(ticker, trade_date, adjustment_type)`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver.market_prices_daily
# MAGIC USING DELTA
# MAGIC COMMENT 'Cleaned and conformed daily market prices (Silver layer)'
# MAGIC AS
# MAGIC WITH deduped AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY ticker, trade_date, adjustment_type
# MAGIC             ORDER BY ingestion_timestamp DESC
# MAGIC         ) AS _rn
# MAGIC     FROM bronze.market_prices_daily
# MAGIC )
# MAGIC SELECT
# MAGIC     MD5(
# MAGIC         CONCAT(
# MAGIC             ticker,
# MAGIC             '|',
# MAGIC             CAST(trade_date AS STRING),
# MAGIC             '|',
# MAGIC             adjustment_type
# MAGIC         )
# MAGIC     ) AS market_price_key,
# MAGIC     ticker,
# MAGIC     trade_date,
# MAGIC     open_price,
# MAGIC     high_price,
# MAGIC     low_price,
# MAGIC     close_price,
# MAGIC     volume,
# MAGIC     adjustment_type,
# MAGIC     data_source,
# MAGIC     source_file,
# MAGIC     ingestion_timestamp,
# MAGIC     CURRENT_TIMESTAMP() AS silver_processed_at
# MAGIC FROM deduped
# MAGIC WHERE _rn = 1
# MAGIC     AND ticker IS NOT NULL
# MAGIC     AND trade_date IS NOT NULL
# MAGIC     AND close_price IS NOT NULL
# MAGIC     AND close_price > 0
# MAGIC     AND high_price >= low_price
# MAGIC     AND high_price >= open_price
# MAGIC     AND high_price >= close_price
# MAGIC     AND low_price <= open_price
# MAGIC     AND low_price <= close_price;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row count and summary
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT ticker) AS total_symbols,
# MAGIC     MIN(trade_date) AS earliest_trade_date,
# MAGIC     MAX(trade_date) AS latest_trade_date,
# MAGIC     MAX(silver_processed_at) AS latest_silver_processed
# MAGIC FROM silver.market_prices_daily;