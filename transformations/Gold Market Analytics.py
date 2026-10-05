# Databricks notebook source
# DBTITLE 1,Gold Market Analytics Header
# MAGIC %md
# MAGIC # Gold Market Analytics
# MAGIC
# MAGIC Creates Gold-layer analytical tables from Silver market price data:
# MAGIC
# MAGIC 1. **gold.fact_market_prices** — Fact table linking market prices to the company dimension with surrogate keys
# MAGIC 2. **gold.market_daily_summary** — Daily market aggregations (avg close, total volume, advancers/decliners)
# MAGIC 3. **gold.market_trend_analytics** — Per-ticker rolling analytics (moving averages, daily returns, volatility)
# MAGIC
# MAGIC **Source:** `silver.market_prices_daily`
# MAGIC **Join:** `gold.dim_company` (on ticker)

# COMMAND ----------

# DBTITLE 1,Create gold.fact_market_prices
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.fact_market_prices
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     s.market_price_key,
# MAGIC     d.company_key,
# MAGIC     s.ticker,
# MAGIC     s.trade_date,
# MAGIC     s.open_price,
# MAGIC     s.high_price,
# MAGIC     s.low_price,
# MAGIC     s.close_price,
# MAGIC     s.volume,
# MAGIC     s.adjustment_type,
# MAGIC     s.data_source,
# MAGIC     s.ingestion_timestamp,
# MAGIC     s.silver_processed_at,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.market_prices_daily s
# MAGIC LEFT JOIN gold.dim_company d
# MAGIC     ON s.ticker = d.ticker
# MAGIC     AND d.is_active = TRUE;

# COMMAND ----------

# DBTITLE 1,Create gold.market_daily_summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.market_daily_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     trade_date,
# MAGIC     COUNT(*) AS symbols_traded,
# MAGIC     AVG(close_price) AS avg_close_price,
# MAGIC     SUM(volume) AS total_volume,
# MAGIC     SUM(CASE WHEN close_price > open_price THEN 1 ELSE 0 END) AS advancers,
# MAGIC     SUM(CASE WHEN close_price < open_price THEN 1 ELSE 0 END) AS decliners,
# MAGIC     SUM(CASE WHEN close_price = open_price THEN 1 ELSE 0 END) AS unchanged,
# MAGIC     MAX(high_price) AS day_high,
# MAGIC     MIN(low_price) AS day_low,
# MAGIC     AVG(volume) AS avg_volume,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.market_prices_daily
# MAGIC GROUP BY trade_date;

# COMMAND ----------

# DBTITLE 1,Create gold.market_trend_analytics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.market_trend_analytics
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH priced AS (
# MAGIC     SELECT
# MAGIC         ticker,
# MAGIC         trade_date,
# MAGIC         close_price,
# MAGIC         volume,
# MAGIC         -- Daily return: (close - prev_close) / prev_close
# MAGIC         (close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date))
# MAGIC             / LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date) AS daily_return,
# MAGIC         -- 20-day simple moving average
# MAGIC         AVG(close_price) OVER (
# MAGIC             PARTITION BY ticker ORDER BY trade_date
# MAGIC             ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
# MAGIC         ) AS sma_20,
# MAGIC         -- 50-day simple moving average
# MAGIC         AVG(close_price) OVER (
# MAGIC             PARTITION BY ticker ORDER BY trade_date
# MAGIC             ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
# MAGIC         ) AS sma_50,
# MAGIC         -- 20-day rolling volatility (stddev of daily returns)
# MAGIC         STDDEV(close_price) OVER (
# MAGIC             PARTITION BY ticker ORDER BY trade_date
# MAGIC             ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
# MAGIC         ) AS rolling_volatility_20d,
# MAGIC         -- 20-day average volume
# MAGIC         AVG(volume) OVER (
# MAGIC             PARTITION BY ticker ORDER BY trade_date
# MAGIC             ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
# MAGIC         ) AS avg_volume_20d
# MAGIC     FROM silver.market_prices_daily
# MAGIC )
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     trade_date,
# MAGIC     close_price,
# MAGIC     volume,
# MAGIC     ROUND(daily_return, 6) AS daily_return,
# MAGIC     ROUND(sma_20, 4) AS sma_20,
# MAGIC     ROUND(sma_50, 4) AS sma_50,
# MAGIC     ROUND(rolling_volatility_20d, 4) AS rolling_volatility_20d,
# MAGIC     avg_volume_20d,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM priced;

# COMMAND ----------

# DBTITLE 1,Validate Gold tables
# MAGIC %sql
# MAGIC -- Row counts for all gold market tables
# MAGIC SELECT 'gold.fact_market_prices' AS table_name, COUNT(*) AS row_count
# MAGIC FROM gold.fact_market_prices
# MAGIC UNION ALL
# MAGIC SELECT 'gold.market_daily_summary', COUNT(*)
# MAGIC FROM gold.market_daily_summary
# MAGIC UNION ALL
# MAGIC SELECT 'gold.market_trend_analytics', COUNT(*)
# MAGIC FROM gold.market_trend_analytics;
# MAGIC
# MAGIC -- Latest daily summary
# MAGIC SELECT *
# MAGIC FROM gold.market_daily_summary
# MAGIC ORDER BY trade_date DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Trend analytics sample
# MAGIC SELECT ticker, trade_date, close_price, daily_return, sma_20, sma_50, rolling_volatility_20d
# MAGIC FROM gold.market_trend_analytics
# MAGIC ORDER BY trade_date DESC, ticker
# MAGIC LIMIT 30;

# COMMAND ----------

