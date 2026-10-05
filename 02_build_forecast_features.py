# Databricks notebook source
# DBTITLE 1,Feature Builder Header
# MAGIC %md
# MAGIC # 02 — Build Forecast Features
# MAGIC
# MAGIC Rebuilds `workspace.features.market_forecast_features` from existing Gold risk tables with strict `as_of_date` discipline. Every feature uses only information available at or before `as_of_date`.
# MAGIC
# MAGIC **Source tables:** `workspace.gold.equity_risk_daily`, `workspace.gold.crypto_risk_daily`, `workspace.gold.cross_asset_risk_daily`
# MAGIC **Target:** `workspace.features.market_forecast_features`
# MAGIC
# MAGIC **Features include:**
# MAGIC - Existing risk metrics (returns, volatility, EWMA, VaR, ES, drawdown)
# MAGIC - Lagged returns (1d, 2d, 3d, 5d)
# MAGIC - Squared and absolute returns
# MAGIC - Volatility changes (1d)
# MAGIC - Rolling skewness and kurtosis (20d, 60d)
# MAGIC - Volume change and z-score
# MAGIC - Cross-asset correlations and their changes

# COMMAND ----------

# DBTITLE 1,Rebuild Feature Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.features.market_forecast_features AS
# MAGIC WITH combined AS (
# MAGIC   SELECT
# MAGIC     entity_id AS asset_id, 'equity' AS asset_class, trade_date AS as_of_date,
# MAGIC     close_price, volume, return_1d, log_return, return_5d, return_20d, return_60d,
# MAGIC     volatility_20d, volatility_60d, ewma_volatility, drawdown, worst_drawdown_252obs,
# MAGIC     var_95, var_99, expected_shortfall_95, expected_shortfall_99, downside_deviation_60d
# MAGIC   FROM workspace.gold.equity_risk_daily
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     entity_id AS asset_id, 'crypto' AS asset_class, trade_date AS as_of_date,
# MAGIC     close_price, volume, return_1d, log_return, return_5d, return_20d, return_60d,
# MAGIC     volatility_20d, volatility_60d, ewma_volatility, drawdown, worst_drawdown_252obs,
# MAGIC     var_95, var_99, expected_shortfall_95, expected_shortfall_99, downside_deviation_60d
# MAGIC   FROM workspace.gold.crypto_risk_daily
# MAGIC ),
# MAGIC cross_with_changes AS (
# MAGIC   SELECT
# MAGIC     trade_date, btc_spy_corr_30d, btc_spy_corr_90d, btc_spy_beta_60d, btc_equity_vol_ratio,
# MAGIC     eth_spy_corr_30d, eth_spy_corr_90d, eth_spy_beta_60d, eth_equity_vol_ratio,
# MAGIC     btc_spy_corr_30d - LAG(btc_spy_corr_30d, 1) OVER (ORDER BY trade_date) AS btc_spy_corr_30d_change,
# MAGIC     btc_spy_corr_90d - LAG(btc_spy_corr_90d, 1) OVER (ORDER BY trade_date) AS btc_spy_corr_90d_change,
# MAGIC     eth_spy_corr_30d - LAG(eth_spy_corr_30d, 1) OVER (ORDER BY trade_date) AS eth_spy_corr_30d_change,
# MAGIC     eth_spy_corr_90d - LAG(eth_spy_corr_90d, 1) OVER (ORDER BY trade_date) AS eth_spy_corr_90d_change
# MAGIC   FROM workspace.gold.cross_asset_risk_daily
# MAGIC ),
# MAGIC with_lags AS (
# MAGIC   SELECT
# MAGIC     c.*,
# MAGIC     LAG(c.return_1d, 1) OVER w AS return_1d_lag1,
# MAGIC     LAG(c.return_1d, 2) OVER w AS return_1d_lag2,
# MAGIC     LAG(c.return_1d, 3) OVER w AS return_1d_lag3,
# MAGIC     LAG(c.return_1d, 5) OVER w AS return_1d_lag5,
# MAGIC     POWER(c.return_1d, 2) AS return_1d_squared,
# MAGIC     POWER(c.log_return, 2) AS log_return_squared,
# MAGIC     ABS(c.return_1d) AS abs_return_1d,
# MAGIC     ABS(c.log_return) AS abs_log_return,
# MAGIC     LAG(c.volatility_20d, 1) OVER w AS vol_20d_lag1,
# MAGIC     LAG(c.volatility_60d, 1) OVER w AS vol_60d_lag1,
# MAGIC     LAG(c.ewma_volatility, 1) OVER w AS ewma_lag1,
# MAGIC     c.volume - LAG(c.volume, 1) OVER w AS volume_change_1d,
# MAGIC     (c.volume / NULLIF(LAG(c.volume, 5) OVER w, 0) - 1.0) AS volume_change_5d,
# MAGIC     (c.volume - AVG(c.volume) OVER w20) / NULLIF(STDDEV(c.volume) OVER w20, 0) AS volume_zscore_20d,
# MAGIC     SKEWNESS(c.log_return) OVER w20 AS rolling_skew_20d,
# MAGIC     SKEWNESS(c.log_return) OVER w60 AS rolling_skew_60d,
# MAGIC     KURTOSIS(c.log_return) OVER w20 AS rolling_kurt_20d,
# MAGIC     KURTOSIS(c.log_return) OVER w60 AS rolling_kurt_60d
# MAGIC   FROM combined c
# MAGIC   WINDOW
# MAGIC     w AS (PARTITION BY c.asset_id ORDER BY c.as_of_date),
# MAGIC     w20 AS (PARTITION BY c.asset_id ORDER BY c.as_of_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
# MAGIC     w60 AS (PARTITION BY c.asset_id ORDER BY c.as_of_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
# MAGIC )
# MAGIC SELECT
# MAGIC   w.asset_id, w.asset_class, w.as_of_date, w.close_price, w.volume,
# MAGIC   w.return_1d, w.log_return, w.return_5d, w.return_20d, w.return_60d,
# MAGIC   w.volatility_20d, w.volatility_60d, w.ewma_volatility,
# MAGIC   w.drawdown, w.worst_drawdown_252obs,
# MAGIC   w.var_95, w.var_99, w.expected_shortfall_95, w.expected_shortfall_99, w.downside_deviation_60d,
# MAGIC   w.return_1d_lag1, w.return_1d_lag2, w.return_1d_lag3, w.return_1d_lag5,
# MAGIC   w.return_1d_squared, w.log_return_squared, w.abs_return_1d, w.abs_log_return,
# MAGIC   w.volatility_20d - w.vol_20d_lag1 AS volatility_20d_change_1d,
# MAGIC   w.volatility_60d - w.vol_60d_lag1 AS volatility_60d_change_1d,
# MAGIC   w.ewma_volatility - w.ewma_lag1 AS ewma_volatility_change_1d,
# MAGIC   w.rolling_skew_20d, w.rolling_skew_60d, w.rolling_kurt_20d, w.rolling_kurt_60d,
# MAGIC   w.volume_change_1d, w.volume_change_5d, w.volume_zscore_20d,
# MAGIC   xc.btc_spy_corr_30d, xc.btc_spy_corr_90d, xc.btc_spy_beta_60d, xc.btc_equity_vol_ratio,
# MAGIC   xc.eth_spy_corr_30d, xc.eth_spy_corr_90d, xc.eth_spy_beta_60d, xc.eth_equity_vol_ratio,
# MAGIC   xc.btc_spy_corr_30d_change, xc.btc_spy_corr_90d_change,
# MAGIC   xc.eth_spy_corr_30d_change, xc.eth_spy_corr_90d_change,
# MAGIC   current_timestamp() AS feature_timestamp
# MAGIC FROM with_lags w
# MAGIC LEFT JOIN cross_with_changes xc ON w.as_of_date = xc.trade_date
# MAGIC ORDER BY w.asset_id, w.as_of_date

# COMMAND ----------

# DBTITLE 1,Verify Features
# MAGIC %sql
# MAGIC -- Verify feature table
# MAGIC SELECT COUNT(*) AS total_rows, COUNT(DISTINCT asset_id) AS assets,
# MAGIC        MIN(as_of_date) AS min_date, MAX(as_of_date) AS max_date
# MAGIC FROM workspace.features.market_forecast_features

# COMMAND ----------

