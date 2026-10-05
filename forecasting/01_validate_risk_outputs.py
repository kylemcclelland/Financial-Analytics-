# Databricks notebook source
# DBTITLE 1,Validation Header
# MAGIC %md
# MAGIC # 01 — Validate Existing Risk Outputs
# MAGIC
# MAGIC Validates that the existing Gold risk tables are present, populated, and have the expected columns before the forecasting pipeline runs. Fails the job if critical tables are missing or empty.

# COMMAND ----------

# DBTITLE 1,Check Required Tables
# MAGIC %sql
# MAGIC -- Check that all required tables exist and have data
# MAGIC SELECT 'gold.equity_risk_daily' AS table_name, COUNT(*) AS row_count FROM workspace.gold.equity_risk_daily
# MAGIC UNION ALL SELECT 'gold.crypto_risk_daily', COUNT(*) FROM workspace.gold.crypto_risk_daily
# MAGIC UNION ALL SELECT 'gold.cross_asset_risk_daily', COUNT(*) FROM workspace.gold.cross_asset_risk_daily
# MAGIC UNION ALL SELECT 'gold.fact_market_prices', COUNT(*) FROM workspace.gold.fact_market_prices
# MAGIC UNION ALL SELECT 'gold.fact_crypto_candles', COUNT(*) FROM workspace.gold.fact_crypto_candles
# MAGIC UNION ALL SELECT 'features.market_forecast_features', COUNT(*) FROM workspace.features.market_forecast_features
# MAGIC UNION ALL SELECT 'analytics.forecasting_data_audit', COUNT(*) FROM workspace.analytics.forecasting_data_audit

# COMMAND ----------

# DBTITLE 1,Check Data Freshness
# MAGIC %sql
# MAGIC -- Validate data freshness — latest date should be within 3 days of today
# MAGIC SELECT 
# MAGIC   'equity_risk_daily' AS source,
# MAGIC   MAX(trade_date) AS latest_date,
# MAGIC   DATEDIFF(CURRENT_DATE(), MAX(trade_date)) AS days_behind
# MAGIC FROM workspace.gold.equity_risk_daily
# MAGIC UNION ALL
# MAGIC SELECT 'crypto_risk_daily', MAX(trade_date), DATEDIFF(CURRENT_DATE(), MAX(trade_date))
# MAGIC FROM workspace.gold.crypto_risk_daily
# MAGIC UNION ALL
# MAGIC SELECT 'cross_asset_risk_daily', MAX(trade_date), DATEDIFF(CURRENT_DATE(), MAX(trade_date))
# MAGIC FROM workspace.gold.cross_asset_risk_daily

# COMMAND ----------

# DBTITLE 1,Check GARCH Readiness
# MAGIC %sql
# MAGIC -- Check for assets with sufficient GARCH history (>=500 obs)
# MAGIC SELECT 
# MAGIC   asset_class,
# MAGIC   COUNT(*) AS total_assets,
# MAGIC   SUM(CASE WHEN sufficient_for_garch THEN 1 ELSE 0 END) AS sufficient_for_garch,
# MAGIC   SUM(CASE WHEN data_quality_status = 'WARN' THEN 1 ELSE 0 END) AS warnings
# MAGIC FROM workspace.analytics.forecasting_data_audit
# MAGIC GROUP BY asset_class
# MAGIC ORDER BY asset_class

# COMMAND ----------

# DBTITLE 1,Validate Critical Columns
# Validate critical columns exist and are non-null
from pyspark.sql.functions import col, count, when

critical_tables = {
  'workspace.gold.equity_risk_daily': ['entity_id', 'trade_date', 'log_return', 'volatility_20d', 'ewma_volatility'],
  'workspace.gold.crypto_risk_daily': ['entity_id', 'trade_date', 'log_return', 'volatility_20d', 'ewma_volatility'],
  'workspace.features.market_forecast_features': ['asset_id', 'as_of_date', 'log_return', 'volatility_20d', 'ewma_volatility']
}

all_valid = True
for table_name, cols in critical_tables.items():
  df = spark.table(table_name)
  null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in cols]).collect()[0]
  total = df.count()
  print(f"\n=== {table_name} ({total:,} rows) ===")
  for c in cols:
    nulls = null_counts[c]
    status = 'OK' if nulls == 0 else f'WARN: {nulls} nulls'
    print(f"  {c}: {status}")
    if nulls > 0 and nulls > total * 0.01:
      all_valid = False
      print(f"  ERROR: >1% nulls in {c} — failing validation")

if all_valid:
  print("\n✅ All validations passed")
else:
  print("\n❌ Validation failed — check warnings above")
  raise ValueError("Data validation failed")

# COMMAND ----------

