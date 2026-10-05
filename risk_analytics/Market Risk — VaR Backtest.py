# Databricks notebook source
# MAGIC %md
# MAGIC # Market Risk — VaR Backtest
# MAGIC
# MAGIC Downstream extension of the Market Risk Calculation job. Evaluates the existing VaR forecasts (95% and 99%) against realized returns using the Kupiec POF test. Reads only from existing Gold risk tables — does not modify any existing tables or code.
# MAGIC
# MAGIC **Inputs:** `gold.equity_risk_daily`, `gold.crypto_risk_daily` (if available)
# MAGIC **Output:** `gold.var_backtest_daily`
# MAGIC **Cadence:** Daily, after the Market Risk Calculation job completes
# MAGIC
# MAGIC ## Look-Ahead Bias Prevention
# MAGIC
# MAGIC The VaR forecast evaluated against the return on date t uses the VaR from the previous trading day (t-1), not date t itself. This ensures the forecast was computed from information available before date t. The existing notebook computes `var_95[t]` using a rolling window that includes `return_1d[t]` (pandas `rolling().quantile()` includes the current position). By shifting the VaR by one day, we compare `var_95[t-1]` against `return_1d[t]`, which is look-ahead-free.
# MAGIC
# MAGIC **Note:** The existing VaR computation at position t includes return_1d[t] in its rolling window. This is a potential issue for real-time forecasting but does not affect the backtest results when the VaR is shifted by one day. Documented as a future recommendation for the existing notebook.

# COMMAND ----------

# DBTITLE 1,DDL — var_backtest_daily
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.var_backtest_daily (
# MAGIC   asset_id STRING NOT NULL COMMENT 'Ticker or product ID',
# MAGIC   asset_type STRING NOT NULL COMMENT 'equity or crypto',
# MAGIC   trade_date DATE NOT NULL COMMENT 'Trading date',
# MAGIC   confidence_level STRING NOT NULL COMMENT '95% or 99%',
# MAGIC   forecast_var DOUBLE COMMENT 'VaR forecast from previous trading day (positive = loss magnitude)',
# MAGIC   realized_return DOUBLE COMMENT 'Actual return on trade_date',
# MAGIC   breach_flag INT COMMENT '1 if realized loss exceeds VaR (return < -VaR), 0 otherwise',
# MAGIC   rolling_breach_count_250d INT COMMENT 'Number of breaches in trailing 250 observations',
# MAGIC   expected_breach_count DOUBLE COMMENT 'Expected breaches = 250 * tail_probability',
# MAGIC   breach_ratio DOUBLE COMMENT 'Observed breaches / expected breaches',
# MAGIC   kupiec_statistic DOUBLE COMMENT 'Kupiec POF test likelihood ratio statistic (chi-squared, 1 df)',
# MAGIC   kupiec_p_value DOUBLE COMMENT 'P-value from chi-squared(1) distribution',
# MAGIC   sufficient_history_flag BOOLEAN COMMENT 'TRUE if >= 250 non-null VaR forecasts as of trade_date',
# MAGIC   calculation_timestamp TIMESTAMP COMMENT 'When this row was computed'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Daily VaR backtesting results evaluating existing VaR forecasts against realized returns'

# COMMAND ----------

# DBTITLE 1,Imports & Config
import math
import numpy as np
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DoubleType, BooleanType, IntegerType, TimestampType
)
from delta.tables import DeltaTable

# Widget: start_date for incremental processing
try:
    dbutils.widgets.text("start_date", "")
    START_DATE = dbutils.widgets.get("start_date").strip()
except Exception:
    START_DATE = ""

TARGET_TABLE = "gold.var_backtest_daily"
BACKTEST_WINDOW = 250
MIN_PERIODS = 20

# COMMAND ----------

# DBTITLE 1,Load Source Data
# Load equity risk table (always exists)
equity_df = (
    spark.table("workspace.gold.equity_risk_daily")
    .select(
        F.col("entity_id"),
        F.col("trade_date"),
        F.col("return_1d"),
        F.col("var_95"),
        F.col("var_99"),
        F.lit("equity").alias("asset_type"),
    )
)

source_df = equity_df

# Load crypto risk table if it exists
if spark.catalog.tableExists("workspace.gold.crypto_risk_daily"):
    crypto_df = (
        spark.table("workspace.gold.crypto_risk_daily")
        .select(
            F.col("entity_id"),
            F.col("trade_date"),
            F.col("return_1d"),
            F.col("var_95"),
            F.col("var_99"),
            F.lit("crypto").alias("asset_type"),
        )
    )
    source_df = equity_df.unionByName(crypto_df)

# Incremental filter: load buffer for rolling window
if START_DATE:
    buffer_date = F.to_date(F.lit(START_DATE)) - F.expr("INTERVAL 400 DAYS")
    source_df = source_df.filter(F.col("trade_date") >= buffer_date)

print(f"Source rows: {source_df.count()}")
print(f"Unique assets: {source_df.select('entity_id').distinct().count()}")

# COMMAND ----------

# DBTITLE 1,Compute VaR Backtest
# ============================================================
# COMPUTE VAR BACKTEST
# ============================================================

BACKTEST_SCHEMA = StructType([
    StructField("asset_id", StringType(), False),
    StructField("asset_type", StringType(), False),
    StructField("trade_date", DateType(), False),
    StructField("confidence_level", StringType(), False),
    StructField("forecast_var", DoubleType(), True),
    StructField("realized_return", DoubleType(), True),
    StructField("breach_flag", IntegerType(), True),
    StructField("rolling_breach_count_250d", IntegerType(), True),
    StructField("expected_breach_count", DoubleType(), True),
    StructField("breach_ratio", DoubleType(), True),
    StructField("kupiec_statistic", DoubleType(), True),
    StructField("kupiec_p_value", DoubleType(), True),
    StructField("sufficient_history_flag", BooleanType(), True),
])


def _kupiec_test(x, N, p):
    """
    Kupiec POF (Proportion of Failures) test.
    Returns (LR statistic, p_value) from chi-squared(1).
    """
    if N == 0:
        return np.nan, np.nan

    p_hat = x / N

    if x == 0:
        LR = -2.0 * N * math.log(1.0 - p)
    elif x == N:
        LR = -2.0 * N * math.log(p)
    else:
        LR = -2.0 * (
            (N - x) * math.log(1.0 - p) + x * math.log(p)
            - (N - x) * math.log(1.0 - p_hat) - x * math.log(p_hat)
        )

    if LR < 0:
        LR = 0.0

    p_value = math.erfc(math.sqrt(LR / 2.0))
    return float(LR), float(p_value)


def compute_var_backtest(pdf):
    """
    Compute VaR backtest metrics for one asset group.

    Input columns: entity_id, trade_date, return_1d, var_95, var_99, asset_type
    """
    pdf = pdf.sort_values("trade_date").copy()
    n = len(pdf)

    if n == 0:
        return pd.DataFrame(columns=[
            "asset_id", "asset_type", "trade_date", "confidence_level",
            "forecast_var", "realized_return", "breach_flag",
            "rolling_breach_count_250d", "expected_breach_count",
            "breach_ratio", "kupiec_statistic", "kupiec_p_value",
            "sufficient_history_flag"
        ])

    returns = pdf["return_1d"].values.astype(float)
    var_95 = pdf["var_95"].values.astype(float)
    var_99 = pdf["var_99"].values.astype(float)
    asset_id = pdf["entity_id"].values
    asset_type = pdf["asset_type"].values
    trade_dates = pdf["trade_date"].values

    results = []

    for conf_label, var_arr, p in [("95%", var_95, 0.05), ("99%", var_99, 0.01)]:
        # Shift VaR by 1 day to prevent look-ahead bias
        # forecast_var[t] = var_arr[t-1]: VaR computed from data up to t-1
        forecast = np.full(n, np.nan)
        forecast[1:] = var_arr[:-1]

        # Breach flag: 1 if realized loss exceeds VaR
        breach = np.zeros(n, dtype=int)
        for i in range(n):
            if np.isfinite(forecast[i]) and np.isfinite(returns[i]):
                breach[i] = 1 if returns[i] < -forecast[i] else 0

        # Rolling breach count (250-obs window)
        breach_series = pd.Series(breach)
        rolling_breaches = breach_series.rolling(
            BACKTEST_WINDOW, min_periods=MIN_PERIODS
        ).sum().values
        rolling_n = breach_series.rolling(
            BACKTEST_WINDOW, min_periods=MIN_PERIODS
        ).count().values

        expected = float(BACKTEST_WINDOW * p)

        # Sufficient history: >= 250 non-null VaR forecasts
        forecast_valid = np.isfinite(forecast)
        cum_valid = np.cumsum(forecast_valid)
        sufficient = cum_valid >= BACKTEST_WINDOW

        for i in range(n):
            x = int(rolling_breaches[i]) if np.isfinite(rolling_breaches[i]) else 0
            N = int(rolling_n[i]) if np.isfinite(rolling_n[i]) else 0
            has_data = np.isfinite(forecast[i]) and np.isfinite(returns[i])

            if N >= MIN_PERIODS and has_data:
                stat, pval = _kupiec_test(x, N, p)
                breach_ratio = float(x / expected) if expected > 0 else np.nan
            else:
                stat, pval = np.nan, np.nan
                breach_ratio = np.nan

            results.append({
                "asset_id": str(asset_id[i]),
                "asset_type": str(asset_type[i]),
                "trade_date": trade_dates[i],
                "confidence_level": conf_label,
                "forecast_var": float(forecast[i]) if np.isfinite(forecast[i]) else None,
                "realized_return": float(returns[i]) if np.isfinite(returns[i]) else None,
                "breach_flag": int(breach[i]),
                "rolling_breach_count_250d": int(x) if N >= MIN_PERIODS else None,
                "expected_breach_count": float(expected) if N >= MIN_PERIODS else None,
                "breach_ratio": float(breach_ratio) if np.isfinite(breach_ratio) else None,
                "kupiec_statistic": float(stat) if np.isfinite(stat) else None,
                "kupiec_p_value": float(pval) if np.isfinite(pval) else None,
                "sufficient_history_flag": bool(sufficient[i]),
            })

    result = pd.DataFrame(results)
    result = result.replace([np.inf, -np.inf], np.nan)
    return result


# Execute
print("Computing VaR backtest...")
result_df = (
    source_df
    .groupBy("entity_id", "asset_type")
    .applyInPandas(compute_var_backtest, schema=BACKTEST_SCHEMA)
    .withColumn("calculation_timestamp", F.current_timestamp())
)

print(f"Result rows: {result_df.count()}")

# COMMAND ----------

# DBTITLE 1,Merge to Delta
# Filter to dates >= START_DATE for incremental MERGE
if START_DATE:
    result_df = result_df.filter(F.col("trade_date") >= F.lit(START_DATE).cast("date"))

# Idempotent Delta MERGE on (asset_id, asset_type, trade_date, confidence_level)
if not spark.catalog.tableExists(TARGET_TABLE):
    result_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"Created {TARGET_TABLE}")
else:
    target = DeltaTable.forName(spark, TARGET_TABLE)
    (
        target.alias("target")
        .merge(
            result_df.alias("source"),
            """
            target.asset_id = source.asset_id
            AND target.asset_type = source.asset_type
            AND target.trade_date = source.trade_date
            AND target.confidence_level = source.confidence_level
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Merged into {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,Validation
# ============================================================
# VALIDATION
# ============================================================

bt = spark.table(TARGET_TABLE)
print(f"=== {TARGET_TABLE} ===")
print(f"Total rows: {bt.count()}")
print(f"Unique assets: {bt.select('asset_id').distinct().count()}")

# Date range
dr = bt.agg(F.min("trade_date").alias("min_date"), F.max("trade_date").alias("max_date")).collect()[0]
print(f"Date range: {dr.min_date} to {dr.max_date}")

# Check SPY 95% breaches
print("\n--- SPY 95% Backtest ---")
spy_95 = bt.filter((F.col("asset_id") == "SPY") & (F.col("confidence_level") == "95%"))
spy_95_full = spy_95.filter(F.col("sufficient_history_flag") == True)
spy_breaches = spy_95_full.filter(F.col("breach_flag") == 1).count()
spy_total = spy_95_full.count()
print(f"Breaches: {spy_breaches} / {spy_total} (expected ~5% = {spy_total * 0.05:.1f})")

# Check SPY 99% breaches
print("\n--- SPY 99% Backtest ---")
spy_99 = bt.filter((F.col("asset_id") == "SPY") & (F.col("confidence_level") == "99%"))
spy_99_full = spy_99.filter(F.col("sufficient_history_flag") == True)
spy_99_breaches = spy_99_full.filter(F.col("breach_flag") == 1).count()
spy_99_total = spy_99_full.count()
print(f"Breaches: {spy_99_breaches} / {spy_99_total} (expected ~1% = {spy_99_total * 0.01:.1f})")

# No infinities check
print("\n--- Infinity/Null Check ---")
for col_name in ["forecast_var", "realized_return", "kupiec_statistic", "kupiec_p_value", "breach_ratio"]:
    inf_count = bt.filter(
        F.col(col_name).isNull() |
        (F.col(col_name) == float('inf')) |
        (F.col(col_name) == float('-inf'))
    ).count()
    print(f"  {col_name}: {inf_count} null/inf rows")

# Sample SPY 95% (latest)
print("\n--- Sample SPY 95% (latest 5) ---")
spy_95.orderBy(F.desc("trade_date")).limit(5).show()

# Sample SPY 99% (latest)
print("\n--- Sample SPY 99% (latest 5) ---")
spy_99.orderBy(F.desc("trade_date")).limit(5).show()