# Databricks notebook source
# MAGIC %md
# MAGIC # Market Risk — Tail Risk
# MAGIC
# MAGIC Downstream extension of the Market Risk Calculation job. Computes daily tail-risk metrics into `gold.asset_tail_risk_daily` from existing Gold risk tables. Does not modify any existing tables, code, or schemas.
# MAGIC
# MAGIC **Inputs:** `gold.equity_risk_daily`, `gold.crypto_risk_daily` (if available)
# MAGIC **Output:** `gold.asset_tail_risk_daily`
# MAGIC **Cadence:** Daily, after the existing analytics job completes

# COMMAND ----------

# DBTITLE 1,DDL — Create tail risk table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.asset_tail_risk_daily (
# MAGIC   asset_id STRING NOT NULL COMMENT 'Ticker or product ID',
# MAGIC   asset_type STRING NOT NULL COMMENT 'equity or crypto',
# MAGIC   trade_date DATE NOT NULL COMMENT 'Trading date',
# MAGIC   current_drawdown DOUBLE COMMENT 'Current drawdown from running peak (<= 0)',
# MAGIC   rolling_max_drawdown_252d DOUBLE COMMENT 'True peak-to-trough max drawdown in trailing 252 obs',
# MAGIC   drawdown_duration_days INT COMMENT 'Consecutive trading days below running peak',
# MAGIC   sortino_60d DOUBLE COMMENT 'Annualized Sortino ratio (60d)',
# MAGIC   calmar_252d DOUBLE COMMENT 'Calmar ratio = abs(annualized return) / abs(max drawdown) (252d)',
# MAGIC   var_975_252d DOUBLE COMMENT 'Historical VaR at 97.5% confidence (252d, positive = loss)',
# MAGIC   expected_shortfall_975_252d DOUBLE COMMENT 'ES at 97.5% (252d, positive = loss, >= VaR)',
# MAGIC   worst_5_return_avg_252d DOUBLE COMMENT 'Avg of 5 worst daily returns (252d, positive = loss)',
# MAGIC   worst_10_return_avg_252d DOUBLE COMMENT 'Avg of 10 worst daily returns (252d, positive = loss)',
# MAGIC   lower_partial_moment_1_252d DOUBLE COMMENT 'LPM order 1: mean of max(0, -return) (252d)',
# MAGIC   lower_partial_moment_2_252d DOUBLE COMMENT 'LPM order 2: mean of max(0, -return)^2 (252d)',
# MAGIC   max_loss_to_total_loss_ratio_252d DOUBLE COMMENT 'Worst single loss / sum of all losses (252d)',
# MAGIC   tail_loss_concentration_5_252d DOUBLE COMMENT 'Sum of 5 worst losses / sum of all losses (252d)',
# MAGIC   sufficient_history_flag BOOLEAN COMMENT 'TRUE if >= 252 non-null returns',
# MAGIC   data_quality_flag BOOLEAN COMMENT 'TRUE if no data quality issues detected',
# MAGIC   calculation_timestamp TIMESTAMP COMMENT 'When this row was computed',
# MAGIC   model_version STRING COMMENT 'Model version identifier'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Daily tail-risk metrics derived from existing Gold risk tables'

# COMMAND ----------

# DBTITLE 1,Imports & Configuration
import math
import numpy as np
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DoubleType, BooleanType, IntegerType, TimestampType
)
from delta.tables import DeltaTable

# ============================================================
# CONFIGURATION
# ============================================================

# Incremental processing: set start_date to recompute only
# from that date forward. Leave empty for full recompute.
try:
    dbutils.widgets.text("start_date", "")
    START_DATE = dbutils.widgets.get("start_date").strip()
except Exception:
    START_DATE = ""

TARGET_TABLE = "gold.asset_tail_risk_daily"
TAIL_WINDOW = 252
MIN_PERIODS_VAR = 60
MIN_PERIODS_OTHER = 20
MODEL_VERSION = "1.0"

# COMMAND ----------

# DBTITLE 1,Load Source Data
# ============================================================
# LOAD SOURCE DATA
# ============================================================

# Equity risk table (always exists)
equity_df = (
    spark.table("workspace.gold.equity_risk_daily")
    .select(
        F.col("entity_id"),
        F.col("trade_date"),
        F.col("close_price"),
        F.col("return_1d"),
        F.col("drawdown"),
        F.col("downside_deviation_60d"),
        F.lit("equity").alias("asset_type"),
        F.lit(252).alias("annualization"),
    )
)

# Crypto risk table (may not exist yet)
source_df = equity_df

if spark.catalog.tableExists("workspace.gold.crypto_risk_daily"):
    crypto_df = (
        spark.table("workspace.gold.crypto_risk_daily")
        .select(
            F.col("entity_id"),
            F.col("trade_date"),
            F.col("close_price"),
            F.col("return_1d"),
            F.col("drawdown"),
            F.col("downside_deviation_60d"),
            F.lit("crypto").alias("asset_type"),
            F.lit(365).alias("annualization"),
        )
    )
    source_df = equity_df.unionByName(crypto_df)

# Apply incremental buffer if start_date is set
if START_DATE:
    buffer_date = F.to_date(F.lit(START_DATE)) - F.expr("INTERVAL 400 DAYS")
    source_df = source_df.filter(F.col("trade_date") >= buffer_date)

total_rows = source_df.count()
total_assets = source_df.select("entity_id").distinct().count()
print(f"Source rows: {total_rows}")
print(f"Unique assets: {total_assets}")
print(f"Asset types: {source_df.select('asset_type').distinct().collect()}")

# COMMAND ----------

# DBTITLE 1,Compute Tail Risk Metrics
# ============================================================
# TAIL RISK COMPUTATION
# ============================================================

TAIL_SCHEMA = StructType([
    StructField("asset_id", StringType(), False),
    StructField("asset_type", StringType(), False),
    StructField("trade_date", DateType(), False),
    StructField("current_drawdown", DoubleType(), True),
    StructField("rolling_max_drawdown_252d", DoubleType(), True),
    StructField("drawdown_duration_days", IntegerType(), True),
    StructField("sortino_60d", DoubleType(), True),
    StructField("calmar_252d", DoubleType(), True),
    StructField("var_975_252d", DoubleType(), True),
    StructField("expected_shortfall_975_252d", DoubleType(), True),
    StructField("worst_5_return_avg_252d", DoubleType(), True),
    StructField("worst_10_return_avg_252d", DoubleType(), True),
    StructField("lower_partial_moment_1_252d", DoubleType(), True),
    StructField("lower_partial_moment_2_252d", DoubleType(), True),
    StructField("max_loss_to_total_loss_ratio_252d", DoubleType(), True),
    StructField("tail_loss_concentration_5_252d", DoubleType(), True),
    StructField("sufficient_history_flag", BooleanType(), True),
    StructField("data_quality_flag", BooleanType(), True),
])


# ---------- Helper functions for custom rolling metrics ----------

def _true_mdd(window_prices):
    """True peak-to-trough max drawdown within a window using running peaks.
    For [100, 50, 120, 110]: running peak = [100, 100, 120, 120],
    drawdowns = [0, -0.5, 0, -0.083], MDD = -50% (not -8.3%)."""
    running_peak = np.maximum.accumulate(window_prices)
    drawdowns = window_prices / running_peak - 1.0
    return float(np.min(drawdowns))


def _var_975(window_returns):
    """Historical VaR at 97.5% (positive = loss)."""
    x = window_returns[np.isfinite(window_returns)]
    if len(x) < MIN_PERIODS_VAR:
        return np.nan
    return float(-np.quantile(x, 0.025))


def _es_975(window_returns):
    """Expected shortfall at 97.5% (positive = loss, >= VaR)."""
    x = window_returns[np.isfinite(window_returns)]
    if len(x) < MIN_PERIODS_VAR:
        return np.nan
    cutoff = np.quantile(x, 0.025)
    tail = x[x <= cutoff]
    if len(tail) == 0:
        return np.nan
    return float(-tail.mean())


def _worst_n(window_returns, n_worst):
    """Average of n worst returns as positive loss magnitude."""
    x = window_returns[np.isfinite(window_returns)]
    if len(x) < n_worst:
        return np.nan
    return float(-np.sort(x)[:n_worst].mean())


def _lpm1(window_returns):
    """Lower partial moment order 1: mean of max(0, -return)."""
    x = window_returns[np.isfinite(window_returns)]
    if len(x) < MIN_PERIODS_OTHER:
        return np.nan
    return float(np.mean(np.maximum(0.0, -x)))


def _lpm2(window_returns):
    """Lower partial moment order 2: mean of max(0, -return)^2."""
    x = window_returns[np.isfinite(window_returns)]
    if len(x) < MIN_PERIODS_OTHER:
        return np.nan
    return float(np.mean(np.maximum(0.0, -x) ** 2))


def _max_loss_ratio(window_returns):
    """Worst single loss / sum of all losses."""
    x = window_returns[np.isfinite(window_returns)]
    neg = x[x < 0]
    if len(neg) == 0:
        return np.nan
    total = float(-neg.sum())
    worst = float(-neg.min())
    if total > 0:
        return float(worst / total)
    return np.nan


def _tail_conc_5(window_returns):
    """Sum of 5 worst losses / sum of all losses."""
    x = window_returns[np.isfinite(window_returns)]
    neg = x[x < 0]
    if len(neg) < 5:
        return np.nan
    total = float(-neg.sum())
    worst5 = float(-np.sort(x)[:5].sum())
    if total > 0:
        return float(worst5 / total)
    return np.nan


def compute_tail_risk(pdf):
    """
    Compute tail-risk metrics for one asset group.
    Called via groupBy(entity_id, asset_type).applyInPandas.
    """
    pdf = pdf.sort_values("trade_date").copy()
    n = len(pdf)

    if n == 0:
        return pd.DataFrame(columns=[
            "asset_id", "asset_type", "trade_date", "current_drawdown",
            "rolling_max_drawdown_252d", "drawdown_duration_days",
            "sortino_60d", "calmar_252d", "var_975_252d",
            "expected_shortfall_975_252d", "worst_5_return_avg_252d",
            "worst_10_return_avg_252d", "lower_partial_moment_1_252d",
            "lower_partial_moment_2_252d", "max_loss_to_total_loss_ratio_252d",
            "tail_loss_concentration_5_252d", "sufficient_history_flag",
            "data_quality_flag"
        ])

    close = pdf["close_price"].values.astype(float)
    returns = pdf["return_1d"].values.astype(float)
    dd_source = pdf["downside_deviation_60d"].values.astype(float)
    annualization = int(pdf["annualization"].iloc[0])

    close_series = pd.Series(close)
    returns_series = pd.Series(returns)

    # 1. Current drawdown (from source table)
    current_drawdown = pdf["drawdown"].values.astype(float)

    # 2. Rolling true peak-to-trough MDD (252d, min_periods=20)
    rolling_mdd = (
        close_series
        .rolling(TAIL_WINDOW, min_periods=MIN_PERIODS_OTHER)
        .apply(_true_mdd, raw=True)
        .values
    )

    # 3. Drawdown duration days (consecutive days below all-time peak)
    running_peak = np.maximum.accumulate(close)
    is_at_peak = close >= running_peak
    duration = 0
    durations = np.zeros(n, dtype=int)
    for i in range(n):
        if is_at_peak[i]:
            duration = 0
        else:
            duration += 1
        durations[i] = duration

    # 4. Sortino ratio (60d)
    mean_return_60d = (
        returns_series
        .rolling(60, min_periods=20)
        .mean()
        .values
    )
    sortino_60d = np.full(n, np.nan)
    for i in range(n):
        if (np.isfinite(mean_return_60d[i]) and np.isfinite(dd_source[i])
                and dd_source[i] != 0):
            sortino_60d[i] = (mean_return_60d[i] * annualization) / dd_source[i]

    # 5. Calmar ratio (252d)
    total_return = (close_series / close_series.shift(TAIL_WINDOW) - 1.0).values
    with np.errstate(over='ignore', invalid='ignore'):
        ann_return = np.where(
            np.isfinite(total_return),
            np.power(1.0 + total_return, annualization / TAIL_WINDOW) - 1.0,
            np.nan
        )
    calmar = np.full(n, np.nan)
    for i in range(n):
        if (np.isfinite(ann_return[i]) and np.isfinite(rolling_mdd[i])
                and rolling_mdd[i] != 0):
            calmar[i] = abs(ann_return[i]) / abs(rolling_mdd[i])

    # 6. VaR 97.5% and ES 97.5% (252d, min_periods=60)
    var_975 = (
        returns_series
        .rolling(TAIL_WINDOW, min_periods=MIN_PERIODS_VAR)
        .apply(_var_975, raw=True)
        .values
    )
    es_975 = (
        returns_series
        .rolling(TAIL_WINDOW, min_periods=MIN_PERIODS_VAR)
        .apply(_es_975, raw=True)
        .values
    )

    # 7. Worst 5 and worst 10 return averages (252d)
    worst_5 = (
        returns_series
        .rolling(TAIL_WINDOW, min_periods=5)
        .apply(lambda x: _worst_n(x, 5), raw=True)
        .values
    )
    worst_10 = (
        returns_series
        .rolling(TAIL_WINDOW, min_periods=10)
        .apply(lambda x: _worst_n(x, 10), raw=True)
        .values
    )

    # 8. Lower partial moments (252d, min_periods=20)
    lpm1 = (
        returns_series
        .rolling(TAIL_WINDOW, min_periods=MIN_PERIODS_OTHER)
        .apply(_lpm1, raw=True)
        .values
    )
    lpm2 = (
        returns_series
        .rolling(TAIL_WINDOW, min_periods=MIN_PERIODS_OTHER)
        .apply(_lpm2, raw=True)
        .values
    )

    # 9. Max loss to total loss ratio (252d)
    max_loss_ratio = (
        returns_series
        .rolling(TAIL_WINDOW, min_periods=MIN_PERIODS_OTHER)
        .apply(_max_loss_ratio, raw=True)
        .values
    )

    # 10. Tail loss concentration 5 (252d)
    tail_conc = (
        returns_series
        .rolling(TAIL_WINDOW, min_periods=MIN_PERIODS_OTHER)
        .apply(_tail_conc_5, raw=True)
        .values
    )

    # 11. Sufficient history flag (cumulative non-null returns >= 252)
    valid_count = np.cumsum(np.isfinite(returns))
    sufficient = valid_count >= TAIL_WINDOW

    # 12. Data quality flag
    # No null close prices (source pre-filtered)
    no_null_close = np.all(np.isfinite(close))
    no_null_close_arr = np.full(n, no_null_close)

    # No infinite values in any metric
    no_inf = np.ones(n, dtype=bool)
    for m in [rolling_mdd, sortino_60d, calmar, var_975, es_975,
              worst_5, worst_10, lpm1, lpm2, max_loss_ratio, tail_conc]:
        no_inf &= ~np.isinf(m)

    # Return_1d has no unexpected null gaps (first row null is expected)
    null_ret = ~np.isfinite(returns)
    null_ret[0] = False
    has_any_gap = np.any(null_ret)
    if not has_any_gap:
        no_gaps = np.ones(n, dtype=bool)
    else:
        cum_nulls = np.cumsum(null_ret)
        no_gaps = np.ones(n, dtype=bool)
        for i in range(n):
            if not sufficient[i]:
                continue
            w_start = i - TAIL_WINDOW + 1
            nulls_in_window = cum_nulls[i] - (cum_nulls[w_start - 1] if w_start > 0 else 0)
            no_gaps[i] = (nulls_in_window == 0)

    dq = no_null_close_arr & no_inf & no_gaps & sufficient

    # Build output DataFrame
    result = pd.DataFrame({
        "asset_id": pdf["entity_id"].values,
        "asset_type": pdf["asset_type"].values,
        "trade_date": pdf["trade_date"].values,
        "current_drawdown": current_drawdown,
        "rolling_max_drawdown_252d": rolling_mdd,
        "drawdown_duration_days": durations,
        "sortino_60d": sortino_60d,
        "calmar_252d": calmar,
        "var_975_252d": var_975,
        "expected_shortfall_975_252d": es_975,
        "worst_5_return_avg_252d": worst_5,
        "worst_10_return_avg_252d": worst_10,
        "lower_partial_moment_1_252d": lpm1,
        "lower_partial_moment_2_252d": lpm2,
        "max_loss_to_total_loss_ratio_252d": max_loss_ratio,
        "tail_loss_concentration_5_252d": tail_conc,
        "sufficient_history_flag": sufficient,
        "data_quality_flag": dq,
    })

    # Replace infinities with NaN
    result = result.replace([np.inf, -np.inf], np.nan)

    return result


# Execute distributed computation
print("Computing tail-risk metrics...")
tail_risk_df = (
    source_df
    .groupBy("entity_id", "asset_type")
    .applyInPandas(compute_tail_risk, schema=TAIL_SCHEMA)
    .withColumn("calculation_timestamp", F.current_timestamp())
    .withColumn("model_version", F.lit(MODEL_VERSION))
)

print("Computation complete.")
print(f"Output rows: {tail_risk_df.count()}")

# COMMAND ----------

# DBTITLE 1,Merge to Delta Table
# ============================================================
# MERGE INTO DELTA TABLE
# ============================================================

# Filter to start_date if incremental run
merge_df = tail_risk_df
if START_DATE:
    merge_df = tail_risk_df.filter(F.col("trade_date") >= F.to_date(F.lit(START_DATE)))

target = DeltaTable.forName(spark, TARGET_TABLE)

(
    target.alias("target")
    .merge(
        merge_df.alias("source"),
        """
        target.asset_id = source.asset_id
        AND target.asset_type = source.asset_type
        AND target.trade_date = source.trade_date
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

result_table = spark.table(TARGET_TABLE)

print(f"=== {TARGET_TABLE} ===")
print(f"Total rows: {result_table.count()}")
print(f"Unique assets: {result_table.select('asset_id').distinct().count()}")

date_range = result_table.agg(
    F.min("trade_date").alias("min_date"),
    F.max("trade_date").alias("max_date")
).collect()[0]
print(f"Date range: {date_range.min_date} to {date_range.max_date}")

# Check ES >= VaR everywhere both non-null
es_var_violations = result_table.filter(
    F.col("expected_shortfall_975_252d").isNotNull()
    & F.col("var_975_252d").isNotNull()
    & (F.col("expected_shortfall_975_252d") < F.col("var_975_252d"))
).count()
print(f"\nES >= VaR violations: {es_var_violations}")

# Check for infinities in numeric columns
inf_cols = [
    "current_drawdown", "rolling_max_drawdown_252d", "sortino_60d", "calmar_252d",
    "var_975_252d", "expected_shortfall_975_252d", "worst_5_return_avg_252d",
    "worst_10_return_avg_252d", "lower_partial_moment_1_252d",
    "lower_partial_moment_2_252d", "max_loss_to_total_loss_ratio_252d",
    "tail_loss_concentration_5_252d"
]
total_inf = 0
for c in inf_cols:
    inf_count = result_table.filter(
        F.isnan(F.col(c)) | (F.col(c) == float('inf')) | (F.col(c) == float('-inf'))
    ).count()
    if inf_count > 0:
        print(f"  WARNING: {c} has {inf_count} inf/nan rows")
    total_inf += inf_count
print(f"Total infinity values: {total_inf}")

# Check sufficient_history_flag
sufficient_count = result_table.filter(F.col("sufficient_history_flag") == True).count()
insufficient_count = result_table.filter(F.col("sufficient_history_flag") == False).count()
print(f"\nsufficient_history_flag: {sufficient_count} TRUE, {insufficient_count} FALSE")

# SPY sample
print("\n=== SPY latest metrics ===")
spy_latest = (
    result_table
    .filter(F.col("asset_id") == "SPY")
    .orderBy(F.desc("trade_date"))
    .limit(1)
    .collect()[0]
)
for col_name in ["trade_date", "current_drawdown", "rolling_max_drawdown_252d",
                  "drawdown_duration_days", "sortino_60d", "calmar_252d",
                  "var_975_252d", "expected_shortfall_975_252d",
                  "worst_5_return_avg_252d", "worst_10_return_avg_252d",
                  "lower_partial_moment_1_252d", "lower_partial_moment_2_252d",
                  "max_loss_to_total_loss_ratio_252d", "tail_loss_concentration_5_252d",
                  "sufficient_history_flag", "data_quality_flag"]:
    print(f"  {col_name}: {getattr(spy_latest, col_name)}")

# Display sample rows
display(
    result_table
    .filter(F.col("asset_id").isin(["SPY", "AAPL", "BTC-USD", "ETH-USD"]))
    .orderBy(F.col("asset_id"), F.desc("trade_date"))
    .limit(20)
)