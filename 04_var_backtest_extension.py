# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — VaR Backtest Extension
# MAGIC
# MAGIC Extends the existing `gold.var_backtest_daily` table (Kupiec POF test, 1M+ rows) with:
# MAGIC
# MAGIC - **Christoffersen independence test** — detects clustered breaches via Markov-chain transition probabilities
# MAGIC - **Conditional coverage test** — joint test combining Kupiec + Christoffersen (chi-squared 2)
# MAGIC - **Breach clustering detection** — flags models where breaches correlate with high-volatility periods
# MAGIC - **GARCH forecast VaR evaluation** — backtests VaR from `analytics.market_forecasts_daily` if populated
# MAGIC
# MAGIC **Does NOT modify** `gold.var_backtest_daily` or the existing Downstream Extensions job — only reads and extends.
# MAGIC
# MAGIC **Source:** `workspace.gold.var_backtest_daily`, `workspace.gold.equity_risk_daily`, `workspace.gold.crypto_risk_daily`, `workspace.analytics.market_forecasts_daily`
# MAGIC **Target:** `workspace.analytics.var_backtests`

# COMMAND ----------

# DBTITLE 1,Imports & Christoffersen Test
import numpy as np
import pandas as pd
from scipy.stats import chi2
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

# ============================================================
# CHRISTOFFERSEN INDEPENDENCE TEST
# ============================================================

def christoffersen_independence_test(breaches, min_periods=20):
    """
    Christoffersen (1998) independence test.
    Uses a 2x2 transition matrix: P(breach|breach) vs P(breach|no breach).
    Returns (LR statistic, p_value) from chi-squared(1).
    """
    breaches = np.asarray(breaches, dtype=int)
    n = len(breaches)
    if n < min_periods:
        return np.nan, np.nan

    # Count transitions
    n00 = n01 = n10 = n11 = 0
    for i in range(1, n):
        if breaches[i - 1] == 0 and breaches[i] == 0:
            n00 += 1
        elif breaches[i - 1] == 0 and breaches[i] == 1:
            n01 += 1
        elif breaches[i - 1] == 1 and breaches[i] == 0:
            n10 += 1
        elif breaches[i - 1] == 1 and breaches[i] == 1:
            n11 += 1

    # P(breach | no breach) and P(breach | breach)
    denom_0 = n00 + n01
    denom_1 = n10 + n11
    pi01 = n01 / denom_0 if denom_0 > 0 else 0.0
    pi11 = n11 / denom_1 if denom_1 > 0 else 0.0
    pi = (n01 + n11) / (denom_0 + denom_1) if (denom_0 + denom_1) > 0 else 0.0

    # LR statistic
    if pi01 == 0 and pi11 == 0:
        return 0.0, 1.0

    # Log-likelihood under H0 (independence): all transitions have same breach prob
    # Use ENDING-state counts as exponents: (n00+n10) no-breach endings, (n01+n11) breach endings
    # NOT starting-state counts (denom_0, denom_1) — those give wrong exponents under H0
    end_no_breach = n00 + n10
    end_breach = n01 + n11
    # Log-likelihood under H1: different breach probs depending on previous state
    def safe_log(x):
        return np.log(x) if x > 0 else 0.0

    LR = -2.0 * (
        (end_no_breach * safe_log(1 - pi) + end_breach * safe_log(pi))
        - (n00 * safe_log(1 - pi01) + n01 * safe_log(pi01))
        - (n10 * safe_log(1 - pi11) + n11 * safe_log(pi11))
    )
    LR = max(LR, 0.0)
    p_value = 1.0 - chi2.cdf(LR, df=1)
    return float(LR), float(p_value)


def conditional_coverage_test(kupiec_stat, christoffersen_stat):
    """
    Conditional coverage test = Kupiec (unconditional) + Christoffersen (independence).
    LR statistic is chi-squared(2).
    """
    cc_stat = kupiec_stat + christoffersen_stat
    p_value = 1.0 - chi2.cdf(cc_stat, df=2)
    return float(cc_stat), float(p_value)


def max_consecutive_breaches(breaches):
    """Find the maximum run of consecutive breach flags."""
    breaches = np.asarray(breaches, dtype=int)
    max_run = 0
    current_run = 0
    for b in breaches:
        if b == 1:
            current_run += 1
            max_run = max(max_run, current_run)
        else:
            current_run = 0
    return int(max_run)


print("✅ Christoffersen independence test and helpers defined")

# COMMAND ----------

# DBTITLE 1,Compute Extended VaR Backtests (Historical)
# ============================================================
# COMPUTE EXTENDED VAR BACKTESTS FOR EXISTING HISTORICAL VAR
# ============================================================
# Source: workspace.gold.var_backtest_daily (already has Kupiec test results)
# We add: Christoffersen independence, conditional coverage, breach clustering,
#         max consecutive breaches, first/last breach dates

print("Loading existing VaR backtest data...")

# Load var_backtest_daily (do NOT cache — let Spark manage)
vb = spark.table("workspace.gold.var_backtest_daily")

# Load volatility for breach clustering detection with renamed columns to avoid join ambiguity
equity_vol = (
    spark.table("workspace.gold.equity_risk_daily")
    .select(F.col("entity_id").alias("v_asset_id"), F.col("trade_date").alias("v_trade_date"), F.col("volatility_20d"), F.lit("equity").alias("v_type"))
)
crypto_vol = (
    spark.table("workspace.gold.crypto_risk_daily")
    .select(F.col("entity_id").alias("v_asset_id"), F.col("trade_date").alias("v_trade_date"), F.col("volatility_20d"), F.lit("crypto").alias("v_type"))
)
vol_df = equity_vol.unionByName(crypto_vol)

# Join with renamed columns to avoid ambiguity
vb_with_vol = vb.join(
    vol_df,
    (vb.asset_id == vol_df.v_asset_id) & (vb.trade_date == vol_df.v_trade_date) & (vb.asset_type == vol_df.v_type),
    "left"
).drop("v_asset_id", "v_trade_date", "v_type")

print(f"Loaded VaR backtest data. Running applyInPandas...")
print("Computing extended backtest metrics per asset × confidence level...")

# Define schema for the Pandas function
RESULT_SCHEMA = T.StructType([
    T.StructField("asset_id", T.StringType(), False),
    T.StructField("asset_class", T.StringType(), False),
    T.StructField("confidence_level", T.StringType(), False),
    T.StructField("model_name", T.StringType(), False),
    T.StructField("evaluation_start", T.DateType(), True),
    T.StructField("evaluation_end", T.DateType(), True),
    T.StructField("total_observations", T.IntegerType(), True),
    T.StructField("expected_breaches", T.DoubleType(), True),
    T.StructField("actual_breaches", T.IntegerType(), True),
    T.StructField("breach_rate", T.DoubleType(), True),
    T.StructField("expected_breach_rate", T.DoubleType(), True),
    T.StructField("kupiec_statistic", T.DoubleType(), True),
    T.StructField("kupiec_p_value", T.DoubleType(), True),
    T.StructField("christoffersen_independence_stat", T.DoubleType(), True),
    T.StructField("christoffersen_independence_p_value", T.DoubleType(), True),
    T.StructField("conditional_coverage_stat", T.DoubleType(), True),
    T.StructField("conditional_coverage_p_value", T.DoubleType(), True),
    T.StructField("breach_clustering_flag", T.BooleanType(), True),
    T.StructField("first_breach_date", T.DateType(), True),
    T.StructField("last_breach_date", T.DateType(), True),
    T.StructField("max_consecutive_breaches", T.IntegerType(), True),
])

def compute_extended_backtest(pdf):
    """Compute extended VaR backtest metrics for one (asset, confidence_level) group."""
    pdf = pdf.sort_values("trade_date").copy()
    n = len(pdf)
    if n < 20:
        return pd.DataFrame(columns=[f.name for f in RESULT_SCHEMA.fields])

    asset_id = pdf["asset_id"].iloc[0]
    asset_class = pdf["asset_type"].iloc[0]  # asset_type in source -> asset_class in target
    conf = pdf["confidence_level"].iloc[0]

    # Filter to rows with sufficient history and valid data
    valid = pdf[pdf["sufficient_history_flag"] == True].copy()
    valid = valid[valid["breach_flag"].notna()]
    vn = len(valid)
    if vn < 20:
        return pd.DataFrame(columns=[f.name for f in RESULT_SCHEMA.fields])

    breach_flags = valid["breach_flag"].values.astype(int)
    vol_20d = valid["volatility_20d"].values.astype(float) if "volatility_20d" in valid.columns else np.full(vn, np.nan)
    trade_dates = valid["trade_date"].values

    # Actual vs expected breaches
    actual_breaches = int(breach_flags.sum())
    # Tail probability from confidence level
    if "99" in conf:
        p = 0.01
    else:
        p = 0.05
    expected_breaches = float(vn * p)
    breach_rate = actual_breaches / vn if vn > 0 else np.nan
    expected_breach_rate = p

    # Kupiec statistic (reuse from existing table — take the last valid value)
    kupiec_rows = pdf[pdf["kupiec_statistic"].notna()]
    kupiec_stat = float(kupiec_rows["kupiec_statistic"].iloc[-1]) if len(kupiec_rows) > 0 else np.nan
    kupiec_pval = float(kupiec_rows["kupiec_p_value"].iloc[-1]) if len(kupiec_rows) > 0 else np.nan

    # Christoffersen independence test
    christ_stat, christ_pval = christoffersen_independence_test(breach_flags)

    # Conditional coverage test
    cc_stat, cc_pval = conditional_coverage_test(kupiec_stat, christ_stat)

    # Breach clustering: correlation between breach_flag and volatility_20d
    valid_vol = vol_20d[np.isfinite(vol_20d)]
    valid_breach = breach_flags[np.isfinite(vol_20d)]
    if len(valid_vol) > 10 and np.std(valid_vol) > 0 and np.std(valid_breach) > 0:
        corr = float(np.corrcoef(valid_vol, valid_breach)[0, 1])
    else:
        corr = 0.0
    # Flag if positive correlation > 0.1 (breaches cluster in high-vol periods)
    clustering_flag = bool(corr > 0.1)

    # First/last breach dates
    breach_dates = trade_dates[breach_flags == 1]
    first_breach = breach_dates[0] if len(breach_dates) > 0 else None
    last_breach = breach_dates[-1] if len(breach_dates) > 0 else None

    # Max consecutive breaches
    max_consec = max_consecutive_breaches(breach_flags)

    result = pd.DataFrame([{
        "asset_id": asset_id,
        "asset_class": asset_class,
        "confidence_level": conf,
        "model_name": "historical",
        "evaluation_start": pd.to_datetime(valid["trade_date"].iloc[0]).date(),
        "evaluation_end": pd.to_datetime(valid["trade_date"].iloc[-1]).date(),
        "total_observations": int(vn),
        "expected_breaches": expected_breaches,
        "actual_breaches": actual_breaches,
        "breach_rate": breach_rate,
        "expected_breach_rate": expected_breach_rate,
        "kupiec_statistic": kupiec_stat,
        "kupiec_p_value": kupiec_pval,
        "christoffersen_independence_stat": christ_stat,
        "christoffersen_independence_p_value": christ_pval,
        "conditional_coverage_stat": cc_stat,
        "conditional_coverage_p_value": cc_pval,
        "breach_clustering_flag": clustering_flag,
        "first_breach_date": pd.to_datetime(first_breach).date() if first_breach is not None else None,
        "last_breach_date": pd.to_datetime(last_breach).date() if last_breach is not None else None,
        "max_consecutive_breaches": max_consec,
    }])
    return result

# Run using applyInPandas for parallelization
historical_results = (
    vb_with_vol
    .groupBy("asset_id", "asset_type", "confidence_level")
    .applyInPandas(compute_extended_backtest, schema=RESULT_SCHEMA)
    .withColumn("updated_at", F.current_timestamp())
)

print(f"Computed extended backtests: {historical_results.count()} rows")
historical_results.printSchema()

# COMMAND ----------

# DBTITLE 1,Evaluate GARCH Forecast VaR
# ============================================================
# EVALUATE GARCH FORECAST VAR FROM market_forecasts_daily
# ============================================================
# If the GARCH engine has populated market_forecasts_daily with VaR forecasts,
# we backtest those VaR estimates against realized returns.
# If the table is empty (GARCH not yet run), we skip gracefully.

print("Checking for GARCH forecast VaR data...")

forecast_count = spark.table("workspace.analytics.market_forecasts_daily").count()
print(f"market_forecasts_daily rows: {forecast_count}")

garch_results = None

if forecast_count > 0:
    # Load forecasts with VaR — only horizon=1 for daily VaR backtesting
    forecasts = spark.table("workspace.analytics.market_forecasts_daily").filter(F.col("horizon") == 1)

    # Load realized returns from risk tables
    equity_returns = (
        spark.table("workspace.gold.equity_risk_daily")
        .select(F.col("entity_id").alias("asset_id"), F.col("trade_date"), F.col("return_1d"))
        .withColumn("asset_class", F.lit("equity"))
    )
    crypto_returns = (
        spark.table("workspace.gold.crypto_risk_daily")
        .select(F.col("entity_id").alias("asset_id"), F.col("trade_date"), F.col("return_1d"))
        .withColumn("asset_class", F.lit("crypto"))
    )
    returns_df = equity_returns.unionByName(crypto_returns)

    # Load volatility for breach clustering
    vol_df2 = (
        spark.table("workspace.gold.equity_risk_daily")
        .select(F.col("entity_id").alias("asset_id"), F.col("trade_date").alias("vol_date"), F.col("volatility_20d"))
        .unionByName(
            spark.table("workspace.gold.crypto_risk_daily")
            .select(F.col("entity_id").alias("asset_id"), F.col("trade_date").alias("vol_date"), F.col("volatility_20d"))
        )
    )

    # The forecast VaR on as_of_date applies to the NEXT trading day
    # We need to match forecast.as_of_date to the PREVIOUS trading day's return
    # i.e., forecast made on date t, return realized on date t+1
    # For simplicity: forecast.as_of_date = t, realized return = return on t+1
    # We join on as_of_date == the lag of trade_date

    # Get next-day return by shifting
    from pyspark.sql.window import Window
    w_asset = Window.partitionBy("asset_id").orderBy("trade_date")
    returns_with_next = returns_df.withColumn(
        "next_trade_date", F.lead("trade_date").over(w_asset)
    ).withColumn(
        "next_return_1d", F.lead("return_1d").over(w_asset)
    )

    # Join forecasts to returns: forecast as_of_date matches return trade_date
    # (the forecast is made AFTER observing returns through as_of_date)
    # The VaR forecast applies to the NEXT period's return
    garch_var_df = (
        forecasts.select(
            F.col("asset_id"), F.col("as_of_date"), F.col("asset_class"),
            F.col("model_name"), F.col("model_distribution"),
            F.col("var_95"), F.col("var_99")
        )
        .join(
            returns_with_next.select(
                F.col("asset_id").alias("r_asset_id"),
                F.col("trade_date").alias("r_trade_date"),
                F.col("next_return_1d")
            ),
            F.col("as_of_date") == F.col("r_trade_date"),
            "inner"
        )
        .drop("r_asset_id", "r_trade_date")
        .withColumnRenamed("as_of_date", "forecast_date")
        .withColumnRenamed("next_return_1d", "realized_return")
        .filter(F.col("realized_return").isNotNull() & F.col("var_95").isNotNull())
    )

    # Add volatility for breach clustering (rename to avoid ambiguous columns)
    vol_df2 = vol_df2.select(
        F.col("asset_id").alias("v2_asset_id"),
        F.col("vol_date").alias("v2_vol_date"),
        F.col("volatility_20d")
    )
    garch_var_df = garch_var_df.join(
        vol_df2,
        (F.col("asset_id") == F.col("v2_asset_id")) & (F.col("forecast_date") == F.col("v2_vol_date")),
        "left"
    ).drop("v2_asset_id", "v2_vol_date")

    print(f"GARCH VaR backtest rows: {garch_var_df.count()}")

    # Schema for GARCH backtest results (same as historical)
    GARCH_SCHEMA = RESULT_SCHEMA

    def compute_garch_backtest(pdf):
        """Compute VaR backtest for one (asset, model, confidence_level) group from GARCH forecasts."""
        pdf = pdf.sort_values("forecast_date").copy()
        n = len(pdf)
        if n < 20:
            return pd.DataFrame(columns=[f.name for f in GARCH_SCHEMA.fields])

        asset_id = pdf["asset_id"].iloc[0]
        asset_class = pdf["asset_class"].iloc[0]
        model_name = pdf["model_name"].iloc[0]

        results = []
        for conf_label, var_col, p in [("95%", "var_95", 0.05), ("99%", "var_99", 0.01)]:
            var_vals = pdf[var_col].values.astype(float)
            returns = pdf["realized_return"].values.astype(float)
            vol = pdf["volatility_20d"].values.astype(float) if "volatility_20d" in pdf.columns else np.full(n, np.nan)
            dates = pdf["forecast_date"].values

            # Breach: return < -VaR (VaR is positive loss magnitude)
            breach = np.where(np.isfinite(var_vals) & np.isfinite(returns), (returns < -var_vals).astype(int), 0)

            actual = int(breach.sum())
            expected = float(n * p)
            breach_rate = actual / n if n > 0 else np.nan

            # Kupiec POF test
            from math import log, erfc, sqrt
            x = actual
            N = n
            p_hat = x / N if N > 0 else 0
            if x == 0:
                LR_kupiec = -2.0 * N * np.log(1.0 - p) if (1.0 - p) > 0 else 0.0
            elif x == N:
                LR_kupiec = -2.0 * N * np.log(p) if p > 0 else 0.0
            else:
                LR_kupiec = -2.0 * (
                    (N - x) * np.log(1.0 - p) + x * np.log(p)
                    - (N - x) * np.log(1.0 - p_hat) - x * np.log(p_hat)
                )
            LR_kupiec = max(LR_kupiec, 0.0)
            kupiec_pval = float(1.0 - chi2.cdf(LR_kupiec, df=1))

            # Christoffersen independence
            christ_stat, christ_pval = christoffersen_independence_test(breach)

            # Conditional coverage
            cc_stat, cc_pval = conditional_coverage_test(LR_kupiec, christ_stat)

            # Breach clustering
            valid_vol = vol[np.isfinite(vol)]
            valid_breach = breach[np.isfinite(vol)]
            if len(valid_vol) > 10 and np.std(valid_vol) > 0 and np.std(valid_breach) > 0:
                corr = float(np.corrcoef(valid_vol, valid_breach)[0, 1])
            else:
                corr = 0.0
            clustering = bool(corr > 0.1)

            # First/last breach dates
            breach_dates = dates[breach == 1]
            first_b = breach_dates[0] if len(breach_dates) > 0 else None
            last_b = breach_dates[-1] if len(breach_dates) > 0 else None

            results.append({
                "asset_id": asset_id,
                "asset_class": asset_class,
                "confidence_level": conf_label,
                "model_name": model_name,
                "evaluation_start": pd.to_datetime(dates[0]).date(),
                "evaluation_end": pd.to_datetime(dates[-1]).date(),
                "total_observations": int(n),
                "expected_breaches": expected,
                "actual_breaches": actual,
                "breach_rate": breach_rate,
                "expected_breach_rate": p,
                "kupiec_statistic": float(LR_kupiec),
                "kupiec_p_value": kupiec_pval,
                "christoffersen_independence_stat": christ_stat,
                "christoffersen_independence_p_value": christ_pval,
                "conditional_coverage_stat": cc_stat,
                "conditional_coverage_p_value": cc_pval,
                "breach_clustering_flag": clustering,
                "first_breach_date": pd.to_datetime(first_b).date() if first_b is not None else None,
                "last_breach_date": pd.to_datetime(last_b).date() if last_b is not None else None,
                "max_consecutive_breaches": max_consecutive_breaches(breach),
            })

        return pd.DataFrame(results)

    garch_results = (
        garch_var_df
        .groupBy("asset_id", "asset_class", "model_name")
        .applyInPandas(compute_garch_backtest, schema=GARCH_SCHEMA)
        .withColumn("updated_at", F.current_timestamp())
    )
    print(f"GARCH VaR backtest results: {garch_results.count()} rows")
else:
    print("⚠️  market_forecasts_daily is empty — GARCH VaR backtest skipped (will run once GARCH engine populates it)")


# COMMAND ----------

# DBTITLE 1,Write to var_backtests Table
# ============================================================
# WRITE RESULTS TO workspace.analytics.var_backtests
# ============================================================

TARGET_TABLE = "workspace.analytics.var_backtests"

# Combine historical and GARCH results
all_results = historical_results
if garch_results is not None:
    all_results = historical_results.unionByName(garch_results)

print(f"Total VaR backtest results to write: {all_results.count()}")

# Idempotent overwrite (atomic — no data loss on partial failure)
all_results.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

print(f"✅ Written to {TARGET_TABLE}")
print(f"Total rows: {spark.table(TARGET_TABLE).count()}")
print(f"Unique assets: {spark.table(TARGET_TABLE).select('asset_id').distinct().count()}")
print(f"Models evaluated: {spark.table(TARGET_TABLE).select('model_name').distinct().collect()}")

# COMMAND ----------

# DBTITLE 1,Summary & Validation
# ============================================================
# SUMMARY & VALIDATION
# ============================================================

TARGET = spark.table(TARGET_TABLE)

print("=" * 79)
print("VaR BACKTEST EXTENSION SUMMARY")
print("=" * 79)

# Overall stats
print(f"\nTotal evaluations: {TARGET.count()}")
print(f"Assets: {TARGET.select('asset_id').distinct().count()}")
print(f"Models: {[r.model_name for r in TARGET.select('model_name').distinct().collect()]}")

# Flag poorly calibrated VaR (Kupiec p < 0.05)
print("\n--- Poorly Calibrated VaR (Kupiec p < 0.05) ---")
poor_kupiec = TARGET.filter(F.col("kupiec_p_value") < 0.05).select(
    "asset_id", "confidence_level", "model_name", "actual_breaches", "expected_breaches",
    "kupiec_p_value"
).orderBy(F.col("kupiec_p_value").asc())
poor_kupiec.show(20, truncate=False)

# Flag independence violations (Christoffersen p < 0.05)
print("\n--- Breach Independence Violations (Christoffersen p < 0.05) ---")
poor_independence = TARGET.filter(F.col("christoffersen_independence_p_value") < 0.05).select(
    "asset_id", "confidence_level", "model_name",
    "christoffersen_independence_stat", "christoffersen_independence_p_value",
    "max_consecutive_breaches"
).orderBy(F.col("christoffersen_independence_p_value").asc())
poor_independence.show(20, truncate=False)

# Flag breach clustering
print("\n--- Breach Clustering Detected ---")
clustering = TARGET.filter(F.col("breach_clustering_flag") == True).select(
    "asset_id", "confidence_level", "model_name", "breach_rate", "expected_breach_rate"
)
clustering.show(20, truncate=False)

# Summary by model
print("\n--- Summary by Model ---")
TARGET.groupBy("model_name", "confidence_level").agg(
    F.count("*").alias("evaluations"),
    F.avg("breach_rate").alias("avg_breach_rate"),
    F.avg("expected_breach_rate").alias("avg_expected_rate"),
    F.avg("kupiec_p_value").alias("avg_kupiec_p"),
    F.avg("christoffersen_independence_p_value").alias("avg_christ_p"),
    F.sum(F.when(F.col("breach_clustering_flag") == True, 1).otherwise(0)).alias("clustering_count")
).orderBy("model_name", "confidence_level").show(20, truncate=False)

print("\n" + "=" * 79)
print("✅ VaR backtest extension complete")
print("=" * 79)