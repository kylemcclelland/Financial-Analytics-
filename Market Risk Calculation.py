# Databricks notebook source
# DBTITLE 1,Market Risk Calculation
# MAGIC %md
# MAGIC # Market Risk Calculation
# MAGIC
# MAGIC This notebook calculates market risk features for equity, crypto, and cross-asset portfolios. Run as separate Databricks job tasks with `mode` parameter set to `equity`, `crypto`, `cross_asset`, or `all`.
# MAGIC
# MAGIC **Recommended DAG:**
# MAGIC
# MAGIC * Stock Silver → `equity`
# MAGIC * Crypto Silver → `crypto`
# MAGIC * `equity` + `crypto` → `cross_asset`
# MAGIC
# MAGIC **Outputs:**
# MAGIC
# MAGIC * `gold.equity_risk_daily`
# MAGIC * `gold.crypto_risk_daily`
# MAGIC * `gold.cross_asset_risk_daily`
# MAGIC
# MAGIC **Notes:**
# MAGIC
# MAGIC * This script calculates risk features, not source ingestion.
# MAGIC * It assumes one daily close per asset/date.
# MAGIC * Adjust the source table/column settings to match your exact Silver tables.

# COMMAND ----------

# DBTITLE 1,Imports
import math
import numpy as np
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DoubleType, TimestampType
)
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Job Parameter
# ============================================================
# 1. JOB PARAMETER
# ============================================================

# In a Databricks notebook, this creates a task parameter.
# For three Job tasks, point all three tasks at this notebook and
# pass mode=equity, mode=crypto, and mode=cross_asset.

try:
    dbutils.widgets.dropdown(
        "mode",
        "equity",
        ["equity", "crypto", "cross_asset", "all"]
    )
    MODE = dbutils.widgets.get("mode")
except Exception:
    # Useful if running as a .py file outside notebook widgets.
    MODE = "equity"

MODE = MODE.lower().strip()

if MODE not in {"equity", "crypto", "cross_asset", "all"}:
    raise ValueError(
        "mode must be equity, crypto, cross_asset, or all"
    )

# COMMAND ----------

# DBTITLE 1,Configuration
# ============================================================
# 2. CONFIGURATION
# ============================================================

# ---------- EQUITY ----------
EQUITY_SOURCE_TABLE = "workspace.bronze.market_prices_daily"
EQUITY_ID_COL = "ticker"
EQUITY_DATE_COL = "trade_date"
EQUITY_CLOSE_COL = "close_price"
EQUITY_VOLUME_COL = "volume"

EQUITY_TARGET_TABLE = "gold.equity_risk_daily"

# ---------- CRYPTO ----------
CRYPTO_SOURCE_TABLE = "workspace.silver.crypto_candles_clean"

# Change this to product_id if your Silver crypto table uses
# Coinbase-style IDs such as BTC-USD.
CRYPTO_ID_COL = "product_id"

CRYPTO_DATE_COL = "trade_date"
CRYPTO_CLOSE_COL = "close_price"
CRYPTO_VOLUME_COL = "volume"

CRYPTO_TARGET_TABLE = "gold.crypto_risk_daily"

# ---------- CROSS-ASSET ----------
CROSS_ASSET_TARGET_TABLE = "gold.cross_asset_risk_daily"

# These must match entity_id values in the Gold risk tables.
EQUITY_BENCHMARK_ID = "SPY"
BTC_ID = "BTC-USD"
ETH_ID = "ETH-USD"

# COMMAND ----------

# DBTITLE 1,Risk Settings
# ============================================================
# 3. RISK SETTINGS
# ============================================================

VOL_WINDOW_SHORT = 20
VOL_WINDOW_LONG = 60

# VaR/ES estimation window.
TAIL_WINDOW = 252

# RiskMetrics-style daily EWMA decay.
EWMA_LAMBDA = 0.94

# Cross-asset windows.
CORR_WINDOW_SHORT = 30
CORR_WINDOW_LONG = 90
BETA_WINDOW = 60

# Annualization.
EQUITY_ANNUALIZATION = 252
CRYPTO_ANNUALIZATION = 365

# COMMAND ----------

# DBTITLE 1,Create Gold Schema
# ============================================================
# 4. CREATE GOLD SCHEMA
# ============================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# DBTITLE 1,Helpers — Risk Calculations
# ============================================================
# 5. HELPERS — RISK CALCULATIONS
# ============================================================

def rolling_expected_shortfall(values, tail_probability):
    """
    Positive loss magnitude of the average return in the
    lower tail.

    Example:
        tail_probability=0.05 -> 95% Expected Shortfall.
    """
    x = np.asarray(values, dtype=float)
    x = x[np.isfinite(x)]

    if len(x) == 0:
        return np.nan

    cutoff = np.quantile(x, tail_probability)
    tail = x[x <= cutoff]

    if len(tail) == 0:
        return np.nan

    return float(-tail.mean())


def calculate_asset_risk(pdf, annualization):
    """
    Calculate daily risk features for one asset.

    Input columns:
        entity_id
        trade_date
        close_price
        volume
    """

    pdf = pdf.sort_values("trade_date").copy()

    pdf["close_price"] = pd.to_numeric(
        pdf["close_price"], errors="coerce"
    )

    pdf["volume"] = pd.to_numeric(
        pdf["volume"], errors="coerce"
    )

    # --------------------------------------------------------
    # RETURNS
    # --------------------------------------------------------

    pdf["return_1d"] = pdf["close_price"].pct_change()

    pdf["log_return"] = np.log(
        pdf["close_price"] / pdf["close_price"].shift(1)
    )

    # --------------------------------------------------------
    # ROLLING VOLATILITY
    # --------------------------------------------------------

    pdf["volatility_20d"] = (
        pdf["log_return"]
        .rolling(
            VOL_WINDOW_SHORT,
            min_periods=VOL_WINDOW_SHORT
        )
        .std()
        * math.sqrt(annualization)
    )

    pdf["volatility_60d"] = (
        pdf["log_return"]
        .rolling(
            VOL_WINDOW_LONG,
            min_periods=VOL_WINDOW_LONG
        )
        .std()
        * math.sqrt(annualization)
    )

    # --------------------------------------------------------
    # EWMA VOLATILITY
    # --------------------------------------------------------

    # alpha = 1 - lambda.
    # The result is annualized for comparability with rolling vol.

    pdf["ewma_volatility"] = (
        pdf["log_return"]
        .ewm(
            alpha=(1.0 - EWMA_LAMBDA),
            adjust=False,
            min_periods=VOL_WINDOW_SHORT
        )
        .std(bias=False)
        * math.sqrt(annualization)
    )

    # --------------------------------------------------------
    # DRAWDOWN
    # --------------------------------------------------------

    running_peak = pdf["close_price"].cummax()

    pdf["drawdown"] = (
        pdf["close_price"] / running_peak
    ) - 1.0

    # Worst observed running drawdown over the prior 252
    # observations. This is deliberately named "worst_drawdown"
    # rather than "max_drawdown" because it is based on the
    # running drawdown series.

    pdf["worst_drawdown_252obs"] = (
        pdf["drawdown"]
        .rolling(252, min_periods=20)
        .min()
    )

    # --------------------------------------------------------
    # HISTORICAL VaR
    # --------------------------------------------------------

    returns = pdf["return_1d"]

    pdf["var_95"] = -(
        returns
        .rolling(TAIL_WINDOW, min_periods=60)
        .quantile(0.05)
    )

    pdf["var_99"] = -(
        returns
        .rolling(TAIL_WINDOW, min_periods=60)
        .quantile(0.01)
    )

    # --------------------------------------------------------
    # EXPECTED SHORTFALL
    # --------------------------------------------------------

    pdf["expected_shortfall_95"] = (
        returns
        .rolling(TAIL_WINDOW, min_periods=60)
        .apply(
            lambda x: rolling_expected_shortfall(x, 0.05),
            raw=True
        )
    )

    pdf["expected_shortfall_99"] = (
        returns
        .rolling(TAIL_WINDOW, min_periods=60)
        .apply(
            lambda x: rolling_expected_shortfall(x, 0.01),
            raw=True
        )
    )

    # --------------------------------------------------------
    # DOWNSIDE DEVIATION
    # --------------------------------------------------------

    def downside_std(x):
        x = np.asarray(x, dtype=float)
        x = x[np.isfinite(x)]
        downside = x[x < 0]

        if len(downside) < 2:
            return np.nan

        return float(
            np.std(downside, ddof=1)
            * math.sqrt(annualization)
        )

    pdf["downside_deviation_60d"] = (
        returns
        .rolling(60, min_periods=20)
        .apply(downside_std, raw=True)
    )

    # --------------------------------------------------------
    # MOMENTUM / TRAILING RETURN
    # --------------------------------------------------------

    pdf["return_5d"] = (
        pdf["close_price"]
        / pdf["close_price"].shift(5)
        - 1.0
    )

    pdf["return_20d"] = (
        pdf["close_price"]
        / pdf["close_price"].shift(20)
        - 1.0
    )

    pdf["return_60d"] = (
        pdf["close_price"]
        / pdf["close_price"].shift(60)
        - 1.0
    )

    return pdf

# COMMAND ----------

# DBTITLE 1,Helpers — Spark Integration
# ============================================================
# 5b. HELPERS — SPARK INTEGRATION
# ============================================================

RISK_SCHEMA = StructType([
    StructField("entity_id", StringType(), False),
    StructField("trade_date", DateType(), False),
    StructField("close_price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("return_1d", DoubleType(), True),
    StructField("log_return", DoubleType(), True),
    StructField("return_5d", DoubleType(), True),
    StructField("return_20d", DoubleType(), True),
    StructField("return_60d", DoubleType(), True),
    StructField("volatility_20d", DoubleType(), True),
    StructField("volatility_60d", DoubleType(), True),
    StructField("ewma_volatility", DoubleType(), True),
    StructField("drawdown", DoubleType(), True),
    StructField("worst_drawdown_252obs", DoubleType(), True),
    StructField("var_95", DoubleType(), True),
    StructField("var_99", DoubleType(), True),
    StructField("expected_shortfall_95", DoubleType(), True),
    StructField("expected_shortfall_99", DoubleType(), True),
    StructField("downside_deviation_60d", DoubleType(), True)
])


def normalize_source(
    source_table,
    id_col,
    date_col,
    close_col,
    volume_col
):
    """
    Normalize different Silver source schemas into one common
    shape for risk calculation.
    """

    df = spark.table(source_table)

    if volume_col in df.columns:
        volume_expr = F.col(volume_col).cast("double")
    else:
        volume_expr = F.lit(None).cast("double")

    return (
        df.select(
            F.col(id_col).cast("string").alias("entity_id"),
            F.to_date(F.col(date_col)).alias("trade_date"),
            F.col(close_col).cast("double").alias("close_price"),
            volume_expr.alias("volume")
        )
        .filter(
            F.col("entity_id").isNotNull()
            & F.col("trade_date").isNotNull()
            & F.col("close_price").isNotNull()
            & (F.col("close_price") > 0)
        )
        .dropDuplicates(["entity_id", "trade_date"])
    )


def calculate_risk_table(
    source_df,
    annualization,
    asset_type
):
    """
    Distributed per-asset calculation using applyInPandas.
    """

    def grouped_calc(pdf):
        out = calculate_asset_risk(
            pdf,
            annualization=annualization
        )

        return out[[
            "entity_id",
            "trade_date",
            "close_price",
            "volume",
            "return_1d",
            "log_return",
            "return_5d",
            "return_20d",
            "return_60d",
            "volatility_20d",
            "volatility_60d",
            "ewma_volatility",
            "drawdown",
            "worst_drawdown_252obs",
            "var_95",
            "var_99",
            "expected_shortfall_95",
            "expected_shortfall_99",
            "downside_deviation_60d"
        ]]

    result = (
        source_df
        .groupBy("entity_id")
        .applyInPandas(
            grouped_calc,
            schema=RISK_SCHEMA
        )
        .withColumn(
            "asset_type",
            F.lit(asset_type)
        )
        .withColumn(
            "calculation_timestamp",
            F.current_timestamp()
        )
    )

    return result


def merge_risk_table(df, target_table):
    """
    Idempotent Delta MERGE on entity_id + trade_date.
    """

    if not spark.catalog.tableExists(target_table):

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        print(f"Created {target_table}")
        return

    target = DeltaTable.forName(
        spark,
        target_table
    )

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            """
            target.entity_id = source.entity_id
            AND target.trade_date = source.trade_date
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Merged into {target_table}")

# COMMAND ----------

# DBTITLE 1,Equity Risk
# ============================================================
# 6. EQUITY RISK
# ============================================================

def run_equity_risk():

    print("\n========================================")
    print("CALCULATING EQUITY RISK")
    print("========================================")

    equity_source = normalize_source(
        source_table=EQUITY_SOURCE_TABLE,
        id_col=EQUITY_ID_COL,
        date_col=EQUITY_DATE_COL,
        close_col=EQUITY_CLOSE_COL,
        volume_col=EQUITY_VOLUME_COL
    )

    equity_risk = calculate_risk_table(
        source_df=equity_source,
        annualization=EQUITY_ANNUALIZATION,
        asset_type="equity"
    )

    merge_risk_table(
        equity_risk,
        EQUITY_TARGET_TABLE
    )

    display(
        equity_risk
        .orderBy(
            F.col("trade_date").desc(),
            F.col("entity_id")
        )
        .limit(100)
    )

# COMMAND ----------

# DBTITLE 1,Crypto Risk
# ============================================================
# 7. CRYPTO RISK
# ============================================================

def run_crypto_risk():

    print("\n========================================")
    print("CALCULATING CRYPTO RISK")
    print("========================================")

    crypto_source = normalize_source(
        source_table=CRYPTO_SOURCE_TABLE,
        id_col=CRYPTO_ID_COL,
        date_col=CRYPTO_DATE_COL,
        close_col=CRYPTO_CLOSE_COL,
        volume_col=CRYPTO_VOLUME_COL
    )

    crypto_risk = calculate_risk_table(
        source_df=crypto_source,
        annualization=CRYPTO_ANNUALIZATION,
        asset_type="crypto"
    )

    merge_risk_table(
        crypto_risk,
        CRYPTO_TARGET_TABLE
    )

    display(
        crypto_risk
        .orderBy(
            F.col("trade_date").desc(),
            F.col("entity_id")
        )
        .limit(100)
    )

# COMMAND ----------

# DBTITLE 1,Cross-Asset Risk
# ============================================================
# 8. CROSS-ASSET RISK
# ============================================================

def rolling_beta(asset_returns, market_returns, window):
    covariance = (
        asset_returns
        .rolling(window, min_periods=max(20, window // 2))
        .cov(market_returns)
    )

    market_variance = (
        market_returns
        .rolling(window, min_periods=max(20, window // 2))
        .var()
    )

    return covariance / market_variance


def run_cross_asset_risk():

    print("\n========================================")
    print("CALCULATING CROSS-ASSET RISK")
    print("========================================")

    if not spark.catalog.tableExists(EQUITY_TARGET_TABLE):
        raise RuntimeError(
            f"{EQUITY_TARGET_TABLE} does not exist. "
            "Run equity risk first."
        )

    if not spark.catalog.tableExists(CRYPTO_TARGET_TABLE):
        raise RuntimeError(
            f"{CRYPTO_TARGET_TABLE} does not exist. "
            "Run crypto risk first."
        )

    equity = (
        spark.table(EQUITY_TARGET_TABLE)
        .filter(F.col("entity_id") == EQUITY_BENCHMARK_ID)
        .select(
            "trade_date",
            F.col("return_1d").alias("equity_return"),
            F.col("volatility_20d").alias("equity_volatility_20d")
        )
        .orderBy("trade_date")
        .toPandas()
    )

    crypto = (
        spark.table(CRYPTO_TARGET_TABLE)
        .filter(
            F.col("entity_id").isin(
                [BTC_ID, ETH_ID]
            )
        )
        .select(
            "entity_id",
            "trade_date",
            "return_1d",
            "volatility_20d"
        )
        .orderBy("trade_date")
        .toPandas()
    )

    if equity.empty:
        raise RuntimeError(
            f"Benchmark {EQUITY_BENCHMARK_ID} was not found in "
            f"{EQUITY_TARGET_TABLE}. Add {EQUITY_BENCHMARK_ID} "
            "to your equity price universe."
        )

    if crypto.empty:
        raise RuntimeError(
            "No configured crypto assets were found. "
            f"Expected {BTC_ID} and/or {ETH_ID}."
        )

    equity["trade_date"] = pd.to_datetime(
        equity["trade_date"]
    )

    crypto["trade_date"] = pd.to_datetime(
        crypto["trade_date"]
    )

    # Start with equity benchmark.
    cross = equity.copy()

    # --------------------------------------------------------
    # BTC
    # --------------------------------------------------------

    btc = (
        crypto[
            crypto["entity_id"] == BTC_ID
        ][[
            "trade_date",
            "return_1d",
            "volatility_20d"
        ]]
        .rename(
            columns={
                "return_1d": "btc_return",
                "volatility_20d": "btc_volatility_20d"
            }
        )
    )

    cross = cross.merge(
        btc,
        on="trade_date",
        how="left"
    )

    # --------------------------------------------------------
    # ETH
    # --------------------------------------------------------

    eth = (
        crypto[
            crypto["entity_id"] == ETH_ID
        ][[
            "trade_date",
            "return_1d",
            "volatility_20d"
        ]]
        .rename(
            columns={
                "return_1d": "eth_return",
                "volatility_20d": "eth_volatility_20d"
            }
        )
    )

    cross = cross.merge(
        eth,
        on="trade_date",
        how="left"
    )

    # --------------------------------------------------------
    # ROLLING CORRELATIONS
    # --------------------------------------------------------

    if "btc_return" in cross.columns:

        cross["btc_spy_corr_30d"] = (
            cross["btc_return"]
            .rolling(
                CORR_WINDOW_SHORT,
                min_periods=20
            )
            .corr(
                cross["equity_return"]
            )
        )

        cross["btc_spy_corr_90d"] = (
            cross["btc_return"]
            .rolling(
                CORR_WINDOW_LONG,
                min_periods=45
            )
            .corr(
                cross["equity_return"]
            )
        )

        cross["btc_spy_beta_60d"] = rolling_beta(
            cross["btc_return"],
            cross["equity_return"],
            BETA_WINDOW
        )

        cross["btc_equity_vol_ratio"] = (
            cross["btc_volatility_20d"]
            / cross["equity_volatility_20d"]
        )

    else:
        cross["btc_spy_corr_30d"] = np.nan
        cross["btc_spy_corr_90d"] = np.nan
        cross["btc_spy_beta_60d"] = np.nan
        cross["btc_equity_vol_ratio"] = np.nan

    if "eth_return" in cross.columns:

        cross["eth_spy_corr_30d"] = (
            cross["eth_return"]
            .rolling(
                CORR_WINDOW_SHORT,
                min_periods=20
            )
            .corr(
                cross["equity_return"]
            )
        )

        cross["eth_spy_corr_90d"] = (
            cross["eth_return"]
            .rolling(
                CORR_WINDOW_LONG,
                min_periods=45
            )
            .corr(
                cross["equity_return"]
            )
        )

        cross["eth_spy_beta_60d"] = rolling_beta(
            cross["eth_return"],
            cross["equity_return"],
            BETA_WINDOW
        )

        cross["eth_equity_vol_ratio"] = (
            cross["eth_volatility_20d"]
            / cross["equity_volatility_20d"]
        )

    else:
        cross["eth_spy_corr_30d"] = np.nan
        cross["eth_spy_corr_90d"] = np.nan
        cross["eth_spy_beta_60d"] = np.nan
        cross["eth_equity_vol_ratio"] = np.nan

    # --------------------------------------------------------
    # CLEAN OUTPUT
    # --------------------------------------------------------

    output_columns = [
        "trade_date",
        "btc_spy_corr_30d",
        "btc_spy_corr_90d",
        "btc_spy_beta_60d",
        "btc_equity_vol_ratio",
        "eth_spy_corr_30d",
        "eth_spy_corr_90d",
        "eth_spy_beta_60d",
        "eth_equity_vol_ratio"
    ]

    for c in output_columns[1:]:
        if c not in cross.columns:
            cross[c] = np.nan

    cross = cross[output_columns].copy()

    cross["trade_date"] = (
        pd.to_datetime(cross["trade_date"])
        .dt.date
    )

    cross = cross.replace(
        [np.inf, -np.inf],
        np.nan
    )

    cross_schema = StructType([
        StructField("trade_date", DateType(), False),
        StructField("btc_spy_corr_30d", DoubleType(), True),
        StructField("btc_spy_corr_90d", DoubleType(), True),
        StructField("btc_spy_beta_60d", DoubleType(), True),
        StructField("btc_equity_vol_ratio", DoubleType(), True),
        StructField("eth_spy_corr_30d", DoubleType(), True),
        StructField("eth_spy_corr_90d", DoubleType(), True),
        StructField("eth_spy_beta_60d", DoubleType(), True),
        StructField("eth_equity_vol_ratio", DoubleType(), True)
    ])

    cross_spark = (
        spark.createDataFrame(
            cross,
            schema=cross_schema
        )
        .withColumn(
            "calculation_timestamp",
            F.current_timestamp()
        )
    )

    if not spark.catalog.tableExists(
        CROSS_ASSET_TARGET_TABLE
    ):

        (
            cross_spark.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(
                CROSS_ASSET_TARGET_TABLE
            )
        )

    else:

        target = DeltaTable.forName(
            spark,
            CROSS_ASSET_TARGET_TABLE
        )

        (
            target.alias("target")
            .merge(
                cross_spark.alias("source"),
                "target.trade_date = source.trade_date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(
        f"Merged into {CROSS_ASSET_TARGET_TABLE}"
    )

    display(
        cross_spark
        .orderBy(
            F.col("trade_date").desc()
        )
        .limit(100)
    )

# COMMAND ----------

# DBTITLE 1,Execution
# ============================================================
# 9. EXECUTION
# ============================================================

if MODE == "equity":
    run_equity_risk()

elif MODE == "crypto":
    run_crypto_risk()

elif MODE == "cross_asset":
    run_cross_asset_risk()

elif MODE == "all":
    run_equity_risk()
    run_crypto_risk()
    run_cross_asset_risk()

# COMMAND ----------

# DBTITLE 1,Summary
# ============================================================
# 10. SUMMARY
# ============================================================

print("\n========================================")
print("MARKET RISK CALCULATION COMPLETE")
print("========================================")
print(f"Mode: {MODE}")
print(f"Equity output: {EQUITY_TARGET_TABLE}")
print(f"Crypto output: {CRYPTO_TARGET_TABLE}")
print(f"Cross-asset output: {CROSS_ASSET_TARGET_TABLE}")
print("========================================")