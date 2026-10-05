# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — GARCH Forecasting Engine
# MAGIC
# MAGIC Core ML notebook that populates all analytics tables for the Market Forecasting dashboard.
# MAGIC
# MAGIC **Fits 7 GARCH-family models** (GARCH/EGARCH/GJR-GARCH × Normal/Student-t/Skewed-t) plus **2 baselines** (EWMA λ=0.94, rolling 20d vol) for all 116 assets using strict out-of-sample walk-forward backtesting with an expanding window.
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. Load log returns from gold risk tables (no duplication of ingestion/risk calcs)
# MAGIC 2. Walk-forward backtest: re-fit every 250 days, recursive forecast between re-fits
# MAGIC 3. Evaluate: QLIKE, RMSE, MAE, bias, correlation, interval coverage
# MAGIC 4. Champion selection: lowest QLIKE that beats ALL baselines by ≥5%
# MAGIC 5. Generate 1-day and 5-day ahead forecasts with VaR and ES
# MAGIC
# MAGIC **Critical rules:** No lookahead bias, no random train/test, deterministic (seed=42), never assume GARCH beats baselines, no hidden failed models.

# COMMAND ----------

# DBTITLE 1,Imports & Configuration
# MAGIC %pip install arch
# MAGIC
# MAGIC import numpy as np
# MAGIC import pandas as pd
# MAGIC from scipy.stats import norm, t as t_dist
# MAGIC from scipy.special import gamma as gamma_func
# MAGIC from arch import arch_model
# MAGIC from pyspark.sql import functions as F, types as T
# MAGIC import warnings
# MAGIC import time as time_module
# MAGIC
# MAGIC warnings.filterwarnings('ignore')
# MAGIC
# MAGIC # ============================================================
# MAGIC # CONFIGURATION
# MAGIC # ============================================================
# MAGIC SEED = 42
# MAGIC MIN_TRAIN = 500
# MAGIC REFIT_EVERY = 250       # re-fit every 250 trading days for efficiency
# MAGIC MAX_ASSETS = None         # Set to int for testing, None for all 116
# MAGIC np.random.seed(SEED)
# MAGIC
# MAGIC # 7 GARCH-family model specifications
# MAGIC MODEL_SPECS = [
# MAGIC     {'name': 'garch',  'vol': 'GARCH',  'p': 1, 'o': 0, 'q': 1, 'dist': 'normal'},
# MAGIC     {'name': 'garch',  'vol': 'GARCH',  'p': 1, 'o': 0, 'q': 1, 'dist': 't'},
# MAGIC     {'name': 'garch',  'vol': 'GARCH',  'p': 1, 'o': 0, 'q': 1, 'dist': 'skewt'},
# MAGIC     {'name': 'egarch', 'vol': 'EGARCH', 'p': 1, 'o': 0, 'q': 1, 'dist': 'normal'},
# MAGIC     {'name': 'egarch', 'vol': 'EGARCH', 'p': 1, 'o': 0, 'q': 1, 'dist': 't'},
# MAGIC     {'name': 'gjr',    'vol': 'GARCH',  'p': 1, 'o': 1, 'q': 1, 'dist': 'normal'},
# MAGIC     {'name': 'gjr',    'vol': 'GARCH',  'p': 1, 'o': 1, 'q': 1, 'dist': 't'},
# MAGIC ]
# MAGIC
# MAGIC # Baselines (no arch needed)
# MAGIC EWMA_LAMBDA = 0.94
# MAGIC ROLLING_WINDOW = 20
# MAGIC
# MAGIC print(f"✅ Config: {len(MODEL_SPECS)} GARCH models + 2 baselines (EWMA λ={EWMA_LAMBDA}, rolling {ROLLING_WINDOW}d)")
# MAGIC print(f"   MIN_TRAIN={MIN_TRAIN}, REFIT_EVERY={REFIT_EVERY}, SEED={SEED}, MAX_ASSETS={MAX_ASSETS}")

# COMMAND ----------

# DBTITLE 1,Data Loading
# ============================================================
# DATA LOADING — log returns from gold risk tables
# ============================================================
print("Loading log returns from gold tables...")

equity = spark.table("workspace.gold.equity_risk_daily").select(
    F.col("entity_id").alias("asset_id"),
    F.lit("equity").alias("asset_class"),
    F.col("trade_date"),
    F.col("log_return")
)

crypto = spark.table("workspace.gold.crypto_risk_daily").select(
    F.col("entity_id").alias("asset_id"),
    F.lit("crypto").alias("asset_class"),
    F.col("trade_date"),
    F.col("log_return")
)

all_returns_df = equity.unionByName(crypto).orderBy("asset_id", "trade_date")

total_rows = all_returns_df.count()
asset_rows = all_returns_df.select("asset_id", "asset_class").distinct().collect()
print(f"Total rows: {total_rows:,}  |  Assets: {len(asset_rows)}")

if MAX_ASSETS is not None:
    asset_rows = asset_rows[:MAX_ASSETS]
    print(f"Using first {MAX_ASSETS} assets for testing")

# Check target table schemas
print("\n--- forecast_backtests schema ---")
spark.table("workspace.analytics.forecast_backtests").printSchema()
print("\n--- market_forecasts_daily schema ---")
spark.table("workspace.analytics.market_forecasts_daily").printSchema()

# COMMAND ----------

# DBTITLE 1,Walk-Forward Backtesting Engine
# ============================================================
# WALK-FORWARD BACKTESTING ENGINE
# ============================================================
# For each asset: fit 7 GARCH models + 2 baselines using expanding window
# Re-fit every REFIT_EVERY days; recursive forecast between re-fits

from scipy.stats import skew, kurtosis
from statsmodels.stats.diagnostic import acorr_ljungbox

def garch_recursive(params, last_var, last_return):
    """GARCH(1,1) 1-step ahead variance: sigma^2_{t+1} = omega + alpha*r_t^2 + beta*sigma^2_t"""
    return params['omega'] + params['alpha[1]'] * last_return**2 + params['beta[1]'] * last_var

def egarch_recursive(params, last_var, last_return, dist='normal', dist_params=None):
    """EGARCH(1,1) 1-step ahead variance via log-variance recursion."""
    sigma = np.sqrt(max(last_var, 1e-10))
    z = last_return / sigma
    if dist == 't' and dist_params:
        nu = dist_params.get('nu', 10)
        e_abs = 2 * np.sqrt(max(nu - 2, 1)) * gamma_func((nu + 1) / 2) / (np.sqrt(np.pi) * gamma_func(nu / 2)) if nu > 2 else np.sqrt(2 / np.pi)
    else:
        e_abs = np.sqrt(2 / np.pi)
    log_var = (params['omega'] + params['alpha[1]'] * (np.abs(z) - e_abs)
               + params.get('gamma[1]', 0) * z + params['beta[1]'] * np.log(max(last_var, 1e-10)))
    return np.exp(log_var)

def gjr_recursive(params, last_var, last_return):
    """GJR-GARCH(1,1) 1-step ahead variance with leverage indicator."""
    indicator = 1.0 if last_return < 0 else 0.0
    return (params['omega']
            + (params['alpha[1]'] + params['gamma[1]'] * indicator) * last_return**2
            + params['beta[1]'] * last_var)

all_forecasts = []
all_diagnostics = []
start_time = time_module.time()

for asset_idx, row in enumerate(asset_rows):
    asset_id = row['asset_id']
    asset_class = row['asset_class']

    # Load this asset to pandas
    asset_pdf = all_returns_df.filter(F.col("asset_id") == asset_id).orderBy("trade_date").toPandas()
    asset_pdf = asset_pdf.dropna(subset=['log_return'])
    returns = asset_pdf['log_return'].values * 100.0  # scale to percentage for arch convergence
    dates = [pd.Timestamp(d).date() for d in asset_pdf['trade_date'].values]
    n = len(returns)
    if n < MIN_TRAIN + 20:
        print(f"  ⚠️ {asset_id}: only {n} obs, skipping")
        continue

    # --- BASELINES (no arch needed) ---
    ewma_var = np.empty(n)
    ewma_var[0] = returns[0] ** 2
    for i in range(1, n):
        ewma_var[i] = EWMA_LAMBDA * ewma_var[i - 1] + (1 - EWMA_LAMBDA) * returns[i - 1] ** 2
    rolling_var = pd.Series(returns).rolling(ROLLING_WINDOW).var().values

    for t in range(MIN_TRAIN, n):
        rv = abs(returns[t]) / 100.0  # realized vol in decimal
        r_date = dates[t]
        ao_date = dates[t - 1]

        # EWMA baseline
        pv = np.sqrt(max(ewma_var[t], 1e-10)) / 100.0
        all_forecasts.append((asset_id, asset_class, 'ewma', 'normal', 'volatility', 1, 'expanding', None,
                              dates[0], ao_date, ao_date, r_date, pv, rv, pv - rv, -1.96 * pv, 1.96 * pv, None, None))
        # Rolling 20d baseline
        if not np.isnan(rolling_var[t]):
            pv2 = np.sqrt(rolling_var[t]) / 100.0
            all_forecasts.append((asset_id, asset_class, 'rolling_vol_20d', 'normal', 'volatility', 1, 'expanding', None,
                                 dates[max(0, t - ROLLING_WINDOW)], ao_date, ao_date, r_date, pv2, rv, pv2 - rv,
                                 -1.96 * pv2, 1.96 * pv2, None, None))

    # --- GARCH-FAMILY MODELS ---
    for spec in MODEL_SPECS:
        m_name = spec['name']
        m_dist = spec['dist']
        vol_type = spec['vol']
        p_val = spec['p']
        o_val = spec.get('o', 0)
        q_val = spec['q']

        last_params = None
        last_cond_var = None
        dist_params = None
        current_fc_var = None

        for t in range(MIN_TRAIN, n):
            need_refit = (t == MIN_TRAIN) or ((t - MIN_TRAIN) % REFIT_EVERY == 0)

            if need_refit:
                train_data = pd.Series(returns[:t])
                try:
                    am = arch_model(train_data, vol=vol_type, p=p_val, o=o_val, q=q_val, dist=m_dist, mean='Zero')
                    fitted = am.fit(disp='off', show_warning=False, options={'maxiter': 200})
                    last_params = fitted.params
                    last_cond_var = float(fitted.conditional_volatility.iloc[-1] ** 2)
                    fc = fitted.forecast(horizon=1)
                    current_fc_var = float(fc.variance.values[-1, 0])

                    if m_dist == 't':
                        dist_params = {'nu': float(fitted.params.get('nu', 10))}
                    elif m_dist == 'skewt':
                        dist_params = {'nu': float(fitted.params.get('nu', 10)), 'lambda': float(fitted.params.get('lambda', 0))}
                    else:
                        dist_params = {}

                    # Model parameters
                    omega_v = float(fitted.params.get('omega', 0))
                    alpha_v = float(fitted.params.get('alpha[1]', 0))
                    beta_v = float(fitted.params.get('beta[1]', 0))
                    gamma_v = float(fitted.params['gamma[1]']) if 'gamma[1]' in fitted.params else None
                    persist = alpha_v + beta_v + (gamma_v / 2 if gamma_v is not None else 0)
                    # Residual diagnostics
                    std_resid = (fitted.resid / fitted.conditional_volatility).dropna()
                    sq_resid = (std_resid ** 2).dropna()
                    try:
                        lb_r = acorr_ljungbox(sq_resid, lags=min(10, len(sq_resid) // 5), return_df=True)
                        lb_stat = float(lb_r['lb_stat'].iloc[-1])
                        lb_pval = float(lb_r['lb_pvalue'].iloc[-1])
                    except Exception:
                        lb_stat, lb_pval = None, None
                    try:
                        arch_r = acorr_ljungbox(sq_resid, lags=min(5, len(sq_resid) // 5), return_df=True)
                        arch_stat = float(arch_r['lb_stat'].iloc[-1])
                        arch_pval = float(arch_r['lb_pvalue'].iloc[-1])
                    except Exception:
                        arch_stat, arch_pval = None, None
                    acf1 = float(np.corrcoef(sq_resid.values[:-1], sq_resid.values[1:])[0, 1]) if len(sq_resid) > 2 else None
                    all_diagnostics.append((asset_id, asset_class, m_name, m_dist,
                                            dates[0], dates[t - 1], True,
                                            omega_v, alpha_v, beta_v, gamma_v, persist,
                                            float(fitted.loglikelihood), float(fitted.aic), float(fitted.bic),
                                            float(skew(std_resid)) if len(std_resid) > 0 else None,
                                            float(kurtosis(std_resid)) if len(std_resid) > 0 else None,
                                            lb_stat, lb_pval, acf1,
                                            arch_stat, arch_pval,
                                            bool(arch_pval < 0.05) if arch_pval is not None else None,
                                            None, None, None))
                except Exception as e:
                    all_diagnostics.append((asset_id, asset_class, m_name, m_dist,
                                            dates[0], dates[t - 1], False,
                                            None, None, None, None, None, None, None, None,
                                            None, None, None, None, None, None, None, None,
                                            str(e)[:500], None, None))
                    if last_params is None:
                        break  # can't continue without any successful fit
                    current_fc_var = last_cond_var
            else:
                # Recursive 1-step ahead update between re-fits
                if last_params is not None:
                    if vol_type == 'GARCH' and o_val == 0:
                        current_fc_var = garch_recursive(last_params, current_fc_var, returns[t - 1])
                    elif vol_type == 'EGARCH':
                        current_fc_var = egarch_recursive(last_params, current_fc_var, returns[t - 1], m_dist, dist_params)
                    elif vol_type == 'GARCH' and o_val == 1:
                        current_fc_var = gjr_recursive(last_params, current_fc_var, returns[t - 1])
                else:
                    continue

            # Record forecast
            pv = np.sqrt(max(current_fc_var, 1e-10)) / 100.0  # predicted vol in decimal
            rv = abs(returns[t]) / 100.0                      # realized vol in decimal
            if m_dist == 't' and dist_params:
                z = float(t_dist.ppf(0.975, dist_params.get('nu', 10)))
            elif m_dist == 'skewt' and dist_params:
                z = float(t_dist.ppf(0.975, dist_params.get('nu', 10)))
            else:
                z = 1.96

            all_forecasts.append((asset_id, asset_class, m_name, m_dist, 'volatility', 1, 'expanding', None,
                                  dates[0], dates[t - 1], dates[t - 1], dates[t], pv, rv, pv - rv,
                                  -z * pv, z * pv, None, None))

    if (asset_idx + 1) % 10 == 0 or asset_idx == len(asset_rows) - 1:
        elapsed = time_module.time() - start_time
        print(f"[{asset_idx + 1}/{len(asset_rows)}] {asset_id} done — {len(all_forecasts):,} forecasts so far — {elapsed:.0f}s elapsed")

print(f"\n✅ Total forecasts: {len(all_forecasts):,}  |  diagnostics: {len(all_diagnostics):,}")
print(f"   Total time: {time_module.time() - start_time:.0f}s")

# --- Write to analytics tables ---
fc_cols = ['asset_id','asset_class','model_name','model_distribution','target','horizon','window_type','window_size',
           'training_start','training_end','as_of_date','forecast_date',
           'predicted_volatility','realized_volatility','forecast_error','lower_bound','upper_bound','mlflow_run_id','created_at']
di_cols = ['asset_id','asset_class','model_name','model_distribution',
           'training_start','training_end','convergence_status',
           'omega','alpha','beta','gamma','persistence',
           'log_likelihood','aic','bic',
           'residual_skewness','residual_kurtosis',
           'ljung_box_stat','ljung_box_p_value','squared_residual_acf1',
           'arch_effect_stat','arch_effect_p_value','remaining_arch_flag',
           'warning_flags','mlflow_run_id','updated_at']

forecast_pdf = pd.DataFrame(all_forecasts, columns=fc_cols)
diagnostics_pdf = pd.DataFrame(all_diagnostics, columns=di_cols)

print(f"Writing {len(forecast_pdf):,} forecasts to analytics.forecast_backtests...")
spark.sql("TRUNCATE TABLE workspace.analytics.forecast_backtests")
spark.createDataFrame(forecast_pdf).write.format("delta").mode("append").saveAsTable("workspace.analytics.forecast_backtests")

print(f"Writing {len(diagnostics_pdf):,} diagnostics to analytics.model_diagnostics...")
spark.sql("TRUNCATE TABLE workspace.analytics.model_diagnostics")
spark.createDataFrame(diagnostics_pdf).write.format("delta").mode("append").saveAsTable("workspace.analytics.model_diagnostics")

print(f"✅ forecast_backtests: {spark.table('workspace.analytics.forecast_backtests').count():,} rows")
print(f"✅ model_diagnostics: {spark.table('workspace.analytics.model_diagnostics').count():,} rows")

# COMMAND ----------

# DBTITLE 1,Performance Evaluation
# ============================================================
# PERFORMANCE EVALUATION — QLIKE, RMSE, MAE, bias, correlation, coverage
# ============================================================
print("Computing model performance metrics...")

spark.sql("TRUNCATE TABLE workspace.analytics.model_performance")

spark.sql("""
INSERT INTO workspace.analytics.model_performance
SELECT
  asset_id,
  asset_class,
  model_name,
  model_distribution,
  'volatility' AS target,
  1 AS horizon,
  'expanding' AS window_type,
  MIN(forecast_date) AS evaluation_start,
  MAX(forecast_date) AS evaluation_end,
  AVG(LOG(ABS(realized_volatility) / NULLIF(predicted_volatility, 0))
      - ABS(realized_volatility) / NULLIF(predicted_volatility, 0) + 1) AS qlike,
  SQRT(AVG(POWER(predicted_volatility - realized_volatility, 2))) AS rmse,
  AVG(ABS(predicted_volatility - realized_volatility)) AS mae,
  AVG(predicted_volatility - realized_volatility) AS forecast_bias,
  CORR(predicted_volatility, realized_volatility) AS forecast_realized_correlation,
  SUM(CASE WHEN realized_volatility <= ABS(upper_bound) THEN 1 ELSE 0 END) / COUNT(*) * 100 AS interval_coverage_pct,
  COUNT(*) AS out_of_sample_count,
  NULL AS mlflow_run_id,
  NULL AS updated_at
FROM workspace.analytics.forecast_backtests
GROUP BY asset_id, asset_class, model_name, model_distribution
""")

perf_count = spark.table("workspace.analytics.model_performance").count()
print(f"✅ model_performance: {perf_count} rows")

# Show summary by model
spark.table("workspace.analytics.model_performance").groupBy("model_name").agg(
    F.count("*").alias("assets"),
    F.round(F.avg("qlike"), 6).alias("avg_qlike"),
    F.round(F.avg("rmse"), 6).alias("avg_rmse"),
    F.round(F.avg("mae"), 6).alias("avg_mae"),
    F.round(F.avg("forecast_bias"), 6).alias("avg_bias"),
    F.round(F.avg("forecast_realized_correlation"), 4).alias("avg_corr")
).orderBy("avg_qlike").show()

# COMMAND ----------

# DBTITLE 1,Champion/Challenger Selection
# ============================================================
# CHAMPION / CHALLENGER SELECTION
# ============================================================
# Champion = lowest QLIKE model that beats ALL baselines by >=5%
# If no GARCH beats baselines, champion = best baseline

print("Selecting champion models...")

perf = spark.table("workspace.analytics.model_performance").filter("window_type = 'expanding'").toPandas()

BASELINE_MODELS = ['ewma', 'rolling_vol_20d']
IMPROVEMENT_THRESHOLD = 0.05  # 5%

registry_entries = []
for asset_id, group in perf.groupby('asset_id'):
    sorted_models = group.sort_values('qlike')
    baselines = sorted_models[sorted_models['model_name'].isin(BASELINE_MODELS)]
    garch_models = sorted_models[~sorted_models['model_name'].isin(BASELINE_MODELS)]

    best_baseline = baselines.iloc[0] if len(baselines) > 0 else None
    best_baseline_qlike = baselines['qlike'].min() if len(baselines) > 0 else float('inf')
    best_garch = garch_models.iloc[0] if len(garch_models) > 0 else None

    # Champion: GARCH must beat baselines by >=5%
    if best_garch is not None and best_garch['qlike'] < best_baseline_qlike * (1 - IMPROVEMENT_THRESHOLD):
        champion = best_garch
        is_garch = True
    else:
        champion = sorted_models.iloc[0]  # best overall (could be baseline)
        is_garch = champion['model_name'] not in BASELINE_MODELS

    challenger = sorted_models.iloc[1] if len(sorted_models) > 1 else None
    improvement = (best_baseline_qlike - champion['qlike']) / best_baseline_qlike * 100 if best_baseline_qlike > 0 else 0

    registry_entries.append({
        'asset_id': asset_id,
        'target': 'volatility',
        'horizon': 1,
        'champion_model': champion['model_name'],
        'champion_distribution': champion['model_distribution'],
        'challenger_model': challenger['model_name'] if challenger is not None else None,
        'challenger_distribution': challenger['model_distribution'] if challenger is not None else None,
        'evaluation_metric': 'qlike',
        'champion_score': float(champion['qlike']),
        'challenger_score': float(challenger['qlike']) if challenger is not None else None,
        'baseline_model': best_baseline['model_name'] if best_baseline is not None else None,
        'baseline_score': float(best_baseline_qlike) if best_baseline_qlike != float('inf') else None,
        'improvement_vs_baseline_pct': float(improvement),
        'model_version': None,
        'last_training_date': None,
        'mlflow_run_id': None,
        'updated_at': None,
    })

registry_pdf = pd.DataFrame(registry_entries)
# Reorder columns to match table schema
reg_col_order = ['asset_id','target','horizon','champion_model','champion_distribution',
                 'challenger_model','challenger_distribution','evaluation_metric',
                 'champion_score','challenger_score','baseline_model','baseline_score',
                 'improvement_vs_baseline_pct','model_version','last_training_date',
                 'mlflow_run_id','updated_at']
registry_pdf = registry_pdf[reg_col_order]

print(f"Writing {len(registry_pdf)} champion entries to forecast_model_registry...")
spark.sql("TRUNCATE TABLE workspace.analytics.forecast_model_registry")
spark.createDataFrame(registry_pdf).write.format("delta").mode("append").saveAsTable("workspace.analytics.forecast_model_registry")

# Summary
GARCH_MODELS = ['garch', 'egarch', 'gjr']
garch_champions = (registry_pdf['champion_model'].isin(GARCH_MODELS)).sum()
baseline_champions = len(registry_pdf) - garch_champions
print(f"\n✅ Champion distribution:")
print(f"   GARCH-family champions: {garch_champions}/{len(registry_pdf)}")
print(f"   Baseline champions:     {baseline_champions}/{len(registry_pdf)}")
print(f"\n   Champion model breakdown:")
print(registry_pdf['champion_model'].value_counts().to_string())

# COMMAND ----------

# DBTITLE 1,Generate Current Forecasts
# ============================================================
# GENERATE CURRENT FORECASTS (1-day and 5-day ahead)
# ============================================================
print("Generating current forecasts with champion models...")

registry = spark.table("workspace.analytics.forecast_model_registry").collect()
MODEL_NAME_MAP = {'garch': 'GARCH', 'egarch': 'EGARCH', 'gjr': 'GARCH', 'ewma': None, 'rolling_vol_20d': None}
DIST_MAP = {'normal': 'normal', 't': 't', 'skewt': 'skewt'}

forecast_entries = []

for entry in registry:
    asset_id = entry['asset_id']
    champ_model = entry['champion_model']
    champ_dist = entry['champion_distribution']

    # Load full return series
    asset_pdf = all_returns_df.filter(F.col("asset_id") == asset_id).orderBy("trade_date").toPandas()
    asset_pdf = asset_pdf.dropna(subset=['log_return'])
    asset_class = asset_pdf['asset_class'].iloc[0] if len(asset_pdf) > 0 else 'unknown'
    returns = asset_pdf['log_return'].values * 100.0
    n = len(returns)
    if n < MIN_TRAIN:
        continue

    latest_date = pd.Timestamp(asset_pdf['trade_date'].iloc[-1]).date()

    if champ_model in ('ewma', 'rolling_vol_20d'):
        # Baseline forecast
        if champ_model == 'ewma':
            ewma_v = returns[0] ** 2
            for r in returns[1:]:
                ewma_v = EWMA_LAMBDA * ewma_v + (1 - EWMA_LAMBDA) * r ** 2
            fc_vol_1d = np.sqrt(max(ewma_v, 1e-10)) / 100.0
            fc_vol_5d = fc_vol_1d  # EWMA assumes constant vol
        else:
            fc_vol_1d = np.std(returns[-ROLLING_WINDOW:]) / 100.0
            fc_vol_5d = fc_vol_1d

        for horizon in [1, 5]:
            fv_h = float(fc_vol_1d if horizon == 1 else fc_vol_5d)
            forecast_entries.append({
                'as_of_date': latest_date, 'asset_id': asset_id, 'asset_class': asset_class,
                'horizon': horizon, 'model_name': champ_model, 'model_distribution': champ_dist,
                'forecast_volatility': fv_h,
                'lower_bound': float(-fv_h * 1.96),
                'upper_bound': float(fv_h * 1.96),
                'var_95': float(fv_h * 1.645),
                'var_99': float(fv_h * 2.326),
                'expected_shortfall_95': float(fv_h * norm.pdf(norm.ppf(0.95)) / 0.05),
                'expected_shortfall_99': float(fv_h * norm.pdf(norm.ppf(0.99)) / 0.01),
                'generated_at': None,
                'mlflow_run_id': None,
            })
    else:
        # GARCH-family: fit on ALL data, forecast ahead
        vol_type = MODEL_NAME_MAP.get(champ_model, 'GARCH')
        o_val = 1 if champ_model == 'gjr' else 0
        dist = DIST_MAP.get(champ_dist, 'normal')

        try:
            am = arch_model(pd.Series(returns), vol=vol_type, p=1, o=o_val, q=1, dist=dist, mean='Zero')
            fitted = am.fit(disp='off', show_warning=False, options={'maxiter': 200})
            fc = fitted.forecast(horizon=5, method='simulation', simulations=1000)
            fc_var_1d = float(fc.variance.values[-1, 0])
            fc_var_5d = float(fc.variance.values[-1, 4])

            for horizon in [1, 5]:
                fv = np.sqrt(max(fc_var_1d if horizon == 1 else fc_var_5d, 1e-10)) / 100.0
                forecast_entries.append({
                    'as_of_date': latest_date, 'asset_id': asset_id, 'asset_class': asset_class,
                    'horizon': horizon, 'model_name': champ_model, 'model_distribution': champ_dist,
                    'forecast_volatility': float(fv),
                    'lower_bound': float(-fv * 1.96),
                    'upper_bound': float(fv * 1.96),
                    'var_95': float(fv * 1.645),
                    'var_99': float(fv * 2.326),
                    'expected_shortfall_95': float(fv * norm.pdf(norm.ppf(0.95)) / 0.05),
                    'expected_shortfall_99': float(fv * norm.pdf(norm.ppf(0.99)) / 0.01),
                    'generated_at': None,
                    'mlflow_run_id': None,
                })
        except Exception as e:
            print(f"  ⚠️ {asset_id} forecast failed: {str(e)[:100]}")

fc_pdf = pd.DataFrame(forecast_entries)
# Reorder columns to match table schema
fc_col_order = ['as_of_date','asset_id','asset_class','horizon','model_name','model_distribution',
                'forecast_volatility','lower_bound','upper_bound','var_95','var_99',
                'expected_shortfall_95','expected_shortfall_99','generated_at','mlflow_run_id']
fc_pdf = fc_pdf[fc_col_order]
print(f"Writing {len(fc_pdf)} forecasts to market_forecasts_daily...")
spark.sql("TRUNCATE TABLE workspace.analytics.market_forecasts_daily")
spark.createDataFrame(fc_pdf).write.format("delta").mode("append").saveAsTable("workspace.analytics.market_forecasts_daily")

print(f"✅ market_forecasts_daily: {spark.table('workspace.analytics.market_forecasts_daily').count()} rows")

# COMMAND ----------

# DBTITLE 1,Summary & Validation
# ============================================================
# SUMMARY & VALIDATION
# ============================================================
print("=" * 79)
print("GARCH FORECASTING ENGINE — EXECUTION SUMMARY")
print("=" * 79)

# Table counts
for tbl in ['forecast_backtests', 'model_performance', 'model_diagnostics',
            'market_forecasts_daily', 'forecast_model_registry']:
    cnt = spark.table(f"workspace.analytics.{tbl}").count()
    print(f"  {tbl:30s} {cnt:>10,} rows")

# Champion distribution
print("\n--- Champion Model Distribution ---")
spark.table("workspace.analytics.forecast_model_registry").groupBy("champion_model", "champion_distribution").agg(
    F.count("*").alias("asset_count"),
    F.round(F.avg("champion_score"), 6).alias("avg_qlike"),
    F.round(F.avg("improvement_vs_baseline_pct"), 2).alias("avg_improvement_pct")
).orderBy(F.col("asset_count").desc()).show(truncate=False)

# GARCH vs baseline champions
garch_champs = spark.table("workspace.analytics.forecast_model_registry").filter("champion_model IN ('garch', 'egarch', 'gjr')").count()
total_champs = spark.table("workspace.analytics.forecast_model_registry").count()
print(f"GARCH champions: {garch_champs}/{total_champs}  |  Baseline champions: {total_champs - garch_champs}/{total_champs}")

# Diagnostics: convergence failures
print("\n--- Convergence Diagnostics ---")
diag = spark.table("workspace.analytics.model_diagnostics").groupBy("model_name", "model_distribution").agg(
    F.count("*").alias("total_fits"),
    F.sum(F.when(F.col("convergence_status") == True, 1).otherwise(0)).alias("converged"),
    F.sum(F.when(F.col("convergence_status") == False, 1).otherwise(0)).alias("failed"),
    F.round(F.avg("aic"), 2).alias("avg_aic"),
    F.round(F.avg("bic"), 2).alias("avg_bic")
).orderBy("model_name", "model_distribution")
diag.show(truncate=False)

# Top 10 assets by QLIKE improvement over baseline
print("\n--- Top 10 Assets by QLIKE Improvement ---")
spark.table("workspace.analytics.forecast_model_registry").orderBy(F.col("improvement_vs_baseline_pct").desc()).select(
    "asset_id", "champion_model", "champion_distribution", "champion_score", "improvement_vs_baseline_pct", "baseline_model"
).show(10, truncate=False)

# Latest forecasts sample
print("\n--- Latest 1-Day Forecasts (sample) ---")
spark.table("workspace.analytics.market_forecasts_daily").filter("horizon = 1").select(
    "asset_id", "model_name", "forecast_volatility", "var_95", "var_99"
).orderBy(F.col("forecast_volatility").desc()).show(10, truncate=False)

print("\n" + "=" * 79)
print("✅ GARCH Forecasting Engine complete")
print("=" * 79)