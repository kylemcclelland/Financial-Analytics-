-- 09: Pipeline execution summary
-- Row counts for all analytics tables populated by this job

SELECT 'forecast_backtests' AS table_name, COUNT(*) AS row_count FROM workspace.analytics.forecast_backtests
UNION ALL
SELECT 'model_performance', COUNT(*) FROM workspace.analytics.model_performance
UNION ALL
SELECT 'model_diagnostics', COUNT(*) FROM workspace.analytics.model_diagnostics
UNION ALL
SELECT 'market_forecasts_daily', COUNT(*) FROM workspace.analytics.market_forecasts_daily
UNION ALL
SELECT 'forecast_model_registry', COUNT(*) FROM workspace.analytics.forecast_model_registry
UNION ALL
SELECT 'var_backtests', COUNT(*) FROM workspace.analytics.var_backtests
UNION ALL
SELECT 'forecasting_data_audit', COUNT(*) FROM workspace.analytics.forecasting_data_audit
ORDER BY table_name;

-- Convergence diagnostics summary
SELECT 
  model_name,
  model_distribution,
  COUNT(*) AS total_fits,
  SUM(CASE WHEN convergence_status = true THEN 1 ELSE 0 END) AS converged,
  SUM(CASE WHEN convergence_status = false THEN 1 ELSE 0 END) AS failed,
  ROUND(AVG(aic), 4) AS avg_aic,
  ROUND(AVG(bic), 4) AS avg_bic,
  ROUND(AVG(log_likelihood), 4) AS avg_loglik
FROM workspace.analytics.model_diagnostics
GROUP BY model_name, model_distribution
ORDER BY model_name, model_distribution;