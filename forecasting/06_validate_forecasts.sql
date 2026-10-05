-- 06: Validate forecast_backtests table after GARCH engine run
-- Checks row counts, asset coverage, and model diversity

SELECT 
  'forecast_backtests' AS table_name,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT asset_id) AS unique_assets,
  COUNT(DISTINCT model_name) AS unique_models,
  COUNT(DISTINCT model_distribution) AS unique_distributions,
  MIN(forecast_date) AS earliest_forecast,
  MAX(forecast_date) AS latest_forecast
FROM workspace.analytics.forecast_backtests;

-- Per-model row counts
SELECT 
  model_name,
  model_distribution,
  COUNT(*) AS row_count,
  COUNT(DISTINCT asset_id) AS asset_count,
  AVG(predicted_volatility) AS avg_predicted_vol,
  AVG(realized_volatility) AS avg_realized_vol,
  AVG(forecast_error) AS avg_forecast_error
FROM workspace.analytics.forecast_backtests
GROUP BY model_name, model_distribution
ORDER BY model_name, model_distribution;