-- 07: Validate var_backtests table after VaR backtest extension
-- Checks Christoffersen test coverage and breach clustering

SELECT 
  'var_backtests' AS table_name,
  COUNT(*) AS total_evaluations,
  COUNT(DISTINCT asset_id) AS unique_assets,
  COUNT(DISTINCT model_name) AS unique_models,
  SUM(CASE WHEN kupiec_p_value < 0.05 THEN 1 ELSE 0 END) AS kupiec_failures,
  SUM(CASE WHEN christoffersen_independence_p_value < 0.05 THEN 1 ELSE 0 END) AS christoffersen_failures,
  SUM(CASE WHEN conditional_coverage_p_value < 0.05 THEN 1 ELSE 0 END) AS conditional_coverage_failures,
  SUM(CASE WHEN breach_clustering_flag = true THEN 1 ELSE 0 END) AS clustering_detected,
  ROUND(AVG(breach_rate) * 100, 4) AS avg_breach_rate_pct,
  ROUND(AVG(expected_breach_rate) * 100, 4) AS avg_expected_rate_pct
FROM workspace.analytics.var_backtests;

-- Summary by model and confidence level
SELECT 
  model_name,
  confidence_level,
  COUNT(*) AS evaluations,
  ROUND(AVG(breach_rate) * 100, 4) AS avg_breach_rate_pct,
  ROUND(AVG(expected_breach_rate) * 100, 4) AS avg_expected_rate_pct,
  ROUND(AVG(kupiec_p_value), 4) AS avg_kupiec_p,
  ROUND(AVG(christoffersen_independence_p_value), 4) AS avg_christ_p,
  SUM(CASE WHEN breach_clustering_flag = true THEN 1 ELSE 0 END) AS clustering_count
FROM workspace.analytics.var_backtests
GROUP BY model_name, confidence_level
ORDER BY model_name, confidence_level;