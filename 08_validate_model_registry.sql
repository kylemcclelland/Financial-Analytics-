-- 08: Validate forecast_model_registry after champion/challenger selection
-- Checks champion distribution and improvement thresholds

SELECT 
  'forecast_model_registry' AS table_name,
  COUNT(*) AS total_entries,
  SUM(CASE WHEN champion_model IN ('garch', 'egarch', 'gjr') THEN 1 ELSE 0 END) AS garch_champions,
  SUM(CASE WHEN champion_model NOT IN ('garch', 'egarch', 'gjr') THEN 1 ELSE 0 END) AS baseline_champions,
  ROUND(AVG(champion_score), 6) AS avg_champion_qlike,
  ROUND(AVG(improvement_vs_baseline_pct), 4) AS avg_improvement_pct
FROM workspace.analytics.forecast_model_registry;

-- Champion model distribution
SELECT 
  champion_model,
  champion_distribution,
  COUNT(*) AS asset_count,
  ROUND(AVG(champion_score), 6) AS avg_qlike,
  ROUND(AVG(improvement_vs_baseline_pct), 4) AS avg_improvement_pct
FROM workspace.analytics.forecast_model_registry
GROUP BY champion_model, champion_distribution
ORDER BY asset_count DESC;