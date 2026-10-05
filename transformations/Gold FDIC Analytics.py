# Databricks notebook source
# DBTITLE 1,Gold FDIC Analytics Header
# MAGIC %md
# MAGIC # Gold FDIC Analytics
# MAGIC
# MAGIC Creates Gold-layer analytical tables from Silver FDIC data:
# MAGIC
# MAGIC 1. **gold.fdic_institution_dimension** — Institution dimension with latest financial snapshot
# MAGIC 2. **gold.fdic_financial_facts** — Quarterly financial fact table linked to institutions
# MAGIC 3. **gold.fdic_failure_analytics** — Bank failure trends by year and state
# MAGIC 4. **gold.fdic_industry_summary** — Industry-wide quarterly aggregates
# MAGIC 5. **gold.fdic_financial_trends** — Per-bank growth rates and rolling metrics
# MAGIC
# MAGIC **Sources:** `silver.fdic_institutions`, `silver.fdic_financials_quarterly`, `silver.fdic_failures`

# COMMAND ----------

# DBTITLE 1,Create gold.fdic_institution_dimension
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.fdic_institution_dimension
# MAGIC USING DELTA
# MAGIC COMMENT 'FDIC institution dimension with latest financial snapshot'
# MAGIC AS
# MAGIC WITH latest_financials AS (
# MAGIC     SELECT
# MAGIC         cert,
# MAGIC         total_assets_thousands AS latest_assets_thousands,
# MAGIC         total_deposits_thousands AS latest_deposits_thousands,
# MAGIC         equity_capital_thousands AS latest_equity_thousands,
# MAGIC         net_income_thousands AS latest_net_income_thousands,
# MAGIC         roa_pct AS latest_roa_pct,
# MAGIC         roe_pct AS latest_roe_pct,
# MAGIC         report_date AS latest_report_date
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             cert,
# MAGIC             total_assets_thousands,
# MAGIC             total_deposits_thousands,
# MAGIC             equity_capital_thousands,
# MAGIC             net_income_thousands,
# MAGIC             roa_pct,
# MAGIC             roe_pct,
# MAGIC             report_date,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY cert ORDER BY report_date DESC) AS _rn
# MAGIC         FROM silver.fdic_financials_quarterly
# MAGIC     ) ranked
# MAGIC     WHERE _rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC     i.institution_key,
# MAGIC     i.cert,
# MAGIC     i.bank_name,
# MAGIC     i.city,
# MAGIC     i.state,
# MAGIC     CASE WHEN i.active = 1 THEN 'Active' ELSE 'Inactive' END AS status,
# MAGIC     i.total_assets_thousands AS reported_assets_thousands,
# MAGIC     i.total_deposits_thousands AS reported_deposits_thousands,
# MAGIC     i.established_date,
# MAGIC     f.latest_assets_thousands,
# MAGIC     f.latest_deposits_thousands,
# MAGIC     f.latest_equity_thousands,
# MAGIC     f.latest_net_income_thousands,
# MAGIC     f.latest_roa_pct,
# MAGIC     f.latest_roe_pct,
# MAGIC     f.latest_report_date,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.fdic_institutions i
# MAGIC LEFT JOIN latest_financials f
# MAGIC     ON i.cert = f.cert;

# COMMAND ----------

# DBTITLE 1,Create gold.fdic_financial_facts
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fdic_financial_facts
# MAGIC USING DELTA
# MAGIC COMMENT 'FDIC quarterly financial facts linked to institution dimension'
# MAGIC AS
# MAGIC SELECT
# MAGIC     f.financial_key,
# MAGIC     d.institution_key,
# MAGIC     f.cert,
# MAGIC     f.bank_name,
# MAGIC     f.report_date,
# MAGIC     YEAR(f.report_date) AS report_year,
# MAGIC     QUARTER(f.report_date) AS report_quarter,
# MAGIC     f.total_assets_thousands,
# MAGIC     f.total_deposits_thousands,
# MAGIC     f.equity_capital_thousands,
# MAGIC     f.net_income_thousands,
# MAGIC     f.net_loans_thousands,
# MAGIC     f.real_estate_loans_thousands,
# MAGIC     f.noncurrent_loans_thousands,
# MAGIC     f.nonaccrual_loans_thousands,
# MAGIC     f.past_due_90_plus_thousands,
# MAGIC     f.net_chargeoffs_thousands,
# MAGIC     f.roa_pct,
# MAGIC     f.roe_pct,
# MAGIC     f.net_interest_margin_pct,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.fdic_financials_quarterly f
# MAGIC LEFT JOIN silver.fdic_institutions d
# MAGIC     ON f.cert = d.cert;

# COMMAND ----------

# DBTITLE 1,Create gold.fdic_failure_analytics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fdic_failure_analytics
# MAGIC USING DELTA
# MAGIC COMMENT 'FDIC bank failure trends by year and state'
# MAGIC AS
# MAGIC SELECT
# MAGIC     failure_year,
# MAGIC     state,
# MAGIC     COUNT(*) AS failure_count,
# MAGIC     SUM(assets_at_failure_thousands) AS total_assets_lost_thousands,
# MAGIC     SUM(deposits_at_failure_thousands) AS total_deposits_lost_thousands,
# MAGIC     SUM(uninsured_deposits_thousands) AS total_uninsured_deposits_thousands,
# MAGIC     SUM(estimated_cost_thousands) AS total_estimated_cost_thousands,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.fdic_failures
# MAGIC WHERE failure_year IS NOT NULL
# MAGIC GROUP BY failure_year, state;

# COMMAND ----------

# DBTITLE 1,Create gold.fdic_industry_summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fdic_industry_summary
# MAGIC USING DELTA
# MAGIC COMMENT 'Industry-wide quarterly FDIC aggregates'
# MAGIC AS
# MAGIC SELECT
# MAGIC     YEAR(report_date) AS report_year,
# MAGIC     QUARTER(report_date) AS report_quarter,
# MAGIC     report_date,
# MAGIC     COUNT(DISTINCT cert) AS reporting_banks,
# MAGIC     SUM(total_assets_thousands) AS industry_total_assets_thousands,
# MAGIC     SUM(total_deposits_thousands) AS industry_total_deposits_thousands,
# MAGIC     SUM(equity_capital_thousands) AS industry_total_equity_thousands,
# MAGIC     SUM(net_income_thousands) AS industry_total_net_income_thousands,
# MAGIC     AVG(roa_pct) AS avg_roa_pct,
# MAGIC     AVG(roe_pct) AS avg_roe_pct,
# MAGIC     AVG(net_interest_margin_pct) AS avg_nim_pct,
# MAGIC     SUM(total_deposits_thousands) / NULLIF(SUM(total_assets_thousands), 0) AS deposit_to_asset_ratio,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.fdic_financials_quarterly
# MAGIC GROUP BY YEAR(report_date), QUARTER(report_date), report_date
# MAGIC ORDER BY report_date;

# COMMAND ----------

# DBTITLE 1,Create gold.fdic_financial_trends
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fdic_financial_trends
# MAGIC USING DELTA
# MAGIC COMMENT 'Per-bank financial growth rates and rolling metrics'
# MAGIC AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT
# MAGIC         cert,
# MAGIC         bank_name,
# MAGIC         report_date,
# MAGIC         total_assets_thousands,
# MAGIC         total_deposits_thousands,
# MAGIC         net_income_thousands,
# MAGIC         roa_pct,
# MAGIC         roe_pct,
# MAGIC         LAG(total_assets_thousands) OVER (PARTITION BY cert ORDER BY report_date) AS prev_assets,
# MAGIC         LAG(total_deposits_thousands) OVER (PARTITION BY cert ORDER BY report_date) AS prev_deposits,
# MAGIC         AVG(roa_pct) OVER (
# MAGIC             PARTITION BY cert ORDER BY report_date
# MAGIC             ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
# MAGIC         ) AS avg_roa_4q,
# MAGIC         AVG(roe_pct) OVER (
# MAGIC             PARTITION BY cert ORDER BY report_date
# MAGIC             ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
# MAGIC         ) AS avg_roe_4q
# MAGIC     FROM silver.fdic_financials_quarterly
# MAGIC )
# MAGIC SELECT
# MAGIC     cert,
# MAGIC     bank_name,
# MAGIC     report_date,
# MAGIC     total_assets_thousands,
# MAGIC     total_deposits_thousands,
# MAGIC     net_income_thousands,
# MAGIC     roa_pct,
# MAGIC     roe_pct,
# MAGIC     CASE
# MAGIC         WHEN prev_assets IS NOT NULL AND prev_assets > 0
# MAGIC         THEN ROUND(((total_assets_thousands - prev_assets) / prev_assets) * 100, 4)
# MAGIC         ELSE NULL
# MAGIC     END AS asset_growth_pct,
# MAGIC     CASE
# MAGIC         WHEN prev_deposits IS NOT NULL AND prev_deposits > 0
# MAGIC         THEN ROUND(((total_deposits_thousands - prev_deposits) / prev_deposits) * 100, 4)
# MAGIC         ELSE NULL
# MAGIC     END AS deposit_growth_pct,
# MAGIC     ROUND(avg_roa_4q, 4) AS avg_roa_4q,
# MAGIC     ROUND(avg_roe_4q, 4) AS avg_roe_4q,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM ranked;

# COMMAND ----------

# DBTITLE 1,Gold Validation
# MAGIC %sql
# MAGIC -- Gold layer row counts
# MAGIC SELECT 'gold.fdic_institution_dimension' AS table_name, COUNT(*) AS row_count
# MAGIC FROM gold.fdic_institution_dimension
# MAGIC UNION ALL
# MAGIC SELECT 'gold.fdic_financial_facts', COUNT(*)
# MAGIC FROM gold.fdic_financial_facts
# MAGIC UNION ALL
# MAGIC SELECT 'gold.fdic_failure_analytics', COUNT(*)
# MAGIC FROM gold.fdic_failure_analytics
# MAGIC UNION ALL
# MAGIC SELECT 'gold.fdic_industry_summary', COUNT(*)
# MAGIC FROM gold.fdic_industry_summary
# MAGIC UNION ALL
# MAGIC SELECT 'gold.fdic_financial_trends', COUNT(*)
# MAGIC FROM gold.fdic_financial_trends;

# COMMAND ----------

