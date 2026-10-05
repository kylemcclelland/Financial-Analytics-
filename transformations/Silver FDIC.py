# Databricks notebook source
# DBTITLE 1,Silver FDIC Header
# MAGIC %md
# MAGIC # Silver FDIC
# MAGIC
# MAGIC Transforms raw Bronze FDIC BankFind data into clean, conformed Silver tables:
# MAGIC
# MAGIC 1. **Institution master** — `silver.fdic_institutions` (dedup on `cert`, surrogate key, active/non-null filters)
# MAGIC 2. **Quarterly financials** — `silver.fdic_financials_quarterly` (dedup on `cert, report_date`, surrogate key)
# MAGIC 3. **Bank failures** — `silver.fdic_failures` (dedup on `failure_id`, surrogate key)
# MAGIC
# MAGIC **Sources:** `bronze.fdic_institutions`, `bronze.fdic_financials_quarterly`, `bronze.fdic_failures`
# MAGIC
# MAGIC **Transformations:** deduplicate keeping latest ingestion, enforce non-null natural keys, add MD5 surrogate keys, add `silver_processed_at` timestamp.

# COMMAND ----------

# DBTITLE 1,Create silver.fdic_institutions
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver.fdic_institutions
# MAGIC USING DELTA
# MAGIC COMMENT 'Cleaned and conformed FDIC institution master (Silver layer)'
# MAGIC AS
# MAGIC WITH deduped AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY cert
# MAGIC             ORDER BY ingestion_timestamp DESC
# MAGIC         ) AS _rn
# MAGIC     FROM bronze.fdic_institutions
# MAGIC )
# MAGIC SELECT
# MAGIC     MD5(CAST(cert AS STRING)) AS institution_key,
# MAGIC     cert,
# MAGIC     bank_name,
# MAGIC     city,
# MAGIC     state,
# MAGIC     active,
# MAGIC     total_assets_thousands,
# MAGIC     total_deposits_thousands,
# MAGIC     established_date,
# MAGIC     data_source,
# MAGIC     ingestion_timestamp,
# MAGIC     CURRENT_TIMESTAMP() AS silver_processed_at
# MAGIC FROM deduped
# MAGIC WHERE _rn = 1
# MAGIC     AND cert IS NOT NULL
# MAGIC     AND bank_name IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,Create silver.fdic_financials_quarterly
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.fdic_financials_quarterly
# MAGIC USING DELTA
# MAGIC COMMENT 'Cleaned and conformed FDIC quarterly financial data (Silver layer)'
# MAGIC AS
# MAGIC WITH deduped AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY cert, report_date
# MAGIC             ORDER BY ingestion_timestamp DESC
# MAGIC         ) AS _rn
# MAGIC     FROM bronze.fdic_financials_quarterly
# MAGIC )
# MAGIC SELECT
# MAGIC     MD5(CONCAT(CAST(cert AS STRING), '|', CAST(report_date AS STRING))) AS financial_key,
# MAGIC     cert,
# MAGIC     bank_name,
# MAGIC     report_date,
# MAGIC     total_assets_thousands,
# MAGIC     total_deposits_thousands,
# MAGIC     equity_capital_thousands,
# MAGIC     net_income_thousands,
# MAGIC     net_loans_thousands,
# MAGIC     real_estate_loans_thousands,
# MAGIC     construction_land_dev_loans_thousands,
# MAGIC     multifamily_re_loans_thousands,
# MAGIC     nonfarm_nonres_re_loans_thousands,
# MAGIC     residential_1_4_family_loans_thousands,
# MAGIC     noncurrent_loans_thousands,
# MAGIC     nonaccrual_loans_thousands,
# MAGIC     past_due_90_plus_thousands,
# MAGIC     net_chargeoffs_thousands,
# MAGIC     roa_pct,
# MAGIC     roe_pct,
# MAGIC     net_interest_margin_pct,
# MAGIC     data_source,
# MAGIC     source_unit,
# MAGIC     ingestion_timestamp,
# MAGIC     CURRENT_TIMESTAMP() AS silver_processed_at
# MAGIC FROM deduped
# MAGIC WHERE _rn = 1
# MAGIC     AND cert IS NOT NULL
# MAGIC     AND report_date IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,Create silver.fdic_failures
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.fdic_failures
# MAGIC USING DELTA
# MAGIC COMMENT 'Cleaned and conformed FDIC bank failure history (Silver layer)'
# MAGIC AS
# MAGIC WITH deduped AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY failure_id
# MAGIC             ORDER BY ingestion_timestamp DESC
# MAGIC         ) AS _rn
# MAGIC     FROM bronze.fdic_failures
# MAGIC )
# MAGIC SELECT
# MAGIC     MD5(failure_id) AS failure_key,
# MAGIC     failure_id,
# MAGIC     cert,
# MAGIC     bank_name,
# MAGIC     city,
# MAGIC     state,
# MAGIC     failure_date,
# MAGIC     failure_year,
# MAGIC     assets_at_failure_thousands,
# MAGIC     deposits_at_failure_thousands,
# MAGIC     uninsured_deposits_thousands,
# MAGIC     estimated_cost_thousands,
# MAGIC     resolution_type,
# MAGIC     acquiring_bank,
# MAGIC     acquiring_city,
# MAGIC     acquiring_state,
# MAGIC     data_source,
# MAGIC     source_unit,
# MAGIC     ingestion_timestamp,
# MAGIC     CURRENT_TIMESTAMP() AS silver_processed_at
# MAGIC FROM deduped
# MAGIC WHERE _rn = 1
# MAGIC     AND failure_id IS NOT NULL
# MAGIC     AND cert IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,Silver Validation
# MAGIC %sql
# MAGIC -- Silver layer row counts
# MAGIC SELECT 'silver.fdic_institutions' AS table_name, COUNT(*) AS row_count
# MAGIC FROM silver.fdic_institutions
# MAGIC UNION ALL
# MAGIC SELECT 'silver.fdic_financials_quarterly', COUNT(*)
# MAGIC FROM silver.fdic_financials_quarterly
# MAGIC UNION ALL
# MAGIC SELECT 'silver.fdic_failures', COUNT(*)
# MAGIC FROM silver.fdic_failures;

# COMMAND ----------

