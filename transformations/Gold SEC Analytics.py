# Databricks notebook source
# DBTITLE 1,Gold SEC Analytics
# MAGIC %md
# MAGIC # Gold SEC Analytics
# MAGIC
# MAGIC Creates Gold-layer analytical tables from Silver SEC data:
# MAGIC
# MAGIC 1. **gold.fact_sec_company_facts** — Fact table linking SEC company facts to the company dimension with surrogate keys
# MAGIC 2. **gold.sec_financial_summary** — Pivoted key financial metrics by company, fiscal year, and period
# MAGIC 3. **gold.sec_concept_summary** — Aggregated concept-level statistics for trend analysis

# COMMAND ----------

# DBTITLE 1,Create gold.fact_sec_company_facts
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_sec_company_facts
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     s.fact_key,
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     s.taxonomy,
# MAGIC     s.concept,
# MAGIC     s.label,
# MAGIC     s.description,
# MAGIC     s.unit,
# MAGIC     s.value,
# MAGIC     s.start_date,
# MAGIC     s.end_date,
# MAGIC     s.filing_date,
# MAGIC     s.form,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period,
# MAGIC     s.frame,
# MAGIC     s.accession_number,
# MAGIC     s.ingestion_timestamp,
# MAGIC     s.silver_processed_at,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.sec_company_facts s
# MAGIC LEFT JOIN gold.dim_company d
# MAGIC     ON CAST(s.cik AS STRING) = d.cik
# MAGIC     AND d.is_active = TRUE;

# COMMAND ----------

# DBTITLE 1,Create Gold tables
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.fact_sec_company_facts
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     s.fact_key,
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     s.taxonomy,
# MAGIC     s.concept,
# MAGIC     s.label,
# MAGIC     s.description,
# MAGIC     s.unit,
# MAGIC     s.value,
# MAGIC     s.start_date,
# MAGIC     s.end_date,
# MAGIC     s.filing_date,
# MAGIC     s.form,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period,
# MAGIC     s.frame,
# MAGIC     s.accession_number,
# MAGIC     s.ingestion_timestamp,
# MAGIC     s.silver_processed_at,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.sec_company_facts s
# MAGIC LEFT JOIN gold.dim_company d
# MAGIC     ON CAST(s.cik AS STRING) = d.cik
# MAGIC     AND d.is_active = TRUE;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.sec_financial_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     d.ticker,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period,
# MAGIC     s.unit,
# MAGIC     MAX(CASE WHEN s.concept = 'Revenues' THEN s.value END) AS revenues,
# MAGIC     MAX(CASE WHEN s.concept = 'NetIncomeLoss' THEN s.value END) AS net_income_loss,
# MAGIC     MAX(CASE WHEN s.concept = 'OperatingIncomeLoss' THEN s.value END) AS operating_income_loss,
# MAGIC     MAX(CASE WHEN s.concept = 'Assets' THEN s.value END) AS total_assets,
# MAGIC     MAX(CASE WHEN s.concept = 'Liabilities' THEN s.value END) AS total_liabilities,
# MAGIC     MAX(CASE WHEN s.concept = 'StockholdersEquity' THEN s.value END) AS stockholders_equity,
# MAGIC     MAX(CASE WHEN s.concept = 'CashCashEquivalentsAtCarryingValue' THEN s.value END) AS cash_and_equivalents,
# MAGIC     MAX(CASE WHEN s.concept = 'ResearchAndDevelopmentExpense' THEN s.value END) AS rd_expense,
# MAGIC     MAX(CASE WHEN s.concept = 'EarningsPerShareBasic' THEN s.value END) AS eps_basic,
# MAGIC     MAX(CASE WHEN s.concept = 'EarningsPerShareDiluted' THEN s.value END) AS eps_diluted,
# MAGIC     MAX(s.filing_date) AS latest_filing_date,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.sec_company_facts s
# MAGIC LEFT JOIN gold.dim_company d
# MAGIC     ON CAST(s.cik AS STRING) = d.cik
# MAGIC     AND d.is_active = TRUE
# MAGIC WHERE s.fiscal_year IS NOT NULL
# MAGIC     AND s.fiscal_period IS NOT NULL
# MAGIC     AND s.concept IN (
# MAGIC         'Revenues', 'NetIncomeLoss', 'OperatingIncomeLoss',
# MAGIC         'Assets', 'Liabilities', 'StockholdersEquity',
# MAGIC         'CashCashEquivalentsAtCarryingValue',
# MAGIC         'ResearchAndDevelopmentExpense',
# MAGIC         'EarningsPerShareBasic', 'EarningsPerShareDiluted'
# MAGIC     )
# MAGIC GROUP BY
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     d.ticker,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period,
# MAGIC     s.unit;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.sec_concept_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     d.ticker,
# MAGIC     s.concept,
# MAGIC     s.label,
# MAGIC     s.unit,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period,
# MAGIC     COUNT(*) AS observation_count,
# MAGIC     MIN(s.value) AS min_value,
# MAGIC     MAX(s.value) AS max_value,
# MAGIC     AVG(s.value) AS avg_value,
# MAGIC     MAX(s.filing_date) AS latest_filing_date,
# MAGIC     MAX(s.end_date) AS latest_end_date,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.sec_company_facts s
# MAGIC LEFT JOIN gold.dim_company d
# MAGIC     ON CAST(s.cik AS STRING) = d.cik
# MAGIC     AND d.is_active = TRUE
# MAGIC WHERE s.fiscal_year IS NOT NULL
# MAGIC GROUP BY
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     d.ticker,
# MAGIC     s.concept,
# MAGIC     s.label,
# MAGIC     s.unit,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period;
# MAGIC
# MAGIC -- Validate row counts
# MAGIC SELECT 'gold.fact_sec_company_facts' AS table_name, COUNT(*) AS row_count
# MAGIC FROM gold.fact_sec_company_facts
# MAGIC UNION ALL
# MAGIC SELECT 'gold.sec_financial_summary', COUNT(*)
# MAGIC FROM gold.sec_financial_summary
# MAGIC UNION ALL
# MAGIC SELECT 'gold.sec_concept_summary', COUNT(*)
# MAGIC FROM gold.sec_concept_summary;
# MAGIC
# MAGIC -- Sample fact table
# MAGIC SELECT company_key, cik, company_name, concept, label, unit, value, fiscal_year, fiscal_period, form, filing_date
# MAGIC FROM gold.fact_sec_company_facts
# MAGIC ORDER BY filing_date DESC
# MAGIC LIMIT 20;
# MAGIC
# MAGIC -- Financial summary
# MAGIC SELECT company_key, ticker, fiscal_year, fiscal_period, revenues, net_income_loss, total_assets, total_liabilities, stockholders_equity, eps_basic, eps_diluted
# MAGIC FROM gold.sec_financial_summary
# MAGIC ORDER BY fiscal_year DESC, fiscal_period;
# MAGIC
# MAGIC -- Concept summary for key metrics
# MAGIC SELECT company_key, ticker, concept, label, unit, fiscal_year, fiscal_period, observation_count, min_value, max_value, avg_value
# MAGIC FROM gold.sec_concept_summary
# MAGIC WHERE concept IN ('Revenues', 'NetIncomeLoss', 'Assets')
# MAGIC ORDER BY fiscal_year DESC, fiscal_period, concept
# MAGIC LIMIT 30;

# COMMAND ----------

# DBTITLE 1,Create gold.fact_sec_company_facts
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_sec_company_facts
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     s.fact_key,
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     s.taxonomy,
# MAGIC     s.concept,
# MAGIC     s.label,
# MAGIC     s.description,
# MAGIC     s.unit,
# MAGIC     s.value,
# MAGIC     s.start_date,
# MAGIC     s.end_date,
# MAGIC     s.filing_date,
# MAGIC     s.form,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period,
# MAGIC     s.frame,
# MAGIC     s.accession_number,
# MAGIC     s.ingestion_timestamp,
# MAGIC     s.silver_processed_at,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.sec_company_facts s
# MAGIC LEFT JOIN gold.dim_company d
# MAGIC     ON CAST(s.cik AS STRING) = d.cik
# MAGIC     AND d.is_active = TRUE;

# COMMAND ----------

# DBTITLE 1,Create gold.sec_financial_summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.sec_financial_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     d.ticker,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period,
# MAGIC     s.unit,
# MAGIC     MAX(CASE WHEN s.concept = 'Revenues' THEN s.value END) AS revenues,
# MAGIC     MAX(CASE WHEN s.concept = 'NetIncomeLoss' THEN s.value END) AS net_income_loss,
# MAGIC     MAX(CASE WHEN s.concept = 'OperatingIncomeLoss' THEN s.value END) AS operating_income_loss,
# MAGIC     MAX(CASE WHEN s.concept = 'Assets' THEN s.value END) AS total_assets,
# MAGIC     MAX(CASE WHEN s.concept = 'Liabilities' THEN s.value END) AS total_liabilities,
# MAGIC     MAX(CASE WHEN s.concept = 'StockholdersEquity' THEN s.value END) AS stockholders_equity,
# MAGIC     MAX(CASE WHEN s.concept = 'CashCashEquivalentsAtCarryingValue' THEN s.value END) AS cash_and_equivalents,
# MAGIC     MAX(CASE WHEN s.concept = 'ResearchAndDevelopmentExpense' THEN s.value END) AS rd_expense,
# MAGIC     MAX(CASE WHEN s.concept = 'EarningsPerShareBasic' THEN s.value END) AS eps_basic,
# MAGIC     MAX(CASE WHEN s.concept = 'EarningsPerShareDiluted' THEN s.value END) AS eps_diluted,
# MAGIC     MAX(s.filing_date) AS latest_filing_date,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.sec_company_facts s
# MAGIC LEFT JOIN gold.dim_company d
# MAGIC     ON CAST(s.cik AS STRING) = d.cik
# MAGIC     AND d.is_active = TRUE
# MAGIC WHERE s.fiscal_year IS NOT NULL
# MAGIC     AND s.fiscal_period IS NOT NULL
# MAGIC     AND s.concept IN (
# MAGIC         'Revenues', 'NetIncomeLoss', 'OperatingIncomeLoss',
# MAGIC         'Assets', 'Liabilities', 'StockholdersEquity',
# MAGIC         'CashCashEquivalentsAtCarryingValue',
# MAGIC         'ResearchAndDevelopmentExpense',
# MAGIC         'EarningsPerShareBasic', 'EarningsPerShareDiluted'
# MAGIC     )
# MAGIC GROUP BY
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     d.ticker,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period,
# MAGIC     s.unit;

# COMMAND ----------

# DBTITLE 1,Create gold.sec_concept_summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.sec_concept_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     d.ticker,
# MAGIC     s.concept,
# MAGIC     s.label,
# MAGIC     s.unit,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period,
# MAGIC     COUNT(*) AS observation_count,
# MAGIC     MIN(s.value) AS min_value,
# MAGIC     MAX(s.value) AS max_value,
# MAGIC     AVG(s.value) AS avg_value,
# MAGIC     MAX(s.filing_date) AS latest_filing_date,
# MAGIC     MAX(s.end_date) AS latest_end_date,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM silver.sec_company_facts s
# MAGIC LEFT JOIN gold.dim_company d
# MAGIC     ON CAST(s.cik AS STRING) = d.cik
# MAGIC     AND d.is_active = TRUE
# MAGIC WHERE s.fiscal_year IS NOT NULL
# MAGIC GROUP BY
# MAGIC     d.company_key,
# MAGIC     s.cik,
# MAGIC     s.company_name,
# MAGIC     d.ticker,
# MAGIC     s.concept,
# MAGIC     s.label,
# MAGIC     s.unit,
# MAGIC     s.fiscal_year,
# MAGIC     s.fiscal_period;

# COMMAND ----------

# DBTITLE 1,Validate Gold tables
# MAGIC %sql
# MAGIC -- Row counts for all gold tables
# MAGIC SELECT 'gold.fact_sec_company_facts' AS table_name, COUNT(*) AS row_count
# MAGIC FROM gold.fact_sec_company_facts
# MAGIC UNION ALL
# MAGIC SELECT 'gold.sec_financial_summary', COUNT(*)
# MAGIC FROM gold.sec_financial_summary
# MAGIC UNION ALL
# MAGIC SELECT 'gold.sec_concept_summary', COUNT(*)
# MAGIC FROM gold.sec_concept_summary;
# MAGIC
# MAGIC -- Sample from fact table
# MAGIC SELECT company_key, cik, company_name, concept, label, unit, value, fiscal_year, fiscal_period, form, filing_date
# MAGIC FROM gold.fact_sec_company_facts
# MAGIC ORDER BY filing_date DESC
# MAGIC LIMIT 20;
# MAGIC
# MAGIC -- Financial summary
# MAGIC SELECT company_key, ticker, fiscal_year, fiscal_period, revenues, net_income_loss, total_assets, total_liabilities, stockholders_equity, eps_basic, eps_diluted
# MAGIC FROM gold.sec_financial_summary
# MAGIC ORDER BY fiscal_year DESC, fiscal_period;
# MAGIC
# MAGIC -- Concept summary for key metrics
# MAGIC SELECT company_key, ticker, concept, label, unit, fiscal_year, fiscal_period, observation_count, min_value, max_value, avg_value
# MAGIC FROM gold.sec_concept_summary
# MAGIC WHERE concept IN ('Revenues', 'NetIncomeLoss', 'Assets')
# MAGIC ORDER BY fiscal_year DESC, fiscal_period, concept
# MAGIC LIMIT 30;