# Databricks notebook source
# DBTITLE 1,FDIC Data Quality Header
# MAGIC %md
# MAGIC # FDIC Data Quality Checks
# MAGIC
# MAGIC Validates data integrity across Silver and Gold FDIC layers:
# MAGIC
# MAGIC 1. **Null key checks** — natural keys should never be null
# MAGIC 2. **Duplicate checks** — no duplicate natural keys after dedup
# MAGIC 3. **Referential integrity** — financials should join to institutions
# MAGIC 4. **Gold table coverage** — all gold tables populated

# COMMAND ----------

# DBTITLE 1,DQ 1: Null Key Checks
# MAGIC %sql
# MAGIC -- DQ 1: Null key checks
# MAGIC SELECT 'Institutions with null cert' AS check_name, COUNT(*) AS failure_count
# MAGIC FROM silver.fdic_institutions
# MAGIC WHERE cert IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Financials with null cert', COUNT(*)
# MAGIC FROM silver.fdic_financials_quarterly
# MAGIC WHERE cert IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Financials with null report_date', COUNT(*)
# MAGIC FROM silver.fdic_financials_quarterly
# MAGIC WHERE report_date IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Failures with null cert', COUNT(*)
# MAGIC FROM silver.fdic_failures
# MAGIC WHERE cert IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Failures with null failure_id', COUNT(*)
# MAGIC FROM silver.fdic_failures
# MAGIC WHERE failure_id IS NULL;

# COMMAND ----------

# DBTITLE 1,DQ 2: Duplicate Checks
# MAGIC %sql
# MAGIC -- DQ 2: Duplicate checks
# MAGIC SELECT 'Duplicate institution certs' AS check_name, COUNT(*) AS duplicate_count
# MAGIC FROM (
# MAGIC     SELECT cert, COUNT(*) AS cnt
# MAGIC     FROM silver.fdic_institutions
# MAGIC     GROUP BY cert
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Duplicate financials (cert, report_date)', COUNT(*)
# MAGIC FROM (
# MAGIC     SELECT cert, report_date, COUNT(*) AS cnt
# MAGIC     FROM silver.fdic_financials_quarterly
# MAGIC     GROUP BY cert, report_date
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Duplicate failure IDs', COUNT(*)
# MAGIC FROM (
# MAGIC     SELECT failure_id, COUNT(*) AS cnt
# MAGIC     FROM silver.fdic_failures
# MAGIC     GROUP BY failure_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC );

# COMMAND ----------

# DBTITLE 1,DQ 3: Referential Integrity
# MAGIC %sql
# MAGIC -- DQ 3: Referential integrity — financials without matching institution
# MAGIC SELECT 'Financials without matching institution' AS check_name, COUNT(*) AS orphan_count
# MAGIC FROM silver.fdic_financials_quarterly f
# MAGIC LEFT JOIN silver.fdic_institutions i
# MAGIC     ON f.cert = i.cert
# MAGIC WHERE i.cert IS NULL;

# COMMAND ----------

# DBTITLE 1,DQ 4: Gold Coverage & Samples
# MAGIC %sql
# MAGIC -- DQ 4: Gold table coverage
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
# MAGIC
# MAGIC -- Latest industry summary
# MAGIC SELECT *
# MAGIC FROM gold.fdic_industry_summary
# MAGIC ORDER BY report_date DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Failure analytics by year (top 10)
# MAGIC SELECT failure_year, SUM(failure_count) AS total_failures
# MAGIC FROM gold.fdic_failure_analytics
# MAGIC GROUP BY failure_year
# MAGIC ORDER BY failure_year DESC
# MAGIC LIMIT 10;

# COMMAND ----------

