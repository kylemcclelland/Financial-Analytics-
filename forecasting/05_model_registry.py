# Databricks notebook source
# DBTITLE 1,Registry Header
# MAGIC %md
# MAGIC # 05 — Model Registry (Champion/Challenger Selection)
# MAGIC
# MAGIC For each asset × target × horizon, selects the champion model (best out-of-sample QLIKE),
# MAGIC the challenger model (second best), and compares against the baseline.
# MAGIC
# MAGIC **Requires meaningful improvement** vs baseline — does not replace champion for trivial differences.
# MAGIC **Source:** `workspace.analytics.model_performance`
# MAGIC **Target:** `workspace.analytics.forecast_model_registry`

# COMMAND ----------

# DBTITLE 1,Champion/Challenger Selection
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Load model performance (out-of-sample, expanding window)
perf = spark.table("workspace.analytics.model_performance").filter(F.col("window_type") == "expanding")

# Rank models by QLIKE per asset × target × horizon (lower QLIKE = better)
rank_w = Window.partitionBy("asset_id", "asset_class", "target", "horizon").orderBy(F.col("qlike").asc())

ranked = perf.withColumn("model_rank", F.row_number().over(rank_w))

# Champion = rank 1, Challenger = rank 2
champion = ranked.filter(F.col("model_rank") == 1).select(
    F.col("asset_id"), F.col("asset_class"), F.col("target"), F.col("horizon"),
    F.col("model_name").alias("champion_model"),
    F.col("model_distribution").alias("champion_distribution"),
    F.col("qlike").alias("champion_score"),
    F.col("mlflow_run_id"),
    F.col("evaluation_end").alias("last_eval_end"),
)

challenger = ranked.filter(F.col("model_rank") == 2).select(
    F.col("asset_id").alias("c_asset_id"),
    F.col("target").alias("c_target"),
    F.col("horizon").alias("c_horizon"),
    F.col("model_name").alias("challenger_model"),
    F.col("model_distribution").alias("challenger_distribution"),
    F.col("qlike").alias("challenger_score"),
)

# Baseline = the simplest model (rolling historical volatility or EWMA)
baseline = ranked.filter(
    F.col("model_name").isin("rolling_vol_20d", "ewma")
).select(
    F.col("asset_id").alias("b_asset_id"),
    F.col("target").alias("b_target"),
    F.col("horizon").alias("b_horizon"),
    F.col("model_name").alias("baseline_model"),
    F.col("qlike").alias("baseline_score"),
)

# Join champion + challenger + baseline
registry = (
    champion
    .join(challenger,
          (champion.asset_id == challenger.c_asset_id) &
          (champion.target == challenger.c_target) &
          (champion.horizon == challenger.c_horizon),
          "left")
    .join(baseline,
          (champion.asset_id == baseline.b_asset_id) &
          (champion.target == baseline.b_target) &
          (champion.horizon == baseline.b_horizon),
          "left")
    .select(
        champion.asset_id,
        champion.target,
        champion.horizon,
        champion.champion_model,
        champion.champion_distribution,
        challenger.challenger_model,
        challenger.challenger_distribution,
        F.lit("qlike").alias("evaluation_metric"),
        champion.champion_score,
        challenger.challenger_score,
        baseline.baseline_model,
        baseline.baseline_score,
        # Improvement vs baseline (positive = champion is better)
        F.when(baseline.baseline_score.isNotNull() & (baseline.baseline_score > 0),
               F.round((baseline.baseline_score - champion.champion_score) / baseline.baseline_score * 100, 2)
        ).otherwise(F.lit(None)).alias("improvement_vs_baseline_pct"),
        F.lit("1.0").alias("model_version"),
        champion.last_eval_end.cast("date").alias("last_training_date"),
        champion.mlflow_run_id,
        F.current_timestamp().alias("updated_at"),
    )
)

# Show summary before writing
print("=== Champion/Challenger Registry ===")
registry.groupBy("champion_model").count().orderBy(F.desc("count")).show()

# Idempotent MERGE into forecast_model_registry
TARGET = "workspace.analytics.forecast_model_registry"
target = DeltaTable.forName(spark, TARGET)

# Clear and repopulate (simple for MVP)
spark.sql(f"TRUNCATE TABLE {TARGET}")
registry.write.format("delta").mode("append").saveAsTable(TARGET)

print(f"✅ Registry updated: {TARGET}")
print(f"Total entries: {spark.table(TARGET).count()}")

# Show champions per asset
print("\n=== Champions by Asset ===")
spark.sql(f"""
SELECT asset_id, target, horizon, champion_model, champion_score, 
       baseline_model, baseline_score, improvement_vs_baseline_pct
FROM {TARGET}
ORDER BY asset_id, target, horizon
""").show(20, truncate=False)

# COMMAND ----------

