# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver — Engagement (silver.engagement_summary)
# MAGIC
# MAGIC Aggregates behavioural signals per customer:
# MAGIC - App session metrics (frequency, duration, features)
# MAGIC - Marketing engagement (impressions, CTR, conversions)
# MAGIC - Push notification open rates
# MAGIC
# MAGIC Output: one row per customer with engagement KPIs.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "bitso_demo"

# COMMAND ----------
# MAGIC %md ## App session metrics

# COMMAND ----------

session_metrics = (
    spark.table(f"{CATALOG}.bronze.app_sessions")
    .groupBy("customer_id")
    .agg(
        F.count("session_id").alias("total_sessions"),
        F.avg("duration_seconds").alias("avg_session_duration_secs"),
        F.max("duration_seconds").alias("max_session_duration_secs"),
        F.sum(F.col("trade_initiated").cast("int")).alias("trade_initiated_sessions"),
        F.countDistinct("device_type").alias("unique_devices"),
        F.countDistinct("primary_feature").alias("unique_features_used"),
        F.max("session_start_ts").alias("last_session_ts"),
        F.min("session_start_ts").alias("first_session_ts"),
    )
    .withColumn("trade_session_rate",
                F.when(F.col("total_sessions") > 0,
                       F.col("trade_initiated_sessions") / F.col("total_sessions")).otherwise(0.0))
    .withColumn("days_between_first_last_session",
                F.datediff(F.col("last_session_ts"), F.col("first_session_ts")))
)

# COMMAND ----------
# MAGIC %md ## Marketing engagement

# COMMAND ----------

mktg_metrics = (
    spark.table(f"{CATALOG}.bronze.marketing_impressions")
    .groupBy("customer_id")
    .agg(
        F.count("impression_id").alias("total_impressions"),
        F.sum(F.col("clicked").cast("int")).alias("total_clicks"),
        F.sum(F.col("converted").cast("int")).alias("total_conversions"),
        F.countDistinct("campaign_id").alias("unique_campaigns_reached"),
        F.countDistinct("channel").alias("unique_channels"),
        F.sum("conversion_value_mxn").alias("total_conversion_value_mxn"),
    )
    .withColumn("ctr", F.when(F.col("total_impressions") > 0,
                              F.col("total_clicks") / F.col("total_impressions")).otherwise(0.0))
    .withColumn("cvr", F.when(F.col("total_clicks") > 0,
                              F.col("total_conversions") / F.col("total_clicks")).otherwise(0.0))
)

# COMMAND ----------
# MAGIC %md ## Push notification engagement

# COMMAND ----------

push_metrics = (
    spark.table(f"{CATALOG}.bronze.push_notifications")
    .groupBy("customer_id")
    .agg(
        F.count("notification_id").alias("total_push_sent"),
        F.sum(F.col("opened").cast("int")).alias("total_push_opened"),
        F.sum(F.col("action_taken").cast("int")).alias("total_push_actions"),
        F.sum(F.col("is_personalised").cast("int")).alias("personalised_push_count"),
    )
    .withColumn("push_open_rate",   F.when(F.col("total_push_sent") > 0,
                                          F.col("total_push_opened") / F.col("total_push_sent")).otherwise(0.0))
    .withColumn("push_action_rate", F.when(F.col("total_push_sent") > 0,
                                          F.col("total_push_actions") / F.col("total_push_sent")).otherwise(0.0))
)

# COMMAND ----------
# MAGIC %md ## Join into engagement_summary

# COMMAND ----------

all_customers = spark.table(f"{CATALOG}.bronze.customers").select("customer_id").dropDuplicates(["customer_id"])

engagement = (
    all_customers
    .join(session_metrics, on="customer_id", how="left")
    .join(mktg_metrics,    on="customer_id", how="left")
    .join(push_metrics,    on="customer_id", how="left")
    .na.fill(0, subset=[
        "total_sessions","avg_session_duration_secs","trade_initiated_sessions",
        "total_impressions","total_clicks","total_conversions",
        "total_push_sent","total_push_opened",
    ])
    .withColumn("_silver_updated_at", F.current_timestamp())
)

(engagement.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.engagement_summary"))

count = spark.table(f"{CATALOG}.silver.engagement_summary").count()
print(f"  {CATALOG}.silver.engagement_summary — {count:,} rows")
display(spark.sql(f"""
    SELECT ROUND(AVG(total_sessions),1)       avg_sessions,
           ROUND(AVG(avg_session_duration_secs),0) avg_dur_secs,
           ROUND(AVG(ctr)*100,2)              avg_ctr_pct,
           ROUND(AVG(push_open_rate)*100,2)   avg_push_open_pct
    FROM {CATALOG}.silver.engagement_summary
"""))
