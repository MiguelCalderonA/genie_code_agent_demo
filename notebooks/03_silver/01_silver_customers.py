# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver — Customers (silver.customers)
# MAGIC
# MAGIC Enriches bronze.customers with:
# MAGIC - Latest KYC level resolved from kyc_events (source of truth)
# MAGIC - Days since registration
# MAGIC - Referral flag (whether this customer was referred)
# MAGIC - Support ticket counts and average CSAT
# MAGIC - Deduplication on customer_id (take latest record)

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG = "bitso_demo"

# COMMAND ----------
# MAGIC %md ## Base customer table

# COMMAND ----------

customers = spark.table(f"{CATALOG}.bronze.customers").dropDuplicates(["customer_id"])

# COMMAND ----------
# MAGIC %md ## Latest KYC level from kyc_events

# COMMAND ----------

kyc_latest = (
    spark.table(f"{CATALOG}.bronze.kyc_events")
    .filter(F.col("event_type") == "approved")
    .groupBy("customer_id")
    .agg(F.max("kyc_level_after").alias("kyc_level_verified"))
)

# COMMAND ----------
# MAGIC %md ## Referral flag

# COMMAND ----------

referred_customers = (
    spark.table(f"{CATALOG}.bronze.referrals")
    .select(F.col("referred_customer_id").alias("customer_id"))
    .distinct()
    .withColumn("was_referred", F.lit(True))
)

# COMMAND ----------
# MAGIC %md ## Support summary

# COMMAND ----------

support_summary = (
    spark.table(f"{CATALOG}.bronze.support_tickets")
    .groupBy("customer_id")
    .agg(
        F.count("ticket_id").alias("total_tickets"),
        F.avg("csat_score").alias("avg_csat"),
        F.sum(F.cast("escalated", "int")).alias("escalated_tickets"),
    )
)

# COMMAND ----------
# MAGIC %md ## Assemble silver.customers

# COMMAND ----------

silver_customers = (
    customers
    .join(kyc_latest,         on="customer_id", how="left")
    .join(referred_customers, on="customer_id", how="left")
    .join(support_summary,    on="customer_id", how="left")
    .withColumn("kyc_level_final",
                F.coalesce(F.col("kyc_level_verified"), F.col("kyc_level")))
    .withColumn("was_referred",
                F.coalesce(F.col("was_referred"), F.lit(False)))
    .withColumn("days_since_registration",
                F.datediff(F.current_date(), F.col("registration_date")))
    .withColumn("customer_age_years",
                F.datediff(F.current_date(), F.col("birth_date")) / 365)
    .withColumn("total_tickets",   F.coalesce(F.col("total_tickets"),   F.lit(0)))
    .withColumn("escalated_tickets",F.coalesce(F.col("escalated_tickets"),F.lit(0)))
    .withColumn("_silver_updated_at", F.current_timestamp())
    .drop("kyc_level_verified", "_ingested_at", "_source_table", "_pipeline_run_id", "_bronze_ingested_at")
)

(silver_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.customers"))

count = spark.table(f"{CATALOG}.silver.customers").count()
print(f"  {CATALOG}.silver.customers — {count:,} rows")
display(spark.sql(f"""
    SELECT risk_segment, kyc_level_final, COUNT(*) n,
           ROUND(AVG(days_since_registration),0) avg_days_active,
           ROUND(AVG(avg_csat),2) avg_csat
    FROM {CATALOG}.silver.customers
    GROUP BY 1,2 ORDER BY 1,2
"""))
