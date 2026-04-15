# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold — Customer 360 (gold.customer_360)
# MAGIC
# MAGIC Business-ready single view of every Bitso customer combining all silver metrics.
# MAGIC This table powers Genie AI/BI dashboards.
# MAGIC
# MAGIC **Key metrics included**
# MAGIC - Identity & demographics
# MAGIC - Lifetime trading volume and frequency
# MAGIC - Deposit behaviour
# MAGIC - KYC and compliance status
# MAGIC - App and marketing engagement
# MAGIC - Support history
# MAGIC - Referral activity
# MAGIC - Propensity label (pre-computed from silver.monthly_trade_summary)

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG = "bitso_demo"

HIGH_VALUE_VOLUME_THRESHOLD  = 200_000   # MXN per month
HIGH_VALUE_TRADE_COUNT_THRESHOLD = 10    # trades per month

# COMMAND ----------
# MAGIC %md ## Customer base

# COMMAND ----------

customers = spark.table(f"{CATALOG}.silver.customers")

# COMMAND ----------
# MAGIC %md ## Lifetime trading metrics

# COMMAND ----------

lifetime_trades = (
    spark.table(f"{CATALOG}.bronze.trades")
    .groupBy("customer_id")
    .agg(
        F.count("trade_id").alias("lifetime_trade_count"),
        F.sum("volume_mxn").alias("lifetime_volume_mxn"),
        F.avg("volume_mxn").alias("avg_trade_volume_mxn"),
        F.sum("fee_mxn").alias("lifetime_fees_paid_mxn"),
        F.countDistinct("trading_pair").alias("unique_pairs_traded"),
        F.max("trade_ts").alias("last_trade_ts"),
        F.min("trade_ts").alias("first_trade_ts"),
        F.sum(F.when(F.col("side") == "buy",  F.col("volume_mxn")).otherwise(0)).alias("buy_volume_mxn"),
        F.sum(F.when(F.col("side") == "sell", F.col("volume_mxn")).otherwise(0)).alias("sell_volume_mxn"),
    )
    .withColumn("days_trading",
                F.datediff(F.col("last_trade_ts"), F.col("first_trade_ts")))
)

# COMMAND ----------
# MAGIC %md ## Average monthly trading (for HVT label)

# COMMAND ----------

monthly_avg = (
    spark.table(f"{CATALOG}.silver.monthly_trade_summary")
    .groupBy("customer_id")
    .agg(
        F.avg("monthly_volume_mxn").alias("avg_monthly_volume_mxn"),
        F.avg("monthly_trade_count").alias("avg_monthly_trade_count"),
        F.count("year_month").alias("active_trading_months"),
    )
)

# COMMAND ----------
# MAGIC %md ## Deposit metrics

# COMMAND ----------

deposit_metrics = (
    spark.table(f"{CATALOG}.bronze.deposits")
    .filter(F.col("status") == "completed")
    .groupBy("customer_id")
    .agg(
        F.count("deposit_id").alias("total_deposits"),
        F.sum("amount_mxn").alias("total_deposited_mxn"),
        F.avg("amount_mxn").alias("avg_deposit_mxn"),
        F.max("deposit_ts").alias("last_deposit_ts"),
        F.countDistinct("payment_method").alias("payment_methods_used"),
    )
)

# COMMAND ----------
# MAGIC %md ## Referral counts

# COMMAND ----------

referral_given = (
    spark.table(f"{CATALOG}.bronze.referrals")
    .groupBy(F.col("referrer_customer_id").alias("customer_id"))
    .agg(F.count("referral_id").alias("referrals_given"))
)

# COMMAND ----------
# MAGIC %md ## Assemble gold.customer_360

# COMMAND ----------

gold = (
    customers
    .join(lifetime_trades,  on="customer_id", how="left")
    .join(monthly_avg,      on="customer_id", how="left")
    .join(deposit_metrics,  on="customer_id", how="left")
    .join(referral_given,   on="customer_id", how="left")
    .join(spark.table(f"{CATALOG}.silver.engagement_summary"), on="customer_id", how="left")
    # --- computed flags ---
    .withColumn("is_high_value_trader",
                F.when(
                    (F.col("avg_monthly_volume_mxn") >= HIGH_VALUE_VOLUME_THRESHOLD) &
                    (F.col("avg_monthly_trade_count") >= HIGH_VALUE_TRADE_COUNT_THRESHOLD),
                    True
                ).otherwise(False))
    .withColumn("days_since_last_trade",
                F.datediff(F.current_date(), F.col("last_trade_ts")))
    .withColumn("days_since_last_deposit",
                F.datediff(F.current_date(), F.col("last_deposit_ts")))
    .withColumn("deposit_to_volume_ratio",
                F.col("lifetime_volume_mxn") / F.when(F.col("total_deposited_mxn") > 0, F.col("total_deposited_mxn")).otherwise(1))
    # --- coalesce nulls ---
    .na.fill(0, subset=["lifetime_trade_count","lifetime_volume_mxn","total_deposits",
                        "total_deposited_mxn","referrals_given","total_sessions"])
    .na.fill(False, subset=["is_high_value_trader"])
    .withColumn("_gold_updated_at", F.current_timestamp())
)

(gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.customer_360"))

count = spark.table(f"{CATALOG}.gold.customer_360").count()
hvt   = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.gold.customer_360 WHERE is_high_value_trader = true").collect()[0][0]
print(f"  {CATALOG}.gold.customer_360 — {count:,} rows  |  High-value traders: {hvt:,} ({100*hvt/count:.1f}%)")

display(spark.sql(f"""
    SELECT is_high_value_trader,
           COUNT(*) n_customers,
           ROUND(AVG(lifetime_volume_mxn),0)  avg_lifetime_vol_mxn,
           ROUND(AVG(lifetime_trade_count),0) avg_trades,
           ROUND(AVG(avg_monthly_volume_mxn),0) avg_monthly_vol_mxn,
           ROUND(AVG(total_sessions),0)       avg_sessions
    FROM {CATALOG}.gold.customer_360
    GROUP BY 1
"""))
