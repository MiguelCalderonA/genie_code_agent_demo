# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver — Transactions (silver.transactions)
# MAGIC
# MAGIC Unified transaction ledger combining deposits, trades, and withdrawals into a single
# MAGIC time-ordered event stream per customer.
# MAGIC
# MAGIC Also produces **silver.monthly_trade_summary** — the aggregation used to label
# MAGIC high-value traders for the ML model.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "bitso_demo"

# COMMAND ----------
# MAGIC %md ## Deposits

# COMMAND ----------

deposits = (
    spark.table(f"{CATALOG}.bronze.deposits")
    .filter(F.col("status") == "completed")
    .select(
        F.col("deposit_id").cast("string").alias("transaction_id"),
        F.col("customer_id"),
        F.col("deposit_ts").alias("event_ts"),
        F.lit("deposit").alias("transaction_type"),
        F.col("amount_mxn"),
        F.col("currency_received").alias("currency"),
        F.col("payment_method"),
        F.lit(None).cast("string").alias("trading_pair"),
        F.lit(0.0).alias("fee_mxn"),
    )
)

# COMMAND ----------
# MAGIC %md ## Trades

# COMMAND ----------

trades = (
    spark.table(f"{CATALOG}.bronze.trades")
    .select(
        F.col("trade_id").cast("string").alias("transaction_id"),
        F.col("customer_id"),
        F.col("trade_ts").alias("event_ts"),
        F.lit("trade").alias("transaction_type"),
        F.col("volume_mxn").alias("amount_mxn"),
        F.col("base_currency").alias("currency"),
        F.col("order_type").alias("payment_method"),
        F.col("trading_pair"),
        F.col("fee_mxn"),
    )
)

# COMMAND ----------
# MAGIC %md ## Withdrawals (completed only)

# COMMAND ----------

withdrawals = (
    spark.table(f"{CATALOG}.bronze.withdrawals")
    .filter(F.col("status") == "completed")
    .select(
        F.col("withdrawal_id").cast("string").alias("transaction_id"),
        F.col("customer_id"),
        F.col("withdrawal_ts").alias("event_ts"),
        F.lit("withdrawal").alias("transaction_type"),
        (-F.col("amount_mxn")).alias("amount_mxn"),  # negative for outflows
        F.col("currency_withdrawn").alias("currency"),
        F.col("destination_type").alias("payment_method"),
        F.lit(None).cast("string").alias("trading_pair"),
        F.col("fee_mxn"),
    )
)

# COMMAND ----------
# MAGIC %md ## Union and write silver.transactions

# COMMAND ----------

transactions = (
    deposits.union(trades).union(withdrawals)
    .withColumn("year_month", F.date_format("event_ts", "yyyy-MM"))
    .withColumn("_silver_updated_at", F.current_timestamp())
)

(transactions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.transactions"))

print(f"  {CATALOG}.silver.transactions — {transactions.count():,} rows")

# COMMAND ----------
# MAGIC %md ## Monthly trade summary (used for ML labelling)

# COMMAND ----------

monthly_summary = (
    spark.table(f"{CATALOG}.silver.transactions")
    .filter(F.col("transaction_type") == "trade")
    .groupBy("customer_id", "year_month")
    .agg(
        F.count("transaction_id").alias("monthly_trade_count"),
        F.sum("amount_mxn").alias("monthly_volume_mxn"),
        F.sum("fee_mxn").alias("monthly_fees_mxn"),
        F.countDistinct("trading_pair").alias("unique_pairs_traded"),
    )
)

(monthly_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.monthly_trade_summary"))

print(f"  {CATALOG}.silver.monthly_trade_summary — {monthly_summary.count():,} rows")
display(spark.sql(f"""
    SELECT transaction_type, COUNT(*) n, ROUND(SUM(amount_mxn),0) total_mxn
    FROM {CATALOG}.silver.transactions
    GROUP BY 1
"""))
