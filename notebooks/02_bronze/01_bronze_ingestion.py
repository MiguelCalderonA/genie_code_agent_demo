# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion with Audit Metadata
# MAGIC
# MAGIC Reads all 10 raw tables and writes them to the **bronze** schema with:
# MAGIC - `_bronze_ingested_at` — pipeline run timestamp
# MAGIC - `_source_table` — origin table name
# MAGIC - `_pipeline_run_id` — unique run identifier
# MAGIC - Basic schema validation (row count checks)
# MAGIC
# MAGIC No business logic is applied here. Bronze = raw + audit trail.

# COMMAND ----------

from datetime import datetime
import uuid

CATALOG    = "bitso_demo"
RUN_ID     = str(uuid.uuid4())
RUN_TS     = datetime.utcnow()

RAW_TABLES = [
    "customers",
    "app_sessions",
    "marketing_impressions",
    "deposits",
    "trades",
    "withdrawals",
    "kyc_events",
    "referrals",
    "support_tickets",
    "push_notifications",
]

print(f"Bronze ingestion run: {RUN_ID}")
print(f"Timestamp: {RUN_TS}")

# COMMAND ----------

from pyspark.sql import functions as F

results = []

for table in RAW_TABLES:
    raw_path    = f"{CATALOG}.raw.{table}"
    bronze_path = f"{CATALOG}.bronze.{table}"

    df = (spark.table(raw_path)
          .withColumn("_source_table",      F.lit(raw_path))
          .withColumn("_pipeline_run_id",   F.lit(RUN_ID))
          .withColumn("_bronze_ingested_at",F.lit(RUN_TS).cast("timestamp")))

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(bronze_path))

    raw_count    = spark.table(raw_path).count()
    bronze_count = spark.table(bronze_path).count()
    ok           = raw_count == bronze_count

    results.append({
        "table":        table,
        "raw_rows":     raw_count,
        "bronze_rows":  bronze_count,
        "row_match":    ok,
    })
    status = "OK" if ok else "MISMATCH"
    print(f"  [{status}] {table}: {raw_count:,} → {bronze_count:,}")

# COMMAND ----------
# MAGIC %md ## Ingestion summary

# COMMAND ----------

import pandas as pd
summary = pd.DataFrame(results)
display(spark.createDataFrame(summary))

mismatches = [r for r in results if not r["row_match"]]
if mismatches:
    raise ValueError(f"Row count mismatches detected: {mismatches}")
print(f"\nAll {len(RAW_TABLES)} tables ingested successfully into bronze.")
