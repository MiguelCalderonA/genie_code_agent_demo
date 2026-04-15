# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 99 — Run All Data Generation Notebooks
# MAGIC
# MAGIC Orchestrates the full mock-data pipeline in order.
# MAGIC Run this notebook from the workspace UI or as a Databricks Job.
# MAGIC
# MAGIC **Execution order matters**: customers must exist before all other tables,
# MAGIC because downstream generators read customer_ids from raw.customers.

# COMMAND ----------

NOTEBOOK_BASE = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
BASE_DIR = "/".join(NOTEBOOK_BASE.split("/")[:-1])  # same folder as this notebook

NOTEBOOKS = [
    ("../00_setup/00_setup",                     "Environment setup"),
    ("01_gen_customers",                          "Customers (10K)"),
    ("02_gen_app_sessions",                       "App sessions (~450K)"),
    ("03_gen_marketing_impressions",              "Marketing impressions (~350K)"),
    ("04_gen_deposits",                           "Deposits (~80K)"),
    ("05_gen_trades",                             "Trades (~180K)"),
    ("06_gen_withdrawals",                        "Withdrawals (~38K)"),
    ("07_gen_kyc_events",                         "KYC events (~22K)"),
    ("08_gen_referrals",                          "Referrals (~5K)"),
    ("09_gen_support_tickets",                    "Support tickets (~22K)"),
    ("10_gen_push_notifications",                 "Push notifications (~500K)"),
]

# COMMAND ----------

import time
results = []

for nb_path, description in NOTEBOOKS:
    full_path = f"{BASE_DIR}/{nb_path}" if not nb_path.startswith("/") else nb_path
    print(f"\n{'='*60}")
    print(f"  Running: {description}")
    print(f"  Path:    {full_path}")
    start = time.time()
    try:
        result = dbutils.notebook.run(full_path, timeout_seconds=1800, arguments={})
        elapsed = round(time.time() - start, 1)
        results.append({"notebook": description, "status": "SUCCESS", "seconds": elapsed})
        print(f"  Done in {elapsed}s")
    except Exception as e:
        elapsed = round(time.time() - start, 1)
        results.append({"notebook": description, "status": f"FAILED: {str(e)[:80]}", "seconds": elapsed})
        print(f"  FAILED: {e}")

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------

import pandas as pd
summary = pd.DataFrame(results)
display(spark.createDataFrame(summary))

failed = [r for r in results if "FAILED" in r["status"]]
if failed:
    print(f"\n{len(failed)} notebooks failed. Check logs above.")
else:
    print("\nAll notebooks completed successfully!")
