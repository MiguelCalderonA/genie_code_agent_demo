# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bitso Marketing Demo — Environment Setup
# MAGIC
# MAGIC Creates the Unity Catalog structure for the end-to-end propensity-to-high-value-trader project.
# MAGIC
# MAGIC | Layer | Schema | Purpose |
# MAGIC |-------|--------|---------|
# MAGIC | Raw | `raw` | Generated mock data landing zone |
# MAGIC | Bronze | `bronze` | Ingested raw data + audit metadata |
# MAGIC | Silver | `silver` | Cleaned, deduplicated, enriched |
# MAGIC | Gold | `gold` | Business-ready aggregations + ML features |

# COMMAND ----------

CATALOG = "bitso_demo"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'Bitso end-to-end marketing analytics demo'")

for schema in ["raw", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"  schema created: {CATALOG}.{schema}")

print(f"\n Catalog '{CATALOG}' ready with 4 schemas.")
