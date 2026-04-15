# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold — ML Feature Store (gold.ml_features)
# MAGIC
# MAGIC Flattened, ML-ready feature table derived from gold.customer_360.
# MAGIC All categorical variables are encoded. Nulls are imputed.
# MAGIC This table is consumed directly by the propensity model notebook.
# MAGIC
# MAGIC **Target**: `label` = 1 if customer is a high-value trader, 0 otherwise.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

CATALOG = "bitso_demo"

# COMMAND ----------

c360 = spark.table(f"{CATALOG}.gold.customer_360")

# COMMAND ----------
# MAGIC %md ## Feature engineering

# COMMAND ----------

features = (
    c360
    # --- Numeric features (direct) ---
    .select(
        "customer_id",
        "is_high_value_trader",
        # Demographics
        F.col("customer_age_years").alias("f_age_years"),
        F.col("days_since_registration").alias("f_tenure_days"),
        # KYC
        F.col("kyc_level_final").alias("f_kyc_level"),
        F.col("was_referred").cast("int").alias("f_was_referred"),
        # Trading
        F.coalesce("lifetime_trade_count",  F.lit(0)).alias("f_lifetime_trade_count"),
        F.coalesce("lifetime_volume_mxn",   F.lit(0)).alias("f_lifetime_volume_mxn"),
        F.coalesce("avg_trade_volume_mxn",  F.lit(0)).alias("f_avg_trade_volume_mxn"),
        F.coalesce("unique_pairs_traded",   F.lit(0)).alias("f_unique_pairs_traded"),
        F.coalesce("active_trading_months", F.lit(0)).alias("f_active_trading_months"),
        F.coalesce("days_since_last_trade", F.lit(999)).alias("f_days_since_last_trade"),
        F.coalesce("deposit_to_volume_ratio",F.lit(0)).alias("f_deposit_to_vol_ratio"),
        # Deposits
        F.coalesce("total_deposits",        F.lit(0)).alias("f_total_deposits"),
        F.coalesce("total_deposited_mxn",   F.lit(0)).alias("f_total_deposited_mxn"),
        F.coalesce("avg_deposit_mxn",       F.lit(0)).alias("f_avg_deposit_mxn"),
        F.coalesce("payment_methods_used",  F.lit(0)).alias("f_payment_methods_used"),
        # Sessions
        F.coalesce("total_sessions",           F.lit(0)).alias("f_total_sessions"),
        F.coalesce("avg_session_duration_secs",F.lit(0)).alias("f_avg_session_dur_secs"),
        F.coalesce("unique_features_used",     F.lit(0)).alias("f_unique_features_used"),
        F.coalesce("trade_session_rate",       F.lit(0)).alias("f_trade_session_rate"),
        # Marketing
        F.coalesce("total_impressions",        F.lit(0)).alias("f_total_impressions"),
        F.coalesce("ctr",                      F.lit(0)).alias("f_mktg_ctr"),
        F.coalesce("unique_campaigns_reached", F.lit(0)).alias("f_unique_campaigns"),
        # Push
        F.coalesce("push_open_rate",   F.lit(0)).alias("f_push_open_rate"),
        F.coalesce("push_action_rate", F.lit(0)).alias("f_push_action_rate"),
        # Support
        F.coalesce("total_tickets",    F.lit(0)).alias("f_support_tickets"),
        F.coalesce("avg_csat",         F.lit(3.0)).alias("f_avg_csat"),
        # Referrals
        F.coalesce("referrals_given",  F.lit(0)).alias("f_referrals_given"),
        # Categorical (will be indexed)
        F.coalesce("country",          F.lit("MX")).alias("country"),
        F.coalesce("gender",           F.lit("M")).alias("gender"),
        F.coalesce("risk_segment",     F.lit("LOW")).alias("risk_segment"),
        F.coalesce("referral_source",  F.lit("organic_search")).alias("referral_source"),
        F.coalesce("preferred_currency",F.lit("MXN")).alias("preferred_currency"),
    )
    .withColumn("label", F.col("is_high_value_trader").cast("int"))
)

# COMMAND ----------
# MAGIC %md ## Encode categoricals

# COMMAND ----------

cat_cols = ["country","gender","risk_segment","referral_source","preferred_currency"]
indexers = [StringIndexer(inputCol=c, outputCol=f"f_{c}_idx", handleInvalid="keep") for c in cat_cols]

pipeline = Pipeline(stages=indexers)
model    = pipeline.fit(features)
features_encoded = model.transform(features).drop(*cat_cols)

# COMMAND ----------
# MAGIC %md ## Write gold.ml_features

# COMMAND ----------

(features_encoded.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.ml_features"))

count = spark.table(f"{CATALOG}.gold.ml_features").count()
pos   = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.gold.ml_features WHERE label=1").collect()[0][0]
print(f"  {CATALOG}.gold.ml_features — {count:,} rows  |  label=1: {pos:,} ({100*pos/count:.1f}%)")

feature_cols = [c for c in features_encoded.columns if c.startswith("f_")]
print(f"  Feature columns ({len(feature_cols)}): {feature_cols}")

display(spark.table(f"{CATALOG}.gold.ml_features").limit(5))
