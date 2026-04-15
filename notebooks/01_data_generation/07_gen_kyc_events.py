# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 07 — KYC Events (raw.kyc_events)
# MAGIC
# MAGIC Generates **~22 K** KYC lifecycle events. Each customer who has a kyc_level > 0
# MAGIC has at least one submission event; upgrades and rejections follow realistic probabilities.

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG     = "bitso_demo"
RANDOM_SEED = 47
END_DATE    = datetime(2024, 12, 31)

rng = np.random.default_rng(RANDOM_SEED)

# COMMAND ----------

customers = spark.sql(f"""
    SELECT customer_id, registration_ts, risk_segment, kyc_level, country
    FROM {CATALOG}.raw.customers
""").toPandas()

DOC_TYPES      = ["national_id","passport","driver_license","voter_id","proof_of_address","selfie_id","proof_of_income"]
EVENT_TYPES    = ["submitted","approved","rejected","upgrade_requested","downgraded"]
REJECT_REASONS = ["blurry_document","expired_document","name_mismatch","address_mismatch",
                  "unsupported_document_type","selfie_mismatch","incomplete_form","sanctions_match"]
REVIEWER_TYPES = ["automated","manual_tier1","manual_tier2"]
REVIEWER_P     = [0.55, 0.30, 0.15]

# COMMAND ----------
# MAGIC %md ## Generate KYC events

# COMMAND ----------

rows = []
eid = 1

for _, cust in customers.iterrows():
    final_level = int(cust["kyc_level"])
    if final_level == 0:
        continue  # unverified, no events

    reg_ts = pd.Timestamp(cust["registration_ts"]).to_pydatetime()
    avail  = max(7200, int((END_DATE - reg_ts).total_seconds()))

    current_level = 0
    ts = reg_ts + timedelta(minutes=int(rng.integers(30, 1440)))  # first KYC attempt 30min–1day after signup

    for target_level in range(1, final_level + 1):
        # Submission event
        rows.append({
            "event_id":           eid,
            "customer_id":        int(cust["customer_id"]),
            "event_ts":           ts,
            "event_type":         "submitted",
            "kyc_level_before":   current_level,
            "kyc_level_after":    current_level,
            "document_type":      rng.choice(DOC_TYPES[:4] if target_level == 1 else DOC_TYPES),
            "rejection_reason":   None,
            "reviewer_type":      rng.choice(REVIEWER_TYPES, p=REVIEWER_P),
            "country":            cust["country"],
            "_ingested_at":       datetime.utcnow(),
        })
        eid += 1
        ts += timedelta(hours=int(rng.integers(1, 48)))

        # Possible rejection before final approval
        if rng.random() < 0.20:
            rows.append({
                "event_id":         eid,
                "customer_id":      int(cust["customer_id"]),
                "event_ts":         ts,
                "event_type":       "rejected",
                "kyc_level_before": current_level,
                "kyc_level_after":  current_level,
                "document_type":    rows[-1]["document_type"],
                "rejection_reason": rng.choice(REJECT_REASONS),
                "reviewer_type":    rng.choice(REVIEWER_TYPES, p=REVIEWER_P),
                "country":          cust["country"],
                "_ingested_at":     datetime.utcnow(),
            })
            eid += 1
            ts += timedelta(hours=int(rng.integers(24, 72)))

            # Resubmission
            rows.append({
                "event_id":         eid,
                "customer_id":      int(cust["customer_id"]),
                "event_ts":         ts,
                "event_type":       "submitted",
                "kyc_level_before": current_level,
                "kyc_level_after":  current_level,
                "document_type":    rng.choice(DOC_TYPES),
                "rejection_reason": None,
                "reviewer_type":    rng.choice(REVIEWER_TYPES, p=REVIEWER_P),
                "country":          cust["country"],
                "_ingested_at":     datetime.utcnow(),
            })
            eid += 1
            ts += timedelta(hours=int(rng.integers(1, 24)))

        # Approval
        rows.append({
            "event_id":         eid,
            "customer_id":      int(cust["customer_id"]),
            "event_ts":         ts,
            "event_type":       "approved",
            "kyc_level_before": current_level,
            "kyc_level_after":  target_level,
            "document_type":    rows[-1]["document_type"],
            "rejection_reason": None,
            "reviewer_type":    rng.choice(REVIEWER_TYPES, p=REVIEWER_P),
            "country":          cust["country"],
            "_ingested_at":     datetime.utcnow(),
        })
        eid += 1
        current_level = target_level
        ts += timedelta(days=int(rng.integers(30, 180)))  # gap between level upgrades
        if ts > END_DATE:
            break

df = pd.DataFrame(rows)
print(f"Generated {len(df):,} KYC events for {df['customer_id'].nunique():,} customers")

# COMMAND ----------

df_spark = spark.createDataFrame(df)
(df_spark.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.raw.kyc_events"))
print(f"  {CATALOG}.raw.kyc_events — {df_spark.count():,} rows")
display(spark.sql(f"SELECT event_type, COUNT(*) n FROM {CATALOG}.raw.kyc_events GROUP BY 1 ORDER BY 2 DESC"))
