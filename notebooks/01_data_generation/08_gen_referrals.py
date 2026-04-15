# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 08 — Referrals (raw.referrals)
# MAGIC
# MAGIC Generates **~5 K** referral pairs. HIGH-value customers refer more people.

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG     = "bitso_demo"
RANDOM_SEED = 48
END_DATE    = datetime(2024, 12, 31)

rng = np.random.default_rng(RANDOM_SEED)

# COMMAND ----------

customers = spark.sql(f"SELECT customer_id, registration_ts, risk_segment FROM {CATALOG}.raw.customers").toPandas()

high_cust   = customers[customers["risk_segment"] == "HIGH"]["customer_id"].values
medium_cust = customers[customers["risk_segment"] == "MEDIUM"]["customer_id"].values
low_cust    = customers[customers["risk_segment"] == "LOW"]["customer_id"].values
all_ids     = customers["customer_id"].values

CHANNELS      = ["in_app_share","whatsapp","email","twitter","instagram","direct_link"]
CHANNEL_P     = [0.30, 0.25, 0.18, 0.10, 0.10, 0.07]
REWARD_STATUS = ["paid","pending","expired","cancelled"]
REWARD_P      = [0.65, 0.20, 0.10, 0.05]

# COMMAND ----------
# MAGIC %md ## Generate referrals

# COMMAND ----------

rows = []
rid = 1
used_pairs = set()

# HIGH referrers: avg 3 referrals each (capped)
# MEDIUM: avg 1.5
# LOW: avg 0.3

referrer_pool = (
    list(rng.choice(high_cust,   size=min(len(high_cust),   1200), replace=False)) * 3 +
    list(rng.choice(medium_cust, size=min(len(medium_cust), 600),  replace=False)) +
    list(rng.choice(low_cust,    size=min(len(low_cust),    200),  replace=False))
)
rng.shuffle(referrer_pool)

reg_ts_map = {int(r["customer_id"]): pd.Timestamp(r["registration_ts"]).to_pydatetime()
              for _, r in customers.iterrows()}

for referrer_id in referrer_pool:
    # Pick a referred customer registered AFTER referrer
    ref_ts = reg_ts_map[int(referrer_id)]
    later  = customers[pd.to_datetime(customers["registration_ts"]) > ref_ts]["customer_id"].values
    if len(later) == 0:
        continue
    referred_id = int(rng.choice(later))
    pair = (int(referrer_id), referred_id)
    if pair in used_pairs:
        continue
    used_pairs.add(pair)

    referred_ts = reg_ts_map[referred_id]
    # Referral event is at the referred customer's registration
    reward_amount = round(float(rng.uniform(50, 500)), 2)
    first_trade_offset = int(rng.integers(1, 30))
    first_trade_ts = referred_ts + timedelta(days=first_trade_offset)
    if first_trade_ts > END_DATE:
        first_trade_ts = None

    rows.append({
        "referral_id":          rid,
        "referrer_customer_id": int(referrer_id),
        "referred_customer_id": referred_id,
        "referral_ts":          referred_ts,
        "channel":              rng.choice(CHANNELS, p=CHANNEL_P),
        "reward_amount_mxn":    reward_amount,
        "reward_status":        rng.choice(REWARD_STATUS, p=REWARD_P),
        "referred_first_trade_ts": first_trade_ts,
        "_ingested_at":         datetime.utcnow(),
    })
    rid += 1

df = pd.DataFrame(rows)
print(f"Generated {len(df):,} referral records")

# COMMAND ----------

df_spark = spark.createDataFrame(df)
(df_spark.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.raw.referrals"))
print(f"  {CATALOG}.raw.referrals — {df_spark.count():,} rows")
display(spark.sql(f"SELECT channel, reward_status, COUNT(*) n, ROUND(AVG(reward_amount_mxn),0) avg_reward FROM {CATALOG}.raw.referrals GROUP BY 1,2 ORDER BY 1,3 DESC"))
