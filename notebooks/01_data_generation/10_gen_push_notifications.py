# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 10 — Push Notifications (raw.push_notifications)
# MAGIC
# MAGIC Generates **~500 K** push notification events.
# MAGIC HIGH-value customers receive more personalised alerts and have higher open rates.

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG     = "bitso_demo"
RANDOM_SEED = 50
END_DATE    = datetime(2024, 12, 31)

rng = np.random.default_rng(RANDOM_SEED)

# COMMAND ----------

customers = spark.sql(f"SELECT customer_id, registration_ts, risk_segment FROM {CATALOG}.raw.customers").toPandas()

NOTIF_PARAMS = {
    "HIGH":   {"n": (90, 35), "open_rate": 0.42, "action_rate": 0.28},
    "MEDIUM": {"n": (45, 18), "open_rate": 0.28, "action_rate": 0.15},
    "LOW":    {"n": (20,  8), "open_rate": 0.18, "action_rate": 0.06},
}

NOTIF_TYPES  = ["price_alert","trade_confirmation","deposit_confirmation","withdrawal_update",
                "marketing_promo","security_alert","kyc_reminder","referral_reward",
                "market_insight","feature_announcement"]
TYPE_P_HIGH  = [0.30,0.20,0.15,0.10,0.08,0.07,0.03,0.03,0.02,0.02]
TYPE_P_LOW   = [0.15,0.10,0.10,0.08,0.25,0.08,0.12,0.05,0.04,0.03]
CHANNELS     = ["fcm_android","apns_ios","web_push"]
CHANNEL_P    = [0.42, 0.48, 0.10]

# Campaign IDs (subset from marketing_impressions)
CAMPAIGN_IDS = [f"C{str(i).zfill(3)}" for i in range(1, 31)]

# COMMAND ----------
# MAGIC %md ## Generate notifications

# COMMAND ----------

rows = []
nid = 1

for _, cust in customers.iterrows():
    seg    = cust["risk_segment"]
    p      = NOTIF_PARAMS[seg]
    n      = max(0, int(rng.normal(*p["n"])))
    reg_ts = pd.Timestamp(cust["registration_ts"]).to_pydatetime()
    avail  = max(86400, int((END_DATE - reg_ts).total_seconds()))

    type_p = TYPE_P_HIGH if seg == "HIGH" else (TYPE_P_LOW if seg == "LOW" else
             [0.22,0.15,0.12,0.09,0.15,0.08,0.08,0.04,0.04,0.03])

    for _ in range(n):
        offset  = rng.integers(0, avail)
        sent_ts = reg_ts + timedelta(seconds=int(offset))
        if sent_ts > END_DATE:
            sent_ts = END_DATE - timedelta(hours=rng.integers(1, 24))

        ntype   = rng.choice(NOTIF_TYPES, p=type_p)
        opened  = bool(rng.random() < p["open_rate"])
        open_ts = (sent_ts + timedelta(minutes=int(rng.integers(1, 60)))) if opened else None
        action  = bool(opened and rng.random() < p["action_rate"])
        is_mktg = ntype in ("marketing_promo","market_insight","feature_announcement")
        camp_id = rng.choice(CAMPAIGN_IDS) if is_mktg else None
        personal = bool(rng.random() < (0.65 if seg == "HIGH" else 0.30))

        rows.append({
            "notification_id":  nid,
            "customer_id":      int(cust["customer_id"]),
            "sent_ts":          sent_ts,
            "notification_type": ntype,
            "channel":          rng.choice(CHANNELS, p=CHANNEL_P),
            "opened":           opened,
            "opened_ts":        open_ts,
            "action_taken":     action,
            "campaign_id":      camp_id,
            "is_personalised":  personal,
            "_ingested_at":     datetime.utcnow(),
        })
        nid += 1

df = pd.DataFrame(rows)
print(f"Generated {len(df):,} push notifications")

# COMMAND ----------

df_spark = spark.createDataFrame(df)
(df_spark.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.raw.push_notifications"))
print(f"  {CATALOG}.raw.push_notifications — {df_spark.count():,} rows")
display(spark.sql(f"""
    SELECT notification_type, COUNT(*) sent,
           ROUND(SUM(CAST(opened AS INT))*100.0/COUNT(*),1) open_pct,
           ROUND(SUM(CAST(action_taken AS INT))*100.0/COUNT(*),1) action_pct
    FROM {CATALOG}.raw.push_notifications
    GROUP BY 1 ORDER BY 2 DESC
"""))
