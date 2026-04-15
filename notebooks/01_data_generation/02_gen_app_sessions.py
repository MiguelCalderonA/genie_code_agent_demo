# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — App Sessions (raw.app_sessions)
# MAGIC
# MAGIC Generates **~450 K** app session events. HIGH-value customers have ~3× more sessions,
# MAGIC longer durations, and higher trade-initiation rates.
# MAGIC
# MAGIC **Schema**
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | session_id | long | PK |
# MAGIC | customer_id | int | FK → raw.customers |
# MAGIC | session_start_ts | timestamp | After registration date |
# MAGIC | session_end_ts | timestamp | start + duration |
# MAGIC | duration_seconds | int | |
# MAGIC | device_type | string | mobile_ios / mobile_android / web |
# MAGIC | os | string | iOS / Android / Windows / macOS |
# MAGIC | app_version | string | semver |
# MAGIC | screen_views | int | Number of screens visited |
# MAGIC | actions_count | int | Taps / clicks in session |
# MAGIC | primary_feature | string | Main feature used |
# MAGIC | trade_initiated | boolean | Whether a trade was started |
# MAGIC | _ingested_at | timestamp | |

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG     = "bitso_demo"
RANDOM_SEED = 42
START_DATE  = datetime(2023, 1, 1)
END_DATE    = datetime(2024, 12, 31)

rng = np.random.default_rng(RANDOM_SEED)

# COMMAND ----------
# MAGIC %md ## Load customer spine

# COMMAND ----------

customers = spark.sql(f"""
    SELECT customer_id, registration_ts, risk_segment
    FROM {CATALOG}.raw.customers
""").toPandas()

# Sessions per customer by segment
seg_sessions = {"HIGH": (80, 40), "MEDIUM": (30, 15), "LOW": (10, 6)}

DEVICES    = ["mobile_ios","mobile_android","web"]
DEVICE_P   = [0.45, 0.40, 0.15]
OS_MAP     = {"mobile_ios":"iOS","mobile_android":"Android","web": None}
WEB_OS     = ["Windows","macOS","Linux"]
APP_VER    = ["3.8.0","3.9.1","3.10.2","3.11.0","4.0.0","4.1.1"]
FEATURES   = ["market_home","portfolio","trade","p2p_buy","p2p_sell","wallet","rewards","kyc","profile","prices"]
FEAT_HIGH  = [0.05,0.10,0.35,0.15,0.15,0.08,0.04,0.02,0.03,0.03]
FEAT_LOW   = [0.25,0.15,0.10,0.10,0.08,0.12,0.05,0.05,0.05,0.05]

# COMMAND ----------
# MAGIC %md ## Generate sessions

# COMMAND ----------

rows = []
session_id = 1
date_range_secs = int((END_DATE - START_DATE).total_seconds())

for _, cust in customers.iterrows():
    seg  = cust["risk_segment"]
    mu, sigma = seg_sessions[seg]
    n_sessions = max(1, int(rng.normal(mu, sigma)))

    reg_ts = pd.Timestamp(cust["registration_ts"]).to_pydatetime()
    avail_secs = max(1, int((END_DATE - reg_ts).total_seconds()))

    # Duration params by segment
    dur_mu  = 900 if seg == "HIGH" else 480 if seg == "MEDIUM" else 180
    dur_sig = 400 if seg == "HIGH" else 220 if seg == "MEDIUM" else 90

    trade_prob = 0.55 if seg == "HIGH" else 0.25 if seg == "MEDIUM" else 0.06
    feat_p     = FEAT_HIGH if seg == "HIGH" else (FEAT_LOW if seg == "LOW" else None)
    if feat_p is None:
        feat_p = [0.12,0.12,0.22,0.12,0.10,0.10,0.04,0.04,0.08,0.06]

    for _ in range(n_sessions):
        offset      = rng.integers(0, avail_secs)
        start_ts    = reg_ts + timedelta(seconds=int(offset))
        if start_ts > END_DATE:
            start_ts = END_DATE - timedelta(hours=rng.integers(1, 48))

        duration    = max(30, int(rng.normal(dur_mu, dur_sig)))
        end_ts      = start_ts + timedelta(seconds=duration)
        device      = rng.choice(DEVICES, p=DEVICE_P)
        os_val      = OS_MAP[device] if device != "web" else rng.choice(WEB_OS)
        trade_init  = bool(rng.random() < trade_prob)

        rows.append({
            "session_id":       session_id,
            "customer_id":      int(cust["customer_id"]),
            "session_start_ts": start_ts,
            "session_end_ts":   end_ts,
            "duration_seconds": duration,
            "device_type":      device,
            "os":               os_val,
            "app_version":      rng.choice(APP_VER),
            "screen_views":     int(rng.integers(2, 30 if seg == "HIGH" else 15)),
            "actions_count":    int(rng.integers(3, 80 if seg == "HIGH" else 30)),
            "primary_feature":  rng.choice(FEATURES, p=feat_p),
            "trade_initiated":  trade_init,
            "_ingested_at":     datetime.utcnow(),
        })
        session_id += 1

df = pd.DataFrame(rows)
print(f"Generated {len(df):,} sessions for {len(customers):,} customers")

# COMMAND ----------
# MAGIC %md ## Write to raw.app_sessions

# COMMAND ----------

df_spark = spark.createDataFrame(df)
(df_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.raw.app_sessions"))

print(f"  {CATALOG}.raw.app_sessions — {df_spark.count():,} rows")
display(spark.sql(f"""
    SELECT device_type, primary_feature, COUNT(*) n, ROUND(AVG(duration_seconds),0) avg_dur
    FROM {CATALOG}.raw.app_sessions
    GROUP BY 1,2 ORDER BY 1,3 DESC
"""))
