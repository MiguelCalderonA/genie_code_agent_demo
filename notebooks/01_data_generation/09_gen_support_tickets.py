# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 09 — Support Tickets (raw.support_tickets)
# MAGIC
# MAGIC Generates **~22 K** customer support tickets.
# MAGIC HIGH-value customers generate more tickets (more activity) but also get faster resolution.

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG     = "bitso_demo"
RANDOM_SEED = 49
END_DATE    = datetime(2024, 12, 31)

rng = np.random.default_rng(RANDOM_SEED)

# COMMAND ----------

customers = spark.sql(f"SELECT customer_id, registration_ts, risk_segment FROM {CATALOG}.raw.customers").toPandas()

TICKET_PARAMS = {
    "HIGH":   {"n": (7,  3), "res_mu": 6,   "res_sig": 4},
    "MEDIUM": {"n": (3,  2), "res_mu": 14,  "res_sig": 8},
    "LOW":    {"n": (1,  1), "res_mu": 24,  "res_sig": 12},
}

CATEGORIES    = ["deposit_issue","withdrawal_issue","trade_issue","kyc_issue","account_access","price_dispute","fee_dispute","feature_request","security_concern","general_inquiry"]
CAT_P         = [0.20,0.15,0.18,0.12,0.10,0.05,0.05,0.05,0.05,0.05]
CHANNELS      = ["live_chat","email","phone","in_app","social_media"]
CHANNEL_P     = [0.40, 0.30, 0.15, 0.10, 0.05]
SENTIMENTS    = ["positive","neutral","negative"]
SENT_HIGH     = [0.25, 0.50, 0.25]
SENT_LOW      = [0.15, 0.40, 0.45]
PRIORITIES    = ["low","medium","high","critical"]
PRIO_HIGH     = [0.15, 0.35, 0.35, 0.15]
PRIO_LOW      = [0.35, 0.45, 0.15, 0.05]

# COMMAND ----------
# MAGIC %md ## Generate tickets

# COMMAND ----------

rows = []
tid = 1

for _, cust in customers.iterrows():
    seg    = cust["risk_segment"]
    p      = TICKET_PARAMS[seg]
    n      = max(0, int(rng.normal(*p["n"])))
    reg_ts = pd.Timestamp(cust["registration_ts"]).to_pydatetime()
    avail  = max(86400, int((END_DATE - reg_ts).total_seconds()))

    sent_p = SENT_HIGH if seg == "HIGH" else (SENT_LOW if seg == "LOW" else [0.20, 0.45, 0.35])
    prio_p = PRIO_HIGH if seg == "HIGH" else PRIO_LOW

    for _ in range(n):
        offset     = rng.integers(0, avail)
        created_ts = reg_ts + timedelta(seconds=int(offset))
        if created_ts > END_DATE:
            created_ts = END_DATE - timedelta(hours=rng.integers(1, 48))

        res_hours = max(0.5, float(rng.normal(p["res_mu"], p["res_sig"])))
        resolved_ts = created_ts + timedelta(hours=res_hours)
        if resolved_ts > END_DATE:
            resolved_ts = END_DATE

        first_resp_hours = res_hours * rng.uniform(0.05, 0.30)
        first_resp_ts    = created_ts + timedelta(hours=first_resp_hours)

        csat = int(rng.integers(1, 6)) if rng.random() < 0.60 else None  # 60% fill CSAT

        rows.append({
            "ticket_id":           tid,
            "customer_id":         int(cust["customer_id"]),
            "created_ts":          created_ts,
            "first_response_ts":   first_resp_ts,
            "resolved_ts":         resolved_ts,
            "category":            rng.choice(CATEGORIES, p=CAT_P),
            "channel":             rng.choice(CHANNELS, p=CHANNEL_P),
            "priority":            rng.choice(PRIORITIES, p=prio_p),
            "sentiment":           rng.choice(SENTIMENTS, p=sent_p),
            "resolution_hours":    round(res_hours, 2),
            "csat_score":          csat,
            "escalated":           bool(rng.random() < (0.25 if seg == "HIGH" else 0.10)),
            "reopened":            bool(rng.random() < 0.08),
            "_ingested_at":        datetime.utcnow(),
        })
        tid += 1

df = pd.DataFrame(rows)
print(f"Generated {len(df):,} support tickets")

# COMMAND ----------

df_spark = spark.createDataFrame(df)
(df_spark.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.raw.support_tickets"))
print(f"  {CATALOG}.raw.support_tickets — {df_spark.count():,} rows")
display(spark.sql(f"SELECT category, sentiment, COUNT(*) n, ROUND(AVG(resolution_hours),1) avg_res_hrs FROM {CATALOG}.raw.support_tickets GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20"))
