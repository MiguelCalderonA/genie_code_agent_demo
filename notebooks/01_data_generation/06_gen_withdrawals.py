# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 — Withdrawals (raw.withdrawals)
# MAGIC
# MAGIC Generates **~38 K** withdrawal events.

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG     = "bitso_demo"
RANDOM_SEED = 46
START_DATE  = datetime(2023, 1, 1)
END_DATE    = datetime(2024, 12, 31)
FX_MXN      = {"MXN": 1.0, "ARS": 0.045, "BRL": 3.8}

rng = np.random.default_rng(RANDOM_SEED)

# COMMAND ----------

customers = spark.sql(f"SELECT customer_id, registration_ts, risk_segment, country FROM {CATALOG}.raw.customers").toPandas()

WITHDRAW_PARAMS = {
    "HIGH":   {"n": (10, 4),  "amt_mu": 20000, "amt_sig": 12000},
    "MEDIUM": {"n": (4,  2),  "amt_mu": 5000,  "amt_sig": 2500},
    "LOW":    {"n": (1,  1),  "amt_mu": 1000,  "amt_sig": 500},
}

DEST_TYPES    = ["bank_account","crypto_wallet","p2p_transfer"]
DEST_P        = [0.55, 0.38, 0.07]
CURRENCIES    = ["MXN","USDT","BTC","ETH","SOL","XRP","ARS","BRL"]
CURR_P        = [0.30,0.28,0.15,0.10,0.06,0.04,0.04,0.03]
NETWORKS      = ["Bitcoin","Ethereum","Solana","XRP Ledger","Tron"]
STATUS_P      = [0.86, 0.07, 0.05, 0.02]

# COMMAND ----------
# MAGIC %md ## Generate withdrawals

# COMMAND ----------

rows = []
wid = 1

for _, cust in customers.iterrows():
    seg    = cust["risk_segment"]
    p      = WITHDRAW_PARAMS[seg]
    n      = max(0, int(rng.normal(*p["n"])))
    reg_ts = pd.Timestamp(cust["registration_ts"]).to_pydatetime()
    avail  = max(86400, int((END_DATE - reg_ts).total_seconds()))
    country = cust["country"]
    local_currency = {"MX":"MXN","AR":"ARS","BR":"BRL"}[country]

    for _ in range(n):
        offset = rng.integers(0, avail)
        ts     = reg_ts + timedelta(seconds=int(offset))
        if ts > END_DATE:
            ts = END_DATE - timedelta(hours=rng.integers(1, 24))

        dest    = rng.choice(DEST_TYPES, p=DEST_P)
        curr    = rng.choice(CURRENCIES, p=CURR_P)
        is_crypto = curr not in ("MXN","ARS","BRL")
        amt_local = max(10.0, round(float(rng.normal(p["amt_mu"], p["amt_sig"])), 2))
        fx        = FX_MXN.get(local_currency, 1.0)
        amt_mxn   = round(amt_local * fx, 2)

        rows.append({
            "withdrawal_id":       wid,
            "customer_id":         int(cust["customer_id"]),
            "withdrawal_ts":       ts,
            "amount_local":        amt_local,
            "local_currency":      local_currency,
            "amount_mxn":          amt_mxn,
            "currency_withdrawn":  curr,
            "destination_type":    dest,
            "network":             rng.choice(NETWORKS) if is_crypto else None,
            "status":              rng.choice(["completed","pending","failed","cancelled"], p=STATUS_P),
            "processing_hours":    int(rng.integers(1,3)) if not is_crypto else int(rng.integers(1,72)),
            "fee_mxn":             round(amt_mxn * 0.001, 2),
            "_ingested_at":        datetime.utcnow(),
        })
        wid += 1

df = pd.DataFrame(rows)
print(f"Generated {len(df):,} withdrawal events")

# COMMAND ----------

df_spark = spark.createDataFrame(df)
(df_spark.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.raw.withdrawals"))
print(f"  {CATALOG}.raw.withdrawals — {df_spark.count():,} rows")
display(spark.sql(f"SELECT destination_type, status, COUNT(*) n, ROUND(AVG(amount_mxn),0) avg_mxn FROM {CATALOG}.raw.withdrawals GROUP BY 1,2 ORDER BY 1,3 DESC"))
