# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 — Trades (raw.trades)
# MAGIC
# MAGIC Generates **~180 K** trade events. This is the primary signal for the propensity model —
# MAGIC HIGH-value customers trade much more frequently and at larger volumes.
# MAGIC
# MAGIC **Schema**
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | trade_id | long | PK |
# MAGIC | customer_id | int | FK |
# MAGIC | trade_ts | timestamp | |
# MAGIC | trading_pair | string | e.g. BTC_MXN |
# MAGIC | base_currency | string | |
# MAGIC | quote_currency | string | |
# MAGIC | side | string | buy / sell |
# MAGIC | amount_base | double | In base currency |
# MAGIC | price_mxn | double | Price per unit |
# MAGIC | volume_mxn | double | Total trade value |
# MAGIC | fee_mxn | double | Fee charged |
# MAGIC | fee_pct | double | Fee rate |
# MAGIC | order_type | string | market / limit |
# MAGIC | platform | string | app_ios / app_android / web / api |
# MAGIC | _ingested_at | timestamp | |

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG     = "bitso_demo"
RANDOM_SEED = 45
START_DATE  = datetime(2023, 1, 1)
END_DATE    = datetime(2024, 12, 31)

rng = np.random.default_rng(RANDOM_SEED)

# COMMAND ----------

customers = spark.sql(f"SELECT customer_id, registration_ts, risk_segment FROM {CATALOG}.raw.customers").toPandas()

TRADE_PARAMS = {
    "HIGH":   {"n": (60, 25), "vol_mu": 18000, "vol_sig": 12000},
    "MEDIUM": {"n": (18,  8), "vol_mu": 3500,  "vol_sig": 2000},
    "LOW":    {"n": (4,   2), "vol_mu": 600,   "vol_sig": 300},
}

# Pairs with approximate base price in MXN (mid-2024 estimates)
PAIRS = [
    {"pair":"BTC_MXN", "base":"BTC","quote":"MXN","price_mu":1_200_000,"price_sig":300_000},
    {"pair":"ETH_MXN", "base":"ETH","quote":"MXN","price_mu":55_000,   "price_sig":15_000},
    {"pair":"USDT_MXN","base":"USDT","quote":"MXN","price_mu":17.0,    "price_sig":0.5},
    {"pair":"XRP_MXN", "base":"XRP", "quote":"MXN","price_mu":9.0,     "price_sig":2.0},
    {"pair":"SOL_MXN", "base":"SOL", "quote":"MXN","price_mu":2800,    "price_sig":800},
    {"pair":"ADA_MXN", "base":"ADA", "quote":"MXN","price_mu":7.0,     "price_sig":2.0},
    {"pair":"MATIC_MXN","base":"MATIC","quote":"MXN","price_mu":12.0,  "price_sig":5.0},
    {"pair":"LTC_MXN", "base":"LTC", "quote":"MXN","price_mu":1200,    "price_sig":300},
    {"pair":"AVAX_MXN","base":"AVAX","quote":"MXN","price_mu":650,     "price_sig":200},
    {"pair":"DOGE_MXN","base":"DOGE","quote":"MXN","price_mu":2.5,     "price_sig":1.0},
]
PAIR_P_HIGH   = [0.30,0.20,0.18,0.07,0.08,0.04,0.04,0.03,0.04,0.02]
PAIR_P_MEDIUM = [0.28,0.20,0.22,0.08,0.06,0.04,0.04,0.03,0.03,0.02]
PAIR_P_LOW    = [0.25,0.15,0.30,0.10,0.05,0.03,0.03,0.03,0.03,0.03]

ORDER_TYPES  = ["market","limit"]
ORDER_P_HIGH = [0.45, 0.55]
ORDER_P_LOW  = [0.70, 0.30]
PLATFORMS    = ["app_ios","app_android","web","api"]
PLAT_HIGH    = [0.35, 0.30, 0.20, 0.15]
PLAT_LOW     = [0.45, 0.40, 0.13, 0.02]

FEE_RATE = 0.0015  # 0.15% taker fee

# COMMAND ----------
# MAGIC %md ## Generate trades

# COMMAND ----------

rows = []
trade_id = 1

for _, cust in customers.iterrows():
    seg = cust["risk_segment"]
    p   = TRADE_PARAMS[seg]
    n   = max(0, int(rng.normal(*p["n"])))
    reg_ts = pd.Timestamp(cust["registration_ts"]).to_pydatetime()
    avail_secs = max(86400, int((END_DATE - reg_ts).total_seconds()))

    pair_p  = PAIR_P_HIGH if seg == "HIGH" else (PAIR_P_MEDIUM if seg == "MEDIUM" else PAIR_P_LOW)
    order_p = ORDER_P_HIGH if seg == "HIGH" else ORDER_P_LOW
    plat_p  = PLAT_HIGH    if seg == "HIGH" else PLAT_LOW

    for _ in range(n):
        offset   = rng.integers(0, avail_secs)
        trade_ts = reg_ts + timedelta(seconds=int(offset))
        if trade_ts > END_DATE:
            trade_ts = END_DATE - timedelta(hours=rng.integers(1, 24))

        pair_info = PAIRS[rng.choice(len(PAIRS), p=pair_p)]
        price     = max(0.01, float(rng.normal(pair_info["price_mu"], pair_info["price_sig"])))
        volume    = max(1.0,  float(rng.normal(p["vol_mu"], p["vol_sig"])))
        amount    = round(volume / price, 8)
        fee       = round(volume * FEE_RATE, 4)

        rows.append({
            "trade_id":       trade_id,
            "customer_id":    int(cust["customer_id"]),
            "trade_ts":       trade_ts,
            "trading_pair":   pair_info["pair"],
            "base_currency":  pair_info["base"],
            "quote_currency": pair_info["quote"],
            "side":           rng.choice(["buy","sell"]),
            "amount_base":    round(amount, 8),
            "price_mxn":      round(price, 4),
            "volume_mxn":     round(volume, 2),
            "fee_mxn":        fee,
            "fee_pct":        FEE_RATE,
            "order_type":     rng.choice(ORDER_TYPES, p=order_p),
            "platform":       rng.choice(PLATFORMS,   p=plat_p),
            "_ingested_at":   datetime.utcnow(),
        })
        trade_id += 1

df = pd.DataFrame(rows)
print(f"Generated {len(df):,} trades — total volume MXN {df['volume_mxn'].sum():,.0f}")

# COMMAND ----------
# MAGIC %md ## Write to raw.trades

# COMMAND ----------

df_spark = spark.createDataFrame(df)
(df_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.raw.trades"))

print(f"  {CATALOG}.raw.trades — {df_spark.count():,} rows")
display(spark.sql(f"""
    SELECT trading_pair,
           COUNT(*) n_trades,
           ROUND(SUM(volume_mxn),0) total_vol_mxn,
           ROUND(AVG(volume_mxn),0) avg_vol_mxn
    FROM {CATALOG}.raw.trades
    GROUP BY 1 ORDER BY 3 DESC
"""))
