# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — Deposits (raw.deposits)
# MAGIC
# MAGIC Generates **~80 K** deposit events. HIGH-value customers make larger and more frequent deposits.
# MAGIC
# MAGIC **Schema**
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | deposit_id | long | PK |
# MAGIC | customer_id | int | FK |
# MAGIC | deposit_ts | timestamp | |
# MAGIC | amount_local | double | In local currency |
# MAGIC | local_currency | string | MXN / ARS / BRL |
# MAGIC | amount_mxn | double | MXN equivalent |
# MAGIC | currency_received | string | Crypto/stable received |
# MAGIC | payment_method | string | SPEI / PIX / TRANSFER / CARD / CRYPTO |
# MAGIC | bank_name | string | nullable |
# MAGIC | network | string | For crypto deposits |
# MAGIC | status | string | completed / pending / failed / reversed |
# MAGIC | processing_minutes | int | Time to confirm |
# MAGIC | is_first_deposit | boolean | |
# MAGIC | _ingested_at | timestamp | |

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG     = "bitso_demo"
RANDOM_SEED = 44

rng = np.random.default_rng(RANDOM_SEED)

# COMMAND ----------

customers = spark.sql(f"SELECT customer_id, registration_ts, risk_segment, country, preferred_currency FROM {CATALOG}.raw.customers").toPandas()

# Avg deposit amount in MXN by segment
DEPOSIT_PARAMS = {
    "HIGH":   {"n": (18, 6),  "amount_mu": 25000, "amount_sig": 15000},
    "MEDIUM": {"n": (7,  3),  "amount_mu": 6000,  "amount_sig": 3000},
    "LOW":    {"n": (2,  1),  "amount_mu": 1500,  "amount_sig": 800},
}

MX_BANKS = ["BBVA México","Santander México","Banamex","Banorte","HSBC México","Scotiabank MX","Inbursa","Bajío"]
AR_BANKS = ["Banco Nación","Galicia","BBVA Argentina","Santander Argentina","ICBC","Macro","Supervielle","HSBC AR"]
BR_BANKS = ["Itaú","Bradesco","Banco do Brasil","Caixa","Nubank","Santander Brasil","BTG Pactual","Inter"]

METHOD_BY_COUNTRY = {
    "MX": (["SPEI","CARD","CRYPTO"],         [0.65, 0.20, 0.15]),
    "AR": (["TRANSFER","CARD","CRYPTO"],      [0.55, 0.25, 0.20]),
    "BR": (["PIX","CARD","CRYPTO","TRANSFER"],[0.60, 0.18, 0.14, 0.08]),
}
CURRENCIES_RECEIVED = ["MXN","USDT","BTC","ETH","SOL","XRP"]
CURR_P              = [0.30, 0.35, 0.18, 0.10, 0.04, 0.03]
NETWORKS            = ["Bitcoin","Ethereum","Solana","XRP Ledger","Tron","BNB Chain"]
STATUS_P            = [0.88, 0.06, 0.04, 0.02]

FX_MXN = {"MXN": 1.0, "ARS": 0.045, "BRL": 3.8}  # approximate

# COMMAND ----------
# MAGIC %md ## Generate deposits

# COMMAND ----------

rows = []
deposit_id = 1
START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2024, 12, 31)

for _, cust in customers.iterrows():
    seg  = cust["risk_segment"]
    p    = DEPOSIT_PARAMS[seg]
    n    = max(0, int(rng.normal(*p["n"])))
    reg_ts = pd.Timestamp(cust["registration_ts"]).to_pydatetime()
    avail_secs = max(86400, int((END_DATE - reg_ts).total_seconds()))
    country    = cust["country"]
    methods, method_p = METHOD_BY_COUNTRY[country]
    local_currency = {"MX":"MXN","AR":"ARS","BR":"BRL"}[country]
    first_done = False

    for i in range(n):
        offset   = rng.integers(0, avail_secs)
        dep_ts   = reg_ts + timedelta(seconds=int(offset))
        if dep_ts > END_DATE:
            dep_ts = END_DATE - timedelta(hours=rng.integers(1, 48))

        method   = rng.choice(methods, p=method_p)
        is_crypto_dep = (method == "CRYPTO")

        amount_local = max(10.0, round(float(rng.normal(p["amount_mu"], p["amount_sig"])), 2))
        amount_mxn   = round(amount_local * FX_MXN[local_currency], 2)

        bank = None
        if method in ("SPEI","TRANSFER","PIX"):
            banks_pool = {"MX": MX_BANKS, "AR": AR_BANKS, "BR": BR_BANKS}[country]
            bank = rng.choice(banks_pool)

        proc_minutes = int(rng.integers(1, 5)) if method == "PIX" else \
                       int(rng.integers(5, 30)) if method == "SPEI" else \
                       int(rng.integers(30, 120)) if is_crypto_dep else \
                       int(rng.integers(1, 10))

        rows.append({
            "deposit_id":          deposit_id,
            "customer_id":         int(cust["customer_id"]),
            "deposit_ts":          dep_ts,
            "amount_local":        amount_local,
            "local_currency":      local_currency,
            "amount_mxn":          amount_mxn,
            "currency_received":   rng.choice(CURRENCIES_RECEIVED, p=CURR_P),
            "payment_method":      method,
            "bank_name":           bank,
            "network":             rng.choice(NETWORKS) if is_crypto_dep else None,
            "status":              rng.choice(["completed","pending","failed","reversed"], p=STATUS_P),
            "processing_minutes":  proc_minutes,
            "is_first_deposit":    (not first_done),
            "_ingested_at":        datetime.utcnow(),
        })
        first_done = True
        deposit_id += 1

df = pd.DataFrame(rows)
print(f"Generated {len(df):,} deposit events")

# COMMAND ----------
# MAGIC %md ## Write to raw.deposits

# COMMAND ----------

df_spark = spark.createDataFrame(df)
(df_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.raw.deposits"))

print(f"  {CATALOG}.raw.deposits — {df_spark.count():,} rows")
display(spark.sql(f"""
    SELECT payment_method, status, COUNT(*) n, ROUND(AVG(amount_mxn),0) avg_mxn
    FROM {CATALOG}.raw.deposits
    GROUP BY 1,2 ORDER BY 1,3 DESC
"""))
