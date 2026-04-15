# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Marketing Impressions (raw.marketing_impressions)
# MAGIC
# MAGIC Generates **~350 K** marketing touch-points across channels.
# MAGIC HIGH-value customers have higher click-through and conversion rates.
# MAGIC
# MAGIC **Schema**
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | impression_id | long | PK |
# MAGIC | customer_id | int | FK |
# MAGIC | campaign_id | string | 30 distinct campaigns |
# MAGIC | campaign_name | string | |
# MAGIC | campaign_type | string | acquisition / retention / reactivation / upsell |
# MAGIC | channel | string | email / push / paid_search / display / social / influencer |
# MAGIC | impression_ts | timestamp | |
# MAGIC | clicked | boolean | |
# MAGIC | click_ts | timestamp | nullable |
# MAGIC | converted | boolean | |
# MAGIC | conversion_value_mxn | double | nullable |
# MAGIC | utm_source | string | |
# MAGIC | utm_medium | string | |
# MAGIC | ab_group | string | control / variant_a / variant_b |
# MAGIC | _ingested_at | timestamp | |

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG     = "bitso_demo"
RANDOM_SEED = 43
START_DATE  = datetime(2023, 1, 1)
END_DATE    = datetime(2024, 12, 31)

rng = np.random.default_rng(RANDOM_SEED)

# COMMAND ----------

customers = spark.sql(f"SELECT customer_id, registration_ts, risk_segment FROM {CATALOG}.raw.customers").toPandas()

CHANNELS     = ["email","push","paid_search","display","social","influencer"]
CHANNEL_P    = [0.30, 0.25, 0.18, 0.12, 0.10, 0.05]
CAMP_TYPES   = ["acquisition","retention","reactivation","upsell"]
CAMP_TYPE_P  = [0.30, 0.35, 0.20, 0.15]
AB_GROUPS    = ["control","variant_a","variant_b"]
AB_P         = [0.50, 0.25, 0.25]

# 30 campaigns
campaigns = [
    {"id":"C001","name":"Welcome Crypto","type":"acquisition","utm_source":"google","utm_medium":"cpc"},
    {"id":"C002","name":"SPEI Deposit Boost","type":"retention","utm_source":"email","utm_medium":"email"},
    {"id":"C003","name":"BTC Halving Special","type":"upsell","utm_source":"social","utm_medium":"social"},
    {"id":"C004","name":"First Trade Cashback","type":"acquisition","utm_source":"facebook","utm_medium":"cpc"},
    {"id":"C005","name":"KYC Upgrade Incentive","type":"upsell","utm_source":"push","utm_medium":"push"},
    {"id":"C006","name":"Re-engage Dormant","type":"reactivation","utm_source":"email","utm_medium":"email"},
    {"id":"C007","name":"ETH Staking Launch","type":"upsell","utm_source":"influencer","utm_medium":"influencer"},
    {"id":"C008","name":"Referral Bonus","type":"acquisition","utm_source":"referral","utm_medium":"referral"},
    {"id":"C009","name":"Weekend Trader Promo","type":"retention","utm_source":"push","utm_medium":"push"},
    {"id":"C010","name":"New Coin Listing","type":"retention","utm_source":"email","utm_medium":"email"},
    {"id":"C011","name":"SOL Ecosystem Campaign","type":"upsell","utm_source":"google","utm_medium":"cpc"},
    {"id":"C012","name":"USDT Stable Yield","type":"upsell","utm_source":"display","utm_medium":"display"},
    {"id":"C013","name":"App Download Drive","type":"acquisition","utm_source":"apple","utm_medium":"app_store"},
    {"id":"C014","name":"Holiday Trading","type":"retention","utm_source":"email","utm_medium":"email"},
    {"id":"C015","name":"P2P Market Awareness","type":"acquisition","utm_source":"social","utm_medium":"social"},
    {"id":"C016","name":"30-Day Inactive Win-back","type":"reactivation","utm_source":"email","utm_medium":"email"},
    {"id":"C017","name":"High Volume Reward","type":"upsell","utm_source":"push","utm_medium":"push"},
    {"id":"C018","name":"Zero-Fee Weekend","type":"retention","utm_source":"push","utm_medium":"push"},
    {"id":"C019","name":"New Year New Crypto","type":"acquisition","utm_source":"google","utm_medium":"cpc"},
    {"id":"C020","name":"DeFi Education Series","type":"upsell","utm_source":"email","utm_medium":"email"},
    {"id":"C021","name":"Argentina Peso Hedge","type":"acquisition","utm_source":"social","utm_medium":"social"},
    {"id":"C022","name":"Brazil PIX Onboarding","type":"acquisition","utm_source":"google","utm_medium":"cpc"},
    {"id":"C023","name":"Loyalty Milestone","type":"retention","utm_source":"push","utm_medium":"push"},
    {"id":"C024","name":"Q4 Trading Challenge","type":"upsell","utm_source":"email","utm_medium":"email"},
    {"id":"C025","name":"Security Feature Launch","type":"retention","utm_source":"push","utm_medium":"push"},
    {"id":"C026","name":"Institutional Referral","type":"acquisition","utm_source":"partner","utm_medium":"partner"},
    {"id":"C027","name":"Tax Season Guide","type":"retention","utm_source":"email","utm_medium":"email"},
    {"id":"C028","name":"Mobile App Rating","type":"retention","utm_source":"push","utm_medium":"push"},
    {"id":"C029","name":"Flash Sale 0% Fee","type":"upsell","utm_source":"push","utm_medium":"push"},
    {"id":"C030","name":"Anniversary Special","type":"retention","utm_source":"email","utm_medium":"email"},
]

seg_impressions = {"HIGH": (30, 12), "MEDIUM": (20, 8), "LOW": (8, 4)}

# CTR and CVR by segment
ctr_map = {"HIGH": 0.22, "MEDIUM": 0.12, "LOW": 0.05}
cvr_map = {"HIGH": 0.18, "MEDIUM": 0.08, "LOW": 0.02}

# COMMAND ----------
# MAGIC %md ## Generate impressions

# COMMAND ----------

rows = []
imp_id = 1

for _, cust in customers.iterrows():
    seg = cust["risk_segment"]
    mu, sigma = seg_impressions[seg]
    n_imps = max(0, int(rng.normal(mu, sigma)))
    reg_ts = pd.Timestamp(cust["registration_ts"]).to_pydatetime()
    avail_secs = max(86400, int((END_DATE - reg_ts).total_seconds()))

    for _ in range(n_imps):
        offset   = rng.integers(0, avail_secs)
        imp_ts   = reg_ts + timedelta(seconds=int(offset))
        if imp_ts > END_DATE:
            imp_ts = END_DATE - timedelta(hours=rng.integers(1, 24))

        camp     = campaigns[rng.integers(0, len(campaigns))]
        clicked  = bool(rng.random() < ctr_map[seg])
        click_ts = (imp_ts + timedelta(minutes=int(rng.integers(1, 30)))) if clicked else None
        conv     = bool(clicked and rng.random() < cvr_map[seg])
        conv_val = float(round(rng.uniform(50, 5000), 2)) if conv else None

        rows.append({
            "impression_id":          imp_id,
            "customer_id":            int(cust["customer_id"]),
            "campaign_id":            camp["id"],
            "campaign_name":          camp["name"],
            "campaign_type":          camp["type"],
            "channel":                rng.choice(CHANNELS, p=CHANNEL_P),
            "impression_ts":          imp_ts,
            "clicked":                clicked,
            "click_ts":               click_ts,
            "converted":              conv,
            "conversion_value_mxn":   conv_val,
            "utm_source":             camp["utm_source"],
            "utm_medium":             camp["utm_medium"],
            "ab_group":               rng.choice(AB_GROUPS, p=AB_P),
            "_ingested_at":           datetime.utcnow(),
        })
        imp_id += 1

df = pd.DataFrame(rows)
print(f"Generated {len(df):,} marketing impressions")

# COMMAND ----------
# MAGIC %md ## Write to raw.marketing_impressions

# COMMAND ----------

df_spark = spark.createDataFrame(df)
(df_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.raw.marketing_impressions"))

print(f"  {CATALOG}.raw.marketing_impressions — {df_spark.count():,} rows")
display(spark.sql(f"""
    SELECT channel, COUNT(*) impressions,
           ROUND(SUM(CAST(clicked AS INT))*100.0/COUNT(*),1) ctr_pct,
           ROUND(SUM(CAST(converted AS INT))*100.0/COUNT(*),1) cvr_pct
    FROM {CATALOG}.raw.marketing_impressions
    GROUP BY 1 ORDER BY 2 DESC
"""))
