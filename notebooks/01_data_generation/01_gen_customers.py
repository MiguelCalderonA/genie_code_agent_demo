# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 — Customer Profiles (raw.customers)
# MAGIC
# MAGIC Generates **10,000** synthetic Bitso customer profiles across Mexico, Argentina, and Brazil.
# MAGIC Customers are pre-segmented into `HIGH / MEDIUM / LOW` risk_segment, which drives
# MAGIC correlated activity volumes in every downstream table.
# MAGIC
# MAGIC **Schema**
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | customer_id | int | PK, 1–10000 |
# MAGIC | email | string | Unique |
# MAGIC | first_name, last_name | string | Localised by country |
# MAGIC | phone | string | E.164 format by country |
# MAGIC | country | string | MX / AR / BR |
# MAGIC | city | string | Top cities per country |
# MAGIC | gender | string | M / F |
# MAGIC | birth_date | date | Age 18–65 |
# MAGIC | registration_date | date | 2023-01-01 – 2024-12-31 |
# MAGIC | referral_source | string | Channel that acquired the customer |
# MAGIC | kyc_level | int | 0=none 1=basic 2=advanced 3=full |
# MAGIC | risk_segment | string | HIGH / MEDIUM / LOW |
# MAGIC | preferred_currency | string | MXN / USDT / BTC / ARS / BRL |
# MAGIC | language | string | es / pt |
# MAGIC | is_active | boolean | |

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG       = "bitso_demo"
N_CUSTOMERS   = 10_000
RANDOM_SEED   = 42
START_DATE    = datetime(2023, 1, 1)
END_DATE      = datetime(2024, 12, 31)

np.random.seed(RANDOM_SEED)

# COMMAND ----------
# MAGIC %md ## Reference data

# COMMAND ----------

MX_FIRST_M = ["Carlos","José","Luis","Jorge","Miguel","Roberto","Fernando","Ricardo","Eduardo","Manuel",
               "Alejandro","David","Antonio","Francisco","Pablo","Andrés","Óscar","Víctor","Sergio","Iván"]
MX_FIRST_F = ["María","Ana","Laura","Gabriela","Sofía","Valeria","Daniela","Alejandra","Fernanda","Paola",
               "Claudia","Mónica","Patricia","Verónica","Marcela","Silvia","Carmen","Elena","Leticia","Karla"]
AR_FIRST_M = ["Matías","Lucas","Martín","Sebastián","Nicolás","Leandro","Emiliano","Facundo","Agustín","Gastón",
               "Diego","Gonzalo","Hernán","Maximiliano","Rodrigo","Santiago","Pablo","Tomás","Leonardo","Cristian"]
AR_FIRST_F = ["Florencia","Valentina","Camila","Luciana","Natalia","Romina","Noelia","Marina","Silvina","Carina",
               "Antonella","Carolina","Melina","Micaela","Soledad","Tamara","Vanina","Ximena","Yanina","Zaira"]
BR_FIRST_M = ["João","Pedro","Lucas","Gabriel","Mateus","Rafael","Felipe","Bruno","Gustavo","Thiago",
               "André","Eduardo","Rodrigo","Leonardo","Marcelo","Fernando","Diego","Alexandre","Paulo","Ricardo"]
BR_FIRST_F = ["Maria","Ana","Larissa","Camila","Beatriz","Juliana","Amanda","Leticia","Isabela","Fernanda",
               "Gabriela","Patricia","Carla","Claudia","Daniela","Mariana","Renata","Simone","Vanessa","Luciana"]

MX_LAST = ["García","Hernández","Martínez","López","González","Pérez","Sánchez","Ramírez","Torres","Flores",
           "Rivera","Gómez","Díaz","Cruz","Reyes","Morales","Jiménez","Ortega","Castillo","Ramos"]
AR_LAST = ["González","Fernández","Rodríguez","Martínez","López","García","Pérez","Sánchez","Giménez","Díaz",
           "Suárez","Medina","Romero","Álvarez","Molina","Ruiz","Acosta","Castro","Moreno","Silva"]
BR_LAST = ["Silva","Santos","Oliveira","Souza","Lima","Pereira","Costa","Rodrigues","Almeida","Nascimento",
           "Carvalho","Ferreira","Araújo","Ribeiro","Gomes","Martins","Barbosa","Rocha","Moura","Cavalcanti"]

MX_CITIES = ["Ciudad de México","Guadalajara","Monterrey","Puebla","Tijuana","León","Zapopan","Mérida","Querétaro","San Luis Potosí"]
AR_CITIES = ["Buenos Aires","Córdoba","Rosario","Mendoza","Tucumán","La Plata","Mar del Plata","Salta","Santa Fe","San Juan"]
BR_CITIES = ["São Paulo","Rio de Janeiro","Salvador","Fortaleza","Belo Horizonte","Manaus","Curitiba","Recife","Porto Alegre","Belém"]

EMAIL_DOMAINS      = ["gmail.com","hotmail.com","yahoo.com","outlook.com","icloud.com","live.com","protonmail.com"]
EMAIL_DOMAIN_PROBS = [0.45, 0.20, 0.15, 0.10, 0.05, 0.03, 0.02]
REFERRAL_SOURCES   = ["organic_search","paid_search","social_media","referral_program","app_store","influencer","email_campaign","partner"]
REFERRAL_PROBS     = [0.20, 0.15, 0.25, 0.18, 0.10, 0.07, 0.03, 0.02]

# COMMAND ----------
# MAGIC %md ## Generate customers

# COMMAND ----------

rng = np.random.default_rng(RANDOM_SEED)

countries = rng.choice(["MX","AR","BR"], size=N_CUSTOMERS, p=[0.60, 0.25, 0.15])
genders   = rng.choice(["M","F"],         size=N_CUSTOMERS, p=[0.58, 0.42])

first_names, last_names, cities = [], [], []
for c, g in zip(countries, genders):
    if c == "MX":
        first_names.append(rng.choice(MX_FIRST_M if g == "M" else MX_FIRST_F))
        last_names.append(rng.choice(MX_LAST));  cities.append(rng.choice(MX_CITIES))
    elif c == "AR":
        first_names.append(rng.choice(AR_FIRST_M if g == "M" else AR_FIRST_F))
        last_names.append(rng.choice(AR_LAST));  cities.append(rng.choice(AR_CITIES))
    else:
        first_names.append(rng.choice(BR_FIRST_M if g == "M" else BR_FIRST_F))
        last_names.append(rng.choice(BR_LAST));  cities.append(rng.choice(BR_CITIES))

# Registration dates spread over 2 years via beta distribution
date_range_days = (END_DATE - START_DATE).days
reg_offsets = (rng.beta(1.5, 1.5, N_CUSTOMERS) * date_range_days).astype(int)
reg_dates   = [START_DATE + timedelta(days=int(d)) for d in reg_offsets]

# Risk segments: first shuffle keeps distribution but breaks ordering assumption
risk_segments = (["HIGH"] * 2000 + ["MEDIUM"] * 3000 + ["LOW"] * 5000)
rng.shuffle(risk_segments := np.array(risk_segments))

# KYC level correlated with risk segment
kyc_map = {
    "HIGH":   ([1, 2, 3],    [0.10, 0.35, 0.55]),
    "MEDIUM": ([0, 1, 2, 3], [0.10, 0.30, 0.45, 0.15]),
    "LOW":    ([0, 1, 2, 3], [0.25, 0.45, 0.25, 0.05]),
}
kyc_levels = [int(rng.choice(kyc_map[s][0], p=kyc_map[s][1])) for s in risk_segments]

# Ages 18–65
ages       = np.clip(rng.normal(30, 8, N_CUSTOMERS), 18, 65).astype(int)
birth_dates = [(START_DATE - timedelta(days=int(a * 365.25))).date() for a in ages]

# Emails – normalise accented chars
def _clean(s):
    return (s.lower()
             .replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
             .replace("ñ","n").replace("ü","u").replace("ã","a").replace("ê","e").replace("ô","o")
             .replace(" ",""))
domains  = rng.choice(EMAIL_DOMAINS, size=N_CUSTOMERS, p=EMAIL_DOMAIN_PROBS)
suffixes = rng.integers(1, 999, size=N_CUSTOMERS)
emails   = [f"{_clean(fn)}.{_clean(ln)[:6]}{sfx}@{d}"
            for fn, ln, sfx, d in zip(first_names, last_names, suffixes, domains)]

# Preferred currency by country
pref_currency = []
for c in countries:
    if c == "MX":   pref_currency.append(rng.choice(["MXN","USDT","BTC"], p=[0.55, 0.30, 0.15]))
    elif c == "AR": pref_currency.append(rng.choice(["USDT","ARS","BTC"],  p=[0.60, 0.25, 0.15]))
    else:           pref_currency.append(rng.choice(["BRL","USDT","BTC"],  p=[0.50, 0.35, 0.15]))

# Phones
phones = []
for c in countries:
    if c == "MX":   phones.append(f"+52 {rng.integers(10,99)} {rng.integers(1000,9999)} {rng.integers(1000,9999)}")
    elif c == "AR": phones.append(f"+54 {rng.integers(11,99)} {rng.integers(1000,9999)}-{rng.integers(1000,9999)}")
    else:           phones.append(f"+55 {rng.integers(11,99)} {rng.integers(10000,99999)}-{rng.integers(1000,9999)}")

df = pd.DataFrame({
    "customer_id":       range(1, N_CUSTOMERS + 1),
    "email":             emails,
    "first_name":        first_names,
    "last_name":         last_names,
    "phone":             phones,
    "country":           countries,
    "city":              cities,
    "gender":            genders,
    "birth_date":        birth_dates,
    "registration_date": [d.date() for d in reg_dates],
    "registration_ts":   reg_dates,
    "referral_source":   rng.choice(REFERRAL_SOURCES, size=N_CUSTOMERS, p=REFERRAL_PROBS),
    "kyc_level":         kyc_levels,
    "risk_segment":      risk_segments,
    "preferred_currency":pref_currency,
    "language":          ["pt" if c == "BR" else "es" for c in countries],
    "is_active":         rng.choice([True, False], size=N_CUSTOMERS, p=[0.85, 0.15]),
    "_ingested_at":      datetime.utcnow(),
})

print(f"Generated {len(df):,} customers")
print(df["risk_segment"].value_counts().to_string())

# COMMAND ----------
# MAGIC %md ## Write to raw.customers

# COMMAND ----------

df_spark = spark.createDataFrame(df)
(df_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.raw.customers"))

print(f"  {CATALOG}.raw.customers — {df_spark.count():,} rows")
display(spark.sql(f"SELECT risk_segment, country, COUNT(*) n FROM {CATALOG}.raw.customers GROUP BY 1,2 ORDER BY 1,2"))
