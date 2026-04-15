# Genie Code Agent Demo — Crypto Exchange HVT Propensity

A hands-on demonstration of **Genie Code on Databricks**: an AI coding agent
that builds a production-grade data platform through natural language prompts.

The demo tells a complete data engineering story — from raw mock data to a live
ML propensity model — for a real business problem in the crypto exchange industry:
identifying which customers are likely to become **High-Value Traders (HVT)**.

---

## What is Genie Code?

Genie Code is Databricks' AI coding agent. You describe what you want to build
in plain language and it writes, runs, and debugs the code for you — notebooks,
Delta Live Table pipelines, jobs, ML models, and Genie spaces — all inside your
Databricks workspace.

This repo demonstrates Genie Code's ability to build an **end-to-end lakehouse
platform** starting from nothing but raw data and a sequence of prompts.

---

## The Business Problem

A crypto exchange operating across Latin America wants to predict which customers
have the highest propensity to become high-volume traders — defined as customers
who trade **≥ 200,000 MXN per month** and execute **≥ 10 trades per month**.
Identifying these customers early allows the marketing team to:

- Prioritise onboarding incentives and KYC upgrade nudges
- Personalise push notifications and campaign targeting
- Assign dedicated account management resources
- Reduce churn among the highest-LTV segment

---

## How the Demo Works

The demo has two parts:

### Part 1 — Infrastructure (built with Claude Code)

Everything in this repo was scaffolded by Claude Code: 10 mock data generation
scripts, a full medallion architecture, and an ML propensity model — all committed
to git and uploaded to a Databricks workspace automatically.

### Part 2 — Platform (built with Genie Code)

Starting with only the raw tables in the catalog, a user walks through
[14 structured prompts](docs/genie_code_prompts.md) in Genie Code on Databricks.
Genie Code builds the entire platform layer by layer: DLT pipelines, jobs,
EDA notebooks, a Genie space, and an ML endpoint — with no manual coding.

---

## Data Model

10 synthetic raw tables represent a crypto exchange data estate across
Mexico, Argentina, and Brazil:

| Table | Records | Description |
|-------|---------|-------------|
| `customers` | 10,000 | Demographics, KYC level, risk segment (MX / AR / BR) |
| `app_sessions` | ~450K | Device, feature usage, session duration, trade initiation |
| `marketing_impressions` | ~350K | 30 campaigns across 6 channels, CTR, conversions |
| `deposits` | ~80K | SPEI / PIX / card / crypto deposits in local currency |
| `trades` | ~180K | 10 crypto pairs (BTC, ETH, SOL…) with volume in MXN |
| `withdrawals` | ~38K | Bank and crypto withdrawals |
| `kyc_events` | ~22K | Submission, approval, and rejection lifecycle |
| `referrals` | ~5K | Referrer → referred pairs with reward status |
| `support_tickets` | ~22K | Category, CSAT score, resolution time, sentiment |
| `push_notifications` | ~500K | Open rates, action rates, personalisation flag |

All tables share `customer_id` as the foreign key linking back to `customers`.
Customers are pre-segmented into HIGH / MEDIUM / LOW risk segments, which
drives correlated activity volumes across every table — creating a realistic,
learnable signal for the ML model.

---

## Medallion Architecture

```
catalog.raw              Raw mock data (static landing zone)
       │
       ▼
catalog.bronze           Raw + audit metadata + DLT quality gates
       │
       ├──► silver.customers          KYC resolved, enriched demographics
       ├──► silver.transactions       Unified deposit + trade + withdrawal ledger
       │    └──► silver.monthly_trade_summary   Monthly grain for HVT labelling
       └──► silver.engagement_summary  Session, marketing & push metrics per customer
                         │
                         ▼
              gold.customer_360        Single business view — powers Genie AI/BI
              gold.ml_features         Encoded, imputed feature table (label = is_HVT)
              gold.ml_feature_metadata Feature dictionary
                         │
                         ▼
              gold.propensity_scores   Model output — score, decile, predicted HVT flag
              gold.hvt_propensity_model  Registered model in Unity Catalog
```

---

## Repo Structure

```
genie_code_agent_demo/
│
├── notebooks/
│   ├── 00_setup/
│   │   └── 00_setup.py                  Create catalog + 4 schemas
│   │
│   ├── 01_data_generation/              10 mock-data generators + orchestrator
│   │   ├── 01_gen_customers.py
│   │   ├── 02_gen_app_sessions.py
│   │   ├── 03_gen_marketing_impressions.py
│   │   ├── 04_gen_deposits.py
│   │   ├── 05_gen_trades.py
│   │   ├── 06_gen_withdrawals.py
│   │   ├── 07_gen_kyc_events.py
│   │   ├── 08_gen_referrals.py
│   │   ├── 09_gen_support_tickets.py
│   │   ├── 10_gen_push_notifications.py
│   │   └── 99_run_all.py               Runs all 10 generators in order
│   │
│   ├── 02_bronze/
│   │   └── 01_bronze_ingestion.py      Audit metadata + row-count validation
│   │
│   ├── 03_silver/
│   │   ├── 01_silver_customers.py      KYC resolution + support enrichment
│   │   ├── 02_silver_transactions.py   Unified ledger + monthly summary
│   │   └── 03_silver_engagement.py     Session + marketing + push aggregations
│   │
│   ├── 04_gold/
│   │   ├── 01_gold_customer_360.py     Full customer view + HVT label + tier
│   │   └── 02_gold_ml_features.py      Encoded features + feature metadata table
│   │
│   └── 05_ml/
│       └── 01_propensity_model.py      GBT model + MLflow + propensity_scores table
│
└── docs/
    └── genie_code_prompts.md           14 Genie Code prompts (the demo script)
```

---

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog and Genie enabled
- Databricks CLI installed and configured (`databricks auth login`)
- Python 3.10+ (for local script development only)

### Step 1 — Generate the raw data

Run the `00_setup` notebook first, then `01_data_generation/99_run_all`
in your Databricks workspace. This creates all 10 raw Delta tables (~1.6M total records).

### Step 2 — Run the Genie Code demo

Open Genie Code in your Databricks workspace and follow the prompts in
[docs/genie_code_prompts.md](docs/genie_code_prompts.md) in order.

The 14 prompts guide Genie Code to build:

1. **Bronze DLT pipeline** — quality-gated ingestion with expectations
2. **Silver DLT pipelines** — customers, transactions, engagement (run in parallel)
3. **Gold DLT pipelines** — customer 360 + ML feature store
4. **Delta constraints & UC tags** — governance and PII classification
5. **Orchestration job** — full pipeline DAG with correct dependency order
6. **EDA notebooks** — customer segmentation, trading behaviour, marketing funnel
7. **Genie AI/BI space** — natural language analytics on the gold layer
8. **Propensity model** — classifier tracked with MLflow, scored to a gold table
9. **Production deployment** — Model Serving endpoint + smoke test

### Step 3 — Explore with Genie

Once the gold layer is built, open the Genie space and ask questions like:
- *"Which acquisition channel produces the most high-value traders?"*
- *"List the top 20 customers by propensity score who are not yet HVTs."*
- *"Show monthly trading volume trends for GOLD tier customers in Mexico."*

---

## Why This Demo?

| Traditional approach | With Genie Code |
|---------------------|-----------------|
| Write DLT pipeline code from scratch | Describe the pipeline, Genie writes it |
| Look up DLT expectation syntax | Describe the quality rules in plain English |
| Manually wire job dependencies | Describe the execution order, Genie configures the DAG |
| Build ML features by hand | Describe the feature logic, Genie generates the code |
| Write REST API calls for Genie space | Describe the space, Genie configures it |

The same platform that took days to build manually is assembled in a single
guided session — without sacrificing code quality, data governance, or ML rigor.
