# Bitso HVT Propensity — Genie Code Prompt Playbook

End-to-end walkthrough for building the full medallion architecture,
ML propensity model, and Genie space using **Genie Code on Databricks**,
starting only from the 10 raw Delta tables in `bitso_demo.raw`.

> **How to use this doc**
> Open Genie Code in your Databricks workspace, paste each prompt in order,
> review what it generates, approve / tweak, then move to the next prompt.
> Each prompt builds on the output of the previous one.

---

## Context you should paste at the start of every new Genie Code session

```
I'm working on the Bitso HVT (High-Value Trader) propensity project.
Catalog: bitso_demo
Raw schema: bitso_demo.raw
10 raw tables: customers, app_sessions, marketing_impressions, deposits,
               trades, withdrawals, kyc_events, referrals,
               support_tickets, push_notifications.
All tables are linked by customer_id (integer PK in customers,
FK in all other tables). Timestamps are after each customer's
registration_ts. The goal is to predict which customers will become
high-value traders (monthly trade volume ≥ 200 K MXN AND ≥ 10 trades/month).
```

---

## Prompt 01 — Raw Data Discovery

**Goal:** Understand the shape, quality, and relationships of all 10 raw tables
before writing a single line of pipeline code.

```
Explore all 10 tables in the bitso_demo.raw schema and produce a data discovery report.

For each table, show me:
1. Exact row count and column count
2. Full schema with data types
3. Null count and null percentage per column
4. Duplicate count on the primary key column
5. Min / max of the main timestamp column to confirm the date range
6. Value distribution of the top categorical columns (country, status, risk_segment, etc.)
7. Referential integrity check: for every table that has a customer_id column,
   what percentage of those customer_ids exist in bitso_demo.raw.customers?

Also give me a summary of the relationships:
- customers is the spine (customer_id 1–10000)
- Every other table uses customer_id as a foreign key
- referrals uses both referrer_customer_id and referred_customer_id
- kyc_events tracks lifecycle events (submitted → approved/rejected) per customer

Flag any data quality issues I should address in the Bronze pipeline.
```

---

## Prompt 02 — Bronze Layer: Spark Declarative Pipeline

**Goal:** Create a DLT (Spark Declarative Pipeline) notebook that ingests all
10 raw tables into `bitso_demo.bronze` with audit metadata and expectations.

```
Create a Spark Declarative Pipeline (Delta Live Tables) notebook called
"bitso_bronze_pipeline" that ingests all 10 raw tables from bitso_demo.raw
into bitso_demo.bronze.

Requirements:
1. Use Python DLT syntax (@dlt.table decorator, @dlt.expect, @dlt.expect_or_drop).
2. Read each raw table with spark.read.table() (batch, not streaming) since
   the raw layer is a static Delta table landing zone.
3. For every bronze table, add these 3 audit columns:
   - _source_table (string literal with the full raw table path)
   - _bronze_ingested_at (current_timestamp())
   - _pipeline_run_id (dlt.read_metadata or spark.conf.get pipeline ID)
4. Apply DLT expectations with expect_or_drop() on critical quality rules:
   - ALL tables: customer_id IS NOT NULL (except referrals: use referrer_customer_id IS NOT NULL)
   - customers: email IS NOT NULL, registration_date IS NOT NULL, kyc_level BETWEEN 0 AND 3
   - trades: volume_mxn > 0, fee_mxn >= 0, side IN ('buy','sell')
   - deposits: amount_mxn > 0, status IN ('completed','pending','failed','reversed')
   - withdrawals: amount_mxn > 0
   - kyc_events: event_type IN ('submitted','approved','rejected','upgrade_requested','downgraded')
   - marketing_impressions: impression_ts IS NOT NULL
   - app_sessions: duration_seconds > 0, session_start_ts IS NOT NULL
5. Use expect() (warning only, keep the row) for soft quality rules:
   - trades: price_mxn > 0
   - deposits: processing_minutes > 0
   - customers: is_active IS NOT NULL
6. Set pipeline target catalog to bitso_demo, target schema to bronze.
7. Add a comment block at the top explaining the bronze layer's purpose
   (no transformations, just audit trail + quality gates).

Show me the full notebook content and the pipeline JSON configuration
(catalog, target schema, channel: PREVIEW, photon: true).
```

---

## Prompt 03 — Silver Layer: Customers Pipeline

**Goal:** Build the first Silver DLT pipeline — enriched, deduplicated customer table
resolving KYC level from the events log.

```
Create a Spark Declarative Pipeline notebook called "bitso_silver_customers_pipeline"
that builds the silver customer table in bitso_demo.silver.

This pipeline reads from bitso_demo.bronze (NOT from raw). Use live.table()
or dlt.read() to reference the bronze tables as DLT sources.

Transform logic to implement:
1. silver_customers (MATERIALIZED VIEW or streaming table):
   - Deduplicate bronze.customers on customer_id (keep latest by registration_ts)
   - Resolve the true KYC level from bronze.kyc_events:
     join to the most recent "approved" event per customer and take
     kyc_level_after as the authoritative KYC level (name it kyc_level_final).
     Fall back to kyc_level from bronze.customers if no approved event exists.
   - Add was_referred: boolean, true if the customer's customer_id appears in
     bronze.referrals.referred_customer_id
   - Add days_since_registration: datediff(current_date(), registration_date)
   - Add customer_age_years: datediff(current_date(), birth_date) / 365.25
   - Add support_ticket_count and avg_csat_score from bronze.support_tickets
   - Add escalated_ticket_count from bronze.support_tickets where escalated = true
   - Drop internal audit columns (_ingested_at, _source_table, _pipeline_run_id, _bronze_ingested_at)
   - Add _silver_updated_at = current_timestamp()

2. DLT expectations (use expect_or_drop for hard constraints):
   - customer_id IS NOT NULL
   - email IS NOT NULL
   - kyc_level_final BETWEEN 0 AND 3
   - days_since_registration >= 0
   - risk_segment IN ('HIGH', 'MEDIUM', 'LOW')

3. Apply CONSTRAINT statements on the resulting Delta table:
   - CONSTRAINT valid_kyc_level EXPECT (kyc_level_final BETWEEN 0 AND 3)
   - CONSTRAINT valid_country EXPECT (country IN ('MX', 'AR', 'BR'))

4. Table properties to set:
   - delta.enableChangeDataFeed = true (for downstream incremental reads)
   - comment: "Silver customer profiles with resolved KYC level and enriched metrics"

Show the full notebook + pipeline config targeting bitso_demo.silver.
```

---

## Prompt 04 — Silver Layer: Transactions Pipeline

**Goal:** Build a unified transaction ledger from deposits, trades, and withdrawals,
plus a monthly trade summary used to label high-value traders.

```
Create a Spark Declarative Pipeline notebook called "bitso_silver_transactions_pipeline"
that builds two silver tables in bitso_demo.silver, reading from bitso_demo.bronze.

Table 1: silver.transactions (unified event ledger)
- Union bronze.deposits, bronze.trades, bronze.withdrawals into one table.
- Normalise to a common schema:
    transaction_id  string  (deposit_id / trade_id / withdrawal_id cast to string)
    customer_id     int
    event_ts        timestamp  (deposit_ts / trade_ts / withdrawal_ts)
    transaction_type string   ('deposit' / 'trade' / 'withdrawal')
    amount_mxn      double   (use amount_mxn for deposits, volume_mxn for trades,
                              -amount_mxn for withdrawals — negative = outflow)
    currency        string   (currency_received / base_currency / currency_withdrawn)
    trading_pair    string   (NULL for deposits and withdrawals)
    fee_mxn         double   (0 for deposits, fee_mxn for trades and withdrawals)
    payment_method  string   (payment_method / order_type / destination_type)
    status          string   (status for deposits and withdrawals, 'completed' for trades)
    year_month      string   (date_format(event_ts, 'yyyy-MM'))
- Filter: only include records where status IN ('completed') or transaction_type = 'trade'
- DLT expectations (expect_or_drop):
    transaction_id IS NOT NULL
    customer_id IS NOT NULL
    event_ts IS NOT NULL
    amount_mxn IS NOT NULL
- DLT expectations (expect / warning only):
    amount_mxn != 0
- Table property: delta.enableChangeDataFeed = true

Table 2: silver.monthly_trade_summary
(MATERIALIZED VIEW — aggregated, not streaming)
- Source: the silver.transactions table above, filtered to transaction_type = 'trade'
- Group by: customer_id, year_month
- Aggregate:
    monthly_trade_count      = count(transaction_id)
    monthly_volume_mxn       = sum(amount_mxn)
    monthly_fees_mxn         = sum(fee_mxn)
    unique_pairs_traded      = countDistinct(trading_pair)
    avg_trade_size_mxn       = avg(amount_mxn)
- DLT expectation: monthly_trade_count > 0, monthly_volume_mxn > 0
- Table comment: "Monthly trade aggregates per customer — source of truth for HVT labelling"

Important: The data relationships matter here.
- Deposits that are NOT 'completed' should be excluded (failed/reversed deposits
  do not represent real customer liquidity).
- Trades are always completed (no status column in raw.trades).
- Withdrawals with status = 'failed' or 'cancelled' should be excluded.

Show full notebook + pipeline config.
```

---

## Prompt 05 — Silver Layer: Engagement Pipeline

**Goal:** Aggregate behavioural signals (sessions, marketing, push) into one
engagement summary per customer.

```
Create a Spark Declarative Pipeline notebook called "bitso_silver_engagement_pipeline"
that builds silver.engagement_summary in bitso_demo.silver, reading from bitso_demo.bronze.

This is a MATERIALIZED VIEW (batch aggregation, not streaming) — one row per customer_id.

Aggregations to compute:

From bronze.app_sessions:
  total_sessions              count(session_id)
  avg_session_duration_secs   avg(duration_seconds)
  max_session_duration_secs   max(duration_seconds)
  trade_initiated_sessions    sum(cast(trade_initiated as int))
  trade_session_rate          trade_initiated_sessions / total_sessions
  unique_devices              countDistinct(device_type)
  unique_features_used        countDistinct(primary_feature)
  last_session_ts             max(session_start_ts)
  days_since_last_session     datediff(current_date(), last_session_ts)

From bronze.marketing_impressions:
  total_impressions           count(impression_id)
  total_clicks                sum(cast(clicked as int))
  total_conversions           sum(cast(converted as int))
  mktg_ctr                    total_clicks / nullif(total_impressions, 0)
  mktg_cvr                    total_conversions / nullif(total_clicks, 0)
  unique_campaigns_reached    countDistinct(campaign_id)
  unique_mktg_channels        countDistinct(channel)
  total_conversion_value_mxn  sum(conversion_value_mxn)

From bronze.push_notifications:
  total_push_sent             count(notification_id)
  total_push_opened           sum(cast(opened as int))
  total_push_actions          sum(cast(action_taken as int))
  push_open_rate              total_push_opened / nullif(total_push_sent, 0)
  push_action_rate            total_push_actions / nullif(total_push_sent, 0)
  personalised_push_count     sum(cast(is_personalised as int))

Start from the full customer spine (all customer_ids from bitso_demo.silver.customers)
and LEFT JOIN each aggregation so that customers with no activity still get a row
with 0 values (coalesce nulls to 0 for counts, to 0.0 for rates).

DLT expectations (expect / warn only — never drop a customer row here):
  total_sessions >= 0
  push_open_rate BETWEEN 0 AND 1
  mktg_ctr BETWEEN 0 AND 1

Add _silver_updated_at = current_timestamp().
Table property comment: "Behavioural engagement metrics per customer across app, marketing, and push channels."

Show full notebook + pipeline config.
```

---

## Prompt 06 — Gold Layer: Customer 360 Pipeline

**Goal:** Build the business-ready customer view that powers Genie AI/BI dashboards.

```
Create a Spark Declarative Pipeline notebook called "bitso_gold_customer360_pipeline"
that builds gold.customer_360 in bitso_demo.gold, reading from bitso_demo.silver.

This is a MATERIALIZED VIEW — one row per customer, joining all silver tables.

Joins to perform (all LEFT JOIN from silver.customers as the spine):
  1. silver.monthly_trade_summary  → aggregate to one row per customer first:
       avg_monthly_volume_mxn      = avg(monthly_volume_mxn)
       avg_monthly_trade_count     = avg(monthly_trade_count)
       active_trading_months       = count(distinct year_month)
       peak_monthly_volume_mxn     = max(monthly_volume_mxn)
  2. silver.transactions (trades only) → aggregate per customer:
       lifetime_trade_count        = count(transaction_id) where type='trade'
       lifetime_volume_mxn         = sum(amount_mxn) where type='trade'
       avg_trade_volume_mxn        = avg(amount_mxn) where type='trade'
       lifetime_fees_paid_mxn      = sum(fee_mxn) where type='trade'
       unique_pairs_traded         = countDistinct(trading_pair) where type='trade'
       last_trade_ts               = max(event_ts) where type='trade'
       first_trade_ts              = min(event_ts) where type='trade'
       buy_volume_mxn              = sum where type='trade' AND payment_method IN buy orders
       days_since_last_trade       = datediff(current_date(), last_trade_ts)
  3. silver.transactions (deposits only) → aggregate per customer:
       total_deposits              = count
       total_deposited_mxn         = sum(amount_mxn)
       avg_deposit_mxn             = avg(amount_mxn)
       last_deposit_ts             = max(event_ts)
       days_since_last_deposit     = datediff(current_date(), last_deposit_ts)
  4. silver.engagement_summary     → all columns (flat join)
  5. bronze.referrals (referrer side) → referrals_given = count(referral_id)

Computed columns to add:
  is_high_value_trader = (avg_monthly_volume_mxn >= 200000 AND avg_monthly_trade_count >= 10)
  deposit_to_volume_ratio = lifetime_volume_mxn / nullif(total_deposited_mxn, 0)
  customer_value_tier:
    CASE WHEN avg_monthly_volume_mxn >= 500000 THEN 'PLATINUM'
         WHEN avg_monthly_volume_mxn >= 200000 THEN 'GOLD'
         WHEN avg_monthly_volume_mxn >= 50000  THEN 'SILVER'
         WHEN lifetime_trade_count > 0         THEN 'BRONZE'
         ELSE 'INACTIVE' END

DLT expectations (expect_or_drop):
  customer_id IS NOT NULL

DLT expectations (expect / warn):
  lifetime_volume_mxn >= 0
  avg_monthly_volume_mxn >= 0

Apply Delta table CONSTRAINT:
  CONSTRAINT valid_value_tier EXPECT (customer_value_tier IN ('PLATINUM','GOLD','SILVER','BRONZE','INACTIVE'))

Table properties:
  delta.enableChangeDataFeed = true
  comment: "Gold customer 360 view — single source of truth for Genie AI/BI dashboards and business reporting"

IMPORTANT: Coalesce all numeric aggregations to 0 so inactive customers still get a row.
Show full notebook + pipeline config.
```

---

## Prompt 07 — Gold Layer: ML Features Pipeline

**Goal:** Build the ML-ready, fully encoded feature table that feeds the propensity model.

```
Create a Spark Declarative Pipeline notebook called "bitso_gold_mlfeatures_pipeline"
that builds two tables in bitso_demo.gold, reading from bitso_demo.gold.customer_360
and bitso_demo.silver.customers.

Table 1: gold.ml_features
A flat, numeric, ML-ready feature table — one row per customer_id.

Select and rename these columns from gold.customer_360 (prefix all features with "f_"):
  f_tenure_days               days_since_registration
  f_age_years                 customer_age_years
  f_kyc_level                 kyc_level_final (int)
  f_was_referred              cast(was_referred as int)
  f_lifetime_trade_count      lifetime_trade_count
  f_lifetime_volume_mxn       lifetime_volume_mxn
  f_avg_trade_volume_mxn      avg_trade_volume_mxn
  f_unique_pairs_traded       unique_pairs_traded
  f_active_trading_months     active_trading_months
  f_days_since_last_trade     days_since_last_trade (coalesce 999)
  f_deposit_to_vol_ratio      deposit_to_volume_ratio (coalesce 0)
  f_total_deposits            total_deposits
  f_total_deposited_mxn       total_deposited_mxn
  f_avg_deposit_mxn           avg_deposit_mxn
  f_payment_methods_used      payment_methods_used (coalesce 0)
  f_total_sessions            total_sessions
  f_avg_session_dur_secs      avg_session_duration_secs
  f_unique_features_used      unique_features_used
  f_trade_session_rate        trade_session_rate (coalesce 0)
  f_total_impressions         total_impressions
  f_mktg_ctr                  mktg_ctr (coalesce 0)
  f_unique_campaigns          unique_campaigns_reached
  f_push_open_rate            push_open_rate (coalesce 0)
  f_push_action_rate          push_action_rate (coalesce 0)
  f_support_tickets           total_tickets (coalesce 0)
  f_avg_csat                  avg_csat_score (coalesce 3.0)
  f_referrals_given           referrals_given (coalesce 0)

Encode these categoricals using integer index mapping (use a lookup CASE WHEN, not MLlib here
since this is a DLT pipeline, not an ML notebook):
  f_country_idx:  MX=0, AR=1, BR=2
  f_gender_idx:   M=0, F=1
  f_risk_seg_idx: LOW=0, MEDIUM=1, HIGH=2   (ordinal — preserves magnitude)
  f_referral_source_idx: (map the 8 referral sources to 0–7 alphabetically)
  f_pref_currency_idx:   (map the currencies to 0–N alphabetically)

Target label:
  label = cast(is_high_value_trader as int)

Table 2: gold.ml_feature_metadata
A metadata table documenting every feature column:
  feature_name   string
  description    string
  data_type      string
  source_table   string
  created_at     timestamp

Populate it with one row per f_ column describing what it represents and where it came from.

DLT expectations on gold.ml_features:
  customer_id IS NOT NULL
  label IN (0, 1)
  f_kyc_level BETWEEN 0 AND 3
  f_tenure_days >= 0

Table comment: "ML feature store for the Bitso HVT propensity model. Label=1 means high-value trader."
Show full notebook + pipeline config.
```

---

## Prompt 08 — Data Quality: Delta Constraints & Tags

**Goal:** Add key Delta constraints and PII tags to the most important tables.

```
Create a short notebook called "bitso_governance" in bitso_demo.

Add these Delta constraints (ALTER TABLE ... ADD CONSTRAINT):
  bitso_demo.bronze.customers  → valid_country: country IN ('MX','AR','BR')
  bitso_demo.bronze.trades     → positive_volume: volume_mxn > 0
  bitso_demo.silver.customers  → valid_risk_segment: risk_segment IN ('HIGH','MEDIUM','LOW')
  bitso_demo.gold.customer_360 → valid_tier: customer_value_tier IN ('PLATINUM','GOLD','SILVER','BRONZE','INACTIVE')

Add table tags (ALTER TABLE ... SET TAGS):
  All bronze tables → layer='bronze', project='bitso_hvt'
  All silver tables → layer='silver', project='bitso_hvt'
  All gold tables   → layer='gold',   project='bitso_hvt'
  bronze.customers and silver.customers → pii='true'

Print "Done" when finished.
```

---

## Prompt 09 — Orchestration: Medallion Job

**Goal:** Create a Databricks Job that runs all 5 DLT pipelines in the correct dependency order.

```
Create a Databricks Job called "bitso_medallion_orchestrator" that runs all
5 Spark Declarative Pipelines in the correct dependency order.

The pipelines and their execution order are:
  Step 1 — bitso_bronze_pipeline          (no dependencies)
  Step 2 — bitso_silver_customers_pipeline (depends on Step 1)
  Step 3 — bitso_silver_transactions_pipeline (depends on Step 1)
  Step 4 — bitso_silver_engagement_pipeline   (depends on Step 1)
  Step 5 — bitso_gold_customer360_pipeline    (depends on Steps 2, 3, 4)
  Step 6 — bitso_gold_mlfeatures_pipeline     (depends on Step 5)
  Step 7 — bitso_apply_constraints_and_tags   (depends on Step 6, run as notebook task)

Job configuration requirements:
  - Use Serverless compute for all pipeline tasks
  - Set max_concurrent_runs = 1
  - Configure email notifications on failure to the workspace owner
  - Add a schedule: daily at 02:00 UTC (cron: 0 0 2 * * ?)
  - Set job timeout to 4 hours
  - Enable health check: if any task fails, mark the entire run as failed

For Steps 2, 3, and 4 (silver pipelines) — these can run IN PARALLEL
after Step 1 completes. Configure them as parallel tasks with the same
dependency on the bronze pipeline task.

Step 5 must wait for ALL three silver tasks to complete before starting.

Also create a second lightweight job called "bitso_data_generation_job"
that runs the 99_run_all notebook (at /Shared/bitso_demo/01_data_generation/99_run_all)
as a prerequisite — this is the job to run when refreshing demo data.
This job should NOT be scheduled (manual trigger only).

Output: the full job JSON configuration that can be submitted via
databricks jobs create --json, AND a notebook that submits it via
the Databricks Jobs REST API (POST /api/2.1/jobs/create).
```

---

## Prompt 10 — Exploratory Analysis: Customer & Trading

**Goal:** Quick EDA notebook with 4 key charts from the gold layer.

```
Create a Python notebook called "eda_01_customer_trading" using matplotlib.
Read from bitso_demo.gold.customer_360 (convert to pandas).

Make 4 charts in a 2×2 grid (fig, axes = plt.subplots(2,2, figsize=(14,10))):
  1. Bar: customer count by customer_value_tier
  2. Scatter: avg_monthly_volume_mxn vs avg_monthly_trade_count,
     coloured by is_high_value_trader. Add dashed lines at x=10 and y=200000.
  3. Bar: avg lifetime_volume_mxn by country
  4. Bar: avg total_sessions by customer_value_tier

Title each chart. Call plt.tight_layout() and display.
```

---

## Prompt 11 — Exploratory Analysis: Marketing & Engagement

**Goal:** Quick EDA notebook with 4 charts covering marketing and app behaviour.

```
Create a Python notebook called "eda_02_marketing_engagement" using matplotlib.

Chart 1 — Marketing CTR by channel:
  Read bitso_demo.bronze.marketing_impressions (use spark, convert to pandas).
  Group by channel: count impressions, sum clicks. Compute ctr = clicks/impressions.
  Bar chart of ctr by channel, sorted descending.

Chart 2 — Push open rate by notification type:
  Read bitso_demo.bronze.push_notifications.
  Group by notification_type: compute open_rate = sum(opened)/count.
  Horizontal bar chart sorted descending.

Chart 3 — Avg session duration by primary feature:
  Read bitso_demo.bronze.app_sessions (sample 50000 rows).
  Group by primary_feature: avg duration_seconds.
  Bar chart sorted descending.

Chart 4 — Support tickets by category:
  Read bitso_demo.bronze.support_tickets.
  Count tickets per category. Bar chart sorted descending.

Show all 4 in a 2×2 grid. Title each. plt.tight_layout() and display.
```

---

## Prompt 12 — Genie Space: Business User Analytics

**Goal:** Create a Databricks AI/BI Genie space backed by the gold layer
so business users can ask natural language questions about customer analytics.

```
Create a Databricks AI/BI Genie space called "Bitso Customer Intelligence" backed
by the bitso_demo.gold tables. Use the Databricks REST API or the workspace UI
instructions to set it up.

Tables to include in the Genie space:
  bitso_demo.gold.customer_360         (primary table — one row per customer)
  bitso_demo.gold.propensity_scores    (one row per customer — HVT propensity score)
  bitso_demo.silver.monthly_trade_summary  (monthly grain — for trend questions)

Genie space configuration:
  title: "Bitso Customer Intelligence"
  description: "Ask questions about Bitso customer behaviour, trading patterns,
                marketing performance, and HVT propensity scores."
  warehouse: use the first available SQL warehouse in the workspace

Write these curated instructions for the Genie space (add as "Instructions" in the UI
or via API as trusted_assets or description context):

---
GENIE INSTRUCTIONS:
This space covers Bitso customer analytics for the Mexico, Argentina, and Brazil markets.

Key definitions:
- High-Value Trader (HVT): a customer with avg monthly trade volume ≥ 200,000 MXN AND ≥ 10 trades per month. Field: is_high_value_trader = true.
- Customer tiers: PLATINUM (≥500K MXN/mo), GOLD (≥200K), SILVER (≥50K), BRONZE (any trade), INACTIVE (no trades).
- Propensity score: a model score from 0 to 1 predicting the probability a customer becomes an HVT. Field: propensity_score in gold.propensity_scores.
- Propensity decile: 10 = highest propensity, 1 = lowest.

Key relationships:
- customer_360 and propensity_scores share customer_id (1:1 join).
- monthly_trade_summary links to customer_360 via customer_id (many:1).

Always express monetary amounts in MXN (Mexican Pesos).
When asked about "top customers", sort by avg_monthly_volume_mxn DESC.
When asked about "at-risk" customers, look for customers with days_since_last_trade > 30.
---

Also write 10 example questions a business user might ask, such as:
  - "How many customers are high-value traders by country?"
  - "What is the average propensity score for customers who have not yet completed KYC level 2?"
  - "Show me the monthly trading volume trend for GOLD tier customers in Mexico"
  - "Which acquisition channel produces the most high-value traders?"
  - "List the top 20 customers by propensity score who are NOT yet high-value traders"
  (add 5 more relevant examples)

Provide:
  a) The curl / REST API call to create the Genie space programmatically
  b) Step-by-step UI instructions as a fallback
  c) A notebook that validates the Genie space is returning correct answers
     by running the 10 example questions via the Genie REST API
```

---

## Prompt 13 — ML Model: Propensity Classifier

**Goal:** Train a propensity model, log it to MLflow, and save scores.

```
Create a Python notebook called "ml_01_propensity_model".

1. Load bitso_demo.gold.ml_features as pandas. Features = all columns starting with "f_". Target = label.

2. Train/test split: 80/20 stratified on label.

3. Train a RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced').
   Wrap in sklearn Pipeline with SimpleImputer(strategy='median') first.

4. Log to MLflow (use mlflow.sklearn.autolog()):
   - Fit the model inside a mlflow.start_run() block named "hvt_propensity_v1"
   - Log test AUC-ROC manually: mlflow.log_metric("test_auc_roc", roc_auc_score(...))
   - Register model as: bitso_demo.gold.hvt_propensity_model

5. Score all 10K customers. Save to bitso_demo.gold.propensity_scores with columns:
   customer_id, propensity_score, propensity_decile (pd.qcut into 10 bins), scored_at.

6. Print test AUC-ROC and top 10 feature importances.
```

---

## Prompt 14 — ML Production: Serving Endpoint

**Goal:** Deploy the registered model to a real-time serving endpoint and test it.

```
Create a Python notebook called "ml_02_model_serving".

1. Use the Databricks REST API (requests + dbutils.notebook.entry_point token) to:
   - Create a Model Serving endpoint named "bitso-hvt-propensity"
   - Serve model: bitso_demo.gold.hvt_propensity_model, latest version
   - Compute size: Small, scale_to_zero_enabled: true

2. Poll the endpoint status every 20s until state = "READY" (max 10 minutes).

3. Once ready, send a test request with 3 sample rows from bitso_demo.gold.ml_features
   and print the returned propensity scores.

4. Print the endpoint URL.
```

---

## Execution Checklist

| # | Prompt | Output | Prerequisite |
|---|--------|--------|-------------|
| 01 | Raw data discovery | Data quality report | Raw tables exist |
| 02 | Bronze DLT pipeline | DLT notebook + config | Prompt 01 |
| 03 | Silver customers pipeline | DLT notebook + config | Prompt 02 |
| 04 | Silver transactions pipeline | DLT notebook + config | Prompt 02 |
| 05 | Silver engagement pipeline | DLT notebook + config | Prompt 02 |
| 06 | Gold customer 360 pipeline | DLT notebook + config | Prompts 03–05 |
| 07 | Gold ML features pipeline | DLT notebook + config | Prompt 06 |
| 08 | 4 constraints + layer tags | ~10 ALTER TABLE statements | Prompt 07 |
| 09 | Orchestration job | Job JSON + API notebook | Prompts 02–08 |
| 10 | EDA: 4 customer/trading charts | Single 2×2 matplotlib grid | Prompt 06 |
| 11 | EDA: 4 marketing/engagement charts | Single 2×2 matplotlib grid | Prompt 06 |
| 12 | Genie space | Space config + sample questions | Prompt 06 |
| 13 | Propensity model + scores table | RF model + MLflow + propensity_scores | Prompt 07 |
| 14 | Model serving endpoint | Endpoint + smoke test | Prompt 13 |

---

## Tips for Working with Genie Code

- **Approve intermediate outputs** — after each prompt, review the generated code before running.
- **Paste error messages back** — if a pipeline fails, paste the error into Genie Code and ask it to fix it.
- **Reference earlier outputs** — e.g. "use the pipeline ID from Prompt 02 in the job config".
- **Ask for explanations** — "explain what the expect_or_drop expectation does on the trades table".
- **Iterate on visuals** — "make the scatter plot in Section 2 larger and add a trend line per tier".
