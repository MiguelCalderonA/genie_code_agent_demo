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

## Prompt 08 — Data Quality: Delta Constraints & Table Tags

**Goal:** Enforce data quality at the storage layer with Delta table constraints and
Unity Catalog tags for governance.

```
Generate a notebook called "bitso_apply_constraints_and_tags" that applies Delta table
constraints, column tags, and table properties across all medallion layers.

SECTION 1 — Delta table CONSTRAINTS (ALTER TABLE ... ADD CONSTRAINT)

bronze.customers:
  valid_kyc_range:      kyc_level BETWEEN 0 AND 3
  valid_country:        country IN ('MX', 'AR', 'BR')

bronze.trades:
  positive_volume:      volume_mxn > 0
  valid_side:           side IN ('buy', 'sell')
  positive_fee:         fee_mxn >= 0

bronze.deposits:
  positive_amount:      amount_mxn > 0
  valid_status:         status IN ('completed','pending','failed','reversed')

bronze.kyc_events:
  valid_event_type:     event_type IN ('submitted','approved','rejected','upgrade_requested','downgraded')

silver.customers:
  valid_kyc_final:      kyc_level_final BETWEEN 0 AND 3
  valid_risk_segment:   risk_segment IN ('HIGH','MEDIUM','LOW')
  non_negative_tenure:  days_since_registration >= 0

gold.customer_360:
  valid_tier:           customer_value_tier IN ('PLATINUM','GOLD','SILVER','BRONZE','INACTIVE')

gold.ml_features:
  valid_label:          label IN (0, 1)

SECTION 2 — Table and column tags (ALTER TABLE ... SET TAGS, ALTER TABLE ... ALTER COLUMN ... SET TAGS)

Apply table-level tags:
  layer = 'bronze' / 'silver' / 'gold'
  domain = 'marketing_analytics'
  project = 'bitso_hvt_propensity'
  pii_contains = 'true' (for customers tables only)

Apply column-level tags on silver.customers and gold.customer_360:
  email column:        pii='true', pii_type='email'
  phone column:        pii='true', pii_type='phone'
  first_name column:   pii='true', pii_type='name'
  last_name column:    pii='true', pii_type='name'
  birth_date column:   pii='true', pii_type='date_of_birth'

SECTION 3 — Table comments (ALTER TABLE ... SET TBLPROPERTIES or COMMENT ON)
Set meaningful table comments on all 9 medallion tables.

SECTION 4 — Row count validation
After applying constraints, run a check:
  - For each table, assert that current row count equals row count before
    (constraints should not drop any rows since data was generated to be valid).
  - Print a pass/fail report.

Show the complete notebook.
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

## Prompt 10 — Exploratory Analysis: Customer Segmentation & Trading Behaviour

**Goal:** Generate a rich EDA notebook with Python visualisations exploring
customer segments and trading patterns using the gold layer.

```
Create a Databricks notebook called "eda_01_customer_and_trading_analysis" in Python
that performs exploratory data analysis on the gold layer using matplotlib and seaborn.
Read all data from bitso_demo.gold.customer_360.

The notebook should have clearly labelled sections with markdown headings.

SECTION 1 — Dataset Overview
- Total customers, HVT count and percentage, customer_value_tier distribution (pie chart)
- Breakdown by country and risk_segment (grouped bar chart)
- Heatmap: count of customers by country × customer_value_tier

SECTION 2 — Trading Behaviour
- Distribution of lifetime_volume_mxn (log scale histogram, coloured by is_high_value_trader)
- Distribution of lifetime_trade_count (log scale, coloured by tier)
- Scatter plot: avg_monthly_volume_mxn vs avg_monthly_trade_count, coloured by tier,
  with a horizontal line at 200K MXN and vertical line at 10 trades (the HVT threshold)
- Box plots: unique_pairs_traded by customer_value_tier
- Line chart: average monthly trading volume over time by tier
  (use bitso_demo.silver.monthly_trade_summary for this chart)

SECTION 3 — Deposit & Liquidity Patterns
- Bar chart: avg_deposit_mxn by customer_value_tier
- Distribution of total_deposited_mxn for HVT vs non-HVT (overlapping KDE plot)
- Bar chart: deposit_to_volume_ratio by tier (shows leverage effect)
- Scatter: total_deposited_mxn vs lifetime_volume_mxn coloured by tier

SECTION 4 — KYC & Compliance
- Bar chart: KYC level distribution by risk_segment
- Stacked bar: HVT rate by kyc_level_final (shows correlation between KYC and HVT)

SECTION 5 — Engagement Signals
- Scatter: total_sessions vs lifetime_trade_count coloured by tier
- Bar: avg push_open_rate by customer_value_tier
- Bar: avg mktg_ctr by customer_value_tier
- Correlation heatmap of all numeric features vs the label (is_high_value_trader)

Use a consistent Bitso-inspired colour palette:
  HVT / PLATINUM = #1DB954 (green)
  GOLD = #F5A623
  SILVER = #9B9B9B
  BRONZE = #8B572A
  INACTIVE = #D0021B

Each chart must have a title, axis labels, and a one-sentence business insight
in the markdown cell following it explaining what the chart shows.
```

---

## Prompt 11 — Exploratory Analysis: Marketing Attribution & Engagement Funnel

**Goal:** Second EDA notebook focused on marketing channels, campaign performance,
and engagement funnel analysis.

```
Create a Databricks notebook called "eda_02_marketing_and_engagement_analysis" in Python
using matplotlib and seaborn. Read from bitso_demo.bronze and bitso_demo.silver tables.

SECTION 1 — Marketing Campaign Performance
Read from bitso_demo.bronze.marketing_impressions.
- Bar chart: impressions, clicks, and conversions per channel (grouped bars)
- Funnel chart: impressions → clicks → conversions for the top 5 campaigns by conversion_value_mxn
- Scatter: CTR vs CVR by campaign, bubble size = total conversion_value_mxn, coloured by campaign_type
- Heatmap: CTR by channel × campaign_type (pivot table)

SECTION 2 — Push Notification Effectiveness
Read from bitso_demo.bronze.push_notifications.
- Bar: open_rate by notification_type (sort descending)
- Bar: action_rate by notification_type
- Scatter: push_open_rate vs push_action_rate by customer (sample 2000 rows),
  coloured by risk_segment from bronze.customers (join needed)
- Line chart: push volume over time (monthly, by notification_type)

SECTION 3 — App Engagement Deep Dive
Read from bitso_demo.bronze.app_sessions.
- Bar: average session duration by primary_feature (sort descending)
- Bar: trade_initiation_rate by device_type
- Histogram: session duration distribution for trade_initiated=True vs False
- Heatmap: session count by day_of_week × hour_of_day (extract from session_start_ts)
  (use only a sample of 50K rows for performance)

SECTION 4 — Referral Network Analysis
Read from bitso_demo.bronze.referrals joined with bronze.customers.
- Bar: referrals_given distribution (how many customers gave 0, 1, 2, 3+ referrals)
- Bar: reward_status breakdown
- Bar: average referrals_given by risk_segment of the referrer
- Bar chart: average propensity_score of referred vs non-referred customers
  (join to bitso_demo.gold.propensity_scores if it exists, otherwise use is_high_value_trader)

SECTION 5 — Customer Support Health
Read from bitso_demo.bronze.support_tickets.
- Bar: ticket count by category (sorted)
- Box plot: resolution_hours by priority
- Bar: average CSAT score by category
- Scatter: total_tickets vs avg_csat_score per customer (sample 2000),
  coloured by risk_segment

Add a final markdown cell with 5 bullet-point business recommendations
derived from the patterns observed in the data.
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

**Goal:** Build and log a production-grade propensity model using the gold ML feature table.

```
Create a Databricks notebook called "ml_01_propensity_model_training" that trains a
Gradient Boosted Tree classifier to predict HVT propensity using MLflow for tracking.

Read features from: bitso_demo.gold.ml_features
Target column: label (1 = HVT, 0 = non-HVT)
Feature columns: all columns starting with "f_"

STEP 1 — Exploratory checks before training
- Print class balance (label=0 vs label=1 counts and percentages)
- If the positive class is < 15%, apply class_weight='balanced' in the model
- Print feature correlation with label (top 10 most correlated features)

STEP 2 — Train / validation / test split
- 70% train, 15% validation, 15% test (stratified on label)
- Print counts per split and positive rate

STEP 3 — Model training with MLflow
Use MLflow autologging + manual metric logging.
Train a sklearn GradientBoostingClassifier with these hyperparameters:
  n_estimators=200, learning_rate=0.08, max_depth=5,
  min_samples_leaf=20, subsample=0.8, random_state=42
Wrap in an sklearn Pipeline with SimpleImputer(strategy='median')
and StandardScaler().

Log to MLflow:
  - All hyperparameters
  - Validation AUC-ROC, AUC-PR, F1, precision, recall at threshold 0.5
  - Cross-validated AUC (5-fold on train set) mean and std
  - Feature importance bar chart as an artifact
  - Confusion matrix as an artifact
  - Classification report as an artifact
  - ROC curve plot as an artifact
  - Precision-Recall curve plot as an artifact
  - The trained model with signature inferred from X_test, y_test

Register the model in Unity Catalog:
  Model name: bitso_demo.gold.hvt_propensity_model
  Alias: challenger (not champion yet — that happens in Prompt 14)

STEP 4 — Score all customers and save predictions
- Score the FULL dataset (all 10K customers, not just test)
- Save to bitso_demo.gold.propensity_scores:
    customer_id, propensity_score, propensity_decile (1–10),
    is_predicted_hvt (score >= 0.50), actual_label, model_version,
    scored_at (current timestamp), run_id (MLflow run ID)
- Overwrite the table (or merge on customer_id if it exists)

STEP 5 — Model evaluation report
Print a final report:
- Test set AUC-ROC and AUC-PR
- Performance by propensity decile: actual HVT rate, predicted HVT rate, lift vs baseline
- Top 15 feature importances with business interpretation for each
- Business impact estimate: if we target the top 2 deciles, how many HVTs do we capture?
  What % of all HVTs is that?
```

---

## Prompt 14 — ML Production: Model Promotion & Batch Scoring Job

**Goal:** Promote the model to production, create a real-time serving endpoint,
and add a weekly batch scoring task to the orchestration job.

```
Create two notebooks and update the orchestration job to operationalise the HVT propensity model.

NOTEBOOK 1: "ml_02_model_promotion"
Purpose: compare challenger vs champion (if one exists) and promote the better model.

Steps:
1. Load the challenger model (alias: challenger) from bitso_demo.gold.hvt_propensity_model
2. If a champion model exists (alias: champion), load it too
3. Run both models on a held-out test set (recreate the same 15% test split using random_state=42)
4. Compare: AUC-ROC, AUC-PR, F1
5. Decision rule:
   - If challenger AUC-ROC > champion AUC-ROC + 0.005: promote challenger to champion
   - Otherwise: keep current champion, log a message, do not update alias
6. If promoted: set alias "champion" on the new version, remove it from the old
7. Log the comparison results as an MLflow run in the same experiment
8. Print a clear PROMOTED / RETAINED decision with metrics

NOTEBOOK 2: "ml_03_model_serving_endpoint"
Purpose: create a Databricks Model Serving endpoint for real-time propensity lookup.

Steps:
1. Check if endpoint "bitso-hvt-propensity" already exists via REST API
2. If not, create it:
   - Model: bitso_demo.gold.hvt_propensity_model (champion alias)
   - Scale to zero: enabled (cost saving for demo)
   - Compute size: Small
3. Wait for endpoint to be READY (poll every 30s, max 10 minutes)
4. Run a smoke test: score 5 sample customers and print the returned propensity scores
5. Print the endpoint URL for use in downstream applications

UPDATE THE ORCHESTRATION JOB:
Add two new tasks at the end of the existing "bitso_medallion_orchestrator" job:

  Step 8 — ml_01_propensity_model_training (depends on Step 6 gold.ml_features)
            Run as notebook task on serverless compute
  Step 9 — ml_02_model_promotion (depends on Step 8)
            Run as notebook task on serverless compute

Also create a standalone job "bitso_weekly_scoring" that:
  1. Runs ml_01_propensity_model_training (retrain on latest data)
  2. Runs ml_02_model_promotion
  3. Scheduled: every Monday at 04:00 UTC
  4. On success: send a Databricks notification to a webhook (placeholder URL)

Output: full notebook content for both notebooks + the updated job JSON config.
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
| 08 | Constraints & tags | Governance notebook | Prompt 07 |
| 09 | Orchestration job | Job JSON + API notebook | Prompts 02–08 |
| 10 | EDA: customers & trading | EDA notebook | Prompt 06 |
| 11 | EDA: marketing & engagement | EDA notebook | Prompt 06 |
| 12 | Genie space | Space config + test notebook | Prompt 06 |
| 13 | Propensity model training | ML notebook + MLflow run | Prompt 07 |
| 14 | Model promotion & serving | 2 notebooks + job update | Prompt 13 |

---

## Tips for Working with Genie Code

- **Approve intermediate outputs** — after each prompt, review the generated code before running.
- **Paste error messages back** — if a pipeline fails, paste the error into Genie Code and ask it to fix it.
- **Reference earlier outputs** — e.g. "use the pipeline ID from Prompt 02 in the job config".
- **Ask for explanations** — "explain what the expect_or_drop expectation does on the trades table".
- **Iterate on visuals** — "make the scatter plot in Section 2 larger and add a trend line per tier".
