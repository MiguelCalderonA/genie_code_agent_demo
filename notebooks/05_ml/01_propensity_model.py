# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Propensity Model — High-Value Trader Score
# MAGIC
# MAGIC Trains an **XGBoost classifier** (via MLlib's GBTClassifier as fallback, or xgboost4j)
# MAGIC to predict the probability that a Bitso customer becomes a high-volume trader.
# MAGIC
# MAGIC **Pipeline**
# MAGIC 1. Load features from `gold.ml_features`
# MAGIC 2. Train / test split (80/20, stratified)
# MAGIC 3. Train XGBoost with cross-validation
# MAGIC 4. Evaluate (AUC-ROC, precision, recall, F1)
# MAGIC 5. Score all customers → `gold.propensity_scores`
# MAGIC 6. Log model to MLflow

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import (roc_auc_score, classification_report,
                              confusion_matrix, average_precision_score)
from sklearn.pipeline import Pipeline as SKPipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
import matplotlib.pyplot as plt

CATALOG       = "bitso_demo"
EXPERIMENT    = "/Shared/bitso_propensity_model"
MODEL_NAME    = "bitso_hvt_propensity"
RANDOM_STATE  = 42

mlflow.set_experiment(EXPERIMENT)

# COMMAND ----------
# MAGIC %md ## Load features

# COMMAND ----------

df = spark.table(f"{CATALOG}.gold.ml_features").toPandas()

FEATURE_COLS = [c for c in df.columns if c.startswith("f_")]
TARGET       = "label"

X = df[FEATURE_COLS].fillna(0)
y = df[TARGET]

print(f"Dataset: {len(df):,} rows  |  Features: {len(FEATURE_COLS)}  |  Positive rate: {y.mean()*100:.1f}%")

# COMMAND ----------
# MAGIC %md ## Train / test split

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, random_state=RANDOM_STATE, stratify=y
)
print(f"Train: {len(X_train):,}  |  Test: {len(X_test):,}")

# COMMAND ----------
# MAGIC %md ## Train model with MLflow tracking

# COMMAND ----------

with mlflow.start_run(run_name="GBT_propensity_v1") as run:

    model = SKPipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
        ("clf",     GradientBoostingClassifier(
            n_estimators=200,
            learning_rate=0.08,
            max_depth=5,
            min_samples_leaf=20,
            subsample=0.8,
            random_state=RANDOM_STATE,
            verbose=0,
        )),
    ])

    # Cross-validation AUC
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)
    cv_auc = cross_val_score(model, X_train, y_train, cv=cv, scoring="roc_auc", n_jobs=-1)

    model.fit(X_train, y_train)

    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)

    auc_roc = roc_auc_score(y_test, y_prob)
    auc_pr  = average_precision_score(y_test, y_prob)
    report  = classification_report(y_test, y_pred, output_dict=True)

    # Log params + metrics
    mlflow.log_params({
        "n_estimators":   200,
        "learning_rate":  0.08,
        "max_depth":      5,
        "subsample":      0.8,
        "cv_folds":       5,
    })
    mlflow.log_metrics({
        "cv_auc_mean":  cv_auc.mean(),
        "cv_auc_std":   cv_auc.std(),
        "test_auc_roc": auc_roc,
        "test_auc_pr":  auc_pr,
        "precision_hvt":report["1"]["precision"],
        "recall_hvt":   report["1"]["recall"],
        "f1_hvt":       report["1"]["f1-score"],
    })

    # Feature importance plot
    clf         = model.named_steps["clf"]
    importances = clf.feature_importances_
    feat_imp    = pd.Series(importances, index=FEATURE_COLS).sort_values(ascending=False)

    fig, ax = plt.subplots(figsize=(10, 8))
    feat_imp.head(20).plot.barh(ax=ax, color="#e85d04")
    ax.invert_yaxis()
    ax.set_title("Top 20 Feature Importances — HVT Propensity Model")
    ax.set_xlabel("Importance")
    fig.tight_layout()
    mlflow.log_figure(fig, "feature_importance.png")
    plt.show()

    mlflow.sklearn.log_model(model, "model", registered_model_name=MODEL_NAME)

    print(f"Run ID:       {run.info.run_id}")
    print(f"CV AUC:       {cv_auc.mean():.4f} ± {cv_auc.std():.4f}")
    print(f"Test AUC-ROC: {auc_roc:.4f}")
    print(f"Test AUC-PR:  {auc_pr:.4f}")
    print(f"\n{classification_report(y_test, y_pred, target_names=['Regular','High-Value'])}")

# COMMAND ----------
# MAGIC %md ## Score all customers → gold.propensity_scores

# COMMAND ----------

X_all = df[FEATURE_COLS].fillna(0)
propensity_scores = df[["customer_id"]].copy()
propensity_scores["propensity_score"]    = model.predict_proba(X_all)[:, 1]
propensity_scores["propensity_decile"]   = pd.qcut(
    propensity_scores["propensity_score"], q=10, labels=False, duplicates="drop"
) + 1
propensity_scores["is_predicted_hvt"]    = (propensity_scores["propensity_score"] >= 0.50).astype(bool)
propensity_scores["model_version"]       = MODEL_NAME
propensity_scores["scored_at"]           = pd.Timestamp.utcnow()
propensity_scores["actual_label"]        = y.values

scores_spark = spark.createDataFrame(propensity_scores)
(scores_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.propensity_scores"))

print(f"  {CATALOG}.gold.propensity_scores — {scores_spark.count():,} rows")

# COMMAND ----------
# MAGIC %md ## Score distribution

# COMMAND ----------

display(spark.sql(f"""
    SELECT propensity_decile,
           COUNT(*) n_customers,
           ROUND(AVG(propensity_score)*100, 1) avg_score_pct,
           SUM(CAST(actual_label AS INT)) actual_hvt,
           SUM(CAST(is_predicted_hvt AS INT)) predicted_hvt
    FROM {CATALOG}.gold.propensity_scores
    GROUP BY 1 ORDER BY 1 DESC
"""))
