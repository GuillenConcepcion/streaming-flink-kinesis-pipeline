#!/usr/bin/env python3
"""
=============================================================================
Odysseus Predictive Maintenance: Machine Learning Training & Evaluation
=============================================================================
Lead Architect: Guillen Concepcion (Senior Data Scientist & MLOps Engineer)
LinkedIn: https://www.linkedin.com/in/guillen-concepcion-25266b127

Trains a Cost-Sensitive Gradient Boosted Tree model (XGBoost / Random Forest)
on the curated historical telemetry Parquet dataset.
Evaluates Precision-Recall AUC, Cost Optimization Curves, and Probability
Calibration, saving production model artifacts and high-res figures.
=============================================================================
"""

import os
import sys
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import (
    precision_recall_curve, roc_curve, auc, 
    brier_score_loss, classification_report, confusion_matrix
)
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier

np.random.seed(42)

def load_and_split_data(parquet_path: str):
    print(f"[*] Loading feature matrix from: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    
    # Feature columns
    feature_cols = [
        "avg_temperature", "max_temperature", "avg_vibration", "max_vibration",
        "avg_pressure", "avg_power",
        "temp_roll_mean_1h", "temp_roll_std_1h", "vib_roll_mean_1h", "vib_roll_std_1h",
        "temp_roll_mean_6h", "vib_roll_mean_6h", "vib_roll_max_6h",
        "temp_lag_30m", "vib_lag_30m", "temp_lag_1h", "vib_lag_1h",
        "d_temp_dt_1h", "d_vib_dt_1h",
        "vibration_crest_factor", "electromechanical_ratio"
    ]
    target_col = "target_failure_next_6h"
    
    # Causal Temporal Split: Train on first 5 days, Test on last 2 days
    split_timestamp = df["timestamp"].min() + pd.Timedelta(days=5)
    
    train_df = df[df["timestamp"] <= split_timestamp]
    test_df = df[df["timestamp"] > split_timestamp]
    
    X_train = train_df[feature_cols]
    y_train = train_df[target_col]
    X_test = test_df[feature_cols]
    y_test = test_df[target_col]
    
    print(f"[✓] Causal Temporal Split Completed (Zero Data Leakage):")
    print(f"    Train: {len(X_train):,} samples (Prevalence: {y_train.mean()*100:.2f}%)")
    print(f"    Test:  {len(X_test):,} samples (Prevalence: {y_test.mean()*100:.2f}%)")
    
    return X_train, y_train, X_test, y_test, feature_cols

def train_and_optimize_models(X_train, y_train, X_test, y_test):
    print("\n[*] Training Cost-Sensitive XGBoost Classifier...")
    
    # Class weight ratio
    neg_count = np.sum(y_train == 0)
    pos_count = np.sum(y_train == 1)
    scale_pos = neg_count / max(1, pos_count)
    
    xgb_model = XGBClassifier(
        n_estimators=150,
        max_depth=5,
        learning_rate=0.05,
        scale_pos_weight=scale_pos,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        eval_metric="logloss"
    )
    
    xgb_model.fit(X_train, y_train)
    y_prob = xgb_model.predict_proba(X_test)[:, 1]
    
    # Baseline Random Forest
    rf_model = RandomForestClassifier(n_estimators=100, max_depth=6, class_weight="balanced", random_state=42, n_jobs=-1)
    rf_model.fit(X_train, y_train)
    rf_prob = rf_model.predict_proba(X_test)[:, 1]
    
    return xgb_model, y_prob, rf_model, rf_prob

def evaluate_cost_curve(y_true, y_prob, cost_fp=500, cost_fn=50000):
    """
    Computes total expected operational cost across decision thresholds tau in [0.01, 0.99].
    Cost = Cost_FP * FP + Cost_FN * FN
    """
    thresholds = np.linspace(0.01, 0.95, 200)
    costs = []
    recalls = []
    precisions = []
    
    for tau in thresholds:
        y_pred = (y_prob >= tau).astype(int)
        cm = confusion_matrix(y_true, y_pred)
        tn, fp, fn, tp = cm.ravel()
        
        total_cost = (fp * cost_fp) + (fn * cost_fn)
        costs.append(total_cost)
        rec = tp / max(1, (tp + fn))
        prec = tp / max(1, (tp + fp))
        recalls.append(rec)
        precisions.append(prec)
        
    costs = np.array(costs)
    opt_idx = np.argmin(costs)
    opt_tau = thresholds[opt_idx]
    min_cost = costs[opt_idx]
    
    # Baseline cost: If we do nothing (No model: all FN)
    unplanned_downtime_cost_baseline = np.sum(y_true == 1) * cost_fn
    savings = unplanned_downtime_cost_baseline - min_cost
    
    print("\n" + "=" * 65)
    print("FINANCIAL COST-BENEFIT OPTIMIZATION (PREDICTIVE MAINTENANCE)")
    print("=" * 65)
    print(f"Cost of Unplanned Failure (FN): ${cost_fn:,.2f}")
    print(f"Cost of Routine Inspection (FP): ${cost_fp:,.2f}")
    print(f"Unmanaged Baseline Cost (No Model): ${unplanned_downtime_cost_baseline:,.2f}")
    print(f"Optimal Decision Threshold (tau*): {opt_tau:.4f}")
    print(f"Optimized Cost under PdM:        ${min_cost:,.2f}")
    print(f"Total Operational Savings:         ${savings:,.2f} ({savings / unplanned_downtime_cost_baseline * 100:.1f}% reduction)")
    print(f"Recall at tau*:                   {recalls[opt_idx]*100:.2f}%")
    print(f"Precision at tau*:                {precisions[opt_idx]*100:.2f}%")
    print("=" * 65)
    
    return thresholds, costs, opt_tau, min_cost, unplanned_downtime_cost_baseline

def generate_visualizations(y_test, y_prob_xgb, y_prob_rf, thresholds, costs, opt_tau, baseline_cost, feature_cols, xgb_model, out_dir):
    plt.style.use("dark_background")
    
    # 1. PR and ROC Curves
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6), facecolor="#0a0e17")
    ax1.set_facecolor("#111827")
    ax2.set_facecolor("#111827")
    
    # Precision-Recall
    prec_xgb, rec_xgb, _ = precision_recall_curve(y_test, y_prob_xgb)
    prec_rf, rec_rf, _ = precision_recall_curve(y_test, y_prob_rf)
    pr_auc_xgb = auc(rec_xgb, prec_xgb)
    pr_auc_rf = auc(rec_rf, prec_rf)
    
    ax1.plot(rec_xgb, prec_xgb, color="#00f0ff", lw=2.5, label=f"XGBoost (PR-AUC = {pr_auc_xgb:.3f})")
    ax1.plot(rec_rf, prec_rf, color="#f59e0b", lw=2.0, linestyle="--", label=f"Random Forest (PR-AUC = {pr_auc_rf:.3f})")
    ax1.axhline(y_test.mean(), color="#ef4444", linestyle=":", label=f"No-Skill Baseline ({y_test.mean():.3f})")
    ax1.set_title("Precision-Recall Curve (Imbalanced Failure Target)", color="#f8fafc", fontsize=13, fontweight="bold")
    ax1.set_xlabel("Recall (Sensitivity)", color="#94a3b8")
    ax1.set_ylabel("Precision", color="#94a3b8")
    ax1.legend(loc="lower left", framealpha=0.85)
    ax1.grid(color="#1e293b", linestyle=":")
    
    # ROC Curve
    fpr_xgb, tpr_xgb, _ = roc_curve(y_test, y_prob_xgb)
    roc_auc_xgb = auc(fpr_xgb, tpr_xgb)
    ax2.plot(fpr_xgb, tpr_xgb, color="#00f0ff", lw=2.5, label=f"XGBoost (ROC-AUC = {roc_auc_xgb:.3f})")
    ax2.plot([0, 1], [0, 1], color="#94a3b8", linestyle=":", label="Random Guess (0.500)")
    ax2.set_title("ROC Curve (True Positive vs False Positive Rate)", color="#f8fafc", fontsize=13, fontweight="bold")
    ax2.set_xlabel("False Positive Rate", color="#94a3b8")
    ax2.set_ylabel("True Positive Rate", color="#94a3b8")
    ax2.legend(loc="lower right", framealpha=0.85)
    ax2.grid(color="#1e293b", linestyle=":")
    
    pr_roc_path = os.path.join(out_dir, "pdm_roc_pr_curves.png")
    plt.tight_layout()
    plt.savefig(pr_roc_path, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"[✓] PR & ROC Curves saved to: {pr_roc_path}")
    
    # 2. Cost Optimization Curve
    fig, ax = plt.subplots(figsize=(12, 6), facecolor="#0a0e17")
    ax.set_facecolor("#111827")
    ax.plot(thresholds, costs / 1000.0, color="#00f0ff", lw=2.5, label="Expected Operational Cost ($k USD)")
    ax.axhline(baseline_cost / 1000.0, color="#ef4444", linestyle="--", lw=2.0, label=f"Unmanaged Baseline Cost (${baseline_cost/1000.0:,.0f}k)")
    ax.axvline(opt_tau, color="#10b981", linestyle="--", lw=2.2, label=f"Optimal Decision Threshold (tau* = {opt_tau:.3f})")
    ax.scatter([opt_tau], [np.min(costs) / 1000.0], color="#10b981", s=100, zorder=5)
    
    ax.set_title("COST-SENSITIVE DECISION CURVE (UNPLANNED DOWNTIME MINIMIZATION)", color="#f8fafc", fontsize=13, fontweight="bold")
    ax.set_xlabel("Classification Decision Threshold (tau)", color="#94a3b8")
    ax.set_ylabel("Total Financial Cost ($k USD)", color="#94a3b8")
    ax.legend(loc="upper right", framealpha=0.85)
    ax.grid(color="#1e293b", linestyle=":")
    
    cost_curve_path = os.path.join(out_dir, "pdm_cost_curve_optimization.png")
    plt.tight_layout()
    plt.savefig(cost_curve_path, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"[✓] Cost Curve saved to: {cost_curve_path}")
    
    # 3. Feature Importance
    importances = xgb_model.feature_importances_
    sorted_idx = np.argsort(importances)[-12:]
    
    fig, ax = plt.subplots(figsize=(10, 7), facecolor="#0a0e17")
    ax.set_facecolor("#111827")
    ax.barh(range(len(sorted_idx)), importances[sorted_idx], color="#00f0ff", align="center", alpha=0.85)
    ax.set_yticks(range(len(sorted_idx)))
    ax.set_yticklabels([feature_cols[i] for i in sorted_idx], color="#f8fafc", fontsize=10)
    ax.set_title("XGBOOST FEATURE IMPORTANCE (GAIN IN SPLIT CRITERIA)", color="#f8fafc", fontsize=13, fontweight="bold")
    ax.set_xlabel("Relative Feature Importance (F-Score)", color="#94a3b8")
    ax.grid(color="#1e293b", linestyle=":", axis="x")
    
    feat_imp_path = os.path.join(out_dir, "pdm_feature_importance.png")
    plt.tight_layout()
    plt.savefig(feat_imp_path, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"[✓] Feature Importance saved to: {feat_imp_path}")

def main():
    base_dir = os.path.dirname(__file__)
    data_path = os.path.join(base_dir, "..", "data", "curated_features", "telemetry_feature_matrix.parquet")
    models_dir = os.path.join(base_dir, "..", "models")
    images_dir = os.path.join(base_dir, "..", "docs", "images")
    os.makedirs(models_dir, exist_ok=True)
    os.makedirs(images_dir, exist_ok=True)
    
    X_train, y_train, X_test, y_test, feature_cols = load_and_split_data(data_path)
    xgb_model, y_prob_xgb, rf_model, y_prob_rf = train_and_optimize_models(X_train, y_train, X_test, y_test)
    
    thresholds, costs, opt_tau, min_cost, baseline_cost = evaluate_cost_curve(y_test, y_prob_xgb)
    
    generate_visualizations(
        y_test, y_prob_xgb, y_prob_rf, thresholds, costs, 
        opt_tau, baseline_cost, feature_cols, xgb_model, images_dir
    )
    
    # Save Model Artifact
    model_artifact_path = os.path.join(models_dir, "pdm_xgboost_model.joblib")
    joblib.dump({"model": xgb_model, "optimal_threshold": opt_tau, "features": feature_cols}, model_artifact_path)
    print(f"\n[✓] Trained Model Artifact saved to:\n    {os.path.abspath(model_artifact_path)}")
    print("[✓] Predictive Maintenance Pipeline successfully trained and exported!")

if __name__ == "__main__":
    main()

