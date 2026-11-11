#!/usr/bin/env python3
"""
=============================================================================
Odysseus Predictive Maintenance: Feature Store & Dataset Builder
=============================================================================
Lead Architect: Guillen Concepcion (Senior Data Scientist & MLOps Engineer)
LinkedIn: https://www.linkedin.com/in/guillen-concepcion-25266b127

This script builds the analytical Feature Store for Predictive Maintenance:
1. Loads/synthesizes multi-day historical window telemetry (matching Athena tables).
2. Computes rolling temporal features (1h, 6h, 24h), lags, derivatives, and physical ratios.
3. Injects failure incident records and constructs a causal, leakage-free Target (Y in {0, 1})
   within an actionable prediction horizon (H = 6h, Lead buffer L = 30m).
4. Persists the curated dataset to columnar Apache Parquet.
=============================================================================
"""

import os
import sys
import numpy as np
import pandas as pd

np.random.seed(42)

def generate_multi_day_telemetry(n_sensors: int = 10, days: int = 7) -> pd.DataFrame:
    """
    Generates multi-day, 10-second resolution sliding window telemetry
    matching Flink's AggregatedSensorMetric schema in Amazon S3.
    """
    print(f"[*] Generating {days} days of telemetry across {n_sensors} industrial sensors...")
    
    # 1 record per minute (representing aggregated windows)
    total_minutes = days * 24 * 60
    base_timestamp = pd.Timestamp("2026-11-01 00:00:00")
    timestamps = [base_timestamp + pd.Timedelta(minutes=m) for m in range(total_minutes)]
    
    records = []
    
    for s_idx in range(1, n_sensors + 1):
        sensor_id = f"TURBINE-{s_idx:03d}"
        
        # Base operational state
        base_temp = 60.0 + np.random.uniform(-3, 3)
        base_vib = 1.2 + np.random.uniform(-0.2, 0.2)
        base_pres = 101.3 + np.random.uniform(-1, 1)
        base_power = 450.0 + np.random.uniform(-15, 15)
        
        # Schedule 1-2 degradation episodes per sensor
        fail_times = np.random.choice(range(600, total_minutes - 600), size=2, replace=False)
        fail_times.sort()
        
        # Generate series
        t_vals = np.random.normal(base_temp, 1.2, total_minutes)
        v_vals = np.random.lognormal(np.log(base_vib), 0.15, total_minutes)
        p_vals = np.random.normal(base_pres, 0.8, total_minutes)
        pw_vals = np.random.normal(base_power, 8.0, total_minutes)
        
        # Inject degradation trajectories
        for ft in fail_times:
            # Degradation starts 12 hours before failure (720 minutes)
            start_deg = ft - 720
            deg_progress = np.linspace(0, 1, 720)
            
            # Non-linear thermal & kinetic drift
            t_vals[start_deg:ft] += deg_progress ** 2 * 28.0  # Climbs up to +28 C
            v_vals[start_deg:ft] += deg_progress ** 3 * 7.5   # Climbs up to +7.5 mm/s
            pw_vals[start_deg:ft] += deg_progress * 85.0      # Increased drag
            
            # Post-failure repair / reset to nominal baseline
            repair_duration = 180 # 3 hours of downtime
            t_vals[ft:ft + repair_duration] = 25.0
            v_vals[ft:ft + repair_duration] = 0.05
            pw_vals[ft:ft + repair_duration] = 10.0
            
        for m_idx in range(total_minutes):
            ts = timestamps[m_idx]
            records.append({
                "sensor_id": sensor_id,
                "window_start": int(ts.timestamp() * 1000),
                "timestamp": ts,
                "avg_temperature": float(t_vals[m_idx]),
                "max_temperature": float(t_vals[m_idx] + np.random.uniform(0.5, 2.5)),
                "avg_vibration": float(v_vals[m_idx]),
                "max_vibration": float(v_vals[m_idx] + np.random.uniform(0.3, 1.8)),
                "avg_pressure": float(p_vals[m_idx]),
                "avg_power": float(pw_vals[m_idx]),
                "sample_count": 60,
                "year": str(ts.year),
                "month": f"{ts.month:02d}",
                "day": f"{ts.day:02d}",
                "hour": f"{ts.hour:02d}"
            })
            
    df = pd.DataFrame(records)
    print(f"[✓] Raw telemetry matrix generated: {len(df):,} rows")
    return df

def build_feature_store_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes time-series feature engineering:
    - Lags (10m, 30m, 1h, 6h)
    - Rolling Statistics (Mean, Std, Max over 1h and 6h windows)
    - Temporal Derivatives (dT/dt, dV/dt)
    - Physical Coupling Ratios (Crest Factor, Electromechanical Ratio)
    - Leakage-Free Predictive Target (Y = 1 in [t_fail - 6h, t_fail - 30m])
    """
    print("[*] Computing Feature Store aggregations and causal target labeling...")
    df = df.sort_values(by=["sensor_id", "timestamp"]).reset_index(drop=True)
    
    # Identify failure ground-truth timestamps (where temp > 85.0 and vib > 6.0)
    df["raw_failure_event"] = (df["avg_temperature"] > 84.0) & (df["avg_vibration"] > 5.5)
    
    feature_dfs = []
    
    for sensor_id, group in df.groupby("sensor_id"):
        grp = group.copy().set_index("timestamp")
        
        # 1. Rolling Features
        grp["temp_roll_mean_1h"] = grp["avg_temperature"].rolling("60min").mean()
        grp["temp_roll_std_1h"] = grp["avg_temperature"].rolling("60min").std().fillna(0)
        grp["vib_roll_mean_1h"] = grp["avg_vibration"].rolling("60min").mean()
        grp["vib_roll_std_1h"] = grp["avg_vibration"].rolling("60min").std().fillna(0)
        
        grp["temp_roll_mean_6h"] = grp["avg_temperature"].rolling("360min").mean()
        grp["vib_roll_mean_6h"] = grp["avg_vibration"].rolling("360min").mean()
        grp["vib_roll_max_6h"] = grp["max_vibration"].rolling("360min").max()
        
        # 2. Time-Lags
        grp["temp_lag_30m"] = grp["avg_temperature"].shift(30)
        grp["vib_lag_30m"] = grp["avg_vibration"].shift(30)
        grp["temp_lag_1h"] = grp["avg_temperature"].shift(60)
        grp["vib_lag_1h"] = grp["avg_vibration"].shift(60)
        
        # 3. Derivatives
        grp["d_temp_dt_1h"] = (grp["avg_temperature"] - grp["temp_lag_1h"]) / 60.0
        grp["d_vib_dt_1h"] = (grp["avg_vibration"] - grp["vib_lag_1h"]) / 60.0
        
        # 4. Domain-Specific Physical Ratios
        grp["vibration_crest_factor"] = grp["max_vibration"] / grp["avg_vibration"].replace(0, np.nan)
        grp["electromechanical_ratio"] = grp["avg_power"] / grp["avg_temperature"].replace(0, np.nan)
        
        # 5. Causal Target Labeling:
        # Prediction Horizon H = 360 min (6 hours), Grace Buffer L = 30 min
        # Y = 1 if a failure occurs between t + 30m and t + 360m
        future_failure_6h = grp["raw_failure_event"].shift(-360).rolling("330min").max().fillna(0)
        grp["target_failure_next_6h"] = (future_failure_6h > 0).astype(int)
        
        # Drop rows where failure is already in progress or under repair
        grp = grp[grp["avg_temperature"] > 35.0]
        
        feature_dfs.append(grp.reset_index())
        
    final_df = pd.concat(feature_dfs, ignore_index=True)
    # Fill remaining initial NaNs from shifting
    final_df = final_df.bfill().ffill()
    
    target_dist = final_df["target_failure_next_6h"].value_axis() if hasattr(final_df["target_failure_next_6h"], "value_axis") else final_df["target_failure_next_6h"].value_counts(normalize=True)
    print(f"[✓] Feature Store built: {final_df.shape[0]:,} rows x {final_df.shape[1]} columns")
    print(f"    Target Distribution (Y=1 Pre-failure 6h Horizon): {target_dist[1]*100:.2f}% ({np.sum(final_df['target_failure_next_6h']):,} samples)")
    return final_df

def main():
    print("=" * 75)
    print("ODYSSEUS DATA SCIENCE & PREDICTIVE MAINTENANCE FEATURE STORE")
    print("=" * 75)
    
    raw_df = generate_multi_day_telemetry(n_sensors=10, days=7)
    features_df = build_feature_store_matrix(raw_df)
    
    # Save to curated directory
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "curated_features")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "telemetry_feature_matrix.parquet")
    
    features_df.to_parquet(out_path, index=False, compression="snappy")
    print(f"[✓] Curated Feature Matrix saved to:\n    {os.path.abspath(out_path)}")
    print(f"    File Size: {os.path.getsize(out_path) / (1024 * 1024):.2f} MB")
    print("=" * 75)

if __name__ == "__main__":
    main()

