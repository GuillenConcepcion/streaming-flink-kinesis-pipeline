#!/usr/bin/env python3
"""
=============================================================================
Odysseus Notebook Builder: 04_unified_statistical_inferential_xai_pdm_suite
=============================================================================
Constructs and executes the 4th interactive research notebook covering:
1. AthenaDataLakeConnector & ATHENA_FEATURE_STORE_SQL (Binary + Multiclass).
2. Stratified Descriptive & 6 Inferential Hypothesis Tests (Welch's t,
   Mann-Whitney U, 2-sample KS, Cohen's d, One-way ANOVA, ADF Stationarity).
3. Chronological 70/15/15 Split, Multi-Model Ensemble Benchmark (LightGBM,
   HistGradientBoosting, RandomForest), and Multiclass Root-Cause Diagnosis.
4. XAI SHAP Global Explainability & Holt-Winters ISO-10816 Forecasting.
5. Embedded Visualization of all 6 Executive Figures & ONNX/Lambda Deployment.
=============================================================================
"""

import os
import json
import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell
from nbconvert.preprocessors import ExecutePreprocessor


def build_notebook():
    nb = new_notebook()
    cells = []

    # Cell 1: Title & Overview
    cells.append(new_markdown_cell("""# 🧠 Odysseus Unified Predictive Maintenance: Inferential Statistics, Multiclass Diagnosis, SHAP XAI & Holt-Winters Forecasting

**Proyecto:** Odysseus AI Platform & Predictive MLOps Framework  
**Lead Architect:** [Guillen Concepcion](https://www.linkedin.com/in/guillen-concepcion-25266b127) *(Senior Data Scientist & MLOps Engineer)*  
**Componente:** Suite Unificada de 5 Etapas — SQL Feature Store, Pruebas Inferenciales, Benchmark Multiclase, XAI SHAP, Pronóstico ISO-10816 y Despliegue ONNX/Lambda  

---

## 🧭 Resumen Ejecutivo (CoNVO Framework)
* **Contexto:** Las turbinas industriales monitorizadas por Apache Flink generan flujos continuos de telemetría (temperatura, vibración RMS, presión y potencia) almacenados en particiones Parquet sobre Amazon S3.
* **Necesidad:** Superar la detección binaria simple incorporando:
  1. **Diagnóstico Multiclase de Causa Raíz:** Distinguir entre `NORMAL`, `OVERHEAT_FAULT`, `BEARING_VIBRATION_WEAR` y `PRESSURE_VALVE_DROP`.
  2. **Rigor Estadístico Inferencial:** Validar con pruebas paramétricas y no paramétricas la separabilidad física de los regímenes antes del modelado.
  3. **Partición Cronológica Tripartita (70/15/15):** Calibrar el umbral óptimo $\\tau^*$ estrictamente en *Validation* para erradicar el *Validation Leakage*.
  4. **Explicabilidad (XAI) y Pronóstico de Umbral Crítico:** Cuantificar la atribución causal con **SHAP (`TreeExplainer`)** y proyectar el cruce del límite normativo **ISO-10816 ($6.5\\text{ mm/s}$)** con **Holt-Winters**.
"""))

    # Cell 2: Imports & Athena Feature Store Execution
    cells.append(new_markdown_cell("""## 1. Extracción y Feature Store en SQL (`AthenaDataLakeConnector` & `ATHENA_FEATURE_STORE_SQL`)
Ejecutamos la consulta SQL analítica en **DuckDB** (emulando **Amazon Athena / Trino**) con funciones de ventana (`LAG` 1h/3h/6h, `AVG`, `STDDEV_SAMP`, `MAX` en 6h y 24h), derivadas discretas ($d\\text{Sensor}/dt$) e índices termo-mecánicos."""))

    cells.append(new_code_cell("""import os
import sys
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import Image, display

sys.path.insert(0, os.path.abspath("../scripts"))
from iot_predictive_maintenance_suite import (
    ATHENA_FEATURE_STORE_SQL,
    AthenaDataLakeConnector,
    run_descriptive_and_inferential_analysis,
    train_and_evaluate_models,
    forecast_sensor_critical_threshold,
)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)

connector = AthenaDataLakeConnector()
df_features = connector.synthesize_lakehouse_telemetry(n_machines=12, n_hours=720)
print(f"[+] Matriz del Feature Store extraída: {df_features.shape[0]:,} filas x {df_features.shape[1]} columnas")
print(f"[+] Prevalencia Pre-Falla (<=12h): {df_features['target_binary'].mean()*100:.2f}%")
print("\\nDistribución de Modos de Falla (target_multiclass):")
print(df_features["target_multiclass"].value_counts())
df_features.head(5)"""))

    # Cell 3: Inferential Analysis
    cells.append(new_markdown_cell("""## 2. Análisis Estadístico Descriptivo Estratificado y Pruebas de Hipótesis Inferenciales
De acuerdo con las directrices de **AGENTS.md**, reportamos **Mediana ($P_{50}$) y Rango Intercuartílico ($\mathrm{IQR}$)** junto con Media y Desviación Estándar, y ejecutamos las 6 pruebas de hipótesis formales:
* **Prueba $t$ de Welch:** Contraste paramétrico con varianzas desiguales.
* **Prueba $U$ de Mann-Whitney:** Contraste no paramétrico robusto frente a alta asimetría.
* **Prueba de Kolmogorov-Smirnov ($D_{\\mathrm{KS}}$):** Divergencia máxima entre distribuciones acumuladas empíricas.
* **Tamaño del Efecto ($d$ de Cohen):** Magnitud física del cambio en unidades de desviación estándar agrupada.
* **ANOVA de una vía ($F$-test):** Separabilidad entre los 4 modos multiclase.
* **Augmented Dickey-Fuller (ADF):** Contraste de raíz unitaria y estacionariedad."""))

    cells.append(new_code_cell("""stats_res = run_descriptive_and_inferential_analysis(df_features)
print("=== ESTADÍSTICOS DESCRIPTIVOS ESTRATIFICADOS (NORMAL vs PRE-FALLA <=12H) ===")
display(stats_res["descriptive"].round(4))

print("\\n=== BATERÍA DE 6 PRUEBAS DE HIPÓTESIS INFERENCIALES ===")
display(stats_res["inferential"])"""))

    # Cell 4: Model Benchmark, Multiclass & SHAP
    cells.append(new_markdown_cell("""## 3. Benchmark de Ensambles (Partición Cronológica 70/15/15), Diagnóstico Multiclase y Explicabilidad SHAP
Evaluamos tres arquitecturas de árboles (`LightGBM_EWS`, `XGB_HistGradientBoosting`, `RandomForest_Balanced`), calibrando el umbral de decisión $\\tau^*$ exclusivamente en el pliegue de **Validación (15%)** bajo restricción de precisión mínima en planta ($\\ge 80\\%$)."""))

    cells.append(new_code_cell("""ml_res = train_and_evaluate_models(df_features)

bench_rows = []
for name, b in ml_res["benchmark"].items():
    bench_rows.append({
        "Model": name,
        "PR_AUC": round(b["pr_auc"], 4),
        "ROC_AUC": round(b["roc_auc"], 4),
        "Optimal_Tau_Val": round(b["opt_tau"], 3),
        "Precision_Test": f"{b['precision']*100:.2f}%",
        "Recall_Test": f"{b['recall']*100:.2f}%",
        "F1_Score_Test": round(b["f1"], 4),
        "Net_Savings_USD": f"${b['net_savings_usd']:,.2f}",
    })

df_bench = pd.DataFrame(bench_rows)
print("=== RESULTADOS DEL BENCHMARK EN TEST (SIN DATA LEAKAGE) ===")
display(df_bench)

print("\\n=== TOP 10 VARIABLES POR IMPACTO CAUSAL GLOBAL |SHAP| ===")
display(ml_res["shap_df"].head(10).round(4))"""))

    # Cell 5: Holt-Winters Forecasting
    cells.append(new_markdown_cell("""## 4. Pronóstico de Serie Temporal hacia el Umbral Crítico ISO-10816 ($6.5\\text{ mm/s}$)
Ajustamos un modelo **Holt-Winters Exponential Smoothing** con estacionalidad diurna de 24 horas e intervalos de predicción al 95% para estimar la ventana de intervención antes de cruzar el límite crítico de vibración."""))

    cells.append(new_code_cell("""forecast_res = forecast_sensor_critical_threshold(df_features, critical_limit_mms=6.5, forecast_horizon_hours=24)
print(f"Límite Crítico Normativo ISO-10816: {forecast_res['critical_limit_mms']} mm/s")
print(f"Cruce proyectado (Banda Superior IC 95%): +{forecast_res['hours_to_critical_upper95']} horas")
forecast_df = pd.DataFrame({
    "Forecast_Mean_mms": forecast_res["forecast_mean"],
    "Lower_95_CI": forecast_res["lower_95"],
    "Upper_95_CI": forecast_res["upper_95"],
}).round(3)
display(forecast_df.head(10))"""))

    # Cell 6: Display 6 Visualizations
    cells.append(new_markdown_cell("""## 5. Galería Completa de las 6 Visualizaciones Ejecutivas
Visualizamos los 6 paneles exportados en `outputs/iot_predictive_maintenance/figures/`:"""))

    cells.append(new_code_cell("""fig_dir = "../outputs/iot_predictive_maintenance/figures"
fig_files = [
    "01_telemetry_degradation_timeline.png",
    "02_inferential_distributions_violin.png",
    "03_model_benchmark_pr_roc_curves.png",
    "04_threshold_optimization_and_multiclass_cm.png",
    "05_shap_global_feature_importance.png",
    "06_timeseries_critical_threshold_forecast.png",
]
for fname in fig_files:
    fpath = os.path.join(fig_dir, fname)
    if os.path.exists(fpath):
        print(f"\\n--- {fname} ---")
        display(Image(filename=fpath, width=900))"""))

    nb["cells"] = cells
    nb["metadata"] = {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.13"},
    }

    nb_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "notebooks"))
    nb_path = os.path.join(nb_dir, "04_unified_statistical_inferential_xai_pdm_suite.ipynb")

    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": nb_dir}})

    with open(nb_path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    print(f"[✓] Successfully built and executed: {nb_path}")


if __name__ == "__main__":
    build_notebook()
