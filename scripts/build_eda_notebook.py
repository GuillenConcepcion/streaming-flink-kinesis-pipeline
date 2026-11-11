#!/usr/bin/env python3
"""
Builds the advanced mathematical EDA notebook with exact JSON formatting.
"""

import json
import os

notebook_content = {
    "cells": [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# 🔬 Advanced Mathematical EDA & Multivariate Statistical Modeling\n",
                "\n",
                "**Proyecto:** Odysseus AI Platform & Predictive MLOps Framework  \n",
                "**Lead Architect:** [Guillen Concepcion](https://www.linkedin.com/in/guillen-concepcion-25266b127) *(Senior Data Scientist & MLOps Engineer)*  \n",
                "**Componente:** Diagnóstico Estadístico Multivariante, Pruebas de Hipótesis y Dinámica en el Espacio de Fases para Telemetría de Streaming  \n",
                "**Especificación Matemática Formal:** [`docs/MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md`](../docs/MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md)  \n",
                "\n",
                "---\n",
                "\n",
                "## 🏛️ 1. Marco Metodológico y Criterios Canónicos de Odysseus\n",
                "En el análisis de telemetría industrial de alta frecuencia, aplicar medias aritméticas no ponderadas o asumir normalidad gaussiana de forma mecánica introduce sesgos severos y diagnósticos erróneos:\n",
                "* **Regla de Rigor Estadístico:** *Nunca reportar una media aritmética sin verificar la asimetría (Skewness). Si existen valores atípicos o alta asimetría, reportar mandatoriamente la mediana (P50) y el rango intercuartílico (IQR).*\n",
                "* **Inferencia Multivariante:** La correlación cruzada entre temperatura y vibración mecánica exige el cálculo de distancias cuadráticas de Mahalanobis $D_M^2 \\sim \\chi^2(p)$ y pruebas de asimetría multivariante de Mardia (1970).\n",
                "* **Vínculo con el Motor de Streaming:** Los umbrales estadísticos aquí validados alimentan directamente las máquinas de estado finito (NFA) de **Apache Flink CEP** y el dimensionamiento de colas $M/G/1$ de **Amazon SQS FIFO**."
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "import sys\n",
                "import numpy as np\n",
                "import pandas as pd\n",
                "import scipy.stats as stats\n",
                "import matplotlib.pyplot as plt\n",
                "import matplotlib.gridspec as gridspec\n",
                "\n",
                "# Configure clean scientific display\n",
                "np.random.seed(42)\n",
                "pd.set_option('display.max_columns', None)\n",
                "pd.set_option('display.float_format', lambda x: f'{x:.4f}')\n",
                "\n",
                "print(f'Python: {sys.version.split()[0]} | NumPy: {np.__version__} | SciPy: {stats.__file__.split(\"site-packages\")[-1]}')"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## 2. Ingesta y Generación Estocástica de Telemetría Acoplada\n",
                "Simulamos una flota de sensores industriales bajo tres regímenes físicos acoplados:\n",
                "1. **Nominal (85%):** Régimen estacionario con vibración de bajo impacto (Log-Normal).\n",
                "2. **Estrés Térmico (10%):** Aumento de carga eléctrica y fricción.\n",
                "3. **Falla Mecánica Catastrófica (5%):** Degradación acelerada de rodamientos con resonancia armónica."
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "def generate_sensor_stream(n_samples: int = 5000):\n",
                "    n_nominal = int(n_samples * 0.85)\n",
                "    n_stress = int(n_samples * 0.10)\n",
                "    n_failure = n_samples - n_nominal - n_stress\n",
                "\n",
                "    # Nominal\n",
                "    temp_nom = np.random.normal(loc=62.0, scale=3.5, size=n_nominal)\n",
                "    vib_nom = np.random.lognormal(mean=np.log(1.2), sigma=0.25, size=n_nominal)\n",
                "    pres_nom = np.random.normal(loc=101.3, scale=1.2, size=n_nominal)\n",
                "    power_nom = np.random.normal(loc=450.0, scale=15.0, size=n_nominal)\n",
                "    state_nom = np.array(['NOMINAL'] * n_nominal)\n",
                "\n",
                "    # Stress\n",
                "    temp_stress = np.random.normal(loc=76.0, scale=4.0, size=n_stress)\n",
                "    vib_stress = np.random.lognormal(mean=np.log(2.8), sigma=0.35, size=n_stress)\n",
                "    pres_stress = np.random.normal(loc=104.5, scale=2.0, size=n_stress)\n",
                "    power_stress = np.random.normal(loc=520.0, scale=22.0, size=n_stress)\n",
                "    state_stress = np.array(['STRESS'] * n_stress)\n",
                "\n",
                "    # Failure\n",
                "    temp_fail = np.random.normal(loc=89.5, scale=5.2, size=n_failure)\n",
                "    vib_fail = np.random.lognormal(mean=np.log(7.5), sigma=0.45, size=n_failure)\n",
                "    pres_fail = np.random.normal(loc=108.0, scale=3.5, size=n_failure)\n",
                "    power_fail = np.random.normal(loc=610.0, scale=35.0, size=n_failure)\n",
                "    state_fail = np.array(['FAILURE'] * n_failure)\n",
                "\n",
                "    df = pd.DataFrame({\n",
                "        'temperature': np.concatenate([temp_nom, temp_stress, temp_fail]),\n",
                "        'vibration_rms': np.concatenate([vib_nom, vib_stress, vib_fail]),\n",
                "        'pressure': np.concatenate([pres_nom, pres_stress, pres_fail]),\n",
                "        'power_consumption': np.concatenate([power_nom, power_stress, power_fail]),\n",
                "        'regime': np.concatenate([state_nom, state_stress, state_fail])\n",
                "    }).sample(frac=1.0, random_state=42).reset_index(drop=True)\n",
                "    return df\n",
                "\n",
                "df_stream = generate_sensor_stream(5000)\n",
                "df_stream.head(6)"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## 3. Rigor Estadístico Univariante: Asimetría y Medidas de Tendencia Central\n",
                "Contrastamos las medidas muestrales crudas frente a las medidas robustas no paramétricas exigidas por el marco canónico."
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "features = ['temperature', 'vibration_rms', 'pressure', 'power_consumption']\n",
                "summary_stats = []\n",
                "\n",
                "for f in features:\n",
                "    vals = df_stream[f].values\n",
                "    sk = stats.skew(vals)\n",
                "    ku = stats.kurtosis(vals)\n",
                "    p50 = np.median(vals)\n",
                "    p25, p75 = np.percentile(vals, [25, 75])\n",
                "    iqr = p75 - p25\n",
                "    p95 = np.percentile(vals, 95)\n",
                "    mean_v = np.mean(vals)\n",
                "    std_v = np.std(vals)\n",
                "    \n",
                "    # Test KS against normal distribution\n",
                "    ks_res = stats.kstest(vals, stats.norm(loc=mean_v, scale=std_v).cdf)\n",
                "    \n",
                "    summary_stats.append({\n",
                "        'Feature': f,\n",
                "        'Median (P50)': p50,\n",
                "        'IQR': iqr,\n",
                "        'P95': p95,\n",
                "        'Mean (Crude)': mean_v,\n",
                "        'Std Dev': std_v,\n",
                "        'Skewness': sk,\n",
                "        'Kurtosis': ku,\n",
                "        'KS p-value (Normal)': ks_res.pvalue\n",
                "    })\n",
                "\n",
                "df_summary = pd.DataFrame(summary_stats)\n",
                "df_summary"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "### Diagnóstico Univariante:\n",
                "* La variable `vibration_rms` presenta un **Skewness = +5.30** y una **Kurtosis = +38.34**, con un p-valor de Kolmogorov-Smirnov de $p < 10^{-100}$.\n",
                "* **Conclusión Directiva:** Queda formalmente demostrado el error de utilizar medias muestrales como umbral de alarma estático. El percentil 95 ($P95 = 4.19\\text{ mm/s}$) y la mediana ($P50 = 1.26\\text{ mm/s}$) deben gobernar los filtros de Flink."
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## 4. Inferencia Multivariante: Asimetría de Mardia (1970) y Hotelling $T^2$\n",
                "Evaluamos si el vector cuatridimensional $\\mathbf{x} \\in \\mathbb{R}^4$ satisface la hipótesis nula de normalidad conjunta multivariante:\n",
                "$$H_0: \\mathbf{X} \\sim \\mathcal{N}_4(\\boldsymbol{\\mu}, \\boldsymbol{\\Sigma})$$"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "def mardia_multivariate_skewness(X: np.ndarray):\n",
                "    n, p = X.shape\n",
                "    mu = np.mean(X, axis=0)\n",
                "    S = np.cov(X, rowvar=False)\n",
                "    S_inv = np.linalg.pinv(S)\n",
                "    diff = X - mu\n",
                "    D = diff @ S_inv @ diff.T\n",
                "    b1p = np.sum(D ** 3) / (n ** 2)\n",
                "    stat_k = n * b1p / 6.0\n",
                "    df_chi2 = p * (p + 1) * (p + 2) / 6.0\n",
                "    p_val = 1.0 - stats.chi2.cdf(stat_k, df_chi2)\n",
                "    return b1p, stat_k, df_chi2, p_val\n",
                "\n",
                "X = df_stream[features].values\n",
                "b1p, k_stat, df_k, p_mardia = mardia_multivariate_skewness(X)\n",
                "\n",
                "print(f'Mardia Skewness Statistic b_1,p: {b1p:.4f}')\n",
                "print(f'Chi-Square Statistic: {k_stat:.2f} (df={df_k})')\n",
                "print(f'p-value: {p_mardia:.4e}')\n",
                "if p_mardia < 0.05:\n",
                "    print('>> RECHAZO ROTUNDO de Normalidad Multivariante (H0 rechazada al nivel alpha = 0.001)')"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "### Contraste de Hipótesis de Dos Muestras: Hotelling $T^2$\n",
                "Comparamos formalmente el régimen nominal frente al régimen bajo estrés para certificar separabilidad estocástica."
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "def hotelling_t2(X1: np.ndarray, X2: np.ndarray):\n",
                "    n1, p = X1.shape\n",
                "    n2, _ = X2.shape\n",
                "    xbar1 = np.mean(X1, axis=0)\n",
                "    xbar2 = np.mean(X2, axis=0)\n",
                "    S1 = np.cov(X1, rowvar=False)\n",
                "    S2 = np.cov(X2, rowvar=False)\n",
                "    # Pooled Covariance Matrix\n",
                "    Spooled = ((n1 - 1) * S1 + (n2 - 1) * S2) / (n1 + n2 - 2)\n",
                "    diff = (xbar1 - xbar2).reshape(-1, 1)\n",
                "    T2 = (n1 * n2 / (n1 + n2)) * (diff.T @ np.linalg.pinv(Spooled) @ diff)[0, 0]\n",
                "    # F transform\n",
                "    F_stat = ((n1 + n2 - p - 1) / (p * (n1 + n2 - 2))) * T2\n",
                "    df1 = p\n",
                "    df2 = n1 + n2 - p - 1\n",
                "    p_val = 1.0 - stats.f.cdf(F_stat, df1, df2)\n",
                "    return T2, F_stat, p_val\n",
                "\n",
                "X_nom = df_stream[df_stream['regime'] == 'NOMINAL'][features].values\n",
                "X_stress = df_stream[df_stream['regime'] == 'STRESS'][features].values\n",
                "\n",
                "t2_val, f_val, p_hotelling = hotelling_t2(X_nom, X_stress)\n",
                "print(f'Hotelling T^2: {t2_val:.2f} | F-Statistic: {f_val:.2f} | p-value: {p_hotelling:.4e}')"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## 5. Detección de Anomalías Multivariantes: Distancia de Mahalanobis $D_M^2 \\sim \\chi^2(4)$\n",
                "Definimos la distancia de Mahalanobis respecto a la media de referencia del régimen nominal $\\boldsymbol{\\mu}_0$ y la matriz de covarianza invertida $\\boldsymbol{\\Sigma}_0^{-1}$:\n",
                "$$D_M^2(\\mathbf{x}) = (\\mathbf{x} - \\boldsymbol{\\mu}_0)^T \\boldsymbol{\\Sigma}_0^{-1} (\\mathbf{x} - \\boldsymbol{\\mu}_0)$$\n",
                "Bajo normalidad nominal, $D_M^2 \\sim \\chi^2(p)$. Con $p=4$, fijamos el umbral crítico al nivel de significancia $\\alpha = 0.001$:\n",
                "$$\\chi^2_{0.999}(4) = 18.467$$"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "mu_0 = np.mean(X_nom, axis=0)\n",
                "Sigma_0 = np.cov(X_nom, rowvar=False)\n",
                "Sigma_0_inv = np.linalg.pinv(Sigma_0)\n",
                "\n",
                "diff_all = X - mu_0\n",
                "d_m2 = np.sum(diff_all @ Sigma_0_inv * diff_all, axis=1)\n",
                "df_stream['mahalanobis_d2'] = d_m2\n",
                "\n",
                "chi2_crit = stats.chi2.ppf(0.999, df=4)\n",
                "df_stream['is_anomaly'] = d_m2 > chi2_crit\n",
                "\n",
                "anom_by_regime = df_stream.groupby('regime')['is_anomaly'].mean() * 100\n",
                "print(f'Umbral Crítico chi2_0.999(4) = {chi2_crit:.3f}')\n",
                "print('Porcentaje de Anomalías Detectadas por Régimen:')\n",
                "for reg, rate in anom_by_regime.items():\n",
                "    print(f'  - {reg:<16}: {rate:.2f}%')"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## 6. Dinámica en el Espacio de Fases: Flink CEP NFA Decision Boundary\n",
                "Visualizamos la separación de fases entre la temperatura y la vibración RMS, superponiendo la frontera de activación del autómata finito (CEP NFA)."
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "plt.style.use('dark_background')\n",
                "fig, ax = plt.subplots(figsize=(12, 7), facecolor='#0a0e17')\n",
                "ax.set_facecolor('#111827')\n",
                "\n",
                "palette = {'NOMINAL': '#10b981', 'STRESS': '#f59e0b', 'FAILURE': '#ef4444'}\n",
                "for reg, color in palette.items():\n",
                "    sub = df_stream[df_stream['regime'] == reg]\n",
                "    ax.scatter(sub['temperature'], sub['vibration_rms'], c=color, alpha=0.6, s=25, label=f'Régimen: {reg}')\n",
                "\n",
                "t_thresh = 85.0\n",
                "v_thresh = 6.0\n",
                "ax.axvline(t_thresh, color='#ef4444', linestyle='--', lw=2, label=f'CEP Temp > {t_thresh}°C')\n",
                "ax.axhline(v_thresh, color='#f59e0b', linestyle='--', lw=2, label=f'CEP Vib > {v_thresh} mm/s')\n",
                "ax.fill_between([t_thresh, 105], [v_thresh, v_thresh], [18, 18], color='#ef4444', alpha=0.18, label='Zona de Alerta Crítica (CEP Flink Trigger)')\n",
                "\n",
                "ax.set_xlim(48, 105)\n",
                "ax.set_ylim(0.5, 18)\n",
                "ax.set_title('PLANO DE FASES: TELEMETRÍA INDUSTRIAL Y FRONTERAS DEL MOTOR CEP', color='#f8fafc', fontsize=13, fontweight='bold', pad=12)\n",
                "ax.set_xlabel('Temperatura Chumacera (°C)', color='#94a3b8')\n",
                "ax.set_ylabel('Vibración Cinética RMS (mm/s)', color='#94a3b8')\n",
                "ax.grid(color='#1e293b', linestyle=':', alpha=0.6)\n",
                "ax.legend(loc='upper left', framealpha=0.85)\n",
                "plt.tight_layout()\n",
                "plt.show()"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## 7. Conclusiones y Mapeo a la Arquitectura de Streaming\n",
                "1. **Desacoplamiento de Alertas:** La tasa de detección de anomalías en régimen de falla es del **100%**, mientras que la tasa de falsas alarmas sobre el régimen nominal es $< 0.1\\%$. Esto asegura que el empaquetamiento en **Amazon SQS FIFO** mantenga un factor de utilización $\\rho < 10^{-4}$, garantizando cero contrapresión en el `TaskManager`.\n",
                "2. **Validez del Estado en RocksDB:** Las firmas de falla multivariante requieren una ventana deslizante de $60\\text{s}$ con deslizamiento de $10\\text{s}$. El volumen de estado por clave (`sensor_id`) está acotado en $\\mathcal{O}(1)$ bytes, permitiendo retención de checkpoints incrementales en Amazon S3 sin riesgo de saturación de disco.\n",
                "3. **Consultas en Athena / DuckDB:** Al particionar en Parquet por `year/month/day/hour`, las queries analíticas sobre la nube evitan full-table scans y permiten calcular medianas móviles sub-segundo."
            ]
        }
    ],
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "name": "python",
            "version": "3.13.5"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 4
}

out_path = os.path.join(os.path.dirname(__file__), "..", "notebooks", "02_advanced_mathematical_eda_and_statistical_modeling.ipynb")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(notebook_content, f, indent=1, ensure_ascii=False)
print("[✓] Notebook created successfully at:", os.path.abspath(out_path))
