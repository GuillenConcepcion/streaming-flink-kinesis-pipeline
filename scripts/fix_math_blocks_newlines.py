#!/usr/bin/env python3
"""
Fixes markdown paragraphs and block math formatting in README.md to guarantee 
100% native KaTeX rendering on GitHub.
"""

import os

def fix_readme_math_formatting():
    path = os.path.join(os.path.dirname(__file__), "..", "README.md")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    target_old = r"""## 🔬 Formulación Matemática, Modelado Estocástico y Fórmulas a Utilizar

> [!NOTE]
> La deducción matemática rigurosa, demostraciones de teoremas y contrastes de hipótesis se encuentran desarrollados exhaustivamente en **[`docs/MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md`](docs/MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md)**.

A continuación se resumen las **9 formulaciones matemáticas fundamentales** aplicadas en este pipeline:

### 1. Teoría de Colas y Eliminación Analítica de Contrapresión ($M/G/1$)
Bajo el anti-patrón de invocar webhooks HTTP síncronos con tiempo de servicio $S \sim \text{LogNormal}$ ($\mathbb{E}[S] = 250\text{ ms} \implies \mu = 4\text{ req/s}$) frente a una ingesta $\lambda = 1,000\text{ ev/s}$, el factor de utilización satura el operador:
$$
\rho = \frac{\lambda}{\mu} = \frac{1000}{4} = 250 \gg 1
$$
Por la **fórmula de Pollaczek-Khinchine**, el tiempo medio de espera $W_q$ en el búfer de red diverge hacia el infinito:
$$
\lim_{\rho \to 1^+} W_q = \lim_{\rho \to 1^+} \frac{\lambda \mathbb{E}[S^2]}{2(1 - \rho)} = \infty
$$
Al desacoplar el envío hacia **Amazon SQS FIFO** ($\mu_{\text{SQS}} \ge 3,000\text{ msg/s}$), el factor de utilización colapsa a $\rho \approx 1.1 \times 10^{-5} \ll 1$, demostrando que $W_q < 0.1\text{ ms}$ y eliminando el 100% de la contrapresión en el TaskManager.

---

### 2. Teorema de Marcas de Agua Monótonas con Desorden Acotado
Para un flujo de telemetría con retardo estocástico de transmisión celular/radio $\Delta_i = t_{p, i} - t_{e, i}$, la marca de agua monótona no decreciente $W(t)$ garantiza la completitud temporal de ventanas:
$$
W(t) = \max_{e \in \mathrm{Observed}(t)} (e.t_e) - \delta_{\mathrm{bounded}}, \quad \text{con } \delta_{\mathrm{bounded}} = 5000\text{ ms}
$$
Para prevenir que un shard inactivo de Kinesis congele el reloj global del stream ($W_{\mathrm{global}} = \min_{k=1}^K W_k$), se implementa la regla de inactividad de $\tau_{\mathrm{idle}} = 30\text{s}$:
$$
t_{\mathrm{wall}} - t_{\mathrm{last}}^{(k)} > \tau_{\mathrm{idle}} \implies \text{Shard } k \text{ pasa a IDLE} \implies W_{\mathrm{global}} = \min_{j \neq k} W_j
$$

---

### 3. Autómata Finito No Determinista de Flink CEP (Composite Failure Pattern)
El motor de Complex Event Processing evalúa de forma continua sobre RocksDB la expresión regular temporal sobre el espacio de estados:
$$
\begin{aligned}
\mathrm{Pattern} = & \; \mathrm{begin}(\text{"thermal-spike"}) \\
& \quad .\mathrm{where}(e_1 \implies e_1.T > 85.0^\circ\mathrm{C}) \\
& \quad .\mathrm{followedBy}(\text{"kinetic-vibration"}) \\
& \quad .\mathrm{where}(e_2 \implies e_2.V_{\mathrm{RMS}} > 6.0\text{ mm/s}) \\
& \quad .\mathrm{within}(\Delta t \le 30\text{s})
\end{aligned}
$$
Complejidad espacial en RocksDB: $\mathcal{O}(|\mathcal{K}|)$ estados activos acotados por sensor.

---

### 4. Asimetría Multivariante de Mardia (1970)
Para certificar estadísticamente el rechazo de la hipótesis nula de normalidad multivariante ($H_0: \mathbf{X} \sim \mathcal{N}_p(\boldsymbol{\mu}, \boldsymbol{\Sigma})$) en el vector $\mathbf{x} = [T, V_{\text{RMS}}, P, W]^T \in \mathbb{R}^4$:
$$
b_{1, p} = \frac{1}{n^2} \sum_{i=1}^n \sum_{j=1}^n \left[ (\mathbf{x}_i - \bar{\mathbf{x}})^T \mathbf{S}^{-1} (\mathbf{x}_j - \bar{\mathbf{x}}) \right]^3
$$
Estadístico asintótico de contraste:
$$
\kappa = \frac{n \cdot b_{1, p}}{6} \xrightarrow{d} \chi^2\left(\frac{p(p+1)(p+2)}{6}\right)
$$
*Resultado en la telemetría del proyecto:* $b_{1, p} = 57.34 \implies \kappa = 47,783.4$ ($df = 20, p < 10^{-100}$), demostrando que asumir normalidad gaussiana para telemetría cinética es un error metodológico severo.

---

### 5. Contraste de Dos Muestras Multivariante: Hotelling $T^2$
Para validar la separabilidad estocástica entre el régimen de operación nominal ($n_1$) y el régimen bajo estrés térmico ($n_2$):
$$
T^2 = \frac{n_1 n_2}{n_1 + n_2} (\bar{\mathbf{x}}_1 - \bar{\mathbf{x}}_2)^T \mathbf{S}_{\mathrm{pooled}}^{-1} (\bar{\mathbf{x}}_1 - \bar{\mathbf{x}}_2)
$$
Transformación exacta a distribución $F$ de Snedecor:
$$
F = \frac{n_1 + n_2 - p - 1}{p(n_1 + n_2 - 2)} T^2 \sim F(p, \; n_1 + n_2 - p - 1)
$$

---

### 6. Distancia Cuadrática de Mahalanobis y Umbral Crítico Chi-Cuadrado
Puntuación de anomalía multivariante ponderada por la matriz de covarianza del régimen nominal $\boldsymbol{\Sigma}_0$:
$$
D_M^2(\mathbf{x}) = (\mathbf{x} - \boldsymbol{\mu}_0)^T \boldsymbol{\Sigma}_0^{-1} (\mathbf{x} - \boldsymbol{\mu}_0) \sim \chi^2(p)
$$
Para $p = 4$ variables físicas al nivel de confianza $\alpha = 0.001$:
$$
\text{Umbral Crítico: } \chi^2_{0.999}(4) = 18.467
$$
$$
D_M^2(\mathbf{x}) > 18.467 \implies \text{Evento Anómalo Crítico}
$$

---

### 7. Deduplicación Criptográfica Absoluta (Paradoja del Cumpleaños)
Generación de la clave primaria idempotente para Amazon SQS FIFO y DynamoDB TTL:
$$
\mathrm{IdempotencyToken} = \mathrm{SHA256}(\mathrm{sensorId} \parallel \mathrm{patternName} \parallel t_{\mathrm{start}} \parallel t_{\mathrm{end}})
$$
Cota superior de probabilidad de colisión para $N = 10^9$ incidentes acumulados:
$$
P(\mathrm{Collision}) \approx 1 - \exp\left(-\frac{N^2}{2 \cdot 2^{256}}\right) \approx \frac{10^{18}}{2^{257}} \approx 4.3 \times 10^{-60} \approx 0
$$
Garantiza cero tormentas de alertas ante repetición de checkpoints o reinicios de Flink.

---

### 8. Cuantificación de Incertidumbre Libre de Distribución (CQR)
Para intervalos de predicción con garantía de cobertura estadística marginal sin supuestos distribucionales:
$$
C(X_{n+1}) = \left[ \hat{q}_{\alpha/2}(X_{n+1}) - Q_{1-\alpha}(E, D_{\mathrm{cal}}), \; \hat{q}_{1-\alpha/2}(X_{n+1}) + Q_{1-\alpha}(E, D_{\mathrm{cal}}) \right]
$$
Garantía formal de cobertura finita:
$$
P(Y_{n+1} \in C(X_{n+1})) \ge 1 - \alpha
$$

---

### 9. Fórmulas de Ingeniería de Features para Data Science
Generadas automáticamente a partir de las columnas agregadas en Apache Parquet:
$$
C_V = \frac{V_{\max}}{V_{\mathrm{avg}}}
$$
$$
\eta_{\mathrm{EM}} = \frac{P_{\mathrm{avg}}}{T_{\mathrm{avg}} - T_{\mathrm{ambient}}}
$$
$$
S_{xx}(f) = \lim_{T \to \infty} \frac{1}{T} \mathbb{E}\left[ |X_T(f)|^2 \right]
$$"""

    target_new = r"""## 🔬 Formulación Matemática, Modelado Estocástico y Fórmulas a Utilizar

> [!NOTE]
> La deducción matemática rigurosa, demostraciones de teoremas y contrastes de hipótesis se encuentran desarrollados exhaustivamente en **[`docs/MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md`](docs/MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md)**.

A continuación se resumen las **9 formulaciones matemáticas fundamentales** aplicadas en este pipeline:

### 1. Teoría de Colas y Eliminación Analítica de Contrapresión (Modelo M/G/1)

Bajo el anti-patrón de invocar webhooks HTTP síncronos con tiempo de servicio $S \sim \text{LogNormal}$ ($\mathbb{E}[S] = 250\text{ ms} \implies \mu = 4\text{ req/s}$) frente a una ingesta $\lambda = 1000\text{ ev/s}$, el factor de utilización satura el operador:

$$
\rho = \frac{\lambda}{\mu} = \frac{1000}{4} = 250 \gg 1
$$

Por la **fórmula de Pollaczek-Khinchine**, el tiempo medio de espera $W_q$ en el búfer de red diverge hacia el infinito:

$$
\lim_{\rho \to 1^+} W_q = \lim_{\rho \to 1^+} \frac{\lambda \mathbb{E}[S^2]}{2(1 - \rho)} = \infty
$$

Al desacoplar el envío hacia **Amazon SQS FIFO** ($\mu_{\text{SQS}} \ge 3000\text{ msg/s}$), el factor de utilización colapsa a $\rho \approx 1.1 \times 10^{-5} \ll 1$, demostrando que $W_q < 0.1\text{ ms}$ y eliminando el 100% de la contrapresión en el TaskManager.

---

### 2. Teorema de Marcas de Agua Monótonas con Desorden Acotado

Para un flujo de telemetría con retardo estocástico de transmisión celular/radio $\Delta_i = t_{p, i} - t_{e, i}$, la marca de agua monótona no decreciente $W(t)$ garantiza la completitud temporal de ventanas:

$$
W(t) = \max_{e \in \mathrm{Observed}(t)} (e.t_e) - \delta_{\mathrm{bounded}}, \quad \text{con } \delta_{\mathrm{bounded}} = 5000\text{ ms}
$$

Para prevenir que un shard inactivo de Kinesis congele el reloj global del stream ($W_{\mathrm{global}} = \min_{k=1}^K W_k$), se implementa la regla de inactividad de $\tau_{\mathrm{idle}} = 30\text{s}$:

$$
t_{\mathrm{wall}} - t_{\mathrm{last}}^{(k)} > \tau_{\mathrm{idle}} \implies \text{Shard } k \text{ pasa a IDLE} \implies W_{\mathrm{global}} = \min_{j \neq k} W_j
$$

---

### 3. Autómata Finito No Determinista de Flink CEP (Composite Failure Pattern)

El motor de Complex Event Processing evalúa de forma continua sobre RocksDB la expresión regular temporal sobre el espacio de estados:

$$
\begin{aligned}
\mathrm{Pattern} = & \; \mathrm{begin}(\text{"thermal-spike"}) \\
& \quad .\mathrm{where}(e_1 \implies e_1.T > 85.0^\circ\mathrm{C}) \\
& \quad .\mathrm{followedBy}(\text{"kinetic-vibration"}) \\
& \quad .\mathrm{where}(e_2 \implies e_2.V_{\mathrm{RMS}} > 6.0\text{ mm/s}) \\
& \quad .\mathrm{within}(\Delta t \le 30\text{s})
\end{aligned}
$$

Complejidad espacial en RocksDB: $\mathcal{O}(|\mathcal{K}|)$ estados activos acotados por sensor.

---

### 4. Asimetría Multivariante de Mardia (1970)

Para certificar estadísticamente el rechazo de la hipótesis nula de normalidad multivariante ($H_0: \mathbf{X} \sim \mathcal{N}_p(\boldsymbol{\mu}, \boldsymbol{\Sigma})$) en el vector $\mathbf{x} = [T, V_{\text{RMS}}, P, W]^T \in \mathbb{R}^4$:

$$
b_{1, p} = \frac{1}{n^2} \sum_{i=1}^n \sum_{j=1}^n \left[ (\mathbf{x}_i - \bar{\mathbf{x}})^T \mathbf{S}^{-1} (\mathbf{x}_j - \bar{\mathbf{x}}) \right]^3
$$

Estadístico asintótico de contraste:

$$
\kappa = \frac{n \cdot b_{1, p}}{6} \xrightarrow{d} \chi^2\left(\frac{p(p+1)(p+2)}{6}\right)
$$

*Resultado en la telemetría del proyecto:* $b_{1, p} = 57.34 \implies \kappa = 47,783.4$ ($df = 20, p < 10^{-100}$), demostrando que asumir normalidad gaussiana para telemetría cinética es un error metodológico severo.

---

### 5. Contraste de Dos Muestras Multivariante: Hotelling T²

Para validar la separabilidad estocástica entre el régimen de operación nominal ($n_1$) y el régimen bajo estrés térmico ($n_2$):

$$
T^2 = \frac{n_1 n_2}{n_1 + n_2} (\bar{\mathbf{x}}_1 - \bar{\mathbf{x}}_2)^T \mathbf{S}_{\mathrm{pooled}}^{-1} (\bar{\mathbf{x}}_1 - \bar{\mathbf{x}}_2)
$$

Transformación exacta a distribución $F$ de Snedecor:

$$
F = \frac{n_1 + n_2 - p - 1}{p(n_1 + n_2 - 2)} T^2 \sim F(p, \; n_1 + n_2 - p - 1)
$$

---

### 6. Distancia Cuadrática de Mahalanobis y Umbral Crítico Chi-Cuadrado

Puntuación de anomalía multivariante ponderada por la matriz de covarianza del régimen nominal $\boldsymbol{\Sigma}_0$:

$$
D_M^2(\mathbf{x}) = (\mathbf{x} - \boldsymbol{\mu}_0)^T \boldsymbol{\Sigma}_0^{-1} (\mathbf{x} - \boldsymbol{\mu}_0) \sim \chi^2(p)
$$

Para $p = 4$ variables físicas al nivel de confianza $\alpha = 0.001$:

$$
\text{Umbral Crítico: } \chi^2_{0.999}(4) = 18.467
$$

$$
D_M^2(\mathbf{x}) > 18.467 \implies \text{Evento Anómalo Crítico}
$$

---

### 7. Deduplicación Criptográfica Absoluta (Paradoja del Cumpleaños)

Generación de la clave primaria idempotente para Amazon SQS FIFO y DynamoDB TTL:

$$
\mathrm{IdempotencyToken} = \mathrm{SHA256}(\mathrm{sensorId} \parallel \mathrm{patternName} \parallel t_{\mathrm{start}} \parallel t_{\mathrm{end}})
$$

Cota superior de probabilidad de colisión para $N = 10^9$ incidentes acumulados:

$$
P(\mathrm{Collision}) \approx 1 - \exp\left(-\frac{N^2}{2 \cdot 2^{256}}\right) \approx \frac{10^{18}}{2^{257}} \approx 4.3 \times 10^{-60} \approx 0
$$

Garantiza cero tormentas de alertas ante repetición de checkpoints o reinicios de Flink.

---

### 8. Cuantificación de Incertidumbre Libre de Distribución (CQR)

Para intervalos de predicción con garantía de cobertura estadística marginal sin supuestos distribucionales:

$$
C(X_{n+1}) = \left[ \hat{q}_{\alpha/2}(X_{n+1}) - Q_{1-\alpha}(E, D_{\mathrm{cal}}), \; \hat{q}_{1-\alpha/2}(X_{n+1}) + Q_{1-\alpha}(E, D_{\mathrm{cal}}) \right]
$$

Garantía formal de cobertura finita:

$$
P(Y_{n+1} \in C(X_{n+1})) \ge 1 - \alpha
$$

---

### 9. Fórmulas de Ingeniería de Features para Data Science

Generadas automáticamente a partir de las columnas agregadas en Apache Parquet:

* **Factor de Cresta Vibracional:**

$$
C_V = \frac{V_{\max}}{V_{\mathrm{avg}}}
$$

* **Relación de Esfuerzo Electromecánico:**

$$
\eta_{\mathrm{EM}} = \frac{P_{\mathrm{avg}}}{T_{\mathrm{avg}} - T_{\mathrm{ambient}}}
$$

* **Densidad Espectral de Potencia (FFT):**

$$
S_{xx}(f) = \lim_{T \to \infty} \frac{1}{T} \mathbb{E}\left[ |X_T(f)|^2 \right]
$$"""

    if target_old in content:
        content = content.replace(target_old, target_new)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        print("[✓] Successfully updated README.md with clean empty lines around all block math!")
    else:
        print("[!] Target old content not found exactly. Checking normalization...")
        # fallback line-by-line replacement
        content_norm = content.replace("\r\n", "\n")
        target_old_norm = target_old.replace("\r\n", "\n")
        target_new_norm = target_new.replace("\r\n", "\n")
        if target_old_norm in content_norm:
            content_norm = content_norm.replace(target_old_norm, target_new_norm)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content_norm)
            print("[✓] Successfully updated README.md (normalized newlines)!")
        else:
            print("[x] Could not find section.")

if __name__ == "__main__":
    fix_readme_math_formatting()
