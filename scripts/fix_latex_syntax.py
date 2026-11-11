#!/usr/bin/env python3
"""
Inspects and fixes all LaTeX syntax issues in README.md and docs/ to ensure 100% compatibility
with GitHub MathJax/KaTeX rendering without '_' allowed only in math mode errors.
"""

import re
import os

def clean_readme():
    path = os.path.join(os.path.dirname(__file__), "..", "README.md")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # Replacements in README.md
    # 1. t_{\text{last\_event}}^{(k)} -> t_{\mathrm{last}}^{(k)}
    content = content.replace(r"t_{\text{last\_event}}^{(k)}", r"t_{\mathrm{last}}^{(k)}")
    content = content.replace(r"t_{\text{last_event}}^{(k)}", r"t_{\mathrm{last}}^{(k)}")
    content = content.replace(r"t_{\text{wall}}", r"t_{\mathrm{wall}}")
    content = content.replace(r"\tau_{\text{idle}}", r"\tau_{\mathrm{idle}}")
    content = content.replace(r"W_{\text{global}}", r"W_{\mathrm{global}}")

    # 2. Pattern in CEP NFA
    old_pattern = r"""$$\begin{aligned}
\text{Pattern} = & \text{begin}("thermal\_spike") \\
& \text{.where}(e_1 \implies e_1.\text{temperature\_celsius} > 85.0^\circ\text{C}) \\
& \text{.followedBy}("kinetic\_vibration") \\
& \text{.where}(e_2 \implies e_2.\text{vibration\_rms} > 6.0\text{ mm/s}) \\
& \text{.within}(\text{Time.seconds}(30))
\end{aligned}$$"""

    new_pattern = r"""$$
\begin{aligned}
\mathrm{Pattern} = & \; \mathrm{begin}(\text{"thermal-spike"}) \\
& \quad .\mathrm{where}(e_1 \implies e_1.T > 85.0^\circ\mathrm{C}) \\
& \quad .\mathrm{followedBy}(\text{"kinetic-vibration"}) \\
& \quad .\mathrm{where}(e_2 \implies e_2.V_{\mathrm{RMS}} > 6.0\text{ mm/s}) \\
& \quad .\mathrm{within}(\Delta t \le 30\text{s})
\end{aligned}
$$"""
    content = content.replace(old_pattern, new_pattern)

    # 3. Idempotency Token
    content = content.replace(
        r"$$\text{IdempotencyToken} = \text{SHA256}(\text{sensor\_id} \parallel \text{pattern\_name} \parallel t_{\text{start}} \parallel t_{\text{end}})$$",
        r"""$$
\mathrm{IdempotencyToken} = \mathrm{SHA256}(\mathrm{sensorId} \parallel \mathrm{patternName} \parallel t_{\mathrm{start}} \parallel t_{\mathrm{end}})
$$"""
    )
    content = content.replace(
        r"$$P(\text{Colisión}) \approx 1 - \exp\left(-\frac{N^2}{2 \cdot 2^{256}}\right) \approx \frac{10^{18}}{2^{257}} \approx 4.3 \times 10^{-60} \approx 0$$",
        r"""$$
P(\mathrm{Collision}) \approx 1 - \exp\left(-\frac{N^2}{2 \cdot 2^{256}}\right) \approx \frac{10^{18}}{2^{257}} \approx 4.3 \times 10^{-60} \approx 0
$$"""
    )

    # 4. Feature engineering formulas
    content = content.replace(
        r"$$\text{Factor de Cresta Vibracional: } C_V = \frac{\text{max\_vibration}}{\text{avg\_vibration}}$$".strip(),
        r"""$$
C_V = \frac{V_{\max}}{V_{\mathrm{avg}}}
$$"""
    )
    content = content.replace(
        r"$$\text{Relación de Esfuerzo Electromecánico: } \eta_{\text{EM}} = \frac{\text{avg\_power}}{\text{avg\_temperature} - T_{\text{ambient}}}$$".strip(),
        r"""$$
\eta_{\mathrm{EM}} = \frac{P_{\mathrm{avg}}}{T_{\mathrm{avg}} - T_{\mathrm{ambient}}}
$$"""
    )
    content = content.replace(
        r"$$\text{Densidad Espectral de Potencia (FFT): } S_{xx}(f) = \lim_{T \to \infty} \frac{1}{T} \mathbb{E}\left[ |X_T(f)|^2 \right]$$".strip(),
        r"""$$
S_{xx}(f) = \lim_{T \to \infty} \frac{1}{T} \mathbb{E}\left[ |X_T(f)|^2 \right]
$$"""
    )

    # 5. Fix single line block math with markdown paragraph wrapping
    content = content.replace(
        r"$$\text{Si } t_{\mathrm{wall}} - t_{\mathrm{last}}^{(k)} > \tau_{\mathrm{idle}} \implies \text{Shard } k \text{ pasa a estado IDLE} \implies W_{\mathrm{global}} = \min_{j \neq k} W_j$$",
        r"""$$
t_{\mathrm{wall}} - t_{\mathrm{last}}^{(k)} > \tau_{\mathrm{idle}} \implies \text{Shard } k \text{ pasa a IDLE} \implies W_{\mathrm{global}} = \min_{j \neq k} W_j
$$"""
    )
    content = content.replace(
        r"$$W(t) = \max_{e \in \text{Observed}(t)} (e.t_e) - \delta_{\text{bounded}}, \quad \text{con } \delta_{\text{bounded}} = 5,000\text{ ms}$$",
        r"""$$
W(t) = \max_{e \in \mathrm{Observed}(t)} (e.t_e) - \delta_{\mathrm{bounded}}, \quad \text{con } \delta_{\mathrm{bounded}} = 5000\text{ ms}
$$"""
    )
    content = content.replace(
        r"$$\rho = \frac{\lambda}{\mu} = \frac{1,000}{4} = 250 \gg 1$$",
        r"""$$
\rho = \frac{\lambda}{\mu} = \frac{1000}{4} = 250 \gg 1
$$"""
    )
    content = content.replace(
        r"$$\lim_{\rho \to 1^+} W_q = \lim_{\rho \to 1^+} \frac{\lambda \mathbb{E}[S^2]}{2(1 - \rho)} = \infty$$",
        r"""$$
\lim_{\rho \to 1^+} W_q = \lim_{\rho \to 1^+} \frac{\lambda \mathbb{E}[S^2]}{2(1 - \rho)} = \infty
$$"""
    )
    content = content.replace(
        r"$$b_{1, p} = \frac{1}{n^2} \sum_{i=1}^n \sum_{j=1}^n \left[ (\mathbf{x}_i - \bar{\mathbf{x}})^T \mathbf{S}^{-1} (\mathbf{x}_j - \bar{\mathbf{x}}) \right]^3$$",
        r"""$$
b_{1, p} = \frac{1}{n^2} \sum_{i=1}^n \sum_{j=1}^n \left[ (\mathbf{x}_i - \bar{\mathbf{x}})^T \mathbf{S}^{-1} (\mathbf{x}_j - \bar{\mathbf{x}}) \right]^3
$$"""
    )
    content = content.replace(
        r"$$\kappa = \frac{n \cdot b_{1, p}}{6} \xrightarrow{d} \chi^2\left(\frac{p(p+1)(p+2)}{6}\right)$$",
        r"""$$
\kappa = \frac{n \cdot b_{1, p}}{6} \xrightarrow{d} \chi^2\left(\frac{p(p+1)(p+2)}{6}\right)
$$"""
    )
    content = content.replace(
        r"$$T^2 = \frac{n_1 n_2}{n_1 + n_2} (\bar{\mathbf{x}}_1 - \bar{\mathbf{x}}_2)^T \mathbf{S}_{\text{pooled}}^{-1} (\bar{\mathbf{x}}_1 - \bar{\mathbf{x}}_2)$$",
        r"""$$
T^2 = \frac{n_1 n_2}{n_1 + n_2} (\bar{\mathbf{x}}_1 - \bar{\mathbf{x}}_2)^T \mathbf{S}_{\mathrm{pooled}}^{-1} (\bar{\mathbf{x}}_1 - \bar{\mathbf{x}}_2)
$$"""
    )
    content = content.replace(
        r"$$F = \frac{n_1 + n_2 - p - 1}{p(n_1 + n_2 - 2)} T^2 \sim F(p, \; n_1 + n_2 - p - 1)$$",
        r"""$$
F = \frac{n_1 + n_2 - p - 1}{p(n_1 + n_2 - 2)} T^2 \sim F(p, \; n_1 + n_2 - p - 1)
$$"""
    )
    content = content.replace(
        r"$$D_M^2(\mathbf{x}) = (\mathbf{x} - \boldsymbol{\mu}_0)^T \boldsymbol{\Sigma}_0^{-1} (\mathbf{x} - \boldsymbol{\mu}_0) \sim \chi^2(p)$$",
        r"""$$
D_M^2(\mathbf{x}) = (\mathbf{x} - \boldsymbol{\mu}_0)^T \boldsymbol{\Sigma}_0^{-1} (\mathbf{x} - \boldsymbol{\mu}_0) \sim \chi^2(p)
$$"""
    )
    content = content.replace(
        r"$$\text{Umbral Crítico: } \chi^2_{0.999}(4) = 18.467$$",
        r"""$$
\text{Umbral Crítico: } \chi^2_{0.999}(4) = 18.467
$$"""
    )
    content = content.replace(
        r"$$\text{Regla de Decisión: } D_M^2(\mathbf{x}) > 18.467 \implies \text{Evento Anómalo Crítico}$$",
        r"""$$
D_M^2(\mathbf{x}) > 18.467 \implies \text{Evento Anómalo Crítico}
$$"""
    )
    content = content.replace(
        r"$$C(X_{n+1}) = \left[ \hat{q}_{\alpha/2}(X_{n+1}) - Q_{1-\alpha}(E, D_{\text{cal}}), \; \hat{q}_{1-\alpha/2}(X_{n+1}) + Q_{1-\alpha}(E, D_{\text{cal}}) \right]$$",
        r"""$$
C(X_{n+1}) = \left[ \hat{q}_{\alpha/2}(X_{n+1}) - Q_{1-\alpha}(E, D_{\mathrm{cal}}), \; \hat{q}_{1-\alpha/2}(X_{n+1}) + Q_{1-\alpha}(E, D_{\mathrm{cal}}) \right]
$$"""
    )
    content = content.replace(
        r"$$P(Y_{n+1} \in C(X_{n+1})) \ge 1 - \alpha$$",
        r"""$$
P(Y_{n+1} \in C(X_{n+1})) \ge 1 - \alpha
$$"""
    )

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print("[✓] README.md sanitized.")

def clean_math_analysis():
    path = os.path.join(os.path.dirname(__file__), "..", "docs", "MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    content = content.replace(r"t_{\text{last\_event}, m}", r"t_{\mathrm{last}, m}")
    content = content.replace(r"$t_{\text{last\_event}, m}$", r"$t_{\mathrm{last}, m}$")
    content = content.replace(r"t_{\text{last\_event}, j}", r"t_{\mathrm{last}, j}")
    content = content.replace(r"e.\text{vibration\_rms}", r"e.V_{\mathrm{RMS}}")
    content = content.replace(r"\text{sensor\_id}", r"\mathrm{sensorId}")
    content = content.replace(r"\text{pattern\_name}", r"\mathrm{patternName}")

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print("[✓] docs/MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md sanitized.")

if __name__ == "__main__":
    clean_readme()
    clean_math_analysis()
