# 🛡️ Plan Maestro de Mantenimiento Predictivo y Prescriptivo (Nivel Ejecutivo y Operativo)

**Proyecto:** Odysseus AI Platform — Enterprise Real-Time IoT Streaming & Predictive Maintenance  
**Lead Architect:** [Guillén Concepción](https://www.linkedin.com/in/guillen-concepcion-25266b127) *(Senior Data Scientist & MLOps Engineer)*  
**Contacto:** `guillenconcepcion@gmail.com` | [GitHub](https://github.com/GuillenConcepcion/streaming-flink-kinesis-pipeline)  
**Estándares de Referencia:** ISO-10816 / ISO-20816 (Severidad Vibratoria), ISO-13381-1 (Pronóstico de Condición), Marco Ejecutivo **CoNVO** & Argumentación **Toulmin**.  

---

## 🏛️ PARTE I: NIVEL EJECUTIVO (C-LEVEL & DIRECCIÓN DE OPERACIONES)

### 1. Resumen Estratégico bajo el Marco CoNVO

* **Contexto (Context):** La flota de turbinas y maquinaria rotativa industrial opera bajo regímenes termo-cinéticos acoplados de alta exigencia. Históricamente, las plantas industriales dependen de mantenimiento correctivo (reparar tras la rotura) o preventivo de calendario fijo (inspeccionar sin importar el desgaste real), generando paradas no programadas con un costo directo e indirecto de **$50,000 USD por evento catastrófico**.
* **Necesidad (Need):** Transformar las señales telemétricas crudas capturadas en tiempo real por **Apache Flink y Amazon Kinesis** en **decisiones prescriptivas anticipadas** ($H = 6\text{ a }12\text{ horas}$ antes del fallo), identificando no solo *cuándo* fallará un activo, sino *qué componente exacto* requiere intervención y *cuántas horas restan* antes de cruzar el límite crítico normativo.
* **Visión (Vision):** Implementar un **Sistema de Gestión de Mantenimiento Basado en Predicción (PdM) de Ciclo Cerrado**, donde las probabilidades calibradas de **XGBoost / LightGBM**, la atribución causal **SHAP** y la proyección **Holt-Winters ISO-10816** disparen automáticamente órdenes de trabajo priorizadas hacia los equipos de ingeniería de planta vía **Amazon SQS FIFO, DynamoDB y SNS**.
* **Resultado (Outcome):** Reducción demostrada del **99.9% en costos por fallas imprevistas** (ahorro neto auditado de **$78,786,500 USD** en el escenario de alta frecuencia y **$6,048,000 USD** en el pliegue de prueba horario), logrando un **100% de captura de fallas críticas** y una **Precisión operativa en planta del 92.55% al 96.80%**.

---

### 2. Argumentación Formal de Negocio (Estructura de Toulmin)

* **Afirmación (*Claim*):** La planta debe adoptar el umbral de decisión sensible al costo ($\tau^* = 0.305\text{ a }0.4635$) y la ventana de intervención temprana de **$+7\text{ horas}$ (Banda Superior IC 95% de Holt-Winters)** como política estándar de despacho de mantenimiento.
* **Datos (*Data*):** 
  1. Las pruebas inferenciales sobre 8,568 ventanas operativas demuestran una divergencia física masiva en las 12 horas previas al fallo: Tamaño del efecto de Cohen $d = +3.75$ en vibración ($p = 2.2 \times 10^{-294}$) y $d = +2.83$ en temperatura ($p = 7.0 \times 10^{-238}$).
  2. El modelo calibrado alcanza $\text{PR-AUC} = 0.9454\text{ a }0.9850$ y reduce el costo esperado de $\$78.85\text{M USD}$ a $\$63.5\text{k USD}$.
* **Garantía (*Warrant*):** Dado que la asimetría financiera entre una rotura no detectada ($C_{\mathrm{FN}} = \$50,000$) y una inspección preventiva ($C_{\mathrm{FP}} = \$500$) es de **100 a 1**, optimizar el umbral sobre la función de pérdida económica esperada domina matemáticamente al umbral ingenuo de $\tau = 0.50$.
* **Respaldo (*Backing*):** Fundamentado en la norma internacional **ISO-13381-1** (Monitoreo de condición y diagnóstico de máquinas — Pronóstico) y en la teoría de decisiones bayesianas sensibles al costo (Elkan, 2001).
* **Refutación y Mitigación de Riesgos (*Rebuttals*):** *¿Qué ocurre si el modelo genera fatiga de alertas en los ingenieros de planta?* Se impuso una restricción dura de **Precisión Mínima en Planta $\ge 80\%$** durante la calibración en el pliegue de Validación (alcanzando $96.8\%$ en Test) y una **barrera de idempotencia SHA-256 en Amazon DynamoDB** con ventana TTL de 15 minutos que suprime el 100% de las alertas duplicadas.

---

### 3. Presupuesto, ROI y KPIs Ejecutivos de Mantenimiento (SLA Industrial)

| Indicador Clave de Desempeño (KPI) | Estado Anterior (Reactivo / Calendario) | Meta Operativa con Plataforma Odysseus PdM | Impacto Estratégico |
| :--- | :---: | :---: | :--- |
| **OEE (*Overall Equipment Effectiveness*)** | $74.0\% - 78.5\%$ | **$\ge 92.0\%$** | $+14\%$ de disponibilidad neta de planta. |
| **MTBF (*Mean Time Between Failures*)** | $180\text{ horas}$ | **$> 720\text{ horas}$ (+300%)** | Extensión de vida útil de rodamientos y sellos. |
| **MTTR (*Mean Time To Repair*)** | $8.5\text{ horas}$ (Cambio de rotor) | **$1.2\text{ horas}$ (Ajuste preventivo)** | Reducción del $85\%$ en tiempo de parada por intervención. |
| **Tasa de Falsas Alarmas (*False Discovery Rate*)** | $> 45\%$ (Umbrales estáticos) | **$3.2\% - 7.4\%$** | Alta confianza del equipo de mantenimiento en terreno. |
| **Costo Medio por Incidente** | $\$50,000\text{ USD}$ (Falla catastrófica) | **$\$500\text{ USD}$ (Inspección en turno)** | **Ahorro neto de $\$49,500\text{ USD}$ por falla evitada.** |

---

## ⚙️ PARTE II: NIVEL OPERATIVO (INGENIERÍA DE PLANTA, TRIAGE Y SOPS)

### 1. Matriz de Triaje Operativo de 4 Niveles (Semáforo de Decisión en Tiempo Real)

El motor de inferencia dual (**PyFlink ONNX en tiempo real** + **AWS Lambda Micro-batch**) clasifica cada activo en uno de 4 niveles de acción combinando la **Probabilidad de Falla ($P$)**, el **Pronóstico Holt-Winters (RUL)** y el **Motor CEP de Flink**:

```
+-------------------------------------------------------------------------------------------------------------------+
|                                  CRONOGRAMA OPERATIVO DE INTERVENCIÓN (24 HORAS)                                  |
|                                                                                                                   |
|   [t + 0h]                       [t + 7h]                               [t + 16h]                     [> t + 16h] |
|   Alerta Predictiva ML           Cruce Banda Superior IC 95%            Cruce Media Holt-Winters      Flink CEP   |
|   (P >= 0.305 / 0.4635)          (Límite Conservador 6.5 mm/s)          (Límite Esperado 6.5 mm/s)    (T>85, V>6) |
|         │                                      │                                       │                    │     |
|         ▼                                      ▼                                       ▼                    ▼     |
|   🟢/🟡 Alistamiento CMMS         🟠 VENTANA ÓPTIMA DE INTERVENCIÓN      🔴 MITIGACIÓN DE CARGA        ⚫ PARADA  |
|   Reserva de repuestos y         Mantenimiento en cambio de turno       Derating eléctrico (-20%) y   DE EMERGENCIA
|   asignación de cuadrilla        ($500 USD | Cero pérdida de planta)    reemplazo inmediato forzado   AUTOMÁTICA  |
+-------------------------------------------------------------------------------------------------------------------+
```

| Nivel de Alerta | Condición de Disparo (Regla Matemática) | Horizonte Temporal (RUL) | Acción Operativa Obligatoria en Planta | SLA de Respuesta |
| :--- | :--- | :---: | :--- | :---: |
| 🟢 **NIVEL 0: NOMINAL** | $P(\text{Falla}) < 0.305$ y $V_{\mathrm{RMS}} < 2.5\text{ mm/s}$ | $> 48\text{ horas}$ | Operación continua nominal. Registro pasivo en Data Lakehouse S3 Parquet. | Monitoreo automático |
| 🟡 **NIVEL 1: VIGILANCIA PREVENTIVA** | $0.305 \le P(\text{Falla}) < 0.4635$ | $12\text{ a }24\text{ horas}$ | Pre-alerta en tablero SCADA. Verificar stock de repuestos según `predicted_failure_mode` y programar inspección en el próximo cambio de turno. | $< 6\text{ horas}$ |
| 🟠 **NIVEL 2: RIESGO ELEVADO (ÓPTIMO)** | $0.4635 \le P(\text{Falla}) < 0.700$ **ó** Cruce Banda IC 95% Holt-Winters ($\le +7\text{h}$) | **$6\text{ a }12\text{ horas}$** | **Despacho de Orden de Trabajo Preventiva (CMMS).** Intervenir antes de la hora $+7\text{h}$ (costo $\$500\text{ USD}$). Evita parada no programada. | **$< 2\text{ horas}$** |
| 🔴 **NIVEL 3: CRÍTICO INMINENTE** | $P(\text{Falla}) \ge 0.700$ **ó** Cruce Media Holt-Winters ($\le +16\text{h}$) **ó** Disparo **Flink CEP** ($T > 85^\circ\text{C} \land V > 6.0\text{ mm/s}$) | **$< 1\text{ a }6\text{ horas}$** | **Intervención de Emergencia / *Load Derating* inmediato (-20% potencia).** Si Flink CEP dispara, ejecutar parada controlada de turbina (*Trip*). | **$< 15\text{ minutos}$** |

---

### 2. Procedimientos Operativos Estándar (SOPs) según Modo de Falla Multiclase y SHAP XAI

Cuando AWS Lambda publica una notificación en **Amazon SNS**, el payload incluye el campo `predicted_failure_mode` y los impulsores **SHAP** dominantes. El equipo de mantenimiento en planta debe ejecutar la lista de verificación correspondiente:

---

#### 🔧 Playbook A: `BEARING_VIBRATION_WEAR` *(Desgaste Mecánico de Rodamientos / Eje)*
* **Precisión Diagnóstica del Modelo:** $72.9\%$ (con $99.9\%$ de especificidad frente a otros modos).
* **Indicadores Físicos y SHAP Asociados:** Impulsado por `vib_pressure_ratio` ($|\text{SHAP}| = 0.932$), `vib_lag_1h` ($|\text{SHAP}| = 0.702$), factor de cresta $C_V > 3.5$ y armónicos espectrales FFT en **BPFO = 120 Hz y 240 Hz**.
* **Protocolo de Actuación en Campo (SOP-MECH-01):**
  1. **Verificación Acústica y Vibratoria Portátil:** Medir velocidad RMS en chumacera delantera y trasera para confirmar lectura telemétrica ($> 3.5\text{ mm/s}$).
  2. **Inspección de Lubricación:** Verificar degradación térmica del lubricante o contaminación por partículas metálicas en el colector magnético.
  3. **Corrección Mecánica en Ventana de Turno ($< +7\text{h}$):** Ejecutar relubricación de alta viscosidad; si persiste el armónico de 120 Hz (pista exterior dañada), **reemplazar el cartucho del rodamiento** y verificar alineación láser del eje ($< 0.05\text{ mm}$).

---

#### 🔥 Playbook B: `OVERHEAT_FAULT` *(Fallo Termodinámico / Sistema de Enfriamiento)*
* **Precisión Diagnóstica del Modelo:** $70.8\%$ en ventana de 12h ($> 95\%$ en las últimas 4 horas previas al fallo).
* **Indicadores Físicos y SHAP Asociados:** Impulsado por `temperature` ($> 68.5^\circ\text{C}$ mediana pre-falla), `thermo_vib_interaction` ($|\text{SHAP}| = 0.301$), derivada térmica positiva $\frac{dT}{dt} > +2.5^\circ\text{C/h}$ y aumento de potencia consumida (`power_kw`).
* **Protocolo de Actuación en Campo (SOP-THERM-02):**
  1. **Mitigación Inmediata (*Load Derating*):** Reducir la carga operativa de la turbina en un **15% a 20%** desde sala de control para frenar la pendiente cuadrática de calentamiento.
  2. **Inspección Termográfica Infrarroja:** Localizar el punto caliente (carcasa del estator vs. cojinete de empuje).
  3. **Intervención en Circuito de Refrigeración:** Verificar caudal y diferencial de presión del intercambiador de calor, purgar aire atrapado en serpentines y limpiar filtros obstruidos.

---

#### 💨 Playbook C: `PRESSURE_VALVE_DROP` *(Despresurización / Fuga en Válvula de Alivio o Sellos)*
* **Precisión Diagnóstica del Modelo:** **$87.5\%$** (el modo de falla con mayor separabilidad temprana).
* **Indicadores Físicos y SHAP Asociados:** Impulsado por caída asimétrica en `pressure` ($\text{Skewness} = -1.67$, cayendo de $102.0\text{ kPa}$ hacia $< 92.0\text{ kPa}$), `press_roll_avg_24h` ($|\text{SHAP}| = 0.308$) y aumento del ratio `vib_pressure_ratio` por cavitación incipiente.
* **Protocolo de Actuación en Campo (SOP-PNEUM-03):**
  1. **Detección Ultrasónica de Fugas:** Inspeccionar el colector principal, sellos mecánicos y válvula solenoide de alivio.
  2. **Conmutación a Línea Redundante (Bypass):** Si la presión cae por debajo del umbral de advertencia ($88.0\text{ kPa}$), activar el circuito neumático/hidráulico secundario sin detener la turbina.
  3. **Reemplazo de Sellos y Calibración:** Sustituir empaquetadura o resorte fatigado de la válvula reguladora y recalibrar presión nominal en $102.0 \pm 1.5\text{ kPa}$.

---

## 🔄 PARTE III: FLUJO DE AUTOMATIZACIÓN Y CICLO CERRADO MLOPS (CONTINUOUS LEARNING)

Para garantizar que el plan de mantenimiento no se degrade con el paso de los meses (*Concept Drift* o cambio de repuestos), la plataforma implementa el siguiente ciclo cerrado de retroalimentación:

```
 [1. DETECCIÓN & PREDICCIÓN]           [2. DESPACHO IDEMPOTENTE]             [3. EJECUCIÓN EN PLANTA]
  Flink ONNX + Athena/XGBoost  ──────►  SQS FIFO -> Lambda -> DynamoDB  ────►  Orden de Trabajo (SOP A/B/C)
  (P >= tau*, Modo Multiclase)          (Token SHA-256, TTL 15 min)            Intervención en < +7h ($500)
               ▲                                                                            │
               │                       [4. FEEDBACK LOOP & REENTRENAMIENTO]                 │
               └──────────────────────  Cierre de OT en DynamoDB / S3 Parquet  ◄────────────┘
                                        (Etiqueta Real Validada por Ingeniero + Monitoreo PSI < 0.10)
```

1. **Auditoría de Cierre de Orden de Trabajo (Ground-Truth Verification):**
   * Al concluir la inspección en planta, el ingeniero confirma en el sistema si el diagnóstico multiclase (`OVERHEAT_FAULT`, `BEARING_VIBRATION_WEAR`, `PRESSURE_VALVE_DROP`) fue exacto o si fue una falsa alarma.
   * Este registro actualiza la tabla de incidentes en **Amazon S3 / DynamoDB**, convirtiéndose automáticamente en nueva evidencia etiquetada para la vista `ATHENA_FEATURE_STORE_SQL`.
2. **Monitoreo de Estabilidad Poblacional (Data Drift PSI):**
   * Semanalmente se evalúa el **Population Stability Index (PSI)** sobre las 5 variables SHAP principales (`vib_pressure_ratio`, `vib_lag_1h`, `vibration`, `pressure`, `temperature`):
   * Si $\mathrm{PSI} < 0.10$: El modelo continúa en producción sin cambios.
   * Si $0.10 \le \mathrm{PSI} < 0.25$: Se emite alerta de monitoreo intensivo y recalibración de umbral $\tau^*$.
   * Si $\mathrm{PSI} \ge 0.25$: Se dispara automáticamente el pipeline de **Reentrenamiento Continuo (CT)** ejecutando [`scripts/iot_predictive_maintenance_suite.py`](../scripts/iot_predictive_maintenance_suite.py) y actualizando el artefacto ONNX/Joblib.
