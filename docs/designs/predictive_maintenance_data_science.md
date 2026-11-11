# 🧠 Especificación de Arquitectura de Data Science: Mantenimiento Predictivo (PdM) & Inferencia Dual

**Proyecto:** Odysseus AI Platform & Predictive MLOps Framework  
**Lead Architect:** [Guillen Concepcion](https://www.linkedin.com/in/guillen-concepcion-25266b127) *(Senior Data Scientist & MLOps Engineer)*  
**Repositorio GitHub:** [streaming-flink-kinesis-pipeline](https://github.com/GuillenConcepcion/streaming-flink-kinesis-pipeline)  
**Dashboard de Arquitectura:** [https://guillenconcepcion.github.io/streaming-flink-kinesis-pipeline/](https://guillenconcepcion.github.io/streaming-flink-kinesis-pipeline/)  

---

## 🧭 1. Definición del Problema de Negocio y Enfoque Metodológico (CoNVO)

### Contexto
El pipeline de streaming actual ingesta telemetría de alta frecuencia de turbinas y maquinaria rotativa en **AWS Kinesis**, agregando métricas en ventanas deslizantes con **Apache Flink 1.18** y persistiendo particiones transaccionales en formato **Apache Parquet en Amazon S3** catalogadas mediante **AWS Glue con Partition Projection**. Un motor reactivo de **Complex Event Processing (CEP)** detecta anomalías cuando ya han ocurrido (temperatura $> 85^\circ\text{C}$ y vibración $> 6\text{ mm/s}$).

### Necesidad de Negocio
En operaciones industriales críticas, la detección reactiva llega tarde: cuando se detecta un patrón de fatiga mecánica en tiempo real, el rodamiento ya ha sufrido deformación plástica o daño irreversible, provocando paradas no programadas (*Unplanned Downtime*), costos de reparación de hasta \$50,000 USD por turbina y riesgos de seguridad física.

### Visión
Evolucionar la plataforma de un paradigma **reactivo en tiempo real** a un paradigma **predictivo proactivo**, utilizando el histórico acumulado en Amazon S3 a través de **Amazon Athena / DuckDB** para entrenar modelos de Machine Learning capaces de **predecir fallas con $H = 6\text{ a }12\text{ horas}$ de antelación**.

### Resultado Operativo
Reducción del 75% en paradas imprevistas mediante el despacho de órdenes de mantenimiento preventivo dirigidas, maximizando el *Mean Time Between Failures* (MTBF) y optimizando el *Mean Time To Repair* (MTTR).

---

## 📐 2. Formulación Matemática del Problema de Data Science

```
                          VENTANA DE OBSERVACIÓN              BUFFER LATENCIA       HORIZONTE DE PREDICCIÓN
                         [ W_obs = 24 horas ]                   [ L = 30m ]              [ H = 6-12 horas ]
───────────────────────────────────────────────────────────────┼─────────────┼─────────────────────────────────────► Tiempo
  t - 24h                      t - 6h                     t         t + 30m                      t + 30m + H
                                                          ▲
                                                          │
                                                    Momento de Inferencia
                                                    ¿P(Falla en [t+L, t+L+H]) >= tau*?
```

### 2.1. Definición de la Variable Objetivo Causal (Target $Y$)
Para un activo industrial $i$ en el instante $t$, la variable objetivo binaria $Y_{i, t} \in \{0, 1\}$ se define formalmente como:

$$
Y_{i, t} = \begin{cases} 
1 & \text{si } \exists \, t_{\text{falla}} \in [t + L, \; t + L + H] \text{ tal que el activo } i \text{ experimenta falla} \\ 
0 & \text{en otro caso (operación nominal continua)} 
\end{cases}
$$

Donde:
* $W_{\text{obs}} = 24\text{ horas}$: Ventana histórica utilizada para el cálculo de tendencias, promedios móviles, derivadas y factores de cresta.
* $L = 30\text{ minutos}$: Período de gracia o latencia operativa (tiempo requerido por el equipo técnico para reaccionar ante una alerta).
* $H = 6\text{ a }12\text{ horas}$: Horizonte de predicción útil (*Prediction Window*).
* Si $t_{\text{falla}} < t + L$, el evento no se etiqueta como falla prevenible (es falla reactiva).

---

### 2.2. Matriz de Decisión Sensible al Costo (*Cost-Sensitive Framework*)
En telemetría de activos de alto valor, los errores de clasificación tienen consecuencias asimétricas:

| Predicción / Realidad | Falla Real ($Y = 1$) | Operación Normal ($Y = 0$) |
| :--- | :--- | :--- |
| **Alerta Positiva ($\hat{Y} = 1$)** | **Verdadero Positivo (TP):** Costo de inspección programada ($C_{\text{TP}} = \$500$). Beneficio: Ahorro de \$49,500. | **Falso Positivo (FP):** Costo de inspección innecesaria / tiempo de técnico ($C_{\text{FP}} = \$500$). |
| **Alerta Negativa ($\hat{Y} = 0$)** | **Falso Negativo (FN):** Rotura catastrófica de turbina, parada de línea de producción y daño colateral ($C_{\text{FN}} = \$50,000$). | **Verdadero Negativo (TN):** Operación nominal sin costo adicional ($C_{\text{TN}} = \$0$). |

La función de costo esperado en función del umbral de decisión $\tau \in [0, 1]$ resulta:

$$
\mathbb{E}[\text{Costo}(\tau)] = C_{\text{FP}} \cdot P(\hat{Y}=1, Y=0 \mid \tau) + C_{\text{FN}} \cdot P(\hat{Y}=0, Y=1 \mid \tau)
$$

El umbral óptimo de negocio $\tau^*$ se determina analíticamente minimizando el costo esperado:

$$
\tau^* = \arg\min_{\tau \in [0, 1]} \mathbb{E}[\text{Costo}(\tau)] \approx \frac{C_{\text{FP}}}{C_{\text{FP}} + C_{\text{FN}}} = \frac{500}{500 + 50000} \approx 0.0099
$$

> [!IMPORTANT]
> Debido a que $C_{\text{FN}} \gg C_{\text{FP}}$ ($100:1$), el umbral estándar de clasificación $\tau = 0.50$ es inadecuado. El modelo debe calibrarse con un umbral sensible al costo para maximizar el **Recall** sin que la tasa de falsas alarmas sobrepase la capacidad del equipo de mantenimiento.

---

## 🗄️ 3. El Feature Store Analítico en Amazon Athena & S3

Aprovechando que Flink ya persiste ventanas de 1 minuto en `s3://...-curated/telemetry/` con **AWS Glue Partition Projection**, construimos la matriz de características analítica mediante consultas vectorizadas en **Amazon Athena / DuckDB**.

```sql
-- Pipeline de Extracción de Features Históricas en Amazon Athena
WITH raw_windows AS (
    SELECT 
        sensor_id,
        from_unixtime(window_start / 1000) AS window_time,
        avg_temperature,
        max_temperature,
        avg_vibration,
        max_vibration,
        avg_pressure,
        avg_power,
        sample_count
    FROM odysseus_streaming_db.curated_sensor_metrics
    WHERE year = '2026' AND month = '11'
),
rolling_features AS (
    SELECT 
        sensor_id,
        window_time,
        avg_temperature,
        avg_vibration,
        avg_power,
        -- Medias móviles históricas (1 hora y 6 horas)
        AVG(avg_temperature) OVER (
            PARTITION BY sensor_id ORDER BY window_time 
            ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
        ) AS temp_rolling_mean_1h,
        STDDEV(avg_temperature) OVER (
            PARTITION BY sensor_id ORDER BY window_time 
            ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
        ) AS temp_rolling_std_1h,
        AVG(avg_vibration) OVER (
            PARTITION BY sensor_id ORDER BY window_time 
            ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
        ) AS vib_rolling_mean_1h,
        STDDEV(avg_vibration) OVER (
            PARTITION BY sensor_id ORDER BY window_time 
            ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
        ) AS vib_rolling_std_1h,
        
        -- Lags temporales (1 hora y 6 horas atrás)
        LAG(avg_temperature, 6) OVER (PARTITION BY sensor_id ORDER BY window_time) AS temp_lag_1h,
        LAG(avg_vibration, 6)   OVER (PARTITION BY sensor_id ORDER BY window_time) AS vib_lag_1h,
        LAG(avg_temperature, 36) OVER (PARTITION BY sensor_id ORDER BY window_time) AS temp_lag_6h,
        
        -- Ratios de ingeniería electro-mecánica
        (max_vibration / NULLIF(avg_vibration, 0)) AS vibration_crest_factor,
        (avg_power / NULLIF(avg_temperature, 0))   AS electromechanical_ratio
    FROM raw_windows
)
SELECT 
    *,
    -- Derivadas temporales (Tasa de calentamiento y tasa de aceleración vibratoria)
    (avg_temperature - temp_lag_1h) / 60.0 AS d_temp_dt,
    (avg_vibration - vib_lag_1h)     / 60.0 AS d_vib_dt
FROM rolling_features;
```

---

## 🤖 4. Modelado Predictivo y Protocolo de Validación

### 4.1. Prevención Absoluta de Data Leakage (Partición Causal)
* **Regla Canónica:** En series temporales de sensores, está estrictamente prohibido usar validación cruzada aleatoria (`KFold` estándar).
* **Estrategia:** Se implementa **Purged Group Time-Series Split**:
  1. Partición temporal hacia adelante (*Forward-Chain*): El conjunto de entrenamiento contiene exclusivamente datos hasta el tiempo $T_{\text{split}}$, y el conjunto de prueba datos posteriores ($T > T_{\text{split}} + L$).
  2. Zona de purga (*Purge Window*): Se eliminan las muestras comprendidas en $[T_{\text{split}} - H, \; T_{\text{split}}]$ para evitar que los labels $Y$ de entrenamiento utilicen información del horizonte del conjunto de test.

### 4.2. Algoritmos de Machine Learning Evaluados
1. **LightGBM / XGBoost:** Modelos primarios para features tabulares y ratios físicos, con ponderación de balanceo (`scale_pos_weight`) y regularización $L_1/L_2$.
2. **Random Forest:** Modelo de ensamble robusto para medir interacciones de variables.
3. **Calibración PAVA / Isotonic:** Dado que los gradient boosted trees producen probabilidades descalibradas en los extremos, se aplica regresión isotónica sobre el conjunto de calibración para garantizar que $\hat{P} = 0.70$ represente verdaderamente un 70% de frecuencia observada.
4. **Explicabilidad Local (SHAP TreeExplainer):**
   $$f(x) = \phi_0 + \sum_{j=1}^M \phi_j(x)$$
   Cada alarma emitida hacia el ingeniero de planta incluye la descomposición de los 3 factores causales principales (ej. *"62% debido al Factor de Cresta Vibracional $C_V$, 28% a la tasa de calentamiento $\frac{dT}{dt}$"*).

---

## ⚡ 5. Arquitectura de Despliegue Dual (Cerrando el Ciclo MLOps)

Para maximizar el impacto operativo, se establecen dos vías de inferencia según la latencia requerida:

```
                                  ARQUITECTURA DE DESPLIEGUE DUAL
                                 
              +--------------------------------------------------------------+
              |           AWS Kinesis Telemetry Ingestion (1,000+ QPS)       |
              +--------------------------------------------------------------+
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
     ┌───────────────────────────────────┐           ┌───────────────────────────────────┐
     │ CAMINO A: STREAMING EMBEDDED ONNX │           │ CAMINO B: SERVERLESS MICRO-BATCH  │
     │      (Latencia: < 2 ms)           │           │      (Latencia: 50 - 250 ms)      │
     ├───────────────────────────────────┤           ├───────────────────────────────────┤
     │ • Flink RichMapFunction           │           │ • SQS FIFO -> AWS Lambda Alerter  │
     │ • ONNX Runtime Java embebido      │           │ • Modelo ML cargado en contenedor │
     │ • Evaluación registro a registro  │           │ • Scoring probabilístico P(Falla) │
     │ • Alerta inmediata en ventana     │           │ • Idempotencia atómica DynamoDB   │
     └───────────────────────────────────┘           └───────────────────────────────────┘
                      │                                               │
                      └───────────────────────┬───────────────────────┘
                                              ▼
                             +---------------------------------+
                             |  Amazon SNS + DynamoDB Audit    |
                             |  Alertas de Mantenimiento PdM   |
                             +---------------------------------+
```

### Camino A: Inferencia en Streaming con ONNX Runtime (Baja Latencia)
* El modelo entrenado en Python se exporta a formato abierto **ONNX** (`model.onnx`).
* Se integra en el pipeline de Apache Flink mediante una función enriquecida (`RichMapFunction` o `AsyncDataStream`) utilizando la librería oficial `onnxruntime` para Java.
* **Ventaja:** Cada evento de ventana es clasificado en menos de $2\text{ milisegundos}$ en memoria sin dependencias de red.

### Camino B: Inferencia Serverless Micro-batch en AWS Lambda (Desacoplado)
* Las alertas o vectores de métricas se enrutan de forma asíncrona hacia **Amazon SQS FIFO**.
* Una función **AWS Lambda** empaquetada como imagen de contenedor (o cargando el artefacto desde S3) ejecuta la inferencia estadística, calcula el `failure_probability_score`, evalúa el umbral $\tau^*$ y registra el diagnóstico con idempotencia en DynamoDB antes de notificar a SNS.
* **Ventaja:** Cero impacto en el heap y memoria del clúster Flink, autoescalado serverless elástico y desacoplamiento total.
