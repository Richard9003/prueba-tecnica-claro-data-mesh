# Databricks notebook source
# MAGIC %md
# MAGIC # 06. Gold — Cliente 360 (Churn + Upsell)
# MAGIC
# MAGIC **Caso:** Cliente 360 — Riesgo de Churn y Oportunidad de Upsell  
# MAGIC **Alcance:** P3 (modelo Gold), P4 (reglas de negocio), P5 (consulta SQL), P6 (optimizaci ón), P7 (data contract).
# MAGIC
# MAGIC **Columnas requeridas (P3):**
# MAGIC - `id_cliente`, `segmento`, `ciudad`
# MAGIC - `producto_actual`
# MAGIC - `consumo_promedio_gb`
# MAGIC - `total_incidencias_red`
# MAGIC - `total_pqr`, `pqr_abiertos`
# MAGIC - `satisfaccion_promedio`
# MAGIC
# MAGIC **Reglas de negocio (P4):**
# MAGIC - `churn_risk`: Alto / Medio / Bajo
# MAGIC - `upsell_flag`: True / False

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO = "claro_postpago"

CLIENTES_SILVER = f"{CATALOGO}.l2_curated.dim_cliente_silver"
PRODUCTOS_SILVER = f"{CATALOGO}.l2_curated.dim_producto_silver"
USO_SILVER = f"{CATALOGO}.l2_curated.fact_uso_servicio_silver"
PQR_SILVER = f"{CATALOGO}.l2_curated.fact_pqr_silver"

GOLD = f"{CATALOGO}.l3_certified.cliente_360_churn_upsell"

GOLD_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fecha de corte (reproducible)
# MAGIC
# MAGIC Usamos la máxima fecha de apertura en PQR como referencia para " últimos 4 meses".

# COMMAND ----------

df_pqr = spark.table(PQR_SILVER)

fecha_corte_row = df_pqr.agg(F.max("fecha_apertura").alias("fecha_corte")).first()
FECHA_CORTE = fecha_corte_row["fecha_corte"]

print(f"Fecha de corte para últimos 4 meses: {FECHA_CORTE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agregados de uso por cliente
# MAGIC
# MAGIC - `consumo_promedio_gb`: promedio de `consumo_datos_gb`.
# MAGIC - `total_incidencias_red`: suma de `incidencias_red`.
# MAGIC - `promedio_incidencias_red`: promedio de `incidencias_red` (para churn_risk).
# MAGIC - `ultimo_periodo_uso`: máximo periodo (para producto actual).

# COMMAND ----------

df_uso = spark.table(USO_SILVER)

uso_por_cliente = (
    df_uso
    .groupBy("id_cliente")
    .agg(
        F.avg("consumo_datos_gb").alias("consumo_promedio_gb"),
        F.sum("incidencias_red").alias("total_incidencias_red"),
        F.avg("incidencias_red").alias("promedio_incidencias_red"),
        F.max("periodo").alias("ultimo_periodo_uso"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Producto actual por cliente
# MAGIC
# MAGIC Tomamos el producto asociado al último periodo de uso válido.
# MAGIC Si hay múltiples productos en el mismo periodo, tomamos el primero por `id_producto`.

# COMMAND ----------

ventana_producto = Window.partitionBy("id_cliente").orderBy(F.desc("periodo"), F.asc("id_producto"))

producto_actual_por_cliente = (
    df_uso
    .join(
        spark.table(PRODUCTOS_SILVER).select(
            "id_producto",
            F.col("nombre_producto").alias("producto_actual"),
            "capacidad_gb",
        ),
        "id_producto",
        "left",
    )
    .withColumn("rn", F.row_number().over(ventana_producto))
    .filter(F.col("rn") == 1)
    .select("id_cliente", "producto_actual", "capacidad_gb")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agregados de PQR por cliente
# MAGIC
# MAGIC - `total_pqr`: total de casos.
# MAGIC - `pqr_abiertos`: casos con `estado_caso = 'Abierto'`.
# MAGIC - `satisfaccion_promedio`: promedio de `satisfaccion_1_5`.
# MAGIC - `pqr_queja_reclamo_ult4m`: PQR de tipo Queja/Reclamo en últimos 4 meses.

# COMMAND ----------

from pyspark.sql.functions import expr

pqr_por_cliente = (
    df_pqr
    .groupBy("id_cliente")
    .agg(
        F.count("*").alias("total_pqr"),
        F.sum(F.when(F.col("estado_caso") == "Abierto", 1).otherwise(0)).alias("pqr_abiertos"),
        F.avg("satisfaccion_1_5").alias("satisfaccion_promedio"),
        F.sum(
            F.when(
                (F.col("tipo_caso").isin("Queja", "Reclamo")) &
                (F.col("fecha_apertura") >= expr(f"add_months('{FECHA_CORTE}', -4)")),
                1
            ).otherwise(0)
        ).alias("pqr_queja_reclamo_ult4m"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Construir Gold (solo clientes activos)
# MAGIC
# MAGIC Columnas mínimas requeridas (P3):
# MAGIC - `id_cliente`, `segmento`, `ciudad`
# MAGIC - `producto_actual`
# MAGIC - `consumo_promedio_gb`
# MAGIC - `total_incidencias_red`
# MAGIC - `total_pqr`, `pqr_abiertos`
# MAGIC - `satisfaccion_promedio`

# COMMAND ----------

df_clientes = spark.table(CLIENTES_SILVER).filter(F.col("estado_cliente") == "Activo")

gold_cliente_360 = (
    df_clientes
    .select("id_cliente", "segmento", "ciudad")
    .join(producto_actual_por_cliente, "id_cliente", "left")
    .join(uso_por_cliente, "id_cliente", "left")
    .join(pqr_por_cliente, "id_cliente", "left")
    .fillna(0, subset=["total_incidencias_red", "total_pqr", "pqr_abiertos", "pqr_queja_reclamo_ult4m"])
    .fillna(0.0, subset=["consumo_promedio_gb", "promedio_incidencias_red", "satisfaccion_promedio"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reglas de negocio: churn_risk y upsell_flag (P4)
# MAGIC
# MAGIC **churn_risk:**
# MAGIC - **Alto**: `pqr_queja_reclamo_ult4m >= 2` **o** `promedio_incidencias_red >= 3`
# MAGIC - **Medio**: `pqr_queja_reclamo_ult4m == 1` **o** `promedio_incidencias_red` entre 1 y 3
# MAGIC - **Bajo**: otro caso
# MAGIC
# MAGIC **upsell_flag:**
# MAGIC - **True**: `consumo_promedio_gb > capacidad_gb * 1.80`
# MAGIC - **False**: otro caso (incluye `capacidad_gb` NULL)

# COMMAND ----------

gold_con_reglas = (
    gold_cliente_360
    .withColumn(
        "churn_risk",
        F.when(
            (F.col("pqr_queja_reclamo_ult4m") >= 2) | (F.col("promedio_incidencias_red") >= 3),
            "Alto"
        ).when(
            (F.col("pqr_queja_reclamo_ult4m") == 1) |
            ((F.col("promedio_incidencias_red") >= 1) & (F.col("promedio_incidencias_red") < 3)),
            "Medio"
        ).otherwise("Bajo"),
    )
    .withColumn(
        "upsell_flag",
        F.when(
            (F.col("capacidad_gb").isNotNull()) &
            (F.col("consumo_promedio_gb") > F.col("capacidad_gb") * F.lit(1.80)),
            True
        ).otherwise(False),
    )
    .withColumn("fecha_actualizacion", F.current_timestamp())
    .withColumn("_gold_run_id", F.lit(GOLD_RUN_ID))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Guardar tabla Gold

# COMMAND ----------

(
    gold_con_reglas.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD)
)

print(f"Gold listo | tabla={GOLD} | registros={gold_con_reglas.count()} | run_id={GOLD_RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validaciones de salida Gold
# MAGIC
# MAGIC - `id_cliente` no nulo y único.
# MAGIC - `churn_risk` en {Alto, Medio, Bajo}.
# MAGIC - `consumo_promedio_gb` no negativo.
# MAGIC - `satisfaccion_promedio` entre 1 y 5 (si no es NULL).

# COMMAND ----------

df_gold = spark.table(GOLD)

id_cliente_nulo = df_gold.filter(
    F.col("id_cliente").isNull() | (F.trim(F.col("id_cliente")) == "")
)

id_cliente_duplicado = (
    df_gold
    .groupBy("id_cliente")
    .count()
    .filter(F.col("count") > 1)
)

churn_risk_invalido = df_gold.filter(
    ~F.col("churn_risk").isin("Alto", "Medio", "Bajo")
)

consumo_negativo = df_gold.filter(F.col("consumo_promedio_gb") < 0)

satisfaccion_invalida = df_gold.filter(
    (F.col("satisfaccion_promedio").isNotNull()) &
    (~F.col("satisfaccion_promedio").between(1, 5))
)

print(f"Validaci ón | id_cliente nulo={id_cliente_nulo.count()}")
print(f"Validaci ón | id_cliente duplicado={id_cliente_duplicado.count()}")
print(f"Validaci ón | churn_risk inv álido={churn_risk_invalido.count()}")
print(f"Validaci ón | consumo negativo={consumo_negativo.count()}")
print(f"Validaci ón | satisfacci ón fuera de 1-5={satisfaccion_invalida.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Muestra de resultados

# COMMAND ----------

display(
    df_gold
    .select(
        "id_cliente",
        "segmento",
        "ciudad",
        "producto_actual",
        "consumo_promedio_gb",
        "total_incidencias_red",
        "promedio_incidencias_red",
        "total_pqr",
        "pqr_abiertos",
        "satisfaccion_promedio",
        "churn_risk",
        "upsell_flag",
    )
    .orderBy(F.desc("churn_risk"), F.asc("id_cliente"))
    .limit(20)
)