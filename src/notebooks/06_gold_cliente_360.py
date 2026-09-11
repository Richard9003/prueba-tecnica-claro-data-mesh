# Databricks notebook source
# MAGIC %md
# MAGIC # 06. Gold — Cliente 360 (Churn + Upsell)
# MAGIC
# MAGIC **Propósito:** Construir y publicar el producto de datos certificado
# MAGIC `cliente_360_churn_upsell`.
# MAGIC
# MAGIC **Alcance:** P3 (modelo Gold) y P4 (reglas de negocio).
# MAGIC
# MAGIC **Grano:** Un registro por cliente activo.
# MAGIC
# MAGIC Uso y PQR se agregan por separado antes del join con clientes. Esto evita
# MAGIC multiplicación de filas y distorsión de conteos, promedios y reglas de riesgo.

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
# MAGIC ## 1. Fecha de corte reproducible
# MAGIC
# MAGIC La ventana de PQR se calcula desde la máxima fecha disponible en Silver,
# MAGIC no desde el reloj del sistema. Así el resultado es reproducible al reprocesar.

# COMMAND ----------

df_pqr = spark.table(PQR_SILVER)
df_uso = spark.table(USO_SILVER)
df_productos = spark.table(PRODUCTOS_SILVER)

fecha_corte = (
    df_pqr
    .agg(F.max("fecha_apertura").alias("fecha_corte"))
    .first()["fecha_corte"]
)

if fecha_corte is None:
    raise ValueError("No es posible construir Gold: no existe fecha_apertura válida en PQR Silver.")

fecha_inicio_4_meses = (
    spark.range(1)
    .select(F.add_months(F.lit(fecha_corte), -4).alias("fecha_inicio"))
    .first()["fecha_inicio"]
)

print(f"Fecha de corte PQR: {fecha_corte}")
print(f"Inicio ventana últimos 4 meses: {fecha_inicio_4_meses}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agregados de uso por cliente

# COMMAND ----------

uso_por_cliente = (
    df_uso
    .groupBy("id_cliente")
    .agg(
        F.avg("consumo_datos_gb").alias("consumo_promedio_gb"),
        F.sum("incidencias_red").cast("long").alias("total_incidencias_red"),
        F.avg("incidencias_red").alias("promedio_incidencias_red"),
        F.max("periodo").alias("ultimo_periodo_uso"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Producto actual por cliente
# MAGIC
# MAGIC Se toma el producto asociado al último periodo de uso válido. Si existen varios
# MAGIC productos para un cliente en el mismo periodo, `id_producto` define un desempate
# MAGIC determinista para preservar reproducibilidad.

# COMMAND ----------

ventana_producto_actual = (
    Window
    .partitionBy("id_cliente")
    .orderBy(F.desc("periodo"), F.asc("id_producto"))
)

producto_actual_por_cliente = (
    df_uso
    .join(
        df_productos.select(
            "id_producto",
            F.col("nombre_producto").alias("producto_actual"),
            "capacidad_gb",
        ),
        "id_producto",
        "left",
    )
    .withColumn("_rn_producto_actual", F.row_number().over(ventana_producto_actual))
    .filter(F.col("_rn_producto_actual") == 1)
    .select("id_cliente", "producto_actual", "capacidad_gb")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agregados de PQR por cliente

# COMMAND ----------

pqr_por_cliente = (
    df_pqr
    .groupBy("id_cliente")
    .agg(
        F.count("*").cast("long").alias("total_pqr"),
        F.sum(
            F.when(F.col("estado_caso") == "Abierto", 1).otherwise(0)
        ).cast("long").alias("pqr_abiertos"),
        F.avg("satisfaccion_1_5").alias("satisfaccion_promedio"),
        F.sum(
            F.when(
                F.col("tipo_caso").isin("Queja", "Reclamo")
                & (F.col("fecha_apertura") >= F.lit(fecha_inicio_4_meses)),
                1,
            ).otherwise(0)
        ).cast("long").alias("pqr_queja_reclamo_ult4m"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Modelo Gold y reglas de negocio
# MAGIC
# MAGIC **Churn Alto:** >=2 PQR Queja/Reclamo en últimos 4 meses O promedio de incidencias >=3.
# MAGIC
# MAGIC **Churn Medio:** 1 PQR Queja/Reclamo en últimos 4 meses O promedio de incidencias >=1 y <3.
# MAGIC
# MAGIC **Upsell:** consumo promedio > 1.80 × capacidad del plan.

# COMMAND ----------

df_clientes_activos = (
    spark.table(CLIENTES_SILVER)
    .filter(F.col("estado_cliente") == "Activo")
    .select("id_cliente", "segmento", "ciudad")
)

df_gold_previo = (
    df_clientes_activos
    .join(producto_actual_por_cliente, "id_cliente", "left")
    .join(uso_por_cliente, "id_cliente", "left")
    .join(pqr_por_cliente, "id_cliente", "left")
    .fillna(
        0,
        subset=[
            "total_incidencias_red",
            "total_pqr",
            "pqr_abiertos",
            "pqr_queja_reclamo_ult4m",
        ],
    )
    .fillna(
        0.0,
        subset=[
            "consumo_promedio_gb",
            "promedio_incidencias_red",
        ],
    )
)

df_gold = (
    df_gold_previo
    .withColumn(
        "churn_risk",
        F.when(
            (F.col("pqr_queja_reclamo_ult4m") >= 2)
            | (F.col("promedio_incidencias_red") >= 3),
            F.lit("Alto"),
        )
        .when(
            (F.col("pqr_queja_reclamo_ult4m") == 1)
            | (
                (F.col("promedio_incidencias_red") >= 1)
                & (F.col("promedio_incidencias_red") < 3)
            ),
            F.lit("Medio"),
        )
        .otherwise(F.lit("Bajo")),
    )
    .withColumn(
        "upsell_flag",
        F.when(
            F.col("capacidad_gb").isNotNull()
            & (F.col("consumo_promedio_gb") > F.col("capacidad_gb") * F.lit(1.80)),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
    .withColumn("fecha_actualizacion", F.current_timestamp())
    .withColumn("_gold_run_id", F.lit(GOLD_RUN_ID))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quality Gate previo a certificación
# MAGIC
# MAGIC Si falla una regla crítica, Gold no se publica. La satisfacción puede ser NULL
# MAGIC para clientes sin PQR; no se reemplaza por 0 porque 0 no pertenece a la escala 1-5.

# COMMAND ----------

validaciones_criticas = {
    "gold_vacio": df_gold.limit(1).count() == 0,
    "id_cliente_nulo": df_gold.filter(
        F.col("id_cliente").isNull() | (F.trim(F.col("id_cliente")) == "")
    ).limit(1).count() > 0,
    "id_cliente_duplicado": (
        df_gold.groupBy("id_cliente")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count() > 0
    ),
    "churn_risk_invalido": df_gold.filter(
        ~F.col("churn_risk").isin("Alto", "Medio", "Bajo")
    ).limit(1).count() > 0,
    "consumo_promedio_negativo": df_gold.filter(
        F.col("consumo_promedio_gb") < 0
    ).limit(1).count() > 0,
    "pqr_abiertos_mayor_total": df_gold.filter(
        F.col("pqr_abiertos") > F.col("total_pqr")
    ).limit(1).count() > 0,
}

fallos_criticos = [
    regla
    for regla, fallo in validaciones_criticas.items()
    if fallo
]

if fallos_criticos:
    raise ValueError(
        "Quality Gate falló. Gold no se publica. "
        f"Reglas incumplidas: {', '.join(fallos_criticos)}"
    )

print("Quality Gate aprobado | todas las reglas críticas cumplen.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Publicación de Gold

# COMMAND ----------

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD)
)

print(
    f"Gold certificado | tabla={GOLD} | "
    f"registros={df_gold.count()} | run_id={GOLD_RUN_ID}"
)