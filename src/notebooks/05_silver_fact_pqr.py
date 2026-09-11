# Databricks notebook source
# MAGIC %md
# MAGIC # 05. Silver — PQR

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
BRONZE = f"{CATALOGO}.l1_raw.fact_pqr_bronze"
SILVER = f"{CATALOGO}.l2_curated.fact_pqr_silver"
SILVER_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# COMMAND ----------

def limpiar_texto(columna):
    return F.initcap(F.regexp_replace(F.trim(F.col(columna)), r"\s+", " "))

fecha_apertura_estandar = F.coalesce(
    F.expr("try_to_date(fecha_apertura, 'yyyy-MM-dd')"),
    F.expr("try_to_date(fecha_apertura, 'dd/MM/yyyy')"),
)

# COMMAND ----------

df_bronze = spark.table(BRONZE)

df_pqr_silver = (
    df_bronze
    .select(
        F.upper(F.trim(F.col("id_caso"))).alias("id_caso"),
        F.upper(F.trim(F.col("id_cliente"))).alias("id_cliente"),
        limpiar_texto("tipo_caso").alias("tipo_caso"),
        limpiar_texto("motivo").alias("motivo"),
        fecha_apertura_estandar.alias("fecha_apertura"),
        limpiar_texto("canal").alias("canal"),
        F.expr("try_cast(dias_resolucion AS INT)").alias("dias_resolucion"),
        F.expr("try_cast(satisfaccion_1_5 AS INT)").alias("satisfaccion_1_5"),
        limpiar_texto("estado_caso").alias("estado_caso"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_pipeline_run_id"),
        F.col("_record_hash"),
        F.lit(SILVER_RUN_ID).alias("_silver_run_id"),
        F.current_timestamp().alias("_silver_timestamp"),
    )
    .dropDuplicates(["id_caso"])
)

# COMMAND ----------

(
    df_pqr_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

print(f"Silver listo | tabla={SILVER} | registros={df_pqr_silver.count()}")

# COMMAND ----------

fechas_nulas = spark.table(SILVER).filter(F.col("fecha_apertura").isNull())

duplicados = (
    spark.table(SILVER)
    .groupBy("id_caso")
    .count()
    .filter(F.col("count") > 1)
)

print(f"Validación | fecha_apertura nula={fechas_nulas.count()}")
print(f"Validación | id_caso duplicado={duplicados.count()}")