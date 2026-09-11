# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Silver — Dimensión Producto

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
BRONZE = f"{CATALOGO}.l1_raw.dim_producto_bronze"
SILVER = f"{CATALOGO}.l2_curated.dim_producto_silver"
SILVER_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# COMMAND ----------

def limpiar_texto(columna):
    return F.initcap(F.regexp_replace(F.trim(F.col(columna)), r"\s+", " "))

def texto_mayusculas(columna):
    return F.upper(F.regexp_replace(F.trim(F.col(columna)), r"\s+", " "))

# COMMAND ----------

df_bronze = spark.table(BRONZE)
columnas_origen = [c for c in df_bronze.columns if not c.startswith("_")]

capacidad_valor = F.regexp_extract(
    F.upper(F.col("nombre_producto")),
    r"([0-9]+(?:\.[0-9]+)?)\s*(GB|MB)",
    1,
)

capacidad_unidad = F.regexp_extract(
    F.upper(F.col("nombre_producto")),
    r"([0-9]+(?:\.[0-9]+)?)\s*(GB|MB)",
    2,
)

# COMMAND ----------

df_producto_silver = (
    df_bronze
    .dropDuplicates(columnas_origen)
    .select(
        texto_mayusculas("id_producto").alias("id_producto"),
        limpiar_texto("nombre_producto").alias("nombre_producto"),
        limpiar_texto("tipo_producto").alias("tipo_producto"),
        texto_mayusculas("tecnologia").alias("tecnologia"),
        F.expr("try_cast(valor_mensual AS BIGINT)").alias("valor_mensual"),
        F.expr("try_cast(vigente AS BOOLEAN)").alias("vigente"),
        F.when(
            capacidad_unidad == "GB",
            capacidad_valor.cast("double"),
        ).when(
            capacidad_unidad == "MB",
            capacidad_valor.cast("double") / F.lit(1024.0),
        ).otherwise(F.lit(None).cast("double")).alias("capacidad_gb"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_pipeline_run_id"),
        F.col("_record_hash"),
        F.lit(SILVER_RUN_ID).alias("_silver_run_id"),
        F.current_timestamp().alias("_silver_timestamp"),
    )
)

(
    df_producto_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

print(
    f"Silver listo | tabla={SILVER} | "
    f"registros={df_producto_silver.count()}"
)

# COMMAND ----------

sin_capacidad = spark.table(SILVER).filter(F.col("capacidad_gb").isNull())
duplicados = (
    spark.table(SILVER)
    .groupBy("id_producto")
    .count()
    .filter(F.col("count") > 1)
)

print(f"Validación | id_producto duplicado={duplicados.count()}")
print(f"Validación | capacidad no extraída={sin_capacidad.count()}")