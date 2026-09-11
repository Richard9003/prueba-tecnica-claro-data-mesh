# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Silver — Dimensión Producto
# MAGIC
# MAGIC **Tratamiento aplicado:**
# MAGIC - Eliminar duplicados exactos.
# MAGIC - Estandarizar texto y tipos.
# MAGIC - Extraer `capacidad_gb` desde `nombre_producto`.
# MAGIC - Convertir MB a GB cuando corresponda.
# MAGIC
# MAGIC **Validaciones de calidad:**
# MAGIC - `id_producto` no nulo y sin duplicados.
# MAGIC - `valor_mensual` no negativo.
# MAGIC - Productos sin capacidad identificable se reportan para revisión.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
BRONZE = f"{CATALOGO}.l1_raw.dim_producto_bronze"
SILVER = f"{CATALOGO}.l2_curated.dim_producto_silver"
SILVER_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# COMMAND ----------

def limpiar_texto(columna):
    return F.initcap(
        F.regexp_replace(
            F.trim(F.col(columna)),
            r"\s+",
            " "
        )
    )

def texto_mayusculas(columna):
    return F.upper(
        F.regexp_replace(
            F.trim(F.col(columna)),
            r"\s+",
            " "
        )
    )

# COMMAND ----------

df_bronze = spark.table(BRONZE)

columnas_origen = [
    columna
    for columna in df_bronze.columns
    if not columna.startswith("_")
]

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
        ).otherwise(
            F.lit(None).cast("double")
        ).alias("capacidad_gb"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_pipeline_run_id"),
        F.col("_record_hash"),
        F.lit(SILVER_RUN_ID).alias("_silver_run_id"),
        F.current_timestamp().alias("_silver_timestamp"),
    )
)

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Validaciones de salida Silver

# COMMAND ----------

df_silver = spark.table(SILVER)

id_producto_nulo = df_silver.filter(
    F.col("id_producto").isNull()
    | (F.trim(F.col("id_producto")) == "")
)

id_producto_duplicado = (
    df_silver
    .groupBy("id_producto")
    .count()
    .filter(F.col("count") > 1)
)

valor_mensual_invalido = df_silver.filter(
    F.col("valor_mensual").isNull()
    | (F.col("valor_mensual") < 0)
)

productos_sin_capacidad = df_silver.filter(
    F.col("capacidad_gb").isNull()
)

print(f"Validación | id_producto nulo={id_producto_nulo.count()}")
print(f"Validación | id_producto duplicado={id_producto_duplicado.count()}")
print(f"Validación | valor_mensual nulo/negativo={valor_mensual_invalido.count()}")
print(f"Validación | capacidad_gb no identificada={productos_sin_capacidad.count()}")