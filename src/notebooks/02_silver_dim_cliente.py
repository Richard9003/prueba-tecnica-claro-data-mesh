# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Silver — Dimensión Cliente

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
BRONZE = f"{CATALOGO}.l1_raw.dim_cliente_bronze"
SILVER = f"{CATALOGO}.l2_curated.dim_cliente_silver"
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

df_sin_duplicados = df_bronze.dropDuplicates(columnas_origen)

# El CSV representa estrato como texto decimal: "1.0", "2.0", etc.
# Se convierte primero a DOUBLE y después a INT.
estrato_tipado = F.expr("try_cast(estrato AS DOUBLE)")

estratos_validos = (
    df_sin_duplicados
    .select(estrato_tipado.alias("estrato"))
    .filter(F.col("estrato").between(1, 6))
)

mediana_estrato = (
    estratos_validos
    .agg(F.expr("percentile_approx(estrato, 0.5)").alias("mediana"))
    .first()["mediana"]
)

print(f"Mediana de estrato calculada: {mediana_estrato}")

# COMMAND ----------

df_cliente_silver = (
    df_sin_duplicados
    .select(
        texto_mayusculas("id_cliente").alias("id_cliente"),
        texto_mayusculas("tipo_documento").alias("tipo_documento"),
        F.expr("try_cast(documento AS BIGINT)").alias("documento"),
        limpiar_texto("nombre_completo").alias("nombre_completo"),
        limpiar_texto("segmento").alias("segmento"),
        F.coalesce(
            estrato_tipado.cast("int"),
            F.lit(mediana_estrato).cast("int"),
        ).alias("estrato"),
        F.when(
            F.col("ciudad").isNull()
            | (F.trim(F.col("ciudad")) == ""),
            F.lit("No informado"),
        ).otherwise(
            limpiar_texto("ciudad")
        ).alias("ciudad"),
        F.expr("try_to_date(fecha_alta, 'yyyy-MM-dd')").alias("fecha_alta"),
        limpiar_texto("canal_adquisicion").alias("canal_adquisicion"),
        limpiar_texto("estado_cliente").alias("estado_cliente"),
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
    df_cliente_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

print(
    f"Silver listo | tabla={SILVER} | "
    f"registros={df_cliente_silver.count()} | "
    f"mediana_estrato={mediana_estrato}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validaciones de salida Silver

# COMMAND ----------

df_silver = spark.table(SILVER)

id_cliente_nulo = df_silver.filter(
    F.col("id_cliente").isNull()
    | (F.trim(F.col("id_cliente")) == "")
)

id_cliente_duplicado = (
    df_silver
    .groupBy("id_cliente")
    .count()
    .filter(F.col("count") > 1)
)

estrato_invalido = df_silver.filter(
    ~F.col("estrato").between(1, 6)
)

ciudad_nula = df_silver.filter(
    F.col("ciudad").isNull()
    | (F.trim(F.col("ciudad")) == "")
)

print(f"Validación | id_cliente nulo={id_cliente_nulo.count()}")
print(f"Validación | id_cliente duplicado={id_cliente_duplicado.count()}")
print(f"Validación | estrato fuera de 1-6={estrato_invalido.count()}")
print(f"Validación | ciudad nula/vacía={ciudad_nula.count()}")

display(
    df_silver
    .filter(F.col("ciudad") == "No informado")
    .select("id_cliente", "estrato", "ciudad", "_source_file")
)