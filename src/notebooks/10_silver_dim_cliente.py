# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Silver — Dimensión Cliente
# MAGIC
# MAGIC Tratamiento aprobado:
# MAGIC - Eliminar duplicados exactos.
# MAGIC - Ciudad nula/vacía: `No informado`.
# MAGIC - Estrato nulo: imputar mediana global de estratos válidos.
# MAGIC - Conservar valor original y trazabilidad de la imputación.

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

# MAGIC %md
# MAGIC ## Preparación y cálculo de mediana
# MAGIC
# MAGIC El estrato llega como texto decimal (`1.0`, `2.0`, etc.).
# MAGIC Se convierte a DOUBLE y luego a INT para mantener el tipo numérico en Silver.

# COMMAND ----------

df_bronze = spark.table(BRONZE)

columnas_origen = [
    columna
    for columna in df_bronze.columns
    if not columna.startswith("_")
]

df_sin_duplicados = df_bronze.dropDuplicates(columnas_origen)

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

# MAGIC %md
# MAGIC ## Construcción Silver

# COMMAND ----------

estrato_origen = F.trim(F.col("estrato"))

estrato_es_nulo = (
    F.col("estrato").isNull()
    | (estrato_origen == "")
)

ciudad_es_nula = (
    F.col("ciudad").isNull()
    | (F.trim(F.col("ciudad")) == "")
)

df_cliente_silver = (
    df_sin_duplicados
    .select(
        texto_mayusculas("id_cliente").alias("id_cliente"),
        texto_mayusculas("tipo_documento").alias("tipo_documento"),
        F.expr("try_cast(documento AS BIGINT)").alias("documento"),
        limpiar_texto("nombre_completo").alias("nombre_completo"),
        limpiar_texto("segmento").alias("segmento"),

        # Trazabilidad de estrato.
        estrato_origen.alias("estrato_origen"),

        # Estrato numérico final: valor fuente o mediana si llegó nulo.
        F.when(
            estrato_es_nulo,
            F.lit(mediana_estrato).cast("int")
        ).otherwise(
            estrato_tipado.cast("int")
        ).alias("estrato"),

        F.when(
            estrato_es_nulo,
            F.lit(True)
        ).otherwise(F.lit(False)).alias("estrato_imputado"),

        F.when(
            estrato_es_nulo,
            F.lit("MEDIANA_GLOBAL")
        ).otherwise(F.lit("NO_APLICA")).alias("estrato_metodo_imputacion"),

        # Ciudad categórica: se usa una categoría explícita, no una ciudad inventada.
        F.when(
            ciudad_es_nula,
            F.lit("No informado")
        ).otherwise(
            limpiar_texto("ciudad")
        ).alias("ciudad"),

        F.when(
            ciudad_es_nula,
            F.lit(True)
        ).otherwise(F.lit(False)).alias("ciudad_imputada"),

        F.when(
            ciudad_es_nula,
            F.lit("NO_INFORMADO")
        ).otherwise(F.lit("NO_APLICA")).alias("ciudad_metodo_tratamiento"),

        F.expr("try_to_date(fecha_alta, 'yyyy-MM-dd')").alias("fecha_alta"),
        limpiar_texto("canal_adquisicion").alias("canal_adquisicion"),
        limpiar_texto("estado_cliente").alias("estado_cliente"),

        # Metadatos heredados de Bronze.
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_pipeline_run_id"),
        F.col("_record_hash"),

        # Metadatos de transformación Silver.
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
print(
    f"Calidad | estratos imputados="
    f"{df_silver.filter(F.col('estrato_imputado')).count()}"
)
print(
    f"Calidad | ciudades tratadas como No informado="
    f"{df_silver.filter(F.col('ciudad_imputada')).count()}"
)

display(
    df_silver
    .filter(F.col("estrato_imputado") | F.col("ciudad_imputada"))
    .select(
        "id_cliente",
        "estrato_origen",
        "estrato",
        "estrato_imputado",
        "estrato_metodo_imputacion",
        "ciudad",
        "ciudad_imputada",
        "ciudad_metodo_tratamiento",
        "_source_file",
    )
    .orderBy("id_cliente")
)