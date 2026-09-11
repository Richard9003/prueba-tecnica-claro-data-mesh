# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Silver — Dimensi ón Cliente
# MAGIC
# MAGIC **Tratamiento:**
# MAGIC - Eliminar duplicados exactos.
# MAGIC - Estrato nulo: imputar mediana global de estratos v álidos.
# MAGIC - Ciudad nula/vac ía: `No informado`.
# MAGIC - Conservar trazabilidad de la imputaci ón.
# MAGIC
# MAGIC **Constraints (despu és de crear la tabla):**
# MAGIC - `id_cliente` NOT NULL y ÚNICO.
# MAGIC - `estrato` BETWEEN 1 AND 6.

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
# Se convierte primero a DOUBLE y despu és a INT.
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
# MAGIC ## Aplicar constraints en la tabla Delta

# COMMAND ----------

# Constraint: id_cliente NOT NULL
spark.sql(f"""
    ALTER TABLE {SILVER}
    ADD CONSTRAINT id_cliente_not_null
    EXPECT (id_cliente IS NOT NULL)
""")

# Constraint: id_cliente ÚNICO (se valida con COUNT DISTINCT = COUNT)
# Nota: Delta no soporta UNIQUE constraint directamente, se valida con query de calidad.

# Constraint: estrato BETWEEN 1 AND 6
spark.sql(f"""
    ALTER TABLE {SILVER}
    ADD CONSTRAINT estrato_valido
    EXPECT (estrato BETWEEN 1 AND 6)
""")

print("Constraints aplicados en dim_cliente_silver")

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

print(f"Validaci ón | id_cliente nulo={id_cliente_nulo.count()}")
print(f"Validaci ón | id_cliente duplicado={id_cliente_duplicado.count()}")
print(f"Validaci ón | estrato fuera de 1-6={estrato_invalido.count()}")
print(f"Validaci ón | ciudad nula/vac ía={ciudad_nula.count()}")