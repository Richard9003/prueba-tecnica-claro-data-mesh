# Databricks notebook source
# MAGIC %md
# MAGIC # 05. Silver — PQR
# MAGIC
# MAGIC **Tratamiento aplicado:**
# MAGIC - Estandarizar llaves, texto y tipos.
# MAGIC - Parsear `fecha_apertura` en formatos `yyyy-MM-dd` y `dd/MM/yyyy`.
# MAGIC - Eliminar duplicados por `id_caso`.
# MAGIC - Enviar registros con errores críticos a cuarentena.
# MAGIC
# MAGIC **Errores críticos:**
# MAGIC - `id_caso` nulo.
# MAGIC - `id_cliente` nulo.
# MAGIC - Fecha no parseable.
# MAGIC - Satisfacción fuera de 1 a 5.
# MAGIC - Días de resolución negativos.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
BRONZE = f"{CATALOGO}.l1_raw.fact_pqr_bronze"
CLIENTES_SILVER = f"{CATALOGO}.l2_curated.dim_cliente_silver"
SILVER = f"{CATALOGO}.l2_curated.fact_pqr_silver"
CUARENTENA = f"{CATALOGO}.l2_curated.fact_pqr_rechazados"
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

fecha_apertura_estandar = F.coalesce(
    F.expr("try_to_date(fecha_apertura, 'yyyy-MM-dd')"),
    F.expr("try_to_date(fecha_apertura, 'dd/MM/yyyy')"),
)

# COMMAND ----------

df_bronze = spark.table(BRONZE)

df_clientes = (
    spark.table(CLIENTES_SILVER)
    .select("id_cliente")
    .dropDuplicates()
    .withColumn("_cliente_existe", F.lit(True))
)

df_tipado = (
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
    )
    .join(df_clientes, "id_cliente", "left")
)

# COMMAND ----------

df_validado = df_tipado.withColumn(
    "motivo_rechazo",
    F.concat_ws(
        " | ",
        F.when(F.col("id_caso").isNull() | (F.trim(F.col("id_caso")) == ""), "ID_CASO_NULO"),
        F.when(F.col("id_cliente").isNull() | (F.trim(F.col("id_cliente")) == ""), "ID_CLIENTE_NULO"),
        F.when(F.col("_cliente_existe").isNull(), "ID_CLIENTE_HUERFANO"),
        F.when(F.col("fecha_apertura").isNull(), "FECHA_APERTURA_INVALIDA"),
        F.when(
            F.col("dias_resolucion").isNull()
            | (F.col("dias_resolucion") < 0),
            "DIAS_RESOLUCION_INVALIDOS",
        ),
        F.when(
            F.col("satisfaccion_1_5").isNull()
            | ~F.col("satisfaccion_1_5").between(1, 5),
            "SATISFACCION_INVALIDA",
        ),
    ),
)

df_rechazados = (
    df_validado
    .filter(F.col("motivo_rechazo") != "")
    .drop("_cliente_existe")
    .withColumn("_rejection_timestamp", F.current_timestamp())
    .withColumn("_rejection_run_id", F.lit(SILVER_RUN_ID))
)

df_pqr_silver = (
    df_validado
    .filter(F.col("motivo_rechazo") == "")
    .drop("_cliente_existe", "motivo_rechazo")
    .dropDuplicates(["id_caso"])
    .withColumn("_silver_run_id", F.lit(SILVER_RUN_ID))
    .withColumn("_silver_timestamp", F.current_timestamp())
)

# COMMAND ----------

(
    df_rechazados.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CUARENTENA)
)

(
    df_pqr_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

print(f"Silver listo | tabla={SILVER} | válidos={df_pqr_silver.count()}")
print(f"Cuarentena lista | tabla={CUARENTENA} | rechazados={df_rechazados.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validaciones de salida Silver

# COMMAND ----------

df_silver = spark.table(SILVER)

id_caso_nulo = df_silver.filter(
    F.col("id_caso").isNull()
    | (F.trim(F.col("id_caso")) == "")
)

id_caso_duplicado = (
    df_silver
    .groupBy("id_caso")
    .count()
    .filter(F.col("count") > 1)
)

id_cliente_huerfano = (
    df_silver
    .join(
        spark.table(CLIENTES_SILVER).select("id_cliente").dropDuplicates(),
        "id_cliente",
        "left_anti",
    )
)

fecha_invalida = df_silver.filter(F.col("fecha_apertura").isNull())

satisfaccion_invalida = df_silver.filter(
    F.col("satisfaccion_1_5").isNull()
    | ~F.col("satisfaccion_1_5").between(1, 5)
)

dias_resolucion_invalidos = df_silver.filter(
    F.col("dias_resolucion").isNull()
    | (F.col("dias_resolucion") < 0)
)

print(f"Validación | id_caso nulo={id_caso_nulo.count()}")
print(f"Validación | id_caso duplicado={id_caso_duplicado.count()}")
print(f"Validación | id_cliente huérfano={id_cliente_huerfano.count()}")
print(f"Validación | fecha_apertura inválida={fecha_invalida.count()}")
print(f"Validación | satisfacción inválida={satisfaccion_invalida.count()}")
print(f"Validación | días resolución inválidos={dias_resolucion_invalidos.count()}")