# Databricks notebook source
# MAGIC %md
# MAGIC # 04. Silver — Uso de Servicio y Cuarentena
# MAGIC
# MAGIC **Tratamiento:**
# MAGIC - Validar integridad referencial contra cliente y producto.
# MAGIC - Validar rangos: consumo >= 0, dias_activo_mes 0-31, incidencias >= 0.
# MAGIC - Validar formato de periodo (YYYY-MM).
# MAGIC - Registros inv álidos → cuarentena con motivo de rechazo.
# MAGIC
# MAGIC **Constraints (despu és de crear la tabla):**
# MAGIC - `id_uso` NOT NULL y ÚNICO.
# MAGIC - `consumo_datos_gb` >= 0.
# MAGIC - `dias_activo_mes` BETWEEN 0 AND 31.
# MAGIC - `incidencias_red` >= 0.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
BRONZE = f"{CATALOGO}.l1_raw.fact_uso_servicio_bronze"
CLIENTES_SILVER = f"{CATALOGO}.l2_curated.dim_cliente_silver"
PRODUCTOS_SILVER = f"{CATALOGO}.l2_curated.dim_producto_silver"
SILVER = f"{CATALOGO}.l2_curated.fact_uso_servicio_silver"
CUARENTENA = f"{CATALOGO}.l2_curated.fact_uso_servicio_rechazados"
SILVER_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# COMMAND ----------

df_bronze = spark.table(BRONZE)

df_clientes = (
    spark.table(CLIENTES_SILVER)
    .select("id_cliente")
    .dropDuplicates()
    .withColumn("_cliente_existe", F.lit(True))
)

df_productos = (
    spark.table(PRODUCTOS_SILVER)
    .select("id_producto")
    .dropDuplicates()
    .withColumn("_producto_existe", F.lit(True))
)

# COMMAND ----------

df_tipado = (
    df_bronze
    .select(
        F.upper(F.trim(F.col("id_uso"))).alias("id_uso"),
        F.upper(F.trim(F.col("id_cliente"))).alias("id_cliente"),
        F.upper(F.trim(F.col("id_producto"))).alias("id_producto"),
        F.trim(F.col("periodo")).alias("periodo"),
        F.expr("try_cast(consumo_datos_gb AS DOUBLE)").alias("consumo_datos_gb"),
        F.expr("try_cast(minutos_voz AS INT)").alias("minutos_voz"),
        F.expr("try_cast(sms_enviados AS INT)").alias("sms_enviados"),
        F.expr("try_cast(dias_activo_mes AS INT)").alias("dias_activo_mes"),
        F.expr("try_cast(incidencias_red AS INT)").alias("incidencias_red"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_pipeline_run_id"),
        F.col("_record_hash"),
    )
    .join(df_clientes, "id_cliente", "left")
    .join(df_productos, "id_producto", "left")
)

# COMMAND ----------

df_validado = df_tipado.withColumn(
    "motivo_rechazo",
    F.concat_ws(
        " | ",
        F.when(F.col("_cliente_existe").isNull(), "ID_CLIENTE_HUERFANO"),
        F.when(F.col("_producto_existe").isNull(), "ID_PRODUCTO_HUERFANO"),
        F.when(
            F.col("consumo_datos_gb").isNull()
            | (F.col("consumo_datos_gb") < 0),
            "CONSUMO_DATOS_INVALIDO",
        ),
        F.when(
            F.col("dias_activo_mes").isNull()
            | ~F.col("dias_activo_mes").between(0, 31),
            "DIAS_ACTIVO_MES_INVALIDO",
        ),
        F.when(
            F.col("incidencias_red").isNull()
            | (F.col("incidencias_red") < 0),
            "INCIDENCIAS_RED_INVALIDAS",
        ),
        F.when(
            ~F.col("periodo").rlike(r"^[0-9]{4}-(0[1-9]|1[0-2])$"),
            "PERIODO_INVALIDO",
        ),
    ),
)

df_rechazados = (
    df_validado
    .filter(F.col("motivo_rechazo") != "")
    .drop("_cliente_existe", "_producto_existe")
    .withColumn("_rejection_timestamp", F.current_timestamp())
    .withColumn("_rejection_run_id", F.lit(SILVER_RUN_ID))
)

df_uso_silver = (
    df_validado
    .filter(F.col("motivo_rechazo") == "")
    .drop("_cliente_existe", "_producto_existe", "motivo_rechazo")
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
    df_uso_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

print(f"Silver listo | tabla={SILVER} | v álidos={df_uso_silver.count()}")
print(f"Cuarentena lista | tabla={CUARENTENA} | rechazados={df_rechazados.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar constraints en la tabla Silver

# COMMAND ----------

# Constraint: id_uso NOT NULL
spark.sql(f"""
    ALTER TABLE {SILVER}
    ADD CONSTRAINT id_uso_not_null
    EXPECT (id_uso IS NOT NULL)
""")

# Constraint: consumo_datos_gb >= 0
spark.sql(f"""
    ALTER TABLE {SILVER}
    ADD CONSTRAINT consumo_no_negativo
    EXPECT (consumo_datos_gb >= 0)
""")

# Constraint: dias_activo_mes BETWEEN 0 AND 31
spark.sql(f"""
    ALTER TABLE {SILVER}
    ADD CONSTRAINT dias_activo_valido
    EXPECT (dias_activo_mes BETWEEN 0 AND 31)
""")

# Constraint: incidencias_red >= 0
spark.sql(f"""
    ALTER TABLE {SILVER}
    ADD CONSTRAINT incidencias_no_negativas
    EXPECT (incidencias_red >= 0)
""")

print("Constraints aplicados en fact_uso_servicio_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validaciones de salida Silver

# COMMAND ----------

df_silver = spark.table(SILVER)

id_uso_nulo = df_silver.filter(
    F.col("id_uso").isNull()
    | (F.trim(F.col("id_uso")) == "")
)

consumo_negativo = df_silver.filter(
    F.col("consumo_datos_gb") < 0
)

dias_invalidos = df_silver.filter(
    ~F.col("dias_activo_mes").between(0, 31)
)

incidencias_negativas = df_silver.filter(
    F.col("incidencias_red") < 0
)

print(f"Validaci ón | id_uso nulo={id_uso_nulo.count()}")
print(f"Validaci ón | consumo negativo={consumo_negativo.count()}")
print(f"Validaci ón | d ías inv álidos={dias_invalidos.count()}")
print(f"Validaci ón | incidencias negativas={incidencias_negativas.count()}")