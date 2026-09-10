# Databricks notebook source
# COMMAND ----------
# Inicializacion de esquemas del dominio Postpago Residencial.
#
# Ejecutar una vez por ambiente antes de desplegar los pipelines.
# Las sentencias son idempotentes: no modifican un esquema existente.
#
# Objetos creados:
# - main.claro_postpago_l1_raw: datos de origen conservados sin transformacion.
# - main.claro_postpago_l2_curated: datos estandarizados y registros de cuarentena.
# - main.claro_postpago_l3_certified: productos de datos certificados para consumo.
# - main.claro_postpago_ops: auditoria de ejecuciones y metricas de calidad.

CATALOG_NAME = "main"

SCHEMAS = {
    "l1_raw": "claro_postpago_l1_raw",
    "l2_curated": "claro_postpago_l2_curated",
    "l3_certified": "claro_postpago_l3_certified",
    "ops": "claro_postpago_ops",
}

for layer, schema_name in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema_name}")
    print(f"Esquema disponible: {CATALOG_NAME}.{schema_name}")