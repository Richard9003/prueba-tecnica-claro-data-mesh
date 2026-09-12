# Databricks notebook source
# MAGIC %md
# MAGIC # 07. Analítica certificada — P5
# MAGIC
# MAGIC **Propósito:** Consulta de consumo sobre el producto Gold certificado.
# MAGIC
# MAGIC **Alcance:** P5.
# MAGIC
# MAGIC La consulta no transforma ni publica datos. Solo lee
# MAGIC `cliente_360_churn_upsell`, construido previamente por el notebook 06.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     segmento,
# MAGIC     ciudad,
# MAGIC     COUNT(CASE WHEN churn_risk = 'Alto' THEN 1 END) AS clientes_riesgo_alto,
# MAGIC     ROUND(AVG(satisfaccion_promedio), 2) AS satisfaccion_promedio
# MAGIC FROM claro_postpago.l3_certified.cliente_360_churn_upsell
# MAGIC GROUP BY segmento, ciudad
# MAGIC ORDER BY clientes_riesgo_alto DESC, satisfaccion_promedio ASC