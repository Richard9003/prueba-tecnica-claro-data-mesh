# Data Contract — Cliente 360 Churn & Upsell

## 1. Identidad

| Campo | Valor |
|---|---|
| Producto | `cliente_360_churn_upsell` |
| Dominio productor | Postpago Residencial |
| Publicación | `claro_postpago.l3_certified.cliente_360_churn_upsell` |
| Owner de negocio | Líder de Postpago Residencial |
| Owner técnico | Data Engineer / Data Product Owner del dominio |
| Consumidores | Mercadeo Digital, Salesforce Data Cloud, Power BI y analítica comercial |
| Versión | 1.1 |

## 2. Propósito y grano

Producto certificado para campañas de retención y venta cruzada. Su grano es un registro por cliente con `estado_cliente = 'Activo'`, tengan o no uso o PQR.

## 3. Fuentes y linaje

| Capa | Fuente |
|---|---|
| Bronze | `dim_cliente_bronze`, `dim_producto_bronze`, `fact_uso_servicio_bronze`, `fact_pqr_bronze` |
| Silver | `dim_cliente_silver`, `dim_producto_silver`, `fact_uso_servicio_silver`, `fact_pqr_silver` |
| Gold | `20_gold_cliente_360.py` |
| Analítica | `21_analitica_p5.py` |

Bronze conserva los valores originales y los metadatos `_source_file`, `_ingestion_timestamp`, `_pipeline_run_id` y `_record_hash`. Silver aplica tipado, normalización y cuarentena.

## 4. Esquema publicado

| Columna | Tipo | Regla |
|---|---|---|
| `id_cliente` | STRING | No nulo y único |
| `segmento` | STRING | Segmento comercial |
| `ciudad` | STRING | `No informado` si el origen no la provee |
| `producto_actual` | STRING | Producto del último periodo de uso válido; puede ser nulo |
| `consumo_promedio_gb` | DOUBLE | Mayor o igual a 0 |
| `total_incidencias_red` | LONG | Mayor o igual a 0 |
| `promedio_incidencias_red` | DOUBLE | Mayor o igual a 0 |
| `total_pqr` | LONG | Mayor o igual a 0 |
| `pqr_abiertos` | LONG | Entre 0 y `total_pqr` |
| `satisfaccion_promedio` | DOUBLE | Entre 1 y 5 cuando existe PQR; nulo sin PQR |
| `churn_risk` | STRING | `Alto`, `Medio` o `Bajo` |
| `upsell_flag` | BOOLEAN | `true` o `false` |
| `fecha_actualizacion` | TIMESTAMP | No nula |
| `_gold_run_id` | STRING | No nulo |

## 5. Reglas de negocio

### `churn_risk`

- `Alto`: al menos 2 PQR de tipo `Queja` o `Reclamo` en los últimos cuatro meses, o promedio de incidencias de red mayor o igual a 3.
- `Medio`: 1 PQR de tipo `Queja` o `Reclamo` en los últimos cuatro meses, o promedio de incidencias entre 1 y menor que 3.
- `Bajo`: cualquier otro caso.

La ventana se calcula desde `MAX(fecha_apertura)` disponible en `fact_pqr_silver`, para que el resultado sea reproducible.

### `upsell_flag`

```text
true cuando consumo_promedio_gb > capacidad_gb × 1.80
```

`capacidad_gb` se deriva de `nombre_producto`. Si no se puede identificar, se publica `false` y se registra como métrica; no se activa upsell con capacidad desconocida.

## 6. SLA

| Aspecto | Compromiso |
|---|---|
| Frecuencia | Batch diario |
| Ventana objetivo | 02:00–06:00, hora Colombia |
| Disponibilidad | Antes de las 08:00 |
| Frescura máxima | 24 horas |
| Retraso tolerado | 2 horas |
| Incidente | Notificar al Data Product Owner y consumidores afectados |

## 7. Quality Gate

Estas reglas bloquean la publicación de una nueva versión:

- Gold no vacío.
- `id_cliente` no nulo y único.
- `churn_risk` dentro del dominio permitido.
- `consumo_promedio_gb >= 0`.
- `pqr_abiertos <= total_pqr`.

Si falla una regla crítica, se conserva la última versión válida y el Job termina con error trazable.

## 8. Reglas de monitoreo

No bloquean por sí solas, pero generan métricas y seguimiento:

- Satisfacción fuera de 1–5 cuando no sea nula.
- Producto sin capacidad identificable.
- Ciudad tratada como `No informado`.
- Estrato imputado en Silver.
- Registros de uso enviados a `fact_uso_servicio_rechazados`.
- Porcentaje de rechazados, volumen, frescura y duración del Job.

## 9. Seguridad

Gold no publica `documento` ni `nombre_completo`. El consumo de Mercadeo Digital se realiza mediante una vista autorizada y con mínimo privilegio. Unity Catalog administra permisos, catálogo, linaje y auditoría cuando el workspace lo permita.

## 10. Operación y evolución

La prueba usa reconstrucción batch determinista desde Bronze y `overwrite` de tablas derivadas. En producción se evolucionará a landing inmutable, control de hash de archivos, watermark, `MERGE` por llave de negocio y recalculo de clientes impactados.

El Job productivo tendrá tareas Bronze independientes, Silver dependientes, Gold con Quality Gate y analítica posterior. El bootstrap se ejecuta una vez por ambiente.

## 11. Gestión de cambios

Cambios de esquema, semántica, reglas, SLA o rutas de notebooks requieren revisión por Pull Request y nueva versión del contrato. Las rutas del Job deben declararse en Databricks Asset Bundles para permitir validación, despliegue y rollback.
