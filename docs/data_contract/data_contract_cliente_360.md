# Data Contract — Cliente 360 (Churn + Upsell)

## 1. Identidad y propósito

| Campo | Valor |
|---|---|
| Producto de datos | `cliente_360_churn_upsell` |
| Dominio productor | Postpago Residencial |
| Capa de publicación | L3 Certified (Gold) |
| Propietario de negocio | Líder de Postpago Residencial |
| Propietario técnico | Data Engineer / Data Product Owner del dominio |
| Consumidores | Mercadeo Digital, Salesforce Data Cloud, Power BI y analítica comercial |
| Versión | 1.0 |
| Fecha de creación | 2026-09-11 |

## 2. Propósito del producto

Producto de datos certificado que consolida información de clientes activos de Postpago Residencial para apoyar campañas de retención y venta cruzada.

Integra atributos de cliente, producto actual, consumo, incidencias de red y PQR. Publica indicadores de riesgo de abandono (`churn_risk`) y oportunidad de upsell (`upsell_flag`).

## 3. Grano y cobertura

- **Grano:** un registro por cliente activo.
- **Población:** clientes con `estado_cliente = 'Activo'` en `dim_cliente_silver`.
- **Cobertura:** todos los clientes activos, tengan o no registros de uso o PQR.
- **Regla de publicación:** el producto solo se certifica cuando supera el quality gate definido en este contrato.

## 4. Esquema publicado

| Columna | Tipo | Descripción | Regla |
|---|---|---|---|
| `id_cliente` | STRING | Identificador técnico del cliente | No nulo y único |
| `segmento` | STRING | Segmento comercial | No nulo |
| `ciudad` | STRING | Ciudad de residencia | No nulo; usar `No informado` si el origen no la provee |
| `producto_actual` | STRING | Producto asociado al último período de uso válido | Puede ser nulo si no existe uso |
| `consumo_promedio_gb` | DOUBLE | Promedio de consumo de datos por cliente | Mayor o igual a 0 |
| `total_incidencias_red` | LONG | Total de incidencias de red | Mayor o igual a 0 |
| `promedio_incidencias_red` | DOUBLE | Promedio de incidencias por período | Mayor o igual a 0 |
| `total_pqr` | LONG | Total de PQR del cliente | Mayor o igual a 0 |
| `pqr_abiertos` | LONG | PQR con estado `Abierto` | Entre 0 y `total_pqr` |
| `satisfaccion_promedio` | DOUBLE | Promedio de satisfacción de PQR | Entre 1 y 5 cuando exista PQR; nulo si no hay PQR |
| `churn_risk` | STRING | Riesgo de abandono | `Alto`, `Medio` o `Bajo` |
| `upsell_flag` | BOOLEAN | Oportunidad de upsell | `true` o `false` |
| `fecha_actualizacion` | TIMESTAMP | Marca temporal de publicación | No nula |
| `_gold_run_id` | STRING | Identificador de ejecución del pipeline | No nulo |

## 5. Semántica de negocio

### `churn_risk`

| Valor | Regla |
|---|---|
| Alto | Dos o más PQR de tipo `Queja` o `Reclamo` en los últimos cuatro meses, o promedio de incidencias de red mayor o igual a 3 |
| Medio | Una PQR de tipo `Queja` o `Reclamo` en los últimos cuatro meses, o promedio de incidencias de red entre 1 y menor que 3 |
| Bajo | Cualquier otro caso |

La ventana de cuatro meses se calcula con base en la máxima `fecha_apertura` disponible en `fact_pqr_silver`, para asegurar reproducibilidad.

### `upsell_flag`

`true` cuando `consumo_promedio_gb > capacidad_gb × 1.80`; de lo contrario, `false`.

`capacidad_gb` se deriva de `nombre_producto` en `dim_producto_silver`. Si no se identifica capacidad, el indicador se publica como `false` y el caso debe revisarse en el catálogo de productos.

## 6. Frecuencia y SLA

| Aspecto | Compromiso |
|---|---|
| Frecuencia | Batch diario |
| Ventana objetivo | 02:00–06:00, hora Colombia |
| Disponibilidad esperada | Antes de las 08:00, hora Colombia |
| Frescura máxima | 24 horas |
| Retraso tolerado | Hasta 2 horas sobre el SLA |
| Notificación de incidente | Data Product Owner y consumidores afectados |

## 7. Calidad y certificación

### Reglas críticas

| Regla | Acción ante incumplimiento |
|---|---|
| Gold no vacío | Bloquear publicación y conservar la última versión válida |
| `id_cliente` no nulo | Bloquear publicación |
| `id_cliente` único | Bloquear publicación |
| `churn_risk` dentro del dominio permitido | Bloquear publicación |
| `consumo_promedio_gb >= 0` | Bloquear publicación |
| `pqr_abiertos <= total_pqr` | Bloquear publicación |

### Reglas de monitoreo

| Regla | Acción ante incumplimiento |
|---|---|
| `satisfaccion_promedio` entre 1 y 5 cuando no sea nula | Registrar métrica e investigar el origen |
| Producto sin capacidad identificable | Registrar métrica; no activar upsell basado en capacidad desconocida |
| Ciudad tratada como `No informado` | Registrar métrica de completitud |
| Estrato imputado en Silver | Registrar métrica de calidad y trazabilidad |

Los registros inválidos de uso se aíslan en `fact_uso_servicio_rechazados`. Bronze preserva los datos originales y Silver aplica la normalización, validación y cuarentena.

## 8. Linaje y trazabilidad

| Elemento | Definición |
|---|---|
| Fuentes Silver | `dim_cliente_silver`, `dim_producto_silver`, `fact_uso_servicio_silver`, `fact_pqr_silver` |
| Productor técnico | `06_gold_cliente_360.py` |
| Evidencia de ejecución | `_gold_run_id` y `fecha_actualizacion` |
| Trazabilidad de origen | `_source_file`, `_ingestion_timestamp`, `_pipeline_run_id` y `_record_hash` preservados en Bronze/Silver |
| Gobierno | Unity Catalog para catálogo, permisos, linaje y auditoría cuando esté disponible |

## 9. Seguridad y acceso

- Clasificación: datos comerciales con identificador técnico de cliente.
- No se publican `documento` ni `nombre_completo` en Gold.
- Principio de acceso: mínimo privilegio.
- Mercadeo Digital debe consumir una vista autorizada con las columnas mínimas necesarias para campañas.
- En un entorno corporativo, Unity Catalog administra permisos mediante grupos y RBAC/ABAC; se aplican vistas dinámicas o enmascaramiento cuando corresponda.

| Rol | Acceso propuesto |
|---|---|
| Data Engineer del dominio | Lectura y modificación controlada |
| Data Steward / Gobierno de Datos | Lectura y auditoría |
| Mercadeo Digital | Lectura sobre vista autorizada |
| Analítica comercial | Lectura bajo autorización del owner |
| Servicio de activación / Data Cloud | Lectura sobre interfaz de consumo autorizada |

## 10. Operación y evolución

La implementación de la prueba usa reconstrucción batch determinista desde Bronze y `overwrite` de las tablas derivadas; por ello, reprocesar el mismo insumo deja el mismo estado final.

Para producción se debe evolucionar a:

- Control de lote y hash de archivo para evitar reprocesar insumos ya tratados.
- Watermark batch y `MERGE` por llave de negocio para cargas incrementales.
- Orquestación con Databricks Jobs/Lakeflow Jobs, dependencias y concurrencia controlada.
- Métricas y alertas de volumen, rechazo, frescura y SLA.
- Liquid Clustering, o partición por `periodo` y `OPTIMIZE`/`ZORDER` si Liquid Clustering no está disponible.
- Databricks Asset Bundles para desplegar el mismo workload entre desarrollo, pruebas y producción.
- Terraform, cuando aplique, para recursos de infraestructura y gobierno: catálogos, esquemas, permisos y ubicaciones externas.

## 11. Gestión de cambios

- Cambios de esquema, semántica, reglas de churn/upsell o SLA requieren nueva versión del contrato.
- Los cambios deben revisarse mediante control de versiones y pull request.
- Los consumidores deben ser notificados antes de cambios incompatibles.
- La versión del pipeline y del contrato debe mantenerse trazable en el repositorio.

## 12. Responsables

| Rol | Responsable |
|---|---|
| Owner de negocio | Líder de Postpago Residencial (por asignar) |
| Owner técnico | Data Engineer / Data Product Owner del dominio |
| Gobierno de datos | Equipo corporativo de Gobierno de Datos |
| Soporte operativo | Equipo responsable del producto de datos |
