# 08. Estrategia de optimización y operación productiva

## Alcance

Este documento responde P6 y describe la evolución de la solución batch de la prueba hacia un pipeline escalable, gobernado y desplegable entre desarrollo, pruebas y producción.

## Tabla objetivo: fact_uso_servicio

La tabla `fact_uso_servicio_silver` puede crecer a cientos de millones de registros y normalmente se consultará por ventanas temporales, cliente y producto.

## Diseño de almacenamiento y rendimiento

### 1. Delta Lake

Persistir Bronze, Silver, cuarentena y Gold en Delta Lake para disponer de transacciones ACID, control de versiones, lecturas consistentes y operaciones de actualización con `MERGE`.

### 2. Liquid Clustering (recomendado para producción)

Usar Liquid Clustering sobre `fact_uso_servicio_silver` con claves candidatas:

```sql
CLUSTER BY (periodo, id_cliente, id_producto)
```

Motivo:

- `periodo` concentra los filtros temporales y las cargas incrementales.
- `id_cliente` e `id_producto` son filtros y claves frecuentes de joins.
- Liquid Clustering adapta la organización física de los datos a cambios en los patrones de consulta, evitando administrar particiones estáticas de forma manual.
- Es preferible cuando el volumen, las consultas y la cardinalidad pueden evolucionar.

Liquid Clustering debe validarse en el workspace/edición antes de adoptarse, porque su disponibilidad depende de la versión y configuración de Databricks.

### 3. Alternativa si Liquid Clustering no está disponible

Particionar por `periodo` y aplicar mantenimiento Delta:

```sql
OPTIMIZE claro_postpago.l2_curated.fact_uso_servicio_silver
ZORDER BY (id_cliente, id_producto);
```

Criterios:

- Particionar por `periodo` porque es una dimensión temporal de cardinalidad controlada.
- No particionar por `id_cliente`: tiene alta cardinalidad y generaría demasiadas particiones y archivos pequeños.
- Usar `ZORDER` por `id_cliente` e `id_producto` cuando los consumidores filtren frecuentemente por esas columnas.
- Ejecutar `OPTIMIZE` según volumen, frecuencia de escritura y tamaño de archivos; no de manera indiscriminada.

## Idempotencia e incrementalidad

La prueba implementa una carga batch completa, reproducible e idempotente por resultado mediante reconstrucción determinista desde Bronze y `overwrite` de las tablas derivadas.

Para producción:

1. Aterrizar cada archivo en una ruta inmutable identificada por fecha y `batch_id`.
2. Registrar en una tabla de control: nombre/ruta de archivo, hash, tamaño, fecha de llegada, `batch_id`, estado, conteos leídos/válidos/rechazados y versión de pipeline.
3. Rechazar o ignorar archivos ya procesados mediante combinación de `source_file_hash` y entidad.
4. Usar `MERGE` sobre tablas Delta con llaves de negocio:
   - Cliente: `id_cliente`.
   - Producto: `id_producto`.
   - Uso: `id_uso`.
   - PQR: `id_caso`.
5. Deduplicar el origen antes de `MERGE`, asegurando una fila por llave; esto evita coincidencias múltiples y resultados no deterministas.
6. Mantener watermark batch por periodo/fecha de evento o por lote procesado para limitar el procesamiento a cambios nuevos.
7. Recalcular Gold solo para clientes impactados cuando el volumen lo justifique.

## Calidad y observabilidad

- Bronze conserva el archivo original y metadatos de ingesta; no corrige datos.
- Silver normaliza, tipa, aplica reglas y envía errores a cuarentena.
- Gold ejecuta un quality gate antes de certificarse.
- Registrar métricas por ejecución: registros leídos, válidos, rechazados, porcentaje de rechazo, frescura, duplicados e incumplimientos por regla.
- Alertar al Data Product Owner cuando falle una regla crítica, el SLA o el umbral de calidad.

## Orquestación y concurrencia

Para producción, orquestar con Databricks Jobs/Lakeflow Jobs mediante tareas dependientes:

```text
Bronze
  ├── Silver Cliente
  ├── Silver Producto
  ├── Silver Uso + Cuarentena
  └── Silver PQR
          ↓
       Gold + Quality Gate
          ↓
       Analítica / consumo
```

- Ejecutar Silver Cliente y Silver Producto antes de Silver Uso.
- Ejecutar Gold solo cuando todas las tablas Silver requeridas finalicen correctamente.
- Limitar la concurrencia del pipeline del producto a una ejecución activa para evitar escrituras simultáneas sobre las mismas tablas destino.
- Usar reintentos controlados y alertas ante fallo, no reprocesos manuales sin trazabilidad.

## Desarrollo, pruebas y producción

Usar Databricks Asset Bundles para declarar y desplegar el workload de forma consistente entre ambientes:

```text
dev  -> desarrollo y pruebas unitarias
qa   -> pruebas de integración y calidad
prod -> ejecución programada y consumo certificado
```

Los Bundles deben versionar en Git:

- Notebooks o archivos fuente.
- Configuración de Jobs, tareas, dependencias, parámetros y schedules.
- Variables por ambiente (catálogo, esquema, rutas, alertas).
- Referencias a librerías y permisos requeridos.

Flujo recomendado:

1. Desarrollo local/Databricks Git folder en rama de trabajo.
2. Pull request con revisión de código.
3. Validación de sintaxis y pruebas de calidad con datos controlados.
4. `databricks bundle validate`.
5. Despliegue a `dev`.
6. Pruebas de integración.
7. Aprobación para despliegue a `prod`.
8. Ejecución del Job y monitoreo posterior.

Terraform complementa los Bundles para infraestructura y gobierno cuando se cuente con permisos corporativos: catálogos, esquemas, grants, identidades, external locations, políticas de cómputo y recursos base de Unity Catalog.

## Seguridad y gobierno

- Unity Catalog administra catálogo, esquemas, permisos, descubrimiento, linaje y auditoría.
- Aplicar principio de mínimo privilegio por roles/grupos.
- Mantener PII como `nombre_completo` y `documento` fuera de Gold si no es necesaria para el caso de uso.
- Entregar a Mercadeo Digital una vista de consumo con las columnas mínimas requeridas.
- Controlar cambios en esquema, reglas de calidad y reglas de churn/upsell mediante el data contract y versionamiento Git.