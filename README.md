# Prueba Técnica - Claro Data Mesh

## Objetivo

Implementar el producto de datos **`cliente_360_churn_upsell`** para el dominio **Postpago Residencial**, consolidando información de clientes, productos, uso y PQR para activar campañas de retenciÃ³n y upsell.

## Fuentes de Datos

| Archivo | DescripciÃ³n |
|---------|-------------|
| `dim_cliente.csv` | Maestro de clientes |
| `dim_producto.csv` | CatÃ¡logo de productos/planes |
| `fact_uso_servicio.csv` | Consumo mensual por cliente y producto |
| `fact_pqr.csv` | Peticiones, quejas y reclamos |

## Arquitectura

```
Fuentes (CSV)
    â†“
Bronze (L1 Raw) â†’ main.claro_postpago.l1_raw.*
    â†“
Silver (L2 Curated) â†’ main.claro_postpago.l2_curated.*
    â†“
Cuarentena â†’ main.claro_postpago.l2_curated.*_quarantine
    â†“
Gold (L3 Certified) â†’ main.claro_postpago.l3_certified.cliente_360_churn_upsell
    â†“
Consumidores â†’ Salesforce Data Cloud, Power BI, Mercadeo Digital
```

## Estructura del Repositorio

```
prueba-tecnica-claro-data-mesh/
â��â�¬â�¬ src/
â�¬â¬¬ â”œâ¬¬ ingestion/          # Notebooks de ingesta a Bronze
â¬¬â¬¬ â”œâ¬¬ silver/             # Transformaciones Silver
â¬¬â¬¬ â”œâ¬¬ gold/               # ConstrucciÃ³n del producto Gold
â¬¬â¬¬ â”œâ¬¬ quality/            # Controles de calidad
â¬¬â¬¬ â””â¬¬ common/             # Utilidades compartidas
â¬¬â¬¬ â”œâ¬¬ resources/          # Recursos estÃ¡ticos
â¬¬â¬¬ â”œâ¬¬ infrastructure/     # IaC (Terraform)
â¬¬â¬¬ â”œâ¬¬ tests/              # Pruebas unitarias
â¬¬â¬¬ â”œâ¬¬ docs/
â¬¬â¬¬ â”‚   â”œâ¬¬ architecture/    # Diagramas
â¬¬â¬¬ â”‚   â””â¬¬ data_contract/   # Contratos de datos
â¬¬â¬¬ â”œâ¬¬ terraform/          # Infraestructura como CÃ³digo
â¬¬â¬¬ â”œâ¬¬ .github/
â¬¬â¬¬ â”‚   â””â¬¬ workflows/       # CI/CD
â¬¬â¬¬ â”œâ¬¬ .gitignore
â¬¬â¬¬ â”œâ¬¬ README.md
â¬¬â¬¬ â””â¬¬ databricks.yml      # Databricks Asset Bundle
```

## Convenciones de Nombres

### Tablas

| Capa | PatrÃ³n | Ejemplo |
|------|--------|---------|
| Bronze | `main.claro_postpago.l1_raw.{tabla}_bronze` | `l1_raw.dim_cliente_bronze` |
| Silver | `main.claro_postpago.l2_curated.{tabla}_silver` | `l2_curated.dim_cliente_silver` |
| Cuarentena | `main.claro_postpago.l2_curated.{tabla}_quarantine` | `l2_curated.fact_uso_servicio_quarantine` |
| Gold | `main.claro_postpago.l3_certified.{producto}` | `l3_certified.cliente_360_churn_upsell` |

### Ramas Git

- `main` â†’ ProducciÃ³n
- `dev` â†’ Desarrollo
- `feature/{descripcion}` â†’ Nuevas funcionalidades

### Commits

```bash
git commit -m "feat: add bronze ingestion"
git commit -m "fix: handle null estrato"
git commit -m "docs: add architecture diagram"
```

## EjecuciÃ³n del Pipeline

### 1. Cargar CSV en Databricks

- Usa **Unity Catalog Volumes** o la opciÃ³n **Upload Data** en la UI.
- Ruta recomendada: `main.claro_postpago.volumes.bronze/`

### 2. Ejecutar Notebooks en Orden

1. `src/ingestion/01_bronze_ingestion.ipynb`
2. `src/silver/02_silver_dimensions.ipynb`
3. `src/silver/03_silver_facts.ipynb`
4. `src/quality/04_quarantine_and_quality.ipynb`
5. `src/gold/05_gold_cliente_360.ipynb`
6. `src/quality/06_gold_quality_gate.ipynb`

### 3. Validar Quality Gate

- Verificar mÃ©tricas de calidad.
- Confirmar que Gold cumple reglas de certificaciÃ³n.

### 4. Exportar Entregables

- Notebooks: `.dbc` o enlace compartido.
- Diagrama: `.png` o `.pdf` desde draw.io.
- PresentaciÃ³n: `.pptx`.

## Entregables

| CÃ³digo | DescripciÃ³n | Formato |
|--------|-------------|---------|
| E1 | Notebooks Bronze/Silver/Gold | `.dbc` o enlace |
| E2 | Scripts de calidad | `.py` / `.sql` |
| E3 | Diagrama de arquitectura | `.png` / `.pdf` |
| E4 | Respuestas teÃ³ricas (T1-T7) | `.pdf` / `.docx` |
| E5 | PresentaciÃ³n ejecutiva | `.pptx` (max 10 lÃ¡minas) |

## Data Contract

| Elemento | Propuesta |
|----------|-----------|
| Producto | `cliente_360_churn_upsell` |
| Dominio | Postpago Residencial |
| Capa | L3 / Certified / Gold |
| Owner de negocio | LÃ¬der de Postpago Residencial |
| Owner tÃ©cnico | Data Engineer del dominio |
| Consumidores | Mercadeo Digital, Data Cloud, Power BI |
| Grano | Un registro por cliente activo |
| SLA | ActualizaciÃ³n diaria, disponible antes de 08:00 |
| Esquema mÃ¬nimo | `id_cliente, segmento, ciudad, producto_actual, consumo_promedio_gb, total_incidencias_red, total_pqr, pqr_abiertos, satisfaccion_promedio, churn_risk, upsell_flag` |
| Reglas de calidad | `id_cliente` no nulo y ÃƒÂºnico, `churn_risk` en {Alto, Medio, Bajo}, `satisfaccion` entre 1 y 5, `consumo` no negativo |
| Seguridad | Acceso restringido, mÃ¬nimo privilegio |
| Trazabilidad | Metadatos de fuente, fecha de ingesta, tablas origen |
| Incumplimiento | No certificar Gold, generar alerta, preservar evidencia |

## OptimizaciÃ³n (P6)

Para `fact_uso_servicio` a gran escala:

- **Particionar por:** `periodo`
- **Z-ORDER por:** `(id_cliente, id_producto)`
- **Comandos:**
  ```sql
  OPTIMIZE l2_curated.fact_uso_servicio_silver ZORDER BY (id_cliente, id_producto);
  VACUUM l2_curated.fact_uso_servicio_silver RETAIN 168 HOURS;
  ```

## PrÃ³ximos Pasos

1. Recibir los 4 CSV reales.
2. Ejecutar perfilamiento.
3. Ajustar reglas de calidad.
4. Implementar transformaciones.
5. Generar resultados y evidencias.

## Contacto

Repositorio: https://github.com/Richard9003/prueba-tecnica-claro-data-mesh