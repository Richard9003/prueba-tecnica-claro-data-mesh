# Data Contract — Cliente 360 (Churn + Upsell)

## 1. Identidad y propósito

| Campo | Valor |
|-------|-------|
| **Producto de datos** | `cliente_360_churn_upsell` |
| **Dominio** | Postpago Residencial |
| **Capa** | L3 Certified (Gold) |
| **Propietario de negocio** | Líder de Postpago Residencial |
| **Propietario técnico** | Data Engineer / Data Product Owner del dominio |
| **Consumidores** | Mercadeo Digital, Salesforce Data Cloud, Power BI, Analista comercial |
| **Fecha de creación** | 2026-09-11 |
| **Versi ón** | 1.0 |

---

## 2. Prop ósito del producto

Tabla certificada que consolida informaci ón de clientes activos del dominio Postpago Residencial, integrando:
- Datos demogr áficos y de segmento.
- Producto actual contratado.
- M étricas de consumo (promedio GB).
- Historial de incidencias de red.
- PQR (total, abiertos, satisfacci ón).
- Indicadores de riesgo de abandono (`churn_risk`) y oportunidad de upsell (`upsell_flag`).

**Caso de uso:** Activaci ón de campa ñas de retenci ón y venta cruzada por parte de Mercadeo Digital.

---

## 3. Esquema t écnico

| Columna | Tipo | Descripci ón | Reglas de calidad |
|---------|------|--------------|-------------------|
| `id_cliente` | STRING | Llave primaria del cliente | NOT NULL, ÚNICO |
| `segmento` | STRING | Segmento comercial (Residencial, Pyme, Corporativo) | NOT NULL |
| `ciudad` | STRING | Ciudad de residencia | NOT NULL, valor "No informado" si origen no la provee |
| `producto_actual` | STRING | Nombre comercial del producto actual | NULL permitido (si no hay uso) |
| `consumo_promedio_gb` | DOUBLE | Promedio de GB consumidos | >= 0 |
| `total_incidencias_red` | LONG | Total de incidencias de red reportadas | >= 0 |
| `promedio_incidencias_red` | DOUBLE | Promedio de incidencias de red por periodo | >= 0 |
| `total_pqr` | LONG | Total de casos PQR | >= 0 |
| `pqr_abiertos` | LONG | Casos PQR con estado "Abierto" | >= 0, <= total_pqr |
| `satisfaccion_promedio` | DOUBLE | Promedio de calificaci ón de satisfacci ón (1-5) | BETWEEN 1 AND 5 (si no es NULL) |
| `churn_risk` | STRING | Riesgo de abandono: "Alto", "Medio", "Bajo" | NOT NULL, IN ('Alto', 'Medio', 'Bajo') |
| `upsell_flag` | BOOLEAN | Oportunidad de upsell (consumo > 80% de capacidad del plan) | NOT NULL |
| `fecha_actualizacion` | TIMESTAMP | Fecha de actualizaci ón del registro | NOT NULL |
| `_gold_run_id` | STRING | ID de ejecuci ón del pipeline | NOT NULL |

---

## 4. Grano y alcance

- **Grano:** Un registro por cliente activo.
- **Filtro:** Solo clientes con `estado_cliente = 'Activo'` en `dim_cliente_silver`.
- **Cobertura:** Todos los clientes activos del dominio Postpago Residencial con al menos un registro de uso o PQR.

---

## 5. Frecuencia y SLA

| Aspecto | Valor |
|---------|-------|
| **Frecuencia de actualizaci ón** | Diaria (batch) |
| **SLA de disponibilidad** | Antes de las 08:00 a.m. (hora Colombia) |
| **Ventana de procesamiento** | 02:00 a.m. – 06:00 a.m. |
| **Retraso m áximo aceptable** | 2 horas |
| **Notificaci ón de fallo** | Email + Slack al Data Product Owner y Mercadeo Digital |

---

## 6. Reglas de calidad (Quality Gate)

### 6.1. Reglas cr íticas (detienen la certificaci ón)

| Regla | Descripci ón | Acci ón si falla |
|-------|--------------|-----------------|
| **Unicidad** | `id_cliente` debe ser único | Detener publicaci ón, alertar, preservar evidencia |
| **No nulidad de clave** | `id_cliente` NOT NULL | Detener publicaci ón, alertar |
| **Tabla vac ía** | Gold no puede quedar sin registros | Detener publicaci ón, alertar, mantener ú ltima versi ón v álida |
| **churn_risk inv álido** | `churn_risk` IN ('Alto', 'Medio', 'Bajo') | Detener publicaci ón, alertar |

### 6.2. Reglas de advertencia (no detienen, pero se registran)

| Regla | Descripci ón | Acci ón si falla |
|-------|--------------|-----------------|
| **Consumo negativo** | `consumo_promedio_gb` >= 0 | Registrar m étrica, investigar en cuarentena |
| **Satisfacci ón fuera de rango** | `satisfaccion_promedio` BETWEEN 1 AND 5 (si no es NULL) | Registrar m étrica, investigar en origen |
| **PQR abiertos > total** | `pqr_abiertos` <= `total_pqr` | Registrar m étrica, investigar |

---

## 7. Sem ántica de m étricas

### 7.1. `churn_risk`

| Valor | Condici ón |
|-------|-----------|
| **Alto** | `pqr_queja_reclamo_ult4m >= 2` **O** `promedio_incidencias_red >= 3` |
| **Medio** | `pqr_queja_reclamo_ult4m == 1` **O** `promedio_incidencias_red` entre 1 y 3 |
| **Bajo** | Otro caso |

**Nota:** `pqr_queja_reclamo_ult4m` = PQR de tipo Queja/Reclamo en los ú ltimos 4 meses (fecha de corte = máxima fecha de apertura en PQR).

### 7.2. `upsell_flag`

| Valor | Condici ón |
|-------|-----------|
| **True** | `consumo_promedio_gb > capacidad_gb * 1.80` (80% por encima de la capacidad del plan) |
| **False** | Otro caso (incluye `capacidad_gb` NULL) |

**Nota:** `capacidad_gb` se extrae de `nombre_producto` en `dim_producto_silver` mediante regex (GB/MB).

---

## 8. Clasificaci ón y seguridad

| Aspecto | Valor |
|---------|-------|
| **Clasificaci ón** | Datos personales y comerciales |
| **PII** | `id_cliente` (identificador t écnico), no se incluyen `documento` ni `nombre_completo` |
| **Acceso** | RBAC/ABAC via Unity Catalog |
| **Principio** | M ínimo privilegio |
| **Consumidores autorizados** | Mercadeo Digital, Analistas comerciales, Servicios de activaci ón (Data Cloud) |
| **Enmascaramiento** | No aplica (solo identificador t écnico) |
| **Retenci ón** | 24 meses (alineado con pol ítica corporativa) |

---

## 9. Trazabilidad y linaje

| Elemento | Descripci ón |
|----------|--------------|
| **Fuentes** | `dim_cliente_silver`, `dim_producto_silver`, `fact_uso_servicio_silver`, `fact_pqr_silver` |
| **Pipeline** | `06_gold_cliente_360.py` |
| **Metadatos** | `_gold_run_id`, `fecha_actualizacion` |
| **Linaje** | Registrado en Unity Catalog (si disponible) |
| **Auditor ía** | Tabla `ops.pipeline_run_audit` (pendiente de implementaci ón) |

---

## 10. Pol ítica de acceso

| Rol | Permisos |
|-----|----------|
| **Data Engineer del dominio** | SELECT, MODIFY |
| **Data Steward / Gobierno** | SELECT |
| **Mercadeo Digital** | SELECT (solo columnas necesarias para campa ñas) |
| **Analistas comerciales** | SELECT (bajo aprobaci ón del owner) |
| **Servicios de activaci ón** | SELECT (vista de consumo con columnas filtradas) |

**Nota:** En producci ón, se recomienda crear una **vista de consumo** con columnas filtradas para Mercadeo Digital (ej. sin `promedio_incidencias_red`, `total_incidencias_red`).

---

## 11. Gesti ón de incumplimiento

| Escenario | Acci ón |
|-----------|---------|
| **Falla regla cr ítica** | - Detener certificaci ón a Gold<br>- Emitir alerta (email + Slack)<br>- Preservar evidencia en cuarentena<br>- Escalar al owner de fuente |
| **Falla regla de advertencia** | - Registrar m étrica en tabla de auditor ía<br>- Continuar con certificaci ón<br>- Investigar en siguiente iteraci ón |
| **Tabla Gold vac ía** | - No publicar<br>- Mantener ú ltima versi ón v álida<br>- Alertar inmediatamente |

---

## 12. Versionamiento y cambios

| Versi ón | Fecha | Cambios | Responsable |
|----------|-------|---------|-------------|
| 1.0 | 2026-09-11 | Versi ón inicial | Ricardo Su árez |

**Pol ítica de cambios:**
- Cambios en esquema → requieren aprobaci ón del owner de negocio y t écnico.
- Cambios en reglas de negocio → requieren actualizaci ón de este contrato y notificaci ón a consumidores.
- Cambios en SLA → requieren aprobaci ón de Gobierno de Datos.

---

## 13. Contacto y soporte

| Rol | Contacto |
|-----|----------|
| **Owner de negocio** | L íder de Postpago Residencial (pendiente de asignar) |
| **Owner t écnico** | Data Engineer / Data Product Owner (Ricardo Su árez) |
| **Soporte t écnico** | richardo.suarez@example.com |
| **Gobierno de Datos** | gobierno.datos@claro.com.co |

---

## 14. Referencias

- **Documento de prueba t écnica:** `1_Prueba_Tecnica_Databricks_Data_Mesh.pdf`
- **Notebook de implementaci ón:** `06_gold_cliente_360.py`
- **Pol ítica de gobierno de datos:** (pendiente de enlace corporativo)