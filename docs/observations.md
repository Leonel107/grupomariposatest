# Observaciones sobre la arquitectura

## 1. Aislamiento por tenant mediante schemas independientes

### Tipo

Decisión de arquitectura con la que tengo una consideración.

### Arquitectura propuesta

La arquitectura productiva plantea un catálogo único por ambiente y un schema independiente para cada tenant:

```text
saas_<env>.bronze_<tenant>
saas_<env>.silver_<tenant>
saas_<env>.gold_<tenant>
```

Por ejemplo:

```text
saas_dev.bronze_sv.deliveries
saas_dev.silver_sv.fact_deliveries
saas_dev.gold_sv.daily_metrics_by_delivery_type
```

La motivación es facilitar el onboarding y mantener gobierno centralizado.

### Consideración

Para una plataforma con un número reducido o moderado de tenants considero razonable esta estrategia. Sin embargo, a medida que el número de tenants crezca significativamente, mantener múltiples schemas y gestionar permisos individualmente puede incrementar la complejidad operativa.

### Propuesta alternativa

Mantendría un esquema lógico común para las tablas compartidas, utilizando `_tenant_id` como columna obligatoria de aislamiento:

```text
saas_<env>.bronze.deliveries
saas_<env>.silver.fact_deliveries
saas_<env>.gold.daily_metrics_by_delivery_type
```

y complementaría el modelo con:

- row-level security;
- políticas de acceso por tenant;
- catálogo centralizado de tenants;
- controles de acceso administrados.

### Trade-offs

**Ventajas:**

- menor cantidad de objetos en el catálogo;
- menor complejidad administrativa;
- facilita análisis cross-tenant;
- facilita operaciones masivas.

**Desventajas:**

- requiere controles de seguridad más estrictos;
- un error de filtrado por `_tenant_id` puede provocar exposición entre tenants;
- el aislamiento lógico depende más de las políticas de acceso.

### Decisión adoptada

No modifiqué la arquitectura provista. Para la prueba implementé el aislamiento mediante paths:

```text
data/<layer>/<tenant>/
```

manteniendo la separación definida y dejando la alternativa como consideración para una evolución futura.

---

# 2. Ambigüedad en la semántica de precio y revenue

### Tipo

Ambigüedad resuelta durante la implementación.

### Problema

Silver contiene tanto:

```text
precio
```

como:

```text
precio_base
```

El primero proviene de la transacción, mientras que el segundo corresponde al catálogo.

Esto puede generar ambigüedad al momento de construir Gold.

### Resolución

Se introdujo explícitamente el concepto:

```text
precio_transaccion
```

manteniendo:

```text
precio_base
```

como atributo informativo proveniente de la dimensión de materiales.

De esta manera:

```text
precio_transaccion
        │
        ▼
cantidad_normalizada_st × precio_transaccion
        │
        ▼
total_revenue
```

mientras que:

```text
precio_base
```

no participa en el cálculo de revenue.

Esta resolución sigue directamente el contrato de Gold, que establece que el revenue debe utilizar el precio de la transacción y no el precio base del catálogo.

### Motivo

La explicitud semántica reduce el riesgo de utilizar accidentalmente el precio incorrecto en futuras transformaciones.

---

# 3. Ambigüedad en la representación de la cantidad normalizada

### Tipo

Ambigüedad resuelta durante la implementación.

### Problema

La arquitectura exige que Gold utilice la cantidad normalizada a ST, pero Bronze conserva la cantidad original y la unidad de origen.

Por ejemplo:

```text
cantidad = 10
unidad = CS
```

no tiene el mismo significado que:

```text
cantidad = 10
unidad = ST
```

### Resolución

Silver genera explícitamente:

```text
cantidad_normalizada_st
```

mediante:

```text
CS → ST
1 CS = 20 ST
```

Por ejemplo:

```text
cantidad = 10
unidad = CS

cantidad_normalizada_st = 200
```

Gold consume exclusivamente:

```text
cantidad_normalizada_st
```

y no vuelve a realizar la conversión.

### Motivo

La transformación pertenece a Silver porque representa una normalización de datos que debe quedar disponible para todos los consumidores downstream.

Esto evita duplicar la regla de conversión en múltiples tablas Gold.

---

# 4. Uso de `is_current` en SCD Type 2

### Tipo

Ambigüedad arquitectónica resuelta en la implementación.

### Problema

La dimensión `dim_materials` contiene:

```text
valid_from
valid_to
is_current
```

Una interpretación incorrecta podría consistir en utilizar solamente:

```text
is_current = true
```

para enriquecer todas las entregas.

Esto produciría resultados incorrectos para registros históricos cuando un material haya cambiado de descripción, categoría o precio.

### Resolución

El enriquecimiento de `fact_deliveries` utiliza la fecha de la entrega y la vigencia de la versión:

```text
valid_from <= fecha_proceso <= valid_to
```

La columna:

```text
is_current
```

se mantiene como indicador informativo, pero no como fuente única de verdad para el join histórico.

Esto corresponde con el requisito explícito de realizar un join temporal y no únicamente un join sobre `is_current`.

### Beneficio

Se preserva la consistencia histórica:

```text
Entrega 2024 → versión del catálogo vigente en 2024
Entrega 2025 → versión del catálogo vigente en 2025
```

---

# 5. Gold como capa derivada y estrategia de idempotencia

### Tipo

Decisión de implementación basada en una consideración de arquitectura.

### Arquitectura propuesta

La arquitectura define Gold como una capa derivada y establece el recomputo por partición de fecha como estrategia de idempotencia.

### Decisión

Se mantiene Gold como una capa no autoritativa.

Para un rango:

```text
2025-01-01 → 2025-06-30
```

el proceso:

1. lee Silver;
2. filtra el rango;
3. recalcula las métricas;
4. sobrescribe el resultado correspondiente.

Esto evita acumular resultados derivados de diferentes ejecuciones.

### Trade-off

La principal ventaja es la simplicidad y reproducibilidad:

```text
mismos datos Silver
+
mismo rango
=
mismo resultado Gold
```

El principal costo es que un reproceso implica recalcular las agregaciones.

Para el volumen de la prueba técnica esta estrategia es adecuada. En un entorno productivo de mayor escala evaluaría estrategias incrementales basadas en particiones o ventanas modificadas.

---

# 6. Manejo de anomalías antes del filtro temporal

### Tipo

Ambigüedad resuelta en la implementación.

### Problema

Existe un rango de ejecución:

```text
start_date
end_date
```

y podría parecer razonable filtrar los registros inmediatamente.

Sin embargo, si un registro tiene:

```text
fecha_proceso = NULL
```

o una fecha inválida, podría desaparecer antes de llegar al proceso de calidad.

### Resolución

El pipeline Silver realiza:

```text
Bronze
  ↓
Normalización de fecha
  ↓
Clasificación de anomalías
  ↓
Cuarentena / válidos / descartados
  ↓
Filtro de rango
```

y no:

```text
Bronze
  ↓
Filtro de fechas
  ↓
Clasificación
```

### Motivo

Una fecha inválida constituye una anomalía que debe ser visible y auditable.

La arquitectura establece explícitamente que las fechas nulas o inválidas deben enviarse a cuarentena.

---

# 7. Mejoras Horizonte 2 — Optimización del procesamiento Spark

### Tipo

Mejora tecnológica propuesta.

La implementación actual se ejecuta localmente:

```text
local[1]
```

Esto es adecuado para reproducibilidad y para el volumen de la prueba, pero no representa necesariamente la configuración de producción.

En una siguiente iteración propondría evaluar:

- Adaptive Query Execution;
- broadcast joins cuando corresponda;
- control de `shuffle partitions`;
- optimización de archivos pequeños;
- `OPTIMIZE`;
- Z-Ordering cuando el patrón de consulta lo justifique;
- métricas de ejecución Spark;
- observabilidad de stages y jobs.

### Beneficio

El objetivo sería reducir:

```text
latencia
+
shuffle
+
I/O
+
costos de computación
```

sin modificar el contrato lógico de las capas.

---

# 8. Mejoras Horizonte 2 — Calidad de datos declarativa

La calidad de datos implementada actualmente permite validar las reglas necesarias para la prueba.

Como siguiente evolución propondría centralizar los contratos de calidad en una estructura declarativa.

Por ejemplo:

```yaml
silver:
  checks:
    - name: valid_quantity
      severity: critical

    - name: valid_price
      severity: critical

    - name: valid_delivery_type
      severity: warning
```

Esto permitiría desacoplar:

```text
regla
+
severidad
+
comportamiento
```

del código Python.

También facilitaría agregar nuevos checks sin modificar significativamente el pipeline.

---

# 9. Mejoras Horizonte 2 — Observabilidad operacional

La siguiente iteración debería ampliar la observabilidad del pipeline.

Además de los `quality_logs`, propondría registrar métricas como:

```text
tenant
batch_id
layer
start_date
end_date
input_records
output_records
quarantined_records
discarded_records
execution_time
status
```

Esto permitiría construir una visión operacional:

```text
             PIPELINE RUN
                  │
       ┌──────────┼──────────┐
       ▼          ▼          ▼
     Bronze     Silver      Gold
       │          │          │
       ▼          ▼          ▼
    records    records    metrics
       │          │          │
       └──────────┼──────────┘
                  ▼
             observability
```

Esto sería especialmente relevante al aumentar el número de tenants.

---

# 10. Mejoras Horizonte 3 — Procesamiento incremental

La implementación actual favorece el recomputo, lo cual simplifica la idempotencia.

A medida que aumente el volumen, propondría evolucionar hacia procesamiento incremental:

```text
Nuevos archivos
      │
      ▼
Detectar cambios
      │
      ▼
Procesar únicamente
particiones afectadas
      │
      ▼
Actualizar Silver
      │
      ▼
Actualizar Gold
```

Para ello podrían evaluarse mecanismos como:

- Delta Change Data Feed;
- Auto Loader;
- Structured Streaming;
- estrategias de watermarking;
- procesamiento incremental por partición.

Esto reduciría el costo de recalcular grandes ventanas históricas.

---

# 11. Mejoras Horizonte 3 — Migración a Databricks + Unity Catalog

La arquitectura final prevista está orientada a:

```text
Databricks
+
ADLS Gen2
+
Unity Catalog
```

La implementación local representa estos componentes mediante:

```text
Spark local
+
filesystem
+
paths por tenant
```

En una evolución productiva propondría migrar los paths hacia tablas gobernadas por Unity Catalog:

```text
saas_dev.bronze_sv.deliveries
saas_dev.silver_sv.fact_deliveries
saas_dev.silver_sv.dim_materials
saas_dev.gold_sv.daily_metrics_by_delivery_type
```

Esto permitiría incorporar:

- gobierno centralizado;
- permisos;
- lineage;
- auditoría;
- políticas de acceso;
- descubrimiento de datos;
- gestión de esquemas.

La arquitectura provista ya contempla este mapeo conceptual.

---

# 12. Resumen de observaciones

| # | Ángulo | Observación | Resolución / propuesta |
|---|---|---|---|
| 1 | Arquitectura | Schema por tenant puede incrementar complejidad a gran escala | Evaluar esquema común + aislamiento mediante políticas |
| 2 | Ambigüedad | Diferencia entre precio transaccional y precio de catálogo | Crear `precio_transaccion` explícito |
| 3 | Ambigüedad | Gold requiere cantidad normalizada | Crear `cantidad_normalizada_st` en Silver |
| 4 | Ambigüedad | Riesgo de usar solo `is_current` en SCD | Join temporal por vigencia |
| 5 | Arquitectura | Gold es derivada | Recomputación para garantizar idempotencia |
| 6 | Ambigüedad | Fechas inválidas podrían perderse por filtro | Clasificar anomalías antes del filtro |
| 7 | Horizonte 2 | Optimización Spark | AQE, shuffle, archivos pequeños, OPTIMIZE |
| 8 | Horizonte 2 | Calidad declarativa | Checks parametrizados mediante YAML |
| 9 | Horizonte 2 | Observabilidad | Métricas operacionales por ejecución |
| 10 | Horizonte 3 | Procesamiento incremental | CDF / Auto Loader / Streaming |
| 11 | Horizonte 3 | Infraestructura productiva | Databricks + ADLS + Unity Catalog |

---

# 13. Conclusión

La arquitectura proporcionada permite construir un MVP sólido para una plataforma de datos multi-tenant. La implementación mantiene la separación de responsabilidades entre las capas:

```text
RAW
  ↓
preservación

BRONZE
  ↓
ingesta + trazabilidad + idempotencia

SILVER
  ↓
calidad + normalización + SCD + enriquecimiento

GOLD
  ↓
métricas de negocio
```

Las principales decisiones adicionales se enfocaron en hacer explícitas las semánticas necesarias para evitar errores downstream, particularmente:

```text
cantidad_normalizada_st
precio_transaccion
```

y en resolver correctamente el enriquecimiento temporal de la dimensión SCD Type 2.

Para una siguiente etapa productiva, las prioridades serían observabilidad, procesamiento incremental, optimización de Spark y migración de los paths locales hacia ADLS Gen2 y Unity Catalog.