# Observaciones y mejoras — RAW → BRONZE → SILVER

## 1. Objetivo

Este documento registra las observaciones, decisiones técnicas y mejoras identificadas durante la implementación de las capas **RAW, BRONZE y SILVER** de la plataforma de datos.

El objetivo es mantener trazabilidad sobre:

- decisiones tomadas durante la implementación;
- diferencias o ambigüedades encontradas respecto de la arquitectura provista;
- criterios utilizados para resolver dichas ambigüedades;
- mejoras tecnológicas identificadas para futuras iteraciones.

La arquitectura de referencia establece una separación clara entre RAW, Bronze, Silver y Gold. RAW conserva los archivos fuente; Bronze preserva el esquema original y agrega metadatos técnicos; Silver constituye la zona limpia, normalizada y enriquecida para consumo analítico.

El alcance actual de este documento comprende exclusivamente las capas **RAW, BRONZE y SILVER**. Las observaciones relacionadas con Gold se documentarán cuando dicha capa sea implementada.

---

# 2. Arquitectura implementada

La implementación actual sigue el siguiente flujo:

```text
                         ┌─────────────────────────┐
                         │          RAW            │
                         │                         │
                         │ CSV original            │
                         │ Sin transformación      │
                         └────────────┬────────────┘
                                      │
                                      │ Ingesta
                                      ▼
                         ┌─────────────────────────┐
                         │        BRONZE           │
                         │                         │
                         │ Delta                   │
                         │ Esquema original        │
                         │ + columnas técnicas     │
                         │ Particionado            │
                         └────────────┬────────────┘
                                      │
                                      │ Transformación
                                      ▼
              ┌─────────────────────────────────────────────┐
              │                   SILVER                    │
              │                                             │
              │  fact_deliveries       dim_materials        │
              │  - Datos limpios        - SCD Type 2        │
              │  - Unidades ST          - Material          │
              │  - Flags                - Descripción       │
              │  - Anomalías            - Categoría         │
              │  - Enriquecimiento      - Precio base      │
              └──────────────────────┬──────────────────────┘
                                     │
                                     ▼
                              GOLD (siguiente etapa)
```

La estructura local utilizada sigue la convención definida por la arquitectura:

```text
data/
├── raw/
│   └── archivos fuente
│
├── bronze/
│   └── <tenant>/
│       └── deliveries/
│           └── fecha_proceso=YYYYMMDD/
│
├── silver/
│   └── <tenant>/
│       ├── fact_deliveries/
│       │   └── fecha_proceso=YYYYMMDD/
│       │
│       └── dim_materials/
│
├── silver_quarantine/
│   └── <tenant>/
│       └── fact_deliveries/
│
└── shared/
    └── quality_logs/
```

La arquitectura especifica que Bronze debe particionarse por `fecha_proceso` y tenant, mientras que `fact_deliveries` debe particionarse por `fecha_proceso` y `dim_materials` no requiere particionamiento debido a su baja cardinalidad.

---

# 3. Observaciones sobre la capa RAW

## OBS-RAW-001 — RAW debe conservar los archivos fuente originales

### Observación

La capa RAW debe considerarse como la representación más cercana posible a la fuente de origen.

Los archivos CSV deben conservarse sin aplicar transformaciones de negocio, filtros, agregaciones, eliminación de registros o modificaciones sobre sus valores originales.

### Importancia

La conservación de los datos originales permite:

- reproducir una ejecución;
- investigar errores;
- comparar RAW contra BRONZE;
- recuperar información ante errores posteriores;
- disponer de una fuente de auditoría;
- validar que las transformaciones posteriores no alteren indebidamente los datos.

### Decisión / mejora aplicada

Se mantiene RAW como zona de aterrizaje de los archivos originales.

No se utiliza RAW como zona para realizar transformaciones de negocio.

---

## OBS-RAW-002 — No utilizar pandas para procesar RAW → BRONZE

### Observación

El procesamiento de los archivos debe realizarse utilizando Spark, evitando cargar el dataset completo en memoria mediante pandas.

### Importancia

El uso de pandas limita la escalabilidad del pipeline y puede provocar problemas de memoria cuando el volumen de datos aumente.

Además, el objetivo de la arquitectura es mantener un procesamiento distribuido mediante Spark.

### Decisión / mejora aplicada

La lectura y procesamiento de los archivos RAW se realiza mediante:

```python
spark.read.csv(...)
```

y las transformaciones mediante operaciones nativas de Spark DataFrame.

No se utiliza pandas para el procesamiento de RAW → BRONZE.

---

## OBS-RAW-003 — Evitar modificaciones sobre el archivo fuente

### Observación

No se debe modificar físicamente el archivo CSV original para solucionar problemas de formato, tipos o calidad de datos durante el proceso de ingestión.

### Importancia

Modificar el archivo fuente rompe la trazabilidad y dificulta determinar qué información llegó originalmente desde el sistema origen.

### Mejora recomendada

Cualquier normalización necesaria debe realizarse en una capa posterior al aterrizaje, manteniendo intacto el archivo RAW.

---

## OBS-RAW-004 — Validación de existencia de archivos fuente

### Observación

El pipeline depende de la existencia de los archivos esperados dentro de `data/raw`.

### Importancia

Una ejecución sin el archivo requerido podría producir errores poco descriptivos o generar resultados incompletos.

### Mejora recomendada

Incorporar validaciones explícitas al inicio del proceso para verificar:

- existencia del archivo;
- nombre esperado;
- extensión;
- tamaño del archivo;
- existencia de encabezados;
- estructura mínima esperada.

Esto permitiría fallar rápidamente con un mensaje funcional y no con un error interno de Spark.

---

## OBS-RAW-005 — Validación de esquema del archivo fuente

### Observación

La inferencia automática de esquema mediante:

```python
.option("inferSchema", True)
```

es conveniente durante el desarrollo, pero puede producir diferencias de tipo dependiendo del contenido del archivo.

### Importancia

La inferencia de tipos puede generar comportamientos diferentes si el contenido de los archivos cambia entre ejecuciones.

### Mejora recomendada

Definir progresivamente un esquema explícito para los archivos RAW cuando el contrato de datos del origen esté formalizado.

La implementación actual mantiene la inferencia para facilitar la ejecución de la prueba técnica.

---

# 4. Observaciones sobre la capa BRONZE

## OBS-BRZ-001 — BRONZE debe conservar todas las columnas originales

### Observación

Bronze debe preservar las columnas provenientes de RAW y agregar únicamente las columnas técnicas requeridas.

### Importancia

Bronze funciona como una copia persistente y trazable de la fuente, por lo que eliminar o modificar columnas originales dificulta la auditoría y la recuperación.

### Decisión / mejora aplicada

Se implementaron validaciones automatizadas que comprueban:

- presencia de todas las columnas RAW;
- ausencia de columnas inesperadas;
- ausencia de columnas duplicadas;
- tipos de datos esperados;
- presencia de columnas técnicas.

La validación RAW → Bronze se ejecutó exitosamente.

---

## OBS-BRZ-002 — Particionado por fecha y tenant

### Observación

La arquitectura establece que Bronze debe particionarse mediante `fecha_proceso` y `_tenant_id`.

### Decisión / mejora aplicada

La escritura Bronze utiliza particionamiento por:

```text
fecha_proceso
_tenant_id
```

Esto permite:

- aislamiento lógico por tenant;
- lectura eficiente por rango de fechas;
- reprocesamiento controlado;
- alineamiento con la arquitectura definida.

---

## OBS-BRZ-003 — Idempotencia mediante overwrite por partición

### Observación

La arquitectura define `replaceWhere` o mecanismo equivalente para evitar duplicados durante reprocesos.

### Decisión / mejora aplicada

Bronze utiliza:

```text
mode("overwrite")
replaceWhere(...)
```

limitando el reemplazo al tenant y rango de fechas procesado.

Una segunda ejecución del mismo rango no genera duplicados.

---

## OBS-BRZ-004 — Trazabilidad mediante columnas técnicas

### Observación

La arquitectura requiere:

```text
_ingestion_timestamp
_source_file
_tenant_id
_batch_id
```

### Decisión / mejora aplicada

Estas columnas se incorporan durante la ingestión Bronze.

Esto permite identificar:

- cuándo fue procesado el registro;
- desde qué archivo provino;
- a qué tenant pertenece;
- qué ejecución produjo el registro.

---

## OBS-BRZ-005 — Las anomalías de negocio permanecen fuera de Bronze

### Observación

Durante la revisión de la arquitectura se identificó que las reglas de anomalías corresponden a la lógica de procesamiento de Silver.

### Decisión / mejora aplicada

Bronze no descarta ni pone en cuarentena registros por:

- cantidades inválidas;
- precios nulos;
- materiales inexistentes;
- tipos de entrega fuera del alcance analítico.

La responsabilidad de estas reglas se mantiene en Silver, donde pueden clasificarse, auditarse y persistirse de acuerdo con la política definida.

Esta decisión evita convertir Bronze en una capa de transformación de negocio y mantiene su función como capa de ingesta y trazabilidad.

---

# 5. Observaciones sobre la capa SILVER

## OBS-SLV-001 — La arquitectura no especifica completamente el orden de aplicación de las reglas de calidad

### Observación

La arquitectura define las reglas de anomalías, normalización, filtrado y enriquecimiento, pero no establece explícitamente el orden exacto en que deben ejecutarse.

Esto es particularmente importante para la fecha, ya que una fecha inválida no debe eliminarse antes de ser clasificada como anomalía.

### Decisión / resolución aplicada

Se estableció el siguiente orden lógico:

```text
BRONZE
   │
   ▼
Normalización de fecha
   │
   ▼
Clasificación de anomalías
   │
   ├──────────────► Cuarentena
   │
   ├──────────────► Descarte
   │
   ▼
Registros válidos
   │
   ▼
Deduplicación exacta
   │
   ▼
Normalización de unidades
   │
   ▼
Flags de negocio
   │
   ▼
Enriquecimiento temporal
   │
   ▼
fact_deliveries
```

No se aplica el filtro de rango de fechas antes de clasificar las anomalías de fecha.

### Justificación

De esta manera, una fecha nula o inválida no desaparece silenciosamente y puede ser enviada a cuarentena con `_quarantine_reason`.

---

## OBS-SLV-002 — La definición de anomalías se centraliza en Silver

### Observación

La arquitectura define seis categorías principales de anomalías:

- `fecha_proceso` nula o inválida;
- cantidad nula, negativa o cero;
- material inexistente en catálogo;
- `tipo_entrega` fuera del conjunto permitido;
- duplicados exactos;
- precio nulo.

La arquitectura también define acciones diferentes para cada una: cuarentena, descarte o deduplicación.

### Decisión / mejora aplicada

La implementación clasifica los registros antes de generar `fact_deliveries`.

Las filas enviadas a cuarentena conservan la información disponible y agregan:

```text
_quarantine_reason
```

Los tipos de entrega fuera de alcance son contabilizados como descartados y no se incorporan a Silver.

Los duplicados exactos se deduplican conservando una sola copia.

### Beneficio

La lógica permite diferenciar entre:

```text
ERROR DE DATOS → CUARENTENA

FUERA DE ALCANCE → DESCARTE

DUPLICADO EXACTO → DEDUPLICACIÓN
```

en lugar de tratar todos los registros inválidos de la misma forma.

---

## OBS-SLV-003 — Uso de una clave de negocio compuesta para fact_deliveries

### Observación

La arquitectura define la siguiente clave de negocio para `fact_deliveries`:

```text
(_tenant_id,
 fecha_proceso,
 transporte,
 ruta,
 material,
 tipo_entrega)
```

y establece que Silver debe utilizar `MERGE INTO` para actualizar registros existentes o insertar nuevos.

### Decisión / mejora aplicada

La implementación utiliza esta clave como criterio de idempotencia para `fact_deliveries`.

Esto permite que una reejecución del mismo período no genere registros duplicados.

### Trade-off

Una clave compuesta de seis columnas puede resultar más costosa que una clave técnica única, pero mantiene explícita la granularidad de negocio y evita introducir una identidad artificial que no está definida por la fuente.

---

## OBS-SLV-004 — Normalización de unidades en Silver

### Observación

La fuente contiene unidades diferentes, mientras que la arquitectura requiere una unidad común:

```text
1 CS = 20 ST
```

Todos los registros analíticos de Silver deben quedar expresados en ST.

### Decisión / mejora aplicada

La conversión se realiza mediante operaciones nativas de Spark.

Conceptualmente:

```text
unidad = CS
cantidad = X

        ↓

cantidad_normalizada_st = X * 20
unidad = ST
```

Los registros originalmente expresados en ST mantienen su cantidad.

### Beneficio

Las capas posteriores pueden realizar agregaciones sin mezclar unidades de medida diferentes.

---

## OBS-SLV-005 — Flags de negocio derivados de tipo_entrega

### Observación

La arquitectura requiere dos indicadores:

```text
is_routine_delivery
is_bonus_delivery
```

### Decisión / mejora aplicada

Se implementaron los flags mediante expresiones Spark:

```text
ZPRE, ZVE1 → is_routine_delivery = true

Z04, Z05 → is_bonus_delivery = true
```

Los tipos de entrega fuera del conjunto permitido son tratados previamente como descartados.

### Beneficio

La clasificación queda materializada en Silver y no necesita ser reconstruida por cada consumidor.

---

## OBS-SLV-006 — SCD Type 2 para dim_materials

### Observación

La arquitectura establece que `dim_materials` debe utilizar SCD Type 2 con:

```text
Clave:
material

Atributos versionados:
descripcion
categoria
precio_base

Control:
valid_from
valid_to
is_current
```

y establece que una sola versión debe permanecer como `is_current = true` para cada SKU.

### Decisión / mejora aplicada

La dimensión se implementa manteniendo las versiones históricas del material en lugar de sobrescribir directamente los atributos.

Esto permite conservar el estado histórico del catálogo.

### Trade-off

SCD Type 2 incrementa la cantidad de registros de la dimensión y la complejidad del procesamiento frente a un modelo overwrite, pero permite reconstruir correctamente el estado histórico del catálogo.

---

## OBS-SLV-007 — Enriquecimiento mediante join temporal

### Observación

Una ambigüedad importante de la arquitectura podría surgir al utilizar únicamente:

```text
is_current = true
```

para enriquecer las transacciones.

Esto sería incorrecto para transacciones históricas.

### Decisión / resolución aplicada

El enriquecimiento utiliza la fecha de la transacción y el período de vigencia de la dimensión:

```text
fact_deliveries.fecha_proceso
BETWEEN
dim_materials.valid_from
AND
dim_materials.valid_to
```

La arquitectura establece explícitamente este comportamiento.

### Justificación

De esta manera, una transacción histórica utiliza el precio y atributos del material correspondientes a la fecha en que ocurrió la operación y no necesariamente los atributos actuales.

---

## OBS-SLV-008 — Separación entre precio de transacción y precio del catálogo

### Observación

La dimensión contiene `precio_base`, mientras que las transacciones contienen su propio `precio`.

No deben tratarse como la misma información.

### Decisión / mejora aplicada

Silver conserva ambos conceptos:

```text
precio
    → precio asociado a la transacción

precio_base
    → precio proveniente del catálogo para la versión
      correspondiente al período
```

### Beneficio

Esto evita perder el contexto histórico del catálogo y permite diferenciar posteriormente el precio transaccional del precio de referencia.

---

## OBS-SLV-009 — `_tenant_id` como identificador técnico de tenant

### Observación

La arquitectura utiliza códigos de tenant en minúscula y establece `_tenant_id` como columna técnica.

### Decisión / mejora aplicada

La implementación mantiene:

```text
SV → sv
HN → hn
```

y utiliza:

```text
_tenant_id
```

como identificador técnico consistente entre las capas.

Esto evita depender directamente del valor original de `pais` para identificar el tenant.

---

## OBS-SLV-010 — Validaciones automatizadas como contrato de la capa

### Observación

La implementación de Silver fue acompañada por pruebas automatizadas que validan estructura, contenido y reglas de transformación.

Actualmente se cuenta con **24 pruebas automatizadas exitosas**.

### Controles implementados

Entre las validaciones se encuentran:

- estructura Delta;
- lectura como Delta;
- existencia de datos;
- `_tenant_id`;
- aislamiento de tenant;
- ausencia de columnas duplicadas;
- columnas requeridas;
- tipos de entrega válidos;
- unidades normalizadas a ST;
- flags de entrega;
- consistencia de flags;
- cantidades válidas;
- precios válidos;
- fechas válidas;
- consistencia tenant/país;
- enriquecimiento de descripción;
- enriquecimiento de categoría;
- enriquecimiento de precio base;
- existencia de materiales enriquecidos;
- `_batch_id`;
- `_ingestion_timestamp`;
- completitud de la clave de negocio;
- ausencia de claves de negocio duplicadas.

### Beneficio

La batería de pruebas funciona como una barrera de regresión antes de avanzar con cambios posteriores.

---

# 6. Mejoras tecnológicas — Horizonte 2

## OBS-SLV-H2-001 — Separar reglas de negocio de la implementación Python

### Observación

Actualmente las reglas de negocio están expresadas mediante lógica Spark dentro del pipeline.

### Mejora propuesta

Externalizar progresivamente reglas como:

```text
tipos_entrega_validos
factor_CS_ST
columnas_clave
reglas_de_anomalias
```

hacia configuración controlada.

### Beneficio

Permitiría modificar reglas sin alterar directamente el código de procesamiento.

### Trade-off

Una mayor parametrización introduce complejidad de configuración y requiere validaciones adicionales sobre los archivos YAML.

---

## OBS-SLV-H2-002 — Implementar un framework formal de Data Quality

### Observación

Las pruebas automatizadas actuales validan el resultado del pipeline, pero un entorno productivo requiere además persistir los resultados de los controles de calidad como métricas operativas.

La arquitectura define una tabla compartida `quality_logs` con información como `_run_id`, `_batch_id`, `tenant_id`, `layer`, `check_name`, severidad, registros evaluados y registros fallidos.

### Mejora propuesta

Implementar un componente reutilizable de Data Quality que permita declarar checks como:

```text
check_name
check_severity
condition
records_checked
records_failed
```

y persistir automáticamente el resultado.

### Beneficio

Permitiría centralizar observabilidad de calidad entre tenants y capas.

---

## OBS-SLV-H2-003 — Incorporar métricas de procesamiento y anomalías

### Observación

Además del resultado final, es útil conocer cuánto volumen fue:

```text
procesado
aceptado
enviado a cuarentena
descartado
deduplicado
```

### Mejora propuesta

Persistir métricas por ejecución y tenant.

Ejemplo:

```text
tenant_id
batch_id
source_records
valid_records
quarantine_records
discarded_records
deduplicated_records
processed_at
```

### Beneficio

Facilita monitoreo operacional y detección de cambios anómalos en el volumen de datos.

---

# 7. Mejoras tecnológicas — Horizonte 3

## OBS-PLT-H3-001 — Migración del almacenamiento local hacia ADLS Gen2 + Unity Catalog

### Observación

La prueba técnica utiliza paths locales para reproducir la arquitectura, mientras que el diseño objetivo está basado en Databricks, ADLS Gen2 y Unity Catalog.

La arquitectura define schemas separados por tenant dentro de un catálogo por ambiente.

### Mejora propuesta

Migrar:

```text
data/bronze/
data/silver/
data/shared/
```

hacia almacenamiento cloud administrado.

La estructura lógica de nombres se mantendría:

```text
saas_<env>.silver_<tenant>.fact_deliveries
saas_<env>.silver_<tenant>.dim_materials
```

### Beneficio

Se obtendrían:

- gobierno centralizado;
- control de acceso;
- auditoría;
- escalabilidad;
- integración con workloads analíticos.

---

## OBS-PLT-H3-002 — Incorporar procesamiento incremental y observabilidad productiva

### Mejora propuesta

En un entorno productivo, el procesamiento podría evolucionar desde ejecución batch local hacia mecanismos incrementales y orquestados.

La arquitectura contempla como evolución posible el uso de tecnologías como Auto Loader/streaming. Esta mejora debe evaluarse según volumen, frecuencia de llegada y SLA.

### Beneficio

Reduciría procesamiento innecesario y permitiría responder más rápidamente ante nuevas entregas de datos.

### Trade-off

Incrementaría significativamente la complejidad operacional frente al procesamiento batch actual.

---

# 8. Resumen de decisiones

| Área | Decisión |
|---|---|
| RAW | Mantener archivos originales sin transformación |
| Bronze | Preservar columnas RAW |
| Bronze | Agregar únicamente metadatos técnicos |
| Bronze | Particionar por fecha y tenant |
| Bronze | `overwrite + replaceWhere` para idempotencia |
| Silver | Clasificar anomalías antes de aplicar filtros que puedan ocultarlas |
| Silver | Cuarentena para errores de datos |
| Silver | Descarte para tipos de entrega fuera de alcance |
| Silver | Deduplicación exacta |
| Silver | Normalización CS → ST |
| Silver | Flags derivados de `tipo_entrega` |
| Silver | `fact_deliveries` con clave de negocio compuesta |
| Silver | `dim_materials` como SCD Type 2 |
| Silver | Join temporal para enriquecimiento |
| Silver | `_tenant_id` como identificador técnico |
| Silver | 24 pruebas automatizadas exitosas |
| Futuro | Data Quality operacional |
| Futuro | ADLS Gen2 + Unity Catalog |
| Futuro | Procesamiento incremental |