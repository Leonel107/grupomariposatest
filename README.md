# SaaS Data Platform

Pipeline de datos **multi-tenant** basado en arquitectura **Medallion**, implementado con **Python, PySpark y Delta Lake**.

La implementación actual cubre las capas:

```text
RAW → BRONZE → SILVER
```

RAW conserva los archivos fuente sin transformación. Bronze realiza la ingesta en Delta Lake preservando las columnas originales y agregando metadata técnica. Silver transforma los datos para consumo analítico mediante limpieza, normalización, manejo de anomalías, enriquecimiento con materiales y modelado SCD Type 2.

---

# 1. Objetivo

El objetivo del proyecto es implementar una plataforma de datos multi-tenant que permita procesar información de entregas de productos manteniendo:

- aislamiento lógico por tenant;
- trazabilidad;
- reproducibilidad;
- idempotencia;
- validaciones automatizadas;
- calidad de datos;
- separación clara entre ingesta y transformación de negocio.

La arquitectura de referencia utiliza el patrón Medallion:

```text
RAW
 │
 ▼
BRONZE
 │
 ▼
SILVER
 │
 ▼
GOLD
```

La implementación actual llega hasta Silver.

---

# 2. Estado actual

| Componente | Estado |
|---|---|
| Python | Implementado |
| PySpark | Implementado |
| Delta Lake | Implementado |
| OmegaConf | Implementado |
| Configuración YAML | Implementada |
| CLI | Implementada |
| RAW | Implementado |
| Bronze | Implementado |
| Idempotencia Bronze | Implementada |
| Validaciones RAW → Bronze | Implementadas |
| Silver | Implementado |
| `fact_deliveries` | Implementado |
| `dim_materials` | Implementado |
| SCD Type 2 | Implementado |
| Manejo de anomalías | Implementado |
| Normalización CS → ST | Implementada |
| Flags de negocio | Implementados |
| Enriquecimiento temporal | Implementado |
| Tests Silver | 24 pruebas exitosas |
| Gold | Pendiente |

---

# 3. Arquitectura

```text
                         ┌───────────────────────┐
                         │         RAW           │
                         │                       │
                         │ CSV original          │
                         │ Sin transformaciones  │
                         └───────────┬───────────┘
                                     │
                                     ▼
                         ┌───────────────────────┐
                         │       BRONZE          │
                         │                       │
                         │ Delta Lake            │
                         │ Esquema RAW           │
                         │ + metadata técnica    │
                         │ Particionado          │
                         └───────────┬───────────┘
                                     │
                                     ▼
              ┌──────────────────────────────────────────┐
              │                  SILVER                   │
              │                                          │
              │  fact_deliveries      dim_materials      │
              │                                          │
              │  Limpieza              SCD Type 2         │
              │  Normalización         Histórico          │
              │  Flags                 Material           │
              │  Anomalías             Descripción        │
              │  Enriquecimiento       Categoría          │
              │                        Precio base        │
              └───────────────────────┬──────────────────┘
                                      │
                                      ▼
                                  GOLD
                             (siguiente etapa)
```

La arquitectura provista define Bronze como una capa de datos ingeridos y Silver como la zona limpia, normalizada y enriquecida para consumo analítico.

---

# 4. Stack tecnológico

| Componente | Versión / tecnología | Uso |
|---|---|---|
| Python | 3.13.2 | Lenguaje de implementación |
| PySpark | 3.5.x | Procesamiento distribuido |
| Delta Lake | 3.3.3 | Almacenamiento transaccional |
| pytest | 9.1.1 | Pruebas automatizadas |
| OmegaConf | Configuración | Gestión jerárquica |
| Git | Git Bash | Control de versiones |

Las versiones utilizadas deben mantenerse sincronizadas con la configuración del proyecto para garantizar reproducibilidad.

---

# 5. Estructura del repositorio

```text
saas-data-platform/
│
├── README.md
├── Makefile
├── pyproject.toml
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── docs/
│   ├── infra.md
│   ├── observations.md
│   └── onboarding-tenant.md
│
├── config/
│   ├── base.yaml
│   │
│   ├── env/
│   │   ├── dev.yaml
│   │   ├── qa.yaml
│   │   └── main.yaml
│   │
│   └── tenants/
│       └── sv.yaml
│
├── data/
│   ├── raw/
│   │   ├── global_mobility_data_entrega_productos.csv
│   │   └── materials_catalog.csv
│   │
│   ├── bronze/
│   │   └── <tenant>/
│   │       └── deliveries/
│   │           └── fecha_proceso=YYYYMMDD/
│   │
│   ├── bronze_quarantine/
│   │
│   ├── silver/
│   │   └── <tenant>/
│   │       ├── fact_deliveries/
│   │       │   └── fecha_proceso=YYYYMMDD/
│   │       │
│   │       └── dim_materials/
│   │
│   ├── silver_quarantine/
│   │   └── <tenant>/
│   │       └── fact_deliveries/
│   │
│   ├── gold/
│   │
│   └── shared/
│       └── quality_logs/
│
├── src/
│   └── saas_pipeline/
│       ├── __init__.py
│       ├── bronze.py
│       ├── silver.py
│       ├── gold.py
│       ├── quality.py
│       ├── config.py
│       └── cli.py
│
├── tests/
│   ├── conftest.py
│   ├── test_spark_environment.py
│   ├── test_bronze.py
│   └── test_silver.py
│
└── mentoring/
    ├── bad_code.py
    ├── good_code.py
    └── code_review.md
```

La estructura mantiene la separación entre código, configuración, datos, pruebas y documentación.

---

# 6. Capa RAW

RAW contiene los archivos originales recibidos desde las fuentes.

Ejemplo:

```text
data/raw/
├── global_mobility_data_entrega_productos.csv
└── materials_catalog.csv
```

RAW no aplica transformaciones de negocio.

Sus principales características son:

- fuente original;
- solo lectura durante el procesamiento;
- conservación de valores originales;
- punto de recuperación y auditoría.

---

# 7. Capa Bronze

Bronze ingesta los datos RAW utilizando PySpark y los almacena como Delta Lake.

La implementación:

- preserva las columnas originales;
- agrega columnas técnicas;
- normaliza `_tenant_id`;
- particiona por `fecha_proceso` y `_tenant_id`;
- soporta procesamiento por tenant;
- soporta rango de fechas;
- utiliza `replaceWhere` para reprocesamiento idempotente.

## Columnas técnicas

```text
_ingestion_timestamp
_source_file
_tenant_id
_batch_id
```

## Ejecución

```bash
python -m saas_pipeline.cli \
  --layer bronze \
  --tenant sv \
  --start-date 2025-01-01 \
  --end-date 2025-06-30
```

---

# 8. Capa Silver

Silver recibe información desde Bronze y aplica las reglas de transformación definidas por la arquitectura.

La capa está compuesta principalmente por:

```text
fact_deliveries
dim_materials
```

## 8.1 `fact_deliveries`

Contiene las transacciones válidas después del procesamiento de Silver.

Se aplican:

- validación de fecha;
- validación de cantidades;
- validación de precio;
- validación de materiales;
- filtrado de tipos de entrega;
- deduplicación exacta;
- normalización de unidades;
- flags de negocio;
- enriquecimiento temporal;
- control de tenant.

La clave de negocio utilizada es:

```text
(_tenant_id,
 fecha_proceso,
 transporte,
 ruta,
 material,
 tipo_entrega)
```

La arquitectura establece `MERGE INTO` como estrategia de idempotencia para esta tabla.

---

# 9. Normalización de unidades

Silver utiliza una unidad común:

```text
ST
```

La conversión definida es:

```text
1 CS = 20 ST
```

Por lo tanto:

```text
cantidad_normalizada_st =
    cantidad * 20
```

cuando:

```text
unidad = CS
```

Los registros que ya se encuentran en ST mantienen su cantidad.

---

# 10. Tipos de entrega

Los únicos tipos de entrega válidos para Silver son:

```text
ZPRE
ZVE1
Z04
Z05
```

Los registros fuera de este conjunto se consideran fuera del alcance analítico y son contabilizados como descartados.

Los flags derivados son:

| Tipo | `is_routine_delivery` | `is_bonus_delivery` |
|---|---:|---:|
| ZPRE | true | false |
| ZVE1 | true | false |
| Z04 | false | true |
| Z05 | false | true |

---

# 11. Manejo de anomalías

Las reglas implementadas son:

| Anomalía | Acción |
|---|---|
| `fecha_proceso` nula o inválida | Cuarentena |
| cantidad nula, negativa o cero | Cuarentena |
| material inexistente en catálogo | Cuarentena |
| `tipo_entrega` fuera de las 4 válidas | Descarte |
| duplicado exacto | Deduplicación |
| precio nulo | Cuarentena |

Las filas enviadas a cuarentena contienen:

```text
_quarantine_reason
```

La arquitectura define explícitamente que las filas en cuarentena deben persistirse en una estructura paralela `<layer>_quarantine_<tenant>.<table>`.

Ejemplo:

```text
data/silver_quarantine/sv/fact_deliveries/
```

Un punto importante de la implementación es que las fechas inválidas se clasifican antes de aplicar un filtro de rango. Esto evita perder anomalías antes de enviarlas a cuarentena.

---

# 12. `dim_materials` — SCD Type 2

El catálogo de materiales se implementa como una dimensión histórica SCD Type 2.

## Clave

```text
material
```

## Atributos versionados

```text
descripcion
categoria
precio_base
```

## Columnas de control

```text
valid_from
valid_to
is_current
```

La arquitectura establece que las transacciones deben utilizar la versión del material vigente en la fecha de la transacción y no simplemente la versión actual.

El enriquecimiento utiliza un join temporal:

```text
fact_deliveries.fecha_proceso
BETWEEN
dim_materials.valid_from
AND
dim_materials.valid_to
```

---

# 13. Idempotencia

La estrategia utilizada por capa es:

```text
BRONZE
    │
    └── overwrite por partición

SILVER fact_deliveries
    │
    └── MERGE por clave de negocio

SILVER dim_materials
    │
    └── SCD Type 2
```

Esto permite reprocesar información sin generar duplicados.

La arquitectura define explícitamente `MERGE INTO` para `fact_deliveries` y para la dimensión SCD Type 2.

---

# 14. Configuración

La configuración utiliza OmegaConf y mantiene separación entre:

```text
config/base.yaml
config/env/<environment>.yaml
config/tenants/<tenant>.yaml
```

La configuración contempla:

```yaml
paths:
  raw:
  bronze:
  silver:
  gold:
  quarantine_root:
  quality_logs:

execution:
  start_date:
  end_date:
  tenant:
  fail_fast:

quality:
  fail_on_critical:
```

El rango de fechas es un parámetro de ejecución y se puede sobrescribir mediante CLI.

Ejemplo:

```bash
--start-date 2025-01-01
--end-date 2025-06-30
```

---

# 15. Reproducibilidad

## 15.1 Crear entorno virtual

Desde Git Bash:

```bash
python -m venv venv
```

Activar:

```bash
source venv/Scripts/activate
```

Verificar:

```bash
python --version
```

Debe mostrar:

```text
Python 3.13.2
```

---

## 15.2 Instalar dependencias

```bash
pip install -e .
```

---

## 15.3 Validar entorno Spark

```bash
pytest tests/test_spark_environment.py -v
```

---

# 16. Ejecutar el pipeline

## Bronze

```bash
python -m saas_pipeline.cli \
  --layer bronze \
  --tenant sv \
  --start-date 2025-01-01 \
  --end-date 2025-06-30
```

## Silver

```bash
python -m saas_pipeline.cli \
  --layer silver \
  --tenant sv \
  --start-date 2025-01-01 \
  --end-date 2025-06-30
```

La ejecución de Silver debe realizarse sobre un Bronze previamente generado.

---

# 17. Tests

## Todos los tests

```bash
pytest -v
```

## Tests Bronze

```bash
pytest tests/test_bronze.py -v
```

## Tests Silver

```bash
pytest tests/test_silver.py -v
```

La implementación actual de Silver cuenta con:

```text
24 pruebas automatizadas exitosas
```

Las pruebas cubren estructura, contenido, tenant, tipos de entrega, unidades, flags, anomalías, fechas, enriquecimiento, metadata y unicidad de la clave de negocio.

---

# 18. Linter

Si el proyecto tiene configurado Ruff:

```bash
ruff check .
```

Para aplicar correcciones automáticas cuando sea apropiado:

```bash
ruff check . --fix
```

El linter debe ejecutarse desde la raíz del repositorio.

---

# 19. Onboarding de un nuevo tenant

El onboarding mantiene la separación definida por la arquitectura.

Por ejemplo, para incorporar:

```text
tenant = hn
```

## Paso 1 — Crear configuración

Crear:

```text
config/tenants/hn.yaml
```

siguiendo la estructura del tenant existente.

---

## Paso 2 — Incorporar la fuente RAW

Agregar los archivos requeridos en:

```text
data/raw/
```

La fuente debe cumplir con las columnas esperadas por el pipeline.

---

## Paso 3 — Ejecutar Bronze

```bash
python -m saas_pipeline.cli \
  --layer bronze \
  --tenant hn \
  --start-date 2025-01-01 \
  --end-date 2025-06-30
```

Se generará:

```text
data/bronze/hn/deliveries/
```

---

## Paso 4 — Validar Bronze

```bash
pytest tests/test_bronze.py -v
```

Las validaciones deben confirmar:

- esquema;
- columnas;
- tipos;
- tenant;
- particiones;
- integridad RAW → Bronze.

---

## Paso 5 — Ejecutar Silver

```bash
python -m saas_pipeline.cli \
  --layer silver \
  --tenant hn \
  --start-date 2025-01-01 \
  --end-date 2025-06-30
```

Se generarán:

```text
data/silver/hn/fact_deliveries/
data/silver/hn/dim_materials/
```

y, cuando corresponda:

```text
data/silver_quarantine/hn/fact_deliveries/
```

---

## Paso 6 — Validar Silver

```bash
pytest tests/test_silver.py -v
```

---

# 20. Flujo de onboarding

```text
                 Nuevo tenant
                      │
                      ▼
             Crear configuración
                      │
                      ▼
              Incorporar RAW
                      │
                      ▼
             Ejecutar Bronze
                      │
                      ▼
             Validar Bronze
                      │
                      ▼
             Ejecutar Silver
                      │
                      ▼
             Validar Silver
                      │
                      ▼
              Tenant habilitado
```

Este procedimiento permite mantener el aislamiento lógico por tenant sin modificar la lógica principal del pipeline.

---

# 21. Validaciones implementadas

## RAW → Bronze

Se validan:

- existencia de Bronze;
- formato Delta;
- existencia de registros;
- columnas técnicas;
- `_tenant_id`;
- tenant esperado;
- particiones;
- columnas RAW;
- ausencia de columnas inesperadas;
- ausencia de columnas duplicadas;
- tipos de datos;
- cantidad de registros;
- integridad de contenido.

## Bronze → Silver

Se validan:

- estructura Delta;
- lectura Delta;
- existencia de registros;
- tenant;
- columnas requeridas;
- tipos de entrega;
- unidades ST;
- flags;
- cantidades;
- precios;
- fechas;
- consistencia tenant/país;
- enriquecimiento de materiales;
- metadata técnica;
- completitud de clave de negocio;
- ausencia de claves duplicadas.

---

# 22. Trazabilidad

Bronze y Silver conservan información técnica para permitir seguimiento de las ejecuciones.

Entre las columnas principales se encuentran:

```text
_ingestion_timestamp
_source_file
_tenant_id
_batch_id
```

Ejemplo:

```text
_ingestion_timestamp = 2026-08-28 19:09:35
_source_file         = global_mobility_data_entrega_productos.csv
_tenant_id           = sv
_batch_id            = <uuid>
```

Esto permite identificar:

- cuándo se procesó el registro;
- cuál fue su archivo fuente;
- a qué tenant pertenece;
- qué ejecución lo generó.

---

# 23. Qué dejé fuera y por qué

La implementación actual se limita deliberadamente a:

```text
RAW → BRONZE → SILVER
```

## 23.1 Capa Gold

Gold no se implementa todavía.

La razón es que el alcance actual termina en Silver y la implementación de Gold se realizará en una etapa posterior.

Por lo tanto, no se han implementado todavía las tablas de métricas agregadas.

---

## 23.2 Procesamiento streaming

No se implementó streaming.

La fuente utilizada en la prueba es un conjunto de archivos CSV y el procesamiento se realiza mediante ejecución batch con PySpark.

Esto permite concentrar el alcance en:

- ingesta;
- transformación;
- calidad;
- idempotencia;
- multi-tenancy.

---

## 23.3 ADLS Gen2

No se implementó almacenamiento físico sobre ADLS Gen2.

La prueba se ejecuta localmente utilizando:

```text
data/
```

Esta estructura representa localmente el almacenamiento que posteriormente podría mapearse a ADLS.

---

## 23.4 Unity Catalog

No se implementó Unity Catalog.

La separación por tenant se representa mediante paths:

```text
data/bronze/sv/
data/silver/sv/
```

en lugar de schemas administrados mediante Unity Catalog.

La arquitectura objetivo define schemas como:

```text
saas_<env>.bronze_<tenant>
saas_<env>.silver_<tenant>
```

pero la prueba utiliza paths locales equivalentes.

---

## 23.5 Infraestructura cloud ejecutable

No se implementó infraestructura cloud funcional.

El proyecto se concentra actualmente en demostrar:

- procesamiento de datos;
- separación por capas;
- aislamiento por tenant;
- trazabilidad;
- idempotencia;
- calidad;
- pruebas automatizadas.

---

## 23.6 Optimización avanzada de Spark

No se incorporaron optimizaciones específicas de grandes clusters productivos como:

- tuning de particiones a escala;
- configuración avanzada de shuffle;
- cluster sizing;
- autoscaling;
- Photon;
- optimización específica de infraestructura cloud.

La ejecución actual es local y el volumen de la prueba no justifica introducir complejidad adicional.

---

# 24. Documentación complementaria

```text
docs/
├── observations.md
├── onboarding-tenant.md
└── infra.md
```

### `observations.md`

Documenta:

- decisiones técnicas;
- ambigüedades;
- trade-offs;
- mejoras realizadas;
- mejoras futuras.

### `onboarding-tenant.md`

Documenta el proceso de incorporación de un nuevo tenant.

### `infra.md`

Documenta la infraestructura objetivo y las consideraciones de despliegue.

---

# 25. Resultado actual

La implementación validada actualmente es:

```text
                    RAW
                     │
                     ▼
                  BRONZE
                     │
        ┌────────────┴────────────┐
        │                         │
   Validaciones             Idempotencia
        │                         │
        └────────────┬────────────┘
                     ▼
                  SILVER
                     │
          ┌──────────┴──────────┐
          │                     │
   fact_deliveries        dim_materials
          │                     │
          │                SCD Type 2
          │                     │
          └──────────┬──────────┘
                     │
              24 tests OK
                     │
                     ▼
             Próxima etapa:
                   GOLD
```

El estado actual demuestra una implementación funcional de RAW, Bronze y Silver, con aislamiento por tenant, procesamiento parametrizable, manejo de anomalías, idempotencia, enriquecimiento temporal y validaciones automatizadas.