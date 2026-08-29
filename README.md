# SaaS Data Platform

Pipeline de datos **multi-tenant** basado en arquitectura **Medallion**, implementado con **Python, PySpark y Delta Lake**.

La solución implementa el flujo completo:

```text
RAW → BRONZE → SILVER → GOLD
```

El objetivo es disponer de una plataforma reproducible para la ingesta, transformación, validación y agregación de datos de entregas para múltiples tenants, manteniendo aislamiento lógico, trazabilidad, calidad e idempotencia.

---

## 1. Arquitectura

La plataforma sigue una arquitectura Medallion con separación lógica por tenant.

```text
                         ┌──────────────────────┐
                         │      FUENTE CSV      │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │         RAW          │
                         │                      │
                         │ Archivo original     │
                         │ Sin transformación   │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │       BRONZE         │
                         │                      │
                         │ Delta Lake           │
                         │ Esquema original     │
                         │ + columnas técnicas  │
                         │ Particionado         │
                         │ Idempotencia         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │        SILVER        │
                         │                      │
                         │ fact_deliveries      │
                         │ Normalización         │
                         │ Anomalías             │
                         │ SCD Type 2             │
                         │ Enriquecimiento       │
                         │ Calidad               │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │         GOLD         │
                         │                      │
                         │ daily_metrics_       │
                         │ by_delivery_type     │
                         │                      │
                         │ Métricas de negocio  │
                         └──────────────────────┘
```

La arquitectura provista establece que cada capa lea únicamente de la capa inmediatamente anterior y que Silver sea la zona de verdad para el consumo analítico downstream.

---

## 2. Objetivos de la implementación

La solución cubre los siguientes objetivos:

- Ingesta de archivos CSV.
- Preservación de los datos originales.
- Almacenamiento en Delta Lake.
- Trazabilidad mediante columnas técnicas.
- Aislamiento lógico por tenant.
- Procesamiento parametrizable por tenant.
- Procesamiento parametrizable por rango de fechas.
- Idempotencia.
- Normalización de unidades.
- Validación y clasificación de anomalías.
- Cuarentena de registros inválidos.
- Deduplicación de registros exactos.
- Dimensión `dim_materials` con SCD Type 2.
- Enriquecimiento temporal de entregas con información del catálogo.
- Cálculo de métricas Gold.
- Pruebas automatizadas para las capas implementadas.
- Configuración mediante YAML y OmegaConf.

---

# 3. Stack tecnológico

| Componente | Tecnología / versión | Uso |
|---|---|---|
| Python | 3.13.2 | Lenguaje de implementación |
| PySpark | 3.5.x | Procesamiento distribuido |
| Delta Lake | 3.3.3 | Almacenamiento transaccional |
| pytest | 9.1.1 | Pruebas automatizadas |
| OmegaConf | Configuración | Configuración jerárquica |
| Ruff / linter configurado | Python | Calidad de código |
| Git / GitHub | Control de versiones | Versionamiento y CI/CD |

Las versiones principales utilizadas se mantienen alineadas con el entorno empleado durante la implementación para favorecer la reproducibilidad.

---

# 4. Estructura del repositorio

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
│   │   └── *.csv
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
│   │       └── dim_materials/
│   │
│   ├── silver_quarantine/
│   │   └── <tenant>/
│   │       └── fact_deliveries/
│   │
│   ├── gold/
│   │   └── <tenant>/
│   │       └── daily_metrics_by_delivery_type/
│   │
│   └── shared/
│       └── quality_logs/
│
├── docs/
│   ├── infra.md
│   ├── observations.md
│   └── onboarding-tenant.md
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
│   ├── test_silver.py
│   └── test_gold.py
│
└── mentoring/
    ├── bad_code.py
    ├── good_code.py
    └── code_review.md
```

El diseño sigue la estructura propuesta por la prueba técnica, donde los datos generados no deben formar parte del versionamiento del repositorio.

---

# 5. Capas de datos

## 5.1 RAW

RAW contiene los archivos recibidos desde la fuente sin transformación de negocio.

Ejemplo:

```text
data/raw/
└── global_mobility_data_entrega_productos.csv
```

Características:

- Mantiene el archivo original.
- No normaliza unidades.
- No elimina registros.
- No aplica reglas de negocio.
- No realiza agregaciones.
- No realiza enriquecimientos.

El objetivo es conservar una representación fiel de la fuente antes de iniciar el procesamiento.

---

# 6. Capa Bronze

Bronze ingesta los datos RAW mediante PySpark y los almacena en Delta Lake.

Ejemplo:

```text
data/bronze/
└── sv/
    └── deliveries/
        ├── fecha_proceso=20250101/
        ├── fecha_proceso=20250102/
        └── ...
```

Bronze conserva las columnas originales:

```text
pais
fecha_proceso
transporte
ruta
tipo_entrega
material
precio
cantidad
unidad
```

y agrega las columnas técnicas:

```text
_ingestion_timestamp
_source_file
_tenant_id
_batch_id
```

Estas columnas permiten identificar cuándo se ingirió el registro, desde qué archivo provino, a qué tenant pertenece y qué ejecución generó el registro.

### Particionado

Bronze se organiza por:

```text
fecha_proceso
_tenant_id
```

La estrategia de particionado permite procesar y reprocesar conjuntos de datos específicos de fecha y tenant.

### Idempotencia

Bronze utiliza overwrite de las particiones correspondientes al rango procesado.

Una segunda ejecución del mismo rango no debe generar registros duplicados.

Conceptualmente:

```text
Primera ejecución

Bronze
├── fecha=20250401
├── fecha=20250402
└── fecha=20250403


Segunda ejecución

Bronze
├── fecha=20250401 ← sobrescrita
├── fecha=20250402 ← sobrescrita
└── fecha=20250403 ← sobrescrita
```

Esta estrategia sigue la estrategia de idempotencia definida en la arquitectura.

---

# 7. Capa Silver

Silver constituye la zona de verdad para los datos analíticos.

Las principales tablas son:

```text
fact_deliveries
dim_materials
```

## 7.1 `fact_deliveries`

Ruta:

```text
data/silver/<tenant>/fact_deliveries/
```

La tabla se construye a partir de Bronze y aplica las reglas de transformación definidas para la prueba.

### Normalización de unidades

Los registros cuya unidad es `CS` se convierten a `ST` utilizando:

```text
1 CS = 20 ST
```

La cantidad normalizada queda disponible en:

```text
cantidad_normalizada_st
```

Todos los registros válidos quedan expresados en una unidad común.

### Precio transaccional

El precio original de la transacción se conserva explícitamente como:

```text
precio_transaccion
```

Este campo representa el precio utilizado posteriormente por Gold para calcular revenue.

El precio del catálogo:

```text
precio_base
```

se mantiene como información de referencia y no sustituye al precio de la transacción.

### Tipos de entrega

Solo se consideran válidos:

```text
ZPRE
ZVE1
Z04
Z05
```

Los flags generados son:

```text
is_routine_delivery
is_bonus_delivery
```

donde:

```text
ZPRE, ZVE1 → is_routine_delivery = true

Z04, Z05 → is_bonus_delivery = true
```

### Manejo de anomalías

Las anomalías se clasifican antes de aplicar el rango de fechas para permitir que registros con fechas inválidas lleguen correctamente a cuarentena.

Entre las reglas implementadas se encuentran:

| Anomalía | Acción |
|---|---|
| Fecha nula/inválida | Cuarentena |
| Cantidad nula | Cuarentena |
| Cantidad negativa/cero | Cuarentena |
| Material inexistente | Cuarentena |
| Precio nulo | Cuarentena |
| Tipo de entrega no válido | Descarte |
| Registro exactamente duplicado | Deduplicación |

La arquitectura establece explícitamente que las filas en cuarentena deben conservar una razón mediante `_quarantine_reason`, mientras que los descartes deben contabilizarse.

### SCD Type 2

`dim_materials` se implementa como una dimensión SCD Type 2.

La clave de negocio es:

```text
material
```

Los atributos versionados son:

```text
descripcion
categoria
precio_base
```

y se utilizan las columnas:

```text
valid_from
valid_to
is_current
```

La implementación permite mantener diferentes versiones del mismo material a través del tiempo.

### Enriquecimiento temporal

`fact_deliveries` se enriquece utilizando la dimensión de materiales considerando la vigencia temporal del registro.

No se utiliza únicamente `is_current`, ya que una entrega histórica debe asociarse con la versión del catálogo vigente en la fecha correspondiente.

La arquitectura exige explícitamente un join temporal con la dimensión SCD Type 2.

---

# 8. Capa Gold

La implementación construye:

```text
daily_metrics_by_delivery_type
```

Ruta:

```text
data/gold/<tenant>/daily_metrics_by_delivery_type/
```

La granularidad es:

```text
(_tenant_id, fecha_proceso, tipo_entrega)
```

Es decir, existe como máximo una fila por combinación de:

```text
tenant + fecha + tipo_entrega
```

## 8.1 Métricas

### `total_units`

Suma de:

```text
cantidad_normalizada_st
```

No se utiliza la cantidad original de Bronze.

```text
total_units =
SUM(cantidad_normalizada_st)
```

### `total_revenue`

Se calcula utilizando el precio de la transacción:

```text
total_revenue =
SUM(
    cantidad_normalizada_st
    * precio_transaccion
)
```

No se utiliza `precio_base`.

Esta distinción es importante porque `precio_base` representa información del catálogo, mientras que `precio_transaccion` representa el precio utilizado en la operación.

### `active_routes`

Número de rutas distintas:

```text
COUNT(DISTINCT ruta)
```

### `active_transports`

Número de transportes distintos:

```text
COUNT(DISTINCT transporte)
```

Las cuatro métricas corresponden al contrato definido para Gold.

---

# 9. Idempotencia Gold

Gold es una capa derivada y no autoritativa.

Por ello, se utiliza una estrategia de recomputación del rango procesado.

```text
Silver
   │
   ▼
Filtrar rango
   │
   ▼
Agrupar
   │
   ▼
Recalcular métricas
   │
   ▼
Gold
```

Una segunda ejecución del mismo tenant y rango debe producir el mismo resultado lógico sin duplicar agregados.

Esta estrategia corresponde con la arquitectura provista, que define Gold como una capa derivada cuyo contenido puede recomputarse.

---

# 10. Multi-tenant

El pipeline permite ejecutar:

### Tenant específico

```powershell
python -m saas_pipeline.cli --layer bronze --tenant sv --start-date 2025-01-01 --end-date 2025-06-30
```

```powershell
python -m saas_pipeline.cli --layer silver --tenant sv --start-date 2025-01-01 --end-date 2025-06-30
```

```powershell
python -m saas_pipeline.cli --layer gold --tenant sv --start-date 2025-01-01 --end-date 2025-06-30
```

### Todos los tenants

```powershell
python -m saas_pipeline.cli --layer bronze --tenant all --start-date 2025-01-01 --end-date 2025-06-30
```

```powershell
python -m saas_pipeline.cli --layer silver --tenant all --start-date 2025-01-01 --end-date 2025-06-30
```

```powershell
python -m saas_pipeline.cli --layer gold --tenant all --start-date 2025-01-01 --end-date 2025-06-30
```

El aislamiento lógico se mantiene mediante:

```text
data/
├── bronze/<tenant>/
├── silver/<tenant>/
└── gold/<tenant>/
```

La arquitectura productiva prevista utiliza schemas independientes por tenant dentro de Unity Catalog; para esta prueba, dichos schemas se representan mediante paths locales.

---

# 11. Configuración

La configuración se encuentra separada del código mediante YAML y OmegaConf.

```text
config/
├── base.yaml
├── env/
│   ├── dev.yaml
│   ├── qa.yaml
│   └── main.yaml
└── tenants/
    └── sv.yaml
```

Los parámetros principales incluyen:

- paths de datos;
- ambiente;
- tenant;
- fechas de procesamiento;
- parámetros de calidad;
- configuración de ejecución.

Esto permite modificar parámetros de ejecución sin modificar la lógica del pipeline.

---

# 12. Requisitos previos

Se requiere:

- Python 3.13.2.
- Java compatible con la versión de Spark utilizada.
- Git.
- PowerShell, CMD o terminal equivalente.

Verificar Python:

```powershell
python --version
```

Verificar Java:

```powershell
java -version
```

---

# 13. Instalación

## 13.1 Clonar repositorio

```powershell
git clone https://github.com/Leonel107/grupomariposatest.git
cd grupomariposatest
```

## 13.2 Crear entorno virtual

```powershell
python -m venv venv
```

## 13.3 Activar entorno

PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
```

CMD:

```cmd
venv\Scripts\activate
```

## 13.4 Instalar dependencias

```powershell
python -m pip install --upgrade pip
pip install -e .
```

---

# 14. Verificar Spark + Delta

Ejecutar:

```powershell
pytest tests/test_spark_environment.py -v
```

La prueba debe finalizar correctamente.

---

# 15. Ejecución completa del pipeline

El pipeline debe ejecutarse respetando el orden de las capas:

```text
RAW
 │
 ▼
Bronze
 │
 ▼
Silver
 │
 ▼
Gold
```

Para el tenant `sv`:

### Bronze

```powershell
python -m saas_pipeline.cli --layer bronze --tenant sv --start-date 2025-01-01 --end-date 2025-06-30
```

### Silver

```powershell
python -m saas_pipeline.cli --layer silver --tenant sv --start-date 2025-01-01 --end-date 2025-06-30
```

### Gold

```powershell
python -m saas_pipeline.cli --layer gold --tenant sv --start-date 2025-01-01 --end-date 2025-06-30
```

---

# 16. Pruebas automatizadas

Las pruebas se encuentran en:

```text
tests/
├── conftest.py
├── test_spark_environment.py
├── test_bronze.py
├── test_silver.py
└── test_gold.py
```

## Todos los tests

```powershell
pytest -v
```

## Bronze

```powershell
pytest tests/test_bronze.py -v
```

## Silver

```powershell
pytest tests/test_silver.py -v
```

## Gold

```powershell
pytest tests/test_gold.py -v
```

## Entorno Spark + Delta

```powershell
pytest tests/test_spark_environment.py -v
```

Las pruebas de Silver cubren, entre otros aspectos:

- estructura Delta;
- lectura Delta;
- presencia de datos;
- aislamiento de tenant;
- ausencia de columnas duplicadas;
- columnas requeridas;
- tipos de entrega;
- normalización de unidades;
- flags de negocio;
- cantidades y precios válidos;
- fechas;
- consistencia tenant/país;
- enriquecimiento de materiales;
- batch ID;
- timestamp de ingesta;
- claves de negocio;
- duplicados.

Las pruebas de Gold validan:

- estructura Delta;
- lectura de la tabla;
- existencia de datos;
- `_tenant_id`;
- granularidad;
- columnas requeridas;
- `total_units`;
- `total_revenue`;
- uso de precio transaccional;
- rutas activas;
- transportes activos;
- consistencia de los cálculos;
- aislamiento entre tenants;
- comportamiento ante múltiples ejecuciones.

---

# 17. Linter

El proyecto debe validarse localmente con el linter configurado en `pyproject.toml`.

Por ejemplo, si se utiliza Ruff:

```powershell
ruff check .
```

Para aplicar correcciones automáticas cuando corresponda:

```powershell
ruff check . --fix
```

El objetivo es detectar problemas de:

- estilo;
- imports;
- código no utilizado;
- errores potenciales;
- convenciones Python.

---

# 18. CI/CD

El repositorio contempla GitHub Actions:

```text
.github/
└── workflows/
    └── ci.yml
```

El workflow debe ejecutarse en:

```text
push
pull_request
```

y validar como mínimo:

```text
Linter
   │
   ▼
Tests
   │
   ▼
Resultado CI
```

Esto permite evitar que cambios que rompan el pipeline o sus pruebas lleguen a la rama principal.

---

# 19. Onboarding de un nuevo tenant

El diseño permite incorporar un nuevo tenant sin modificar la lógica principal del pipeline.

Supongamos el tenant:

```text
hn
```

## Paso 1 — Crear configuración

Agregar:

```text
config/tenants/hn.yaml
```

siguiendo la estructura de `sv.yaml`.

## Paso 2 — Incorporar fuente RAW

Agregar los archivos correspondientes a:

```text
data/raw/
```

## Paso 3 — Ejecutar Bronze

```powershell
python -m saas_pipeline.cli --layer bronze --tenant hn --start-date 2025-01-01 --end-date 2025-06-30
```

## Paso 4 — Ejecutar Silver

```powershell
python -m saas_pipeline.cli --layer silver --tenant hn --start-date 2025-01-01 --end-date 2025-06-30
```

## Paso 5 — Ejecutar Gold

```powershell
python -m saas_pipeline.cli --layer gold --tenant hn --start-date 2025-01-01 --end-date 2025-06-30
```

## Paso 6 — Validar

Comprobar:

```text
Bronze
 ├── tenant correcto
 ├── esquema correcto
 └── particiones correctas

Silver
 ├── tenant correcto
 ├── anomalías
 ├── unidades
 ├── SCD
 └── enriquecimiento

Gold
 ├── tenant correcto
 ├── granularidad
 └── métricas
```

El onboarding sigue el principio de que incorporar un tenant debe requerir principalmente configuración y disponibilidad de la fuente, no modificaciones de la lógica central. La arquitectura fue planteada precisamente para facilitar el onboarding mediante schemas por tenant.

---

# 20. Trazabilidad

Las columnas técnicas permiten seguir el origen de un registro a través del pipeline:

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
_batch_id            = <UUID>
```

Esto permite identificar:

- cuándo fue ingerido;
- de qué archivo provino;
- a qué tenant pertenece;
- qué ejecución generó el registro.

---

# 21. Qué dejé fuera y por qué

La implementación se mantiene dentro del alcance de un MVP reproducible y no incorpora componentes que requieren infraestructura externa o exceden los objetivos principales de la prueba.

## 21.1 Streaming / Auto Loader

No se implementó streaming ni Auto Loader.

La fuente proporcionada es un archivo CSV y el procesamiento implementado es batch.

Una futura evolución podría incorporar Auto Loader para fuentes continuas.

La propia prueba técnica considera Auto Loader/streaming como una funcionalidad adicional.

---

## 21.2 ADLS Gen2

No se implementó almacenamiento físico sobre ADLS Gen2.

Se utilizan paths locales:

```text
data/
```

Esto permite reproducir el pipeline sin depender de una cuenta cloud.

La arquitectura define explícitamente el mapeo entre los paths locales y la futura estructura en ADLS/Unity Catalog.

---

## 21.3 Unity Catalog

No se implementó Unity Catalog funcional.

La separación lógica se representa mediante:

```text
data/bronze/<tenant>/
data/silver/<tenant>/
data/gold/<tenant>/
```

En un entorno Databricks, estos paths podrían mapearse a:

```text
saas_<env>.bronze_<tenant>
saas_<env>.silver_<tenant>
saas_<env>.gold_<tenant>
```

La prueba técnica plantea esta estructura como el destino productivo.

---

## 21.4 Terraform ejecutable

No se implementó infraestructura Terraform funcional contra un proveedor cloud.

La infraestructura queda documentada conceptualmente en:

```text
docs/infra.md
```

La prueba únicamente exige un snippet ilustrativo y no un `terraform plan` contra una cuenta real.

---

## 21.5 Segunda tabla Gold

No se implementó una segunda tabla Gold.

El alcance obligatorio requería al menos:

```text
daily_metrics_by_delivery_type
```

Una segunda tabla, como top materiales por tenant y mes, se considera una extensión.

La prueba la clasifica como bonus opcional.

---

## 21.6 Dashboard

No se implementó un dashboard sobre Gold.

La prioridad fue garantizar:

```text
correctitud
+
calidad
+
idempotencia
+
multi-tenancy
+
tests
```

La implementación de un dashboard en Databricks SQL o Streamlit queda como evolución posterior.

---

## 21.7 Optimización avanzada para producción

No se incorporaron optimizaciones específicas de un cluster productivo distribuido, debido a que la ejecución actual se realiza localmente.

Entre las optimizaciones que podrían evaluarse posteriormente están:

- configuración de particiones;
- broadcast joins;
- AQE;
- optimización de archivos pequeños;
- Z-Ordering;
- OPTIMIZE;
- caching selectivo;
- tuning de Spark.

La prueba está orientada principalmente a demostrar la correcta implementación de las capas y sus contratos.

---

# 22. Estado final

| Componente | Estado |
|---|---|
| Python | Implementado |
| PySpark | Implementado |
| Delta Lake | Implementado |
| YAML / OmegaConf | Implementado |
| CLI | Implementada |
| RAW | Implementado |
| Bronze | Implementado |
| Silver | Implementado |
| Gold | Implementado |
| Multi-tenant | Implementado |
| Idempotencia Bronze | Implementada |
| Idempotencia Silver | Implementada |
| Idempotencia Gold | Implementada |
| Manejo de anomalías | Implementado |
| Cuarentena | Implementada |
| SCD Type 2 | Implementado |
| Join temporal | Implementado |
| Calidad de datos | Implementada |
| Tests Bronze | Implementados |
| Tests Silver | Implementados |
| Tests Gold | Implementados |
| CI/CD | Implementado según configuración del repositorio |
| Infraestructura cloud | Fuera del alcance |
| Unity Catalog funcional | Fuera del alcance |
| Streaming | Fuera del alcance |
| Dashboard | Fuera del alcance |

---

# 23. Principio de diseño final

La separación de responsabilidades queda definida como:

```text
RAW
 │
 │ Datos originales
 │ Sin transformación
 ▼
BRONZE
 │
 │ Ingesta
 │ Delta
 │ Trazabilidad
 │ Particionado
 │ Idempotencia
 ▼
SILVER
 │
 │ Limpieza
 │ Normalización
 │ Calidad
 │ Anomalías
 │ SCD Type 2
 │ Enriquecimiento
 ▼
GOLD
 │
 │ Agregaciones
 │ Métricas de negocio
 │ Consumo analítico
 ▼
CONSUMO
```

El principio central de la implementación es mantener una separación clara entre **ingesta, transformación y consumo analítico**, de modo que Bronze preserve la información de origen, Silver represente datos confiables y normalizados, y Gold exponga métricas derivadas para análisis.