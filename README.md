# SaaS Data Platform

Pipeline de datos multi-tenant basado en arquitectura **Medallion**, implementado con **Python, PySpark y Delta Lake**.

La implementación actual cubre las capas **RAW → Bronze**. La capa RAW conserva los archivos de origen sin transformación, mientras que Bronze ingesta los datos en formato Delta Lake, preservando las columnas originales y agregando columnas técnicas para trazabilidad.

---

## 1. Arquitectura

La plataforma sigue una arquitectura Medallion con separación por tenant:

```text
                     ┌─────────────────────────┐
                     │       FUENTE CSV        │
                     └────────────┬────────────┘
                                  │
                                  ▼
                     ┌─────────────────────────┐
                     │          RAW            │
                     │                         │
                     │ CSV original            │
                     │ Sin transformación      │
                     │ Solo lectura             │
                     └────────────┬────────────┘
                                  │
                                  │ Ingesta
                                  ▼
                     ┌─────────────────────────┐
                     │        BRONZE           │
                     │                         │
                     │ Delta Lake              │
                     │ Esquema original        │
                     │ + columnas técnicas     │
                     │ Particionado por fecha  │
                     │ + tenant                │
                     └─────────────────────────┘
```

La arquitectura implementada establece que:

- **RAW** contiene los archivos crudos tal como llegan desde la fuente.
- **Bronze** contiene los datos ingeridos en formato Delta Lake.
- Bronze preserva el esquema original y agrega columnas técnicas.
- Bronze se particiona por `fecha_proceso` y `_tenant_id`.
- La ejecución permite procesar un tenant específico.
- El procesamiento permite parametrizar un rango de fechas.
- La configuración se mantiene separada de la lógica de procesamiento.

---

# 2. Stack tecnológico

| Componente | Versión / tecnología | Uso |
|---|---|---|
| Python | 3.13.2 | Lenguaje de implementación |
| PySpark | 3.5.x | Procesamiento de datos |
| Delta Lake | 3.3.3 | Almacenamiento transaccional |
| pytest | 9.1.1 | Pruebas automatizadas |
| OmegaConf | Configuración | Gestión de configuración |

Las versiones utilizadas deben mantenerse sincronizadas con la configuración del proyecto para garantizar la reproducibilidad del entorno.

---

# 3. Estructura del repositorio

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
│   │   └── *.csv
│   │
│   ├── bronze/
│   │   └── <tenant>/
│   │       └── <table>/
│   │           └── fecha_proceso=YYYYMMDD/
│   │
│   ├── bronze_quarantine/
│   │
│   ├── silver/
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
│   ├── test_spark_environment.py
│   └── test_bronze.py
│
└── mentoring/
    ├── bad_code.py
    ├── good_code.py
    └── code_review.md
```

### Estado actual

La estructura mantiene la separación definida para el proyecto entre:

- configuración;
- datos;
- código;
- pruebas;
- documentación.

Sin embargo, la implementación funcional desarrollada y validada hasta este momento corresponde únicamente a:

```text
RAW → BRONZE
```

Los directorios correspondientes a capas posteriores forman parte de la estructura del repositorio, pero no se consideran funcionalidades implementadas en esta etapa.

---

# 4. Capa RAW

La capa RAW contiene los archivos originales recibidos desde las fuentes.

Ejemplo:

```text
data/raw/
└── global_mobility_data_entrega_productos.csv
```

RAW mantiene los datos sin transformaciones de negocio.

Características:

- Formato CSV.
- Conservación del archivo original.
- Sin normalización de datos.
- Sin aplicación de reglas de negocio.
- Sin eliminación de registros.
- Sin agregaciones.
- Sin enriquecimiento.

El propósito de esta capa es disponer de una representación fiel de los datos recibidos antes de cualquier procesamiento.

---

# 5. Capa Bronze

Bronze ingesta los archivos RAW utilizando PySpark y los almacena en formato Delta Lake.

Ejemplo:

```text
data/bronze/
└── sv/
    └── deliveries/
        ├── fecha_proceso=20250401/
        ├── fecha_proceso=20250402/
        ├── fecha_proceso=20250403/
        └── ...
```

Bronze conserva las columnas originales y agrega información técnica relacionada con la ingesta.

## 5.1 Columnas originales

Para el dataset actualmente procesado:

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

Estas columnas se conservan en Bronze.

## 5.2 Columnas técnicas

Bronze agrega:

```text
_ingestion_timestamp
_source_file
_tenant_id
_batch_id
```

Estas columnas permiten mantener la trazabilidad de los registros durante el proceso de ingesta.

### `_ingestion_timestamp`

Indica el momento en que el registro fue ingerido.

### `_source_file`

Identifica el archivo RAW del cual proviene el registro.

### `_tenant_id`

Identifica el tenant al que pertenece el registro.

### `_batch_id`

Identifica la ejecución específica del pipeline que generó los datos.

---

# 6. Particionado Bronze

La tabla Bronze se encuentra particionada mediante:

```text
fecha_proceso
_tenant_id
```

La estructura física resultante sigue el concepto:

```text
data/bronze/<tenant>/<table>/
    fecha_proceso=YYYYMMDD/
    _tenant_id=<tenant>/
```

El particionado permite organizar los datos por fecha y tenant y constituye la base para realizar operaciones sobre particiones específicas.

---

# 7. Configuración

La configuración se mantiene separada de la lógica de procesamiento mediante archivos YAML y OmegaConf.

Estructura:

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

La configuración permite separar parámetros del código fuente.

Entre los parámetros utilizados para RAW → Bronze se encuentran:

- paths de entrada;
- paths de salida;
- tenant;
- fecha inicial;
- fecha final;
- configuración de ejecución;
- parámetros de calidad.

La separación permite modificar parámetros de ejecución sin modificar la lógica de procesamiento.

---

# 8. Requisitos previos

Para ejecutar el proyecto se requiere:

- Python 3.13.2.
- Java compatible con Spark.
- Git.
- Terminal PowerShell, CMD o equivalente.

Verificar Python:

```powershell
python --version
```

Resultado esperado:

```text
Python 3.13.2
```

Verificar Java:

```powershell
java -version
```

---

# 9. Levantar el entorno

## 9.1 Clonar el repositorio

```powershell
git clone <repository-url>
cd saas-data-platform
```

## 9.2 Crear el entorno virtual

```powershell
python -m venv venv
```

## 9.3 Activar el entorno virtual

En PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
```

En CMD:

```cmd
venv\Scripts\activate
```

El terminal debería mostrar:

```text
(venv)
```

## 9.4 Instalar las dependencias

```powershell
python -m pip install --upgrade pip
pip install -e .
```

Una vez instaladas las dependencias, el proyecto puede ejecutarse desde el entorno virtual.

---

# 10. Verificación del entorno Spark + Delta

Antes de ejecutar Bronze se puede verificar que Spark y Delta Lake estén correctamente configurados:

```powershell
pytest tests/test_spark_environment.py -v
```

Resultado esperado:

```text
tests/test_spark_environment.py::test_spark_and_delta_environment PASSED
```

Esta prueba permite verificar que el entorno puede inicializar Spark y trabajar con Delta Lake.

---

# 11. Ejecución del pipeline Bronze

La ejecución se realiza mediante la CLI:

```powershell
python -m saas_pipeline.cli --layer bronze --tenant sv --start-date 2025-01-01 --end-date 2025-06-30
```

Los parámetros representan:

| Parámetro | Descripción |
|---|---|
| `--layer bronze` | Indica que se ejecutará la capa Bronze |
| `--tenant sv` | Tenant que será procesado |
| `--start-date` | Fecha inicial del procesamiento |
| `--end-date` | Fecha final del procesamiento |

La ejecución genera la salida en:

```text
data/bronze/sv/deliveries/
```

Una ejecución exitosa genera un mensaje similar a:

```text
Bronze completed |
tenant=sv |
batch_id=<UUID> |
output=data\bronze\sv\deliveries
```

---

# 12. Idempotencia

Bronze utiliza una estrategia de sobrescritura por partición para evitar duplicaciones cuando se reprocesa el mismo rango de datos.

Conceptualmente:

```text
Primera ejecución

RAW
 │
 ▼
Bronze
 ├── fecha=20250401
 ├── fecha=20250402
 └── fecha=20250403


Reejecución

RAW
 │
 ▼
Bronze
 ├── fecha=20250401  ← sobrescrita
 ├── fecha=20250402  ← sobrescrita
 └── fecha=20250403  ← sobrescrita
```

El objetivo es que la ejecución repetida del mismo tenant y rango de fechas sea reproducible y no genere registros duplicados.

---

# 13. Validaciones RAW → Bronze

Se implementaron pruebas automatizadas para validar la integridad de la ingesta.

Las pruebas cubren:

### Estructura Delta

Verifica que la salida Bronze corresponda a una tabla Delta válida.

### Lectura como Delta

Verifica que la tabla pueda ser leída mediante:

```python
spark.read.format("delta")
```

### Existencia de datos

Verifica que la tabla Bronze contenga registros.

### Columnas técnicas

Comprueba la existencia de:

```text
_ingestion_timestamp
_source_file
_tenant_id
_batch_id
```

### Tenant

Verifica que:

- `_tenant_id` exista;
- los registros pertenezcan al tenant esperado;
- no se mezclen tenants durante la ejecución.

### Particiones

Verifica que Bronze esté particionado correctamente.

### Preservación de columnas RAW

Verifica que todas las columnas originales de RAW estén presentes en Bronze.

```text
RAW columns ⊆ Bronze columns
```

### Ausencia de columnas adicionales de negocio

Verifica que Bronze contenga únicamente:

```text
columnas RAW
+
columnas técnicas
```

sin incorporar transformaciones de negocio adicionales.

### Tipos de datos

Se validan los tipos esperados de las columnas originales y técnicas.

### Columnas duplicadas

Se verifica que no existan nombres de columnas duplicados.

### Cantidad de registros

Se valida la correspondencia de registros entre RAW y Bronze considerando el alcance definido para la ejecución.

### Integridad de contenido

Se verifica que los valores de las columnas originales se preserven durante la ingesta RAW → Bronze.

Estas validaciones permiten demostrar que la capa Bronze conserva la información de origen y no introduce modificaciones de negocio durante la ingesta.

---

# 14. Ejecutar tests

Para ejecutar todas las pruebas:

```powershell
pytest -v
```

Para ejecutar únicamente las pruebas de Bronze:

```powershell
pytest tests/test_bronze.py -v
```

Para ejecutar la prueba del entorno:

```powershell
pytest tests/test_spark_environment.py -v
```

El resultado esperado es que todas las pruebas finalicen correctamente:

```text
============================= test session starts =============================
...
PASSED
...
============================= X passed =============================
```

---

# 15. Onboarding de un nuevo tenant

El diseño permite incorporar un nuevo tenant sin modificar la lógica principal del procesamiento Bronze.

Por ejemplo, para incorporar el tenant `hn`:

```text
config/
└── tenants/
    ├── sv.yaml
    └── hn.yaml
```

El nuevo tenant debe contar con su configuración correspondiente.

La fuente de datos se incorpora a:

```text
data/raw/
```

Posteriormente se ejecuta Bronze indicando el nuevo tenant:

```powershell
python -m saas_pipeline.cli --layer bronze --tenant hn --start-date 2025-01-01 --end-date 2025-06-30
```

La salida correspondiente se almacena bajo:

```text
data/bronze/
└── hn/
    └── deliveries/
```

## Flujo de onboarding

```text
Nuevo tenant
     │
     ▼
Crear configuración
     │
     ▼
Incorporar fuente RAW
     │
     ▼
Ejecutar Bronze
     │
     ▼
Validar tenant
     │
     ▼
Validar esquema
     │
     ▼
Validar particiones
     │
     ▼
Validar integridad RAW → Bronze
```

Los principales controles durante el onboarding son:

1. Configuración del tenant.
2. Disponibilidad de la fuente RAW.
3. Ejecución correcta del pipeline.
4. Validación de `_tenant_id`.
5. Validación de columnas.
6. Validación de tipos.
7. Validación de particiones.
8. Validación de cantidad de registros.
9. Validación de integridad del contenido.

---

# 16. Trazabilidad

Cada registro Bronze contiene información técnica que permite rastrear su origen.

Ejemplo:

```text
_ingestion_timestamp = 2026-08-28 19:09:35
_source_file         = global_mobility_data_entrega_productos.csv
_tenant_id           = sv
_batch_id            = 24ee2ca9-6ab4-4b92-a3a1-64816abb312e
```

Esto permite identificar:

- cuándo fue ingerido el registro;
- desde qué archivo provino;
- a qué tenant pertenece;
- qué ejecución generó el registro.

La trazabilidad constituye uno de los principales objetivos de la capa Bronze.

---

# 17. Qué dejé fuera y por qué

La implementación actual se limita deliberadamente a:

```text
RAW → BRONZE
```

Las siguientes funcionalidades no forman parte de la implementación actual.

## 17.1 Transformaciones de negocio

Bronze no aplica transformaciones como:

- normalización de unidades;
- cálculo de revenue;
- creación de flags de negocio;
- agregaciones;
- enriquecimiento de información;
- aplicación de reglas analíticas.

La razón es preservar Bronze como una capa de ingesta y trazabilidad.

---

## 17.2 Reglas de calidad de negocio

No se aplican en Bronze reglas de negocio como:

- validación analítica de cantidades;
- validación de precios;
- clasificación de registros;
- validaciones relacionadas con materiales;
- reglas específicas sobre tipos de entrega.

Estas reglas no forman parte de la lógica de ingesta implementada actualmente.

---

## 17.3 Streaming

No se implementó procesamiento streaming.

La ingesta actual utiliza archivos CSV como fuente de entrada y procesamiento batch mediante PySpark.

---

## 17.4 Almacenamiento cloud

No se implementó almacenamiento sobre ADLS Gen2 ni otro servicio cloud.

El almacenamiento utilizado para la prueba se representa mediante:

```text
data/
```

Esto permite reproducir localmente la arquitectura de almacenamiento.

---

## 17.5 Unity Catalog

No se implementó Unity Catalog.

La separación por tenant se representa mediante paths locales:

```text
data/bronze/sv/
```

en lugar de schemas físicos administrados mediante Unity Catalog.

---

## 17.6 Infraestructura cloud ejecutable

No se implementó infraestructura cloud funcional.

La implementación actual se concentra en el procesamiento de datos y sus validaciones dentro del entorno local.

---

## 17.7 Optimización avanzada de Spark

No se incorporaron optimizaciones avanzadas específicas de un entorno productivo distribuido, debido a que la ejecución actual se realiza localmente.

El objetivo de esta etapa es demostrar:

- correcta ingesta;
- preservación del esquema;
- trazabilidad;
- particionado;
- aislamiento por tenant;
- idempotencia;
- validación automatizada.

---

# 18. Reproducibilidad

El flujo para reproducir la implementación es:

```text
Clonar repositorio
       │
       ▼
Crear entorno virtual
       │
       ▼
Activar venv
       │
       ▼
Instalar dependencias
       │
       ▼
Validar Spark + Delta
       │
       ▼
Ejecutar tests
       │
       ▼
Incorporar RAW
       │
       ▼
Ejecutar Bronze
       │
       ▼
Validar resultado
```

Comandos principales:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -e .
pytest -v
python -m saas_pipeline.cli --layer bronze --tenant sv --start-date 2025-01-01 --end-date 2025-06-30
```

---

# 19. Estado actual del proyecto

| Componente | Estado |
|---|---|
| Entorno Python | Implementado |
| Spark | Implementado |
| Delta Lake | Implementado |
| Configuración YAML | Implementada |
| OmegaConf | Implementado |
| CLI | Implementada |
| RAW | Implementado |
| Bronze | Implementado |
| Delta Bronze | Implementado |
| Particionado Bronze | Implementado |
| Aislamiento por tenant | Implementado |
| Batch ID | Implementado |
| Columnas técnicas | Implementado |
| Idempotencia Bronze | Implementado |
| Tests RAW → Bronze | Implementados |
| Validación de esquema | Implementada |
| Validación de contenido | Implementada |
| Silver | Fuera del alcance actual |
| Gold | Fuera del alcance actual |

---

# 20. Comandos principales

### Crear entorno

```powershell
python -m venv venv
```

### Activar entorno

```powershell
.\venv\Scripts\Activate.ps1
```

### Instalar dependencias

```powershell
pip install -e .
```

### Validar entorno Spark + Delta

```powershell
pytest tests/test_spark_environment.py -v
```

### Ejecutar todos los tests

```powershell
pytest -v
```

### Ejecutar tests Bronze

```powershell
pytest tests/test_bronze.py -v
```

### Ejecutar Bronze

```powershell
python -m saas_pipeline.cli --layer bronze --tenant sv --start-date 2025-01-01 --end-date 2025-06-30
```

---

# 21. Documentación complementaria

```text
docs/
├── observations.md
├── onboarding-tenant.md
└── infra.md
```

### `observations.md`

Contiene las observaciones realizadas sobre la arquitectura inicial y las mejoras aplicadas durante la implementación de RAW → Bronze.

### `onboarding-tenant.md`

Contiene el procedimiento para incorporar un nuevo tenant siguiendo la arquitectura definida.

### `infra.md`

Contiene la documentación relacionada con la infraestructura considerada para el proyecto.

---

# 22. Principio de diseño

La implementación mantiene una separación clara de responsabilidades:

```text
RAW
 │
 │  Datos originales
 │  Sin transformaciones
 │
 ▼
BRONZE
 │
 │  Ingesta
 │  Delta Lake
 │  Trazabilidad
 │  Preservación del esquema
 │  Particionado
 │  Aislamiento por tenant
 │  Idempotencia
 │
 ▼
Capas posteriores
Fuera del alcance actual
```

El principio aplicado hasta esta etapa es:

> **Bronze debe preservar la información de origen y agregar únicamente la información técnica necesaria para controlar y rastrear la ingesta.**

De esta manera se mantiene una separación entre los datos originales y las transformaciones de negocio, evitando introducir lógica analítica en la capa de ingesta.