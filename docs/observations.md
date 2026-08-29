# Observaciones y mejoras — RAW → BRONZE

## 1. Objetivo

Este documento registra las observaciones, decisiones técnicas y mejoras aplicadas durante la implementación de las capas **RAW** y **BRONZE** de la plataforma de datos.

El objetivo es mantener trazabilidad sobre las diferencias entre la arquitectura inicialmente propuesta y la implementación realizada, así como documentar las decisiones técnicas que permitan mantener la integridad, reproducibilidad y mantenibilidad del pipeline.

El alcance de este documento se limita exclusivamente a las capas **RAW** y **BRONZE** implementadas hasta el momento.

---

# 2. Arquitectura considerada

La arquitectura implementada hasta este punto mantiene la separación entre:

```text
data/
├── raw/
│   └── archivos fuente originales
│
└── bronze/
    └── sv/
        └── deliveries/
            ├── _delta_log/
            └── archivos Delta
```

La capa RAW representa los archivos fuente recibidos sin transformación de negocio.

La capa BRONZE representa una copia persistente de los datos procesada mediante Spark y almacenada en formato Delta Lake, incorporando únicamente información técnica necesaria para trazabilidad y operación del pipeline.

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

Por ejemplo, una columna que normalmente contiene valores numéricos podría ser interpretada de manera diferente si aparecen valores atípicos.

### Mejora recomendada

Definir progresivamente un esquema explícito para los archivos RAW cuando el contrato de datos del origen esté formalizado.

La implementación actual puede mantener la inferencia mientras se valida y documenta el esquema esperado.

---

# 4. Observaciones sobre la capa BRONZE

## OBS-BRZ-001 — BRONZE debe conservar todas las columnas originales

### Observación

La capa BRONZE no debe eliminar columnas provenientes de RAW.

### Importancia

BRONZE debe conservar suficiente información para permitir trazabilidad entre la fuente original y las capas posteriores.

Eliminar columnas en esta etapa podría provocar pérdida de información antes de que se hayan realizado las transformaciones analíticas correspondientes.

### Decisión / mejora aplicada

Se implementó una validación que verifica que:

- todas las columnas RAW existan en BRONZE;
- no existan columnas RAW faltantes;
- no existan columnas adicionales que no estén justificadas técnicamente.

La prueba correspondiente valida la preservación del esquema original.

---

## OBS-BRZ-002 — Separación entre columnas de negocio y columnas técnicas

### Observación

Las columnas originales se mantienen separadas conceptualmente de las columnas agregadas por el pipeline.

### Columnas técnicas implementadas

Actualmente BRONZE incorpora:

```text
_ingestion_timestamp
_source_file
_tenant_id
_batch_id
```

### Importancia

Estas columnas permiten conocer:

- cuándo se procesó el registro;
- de qué archivo provino;
- a qué tenant pertenece;
- a qué ejecución o lote de procesamiento corresponde.

### Decisión / mejora aplicada

Se incorporaron columnas técnicas sin modificar las columnas originales del dataset.

---

## OBS-BRZ-003 — Trazabilidad mediante batch_id

### Observación

Cada ejecución de ingestión debe poder identificarse de manera independiente.

### Decisión / mejora aplicada

Se incorpora:

```text
_batch_id
```

como identificador de ejecución.

El valor se genera para cada ejecución del pipeline y permite asociar los registros procesados con un lote específico.

### Beneficio

Esto permite investigar posteriormente qué registros fueron generados durante una determinada ejecución.

---

## OBS-BRZ-004 — Trazabilidad mediante archivo fuente

### Observación

Es necesario conocer el archivo de origen de cada registro almacenado en BRONZE.

### Decisión / mejora aplicada

Se incorpora:

```text
_source_file
```

para conservar el nombre del archivo RAW utilizado durante la ingestión.

### Beneficio

Permite rastrear un registro desde BRONZE hacia su archivo de origen.

---

## OBS-BRZ-005 — Trazabilidad temporal de ingestión

### Observación

La fecha contenida en los datos de negocio no necesariamente representa el momento en que el registro fue procesado por la plataforma.

### Decisión / mejora aplicada

Se incorpora:

```text
_ingestion_timestamp
```

para registrar el timestamp correspondiente a la ingestión.

### Beneficio

Permite diferenciar:

```text
fecha del dato
      vs.
fecha de ingestión
```

Esta distinción es importante para auditoría y diagnóstico de pipelines.

---

# 5. Particionamiento de BRONZE

## OBS-BRZ-006 — Particionamiento por fecha y tenant

### Observación

La información BRONZE puede crecer significativamente en volumen, por lo que almacenar todos los registros en una única partición física no es conveniente.

### Decisión / mejora aplicada

La tabla Delta BRONZE se encuentra particionada mediante:

```text
fecha_proceso
_tenant_id
```

### Beneficios

El particionamiento permite:

- reducir el volumen de datos leído en determinadas consultas;
- aprovechar partition pruning;
- separar físicamente los datos por tenant;
- facilitar operaciones sobre rangos temporales;
- mantener una organización consistente de los datos.

### Validación

La estructura Delta fue validada verificando que las columnas de particionamiento sean:

```text
fecha_proceso
_tenant_id
```

---

# 6. Multi-tenancy

## OBS-BRZ-007 — Identificación explícita del tenant

### Observación

La arquitectura contempla múltiples tenants, por lo que el tenant debe formar parte de la información técnica del registro.

### Decisión / mejora aplicada

Se incorpora:

```text
_tenant_id
```

en BRONZE.

Además, la ejecución del pipeline recibe explícitamente el tenant mediante el parámetro:

```text
--tenant sv
```

### Validación

Se implementaron pruebas para verificar que:

1. la columna `_tenant_id` exista;
2. los registros pertenezcan al tenant esperado;
3. no se mezclen registros de diferentes tenants dentro de una ejecución destinada a un tenant específico.

---

# 7. Preservación de registros

## OBS-BRZ-008 — Validación de cantidad de registros RAW → BRONZE

### Observación

Durante la implementación se identificó la necesidad de demostrar que el proceso de ingestión no elimina registros de manera accidental.

### Decisión / mejora aplicada

Se incorporó una prueba de conteo entre RAW y BRONZE.

La validación considera que:

```text
cantidad de registros RAW
        =
cantidad de registros BRONZE
```

para el mismo conjunto de datos y rango procesado.

### Importancia

Esta prueba permite detectar:

- filtros involuntarios;
- registros descartados;
- errores de lectura;
- problemas de ingestión;
- modificaciones incorrectas de la lógica.

---

# 8. Integridad del contenido RAW → BRONZE

## OBS-BRZ-009 — Validación de contenido

### Observación

La validación del número de registros por sí sola no garantiza que los datos sean idénticos.

Es posible mantener la misma cantidad de filas pero alterar sus valores.

### Decisión / mejora aplicada

Se incorporaron pruebas para validar la integridad del contenido entre RAW y BRONZE.

La validación considera las columnas originales y permite comprobar que los registros originales no hayan sido modificados durante la ingestión.

### Importancia

Esto proporciona una validación más sólida del proceso:

```text
RAW
 │
 │ preservación de columnas
 │ preservación de tipos
 │ preservación de registros
 │ preservación de valores
 ▼
BRONZE
```

---

# 9. Validación de tipos de datos

## OBS-BRZ-010 — Preservación de tipos de columnas RAW

### Observación

No basta con validar que una columna exista. También debe comprobarse que su tipo de dato sea consistente.

### Decisión / mejora aplicada

Se incorporaron pruebas para verificar los tipos de las columnas originales.

Asimismo, se validaron los tipos correspondientes a las columnas técnicas.

### Beneficio

Esto permite detectar cambios accidentales como:

```text
integer → string
double  → string
timestamp → string
```

que podrían afectar procesos posteriores.

---

# 10. Validación de columnas duplicadas

## OBS-BRZ-011 — Control de columnas duplicadas

### Observación

Un DataFrame con columnas duplicadas puede generar ambigüedad durante las transformaciones posteriores.

### Decisión / mejora aplicada

Se incorporó una prueba que verifica que BRONZE no contenga nombres de columnas duplicados.

### Resultado esperado

Cada nombre de columna debe ser único dentro del esquema BRONZE.

---

# 11. Formato Delta Lake

## OBS-BRZ-012 — Persistencia de BRONZE en Delta Lake

### Observación

La capa BRONZE requiere un formato que permita almacenar los datos de forma estructurada y soportar operaciones propias de una plataforma de datos.

### Decisión / mejora aplicada

BRONZE se implementó utilizando Delta Lake.

La estructura resultante contiene:

```text
data/bronze/sv/deliveries/
├── _delta_log/
└── archivos parquet
```

### Beneficios

Delta permite disponer de:

- almacenamiento columnar;
- transacciones ACID;
- metadatos de tabla;
- evolución controlada del esquema;
- historial de operaciones;
- integración nativa con Spark.

---

# 12. Validación de lectura Delta

## OBS-BRZ-013 — BRONZE debe poder ser leído como Delta

### Observación

La generación de archivos Parquet no garantiza que la tabla se haya generado correctamente como Delta.

### Decisión / mejora aplicada

Se incorporó una prueba que lee explícitamente BRONZE mediante:

```python
spark.read.format("delta").load(...)
```

### Beneficio

Esto valida que la salida generada sea realmente una tabla Delta funcional.

---

# 13. Validaciones automatizadas

## OBS-BRZ-014 — Incorporación de pruebas automatizadas

### Observación

Las validaciones manuales mediante comandos de Spark permiten inspeccionar los resultados, pero no son suficientes como mecanismo permanente de control.

### Decisión / mejora aplicada

Se implementó una suite de pruebas mediante `pytest`.

Actualmente se validan aspectos como:

- estructura Delta;
- lectura como Delta;
- existencia de datos;
- columnas técnicas;
- tenant;
- tenant esperado;
- particionamiento;
- preservación de columnas RAW;
- ausencia de columnas inesperadas;
- ausencia de columnas duplicadas;
- tipos de datos;
- tipos de columnas técnicas;
- cantidad de registros;
- integridad del contenido.

### Resultado

Las pruebas implementadas para RAW → BRONZE se encuentran aprobadas.

---

# 14. Validación formal del contrato RAW → BRONZE

## OBS-BRZ-015 — Establecimiento de un contrato técnico entre capas

### Observación

Durante la implementación se identificó que la relación entre RAW y BRONZE debía validarse explícitamente y no depender únicamente de una inspección visual.

### Decisión / mejora aplicada

Se estableció un conjunto de invariantes para el proceso RAW → BRONZE:

```text
1. No perder columnas originales
2. No agregar columnas no justificadas
3. No duplicar columnas
4. Mantener los tipos esperados
5. No perder registros
6. No alterar valores originales
7. Agregar únicamente columnas técnicas
8. Mantener el tenant correcto
9. Mantener el particionamiento definido
10. Generar una tabla Delta válida
```

Estas condiciones constituyen actualmente el contrato técnico de la ingestión RAW → BRONZE.

---

# 15. Observaciones de operación en entorno Windows

## OBS-OPS-001 — Mensajes de Spark relacionados con archivos temporales

### Observación

Durante las ejecuciones locales se observaron mensajes como:

```text
Exception while deleting Spark temp dir
Failed to delete
org.antlr_antlr4-runtime-4.9.3.jar
```

También se observaron mensajes relacionados con `ShutdownHookManager`.

### Evaluación

Estos mensajes aparecen durante la limpieza de archivos temporales generados por Spark y no impidieron completar correctamente las ejecuciones.

La evidencia principal es que el pipeline finalizó correctamente y generó la salida BRONZE esperada.

### Consideración

El comportamiento está relacionado con el entorno local Windows y el manejo de archivos temporales/JAR utilizados por Spark.

### Mejora recomendada

Mantener monitoreado este comportamiento durante el desarrollo local. En caso de que posteriormente provoque fallos reales de ejecución, deberá evaluarse una configuración específica del entorno Windows o la ejecución del pipeline en un entorno Linux.

---

# 16. Observaciones sobre logging

## OBS-OPS-002 — Nivel de logging de Spark

### Observación

Durante las ejecuciones se muestran mensajes como:

```text
Setting default log level to "WARN".
```

### Evaluación

Este comportamiento corresponde a la configuración por defecto de Spark y no representa un error del pipeline.

### Mejora recomendada

Establecer posteriormente una configuración centralizada de logging para controlar:

- nivel de log;
- mensajes funcionales;
- errores;
- advertencias;
- identificación de ejecución;
- trazabilidad del pipeline.

La mejora debe realizarse sin modificar la lógica funcional de RAW → BRONZE.

---

# 17. Observaciones sobre representación de planes Spark

## OBS-OPS-003 — Truncamiento de planes de ejecución

### Observación

Durante algunas operaciones se observó:

```text
WARN SparkStringUtils:
Truncated the string representation of a plan since it was too large.
```

### Evaluación

Este mensaje corresponde a la representación textual del plan de ejecución de Spark y no indica pérdida ni corrupción de datos.

### Mejora recomendada

No modificar esta configuración únicamente para eliminar el warning.

En caso de requerir análisis detallado del plan de ejecución, puede ajustarse específicamente:

```text
spark.sql.debug.maxToStringFields
```

durante tareas de diagnóstico.

---

# 18. Observaciones sobre configuración de Spark + Delta

## OBS-OPS-004 — Configuración consistente de Spark y Delta

### Observación

Las operaciones Delta requieren que la `SparkSession` tenga correctamente configuradas las extensiones y el catálogo Delta.

Durante las validaciones manuales se comprobó que crear una sesión Spark sin dicha configuración produce errores como:

```text
DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG
```

### Decisión / mejora aplicada

La creación de la sesión Spark utilizada por el pipeline y las pruebas se centraliza mediante la configuración correspondiente de Delta.

Para las inspecciones manuales debe utilizarse igualmente la configuración de Spark compatible con Delta.

### Beneficio

Se evita que diferentes componentes del proyecto creen sesiones Spark incompatibles entre sí.

---

# 19. Observaciones sobre configuración del entorno de desarrollo

## OBS-ENV-001 — Entorno virtual reproducible

### Observación

El proyecto utiliza un entorno virtual Python denominado:

```text
venv
```

### Decisión

Se mantiene esta estructura, ya que el nombre del entorno virtual no afecta el funcionamiento de Python, PySpark ni Delta mientras las dependencias estén correctamente instaladas.

### Mejora aplicada

La configuración necesaria para ejecutar el proyecto se mantiene asociada a la configuración del proyecto y sus dependencias, evitando depender de configuraciones manuales realizadas únicamente durante una sesión de terminal.

---

# 20. Observaciones sobre versiones

## OBS-ENV-002 — Control de versiones de dependencias

### Observación

PySpark, Delta Lake, Python y Java mantienen una relación de compatibilidad que debe controlarse.

### Mejora recomendada

Mantener documentadas las versiones utilizadas en el proyecto, incluyendo como mínimo:

```text
Python
PySpark
Delta Lake
Java
pytest
```

Esto permitirá reproducir el entorno y reducir problemas derivados de actualizaciones no controladas.

---

# 21. Mejoras actualmente implementadas

A la fecha, las siguientes mejoras han sido implementadas en RAW → BRONZE:

| Mejora | Estado |
|---|---|
| Procesamiento utilizando Spark | Implementado |
| Eliminación de dependencia de pandas para procesamiento distribuido | Implementado |
| Persistencia BRONZE en Delta Lake | Implementado |
| Particionamiento por `fecha_proceso` | Implementado |
| Particionamiento por `_tenant_id` | Implementado |
| Identificación del tenant | Implementado |
| `_batch_id` | Implementado |
| `_source_file` | Implementado |
| `_ingestion_timestamp` | Implementado |
| Preservación de columnas RAW | Implementado |
| Preservación de tipos de datos | Implementado |
| Validación de cantidad de registros | Implementado |
| Validación de integridad del contenido | Implementado |
| Validación de columnas duplicadas | Implementado |
| Validación de columnas técnicas | Implementado |
| Validación automatizada mediante pytest | Implementado |
| Validación de lectura como Delta | Implementado |

---

# 22. Mejoras recomendadas para RAW → BRONZE

Las siguientes mejoras se consideran oportunidades técnicas para fortalecer las capas actualmente implementadas, pero **no forman parte de la implementación funcional actual**:

### MEJ-001 — Schema explícito para RAW

Reemplazar progresivamente `inferSchema` por esquemas definidos explícitamente cuando el contrato de datos del origen se encuentre formalizado.

### MEJ-002 — Validación de archivos de entrada

Agregar validaciones previas para comprobar existencia, tamaño, formato y estructura mínima de los archivos RAW.

### MEJ-003 — Validación de esquema de entrada

Detectar automáticamente cambios en nombres, cantidad o tipos de columnas del archivo fuente.

### MEJ-004 — Control de archivos duplicados

Evaluar un mecanismo para detectar si un mismo archivo RAW está siendo procesado nuevamente de forma accidental.

### MEJ-005 — Idempotencia

Evaluar que una misma ejecución de ingestión pueda repetirse sin generar registros duplicados en BRONZE.

### MEJ-006 — Control de calidad de datos en ingestión

Incorporar métricas de calidad relacionadas con:

- registros nulos;
- columnas obligatorias;
- valores inválidos;
- formatos incorrectos;
- registros rechazados.

Estas validaciones deben realizarse sin alterar la función de BRONZE como capa de preservación.

### MEJ-007 — Logging estructurado

Centralizar logs del pipeline incluyendo:

```text
tenant
batch_id
archivo
inicio
fin
cantidad de registros
estado
error
```

### MEJ-008 — Métricas de ejecución

Registrar métricas de cada ejecución, como:

```text
RAW records
BRONZE records
duración
archivos procesados
batch_id
tenant
```

### MEJ-009 — Gestión controlada de errores

Mejorar los mensajes de error para distinguir entre:

- archivo inexistente;
- archivo inválido;
- esquema incompatible;
- error de Spark;
- error de Delta;
- error de escritura;
- error de configuración.

### MEJ-010 — Configuración externa

Evitar que parámetros funcionales como rutas, nombres de archivos o configuraciones de ingestión queden directamente codificados dentro de la lógica.

Cuando corresponda, estos parámetros deberían provenir de configuración.

### MEJ-011 — Limpieza controlada de temporales

Evaluar la configuración del entorno local para reducir los mensajes asociados a la eliminación de archivos temporales de Spark observados en Windows.

### MEJ-012 — Pruebas de regresión

Mantener la suite de pruebas RAW → BRONZE como requisito previo para modificar la lógica de ingestión.

Toda modificación de la implementación debe demostrar que las invariantes establecidas continúan cumpliéndose.

---

# 23. Principios establecidos para RAW → BRONZE

La implementación actual establece los siguientes principios:

```text
RAW
 │
 │ Datos originales
 │ Sin transformación de negocio
 │
 ▼
BRONZE
 │
 ├── Conserva columnas originales
 ├── Conserva tipos esperados
 ├── Conserva registros
 ├── Conserva valores
 ├── Agrega metadatos técnicos
 ├── Identifica tenant
 ├── Identifica batch
 ├── Identifica archivo fuente
 ├── Registra timestamp de ingestión
 ├── Particiona por fecha y tenant
 └── Persiste en Delta Lake
```

La capa BRONZE se mantiene, por tanto, como una capa de **ingestión y persistencia trazable**, evitando introducir transformaciones de negocio que correspondan a etapas posteriores del pipeline.

---

# 24. Estado actual

El flujo implementado y validado actualmente es:

```text
Archivo CSV
    │
    ▼
   RAW
    │
    │ Spark
    │ Validaciones
    │ Metadatos técnicos
    │
    ▼
  BRONZE
    │
    ├── Delta Lake
    ├── Partition: fecha_proceso
    ├── Partition: _tenant_id
    ├── _ingestion_timestamp
    ├── _source_file
    ├── _tenant_id
    └── _batch_id
```

La implementación RAW → BRONZE cuenta actualmente con pruebas automatizadas para validar estructura, esquema, tipos, columnas, registros, tenant, particionamiento e integridad del contenido.

Las pruebas implementadas se encuentran aprobadas.

---

# 25. Criterio de cierre de RAW → BRONZE

Se considera que la implementación de RAW → BRONZE cumple con el objetivo definido cuando:

- RAW conserva los archivos fuente.
- BRONZE se genera en formato Delta.
- Las columnas originales se conservan.
- Los tipos esperados se conservan.
- No existen columnas duplicadas.
- No se pierden registros.
- No se alteran los valores originales.
- Las columnas técnicas están presentes.
- El tenant se encuentra correctamente identificado.
- El batch de ejecución es trazable.
- El archivo fuente es trazable.
- La ingestión posee timestamp.
- La tabla está particionada según la arquitectura definida.
- Las pruebas automatizadas de RAW → BRONZE son exitosas.

Con estos criterios cumplidos, la implementación de las capas RAW y BRONZE queda formalmente validada para efectos del alcance actual del proyecto.