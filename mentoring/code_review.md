# Code Review - Entrega del ingeniero junior

## Objetivo

El código revisado corresponde a una implementación hipotética de un procesamiento de datos para un tenant/país.

El objetivo de esta revisión es identificar problemas técnicos que puedan afectar la escalabilidad, mantenibilidad, calidad, idempotencia y soporte multi-tenant del pipeline.

---

## 1. Uso de pandas para procesar datos que deberían manejarse con Spark

### Qué está mal

El código utiliza pandas para leer y transformar el archivo:

```python
df = pd.read_csv(file_path)
```

Posteriormente convierte el resultado a Spark:

```python
sdf = spark.createDataFrame(out)
```

El procesamiento principal ocurre en pandas y luego los datos se trasladan nuevamente a Spark.

### Por qué importa

pandas realiza el procesamiento principalmente en memoria dentro del proceso local. Esto limita la escalabilidad cuando aumenta el volumen de datos y genera un movimiento innecesario entre pandas y Spark.

### Cómo se corrige

La lectura y transformación deben realizarse directamente con Spark:

```python
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(file_path)
)
```

Las operaciones posteriores deben utilizar transformaciones Spark como `filter`, `withColumn`, `when` y `select`.

---

## 2. Iteración fila por fila mediante `iterrows()`

### Qué está mal

El código utiliza:

```python
for i, row in df.iterrows():
```

y ejecuta la lógica de negocio registro por registro.

### Por qué importa

La iteración fila por fila impide aprovechar adecuadamente el procesamiento vectorizado y distribuido. En un pipeline Spark constituye un antipatrón y puede degradar considerablemente el rendimiento.

### Cómo se corrige

La lógica debe expresarse como transformaciones declarativas de Spark.

Por ejemplo:

```python
normalized_quantity = F.when(
    F.col("unidad") == "CS",
    F.col("cantidad") * 20,
).otherwise(
    F.col("cantidad")
)
```

De esta forma Spark puede optimizar y distribuir la operación.

---

## 3. Lógica de negocio hardcoded

### Qué está mal

Las reglas de negocio están directamente codificadas:

```python
if row["tipo_entrega"] == "ZPRE" or row["tipo_entrega"] == "ZVE1":
```

y:

```python
qty = row["cantidad"] * 20
```

Los valores `ZPRE`, `ZVE1` y `20` están acoplados al código.

### Por qué importa

Si una regla cambia, es necesario modificar el código fuente. Esto dificulta el mantenimiento, las pruebas y la adaptación de reglas diferentes por tenant.

### Cómo se corrige

Las reglas deben separarse de la lógica de procesamiento. Por ejemplo:

```python
@dataclass(frozen=True)
class ProcessingConfig:
    allowed_delivery_types: tuple[str, ...] = ("ZPRE", "ZVE1")
    units_to_normalize: tuple[str, ...] = ("CS",)
    units_per_case: int = 20
```

La transformación utiliza esta configuración en lugar de valores dispersos en el código.

---

## 4. Ausencia de validaciones de entrada

### Qué está mal

El código asume que el archivo existe y que contiene todas las columnas necesarias. No valida existencia del archivo, columnas obligatorias, tenant/país, estructura del DataFrame ni parámetros de entrada.

### Por qué importa

Los pipelines deben fallar de forma explícita cuando reciben datos que no cumplen el contrato esperado. Sin validaciones, un problema de entrada puede manifestarse posteriormente como un error más difícil de diagnosticar.

### Cómo se corrige

Se debe validar el esquema antes de ejecutar las transformaciones:

```python
required_columns = {
    "pais",
    "fecha_proceso",
    "material",
    "tipo_entrega",
    "unidad",
    "cantidad",
    "precio",
}

missing_columns = required_columns - set(df.columns)

if missing_columns:
    raise ValueError(
        f"Missing required columns: {sorted(missing_columns)}"
    )
```

También se deben validar los parámetros recibidos.

---

## 5. Ausencia de tipado y contratos explícitos

### Qué está mal

La función original se define como:

```python
def process(file_path, country):
```

No especifica tipos de entrada ni tipo de retorno.

### Por qué importa

Los type hints permiten documentar el contrato de las funciones, mejorar la legibilidad y facilitar la detección temprana de errores durante el desarrollo.

### Cómo se corrige

Se deben utilizar anotaciones de tipo:

```python
def process(
    spark: SparkSession,
    file_path: str,
    country: str,
    output_base_path: str,
    config: ProcessingConfig | None = None,
) -> DataFrame:
```

La configuración puede representarse mediante una estructura tipada como `dataclass`.

---

## 6. Naming inconsistente y poco descriptivo

### Qué está mal

El código utiliza nombres demasiado genéricos como:

```python
df
out
qty
```

### Por qué importa

Los nombres poco descriptivos dificultan comprender el código y aumentan la carga cognitiva durante el mantenimiento.

### Cómo se corrige

Se deben utilizar nombres que expresen claramente el significado de los datos:

```python
filtered_df
normalized_quantity
output_path
input_path
result
```

La nomenclatura debe mantenerse consistente en todo el proyecto.

---

## 7. Manejo de errores inexistente

### Qué está mal

El código no establece un flujo explícito para controlar errores de lectura, transformación o escritura.

### Por qué importa

Cuando el pipeline falle, será difícil determinar si el problema proviene de la entrada, los parámetros, las transformaciones o la escritura.

### Cómo se corrige

Se deben validar errores conocidos y utilizar manejo controlado de excepciones:

```python
try:
    result = process(...)
except Exception as exc:
    print(f"Processing failed: {exc}")
    raise
finally:
    spark.stop()
```

En una implementación productiva se debería utilizar logging estructurado en lugar de `print`.

---

## 8. Escritura no preparada para un procesamiento reproducible

### Qué está mal

El código escribe directamente en:

```python
"/tmp/output/" + country
```

La ruta está hardcoded y no existe una estrategia explícita de almacenamiento alineada con la arquitectura de datos.

### Por qué importa

Una solución de datos debe permitir ejecuciones reproducibles y tener una estrategia clara para volver a procesar un tenant o un conjunto de datos.

### Cómo se corrige

La ruta debe ser configurable:

```python
output_path = Path(output_base_path) / country
```

En producción, la escritura debe alinearse con la estructura de capas, particionamiento y estrategia de almacenamiento definida por la arquitectura.

---

## 9. Falta de soporte multi-tenant explícito

### Qué está mal

Aunque la función recibe `country`, el parámetro representa implícitamente el tenant. El concepto de tenant no está modelado explícitamente ni se utiliza de manera consistente en el procesamiento.

### Por qué importa

En una plataforma SaaS multi-tenant se debe garantizar el aislamiento de datos y que cada ejecución identifique claramente al tenant correspondiente.

### Cómo se corrige

El tenant debe formar parte explícita del contrato de procesamiento:

```python
process(
    spark=spark,
    file_path=file_path,
    country=tenant_id,
    output_base_path=output_path,
)
```

En una implementación completa, `tenant_id` debería formar parte de la configuración y del modelo de datos.

---

## 10. Ausencia de pruebas automatizadas

### Qué está mal

El código no contiene pruebas automatizadas.

### Por qué importa

Sin tests no existe una garantía de que las reglas de transformación sigan funcionando después de realizar modificaciones.

### Cómo se corrige

Se deben crear pruebas unitarias sobre la lógica de transformación. Como mínimo:

1. Verificar el filtrado por tipo de entrega.
2. Verificar la conversión de unidades.
3. Verificar el cálculo del total.
4. Verificar el rechazo de esquemas incompletos.
5. Verificar el aislamiento por tenant.

---

# Priorización de las observaciones

## Prioridad alta

- Uso de pandas en lugar de Spark.
- Iteración fila por fila.
- Ausencia de validaciones.
- Manejo de errores inexistente.
- Falta de soporte multi-tenant.

Estos problemas pueden afectar directamente la escalabilidad, robustez y operación del pipeline.

## Prioridad media

- Lógica de negocio hardcoded.
- Escritura no configurable/reproducible.
- Ausencia de tests.

Estos problemas afectan principalmente la mantenibilidad, evolución y confiabilidad.

## Prioridad baja/media

- Naming inconsistente.
- Ausencia de tipado.

No necesariamente provocan un fallo inmediato, pero afectan la calidad y mantenibilidad del código.

---

# Cómo se lo explicaría al junior

Mi feedback sería técnico y constructivo, enfocándome primero en el impacto de las decisiones de implementación y no únicamente en señalar errores.

Le explicaría que la solución puede funcionar con un volumen pequeño, pero que algunas decisiones no son adecuadas para un pipeline distribuido con Spark y una arquitectura multi-tenant.

Primero le pediría revisar el modelo de ejecución de Spark y la diferencia entre procesamiento local con pandas y procesamiento distribuido con DataFrames de Spark. Luego le pediría investigar transformaciones vectorizadas/declarativas en Spark y por qué debe evitarse la iteración fila por fila.

También debería estudiar separación entre configuración y lógica de negocio, type hints, validación de esquemas, manejo de excepciones y logging.

Finalmente, le pediría investigar testing de pipelines de datos con pytest, idempotencia de procesos batch y patrones de diseño para arquitecturas multi-tenant.

La expectativa no sería simplemente copiar una versión refactorizada, sino comprender por qué cada cambio mejora la escalabilidad, mantenibilidad, observabilidad o confiabilidad del pipeline.
