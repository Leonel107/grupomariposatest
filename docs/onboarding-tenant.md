# Onboarding de un nuevo tenant

## 1. Objetivo

Este documento describe el procedimiento para incorporar un nuevo tenant a la
plataforma SaaS de datos siguiendo la arquitectura implementada.

El proceso busca que un nuevo tenant pueda ser incorporado sin modificar la
lógica principal del pipeline, manteniendo el aislamiento de los datos y la
misma secuencia de procesamiento utilizada por los demás tenants.

Como ejemplo se utilizará el tenant `hn`.

---

## 2. Arquitectura del tenant

Cada tenant sigue el flujo de procesamiento:

    Fuente de datos
          |
          v
       Bronze
          |
          v
       Silver
          |
          v
     Quality Gate
          |
          v
        Gold

Los registros generados por las validaciones de calidad se almacenan de forma
independiente en:

    shared/quality_logs/

La estructura lógica esperada para el tenant `hn` es:

    data/
    ├── bronze/
    │   └── hn/
    │       └── deliveries/
    │
    ├── silver/
    │   └── hn/
    │       └── dim_materials/
    │
    ├── gold/
    │   └── hn/
    │       └── daily_metrics_by_delivery_type/
    │
    └── shared/
        └── quality_logs/

En un entorno productivo, estas rutas corresponden a los recursos de
almacenamiento administrados mediante ADLS y las tablas pueden registrarse
mediante Unity Catalog.

---

## 3. Paso 1: Registrar el tenant

El primer paso consiste en registrar el nuevo tenant en la configuración
utilizada por la plataforma.

Para este ejemplo:

    tenant_id = hn

El identificador debe ser único y utilizarse consistentemente en las
diferentes capas del pipeline.

No se debe modificar la lógica de transformación para incorporar un tenant
específico. El tenant debe ser tratado como un parámetro de ejecución.

---

## 4. Paso 2: Provisionar la infraestructura

Antes de ejecutar el pipeline, se deben provisionar los recursos necesarios
para el nuevo tenant.

Como mínimo se consideran:

- Schema correspondiente al tenant en Unity Catalog.
- Ruta de almacenamiento para Bronze.
- Ruta de almacenamiento para Silver.
- Ruta de almacenamiento para Gold.
- Acceso al almacenamiento.
- Permisos correspondientes al tenant.
- Secretos o credenciales requeridos por el entorno productivo.

La automatización de estos recursos se describe en `docs/infra.md`.

Para el tenant `hn`, Terraform recibiría el identificador:

    tenant_id = "hn"

y utilizaría dicho valor para construir las rutas y recursos asociados.

---

## 5. Paso 3: Preparar la capa Bronze

La capa Bronze recibe los datos de origen manteniendo una representación
cercana a la fuente original.

Para `hn`, los datos deben quedar asociados al tenant:

    bronze/hn/deliveries/

La ejecución debe conservar la identificación del tenant mediante el campo
correspondiente de la arquitectura.

El objetivo de esta capa es disponer de una representación persistente de los
datos de entrada antes de aplicar las transformaciones posteriores.

---

## 6. Paso 4: Procesar la capa Silver

La capa Silver aplica las transformaciones y reglas de limpieza definidas
por la plataforma.

Entre las responsabilidades de esta capa se encuentran:

- Normalización de datos.
- Conversión de unidades.
- Validación de claves de negocio.
- Estandarización de atributos.
- Enriquecimiento mediante dimensiones.
- Aplicación de la lógica SCD definida por la arquitectura.

Para `hn`, los datos transformados se almacenan bajo:

    silver/hn/

La lógica utilizada debe ser la misma que para los demás tenants.

---

## 7. Paso 5: Ejecutar las validaciones de calidad

Antes de generar Gold se ejecutan las validaciones de calidad correspondientes
al tenant.

Las validaciones producen registros con el contrato definido para
`quality_logs`:

    _run_id
    _batch_id
    tenant_id
    layer
    table_name
    check_name
    check_severity
    records_checked
    records_failed
    check_passed
    executed_at

Los resultados se almacenan en:

    shared/quality_logs/

Las validaciones críticas forman parte del Quality Gate.

Si una validación crítica falla, el pipeline debe detenerse y no continuar
hacia la generación de Gold.

---

## 8. Paso 6: Generar la capa Gold

Cuando las validaciones críticas son satisfactorias, el pipeline puede
continuar hacia Gold.

Para `hn`, los datasets agregados se almacenan bajo:

    gold/hn/

En esta capa se generan los datasets orientados al consumo analítico y a las
métricas definidas por el proyecto.

Por ejemplo:

    gold/hn/daily_metrics_by_delivery_type/

---

## 9. Paso 7: Validar el onboarding

Una vez finalizado el procesamiento, se debe verificar:

1. Existencia de los datos Bronze del tenant.
2. Existencia de los datasets Silver.
3. Ejecución satisfactoria de los controles de calidad.
4. Existencia de los registros correspondientes en `quality_logs`.
5. Cumplimiento del Quality Gate.
6. Existencia de los datasets Gold.
7. Correcta identificación del tenant en las diferentes capas.

Para `hn`, la validación debe confirmar que los datos generados pertenecen al
tenant `hn` y que no existe contaminación cruzada con otros tenants.

---

## 10. Resultado esperado

El onboarding se considera satisfactorio cuando el tenant `hn` puede recorrer
el flujo completo:

    hn
    |
    +--> Bronze
    |
    +--> Silver
    |
    +--> Quality
    |       |
    |       +--> PASS --> Gold
    |       |
    |       +--> FAIL --> Detener pipeline
    |
    +--> Gold

El procedimiento permite incorporar nuevos tenants utilizando la misma lógica
de procesamiento y evitando implementar transformaciones específicas para cada
tenant.

La infraestructura requerida para este proceso se encuentra descrita en
`docs/infra.md`.