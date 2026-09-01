# Infraestructura para el onboarding de tenants

## 1. Objetivo

Este documento describe la infraestructura que sería provisionada mediante
Terraform para soportar el onboarding de nuevos tenants de la plataforma SaaS
de datos.

Terraform se utilizaría para automatizar la creación y configuración de los
recursos necesarios en un entorno productivo basado en Azure, ADLS y
Databricks.

El código presentado es ilustrativo. No se requiere ejecutar `terraform plan`
contra una cuenta real.

---

## 2. Recursos de infraestructura

Para incorporar un nuevo tenant se considera provisionar o configurar:

- Schema del tenant en Unity Catalog.
- Paths de almacenamiento en ADLS.
- External Locations cuando corresponda.
- Credenciales administradas para acceder al almacenamiento.
- Secretos requeridos por los servicios.
- Permisos sobre schemas y almacenamiento.
- Configuración específica del ambiente.

La estructura lógica de almacenamiento es:

    <storage>/
    ├── bronze/<tenant>/
    ├── silver/<tenant>/
    ├── gold/<tenant>/
    └── shared/quality_logs/

El directorio `shared/quality_logs` es común para los registros de calidad de
los diferentes tenants.

---

## 3. Onboarding del tenant `hn`

Para incorporar el tenant `hn`, Terraform recibiría el identificador:

    tenant_id = "hn"

A partir de este valor se construirían los recursos específicos del tenant.

Por ejemplo:

    bronze/hn/
    silver/hn/
    gold/hn/

El schema de Unity Catalog también utilizaría el identificador correspondiente
al tenant.

El objetivo es que la infraestructura pueda reutilizarse para otros tenants
cambiando únicamente las variables de entrada.

---

## 4. Snippet ilustrativo de Terraform

El siguiente módulo representa conceptualmente los recursos principales que
participarían en el onboarding.

```hcl
variable "tenant_id" {
  type        = string
  description = "Identificador único del tenant"
}

variable "storage_account" {
  type        = string
  description = "ID de la cuenta ADLS"
}

variable "container" {
  type        = string
  description = "Container principal del Data Lake"
}

variable "catalog_name" {
  type        = string
  description = "Catálogo de Unity Catalog"
}

locals {
  tenant_path = "${var.container}/${var.tenant_id}"
}

resource "databricks_schema" "tenant" {
  catalog_name = var.catalog_name
  name         = var.tenant_id
  comment      = "Schema para tenant ${var.tenant_id}"
}

resource "azurerm_storage_data_lake_gen2_path" "bronze" {
  storage_account_id = var.storage_account
  filesystem_name    = var.container
  path               = "bronze/${var.tenant_id}"
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "silver" {
  storage_account_id = var.storage_account
  filesystem_name    = var.container
  path               = "silver/${var.tenant_id}"
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "gold" {
  storage_account_id = var.storage_account
  filesystem_name    = var.container
  path               = "gold/${var.tenant_id}"
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "quality_logs" {
  storage_account_id = var.storage_account
  filesystem_name    = var.container
  path               = "shared/quality_logs"
  resource           = "directory"
}

resource "databricks_grants" "tenant_schema" {
  schema = databricks_schema.tenant.id

  grant {
    principal  = "data-engineers"
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY"]
  }
}

output "tenant_schema" {
  value = databricks_schema.tenant.name
}

output "tenant_path" {
  value = local.tenant_path
}