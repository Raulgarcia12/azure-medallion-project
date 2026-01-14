# 🏗️ Azure Data Engineering Project: Storage + Databricks + Data Factory

## Proyecto Educativo: Arquitectura Medallion con Servicios Azure

Este proyecto te guiará paso a paso para construir una pipeline de datos completa utilizando:
- **Azure Blob Storage** - Almacenamiento de datos
- **Azure Databricks** - Procesamiento con arquitectura Medallion (Bronze → Silver → Gold)
- **Azure Data Factory** - Orquestación de pipelines

### 📊 Dataset: Online Retail Dataset (Kaggle)

Usaremos el dataset **Online Retail II** de Kaggle que contiene transacciones de una tienda de retail online del Reino Unido.

**Descarga:** https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci

---

## 🏛️ Arquitectura del Proyecto

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          AZURE DATA FACTORY                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                 │
│  │  Pipeline   │───▶│  Pipeline   │───▶│  Pipeline   │                 │
│  │   Bronze    │    │   Silver    │    │    Gold     │                 │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                 │
└─────────┼──────────────────┼──────────────────┼────────────────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         AZURE DATABRICKS                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                 │
│  │   Bronze    │───▶│   Silver    │───▶│    Gold     │                 │
│  │  (Raw Data) │    │ (Cleaned)   │    │(Aggregated) │                 │
│  └─────────────┘    └─────────────┘    └─────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
          ▲                  │                  │
          │                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        AZURE DATA LAKE GEN2                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                 │
│  │  /landing   │    │   /bronze   │    │  /silver    │    /gold        │
│  │  (CSV raw)  │    │  (Parquet)  │    │  (Delta)    │   (Delta)       │
│  └─────────────┘    └─────────────┘    └─────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Guía Rápida de Implementación

### Paso 1: Configurar Azure & Service Principal
1. Seguir la guía en `docs/service_principal_setup.md` para crear tu identidad de servicio en Azure AD.
2. Asignar el rol **Storage Blob Data Contributor** al Service Principal en tu Storage Account.
3. Crear los containers (`landing`, `bronze`, `silver`, `gold`).

### Paso 2: Configuración de Entorno
1. Clonar este repositorio.
2. En los notebooks de `notebooks/`, configurar tus credenciales:
   ```python
   storage_account_name = "tu-storage-account"
   service_principal_client_id = "tu-client-id"
   service_principal_client_secret = "tu-client-secret"
   service_principal_tenant_id = "tu-tenant-id"
   ```
   > 🔒 **Recomendación**: En producción, utilizar Azure Key Vault backed scopes en Databricks.

### Paso 3: Ejecución de la Pipeline
1. **Ingesta (Bronze)**: Ejecutar `01_bronze_ingestion.py`. Ingesta datos crudos a Parquet.
2. **Transformación (Silver)**: Ejecutar `02_silver_transformation.py`. Limpieza, deduplicación y guardado en Delta Lake.
3. **Agregación (Gold)**: Ejecutar `03_gold_aggregation.py`. Genera métricas de negocio.

Nota: Este proyecto utiliza **Vistas Temporales** de Spark para compatibilidad con workspaces modernos donde DBFS root está deshabilitado.

---

## 📚 Conceptos Clave que Aprenderás

### Azure Data Lake Gen2
- Jerarquías de archivos y seguridad con ACLs/RBAC.
- Integración segura vía OAuth 2.0 (Service Principal).

### Azure Databricks & Delta Lake
- **Arquitectura Medallion** (Bronze/Silver/Gold).
- **Delta Lake**: ACID transactions, Time Travel, Schema Enforcement.
- **PySpark**: Transformaciones eficientes y optimizadas.
- Manejo de secretos y seguridad en notebooks.

---

## 📧 Contacto
Proyecto creado para el curso de Data Engineering.

