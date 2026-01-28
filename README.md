# Azure Data Engineering Project: Storage + Databricks + Data Factory

## Proyecto: Arquitectura Medallion con Servicios Azure

- **Azure Blob Storage** - Almacenamiento de datos
- **Azure Databricks** - Procesamiento con arquitectura Medallion (Bronze → Silver → Gold)
- **Azure Data Factory** - Orquestación de pipelines

### Dataset: Online Retail Dataset (Kaggle)

Usaremos el dataset **Online Retail II** de Kaggle que contiene transacciones de una tienda de retail online del Reino Unido.

 https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci


## Arquitectura del Proyecto

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

## Implementación

### Paso 1: Configure Azure & Service Principal
1. Puedes seguir la guía en `docs/service_principal_setup.md` para crear tu identidad de servicio en Azure AD.
2. Asigne el rol **Storage Blob Data Contributor** al Service Principal en tu Storage Account.
3. Cree los containers (`landing`, `bronze`, `silver`, `gold`).

### Paso 2: Configuración de Entorno
1. Clone este repositorio.
2. En los notebooks de `notebooks/`, configure credenciales:
   ```python
   storage_account_name = "storage-account"
   service_principal_client_id = "client-id"
   service_principal_client_secret = "client-secret"
   service_principal_tenant_id = "tenant-id"
   ```
   > En producción, utilizar Azure Key Vault backed scopes en Databricks.

### Paso 3: Ejecución de la Pipeline
1. **Ingesta (Bronze)**: Ejecutar `01_bronze_ingestion.py`. Ingesta datos crudos a Parquet.
2. **Transformación (Silver)**: Ejecutar `02_silver_transformation.py`. Limpieza, deduplicación y guardado en Delta Lake.
3. **Agregación (Gold)**: Ejecutar `03_gold_aggregation.py`. Genera métricas de negocio.

Nota: Este proyecto utiliza **Vistas Temporales** de Spark para compatibilidad con workspaces modernos donde DBFS root está deshabilitado.

Esta vista es de la orquestacion de DataFactory
<img width="1857" height="884" alt="image" src="https://github.com/user-attachments/assets/8cfb1b89-fd6e-4584-a557-8c78d39abed5" />
 Esta Vista es desde Databricks haciendo un job y cargando todo para orquestarlo desde Azure DataFactory
 <img width="1780" height="807" alt="image" src="https://github.com/user-attachments/assets/f921e186-46e1-4677-91ce-7a83da97adcc" />
<img width="1533" height="780" alt="image" src="https://github.com/user-attachments/assets/4ca68bca-3045-4fd0-a9ff-1c03baa6a003" />


---

## Conceptos Clave:

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
Proyecto creado por GRaul Garcia. GRaulgarcia12@gmail.com

