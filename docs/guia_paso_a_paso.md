## Tabla de Contenidos

1. [Prerrequisitos](#prerrequisitos)
2. [Paso 1: Crear Azure Storage Account](#paso-1-crear-azure-storage-account)
3. [Paso 2: Descargar y Subir Datos](#paso-2-descargar-y-subir-datos)
4. [Paso 3: Configurar Azure Databricks](#paso-3-configurar-azure-databricks)
5. [Paso 4: Configurar Azure Data Factory](#paso-4-configurar-azure-data-factory)
6. [Paso 5: Ejecutar el Pipeline](#paso-5-ejecutar-el-pipeline)
7. [Troubleshooting](#troubleshooting)

---

## Prerrequisitos

Antes de comenzar, asegúrate de tener:

- ✅ Cuenta de Azure (puede ser Free Tier)
- ✅ Cuenta de Kaggle para descargar el dataset
- ✅ Azure CLI instalado (opcional, pero recomendado)

---

## Paso 1: Crear Azure Storage Account

### 1.1 Desde Azure Portal

1. Ve a [Azure Portal](https://portal.azure.com)
2. Busca "Storage accounts" en la barra de búsqueda
3. Click en "+ Create"

### 1.2 Configuración Básica

| Campo | Valor |
|-------|-------|
| Subscription | Tu suscripción |
| Resource group | Crear nuevo: `rg-medallion-project` |
| Storage account name | `stmedallionproject` (debe ser único) |
| Region | El más cercano a ti |
| Performance | Standard |
| Redundancy | LRS (para desarrollo) |

### 1.3 Configuración Avanzada

- **Require secure transfer**: ✅ Enabled
- **Allow Blob anonymous access**: ❌ Disabled
- **Enable hierarchical namespace**: ❌ Disabled (usaremos Blob simple)

### 1.4 Crear los Containers

Una vez creado el Storage Account:

1. Ve al Storage Account creado
2. En el menú lateral, click en "Containers"
3. Crea 4 containers:
   - `landing` (para datos raw CSV)
   - `bronze` (datos en Parquet)
   - `silver` (datos limpios en Delta)
   - `gold` (agregaciones en Delta)

### 1.5 Obtener Access Key

1. En el Storage Account, ve a "Access keys"
2. Click en "Show" para ver las keys
3. Copia `key1` - la necesitarás para Databricks

```
IMPORTANTE: Guarda esta key de forma segura.
En producción, usa Key Vault o Managed Identity.
```

---

## Paso 2: Descargar y Subir Datos

### 2.1 Descargar Dataset de Kaggle

1. Ve a: https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci
2. Click en "Download"
3. Descomprime el archivo

### 2.2 Subir CSV al Container Landing

**Opción A: Azure Portal**
1. Ve a tu Storage Account > Containers > landing
2. Click en "Upload"
3. Selecciona el archivo `online_retail_II.csv`

**Opción B: Azure CLI**
```bash
az storage blob upload \
    --account-name stmedallionproject \
    --container-name landing \
    --name online_retail_II.csv \
    --file ./online_retail_II.csv \
    --account-key <TU_ACCESS_KEY>
```

**Opción C: Azure Storage Explorer**
1. Descarga Azure Storage Explorer
2. Conecta con tu cuenta
3. Arrastra el archivo al container landing

---

## Paso 3: Configurar Azure Databricks

### 3.1 Crear Workspace de Databricks

1. En Azure Portal, busca "Azure Databricks"
2. Click en "+ Create"
3. Configuración:

| Campo | Valor |
|-------|-------|
| Workspace name | `dbw-medallion-project` |
| Region | Mismo que Storage Account |
| Pricing Tier | Standard (para desarrollo) |

### 3.2 Crear un Cluster

1. Abre el workspace de Databricks
2. Ve a "Compute" en el menú lateral
3. Click en "Create Cluster"

**Configuración del Cluster:**
| Campo | Valor |
|-------|-------|
| Cluster name | `cluster-medallion` |
| Cluster mode | Single Node (para desarrollo) |
| Databricks Runtime | 14.3 LTS (o la más reciente LTS) |
| Node type | Standard_DS3_v2 |
| Terminate after | 60 minutes de inactividad |

### 3.3 Importar Notebooks

1. En Databricks, ve a "Workspace"
2. Click derecho en "Shared" > "Create" > "Folder"
3. Nombra la carpeta `medallion-project`
4. Para cada notebook del proyecto:
   - Click derecho > "Import"
   - Selecciona el archivo .py
   - Repite para los 4 notebooks

### 3.4 Configurar Conexión al Storage

1. Abre el notebook `00_mount_storage.py`
2. Modifica las variables:

```python
storage_account_name = "stmedallionproject"  # Tu storage account
storage_account_key = "tu-access-key-aqui"   # Tu access key
```

3. Ejecuta el notebook para montar el storage

### 3.5 Obtener Token de Acceso (para Data Factory)

1. En Databricks, click en tu perfil (esquina superior derecha)
2. Ve a "Settings" > "Developer" > "Access tokens"
3. Click en "Generate new token"
4. Descripción: "Data Factory Access"
5. Lifetime: 90 días (ajustar según necesidad)
6. **Guarda el token** - solo se muestra una vez

---

## Paso 4: Configurar Azure Data Factory

### 4.1 Crear Instancia de Data Factory

1. En Azure Portal, busca "Data Factory"
2. Click en "+ Create"
3. Configuración:

| Campo | Valor |
|-------|-------|
| Name | `adf-medallion-project` |
| Region | Mismo que los otros recursos |
| Version | V2 |

4. Click en "Review + create"

### 4.2 Abrir Data Factory Studio

1. Una vez creado, click en "Launch Studio"
2. Esto abre la interfaz de desarrollo de ADF

### 4.3 Crear Linked Services

**Linked Service: Azure Blob Storage**

1. Ve a "Manage" > "Linked services"
2. Click en "+ New"
3. Busca "Azure Blob Storage"
4. Configuración:
   - Name: `ls_AzureBlobStorage`
   - Authentication: Account key
   - Account selection: From Azure subscription
   - Storage account: `stmedallionproject`
5. Click en "Test connection" y luego "Create"

**Linked Service: Azure Databricks**

1. Click en "+ New"
2. Busca "Azure Databricks"
3. Configuración:
   - Name: `ls_AzureDatabricks`
   - Databricks workspace: Seleccionar tu workspace
   - Select cluster: Existing interactive cluster
   - Access token: Pegar el token generado en 3.5
   - Cluster: Seleccionar `cluster-medallion`
4. Click en "Test connection" y luego "Create"

### 4.4 Crear Datasets (Opcional para este proyecto)

Los datasets ya están definidos en los JSON del proyecto. Puedes importarlos:

1. Ve a "Author" > "Datasets"
2. Click en "..." > "Import from pipeline template"
3. O crea manualmente siguiendo los JSON

### 4.5 Crear Pipelines

**Pipeline Bronze:**
1. Ve a "Author" > "Pipelines"
2. Click en "+" > "New pipeline"
3. Nombre: `pl_bronze_ingestion`
4. Arrastra "Databricks Notebook" al canvas
5. Configura:
   - Linked service: `ls_AzureDatabricks`
   - Notebook path: `/Shared/medallion-project/01_bronze_ingestion`

**Repite para Silver y Gold**, cambiando el notebook path.

**Pipeline Maestro:**
1. Crea nuevo pipeline: `pl_master_medallion`
2. Arrastra "Execute Pipeline" para cada capa
3. Configura las dependencias:
   - Mount Storage → Bronze → Silver → Gold

---

## Paso 5: Ejecutar el Pipeline

### 5.1 Ejecución Manual

1. Abre `pl_master_medallion`
2. Click en "Debug" para prueba
3. O click en "Add trigger" > "Trigger now" para ejecución completa

### 5.2 Monitorear Ejecución

1. Ve a "Monitor" en el menú lateral
2. Verás el estado de cada pipeline
3. Click en el nombre para ver detalles

### 5.3 Verificar Resultados

En Databricks, ejecuta estas queries:

```sql
-- Verificar Bronze
SELECT COUNT(*) FROM retail_medallion.bronze_online_retail;

-- Verificar Silver
SELECT COUNT(*) FROM retail_medallion.silver_online_retail;

-- Verificar Gold
SELECT * FROM retail_medallion.gold_sales_by_country LIMIT 5;
```

### 5.4 Crear Schedule (Opcional)

1. En el pipeline, click en "Add trigger" > "New/Edit"
2. Click en "+ New"
3. Configura el schedule (diario, horario, etc.)
4. Activa el trigger

---

## Troubleshooting

### Error: "Mount point already exists"
```python
# En el notebook 00, desmonta primero:
dbutils.fs.unmount("/mnt/landing")
```

### Error: "Access denied" en Storage
- Verifica que la access key sea correcta
- Asegúrate que el container existe
- Revisa los permisos del Storage Account

### Error: "Cluster not found" en Data Factory
- Verifica que el cluster esté encendido
- Revisa el token de acceso (puede haber expirado)

### Error: "Notebook not found"
- Verifica la ruta del notebook en Databricks
- Asegúrate que está en `/Shared/medallion-project/`

### Pipeline falla sin error claro
1. Ve a Monitor > Pipeline runs
2. Click en el pipeline fallido
3. Click en la actividad que falló
4. Revisa "Error details" y "Output"

---

## Métricas de Éxito

Al finalizar, deberías tener:

| Capa | Formato | Registros Aprox. |
|------|---------|------------------|
| Landing | CSV | ~500,000 |
| Bronze | Parquet | ~500,000 |
| Silver | Delta | ~400,000 |
| Gold | Delta | Agregados |

---

## Conceptos Aprendidos

Al completar este proyecto habrás practicado:

### Azure Storage
- ✅ Crear Storage Accounts
- ✅ Gestionar containers y blobs
- ✅ Usar access keys
- ✅ Subir archivos

### Azure Databricks
- ✅ Crear workspaces y clusters
- ✅ Montar storage externo
- ✅ Trabajar con Spark DataFrames
- ✅ Implementar arquitectura Medallion
- ✅ Usar Delta Lake

### Azure Data Factory
- ✅ Crear Linked Services
- ✅ Configurar Datasets
- ✅ Diseñar Pipelines
- ✅ Usar Databricks Notebook Activity
- ✅ Orquestar flujos de datos
- ✅ Monitorear ejecuciones


