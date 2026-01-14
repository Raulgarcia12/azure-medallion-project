# 🎓 Capacidades y Servicios Azure - Resumen de Aprendizaje

Este documento resume todas las capacidades y servicios de Azure que se practican en este proyecto.

---

## 📦 Azure Blob Storage

### Conceptos Cubiertos

| Concepto | Descripción | Dónde se usa |
|----------|-------------|--------------|
| **Storage Account** | Contenedor principal de almacenamiento | Creación inicial |
| **Containers** | Agrupación lógica de blobs | landing, bronze, silver, gold |
| **Blobs** | Archivos almacenados | CSV, Parquet, Delta |
| **Access Keys** | Autenticación basada en llaves | Conexión Databricks |
| **Connection String** | Cadena de conexión completa | Data Factory Linked Service |

### Operaciones Practicadas
- ✅ Crear Storage Account
- ✅ Crear y gestionar containers
- ✅ Subir archivos (blobs)
- ✅ Configurar permisos de acceso
- ✅ Obtener y usar Access Keys

### Formatos de Datos
- **Landing**: CSV (datos crudos)
- **Bronze**: Parquet (columnar, comprimido)
- **Silver/Gold**: Delta Lake (ACID, time travel)

---

## 🔷 Azure Databricks

### Conceptos Cubiertos

| Concepto | Descripción | Dónde se usa |
|----------|-------------|--------------|
| **Workspace** | Ambiente de desarrollo | Contiene notebooks, clusters |
| **Cluster** | Recursos de cómputo | Ejecuta código Spark |
| **Notebook** | Código interactivo | Transformaciones ETL |
| **Mount Point** | Acceso a storage externo | `/mnt/landing`, etc. |
| **Delta Lake** | Formato de tabla ACID | Capas Silver y Gold |
| **Catalog** | Metadatos de tablas | `retail_medallion` database |

### Operaciones Practicadas

**Gestión de Clusters:**
- ✅ Crear cluster interactivo
- ✅ Configurar auto-terminación
- ✅ Seleccionar runtime y tipo de nodo

**Acceso a Datos:**
- ✅ Montar Azure Storage con `dbutils.fs.mount()`
- ✅ Configurar Spark con access key
- ✅ Usar rutas `wasbs://` y `/mnt/`

**Spark Operations:**
- ✅ Leer CSV con inferencia de schema
- ✅ Escribir Parquet particionado
- ✅ Escribir Delta Lake
- ✅ Transformaciones con PySpark
- ✅ Funciones de fecha y tiempo
- ✅ Agregaciones y window functions

**Delta Lake:**
- ✅ Escribir en formato Delta
- ✅ Optimizar tablas (`OPTIMIZE`)
- ✅ Ver historial (`DESCRIBE HISTORY`)
- ✅ Crear tablas externas

### Arquitectura Medallion

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
│  Raw Data   │     │   Cleaned   │     │  Business   │
│  (Parquet)  │     │   (Delta)   │     │   (Delta)   │
└─────────────┘     └─────────────┘     └─────────────┘
```

**Bronze (Capa de Ingesta):**
- Datos en formato original
- Sin transformaciones
- Metadatos de ingesta agregados

**Silver (Capa de Transformación):**
- Datos limpios y validados
- Duplicados eliminados
- Nulos manejados
- Schema normalizado

**Gold (Capa de Negocio):**
- Agregaciones por dimensión
- KPIs listos para consumo
- Optimizado para queries

---

## 🏭 Azure Data Factory

### Conceptos Cubiertos

| Concepto | Descripción | Dónde se usa |
|----------|-------------|--------------|
| **Linked Service** | Conexión a fuentes/destinos | Storage, Databricks |
| **Dataset** | Definición de datos | CSV, Parquet |
| **Pipeline** | Flujo de trabajo | Bronze, Silver, Gold, Master |
| **Activity** | Acción individual | Notebook, Copy, Validation |
| **Trigger** | Iniciador de pipeline | Scheduled, Event-based |

### Tipos de Activities Usadas

| Activity | Descripción | Ejemplo |
|----------|-------------|---------|
| **Databricks Notebook** | Ejecuta notebook en cluster | Notebooks medallion |
| **Execute Pipeline** | Llama otro pipeline | Pipeline maestro |
| **Copy Data** | Mueve datos entre fuentes | CSV a Parquet |
| **Validation** | Valida existencia de datos | Verificar CSV existe |
| **Get Metadata** | Obtiene info del archivo | Tamaño, fecha modificación |
| **Set Variable** | Asigna valor a variable | Guardar métricas |

### Linked Services Configurados

**Azure Blob Storage:**
```json
{
    "type": "AzureBlobStorage",
    "connectionString": "DefaultEndpointsProtocol=https;..."
}
```

**Azure Databricks:**
```json
{
    "type": "AzureDatabricks",
    "domain": "https://xxx.azuredatabricks.net",
    "accessToken": "dapi...",
    "existingClusterId": "xxxx-xxxxxx-xxxxxxxx"
}
```

### Tipos de Triggers

| Trigger | Descripción | Caso de Uso |
|---------|-------------|-------------|
| **Schedule** | Basado en tiempo | Ejecución diaria 6AM |
| **Blob Events** | Basado en eventos | Archivo nuevo en landing |
| **Tumbling Window** | Ventanas de tiempo | Procesamiento por hora |
| **Manual** | Ejecución manual | Debugging |

### Patrones de Orquestación

**Pipeline Secuencial:**
```
Mount → Bronze → Silver → Gold
```

**Manejo de Dependencias:**
```json
"dependsOn": [
    {
        "activity": "Previous Activity",
        "dependencyConditions": ["Succeeded"]
    }
]
```

---

## 🔐 Consideraciones de Seguridad (Producción)

Este proyecto usa Access Keys directos para simplificar el aprendizaje. 
En producción, deberías usar:

| Método Desarrollo | Método Producción |
|-------------------|-------------------|
| Access Key en código | Azure Key Vault |
| Token estático | Managed Identity |
| Permisos amplios | RBAC granular |
| Sin encriptación | Customer-managed keys |

### Migración a Producción

1. **Key Vault Integration:**
   - Crear Azure Key Vault
   - Almacenar secrets (Access Keys, Tokens)
   - Referenciar desde Data Factory

2. **Managed Identity:**
   - Habilitar en Data Factory
   - Asignar roles en Storage Account
   - Eliminar access keys del código

3. **Databricks Secret Scopes:**
   - Crear scope backed by Key Vault
   - Usar `dbutils.secrets.get()` en notebooks

---

## 📊 Métricas del Proyecto

| Métrica | Valor Aproximado |
|---------|------------------|
| Registros Landing (CSV) | ~500,000 |
| Registros Bronze (Parquet) | ~500,000 |
| Registros Silver (Delta) | ~400,000 |
| Países en Gold | ~40 |
| Productos únicos | ~4,000 |
| Clientes únicos | ~4,300 |

---

## 🎯 Skills Demostrados

Al completar este proyecto, puedes demostrar experiencia en:

- ✅ Diseño de arquitectura Medallion
- ✅ Azure Blob Storage administration
- ✅ Azure Databricks development
- ✅ Apache Spark / PySpark
- ✅ Delta Lake implementation
- ✅ Azure Data Factory orchestration
- ✅ ETL pipeline development
- ✅ Data quality management
- ✅ Cloud data engineering

---

*Proyecto educativo para el curso de Data Engineering*
