# Databricks notebook source
# MAGIC %md
# MAGIC # 🔌 Configurar Acceso a Azure Data Lake Storage Gen2 en Databricks
# MAGIC 
# MAGIC Este notebook configura el acceso directo entre Databricks y ADLS Gen2
# MAGIC usando Access Key (método directo sin montaje DBFS).
# MAGIC 
# MAGIC **Nota**: Los montajes DBFS están deshabilitados en workspaces modernos de Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración de Variables
# MAGIC 
# MAGIC ⚠️ **IMPORTANTE**: Reemplaza estos valores con los de tu Storage Account

# COMMAND ----------

# ========================================
# CONFIGURACIÓN DEL STORAGE ACCOUNT
# ========================================

storage_account_name = "tuStorageAccount"  # Reemplazar con tu nombre de storage
container_names = ["landing", "bronze", "silver", "gold"]

# ----------------------------------------
# Opción 1: Access Key (menos seguro, más simple)
# ----------------------------------------
storage_account_key = "tuAccessKey"  # Reemplazar con tu Access Key

# ----------------------------------------
# Opción 2: Service Principal (recomendado para producción)
# ----------------------------------------
service_principal_client_id = "tuClientId"        # Application (client) ID
service_principal_client_secret = "tuClientSecret"  # Client Secret
service_principal_tenant_id = "tuTenantId"        # Directory (tenant) ID

# ----------------------------------------
# Seleccionar método de autenticación
# ----------------------------------------
# Opciones: "access_key" o "service_principal"
auth_method = "service_principal"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración de Acceso Directo a ADLS Gen2
# MAGIC 
# MAGIC Este notebook soporta dos métodos de autenticación:
# MAGIC 
# MAGIC | Método | Seguridad | Uso Recomendado |
# MAGIC |--------|-----------|-----------------|
# MAGIC | **Access Key** | Básica | Desarrollo/Pruebas |
# MAGIC | **Service Principal** | Alta (OAuth 2.0) | Producción |
# MAGIC 
# MAGIC **Nota**: Los montajes DBFS están deshabilitados en workspaces modernos de Databricks.

# COMMAND ----------

def configure_access_key(storage_account_name, storage_account_key):
    """
    Configura el acceso directo a ADLS Gen2 usando Access Key.
    
    ⚠️ Método menos seguro, recomendado solo para desarrollo/pruebas.
    
    Args:
        storage_account_name: Nombre del Storage Account
        storage_account_key: Access Key del Storage Account
    
    Returns:
        bool: True si la configuración fue exitosa
    """
    try:
        # Configurar la clave de acceso para ADLS Gen2
        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
            storage_account_key
        )
        print(f"🔑 Acceso a ADLS Gen2 configurado con Access Key: {storage_account_name}")
        return True
    except Exception as e:
        print(f"❌ Error configurando Access Key: {str(e)}")
        return False

# COMMAND ----------

def configure_service_principal(storage_account_name, client_id, client_secret, tenant_id):
    """
    Configura el acceso directo a ADLS Gen2 usando Service Principal (OAuth 2.0).
    
    ✅ Método recomendado para producción.
    
    Args:
        storage_account_name: Nombre del Storage Account
        client_id: Application (Client) ID del Service Principal
        client_secret: Client Secret del Service Principal
        tenant_id: Directory (Tenant) ID
    
    Returns:
        bool: True si la configuración fue exitosa
    """
    try:
        # Configurar OAuth 2.0 con Service Principal
        spark.conf.set(
            f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net",
            "OAuth"
        )
        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net",
            client_id
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",
            client_secret
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        )
        
        print(f"🔐 Service Principal configurado exitosamente para: {storage_account_name}")
        print(f"   Client ID: {client_id[:8]}...{client_id[-4:]}")
        print(f"   Tenant ID: {tenant_id[:8]}...{tenant_id[-4:]}")
        return True
    except Exception as e:
        print(f"❌ Error configurando acceso: {str(e)}")
        return False

# COMMAND ----------

def verify_container_access(container_name, storage_account_name):
    """
    Verifica que se pueda acceder a un container específico
    """
    try:
        path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
        dbutils.fs.ls(path)
        print(f"✅ Acceso verificado para container: '{container_name}'")
        return True
    except Exception as e:
        print(f"⚠️ Container '{container_name}' no accesible o vacío: {str(e)}")
        return False

# COMMAND ----------

# Configurar acceso a ADLS Gen2
print("=" * 60)
print("🚀 Configurando acceso a ADLS Gen2...")
print(f"📋 Método de autenticación: {auth_method.upper()}")
print("=" * 60)

# Seleccionar método de autenticación
config_success = False

if auth_method == "service_principal":
    print("\n🔐 Usando Service Principal (OAuth 2.0)...")
    config_success = configure_service_principal(
        storage_account_name,
        service_principal_client_id,
        service_principal_client_secret,
        service_principal_tenant_id
    )
elif auth_method == "access_key":
    print("\n🔑 Usando Access Key...")
    config_success = configure_access_key(storage_account_name, storage_account_key)
else:
    print(f"❌ Método de autenticación no válido: '{auth_method}'")
    print("   Opciones válidas: 'access_key' o 'service_principal'")

# Verificar acceso a containers
if config_success:
    print("\n📦 Verificando acceso a containers:")
    print("-" * 60)
    for container in container_names:
        verify_container_access(container, storage_account_name)

print("=" * 60)
print("✅ Configuración completada")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Información de Rutas de Acceso
# MAGIC 
# MAGIC Con la configuración anterior, puedes acceder a tus containers usando el protocolo ADLS Gen2:
# MAGIC 
# MAGIC ```python
# MAGIC # Formato de ruta ADLS Gen2:
# MAGIC # abfss://<container>@<storage_account>.dfs.core.windows.net/<path>
# MAGIC 
# MAGIC # Ejemplo:
# MAGIC df = spark.read.parquet(f"abfss://landing@{storage_account_name}.dfs.core.windows.net/data/")
# MAGIC ```

# COMMAND ----------

# Mostrar información de acceso
print("✅ Configuración de acceso directo completada")
print(f"📂 Formato de ruta: abfss://<container>@{storage_account_name}.dfs.core.windows.net/<path>")
print(f"\n📋 Containers disponibles: {', '.join(container_names)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probar Acceso a Containers

# COMMAND ----------

# Verificar acceso al container landing usando ADLS Gen2
try:
    landing_path = f"abfss://landing@{storage_account_name}.dfs.core.windows.net/"
    files = dbutils.fs.ls(landing_path)
    print(f"📁 Archivos en container 'landing':")
    for file in files[:10]:  # Mostrar solo los primeros 10
        print(f"  - {file.name} ({file.size} bytes)")
    if len(files) > 10:
        print(f"  ... y {len(files) - 10} archivos más")
except Exception as e:
    print(f"⚠️ Container 'landing' vacío o no accesible: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones Útiles para el Proyecto

# COMMAND ----------

def get_storage_path(layer: str, path: str = "", storage_account: str = None) -> str:
    """
    Retorna la ruta completa ADLS Gen2 para una capa del medallion
    
    Args:
        layer: 'landing', 'bronze', 'silver', 'gold'
        path: ruta adicional dentro del container
        storage_account: nombre del storage account (usa la variable global si no se especifica)
    
    Returns:
        Ruta completa ADLS Gen2 del archivo/directorio
    """
    if storage_account is None:
        storage_account = storage_account_name
    
    base_path = f"abfss://{layer}@{storage_account}.dfs.core.windows.net"
    if path:
        return f"{base_path}/{path.lstrip('/')}"
    return base_path + "/"


def list_files(layer: str, path: str = "") -> list:
    """
    Lista archivos en una capa específica usando ADLS Gen2
    """
    full_path = get_storage_path(layer, path)
    try:
        return dbutils.fs.ls(full_path)
    except:
        return []


def file_exists(layer: str, path: str) -> bool:
    """
    Verifica si un archivo existe en ADLS Gen2
    """
    try:
        dbutils.fs.ls(get_storage_path(layer, path))
        return True
    except:
        return False


# Ejemplo de uso de las funciones
print("\n📚 Funciones auxiliares disponibles:")
print("  - get_storage_path(layer, path): Obtiene ruta ADLS Gen2")
print("  - list_files(layer, path): Lista archivos en una capa")
print("  - file_exists(layer, path): Verifica existencia de archivo")
print("\nEjemplo:")
print(f"  get_storage_path('landing', 'data/file.csv')")
print(f"  → {get_storage_path('landing', 'data/file.csv')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Próximo Paso
# MAGIC 
# MAGIC Continúa con el notebook **01_bronze_ingestion.py** para ingestar los datos raw.
# MAGIC 
# MAGIC **Nota**: Asegúrate de actualizar las rutas en tus notebooks para usar el formato ADLS Gen2:
# MAGIC ```python
# MAGIC abfss://<container>@<storage_account>.dfs.core.windows.net/<path>
# MAGIC ```
