
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime


# CONFIGURACIÓN DEL STORAGE ACCOUNT


storage_account_name = "adlsdatahack90"  # Ejemplo


# Credenciales de Service Principal

service_principal_client_id = "ClientId"       
service_principal_client_secret = "ClientSecret"  
service_principal_tenant_id = "TenantId"        



# CONFIGURAR AUTENTICACIÓN OAUTH 2.0

def configure_service_principal():
    """Configura acceso a ADLS Gen2 con Service Principal"""
    try:
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
            service_principal_client_id
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",
            service_principal_client_secret
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{service_principal_tenant_id}/oauth2/token"
        )
        print(f"🔐 Service Principal configurado para: {storage_account_name}")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

# Ejecutar configuración
configure_service_principal()

# DEFINIR PATHS ADLS GEN2

# Paths del proyecto usando ADLS Gen2 (abfss://)
def get_adls_path(container: str, path: str = "") -> str:
    """Genera la ruta ADLS Gen2 completa"""
    base = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

LANDING_PATH = get_adls_path("landing")
BRONZE_PATH = get_adls_path("bronze")

# Archivo de entrada
INPUT_FILE = get_adls_path("landing", "online_retail_II.csv")

# Carpeta de salida Bronze
OUTPUT_PATH = get_adls_path("bronze", "online_retail")

# Ver archivos disponibles en landing para hacer la exploracion
print("Archivos en Landing:")
try:
    files = dbutils.fs.ls(LANDING_PATH)
    for f in files:
        size_mb = f.size / (1024 * 1024)
        print(f"  - {f.name} ({size_mb:.2f} MB)")
except Exception as e:
    print(f"⚠️ Error: {e}")
    print("Asegúrate de haber subido el archivo CSV a la carpeta landing")

# Schema explícito para mejor control y performance
retail_schema = StructType([
    StructField("Invoice", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Customer ID", DoubleType(), True),
    StructField("Country", StringType(), True)
])

# Leer CSV con schema inferido
df_raw = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("encoding", "UTF-8")
    .load(INPUT_FILE)
)

print(f"✅ Datos cargados: {df_raw.count():,} registros")

# Ver schema
print("📋 Schema del DataFrame:")
df_raw.printSchema()

# Muestra de datos
display(df_raw.limit(10))

# Estadísticas básicas
print("📊 Estadísticas del Dataset:")
print(f"  - Total registros: {df_raw.count():,}")
print(f"  - Total columnas: {len(df_raw.columns)}")
print(f"  - Columnas: {df_raw.columns}")

# Verificar valores nulos por columna
print("🔍 Valores nulos por columna:")
null_counts = df_raw.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df_raw.columns
])
display(null_counts)

# Agregar columnas de metadatos para trazabilidad
df_bronze = (df_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_ingestion_date", current_date())
)

print("✅ Metadatos agregados:")
print("  - _ingestion_timestamp: Timestamp de ingesta")
print("  - _source_file: Archivo fuente")
print("  - _ingestion_date: Fecha de ingesta")

# Verificar metadatos
display(df_bronze.select("_ingestion_timestamp", "_source_file", "_ingestion_date").limit(5))

# Guardar en formato Parquet con partición por fecha de ingesta
(df_bronze
    .write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("_ingestion_date")
    .save(OUTPUT_PATH)
)

print(f"✅ Datos guardados en: {OUTPUT_PATH}")

# Listar archivos creados
print("📁 Archivos en Bronze:")
files = dbutils.fs.ls(OUTPUT_PATH)
for f in files:
    print(f"  - {f.name}")

# Leer y verificar datos guardados
df_verify = spark.read.parquet(OUTPUT_PATH)
print(f"✅ Verificación: {df_verify.count():,} registros en Bronze")

#Crear Vista Temporal 

df_verify = spark.read.parquet(OUTPUT_PATH)
df_verify.createOrReplaceTempView("bronze_online_retail")

print("✅ Vista temporal 'bronze_online_retail' creada")
print("   Puedes usar: SELECT * FROM bronze_online_retail")


# Verificar con SQL usando la vista temporal
print("📋 Muestra de datos desde la vista temporal:")
display(spark.sql("SELECT * FROM bronze_online_retail LIMIT 5"))

# Resumen final
summary = {
    "Capa": "Bronze",
    "Formato Entrada": "CSV",
    "Formato Salida": "Parquet",
    "Registros": df_bronze.count(),
    "Columnas Originales": len(df_raw.columns),
    "Columnas Totales": len(df_bronze.columns),
    "Path": OUTPUT_PATH,
    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

print("=" * 60)
print("📊 RESUMEN CAPA BRONZE")
print("=" * 60)
for key, value in summary.items():
    print(f"  {key}: {value}")
print("=" * 60)
