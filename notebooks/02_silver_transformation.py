# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Capa Silver: Transformación y Limpieza
# MAGIC 
# MAGIC ## Objetivo
# MAGIC Aplicar transformaciones de limpieza y calidad de datos:
# MAGIC - Eliminar duplicados
# MAGIC - Manejar valores nulos
# MAGIC - Normalizar nombres de columnas
# MAGIC - Filtrar registros inválidos
# MAGIC - Guardar en formato Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports y Configuración

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Paths y Configuración ADLS Gen2

# COMMAND ----------

# ========================================
# CONFIGURACIÓN DEL STORAGE ACCOUNT
# ========================================

# ⚠️ IMPORTANTE: Actualiza estos valores
storage_account_name = "adlsdatahack90"  # Tu Storage Account

# ----------------------------------------
# Credenciales de Service Principal
# ----------------------------------------
service_principal_client_id = "tuClientId"        # Application (client) ID
service_principal_client_secret = "tuClientSecret"  # Client Secret
service_principal_tenant_id = "tuTenantId"        # Directory (tenant) ID

# COMMAND ----------

# ========================================
# CONFIGURAR AUTENTICACIÓN OAUTH 2.0
# ========================================

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

# COMMAND ----------

# ========================================
# DEFINIR PATHS ADLS GEN2
# ========================================

# Paths del proyecto usando ADLS Gen2 (abfss://)
def get_adls_path(container: str, path: str = "") -> str:
    """Genera la ruta ADLS Gen2 completa"""
    base = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

BRONZE_PATH = get_adls_path("bronze", "online_retail")
SILVER_PATH = get_adls_path("silver", "online_retail")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer Datos de Bronze

# COMMAND ----------

# Leer datos de la capa Bronze
df_bronze = spark.read.parquet(BRONZE_PATH)

print(f"✅ Datos leídos de Bronze: {df_bronze.count():,} registros")
print(f"📋 Columnas: {df_bronze.columns}")

# COMMAND ----------

# Vista previa
display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análisis de Calidad de Datos

# COMMAND ----------

# Análisis de nulos
print("🔍 Análisis de Valores Nulos:")
print("-" * 40)

total_rows = df_bronze.count()
for col_name in df_bronze.columns:
    null_count = df_bronze.filter(col(col_name).isNull()).count()
    pct = (null_count / total_rows) * 100
    if null_count > 0:
        print(f"  {col_name}: {null_count:,} ({pct:.2f}%)")

# COMMAND ----------

# Análisis de duplicados
print("🔍 Análisis de Duplicados:")
duplicates = df_bronze.count() - df_bronze.dropDuplicates().count()
print(f"  Registros duplicados: {duplicates:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformaciones de Limpieza

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Normalizar Nombres de Columnas

# COMMAND ----------

def normalize_column_name(name: str) -> str:
    """Normaliza nombres de columnas: snake_case, sin espacios"""
    return (name
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
    )

# Aplicar normalización
df_normalized = df_bronze
for old_name in df_bronze.columns:
    new_name = normalize_column_name(old_name)
    df_normalized = df_normalized.withColumnRenamed(old_name, new_name)

print("✅ Columnas normalizadas:")
print(f"  {df_normalized.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Filtrar Registros Inválidos

# COMMAND ----------

# Filtrar registros con cantidad <= 0 o precio <= 0 (devoluciones y errores)
df_valid = (df_normalized
    .filter(col("quantity") > 0)
    .filter(col("price") > 0)
)

removed_count = df_normalized.count() - df_valid.count()
print(f"✅ Registros eliminados (cantidad/precio inválidos): {removed_count:,}")
print(f"  Registros restantes: {df_valid.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Manejar Valores Nulos

# COMMAND ----------

# Análisis de Customer ID nulos
null_customers = df_valid.filter(col("customer_id").isNull()).count()
print(f"📊 Registros sin Customer ID: {null_customers:,}")

# Para este proyecto, mantendremos los registros sin customer_id
# pero los marcaremos como "ANONYMOUS"
df_cleaned = (df_valid
    .withColumn("customer_id", 
        when(col("customer_id").isNull(), lit(-1))
        .otherwise(col("customer_id").cast("integer"))
    )
    .withColumn("description",
        when(col("description").isNull(), lit("NO DESCRIPTION"))
        .otherwise(col("description"))
    )
)

print("✅ Valores nulos manejados")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Eliminar Duplicados

# COMMAND ----------

# Eliminar duplicados exactos
df_deduplicated = df_cleaned.dropDuplicates()

duplicates_removed = df_cleaned.count() - df_deduplicated.count()
print(f"✅ Duplicados eliminados: {duplicates_removed:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Agregar Columnas Calculadas

# COMMAND ----------

# Agregar columnas útiles para análisis
df_enriched = (df_deduplicated
    # Total de línea
    .withColumn("line_total", round(col("quantity") * col("price"), 2))
    
    # Extraer componentes de fecha
    .withColumn("invoice_year", year(col("invoicedate")))
    .withColumn("invoice_month", month(col("invoicedate")))
    .withColumn("invoice_day", dayofmonth(col("invoicedate")))
    .withColumn("invoice_hour", hour(col("invoicedate")))
    .withColumn("day_of_week", dayofweek(col("invoicedate")))
    
    # Indicador de transacción cancelada (Invoice empieza con 'C')
    .withColumn("is_cancelled", 
        when(col("invoice").startswith("C"), lit(True))
        .otherwise(lit(False))
    )
    
    # Timestamp de procesamiento Silver
    .withColumn("_silver_timestamp", current_timestamp())
)

print("✅ Columnas calculadas agregadas:")
new_cols = [c for c in df_enriched.columns if c not in df_deduplicated.columns]
print(f"  {new_cols}")

# COMMAND ----------

# Vista previa del resultado
display(df_enriched.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardar en Capa Silver (Delta Lake)

# COMMAND ----------

# Guardar en formato Delta con partición por año y mes
(df_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("invoice_year", "invoice_month")
    .save(SILVER_PATH)
)

print(f"✅ Datos guardados en: {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimizar Tabla Delta

# COMMAND ----------

# Optimizar la tabla Delta para mejor performance
spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}`")
print("✅ Tabla Delta optimizada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar Escritura

# COMMAND ----------

# Verificar datos guardados
df_verify = spark.read.format("delta").load(SILVER_PATH)
print(f"✅ Verificación: {df_verify.count():,} registros en Silver")

# COMMAND ----------

# Ver historial de versiones Delta
display(spark.sql(f"DESCRIBE HISTORY delta.`{SILVER_PATH}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registrar como Vista/Tabla (Compatible con Unity Catalog)
# MAGIC 
# MAGIC **Nota**: DBFS público está deshabilitado en workspaces modernos.

# COMMAND ----------

# ========================================
# OPCIÓN 1: Crear Vista Temporal
# ========================================

df_verify = spark.read.format("delta").load(SILVER_PATH)
df_verify.createOrReplaceTempView("silver_online_retail")

print("✅ Vista temporal 'silver_online_retail' creada")
print("   Puedes usar: SELECT * FROM silver_online_retail")

# COMMAND ----------

# ========================================
# OPCIÓN 2: Unity Catalog (Si está habilitado)
# ========================================
# Descomenta si tienes Unity Catalog

"""
spark.sql("CREATE SCHEMA IF NOT EXISTS tu_catalogo.retail_medallion")
spark.sql(f'''
    CREATE TABLE IF NOT EXISTS tu_catalogo.retail_medallion.silver_online_retail
    USING DELTA
    LOCATION '{SILVER_PATH}'
''')
print("✅ Tabla Unity Catalog creada")
"""

# COMMAND ----------

# Query de prueba usando vista temporal
print("📊 Top 10 países por revenue:")
display(spark.sql("""
    SELECT 
        country,
        COUNT(*) as transactions,
        ROUND(SUM(line_total), 2) as total_revenue,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM silver_online_retail
    WHERE is_cancelled = false
    GROUP BY country
    ORDER BY total_revenue DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de la Capa Silver

# COMMAND ----------

# Comparación Bronze vs Silver
bronze_count = df_bronze.count()
silver_count = df_enriched.count()
reduction = bronze_count - silver_count
reduction_pct = (reduction / bronze_count) * 100

summary = {
    "Capa": "Silver",
    "Formato Entrada": "Parquet (Bronze)",
    "Formato Salida": "Delta Lake",
    "Registros Bronze": f"{bronze_count:,}",
    "Registros Silver": f"{silver_count:,}",
    "Reducción": f"{reduction:,} ({reduction_pct:.1f}%)",
    "Columnas Nuevas": len(df_enriched.columns) - len(df_bronze.columns),
    "Path": SILVER_PATH,
    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

print("=" * 60)
print("📊 RESUMEN CAPA SILVER")
print("=" * 60)
for key, value in summary.items():
    print(f"  {key}: {value}")
print("=" * 60)

# COMMAND ----------

# Transformaciones aplicadas
print("\n🔧 Transformaciones Aplicadas:")
print("  1. ✅ Normalización de nombres de columnas")
print("  2. ✅ Filtrado de cantidad/precio inválidos")
print("  3. ✅ Manejo de valores nulos")
print("  4. ✅ Eliminación de duplicados")
print("  5. ✅ Columnas calculadas (line_total, fechas, is_cancelled)")
print("  6. ✅ Conversión a formato Delta Lake")
print("  7. ✅ Particionamiento por año/mes")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Próximo Paso
# MAGIC 
# MAGIC Continúa con el notebook **03_gold_aggregation.py** para crear las agregaciones de negocio.
