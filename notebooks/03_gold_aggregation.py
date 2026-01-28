

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime


storage_account_name = "adlsdatahack90" 

# ----------------------------------------
# Credenciales de Service Principal
# ----------------------------------------
service_principal_client_id = "ClientId"        
service_principal_client_secret = "ClientSecret" 
service_principal_tenant_id = "TenantId"        

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


# ========================================
# DEFINIR PATHS ADLS GEN2
# ========================================

# Paths del proyecto usando ADLS Gen2 (abfss://)
def get_adls_path(container: str, path: str = "") -> str:
    """Genera la ruta ADLS Gen2 completa"""
    base = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

SILVER_PATH = get_adls_path("silver", "online_retail")
GOLD_PATH = get_adls_path("gold")

# Subcarpetas Gold
GOLD_SALES_BY_COUNTRY = get_adls_path("gold", "sales_by_country")
GOLD_TOP_PRODUCTS = get_adls_path("gold", "top_products")
GOLD_CUSTOMER_METRICS = get_adls_path("gold", "customer_metrics")
GOLD_DAILY_TRENDS = get_adls_path("gold", "daily_trends")

# Leer datos de la capa Silver
df_silver = (spark.read
    .format("delta")
    .load(SILVER_PATH)
    .filter(col("is_cancelled") == False)  # Excluir cancelaciones
)

print(f"✅ Datos leídos de Silver: {df_silver.count():,} registros")
print(f"  (Excluyendo transacciones canceladas)")


# Cache para mejor performance en múltiples agregaciones
df_silver.cache()
print("✅ DataFrame cacheado para optimizar agregaciones")


df_sales_country = (df_silver
    .groupBy("country")
    .agg(
        count("invoice").alias("total_transactions"),
        countDistinct("invoice").alias("unique_invoices"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("total_items_sold"),
        round(sum("line_total"), 2).alias("total_revenue"),
        round(avg("line_total"), 2).alias("avg_transaction_value"),
        round(avg("quantity"), 2).alias("avg_items_per_transaction"),
        min("invoicedate").alias("first_purchase_date"),
        max("invoicedate").alias("last_purchase_date")
    )
    .withColumn("revenue_per_customer", 
        round(col("total_revenue") / col("unique_customers"), 2)
    )
    .withColumn("_gold_timestamp", current_timestamp())
    .orderBy(desc("total_revenue"))
)

display(df_sales_country)

# Guardar tabla Gold
(df_sales_country
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_SALES_BY_COUNTRY)
)

print(f"✅ Tabla sales_by_country guardada: {df_sales_country.count()} países")

# Métricas por producto
df_products = (df_silver
    .groupBy("stockcode", "description")
    .agg(
        sum("quantity").alias("total_quantity_sold"),
        round(sum("line_total"), 2).alias("total_revenue"),
        countDistinct("invoice").alias("times_purchased"),
        countDistinct("customer_id").alias("unique_buyers"),
        round(avg("price"), 2).alias("avg_price"),
        countDistinct("country").alias("countries_sold")
    )
)

# Agregar rankings
window_qty = Window.orderBy(desc("total_quantity_sold"))
window_rev = Window.orderBy(desc("total_revenue"))

df_top_products = (df_products
    .withColumn("rank_by_quantity", rank().over(window_qty))
    .withColumn("rank_by_revenue", rank().over(window_rev))
    .withColumn("_gold_timestamp", current_timestamp())
    .orderBy("rank_by_revenue")
)

display(df_top_products.limit(20))


# Guardar tabla Gold
(df_top_products
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_TOP_PRODUCTS)
)

print(f"✅ Tabla top_products guardada: {df_top_products.count()} productos")

# Fecha de referencia (última fecha en el dataset)
max_date = df_silver.select(max("invoicedate")).collect()[0][0]
print(f"📅 Fecha de referencia para Recency: {max_date}")

# Calcular métricas RFM
df_rfm = (df_silver
    .filter(col("customer_id") != -1)  # Excluir clientes anónimos
    .groupBy("customer_id")
    .agg(
        # Recency: días desde última compra
        datediff(lit(max_date), max("invoicedate")).alias("recency_days"),
        
        # Frequency: número de compras únicas
        countDistinct("invoice").alias("frequency"),
        
        # Monetary: valor total gastado
        round(sum("line_total"), 2).alias("monetary_value"),
        
        # Métricas adicionales
        count("*").alias("total_items_purchased"),
        round(avg("line_total"), 2).alias("avg_order_value"),
        min("invoicedate").alias("first_purchase"),
        max("invoicedate").alias("last_purchase"),
        countDistinct("country").alias("countries_purchased_from")
    )
)

# Calcular percentiles para scoring
quantiles_r = df_rfm.approxQuantile("recency_days", [0.25, 0.5, 0.75], 0.05)
quantiles_f = df_rfm.approxQuantile("frequency", [0.25, 0.5, 0.75], 0.05)
quantiles_m = df_rfm.approxQuantile("monetary_value", [0.25, 0.5, 0.75], 0.05)

# Asignar scores RFM (1-4)
df_customer_metrics = (df_rfm
    .withColumn("r_score",
        when(col("recency_days") <= quantiles_r[0], 4)
        .when(col("recency_days") <= quantiles_r[1], 3)
        .when(col("recency_days") <= quantiles_r[2], 2)
        .otherwise(1)
    )
    .withColumn("f_score",
        when(col("frequency") >= quantiles_f[2], 4)
        .when(col("frequency") >= quantiles_f[1], 3)
        .when(col("frequency") >= quantiles_f[0], 2)
        .otherwise(1)
    )
    .withColumn("m_score",
        when(col("monetary_value") >= quantiles_m[2], 4)
        .when(col("monetary_value") >= quantiles_m[1], 3)
        .when(col("monetary_value") >= quantiles_m[0], 2)
        .otherwise(1)
    )
    .withColumn("rfm_score", col("r_score") + col("f_score") + col("m_score"))
    .withColumn("customer_segment",
        when(col("rfm_score") >= 10, "Champions")
        .when(col("rfm_score") >= 8, "Loyal Customers")
        .when(col("rfm_score") >= 6, "Potential Loyalists")
        .when(col("rfm_score") >= 4, "At Risk")
        .otherwise("Lost")
    )
    .withColumn("_gold_timestamp", current_timestamp())
)

display(df_customer_metrics.orderBy(desc("monetary_value")).limit(20))


# Distribución de segmentos
display(df_customer_metrics.groupBy("customer_segment").count().orderBy(desc("count")))


# Guardar tabla Gold
(df_customer_metrics
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_CUSTOMER_METRICS)
)

print(f"✅ Tabla customer_metrics guardada: {df_customer_metrics.count()} clientes")


df_daily = (df_silver
    .withColumn("date", to_date("invoicedate"))
    .groupBy("date")
    .agg(
        countDistinct("invoice").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("total_items"),
        round(sum("line_total"), 2).alias("daily_revenue"),
        round(avg("line_total"), 2).alias("avg_order_value"),
        countDistinct("stockcode").alias("unique_products_sold"),
        countDistinct("country").alias("countries_with_orders")
    )
    .withColumn("day_of_week", dayofweek("date"))
    .withColumn("day_name", 
        when(col("day_of_week") == 1, "Sunday")
        .when(col("day_of_week") == 2, "Monday")
        .when(col("day_of_week") == 3, "Tuesday")
        .when(col("day_of_week") == 4, "Wednesday")
        .when(col("day_of_week") == 5, "Thursday")
        .when(col("day_of_week") == 6, "Friday")
        .otherwise("Saturday")
    )
    .withColumn("month", month("date"))
    .withColumn("year", year("date"))
    .withColumn("_gold_timestamp", current_timestamp())
    .orderBy("date")
)

display(df_daily)

# Guardar tabla Gold
(df_daily
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_DAILY_TRENDS)
)

print(f"✅ Tabla daily_trends guardada: {df_daily.count()} días")

# Registrar todas las tablas Gold
gold_tables = [
    ("gold_sales_by_country", GOLD_SALES_BY_COUNTRY),
    ("gold_top_products", GOLD_TOP_PRODUCTS),
    ("gold_customer_metrics", GOLD_CUSTOMER_METRICS),
    ("gold_daily_trends", GOLD_DAILY_TRENDS)
]


#  Crear Vistas Temporales


print("📊 Creando vistas temporales...")
for table_name, path in gold_tables:
    # Usar el nombre sin el prefijo 'gold_' para simplificar consultas
    view_name = table_name.replace("gold_", "")
    
    df_temp = spark.read.format("delta").load(path)
    df_temp.createOrReplaceTempView(view_name)
    print(f"  ✅ Vista temporal '{view_name}' creada (Path: {path})")

print("\n   Puedes consultar usando: SELECT * FROM sales_by_country")


df_silver.unpersist()
print("✅ Cache liberado")


print("=" * 70)
print("📊 RESUMEN CAPA GOLD")
print("=" * 70)
print("\n🥇 Tablas Gold Creadas:\n")

for table_name, path in gold_tables:
    count = spark.read.format("delta").load(path).count()
    print(f"  📌 {table_name}")
    print(f"     Path: {path}")
    print(f"     Registros: {count:,}")
    print()

print("=" * 70)
print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)



display(spark.sql("""
    SELECT country, total_revenue, unique_customers, revenue_per_customer
    FROM sales_by_country
    ORDER BY total_revenue DESC
    LIMIT 5
"""))



display(spark.sql("""
    SELECT 
        customer_segment,
        COUNT(*) as customers,
        ROUND(AVG(monetary_value), 2) as avg_value,
        ROUND(AVG(frequency), 1) as avg_frequency
    FROM customer_metrics
    GROUP BY customer_segment
    ORDER BY avg_value DESC
"""))



display(spark.sql("""
    SELECT 
        day_name,
        ROUND(AVG(daily_revenue), 2) as avg_revenue,
        ROUND(AVG(total_orders), 0) as avg_orders
    FROM daily_trends
    GROUP BY day_name, day_of_week
    ORDER BY day_of_week
"""))

