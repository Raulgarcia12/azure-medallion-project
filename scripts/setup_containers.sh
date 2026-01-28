#!/bin/bash

STORAGE_ACCOUNT_NAME="StorageAccount"
RESOURCE_GROUP="ResourceGroup"

# Containers a crear
CONTAINERS=("landing" "bronze" "silver" "gold")

echo "=============================================="
echo "🚀 Creando containers en Azure Blob Storage"
echo "=============================================="
echo "Storage Account: $STORAGE_ACCOUNT_NAME"
echo "Resource Group: $RESOURCE_GROUP"
echo ""

# Verificar que Azure CLI está instalado
if ! command -v az &> /dev/null; then
    echo "❌ Azure CLI no está instalado. Por favor instálalo primero."
    echo "   https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
    exit 1
fi

# Verificar login
echo "📋 Verificando autenticación..."
if ! az account show &> /dev/null; then
    echo "❌ No estás autenticado. Ejecuta 'az login' primero."
    exit 1
fi

# Obtener la cuenta activa
CURRENT_ACCOUNT=$(az account show --query name -o tsv)
echo "✅ Conectado a: $CURRENT_ACCOUNT"
echo ""

# Obtener connection string
echo "🔑 Obteniendo credenciales del storage account..."
CONNECTION_STRING=$(az storage account show-connection-string \
    --name $STORAGE_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --query connectionString -o tsv)

if [ -z "$CONNECTION_STRING" ]; then
    echo "❌ No se pudo obtener el connection string. Verifica el nombre del storage account y resource group."
    exit 1
fi

echo "✅ Connection string obtenido"
echo ""

# Crear containers
echo "📦 Creando containers..."
for container in "${CONTAINERS[@]}"; do
    echo -n "  Creando '$container'... "
    
    # Verificar si ya existe
    EXISTS=$(az storage container exists \
        --name $container \
        --connection-string "$CONNECTION_STRING" \
        --query exists -o tsv)
    
    if [ "$EXISTS" = "true" ]; then
        echo "⚠️  Ya existe"
    else
        az storage container create \
            --name $container \
            --connection-string "$CONNECTION_STRING" \
            --public-access off \
            --output none
        echo "✅ Creado"
    fi
done

echo ""
echo "=============================================="
echo "✅ Setup completado!"
echo "=============================================="
echo ""
echo "📋 Containers creados:"
for container in "${CONTAINERS[@]}"; do
    echo "  - $container"
done
echo ""
echo "📝 Próximos pasos:"
echo "  1. Descarga el dataset de Kaggle"
echo "  2. Sube el CSV al container 'landing'"
echo "  3. Configura Databricks y los notebooks"
echo "  4. Configura Data Factory"
echo ""

# Mostrar Access Key para referencia
echo "🔐 Access Key (para configurar Databricks):"
ACCESS_KEY=$(az storage account keys list \
    --account-name $STORAGE_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --query '[0].value' -o tsv)
echo "  $ACCESS_KEY"
echo ""
echo "⚠️  IMPORTANTE: Guarda esta key de forma segura"
