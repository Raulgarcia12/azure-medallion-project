#!/bin/bash

###############################################################################
# Script de Configuración: Service Principal para ADLS Gen2
###############################################################################

set -e  # Salir si hay algún error

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para imprimir mensajes
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Banner
echo "=============================================================================="
echo "  🔐 Configuración de Service Principal para ADLS Gen2"
echo "=============================================================================="
echo ""

# Verificar que Azure CLI está instalado
if ! command -v az &> /dev/null; then
    print_error "Azure CLI no está instalado"
    echo "Instala Azure CLI desde: https://docs.microsoft.com/cli/azure/install-azure-cli"
    exit 1
fi

print_success "Azure CLI encontrado"

# Verificar que el usuario está autenticado
print_info "Verificando autenticación..."
if ! az account show &> /dev/null; then
    print_warning "No estás autenticado en Azure"
    print_info "Iniciando sesión..."
    az login
fi

# Obtener información de la suscripción actual
SUBSCRIPTION_ID=$(az account show --query id -o tsv)
SUBSCRIPTION_NAME=$(az account show --query name -o tsv)

print_success "Autenticado en suscripción: $SUBSCRIPTION_NAME"
echo ""

# Solicitar información del usuario
echo "=============================================================================="
echo "  📋 Configuración"
echo "=============================================================================="
echo ""

read -p "Nombre del Service Principal [databricks-adls-sp]: " SP_NAME
SP_NAME=${SP_NAME:-databricks-adls-sp}

read -p "Nombre del Resource Group: " RESOURCE_GROUP
if [ -z "$RESOURCE_GROUP" ]; then
    print_error "El Resource Group es obligatorio"
    exit 1
fi

read -p "Nombre del Storage Account: " STORAGE_ACCOUNT_NAME
if [ -z "$STORAGE_ACCOUNT_NAME" ]; then
    print_error "El Storage Account es obligatorio"
    exit 1
fi

echo ""
print_info "Selecciona el rol a asignar:"
echo "  1) Storage Blob Data Reader (Solo lectura)"
echo "  2) Storage Blob Data Contributor (Lectura/Escritura) [Recomendado]"
echo "  3) Storage Blob Data Owner (Control total)"
read -p "Opción [2]: " ROLE_OPTION
ROLE_OPTION=${ROLE_OPTION:-2}

case $ROLE_OPTION in
    1)
        ROLE="Storage Blob Data Reader"
        ;;
    2)
        ROLE="Storage Blob Data Contributor"
        ;;
    3)
        ROLE="Storage Blob Data Owner"
        ;;
    *)
        print_error "Opción inválida"
        exit 1
        ;;
esac

echo ""
echo "=============================================================================="
echo "  📝 Resumen de Configuración"
echo "=============================================================================="
echo "Service Principal: $SP_NAME"
echo "Resource Group: $RESOURCE_GROUP"
echo "Storage Account: $STORAGE_ACCOUNT_NAME"
echo "Rol: $ROLE"
echo "Suscripción: $SUBSCRIPTION_NAME"
echo "=============================================================================="
echo ""

read -p "¿Continuar con la creación? (y/n): " CONFIRM
if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
    print_warning "Operación cancelada"
    exit 0
fi

echo ""
print_info "Creando Service Principal..."

# Construir el scope del Storage Account
SCOPE="/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT_NAME"

# Crear Service Principal
SP_OUTPUT=$(az ad sp create-for-rbac \
    --name "$SP_NAME" \
    --role "$ROLE" \
    --scopes "$SCOPE" \
    --output json)

# Extraer credenciales
CLIENT_ID=$(echo $SP_OUTPUT | jq -r '.appId')
CLIENT_SECRET=$(echo $SP_OUTPUT | jq -r '.password')
TENANT_ID=$(echo $SP_OUTPUT | jq -r '.tenant')

echo ""
print_success "Service Principal creado exitosamente!"
echo ""

# Mostrar credenciales
echo "=============================================================================="
echo "  🔑 CREDENCIALES DEL SERVICE PRINCIPAL"
echo "=============================================================================="
echo ""
echo "⚠️  IMPORTANTE: Guarda estas credenciales de forma segura."
echo "   El Client Secret solo se muestra una vez."
echo ""
echo "Application (Client) ID:"
echo "  $CLIENT_ID"
echo ""
echo "Client Secret:"
echo "  $CLIENT_SECRET"
echo ""
echo "Directory (Tenant) ID:"
echo "  $TENANT_ID"
echo ""
echo "Storage Account:"
echo "  $STORAGE_ACCOUNT_NAME"
echo ""
echo "Endpoint:"
echo "  ${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
echo ""
echo "=============================================================================="
echo ""

# Generar código de ejemplo para Databricks
echo "=============================================================================="
echo "  📋 Código para Databricks"
echo "=============================================================================="
echo ""
echo "Copia este código en tu notebook de Databricks:"
echo ""
echo "# Configuración del Storage Account (ADLS Gen2)"
echo "storage_account_name = \"$STORAGE_ACCOUNT_NAME\""
echo "service_principal_client_id = \"$CLIENT_ID\""
echo "service_principal_client_secret = \"$CLIENT_SECRET\""
echo "service_principal_tenant_id = \"$TENANT_ID\""
echo ""
echo "# Configurar Service Principal"
echo "configure_service_principal("
echo "    storage_account_name,"
echo "    service_principal_client_id,"
echo "    service_principal_client_secret,"
echo "    service_principal_tenant_id"
echo ")"
echo ""
echo "=============================================================================="
echo ""

# Guardar credenciales en archivo (opcional)
read -p "¿Guardar credenciales en archivo local? (y/n): " SAVE_FILE
if [ "$SAVE_FILE" == "y" ] || [ "$SAVE_FILE" == "Y" ]; then
    CREDENTIALS_FILE="sp_credentials_${STORAGE_ACCOUNT_NAME}.txt"
    
    cat > "$CREDENTIALS_FILE" << EOF
# Service Principal Credentials
# Created: $(date)
# Storage Account: $STORAGE_ACCOUNT_NAME

Application (Client) ID: $CLIENT_ID
Client Secret: $CLIENT_SECRET
Directory (Tenant) ID: $TENANT_ID
Storage Account: $STORAGE_ACCOUNT_NAME
Endpoint: ${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net
Role: $ROLE

# Databricks Configuration
storage_account_name = "$STORAGE_ACCOUNT_NAME"
service_principal_client_id = "$CLIENT_ID"
service_principal_client_secret = "$CLIENT_SECRET"
service_principal_tenant_id = "$TENANT_ID"
EOF

    print_success "Credenciales guardadas en: $CREDENTIALS_FILE"
    print_warning "IMPORTANTE: Mantén este archivo seguro y no lo subas a control de versiones"
    
    # Agregar al .gitignore si existe
    if [ -f ".gitignore" ]; then
        if ! grep -q "sp_credentials_" .gitignore; then
            echo "sp_credentials_*.txt" >> .gitignore
            print_info "Agregado sp_credentials_*.txt al .gitignore"
        fi
    fi
fi

echo ""
print_success "¡Configuración completada!"
echo ""
print_info "Próximos pasos:"
echo "  1. Copia las credenciales a tu notebook de Databricks"
echo "  2. Ejecuta el notebook 00_mount_storage.py"
echo "  3. Verifica que el montaje sea exitoso"
echo ""
print_warning "Recomendación: Usa Azure Key Vault para almacenar secretos en producción"
echo ""
