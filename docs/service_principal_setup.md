# 🔐 Guía de Configuración: Service Principal para ADLS Gen2

Esta guía te llevará paso a paso para crear y configurar un Service Principal en Azure Active Directory para autenticarte de forma segura con Azure Data Lake Storage Gen2.

---

## 📋 Requisitos Previos

- Suscripción activa de Azure
- Permisos de **Contributor** o **Owner** en la suscripción
- Permisos para crear aplicaciones en Azure Active Directory
- Azure CLI instalado (opcional, pero recomendado)

---

## 🎯 ¿Qué es un Service Principal?

Un **Service Principal** es una identidad de seguridad utilizada por aplicaciones, servicios y herramientas de automatización para acceder a recursos específicos de Azure. Es similar a una "cuenta de servicio" en otros sistemas.

### Ventajas sobre Access Key:
- ✅ **Seguridad**: No expone las claves del Storage Account
- ✅ **Control granular**: Permisos específicos con RBAC
- ✅ **Auditoría**: Registro completo de accesos
- ✅ **Rotación**: Cambio de secretos sin afectar el código

---

## 🚀 Método 1: Crear Service Principal con Azure Portal

### Paso 1: Crear una App Registration

1. Inicia sesión en [Azure Portal](https://portal.azure.com)
2. Navega a **Azure Active Directory**
3. En el menú lateral, selecciona **App registrations**
4. Haz clic en **+ New registration**
5. Configura:
   - **Name**: `databricks-adls-sp` (o el nombre que prefieras)
   - **Supported account types**: "Accounts in this organizational directory only"
   - **Redirect URI**: Dejar en blanco
6. Haz clic en **Register**

### Paso 2: Obtener las Credenciales

Después de crear la aplicación, verás la página de **Overview**:

1. **Application (client) ID**: Copia este valor
   ```
   service_principal_client_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
   ```

2. **Directory (tenant) ID**: Copia este valor
   ```
   service_principal_tenant_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
   ```

### Paso 3: Crear un Client Secret

1. En el menú lateral de la aplicación, selecciona **Certificates & secrets**
2. Haz clic en **+ New client secret**
3. Configura:
   - **Description**: `databricks-secret`
   - **Expires**: Selecciona la duración (recomendado: 12 meses)
4. Haz clic en **Add**
5. **IMPORTANTE**: Copia el **Value** inmediatamente (solo se muestra una vez)
   ```
   service_principal_client_secret = "tu-secreto-aqui"
   ```

### Paso 4: Asignar Permisos al Storage Account

1. Navega a tu **Storage Account** en Azure Portal
2. En el menú lateral, selecciona **Access Control (IAM)**
3. Haz clic en **+ Add** → **Add role assignment**
4. Configura:
   - **Role**: Selecciona uno de los siguientes según tus necesidades:
     - **Storage Blob Data Contributor** - Lectura, escritura y eliminación
     - **Storage Blob Data Reader** - Solo lectura
     - **Storage Blob Data Owner** - Control total
5. En la pestaña **Members**:
   - **Assign access to**: User, group, or service principal
   - Haz clic en **+ Select members**
   - Busca el nombre de tu aplicación (`databricks-adls-sp`)
   - Selecciónala y haz clic en **Select**
6. Haz clic en **Review + assign**

---

## 🖥️ Método 2: Crear Service Principal con Azure CLI

Este método es más rápido y automatizado.

### Paso 1: Instalar Azure CLI

Si no lo tienes instalado:
- **Windows**: Descarga desde [aka.ms/installazurecliwindows](https://aka.ms/installazurecliwindows)
- **macOS**: `brew install azure-cli`
- **Linux**: Sigue las instrucciones en [docs.microsoft.com](https://docs.microsoft.com/cli/azure/install-azure-cli)

### Paso 2: Iniciar Sesión

```bash
az login
```

### Paso 3: Crear el Service Principal

```bash
# Reemplaza con tu información
SUBSCRIPTION_ID="tu-subscription-id"
STORAGE_ACCOUNT_NAME="tuStorageAccount"
RESOURCE_GROUP="tuResourceGroup"
SP_NAME="databricks-adls-sp"

# Crear Service Principal
az ad sp create-for-rbac \
  --name $SP_NAME \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT_NAME
```

### Paso 4: Guardar las Credenciales

El comando anterior devolverá un JSON con las credenciales:

```json
{
  "appId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "displayName": "databricks-adls-sp",
  "password": "tu-secreto-generado",
  "tenant": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

Mapea estos valores:
- `appId` → `service_principal_client_id`
- `password` → `service_principal_client_secret`
- `tenant` → `service_principal_tenant_id`

---

## 🔒 Mejores Prácticas de Seguridad

### 1. Usar Azure Key Vault (Recomendado para Producción)

En lugar de hardcodear los secretos en el notebook, usa Azure Key Vault:

```python
# En Databricks, crear un secret scope
# CLI de Databricks:
# databricks secrets create-scope --scope adls-secrets

# Almacenar secretos
# databricks secrets put --scope adls-secrets --key client-id
# databricks secrets put --scope adls-secrets --key client-secret
# databricks secrets put --scope adls-secrets --key tenant-id

# En el notebook:
client_id = dbutils.secrets.get(scope="adls-secrets", key="client-id")
client_secret = dbutils.secrets.get(scope="adls-secrets", key="client-secret")
tenant_id = dbutils.secrets.get(scope="adls-secrets", key="tenant-id")
```

### 2. Principio de Mínimo Privilegio

Asigna solo los permisos necesarios:
- **Lectura**: `Storage Blob Data Reader`
- **Lectura/Escritura**: `Storage Blob Data Contributor`
- **Control total**: `Storage Blob Data Owner` (solo si es absolutamente necesario)

### 3. Rotación de Secretos

- Configura expiración de secretos (máximo 12 meses)
- Crea un nuevo secreto antes de que expire el actual
- Actualiza las referencias en Databricks
- Elimina el secreto antiguo

### 4. Auditoría

Habilita logging en el Storage Account:
1. Navega a **Storage Account** → **Diagnostic settings**
2. Agrega configuración de diagnóstico
3. Selecciona logs de **StorageRead**, **StorageWrite**, **StorageDelete**
4. Envía a Log Analytics Workspace

---

## ✅ Verificar la Configuración

### Prueba 1: Verificar Permisos

```bash
# Verificar que el Service Principal tiene acceso
az role assignment list \
  --assignee <client-id> \
  --scope /subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<storage-account>
```

### Prueba 2: Probar en Databricks

Ejecuta el notebook `00_mount_storage.py` con las credenciales del Service Principal:

```python
storage_account_name = "tuStorageAccount"
service_principal_client_id = "tu-client-id"
service_principal_client_secret = "tu-client-secret"
service_principal_tenant_id = "tu-tenant-id"

configure_service_principal(
    storage_account_name,
    service_principal_client_id,
    service_principal_client_secret,
    service_principal_tenant_id
)
```

Si ves el mensaje `✅ Service Principal configurado exitosamente`, ¡todo está funcionando!

---

## 🐛 Troubleshooting

### Error: "Forbidden" o "Authorization failed"

**Causa**: El Service Principal no tiene permisos suficientes.

**Solución**:
1. Verifica que asignaste el rol correcto en el Storage Account
2. Espera 5-10 minutos para que los permisos se propaguen
3. Verifica que estás usando el Storage Account correcto

### Error: "Invalid client secret"

**Causa**: El secreto es incorrecto o ha expirado.

**Solución**:
1. Genera un nuevo client secret en Azure Portal
2. Actualiza la configuración en Databricks

### Error: "AADSTS700016: Application not found"

**Causa**: El Client ID es incorrecto o la aplicación fue eliminada.

**Solución**:
1. Verifica el Client ID en Azure Portal → App registrations
2. Asegúrate de copiar el **Application (client) ID**, no el Object ID

---

## 📚 Recursos Adicionales

- [Documentación oficial de Service Principals](https://docs.microsoft.com/azure/active-directory/develop/app-objects-and-service-principals)
- [RBAC para Azure Storage](https://docs.microsoft.com/azure/storage/common/storage-auth-aad-rbac-portal)
- [Databricks Secret Scopes](https://docs.microsoft.com/azure/databricks/security/secrets/secret-scopes)
- [ADLS Gen2 Access Control](https://docs.microsoft.com/azure/storage/blobs/data-lake-storage-access-control)

---

## 🎓 Próximos Pasos

1. ✅ Crear Service Principal (completado con esta guía)
2. 📝 Configurar secretos en Azure Key Vault
3. 🔗 Conectar Databricks con Key Vault
4. 🚀 Ejecutar notebooks con autenticación segura
