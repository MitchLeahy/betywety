#!/bin/bash
# Create service principal for Databricks to access Key Vault and Storage
# Run from infrastructure/arm after deployment. Usage: ./create-service-principal.sh [resource-group] [deployment-name]

set -e
RG="${1:-rg-kalshi-pipeline}"
DEPLOY_NAME="${2:-kalshi-deploy}"
SP_NAME="sp-kalshi-databricks"

echo "Resource group: $RG"
echo "Deployment name: $DEPLOY_NAME"
echo ""

# Get outputs
echo "Fetching deployment outputs..."
KV_NAME=$(az deployment group show -g "$RG" -n "$DEPLOY_NAME" --query properties.outputs.keyVaultName.value -o tsv)
STORAGE_NAME=$(az deployment group show -g "$RG" -n "$DEPLOY_NAME" --query properties.outputs.storageAccountName.value -o tsv)
SUB_ID=$(az account show --query id -o tsv)
KV_SCOPE="/subscriptions/$SUB_ID/resourceGroups/$RG/providers/Microsoft.KeyVault/vaults/$KV_NAME"
STORAGE_SCOPE="/subscriptions/$SUB_ID/resourceGroups/$RG/providers/Microsoft.Storage/storageAccounts/$STORAGE_NAME"

echo "Key Vault: $KV_NAME"
echo "Storage:   $STORAGE_NAME"
echo ""

# Create SP with Key Vault access
echo "Creating service principal: $SP_NAME"
SP_OUTPUT=$(az ad sp create-for-rbac \
  --name "$SP_NAME" \
  --role "Key Vault Secrets User" \
  --scopes "$KV_SCOPE" \
  --output json)
APP_ID=$(echo "$SP_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('appId',''))")
PASSWORD=$(echo "$SP_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('password',''))")

echo "App ID: $APP_ID"
echo ""

# Grant Storage access
echo "Granting Storage Blob Data Contributor to $SP_NAME..."
az role assignment create \
  --assignee "$APP_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "$STORAGE_SCOPE" \
  --output none

echo ""
echo "Done. Save these for Databricks:"
echo "  App ID:     $APP_ID"
echo "  Secret:     $PASSWORD"
echo "  Tenant ID:  $(az account show --query tenantId -o tsv)"
echo ""
echo "Add secrets to Key Vault:"
echo "  az keyvault secret set --vault-name $KV_NAME --name kalshi-api-key --value \"YOUR_API_KEY\""
echo "  az keyvault secret set --vault-name $KV_NAME --name kalshi-private-key --file /path/to/your.pem"
echo "  az keyvault secret set --vault-name $KV_NAME --name sp-client-secret --value \"$PASSWORD\""
