# ARM Deployment for Kalshi Pipeline

Deploys Azure resources for the Kalshi college basketball live price pipeline. See [project README](../../README.md) for data flow diagram.

## Resources Created

| Resource | Type | Purpose |
|----------|------|---------|
| Storage Account | ADLS Gen2 | Delta tables (bronze/silver/gold) |
| Container | kalshi-data | Data container in storage |
| Key Vault | Azure Key Vault | Kalshi API key and PEM |
| Databricks Workspace | Azure Databricks | Notebooks, jobs, streaming |

**Note**: Databricks workspace does not support managed identity in ARM. You must manually create a service principal and grant it access to Key Vault and Storage (see Post-deployment below).

## Prerequisites

- Azure CLI (`az`) logged in: `az login`
- Subscription selected: `az account set --subscription "<id>"`

## Deploy

### 1. Create resource group (optional)

```bash
az group create --name rg-kalshi-pipeline --location eastus2
```

### 2. Deploy main template

```bash
cd infrastructure/arm
az deployment group create \
  --resource-group rg-kalshi-pipeline \
  --name kalshi-deploy \
  --template-file mainTemplate.json \
  --parameters parameters.json
```

### 3. Post-deployment (required)

1. **Create a service principal** — run the script (use `mainTemplate` if you deployed before adding `--name kalshi-deploy`):

   ```bash
   chmod +x create-service-principal.sh
   ./create-service-principal.sh rg-kalshi-pipeline mainTemplate
   ```

   Save the App ID, Secret, and Tenant ID from the output for Databricks.

   Or in Azure Portal: create an App registration, add a client secret, then assign:
   - Key Vault Secrets User (on the Key Vault)
   - Storage Blob Data Contributor (on the Storage account)

2. **Add secrets to Key Vault** (if starting in a new terminal, run the `RG`, `SUB_ID`, `KV_NAME` lines from step 1 again):

   ```bash
   az keyvault secret set --vault-name $KV_NAME --name kalshi-api-key --value "YOUR_API_KEY"
   az keyvault secret set --vault-name $KV_NAME --name kalshi-private-key --file /path/to/your.pem
   az keyvault secret set --vault-name $KV_NAME --name sp-client-secret --value "<SP_PASSWORD_FROM_STEP_1>"
   ```

3. **Link Key Vault to Databricks** (uses the service principal from step 1):

   - Workspace > Settings > Workspace > Security > Secret scopes
   - Create secret scope > Azure Key Vault-backed
   - Scope name: `kalshi-secrets`
   - DNS name: `https://<keyVaultName>.vault.azure.net/`
   - Resource ID: from Key Vault Properties in Azure Portal

4. **Configure ADLS access in Databricks**:

   - Upload and run `notebooks/mount_adls.ipynb`
   - Set `STORAGE_ACCOUNT` from: `az deployment group show -g rg-kalshi-pipeline -n kalshi-deploy --query properties.outputs.storageAccountName.value -o tsv`
   - Run all cells (requires `sp-client-secret` in Key Vault)
   - Uses direct ABFSS paths (no mount) - works when DBFS mounts are disabled. Use `KALSHI_DATA_PATH` in your pipelines.

5. **Run the pipeline notebooks** (in order):

   - `notebooks/kalshi_ingestion_pipeline.py` — REST fetch, bronze, silver, ticker inspection, price update
   - `notebooks/kalshi_websocket_stream.py` — Stream live ticker/trade data to bronze (run ingestion first so silver/markets exists)

## Outputs

After deployment, view outputs.

```bash
az deployment group show -g rg-kalshi-pipeline -n kalshi-deploy --query properties.outputs -o table
```

| Output | Description |
|--------|-------------|
| storageAccountName | ADLS storage account name |
| keyVaultName | Key Vault name |
| keyVaultUri | Key Vault URI for secret scope |
| databricksWorkspaceUrl | Databricks workspace URL |
| adlsPath | ABFSS path for kalshi-data container |

## Customize

Edit `parameters.json`:

- `projectName`: prefix for resource names
- `location`: Azure region (e.g. eastus2, westus2)
- `databricksPricingTier`: `standard` or `premium`
