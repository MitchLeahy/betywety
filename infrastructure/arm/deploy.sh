#!/bin/bash
# Deploy ARM template for Kalshi pipeline
# Prereqs: az login, az account set

set -e
RESOURCE_GROUP="${1:-rg-kalshi-pipeline}"
LOCATION="${2:-eastus2}"

echo "Creating resource group: $RESOURCE_GROUP in $LOCATION"
az group create --name "$RESOURCE_GROUP" --location "$LOCATION" --output table

echo "Deploying ARM template..."
az deployment group create \
  --resource-group "$RESOURCE_GROUP" \
  --name kalshi-deploy \
  --template-file mainTemplate.json \
  --parameters parameters.json \
  --output table

echo ""
echo "Deployment complete. Outputs:"
az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --name kalshi-deploy \
  --query properties.outputs \
  --output table
