#!/usr/bin/env bash
# Azure resources behind the smart-claims project (reference / re-creation script).
# Mirrors what exists in the subscription today. Review before running: it creates billable resources.
# Names are the ones used in this project; storage-account and SQL-server names are globally unique, change them.
set -euo pipefail

LOCATION=centralindia
RG=rg-databricks-certification
STORAGE=certificationprep001          # ADLS Gen2 (hierarchical namespace) - UC metastore root + claim photos
EH_NAMESPACE=sensor-stream            # Event Hubs, Standard tier (Kafka endpoint requires Standard+)
EH_NAME=telemetry-events              # 5 partitions
SQL_SERVER=jd-sql-learn-2026
SQL_DB=jd-sql-learn-2026
SQL_ADMIN=jd-sql-learn-2026
SQL_PASSWORD=${SQL_PASSWORD:?export SQL_PASSWORD first (never commit it)}
ACCESS_CONNECTOR=ac-databricks-certification
WORKSPACE=adbw-databricks-certification

az group create -n "$RG" -l "$LOCATION"

# --- Storage: ADLS Gen2 with two containers ---------------------------------------------------
az storage account create -g "$RG" -n "$STORAGE" -l "$LOCATION" \
  --sku Standard_LRS --kind StorageV2 --enable-hierarchical-namespace true
az storage container create --account-name "$STORAGE" --auth-mode login -n metastore   # Unity Catalog metastore root
az storage container create --account-name "$STORAGE" --auth-mode login -n claims      # external volume customer_claims

# --- Access connector (managed identity Databricks uses to reach the storage) -----------------
az databricks access-connector create -g "$RG" -n "$ACCESS_CONNECTOR" -l "$LOCATION" \
  --identity-type SystemAssigned
CONNECTOR_PRINCIPAL=$(az databricks access-connector show -g "$RG" -n "$ACCESS_CONNECTOR" \
  --query identity.principalId -o tsv)
STORAGE_ID=$(az storage account show -g "$RG" -n "$STORAGE" --query id -o tsv)
az role assignment create --assignee-object-id "$CONNECTOR_PRINCIPAL" --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" --scope "$STORAGE_ID"

# --- Event Hubs: Standard tier, 5 partitions ---------------------------------------------------
az eventhubs namespace create -g "$RG" -n "$EH_NAMESPACE" -l "$LOCATION" --sku Standard
az eventhubs eventhub create -g "$RG" --namespace-name "$EH_NAMESPACE" -n "$EH_NAME" --partition-count 5
# The producer (src/smart_clames/producer.py) authenticates with a Send policy; Databricks uses the managed identity below.
az eventhubs eventhub authorization-rule create -g "$RG" --namespace-name "$EH_NAMESPACE" --eventhub-name "$EH_NAME" \
  -n producer-send --rights Send
# Databricks reads via a Unity Catalog SERVICE credential backed by the access connector: grant it Data Receiver.
EH_ID=$(az eventhubs namespace show -g "$RG" -n "$EH_NAMESPACE" --query id -o tsv)
az role assignment create --assignee-object-id "$CONNECTOR_PRINCIPAL" --assignee-principal-type ServicePrincipal \
  --role "Azure Event Hubs Data Receiver" --scope "$EH_ID"

# --- Azure SQL Database: serverless General Purpose, auto-pause 60 min ------------------------
az sql server create -g "$RG" -n "$SQL_SERVER" -l "$LOCATION" -u "$SQL_ADMIN" -p "$SQL_PASSWORD"
az sql db create -g "$RG" -s "$SQL_SERVER" -n "$SQL_DB" \
  --edition GeneralPurpose --family Gen5 --compute-model Serverless --capacity 2 --auto-pause-delay 60
# 0.0.0.0 = "Allow Azure services" (lets the Databricks gateway in); add your own client IP for local scripts.
az sql server firewall-rule create -g "$RG" -s "$SQL_SERVER" -n AllowAllWindowsAzureIps \
  --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0
az sql server firewall-rule create -g "$RG" -s "$SQL_SERVER" -n MyClientIP \
  --start-ip-address "$(curl -s https://api.ipify.org)" --end-ip-address "$(curl -s https://api.ipify.org)"

echo "Done. Next: run sql/01..03 on the database, then databricks/setup/unity_catalog.sql in the workspace."
