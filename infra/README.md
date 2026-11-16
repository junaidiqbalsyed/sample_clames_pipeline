# infra/

Infrastructure as scripts. Review before running: these create billable resources.

| Path | Purpose |
|---|---|
| `azure/setup.sh` | `az` CLI script for resource group, ADLS Gen2, access connector, Event Hubs, Azure SQL |
| `databricks/unity_catalog.sql` | Catalog, schemas, external location and volumes |
| `databricks/pipelines/*.json` | Sanitized specs of the two Databricks pipelines (ETL + Lakeflow Connect ingestion) |
