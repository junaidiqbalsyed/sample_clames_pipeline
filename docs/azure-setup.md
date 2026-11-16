# Azure setup

Region **Central India**, resource group `rg-databricks-certification`. A re-runnable reference script is in [`infra/azure/setup.sh`](../infra/azure/setup.sh).

| Resource | Name | Config | Purpose |
|---|---|---|---|
| Databricks workspace | `adbw-databricks-certification` | Premium, Unity Catalog | Pipelines, volumes, ML |
| Storage account | `certificationprep001` | StorageV2, **HNS on (ADLS Gen2)**, LRS; containers `metastore`, `claims` | UC metastore root; external volume with claim photos |
| Access connector | `ac-databricks-certification` | System-assigned managed identity, *Storage Blob Data Contributor* on the storage, *Azure Event Hubs Data Receiver* on the namespace | Keyless access from Unity Catalog (storage credential + service credential) |
| Event Hubs namespace | `sensor-stream` | **Standard** (Kafka endpoint needs Standard+), hub `telemetry-events`, **5 partitions** | Telematics stream |
| Azure SQL server / DB | `jd-sql-learn-2026` | General Purpose **serverless** Gen5 (2 vCores), auto-pause 60 min | Source of `customers`, `policies`, `claims` |

Firewall on the SQL server: *Allow Azure services* (0.0.0.0) so the Databricks gateway can connect, plus your own client IP for the Python loaders.

## Gotchas

- **Serverless auto-pause:** the first connection after idle can time out; the loaders retry 5 times. The Databricks gateway also needs the DB awake.
- **Kafka endpoint** is `<namespace>.servicebus.windows.net:9093`; Databricks authenticates with a UC *service credential* (scope `https://<namespace>.servicebus.windows.net/.default`), not a connection string.
- **Blob soft-delete (7 days)** on the storage account saved the project once: `labels.csv` was deleted by mistake and restored with `az storage blob undelete`.
- Rotate any password that was ever pasted into chat or a terminal; keep secrets only in `.env` (git-ignored) or a secret store.

## Local environment

```bash
cp .env.example .env     # fill in Event Hubs + Azure SQL values (never commit .env)
uv sync
```

Run scripts by path (`uv run python src/smart_clames/<script>.py`); on macOS the editable-install `.pth` can be hidden (`chflags nohidden .venv/lib/python*/site-packages/*.pth`).
