# sample_clames_pipeline (Databricks ETL)

Lakeflow Spark Declarative Pipeline source, one folder per medallion layer under `transformations/`:

| Folder | Role |
|---|---|
| `00_landing/` | Kafka (Event Hubs) → raw table |
| `01_bronze/` | JSON parsing, Auto Loader for training images and labels |
| `02_silver/` | cleaning and data-quality expectations |
| `03_gold/` | joins, aggregates, geocoding |

Pipeline settings and deployment commands: [`docs/databricks-setup.md`](../../docs/databricks-setup.md). Pipeline spec: `infra/databricks/pipelines/sample_clames_pipeline.json`.
