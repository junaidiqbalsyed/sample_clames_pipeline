# pipelines/

Databricks Lakeflow Spark Declarative Pipeline code.

`sample_clames_pipeline/transformations/` is the ETL pipeline, one folder per medallion layer:
`00_landing` (Kafka raw) → `01_bronze` (parse / Auto Loader) → `02_silver` (clean + expectations) → `03_gold` (joins, aggregates, geocoding).
Pipeline settings and deployment commands: `docs/databricks-setup.md`.
