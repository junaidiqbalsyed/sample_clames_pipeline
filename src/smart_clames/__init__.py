"""smart_clames: tooling around the sample_clames_pipeline lakehouse project.

Subpackages (each script is run by path, e.g. ``uv run python src/smart_clames/streaming/producer.py``):

* ``streaming``      - telematics simulator that publishes events to Azure Event Hubs.
* ``database``       - loaders that create and fill the Azure SQL source tables.
* ``synthetic_data`` - generators for synthetic SQL data, claim photos and the damage-classifier training set.

The Databricks ETL code itself lives in ``pipelines/sample_clames_pipeline`` (not in this package).
"""
