# src/

All source code.

- `sample_clames_pipeline/` - the Databricks ETL pipeline (see its README).
- `smart_clames/` - Python tooling, run by path with `uv run python <path>`:

| Script | Purpose |
|---|---|
| `streaming/build_profile.py` | Distil telematics parquet into `profile.json` |
| `streaming/producer.py` | Simulate cars and publish telemetry to Event Hubs (`--dry-run` prints instead; it rewrites `data/replay_plan.csv`) |
| `database/load_sql_server.py` | Create tables and load the original CSVs (drops tables) |
| `database/load_synthetic.py` | Add synthetic rows (`--reload` replaces only synthetic rows) |
| `synthetic_data/generate_synthetic.py` | Customers/policies/claims, fraud scenarios, garbage values, answer keys |
| `synthetic_data/generate_claim_images.py` | One EXIF-tagged photo per claim + metadata CSV |
| `synthetic_data/fetch_damage_photos.py` | Download CC-licensed Wikimedia Commons photos |
| `synthetic_data/build_training_set.py` | Damage-classifier training set + `labels.csv` |
