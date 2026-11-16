# data/

Small sample datasets that are committed to git. Large binaries (photos, training images) live in Databricks volumes / ADLS and are git-ignored.

| Path | What it is |
|---|---|
| `telematics/` | Original telematics parquet sample (780k rows). Input of `src/smart_clames/streaming/build_profile.py` |
| `sql_server/` | Original `customers`, `policies`, `claims` CSVs (loaded first into Azure SQL) |
| `sql_server_synthetic/` | Generated CSVs plus answer keys: `claims_ground_truth.csv` (fraud label + scenario), `dirty_manifest.csv` / `dirty_issues.csv` (every injected garbage value) |
| `claims/metadata/image_metadata.csv` | One row per claim photo (EXIF info, expected fraud signal) |
| `replay_plan.csv` | Answer key written by the producer in replay mode |
| `_archive/` | Local only, git-ignored: generator inputs and third-party photos |
