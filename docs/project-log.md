# Smart Claims – project log & recreation guide

Living record of everything built in this folder **from day one (2026-11-12)**, written so the whole project can be
rebuilt from scratch. Updated after every change. No credentials are stored here (they live in `.env`, git-ignored).

_Last updated: 2026-11-14_ (repo later restructured: scripts now in `src/smart_clames/{streaming,database,synthetic_data}/`, data in `data/`, pipeline code in `pipelines/`; paths below are the historical ones)

**Contents:** 1 Overview · 2 Who did the work · 3 Recreate from scratch · 4 History by phase · 5 Data inventory ·
6 Script reference · 7 Design details · 7b Azure SQL: original load + synthetic data (full process) · 7c Databricks side · 8 Gotchas · 9 Open to-do · 10 Change log

---

## 1. Overview

Learning project (not production): an end-to-end smart-claims pipeline on Azure + Databricks.

```
 telematics (simulated cars) ──► Azure Event Hubs ──► Databricks (Kafka endpoint) ──► Lakeflow bronze/silver/gold
 Azure SQL: customers/policies/claims ──► Lakeflow Connect (CT + CDC) ──────────────►          │
 claim photos + metadata (Databricks volume) ─────────────────────────────────────────► fraud model + CV damage model
 training photos (Databricks volume) ─────────────────────────────────────────────────► damage classifier (ok/minor/major)
```

Join keys across everything: `chassis_no` (telematics <-> policies.CHASSIS_NO <-> image metadata), `policy_no`
(claims <-> policies), `claim_no` (claims <-> ground truth <-> image metadata <-> replay plan).

Key resource names: Event Hubs namespace `smartclamesdev` (UK South; hub name in `.env.example` is
`eh-smart-clames-dev-uk-south`, the hub Databricks currently reads is topic `telemetry-events` with 5 partitions);
Azure SQL server `jd-sql-learn-2026.database.windows.net`, database `jd-sql-learn-2026` (serverless, auto-pauses);
Databricks workspace `https://adb-7405614338288360.0.azuredatabricks.net` (CLI profile `adb-7405614338288360`),
catalog `sample_clames`, schema `00_landing`, volumes `training_images` (managed), `claims` (managed / internal) and **`customer_claims` (EXTERNAL, ADLS Gen2 container `claims` = the location of record for claim photos + metadata)**.

---

## 2. Who did the work ("agents")

No sub-agents or other agent types were ever spawned. All work was done by **Claude Code** (model: Sonnet-class) in three
sessions in this folder (transcripts: `~/.claude/projects/-Users-junaid-Documents-smart-clames/*.jsonl`; shared memory:
`~/.claude/projects/-Users-junaid-Documents-smart-clames/memory/`):

| Session | When (UTC) | What it did |
|---|---|---|
| `78de645d…` | 2026-11-13 04:48 | Interrupted immediately, no work |
| `295a23c7…` | 2026-11-12 04:44 → 2026-11-14 06:00 | Event Hubs guidance, first producer, producer v2 (realistic), `build_profile.py`, Azure SQL schema + loaders, synthetic data generator, extra fraud scenarios, column renames, Change Tracking/CDC, Databricks Lakeflow utility objects |
| `4bfa1841…` | 2026-11-13 → 2026-11-14 | Producer partition key + replay mode, claim photos, real-photo sourcing, training set, accident images, Databricks volume uploads, this log |

Sessions 2 and 3 overlapped in time. Skills used: `databricks-lakeflow-connect`, `databricks-core`. One Firecrawl web search
(dataset discovery). The user supplied: the parquet telematics data, the SQL CSVs, original claim images + metadata,
`training_imgs` (56 images) and the `Accident_Detection` images.

---

## 3. Recreate from scratch (in order)

**Prereqs:** macOS, `uv` (project is Python >= 3.15, see `.python-version`), Databricks CLI >= 1.0 (`/opt/homebrew/bin/databricks`),
an Azure subscription, a Databricks workspace with Unity Catalog.

1. **Python env:** `uv sync` — then `chflags nohidden .venv/lib/python3.15/site-packages/*.pth` (macOS hides the editable-install
   `.pth`, which breaks `python -m smart_clames…`; or always run scripts by path: `uv run python src/smart_clames/<x>.py`).
   Dependencies: `azure-eventhub azure-identity mssql-python pyarrow python-dotenv pillow`.
2. **Sample data:** put the telematics parquet files in `data/telematics/` (9 files, 780,060 rows) and the original CSVs in
   `data/sql_server/{customers,policies,claims}.csv`; original claim images/metadata went to `data/_archive/`.
3. **Azure:** create Event Hubs namespace (Standard tier or above — Kafka needs it) + a hub; add a shared-access policy with Send+Listen.
   Create an Azure SQL Database (serverless). Copy `.env.example` to `.env` and fill `EVENTHUB_CONNECTION_STRING`, `EVENTHUB_NAME`,
   `AZURE_SQL_HOST/PORT/DATABASE/USER/PASSWORD`.
4. **Telematics profile:** `uv run python src/smart_clames/build_profile.py` → `src/smart_clames/profile.json` (bbox, speed quantiles, VIN prefixes, dup/bad-VIN rates).
5. **SQL schema + originals:** `uv run python src/smart_clames/load_sql_server.py` (creates tables from `sql/01_create_tables.sql`, loads
   7,061 customers / 12,236 policies / 12,991 claims). ⚠ It DROPS the tables — only run on an empty/fresh DB.
6. **Synthetic data:** `uv run python src/smart_clames/generate_synthetic.py` (defaults: 23,000 customers, seed 2026, fraud-rate 0.40,
   dirty-rate 1.0) → `data/sql_server_synthetic/` (CSVs + `claims_ground_truth.csv`, `dirty_manifest.csv`, `dirty_issues.csv`);
   then `uv run python src/smart_clames/load_synthetic.py` (`--reload` to replace earlier synthetic rows).
7. **Change tracking for Lakeflow:** run `sql/02_enable_change_tracking.sql` (CT + CDC on the 3 tables), then `sql/03_lakeflow_utility_objects.sql`
   (Databricks utility script v1.7), then `lakeflowSetupChangeTracking @Tables = NULL, @User = '<sql user>', @CreateDdlSupportingObjects = 1`
   (CT retention set to 14 days explicitly; default is 2) and `lakeflowFixPermissions`. Column-rename gotcha: see §8.
8. **Telematics stream:** `uv run python src/smart_clames/producer.py --vehicles 50 --speedup 30` (replay of claims) or `--live --interval 2`.
   Test first with `--dry-run`.
9. **Claim photos:** (a) `fetch_damage_photos.py --max 300` → review by eye, keep 75 → `_archive/source_photos/` (+ `real_photos.csv`);
   (b) run the producer once (writes `replay_plan.csv`); (c) `generate_claim_images.py --all` → `claims/images/` + `claims/metadata/image_metadata.csv`.
10. **Training set:** download HF `DrBimmer/comprehensive-car-damage` (command in `build_training_set.py` header), then
    `build_training_set.py --hf-dir <path>`; accident images were folded in by a one-off script (see §4, phase 7).
11. **Databricks volumes:** claim photos/metadata belong in the **external** volume `customer_claims` (ADLS Gen2 container `claims`, backed by external location `claims_external_location`); upload straight to it, or to managed `claims` then copy server-side (serverless job, §7c). `databricks fs cp <dir> dbfs:/Volumes/sample_clames/00_landing/<volume>/… --recursive --overwrite --profile adb-7405614338288360`;
    verify counts per folder; remove stray `.DS_Store`.
12. **Databricks read side (not built yet):** Kafka source `smartclamesdev.servicebus.windows.net:9093`, SASL_SSL/PLAIN, username `$ConnectionString`,
    password = connection string, topic = hub name; cast `value` to string, `from_json` with the 5-column schema.

---

## 4. History by phase

### Phase 1 – Event Hubs + first producer (2026-11-12)
- User had parquet telematics (cols `chassis_no, latitude, longitude, event_timestamp, speed`; 2015 timestamps, NYC box, ~1 ping/car/min) and an empty Event Hubs namespace.
- Claude explained the flow (producer → Event Hubs → Databricks Kafka → Lakeflow), first wrote a parquet **replay** producer, then (user wanted fresh "now" data) a **simulator**; the user then asked to write their own producer, creds were put in `.env`, a 9-event test (3 cars × 3 ticks) reached the hub; portal showed UTC timestamps vs IST display.
- Explained "tick" (one loop round: move all cars → 1 event each → send batch → sleep `--interval`).

### Phase 2 – Realistic producer (2026-11-12/14)
- `build_profile.py` distils the 780k rows into `profile.json`; `producer.py` v2: real VIN prefixes + valid check digit, speed from the real inverse CDF, accel/brake limits (3.5/7 mph/s), street grid rotated 29°, turns every 80–270 m, ~4 m GPS noise, fixed cadence, `--dirty` (dup 3.8%, bad VIN 1.7%).
- Calibration vs sample: speed p10/med/p90/p99 7.6/35.5/55.0/70.3 vs 6.3/37.2/56.6/70.7 real. (Sample's speed vs movement is inconsistent; the simulator is physically consistent on purpose.)

### Phase 3 – Azure SQL + synthetic data (2026-11-14)
- Checked linkage: telematics chassis ↔ policies ↔ claims (claims reach chassis via policies; 11,577 policy chassis all in telematics; every claim's car has telematics; 8 telematics cars have no policy).
- `sql/01_create_tables.sql` + `load_sql_server.py` (serverless DB auto-pause → retry built into `connect()`).
- Original claims data judged "out of place" (random noise) → `generate_synthetic.py` created ~100k consistent rows (23,000 customers, 68,512 policies, claims), with an answer key.
- User asked for more false claims → fraud share raised to 40%; scenario mix in §7. Final DB: customers 30,061 / policies 80,748 / claims 26,307 (original + synthetic; synthetic keys: `customer_id >= 100000`, `POLICY_NO` starts with `3`).
- Claims columns renamed in DB and everywhere: `age→driver_age, date→incident_date, hour→incident_hour, type→incident_type, severity→incident_severity` (CSV headers unchanged; `DB_COLUMN` maps).
- Change Tracking + CDC enabled on all 3 tables; Databricks utility objects v1.7 + DDL audit table/trigger (`lakeflowDdlAudit_1_7`) installed; CT retention 14 days; permissions fixed for the SQL user.

### Phase 4 – Producer v3: partition key, real VINs, replay (2026-11-13/14)
- Explained partitions (5), keys; `send_all` now groups by VIN and sends with `partition_key=vin` (Kafka `key` shows the VIN).
- VINs now come from real policy chassis numbers; then **replay mode**: one car per claim, timestamps = claim incident time (NY→UTC), crash/parked/theft/faked-theft behaviour, true severity from ground truth, car steered to a deterministic incident location; `replay_plan.csv` is the answer key. Verified: crash positions median 4 m from target; speed 22.69→0.0 at the incident second.

### Phase 5 – Image metadata decisions (2026-11-14)
- `dbo.image_metadata` was added to Azure SQL then **dropped** (user: images + metadata live in storage, not SQL); CDC leftovers cleaned; scripts removed.

### Phase 6 – Claim photos (2026-11-14)
- Problem: only 15 shared images. Google Images was NOT used (copyright/ToS); instead `fetch_damage_photos.py` pulled 300 CC-licensed photos from Wikimedia Commons; contact sheets reviewed by eye; **75 kept** (22 Low/27 Medium/26 High), 225 junk deleted.
- `generate_claim_images.py --all`: 24,448 unique photos (one per claim that has a photo) with EXIF; fraud mapping in §7. Original claims' photo class re-mapped by severity (the original pairing was random).
- Reorganised: one image folder + one metadata CSV; originals parked in `data/_archive/`.

### Phase 7 – Damage-classifier training set (2026-11-14)
- Original `training_imgs`: 56 PNGs (17 ok / 21 minor / 18 major, 1024², clean close-ups).
- Added HF `DrBimmer/comprehensive-car-damage` (MIT, 2,300 real photos; 15 dupes dropped): **Normal→ok, Crushed→minor, Breakage→major** (first mapping was reversed; user spotted it, verified by eye and fixed).
- Added user's `Accident_Detection` (YOLO, 9,002 imgs): 149 box-less junk + 351 dupes dropped; 8,502 → all `major`, `source=accident9k`; original folder deleted (originals still in `~/Downloads`).
- The 75 Commons photos are held out as `test_street/` (claim photos derive from them → never train on `claims/images`).
- Final: 10,913 images, `labels.csv` verified 1:1 with files.

### Phase 8 – Databricks volumes (2026-11-14)
- Training set uploaded to `/Volumes/sample_clames/00_landing/training_images/` (per-folder counts verified; stray `.DS_Store` removed); local copy deleted (volume is the only copy).
- Claim photos were first uploaded **by mistake** to the managed (internal) volume `/Volumes/sample_clames/00_landing/claims/images/` — the intended target was the EXTERNAL volume `customer_claims` (flat; 24,448 claim photos + the 15 originals from `claims/images 2`). Verified 24,463 = 24,463 by name; local `claims/images` and `claims/images 2` then deleted. Afterwards everything was copied server-side to the **external** volume `customer_claims` (ADLS Gen2), which is the correct location of record; the managed copy in `claims` is the mistaken one and is redundant. `claims/metadata/image_metadata.csv` was also uploaded (by the user) to `…/claims/metadata/` — byte-identical to the local copy.

---

## 5. Data inventory

| Dataset | Where | Size / counts |
|---|---|---|
| Telematics parquet (sample) | `data/telematics/` | 9 files, 780,060 rows, 11,585 chassis |
| Original SQL CSVs | `data/sql_server/` | 7,061 / 12,236 / 12,991 |
| Synthetic SQL CSVs + keys | `data/sql_server_synthetic/` | customers 23,000; policies 68,512; claims 13,316; `claims_ground_truth.csv`; `dirty_manifest.csv` (5,330 injected values); `dirty_issues.csv` (37 issue types) |
| Azure SQL | server above | customers 30,061 / policies 80,748 / claims 26,307 |
| Replay answer key | `data/replay_plan.csv` | one row per streamed car (rewritten each run) |
| Claim photos | **external** volume `/Volumes/sample_clames/00_landing/customer_claims/images/` (ADLS Gen2 container `claims`); the mistaken upload to managed `…/claims/images/` is redundant; local copy deleted | 24,448 + 15 originals |
| Image metadata | local `data/claims/metadata/image_metadata.csv` + external `…/customer_claims/metadata/` (+ the mistaken managed copy in `…/claims/metadata/`) | 24,448 rows |
| Training set | volume `…/training_images/` | train 645/574/6,688 · val 81/72/1,734 · test 80/71/896 · test_street 0/22/53 (ok/minor/major) |
| Archive (generator inputs) | `data/_archive/` | `original_15_images/`, `original_image_metadata.csv`, `synthetic_image_metadata.csv`, `source_photos/` (75 + licences), `original_56_training_imgs/` |

---

## 6. Script reference (`src/smart_clames/`)

| Script | Purpose | Key flags |
|---|---|---|
| `build_profile.py` | parquet → `profile.json` | – |
| `producer.py` | simulator → Event Hubs | `--vehicles --interval --ticks --dry-run --dirty --live --claims-from --lead-minutes --speedup --random-vins --seed` |
| `load_sql_server.py` | create tables + load original CSVs (**drops tables**) | – |
| `generate_synthetic.py` | synthetic customers/policies/claims + answer keys | `--customers --seed --as-of --fraud-rate --dirty-rate` |
| `load_synthetic.py` | append synthetic rows to Azure SQL | `--reload` |
| `fetch_damage_photos.py` | Wikimedia Commons CC photos → `_archive/source_photos/` | `--max` |
| `generate_claim_images.py` | per-claim photos + metadata CSV (overwrites CSV: use `--all`) | `--all --limit --seed` |
| `build_training_set.py` | HF + original 56 + Commons → training splits + `labels.csv` | `--hf-dir` |
| `sql/01…03_*.sql` | schema; CT+CDC; Databricks utility objects v1.7 | – |

---

## 7. Design details

**Synthetic generator.** Consistent NYC customers (valid borough/ZIP), real US makes/models/years, VINs with correct WMI/year code/check digit,
depreciation, premiums driven by age/borough/use/value, annual terms with renewals, claims inside the policy term, amounts scaling with severity
and vehicle value, TP policies never pay own damage. Scenarios (share of all claims → counts): genuine 7,500, inflated_claim 1,630, staged_accident 978,
serial_claimant 872, soon_after_inception 869, theft_fraud 543, duplicate_submission 435, genuine_repeat 266, genuine_hard_negative 133, genuine_two_insured 90
(fraud ≈ 40%; `--fraud-rate 0.10` for a realistic base rate). `suspicious_activity` flag is deliberately noisy in both directions. Garbage injected for
`@dp.expect` practice (37 issue types: negative/zero/absurd premiums, malformed/bad-checksum VINs, make/model mismatch, duplicate policies, future dates,
incident outside term, theft with collision, …) with suggested expectations in `dirty_issues.csv`.

**Replay / telematics.** Event stamped in business time; collision → instant stop (impact speed by true severity: Trivial 6–14, Minor 14–28, Major 28–48,
Total Loss 40–62 mph), 2–10 min frozen; parked → stationary; theft → parked then driven; theft_fraud → no event; incident location = pure function of `claim_no`
(`incident_location`), car steered by dry-run to arrive there ±5 m; duplicates share the original's location; incident local time is NY → UTC.

**Photo ↔ claim mapping (fraud realism).** genuine → photo class from TRUE severity; inflated_claim → photo shows the true (smaller) damage; duplicate_submission → byte-identical
file to the original claim; serial_claimant / staged_accident → one source photo per `group_id` (re-cropped/re-lit per claim); theft_fraud → no photo.
EXIF: phone model, time = 20 min–48 h after incident, GPS = incident location ±12 m. `expected_signal` column documents each fraud tell.

**Training classes.** ok / minor / major. HF Crushed = mild dents → minor; Breakage = broken parts → major. Known biases in §8.

---

## 7b. Azure SQL data: original load + synthetic data – the full process

### A. Original data → Azure SQL (`sql/01_create_tables.sql`, `load_sql_server.py`)
- Tables: `customers(customer_id INT PK, date_of_birth DATE, borough, neighborhood, zip_code CHAR(5), name NVARCHAR(100))`,
  `policies(POLICY_NO VARCHAR(20) PK, CUST_ID FK, POLICYTYPE COMP|TP, dates, MAKE, MODEL, MODEL_YEAR, CHASSIS_NO, USE_OF_VEHICLE, PRODUCT, SUM_INSURED/PREMIUM DECIMAL(12,2), DEDUCTABLE INT)`,
  `claims(claim_no UNIQUEIDENTIFIER PK, policy_no FK, claim_date, months_as_customer, injury/property/vehicle/total, collision_type, number_of_vehicles_involved, driver_age, insured_relationship, license_issue_date, incident_date/hour/type/severity, number_of_witnesses, suspicious_activity BIT)`.
  Column names mirror the CSV headers (incl. the source's `DEDUCTABLE` spelling). `POLICY_NO` is text (suffixes like `/1`, `/RI`); `name` is NVARCHAR (umlauts).
  Constraints: PKs, FKs, CHECKs (`total = injury+property+vehicle`, hour 0–23, expiry >= effective); indexes on `policies.CUST_ID`, `policies.CHASSIS_NO` (telematics join key), `claims.policy_no`.
- Library: Microsoft `mssql-python` (bundles its ODBC driver); `python-tds` was tried and removed (pyOpenSSL incompatibility).
- Three unavoidable fixes to the source data (loader reports them): duplicate policy `102120560` (kept the row with premium 1900) → 12,236 policies; bad date `30/11/0001` → NULL; `58140.001` rounded to 58140.00.
- Dirty values deliberately left in: ~100 negative premiums, claim age 220 (+6 over 100), chassis numbers with spaces/trailing dots/`REFER LIST`/`AS PER LIST ATCHD.`, 2,257 claims with `claim_date` before the incident, licence date `01-01-1900`.
- Verified: counts, NULL counts, sums, min/max equal CSV vs DB; all 12,991 claims join through policies to customers.

### B. Linkage check (why synthetic data was needed)
Telematics ↔ policies: all 11,577 policy chassis exist in telematics (8 telematics cars have no policy; 99.94% of rows belong to an insured car); every claim's car has telematics.
`image_metadata.csv` listed 13,001 claims (12,991 in claims; 10 orphans); chassis matched the policy for 12,976 (all after normalising: remove spaces/dots, upper-case).
The original claims were judged "totally out of place": severity independent of amount, 4 vehicles in every multi-vehicle crash, fraud flag random → not usable for modelling.

### C. Synthetic generator pipeline (`generate_synthetic.py`, seeded; same `--seed` ⇒ same data)
1. **Reference data** (names by ethnicity mix, NYC borough/neighbourhood/ZIP combos via `load_geography`, US makes/models/years/plants/value classes, use-of-vehicle, borough and use claim multipliers).
2. `make_customer` → adults with consistent geography; `make_vehicle` → real WMI + year code + plant + valid check digit (`vin_check_digit`, `make_vin`), unique VINs.
3. `build_policies` → annual terms, renewals on the same car, value depreciation (`vehicle_value`), premium from `driver_age_factor`, borough, use, value; COMP vs TP products and deductibles.
4. `build_claims` (driven by `claim_probability` = base 0.10/yr × age × use × borough × class, TP ×0.45) → claims inside the policy term, reported after the incident, amounts scale with severity and vehicle value; theft / parked / single / multi-vehicle behave differently; TP never pays own damage; scenario injection (see below); `assign_images` links each claim to one of the 15 photos by TRUE severity (Low=dent, Medium=heavy, High=crushed; classes `IMAGE_CLASS`), duplicates reuse the original's image, thefts have none.
5. `validate` → internal consistency asserts; `summarize`.
6. `flatten` → CSV rows; `inject_dirt` → exactly ONE issue per affected row (4.4% of rows, 37 issue types, only garbage that the SQL CHECK/FK constraints still accept), logged per value in `dirty_manifest.csv` (key, issue, column, original, injected) and per type in `dirty_issues.csv` (suggested `@dp.expect` rule + count). `--dirty-rate 0` = clean, 2 = double garbage.
7. `write_csvs` + `write_labels` → `claims_ground_truth.csv` (`claim_no,is_fraud,scenario,group_id,linked_claim_no,claimed_severity,true_severity,suspicious_activity_flag`) and `image_metadata.csv`.
Bug found and fixed: ZIP for the "wrong borough" garbage came from a Python `set` (order varies by hash seed) → made deterministic; output verified identical across three hash seeds.

### D. Scenario design (final, fraud 40% of synthetic claims)
Genuine baseline + genuine repeat claimants (unlucky drivers, 2–3 claims), genuine hard negatives (new customer, real claim that looks suspicious), genuine two-insured collisions (two of our customers hit each other, linked but legitimate); frauds: inflated claim (says Major/Total Loss, photo shows a dent), soon-after-inception (big claim within weeks, slow reporting), staged-accident ring (3–5 customers, same night, injuries, 2–3 witnesses), theft fraud (night, no witnesses, late report), serial claimant (4–7 small parked-car claims just over the deductible), duplicate submission (same incident again days later).
Version 1 had ~10% fraud (shares in `FRAUD_SHARE` ≈ 9.8%); user asked for more false claims → `--fraud-rate` default raised to **0.40** (13,316 claims, 5,327 fraudulent). Flag `suspicious_activity` is imperfect on purpose (`FLAG_RATE` per scenario; ≈96% precision / 81% recall at 40% fraud). Look-alike genuine cases stay thin (133 hard negatives, 90 two-insured) – scale them up if the model finds it too easy.

### E. Loading into Azure SQL (`load_synthetic.py`)
Appends synthetic rows without touching originals (keys: `customer_id >= 100000`, `POLICY_NO LIKE '3%'`); `--reload` deletes only those rows first (claims → policies → customers) then re-inserts. Same type conversions as the original loader (`DB_COLUMN` maps CSV `age/date/hour/type/severity` → DB `driver_age/incident_date/incident_hour/incident_type/incident_severity`).
First load (fraud ≈10%): synthetic customers 23,000 / policies 68,512 / claims 8,861; 22 of 24 garbage checks matched the DB exactly (2 mismatches were check-query artefacts). Reload after the fraud increase: claims 13,316 (4,526 flagged), DB total claims 26,307; the answer-key CSVs were regenerated to match the DB (field-by-field garbage check was not re-run after that reload).
Final DB: customers 30,061 · policies 80,748 · claims 26,307.

### F. After loading
Column rename on `dbo.claims` (CDC was already on, so: disable CDC → drop `ck_claims_hour` → `sp_rename` ×5 → recreate constraint on `incident_hour` → re-enable CDC; first attempt failed halfway on the constraint, the script was written to recover); schema file, loaders and answer keys updated to the new names; loader tested by inserting and rolling back one row. Then CT+CDC script (`sql/02_…`, adapted from the user's template: `USE claims_dev`/`demo.*`/`rds_cdc_enable_db` replaced by `[jd-sql-learn-2026]`, `dbo.*`, `sys.sp_cdc_enable_db`) and the Lakeflow utility objects v1.7 + DDL audit objects. Re-running `load_sql_server.py` afterwards would drop the tables and these settings.

## 7c. Databricks side – what exists and how it was done

**Workspace / auth.** `https://adb-7405614338288360.0.azuredatabricks.net`, CLI v1.26.0 (`/opt/homebrew/bin/databricks`), profile `adb-7405614338288360` (valid).
A second profile `dbw_dataengineering_sandbox_uksouth` (host `adb-7405606941963930…`, the default) has an **expired login** — never auto-pick a profile, always pass `--profile`.
Always address Unity Catalog names literally (`00_landing` starts with a digit: quote it in SQL with backticks).

**Unity Catalog layout (medallion).** Catalog `sample_clames` → schemas `00_landing`, `01_bronze`, `02_silver`, `03_gold` (+ `default`, `information_schema`).

**Volumes in `sample_clames.00_landing`:**
| Volume | Purpose | Contents now |
|---|---|---|
| `training_images` (MANAGED = internal) | damage-classifier data | `train/ val/ test/ test_street/` × `{ok,minor,major}` + `labels.csv` (10,913 images) |
| `claims` (**MANAGED = internal**; storage `abfss://metastore@certificationprep001.dfs.core.windows.net/<metastore-id>/volumes/<volume-id>`) | claim photos + metadata were uploaded here **by mistake** (meant for the external volume); redundant now that `customer_claims` has everything | `images/` 24,463 files (24,448 `<claim_no>.jpg` + 15 original `N_Low/Medium/High.jpg`), `metadata/image_metadata.csv` (9,929,277 bytes, identical to the local file), empty `archive/` and `autoloader_metadata/` (Auto Loader checkpoints/schema location) |
| `customer_claims` (**EXTERNAL**; storage `abfss://claims@certificationprep001.dfs.core.windows.net/`, external location `claims_external_location`, credential `claims-external-container`) | **location of record** for claim photos + metadata (your ADLS Gen2 container); full server-side copy of `claims` made 2026-11-14 | same layout as `claims`: `images/` 24,463 + `metadata/image_metadata.csv` + empty `archive/`, `autoloader_metadata/` |
| `__databricks_ingestion_gateway_staging_data-25a71756-…` | auto-created staging for the Lakeflow Connect gateway — **do not delete or rename** |

**Storage / external locations (UC):** `claims_external_location` → `abfss://claims@certificationprep001.dfs.core.windows.net/` (credential `claims-external-container`) backs the external volume `customer_claims`; `certificationprep001-metastore` → `abfss://metastore@certificationprep001.dfs.core.windows.net/` (managed volumes live here); `adbw_databricks_certification` → workspace default storage.
**Internal vs external:** managed volumes (`claims`, `training_images`) store files inside the metastore's storage and Unity Catalog owns their lifecycle (dropping the volume deletes the files). The external volume `customer_claims` points at your own ADLS Gen2 container, so files stay in that container even if the volume is dropped — this is where the data is meant to be maintained.

**Lakeflow Connect objects the user already created in the UI** (not by Claude): UC connections `azure_sql_cdc` (SQLSERVER), `azure_sql_upsert_cdc` (SQLSERVER), `azure_sql_learn_2026` (listed as MYSQL type);
pipelines `azure_sql_cdc_new` (id `25a71756-1187-4321-8ebd-35d0946bb498`, the gateway; state IDLE) and `New Pipeline 2026-11-13 20:46` (id `83bed603-…`, IDLE).
SQL-side prerequisites for these are in §7b-F.

**How data was moved (CLI, read-only checks first):**
```bash
P=adb-7405614338288360
databricks volumes read sample_clames.00_landing.<volume> --profile $P          # confirm it exists / is empty
databricks fs ls dbfs:/Volumes/sample_clames/00_landing/<volume> --profile $P   # list (one name per line)
databricks fs cp <local dir> dbfs:/Volumes/sample_clames/00_landing/<volume>/<dir> --recursive --overwrite --profile $P
databricks fs rm dbfs:/Volumes/.../.DS_Store --profile $P                       # remove stray macOS files
```
Run long uploads in the background with output to a log (≈250 files/min; training 10.9k files ≈15 min, claims 24.5k ≈ 1.5 h). `fs cp --recursive A dbfs:/…/B` copies A's *contents* into B, so
`images 2` and `images` were both copied into one flat `claims/images/` (names never collide: GUIDs vs `N_Class.jpg`).
**Verification used before deleting local data:** per-folder file counts and a name-by-name `comm` of `ls` lists (volume vs local) — both identical (training set after removing 2 `.DS_Store`; claims 24,463 = 24,463, 0 differences).
Local copies of both datasets were then deleted at the user's request ("move"), so the volumes are now the only copies of the training images and claim photos (originals/sources remain in `data/_archive/` and `~/Downloads`).

**Volume-to-volume copy (server-side, no laptop transfer):** a one-off serverless job (`databricks jobs submit`, run 980927569687259) ran a notebook that walks `claims/`, recreates the folders and copies every file with `dbutils.fs.cp` in a 32-thread pool, then counts files per top folder in both volumes.
Direction: managed `claims` (internal) → external `customer_claims` (ADLS Gen2 container `claims`).
Result: 24,464 files copied, `{images: 24463, metadata: 1}` in both volumes, `match: true`. Notebook left at `/Workspace/Users/<user>/.ai_dev_kit/copy_volume` (safe to delete); source `claims` untouched.
**Compute check after the copy (all off):** cluster `dlt-execution-…` TERMINATED, `Serverless Starter Warehouse` STOPPED, both pipelines IDLE, no active job runs, no apps, no vector-search endpoints (the READY serving endpoints are Databricks-hosted foundation models, no idle compute of ours).

**Not built yet on the Databricks side:** Kafka/Event Hubs bronze stream, bronze/silver/gold tables, Auto Loader over `claims/images` + metadata join, fraud/CV models.
Read pattern for the stream: Kafka bootstrap `smartclamesdev.servicebus.windows.net:9093`, SASL_SSL + PLAIN, username `$ConnectionString`, password = hub connection string, topic = hub name (`telemetry-events`);
`value` is binary → `cast(value as string)` → `from_json` with `chassis_no string, latitude double, longitude double, event_timestamp string(UTC), speed double`; `key` = VIN (partition key), dedupe on (chassis_no, event_timestamp).

---

## 8. Gotchas

- **Do not re-run `load_sql_server.py`** on the live DB (drops tables, wipes CT/CDC, trips the DDL trigger). Use `load_synthetic.py --reload`.
- Renaming a column on a CDC table: disable CDC on it → drop CHECK constraints on the column (e.g. `ck_claims_hour`) → `sp_rename` → re-enable CDC.
- Serverless Azure SQL auto-pauses: first connection may time out (retry exists).
- macOS hides the venv `.pth` → run scripts by path or `chflags nohidden`.
- macOS `sed -i` needs `''`; stray `.DS_Store` files get uploaded with `fs cp` — delete them.
- `generate_claim_images.py` overwrites `image_metadata.csv` with only the rows of that run → always use `--all`.
- Training set is ~90% `major` (accident scenes): filter/downsample by `source` before training. HF labels are noisy; many `ok` photos carry a used-car-site watermark (shortcut risk).
- Accident images came from Google Images (copyright with owners): private study only. Commons photos keep their CC licences in `real_photos.csv`/`image_metadata.csv`.
- Claim photos come from 75 sources only; original `claims.csv` severity/fraud is mostly noise (use synthetic for modelling).
- The SQL password was pasted in chat earlier: rotate it. Event Hub name in `.env.example` differs from the hub Databricks reads (`telemetry-events`).

---

## 9. Open to-do

- [x] Claim-photo upload verified (24,463) and local photo folders deleted; metadata CSV already in the volume.
- [ ] Point downstream reads (Auto Loader over images + metadata join) at the **external** volume `customer_claims`; clear the mistaken managed copy in `claims` (images + metadata) once confirmed.
- [ ] Databricks: UC connection, gateway, Lakeflow Connect ingestion pipeline for the 3 SQL tables.
- [ ] Event Hubs → Databricks streaming (bronze/silver: parse JSON, cast UTC timestamp, dedupe, bad-VIN expectations).
- [ ] Baseline damage classifier (balance classes first), evaluate on `test_street`.
- [ ] Fraud model on claims + telematics + photo signals (`claims_ground_truth.csv`, `expected_signal`).

---

## 10. Change log

| Date | Change |
|---|---|
| 2026-11-12 | Event Hubs guidance; parquet replay producer → simulator; user's creds in `.env`; 9-event test send |
| 2026-11-12/14 | `build_profile.py` + realistic producer v2; Azure SQL schema + loader; originals loaded |
| 2026-11-14 | Synthetic generator + loader; fraud share 40%; column renames; CT + CDC; Lakeflow utility objects v1.7, DDL audit, retention 14 d |
| 2026-11-13/14 | Producer: partition key = VIN, real policy VINs, replay mode with time/location-aligned events |
| 2026-11-14 | `dbo.image_metadata` added then dropped (storage decision) |
| 2026-11-14 | Real Commons photos (300 → 75 kept); 24,448 per-claim photos with fraud mapping; one image folder + one metadata CSV |
| 2026-11-14 | Training set: HF + original 56 + accident9k, mapping fixed (Crushed↔Breakage), splits + `labels.csv` |
| 2026-11-14 | Training set uploaded to Databricks volume, local deleted |
| 2026-11-14 | Claim photos uploaded to `/Volumes/sample_clames/00_landing/claims/images/` (24,463, verified by name) — by mistake, meant for the external volume; local `claims/images` + `images 2` deleted |
| 2026-11-14 | This log created, rewritten into full recreation guide with history from session start and agent/session record |
| 2026-11-14 | Log extended: full synthetic-data/Azure SQL process (§7b) and Databricks workspace/volume/CLI details (§7c) |
| 2026-11-14 | Copied volume `claims` → `customer_claims` server-side (serverless job, 24,464 files, counts match); verified all compute off |
| 2026-11-14 | Log corrected: claim photos' location of record is the EXTERNAL volume `customer_claims` (ADLS Gen2 container `claims`); `claims` and `training_images` are managed/internal volumes |
| 2026-11-14 | Clarified: the managed `claims` upload was a mistake; the external volume `customer_claims` is the intended and only needed location |
| 2026-11-15 | `labels.csv` was deleted from `training_images` by mistake; recovered via Azure blob soft-delete (7-day retention on `certificationprep001`) with `az storage blob undelete`; verified 10,913 rows. Pipeline `raw_training_labels` was failing with CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE because the file was missing. Note: `val/` also has stray `major 2`/`minor 2`/`ok 2` folders to check. |
