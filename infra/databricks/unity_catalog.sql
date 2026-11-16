-- Unity Catalog objects for the smart-claims project (run in a Databricks SQL editor / notebook).
-- Reflects what exists in the workspace today. Storage credentials, the SERVICE credential and the
-- UC connections are created in Catalog Explorer (they need the Azure access connector resource id),
-- see docs/databricks-setup.md.

-- 1. Catalog + medallion schemas (names start with a digit -> always backtick-quote them)
CREATE CATALOG IF NOT EXISTS sample_clames;
CREATE SCHEMA IF NOT EXISTS sample_clames.`00_landing`;
CREATE SCHEMA IF NOT EXISTS sample_clames.`01_bronze`;
CREATE SCHEMA IF NOT EXISTS sample_clames.`02_silver`;
CREATE SCHEMA IF NOT EXISTS sample_clames.`03_gold`;

-- 2. External location over the ADLS Gen2 'claims' container (storage credential `claims-external-container`
--    is created in the UI from the Azure access connector).
CREATE EXTERNAL LOCATION IF NOT EXISTS claims_external_location
  URL 'abfss://claims@certificationprep001.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `claims-external-container`)
  COMMENT 'Claim photos + metadata';

-- 3. Volumes
-- EXTERNAL: location of record for claim photos (images/) and image_metadata.csv (metadata/)
CREATE EXTERNAL VOLUME IF NOT EXISTS sample_clames.`00_landing`.customer_claims
  LOCATION 'abfss://claims@certificationprep001.dfs.core.windows.net/';
-- MANAGED: damage-classifier training images (train|val|test|test_street / ok|minor|major + labels.csv)
CREATE VOLUME IF NOT EXISTS sample_clames.`00_landing`.training_images;

-- 4. Sanity checks
SHOW SCHEMAS IN sample_clames;
SHOW VOLUMES IN sample_clames.`00_landing`;
LIST '/Volumes/sample_clames/00_landing/customer_claims/images' ;
