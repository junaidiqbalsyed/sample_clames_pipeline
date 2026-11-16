from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_extract

# ==============================================================================
# 1. BRONZE TABLE: RAW TRAINING IMAGES
# ==============================================================================
# Auto Loader (cloudFiles) scans /Volumes/sample_clames/00_landing/training_images
# recursively by default, capturing images nested inside split/class subfolders
# (e.g., .../training_images/test/major/acc9k_00fe2e489fd9.jpg).
@dp.table(
    name="sample_clames.01_bronze.raw_training_images",
    comment="Raw image binaries ingested recursively from landing subfolders",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def raw_training_images() -> DataFrame:
    return (
        spark.readStream
        .format("cloudFiles")
        # Read images as raw binary objects (schema: path, modificationTime, length, content)
        .option("cloudFiles.format", "binaryFile")
        # Exclude labels.csv and any non-image metadata files from the binary stream
        .option("pathGlobFilter", "*.{jpg,jpeg,png,JPG,JPEG,PNG}")
        # Process 100 images per micro-batch (change to 1 only for single-file debugging)
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load("/Volumes/sample_clames/00_landing/training_images")
        # Extract the dataset split ('train', 'val', 'test', 'test_street') from the path
        .withColumn("split", regexp_extract(col("path"), r"/training_images/([^/]+)/", 1))
        # Extract the nested subfolder label (e.g., 'major') if present
        .withColumn("folder_label", regexp_extract(col("path"), r"/training_images/[^/]+/([^/]+)/", 1))
        # Extract the standalone filename (e.g., 'acc9k_00fe2e489fd9.jpg') for Silver joins
        .withColumn("file_name", regexp_extract(col("path"), r"([^/]+)$", 1))
    )


# ==============================================================================
# 2. BRONZE TABLE: RAW TRAINING LABELS
# ==============================================================================
# Ingests labels.csv separately as a structured table so CSV rows don't corrupt
# the binary image table above.
@dp.table(
    name="sample_clames.01_bronze.raw_training_labels",
    comment="Structured ground-truth metadata from labels.csv",
    table_properties={
        "quality": "bronze",
        "layer": "bronze", 
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def raw_training_labels() -> DataFrame:
    return (
        spark.readStream
        .format("cloudFiles")
        # Parse as structured CSV instead of raw binary
        .option("cloudFiles.format", "csv")
        # Use the first row of labels.csv as column names
        .option("header", "true")
        # Automatically detect column data types (strings, integers, etc.)
        .option("cloudFiles.inferColumnTypes", "true")
        # Automatically add new columns if labels.csv is updated with extra fields
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # add new columns / # rescue / # ignore
        # Capture any type mismatches or unparsed columns in a JSON rescue column
        .option("rescuedDataColumn", "_rescued_data")
        # Store Auto Loader's inferred schema inside the landing volume's checkpoint area
        .option("cloudFiles.schemaLocation", "/Volumes/sample_clames/00_landing/training_images/_schemas/labels")
        # Strictly target only labels.csv and ignore all image files
        .option("pathGlobFilter", "labels.csv")
        .load("/Volumes/sample_clames/00_landing/training_images")
    )