from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

# Fetch connection parameters securely from Pipeline Settings -> Configuration
EVENT_HUB_NAMESPACE = spark.conf.get("event_hub_namespace")
EVENT_HUB_NAME = spark.conf.get("event_hub_name")
SERVICE_CREDENTIAL_NAME = spark.conf.get("service_credential_name")

BOOTSTRAP_SERVERS = f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093"

# Azure Event Hubs (Kafka Endpoint) stream options
eventhub_config = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "subscribe": EVENT_HUB_NAME,
    "databricks.serviceCredential": SERVICE_CREDENTIAL_NAME,
    "databricks.serviceCredential.scope": f"https://{EVENT_HUB_NAMESPACE}.servicebus.windows.net/.default",
    "startingOffsets": "earliest",               # Reads full available retention on initial load; checkpoints track progress after
    "failOnDataLoss": "false",                   # Prevents pipeline failure if Event Hubs retention expires before consumption
    "maxOffsetsPerTrigger": "100000",            # Caps records per micro-batch to prevent memory spikes on backfills
    "minPartitions": "1",                        # Parallelizes Kafka partition reads across Spark tasks
    "kafka.request.timeout.ms": "60000",         # Tolerates transient Azure Event Hubs network latency
    "kafka.session.timeout.ms": "30000"
}

@dp.table(
    name="sample_clames.00_landing.raw_telemetry_events",
    comment="Unparsed raw Kafka/Event Hubs envelope from telemetry-events",
    table_properties={
        "quality": "landing",
        "layer": "landing",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.reset.allowed": "false"       # Protects raw landing archive from accidental Full Refresh wipes
    }
)
def telemetry_events_raw() -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .options(**eventhub_config)
        .load()
        .withColumn("landing_timestamp", current_timestamp())
    )