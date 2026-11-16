# 0003. Read Event Hubs with a Unity Catalog service credential

**Status:** accepted · **Date:** 2026-11-15

**Context.** The Kafka endpoint can be read with a connection string (a secret) or with Microsoft Entra identity.
**Decision.** The landing table uses the service credential `eventhub_sensor_stream_cred`, backed by the Azure access connector's managed identity (role *Azure Event Hubs Data Receiver*). The simulator, which only sends, uses a shared access policy with Send permission from `.env`.
**Consequences.** No secret in pipeline code or configuration; access is revoked by removing the role assignment.
