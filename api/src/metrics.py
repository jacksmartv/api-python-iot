"""
Prometheus metrics for the telemetry API.
"""

from prometheus_client import Counter, Gauge, Histogram

# Payload counters
PAYLOADS_RECEIVED = Counter(
    "telemetry_payloads_received_total",
    "Total payloads received",
    ["device_serial"],
)

PAYLOADS_PROCESSED = Counter(
    "telemetry_payloads_processed_total",
    "Total payloads processed successfully",
    ["device_serial"],
)

PAYLOADS_FAILED = Counter(
    "telemetry_payloads_failed_total",
    "Total failed payloads",
    ["device_serial", "error_type"],
)

# Buffer metrics
BUFFER_SIZE = Gauge(
    "telemetry_buffer_size",
    "Current size of the ingest buffer",
)

BUFFER_FLUSHES = Counter(
    "telemetry_buffer_flushes_total",
    "Total buffer flushes",
    ["trigger"],  # "size" or "time"
)

# Processing times
PROCESSING_TIME = Histogram(
    "telemetry_processing_seconds",
    "Payload processing time",
    ["operation"],  # "parse", "db_insert", "total"
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# Sensor metrics
MEASUREMENTS_INSERTED = Counter(
    "telemetry_measurements_inserted_total",
    "Total measurements inserted",
    ["device_serial", "sensor_index"],
)

# Database metrics
DB_CONNECTIONS_ACTIVE = Gauge(
    "telemetry_db_connections_active",
    "Active database connections",
)

DB_QUERY_TIME = Histogram(
    "telemetry_db_query_seconds",
    "Database query time",
    ["query_type"],  # "insert", "select", "upsert"
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# MQTT consumer state
MQTT_CONNECTED = Gauge(
    "mqtt_consumer_connected",
    "1 if the MQTT consumer is connected to the broker, 0 if disconnected",
)

# Buffer overflow
BUFFER_OVERFLOW_DROPS = Counter(
    "telemetry_buffer_overflow_drops_total",
    "Payloads dropped due to buffer overflow during a DB outage",
)

# LoRa Gateway v1.0.0 — device ingested via gateway/+/rx but with no linked spatial asset.
# Early warning for sensors that are transmitting but don't show up on the map (unexpected UID).
GATEWAY_RX_UNLINKED_DEVICE = Counter(
    "gateway_rx_unlinked_device_total",
    "HM v3.6 frames ingested from a device with no linked spatial asset",
    ["gw"],
)

# Flush retries (does not increment PAYLOADS_FAILED — the payload is not lost)
FLUSH_RETRY_COUNT = Counter(
    "telemetry_flush_retry_total",
    "Flush retries due to DB errors",
)

# Outgoing MQTT commands published to gateway/{serial}/cmd (via CommandService)
COMMANDS_PUBLISHED = Counter(
    "gateway_commands_published_total",
    "Outgoing MQTT commands published to the gateway",
    ["target", "action", "result"],  # result: ok | failed
)

# Final result of an OTA firmware deploy. Incremented in
# firmware_deployment._apply_ota_result, covers both confirmation sources (cmd/ack and /events)
# without double counting — only the transition that actually closes the deployment.
FIRMWARE_OTA_RESULT = Counter(
    "firmware_ota_deploy_result_total",
    "Final result of an OTA firmware deploy to a gateway",
    ["gw", "result"],  # result: success | failed
)
FIRMWARE_OTA_DURATION = Histogram(
    "firmware_ota_duration_seconds",
    "Time between sending the OTA command (command_sent_at) and the final result (acked_at)",
    buckets=[5, 15, 30, 60, 120, 300, 600],
)

# Flush duration
FLUSH_DURATION = Histogram(
    "telemetry_flush_duration_seconds",
    "Duration of each buffer flush",
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
)

# --- Uplink frame recovery by seq batch (gap_recovery V3) ---
GAP_RECOVERY_RECOVERED = Counter(
    "gap_recovery_recovered_total",
    "Frames INSERTED (actually recovered) by the recovery batch",
    ["gw"],
)
GAP_RECOVERY_READ_BUSY = Counter(
    "gap_recovery_read_busy_total",
    "read_busy responses from the gateway (health: SD card busy)",
    ["gw"],
)
GAP_RECOVERY_LAST_RUN = Gauge(
    "gap_recovery_last_run_timestamp_seconds",
    "Epoch of the last job run (heartbeat)",
)
GAP_RECOVERY_BATCHES_REQUESTED = Counter(
    "gap_recovery_batches_requested_total",
    "Batches (read seqs[...]) successfully published to the gateway",
    ["gw"],
)
GAP_RECOVERY_NOT_FOUND = Counter(
    "gap_recovery_not_found_total",
    "Seqs reported missing by the gateway (permanent loss, not present on the SD card)",
    ["gw"],
)
GAP_RECOVERY_PENDING = Gauge(
    "gap_recovery_pending",
    "Seqs in pending state (gaps detected, not yet recovered)",
    ["gw"],
)
GAP_RECOVERY_INFLIGHT = Gauge(
    "gap_recovery_inflight",
    "Seqs in inflight state (included in a requested batch, awaiting response)",
    ["gw"],
)

# Retention service
RETENTION_LAST_SUCCESS = Gauge(
    "telemetry_retention_last_success_timestamp_seconds",
    "Unix timestamp of the last successful retention cleanup run",
)

RETENTION_CLEANUP_ERRORS = Counter(
    "telemetry_retention_cleanup_errors_total",
    "Failures in the periodic retention cleanup task",
)

# Spatial telemetry snapshot job (Sprint 3)
SPATIAL_SNAPSHOT_LAST_SUCCESS = Gauge(
    "spatial_snapshot_last_success_timestamp_seconds",
    "Unix timestamp of the last successful spatial snapshot refresh run",
)

SPATIAL_SNAPSHOT_LAST_STARTED = Gauge(
    "spatial_snapshot_last_started_timestamp_seconds",
    "Unix timestamp of the last started spatial snapshot refresh run",
)

SPATIAL_SNAPSHOT_DURATION = Histogram(
    "spatial_snapshot_duration_seconds",
    "Duration of the spatial snapshot refresh",
    # explicit buckets: the job normally runs sub-second; the useful signal for
    # degradation is in the 1-30s range (Prometheus defaults are tuned for HTTP latency).
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)

SPATIAL_SNAPSHOT_ROWS = Gauge(
    "spatial_snapshot_rows",
    "Rows materialized in the last spatial snapshot refresh run",
)

SPATIAL_SNAPSHOT_REFRESH_ERRORS = Counter(
    "spatial_snapshot_refresh_errors_total",
    "Failures in the periodic spatial snapshot refresh task",
)
