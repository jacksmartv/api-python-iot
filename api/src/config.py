import uuid

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str = "postgresql+asyncpg://app_user:app_password@localhost:5432/app_db"

    # Buffer settings for near real-time
    buffer_max_size: int = 100  # Flush when there are N messages
    buffer_max_seconds: float = 2.0  # Flush every N seconds

    # API settings
    api_prefix: str = "/api/v1"

    # Authentication - stored as comma-separated string, converted to set
    api_keys: str = "dev-key-change-me"

    # JWT settings for webapp auth
    jwt_secret: str = "change-me-in-production-use-a-long-random-string"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60 * 24  # 24 hours

    # CORS
    cors_origins: str = "http://localhost:5173,http://localhost:3001"

    # DB URL with DDL permissions for migrations (uses database_url if empty)
    migration_database_url: str = ""

    # Retention settings
    telemetry_retention_days: int = 90   # retention days in telemetry.measurement
    raw_retention_days: int = 14         # retention days in raw.telemetry_payload
    monitoring_retention_days: int = 90  # retention days in monitoring.*

    # Alertmanager URL for alert proxying
    alertmanager_url: str = "http://alertmanager:9093"

    # Spatial Management (buildings/floors/assets module)
    # V1 single-tenant: org_id hardcoded. V2 replaces it with the JWT claim.
    default_org_id: str = "00000000-0000-0000-0000-000000000001"
    # Storage for floorplans (Sprint 2)
    storage_provider: str = "local"       # local | s3 | gcs | azure
    storage_local_dir: str = "/app/data/floorplans"
    storage_url_prefix: str = "/static/floorplans"
    storage_bucket: str = ""
    storage_region: str = ""
    storage_access_key: str = ""
    storage_secret_key: str = ""
    # Guards for the SVG Ingestion Pipeline (Sprint 2)
    plan_max_nodes: int = 8000            # nodes in the normalized SVG
    plan_max_upload_mb: int = 20          # size of the raw uploaded file
    plan_max_svg_kb: int = 4096           # size of the already-optimized SVG

    # Firmware OTA (dedicated S3 bucket, separate from floorplan storage). Credentials: standard
    # boto3 chain (IAM role of the EC2 instance profile, or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
    # from the environment locally).
    firmware_storage_provider: str = "local"   # "s3" in dev/prod via env var
    firmware_bucket: str = ""
    firmware_region: str = "us-east-1"
    firmware_s3_prefix: str = "gateway"
    firmware_max_upload_mb: int = 8            # real .bin is ~1.2-3MB, margin without being too lax
    plan_max_raster_pixels: int = 50_000_000  # anti decompression bomb (PNG/JPG)
    plan_processing_timeout_s: int = 600  # unstick hung uploads
    # Telemetry over the floorplan (Sprint 3)
    spatial_snapshot_refresh_s: int = 60       # how often the snapshot job recalculates
    spatial_online_threshold_min: int = 180    # online/offline threshold based on last_ts age

    # Recovery of uplink frames by BATCH of seqs (gap_recovery V3). The job requests the missing
    # gw_seq from the gateway with read seqs:[...] (batch, one read at a time → no read_busy).
    # Per-seq state in gw_seq_recovery.
    gap_recovery_enabled: bool = True
    gap_recovery_interval_s: int = 300         # how often it runs
    gap_recovery_window_hours: int = 6         # window of gaps + "recent traffic"
    # returns inflight → pending if no response arrives. 600s (not 60s): the firmware scans
    # the entire gwlog.txt looking for each seq, and on a large SD card that scan can take
    # several minutes — a short timeout would discard the real response when it arrives late.
    gap_recovery_inflight_timeout_s: int = 600
    gap_recovery_max_batch_wifi: int = 20      # seq per command on WiFi gateways (firmware limit)
    # The firmware spec documents "LTE: max 5 seq" as the limit of the storage read command, but
    # that's not enough in practice: the RESPONSE (not the command) is also subject to the MQTT
    # payload limit on LTE (480B, same limit as gw_get). With the real size of an HM v3.6 `raw`
    # frame (~68 hex chars plus the line's JSON overhead), 5 seqs produce a response of ~814B and
    # the firmware rejects it. In practice 2 seqs fit with margin (~403B), 3 already exceeds 480B.
    gap_recovery_max_batch_lte: int = 2        # seq/command on LTE (response payload limit)

    # MQTT settings
    mqtt_enabled: bool = False  # Habilitar consumo MQTT
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: str = ""
    mqtt_password: str = ""
    mqtt_client_id: str = "sensorhub-api"
    # telemetry/#: legacy nodes/gateway. gateway/+/{rx,telemetry,status}: Gateway LoRa v1.0.0.
    # rx=HM v3.6 frames, telemetry=GW health, status=presence (online on connect).
    # storage/data=response to the storage read command (recovery of missing gw_seq).
    # Explicit subscriptions (not gateway/#): cmd/ack is left out (we don't use it).
    mqtt_topics: str = (
        "telemetry/#,gateway/+/rx,gateway/+/telemetry,gateway/+/status,"
        "gateway/+/storage/data,gateway/+/cmd/ack,gateway/+/events"
    )  # Comma-separated topics
    mqtt_topic_serial_pattern: str = r"telemetry/([^/]+)"  # Captures the first topic segment

    @property
    def api_keys_set(self) -> set[str]:
        """Returns API keys as a set."""
        return set(key.strip() for key in self.api_keys.split(",") if key.strip())

    @property
    def cors_origins_list(self) -> list[str]:
        """Returns CORS origins as a list."""
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @property
    def mqtt_topics_list(self) -> list[str]:
        """Returns MQTT topics as a list."""
        return [topic.strip() for topic in self.mqtt_topics.split(",") if topic.strip()]

    @property
    def default_org_uuid(self) -> uuid.UUID:
        """DEFAULT_ORG_ID parsed as UUID. Fails loudly if invalid
        (same principle as jwt_secret: the service must not start with a bad config)."""
        return uuid.UUID(self.default_org_id)


settings = Settings()
