# SensorHub Telemetry API

A telemetry ingestion API for IoT devices, backed by PostgreSQL. Built to handle two
ingestion paths (HTTP and MQTT) into a shared buffer, decode multiple binary/JSON
device payload formats, track fleet health, and self-heal gaps in uplink data —
all with async FastAPI + asyncpg.

## Architecture

```
                                    ┌──────────────────────┐
┌─────────────────┐                 │                      │     ┌────────────────┐
│ IoT Devices     │──Unsecured MQTT▶│  MQTT Gateway         │     │                │
│ (Sensors)       │                 │  (Local)             │     │                │
└─────────────────┘                 └──────────┬───────────┘     │                │
                                               │                 │  PostgreSQL    │
                                    MQTT Subscribe               │  (5 schemas)   │
                                               │                 │                │
                                               ▼                 │                │
┌─────────────────┐     ┌──────────────────────┐                 │                │
│ Other Clients   │────▶│  FastAPI + Buffer    │────────────────▶│                │
│ (HTTP API)      │     │  (Near Real-Time)    │                 └────────────────┘
└─────────────────┘     └──────────┬───────────┘
                                   │
                       ┌───────────┴───────────┐
                       ▼                       ▼
            ┌──────────────────┐   ┌──────────────────────┐
            │  Prometheus      │   │  Loki                │
            │  /metrics        │   │  (JSON logs via      │
            │  /internal/      │   │   Promtail)          │
            │  metrics/sensors │   └──────────────────────┘
            └──────────────────┘
```

### Dual Ingestion (HTTP + MQTT)

The API supports two simultaneous ingestion sources that share the same buffer:

1. **HTTP API**: REST endpoint protected by API key.
2. **MQTT Consumer**: subscribes to the broker with automatic reconnection (5s backoff).
   It uses a persistent session (`clean_session=False` plus a stable `MQTT_CLIENT_ID`,
   see `services/mqtt_consumer.py`). Without this, every reconnect discards the broker
   session and, with it, any QoS≥1 message published while the client was disconnected
   (e.g. a `gateway/{id}/storage/data` response to a gap-recovery `storage read`,
   published with `qos=1`). That matters in an environment that restarts frequently
   (local Docker). The broker must also have persistence enabled if it needs to survive
   its own restarts.

### PostgreSQL Schemas

| Schema | Description |
|--------|-------------|
| `core` | `device`, `sensor`, `gateway`, `user`, `calibration`, `comparison_group`, `firmware_release` (OTA firmware releases), `firmware_deployment` / `firmware_deployment_gateway` (deploying a release to a gateway over MQTT) |
| `telemetry` | Physical measurements (time-series): `measurement` |
| `monitoring` | Operational state: `device_status` (history), `device_runtime` (live state: alarm/low battery), `gateway_status`, `gateway_config`, `incident`, `event` (with `severity`), `gw_seq_recovery` (state of lost-uplink-frame recovery) |
| `raw` | Original JSON payload: `telemetry_payload` |
| `spatial` | Spatial management: `building`, `floor`, `layer`, `asset`, `asset_layer`, `asset_position_history`, `asset_telemetry_snapshot` |

### Ingestion Flow

1. The API receives JSON via HTTP or MQTT.
2. The MQTT consumer checks whether the payload is a gateway payload (`TSgw` or `csq`
   present):
   - **Gateway** → `ingest_gateway()`: persists directly into `monitoring.gateway_status`
     (no buffer, idempotent via `ON CONFLICT DO NOTHING`). On first sighting it emits a
     `gateway.registered` event to `monitoring.event`.
   - **Sensor** → `ingest()`: appends to the shared buffer. On first sighting it emits a
     `device.registered` event to `monitoring.event`.
3. The buffer flushes once it reaches 100 payloads (`BUFFER_MAX_SIZE`) or every 2s
   (`BUFFER_MAX_SECONDS`), whichever comes first.
4. Flush: each payload is committed individually, with 3 retries before it's discarded
   as a poison pill.
5. The payload is stored in `raw.telemetry_payload` (with a dedup hash).
6. An autodiscovery parser inspects each `TS{id}` field, matches registered prefixes,
   and generates sub-sensors with canonical `node_id` and `sensor_key`.
7. Measurements are inserted into `telemetry.measurement`
   (`ON CONFLICT DO NOTHING` on `(sensor_id, ts)`).
8. Operational status is recorded in `monitoring.device_status` when RSSI/buffer
   fields are present.

### Automatic Recovery of Lost Uplink Frames (gap recovery)

Most of the API is passive ingestion, but this subsystem is different: the backend
actively **publishes outbound commands**. A self-healing job detects gaps in `gw_seq`
(uplink numbering, gateway → backend — what the gateway sent but never arrived), asks
the gateway to resend exactly those sequence numbers in a single **batch**
(`read seqs:[...]` over MQTT), and re-ingests the returned frames.

- **Job** (`services/gap_recovery.py`, every `GAP_RECOVERY_INTERVAL_S` = 300s): for
  each gateway with recent traffic it (1) **detects** new gaps
  (`gap_detection.py::compute_gw_seq_gaps`, 6h window) and persists them as `pending`
  rows in `monitoring.gw_seq_recovery`, then (2) **requests** a batch of pending rows
  directly from that table (`ORDER BY gw_seq LIMIT N`, batch size depends on
  transport), marks them `inflight`, and publishes the read command. Only one read is
  in flight per gateway at a time, which avoids `read_busy` errors from the firmware.
  Requesting from the persisted table rather than re-deriving from the live detection
  window each run avoids orphaned gaps — an old pending row that's since fallen out of
  the detection window is still retried until resolved.
- **Response** arrives asynchronously and correlates back to the `inflight` batch by
  seq-set; found sequences are re-ingested (dedup-safe) and transition to `recovered`,
  explicit `missing[]` sequences (definitive loss, not inferred) transition to
  `not_found`.
- **Why batched requests, not one sequence at a time**: requesting sequences
  one-by-one makes each read scan the gateway's full on-device log and causes
  concurrent reads to collide (`read_busy`). Batching by explicit sequence number
  keeps the request close to the domain model while staying within the firmware's
  single-command protocol.
- **Inflight timeout** (`GAP_RECOVERY_INFLIGHT_TIMEOUT_S`, 600s) returns a stuck batch
  to `pending` if no response arrives, as an anti-deadlock safeguard — set generously
  because scanning a large on-device log can take minutes.
- **Batch size differs by transport** (`GAP_RECOVERY_MAX_BATCH_WIFI` / `_LTE`):
  transport is detected from explicit gateway config when available, falling back to
  inference from signal fields. The LTE batch size is small and conservative because
  the real constraint is MQTT response payload size, which depends on how many
  sequences are returned and how large each frame is — not just a fixed command-level
  limit.
- **Observability**: lightweight domain events (`gateway.uplink_gap_recovered` /
  `_lost`) in `monitoring.event`, per-sequence state in `monitoring.gw_seq_recovery`
  (exposed via `GET /recovery`), and structured logs.
- **Scope**: only `gw_seq` (uplink, gateway→backend) is recoverable this way. Gaps in
  `node_seq` (node→gateway) are RF loss — those frames were never received by the
  gateway in the first place, so there's nothing to request.

## Project Structure

```
api/
├── pyproject.toml              # Deps (httpx, python-json-logger, python-multipart, scour, pillow)
├── Dockerfile
├── entrypoint.sh               # migrate.py + uvicorn --log-config src/log_config.json
├── migrations/                 # Sequential, numbered SQL migrations, applied automatically on startup
│   ├── 001_init.sql            # Initial DDL (schemas, base tables)
│   ├── 002_users.sql           # core.user
│   ├── 005_gateway_status.sql  # monitoring.gateway_status
│   ├── 007_production_hardening.sql  # node_id/sensor_key, gateway_id FKs, dedup
│   ├── 008_event_table.sql     # monitoring.event (fleet activity log)
│   ├── 010_spatial.sql         # spatial schema: building/floor/layer/asset + history/snapshot
│   ├── 014_device_runtime.sql  # monitoring.device_runtime (live state) + event severity
│   ├── 015_gw_seq_recovery.sql # monitoring.gw_seq_recovery (uplink frame recovery)
│   ├── 017_gw_seq_recovery_v3.sql # inflight state + batch_id (gap recovery batching)
│   ├── 019_gateway_config_v3.sql # monitoring.gateway_config_v3 (JSON gateway config)
│   └── ...                     # 20 migrations total
└── src/
    ├── main.py                 # Lifespan: DB wait → ingestion → MQTT → retention + snapshot + gap_recovery
    ├── config.py                # Settings (incl. alertmanager_url, retention days)
    ├── database.py               # asyncpg engine with pool_timeout=5s
    ├── migrate.py               # Automatic migrations on startup
    ├── log_config.json          # JSON logging config for uvicorn
    ├── metrics.py               # Prometheus counters and gauges
    ├── auth_jwt.py               # JWT + roles
    ├── schemas/                 # Pydantic models (request/response)
    ├── models/
    │   ├── core.py               # Device (with gateway_id FK), Sensor (node_id, sensor_key), Gateway
    │   ├── monitoring.py         # DeviceStatus, DeviceRuntime, FleetEvent, GwSeqRecovery
    │   ├── gateway.py            # GatewayStatus
    │   ├── raw.py                 # TelemetryPayloadRaw
    │   ├── telemetry.py          # Measurement
    │   └── spatial.py            # Building, Floor, Layer, Asset, AssetLayer,
    │                             #   AssetPositionHistory, AssetTelemetrySnapshot
    ├── routes/
    │   ├── telemetry.py          # POST /telemetry/ingest[/batch]
    │   ├── devices.py            # CRUD + measurements + stats
    │   ├── gateways.py           # List + status + health + seq-gaps + recovery
    │   ├── events.py              # GET /events (fleet activity feed)
    │   ├── status.py              # GET /status/alerts (Alertmanager proxy)
    │   ├── auth.py                # Login, register, user CRUD
    │   ├── spatial.py             # CRUD buildings/floors/layers/assets + plan upload
    │   └── internal_metrics.py   # GET /internal/metrics/sensors (SQL → Prometheus)
    └── services/
        ├── ingestion.py           # Buffer, flush, get_or_create_*, event emission
        ├── mqtt_consumer.py       # aiomqtt with reconnection and MQTT_CONNECTED gauge
        ├── payload_parser.py      # Autodiscovery, MAX_SUBSENSORS=16 (fixed)
        ├── retention.py           # Hourly cleanup, batched 10k rows at a time
        ├── telemetry_snapshot.py  # 60s job: refreshes spatial.asset_telemetry_snapshot
        ├── gap_detection.py       # compute_gw_seq_gaps (single source of truth: endpoint + job)
        ├── gap_recovery.py        # 300s job: requests lost uplink frames (batched read seqs:[...])
        ├── command_service.py     # send_command: generic outbound MQTT commands, request_id + metrics
        ├── storage.py             # StorageBackend + LocalStorage (floorplans; S3 stub)
        └── floorplan.py           # SVG ingestion pipeline (validate→sanitize→optimize→guard)
```

## Endpoints

### Telemetry (API Key)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/telemetry/ingest` | Ingest a single payload |
| `POST` | `/api/v1/telemetry/ingest/batch` | Ingest multiple payloads |
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus metrics (in-process) |
| `GET` | `/internal/metrics/sensors` | SQL-backed metrics (last_seen per device/gateway) |

### Web UI Authentication (JWT)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/auth/login` | Log in, returns a JWT |
| `GET` | `/api/v1/auth/me` | Current user |
| `POST` | `/api/v1/auth/register` | Create the first admin user |
| `POST` | `/api/v1/auth/users` | Create a user (Admin) |
| `GET` | `/api/v1/auth/users` | List users (Admin) |
| `PATCH` | `/api/v1/auth/users/{id}` | Update a user (Admin) |
| `DELETE` | `/api/v1/auth/users/{id}` | Delete a user (Admin) |

### Gateways (JWT)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/gateways` | List gateways with latest status, online state, and `lat`/`lng` (GPS from the latest telemetry) |
| `GET` | `/api/v1/gateways/{serial}/status` | Status history (`?hours=24&limit=100`); includes `wifi_rssi`/`temp_c` per row for charting |
| `GET` | `/api/v1/gateways/{serial}/health` | Health + presence snapshot: `wifi_rssi`, `uptime_sec`, `temp_c`, `heap_free`, `fw`, `lora_freq`, `gps_*`, `state`, `freq`, plus `last_telemetry_at`/`last_status_at` for freshness |
| `GET` | `/api/v1/gateways/{serial}/seq-gaps` | Uplink completeness by `gw_seq` (gateway→backend, `?hours=24&max_gap=1000`): completeness %, missing count, gap list. Distinct from the per-device seq-gaps (node→gateway) |
| `GET` | `/api/v1/gateways/{serial}/recovery` | Status of the lost-uplink-frame recovery mechanism (`?status=all\|pending\|recovered\|not_found\|abandoned&hours=168&limit&offset`): `stats` (counts + success %), `total_items`, and `items` per `gw_seq`. Backs the Recovery panel in the UI |
| `GET` | `/api/v1/gateways/{serial}/config` | Latest active gateway config. Prefers the newer JSON config format (`schema_version:"v3"`, 5 sections) when available, falling back to the legacy binary config (`schema_version:"legacy"`). `404` if neither exists |
| `POST` | `/api/v1/gateways/{serial}/cmd` | Publishes an outbound command to the gateway over MQTT. Currently **read-only**: triggers a config read that the gateway answers asynchronously, populating the config row. Responds `202 {request_id}`. Supports an optional `section` parameter so a constrained-transport gateway can be asked for one config section at a time, with the backend merging each partial response into the existing row rather than overwriting it |
| `GET` | `/api/v1/gateways/{serial}/devices` | Linked devices with `sensor_count`, `last_seen`, and `online` |
| `PATCH` | `/api/v1/gateways/{serial}` | Update the gateway's `display_name` (Admin) |
| `GET` | `/api/v1/gateways/{serial}/device-count` | Count of linked devices (Admin) |
| `DELETE` | `/api/v1/gateways/{serial}` | Delete a gateway and its history (Admin) |
| `GET` | `/api/v1/gateways/{serial}/export` | Export a full gateway record as a JSON backup |

#### Gateway cascade delete

`DELETE /gateways/{serial}` follows this order to respect foreign keys:

1. `monitoring.gateway_status` — deleted explicitly (referenced by `serial_number`, no FK cascade)
2. `core.gateway` — cascades automatically to `monitoring.gateway_config` (`ON DELETE CASCADE`)
3. `core.device.gateway_id` — set to `NULL` automatically (`ON DELETE SET NULL`); devices themselves are not deleted

### Devices (JWT)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/devices/stats` | Fleet-wide statistics |
| `GET` | `/api/v1/devices` | List devices |
| `GET` | `/api/v1/devices/{id}` | Device detail (includes `gateway_serial`, `last_seen`, `display_name`, and the node's current OTA-reported config: `measure_cycles`/`send_cycles`/`hum_alarm_threshold`, `null` until the first frame arrives) |
| `POST` | `/api/v1/devices` | Create a device (Admin) |
| `PATCH` | `/api/v1/devices/{id}` | Update `display_name` and/or metadata (Admin) |
| `PATCH` | `/api/v1/devices/{id}/sensors/{sensor_id}` | Update sensor type (Admin) |
| `DELETE` | `/api/v1/devices/{id}` | Delete a device and all its data (Admin) |
| `GET` | `/api/v1/devices/{id}/export` | Export a full device record as a JSON backup |
| `GET` | `/api/v1/devices/{id}/measurements/export` | Export raw measurements as a streaming CSV (`?hours=N`) |
| `GET` | `/api/v1/devices/{id}/measurements` | Device measurements |
| `GET` | `/api/v1/devices/{id}/measurements/grouped` | Measurements grouped by sensor |
| `GET` | `/api/v1/devices/{id}/status` | Operational status history |
| `GET` | `/api/v1/devices/{id}/seq-gaps` | Missing-packet detection by `node_seq` (`?hours=24&max_gap=100`): completeness %, total, missing count, gap list |

#### Device cascade delete

`DELETE /devices/{id}` follows this order to respect foreign keys (no DB-level cascade):

1. `telemetry.measurement` — all measurements for the device's sensors
2. `monitoring.device_status` — operational status history
3. `core.sensor` — the device's sensors
4. `raw.telemetry_payload` — original JSON payloads
5. `core.device` — the device itself

#### CSV measurement export

`GET /devices/{id}/measurements/export` returns a streaming CSV with columns:

```
ts, serial_number, sensor_index, sensor_key, temperature_c, humidity_pct, voltage_cond_v, supply_mv, msg_counter
```

The optional `?hours=N` parameter limits the range (1–8760h); omitting it exports the
full history. The response uses `StreamingResponse`, so large datasets don't need to be
loaded into memory at once.

### Events and Status (JWT)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/events` | Fleet activity feed. Filters: `?hours=24&limit=50&event_type=&severity=&gateway_serial=`. `gateway_serial` includes events from the gateway **and its nodes**; `severity` is one of `info\|warning\|critical` |
| `GET` | `/api/v1/status/alerts` | Active alerts proxied from Alertmanager (returns `[]` if it's offline) |

### Firmware OTA (JWT, admin)

Firmware releases are hosted in object storage, uploaded through the web UI, and
listed from a catalog table (`core.firmware_release` — each version exists exactly
once). A separate `deploy` step pushes a specific release to a specific gateway over
MQTT. Release and deployment are modeled as distinct concepts: a deployment is an
event separate from the catalog entry, since the same version can be deployed to
multiple gateways, each tracked independently. Two tables back this:
`core.firmware_deployment` (the deploy event) and `core.firmware_deployment_gateway`
(per-target-gateway state: `pending → command_sent → success|failed|timeout`).

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/firmware` | Uploads a `.bin` to object storage and registers the row. Multipart `file` + `version` (`X.Y.Z` with optional `-TAG` suffix). Validates extension, size (≤ `FIRMWARE_MAX_UPLOAD_MB`), and version uniqueness (versions are **immutable** — `409` on duplicate). Stored under a fixed key derived from version rather than the original filename, avoiding filename-variant sprawl in the bucket. Responds `201` with `checksum_sha256`, `etag`, `public_url` |
| `GET` | `/api/v1/firmware` | Lists all releases, most recent first |
| `DELETE` | `/api/v1/firmware/{id}` | Deletes the row **and** the underlying object. Storage failures propagate as `502` rather than failing silently. No soft-delete; deleting and re-uploading under the same version frees it up again |
| `POST` | `/api/v1/gateways/{serial}/firmware/deploy` | Body `{"firmware_release_id": UUID}`. Resolves `url`/`version`/`sha256` server-side (never trusts client-supplied values) and publishes an `ota` command over MQTT. Uses two separate transactions rather than one held open across the publish call, so a DB lock isn't held during network I/O. Responds `202`. `404` if the release doesn't exist; `503` if MQTT is unavailable — with no rollback, since the row committed in step one stays `pending` with an `error_detail`, forming a record of the failed attempt |
| `GET` | `/api/v1/gateways/{serial}/firmware/deploy/{deployment_id}` | Polls the status of one deployment by id until it leaves `pending`/`command_sent` |

**Deployment ack**: the gateway's asynchronous ack correlates back to a deployment row
via an echoed request id. A single conditional `UPDATE` applies the `success`/`failed`
transition, guarded to only touch rows still in a non-terminal state — since MQTT QoS
1 can redeliver the same ack more than once, and a terminal row must not be
overwritten by a redelivery.

**Testing without real hardware**: the general gateway simulator
(`scripts/lora_simulator.py`) can subscribe to the command topic and respond to `ota`
commands with a simulated ack after a delay, standing in for download + flash time:

```bash
python scripts/lora_simulator.py --imei GW-TEST-FW-01 --nodes 0                  # ack ok:true
python scripts/lora_simulator.py --imei GW-TEST-FW-01 --nodes 0 --ota-fail       # ack ok:false
```

The simulated gateway registers itself normally, so a deploy triggered from the UI or
`curl` exercises the complete cycle end to end without touching physical hardware.

**Observability**: Prometheus counters/histograms track deploy success, failure, and
duration, only incrementing on a real state transition (not on ignored redeliveries).
An alert fires on a sustained failure rate (3+ in 30 minutes) rather than a single
failure, since an isolated failure is often transient (gateway offline, out of
coverage) and the underlying metric already records every failure regardless.

**Object storage**: firmware binaries live in a dedicated bucket, separate from
floorplan storage, with public read access scoped only to the firmware prefix via
bucket policy (no ACLs, no bucket listing). Objects are uploaded with a long-lived
cache header (versions are immutable, so caching indefinitely is safe). Credentials
are resolved through the standard cloud SDK credential chain rather than hardcoded —
an IAM role scoped to this bucket in a deployed environment, or environment-variable
credentials for a narrowly scoped local-dev identity in Docker.

**Settings** (`config.py`): `firmware_storage_provider` (`local`|`s3`),
`firmware_bucket`, `firmware_region`, `firmware_s3_prefix`, `firmware_max_upload_mb`.

### Spatial Management (JWT)

Visualizes buildings/floors/assets on top of floor plans. **Single-tenant in V1**: all
tables have an `org_id` column, but it's currently hardcoded to a single default
value; multi-tenancy is a planned V2.

| Method | Path | Role | Description |
|--------|------|------|-------------|
| `GET` | `/api/v1/spatial/buildings` | any | List buildings with `floor_count` |
| `POST` | `/api/v1/spatial/buildings` | admin | Create a building |
| `GET` | `/api/v1/spatial/buildings/{id}` | any | Building detail |
| `PATCH` | `/api/v1/spatial/buildings/{id}` | admin | Update a building |
| `DELETE` | `/api/v1/spatial/buildings/{id}` | admin | Soft delete |
| `GET` | `/api/v1/spatial/buildings/{id}/floors` | any | Floors in a building |
| `POST` | `/api/v1/spatial/buildings/{id}/floors` | admin | Create a floor |
| `GET` | `/api/v1/spatial/floors/{id}` | any | Floor detail with `asset_count` |
| `PATCH` | `/api/v1/spatial/floors/{id}` | admin | Update a floor |
| `DELETE` | `/api/v1/spatial/floors/{id}` | admin | Soft delete |
| `GET` | `/api/v1/spatial/floors/{id}/layers` | any | Layers on a floor |
| `POST` | `/api/v1/spatial/floors/{id}/layers` | admin | Create a layer |
| `PATCH` | `/api/v1/spatial/layers/{id}` | admin | Update a layer |
| `DELETE` | `/api/v1/spatial/layers/{id}` | admin | Soft delete |
| `GET` | `/api/v1/spatial/floors/{id}/assets` | any | Assets on a floor |
| `POST` | `/api/v1/spatial/floors/{id}/assets` | user/admin | Create an asset |
| `POST` | `/api/v1/spatial/floors/{id}/assets/bulk` | user/admin | Bulk import (CSV/JSON) |
| `GET` | `/api/v1/spatial/assets/{id}` | any | Asset detail |
| `PATCH` | `/api/v1/spatial/assets/{id}` | user/admin | Update an asset |
| `PATCH` | `/api/v1/spatial/assets/{id}/position` | user/admin | Move an asset (optimistic lock + history) |
| `DELETE` | `/api/v1/spatial/assets/{id}` | user/admin | Soft delete |
| `POST` | `/api/v1/spatial/floors/{id}/plan` | admin | Upload a floor plan (SVG/PNG/JPG) → `202` async |
| `GET` | `/api/v1/spatial/floors/{id}/plan/status` | any | Floor plan processing status |
| `DELETE` | `/api/v1/spatial/floors/{id}/plan` | admin | Delete the plan (idempotent) |
| `GET` | `/api/v1/spatial/floors/{id}/telemetry` | any | Telemetry for a floor's assets (reads the snapshot) |

**Asset ≠ Device**: an `asset` is the spatial entity; `device_id` is a **nullable** FK
to `core.device`. Assets with a device (cameras, sensors, HVAC units, meters) show
telemetry; assets without one (e.g. fire extinguishers, emergency exits) exist on the
plan without telemetry.

**Soft delete and optimistic locking**: `DELETE` performs `UPDATE ... SET deleted_at
= NOW()`, never a physical delete, and every `GET` filters it out through a shared
helper. `PATCH /assets/{id}/position` requires an `expected_version`; a mismatch
against the asset's current version returns `409 Conflict` — a defense against
concurrent writes when drag-and-drop UI edits race with live position updates. Each
move writes a row to `asset_position_history` in the same transaction.

#### SVG ingestion pipeline (floor plans)

Uploading a plan normalizes it into a safe, servable SVG — **the original uploaded
file is never stored**, only the normalized output. Asset positions are stored as
percentages of the plan's viewBox, so they stay correct if the image is
re-rendered at a different resolution.

**Async flow**: `POST /plan` validates and returns `202` with `plan_status='processing'`;
a background task runs the pipeline while the client polls `GET /plan/status` until
`ready`/`failed`.

**Pipeline**: `validate (magic bytes) → to_svg → sanitize → optimize → normalize
(viewBox + flatten transforms) → guard`. Security and robustness measures:

| Defense | What it does |
|---|---|
| **Real signature check** | File type detected by magic bytes, not extension or Content-Type |
| **Upload size limit** | Raw file over the configured max → `413` before the pipeline runs |
| **Decompression bomb guard** | Oversized rasters rejected before loading into memory |
| **XSS sanitization** | Element allowlist; strips `<script>`, `<foreignObject>`, `on*` attributes, `javascript:` hrefs |
| **Anti-SSRF** | External `href`s in `<use>`/`<image>` are stripped (only local refs allowed) |
| **Early guards** | Raw SVG size and node count validated **before** the expensive optimize step (avoids hanging on a pathological `<path>`) |
| **Double guard** | Normalized SVG must satisfy both a node-count and a byte-size ceiling |
| **Bounded flattening** | Only translate/scale transforms are flattened; rotate/matrix/skew fail the upload rather than silently drift asset positions |
| **Path traversal** | Storage resolves keys within its base directory; any escape attempt errors out |
| **Content-Type** | SVGs are served as `image/svg+xml`, meant for `<img>`/texture use, never an `<iframe>` |

**Concurrency**: only one upload per floor at a time, enforced by a conditional atomic
status update rather than an external lock. A job that dies leaves the floor stuck in
`processing` only until a timeout, after which a status check marks it `failed`.
**Versioning**: each successful upload increments a version counter; only the current
version's files are retained. Storage is behind a small pluggable interface (local
disk by default, ready for an object-store backend later).

> **Operational note**: this pipeline requires running `uvicorn` with a **single
> worker**, since it uses in-process background tasks with an in-memory concurrency
> lock. Multiple workers would need a dedicated task queue instead.

#### Telemetry on the floor plan (snapshot)

Each asset linked to a device shows its latest telemetry and online/offline status
without paying for an expensive join on every poll. A periodic job (every 60s)
recomputes a denormalized snapshot table for all assets with a device, using a single
`DISTINCT ON` pass to grab the latest measurement and status per device — the
expensive join lives in this background job, off the request path. The read endpoint
then does a plain primary-key lookup against the snapshot. Status (online/offline/
unknown) is derived from the age of the last reading and recalculated every cycle,
since the passage of time changes it even without new telemetry. The job is
self-healing: an anti-overlap lock skips a cycle if the previous one is still running,
orphaned rows are cleaned up when an asset loses its device link, and dedicated
metrics track success/failure and duration. Same single-worker requirement as the
upload pipeline.

## Running Locally

```bash
# From the project root
docker compose up -d

# With pgAdmin (manual inspection tool; not started in normal deploys)
docker compose --profile tools up -d

# With Grafana (monitoring)
docker compose --profile monitoring up -d
```

### Service URLs

| Service | URL | Credentials |
|----------|-----|--------------|
| **Web UI** | http://localhost:3001 | `admin@example.com` / `admin123` |
| **API** | http://localhost:8000 | Header `X-API-Key` |
| **API Docs** | http://localhost:8000/docs | — |
| **Prometheus** | http://localhost:9090 | — |
| **Alertmanager** | http://localhost:9093 | — |
| **Loki** | http://localhost:3100 | — |
| **Grafana** | http://localhost:3000 | `admin` / `admin` |
| **pgAdmin** | http://localhost:5050 | `admin@local.dev` / `admin` |

## Configuration

### Environment Variables

```bash
# Database
DATABASE_URL=postgresql+asyncpg://app_user:app_password@localhost:5432/app_db
MIGRATION_DATABASE_URL=            # DB URL with DDL permissions for migrations (default: reuses DATABASE_URL)

# Buffer (near real-time)
BUFFER_MAX_SIZE=100           # Flush once N messages accumulate
BUFFER_MAX_SECONDS=2.0        # Flush every N seconds

# API
API_PREFIX=/api/v1
API_KEYS=key1,key2,key3

# JWT (web UI)
JWT_SECRET=change-me-in-production-use-a-long-random-string
JWT_ALGORITHM=HS256
JWT_EXPIRE_MINUTES=1440

# CORS
CORS_ORIGINS=http://localhost:5173,http://localhost:3001

# Data retention
TELEMETRY_RETENTION_DAYS=90   # telemetry.measurement
RAW_RETENTION_DAYS=14         # raw.telemetry_payload
MONITORING_RETENTION_DAYS=90  # monitoring.*

# Alertmanager (for the /status/alerts proxy)
ALERTMANAGER_URL=http://alertmanager:9093

# Spatial Management
DEFAULT_ORG_ID=00000000-0000-0000-0000-000000000001  # single org in V1 (must be a valid UUID)
# Floor plan storage
STORAGE_PROVIDER=local                                # local (V1) | s3 | gcs | azure (V2)
STORAGE_LOCAL_DIR=/app/data/floorplans                # Docker volume (provider=local)
STORAGE_URL_PREFIX=/static/floorplans                 # path where plans are served from
STORAGE_BUCKET=                                       # (provider=s3/gcs)
STORAGE_REGION=
STORAGE_ACCESS_KEY=
STORAGE_SECRET_KEY=
# SVG ingestion pipeline guards
PLAN_MAX_NODES=8000                                   # nodes in the normalized SVG
PLAN_MAX_UPLOAD_MB=20                                 # raw uploaded file size
PLAN_MAX_SVG_KB=4096                                  # size of the optimized SVG
PLAN_MAX_RASTER_PIXELS=50000000                       # anti decompression-bomb (PNG/JPG)
PLAN_PROCESSING_TIMEOUT_S=600                         # unstick a hung upload
# Telemetry-on-plan (snapshot job)
SPATIAL_SNAPSHOT_REFRESH_S=60                         # how often the snapshot recomputes
SPATIAL_ONLINE_THRESHOLD_MIN=180                      # online/offline threshold by data age

# MQTT Consumer
MQTT_ENABLED=false
MQTT_HOST=localhost
MQTT_PORT=1883
MQTT_USERNAME=
MQTT_PASSWORD=
MQTT_CLIENT_ID=sensorhub-api
MQTT_TOPICS=telemetry/#,gateway/+/rx,gateway/+/telemetry,gateway/+/status,gateway/+/storage/data,gateway/+/cmd/ack,gateway/+/events
MQTT_TOPIC_SERIAL_PATTERN=telemetry/([^/]+)

# Gap recovery (automatic recovery of lost uplink frames)
GAP_RECOVERY_ENABLED=true
GAP_RECOVERY_INTERVAL_S=300            # how often the job runs
GAP_RECOVERY_WINDOW_HOURS=6            # detection window + "recent traffic" cutoff (bump for prod)
GAP_RECOVERY_INFLIGHT_TIMEOUT_S=600    # inflight → pending if no response arrives (see note above)
GAP_RECOVERY_MAX_BATCH_WIFI=20         # seqs per storage-read command on WiFi gateways
GAP_RECOVERY_MAX_BATCH_LTE=2           # seqs per storage-read command on LTE gateways (response payload limit, not command limit)
```

## Payload Formats

### Format v1 (Sensors — Recommended)

```json
{
  "sn": "DEVICE-001",
  "TS1": 20260203.140509,
  "ti1": 22.5,
  "ts1": 18.3,
  "vc1": 1.82,
  "h1": 65.0,
  "v1": 3300,
  "o1": 12345,
  "s1": -65,
  "um1": 10,
  "tm1": 50
}
```

The parser performs **autodiscovery** of sensor types. For each `TS{id}` field, it
looks up prefixes from a sensor-key registry. Each matching prefix generates a
sub-sensor with `node_id` (the `{id}` suffix of the timestamp field) and `sensor_key`
(the prefix).

**Registered prefixes:**

| Prefix | DB field | `sensor_type` |
|---------|----------|---------------|
| `ti` | `temperature_c` | `temperature_interior` |
| `ts` | `temperature_c` | `temperature_surface` |
| `t` | `temperature_c` | `temperature` (legacy) |
| `vc` | `voltage_cond_v` | `conductivity` |
| `h` | `humidity_pct` | `humidity` |

`MAX_SUBSENSORS = 16` is a fixed constant, not derived from registry size — adding new
sensor types to the registry doesn't shift the `sensor_index` of existing ones.

### Gateway Format (Heartbeat)

Detected by the presence of `TSgw` or `csq` in the payload. Persisted directly to
`monitoring.gateway_status`, bypassing the buffer.

```json
{
  "TSgw": 20260425.143000,
  "csq": 18,
  "erf": 0,
  "rst": 1,
  "vgw": 12400
}
```

### Binary Relay Format (BLE via gateway firmware)

Some gateway firmware relays raw BLE packets on a 5-segment MQTT topic. The gateway's
identifier is the second topic segment; the BLE device's MAC address is the fourth
segment (and also encoded inside the binary payload).

**Topic**: `telemetry/{gateway-id}/HM/28:39:C9:8D:54:20:01:8A/data`

**Payload**:
```json
{"ts": 1781696452, "rssi": -79, "d": "484D2839C98D5420018A1304910D0E06D101200302000A000F003B9D"}
```

The `d` field is a hex-encoded binary packet, in one of two sizes:

| Size | Hex chars | Description |
|--------|-----------|-------------|
| 22 bytes | 44 chars | Standard packet |
| 28 bytes | 56 chars | Extended packet (6 extra bytes) |

**Binary layout** (little-endian, first 22 bytes common to both variants):

| Offset | Bytes | Type      | Field    | Example                        |
|--------|-------|-----------|----------|---------------------------------------------|
| 0      | 2     | char[2]   | type     | `"HM"` — magic bytes                        |
| 2      | 8     | uint8[8]  | uid      | device MAC → `2839c98d5420018a`             |
| 10     | 2     | uint16 LE | seq      | 1043 (message counter)                      |
| 12     | 2     | uint16 LE | vcc      | 3473 mV                                     |
| 14     | 2     | int16 LE  | temp     | 1550 → 15.50°C (÷100)                       |
| 16     | 2     | uint16 LE | humidity | 465 → 46.5% RH (÷10)                        |
| 18     | 1     | uint8     | pending  | 0 (messages buffered on the device)         |
| 19     | 1     | uint8     | free     | 15 (free slots in the on-device buffer)     |
| 20     | 2     | uint16    | crc      | stored, not validated                       |

**Routing**: the parser uses the MAC address from the topic as the device's serial
number, which is more reliable than extracting it from the binary payload. The device
is registered in `core.device` with `gateway_id` pointing at the relaying gateway,
using last-seen-via semantics (a device can roam between gateways).

### Binary Gateway Config Format

A 416-byte little-endian binary blob, published on a dedicated config topic. The MQTT
consumer detects it by topic shape before attempting to parse anything as JSON.

| Offset | Bytes | Field | Example |
|--------|-------|-------|---------|
| 0 | 40 | `broker` (char[40]) | `"mqtt.example.com"` |
| 40 | 2 | `port` (uint16 LE) | `1883` |
| 42 | 20 | `client_id` (char[20]) | `"gw01"` |
| 62 | 20 | `fw_type` (char[20]) | `"gatewayv3"` |
| 82 | 20 | `topic_prefix` (char[20]) | `"telemetry"` |
| 110 | 2 | `interval_s` (uint16 LE) | `60` |
| 112 | 2 | `supply_mv` (uint16 LE) | `3900` |
| 114 | 64 | `broker2` (char[64]) | `"mqtt-backup.example.com"` |
| 414 | 2 | `crc` (uint16 LE) | `0xFED8` |

Persisted to `monitoring.gateway_config` with all fields decoded, plus the raw bytes
for future re-parsing. Retrievable via `GET /api/v1/gateways/{serial}/config`.

### Legacy Format

```json
{
  "sn": "DEVICE001",
  "schema_v": 1,
  "status": {"rssi": -65, "buf_used": 10, "buf_total": 100, "supply": 3300},
  "sensor_0": {"timestamp": 20250205.143022, "temp": 25.5, "volt_cond": 1.2, "supply": 3300, "msg_cnt": 12345}
}
```

### Numeric Timestamp

`20260203.140509` → `2026-02-03 14:05:09 UTC`

## Prometheus Metrics

### In-Process (`GET /metrics`)

| Metric | Type | Labels | Description |
|---------|------|--------|-------------|
| `telemetry_payloads_received_total` | Counter | `device_serial` | Payloads received |
| `telemetry_payloads_processed_total` | Counter | `device_serial` | Payloads processed |
| `telemetry_payloads_failed_total` | Counter | `device_serial`, `error_type` | Payloads failed or discarded |
| `telemetry_buffer_size` | Gauge | — | Current buffer size |
| `telemetry_buffer_flushes_total` | Counter | `trigger` (size/time) | Buffer flushes |
| `telemetry_buffer_overflow_drops_total` | Counter | — | Payloads dropped due to overflow |
| `telemetry_flush_retry_total` | Counter | — | Flush retries after a DB error |
| `telemetry_flush_duration_seconds` | Histogram | — | Duration of each flush |
| `telemetry_processing_seconds` | Histogram | `operation` | Processing time per operation |
| `telemetry_measurements_inserted_total` | Counter | `device_serial`, `sensor_index` | Measurements inserted |
| `mqtt_consumer_connected` | Gauge | — | 1=connected, 0=reconnecting |
| `telemetry_retention_last_success_timestamp_seconds` | Gauge | — | Timestamp of the last successful cleanup |
| `telemetry_retention_cleanup_errors_total` | Counter | — | Errors during retention cleanup |
| `spatial_snapshot_last_success_timestamp_seconds` | Gauge | — | Last successful spatial snapshot refresh |
| `spatial_snapshot_last_started_timestamp_seconds` | Gauge | — | Last started refresh (detects a hung job vs. last_success) |
| `spatial_snapshot_duration_seconds` | Histogram | — | Refresh duration (0.05–30s buckets; detects degradation) |
| `spatial_snapshot_rows` | Gauge | — | Rows materialized in the last run |
| `spatial_snapshot_refresh_errors_total` | Counter | — | Refresh task failures |

### SQL-Backed (`GET /internal/metrics/sensors`)

25s cache, falling back to a stale cache if the DB is unresponsive.

| Metric | Labels | Description |
|---------|--------|-------------|
| `iot_device_last_seen_seconds` | `serial_number`, `gateway_serial` | Unix timestamp of the last measurement |
| `iot_gateway_last_seen_seconds` | `serial_number` | Unix timestamp of the last heartbeat |

## Structured Logging

All logs are JSON with fields: `timestamp`, `logger`, `level`, `message`.

Error paths include additional fields via `extra={}`:

```json
{"timestamp": "2026-05-24T20:06:20", "logger": "src.services.ingestion",
 "level": "ERROR", "message": "Payload failed",
 "device_serial": "DEV-001", "error_type": "IntegrityError"}
```

`device_serial` is a JSON field in the log, **not** a Loki label (it's high
cardinality). To filter on it in LogQL:
```
{service="api"} | json | device_serial="DEV-001"
```

## Data Retention

A background task runs hourly and deletes rows in batches of 10,000 to avoid lock
pressure:

| Table | Default retention | Variable |
|-------|---------------------|----------|
| `telemetry.measurement` | 90 days | `TELEMETRY_RETENTION_DAYS` |
| `raw.telemetry_payload` | 14 days | `RAW_RETENTION_DAYS` |
| `monitoring.device_status` | 90 days | `MONITORING_RETENTION_DAYS` |
| `monitoring.gateway_status` | 90 days | `MONITORING_RETENTION_DAYS` |

## Fleet Events

`monitoring.event` is an append-only log of discrete fleet occurrences.

| Event | Emitted when |
|--------|----------------|
| `device.registered` | First telemetry from a new device |
| `gateway.registered` | First MQTT heartbeat from a new gateway |
| `sensor.humidity_alarm` / `_cleared` | A node's alarm flag transitions false↔true |
| `sensor.low_battery` / `_cleared` | A node's low-battery flag transitions false↔true |
| `gateway.<type>` (boot/sd_fail/sd_space_low/net_change/power_mains/power_batt) | Gateway lifecycle events reported over MQTT |
| `gateway.uplink_gap_recovered` / `gateway.uplink_gap_lost` | Gap recovery job outcomes |

Events are queried via `GET /api/v1/events` (filterable by `hours`, `limit`,
`event_type`).

## Display Names

Gateways and devices can have a friendly name stored in a `metadata_` JSONB column
under the key `display_name` — no extra column or migration required. A `PATCH`
merges `display_name` into the existing JSONB without overwriting other keys.
Passing an empty string clears the name (converted to `null` server-side, not stored
as `""`).

```bash
curl -X PATCH /api/v1/gateways/{serial} \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"display_name": ""}'
```

`display_name` and `last_seen` are surfaced consistently across the relevant list and
detail endpoints (`GET /gateways`, `GET /gateways/{serial}/devices`, `GET /devices`,
`GET /devices/{id}`, and the corresponding `PATCH` responses).

---

## Migrations

Migrations apply automatically on API startup and are tracked in
`public.schema_migrations`.

```bash
# View applied migrations
docker compose exec postgres psql -U app_user -d app_db \
  -c "SELECT version, applied_at FROM schema_migrations ORDER BY applied_at"

# Run manually
docker compose exec api python -m src.migrate
```

## MQTT Ingestion

```bash
# Enable and configure
MQTT_ENABLED=true
MQTT_HOST=gateway.local
MQTT_TOPICS=telemetry/#
MQTT_TOPIC_SERIAL_PATTERN=telemetry/([^/]+)
```

The consumer subscribes to `telemetry/#` to capture all legacy firmware topic
patterns. The serial number is always extracted from the topic's **first segment**:

```
telemetry/DEV-001/data                          →  serial = DEV-001
telemetry/869951036943482/HM/28:39:.../data     →  serial = 869951036943482  (gateway id)
telemetry/869951036943482/GW/activeConfig       →  serial = 869951036943482
```

Routing is done by topic structure **before** attempting to decode JSON:
1. Topic contains `/GW/` → binary payload → `decode_gw_config()` → `monitoring.gateway_config`
2. Topic contains `/HM/` → JSON with a hex `d` field → BLE sensor → `parse_hm_payload()`, using the MAC from the topic
3. JSON payload with `TSgw` or `csq` → gateway heartbeat → `monitoring.gateway_status`
4. Anything else → generic or legacy sensor payload → buffer → `telemetry.measurement`

#### LoRa gateway topics (`gateway/{id}/...`)

In addition to the legacy `telemetry/#` firmware, the consumer subscribes explicitly
to a newer gateway firmware's uplink topics:

| Topic | What it is | Handler | Destination |
|-------|--------|---------|---------|
| `gateway/+/rx` | A binary sensor frame from a LoRa node (hex-encoded in `raw`) | `parse_gateway_rx()` | `telemetry.measurement` (temperature + humidity sensors) |
| `gateway/+/telemetry` | Gateway health heartbeat (every `telemetry_interval_sec`) | `parse_gateway_telemetry()` | `monitoring.gateway_status` |
| `gateway/+/status` | **Presence**: the gateway just connected | `parse_gateway_status()` | `monitoring.gateway_status` |

- **`status` vs `telemetry`**: `status` means *presence* (backend-observed connect
  time); `telemetry` means *health* (voltage, signal, etc.) — the two are not
  conflated.
- **`gateway/+/cmd/ack`, `gateway/+/events`, and `gateway/+/storage/data`** carry
  responses/events for outbound commands (the control path), not spontaneous uplinks,
  and are routed through a per-suffix dispatch table rather than a chain of
  conditionals. Ack handling persists v3 config sections and supports merging partial
  responses (see the `POST /cmd` endpoint above); `/events` become `gateway.<type>`
  fleet events; `/storage/data` feeds the gap recovery response handler.
- **Idempotency of `/rx` replays**: firmware automatically resends buffered frames on
  reconnect. Uniqueness is enforced at the ingestion layer (`ON CONFLICT` on
  `(sensor_id, ts)`), not in the parser — each frame carries its own original
  timestamp, so a replay naturally dedups against the first delivery. A frame without
  a usable timestamp is discarded rather than inserted with a fabricated "now"
  timestamp.

**Health/GPS fields**: gateway telemetry carries more than voltage — GPS, WiFi RSSI,
uptime, onboard temperature, free heap, firmware version, LoRa frequency. These are
parsed out of the raw JSON and surfaced through dedicated response fields
(`GET /gateways/{serial}/health`, `lat`/`lng` on `GET /gateways`) rather than left
buried in an opaque payload blob.

**Node humidity is raw ADC, not a percentage** — no calibration curve exists yet for
this hardware, so the value is stored as-is under the existing column and sensor
type. Once a calibration curve is available, ingestion can switch to persisting the
converted percentage without changing the sensor contract.

**Node alarm/battery events**: the sensor frame carries alarm and low-battery flags.
Ingestion detects **state transitions**, not one event per frame, and emits
`sensor.humidity_alarm`/`_cleared` and `sensor.low_battery`/`_cleared` accordingly.
Live state (current flags + per-flag changed-at timestamps) lives in
`monitoring.device_runtime`, a single upserted row per device — distinct from
`device_status`, which is a time-series history table. The same table also mirrors
the node's current OTA-configured cycle/threshold settings, which arrive on every
uplink frame; since the node has no dedicated "read config" command, this passive
mirroring is the only way to observe its live configuration.

**Missing-packet detection — two independent sequence counters.** Loss is tracked at
two distinct hops, each with its own counter, storage, and endpoint:

| Sequence | Counts | Hop | Stored in | Endpoint / metric |
|---|---|---|---|---|
| `node_seq` | Packets the **node** transmitted | node → gateway (RF) | `telemetry.measurement.msg_counter` | `GET /devices/{id}/seq-gaps` · `iot_device_seq_completeness_pct` |
| `gw_seq` | Frames the **gateway** relayed | gateway → backend (uplink) | `raw.telemetry_payload.payload->>'seq'` | `GET /gateways/{serial}/seq-gaps` · `iot_gateway_uplink_completeness_pct` |

Rules common to both:
- Order by **sequence value**, not arrival time — devices resend out of order (replay
  from local storage), so ordering by arrival time would produce false gaps.
- Jumps `>= max_gap` are treated as a counter reset/wraparound rather than loss,
  configurable via query parameter.
- The time window filters by **when the frame was originally generated**, not when
  the backend received it — otherwise a gap-recovered frame (old data, freshly
  inserted) would land in a "recent" window and create artificial gaps around it,
  making a healthy gateway look lossy.
- Each feeds its own completeness metric and alert threshold, surfaced as separate
  panels on the device and gateway detail pages.
- **Dedup by `(sensor_id, msg_counter)`** in addition to `(sensor_id, ts)`: a node can
  retransmit an identical reading as an RF-layer retry, which the gateway relays as a
  distinct uplink with its own sequence number and receive time — without this extra
  constraint it would insert as a spurious "new" row. A partial unique index closes
  this gap without replacing the timestamp-based primary key, which gap recovery
  still relies on.

The repo ships several simulators (`scripts/`) so the full stack can be exercised
without physical hardware. All support `--broker`/`--username`/`--password` and run
either from the host or via `docker compose exec api python scripts/....py`.

| Script | Simulates | Useful options |
|---|---|---|
| `mqtt_simulator.py` | Standard v1 sensor payloads + gateway heartbeats, published live over MQTT using the same serials as `seed_data.py` so seeded history and live data correlate in dashboards | `--gateways`, `--devices`, `--nodes`, `--sensor-types`, `--interval`, `--gw-interval` |
| `hm_simulator.py` | The BLE-relay firmware's three topic shapes (heartbeat, relayed BLE sensor frame, binary config) | `--imei`, `--gateways`, `--devices`, `--extended` (28-byte packets), `--with-config` |
| `lora_simulator.py` | The newer JSON-config gateway firmware, including responding to outbound `gw_get`/`ota` commands with realistic acks | `--imei`, `--nodes`, `--ota-delay`, `--ota-fail` |
| `alert_simulator.py` | Traffic crafted to cross each configured Prometheus alert threshold, using dedicated `*-SIM-*` serials that don't collide with other seeded/simulated data | `--scenario list\|all-concurrent\|<name>` |

```bash
python scripts/mqtt_simulator.py --gateways 3 --devices 8 --nodes 2
python scripts/hm_simulator.py --imei 869951036943482 --devices 4 --extended
docker compose exec api python scripts/alert_simulator.py --scenario all-concurrent --broker mqtt
```

`alert_simulator.py --scenario all-concurrent` runs every alert scenario in parallel,
staggered so each fires at a different point in the timeline — useful for
demonstrating the full alerting surface in one run (`buffer-flood` is excluded since
its traffic volume would drown out the others). Watch results at
`http://localhost:9090/alerts` (Prometheus) or `http://localhost:9093` (Alertmanager).

The API's MQTT consumer detects simulator (and real hardware) topics purely by
structure, exactly as described in the ingestion sections above — no simulator-aware
code exists in the API itself.

---

## Sample Data (`scripts/seed_data.py`)

Generates a complete fleet with history for the last N hours, written directly to the
database — useful for populating dashboards before starting a live simulator, or for
testing without waiting for data to accumulate.

```bash
# Defaults: 2 gateways, 4 devices/gateway, 2 nodes/device, 24h of history
python scripts/seed_data.py

# Wipe and reseed with a larger fleet
python scripts/seed_data.py --reset --gateways 3 --devices 8 --nodes 3 --hours 48

# Wipe only, no reseed
python scripts/clean_db.py
```

| Option | Default | Description |
|--------|---------|-------------|
| `--gateways` | `2` | Gateways to create |
| `--devices` | `4` | Devices per gateway |
| `--nodes` | `2` | Sensor nodes per device |
| `--hours` | `24` | Hours of measurement history |
| `--reset` | — | Wipe all data before seeding |

Creates in the database:
- **Admin user**: `admin@example.com` / `admin123`
- Gateways in `core.gateway`, linked via FK from `core.device.gateway_id`
- Sensors with correct `node_id`/`sensor_key` (`ti`, `ts`, `vc`, `h`), compatible with
  the parser
- Historical measurements every 5 minutes in `telemetry.measurement`
- `gateway.registered` and `device.registered` events in `monitoring.event`
- History in `monitoring.gateway_status` and `monitoring.device_status`

## Database Cleanup (`scripts/clean_db.py`)

```bash
# Clear fleet data, keep users
python scripts/clean_db.py

# Also remove non-admin users
python scripts/clean_db.py --users

# Clear absolutely everything, including the admin user
python scripts/clean_db.py --all
```

Respects foreign key order and clears every table, including `monitoring.event` and
`monitoring.incident`.

## Local Development (without Docker for the API)

```bash
cd api
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Start only the DB and MQTT broker
cd .. && docker compose up -d postgres mqtt

cd api && cp .env.example .env
# Set MQTT_ENABLED=true, MQTT_HOST=localhost if you want MQTT locally

uvicorn src.main:app --reload --port 8000
```
