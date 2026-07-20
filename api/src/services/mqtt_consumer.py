"""
MQTT consumption service for telemetry ingestion.

Connects to an MQTT broker (typically on the gateway) and consumes
telemetry messages from devices, passing them to the shared
ingestion service.

Expected topic structure (gatewayv3 firmware):
- telemetry/{serial}/data              — gateway heartbeat (JSON)
- telemetry/{IMEI}/HM/{MAC}/data       — relayed BLE sensor (JSON with hex "d" field)
- telemetry/{IMEI}/GW/setConfig        — config written to the gateway (416-byte binary)
- telemetry/{IMEI}/GW/activeConfig     — active config in firmware (416-byte binary)

Gateway LoRa v1.0.0 (uplinks, routed by _handle_gateway_topic):
- gateway/{IMEI}/rx                     — HM v3.6 frame from a node (hex in "raw")
- gateway/{IMEI}/telemetry              — gateway health (every 300 s)
- gateway/{IMEI}/status                 — presence (online on connect; {"gw","state","freq"})
- gateway/{IMEI}/storage/data           — response to the storage read command (gw_seq recovery)
  (cmd/ack is NOT subscribed: we don't use it)

The backend also PUBLISHES outgoing commands on gateway/{IMEI}/cmd (publish_command), for now only
`storage read` to recover lost frames.

The serial_number (IMEI or name) is always extracted from the first segment of the topic.
The subscription uses `telemetry/#` + `gateway/+/{rx,telemetry,status,storage/data}`
(see MQTT_TOPICS).
"""

import asyncio
import json
import logging
import re
from contextlib import suppress

import aiomqtt
from sqlalchemy import text

from ..config import settings
from ..database import async_session
from ..metrics import GATEWAY_RX_UNLINKED_DEVICE, MQTT_CONNECTED, PAYLOADS_FAILED
from .ingestion import ingestion_service
from .payload_parser import (
    decode_gw_config,
    is_gateway_payload,
    is_hm_payload,
    parse_gateway_payload,
    parse_gateway_rx,
    parse_gateway_status,
    parse_gateway_telemetry,
    parse_hm_payload,
)

logger = logging.getLogger(__name__)


class MQTTConsumer:
    """
    MQTT consumer that receives telemetry and passes it to the ingestion service.

    Automatically reconnects on disconnection.
    """

    def __init__(self):
        self._running = False
        self._task: asyncio.Task | None = None
        self._reconnect_interval = 5.0  # seconds between retries
        # Reference to the active client to publish outgoing commands (storage read).
        # None while (re)connecting; publish_command checks it and retries later.
        self._client: aiomqtt.Client | None = None
        # Dispatch of gateway/{id}/<suffix> topics -> handler (bound methods). The match is by
        # topic suffix; the more specific ones (/cmd/ack, /storage/data) don't collide with /cmd or
        # /storage because we compare the full suffix. Scalable without an if/elif tree.
        self._SUFFIX_HANDLERS = {
            "/rx": self._handle_rx,
            "/telemetry": self._handle_telemetry,
            "/status": self._handle_status,
            "/storage/data": self._handle_storage_data,
            "/cmd/ack": self._handle_cmd_ack,
            "/events": self._handle_gw_event,
        }

    async def start(self):
        """Starts the MQTT consumer."""
        if not settings.mqtt_enabled:
            logger.info("MQTT consumer disabled (MQTT_ENABLED=false)")
            return

        self._running = True
        self._task = asyncio.create_task(self._run_forever())
        logger.info(
            f"MQTT consumer started - connecting to {settings.mqtt_host}:{settings.mqtt_port}"
        )

    async def stop(self):
        """Stops the MQTT consumer."""
        self._running = False
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        logger.info("MQTT consumer stopped")

    async def publish_command(self, gw_serial: str, command: dict) -> bool:
        """Publishes an outgoing command to the gateway on gateway/{serial}/cmd (qos 1).

        Returns True if it was published, False if the client is unavailable (reconnecting) or it
        failed — the caller (recovery job) retries on the next run. qos=1: it matters that the
        command arrives (unlike the gateway's uplink, which is qos 0).
        """
        client = self._client
        if client is None:
            logger.warning(
                f"publish_command skipped: MQTT client not connected (gw={gw_serial})"
            )
            return False
        try:
            await client.publish(
                f"gateway/{gw_serial}/cmd", json.dumps(command), qos=1, retain=False
            )
            return True
        except aiomqtt.MqttError as e:
            logger.warning(f"publish_command failed for gw={gw_serial}: {e}")
            return False

    async def _run_forever(self):
        """Main loop with automatic reconnection."""
        while self._running:
            try:
                await self._connect_and_consume()
            except aiomqtt.MqttError as e:
                if self._running:
                    MQTT_CONNECTED.set(0)
                    logger.error(
                        f"MQTT connection error: {e}. "
                        f"Reconnecting in {self._reconnect_interval}s..."
                    )
                    await asyncio.sleep(self._reconnect_interval)
            except Exception as e:
                if self._running:
                    MQTT_CONNECTED.set(0)
                    logger.exception(f"Unexpected error in MQTT consumer: {e}")
                    await asyncio.sleep(self._reconnect_interval)

    async def _connect_and_consume(self):
        """Connects to the broker and consumes messages.

        clean_session=False (persistent session, MQTTv3.1.1): without this, every reconnection
        discards the session on the broker and with it any QoS>=1 message published while we were
        disconnected (e.g. the gateway/{id}/storage/data response to a gap_recovery `storage read`,
        published with qos=1). With clean_session=False + the SAME client_id on every
        reconnection, the broker retains subscriptions + queued QoS>=1 messages and redelivers them
        on reconnect. Requires a stable MQTT_CLIENT_ID (not dynamically generated) and the broker
        to have persistence enabled if it also needs to survive a restart of the broker itself.
        """
        async with aiomqtt.Client(
            hostname=settings.mqtt_host,
            port=settings.mqtt_port,
            username=settings.mqtt_username or None,
            password=settings.mqtt_password or None,
            identifier=settings.mqtt_client_id,
            clean_session=False,
        ) as client:
            MQTT_CONNECTED.set(1)
            # available for publish_command for the duration of the connection
            self._client = client
            try:
                for topic in settings.mqtt_topics_list:
                    await client.subscribe(topic)
                    logger.info(f"Subscribed to MQTT topic: {topic}")

                async for message in client.messages:
                    if not self._running:
                        break
                    await self._handle_message(message)
            finally:
                self._client = None  # on exit/reconnect, publish_command must retry
        MQTT_CONNECTED.set(0)

    async def _handle_message(self, message: aiomqtt.Message):
        """Processes a received MQTT message."""
        topic = str(message.topic)
        # Ensure we work with plain bytes regardless of aiomqtt version
        raw = message.payload
        if isinstance(raw, (bytes, bytearray, memoryview)):
            payload_bytes = bytes(raw)
        else:
            payload_bytes = raw.encode()

        # Gateway LoRa v1.0.0 — topics gateway/{id}/{rx|telemetry}. Route BEFORE
        # _extract_serial_from_topic (which uses the telemetry/ pattern and would return None here).
        if topic.startswith("gateway/"):
            await self._handle_gateway_topic(topic, payload_bytes)
            return

        serial_number = self._extract_serial_from_topic(topic)
        if not serial_number:
            logger.warning(f"Could not extract serial number from topic: {topic}")
            return

        # GW config topics carry raw binary — must route BEFORE attempting json.loads()
        if "/GW/" in topic:
            await self._handle_gw_config(topic, serial_number, payload_bytes)
            return

        try:
            payload_str = payload_bytes.decode("utf-8")
            payload = json.loads(payload_str)

            logger.debug(
                f"Received MQTT message from {serial_number}: {len(payload_str)} bytes"
            )

            # HM sensor relay: topic contains /HM/{MAC}/data
            # Extract device MAC from topic (segment index 3: telemetry/{gw}/HM/{mac}/data)
            if "/HM/" in topic:
                parts = topic.split("/")
                device_mac = parts[3] if len(parts) > 3 else None
                parsed = parse_hm_payload(
                    payload,
                    gateway_serial=serial_number,
                    device_mac_from_topic=device_mac,
                )
                await ingestion_service.ingest_parsed(parsed)
                return

            # Gateway heartbeat
            if is_gateway_payload(payload):
                parsed_gw = parse_gateway_payload(payload, serial_number)
                await ingestion_service.ingest_gateway(parsed_gw)
                return

            # Legacy HM detection (topic doesn't have /HM/ but payload has "d" hex field)
            if is_hm_payload(payload):
                parsed = parse_hm_payload(payload, gateway_serial=serial_number)
                await ingestion_service.ingest_parsed(parsed)
                return

            # Generic sensor payload
            await ingestion_service.ingest(payload, serial_number=serial_number)

        except json.JSONDecodeError as e:
            logger.error(
                f"Invalid JSON from {serial_number} on topic {topic}: {e}",
                extra={"device_serial": serial_number, "error_type": "JSONDecodeError"},
            )
            PAYLOADS_FAILED.labels(
                device_serial=serial_number,
                error_type="JSONDecodeError"
            ).inc()
        except Exception as e:
            logger.error(
                f"Error processing message from {serial_number}: {e}",
                extra={"device_serial": serial_number, "error_type": type(e).__name__},
            )
            PAYLOADS_FAILED.labels(
                device_serial=serial_number,
                error_type=type(e).__name__
            ).inc()

    async def _handle_gw_config(self, topic: str, serial_number: str, raw: bytes):
        """Processes binary gateway configuration messages."""
        config_type = "activeConfig" if "activeConfig" in topic else "setConfig"
        config = decode_gw_config(raw)
        logger.info(
            "Gateway config received",
            extra={
                "serial": serial_number,
                "config_type": config_type,
                "broker": config.get("broker"),
                "fw_type": config.get("fw_type"),
                "interval_s": config.get("interval_s"),
                "raw_len": len(raw),
            },
        )
        try:
            await ingestion_service.ingest_gateway_config(
                serial_number, config_type, config, raw
            )
        except Exception as e:
            logger.error(
                f"Failed to persist gateway config for {serial_number}: {e}",
                extra={"device_serial": serial_number, "error_type": type(e).__name__},
            )

    async def _handle_gateway_topic(self, topic: str, raw_bytes: bytes):
        """Gateway LoRa v1.0.0 — gateway/{gw_id}/rx (HM v3.6 frames) and /telemetry (health)."""
        parts = topic.split("/")
        gw_id = parts[1] if len(parts) > 1 else ""
        if not gw_id:
            logger.warning(f"Could not extract gw_id from gateway topic: {topic}")
            return

        try:
            wrapper = json.loads(raw_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(
                f"Invalid JSON on gateway topic {topic}: {e}",
                extra={"device_serial": gw_id, "error_type": "JSONDecodeError"},
            )
            PAYLOADS_FAILED.labels(device_serial=gw_id, error_type="JSONDecodeError").inc()
            return

        # Dispatch by topic suffix (see _SUFFIX_HANDLERS). The match is by full suffix, so
        # /cmd/ack doesn't collide with /cmd nor /storage/data with /storage.
        handler = None
        for suffix, fn in self._SUFFIX_HANDLERS.items():
            if topic.endswith(suffix):
                handler = fn
                break
        if handler is None:
            return  # gateway/* topic not handled (e.g. the very /cmd we publish ourselves)

        try:
            await handler(gw_id, wrapper)
        except Exception as e:
            logger.error(
                f"Error processing gateway topic {topic}: {e}",
                extra={"device_serial": gw_id, "error_type": type(e).__name__},
            )
            PAYLOADS_FAILED.labels(device_serial=gw_id, error_type=type(e).__name__).inc()

    async def _handle_rx(self, gw_id: str, wrapper: dict):
        """gateway/{id}/rx — LoRa frame from an HM v3.6 node."""
        res = parse_gateway_rx(wrapper, gw_serial=gw_id)
        if not res.ok or res.payload is None:
            logger.warning(
                f"HM v3.6 frame rejected ({res.reason}) from gw {gw_id}",
                extra={"device_serial": gw_id, "error_type": f"HMv36_{res.reason}"},
            )
            PAYLOADS_FAILED.labels(device_serial=gw_id, error_type=f"HMv36_{res.reason}").inc()
            return
        # unknown msg_type: doesn't discard, just visibility (spec §3.5: 0x01/0x05 known)
        decoded = res.payload.raw_payload.get("decoded", {})
        if decoded.get("msg_type") not in (1, 5):
            logger.warning(f"Unknown HM msg_type {decoded.get('msg_type')} from gw {gw_id}")
            PAYLOADS_FAILED.labels(device_serial=gw_id, error_type="HMv36UnknownMsgType").inc()
        device_id = await ingestion_service.ingest_parsed(res.payload)
        await self._track_unlinked_device(device_id, gw_id)

    async def _handle_telemetry(self, gw_id: str, wrapper: dict):
        """gateway/{id}/telemetry — gateway health."""
        gw_status = parse_gateway_telemetry(wrapper, gw_serial=gw_id)
        await ingestion_service.ingest_gateway(gw_status)

    async def _handle_status(self, gw_id: str, wrapper: dict):
        """gateway/{id}/status — presence (uplink on connect). state/freq -> raw_payload."""
        gw_status = parse_gateway_status(wrapper, gw_serial=gw_id)
        await ingestion_service.ingest_gateway(gw_status)

    async def _handle_cmd_ack(self, gw_id: str, wrapper: dict):
        """gateway/{id}/cmd/ack — response to commands (target gw/node/lora).

        We process the `gw_get` config and the OTA deploy result (`ota`). Other acks
        (node_list, lora_downlink, errors) are just logged for now (a future phase will
        process them).
        """
        # local import: firmware_deployment doesn't import this module, same pattern as gap_recovery
        from .firmware_deployment import handle_ota_ack

        action = wrapper.get("action")
        if action == "gw_get":
            await ingestion_service.ingest_gateway_config_v3(gw_id, wrapper)
        elif action == "gw_ota":
            # OTA START ack (the firmware confirmed it received the command) — it's not the
            # final result, doesn't correlate the deployment yet. Same criterion as
            # ota_start from the /events feed (see handle_ota_event): informational, closes nothing.
            logger.info(
                "gw_ota ack received (start, not the final result)",
                extra={"device_serial": gw_id, "raw_wrapper": wrapper},
            )
        elif action in ("ota", "gw_ota_done"):
            # gw_ota_done = final RESULT ack (assumed based on observed firmware traffic; the
            # exact shape of the payload — whether it carries "id"/"ok"/"error" same as "ota" —
            # isn't confirmed, handle_ota_ack already logs and safely discards if it doesn't match).
            await handle_ota_ack(gw_id, wrapper)
        else:
            logger.info(
                f"unhandled cmd/ack (action={action}) from gw {gw_id}",
                extra={"device_serial": gw_id},
            )
            # log the full wrapper for unrecognized acks — without this, we'd lose the
            # exact shape of the real payload the first time a new action appears (the firmware
            # has been observed using gw_ota/gw_ota_done, not "ota" as originally assumed).
            logger.debug(
                f"unrecognized raw cmd/ack payload from gw {gw_id}",
                extra={"device_serial": gw_id, "raw_wrapper": wrapper},
            )

    async def _handle_gw_event(self, gw_id: str, wrapper: dict):
        """gateway/{id}/events — gateway events (boot, sd_*, net_change, power_*, ota_*).

        ota_start/ota_ok/ota_fail are, besides being a domain event (persisted like
        any other via ingest_gateway_event), a second source of confirmation for the firmware
        deploy — see handle_ota_event.
        """
        # local import: firmware_deployment doesn't import this module, same pattern as gap_recovery
        from .firmware_deployment import handle_ota_event

        await ingestion_service.ingest_gateway_event(gw_id, wrapper)
        if wrapper.get("type") in ("ota_start", "ota_ok", "ota_fail"):
            await handle_ota_event(gw_id, wrapper)

    async def _handle_storage_data(self, gw_id: str, wrapper: dict):
        """Response to the storage read command (gateway/{id}/storage/data).

        Delegates to gap_recovery.handle_storage_response, which applies the state transitions in
        monitoring.gw_seq_recovery, ingests the recovered frame (synchronously) and emits the
        domain events.
        """
        # local import: gap_recovery imports this module (publish_command) -> avoids an import cycle
        from .gap_recovery import handle_storage_response

        await handle_storage_response(gw_id, wrapper)

    async def _track_unlinked_device(self, device_id, gw_id: str):
        """Metric: device ingested via /rx but with no linked spatial asset (unmapped sensor)."""
        try:
            async with async_session() as session:
                result = await session.execute(
                    text(
                        "SELECT 1 FROM spatial.asset "
                        "WHERE device_id = :dev AND deleted_at IS NULL LIMIT 1"
                    ),
                    {"dev": str(device_id)},
                )
                if result.first() is None:
                    GATEWAY_RX_UNLINKED_DEVICE.labels(gw=gw_id).inc()
        except Exception as e:  # noqa: BLE001 — the metric must not break ingestion
            logger.debug(f"unlinked-device check failed for {gw_id}: {e}")

    def _extract_serial_from_topic(self, topic: str) -> str | None:
        """
        Extracts the serial number from the MQTT topic (first segment after the prefix).

        Examples:
        - telemetry/arbo1/data                         -> arbo1
        - telemetry/869951036943482/HM/28:39:.../data  -> 869951036943482
        - telemetry/869951036943482/GW/setConfig       -> 869951036943482

        Configurable via MQTT_TOPIC_SERIAL_PATTERN.
        """
        pattern = settings.mqtt_topic_serial_pattern
        match = re.search(pattern, topic)
        if match:
            return match.group(1)
        return None


# Consumer singleton
mqtt_consumer = MQTTConsumer()
