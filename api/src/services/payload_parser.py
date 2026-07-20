"""
Parser for sensor telemetry payloads.

Supports sensor-type autodiscovery based on the keys present in the
payload. Each prefix registered in SENSOR_KEY_REGISTRY generates an
independent sub-sensor with its own sensor_index.

Format v1 (keys with numeric suffix per node):
- TS{id}: Timestamp YYYYMMDD.HHMMSS
- ti{id}: Interior temperature °C
- ts{id}: Surface temperature °C
- t{id}:  Generic temperature °C (legacy)
- vc{id}: Conductivity voltage V
- h{id}:  Direct humidity %
- v{id}:  Supply voltage mV (node level)
- o{id}:  Message counter (node level)
- s{id}:  RSSI dBm (node level)
- um{id}: Buffer used (node level)
- tm{id}: Buffer total (node level)

Gateway format (gateway heartbeat):
- TSgw: Timestamp
- csq:  Cellular signal (0-31)
- erf:  Error flags
- rst:  Reset counter
- vgw:  Gateway voltage mV
"""

import logging
import re
import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal, NamedTuple, TypedDict

logger = logging.getLogger(__name__)

# ── Sensor type registry ────────────────────────────────────────────────────
# key: key prefix in the payload
# value: (field in ParsedSensorData, sensor_type_label)

class SensorKeyDef(NamedTuple):
    field: str        # field in ParsedSensorData that receives the value
    sensor_type: str  # value to store in core.sensor.sensor_type


SENSOR_KEY_REGISTRY: dict[str, SensorKeyDef] = {
    "ti": SensorKeyDef("temperature_c", "temperature_interior"),
    "ts": SensorKeyDef("temperature_c", "temperature_surface"),
    "t":  SensorKeyDef("temperature_c", "temperature"),
    "vc": SensorKeyDef("voltage_cond_v", "conductivity"),
    "h":  SensorKeyDef("humidity_pct", "humidity"),
}

# Node-level keys (shared across all sub-sensors of the same id)
NODE_KEYS = {"v", "o", "s", "um", "tm"}


@dataclass
class ParsedSensorData:
    """Parsed data for an individual sensor."""
    sensor_index: int
    sensor_type: str | None
    timestamp: datetime
    # Canonical sensor identity (migration 007): node_id = TS{id} number, sensor_key = prefix
    node_id: int | None = None
    sensor_key: str | None = None
    temperature_c: float | None = None
    humidity_pct: float | None = None
    voltage_cond_v: float | None = None
    supply_mv: int | None = None
    msg_counter: int | None = None
    rssi_dbm: int | None = None
    buffer_used: int | None = None
    buffer_total: int | None = None


@dataclass
class ParsedPayload:
    """Parsed payload with all sensors."""
    serial_number: str
    schema_version: int
    sensors: list[ParsedSensorData]
    raw_payload: dict
    is_gateway: bool = False
    gateway_serial: str | None = None
    # FRAME-level alarm state (not per sensor). None for non-HM frames that don't carry it.
    # Ingest uses these to detect transitions and emit events (see device_runtime).
    alarm: bool | None = None
    low_batt: bool | None = None


@dataclass
class ParsedGatewayStatus:
    """Parsed status of a gateway."""
    serial_number: str
    timestamp: datetime
    csq: int | None
    erf: int | None
    rst: int | None
    vgw: int | None
    raw_payload: dict


def voltage_to_humidity(voltage_v: float | None) -> float | None:
    """Converts conductivity voltage to humidity percentage."""
    if voltage_v is None or voltage_v <= 0:
        return None

    import math
    A = 25.1
    B = 22.4
    humidity = A * math.log(voltage_v) + B
    return round(max(0.0, min(100.0, humidity)), 1)


def parse_numeric_timestamp(value: float) -> datetime | None:
    """Converts a numeric timestamp to UTC datetime.

    Supports two formats:
    - Unix epoch (seconds): value in [1e9, 2e10) — range 2001-2286
    - Proprietary format YYYYMMDD.HHMMSS — values in ~[2e7, 2.1e7)
    The ranges don't overlap, so detection is unambiguous.
    """
    if value is None:
        return None

    try:
        if 1_000_000_000 <= value < 20_000_000_000:
            return datetime.fromtimestamp(value, tz=timezone.utc)

        int_part = int(value)
        decimal_part = value - int_part

        year = int_part // 10000
        month = (int_part // 100) % 100
        day = int_part % 100

        time_val = round(decimal_part * 1000000)
        hour = time_val // 10000
        minute = (time_val // 100) % 100
        second = time_val % 100

        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def is_gateway_payload(payload: dict) -> bool:
    """Detects whether the payload is a gateway heartbeat."""
    return "TSgw" in payload or "csq" in payload


def parse_gateway_payload(payload: dict, serial_number: str) -> ParsedGatewayStatus:
    """Parses a gateway heartbeat payload."""
    ts_value = payload.get("TSgw")
    timestamp = (
        parse_numeric_timestamp(float(ts_value))
        if isinstance(ts_value, (int, float))
        else datetime.now(timezone.utc)
    )
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    return ParsedGatewayStatus(
        serial_number=serial_number,
        timestamp=timestamp,
        csq=_get_int(payload, "csq"),
        erf=_get_int(payload, "erf"),
        rst=_get_int(payload, "rst"),
        vgw=_get_int(payload, "vgw"),
        raw_payload=payload,
    )


def parse_sensor_payload_v1(payload: dict) -> ParsedPayload:
    """
    Parses a v1 payload with sensor-type autodiscovery.

    For each node (identified by TS{id}), detects which prefixes from
    SENSOR_KEY_REGISTRY are present and generates a sub-sensor for each one.
    sensor_index is computed as: (sensor_id - 1) * MAX_SUBSENSORS + sub_index
    where MAX_SUBSENSORS = len(SENSOR_KEY_REGISTRY) to leave room.
    """
    schema_version = payload.get("schema_version", 1)
    # fixed — never derive from registry size: adding a type shifts all existing sensor_index values
    MAX_SUBSENSORS = 16

    # Detect all node IDs by presence of TS{id}
    sensor_ids: set[int] = set()
    ts_pattern = re.compile(r"^TS(\d+)$")
    for key in payload.keys():
        match = ts_pattern.match(key)
        if match:
            sensor_ids.add(int(match.group(1)))

    sensors: list[ParsedSensorData] = []

    for sensor_id in sorted(sensor_ids):
        ts_value = payload.get(f"TS{sensor_id}")
        if not isinstance(ts_value, (int, float)):
            continue
        timestamp = parse_numeric_timestamp(float(ts_value))
        if timestamp is None:
            continue

        # Node-level values (shared across sub-sensors)
        supply_mv = _get_int(payload, f"v{sensor_id}")
        msg_counter = _get_int(payload, f"o{sensor_id}")
        rssi_dbm = _get_int(payload, f"s{sensor_id}")
        buffer_used = _get_int(payload, f"um{sensor_id}")
        buffer_total = _get_int(payload, f"tm{sensor_id}")

        # Autodiscovery: detect which registry prefixes are present in the payload
        # Sort by longest prefix first to prevent "t" from consuming "ti" or "ts"
        sorted_prefixes = sorted(SENSOR_KEY_REGISTRY.keys(), key=len, reverse=True)
        found_prefixes: list[str] = []
        for prefix in sorted_prefixes:
            key = f"{prefix}{sensor_id}"
            if key in payload:
                found_prefixes.append(prefix)

        if not found_prefixes:
            continue

        for sub_index, prefix in enumerate(found_prefixes):
            key_def = SENSOR_KEY_REGISTRY[prefix]
            raw_value = payload.get(f"{prefix}{sensor_id}")

            sensor_data = ParsedSensorData(
                sensor_index=(sensor_id - 1) * MAX_SUBSENSORS + sub_index,
                sensor_type=key_def.sensor_type,
                timestamp=timestamp,
                node_id=sensor_id,
                sensor_key=prefix,
                supply_mv=supply_mv,
                msg_counter=msg_counter,
                rssi_dbm=rssi_dbm,
                buffer_used=buffer_used,
                buffer_total=buffer_total,
            )

            # Assign the value to the corresponding field
            if key_def.field == "temperature_c":
                sensor_data.temperature_c = _get_float_val(raw_value)
            elif key_def.field == "voltage_cond_v":
                voltage = _get_float_val(raw_value)
                sensor_data.voltage_cond_v = voltage
                sensor_data.humidity_pct = voltage_to_humidity(voltage)
            elif key_def.field == "humidity_pct":
                sensor_data.humidity_pct = _get_float_val(raw_value)

            sensors.append(sensor_data)

    serial_number = payload.get("sn", payload.get("serial_number", ""))

    return ParsedPayload(
        serial_number=serial_number,
        schema_version=schema_version,
        sensors=sensors,
        raw_payload=payload,
    )


def parse_legacy_payload(payload: dict) -> ParsedPayload:
    """
    Parses the legacy format with sensor_0, sensor_1, etc.

    Format:
    {
        "sn": "DEVICE001",
        "schema_v": 1,
        "status": {"rssi": -65, "buf_used": 10, "buf_total": 100, "supply": 3300},
        "sensor_0": {"timestamp": 20250205.143022, "temp": 25.5, ...}
    }
    """
    serial_number = payload.get("sn", payload.get("serial_number", ""))
    schema_version = payload.get("schema_v", payload.get("schema_version", 1))
    status = payload.get("status", {})

    sensors: list[ParsedSensorData] = []
    sensor_pattern = re.compile(r"^sensor_(\d+)$")

    for key, sensor_data in payload.items():
        match = sensor_pattern.match(key)
        if not match or not isinstance(sensor_data, dict):
            continue

        sensor_index = int(match.group(1))
        ts_value = sensor_data.get("timestamp")
        timestamp = (
            parse_numeric_timestamp(ts_value) if isinstance(ts_value, (int, float)) else None
        )

        if timestamp is None:
            continue

        voltage_cond = _get_float(sensor_data, "volt_cond")
        parsed_sensor = ParsedSensorData(
            sensor_index=sensor_index,
            sensor_type="temperature",
            timestamp=timestamp,
            temperature_c=_get_float(sensor_data, "temp"),
            humidity_pct=voltage_to_humidity(voltage_cond),
            voltage_cond_v=voltage_cond,
            supply_mv=_get_int(sensor_data, "supply"),
            msg_counter=_get_int(sensor_data, "msg_cnt"),
            rssi_dbm=status.get("rssi"),
            buffer_used=status.get("buf_used"),
            buffer_total=status.get("buf_total"),
        )
        sensors.append(parsed_sensor)

    return ParsedPayload(
        serial_number=serial_number,
        schema_version=schema_version,
        sensors=sensors,
        raw_payload=payload,
    )


def decode_hm_packet(hex_str: str) -> dict | None:
    """Decodes a binary HM packet from its hex representation.

    Accepts 44 chars (22 bytes, standard) or 56 chars (28 bytes, extended).
    The standard layout is `<2s8sHHhHBBH`; the 6 extra bytes in the extended
    format are stored in `extra` as a tuple without interpretation yet.
    """
    if len(hex_str) not in (44, 56):
        return None
    try:
        data = bytes.fromhex(hex_str)
    except ValueError:
        return None
    type_, uid_bytes, seq, vcc, temp_raw, hum_raw, pending, free, crc = struct.unpack_from(
        "<2s8sHHhHBBH", data
    )
    if type_ != b"HM":
        return None
    result: dict = {
        "uid": uid_bytes.hex(),
        "seq": seq,
        "vcc_mv": vcc,
        "temp_c": temp_raw / 100.0,
        "humidity_pct": hum_raw / 10.0,
        "pending": pending,
        "free": free,
        "crc": crc,
    }
    if len(hex_str) == 56:
        # 6 extra bytes after standard 22 — meaning TBD, stored for future interpretation
        result["extra"] = struct.unpack_from("<HHH", data, 22)
    return result


def is_hm_payload(payload: dict) -> bool:
    """Detects whether the payload is a binary HM packet relayed by a gateway."""
    d = payload.get("d")
    return isinstance(d, str) and len(d) in (44, 56) and "ts" in payload


def decode_gw_config(data: bytes) -> dict:
    """Decodes the gatewayv3 configuration struct.

    The gateway can publish the struct in two ways:
      - 416 raw binary bytes (originally expected format)
      - 832 bytes: the struct hex-encoded as ASCII (each byte → 2 lowercase hex chars)

    Struct fields (little-endian, 416 bytes):
      [0:40]    broker (char[40])
      [40:42]   port (uint16)
      [42:62]   client_id (char[20])
      [62:82]   fw_type (char[20])
      [82:102]  topic_prefix (char[20])
      [110:112] interval_s (uint16)
      [112:114] supply_mv (uint16)
      [114:178] broker2 (char[64])
      [414:416] crc (uint16)
    """
    # Gateway sends an ASCII hex-string (832 bytes): decode to binary first
    if len(data) == 832:
        try:
            data = bytes.fromhex(data.decode("ascii"))
        except (ValueError, UnicodeDecodeError):
            return {}
    if len(data) != 416:
        return {}
    broker = data[0:40].rstrip(b"\x00").decode("ascii", errors="ignore")
    port = struct.unpack_from("<H", data, 40)[0]
    client_id = data[42:62].rstrip(b"\x00").decode("ascii", errors="ignore")
    fw_type = data[62:82].rstrip(b"\x00").decode("ascii", errors="ignore")
    topic_prefix = data[82:102].rstrip(b"\x00").decode("ascii", errors="ignore")
    interval_s = struct.unpack_from("<H", data, 110)[0]
    supply_mv = struct.unpack_from("<H", data, 112)[0]
    broker2 = data[114:178].rstrip(b"\x00").decode("ascii", errors="ignore")
    return {
        "broker": broker,
        "port": port,
        "client_id": client_id,
        "fw_type": fw_type,
        "topic_prefix": topic_prefix,
        "interval_s": interval_s,
        "supply_mv": supply_mv,
        "broker2": broker2,
    }


def parse_hm_payload(
    payload: dict,
    gateway_serial: str,
    device_mac_from_topic: str | None = None,
) -> ParsedPayload:
    """Parses a binary HM payload relayed by a gateway.

    device_mac_from_topic: MAC extracted from the topic (e.g. "28:39:C9:8D:54:20:01:8A"). Kept in
    the signature for compatibility but is NO LONGER used as the source of the serial: node
    identity comes from the frame's normalized uid (normalize_serial), same as parse_gateway_rx, so
    that both paths produce the same serial_number for a given node.
    """
    decoded = decode_hm_packet(payload["d"])
    if decoded is None:
        return ParsedPayload(
            serial_number="",
            schema_version=2,
            sensors=[],
            raw_payload=payload,
            gateway_serial=gateway_serial,
        )

    ts_value = payload.get("ts")
    timestamp = (
        parse_numeric_timestamp(float(ts_value))
        if isinstance(ts_value, (int, float))
        else datetime.now(timezone.utc)
    )
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    # Node identity = frame uid, normalized to canonical MAC (same criterion as
    # parse_gateway_rx). Do NOT use device_mac_from_topic as the source: the frame's uid is the
    # source of truth, and normalizing guarantees /HM/ and gateway/+/rx produce the SAME serial
    # for the same node.
    device_serial = normalize_serial(decoded["uid"])
    if device_serial is None:
        # malformed uid → don't create a device with a garbage serial
        return ParsedPayload(
            serial_number="",
            schema_version=2,
            sensors=[],
            raw_payload=payload,
            gateway_serial=gateway_serial,
        )

    rssi = _get_int(payload, "rssi")
    temp_sensor = ParsedSensorData(
        sensor_index=0,
        sensor_type="temperature",
        timestamp=timestamp,
        node_id=1,
        sensor_key="t",
        temperature_c=decoded["temp_c"],
        supply_mv=decoded["vcc_mv"],
        msg_counter=decoded["seq"],
        rssi_dbm=rssi,
        buffer_used=decoded["pending"],
        buffer_total=None,
    )
    hum_sensor = ParsedSensorData(
        sensor_index=1,
        sensor_type="humidity",
        timestamp=timestamp,
        node_id=1,
        sensor_key="h",
        humidity_pct=decoded["humidity_pct"],
        supply_mv=decoded["vcc_mv"],
        msg_counter=decoded["seq"],
        rssi_dbm=rssi,
        buffer_used=decoded["pending"],
        buffer_total=None,
    )

    return ParsedPayload(
        serial_number=device_serial,
        schema_version=2,
        sensors=[temp_sensor, hum_sensor],
        raw_payload=payload,
        gateway_serial=gateway_serial,
    )


# ───────────────────────────────────────────────────────────────────────────
# Gateway LoRa v1.0.0 — HM v3.6 (topic gateway/{id}/rx + /telemetry)
# 34-byte frame, distinct from the legacy HM of decode_hm_packet (22/28 bytes).
# Spec: MQTT_Uplink_Backend.pdf §3.
# ───────────────────────────────────────────────────────────────────────────

# ProtocolFrame v3.6 constants (little-endian, packed C struct)
HM_HEADER_LEN = 18          # type(2)+ver(3)+msg(1)+flags(1)+uid(8)+seq(2)+payload_len(1)
HM_CRC_LEN = 2
HMPAYLOAD_V36_SIZE = 14     # v1 ONLY supports this payload (no arbitrary size)
UID_HEX_LEN = 16            # 8 bytes → 16 hex chars

DecodeFailure = Literal["crc", "version", "format", "size", "serial", "timestamp"]


class HMv36Decoded(TypedDict):
    uid: str
    node_seq: int
    msg_type: int
    flags: int
    vcc_mv: int
    temp_c: float
    humidity_adc: int
    hum_alarm: int
    measure_cycles: int
    send_cycles: int
    pending: int
    free_slots: int
    alarm: bool
    low_batt: bool


class DecodeResult(NamedTuple):
    data: HMv36Decoded | None       # None if it failed
    reason: DecodeFailure | None    # None if ok; the reason if it failed

    @property
    def ok(self) -> bool:
        # state is defined by the reason, not data → no impossible states
        return self.reason is None


class RxResult(NamedTuple):
    payload: ParsedPayload | None
    reason: DecodeFailure | None

    @property
    def ok(self) -> bool:
        return self.reason is None


def crc16_ccitt(data: bytes) -> int:
    """CRC16-CCITT (poly 0x1021, init 0xFFFF) — same algorithm as the gateway's protocol.c."""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def normalize_serial(s: str) -> str | None:
    """Normalizes a uid to a canonical MAC (8 bytes → 'AA:BB:...', upper).

    Returns None if it isn't exactly 16 hex chars — a malformed uid must NOT
    slip through as a valid serial (it would create an orphaned device).
    """
    hexed = re.sub(r"[^0-9A-Fa-f]", "", s).upper()
    if len(hexed) != UID_HEX_LEN:
        return None
    return ":".join(hexed[i : i + 2] for i in range(0, len(hexed), 2))


def _ts_from_ms(ts_ms: object) -> datetime | None:
    """Converts ts_ms (epoch ms from the wrapper) to a UTC datetime only if it's plausible.

    Returns None if missing or out of range (1970 / far future from a GW with no RTC).
    Doesn't invent now(): the parser only transforms. The decision of what to do with a missing
    ts belongs to the ingestion layer (idempotency).
    """
    try:
        dt = datetime.fromtimestamp(float(ts_ms) / 1000, tz=timezone.utc)  # type: ignore[arg-type]
        now = datetime.now(timezone.utc)
        if datetime(2020, 1, 1, tzinfo=timezone.utc) < dt < now + timedelta(minutes=5):
            return dt
    except (ValueError, TypeError, OverflowError, OSError):
        pass
    return None


def _sane_ts(ts_ms: object) -> datetime:
    """Like _ts_from_ms but with a fallback to now() when the ts isn't usable.

    Correct ONLY for events whose semantics ARE "now" (gateway status/telemetry:
    'the backend saw this now'). Do NOT use for an /rx measurement, where a now()
    would break replay dedup — there _ts_from_ms is used instead (which returns None).
    """
    return _ts_from_ms(ts_ms) or datetime.now(timezone.utc)


def decode_hm_v36(raw_hex: str) -> DecodeResult:
    """Decodes an HM v3.6 ProtocolFrame (34 bytes) from its hex.

    Validations in order: hex -> HM header -> version -> payload_len -> size -> CRC -> unpack.
    Returns DecodeResult(data, reason): reason is None if ok, or the reason for the discard
    (crc/version/format/size) so the caller can emit the correct metric.
    """
    try:
        data = bytes.fromhex(raw_hex)
    except ValueError:
        return DecodeResult(None, "format")
    # Tolerate ONLY a residual RadioHead header (FF FF 00 00 = 4 bytes), not a spurious HM
    i = data.find(b"HM")
    if i == -1 or i > 4:
        return DecodeResult(None, "format")
    data = data[i:]
    if len(data) < HM_HEADER_LEN + HM_CRC_LEN:
        return DecodeResult(None, "size")
    if data[2] != 1:  # ver_major: v2.x could change the layout
        return DecodeResult(None, "version")
    payload_len = data[17]
    if payload_len != HMPAYLOAD_V36_SIZE:  # v1 only supports 14
        return DecodeResult(None, "format")
    expected = HM_HEADER_LEN + HMPAYLOAD_V36_SIZE + HM_CRC_LEN
    if len(data) != expected:
        return DecodeResult(None, "size")
    crc_off = HM_HEADER_LEN + HMPAYLOAD_V36_SIZE
    crc_frame = struct.unpack_from("<H", data, crc_off)[0]
    if crc16_ccitt(data[:crc_off]) != crc_frame:  # CRC before unpack
        return DecodeResult(None, "crc")
    (
        type_, _vma, _vmi, _vpa, msg_type, flags, uid,
        seq, _plen, vcc, temp, hum_adc, hum_alarm, mcyc, scyc, pending, free,
    ) = struct.unpack_from("<2sBBBBB8sHBHhHHHHBB", data)
    if type_ != b"HM":
        return DecodeResult(None, "format")
    decoded: HMv36Decoded = {
        "uid": uid.hex().upper(),
        "node_seq": seq,
        "msg_type": msg_type,
        "flags": flags,
        "vcc_mv": vcc,
        "temp_c": temp / 100.0,
        "humidity_adc": hum_adc,
        "hum_alarm": hum_alarm,
        "measure_cycles": mcyc,
        "send_cycles": scyc,
        "pending": pending,
        "free_slots": free,
        "alarm": bool(flags & 0x02) or msg_type == 0x05,
        "low_batt": bool(flags & 0x04),
    }
    return DecodeResult(decoded, None)


def parse_gateway_rx(wrapper: dict, gw_serial: str) -> RxResult:
    """From the gateway/{id}/rx JSON wrapper to a ParsedPayload (1 temperature sensor).

    Reuses the parse_hm_payload mold. Humidity comes as raw ADC (not %): it's stored
    in raw_payload, humidity_pct is NOT derived without a calibration curve.
    """
    res = decode_hm_v36(wrapper.get("raw", ""))
    if not res.ok or res.data is None:
        return RxResult(None, res.reason)
    decoded = res.data

    serial = normalize_serial(decoded["uid"])
    if serial is None:
        return RxResult(None, "serial")

    # The parser only transforms: uses the frame's ts_ms, does NOT invent now() (would break
    # SD replay dedup). If the frame doesn't carry a usable ts we can't give the measurement a
    # stable identity (PK (sensor_id, ts); no unique index on (sensor_id, node_seq)). In that
    # case it's discarded WITH visibility, instead of inserting a duplicate dated with now().
    # In practice this holds: /rx carries ts_ms.
    timestamp = _ts_from_ms(wrapper.get("ts_ms"))
    if timestamp is None:
        logger.warning(
            "HM v3.6 /rx with no usable ts_ms (gw=%s uid=%s node_seq=%s) — discarded to avoid "
            "duplicating the measurement with now()",
            gw_serial, decoded["uid"], decoded["node_seq"],
        )
        return RxResult(None, "timestamp")
    rssi = _get_int(wrapper, "rssi")

    temp_sensor = ParsedSensorData(
        sensor_index=0,
        sensor_type="temperature",
        timestamp=timestamp,
        node_id=1,
        sensor_key="t",
        temperature_c=decoded["temp_c"],
        supply_mv=decoded["vcc_mv"],
        msg_counter=decoded["node_seq"],
        rssi_dbm=rssi,
        buffer_used=decoded["pending"],
        buffer_total=None,
    )

    # Humidity (slot 1): the frame carries humidity as raw ADC 0-1023, NOT as %. Without the
    # manufacturer's official calibration curve, we store the raw ADC in the humidity_pct
    # column — the physical magnitude is the same (wood moisture), only the representation
    # changes. The effective UNIT exposed by API/UI is "ADC", not "%". Once the ADC->% curve
    # exists, ingest will start persisting the percentage WITHOUT changing the sensor or the
    # contract (same sensor_type 'humidity', same slot).
    humidity_sensor = ParsedSensorData(
        sensor_index=1,
        sensor_type="humidity",
        timestamp=timestamp,
        node_id=1,
        sensor_key="h",
        humidity_pct=float(decoded["humidity_adc"]),  # TEMP: raw ADC, not % (see note above)
        msg_counter=decoded["node_seq"],
        rssi_dbm=rssi,
    )

    payload = ParsedPayload(
        serial_number=serial,
        schema_version=2,
        sensors=[temp_sensor, humidity_sensor],
        # raw_payload keeps the wrapper + the decoded fields (ADC humidity, flags, etc.)
        raw_payload={**wrapper, "decoded": dict(decoded)},
        gateway_serial=gw_serial,
        # Alarm state of the frame -> ingest detects transitions and emits events.
        alarm=decoded["alarm"],
        low_batt=decoded["low_batt"],
    )
    return RxResult(payload, None)


def parse_gateway_telemetry(payload: dict, gw_serial: str) -> ParsedGatewayStatus:
    """Gateway health (gateway/{id}/telemetry) -> ParsedGatewayStatus.

    vgw <- voltage_mv; csq <- lte_csq (cellular signal, 0-31); the rest (wifi_rssi, uptime_sec,
    temp_c, heap_free, fw) stays in raw_payload (JSONB). No migration.
    """
    return ParsedGatewayStatus(
        serial_number=gw_serial,
        timestamp=_sane_ts(payload.get("ts_ms")),
        csq=_get_int(payload, "lte_csq"),
        erf=None,
        rst=None,
        vgw=_get_int(payload, "voltage_mv"),
        raw_payload=payload,
    )


def parse_gateway_status(payload: dict, gw_serial: str) -> ParsedGatewayStatus:
    """Gateway presence (gateway/{id}/status, uplink on connect) -> ParsedGatewayStatus.

    Spec §6: {"gw","state":"online","freq"}. state/freq stay in raw_payload exactly as they arrive
    (unnormalized: if 'reconnecting'/'wifi_lost' arrives tomorrow, no info is lost). There's no
    voltage/measurement; this uplink only marks that the GW just connected.
    """
    pkt_gw = payload.get("gw")
    if pkt_gw and str(pkt_gw) != gw_serial:
        logger.warning(
            "gateway status serial mismatch: payload.gw=%s topic=%s", pkt_gw, gw_serial
        )
    return ParsedGatewayStatus(
        serial_number=gw_serial,
        # explicit now() — NOT _sane_ts(ts_ms): the ts does NOT date a remote event, it dates the
        # instant THIS backend saw the gateway connect. Opposite semantics to /rx (§3.2).
        timestamp=datetime.now(timezone.utc),
        csq=None,
        erf=None,
        rst=None,
        vgw=None,
        raw_payload=payload,  # includes state (literal) and freq
    )


def parse_payload(payload: dict, serial_number: str | None = None) -> ParsedPayload:
    """
    Parses a payload, automatically detecting the format.

    Args:
        payload: The received JSON payload
        serial_number: Optional serial number (useful for MQTT where it comes from the topic)

    Returns:
        Normalized ParsedPayload
    """
    if is_hm_payload(payload):
        # serial_number from topic = gateway MAC; device serial extracted from binary
        return parse_hm_payload(payload, gateway_serial=serial_number or "")

    has_ts_keys = any(key.startswith("TS") and key[2:].isdigit() for key in payload.keys())
    has_sensor_keys = any(key.startswith("sensor_") for key in payload.keys())

    if has_ts_keys:
        result = parse_sensor_payload_v1(payload)
    elif has_sensor_keys:
        result = parse_legacy_payload(payload)
    else:
        result = ParsedPayload(
            serial_number=payload.get("sn", payload.get("serial_number", "")),
            schema_version=payload.get("schema_version", 1),
            sensors=[],
            raw_payload=payload,
        )

    if serial_number:
        result.serial_number = serial_number

    return result


def _get_float(data: dict, key: str) -> float | None:
    """Extrae un valor float de forma segura."""
    value = data.get(key)
    return _get_float_val(value)


def _get_float_val(value: object) -> float | None:
    """Convierte un valor a float de forma segura."""
    if value is None:
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return None


def _get_int(data: dict, key: str) -> int | None:
    """Extrae un valor int de forma segura."""
    value = data.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
