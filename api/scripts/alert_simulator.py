#!/usr/bin/env python3
"""
Alert scenario simulator for SensorHub.

Publishes MQTT payloads designed to trigger each of the alerts configured
in prometheus/rules/. Each scenario is independent, or all of them can run
in parallel with --scenario all-concurrent.

Alerts covered:
  IoT:
    GatewayOffline          — GW-SIM-01 stops sending heartbeats       [~13 min]
    SensorMissingTelemetry  — DEV-SIM-01 stops sending telemetry       [~35 min]
    SensorTemperatureHigh   — DEV-SIM-02 reports sustained >40°C       [~7 min]
    SensorBatteryLow        — DEV-SIM-05 reports sustained <2900mV     [~20 min]

  Infrastructure (via MQTT):
    IngestionErrorRateHigh  — malformed payloads                       [~3 min]
    IngestionSilent         — all devices stop                         [~10 min]
    IngestionBufferNearFull — massive payload flood                    [<1 min]

Usage:
    python scripts/alert_simulator.py --scenario list
    python scripts/alert_simulator.py --scenario all-concurrent
    python scripts/alert_simulator.py --scenario temperature-high
    python scripts/alert_simulator.py --scenario gateway-offline
    python scripts/alert_simulator.py --scenario baseline
"""

import argparse
import json
import random
import sys
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

DEFAULT_BROKER = "localhost"
DEFAULT_PORT   = 1883
DEFAULT_TOPIC  = "telemetry/{serial}/data"

# Dedicated serials — don't collide with the seed (GW-001, DEV-0001...)
GW_SERIALS  = ["GW-SIM-01", "GW-SIM-02"]
DEV_SERIALS = [f"DEV-SIM-{i:02d}" for i in range(1, 7)]

# DEV-SIM-02 is reserved for SensorTemperatureHigh — the other threads
# avoid writing to this serial so they don't overwrite the high temperature.
DEV_SERIALS_NO_TEMP = [d for d in DEV_SERIALS if d != "DEV-SIM-02"]

# Thresholds that must match prometheus/rules/02-alert-iot.yml
TEMP_THRESHOLD   = 40.0   # SensorTemperatureHigh:   avg[5m] > 40 for:2m
GW_OFFLINE_SECS  = 600    # GatewayOffline:          > 10 min  for:3m  → total ~13 min
DEV_MISSING_SECS = 1800   # SensorMissingTelemetry:  > 30 min  for:5m  → total ~35 min
BATTERY_LOW_MV   = 2700   # SensorBatteryLow:        avg[10m] < 2900   for:10m → total ~20 min

# DEV-SIM-05 reserved for low battery
DEV_BATTERY_SERIAL = "DEV-SIM-05"


# ---------------------------------------------------------------------------
# Payload helpers
# ---------------------------------------------------------------------------

def _ts(unix: bool = False) -> float:
    """Generates payload timestamp — proprietary format YYYYMMDD.HHmmss or Unix epoch."""
    now = datetime.now(timezone.utc)
    if unix:
        return now.timestamp()
    date_part = now.year * 10000 + now.month * 100 + now.day
    time_part = now.hour * 10000 + now.minute * 100 + now.second
    return round(date_part + time_part / 1_000_000, 6)


# DEV_SERIALS with an odd index use Unix epoch; even ones use YYYYMMDD.HHmmss
def _dev_unix(serial: str) -> bool:
    """True if the device's serial should use Unix epoch."""
    try:
        idx = DEV_SERIALS.index(serial)
        return idx % 2 == 1
    except ValueError:
        return False


def make_device_payload(
    node_id: int = 1,
    temp: float | None = None,
    serial: str = "",
    supply_mv: int | None = None,
) -> dict:
    ti = temp if temp is not None else round(random.uniform(20.0, 25.0), 2)
    vcc = supply_mv if supply_mv is not None else random.randint(3200, 3500)
    return {
        "schema_version": 1,
        f"TS{node_id}":  _ts(unix=_dev_unix(serial)),
        f"o{node_id}":   random.randint(1, 9999),
        f"v{node_id}":   vcc,
        f"s{node_id}":   random.randint(-80, -40),
        f"um{node_id}":  random.randint(0, 20),
        f"tm{node_id}":  100,
        f"ti{node_id}":  ti,
        f"ts{node_id}":  round(ti - random.uniform(1.0, 3.0), 2),
        f"h{node_id}":   round(random.uniform(30.0, 60.0), 1),
    }


def make_gateway_payload(serial: str = "") -> dict:
    # GW-SIM-01 (index 0) → YYYYMMDD, GW-SIM-02 (index 1) → Unix epoch
    try:
        unix = GW_SERIALS.index(serial) % 2 == 1
    except ValueError:
        unix = False
    return {
        "TSgw": _ts(unix=unix),
        "csq":  random.randint(12, 25),
        "erf":  0,
        "rst":  0,
        "vgw":  random.randint(12000, 13000),
    }


def make_malformed_payload() -> dict:
    # Payload with no TS{n} key at all — the parser doesn't discover any sensor
    # → sensors=[] → PAYLOADS_FAILED{error_type="NoSensorsDiscovered"} gets incremented
    return {
        "schema_version": 1,
        "INVALID_KEY": random.randint(1, 999),
        "garbage": "data",
    }


# ---------------------------------------------------------------------------
# MQTT client (thread-safe — paho loop runs in its own thread)
# ---------------------------------------------------------------------------

class SimClient:
    def __init__(self, broker: str, port: int, topic: str, client_id: str = "sensorhub-alert-sim"):
        self.broker = broker
        self.port   = port
        self.topic  = topic
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self._client.on_connect    = lambda c, u, f, rc, p: None
        self._client.on_disconnect = lambda c, u, f, rc, p: None
        self._lock = threading.Lock()

    def connect(self):
        try:
            self._client.connect(self.broker, self.port, keepalive=60)
            self._client.loop_start()
            time.sleep(0.5)
        except Exception as e:
            print(f"  ERROR connecting to broker: {e}")
            print("  Verify: docker compose up -d mqtt")
            sys.exit(1)

    def disconnect(self):
        self._client.loop_stop()
        self._client.disconnect()

    def publish(self, serial: str, payload: dict) -> bool:
        topic = self.topic.format(serial=serial)
        with self._lock:
            r = self._client.publish(topic, json.dumps(payload), qos=1)
        return r.rc == mqtt.MQTT_ERR_SUCCESS


# ---------------------------------------------------------------------------
# Individual scenarios (all use the same SimClient)
# ---------------------------------------------------------------------------

def _log(tag: str, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] {tag:22s} {msg}", flush=True)


def run_baseline(sim: SimClient, duration: int = 60, interval: float = 5.0,
                 stop_event: threading.Event | None = None):
    """Publishes normal traffic for all serials during `duration` seconds."""
    end = time.time() + duration
    while time.time() < end:
        if stop_event and stop_event.is_set():
            break
        for gw in GW_SERIALS:
            sim.publish(gw, make_gateway_payload(serial=gw))
        for dev in DEV_SERIALS:
            sim.publish(dev, make_device_payload(serial=dev))
        time.sleep(interval)


# ── IoT: high temperature ────────────────────────────────────────────────────

def thread_temperature_high(sim: SimClient, stop: threading.Event):
    """
    DEV-SIM-02 reports sustained temperature >40°C.
    SensorTemperatureHigh: avg_over_time[5m] > 40 + for:2m → FIRING in ~7 min.
    """
    HOT = 44.5
    INTERVAL = 8.0
    _log("TEMP-HIGH", f"DEV-SIM-02 sending {HOT}°C (threshold >40°C, alert in ~7 min)")
    while not stop.is_set():
        # GW-SIM-02 only — GW-SIM-01 is exclusive to the gateway_offline thread
        sim.publish("GW-SIM-02", make_gateway_payload(serial="GW-SIM-02"))
        temp = HOT + random.uniform(-0.5, 0.5)
        sim.publish("DEV-SIM-02", make_device_payload(temp=temp, serial="DEV-SIM-02"))
        time.sleep(INTERVAL)
    _log("TEMP-HIGH", "stopped")


# ── IoT: low battery ─────────────────────────────────────────────────────────

def thread_battery_low(sim: SimClient, stop: threading.Event):
    """
    DEV-SIM-05 reports sustained supply voltage below 2900 mV.
    SensorBatteryLow: avg_over_time[10m] < 2900 + for:10m → FIRING in ~20 min.
    """
    INTERVAL = 10.0
    _log("BATTERY-LOW", f"DEV-SIM-05 sending {BATTERY_LOW_MV}mV (threshold <2900mV, alert in ~20 min)")  # noqa: E501
    while not stop.is_set():
        vcc = BATTERY_LOW_MV + random.randint(-30, 30)
        sim.publish(
            DEV_BATTERY_SERIAL,
            make_device_payload(serial=DEV_BATTERY_SERIAL, supply_mv=vcc),
        )
        time.sleep(INTERVAL)
    _log("BATTERY-LOW", "stopped")


# ── IoT: gateway offline ─────────────────────────────────────────────────────

def thread_gateway_offline(sim: SimClient, stop: threading.Event):
    """
    GW-SIM-01 stops sending heartbeats after the initial baseline.
    GatewayOffline: > 10 min without heartbeat + for:3m → FIRING ~13 min after going silent.
    """
    BASELINE_SECS = 45  # enough for sensor-state to record it
    INTERVAL = 10.0

    _log("GW-OFFLINE", f"GW-SIM-01 baseline {BASELINE_SECS}s then disappears")
    end_baseline = time.time() + BASELINE_SECS
    while time.time() < end_baseline and not stop.is_set():
        sim.publish("GW-SIM-01", make_gateway_payload(serial="GW-SIM-01"))
        sim.publish("GW-SIM-02", make_gateway_payload(serial="GW-SIM-02"))
        time.sleep(INTERVAL)

    _log("GW-OFFLINE", "GW-SIM-01 OFFLINE — GW-SIM-02 still active (alert in ~13 min)")
    while not stop.is_set():
        sim.publish("GW-SIM-02", make_gateway_payload(serial="GW-SIM-02"))   # GW-SIM-01 silenced
        time.sleep(INTERVAL)
    _log("GW-OFFLINE", "stopped")


# ── IoT: device missing ──────────────────────────────────────────────────────

def thread_sensor_missing(sim: SimClient, stop: threading.Event):
    """
    DEV-SIM-01 stops sending telemetry after the baseline.
    SensorMissingTelemetry: > 30 min without data + for:5m → FIRING in ~35 min.
    """
    BASELINE_SECS = 45
    INTERVAL = 10.0

    _log("DEV-MISSING", f"DEV-SIM-01 baseline {BASELINE_SECS}s then disappears")
    end_baseline = time.time() + BASELINE_SECS
    while time.time() < end_baseline and not stop.is_set():
        for dev in DEV_SERIALS_NO_TEMP:  # skip DEV-SIM-02 (reserved for temp-high)
            sim.publish(dev, make_device_payload(serial=dev))
        time.sleep(INTERVAL)

    _log("DEV-MISSING", "DEV-SIM-01 OFFLINE — the rest stay active (alert in ~35 min)")
    while not stop.is_set():
        for dev in DEV_SERIALS:
            # Skip DEV-SIM-01 (offline) and DEV-SIM-02 (reserved for temperature-high)
            if dev not in ("DEV-SIM-01", "DEV-SIM-02"):
                sim.publish(dev, make_device_payload(serial=dev))
        time.sleep(INTERVAL)
    _log("DEV-MISSING", "stopped")


# ── Infra: ingestion errors ──────────────────────────────────────────────────

def thread_ingestion_errors(sim: SimClient, stop: threading.Event):
    """
    Mixes in ~20% malformed payloads to force IngestionErrorRateHigh (>5%).
    Runs in 30s bursts every 60s to sustain the error rate.
    Uses DEV_SERIALS_NO_TEMP so it doesn't overwrite DEV-SIM-02's high temperature.
    """
    ERROR_RATIO = 0.20
    _log("INGEST-ERR", f"Bursts with {int(ERROR_RATIO*100)}% malformed payloads (alert in ~3 min)")  # noqa: E501
    while not stop.is_set():
        # 30s burst
        burst_end = time.time() + 30
        count_ok = count_bad = 0
        while time.time() < burst_end and not stop.is_set():
            for dev in DEV_SERIALS_NO_TEMP:
                if random.random() < ERROR_RATIO:
                    sim.publish(dev, make_malformed_payload())
                    count_bad += 1
                else:
                    sim.publish(dev, make_device_payload(serial=dev))
                    count_ok += 1
            time.sleep(0.3)
        _log("INGEST-ERR", f"burst: {count_ok} OK / {count_bad} malformed")
        # 30s pause between bursts
        stop.wait(30)
    _log("INGEST-ERR", "stopped")


# ── Infra: ingestion silence ─────────────────────────────────────────────────

def thread_ingestion_silent(sim: SimClient, stop: threading.Event):
    """
    All devices stop for >5 min.
    IngestionSilent: rate == 0 for:5m → FIRING ~10 min after going silent.
    Gateways keep heartbeating (they're not measurements).
    """
    BASELINE_SECS = 45
    SILENT_SECS   = 700   # >10 min to exceed threshold + for:
    INTERVAL      = 10.0

    _log("SILENT", f"Devices active for {BASELINE_SECS}s, then all offline for {SILENT_SECS}s")
    end_baseline = time.time() + BASELINE_SECS
    while time.time() < end_baseline and not stop.is_set():
        for dev in DEV_SERIALS_NO_TEMP:  # skip DEV-SIM-02 (reserved for temp-high)
            sim.publish(dev, make_device_payload(serial=dev))
        time.sleep(INTERVAL)

    _log("SILENT", "All DEVICES OFFLINE — gateways keep going (alert in ~10 min)")
    silent_end = time.time() + SILENT_SECS
    while time.time() < silent_end and not stop.is_set():
        # GW-SIM-02 only — GW-SIM-01 is exclusive to the gateway_offline thread
        sim.publish("GW-SIM-02", make_gateway_payload(serial="GW-SIM-02"))
        time.sleep(INTERVAL)

    _log("SILENT", "Devices coming back online")
    while not stop.is_set():
        for dev in DEV_SERIALS_NO_TEMP:  # skip DEV-SIM-02 (reserved for temp-high)
            sim.publish(dev, make_device_payload(serial=dev))
        time.sleep(INTERVAL)
    _log("SILENT", "stopped")


# ── Infra: buffer flood ──────────────────────────────────────────────────────

def thread_buffer_flood(sim: SimClient, stop: threading.Event):
    """
    Floods 50 serials at high frequency to try to outpace the flush rate.
    IngestionBufferNearFull: buffer_size > 80 for:30s.
    Note: in environments with a fast DB, the buffer drains before it fills up.
    """
    FLOOD_SERIALS = [f"DEV-FLOOD2-{i:03d}" for i in range(50)]
    INTERVAL = 0.05   # 20 pubs/s per serial → 1000 payloads/s total

    _log("BUF-FLOOD", f"Flooding with {len(FLOOD_SERIALS)} serials at ~{1/INTERVAL:.0f} pub/s")
    _log("BUF-FLOOD", "Verify: http://localhost:9090/graph?g0.expr=telemetry_buffer_size")
    while not stop.is_set():
        for serial in FLOOD_SERIALS:
            sim.publish(serial, make_device_payload(serial=serial))
        time.sleep(INTERVAL)
    _log("BUF-FLOOD", "stopped")


# ---------------------------------------------------------------------------
# all-concurrent scenario
# ---------------------------------------------------------------------------

def scenario_all_concurrent(sim: SimClient):
    """
    Runs all scenarios in parallel using independent threads.

    Approximate alert timeline:
      T+0:00  → Everything starts
      T+3:00  → IngestionErrorRateHigh     FIRING  (error bursts)
      T+7:00  → SensorTemperatureHigh      FIRING  (DEV-SIM-02)
      T+10:00 → IngestionSilent            FIRING  (all devices)
      T+13:00 → GatewayOffline             FIRING  (GW-SIM-01)
      T+20:00 → SensorBatteryLow           FIRING  (DEV-SIM-05)
      T+35:00 → SensorMissingTelemetry     FIRING  (DEV-SIM-01)

    buffer-flood excluded — use --scenario buffer-flood separately.
    """
    print(f"\n{'='*65}")
    print("  MODE: all-concurrent")
    print("  All scenarios running in parallel")
    print(f"{'='*65}")
    print(f"""
  Expected alert timeline:
    ~3 min  → IngestionErrorRateHigh    (malformed payloads)
    ~7 min  → SensorTemperatureHigh     (DEV-SIM-02 at 44°C)
    ~10 min → IngestionSilent           (devices offline)
    ~13 min → GatewayOffline            (GW-SIM-01 disappears)
    ~20 min → SensorBatteryLow          (DEV-SIM-05 at 2700mV)
    ~35 min → SensorMissingTelemetry    (DEV-SIM-01 disappears)

  Note: buffer-flood excluded — run it separately with --scenario buffer-flood

  Monitor at:
    Prometheus:   http://localhost:9090/alerts
    Alertmanager: http://localhost:9093
    Grafana:      http://localhost:3000 → IoT Fleet / Infrastructure

  Serials:
    Gateways: {', '.join(GW_SERIALS)}
    Devices:  {', '.join(DEV_SERIALS)}

  Ctrl+C to stop everything
""")

    stop = threading.Event()

    threads = [
        threading.Thread(
            target=thread_temperature_high, args=(sim, stop), name="temp-high", daemon=True
        ),
        threading.Thread(
            target=thread_battery_low, args=(sim, stop), name="battery-low", daemon=True
        ),
        threading.Thread(
            target=thread_gateway_offline, args=(sim, stop), name="gw-offline", daemon=True
        ),
        threading.Thread(
            target=thread_sensor_missing, args=(sim, stop), name="dev-missing", daemon=True
        ),
        threading.Thread(
            target=thread_ingestion_errors, args=(sim, stop), name="ingest-err", daemon=True
        ),
        threading.Thread(
            target=thread_ingestion_silent, args=(sim, stop), name="silent", daemon=True
        ),
        # buffer-flood excluded from all-concurrent — run it separately with --scenario buffer-flood
    ]

    for t in threads:
        t.start()
        time.sleep(0.2)  # staggered so the MQTT connection isn't saturated on startup

    try:
        # Status loop: prints alert state every 60s
        cycle = 0
        while True:
            time.sleep(60)
            cycle += 1
            ts = datetime.now().strftime("%H:%M:%S")
            alive = sum(1 for t in threads if t.is_alive())
            print(f"\n  ── Status [{ts}] T+{cycle}min | {alive}/{len(threads)} threads active ──")
            _print_alert_status()
    except KeyboardInterrupt:
        print("\n\n  Stopping all threads...")
        stop.set()
        for t in threads:
            t.join(timeout=5)
        print("  All threads stopped.")


def _print_alert_status():
    """Prints the current alert status by querying Prometheus."""
    try:
        import urllib.request
        url = "http://localhost:9090/api/v1/query?query=ALERTS"
        with urllib.request.urlopen(url, timeout=3) as resp:
            data = json.loads(resp.read())
        alerts = data["data"]["result"]
        if not alerts:
            print("  No active alerts")
            return
        for a in sorted(alerts, key=lambda x: x["metric"]["alertname"]):
            m = a["metric"]
            state  = m["alertstate"].upper()
            name   = m["alertname"]
            serial = m.get("serial_number", m.get("gateway_serial", ""))
            node   = m.get("node_id", "")
            key    = m.get("sensor_key", "")
            sev    = m.get("severity", "?")
            detail = f" {serial}" if serial else ""
            detail += f" node={node}({key})" if node else ""
            icon = "🔴" if state == "FIRING" else "🟡"
            print(f"  {icon} [{state}] {name}{detail} ({sev})")
    except Exception:
        print("  (could not query Prometheus)")


# ---------------------------------------------------------------------------
# Individual scenarios (presentation wrappers)
# ---------------------------------------------------------------------------

def scenario_baseline(sim: SimClient, duration: int = 120, interval: float = 3.0):
    """Normal traffic — useful for resetting state after a scenario."""
    _header("BASELINE", "Normal traffic — all devices and gateways active")
    print(f"  Duration: {duration}s  Ctrl+C to end early\n")
    stop = threading.Event()
    try:
        run_baseline(sim, duration=duration, interval=interval, stop_event=stop)
    except KeyboardInterrupt:
        stop.set()
        print("\n  Baseline interrupted")


def scenario_battery_low(sim: SimClient):
    _header("BATTERY-LOW", "DEV-SIM-05 reports sustained supply voltage <2900mV")
    _thresholds("SensorBatteryLow", "avg[10m] <2900mV + for:10m ≈ 20 min total")
    stop = threading.Event()
    try:
        thread_battery_low(sim, stop)
    except KeyboardInterrupt:
        stop.set()
        print("\n  Scenario interrupted")


def scenario_gateway_offline(sim: SimClient):
    _header("GATEWAY-OFFLINE", "GW-SIM-01 stops sending heartbeats")
    _thresholds("GatewayOffline", "10 min without heartbeat + for:3m ≈ 13 min total")
    stop = threading.Event()
    try:
        thread_gateway_offline(sim, stop)
    except KeyboardInterrupt:
        stop.set()
        print("\n  Scenario interrupted")


def scenario_sensor_missing(sim: SimClient):
    _header("SENSOR-MISSING", "DEV-SIM-01 stops sending telemetry")
    _thresholds("SensorMissingTelemetry", "30 min without data + for:5m ≈ 35 min total")
    stop = threading.Event()
    try:
        thread_sensor_missing(sim, stop)
    except KeyboardInterrupt:
        stop.set()
        print("\n  Scenario interrupted")


def scenario_temperature_high(sim: SimClient):
    _header("TEMPERATURE-HIGH", "DEV-SIM-02 reports sustained temperature >40°C")
    _thresholds("SensorTemperatureHigh", "avg[5m] >40°C + for:2m ≈ 7 min total")
    stop = threading.Event()
    try:
        thread_temperature_high(sim, stop)
    except KeyboardInterrupt:
        stop.set()
        print("\n  Scenario interrupted")


def scenario_ingestion_errors(sim: SimClient):
    _header("INGESTION-ERRORS", "Bursts of malformed payloads → error rate >5%")
    _thresholds("IngestionErrorRateHigh", "error rate >5% for:3m")
    stop = threading.Event()
    try:
        thread_ingestion_errors(sim, stop)
    except KeyboardInterrupt:
        stop.set()
        print("\n  Scenario interrupted")


def scenario_ingestion_silent(sim: SimClient):
    _header("INGESTION-SILENT", "All devices offline → zero measurements")
    _thresholds("IngestionSilent", "rate == 0 for:5m ≈ 10 min total")
    stop = threading.Event()
    try:
        thread_ingestion_silent(sim, stop)
    except KeyboardInterrupt:
        stop.set()
        print("\n  Scenario interrupted")


def scenario_buffer_flood(sim: SimClient):
    _header("BUFFER-FLOOD", "Massive payload flood → buffer near full")
    _thresholds("IngestionBufferNearFull", "buffer_size > 80 for:30s")
    stop = threading.Event()
    try:
        thread_buffer_flood(sim, stop)
    except KeyboardInterrupt:
        stop.set()
        print("\n  Scenario interrupted")


# ---------------------------------------------------------------------------
# Presentation helpers
# ---------------------------------------------------------------------------

def _header(name: str, description: str):
    print(f"\n{'='*65}")
    print(f"  SCENARIO: {name}")
    print(f"  {description}")
    print(f"{'='*65}")


def _thresholds(alert: str, condition: str):
    print(f"\n  Target alert: {alert}")
    print(f"  Condition:    {condition}")
    print("  Check at:     http://localhost:9090/alerts")
    print("                http://localhost:9093 (Alertmanager)")
    print()


SCENARIOS = {
    "all-concurrent":   (None,                       "ALL scenarios in parallel [recommended]"),
    "baseline":         (scenario_baseline,          "Normal traffic — reset state"),
    "gateway-offline":  (scenario_gateway_offline,
                         "GW-SIM-01 offline → GatewayOffline [~13 min]"),
    "sensor-missing":   (scenario_sensor_missing,
                         "DEV-SIM-01 offline → SensorMissingTelemetry [~35 min]"),
    "temperature-high": (scenario_temperature_high,
                         "DEV-SIM-02 >40°C → SensorTemperatureHigh [~7 min]"),
    "battery-low":      (scenario_battery_low,
                         "DEV-SIM-05 <2900mV → SensorBatteryLow [~20 min]"),
    "ingestion-errors": (scenario_ingestion_errors,
                         "Malformed payloads → IngestionErrorRateHigh [~3 min]"),
    "ingestion-silent": (scenario_ingestion_silent,
                         "All devices off → IngestionSilent [~10 min]"),
    "buffer-flood":     (scenario_buffer_flood,
                         "Payload flood → IngestionBufferNearFull [<1 min]"),
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Alert scenario simulator for SensorHub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--scenario", "-s",
                        choices=list(SCENARIOS.keys()) + ["list"],
                        default="list",
                        metavar="SCENARIO",
                        help="Scenario to run. 'list' to see all of them.")
    parser.add_argument("--broker", "-b", default=DEFAULT_BROKER)
    parser.add_argument("--port",   "-p", type=int, default=DEFAULT_PORT)
    parser.add_argument("--topic",  "-t", default=DEFAULT_TOPIC)
    args = parser.parse_args()

    if args.scenario == "list":
        print("\nAvailable scenarios:\n")
        max_len = max(len(k) for k in SCENARIOS)
        for name, (_, desc) in SCENARIOS.items():
            marker = " ★" if name == "all-concurrent" else ""
            print(f"  {name:<{max_len}}  {desc}{marker}")
        print("\nUsage: python scripts/alert_simulator.py --scenario <name>")
        print("\nSerials used (don't collide with the seed):")
        print(f"  Gateways: {', '.join(GW_SERIALS)}")
        print(f"  Devices:  {', '.join(DEV_SERIALS)}")
        print()
        return

    sim = SimClient(args.broker, args.port, args.topic)
    sim.connect()

    try:
        if args.scenario == "all-concurrent":
            scenario_all_concurrent(sim)
        else:
            fn, _ = SCENARIOS[args.scenario]
            fn(sim)
    finally:
        sim.disconnect()
        print("  Simulator disconnected.")


if __name__ == "__main__":
    main()
