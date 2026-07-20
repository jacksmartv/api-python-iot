#!/usr/bin/env python3
"""
LoRa Gateway v1.0.0 Simulator — replicates the real LoRa gateway's MQTT uplinks.

Publishes to the real Gateway LoRa v1.0.0 topics (different from the legacy gatewayv3
simulated by hm_simulator.py):

  gateway/{IMEI}/rx          — HM v3.6 frame from a node (34 B, hex in "raw") + JSON wrapper
  gateway/{IMEI}/telemetry   — gateway health: GPS, wifi_rssi, uptime, heap, MCU temp
  gateway/{IMEI}/status      — presence: online on connect

It also subscribes to gateway/{IMEI}/cmd and responds to `ota` commands (firmware deploy)
with a simulated ack on gateway/{IMEI}/cmd/ack after a delay (--ota-delay, default 3s) —
simulating the real download+flash time. By default the ack is
{"ok":true}; with --ota-fail it simulates a real reflash failure ({"ok":false,"error":...}). This
is used to test the full Phase 2 flow (UI → POST deploy → MQTT → simulator → ack → DB state)
without publishing acks by hand with mosquitto_pub, and without touching any real physical gateway.

Reuses crc16_ccitt() from the real parser (src.services.payload_parser) so frames are ALWAYS
valid and get decoded by the backend the same way the real firmware's would.

Covers all the features from recent sprints:
  - HM v3.6 ingestion (temp + humidity ADC)
  - Extended health + gateway GPS (WebUI New Data sprint)
  - /status presence
  - Node alarm events (--alarm) → sensor.humidity_alarm / sensor.low_battery
  - Robustness: SD replay (--replay, dedup), invalid frames (--bad-crc, --no-ts)
  - Fleet: --gateways N --nodes M with distinct GPS

Usage:
    # Basic fleet: 1 gateway, 3 nodes, valid frames every 5s
    python scripts/lora_simulator.py

    # From the container
    docker compose exec api python scripts/lora_simulator.py --broker mqtt

    # Fleet: 3 gateways (distinct GPS), 5 nodes each
    python scripts/lora_simulator.py --gateways 3 --nodes 5

    # Trigger a humidity alarm on the 1st node (generates event + cleared)
    python scripts/lora_simulator.py --alarm humidity

    # Trigger low battery
    python scripts/lora_simulator.py --alarm low-batt

    # Robustness: resend each frame 2x (simulates SD replay → dedup, 1 measurement)
    python scripts/lora_simulator.py --replay

    # Robustness: frames with invalid CRC (should be discarded)
    python scripts/lora_simulator.py --bad-crc

    # Robustness: frames without ts_ms (should be discarded, not duplicated with now())
    python scripts/lora_simulator.py --no-ts

    # "Fake" gateway with a fixed IMEI to test the OTA firmware deploy (Phase 2) without
    # touching real hardware. It registers itself only (status+telemetry), and responds to
    # the ota command with a simulated ack after 3s. Use that IMEI as {serial} when deploying
    # from the UI/curl.
    python scripts/lora_simulator.py --imei GW-TEST-FW-01 --nodes 0

    # Same case, but simulating that the reflash fails (ack ok:false)
    python scripts/lora_simulator.py --imei GW-TEST-FW-01 --nodes 0 --ota-fail

    # Against the dev broker on AWS (once deployed), to test there too
    python scripts/lora_simulator.py --imei GW-TEST-FW-01 --nodes 0 \\
        --broker <broker-host> --username sensorhub_api_local --password ...
"""

import argparse
import json
import os
import random
import signal
import struct
import sys
import threading
import time

import paho.mqtt.client as mqtt

# Reuse the parser's real CRC (guarantees frames the backend accepts just like the firmware).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.services.payload_parser import crc16_ccitt  # noqa: E402

DEFAULT_BROKER = "localhost"
DEFAULT_PORT = 1883
DEFAULT_GATEWAYS = 1
DEFAULT_NODES = 3
DEFAULT_INTERVAL = 5.0
DEFAULT_IMEI = "869951036951675"

# Base coordinates (Montevideo, like the real gateway) — each gateway spreads out a bit.
_GPS_BASE_LAT = -34.8349
_GPS_BASE_LON = -56.1921

# msg_type / flags (spec §3.5 / §3.6)
MSG_TELEMETRY = 0x01
MSG_ALARM = 0x05
FLAG_NONE = 0x00
FLAG_ALARM = 0x02
FLAG_LOW_BATT = 0x04

# struct ProtocolFrame HM v3.6 (34 bytes, packed) — same layout as decode_hm_v36.
# <2s BBB B B 8s H B H h H H H H B B> =
#   type, ver_major, ver_minor, ver_patch, msg_type, flags, uid, seq, payload_len,
#   vcc, temp, humidity, humAlarm, measureCycles, sendCycles, pending, freeSlots
_FRAME_FMT = "<2sBBBBB8sHBHhHHHHBB"


def _random_imei() -> str:
    return str(860000000000000 + random.randint(0, 999999999))


def _random_uid_hex() -> str:
    """8-byte UID (DS18B20-style 28...) as 16 hex chars."""
    return "28" + os.urandom(7).hex().upper()


def build_hm_v36_frame(
    uid_hex: str,
    seq: int,
    vcc_mv: int,
    temp_c: float,
    humidity_adc: int,
    hum_alarm: int = 700,
    msg_type: int = MSG_TELEMETRY,
    flags: int = FLAG_NONE,
    bad_crc: bool = False,
) -> str:
    """HM v3.6 frame (34 B) as a hex string (valid CRC unless bad_crc)."""
    uid = bytes.fromhex(uid_hex)
    body = struct.pack(
        _FRAME_FMT,
        b"HM", 1, 1, 1, msg_type, flags, uid,
        seq & 0xFFFF, 14,
        vcc_mv & 0xFFFF, int(temp_c * 100), humidity_adc & 0xFFFF, hum_alarm & 0xFFFF,
        1, 2, 0, 15,
    )
    crc = crc16_ccitt(body)
    if bad_crc:
        crc ^= 0xFFFF  # corrupt the CRC on purpose
    return (body + struct.pack("<H", crc)).hex().upper()


class NodeSim:
    """A LoRa HM node: temp/humidity that drift smoothly; controllable alarm state."""

    def __init__(self, uid_hex: str):
        self.uid = uid_hex
        self.seq = random.randint(0, 1000)
        self.base_temp = random.uniform(10.0, 30.0)
        self.base_hum_adc = random.randint(300, 600)  # ADC 0-1023
        self.hum_alarm = 700
        # externally forced alarm state (None = normal)
        self.force_alarm = False
        self.force_low_batt = False

    def next_frame(self, bad_crc: bool = False, no_temp_drift: bool = False) -> tuple[str, dict]:
        self.seq = (self.seq + 1) & 0xFFFF
        temp = max(-10.0, min(60.0, self.base_temp + random.gauss(0, 1.0)))
        vcc = random.randint(2900, 3600)
        flags = FLAG_NONE
        msg_type = MSG_TELEMETRY
        hum_adc = max(0, min(1023, self.base_hum_adc + random.randint(-20, 20)))

        if self.force_alarm:
            hum_adc = self.hum_alarm + 150  # crosses the threshold
            flags |= FLAG_ALARM
            msg_type = MSG_ALARM
        if self.force_low_batt:
            vcc = 2980  # < 3300 → FLAG_LOW_BATT
            flags |= FLAG_LOW_BATT

        raw = build_hm_v36_frame(
            self.uid, self.seq, vcc, temp, hum_adc,
            hum_alarm=self.hum_alarm, msg_type=msg_type, flags=flags, bad_crc=bad_crc,
        )
        meta = {"temp": temp, "hum_adc": hum_adc, "vcc": vcc, "flags": flags, "msg_type": msg_type}
        return raw, meta


class LoRaGatewayWorker(threading.Thread):
    """Simulates a LoRa Gateway v1.0.0: publishes rx (nodes) + telemetry (health/GPS) + status,
    and responds to ota commands received on gateway/{IMEI}/cmd with a simulated ack."""

    def __init__(self, broker, port, imei, num_nodes, interval, username, password,
                 idx, replay, bad_crc, no_ts, ota_fail, ota_delay):
        super().__init__(daemon=True)
        self.imei = imei
        self.interval = interval
        self.replay = replay
        self.bad_crc = bad_crc
        self.no_ts = no_ts
        self.ota_fail = ota_fail
        self.ota_delay = ota_delay
        self.running = False
        self._cycle = 0
        self._gw_seq = random.randint(0, 5000)
        self._uptime = random.randint(1000, 100000)
        # Distinct GPS per gateway (spread ~100m per index)
        self.lat = round(_GPS_BASE_LAT + idx * 0.001, 6)
        self.lon = round(_GPS_BASE_LON + idx * 0.001, 6)
        self.nodes = [NodeSim(_random_uid_hex()) for _ in range(num_nodes)]

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if username:
            self.client.username_pw_set(username, password)
        self.client.on_message = self._on_cmd
        self.client.connect(broker, port, keepalive=60)
        self.client.subscribe(f"gateway/{self.imei}/cmd")
        self.client.loop_start()

    def _on_cmd(self, _client, _userdata, msg):
        """Callback for gateway/{IMEI}/cmd — currently only processes action=='ota' (Phase 2
        firmware OTA). The request_id (the command's "id" field) is echoed back verbatim in
        the ack, same as the real firmware does with gw_get."""
        try:
            cmd = json.loads(msg.payload.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            return
        if cmd.get("action") != "ota":
            return
        request_id = cmd.get("id")
        params = cmd.get("params", {})
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] CMD    {self.imei}  ota received  version={params.get('version')}  "
              f"id={request_id}  → simulating {'FAILURE' if self.ota_fail else 'download+flash'} "
              f"({self.ota_delay}s)")
        threading.Timer(self.ota_delay, self._send_ota_ack, args=(request_id,)).start()

    def _send_ota_ack(self, request_id):
        topic = f"gateway/{self.imei}/cmd/ack"
        if self.ota_fail:
            ack = {"ok": False, "action": "ota", "id": request_id, "error": "flash_failed"}
        else:
            ack = {"ok": True, "action": "ota", "id": request_id, "ts_ms": self._now_ms()}
        self.client.publish(topic, json.dumps(ack), qos=1)
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] ACK    {self.imei}  ota {'FAILED' if self.ota_fail else 'success'}  "
              f"id={request_id}  → {topic}")

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _publish_status(self, ts: str):
        topic = f"gateway/{self.imei}/status"
        payload = {"gw": self.imei, "state": "online", "freq": 434000000}
        self.client.publish(topic, json.dumps(payload), qos=0)
        print(f"[{ts}] STATUS {self.imei}  online  → {topic}")

    def _publish_telemetry(self, ts: str):
        topic = f"gateway/{self.imei}/telemetry"
        self._uptime += int(self.interval)
        payload = {
            "gw": self.imei, "type": "telemetry", "fw": "1.0.0",
            "ts_ms": self._now_ms(), "uptime_sec": self._uptime,
            "heap_free": random.randint(80000, 90000),
            "wifi_rssi": random.randint(-70, -45),
            "voltage_mv": random.randint(500, 650),
            "temp_c": round(random.uniform(20, 30), 1),
            "lora_freq": 434000000, "telemetry_interval_sec": 300,
            "gps_fix": True, "gps_lat": self.lat, "gps_lon": self.lon,
            "gps_alt_m": round(random.uniform(20, 25), 1),
            "gps_hdop": round(random.uniform(0.5, 1.2), 1),
            "gps_sats": random.randint(15, 24),
        }
        self.client.publish(topic, json.dumps(payload), qos=0)
        print(f"[{ts}] TELEM  {self.imei}  gps={self.lat},{self.lon} "
              f"wifi={payload['wifi_rssi']}dBm  → {topic}")

    def _publish_rx(self, node: NodeSim, ts: str):
        topic = f"gateway/{self.imei}/rx"
        raw, meta = node.next_frame(bad_crc=self.bad_crc)
        self._gw_seq = (self._gw_seq + 1) & 0xFFFFFFFF
        wrapper = {
            "gw": self.imei, "seq": self._gw_seq,
            "rssi": random.randint(-90, -40), "snr": random.randint(5, 12),
            "freq": 434000000, "raw": raw,
        }
        if not self.no_ts:
            wrapper["ts_ms"] = self._now_ms()

        reps = 2 if self.replay else 1
        for i in range(reps):
            w = dict(wrapper)
            if i > 0:
                w["mqtt_sent"] = True  # simulates the extra field from SD replay
            self.client.publish(topic, json.dumps(w), qos=0)

        alarm_tag = ""
        if meta["flags"] & FLAG_ALARM:
            alarm_tag = " [ALARM]"
        if meta["flags"] & FLAG_LOW_BATT:
            alarm_tag += " [LOW_BATT]"
        extra = ""
        if self.replay:
            extra += " (x2 replay)"
        if self.bad_crc:
            extra += " (bad-crc)"
        if self.no_ts:
            extra += " (no-ts)"
        print(f"[{ts}] RX     {self.imei} → {node.uid}  t={meta['temp']:.1f}°C "
              f"hum_adc={meta['hum_adc']} vcc={meta['vcc']}mV{alarm_tag}{extra}")

    def run(self):
        self.running = True
        self._publish_status(time.strftime("%H:%M:%S"))  # presence on connect
        while self.running:
            self._cycle += 1
            ts = time.strftime("%H:%M:%S")
            # gateway telemetry every 6 cycles (like the real one every ~300s)
            if self._cycle == 1 or self._cycle % 6 == 0:
                self._publish_telemetry(ts)
            for node in self.nodes:
                self._publish_rx(node, ts)
            time.sleep(self.interval)

    def stop(self):
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()


class LoRaSimulator:
    def __init__(self, args):
        self.args = args
        if args.imei:
            imeis = [args.imei]
        elif args.gateways == 1:
            imeis = [DEFAULT_IMEI]
        else:
            imeis = [_random_imei() for _ in range(args.gateways)]

        self.workers = [
            LoRaGatewayWorker(
                broker=args.broker, port=args.port, imei=imei, num_nodes=args.nodes,
                interval=args.interval, username=args.username, password=args.password,
                idx=i, replay=args.replay, bad_crc=args.bad_crc, no_ts=args.no_ts,
                ota_fail=args.ota_fail, ota_delay=args.ota_delay,
            )
            for i, imei in enumerate(imeis)
        ]

        # Alarm scenario: force it on the 1st node of the 1st gateway, clear it after N cycles.
        self._alarm = args.alarm
        if self._alarm and self.workers:
            node = self.workers[0].nodes[0]
            if self._alarm == "humidity":
                node.force_alarm = True
            elif self._alarm == "low-batt":
                node.force_low_batt = True
            self._alarm_node = node
            self._alarm_clear_after = args.alarm_clear_after
        else:
            self._alarm_node = None

    def start(self):
        total = sum(len(w.nodes) for w in self.workers)
        print(f"\n{'='*70}")
        print("  SensorHub — LoRa Gateway v1.0.0 Simulator")
        print(f"{'='*70}")
        print(f"  Gateways: {len(self.workers)}  ·  Nodes: {total}")
        print("  Topics:  gateway/{IMEI}/rx · /telemetry · /status")
        mods = []
        if self.args.alarm:
            mods.append(
                f"alarm={self.args.alarm} (clears after {self.args.alarm_clear_after} cycles)"
            )
        if self.args.replay:
            mods.append("replay x2")
        if self.args.bad_crc:
            mods.append("bad-crc")
        if self.args.no_ts:
            mods.append("no-ts")
        if mods:
            print(f"  Modes:   {', '.join(mods)}")
        for w in self.workers:
            print(f"  GW {w.imei}  gps={w.lat},{w.lon}  ({len(w.nodes)} nodes)")
        print(f"{'='*70}\n  Ctrl+C to stop\n")

        for w in self.workers:
            w.start()

        try:
            cycle = 0
            while True:
                time.sleep(1)
                cycle += 1
                # clear the alarm after N*interval seconds → triggers the _cleared event
                if (self._alarm_node is not None
                        and cycle == int(self._alarm_clear_after * self.args.interval)):
                    self._alarm_node.force_alarm = False
                    self._alarm_node.force_low_batt = False
                    print(f"\n  >>> Alarm cleared on {self._alarm_node.uid} "
                          f"(should emit *_cleared)\n")
        except KeyboardInterrupt:
            print("\n  Stopping...")
        finally:
            self.stop()

    def stop(self):
        for w in self.workers:
            w.stop()
        print("  Simulator stopped")


def main():
    p = argparse.ArgumentParser(
        description="LoRa Gateway v1.0.0 Simulator (rx HM v3.6 / telemetry GPS / status / alarms)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--broker", "-b", default=DEFAULT_BROKER)
    p.add_argument("--port", "-p", type=int, default=DEFAULT_PORT)
    p.add_argument("--imei", default=None, help=f"Fixed IMEI (default: {DEFAULT_IMEI})")
    p.add_argument("--gateways", "-G", type=int, default=DEFAULT_GATEWAYS)
    p.add_argument("--nodes", "-n", type=int, default=DEFAULT_NODES, help="HM nodes per gateway")
    p.add_argument("--interval", "-i", type=float, default=DEFAULT_INTERVAL)
    p.add_argument("--alarm", choices=["humidity", "low-batt"], default=None,
                   help="Force an alarm on the 1st node (generates an event) and then clear it")
    p.add_argument("--alarm-clear-after", type=int, default=4, metavar="N",
                   help="Clear the alarm after N cycles (default 4) → emits *_cleared")
    p.add_argument("--replay", action="store_true",
                   help="Publish each frame 2x (2nd with mqtt_sent:true) → tests SD dedup")
    p.add_argument("--bad-crc", action="store_true", help="Frames with invalid CRC (→ discarded)")
    p.add_argument("--no-ts", action="store_true",
                   help="Frames without ts_ms (→ discarded by HMv36_timestamp)")
    p.add_argument("--ota-fail", action="store_true",
                   help="Simulate a reflash failure when receiving an ota command (ack ok:false)")
    p.add_argument("--ota-delay", type=float, default=3.0, metavar="SEC",
                   help="Delay before responding to the ota ack, simulates download+flash "
                        "(default 3s)")
    p.add_argument("--username", default=None)
    p.add_argument("--password", default=None)
    args = p.parse_args()

    try:
        sim = LoRaSimulator(args)
    except Exception as e:
        print(f"  Error connecting to broker: {e}")
        print("  Verify broker:  docker compose up -d mqtt")
        sys.exit(1)

    signal.signal(signal.SIGINT, lambda s, f: sim.stop() or sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sim.stop() or sys.exit(0))
    sim.start()


if __name__ == "__main__":
    main()
