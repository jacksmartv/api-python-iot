#!/usr/bin/env python3
"""
MQTT sensor simulator for local development and testing.

Publishes device payloads and gateway heartbeats to the Mosquitto broker,
using the same serials as seed_data.py (GW-001, DEV-0001...) so that the
simulator's data correlates with the seeded historical data.

Usage:
    # Basic simulation — same serials as the default seed
    python scripts/mqtt_simulator.py

    # Large fleet: 3 gateways, 8 devices per gateway, 2 nodes per device
    python scripts/mqtt_simulator.py --gateways 3 --devices 8 --nodes 2

    # Fast intervals for testing (1 second)
    python scripts/mqtt_simulator.py --interval 1

    # Gateways only (no devices), to test heartbeat
    python scripts/mqtt_simulator.py --devices 0

    # With MQTT authentication
    python scripts/mqtt_simulator.py --username user --password pass

    # Remote broker
    python scripts/mqtt_simulator.py --broker 192.168.1.100
"""

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

DEFAULT_BROKER = "localhost"
DEFAULT_PORT = 1883
DEFAULT_GATEWAYS = 2
DEFAULT_DEVICES_PER_GW = 4
DEFAULT_NODES_PER_DEVICE = 2
DEFAULT_INTERVAL = 5.0
DEFAULT_GW_INTERVAL_CYCLES = 6   # gateway heartbeat every N device cycles
DEFAULT_TOPIC = "telemetry/{serial}/data"

# Prefixes supported by payload_parser.py — same order as SENSOR_KEY_REGISTRY
SENSOR_PREFIXES = ["ti", "ts", "vc", "h"]


def _make_ts(now: datetime, unix: bool = False) -> float:
    """Generates the timestamp in the payload's format.

    unix=False → proprietary format YYYYMMDD.HHmmss
    unix=True  → Unix epoch in seconds (float)
    """
    if unix:
        return now.timestamp()
    date_part = now.year * 10000 + now.month * 100 + now.day
    time_part = now.hour * 10000 + now.minute * 100 + now.second
    return round(date_part + time_part / 1_000_000, 6)


class DeviceSimulator:
    """Simulates an IoT device with N sensor nodes."""

    def __init__(self, serial: str, nodes: int, prefixes: list[str], unix_ts: bool = False):
        self.serial = serial
        self.nodes = nodes
        self.prefixes = prefixes
        self.unix_ts = unix_ts  # True → Unix epoch; False → YYYYMMDD.HHmmss
        # Per-node state
        self.base_ti   = [random.uniform(18.0, 32.0) for _ in range(nodes)]
        self.base_ts   = [self.base_ti[i] - random.uniform(1.0, 5.0) for i in range(nodes)]
        self.base_vc   = [random.uniform(1.0, 2.5)   for _ in range(nodes)]
        self.base_h    = [random.uniform(30.0, 70.0)  for _ in range(nodes)]
        self.msg_ctr   = [random.randint(1, 10000)    for _ in range(nodes)]

    def generate_payload(self) -> dict:
        now = datetime.now(timezone.utc)
        ts_val = _make_ts(now, unix=self.unix_ts)
        payload: dict = {"schema_version": 1}

        for i in range(self.nodes):
            nid = i + 1  # node_id is 1-based (TS1, TS2, ...)
            self.msg_ctr[i] += 1

            ti = round(self.base_ti[i] + random.gauss(0, 1.5), 2)
            ts = round(self.base_ts[i] + random.gauss(0, 1.0), 2)
            vc = round(max(0.5, min(3.0, self.base_vc[i] + random.gauss(0, 0.15))), 3)
            h  = round(max(0.0, min(100.0, self.base_h[i] + random.gauss(0, 2.0))), 1)

            payload[f"TS{nid}"] = ts_val
            payload[f"o{nid}"]  = self.msg_ctr[i]
            payload[f"v{nid}"]  = random.randint(3000, 3500)   # supply mV
            payload[f"s{nid}"]  = random.randint(-90, -30)      # RSSI dBm
            payload[f"um{nid}"] = random.randint(0, 50)         # buffer used
            payload[f"tm{nid}"] = 100                           # buffer total

            for prefix in self.prefixes:
                if prefix == "ti":
                    payload[f"ti{nid}"] = ti
                elif prefix == "ts":
                    payload[f"ts{nid}"] = ts
                elif prefix == "vc":
                    payload[f"vc{nid}"] = vc
                elif prefix == "h":
                    payload[f"h{nid}"] = h

        return payload


class GatewaySimulator:
    """Simulates a gateway sending periodic heartbeats."""

    def __init__(self, serial: str, unix_ts: bool = False):
        self.serial = serial
        self.unix_ts = unix_ts  # True → Unix epoch; False → YYYYMMDD.HHmmss
        self.rst = 0
        self.base_csq = random.randint(10, 25)
        self.base_vgw = random.randint(11800, 13200)

    def generate_payload(self) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "TSgw": _make_ts(now, unix=self.unix_ts),
            "csq":  max(0, min(31, self.base_csq + random.randint(-2, 2))),
            "erf":  0,
            "rst":  self.rst,
            "vgw":  self.base_vgw + random.randint(-150, 150),
        }


class MQTTSimulator:

    def __init__(
        self,
        broker: str,
        port: int,
        gateways: int,
        devices_per_gw: int,
        nodes_per_device: int,
        interval: float,
        gw_interval_cycles: int,
        topic: str,
        prefixes: list[str],
        username: str | None = None,
        password: str | None = None,
    ):
        self.broker = broker
        self.port = port
        self.interval = interval
        self.gw_interval_cycles = gw_interval_cycles
        self.topic = topic
        self.running = False
        self._cycle = 0

        # Serials consistent with seed_data.py (GW-001, DEV-0001...)
        # Even gateways (index 0, 2, ...) → proprietary format YYYYMMDD.HHmmss
        # Odd gateways (index 1, 3, ...) → Unix epoch
        # Devices inherit their gateway's format.
        self.gateways = [
            GatewaySimulator(f"GW-{i+1:03d}", unix_ts=(i % 2 == 1)) for i in range(gateways)
        ]
        self.devices: list[DeviceSimulator] = []
        dev_counter = 1
        for gw_idx in range(gateways):
            unix = gw_idx % 2 == 1
            for _ in range(devices_per_gw):
                self.devices.append(
                    DeviceSimulator(
                        f"DEV-{dev_counter:04d}", nodes_per_device, prefixes, unix_ts=unix
                    )
                )
                dev_counter += 1

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if username:
            self.client.username_pw_set(username, password)
        self.client.on_connect    = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc, props):
        if rc == 0:
            print(f"  Connected to broker {self.broker}:{self.port}")
        else:
            print(f"  Connection error: rc={rc}")

    def _on_disconnect(self, client, userdata, flags, rc, props):
        if rc != 0:
            print(f"  Unexpectedly disconnected: rc={rc}")

    def _publish(self, serial: str, payload: dict) -> bool:
        topic = self.topic.format(serial=serial)
        result = self.client.publish(topic, json.dumps(payload), qos=1)
        return result.rc == mqtt.MQTT_ERR_SUCCESS

    def start(self):
        n_sensors = (
            self.devices[0].nodes * len(self.devices[0].prefixes)
            if self.devices else 0
        )
        print(f"\n{'='*60}")
        print("  SensorHub — MQTT Simulator")
        print(f"{'='*60}")
        print(f"  Broker:           {self.broker}:{self.port}")
        print(f"  Gateways:         {len(self.gateways)}")
        print(f"  Devices:          {len(self.devices)}")
        n_nodes = self.devices[0].nodes if self.devices else 0
        n_types = len(self.devices[0].prefixes if self.devices else [])
        print(f"  Sensors/device:   {n_sensors} ({n_nodes} nodes × {n_types} types)")
        print(f"  Sensor types:     {', '.join(self.devices[0].prefixes if self.devices else [])}")
        print(f"  Device interval:  {self.interval}s")
        gw_interval_s = self.gw_interval_cycles * self.interval
        print(f"  GW interval:      every {self.gw_interval_cycles} cycles ({gw_interval_s:.0f}s)")
        print(f"  Topic:            {self.topic}")
        ts_summary = ", ".join(
            f"GW-{i+1:03d}={'unix' if gw.unix_ts else 'YYYYMMDD'}"
            for i, gw in enumerate(self.gateways)
        )
        print(f"  Timestamps:       {ts_summary}")
        print(f"{'='*60}")
        print("  Ctrl+C to stop\n")

        try:
            self.client.connect(self.broker, self.port, keepalive=60)
            self.client.loop_start()
            time.sleep(0.5)  # wait for connection
        except Exception as e:
            print(f"  Connection error: {e}")
            print("  Verify the MQTT broker is running:")
            print("    docker compose up -d mqtt")
            sys.exit(1)

        self.running = True
        msg_count = 0

        try:
            while self.running:
                self._cycle += 1
                ts = datetime.now().strftime("%H:%M:%S")

                # Gateway heartbeat
                if self._cycle % self.gw_interval_cycles == 1:
                    for gw in self.gateways:
                        payload = gw.generate_payload()
                        ok = self._publish(gw.serial, payload)
                        if ok:
                            msg_count += 1
                            print(
                                f"[{ts}] GW  {gw.serial} "
                                f"csq={payload['csq']:02d} vgw={payload['vgw']}mV"
                            )

                # Device telemetry
                for dev in self.devices:
                    payload = dev.generate_payload()
                    ok = self._publish(dev.serial, payload)
                    if ok:
                        msg_count += 1
                        # Show first node's values only, to avoid flooding the console
                        ti1 = payload.get("ti1", "—")
                        h1  = payload.get("h1", "—")
                        print(
                            f"[{ts}] DEV {dev.serial} "
                            f"ti={ti1}°C h={h1}% "
                            f"({dev.nodes}n×{len(dev.prefixes)}t) #{msg_count}"
                        )
                    else:
                        print(f"[{ts}] ERROR publishing {dev.serial}")

                time.sleep(self.interval)

        except KeyboardInterrupt:
            print("\n  Stopping simulator...")
        finally:
            self.stop()

    def stop(self):
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()
        print("  Simulator stopped")


def main():
    parser = argparse.ArgumentParser(
        description="MQTT simulator for local testing of SensorHub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Defaults: 2 gateways, 4 devices/gw, 2 nodes/device, every 5s
  python scripts/mqtt_simulator.py

  # Only interior and surface temperature (faster to read on console)
  python scripts/mqtt_simulator.py --sensor-types ti ts

  # Publish fast to fill up Grafana
  python scripts/mqtt_simulator.py --interval 1 --devices 8

  # Large fleet
  python scripts/mqtt_simulator.py --gateways 3 --devices 10 --nodes 3

  # Remote broker with authentication
  python scripts/mqtt_simulator.py --broker 10.0.0.5 --username dev --password secret
        """,
    )
    parser.add_argument("--broker",  "-b", default=DEFAULT_BROKER)
    parser.add_argument("--port",    "-p", type=int, default=DEFAULT_PORT)
    parser.add_argument("--gateways","-g", type=int, default=DEFAULT_GATEWAYS,
                        help=f"Number of gateways (default: {DEFAULT_GATEWAYS})")
    parser.add_argument("--devices", "-d", type=int, default=DEFAULT_DEVICES_PER_GW,
                        help=f"Devices per gateway (default: {DEFAULT_DEVICES_PER_GW})")
    parser.add_argument("--nodes",   "-n", type=int, default=DEFAULT_NODES_PER_DEVICE,
                        help=f"Sensor nodes per device (default: {DEFAULT_NODES_PER_DEVICE})")
    parser.add_argument("--interval","-i", type=float, default=DEFAULT_INTERVAL,
                        help=f"Seconds between cycles (default: {DEFAULT_INTERVAL})")
    parser.add_argument("--gw-interval", type=int, default=DEFAULT_GW_INTERVAL_CYCLES,
                        help=f"GW heartbeat every N cycles (default: {DEFAULT_GW_INTERVAL_CYCLES})")
    parser.add_argument("--topic",   "-t", default=DEFAULT_TOPIC,
                        help=f"Topic template (default: {DEFAULT_TOPIC})")
    parser.add_argument("--sensor-types", nargs="+", choices=SENSOR_PREFIXES,
                        default=None, metavar="PREFIX",
                        help=f"Sensor types: {' '.join(SENSOR_PREFIXES)} (default: all)")
    parser.add_argument(
        "--username", default=None, help="MQTT username (if the broker requires auth)"
    )
    parser.add_argument("--password", default=None, help="MQTT password")

    args = parser.parse_args()

    sim = MQTTSimulator(
        broker=args.broker,
        port=args.port,
        gateways=args.gateways,
        devices_per_gw=args.devices,
        nodes_per_device=args.nodes,
        interval=args.interval,
        gw_interval_cycles=args.gw_interval,
        topic=args.topic,
        prefixes=args.sensor_types or SENSOR_PREFIXES,
        username=args.username,
        password=args.password,
    )

    signal.signal(signal.SIGINT,  lambda s, f: setattr(sim, "running", False))
    signal.signal(signal.SIGTERM, lambda s, f: setattr(sim, "running", False))

    sim.start()


if __name__ == "__main__":
    main()
