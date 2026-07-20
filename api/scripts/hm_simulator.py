#!/usr/bin/env python3
"""
gatewayv3 HM simulator — replicates the actual behavior of the gatewayv3 firmware.

Publishes to the real topics observed on the AWS broker:

  telemetry/{IMEI}/data              — gateway heartbeat (JSON)
  telemetry/{IMEI}/HM/{MAC}/data     — relayed BLE sensor (JSON with hex "d" field)
  telemetry/{IMEI}/GW/activeConfig   — active config (binary 416 bytes, optional)

This replicates exactly what the gatewayv3 firmware does, including:
  - Standard HM packets (22 bytes, 44 hex chars)
  - Extended HM packets (28 bytes, 56 hex chars) — same structure + 6 extra bytes
  - Binary config decodable with decode_gw_config()

Usage:
    # 1 gateway (fake IMEI), 3 BLE devices, every 5s
    python scripts/hm_simulator.py

    # From the Docker container
    docker compose exec api python scripts/hm_simulator.py --broker mqtt

    # Fixed IMEI (to reproduce specific sessions)
    python scripts/hm_simulator.py --imei 869951036943482 --devices 4

    # Multiple gateways with random IMEIs
    python scripts/hm_simulator.py --gateways 3 --devices 4

    # With extended packets (28 bytes) interleaved
    python scripts/hm_simulator.py --extended

    # Publish binary config on start and every N cycles
    python scripts/hm_simulator.py --with-config --config-interval 10
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

DEFAULT_BROKER = "localhost"
DEFAULT_PORT = 1883
DEFAULT_GATEWAYS = 1
DEFAULT_DEVICES = 3
DEFAULT_INTERVAL = 5.0
DEFAULT_IMEI = "869951036943482"

# Base IMEI for randomly generated gateways
_IMEI_BASE = 860000000000000


def _random_imei() -> str:
    return str(_IMEI_BASE + random.randint(0, 999999999))


def _random_mac_8() -> str:
    """Generates an 8-byte MAC in XX:XX:XX:XX:XX:XX:XX:XX format."""
    raw = os.urandom(8)
    return ":".join(f"{b:02X}" for b in raw)


def _mac_to_hex(mac: str) -> str:
    """Converts 'AA:BB:CC:DD:EE:FF:00:11' → 'aabbccddeeff0011'."""
    return mac.replace(":", "").lower()


def _build_hm_packet(
    mac: str,
    seq: int,
    temp_c: float,
    hum_pct: float,
    vcc_mv: int,
    pending: int = 0,
    free: int = 15,
    crc: int = 0x0000,
    extended: bool = False,
) -> str:
    """Builds an HM packet and returns it as a hex string.

    Standard:  22 bytes → 44 hex chars
    Extended:  28 bytes → 56 hex chars  (6 extra bytes: <HHH> = 10, 15, 40251)
    """
    uid_bytes = bytes.fromhex(_mac_to_hex(mac))
    data = struct.pack(
        "<2s8sHHhHBBH",
        b"HM",
        uid_bytes,
        seq & 0xFFFF,
        vcc_mv & 0xFFFF,
        int(temp_c * 100) & 0xFFFF,
        int(hum_pct * 10) & 0xFFFF,
        pending & 0xFF,
        free & 0xFF,
        crc & 0xFFFF,
    )
    if extended:
        data += struct.pack("<HHH", 10, 15, 40251)
    return data.hex().upper()


def _build_gw_config(
    imei: str,
    broker: str = "localhost",
    port: int = 1883,
    interval_s: int = 60,
    supply_mv: int = 3900,
    broker2: str = "",
) -> bytes:
    """Builds the 416-byte binary config struct for gatewayv3."""
    data = bytearray(416)
    # broker [0:40]
    b = broker.encode("ascii")[:40]
    data[0:len(b)] = b
    # port [40:42]
    struct.pack_into("<H", data, 40, port)
    # client_id [42:62] — use the last 8 digits of the IMEI
    cid = f"gw{imei[-8:]}".encode("ascii")[:20]
    data[42:42 + len(cid)] = cid
    # fw_type [62:82]
    data[62:71] = b"gatewayv3"
    # topic_prefix [82:102]
    data[82:91] = b"telemetry"
    # interval_s [110:112]
    struct.pack_into("<H", data, 110, interval_s)
    # supply_mv [112:114]
    struct.pack_into("<H", data, 112, supply_mv)
    # broker2 [114:178]
    if broker2:
        b2 = broker2.encode("ascii")[:64]
        data[114:114 + len(b2)] = b2
    # crc [414:416] — placeholder, not validated
    struct.pack_into("<H", data, 414, 0xFED8)
    return bytes(data)


class HMDeviceSimulator:
    """Simulates a BLE sensor relayed by the gateway."""

    def __init__(self, mac: str, extended: bool = False):
        self.mac = mac
        self.extended = extended
        self.seq = random.randint(0, 1000)
        self.base_temp = random.uniform(15.0, 30.0)
        self.base_hum = random.uniform(35.0, 80.0)

    def generate(self) -> tuple[str, float, float, int, int]:
        """Returns (hex_packet, temp_c, hum_pct, vcc_mv, rssi)."""
        self.seq = (self.seq + 1) & 0xFFFF
        temp = max(-10.0, min(60.0, self.base_temp + random.gauss(0, 1.5)))
        hum = max(0.0, min(99.9, self.base_hum + random.gauss(0, 2.0)))
        vcc = random.randint(2900, 3500)
        rssi = random.randint(-90, -30)
        hex_str = _build_hm_packet(
            mac=self.mac,
            seq=self.seq,
            temp_c=temp,
            hum_pct=hum,
            vcc_mv=vcc,
            extended=self.extended,
        )
        return hex_str, temp, hum, vcc, rssi


class GatewayV3Worker(threading.Thread):
    """Thread that simulates a gatewayv3 gateway with the firmware's real topics."""

    def __init__(
        self,
        broker: str,
        port: int,
        imei: str,
        num_devices: int,
        interval: float,
        username: str | None,
        password: str | None,
        extended: bool = False,
        with_config: bool = False,
        config_interval: int = 0,
        mqtt_broker_for_config: str = "localhost",
    ):
        super().__init__(daemon=True)
        self.imei = imei
        self.interval = interval
        self.with_config = with_config
        self.config_interval = config_interval
        self.mqtt_broker_for_config = mqtt_broker_for_config
        self.running = False
        self._cycle = 0
        self._base_csq = random.randint(10, 25)
        self._base_vgw = random.randint(11800, 13200)

        # BLE devices — some with extended packets if --extended
        self.devices = []
        for i in range(num_devices):
            mac = _random_mac_8()
            use_extended = extended and (i % 3 == 0)  # 1 in 3 uses extended format
            self.devices.append(HMDeviceSimulator(mac, extended=use_extended))

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if username:
            self.client.username_pw_set(username, password)
        self.client.connect(broker, port, keepalive=60)
        self.client.loop_start()

    def _publish_gw_heartbeat(self, ts: str):
        """Publishes heartbeat on telemetry/{IMEI}/data."""
        topic = f"telemetry/{self.imei}/data"
        payload = {
            "TSgw": int(time.time()),
            "csq": max(0, min(31, self._base_csq + random.randint(-2, 2))),
            "erf": 0,
            "rst": 0,
            "vgw": self._base_vgw + random.randint(-150, 150),
        }
        result = self.client.publish(topic, json.dumps(payload), qos=1)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(
                f"[{ts}] GW  {self.imei}"
                f"  csq={payload['csq']:02d} vgw={payload['vgw']}mV"
                f"  → {topic}"
            )

    def _publish_hm_sensor(self, dev: HMDeviceSimulator, ts: str):
        """Publishes a BLE sensor on telemetry/{IMEI}/HM/{MAC}/data."""
        topic = f"telemetry/{self.imei}/HM/{dev.mac}/data"
        hex_str, temp, hum, vcc, rssi = dev.generate()
        payload = {"ts": int(time.time()), "rssi": rssi, "d": hex_str}
        result = self.client.publish(topic, json.dumps(payload), qos=1)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            ext_tag = "[ext]" if dev.extended else "     "
            print(
                f"[{ts}] HM {ext_tag} {self.imei} → {dev.mac}"
                f"  t={temp:.1f}°C h={hum:.1f}%"
                f" vcc={vcc}mV rssi={rssi}dBm #{dev.seq}"
            )
        else:
            print(f"[{ts}] ERROR publishing HM {dev.mac}")

    def _publish_gw_config(self, ts: str):
        """Publishes binary config on telemetry/{IMEI}/GW/activeConfig."""
        topic = f"telemetry/{self.imei}/GW/activeConfig"
        raw = _build_gw_config(
            imei=self.imei,
            broker=self.mqtt_broker_for_config,
            port=1883,
            interval_s=60,
            supply_mv=3900,
        )
        result = self.client.publish(topic, raw, qos=1)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(
                f"[{ts}] CFG {self.imei}"
                f"  broker={self.mqtt_broker_for_config} ({len(raw)}B)"
                f"  → {topic}"
            )
        else:
            print(f"[{ts}] ERROR publishing config {self.imei}")

    def run(self):
        self.running = True
        while self.running:
            self._cycle += 1
            ts = time.strftime("%H:%M:%S")

            # Heartbeat every 6 cycles (same pattern as the real gatewayv3)
            if self._cycle == 1 or self._cycle % 6 == 0:
                self._publish_gw_heartbeat(ts)

            # Binary config at the start and every config_interval cycles
            if self.with_config and (
                self._cycle == 1
                or (self.config_interval > 0 and self._cycle % self.config_interval == 0)
            ):
                self._publish_gw_config(ts)

            # BLE sensors
            for dev in self.devices:
                self._publish_hm_sensor(dev, ts)

            time.sleep(self.interval)

    def stop(self):
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()


class GatewayV3Simulator:

    def __init__(
        self,
        broker: str,
        port: int,
        imeis: list[str],
        devices_per_gw: int,
        interval: float,
        username: str | None = None,
        password: str | None = None,
        extended: bool = False,
        with_config: bool = False,
        config_interval: int = 0,
        mqtt_broker_for_config: str = "localhost",
    ):
        self.workers: list[GatewayV3Worker] = []
        for imei in imeis:
            self.workers.append(GatewayV3Worker(
                broker=broker,
                port=port,
                imei=imei,
                num_devices=devices_per_gw,
                interval=interval,
                username=username,
                password=password,
                extended=extended,
                with_config=with_config,
                config_interval=config_interval,
                mqtt_broker_for_config=mqtt_broker_for_config,
            ))

    def start(self):
        total_devices = sum(len(w.devices) for w in self.workers)
        ext_count = sum(1 for w in self.workers for d in w.devices if d.extended)
        print(f"\n{'='*70}")
        print("  SensorHub — gatewayv3 Simulator (real topics)")
        print(f"{'='*70}")
        print(f"  Gateways:       {len(self.workers)}")
        print(f"  BLE devices:    {total_devices} total ({ext_count} with extended packets)")
        print("  Topics used:")
        print("    telemetry/{IMEI}/data              → gateway heartbeat")
        print("    telemetry/{IMEI}/HM/{MAC}/data    → BLE sensor")
        if any(w.with_config for w in self.workers):
            print("    telemetry/{IMEI}/GW/activeConfig  → binary config 416B")
        print(f"{'='*70}")
        for w in self.workers:
            print(f"  GW IMEI={w.imei}  ({len(w.devices)} devices)")
            for d in w.devices:
                tag = " [ext]" if d.extended else ""
                print(f"    ↳ {d.mac}{tag}")
        print(f"{'='*70}")
        print("  Ctrl+C to stop\n")

        for w in self.workers:
            w.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n  Stopping simulator...")
        finally:
            self.stop()

    def stop(self):
        for w in self.workers:
            w.stop()
        print("  Simulator stopped")


def main():
    parser = argparse.ArgumentParser(
        description="gatewayv3 simulator with real topics for testing SensorHub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 1 gateway (default IMEI), 3 BLE devices, every 5s
  python scripts/hm_simulator.py

  # From the Docker container
  docker compose exec api python scripts/hm_simulator.py --broker mqtt

  # Fixed IMEI (reproduce a real gateway session)
  python scripts/hm_simulator.py --imei 869951036943482 --devices 4

  # 3 gateways with random IMEIs
  python scripts/hm_simulator.py --gateways 3 --devices 4

  # With extended packets (28 bytes) interleaved
  python scripts/hm_simulator.py --extended

  # With binary config on start and every 10 cycles
  python scripts/hm_simulator.py --with-config --config-interval 10

  # All together: real IMEI, extended, config, fast interval
  python scripts/hm_simulator.py --imei 869951036943482 --devices 3 \\
    --extended --with-config --interval 2
        """,
    )
    parser.add_argument("--broker", "-b", default=DEFAULT_BROKER,
                        help=f"MQTT broker host (default: {DEFAULT_BROKER})")
    parser.add_argument("--port", "-p", type=int, default=DEFAULT_PORT,
                        help=f"MQTT port (default: {DEFAULT_PORT})")
    parser.add_argument("--imei", default=None,
                        help=f"Fixed IMEI for a single gateway (default: {DEFAULT_IMEI})")
    parser.add_argument("--gateways", "-G", type=int, default=DEFAULT_GATEWAYS,
                        help=f"Gateways with random IMEIs (default: {DEFAULT_GATEWAYS})")
    parser.add_argument("--devices", "-d", type=int, default=DEFAULT_DEVICES,
                        help=f"BLE devices per gateway (default: {DEFAULT_DEVICES})")
    parser.add_argument("--interval", "-i", type=float, default=DEFAULT_INTERVAL,
                        help=f"Seconds between cycles (default: {DEFAULT_INTERVAL})")
    parser.add_argument("--extended", action="store_true",
                        help="Interleave extended 28-byte packets (1 in 3 devices)")
    parser.add_argument("--with-config", action="store_true",
                        help="Publish binary config (GW/activeConfig) on start")
    parser.add_argument("--config-interval", type=int, default=0, metavar="N",
                        help="Re-publish config every N cycles (0 = only on start)")
    parser.add_argument("--config-broker", default=DEFAULT_BROKER, metavar="HOST",
                        help="Broker to write into the binary config (default: same as --broker)")
    parser.add_argument("--username", default=None, help="MQTT username")
    parser.add_argument("--password", default=None, help="MQTT password")

    args = parser.parse_args()

    if args.imei:
        imeis = [args.imei]
    elif args.gateways == 1:
        imeis = [DEFAULT_IMEI]
    else:
        imeis = [_random_imei() for _ in range(args.gateways)]

    try:
        sim = GatewayV3Simulator(
            broker=args.broker,
            port=args.port,
            imeis=imeis,
            devices_per_gw=args.devices,
            interval=args.interval,
            username=args.username,
            password=args.password,
            extended=args.extended,
            with_config=args.with_config,
            config_interval=args.config_interval,
            mqtt_broker_for_config=args.config_broker,
        )
    except Exception as e:
        print(f"  Error connecting to broker: {e}")
        print("  Verify the MQTT broker is running:")
        print("    docker compose up -d mqtt")
        sys.exit(1)

    signal.signal(signal.SIGINT, lambda s, f: sim.stop() or sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sim.stop() or sys.exit(0))

    sim.start()


if __name__ == "__main__":
    main()
