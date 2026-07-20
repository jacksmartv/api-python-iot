#!/usr/bin/env python3
"""
Development data seed for SensorHub.

Creates gateways, devices, sensors, and historical measurements directly
in the database, simulating an active fleet with data from the last 24h.

Usage:
    python scripts/seed_data.py
    python scripts/seed_data.py --hours 48 --devices 16 --gateways 3
    python scripts/seed_data.py --reset   # clears everything before seeding
"""

import argparse
import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import bcrypt

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://app_user:app_password@localhost:5432/app_db",
).replace("+asyncpg", "")

# Sensor prefixes supported by payload_parser.py
# node_id = node number (TS{id}), sensor_key = prefix
SENSOR_KEYS = ["ti", "ts", "vc", "h"]  # interior temp, surface temp, conductivity, humidity
MAX_SUBSENSORS = 16  # same as in payload_parser.py

# sensor_index = (node_id - 1) * MAX_SUBSENSORS + sub_index
def sensor_index_for(node_id: int, sensor_key: str) -> int:
    sub_index = SENSOR_KEYS.index(sensor_key)
    return (node_id - 1) * MAX_SUBSENSORS + sub_index


async def reset_data(conn: asyncpg.Connection) -> None:
    """Clears all fleet data before seeding (FK order: children → parents)."""
    # spatial.* references core.device → first
    for tbl in (
        "spatial.asset_position_history", "spatial.asset_telemetry_snapshot",
        "spatial.asset_layer", "spatial.asset", "spatial.layer",
        "spatial.floor", "spatial.building",
        "monitoring.event", "monitoring.incident", "monitoring.device_runtime",
        "monitoring.device_status", "monitoring.gateway_config", "monitoring.gateway_status",
        "telemetry.measurement", "raw.telemetry_payload",
        "core.sensor", "core.device", "core.gateway",
    ):
        await conn.execute(f"DELETE FROM {tbl}")
    print("  Database cleared")


async def seed_user(conn: asyncpg.Connection) -> None:
    hashed = bcrypt.hashpw(b"admin123", bcrypt.gensalt()).decode()
    await conn.execute(
        """
        INSERT INTO core.user (email, hashed_password, name, role, is_active)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (email) DO NOTHING
        """,
        "admin@example.com", hashed, "Admin", "admin", True,
    )
    print("  admin@example.com / admin123")


async def seed_gateways(conn: asyncpg.Connection, count: int) -> list[dict]:
    """Creates gateways in core.gateway + history in monitoring.gateway_status."""
    now = datetime.now(timezone.utc)
    gateways = []

    for i in range(count):
        serial = f"GW-{i+1:03d}"
        gw_id = uuid.uuid4()

        await conn.execute(
            """
            INSERT INTO core.gateway (id, serial_number)
            VALUES ($1, $2)
            ON CONFLICT (serial_number) DO UPDATE SET serial_number = EXCLUDED.serial_number
            RETURNING id
            """,
            gw_id, serial,
        )
        # May already exist — fetch the real id
        row = await conn.fetchrow("SELECT id FROM core.gateway WHERE serial_number = $1", serial)
        gw_id = row["id"]

        # Registration event
        await conn.execute(
            """
            INSERT INTO monitoring.event
                (id, occurred_at, event_type, entity_type, entity_id, serial_number)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING
            """,
            uuid.uuid4(),
            now - timedelta(hours=24),
            "gateway.registered",
            "gateway",
            gw_id,
            serial,
        )

        # gateway_status history — every 5 minutes for the last 24h
        base_csq = random.randint(12, 25)
        base_vgw = random.randint(11800, 13200)
        status_rows = []
        for minutes_ago in range(0, 24 * 60, 5):
            ts = now - timedelta(minutes=minutes_ago)
            csq = max(0, min(31, base_csq + random.randint(-3, 3)))
            vgw = base_vgw + random.randint(-200, 200)
            status_rows.append((
                uuid.uuid4(), serial, ts, csq, 0, 0, vgw,
                json.dumps({"seed": True}), gw_id,
            ))

        await conn.executemany(
            """
            INSERT INTO monitoring.gateway_status
                (id, serial_number, ts, csq, erf, rst, vgw, raw_payload, gateway_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (serial_number, ts) DO NOTHING
            """,
            status_rows,
        )

        gateways.append({"id": gw_id, "serial": serial})
        print(f"  {serial} — {len(status_rows)} status records")

    return gateways


async def seed_devices(
    conn: asyncpg.Connection,
    gateways: list[dict],
    devices_per_gateway: int,
    nodes_per_device: int,
) -> list[dict]:
    """Creates devices with their sensors, associated with gateways."""
    now = datetime.now(timezone.utc)
    devices = []
    device_counter = 1

    for gw in gateways:
        for _ in range(devices_per_gateway):
            serial = f"DEV-{device_counter:04d}"
            device_counter += 1
            dev_id = uuid.uuid4()

            await conn.execute(
                """
                INSERT INTO core.device (id, serial_number, gateway_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (serial_number) DO UPDATE SET gateway_id = EXCLUDED.gateway_id
                """,
                dev_id, serial, gw["id"],
            )
            row = await conn.fetchrow("SELECT id FROM core.device WHERE serial_number = $1", serial)
            dev_id = row["id"]

            # Registration event
            await conn.execute(
                """
                INSERT INTO monitoring.event
                (id, occurred_at, event_type, entity_type, entity_id, serial_number)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT DO NOTHING
                """,
                uuid.uuid4(),
                now - timedelta(hours=23, minutes=random.randint(0, 59)),
                "device.registered",
                "device",
                dev_id,
                serial,
            )

            # Sensors: node_id 1..N, each with all SENSOR_KEYS
            sensor_ids_by_node: dict[int, dict[str, uuid.UUID]] = {}
            for node_id in range(1, nodes_per_device + 1):
                sensor_ids_by_node[node_id] = {}
                for sensor_key in SENSOR_KEYS:
                    s_id = uuid.uuid4()
                    sidx = sensor_index_for(node_id, sensor_key)
                    await conn.execute(
                        """
                        INSERT INTO core.sensor
                            (id, device_id, sensor_index, sensor_type, node_id, sensor_key)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (device_id, sensor_index) DO NOTHING
                        """,
                        s_id, dev_id, sidx, sensor_key, node_id, sensor_key,
                    )
                    # Fetch the real id (may have been inserted earlier)
                    row = await conn.fetchrow(
                        "SELECT id FROM core.sensor WHERE device_id=$1 AND sensor_index=$2",
                        dev_id, sidx,
                    )
                    sensor_ids_by_node[node_id][sensor_key] = row["id"]

            devices.append({
                "id": dev_id,
                "serial": serial,
                "gateway_id": gw["id"],
                "gateway_serial": gw["serial"],
                "sensor_ids_by_node": sensor_ids_by_node,
            })

    print(f"  {len(devices)} devices "
          f"({nodes_per_device} nodes × {len(SENSOR_KEYS)} sensors each)")
    return devices


async def seed_measurements(
    conn: asyncpg.Connection,
    devices: list[dict],
    hours: int,
) -> None:
    """Generates realistic measurements for the last N hours (every 5 minutes)."""
    now = datetime.now(timezone.utc)
    total = 0
    batch: list[tuple] = []

    for dev in devices:
        for node_id, sensors in dev["sensor_ids_by_node"].items():
            base_ti = random.uniform(18.0, 32.0)   # interior temperature
            base_ts = base_ti - random.uniform(1.0, 5.0)  # surface always lower
            base_vc = random.uniform(1.0, 2.5)     # conductivity
            base_h  = random.uniform(30.0, 70.0)   # humidity

            for minutes_ago in range(0, hours * 60, 5):
                ts = now - timedelta(minutes=minutes_ago)
                supply_mv = random.randint(3000, 3500)

                vals = {
                    "ti": round(base_ti + random.gauss(0, 1.5), 2),
                    "ts": round(base_ts + random.gauss(0, 1.0), 2),
                    "vc": round(max(0.5, min(3.0, base_vc + random.gauss(0, 0.15))), 3),
                    "h":  round(max(0.0, min(100.0, base_h + random.gauss(0, 2.0))), 1),
                }

                for sensor_key, sensor_id in sensors.items():
                    v = vals[sensor_key]
                    temperature_c = v if sensor_key in ("ti", "ts") else None
                    humidity_pct  = v if sensor_key == "h" else None
                    voltage_cond_v = v if sensor_key == "vc" else None

                    batch.append((
                        sensor_id, ts,
                        temperature_c, humidity_pct, voltage_cond_v,
                        supply_mv, random.randint(1, 65535),
                    ))

                    if len(batch) >= 2000:
                        await conn.executemany(
                            """
                            INSERT INTO telemetry.measurement
                                (sensor_id, ts, temperature_c, humidity_pct,
                                 voltage_cond_v, supply_mv, msg_counter)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT DO NOTHING
                            """,
                            batch,
                        )
                        total += len(batch)
                        batch = []

    if batch:
        await conn.executemany(
            """
            INSERT INTO telemetry.measurement
                (sensor_id, ts, temperature_c, humidity_pct, voltage_cond_v, supply_mv, msg_counter)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING
            """,
            batch,
        )
        total += len(batch)

    print(f"  {total:,} measurements inserted (last {hours}h, every 5 min)")


async def seed_device_status(conn: asyncpg.Connection, devices: list[dict]) -> None:
    """Generates device_status records for the last 24h."""
    now = datetime.now(timezone.utc)
    rows = []
    for dev in devices:
        for hours_ago in range(0, 25):
            rows.append((
                dev["id"],
                now - timedelta(hours=hours_ago),
                random.randint(-90, -30),
                random.randint(0, 50),
                100,
                random.randint(3000, 3500),
            ))

    await conn.executemany(
        """
        INSERT INTO monitoring.device_status
            (device_id, ts, rssi_dbm, buffer_used, buffer_total, supply_mv)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    print(f"  {len(rows)} device_status records")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Development data seed for SensorHub")
    parser.add_argument("--gateways", type=int, default=2, help="Number of gateways (default: 2)")
    parser.add_argument("--devices", type=int, default=4,
                        help="Devices per gateway (default: 4)")
    parser.add_argument("--nodes", type=int, default=2,
                        help="Sensor nodes per device (default: 2)")
    parser.add_argument("--hours", type=int, default=24,
                        help="Hours of measurement history (default: 24)")
    parser.add_argument("--reset", action="store_true",
                        help="Clear all data before seeding")
    args = parser.parse_args()

    total_devices = args.gateways * args.devices
    sensors_per_device = args.nodes * len(SENSOR_KEYS)
    total_sensors = total_devices * sensors_per_device
    measurements_per_sensor = (args.hours * 60) // 5
    total_measurements = total_sensors * measurements_per_sensor

    print(f"\n{'='*55}")
    print("  SensorHub — Development Data Seed")
    print(f"{'='*55}")
    print(f"  Gateways:         {args.gateways}")
    print(f"  Devices:          {total_devices} ({args.devices} per gateway)")
    print(f"  Sensors:          {total_sensors} ({args.nodes} nodes × {len(SENSOR_KEYS)} types)")
    print(f"  Est. measurements: {total_measurements:,} ({args.hours}h × every 5min)")
    print(f"{'='*55}\n")

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if args.reset:
            print("Clearing existing data...")
            await reset_data(conn)

        print("Admin user:")
        await seed_user(conn)

        print("\nGateways:")
        gateways = await seed_gateways(conn, args.gateways)

        print("\nDevices and sensors:")
        devices = await seed_devices(conn, gateways, args.devices, args.nodes)

        print("\nMeasurements:")
        await seed_measurements(conn, devices, args.hours)

        print("\nDevice status:")
        await seed_device_status(conn, devices)

        print(f"\n{'='*55}")
        print("  Seed complete")
        print(f"{'='*55}")
        print("  Login: admin@example.com / admin123")
        print("  WebUI: http://localhost:3001")
        print("  API:   http://localhost:8000/docs")
        print()

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
