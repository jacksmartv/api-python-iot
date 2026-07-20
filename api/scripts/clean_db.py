#!/usr/bin/env python3
"""
Cleans the SensorHub database to start from scratch.

Usage:
    python scripts/clean_db.py           # clears data, keeps users
    python scripts/clean_db.py --users   # also deletes non-admin users
    python scripts/clean_db.py --all     # deletes absolutely everything
"""

import argparse
import asyncio
import os

import asyncpg

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://app_user:app_password@localhost:5432/app_db",
).replace("+asyncpg", "")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Cleans the SensorHub database")
    parser.add_argument("--users", action="store_true", help="Also delete non-admin users")
    parser.add_argument("--all", dest="all_data", action="store_true",
                        help="Delete everything, including admin")
    args = parser.parse_args()

    print(f"\n{'='*50}")
    print("  SensorHub — Database cleanup")
    print(f"{'='*50}\n")

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Order that respects foreign keys (children before parents).
        # spatial.* references core.device/gateway → goes first.
        steps = [
            # spatial (asset references device; the rest reference building/floor/asset)
            ("spatial.asset_position_history",  "DELETE FROM spatial.asset_position_history"),
            ("spatial.asset_telemetry_snapshot","DELETE FROM spatial.asset_telemetry_snapshot"),
            ("spatial.asset_layer",             "DELETE FROM spatial.asset_layer"),
            ("spatial.asset",                   "DELETE FROM spatial.asset"),
            ("spatial.layer",                   "DELETE FROM spatial.layer"),
            ("spatial.floor",                   "DELETE FROM spatial.floor"),
            ("spatial.building",                "DELETE FROM spatial.building"),
            # monitoring + telemetry + raw (reference device/sensor/gateway)
            ("monitoring.event",         "DELETE FROM monitoring.event"),
            ("monitoring.incident",      "DELETE FROM monitoring.incident"),
            ("monitoring.device_runtime","DELETE FROM monitoring.device_runtime"),
            ("monitoring.device_status", "DELETE FROM monitoring.device_status"),
            ("monitoring.gateway_config","DELETE FROM monitoring.gateway_config"),
            ("monitoring.gateway_status","DELETE FROM monitoring.gateway_status"),
            ("telemetry.measurement",    "DELETE FROM telemetry.measurement"),
            ("raw.telemetry_payload",    "DELETE FROM raw.telemetry_payload"),
            # core (parents last)
            ("core.sensor",              "DELETE FROM core.sensor"),
            ("core.device",             "DELETE FROM core.device"),
            ("core.gateway",            "DELETE FROM core.gateway"),
        ]

        for label, sql in steps:
            result = await conn.execute(sql)
            count = int(result.split()[-1])
            print(f"  {count:>6} rows  ← {label}")

        if args.all_data:
            result = await conn.execute("DELETE FROM core.user")
            count = int(result.split()[-1])
            print(f"  {count:>6} rows  ← core.user (all)")
        elif args.users:
            result = await conn.execute("DELETE FROM core.user WHERE role != 'admin'")
            count = int(result.split()[-1])
            print(f"  {count:>6} rows  ← core.user (non-admin)")

        print(f"\n{'='*50}")
        print("  Database clean")
        print(f"{'='*50}\n")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
