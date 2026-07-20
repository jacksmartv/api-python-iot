"""
Automatic migration script.

Runs SQL migrations in order if they haven't been applied yet.
Uses a control table to track which migrations have already been applied.
"""

import asyncio
import logging
from pathlib import Path

import asyncpg

from .config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"


def get_asyncpg_url() -> str:
    """Convert the SQLAlchemy URL to asyncpg format."""
    url = settings.migration_database_url or settings.database_url
    return url.replace("postgresql+asyncpg://", "postgresql://")


async def ensure_migrations_table(conn: asyncpg.Connection) -> None:
    """Create the migrations control table if it doesn't exist."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)


async def get_applied_migrations(conn: asyncpg.Connection) -> set[str]:
    """Get the migrations that have already been applied."""
    rows = await conn.fetch("SELECT version FROM public.schema_migrations")
    return {row["version"] for row in rows}


async def apply_migration(conn: asyncpg.Connection, migration_file: Path) -> None:
    """Apply a migration."""
    version = migration_file.stem  # e.g., "001_init"
    sql = migration_file.read_text()

    logger.info(f"Applying migration: {version}")

    async with conn.transaction():
        await conn.execute(sql)
        await conn.execute(
            "INSERT INTO public.schema_migrations (version) VALUES ($1)",
            version,
        )

    logger.info(f"Migration {version} applied successfully")


async def run_migrations() -> None:
    """Run all pending migrations."""
    logger.info("Starting migrations...")

    # Connect to the database
    conn = await asyncpg.connect(get_asyncpg_url())

    try:
        # Ensure the migrations table exists
        await ensure_migrations_table(conn)

        # Get applied migrations
        applied = await get_applied_migrations(conn)
        logger.info(f"Applied migrations: {applied or 'none'}")

        # Get sorted migration files
        migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

        if not migration_files:
            logger.warning(f"No migration files found in {MIGRATIONS_DIR}")
            return

        # Apply pending migrations
        pending_count = 0
        for migration_file in migration_files:
            version = migration_file.stem
            if version not in applied:
                await apply_migration(conn, migration_file)
                pending_count += 1

        if pending_count == 0:
            logger.info("No pending migrations")
        else:
            logger.info(f"Applied {pending_count} migration(s)")

    finally:
        await conn.close()


def main() -> None:
    """Entry point to run migrations."""
    asyncio.run(run_migrations())


if __name__ == "__main__":
    main()
