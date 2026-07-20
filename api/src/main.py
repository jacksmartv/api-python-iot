"""
Telemetry ingestion API - FastAPI Application
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pythonjsonlogger import jsonlogger
from sqlalchemy import text

from .config import settings
from .database import engine
from .routes import (
    auth_router,
    calibration_router,
    comparison_router,
    devices_router,
    events_router,
    firmware_deployment_router,
    firmware_router,
    gateways_router,
    internal_metrics_router,
    spatial_router,
    status_router,
    telemetry_router,
)
from .services import ingestion_service
from .services.gap_recovery import run_gap_recovery
from .services.mqtt_consumer import mqtt_consumer
from .services.retention import run_retention_cleanup
from .services.telemetry_snapshot import run_snapshot_refresh


def _configure_logging() -> None:
    """Configures structured JSON logging for all loggers.

    uvicorn also uses this via --log-config, but this guarantees that any
    code running before the server (migrations, startup) also logs JSON.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(
        jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
            rename_fields={"asctime": "timestamp", "levelname": "level", "name": "logger"},
        )
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)


_configure_logging()
logger = logging.getLogger(__name__)


async def _wait_for_db(retries: int = 10, delay: float = 2.0) -> None:
    """Waits until the DB is available before starting services."""
    for i in range(retries):
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            logger.info("Database connection verified")
            return
        except Exception as e:
            if i == retries - 1:
                raise RuntimeError(f"DB unreachable after {retries} attempts") from e
            logger.warning(f"DB not ready ({e}), retrying in {delay}s ({i + 1}/{retries})")
            await asyncio.sleep(delay)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle management to start/stop services."""
    logger.info("Starting application...")
    # Fail-fast: DEFAULT_ORG_ID must be a valid UUID (spatial single-tenant V1).
    try:
        _ = settings.default_org_uuid
    except ValueError as e:
        raise RuntimeError(f"Invalid DEFAULT_ORG_ID: {settings.default_org_id!r}") from e
    await _wait_for_db()
    await ingestion_service.start()
    await mqtt_consumer.start()
    retention_task = asyncio.create_task(run_retention_cleanup())
    snapshot_task = asyncio.create_task(run_snapshot_refresh())
    gap_recovery_task = asyncio.create_task(run_gap_recovery())
    yield
    logger.info("Shutting down application...")
    retention_task.cancel()
    snapshot_task.cancel()
    gap_recovery_task.cancel()
    await mqtt_consumer.stop()
    await ingestion_service.stop()


app = FastAPI(
    title="SensorHub Telemetry API",
    description="Telemetry ingestion API for IoT devices",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware for webapp
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(telemetry_router, prefix=settings.api_prefix)
app.include_router(auth_router, prefix=settings.api_prefix)
app.include_router(devices_router, prefix=settings.api_prefix)
app.include_router(calibration_router, prefix=settings.api_prefix)
app.include_router(comparison_router, prefix=settings.api_prefix)
app.include_router(gateways_router, prefix=settings.api_prefix)
app.include_router(events_router, prefix=settings.api_prefix)
app.include_router(status_router, prefix=settings.api_prefix)
app.include_router(spatial_router, prefix=settings.api_prefix)
app.include_router(firmware_router, prefix=settings.api_prefix)
app.include_router(firmware_deployment_router, prefix=settings.api_prefix)
app.include_router(internal_metrics_router)

# Serve normalized floorplans (Sprint 2, local storage). Not used in prod with S3.
if settings.storage_provider == "local":
    Path(settings.storage_local_dir).mkdir(parents=True, exist_ok=True)
    app.mount(
        settings.storage_url_prefix,
        StaticFiles(directory=settings.storage_local_dir),
        name="floorplans",
    )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    """Prometheus metrics endpoint."""
    return PlainTextResponse(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
