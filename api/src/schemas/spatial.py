"""
Pydantic schemas for the Spatial Management module.

asset_type is validated with an app-side extensible Enum (not a PG enum):
adding a new type just means adding a member here, with no DB migration needed.
"""

import uuid
from datetime import datetime
from enum import Enum

from pydantic import AliasChoices, BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class AssetType(str, Enum):
    temperature_sensor = "temperature_sensor"
    humidity_sensor = "humidity_sensor"
    co2_sensor = "co2_sensor"
    occupancy_sensor = "occupancy_sensor"
    camera = "camera"
    hvac_unit = "hvac_unit"
    energy_meter = "energy_meter"
    fire_alarm = "fire_alarm"
    access_control = "access_control"
    physical_asset = "physical_asset"


# ---------------------------------------------------------------------------
# Building
# ---------------------------------------------------------------------------
class BuildingCreate(BaseModel):
    name: str
    address: str | None = None
    city: str | None = None
    country: str | None = None
    lat: float | None = None
    lng: float | None = None
    timezone: str = "UTC"
    metadata: dict | None = None


class BuildingUpdate(BaseModel):
    name: str | None = None
    address: str | None = None
    city: str | None = None
    country: str | None = None
    lat: float | None = None
    lng: float | None = None
    timezone: str | None = None
    metadata: dict | None = None


class BuildingResponse(BaseModel):
    id: uuid.UUID
    name: str
    address: str | None
    city: str | None
    country: str | None
    lat: float | None
    lng: float | None
    timezone: str
    # The ORM maps the 'metadata' column to the 'metadata_' attribute (metadata is reserved
    # in SQLAlchemy). We read from both so model_validate(orm) doesn't pick up Base.metadata.
    metadata: dict | None = Field(
        default=None, validation_alias=AliasChoices("metadata_", "metadata")
    )
    created_at: datetime
    floor_count: int = 0

    model_config = {"from_attributes": True, "populate_by_name": True}


# ---------------------------------------------------------------------------
# Floor
# ---------------------------------------------------------------------------
class FloorCreate(BaseModel):
    name: str
    level: int = 0
    sort_order: int = 0
    plan_width_m: float | None = None
    plan_height_m: float | None = None


class FloorUpdate(BaseModel):
    name: str | None = None
    level: int | None = None
    sort_order: int | None = None
    plan_width_m: float | None = None
    plan_height_m: float | None = None


class FloorResponse(BaseModel):
    id: uuid.UUID
    building_id: uuid.UUID
    name: str
    level: int
    sort_order: int
    plan_url: str | None
    plan_viewbox: str | None
    plan_width_m: float | None
    plan_height_m: float | None
    plan_version: int
    plan_status: str
    asset_count: int = 0
    created_at: datetime

    model_config = {"from_attributes": True}


class PlanStatusResponse(BaseModel):
    """Processing status of a floor's plan (Sprint 2)."""

    plan_status: str
    plan_version: int
    plan_url: str | None = None
    plan_viewbox: str | None = None
    plan_node_count: int | None = None
    plan_svg_size_kb: int | None = None
    plan_error: str | None = None

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Layer
# ---------------------------------------------------------------------------
class LayerCreate(BaseModel):
    name: str
    color: str = "#3b82f6"
    icon: str = "circle"
    default_visible: bool = True
    sort_order: int = 0


class LayerUpdate(BaseModel):
    name: str | None = None
    color: str | None = None
    icon: str | None = None
    default_visible: bool | None = None
    sort_order: int | None = None


class LayerResponse(BaseModel):
    id: uuid.UUID
    floor_id: uuid.UUID
    name: str
    color: str
    icon: str
    default_visible: bool
    sort_order: int

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------
class AssetCreate(BaseModel):
    asset_type: AssetType
    name: str
    display_name: str | None = None
    layer_id: uuid.UUID | None = None
    device_id: uuid.UUID | None = None
    pos_x: float | None = None
    pos_y: float | None = None
    rotation_deg: float = 0
    properties: dict | None = None
    tags: list[str] = Field(default_factory=list)
    pos_source: str = "manual"


class AssetUpdate(BaseModel):
    name: str | None = None
    display_name: str | None = None
    asset_type: AssetType | None = None
    layer_id: uuid.UUID | None = None
    device_id: uuid.UUID | None = None
    properties: dict | None = None
    tags: list[str] | None = None


class AssetPositionUpdate(BaseModel):
    pos_x: float
    pos_y: float
    rotation_deg: float = 0
    expected_version: int  # optimistic locking


class AssetResponse(BaseModel):
    id: uuid.UUID
    floor_id: uuid.UUID
    building_id: uuid.UUID
    layer_id: uuid.UUID | None
    device_id: uuid.UUID | None
    asset_type: str
    name: str
    display_name: str | None
    pos_x: float | None
    pos_y: float | None
    rotation_deg: float
    version: int
    properties: dict | None = None
    tags: list[str]
    pos_source: str
    created_at: datetime

    model_config = {"from_attributes": True}


class AssetBulkCreate(BaseModel):
    assets: list[AssetCreate]


# ---------------------------------------------------------------------------
# Telemetry overlaid on the plan (Sprint 3)
# ---------------------------------------------------------------------------
class AssetTelemetryItem(BaseModel):
    asset_id: uuid.UUID
    device_id: uuid.UUID | None
    last_ts: datetime | None
    rssi_ts: datetime | None
    temperature_c: float | None
    humidity_pct: float | None
    supply_mv: int | None
    rssi_dbm: int | None
    status: str | None  # online / offline / unknown

    model_config = {"from_attributes": True}


class FloorTelemetryResponse(BaseModel):
    # floor-wide freshness (MAX(updated_at)); None if there is no snapshot
    snapshot_updated_at: datetime | None
    items: list[AssetTelemetryItem]
