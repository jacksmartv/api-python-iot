"""
CRUD endpoints for the Spatial Management module (buildings/floors/layers/assets).

Cross-cutting rules (Sprint 1):
- org_id ALWAYS comes from get_current_org_id (never settings.default_org_id directly).
- Every GET starts from alive(model, org_id): filters deleted_at IS NULL + org.
- DELETE = soft delete (UPDATE deleted_at = NOW()), never a physical delete.
- Creating/editing buildings and floors -> require_admin. Assets -> require_user_or_admin.
- /assets/{id}/position uses optimistic locking (version) + history in the same txn.
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    HTTPException,
    UploadFile,
    status,
)
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth_jwt import (
    get_current_user,
    require_admin,
    require_any_role,
    require_user_or_admin,
)
from ..config import settings
from ..database import async_session, get_db
from ..models import (
    Asset,
    AssetPositionHistory,
    AssetTelemetrySnapshot,
    Building,
    Floor,
    Layer,
    User,
)
from ..schemas.spatial import (
    AssetBulkCreate,
    AssetCreate,
    AssetPositionUpdate,
    AssetResponse,
    AssetTelemetryItem,
    AssetUpdate,
    BuildingCreate,
    BuildingResponse,
    BuildingUpdate,
    FloorCreate,
    FloorResponse,
    FloorTelemetryResponse,
    FloorUpdate,
    LayerCreate,
    LayerResponse,
    LayerUpdate,
    PlanStatusResponse,
)
from ..services.floorplan import PlanRejected, process_floorplan
from ..services.storage import get_storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/spatial", tags=["spatial"])


# ---------------------------------------------------------------------------
# org_id contract (single place) — V1 single-tenant, V2 flip to JWT claim.
# ---------------------------------------------------------------------------
SPATIAL_MULTI_ORG = False  # single flip to True in V2


async def get_current_org_id(user: User = Depends(get_current_user)) -> uuid.UUID:
    """V1: single org (DEFAULT_ORG_ID). V2: JWT claim (403 if missing).
    Endpoints depend on THIS function, never on settings.default_org_id directly."""
    if not SPATIAL_MULTI_ORG:
        return settings.default_org_uuid
    org_id = getattr(user, "org_id", None)
    if org_id is None:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "Token missing org_id in multi-org system"
        )
    return org_id


def alive(model, org_id: uuid.UUID):
    """Base select(): only live rows for the current org."""
    return select(model).where(model.deleted_at.is_(None), model.org_id == org_id)


# ===========================================================================
# BUILDINGS
# ===========================================================================
@router.get("/buildings", response_model=list[BuildingResponse])
async def list_buildings(
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_any_role),
):
    floor_count_sq = (
        select(func.count(Floor.id))
        .where(Floor.building_id == Building.id, Floor.deleted_at.is_(None))
        .scalar_subquery()
    )
    result = await db.execute(
        select(Building, floor_count_sq.label("floor_count"))
        .where(Building.deleted_at.is_(None), Building.org_id == org_id)
        .order_by(Building.name)
    )
    out = []
    for building, fc in result.all():
        resp = BuildingResponse.model_validate(building)
        resp.floor_count = fc or 0
        out.append(resp)
    return out


@router.post("/buildings", response_model=BuildingResponse, status_code=status.HTTP_201_CREATED)
async def create_building(
    data: BuildingCreate,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    user: User = Depends(require_admin),
):
    building = Building(
        org_id=org_id,
        name=data.name,
        address=data.address,
        city=data.city,
        country=data.country,
        lat=data.lat,
        lng=data.lng,
        timezone=data.timezone,
        metadata_=data.metadata,
        created_by=user.id,
    )
    db.add(building)
    await db.commit()
    await db.refresh(building)
    return BuildingResponse.model_validate(building)


async def _get_building_or_404(db, org_id, building_id) -> Building:
    result = await db.execute(
        alive(Building, org_id).where(Building.id == building_id)
    )
    building = result.scalar_one_or_none()
    if building is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Building not found")
    return building


@router.get("/buildings/{building_id}", response_model=BuildingResponse)
async def get_building(
    building_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_any_role),
):
    building = await _get_building_or_404(db, org_id, building_id)
    return BuildingResponse.model_validate(building)


@router.patch("/buildings/{building_id}", response_model=BuildingResponse)
async def update_building(
    building_id: uuid.UUID,
    data: BuildingUpdate,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_admin),
):
    building = await _get_building_or_404(db, org_id, building_id)
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(building, "metadata_" if field == "metadata" else field, value)
    await db.commit()
    await db.refresh(building)
    return BuildingResponse.model_validate(building)


@router.delete("/buildings/{building_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_building(
    building_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_admin),
):
    building = await _get_building_or_404(db, org_id, building_id)
    building.deleted_at = func.now()
    await db.commit()


# ===========================================================================
# FLOORS
# ===========================================================================
@router.get("/buildings/{building_id}/floors", response_model=list[FloorResponse])
async def list_floors(
    building_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_any_role),
):
    await _get_building_or_404(db, org_id, building_id)
    result = await db.execute(
        alive(Floor, org_id)
        .where(Floor.building_id == building_id)
        .order_by(Floor.sort_order, Floor.level)
    )
    return [FloorResponse.model_validate(f) for f in result.scalars().all()]


@router.post(
    "/buildings/{building_id}/floors",
    response_model=FloorResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_floor(
    building_id: uuid.UUID,
    data: FloorCreate,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_admin),
):
    await _get_building_or_404(db, org_id, building_id)
    floor = Floor(
        building_id=building_id,
        org_id=org_id,
        name=data.name,
        level=data.level,
        sort_order=data.sort_order,
        plan_width_m=data.plan_width_m,
        plan_height_m=data.plan_height_m,
    )
    db.add(floor)
    await db.commit()
    await db.refresh(floor)
    return FloorResponse.model_validate(floor)


async def _get_floor_or_404(db, org_id, floor_id) -> Floor:
    result = await db.execute(alive(Floor, org_id).where(Floor.id == floor_id))
    floor = result.scalar_one_or_none()
    if floor is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Floor not found")
    return floor


@router.get("/floors/{floor_id}", response_model=FloorResponse)
async def get_floor(
    floor_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_any_role),
):
    floor = await _get_floor_or_404(db, org_id, floor_id)
    count = await db.execute(
        select(func.count(Asset.id)).where(
            Asset.floor_id == floor_id, Asset.deleted_at.is_(None)
        )
    )
    resp = FloorResponse.model_validate(floor)
    resp.asset_count = count.scalar() or 0
    return resp


@router.patch("/floors/{floor_id}", response_model=FloorResponse)
async def update_floor(
    floor_id: uuid.UUID,
    data: FloorUpdate,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_admin),
):
    floor = await _get_floor_or_404(db, org_id, floor_id)
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(floor, field, value)
    await db.commit()
    await db.refresh(floor)
    return FloorResponse.model_validate(floor)


@router.delete("/floors/{floor_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_floor(
    floor_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_admin),
):
    floor = await _get_floor_or_404(db, org_id, floor_id)
    floor.deleted_at = func.now()
    await db.commit()


# ===========================================================================
# LAYERS
# ===========================================================================
@router.get("/floors/{floor_id}/layers", response_model=list[LayerResponse])
async def list_layers(
    floor_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_any_role),
):
    await _get_floor_or_404(db, org_id, floor_id)
    result = await db.execute(
        alive(Layer, org_id).where(Layer.floor_id == floor_id).order_by(Layer.sort_order)
    )
    return [LayerResponse.model_validate(layer) for layer in result.scalars().all()]


@router.post(
    "/floors/{floor_id}/layers",
    response_model=LayerResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_layer(
    floor_id: uuid.UUID,
    data: LayerCreate,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_admin),
):
    await _get_floor_or_404(db, org_id, floor_id)
    layer = Layer(
        floor_id=floor_id,
        org_id=org_id,
        name=data.name,
        color=data.color,
        icon=data.icon,
        default_visible=data.default_visible,
        sort_order=data.sort_order,
    )
    db.add(layer)
    await db.commit()
    await db.refresh(layer)
    return LayerResponse.model_validate(layer)


@router.patch("/layers/{layer_id}", response_model=LayerResponse)
async def update_layer(
    layer_id: uuid.UUID,
    data: LayerUpdate,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_admin),
):
    result = await db.execute(alive(Layer, org_id).where(Layer.id == layer_id))
    layer = result.scalar_one_or_none()
    if layer is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Layer not found")
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(layer, field, value)
    await db.commit()
    await db.refresh(layer)
    return LayerResponse.model_validate(layer)


@router.delete("/layers/{layer_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_layer(
    layer_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_admin),
):
    result = await db.execute(alive(Layer, org_id).where(Layer.id == layer_id))
    layer = result.scalar_one_or_none()
    if layer is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Layer not found")
    layer.deleted_at = func.now()
    await db.commit()


# ===========================================================================
# ASSETS
# ===========================================================================
def _asset_from_create(data: AssetCreate, floor: Floor, org_id, user_id) -> Asset:
    return Asset(
        floor_id=floor.id,
        building_id=floor.building_id,
        org_id=org_id,
        layer_id=data.layer_id,
        device_id=data.device_id,
        asset_type=data.asset_type.value,
        name=data.name,
        display_name=data.display_name,
        pos_x=data.pos_x,
        pos_y=data.pos_y,
        rotation_deg=data.rotation_deg,
        plan_version=floor.plan_version,
        properties=data.properties,
        tags=data.tags,
        pos_source=data.pos_source,
        created_by=user_id,
    )


@router.get("/floors/{floor_id}/assets", response_model=list[AssetResponse])
async def list_assets(
    floor_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_any_role),
):
    await _get_floor_or_404(db, org_id, floor_id)
    result = await db.execute(
        alive(Asset, org_id).where(Asset.floor_id == floor_id).order_by(Asset.created_at)
    )
    return [AssetResponse.model_validate(a) for a in result.scalars().all()]


@router.post(
    "/floors/{floor_id}/assets",
    response_model=AssetResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_asset(
    floor_id: uuid.UUID,
    data: AssetCreate,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    user: User = Depends(require_user_or_admin),
):
    floor = await _get_floor_or_404(db, org_id, floor_id)
    asset = _asset_from_create(data, floor, org_id, user.id)
    db.add(asset)
    await db.commit()
    await db.refresh(asset)
    return AssetResponse.model_validate(asset)


@router.post(
    "/floors/{floor_id}/assets/bulk",
    response_model=list[AssetResponse],
    status_code=status.HTTP_201_CREATED,
)
async def bulk_create_assets(
    floor_id: uuid.UUID,
    data: AssetBulkCreate,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    user: User = Depends(require_user_or_admin),
):
    """Bulk import (CSV/JSON). Insert in a single transaction."""
    floor = await _get_floor_or_404(db, org_id, floor_id)
    assets = [_asset_from_create(a, floor, org_id, user.id) for a in data.assets]
    db.add_all(assets)
    await db.commit()
    for a in assets:
        await db.refresh(a)
    return [AssetResponse.model_validate(a) for a in assets]


async def _get_asset_or_404(db, org_id, asset_id) -> Asset:
    result = await db.execute(alive(Asset, org_id).where(Asset.id == asset_id))
    asset = result.scalar_one_or_none()
    if asset is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Asset not found")
    return asset


@router.get("/assets/{asset_id}", response_model=AssetResponse)
async def get_asset(
    asset_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_any_role),
):
    asset = await _get_asset_or_404(db, org_id, asset_id)
    return AssetResponse.model_validate(asset)


@router.patch("/assets/{asset_id}", response_model=AssetResponse)
async def update_asset(
    asset_id: uuid.UUID,
    data: AssetUpdate,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_user_or_admin),
):
    asset = await _get_asset_or_404(db, org_id, asset_id)
    for field, value in data.model_dump(exclude_unset=True).items():
        if field == "asset_type" and value is not None:
            value = value.value if hasattr(value, "value") else value
        setattr(asset, field, value)
    await db.commit()
    await db.refresh(asset)
    return AssetResponse.model_validate(asset)


@router.patch("/assets/{asset_id}/position", response_model=AssetResponse)
async def update_asset_position(
    asset_id: uuid.UUID,
    data: AssetPositionUpdate,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    user: User = Depends(require_user_or_admin),
):
    """Moves an asset with optimistic locking + writes history (same txn).
    409 if expected_version doesn't match (another writer won)."""
    asset = await _get_asset_or_404(db, org_id, asset_id)
    if asset.version != data.expected_version:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Version conflict: expected {data.expected_version}, got {asset.version}",
        )
    asset.pos_x = data.pos_x
    asset.pos_y = data.pos_y
    asset.rotation_deg = data.rotation_deg
    asset.version = asset.version + 1
    db.add(
        AssetPositionHistory(
            asset_id=asset.id,
            org_id=org_id,
            pos_x=data.pos_x,
            pos_y=data.pos_y,
            rotation_deg=data.rotation_deg,
            plan_version=asset.plan_version or 0,
            source="api",
            moved_by=user.id,
        )
    )
    await db.commit()
    await db.refresh(asset)
    return AssetResponse.model_validate(asset)


@router.delete("/assets/{asset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_asset(
    asset_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_user_or_admin),
):
    asset = await _get_asset_or_404(db, org_id, asset_id)
    asset.deleted_at = func.now()
    await db.commit()


# ===========================================================================
# FLOOR PLAN (Sprint 2 — SVG Ingestion Pipeline)
# ===========================================================================
def _is_processing_stale(floor: Floor) -> bool:
    """True if a job stuck in 'processing' exceeded the timeout (unstick hung jobs)."""
    if floor.plan_status != "processing" or floor.plan_processing_started_at is None:
        return False
    age = datetime.now(timezone.utc) - floor.plan_processing_started_at
    return age > timedelta(seconds=settings.plan_processing_timeout_s)


async def _process_plan_job(floor_id: uuid.UUID, org_id: uuid.UUID, raw: bytes, new_version: int):
    """Background: runs the pipeline and publishes the result. Own session."""
    storage = get_storage()
    raster_key = f"{floor_id}/v{new_version}_raster"
    svg_key = f"{floor_id}/v{new_version}.svg"
    try:
        # raster_url is only computed with the real extension inside the pipeline; we use a
        # .png placeholder and rewrite it if it turns out to be jpg (raster_to_svg only needs
        # the base url).
        raster_url_png = storage.public_url(f"{raster_key}.png")
        plan = await asyncio.to_thread(process_floorplan, raw, raster_url_png)

        if plan.raster_bytes is not None:
            ext = plan.raster_ext or "png"
            rkey = f"{raster_key}.{ext}"
            await storage.save(rkey, plan.raster_bytes, f"image/{ext}")
            # if it was jpg, fix the href in the svg
            if ext != "png":
                plan.svg_bytes = plan.svg_bytes.replace(
                    raster_url_png.encode(), storage.public_url(rkey).encode()
                )
        svg_url = await storage.save(svg_key, plan.svg_bytes, "image/svg+xml")

        async with async_session() as db:
            floor = await db.get(Floor, floor_id)
            if floor is None:
                return
            prev_version = floor.plan_version
            floor.plan_url = svg_url
            floor.plan_viewbox = plan.viewbox
            floor.plan_node_count = plan.node_count
            floor.plan_svg_size_kb = plan.svg_size_kb
            floor.plan_version = new_version
            floor.plan_status = "ready"
            floor.plan_error = None
            floor.plan_processing_started_at = None
            await db.commit()

        # "Current version only" policy: delete files from the previous version
        if prev_version >= 1:
            for old in (
                f"{floor_id}/v{prev_version}.svg",
                f"{floor_id}/v{prev_version}_raster.png",
                f"{floor_id}/v{prev_version}_raster.jpg",
            ):
                try:
                    await storage.delete(old)
                except Exception as e:  # noqa: BLE001 — best-effort cleanup
                    logger.warning("floorplan old version cleanup failed: %s", e)

    except Exception as e:  # noqa: BLE001 — PlanRejected or other
        reason = str(e) if isinstance(e, PlanRejected) else "internal processing error"
        logger.warning("floorplan processing failed (floor=%s): %s", floor_id, e)
        async with async_session() as db:
            floor = await db.get(Floor, floor_id)
            if floor is not None:
                floor.plan_status = "failed"
                floor.plan_error = reason
                floor.plan_processing_started_at = None
                await db.commit()


@router.post(
    "/floors/{floor_id}/plan",
    response_model=PlanStatusResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def upload_floor_plan(
    floor_id: uuid.UUID,
    background: BackgroundTasks,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_admin),
):
    """Uploads a floor plan (SVG/PNG/JPG). Processes async; returns 202 + processing."""
    floor = await _get_floor_or_404(db, org_id, floor_id)

    raw = await file.read()
    if not raw:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Empty file")
    if len(raw) > settings.plan_max_upload_mb * 1024 * 1024:
        raise HTTPException(
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            f"File exceeds {settings.plan_max_upload_mb} MB limit",
        )

    # Atomic lock: switch to 'processing' ONLY if there's no active job. A single conditional
    # UPDATE avoids the race between two simultaneous POSTs (a read-check alone isn't enough).
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=settings.plan_processing_timeout_s)
    result = await db.execute(
        update(Floor)
        .where(
            Floor.id == floor_id,
            Floor.org_id == org_id,
            Floor.deleted_at.is_(None),
            (Floor.plan_status != "processing")
            | (Floor.plan_processing_started_at < cutoff),
        )
        .values(
            plan_status="processing",
            plan_processing_started_at=func.now(),
            plan_error=None,
        )
        .returning(Floor.plan_version)
    )
    row = result.first()
    if row is None:
        raise HTTPException(status.HTTP_409_CONFLICT, "Plan processing already in progress")
    await db.commit()

    new_version = row[0] + 1
    background.add_task(_process_plan_job, floor_id, org_id, raw, new_version)
    await db.refresh(floor)
    return PlanStatusResponse.model_validate(floor)


@router.get("/floors/{floor_id}/plan/status", response_model=PlanStatusResponse)
async def get_floor_plan_status(
    floor_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_any_role),
):
    floor = await _get_floor_or_404(db, org_id, floor_id)
    # Unstick hung jobs: expired processing -> failed
    if _is_processing_stale(floor):
        floor.plan_status = "failed"
        floor.plan_error = "processing timeout"
        floor.plan_processing_started_at = None
        await db.commit()
        await db.refresh(floor)
    return PlanStatusResponse.model_validate(floor)


@router.delete("/floors/{floor_id}/plan", response_model=PlanStatusResponse)
async def delete_floor_plan(
    floor_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_admin),
):
    """Deletes the floor plan and its files. Idempotent. 409 if processing is active."""
    floor = await _get_floor_or_404(db, org_id, floor_id)
    if floor.plan_status == "processing" and not _is_processing_stale(floor):
        raise HTTPException(status.HTTP_409_CONFLICT, "Plan processing in progress")

    if floor.plan_status == "none" and floor.plan_url is None:
        return PlanStatusResponse.model_validate(floor)  # idempotent

    storage = get_storage()
    v = floor.plan_version
    for key in (
        f"{floor_id}/v{v}.svg",
        f"{floor_id}/v{v}_raster.png",
        f"{floor_id}/v{v}_raster.jpg",
    ):
        try:
            await storage.delete(key)
        except Exception as e:  # noqa: BLE001 — best-effort
            logger.warning("floorplan delete failed: %s", e)

    floor.plan_status = "none"
    floor.plan_url = None
    floor.plan_viewbox = None
    floor.plan_node_count = None
    floor.plan_svg_size_kb = None
    floor.plan_error = None
    floor.plan_processing_started_at = None
    await db.commit()
    await db.refresh(floor)
    return PlanStatusResponse.model_validate(floor)


# ===========================================================================
# TELEMETRY OVER THE FLOOR PLAN (Sprint 3) — snapshot read-only
# ===========================================================================
@router.get("/floors/{floor_id}/telemetry", response_model=FloorTelemetryResponse)
async def get_floor_telemetry(
    floor_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    org_id: uuid.UUID = Depends(get_current_org_id),
    _user: User = Depends(require_any_role),
):
    """Reads the floor's asset telemetry from the snapshot (PK lookup, no JOIN
    to measurement). The snapshot is kept fresh by the Sprint 3 periodic job.
    Assets without a device are not in the snapshot -> they don't appear."""
    await _get_floor_or_404(db, org_id, floor_id)
    result = await db.execute(
        select(AssetTelemetrySnapshot)
        .join(Asset, Asset.id == AssetTelemetrySnapshot.asset_id)
        .where(
            Asset.floor_id == floor_id,
            Asset.deleted_at.is_(None),
            Asset.org_id == org_id,
        )
    )
    rows = result.scalars().all()
    items = [AssetTelemetryItem.model_validate(r) for r in rows]
    snapshot_updated_at = max((r.updated_at for r in rows), default=None)
    return FloorTelemetryResponse(snapshot_updated_at=snapshot_updated_at, items=items)
