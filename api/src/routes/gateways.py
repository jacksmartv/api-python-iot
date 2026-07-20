"""
Endpoints for IoT gateway monitoring.
"""

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth_jwt import require_admin, require_any_role
from ..database import get_db
from ..models import Device, Gateway, GatewayStatus, Measurement, Sensor
from ..models.user import User
from ..schemas.gateway import (
    GatewayCommandRequest,
    GatewayCommandResponse,
    GatewayConfigResponse,
    GatewayConfigV3Response,
    GatewayDeviceResponse,
    GatewayHealthResponse,
    GatewayListResponse,
    GatewaySeqGap,
    GatewayStatusResponse,
    GatewayUpdate,
    GatewayUplinkGapsResponse,
    GwRecoveryGaps,
    GwRecoveryItem,
    GwRecoveryResponse,
    GwRecoveryStats,
)
from ..services.command_service import send_command
from ..services.gap_detection import compute_gw_seq_gaps
from ..utils.gateway import parse_gps

router = APIRouter(prefix="/gateways", tags=["gateways"])

ONLINE_THRESHOLD_MINUTES = 180


@router.get("", response_model=list[GatewayListResponse])
async def list_gateways(
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
):
    """Lists all gateways with their latest status."""
    now = datetime.now(timezone.utc)
    online_cutoff = now - timedelta(minutes=ONLINE_THRESHOLD_MINUTES)

    result = await db.execute(
        text("""
            SELECT
                g.serial_number,
                g.metadata,
                g.created_at,
                GREATEST(gs.ts, gc.received_at) AS last_seen,
                gs.csq,
                gs.vgw,
                tel.raw_payload AS tel_payload
            FROM core.gateway g
            LEFT JOIN LATERAL (
                SELECT ts, csq, vgw
                FROM monitoring.gateway_status
                WHERE serial_number = g.serial_number
                ORDER BY ts DESC
                LIMIT 1
            ) gs ON true
            LEFT JOIN LATERAL (
                SELECT received_at
                FROM monitoring.gateway_config
                WHERE serial_number = g.serial_number
                ORDER BY received_at DESC
                LIMIT 1
            ) gc ON true
            LEFT JOIN LATERAL (
                SELECT raw_payload
                FROM monitoring.gateway_status
                WHERE serial_number = g.serial_number
                  AND raw_payload->>'type' = 'telemetry'
                ORDER BY ts DESC
                LIMIT 1
            ) tel ON true
            ORDER BY GREATEST(gs.ts, gc.received_at) DESC NULLS LAST
        """)
    )

    rows = result.all()

    def _row_to_response(row):
        gps = parse_gps(row.tel_payload or {})
        return GatewayListResponse(
            serial_number=row.serial_number,
            display_name=(row.metadata or {}).get("display_name") or None,
            metadata=row.metadata,
            created_at=row.created_at,
            last_seen=row.last_seen,
            csq=row.csq,
            vgw=row.vgw,
            online=row.last_seen is not None and row.last_seen >= online_cutoff,
            lat=gps[0] if gps else None,
            lng=gps[1] if gps else None,
        )

    return [
        _row_to_response(row)
        for row in rows
    ]


@router.get("/{serial_number}/status", response_model=list[GatewayStatusResponse])
async def get_gateway_status(
    serial_number: str,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(100, ge=1, le=1000),
):
    """Historial de status de una gateway."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    result = await db.execute(
        select(GatewayStatus)
        .where(
            GatewayStatus.serial_number == serial_number,
            GatewayStatus.ts >= since,
        )
        .order_by(GatewayStatus.ts.desc())
        .limit(limit)
    )
    rows = result.scalars().all()

    if not rows:
        # Verificar si la gateway existe
        exists = await db.execute(
            select(GatewayStatus.serial_number)
            .where(GatewayStatus.serial_number == serial_number)
            .limit(1)
        )
        if not exists.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Gateway '{serial_number}' not found",
            )

    def _status_to_response(row):
        rp = row.raw_payload or {}
        return GatewayStatusResponse(
            id=str(row.id),
            serial_number=row.serial_number,
            ts=row.ts,
            csq=row.csq,
            erf=row.erf,
            rst=row.rst,
            vgw=row.vgw,
            # Derived from raw_payload per row (for historical health charts)
            wifi_rssi=_as_int(rp.get("wifi_rssi")),
            temp_c=_as_float(rp.get("temp_c")),
        )

    return [_status_to_response(row) for row in rows]


@router.get(
    "/{serial_number}/config",
    response_model=GatewayConfigV3Response | GatewayConfigResponse,
)
async def get_gateway_config(
    serial_number: str,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
    config_type: str = Query("activeConfig", pattern="^(activeConfig|setConfig)$"),
):
    """Returns the gateway config, detecting the format (v3 JSON or legacy binary).

    If the gateway has v3 config (new firmware, response to gw_get) -> returns it with
    schema_version="v3". Otherwise falls back to the legacy binary snapshot
    (schema_version="legacy"). The UI calls this single endpoint.
    """
    # 1. prefer v3 config (new firmware)
    v3 = await db.execute(
        text("""
            SELECT config, fw, net_iface, request_id, gateway_ts_ms,
                   first_seen_at, config_updated_at, received_at
            FROM monitoring.gateway_config_v3
            WHERE serial_number = :sn
        """),
        {"sn": serial_number},
    )
    v3_row = v3.one_or_none()
    if v3_row is not None:
        return GatewayConfigV3Response(**v3_row._mapping)

    # 2. fallback: legacy binary (setConfig/activeConfig)
    result = await db.execute(
        text("""
            SELECT config_type, received_at, broker, port, client_id,
                   fw_type, topic_prefix, interval_s, supply_mv, broker2
            FROM monitoring.gateway_config
            WHERE serial_number = :sn
              AND config_type   = :ct
            ORDER BY received_at DESC
            LIMIT 1
        """),
        {"sn": serial_number, "ct": config_type},
    )
    row = result.one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No config found for gateway '{serial_number}'",
        )
    return GatewayConfigResponse(**row._mapping)


@router.post(
    "/{serial_number}/cmd",
    response_model=GatewayCommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def post_gateway_command(
    serial_number: str,
    cmd: GatewayCommandRequest,
    _user: User = Depends(require_any_role),
):
    """Publishes a command to the gateway. PHASE 1: ONLY {"target":"gw","action":"get"} (read-only).

    Strict validation: the body must be target=gw + action=get, with optional `section`
    (extra='forbid' rejects params/id/etc.). The request_id is generated by the backend
    (CommandService). The gateway's response arrives asynchronously via cmd/ack and updates
    the config (poll GET /config).

    `section`: without it, the firmware returns the full config (~1KB) — on LTE gateways this
    exceeds the MQTT payload limit (max 480B) and the ack comes back with
    `{"ok":false,"error":"payload_too_large"}` with no config, silently ignored by
    ingest_gateway_config_v3. Requesting with `section` (provision|runtime|lora|connect|system)
    keeps the response under the limit; the UI iterates the 5 sections when the gateway is on LTE.
    """
    if cmd.target != "gw" or cmd.action != "get":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Phase 1: only {\"target\":\"gw\",\"action\":\"get\"} (read) is allowed.",
        )
    command: dict = {"target": "gw", "action": "get"}
    if cmd.section:
        command["section"] = cmd.section
    result = await send_command(serial_number, command)
    if not result.ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not publish the command (MQTT unavailable). Retry.",
        )
    assert result.request_id is not None  # inject_id=True (default) always generates it
    return GatewayCommandResponse(request_id=result.request_id, published=True)


def _as_int(v: object) -> int | None:
    return int(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _as_float(v: object) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


@router.get("/{serial_number}/health", response_model=GatewayHealthResponse)
async def get_gateway_health(
    serial_number: str,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
):
    """Health + presence snapshot of the gateway (latest telemetry + latest status).

    Derives the fields from raw_payload in Python (not in SQL). The two blocks (telemetry/status)
    are independent: if one is missing, its fields are null. 404 only if the gateway doesn't exist.
    """
    # The gateway must exist in core.gateway (404 if not)
    exists = await db.execute(
        select(Gateway.serial_number).where(Gateway.serial_number == serial_number).limit(1)
    )
    if exists.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Gateway '{serial_number}' not found",
        )

    # Explicit selection by type (doesn't assume arrival order): latest telemetry and latest status.
    result = await db.execute(
        text("""
            SELECT
                tel.ts          AS last_telemetry_at,
                tel.raw_payload AS tel_payload,
                st.ts           AS last_status_at,
                st.raw_payload  AS st_payload
            FROM (SELECT 1) _
            LEFT JOIN LATERAL (
                SELECT ts, raw_payload
                FROM monitoring.gateway_status
                WHERE serial_number = :sn AND raw_payload->>'type' = 'telemetry'
                ORDER BY ts DESC LIMIT 1
            ) tel ON true
            LEFT JOIN LATERAL (
                SELECT ts, raw_payload
                FROM monitoring.gateway_status
                WHERE serial_number = :sn AND raw_payload ? 'state'
                ORDER BY ts DESC LIMIT 1
            ) st ON true
        """),
        {"sn": serial_number},
    )
    row = result.one()

    tel = row.tel_payload or {}
    st = row.st_payload or {}
    gps = parse_gps(tel)

    return GatewayHealthResponse(
        serial_number=serial_number,
        last_telemetry_at=row.last_telemetry_at,
        last_status_at=row.last_status_at,
        wifi_rssi=_as_int(tel.get("wifi_rssi")),
        uptime_sec=_as_int(tel.get("uptime_sec")),
        temp_c=_as_float(tel.get("temp_c")),
        heap_free=_as_int(tel.get("heap_free")),
        fw=tel.get("fw") if isinstance(tel.get("fw"), str) else None,
        lora_freq=_as_int(tel.get("lora_freq")),
        gps_lat=gps[0] if gps else None,
        gps_lon=gps[1] if gps else None,
        gps_sats=_as_int(tel.get("gps_sats")),
        gps_hdop=_as_float(tel.get("gps_hdop")),
        gps_alt_m=_as_float(tel.get("gps_alt_m")),
        state=st.get("state") if isinstance(st.get("state"), str) else None,
        freq=_as_int(st.get("freq")),
    )


# Threshold to discard gw_seq jumps that are NOT real loss: gateway reset/reboot or
# wraparound. gw_seq is uint32 (wraparound is far off); a legitimate uplink gap of >1000 frames
# is implausible (the SD replay recovers it). Configurable via query param.
@router.get("/{serial_number}/seq-gaps", response_model=GatewayUplinkGapsResponse)
async def get_gateway_seq_gaps(
    serial_number: str,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
    hours: int = Query(24, ge=1, le=168),
    max_gap: int = Query(
        1000, ge=2,
        description="gw_seq jumps >= max_gap are discarded as a gateway reset/reboot, not "
                    "as uplink loss.",
    ),
):
    """Continuity of gw_seq (gateway -> backend uplink), distinct from node_seq (RF, per device).

    gw_seq is the 'seq' field of the /rx wrapper (increments by 1 for every frame the gateway
    relays from all its nodes). It measures the health of the gateway's uplink as a whole. It's read
    from raw.telemetry_payload (where the full wrapper is already persisted). Ordered by seq value.

    The computation lives in services/gap_detection.py (single source, shared with the recovery
    job). Here grace_period_s=0: the endpoint reports all gaps; the job applies grace.
    """
    result = await compute_gw_seq_gaps(db, serial_number, hours, max_gap=max_gap)

    return GatewayUplinkGapsResponse(
        serial_number=serial_number,
        hours=hours,
        total_packets=result.total_packets,
        first_seq=result.first_seq,
        last_seq=result.last_seq,
        missing_total=result.missing_total,
        completeness_pct=result.completeness_pct,
        gaps=[
            GatewaySeqGap(
                after_seq=g.after_seq, before_seq=g.before_seq, missing=g.missing, at=g.at
            )
            for g in result.gaps
        ],
    )


@router.get("/{serial_number}/recovery", response_model=GwRecoveryResponse)
async def get_gateway_recovery(
    serial_number: str,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
    status_filter: str = Query(
        "all", alias="status",
        description="pending | inflight | recovered | not_found | all",
    ),
    hours: int = Query(6, ge=1, le=168, description="window for computing uplink gaps"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Recovery status by seq (Recovery tab, gap_recovery V3).

    Per-status counters + paginated items from `gw_seq_recovery` + uplink gaps
    (compute_gw_seq_gaps, reads raw). See sprint GAP_RECOVERY_V3 §3.7.
    """
    # 1. stats by status (over the gateway's entire table)
    stats_row = (
        await db.execute(
            text("""
                SELECT
                    count(*) FILTER (WHERE status = 'pending')   AS pending,
                    count(*) FILTER (WHERE status = 'inflight')  AS inflight,
                    count(*) FILTER (WHERE status = 'recovered') AS recovered,
                    count(*) FILTER (WHERE status = 'not_found') AS not_found
                FROM monitoring.gw_seq_recovery WHERE gw_serial = :gw
            """),
            {"gw": serial_number},
        )
    ).one()
    terminal = stats_row.recovered + stats_row.not_found
    success_pct = round(stats_row.recovered / terminal * 100, 2) if terminal else None
    stats = GwRecoveryStats(
        pending=stats_row.pending,
        inflight=stats_row.inflight,
        recovered=stats_row.recovered,
        not_found=stats_row.not_found,
        success_pct=success_pct,
    )

    # 2. items (paginado + filtro por status)
    where_status = "" if status_filter == "all" else "AND status = :status"
    params: dict = {"gw": serial_number, "limit": limit, "offset": offset}
    if status_filter != "all":
        params["status"] = status_filter

    total_items = (
        await db.execute(
            text(f"""
                SELECT count(*) FROM monitoring.gw_seq_recovery
                WHERE gw_serial = :gw {where_status}
            """),
            params,
        )
    ).scalar_one()

    rows = (
        await db.execute(
            text(f"""
                SELECT gw_seq, status, reason,
                       COALESCE(recovered_at, last_response_at, inflight_at, first_attempt_at)
                           AS updated_at,
                       recovered_at, last_response_at
                FROM monitoring.gw_seq_recovery
                WHERE gw_serial = :gw {where_status}
                ORDER BY gw_seq DESC
                LIMIT :limit OFFSET :offset
            """),
            params,
        )
    ).all()
    items = [
        GwRecoveryItem(
            gw_seq=r.gw_seq, status=r.status, reason=r.reason,
            updated_at=r.updated_at, recovered_at=r.recovered_at,
            last_response_at=r.last_response_at,
        )
        for r in rows
    ]

    # 3. gaps de uplink (independiente del mecanismo — lee raw)
    gap_result = await compute_gw_seq_gaps(db, serial_number, hours)
    gaps = GwRecoveryGaps(
        missing_total=gap_result.missing_total,
        completeness_pct=gap_result.completeness_pct,
        first_seq=gap_result.first_seq,
        last_seq=gap_result.last_seq,
    )

    return GwRecoveryResponse(
        serial_number=serial_number, stats=stats, total_items=total_items,
        items=items, gaps=gaps,
    )


@router.get("/{serial_number}/device-count")
async def get_gateway_device_count(
    serial_number: str,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_admin),
):
    """Counts devices linked to a gateway (for delete confirmation)."""
    result = await db.execute(
        select(func.count(Device.id))
        .join(Gateway, Gateway.id == Device.gateway_id)
        .where(Gateway.serial_number == serial_number)
    )
    return {"device_count": result.scalar() or 0}


@router.get("/{serial_number}/devices", response_model=list[GatewayDeviceResponse])
async def get_gateway_devices(
    serial_number: str,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
):
    """Lists the devices linked to a gateway with last_seen and sensor_count."""
    now = datetime.now(timezone.utc)
    online_cutoff = now - timedelta(hours=24)

    # Subquery: last measurement per device
    last_seen_sq = (
        select(Sensor.device_id, func.max(Measurement.ts).label("last_seen"))
        .join(Measurement, Measurement.sensor_id == Sensor.id)
        .group_by(Sensor.device_id)
        .subquery()
    )

    result = await db.execute(
        select(
            Device.id,
            Device.serial_number,
            Device.metadata_.label("metadata"),
            func.count(Sensor.id.distinct()).label("sensor_count"),
            last_seen_sq.c.last_seen,
        )
        .join(Gateway, Gateway.id == Device.gateway_id)
        .outerjoin(Sensor, Sensor.device_id == Device.id)
        .outerjoin(last_seen_sq, last_seen_sq.c.device_id == Device.id)
        .where(Gateway.serial_number == serial_number)
        .group_by(Device.id, Device.serial_number, Device.metadata_, last_seen_sq.c.last_seen)
        .order_by(Device.serial_number)
    )
    rows = result.all()
    return [
        GatewayDeviceResponse(
            id=row.id,
            serial_number=row.serial_number,
            display_name=(row.metadata or {}).get("display_name") or None,
            sensor_count=row.sensor_count or 0,
            last_seen=row.last_seen,
            online=row.last_seen is not None and row.last_seen >= online_cutoff,
        )
        for row in rows
    ]


@router.patch("/{serial_number}", response_model=GatewayListResponse)
async def update_gateway(
    serial_number: str,
    data: GatewayUpdate,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_admin),
):
    """Updates metadata for a gateway (admin only)."""
    result = await db.execute(select(Gateway).where(Gateway.serial_number == serial_number))
    gateway = result.scalar_one_or_none()
    if gateway is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Gateway '{serial_number}' not found",
        )

    current = dict(gateway.metadata_ or {})
    if data.display_name is not None:
        current["display_name"] = data.display_name
    gateway.metadata_ = current

    await db.commit()
    await db.refresh(gateway)

    meta = gateway.metadata_ or {}
    return GatewayListResponse(
        serial_number=gateway.serial_number,
        display_name=meta.get("display_name") or None,
        metadata=meta,
        created_at=gateway.created_at,
        last_seen=None,
        csq=None,
        vgw=None,
        online=False,
        # last_seen/csq/vgw not needed for the update response — client already has them
    )


@router.delete("/{serial_number}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_gateway(
    serial_number: str,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_admin),
):
    """Deletes a gateway and all its monitoring data (admin only).

    Cascade:
    - monitoring.gateway_status: explicit delete (referenced by serial_number)
    - monitoring.gateway_config: CASCADE por FK gateway_id
    - core.device.gateway_id: SET NULL por FK ON DELETE SET NULL
    """
    result = await db.execute(select(Gateway).where(Gateway.serial_number == serial_number))
    gateway = result.scalar_one_or_none()
    if gateway is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Gateway '{serial_number}' not found",
        )

    await db.execute(
        delete(GatewayStatus).where(GatewayStatus.serial_number == serial_number)
    )
    await db.delete(gateway)
    await db.commit()


@router.get("/{serial_number}/export")
async def export_gateway(
    serial_number: str,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
):
    """Exporta todos los datos de un gateway como JSON backup."""
    result = await db.execute(select(Gateway).where(Gateway.serial_number == serial_number))
    gateway = result.scalar_one_or_none()
    if gateway is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Gateway '{serial_number}' not found",
        )

    status_result = await db.execute(
        select(GatewayStatus)
        .where(GatewayStatus.serial_number == serial_number)
        .order_by(GatewayStatus.ts.asc())
    )
    status_rows = status_result.scalars().all()

    config_result = await db.execute(
        text("""
            SELECT config_type, received_at, broker, port, client_id,
                   fw_type, topic_prefix, interval_s, supply_mv, broker2
            FROM monitoring.gateway_config
            WHERE serial_number = :sn
            ORDER BY received_at ASC
        """),
        {"sn": serial_number},
    )
    config_rows = config_result.mappings().all()

    return {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "gateway": {
            "serial_number": gateway.serial_number,
            "created_at": gateway.created_at.isoformat(),
        },
        "status_history": [
            {
                "ts": s.ts.isoformat(),
                "csq": s.csq,
                "erf": s.erf,
                "rst": s.rst,
                "vgw": s.vgw,
            }
            for s in status_rows
        ],
        "config_history": [
            {**dict(row), "received_at": row["received_at"].isoformat()}
            for row in config_rows
        ],
    }
