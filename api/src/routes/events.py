"""
Endpoints for the fleet event feed.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth_jwt import require_any_role
from ..database import get_db
from ..models import Device, FleetEvent, Gateway
from ..models.user import User

router = APIRouter(prefix="/events", tags=["events"])


@router.get("")
async def list_events(
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=200),
    event_type: str | None = Query(None),
    severity: str | None = Query(None),
    gateway_serial: str | None = Query(
        None, description="Events for the gateway AND its linked nodes"
    ),
) -> list[dict[str, Any]]:
    """Returns recent fleet events in descending order.

    Optional filters (general-purpose capability, not tied to a specific view): event_type,
    severity, gateway_serial. gateway_serial brings back events from the gateway itself plus
    those from its node devices (alarms carry the node's serial, not the gateway's).
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    stmt = (
        select(FleetEvent)
        .where(FleetEvent.occurred_at >= since)
        .order_by(FleetEvent.occurred_at.desc())
        .limit(limit)
    )
    if event_type:
        stmt = stmt.where(FleetEvent.event_type == event_type)
    if severity:
        stmt = stmt.where(FleetEvent.severity == severity)  # uses ix_event_severity
    if gateway_serial:
        # events from the gateway (own serial) OR from its nodes (entity_id ∈ gateway's devices)
        node_ids = (
            select(Device.id)
            .join(Gateway, Gateway.id == Device.gateway_id)
            .where(Gateway.serial_number == gateway_serial)
            .scalar_subquery()
        )
        stmt = stmt.where(
            or_(
                FleetEvent.serial_number == gateway_serial,
                FleetEvent.entity_id.in_(node_ids),
            )
        )

    result = await db.execute(stmt)
    rows = result.scalars().all()
    return [
        {
            "id": str(row.id),
            "occurred_at": row.occurred_at.isoformat(),
            "event_type": row.event_type,
            "entity_type": row.entity_type,
            "entity_id": str(row.entity_id),
            "serial_number": row.serial_number,
            "severity": row.severity,
            "payload": row.payload,
        }
        for row in rows
    ]
