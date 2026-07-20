"""
System status endpoints: Alertmanager alert proxy.
"""

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from ..auth_jwt import require_any_role
from ..config import settings
from ..models.user import User

router = APIRouter(prefix="/status", tags=["status"])
logger = logging.getLogger(__name__)


@router.get("/alerts")
async def get_alerts(
    _user: User = Depends(require_any_role),
) -> list[dict[str, Any]]:
    """Proxy for active alerts from Alertmanager."""
    url = f"{settings.alertmanager_url}/api/v2/alerts"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                url,
                params={"active": "true", "silenced": "false", "inhibited": "false"},
            )
            response.raise_for_status()
            alerts = response.json()
            return [
                {
                    "fingerprint": a.get("fingerprint"),
                    "status": a.get("status", {}).get("state"),
                    "labels": a.get("labels", {}),
                    "annotations": a.get("annotations", {}),
                    "starts_at": a.get("startsAt"),
                    "generator_url": a.get("generatorURL"),
                }
                for a in alerts
            ]
    except httpx.ConnectError:
        return []
    except httpx.HTTPStatusError as e:
        logger.warning(f"Alertmanager returned {e.response.status_code}")
        return []
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        raise HTTPException(status_code=503, detail="Alert service unavailable")
