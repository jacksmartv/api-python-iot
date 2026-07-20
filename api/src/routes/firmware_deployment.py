"""
Endpoint for deploying OTA firmware to a specific gateway, via MQTT command.

Different from firmware.py (release catalog, Phase 1) — this is where the actual deploy to a
gateway is triggered.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth_jwt import require_admin
from ..database import get_db
from ..models import FirmwareDeploymentGateway, User
from ..schemas.firmware_deployment import FirmwareDeployRequest, FirmwareDeployResponse
from ..services.firmware_deployment import (
    FirmwareReleaseNotFound,
    MqttUnavailable,
    deploy_firmware,
)

router = APIRouter(prefix="/gateways", tags=["firmware-deployment"])


@router.post(
    "/{serial_number}/firmware/deploy",
    response_model=FirmwareDeployResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def deploy_firmware_to_gateway(
    serial_number: str,
    body: FirmwareDeployRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        row = await deploy_firmware(db, serial_number, body.firmware_release_id, user.id)
    except FirmwareReleaseNotFound as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Release not found"
        ) from e
    except MqttUnavailable as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Could not publish the command (MQTT unavailable). "
                "The deploy was recorded as pending."
            ),
        ) from e
    return FirmwareDeployResponse.model_validate(row)


@router.get(
    "/{serial_number}/firmware/deploy/{deployment_id}",
    response_model=FirmwareDeployResponse,
)
async def get_firmware_deploy_status(
    serial_number: str,
    deployment_id: UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_admin),
):
    """Point lookup of a deploy's status — used by the UI to poll for the final result
    (success/failed) after the initial POST, without needing a list/pagination."""
    row = (
        await db.execute(
            select(FirmwareDeploymentGateway).where(
                FirmwareDeploymentGateway.deployment_id == deployment_id,
                FirmwareDeploymentGateway.gw_serial == serial_number,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Deployment not found")
    return FirmwareDeployResponse.model_validate(row)
