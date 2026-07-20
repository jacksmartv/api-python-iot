"""
Pydantic schemas for deploying OTA firmware to a gateway via MQTT command.

Distinct from firmware.py (the release catalog, Phase 1).
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class FirmwareDeployRequest(BaseModel):
    firmware_release_id: UUID

    model_config = {"extra": "forbid"}


class FirmwareDeployResponse(BaseModel):
    deployment_id: UUID
    gw_serial: str
    status: str
    request_id: UUID | None
    error_detail: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}
