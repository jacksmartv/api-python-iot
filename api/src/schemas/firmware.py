"""
Pydantic schemas for OTA firmware releases (.bin) uploaded to S3.
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class FirmwareReleaseResponse(BaseModel):
    id: UUID
    original_filename: str
    version: str
    target: str
    s3_key: str
    public_url: str
    size_bytes: int
    checksum_sha256: str
    etag: str | None = None
    created_by: UUID | None = None
    created_by_email: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}
