"""
Endpoints for OTA firmware releases (.bin) — uploads to S3, registers in core.firmware_release.

Phase 1: upload + listing only. Deploying to a gateway via the MQTT "ota" command is deferred to
a future phase (the firmware doesn't support it yet).
"""

import hashlib
import logging
import re
from pathlib import Path
from uuid import UUID

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth_jwt import require_admin
from ..config import settings
from ..database import get_db
from ..models import FirmwareRelease, User
from ..schemas import FirmwareReleaseResponse
from ..services.storage import get_firmware_storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/firmware", tags=["firmware"])

# X.Y.Z with an optional -TAG suffix (up to 5 alphanumeric chars, e.g. -beta1, -rc1) — same
# pattern the frontend validates (FirmwarePage.tsx). Not full semver (ordering/parsing wasn't
# requested, see plan) — it just catches obvious typos like "1.0.a" or "v1".
_VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+(-[a-zA-Z0-9]{1,5})?$")


@router.post("", response_model=FirmwareReleaseResponse, status_code=status.HTTP_201_CREATED)
async def upload_firmware(
    file: UploadFile = File(...),
    version: str = Form(...),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_admin),
):
    version = version.strip()
    if not _VERSION_PATTERN.match(version):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="version must be in X.Y.Z format (e.g. 1.0.10)",
        )

    original_filename = Path(file.filename or "").name
    if not original_filename.lower().endswith(".bin"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The file must have a .bin extension",
        )

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")
    if len(raw) > settings.firmware_max_upload_mb * 1024 * 1024:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File exceeds the {settings.firmware_max_upload_mb}MB maximum",
        )

    existing = await db.execute(
        text("SELECT 1 FROM core.firmware_release WHERE version = :v"), {"v": version}
    )
    if existing.first() is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Version {version!r} was already uploaded (versions are immutable)",
        )

    checksum = hashlib.sha256(raw).hexdigest()
    s3_key = f"{settings.firmware_s3_prefix}/{version}/firmware.bin"
    storage = get_firmware_storage()
    try:
        public_url, etag = await storage.put_object_with_metadata(
            s3_key,
            raw,
            content_type="application/octet-stream",
            cache_control="public,max-age=31536000,immutable",
            metadata={"version": version, "sha256": checksum},
        )
    except ClientError as e:
        logger.error("firmware upload to S3 failed (version=%s): %s", version, e)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Could not upload the file to S3",
        ) from e

    release = FirmwareRelease(
        original_filename=original_filename,
        version=version,
        s3_key=s3_key,
        public_url=public_url,
        size_bytes=len(raw),
        checksum_sha256=checksum,
        etag=etag,
        created_by=user.id,
    )
    db.add(release)
    try:
        await db.commit()
    except IntegrityError as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Version {version!r} was already uploaded (versions are immutable)",
        ) from e
    await db.refresh(release)

    return FirmwareReleaseResponse(
        id=release.id,
        original_filename=release.original_filename,
        version=release.version,
        target=release.target,
        s3_key=release.s3_key,
        public_url=release.public_url,
        size_bytes=release.size_bytes,
        checksum_sha256=release.checksum_sha256,
        etag=release.etag,
        created_by=release.created_by,
        created_by_email=user.email,
        created_at=release.created_at,
    )


@router.get("", response_model=list[FirmwareReleaseResponse])
async def list_firmware(
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_admin),
):
    result = await db.execute(
        text("""
            SELECT r.id, r.original_filename, r.version, r.target, r.s3_key, r.public_url,
                   r.size_bytes, r.checksum_sha256, r.etag, r.created_by, r.created_at,
                   u.email AS created_by_email
            FROM core.firmware_release r
            LEFT JOIN core."user" u ON u.id = r.created_by
            ORDER BY r.created_at DESC
        """)
    )
    return [FirmwareReleaseResponse(**row._mapping) for row in result]


@router.delete("/{firmware_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_firmware(
    firmware_id: UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_admin),
):
    """Deletes both the row AND the actual object in S3 — unlike the upload's "immutable
    versions" policy (which prevents re-uploading the SAME version), this is an explicit,
    deliberate delete by the admin, not a replacement. No soft-delete: if recovery is needed,
    the .bin has to be re-uploaded (S3 bucket versioning can still keep the old object around,
    but there's no UI to restore versions)."""
    release = (
        await db.execute(select(FirmwareRelease).where(FirmwareRelease.id == firmware_id))
    ).scalar_one_or_none()
    if release is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Release not found")

    storage = get_firmware_storage()
    try:
        await storage.delete_strict(release.s3_key)
    except ClientError as e:
        logger.error("firmware delete from S3 failed (id=%s): %s", firmware_id, e)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Could not delete the file from S3",
        ) from e

    await db.delete(release)
    await db.commit()
