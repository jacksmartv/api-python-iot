"""
Storage abstraction for files from the Spatial module (floor plans) and OTA firmware.

V1: LocalStorage (filesystem on a Docker volume) for floorplans. S3Storage (boto3) for firmware
— its own bucket/credentials, separate from floorplans (see get_firmware_storage()). GCS remains
an explicit stub behind the same interface for V2/prod.
"""

import asyncio
import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from ..config import settings

logger = logging.getLogger(__name__)


class StorageBackend:
    """Minimal storage interface. public_url is synchronous (doesn't touch disk)."""

    async def save(self, key: str, data: bytes, content_type: str) -> str:
        raise NotImplementedError

    async def delete(self, key: str) -> None:
        raise NotImplementedError

    def public_url(self, key: str) -> str:
        raise NotImplementedError


class LocalStorage(StorageBackend):
    """Stores files in a local base directory, served via StaticFiles."""

    def __init__(self, base_dir: str, url_prefix: str):
        self.base = Path(base_dir).resolve()
        self.url_prefix = url_prefix.rstrip("/")

    def _resolve(self, key: str) -> Path:
        """Resolves key within base. Blocks path traversal."""
        path = (self.base / key).resolve()
        if not path.is_relative_to(self.base):
            raise ValueError(f"blocked path traversal: {key!r}")
        return path

    async def save(self, key: str, data: bytes, content_type: str) -> str:
        path = self._resolve(key)
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(path.write_bytes, data)
        return self.public_url(key)

    async def delete(self, key: str) -> None:
        path = self._resolve(key)
        await asyncio.to_thread(path.unlink, True)  # missing_ok=True

    def public_url(self, key: str) -> str:
        return f"{self.url_prefix}/{key}"


class S3Storage(StorageBackend):
    """Uploads to S3 using synchronous boto3 wrapped in asyncio.to_thread (same pattern as
    LocalStorage with path.write_bytes). Credentials: boto3's standard chain — the instance
    profile's IAM role on EC2 (dev/prod), or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY from the
    environment in local docker (not read from explicit settings.*, boto3 already picks them up
    on its own if they're in the environment).
    """

    def __init__(self, bucket: str, region: str, url_prefix: str):
        self.bucket = bucket
        self.region = region
        self.url_prefix = url_prefix.rstrip("/")
        self._client = boto3.client("s3", region_name=region)

    async def save(self, key: str, data: bytes, content_type: str) -> str:
        await asyncio.to_thread(
            self._client.put_object,
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        return self.public_url(key)

    async def put_object_with_metadata(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
        cache_control: str,
        metadata: dict[str, str],
    ) -> tuple[str, str | None]:
        """Like save(), but allows CacheControl/Metadata and also returns S3's ETag
        (an identifier returned by put_object, not a cryptographic validation mechanism —
        for simple objects it's usually the MD5 of the content, but it stops being that for
        multipart uploads). Used by firmware to persist the ETag alongside its own
        checksum_sha256."""
        result = await asyncio.to_thread(
            self._client.put_object,
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            ContentLength=len(data),
            CacheControl=cache_control,
            Metadata=metadata,
        )
        etag = result.get("ETag", "").strip('"') or None
        return self.public_url(key), etag

    async def delete(self, key: str) -> None:
        try:
            await asyncio.to_thread(self._client.delete_object, Bucket=self.bucket, Key=key)
        except ClientError as e:
            logger.warning("s3 delete failed: %s", e)

    async def delete_strict(self, key: str) -> None:
        """Like delete(), but propagates ClientError instead of just logging — used by firmware,
        where a delete that fails in S3 but still deletes the row would leave an orphaned object
        paying for storage forever without anyone noticing."""
        await asyncio.to_thread(self._client.delete_object, Bucket=self.bucket, Key=key)

    def public_url(self, key: str) -> str:
        return f"{self.url_prefix}/{key}"


def get_storage() -> StorageBackend:
    """Factory based on settings.storage_provider (floorplans)."""
    if settings.storage_provider == "local":
        return LocalStorage(settings.storage_local_dir, settings.storage_url_prefix)
    raise NotImplementedError(
        f"storage_provider {settings.storage_provider!r} not implemented (S3/GCS: V2)"
    )


def get_firmware_storage() -> S3Storage:
    """Storage factory for firmware — its own bucket/prefix, separate from floorplans."""
    if settings.firmware_storage_provider != "s3":
        raise NotImplementedError(
            f"firmware_storage_provider {settings.firmware_storage_provider!r} "
            "must be 's3' outside of tests"
        )
    url_prefix = f"https://{settings.firmware_bucket}.s3.{settings.firmware_region}.amazonaws.com"
    return S3Storage(settings.firmware_bucket, settings.firmware_region, url_prefix)
