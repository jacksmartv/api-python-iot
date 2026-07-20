"""
OTA firmware deployment to a gateway via MQTT command, and processing of the result ack.

Two separate transactions (not a single one held open during the MQTT publish), so as not to
hold Postgres locks/connection while waiting for aiomqtt confirmation:
  1. create firmware_deployment + firmware_deployment_gateway as 'pending', commit.
  2. publish the ota command (send_command).
  3. if ok: UPDATE to 'command_sent' + request_id, commit. If it fails: UPDATE with error_detail,
     commit, and MqttUnavailable is raised — the row stays 'pending' (not rolled back, transaction
     1 already closed).

handle_ota_ack (gateway/{serial}/cmd/ack) and handle_ota_event (gateway/{serial}/events, type
ota_ok/ota_fail) are TWO INDEPENDENT SOURCES of the same OTA result — either one can arrive
first, or be missing (if the ack is lost but the event arrives, or vice versa, the deployment
still gets closed). They are not redundant with each other: they share the same state transition
via _apply_ota_result, but each is a real signal from the firmware that may be the only one to
arrive.
"""

import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import async_session
from ..metrics import FIRMWARE_OTA_DURATION, FIRMWARE_OTA_RESULT
from ..models import (
    FirmwareDeployment,
    FirmwareDeploymentGateway,
    FirmwareDeploymentStatus,
    FirmwareRelease,
)
from .command_service import send_command

logger = logging.getLogger(__name__)


class FirmwareReleaseNotFound(Exception):
    pass


class MqttUnavailable(Exception):
    pass


async def deploy_firmware(
    db: AsyncSession,
    serial_number: str,
    firmware_release_id: UUID,
    user_id: UUID | None,
) -> FirmwareDeploymentGateway:
    """Creates deployment+deployment_gateway and publishes the ota command to the gateway.

    Raises FirmwareReleaseNotFound / MqttUnavailable — the router translates them to 404/503,
    this function has no knowledge of HTTP.
    """
    release = (
        await db.execute(
            select(FirmwareRelease).where(FirmwareRelease.id == firmware_release_id)
        )
    ).scalar_one_or_none()
    if release is None:
        raise FirmwareReleaseNotFound(str(firmware_release_id))

    deployment = FirmwareDeployment(firmware_release_id=release.id, created_by=user_id)
    db.add(deployment)
    await db.flush()

    deployment_gateway = FirmwareDeploymentGateway(
        deployment_id=deployment.id,
        gw_serial=serial_number,
        status="pending",
    )
    db.add(deployment_gateway)
    await db.commit()
    await db.refresh(deployment_gateway)

    command = {
        "target": "gw",
        "action": "ota",
        "params": {
            "url": release.public_url,
            "version": release.version,
            "sha256": release.checksum_sha256,
        },
    }
    result = await send_command(serial_number, command)

    if not result.ok:
        deployment_gateway.error_detail = "Could not publish the command (MQTT unavailable)."
        await db.commit()
        raise MqttUnavailable(serial_number)

    if result.request_id is None:
        raise RuntimeError("send_command.ok without request_id — precondition violation")

    deployment_gateway.status = "command_sent"
    deployment_gateway.request_id = UUID(result.request_id)
    deployment_gateway.command_sent_at = func.now()
    await db.commit()
    await db.refresh(deployment_gateway)
    return deployment_gateway


async def _apply_ota_result(
    request_id: UUID,
    # gw_serial is only for metric labels (FIRMWARE_OTA_RESULT/_DURATION), not used for correlation
    gw_serial: str,
    new_status: FirmwareDeploymentStatus,
    error_detail: str | None,
) -> bool:
    """UPDATE shared by handle_ota_ack and handle_ota_event — same idempotency rule for
    both sources (cmd/ack and /events): only transitions from pending/command_sent, preserves
    acked_at if it was already set (the first signal to arrive wins), and does not overwrite
    error_detail with None if one was already present. Rows already in a terminal state
    (success/failed/timeout) are left untouched — QoS 1 can redeliver the same message more
    than once.

    Increments FIRMWARE_OTA_RESULT/_DURATION only when the transition is actually applied (not
    on ignored redeliveries).

    Returns True if applied, False if the row didn't exist or was already in a terminal state.
    """
    async with async_session() as session:
        result = await session.execute(
            text("""
                UPDATE core.firmware_deployment_gateway
                SET status = :new_status, error_detail = COALESCE(:error_detail, error_detail),
                    acked_at = COALESCE(acked_at, NOW())
                WHERE request_id = :request_id
                  AND status IN ('pending', 'command_sent')
                RETURNING command_sent_at, acked_at
            """),
            {"new_status": new_status, "error_detail": error_detail, "request_id": request_id},
        )
        row = result.first()
        await session.commit()

    if row is None:
        return False

    FIRMWARE_OTA_RESULT.labels(gw=gw_serial, result=new_status).inc()
    command_sent_at: datetime | None = row.command_sent_at
    acked_at: datetime | None = row.acked_at
    if command_sent_at is not None and acked_at is not None:
        FIRMWARE_OTA_DURATION.observe((acked_at - command_sent_at).total_seconds())
    return True


async def handle_ota_ack(gw_serial: str, wrapper: dict) -> None:
    """Processes gateway/{serial}/cmd/ack with action='ota'.

    Correlates by wrapper["id"] (request_id that the gateway reflects back, the same field
    already used by the gw_get ack). See _apply_ota_result for the idempotency rule shared with
    handle_ota_event (the other source of the same result, see the module docstring).
    """
    raw_id = wrapper.get("id")
    if raw_id is None:
        logger.warning(
            "ota ack without id, cannot correlate", extra={"device_serial": gw_serial}
        )
        return
    try:
        request_id = UUID(str(raw_id))
    except (ValueError, TypeError):
        logger.warning(
            "ota ack with non-UUID id, cannot correlate",
            extra={"device_serial": gw_serial, "id": raw_id},
        )
        return

    ok = wrapper.get("ok", False)
    new_status: FirmwareDeploymentStatus = "success" if ok else "failed"
    error_detail = wrapper.get("error") if not ok else None

    applied = await _apply_ota_result(request_id, gw_serial, new_status, error_detail)
    if applied:
        logger.info(
            f"ota ack applied: status={new_status}",
            extra={"device_serial": gw_serial, "request_id": str(request_id)},
        )
    else:
        logger.info(
            "ota ack not applied (unknown request_id or row already in terminal state)",
            extra={"device_serial": gw_serial, "request_id": str(request_id)},
        )


async def handle_ota_event(gw_serial: str, event: dict) -> None:
    """Processes gateway/{serial}/events with type in (ota_start, ota_ok, ota_fail).

    handle_ota_ack (cmd/ack) and this handler (/events) are TWO INDEPENDENT SOURCES of the same
    OTA result — either one can arrive first or be missing; both share exactly the same state
    transition via _apply_ota_result(). They are not redundant with each other.

    Correlates ota_ok/ota_fail by event['detail'] (assumption pending validation: assumed to
    equal the request_id sent in the original ota command — if the firmware uses a different
    identifier, this correlator will need to be adapted)."""
    etype = event.get("type")
    if etype == "ota_start":
        logger.info(
            "ota_start received (informational, does not correlate by id)",
            extra={"device_serial": gw_serial, "fw": event.get("fw"), "url": event.get("detail")},
        )
        return
    if etype not in ("ota_ok", "ota_fail"):
        return

    try:
        request_id = UUID(str(event.get("detail")))
    except (ValueError, TypeError):
        logger.warning(
            f"{etype} without valid detail UUID, cannot correlate",
            extra={"device_serial": gw_serial, "detail": event.get("detail")},
        )
        return

    new_status: FirmwareDeploymentStatus = "success" if etype == "ota_ok" else "failed"
    applied = await _apply_ota_result(request_id, gw_serial, new_status, error_detail=None)
    if applied:
        logger.info(
            f"{etype} applied: status={new_status}",
            extra={"device_serial": gw_serial, "request_id": str(request_id)},
        )
        return
    # distinguish "that request_id doesn't exist" from "was already terminal" — this helps a lot
    # in real troubleshooting, worth the extra SELECT just for the log.
    async with async_session() as session:
        exists = await session.execute(
            text("SELECT 1 FROM core.firmware_deployment_gateway WHERE request_id = :rid"),
            {"rid": request_id},
        )
    reason = (
        "deployment already finished (terminal state)"
        if exists.first() is not None
        else "deployment not found"
    )
    logger.info(
        f"{etype} not applied: {reason}",
        extra={"device_serial": gw_serial, "request_id": str(request_id)},
    )
