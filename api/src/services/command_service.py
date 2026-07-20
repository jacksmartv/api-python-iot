"""Generic layer for outgoing MQTT commands to the gateway.

Centralizes sending commands to `gateway/{serial}/cmd`: generates a server-side `request_id`,
injects it as `"id"` into the command (the firmware reflects it back in the `cmd/ack` response),
publishes (qos 1), logs, and records the metric. Returns a CommandResult(ok, request_id) for
correlation.

All outgoing commands should go through here (gw get/set, gap recovery's storage read, node
OTA, reboot, LoRa downlink...), to share request_id + metrics + logs in one place. The low-level
transport is still `mqtt_consumer.publish_command`; this layer wraps it.
"""

import logging
import uuid

from ..metrics import COMMANDS_PUBLISHED

logger = logging.getLogger(__name__)


class CommandResult:
    """Result of send_command: `ok` (bool) for the flow, `request_id` for correlation.

    It is truthy iff the command was published (`bool(result)` == `result.ok`), so callers
    that only care about success/failure can do `if not await send_command(...)`.
    """

    __slots__ = ("ok", "request_id")

    def __init__(self, ok: bool, request_id: str | None):
        self.ok = ok
        self.request_id = request_id

    def __bool__(self) -> bool:
        return self.ok


async def send_command(gw_serial: str, command: dict, *, inject_id: bool = True) -> CommandResult:
    """Publishes an outgoing command to the gateway. Returns CommandResult(ok, request_id).

    - `command`: the command object (e.g. {"target":"gw","action":"get"}). Must NOT include `id`;
      this layer generates it (if `inject_id`).
    - `inject_id`: if True (default), generates a uuid and injects it as `command["id"]` to
      correlate with the cmd/ack response. Set to False only for commands whose correlation
      doesn't use the id (e.g. gap_recovery, which correlates by a set of seqs with its own
      batch_id).

    `result.ok` = it was published; `result.request_id` = the id (None if inject_id=False or if
    it failed). CommandResult is truthy iff ok, so the caller can do
    `if not await send_command(...)`.
    """
    # local import: mqtt_consumer doesn't import this module, but we keep the gap_recovery pattern
    from .mqtt_consumer import mqtt_consumer

    request_id = str(uuid.uuid4()) if inject_id else None
    payload = dict(command)
    if inject_id:
        payload["id"] = request_id

    target = str(payload.get("target", "unknown"))
    action = str(payload.get("action", "params" if "params" in payload else "unknown"))

    ok = await mqtt_consumer.publish_command(gw_serial, payload)
    COMMANDS_PUBLISHED.labels(
        target=target, action=action, result="ok" if ok else "failed"
    ).inc()

    if not ok:
        logger.warning(
            "command not published (mqtt unavailable)",
            extra={"device_serial": gw_serial, "target": target, "action": action},
        )
        return CommandResult(False, None)

    logger.info(
        "command published",
        extra={
            "device_serial": gw_serial,
            "target": target,
            "action": action,
            "request_id": request_id,
        },
    )
    return CommandResult(True, request_id)
