"""
Recovery of uplink frames by seq BATCH (gap_recovery V3).

The job detects the missing gw_seq (compute_gw_seq_gaps) and asks the gateway for exactly those,
in a batch, with `read seqs:[...]` (one read at a time -> no read_busy; the firmware recommends
it). The response carries the frames found (`lines`) and the unrecoverable ones (`missing`).
Per-seq state in monitoring.gw_seq_recovery (pending -> inflight -> recovered / not_found).

This module brings together:
- the periodic job (run_gap_recovery / gap_recovery_once)
- selecting + publishing the batch (serialized per gateway, with batch_id)
- the response handler (handle_storage_response): correlation, ingestion, state transitions
"""

import asyncio
import json
import logging
import time
import uuid as uuid_module
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..database import async_session
from ..metrics import (
    GAP_RECOVERY_BATCHES_REQUESTED,
    GAP_RECOVERY_INFLIGHT,
    GAP_RECOVERY_LAST_RUN,
    GAP_RECOVERY_NOT_FOUND,
    GAP_RECOVERY_PENDING,
    GAP_RECOVERY_READ_BUSY,
    GAP_RECOVERY_RECOVERED,
)
from .gap_detection import compute_gw_seq_gaps
from .ingestion import IngestResult, ingestion_service
from .payload_parser import parse_gateway_rx

logger = logging.getLogger(__name__)

_run_lock = asyncio.Lock()

# How many missing gw_seq are recorded (UPSERT to pending) at most per job run. It used to
# be batch_size*4 (80) — insufficient against gaps of hundreds/thousands of seqs: since
# gap_detection sorts gaps by timestamp descending, a large recent gap would hog the quota and
# the older gaps would never finish being fully inserted (they stayed truncated forever, see
# the comment in _recover_gateway). It doesn't affect how many are REQUESTED per MQTT batch
# (that's still limited by GAP_RECOVERY_MAX_BATCH_WIFI/LTE) — only how many are
# detected/persisted as pending per run.
_GAP_DETECTION_LIMIT = 5000


async def _emit_event(
    session: AsyncSession, gw_serial: str, event_type: str, severity: str, payload: dict
) -> None:
    """Emits a domain event into monitoring.event (Event Feed). entity_type=gateway."""
    await session.execute(
        text("""
            INSERT INTO monitoring.event
                (id, occurred_at, event_type, entity_type, entity_id,
                 serial_number, severity, payload)
            SELECT :id, now(), :event_type, 'gateway', g.id,
                   :serial, :severity, cast(:payload as jsonb)
            FROM core.gateway g WHERE g.serial_number = :serial
        """),
        {
            "id": str(uuid_module.uuid4()),
            "event_type": event_type,
            "serial": gw_serial,
            "severity": severity,
            "payload": json.dumps(payload),
        },
    )


# ---------------------------------------------------------------------------
# Response handler (invoked by the consumer from gateway/{id}/storage/data)
# ---------------------------------------------------------------------------

async def handle_storage_response(gw_serial: str, wrapper: dict) -> None:
    """Processes a `storage read seqs[...]` response (V3). Invoked by the consumer.

    Correlation by seq-set (match against inflight). Validates a partial batch. Ingests lines[],
    marks seqs recovered/not_found (missing verified against raw).
    """
    msg_type = wrapper.get("type")

    if msg_type == "storage_error":
        error = wrapper.get("error")
        logger.warning(f"storage_error from gw {gw_serial}: {error}")
        if error == "read_busy":
            GAP_RECOVERY_READ_BUSY.labels(gw=gw_serial).inc()
        # Does NOT change the state of the inflight ones -> the timeout returns them to pending
        # and they're retried.
        return

    if msg_type != "storage_read":
        logger.debug(f"ignoring non-storage_read on storage/data from gw {gw_serial}")
        return

    resp_seqs = wrapper.get("seqs")
    if resp_seqs is None:
        # read by offset/single seq (manual diagnostics) — not from our batch job
        logger.debug(f"storage_read without seqs from gw {gw_serial}, ignoring in job")
        return
    resp_set = {int(s) for s in resp_seqs}
    lines = wrapper.get("lines") or []
    missing = {int(s) for s in (wrapper.get("missing") or [])}
    found_count = wrapper.get("found_count", len(lines))
    requested = wrapper.get("requested", len(resp_set))
    now = datetime.now(timezone.utc)

    # 1. correlation: the response's seqs must match (SET) one of our inflight batches.
    async with async_session() as session:
        inflight_rows = (
            await session.execute(
                text("""
                    SELECT gw_seq FROM monitoring.gw_seq_recovery
                    WHERE gw_serial = :gw AND status = 'inflight'
                """),
                {"gw": gw_serial},
            )
        ).all()
        inflight_set = {r.gw_seq for r in inflight_rows}

    if not resp_set or not resp_set.issubset(inflight_set):
        # old / out-of-sync / different-origin response -> discard (don't mix batches)
        logger.warning(
            f"storage_read seqs don't match inflight for gw {gw_serial} -> "
            "discarding (stale response)"
        )
        return

    # 2. validate partial batch: every requested seq must appear in lines or in missing.
    if found_count + len(missing) != requested:
        logger.warning(
            f"invalid batch gw {gw_serial}: found({found_count})+missing({len(missing)})"
            f" != requested({requested}) -> leaving inflight (timeout will retry)"
        )
        return  # do NOT touch states; the timeout returns them to pending

    # 3. ingest the frames found
    inserted_seqs: list[int] = []
    async with async_session() as session:
        for line in lines:
            seq = line.get("seq")
            if seq is None:
                continue
            seq = int(seq)
            res = parse_gateway_rx(line, gw_serial=gw_serial)
            if not res.ok or res.payload is None:
                logger.warning(f"parse failed ({res.reason}) gw {gw_serial} s={seq}")
                # leave the seq inflight -> the timeout returns it to pending (retryable)
                continue
            ingest_res = await ingestion_service.ingest_recovered_frame(
                res.payload, source="storage_scan"
            )
            if ingest_res == IngestResult.FAILED:
                continue  # leave inflight -> retry
            # INSERTED or ALREADY_PRESENT -> recovered
            await session.execute(
                text("""
                    UPDATE monitoring.gw_seq_recovery
                    SET status = 'recovered', recovered_at = :now,
                        last_response_at = :now, inflight_at = NULL, batch_id = NULL
                    WHERE gw_serial = :gw AND gw_seq = :seq
                """),
                {"now": now, "gw": gw_serial, "seq": seq},
            )
            if ingest_res == IngestResult.INSERTED:
                inserted_seqs.append(seq)

        # 4. missing: verify against raw BEFORE marking not_found (it may have arrived live via /rx)
        lost_seqs: list[int] = []
        for seq in missing:
            in_raw = (
                await session.execute(
                    text("""
                        SELECT EXISTS (SELECT 1 FROM raw.telemetry_payload r
                            WHERE r.payload->>'gw' = :gw AND (r.payload->>'seq')::bigint = :seq)
                    """),
                    {"gw": gw_serial, "seq": seq},
                )
            ).scalar_one()
            if in_raw:
                # already in raw (arrived live) -> recovered, not not_found
                await session.execute(
                    text("""
                        UPDATE monitoring.gw_seq_recovery
                        SET status = 'recovered', recovered_at = :now,
                            last_response_at = :now, inflight_at = NULL, batch_id = NULL
                        WHERE gw_serial = :gw AND gw_seq = :seq
                    """),
                    {"now": now, "gw": gw_serial, "seq": seq},
                )
            else:
                await session.execute(
                    text("""
                        UPDATE monitoring.gw_seq_recovery
                        SET status = 'not_found', reason = 'NOT_FOUND',
                            last_response_at = :now, inflight_at = NULL, batch_id = NULL
                        WHERE gw_serial = :gw AND gw_seq = :seq
                    """),
                    {"now": now, "gw": gw_serial, "seq": seq},
                )
                lost_seqs.append(seq)

        # 5. domain events (lightweight payload). recovered ONLY the real INSERTED ones.
        if inserted_seqs:
            await _emit_event(
                session, gw_serial, "gateway.uplink_gap_recovered", "info",
                {"gateway_serial": gw_serial, "recovered_count": len(inserted_seqs),
                 "first_seq": min(inserted_seqs), "last_seq": max(inserted_seqs)},
            )
        for seq in lost_seqs:
            await _emit_event(
                session, gw_serial, "gateway.uplink_gap_lost", "warning",
                {"gateway_serial": gw_serial, "seq": seq, "reason": "NOT_FOUND"},
            )
        await session.commit()

    if inserted_seqs:
        GAP_RECOVERY_RECOVERED.labels(gw=gw_serial).inc(len(inserted_seqs))
    if lost_seqs:
        GAP_RECOVERY_NOT_FOUND.labels(gw=gw_serial).inc(len(lost_seqs))


# ---------------------------------------------------------------------------
# Sweep job
# ---------------------------------------------------------------------------

async def gap_recovery_once() -> int:
    """One run of the V3 job. Returns how many batches were requested. Anti-overlap lock."""
    if _run_lock.locked():
        logger.warning("gap_recovery already in progress, skipping cycle")
        return 0

    batches = 0
    async with _run_lock:
        GAP_RECOVERY_LAST_RUN.set(time.time())
        async with async_session() as session:
            gateways = await _gateways_with_recent_traffic(session)
        for gw_serial in gateways:
            batches += await _recover_gateway(gw_serial)
        await _refresh_gauges()
    return batches


async def _gateways_with_recent_traffic(session: AsyncSession) -> list[str]:
    """Gateways with at least one /rx in the window (same source as the gap computation)."""
    rows = (
        await session.execute(
            text("""
                SELECT payload->>'gw' AS gw, MAX(received_at) AS last_rx
                FROM raw.telemetry_payload
                WHERE payload ? 'raw' AND payload ? 'gw'
                  AND received_at > now() - make_interval(hours => :hours)
                GROUP BY payload->>'gw'
            """),
            {"hours": settings.gap_recovery_window_hours},
        )
    ).all()
    return [r.gw for r in rows if r.gw]


async def _gateway_batch_size(session: AsyncSession, gw_serial: str) -> int:
    """Max seqs per batch according to the gateway's transport (WiFi 20 / LTE 5, firmware limit).

    Determines the gateway's transport to pick the firmware limit (WiFi <=20 / LTE <=5):
    1) `net_iface` from gateway_config_v3 (Gateway LoRa v1.0.0, reliable source — published by the
       firmware itself in `connect.net_iface`).
    2) fw_type from gateway_config (legacy, 416B binary config).
    3) signal from the latest gateway_status: `lte_csq` present with a value -> LTE; otherwise
       `wifi_rssi` present -> WiFi. If it can't be determined, conservative (LTE/5).

    Historical note: (2) used to look at `raw_payload->>'csq'`, which never existed in the v3
    firmware's `/telemetry` (the real field is `lte_csq`, see README "csq <- lte_csq from the
    /telemetry payload") — and it only compared the presence of `wifi_rssi`, which the firmware
    ALWAYS sends (even on LTE, with value 0). Result: every LTE gateway was classified as WiFi ->
    batch of 20 instead of 5, exceeding the firmware's real limit. The explicit `net_iface` from
    gateway_config_v3 avoids relying on this inference in the common case (gateway already did a
    gw_get at least once).
    """
    v3_row = (
        await session.execute(
            text("""
                SELECT net_iface FROM monitoring.gateway_config_v3
                WHERE serial_number = :gw
            """),
            {"gw": gw_serial},
        )
    ).first()
    iface = (v3_row.net_iface or "").lower() if v3_row and v3_row.net_iface else ""
    if iface == "lte":
        return settings.gap_recovery_max_batch_lte
    if iface == "wifi":
        return settings.gap_recovery_max_batch_wifi

    fw_row = (
        await session.execute(
            text("""
                SELECT fw_type FROM monitoring.gateway_config
                WHERE serial_number = :gw ORDER BY received_at DESC LIMIT 1
            """),
            {"gw": gw_serial},
        )
    ).first()
    fw = (fw_row.fw_type or "").lower() if fw_row and fw_row.fw_type else ""
    if "lte" in fw or "4g" in fw or "modem" in fw:
        return settings.gap_recovery_max_batch_lte
    if "wifi" in fw:
        return settings.gap_recovery_max_batch_wifi

    # no net_iface nor fw_type: infer from the latest gateway_status (lte_csq with a value -> LTE;
    # otherwise, wifi_rssi present -> WiFi)
    sig = (
        await session.execute(
            text("""
                SELECT raw_payload->>'wifi_rssi' AS wifi, raw_payload->>'lte_csq' AS lte_csq
                FROM monitoring.gateway_status
                WHERE serial_number = :gw AND raw_payload IS NOT NULL
                ORDER BY ts DESC LIMIT 1
            """),
            {"gw": gw_serial},
        )
    ).first()
    if sig is not None:
        if sig.lte_csq is not None:
            return settings.gap_recovery_max_batch_lte
        if sig.wifi is not None:
            return settings.gap_recovery_max_batch_wifi
    return settings.gap_recovery_max_batch_lte  # unknown -> conservative


async def _recover_gateway(gw_serial: str) -> int:
    """Requests ONE batch of missing seqs from the gateway (V3). Returns 1 if it published
    a batch, 0 if not.

    Serialized (one read at a time per gw): if there's a non-expired inflight batch, skip.
    DETECTS new gaps with compute_gw_seq_gaps (window) and UPSERTs them to pending, but SELECTS
    what to request FROM THE TABLE (status='pending') — this way pending items that age out of
    the detection window do NOT end up orphaned. Marks the N chosen ones inflight with batch_id
    and publishes read seqs[...]. The handler processes the response asynchronously.
    """
    async with async_session() as session:
        # 1. one-read-at-a-time guard: is there an inflight batch for this gw?
        inflight = (
            await session.execute(
                text("""
                    SELECT count(*) AS n, min(inflight_at) AS since
                    FROM monitoring.gw_seq_recovery
                    WHERE gw_serial = :gw AND status = 'inflight'
                """),
                {"gw": gw_serial},
            )
        ).one()
        if inflight.n > 0:
            age = (
                (datetime.now(timezone.utc) - inflight.since).total_seconds()
                if inflight.since else 1e9
            )
            if age < settings.gap_recovery_inflight_timeout_s:
                return 0  # batch in flight, not expired -> wait
            # expired (response never arrived) -> return inflight -> pending
            await session.execute(
                text("""
                    UPDATE monitoring.gw_seq_recovery
                    SET status = 'pending', inflight_at = NULL, batch_id = NULL
                    WHERE gw_serial = :gw AND status = 'inflight'
                """),
                {"gw": gw_serial},
            )
            await session.commit()

        # 2a. DETECT new gaps (window) -> UPSERT to pending the ones that are NOT in raw and are not
        #     terminal. Only records them; the REQUEST is made from the table (2b), not the window.
        batch_size = await _gateway_batch_size(session, gw_serial)
        result = await compute_gw_seq_gaps(session, gw_serial, settings.gap_recovery_window_hours)
        # Detection limit per run: gap_detection sorts gaps by timestamp descending
        # (most recent first, see GwSeqGapResult.gaps.sort in gap_detection.py). With a small
        # limit (previously: batch_size*4 = 80), a large recent gap hogs the quota and the older
        # gaps end up inserted only PARTIALLY forever — they never finish being recorded as
        # 'pending' nor get requested in full when several large gaps happen concurrently.
        # _GAP_DETECTION_LIMIT is generous to cover several large concurrent gaps in one run.
        missing_seqs = result.all_missing_seqs[:_GAP_DETECTION_LIMIT]
        if missing_seqs:
            # UPSERT in a single statement (unnest), not one INSERT per seq — with a high
            # _GAP_DETECTION_LIMIT (5000) an individual loop would be too many roundtrips per run.
            await session.execute(
                text("""
                    INSERT INTO monitoring.gw_seq_recovery (gw_serial, gw_seq, status)
                    SELECT :gw, s, 'pending'
                    FROM unnest(CAST(:seqs AS bigint[])) AS s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM raw.telemetry_payload r
                        WHERE r.payload->>'gw' = :gw AND (r.payload->>'seq')::bigint = s)
                    ON CONFLICT (gw_serial, gw_seq) DO NOTHING
                """),
                {"gw": gw_serial, "seqs": missing_seqs},
            )

        # 2b. SELECT what to request FROM THE TABLE (not from the window). This way old pending
        #     items that already fell outside the detection window do NOT end up orphaned —
        #     they keep being requested. First, auto-resolve pending items that are already in
        #     raw (ghosts / arrived live) -> recovered without requesting them.
        await session.execute(
            text("""
                UPDATE monitoring.gw_seq_recovery g
                SET status = 'recovered', recovered_at = now()
                WHERE g.gw_serial = :gw AND g.status = 'pending'
                  AND EXISTS (SELECT 1 FROM raw.telemetry_payload r
                              WHERE r.payload->>'gw' = g.gw_serial
                                AND (r.payload->>'seq')::bigint = g.gw_seq)
            """),
            {"gw": gw_serial},
        )
        pick = (
            await session.execute(
                text("""
                    SELECT gw_seq FROM monitoring.gw_seq_recovery
                    WHERE gw_serial = :gw AND status = 'pending'
                    ORDER BY gw_seq LIMIT :n
                """),
                {"gw": gw_serial, "n": batch_size},
            )
        ).all()
        seqs = [r.gw_seq for r in pick]

        if not seqs:
            return 0  # nothing pending to request

        # 3. mark those seqs inflight with a batch_id and UPSERT (only these N, not thousands)
        batch_id = str(uuid_module.uuid4())
        now = datetime.now(timezone.utc)
        for seq in seqs:
            await session.execute(
                text("""
                    INSERT INTO monitoring.gw_seq_recovery
                        (gw_serial, gw_seq, status, inflight_at, batch_id)
                    VALUES (:gw, :seq, 'inflight', :now, :bid)
                    ON CONFLICT (gw_serial, gw_seq) DO UPDATE
                        SET status = 'inflight', inflight_at = :now, batch_id = :bid
                """),
                {"gw": gw_serial, "seq": seq, "now": now, "bid": batch_id},
            )
        await session.commit()

    # 4. publish the batch (outside the transaction). If it fails -> return to pending.
    #    Via CommandService (unified metric + log). inject_id=False: the response is
    #    correlated by the SET of seqs against the batch_id, not by the command's id.
    from .command_service import send_command

    ok = await send_command(
        gw_serial, {"target": "storage", "action": "read", "seqs": seqs}, inject_id=False
    )
    if not ok:
        async with async_session() as session:
            await session.execute(
                text("""
                    UPDATE monitoring.gw_seq_recovery
                    SET status = 'pending', inflight_at = NULL, batch_id = NULL
                    WHERE gw_serial = :gw AND batch_id = :bid
                """),
                {"gw": gw_serial, "bid": batch_id},
            )
            await session.commit()
        return 0
    GAP_RECOVERY_BATCHES_REQUESTED.labels(gw=gw_serial).inc()
    logger.info(
        f"gap_recovery: batch of {len(seqs)} seqs requested from gw {gw_serial} "
        f"(batch {batch_id[:8]})"
    )
    return 1


async def _refresh_gauges() -> None:
    """Sets the pending/inflight gauges per gateway."""
    async with async_session() as session:
        rows = (
            await session.execute(
                text("""
                    SELECT gw_serial,
                           count(*) FILTER (WHERE status = 'pending')  AS pending,
                           count(*) FILTER (WHERE status = 'inflight') AS inflight
                    FROM monitoring.gw_seq_recovery
                    GROUP BY gw_serial
                """)
            )
        ).all()
    for r in rows:
        GAP_RECOVERY_PENDING.labels(gw=r.gw_serial).set(r.pending)
        GAP_RECOVERY_INFLIGHT.labels(gw=r.gw_serial).set(r.inflight)


async def run_gap_recovery() -> None:
    """Background task: runs gap_recovery_once every gap_recovery_interval_s.
    Sequential (sleep after) -> no overlap. Does not re-raise exceptions, to keep the task alive."""
    if not settings.gap_recovery_enabled:
        logger.info("gap_recovery disabled (GAP_RECOVERY_ENABLED=false)")
        return
    while True:
        try:
            n = await gap_recovery_once()
            if n:
                logger.info("gap_recovery: %d batches requested", n)
        except Exception as e:  # noqa: BLE001 — keep the task alive
            logger.error("gap_recovery run failed: %s", e)
        await asyncio.sleep(settings.gap_recovery_interval_s)
