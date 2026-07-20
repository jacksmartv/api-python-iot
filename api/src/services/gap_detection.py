"""
gw_seq gap detection (uplink gateway → backend).

Single source of the gap logic: used by the GET /gateways/{serial}/seq-gaps endpoint and the
automatic recovery job (services/gap_recovery.py). gw_seq is the 'seq' field of the /rx wrapper
that gets persisted in raw.telemetry_payload; it increments by 1 for every frame the gateway
relays.
"""

from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class GwSeqGap:
    """Gap in gw_seq: between after_seq and before_seq, `missing` intermediate frames
    are missing."""

    after_seq: int
    before_seq: int
    missing: int
    at: datetime

    @property
    def missing_seqs(self) -> list[int]:
        """The concrete gw_seq values missing in this gap (after_seq+1 .. before_seq-1)."""
        return list(range(self.after_seq + 1, self.before_seq))


@dataclass
class GwSeqGapResult:
    """Result of the gw_seq continuity analysis for a gateway over a window."""

    total_packets: int
    first_seq: int | None
    last_seq: int | None
    missing_total: int
    completeness_pct: float | None
    gaps: list[GwSeqGap] = field(default_factory=list)

    @property
    def all_missing_seqs(self) -> list[int]:
        """All missing gw_seq values (flattened from all gaps), ascending."""
        out: list[int] = []
        for gap in self.gaps:
            out.extend(gap.missing_seqs)
        return out


async def compute_gw_seq_gaps(
    db: AsyncSession,
    gw_serial: str,
    hours: int,
    max_gap: int = 1000,
    grace_period_s: int = 0,
) -> GwSeqGapResult:
    """Computes the gw_seq gaps of a gateway over the last `hours` hours.

    - Orders by seq value (NOT by ts: FIFO/replay reorders it), discards jumps >= max_gap
      (gateway reset/reboot/wraparound) and negative ones.
    - The `hours` window filters by **the frame's `ts_ms`** (when the gateway generated it), NOT
      by `received_at` (when the backend inserted it). These differ when gap_recovery brings back
      old frames: they end up with `received_at`=now but `ts_ms` stays the original value.
      Filtering by `received_at` would pull those recovered frames into the "recent" window and
      generate artificial gaps between the recovered block (old seqs) and live traffic (new
      seqs) — the same gateway could show spans of thousands of seqs and completeness ~5% even
      though the actual traffic from the last 24h was intact. Every `/rx` frame carries `ts_ms`
      (see parse_gateway_rx).
    - `grace_period_s`: discards gaps whose upper edge is less than N seconds from the last
      received frame — a frame that just arrived may not yet be on the gateway's SD card
      (a storage read would give a spurious found:false). Still compares by `received_at`
      (insertion freshness, not occurrence). Default 0 = no grace (original endpoint behavior).
    """
    rows = (
        await db.execute(
            text("""
                WITH seqs AS (
                    SELECT (payload->>'seq')::bigint AS seq, received_at,
                           to_timestamp((payload->>'ts_ms')::bigint / 1000.0) AS frame_ts,
                           LAG((payload->>'seq')::bigint)
                               OVER (ORDER BY (payload->>'seq')::bigint) AS prev_seq
                    FROM raw.telemetry_payload
                    WHERE payload->>'gw' = :gw
                      AND payload ? 'raw'
                      AND payload ? 'seq'
                      AND payload ? 'ts_ms'
                      AND to_timestamp((payload->>'ts_ms')::bigint / 1000.0)
                          > now() - make_interval(hours => :hours)
                )
                SELECT seq, prev_seq, received_at FROM seqs ORDER BY seq
            """),
            {"gw": gw_serial, "hours": hours},
        )
    ).all()

    total = len(rows)
    first_seq = rows[0].seq if rows else None
    last_seq = rows[-1].seq if rows else None
    last_received_at = max((r.received_at for r in rows), default=None)

    gaps: list[GwSeqGap] = []
    missing_total = 0
    for r in rows:
        if r.prev_seq is None:
            continue
        delta = r.seq - r.prev_seq
        if 1 < delta < max_gap:
            # Grace period: if this gap is right up against the last received frame, the missing
            # seq might still be in transit / not yet flushed to SD → we don't consider it
            # recoverable yet.
            if grace_period_s > 0 and last_received_at is not None:
                age_s = (last_received_at - r.received_at).total_seconds()
                if age_s < grace_period_s:
                    continue
            miss = delta - 1
            missing_total += miss
            gaps.append(
                GwSeqGap(after_seq=r.prev_seq, before_seq=r.seq, missing=miss, at=r.received_at)
            )

    completeness = round(total / (total + missing_total) * 100, 2) if total else None
    gaps.sort(key=lambda g: g.at, reverse=True)

    return GwSeqGapResult(
        total_packets=total,
        first_seq=first_seq,
        last_seq=last_seq,
        missing_total=missing_total,
        completeness_pct=completeness,
        gaps=gaps,
    )
