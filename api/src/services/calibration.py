"""
Wood moisture calibration service (ADC → %CH).

Model: ch = m·z + c, with z = log10((1023 - adc) / adc), fitted by least
squares over the VALID points. The resistive method is only reliable between
8% and 25%: points outside that range (or with adc outside 1-1022) are marked
invalid and excluded from the fit.

The calculation functions are pure (no DB) so they can be tested standalone.
`resolve_active_calibration` does touch the DB (Stage 2, applied in responses).
"""

import math
import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Calibration, CalibrationSensor

# Reliable range of the resistive method
CH_MIN_RELIABLE = 8.0
CH_MAX_RELIABLE = 25.0
# Valid adc (avoids division by zero / invalid log in z)
ADC_MIN = 1
ADC_MAX = 1022
MIN_VALID_POINTS = 3
R2_WARN_THRESHOLD = 0.98


@dataclass
class RegressionResult:
    m: float
    c: float
    r_squared: float
    ch_min: float
    ch_max: float
    points: list[dict]  # all points with their "valid" flag
    valid_count: int

    @property
    def low_r2(self) -> bool:
        return self.r_squared < R2_WARN_THRESHOLD


def point_is_valid(adc: float, ch_percent: float) -> bool:
    """A point enters the fit only if adc∈[1,1022] and ch∈[8,25]."""
    return ADC_MIN <= adc <= ADC_MAX and CH_MIN_RELIABLE <= ch_percent <= CH_MAX_RELIABLE


def _z(adc: float) -> float:
    return math.log10((1023 - adc) / adc)


def compute_regression(points: list[dict]) -> RegressionResult:
    """Marks each point as valid/invalid and fits ch = m·z + c by least
    squares over the valid ones.

    `points`: list of {"adc": int, "ch_percent": float}.
    Returns the coefficients, R² and the validity range (min/max of the valid ch).
    Raises ValueError if fewer than MIN_VALID_POINTS valid points remain or if the
    fit is degenerate.
    """
    marked: list[dict] = []
    valid: list[tuple[float, float]] = []
    for p in points:
        adc = float(p["adc"])
        ch = float(p["ch_percent"])
        ok = point_is_valid(adc, ch)
        marked.append({"adc": p["adc"], "ch_percent": p["ch_percent"], "valid": ok})
        if ok:
            valid.append((adc, ch))

    if len(valid) < MIN_VALID_POINTS:
        raise ValueError(
            f"At least {MIN_VALID_POINTS} valid points are required "
            f"(ch between {CH_MIN_RELIABLE:g}% and {CH_MAX_RELIABLE:g}%, adc between "
            f"{ADC_MIN} and {ADC_MAX}); got {len(valid)}."
        )

    zs = [_z(adc) for adc, _ in valid]
    chs = [ch for _, ch in valid]
    n = len(valid)
    sum_z = sum(zs)
    sum_ch = sum(chs)
    sum_zch = sum(z * c for z, c in zip(zs, chs))
    sum_z2 = sum(z * z for z in zs)

    denom = n * sum_z2 - sum_z * sum_z
    if denom == 0:
        raise ValueError(
            "Degenerate points: all adc values produce the same z; cannot fit."
        )

    m = (n * sum_zch - sum_z * sum_ch) / denom
    c = (sum_ch - m * sum_z) / n

    mean_ch = sum_ch / n
    ss_res = sum((ch - (m * z + c)) ** 2 for z, ch in zip(zs, chs))
    ss_tot = sum((ch - mean_ch) ** 2 for ch in chs)
    r_squared = 1.0 - ss_res / ss_tot if ss_tot != 0 else 1.0

    return RegressionResult(
        m=m,
        c=c,
        r_squared=r_squared,
        ch_min=min(chs),
        ch_max=max(chs),
        points=marked,
        valid_count=n,
    )


def apply_calibration(
    adc: float | None, m: float, c: float, ch_min: float, ch_max: float
) -> tuple[float | None, bool]:
    """Converts a raw ADC value to %CH using the calibration. Returns (chp, extrapolated).

    chp rounded to 1 decimal. extrapolated=True if it falls outside the validity range.
    If adc is None or outside (0, 1023) exclusive → (None, False).
    """
    if adc is None or adc <= 0 or adc >= 1023:
        return None, False
    chp = round(m * _z(adc) + c, 1)
    return chp, (chp < ch_min or chp > ch_max)


async def resolve_active_calibrations_for_sensors(
    db: AsyncSession, sensor_ids: list[uuid.UUID]
) -> dict[uuid.UUID, Calibration]:
    """Map {sensor_id → active calibration} for a set of sensors.

    A sensor belongs to at most one active calibration; if by mistake there is more
    than one (two active materials sharing it), the most recent one wins.
    """
    if not sensor_ids:
        return {}
    rows = (
        await db.execute(
            select(CalibrationSensor.sensor_id, Calibration)
            .join(Calibration, Calibration.id == CalibrationSensor.calibration_id)
            .where(CalibrationSensor.sensor_id.in_(sensor_ids))
            .where(Calibration.is_active.is_(True))
            .order_by(Calibration.created_at.desc())
        )
    ).all()
    out: dict[uuid.UUID, Calibration] = {}
    for sensor_id, calib in rows:
        # the first one (most recent) wins
        out.setdefault(sensor_id, calib)
    return out
