"""
Tests for calibration math (pure functions, no DB).

Run:  cd api && python -m pytest tests/test_calibration.py -q
"""

import math

import pytest

from src.services.calibration import (
    apply_calibration,
    compute_regression,
    point_is_valid,
)


def _z(adc):
    return math.log10((1023 - adc) / adc)


def _points_from_line(m, c, adcs):
    """Generates points (adc, ch) that fall EXACTLY on ch = m·z + c."""
    return [{"adc": a, "ch_percent": round(m * _z(a) + c, 6)} for a in adcs]


def test_point_is_valid_ranges():
    assert point_is_valid(512, 15.0) is True
    assert point_is_valid(512, 7.9) is False       # ch < 8
    assert point_is_valid(512, 25.1) is False      # ch > 25
    assert point_is_valid(0, 15.0) is False        # adc < 1
    assert point_is_valid(1023, 15.0) is False     # adc > 1022
    assert point_is_valid(1, 8.0) is True          # inclusive edges
    assert point_is_valid(1022, 25.0) is True


def test_recovers_known_coefficients():
    m, c = -6.5, 14.0
    # adcs chosen so that ch falls within 8-25
    pts = _points_from_line(m, c, [300, 450, 512, 640, 720])
    res = compute_regression(pts)
    assert res.m == pytest.approx(m, abs=1e-6)
    assert res.c == pytest.approx(c, abs=1e-6)
    assert res.r_squared == pytest.approx(1.0, abs=1e-9)
    assert res.valid_count == 5
    assert all(p["valid"] for p in res.points)


def test_invalid_points_excluded_from_fit():
    m, c = -6.5, 14.0
    pts = _points_from_line(m, c, [300, 450, 512, 640])
    # two points out of range: marked invalid and NOT counted
    pts.append({"adc": 512, "ch_percent": 30.0})   # ch > 25
    pts.append({"adc": 0, "ch_percent": 15.0})     # invalid adc
    res = compute_regression(pts)
    assert res.valid_count == 4
    assert res.m == pytest.approx(m, abs=1e-6)
    invalid = [p for p in res.points if not p["valid"]]
    assert len(invalid) == 2


def test_fewer_than_three_valid_raises():
    pts = [
        {"adc": 300, "ch_percent": 15.0},
        {"adc": 450, "ch_percent": 14.0},
        {"adc": 512, "ch_percent": 30.0},  # invalid → 2 valid remain
    ]
    with pytest.raises(ValueError):
        compute_regression(pts)


def test_validity_range_is_min_max_of_valid_ch():
    pts = [
        {"adc": 300, "ch_percent": 20.0},
        {"adc": 512, "ch_percent": 14.0},
        {"adc": 720, "ch_percent": 9.0},
    ]
    res = compute_regression(pts)
    assert res.ch_min == pytest.approx(9.0)
    assert res.ch_max == pytest.approx(20.0)


def test_low_r2_flag():
    # noisy points → low R²
    pts = [
        {"adc": 300, "ch_percent": 10.0},
        {"adc": 450, "ch_percent": 22.0},
        {"adc": 512, "ch_percent": 9.0},
        {"adc": 640, "ch_percent": 24.0},
    ]
    res = compute_regression(pts)
    assert res.low_r2 is True


def test_apply_calibration_and_extrapolation():
    m, c = -6.5, 14.0
    # within the validity range [9, 20]
    chp, extra = apply_calibration(512, m, c, ch_min=9.0, ch_max=20.0)
    assert chp == round(m * _z(512) + c, 1)
    assert extra is False
    # high adc → chp above the [9,20] range → extrapolated
    chp_hi, extra_hi = apply_calibration(950, m, c, ch_min=9.0, ch_max=20.0)
    assert chp_hi > 20.0
    assert extra_hi is True


def test_apply_calibration_out_of_bounds_returns_none():
    m, c = -6.5, 14.0
    assert apply_calibration(None, m, c, 9.0, 20.0) == (None, False)
    assert apply_calibration(0, m, c, 9.0, 20.0) == (None, False)
    assert apply_calibration(1023, m, c, 9.0, 20.0) == (None, False)


def test_apply_calibration_rounds_to_one_decimal():
    chp, _ = apply_calibration(512, -6.5123, 14.0, 0.0, 100.0)
    # exactly 1 decimal place
    assert chp == round(chp, 1)
