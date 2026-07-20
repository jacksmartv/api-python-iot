"""Gateway domain helpers.

Unlike payload_parser.py (which parses *incoming* MQTT payloads), these helpers read
JSON already persisted in monitoring.gateway_status.raw_payload to expose it via the API.
"""


def parse_gps(payload: dict | None) -> tuple[float, float] | None:
    """Extract a plausible (lat, lon) from a gateway's raw_payload, or None.

    Validates range (lat in [-90,90], lon in [-180,180]), discards (0,0) and a false gps_fix.
    Mirrors the frontend's validLatLng() (GlobalMap.tsx). Reused by /gateways, /health and export.
    """
    p = payload or {}
    if p.get("gps_fix") is False:
        return None
    lat, lon = p.get("gps_lat"), p.get("gps_lon")
    # bool is a subclass of int in Python: exclude it explicitly so True/False aren't accepted
    if isinstance(lat, bool) or isinstance(lon, bool):
        return None
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
        return None
    return float(lat), float(lon)
