"""
SVG Ingestion Pipeline (Sprint 2).

Normalizes an uploaded floorplan (SVG/PNG/JPG) into a safe, servable SVG.
NEVER stores the client's original file; only the normalized SVG (+ raster kept separately).

Flow: validate → to_svg → sanitize → optimize → normalize(flatten) → guard.
Pure functions, testable without DB or FastAPI. process_floorplan() orchestrates and
raises PlanRejected(reason) on any rejection.
"""

import io
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass

from PIL import Image
from scour import scour

from ..config import settings

SVG_NS = "http://www.w3.org/2000/svg"
XLINK_NS = "http://www.w3.org/1999/xlink"

# Allowed graphical elements (allowlist). Everything else is removed.
ALLOWED_TAGS = {
    "svg", "g", "path", "rect", "circle", "ellipse", "line",
    "polyline", "polygon", "text", "tspan", "defs", "use", "symbol",
    "image", "title", "desc", "linearGradient", "radialGradient", "stop",
    "clipPath", "marker", "pattern",
}
# Transforms supported in V1. rotate/matrix/skew → rejected (avoids asset drift).
_SUPPORTED_TRANSFORM = re.compile(r"^\s*(translate|scale)\s*\([^)]*\)\s*$")
_TRANSFORM_FUNC = re.compile(r"([a-zA-Z]+)\s*\(")


class PlanRejected(Exception):
    """The floorplan fails a validation/guard. message is actionable for the user."""


@dataclass
class NormalizedPlan:
    svg_bytes: bytes
    viewbox: str
    node_count: int
    svg_size_kb: int
    raster_bytes: bytes | None = None   # original PNG/JPG to store separately
    raster_ext: str | None = None       # 'png' | 'jpg'


# ---------------------------------------------------------------------------
# [1] validate — real signature (magic bytes), not extension or Content-Type
# ---------------------------------------------------------------------------
def detect_kind(raw: bytes) -> str:
    """Detects png/jpg/svg by content. Anything else → PlanRejected."""
    if raw[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if raw[:3] == b"\xff\xd8\xff":
        return "jpg"
    head = raw[:512].lstrip()
    if head[:5] == b"<?xml" or head[:4] == b"<svg" or b"<svg" in raw[:1024]:
        return "svg"
    raise PlanRejected("Unsupported file type: only SVG, PNG or JPG are accepted")


def validate(raw: bytes) -> str:
    if len(raw) > settings.plan_max_upload_mb * 1024 * 1024:
        raise PlanRejected(f"File exceeds {settings.plan_max_upload_mb} MB upload limit")
    return detect_kind(raw)


# ---------------------------------------------------------------------------
# [2] to_svg — SVG passes through; PNG/JPG → <svg><image href=raster_url></svg>
# ---------------------------------------------------------------------------
def raster_dimensions(raw: bytes) -> tuple[int, int]:
    """W,H of the raster via Pillow + decompression bomb guard."""
    with Image.open(io.BytesIO(raw)) as img:
        w, h = img.size
    if w * h > settings.plan_max_raster_pixels:
        raise PlanRejected("Raster dimensions exceed limit (possible decompression bomb)")
    return w, h


def raster_to_svg(raw: bytes, kind: str, raster_url: str) -> tuple[bytes, str]:
    """Wraps a raster as SVG with an external <image href> (not base64)."""
    w, h = raster_dimensions(raw)
    svg = (
        f'<svg xmlns="{SVG_NS}" xmlns:xlink="{XLINK_NS}" '
        f'viewBox="0 0 {w} {h}">'
        f'<image href="{raster_url}" x="0" y="0" width="{w}" height="{h}"/>'
        f"</svg>"
    )
    return svg.encode(), f"0 0 {w} {h}"


# ---------------------------------------------------------------------------
# [3] sanitize — allowlist + remove script/event handlers/external refs
# ---------------------------------------------------------------------------
def _localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _href_is_safe(value: str, allow_local_raster: bool) -> bool:
    v = value.strip()
    if v.startswith("#"):
        return True
    if allow_local_raster and v.startswith(settings.storage_url_prefix):
        return True
    return False


def sanitize_svg(svg_bytes: bytes) -> bytes:
    """Allowlist of elements; removes script/foreignObject/on*/javascript:/external refs.
    Preserves xmlns and xmlns:xlink."""
    try:
        root = ET.fromstring(svg_bytes)
    except ET.ParseError as e:
        raise PlanRejected(f"Invalid SVG XML: {e}")

    if _localname(root.tag) != "svg":
        raise PlanRejected("Root element is not <svg>")

    def clean(el: ET.Element) -> None:
        for child in list(el):
            name = _localname(child.tag)
            if name not in ALLOWED_TAGS:
                el.remove(child)
                continue
            # attributes: strip on*, href javascript:/external
            for attr in list(child.attrib):
                aname = _localname(attr).lower()
                aval = child.attrib[attr]
                if aname.startswith("on"):
                    del child.attrib[attr]
                elif aname == "href":
                    allow_raster = name == "image"
                    if aval.strip().lower().startswith("javascript:") or not _href_is_safe(
                        aval, allow_raster
                    ):
                        del child.attrib[attr]
            clean(child)

    clean(root)
    # Guarantee namespaces (review#7)
    root.set("xmlns", SVG_NS)
    root.set("xmlns:xlink", XLINK_NS)
    return ET.tostring(root, encoding="utf-8")


# ---------------------------------------------------------------------------
# [4] optimize — scour
# ---------------------------------------------------------------------------
def optimize_svg(svg_bytes: bytes) -> bytes:
    opts = scour.parse_args([])
    opts.remove_metadata = True
    opts.enable_viewboxing = True
    opts.strip_comments = True
    opts.shorten_ids = False
    cleaned = scour.scourString(svg_bytes.decode("utf-8"), opts)
    return cleaned.encode("utf-8")


# ---------------------------------------------------------------------------
# [5] normalize — viewBox + flatten (translate/scale only)
# ---------------------------------------------------------------------------
def extract_viewbox(svg_bytes: bytes) -> str:
    root = ET.fromstring(svg_bytes)
    vb = root.get("viewBox")
    if vb:
        return vb.strip()
    w, h = root.get("width"), root.get("height")
    if w and h:
        wn = re.sub(r"[^\d.]", "", w)
        hn = re.sub(r"[^\d.]", "", h)
        if wn and hn:
            return f"0 0 {wn} {hn}"
    raise PlanRejected("SVG has no viewBox nor width/height; cannot establish coordinate system")


def _normalize_viewbox(vb: str) -> str:
    """Converts each viewBox value to a plain number (no scientific notation
    like '1e3', which breaks coordinate parsing on the frontend)."""
    out = []
    for tok in vb.replace(",", " ").split():
        try:
            f = float(tok)
            out.append(str(int(f)) if f.is_integer() else repr(f))
        except ValueError:
            out.append(tok)
    return " ".join(out)


def assert_transforms_supported(svg_bytes: bytes) -> None:
    """Rejects rotate/matrix/skew. Only translate/scale are safe to flatten in V1."""
    root = ET.fromstring(svg_bytes)
    for el in root.iter():
        t = el.get("transform")
        if not t:
            continue
        for func in _TRANSFORM_FUNC.findall(t):
            if func not in ("translate", "scale"):
                raise PlanRejected(
                    f"SVG contains unsupported {func}() transform; "
                    "re-export with transforms flattened/applied"
                )


# ---------------------------------------------------------------------------
# [6] guard — node_count + svg_size_kb
# ---------------------------------------------------------------------------
def count_nodes(svg_bytes: bytes) -> int:
    """Counts ALL elements (incl. defs/symbol/use), not just visible ones."""
    root = ET.fromstring(svg_bytes)
    return sum(1 for _ in root.iter())


def assert_within_limits(svg_bytes: bytes, node_count: int) -> int:
    size_kb = (len(svg_bytes) + 1023) // 1024
    if node_count > settings.plan_max_nodes:
        raise PlanRejected(
            f"Plan has {node_count} nodes, exceeds limit of {settings.plan_max_nodes}; "
            "simplify the drawing before uploading"
        )
    if size_kb > settings.plan_max_svg_kb:
        raise PlanRejected(
            f"Normalized SVG is {size_kb} KB, exceeds limit of {settings.plan_max_svg_kb} KB"
        )
    return size_kb


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def process_floorplan(raw: bytes, raster_url: str) -> NormalizedPlan:
    """Full pipeline. raster_url = public_url where the raster will be stored
    (only used if the file is PNG/JPG). Raises PlanRejected on any rejection."""
    kind = validate(raw)

    raster_bytes = None
    raster_ext = None
    if kind in ("png", "jpg"):
        svg_bytes, _ = raster_to_svg(raw, kind, raster_url)
        raster_bytes = raw
        raster_ext = kind
    else:
        svg_bytes = raw

    svg_bytes = sanitize_svg(svg_bytes)
    assert_transforms_supported(svg_bytes)
    # Early guards BEFORE optimize: scour can hang on pathological SVGs
    # (a <path> with hundreds of thousands of points, or tens of thousands of elements).
    # We reject fast if the raw SVG already exceeds the output or node limit —
    # if it comes in larger than the final limit, scour won't save it.
    pre_kb = (len(svg_bytes) + 1023) // 1024
    if pre_kb > settings.plan_max_svg_kb:
        raise PlanRejected(
            f"SVG is {pre_kb} KB (limit {settings.plan_max_svg_kb} KB); simplify the drawing"
        )
    pre_nodes = count_nodes(svg_bytes)
    if pre_nodes > settings.plan_max_nodes:
        raise PlanRejected(
            f"Plan has {pre_nodes} nodes, exceeds limit of {settings.plan_max_nodes}; "
            "simplify the drawing before uploading"
        )
    # Extract viewBox BEFORE scour: scour can rewrite 1000 -> 1e3 (scientific
    # notation) which breaks coordinate parsing on the frontend. We take the clean
    # value from the sanitized SVG and normalize it to plain numbers.
    viewbox = _normalize_viewbox(extract_viewbox(svg_bytes))
    # optimize only benefits vector SVG; for raster it's trivial but harmless
    svg_bytes = optimize_svg(svg_bytes)
    node_count = count_nodes(svg_bytes)
    size_kb = assert_within_limits(svg_bytes, node_count)

    return NormalizedPlan(
        svg_bytes=svg_bytes,
        viewbox=viewbox,
        node_count=node_count,
        svg_size_kb=size_kb,
        raster_bytes=raster_bytes,
        raster_ext=raster_ext,
    )
