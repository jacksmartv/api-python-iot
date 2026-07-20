from .auth import router as auth_router
from .calibration import router as calibration_router
from .comparison import router as comparison_router
from .devices import router as devices_router
from .events import router as events_router
from .firmware import router as firmware_router
from .firmware_deployment import router as firmware_deployment_router
from .gateways import router as gateways_router
from .internal_metrics import router as internal_metrics_router
from .spatial import router as spatial_router
from .status import router as status_router
from .telemetry import router as telemetry_router

__all__ = [
    "auth_router",
    "calibration_router",
    "comparison_router",
    "devices_router",
    "events_router",
    "firmware_router",
    "firmware_deployment_router",
    "gateways_router",
    "internal_metrics_router",
    "spatial_router",
    "status_router",
    "telemetry_router",
]
