"""
Telemetry ingestion endpoints.
"""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from ..auth import verify_api_key
from ..services import ingestion_service

router = APIRouter(
    prefix="/telemetry",
    tags=["telemetry"],
    dependencies=[Depends(verify_api_key)],
)


@router.post(
    "/ingest",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a telemetry payload",
    description="""
    Receives a JSON telemetry payload from a device.

    The payload is processed near real-time:
    - The original payload is saved for auditing
    - Each sensor's measurements are extracted and stored
    - The device's operational status is recorded

    Returns 202 Accepted since processing may be asynchronous.
    """,
)
async def ingest_telemetry(payload: dict[str, Any]) -> dict:
    """
    Telemetry ingestion endpoint.

    Example payload:
    ```json
    {
        "sn": "DEVICE001",
        "schema_v": 1,
        "status": {
            "rssi": -65,
            "buf_used": 10,
            "buf_total": 100,
            "supply": 3300
        },
        "sensor_0": {
            "timestamp": 20250205.143022,
            "temp": 25.5,
            "volt_cond": 1.2,
            "supply": 3300,
            "msg_cnt": 12345
        },
        "sensor_1": {
            "timestamp": 20250205.143022,
            "temp": 26.0,
            "volt_cond": 1.1,
            "supply": 3280,
            "msg_cnt": 12346
        }
    }
    ```
    """
    serial_number = payload.get("sn") or payload.get("serial_number")
    if not serial_number:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing serial number (sn or serial_number)",
        )

    try:
        device_id = await ingestion_service.ingest(payload)
        return {
            "status": "accepted",
            "device_id": str(device_id),
            "message": "Payload queued for processing",
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to ingest payload: {str(e)}",
        )


@router.post(
    "/ingest/batch",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest multiple telemetry payloads",
)
async def ingest_telemetry_batch(payloads: list[dict[str, Any]]) -> dict:
    """
    Endpoint for ingesting multiple payloads in a single call.

    Useful for devices that send accumulated data or for
    reprocessing historical data.
    """
    if not payloads:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Empty payload list",
        )

    results = []
    errors = []

    for i, payload in enumerate(payloads):
        serial_number = payload.get("sn") or payload.get("serial_number")
        if not serial_number:
            errors.append({"index": i, "error": "Missing serial number"})
            continue

        try:
            device_id = await ingestion_service.ingest(payload)
            results.append({"index": i, "device_id": str(device_id)})
        except Exception as e:
            errors.append({"index": i, "error": str(e)})

    return {
        "status": "accepted",
        "processed": len(results),
        "errors": len(errors),
        "results": results,
        "error_details": errors if errors else None,
    }
