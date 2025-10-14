"""
Health check endpoints for MNP Gateway API
"""
from fastapi import APIRouter
from config import settings, get_madrid_time_iso, get_madrid_time_readable
import time
from datetime import datetime
from services.logger import logger, payload_logger # Use the centralized logger

start_time = time.time()

router = APIRouter()

@router.get(
    "/healthcheck",
    # dependencies=[Depends(verify_basic_auth)],
    summary="Health Check V2",
    description="Check the health status of the MNP Gateway service",
    response_description="Service health status with timestamp",
    tags=["Monitoring"]
)
async def healthcheck():
    """
    Health check endpoint returning a status and Madrid timestamps.
    """
    now_iso = get_madrid_time_iso()
    now_readable = get_madrid_time_readable()
    
    # Get human readable uptime
    uptime_human, uptime_seconds = get_uptime_human_readable(start_time)
    
    # Log the healthcheck event using centralized payload logger
    payload_logger.info({"event": "healthcheck", "timestamp_iso": now_iso})

    return {
        "status": "ok",
        "version": settings.API_VERSION_V2,
        "timestamp_iso": now_iso,
        "timestamp": now_readable,
        # "uptime_seconds": uptime_seconds,
        "uptime_human_readable": uptime_human
    }

def get_uptime_human_readable(start_time):
    """Convert uptime seconds to human readable format"""
    uptime_seconds = time.time() - start_time
    hours = int(uptime_seconds // 3600)
    minutes = int((uptime_seconds % 3600) // 60)
    seconds = int(uptime_seconds % 60)
    return f"{hours} hours {minutes} min, {seconds} sec", uptime_seconds