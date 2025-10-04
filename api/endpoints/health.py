"""
Health check endpoints for MNP Gateway API
"""
from fastapi import APIRouter
from config import settings, logger, get_madrid_time_iso, get_madrid_time_readable
import time
from datetime import datetime

router = APIRouter()

@router.get("/health")
async def health_check():
    """Basic health check endpoint"""
    logger.info("MNP healthcheck accessed")
    current_time = time.time()
    human_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "status": "healthy", 
        "service": settings.API_TITLE,
        "version": settings.API_VERSION,
        "timestamp_iso": get_madrid_time_iso(),  # Madrid time in ISO format
        "timestamp_readable": get_madrid_time_readable(),  # Human readable Madrid time
        "timezone": "Europe/Madrid"
    }