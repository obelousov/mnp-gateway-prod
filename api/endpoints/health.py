"""
Health check endpoints for MNP Gateway API
"""
from fastapi import APIRouter
from config import settings, logger
import time

router = APIRouter()

@router.get("/health")
async def health_check():
    """Basic health check endpoint"""
    logger.info("MNP healthcheck accessed")
    return {
        "status": "healthy", 
        "service": settings.API_TITLE,
        "version": settings.API_VERSION,
        "timestamp": time.time()
    }