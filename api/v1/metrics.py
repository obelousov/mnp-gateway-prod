# api/v1/metrics.py
from fastapi import APIRouter, Request, Response, Depends
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import time
import logging

# Import your existing metrics or create them here
from ..core.metrics import (  # We'll create this next
    REQUEST_COUNT,
    REQUEST_LATENCY, 
    ACTIVE_REQUESTS,
    PORT_IN_REQUESTS,
    PORT_IN_PROCESSING_TIME,
    DATABASE_CONNECTIONS
)

router = APIRouter(tags=["monitoring"])

@router.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

@router.get("/health")
async def health_check():
    """Simple health check endpoint"""
    return {"status": "healthy", "service": "mnp-api"}

# Optional: Add more monitoring endpoints
@router.get("/status")
async def status():
    """Service status with basic metrics"""
    return {
        "status": "running",
        "timestamp": time.time(),
        "service": "mnp-api"
    }