# api/core/middleware.py
import time
from fastapi import Request
from .metrics import REQUEST_COUNT, REQUEST_LATENCY, ACTIVE_REQUESTS

async def prometheus_middleware(request: Request, call_next):
    """ middleware """
    start_time = time.time()
    ACTIVE_REQUESTS.inc()
    
    try:
        response = await call_next(request)
        # Record successful request
        REQUEST_COUNT.labels(
            method=request.method, 
            endpoint=request.url.path, 
            status_code=response.status_code
        ).inc()
        return response
        
    except Exception:
        # Record exception as 500 error
        REQUEST_COUNT.labels(
            method=request.method, 
            endpoint=request.url.path, 
            status_code=500
        ).inc()
        raise
        
    finally:
        ACTIVE_REQUESTS.dec()
        processing_time = time.time() - start_time
        REQUEST_LATENCY.labels(
            method=request.method, 
            endpoint=request.url.path
        ).observe(processing_time)