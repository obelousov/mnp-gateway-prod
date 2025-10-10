from fastapi import FastAPI
from api.endpoints import bss_requests, health
from config import settings
from services.logger import logger, payload_logger, log_payload

logger.debug("Starting MNP Gateway API")

app = FastAPI(
    title=settings.API_TITLE,           # Refer as settings.API_TITLE
    description=settings.API_DESCRIPTION, # Refer as settings.API_DESCRIPTION  
    version=settings.API_VERSION         # Refer as settings.API_VERSION
)

# Include routers
app.include_router(
    bss_requests.router, 
    prefix=settings.API_V1_PREFIX,      # Refer as settings.API_V1_PREFIX
    tags=["BSS Requests"]
)
app.include_router(
    health.router, 
    prefix=settings.API_V1_PREFIX,      # Refer as settings.API_V1_PREFIX
    tags=["Health"]
)

@app.get("/")
async def root():
    return {"message": "MNP Gateway Service is running"}