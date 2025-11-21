from fastapi import FastAPI, HTTPException, status, Depends,Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from api.endpoints import bss_requests, health, italy_requests
from config import settings
# from services.logger import logger
from services.logger_simple import logger
import secrets
from api.v2.endpoints import health as health_v2
from api.v1 import bss, metrics, orders, return_request
from api.core.middleware import prometheus_middleware
import logging
from fastapi.logger import logger as fastapi_logger

# Configure Uvicorn to use custom JSON logger
uvicorn_logger = logging.getLogger("uvicorn")
uvicorn_logger.handlers = logger.handlers
uvicorn_logger.setLevel(logger.level)
uvicorn_logger.propagate = False

# Configure uvicorn.access logger
uvicorn_access_logger = logging.getLogger("uvicorn.access")
uvicorn_access_logger.handlers = logger.handlers
# uvicorn_access_logger.setLevel(logger.level)
uvicorn_access_logger.setLevel(logging.WARNING)  # CHANGED FROM logger.level
uvicorn_access_logger.propagate = False

# Configure FastAPI's logger
fastapi_logger.handlers = logger.handlers
fastapi_logger.setLevel(logger.level)

logger.debug("Starting MNP Gateway API")

app = FastAPI(
    title=settings.API_TITLE,           # Refer as settings.API_TITLE
    description=settings.API_DESCRIPTION, # Refer as settings.API_DESCRIPTION  
    version=settings.API_VERSION, # Refer as settings.API_VERSION
    docs_url=None,  # Disable default docs
    redoc_url=None  # Disable default redoc
)

# Custom middleware to log requests in JSON format
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Custom middleware to log HTTP requests in JSON format"""
    response = await call_next(request)
    
    # Create access log record
    access_log_record = {
        "client_ip": request.client.host if request.client else "unknown",
        "method": request.method,
        "path": request.url.path,
        "status_code": response.status_code,
        "user_agent": request.headers.get("user-agent", ""),
        "query_params": str(request.query_params) if request.query_params else ""
    }
    
    # Log using custom JSON logger
    # logger.debug(
    #     "HTTP %s %s - %s - %s",
    #     request.method,
    #     request.url.path,
    #     response.status_code,
    #     access_log_record
    # )

    # TODO: enable if required
    # logger.debug(
    #     "HTTP: %s",
    #     access_log_record
    # )

    
    return response

# Basic Auth credentials (store in environment variables in production)
SWAGGER_USERNAME = settings.SWAGGER_USERNAME  # Default: "admin"
SWAGGER_PASSWORD = settings.SWAGGER_PASSWORD  # Default
security = HTTPBasic()

def authenticate_swagger(credentials: HTTPBasicCredentials):
    correct_username = secrets.compare_digest(credentials.username, SWAGGER_USERNAME)
    correct_password = secrets.compare_digest(credentials.password, SWAGGER_PASSWORD)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

@app.get("/docs", include_in_schema=False)
async def get_swagger(credentials: HTTPBasicCredentials = Depends(security)):
    authenticate_swagger(credentials)
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Docs")

@app.get("/redoc", include_in_schema=False)
async def get_redoc(credentials: HTTPBasicCredentials = Depends(security)):
    authenticate_swagger(credentials)
    return get_redoc_html(openapi_url="/openapi.json", title="ReDoc")

# Include routers
app.include_router(
    bss_requests.router, 
    prefix=settings.API_PREFIX,      # Refer as settings.API_V1_PREFIX
    # tags=["BSS Requests"]
)
app.include_router(
    health.router, 
    prefix=settings.API_PREFIX,      # Refer as settings.API_V1_PREFIX
    tags=["Health"]
)
app.include_router(
    italy_requests.router, 
    prefix=settings.API_PREFIX,      # Refer as settings.API_V1_PREFIX
    # tags=["BSS Requests"]
)

app.include_router(
    health_v2.router, 
    prefix=settings.API_PREFIX_V2,      # Refer as settings.API_V1_PREFIX
    tags=["Health/v2"]
)

app.include_router(
    bss.router,
    prefix=settings.API_PREFIX,      # Refer as settings.API_V1_PREFIX
    # tags=["BSS Webhook"]
)

app.include_router(
    orders.router,
    prefix=settings.API_PREFIX,      # Refer as settings.API_V1_PREFIX
    # tags=["BSS Webhook"]
)

# Add middleware
app.middleware("http")(prometheus_middleware)

# Include metrics router
app.include_router(
    metrics.router,
    prefix=settings.API_PREFIX  # This will make endpoints: /api/v1/metrics, /api/v1/health
)

# include retrun request router
app.include_router(
    return_request.router,
    prefix=settings.API_PREFIX,      # Refer as settings.API_V1_PREFIX
    # tags=["BSS Webhook"]
)


@app.get("/",
        include_in_schema=False  # This hides the endpoint from Swagger)
        )
async def root():
    return {"message": "MNP Gateway Service is running"}