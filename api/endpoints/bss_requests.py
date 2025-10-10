from fastapi import APIRouter, HTTPException, status, Header
from config import settings
import time
from services.database_service import save_portin_request_db, save_cancel_request_db
from tasks.tasks import submit_to_central_node
from services.logger import logger, payload_logger, log_payload
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

@router.post(
    "/query-msisdn",
    summary="Query MSISDN portability details", 
    description="Accept MSISDN query from BSS, store in DB, and forward to Central Node"
)
async def query_msisdn_details():
    """Basic health check endpoint"""
    logger.info("Query MSISDN endpoint accessed")
    return {
        "status": "MSISDN Query accepted",
        "service": settings.API_DESCRIPTION,
        "version": settings.API_VERSION, 
        "timestamp": time.time()
    }

@router.post('/port-in', status_code=status.HTTP_202_ACCEPTED)  # Use status codes properly
async def portin_request(alta_data: dict):
    """
    1. Accept the request from BSS.
    2. Save it to the database immediately.
    3. Queue the task for background processing.
    4. Return an immediate 202 Accepted response.
    """
    try:
        logger.debug("Processing port-in request")
    
        # Conditional payload logging
        log_payload('BSS', 'PORT_IN', 'REQUEST', str(alta_data))

        # 1. & 2. Create and save the DB record
        new_request_id = save_portin_request_db(alta_data)
        
        # 3. Launch the background task, passing the ID of the new record
        submit_to_central_node.delay(new_request_id)

        # 4. Tell the BSS "We got it, processing now."
        return {
            "message": "Request accepted", 
            "id": new_request_id,
            "session_code": alta_data.get('codigoSesion'),
            "status": "PROCESSING"  # Add the status field back
        }
        
    except Exception as e:
        logger.error("Error in port-in endpoint: %s", str(e))  # Lazy logging
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process request: {str(e)}"
        ) from e  # Explicit exception chaining

class CancelPortabilityRequest(BaseModel):
    """
    Pydantic class to validate query
    """
    reference_code: str
    cancellation_reason: str
    cancellation_initiated_by_donor: bool
    session_code: Optional[str] = None

# @router.post("/cancel")
# async def cancel_portability(
#     request: CancelPortabilityRequest
# ):
@router.post('/cancel', status_code=status.HTTP_202_ACCEPTED)  # Use status codes properly
async def cancel_portability(alta_data: dict):

    """
    1. Accept the cancel request from BSS.
    2. Save it to the database immediately.
    3. Queue the task for query to NC processing.
    4. Return an immediate 202 Accepted response.
    """
    try:
        # Generate a unique ID for this cancellation request
        # request_id = str(uuid.uuid4())
        
        # logger.info("Processing cancellation request for reference: %s", alta_data.reference_code)
        logger.info("Processing cancellation request")
        
        # 1. Log the incoming payload
        log_payload('BSS', 'CANCEL_PORTABILITY', 'REQUEST', str(alta_data))
        
        # 2. Save to database immediately
        request_id = save_cancel_request_db(alta_data)
        
        # db_record = {
        #     "request_id": request_id,
        #     "reference_code": request.reference_code,
        #     "cancellation_reason": request.cancellation_reason,
        #     "cancellation_initiated_by_donor": request.cancellation_initiated_by_donor,
        #     "session_code": request.session_code,
        #     "status": "PROCESSING"
        # }
        
        # Save to database (pseudo-code - adapt to your ORM)
        # await save_cancel_request(db_record)
        
        # 3. Queue the task for NC processing
        # Using Celery task
        # process_nc_cancel_task.delay(
        #     reference_code=request.reference_code,
        #     cancellation_reason=request.cancellation_reason,
        #     cancellation_initiated_by_donor=request.cancellation_initiated_by_donor,
        #     session_code=request.session_code,
        #     internal_request_id=request_id
        # )
        
        # Alternative: Using FastAPI BackgroundTasks
        # background_tasks.add_task(process_nc_cancel_sync, request.dict(), request_id)
        
        # logger.info(f"Cancellation request queued for NC processing: {request_id}")
        
        # 4. Return immediate 202 Accepted response
        return {
            "message": "Cancellation request accepted and queued for processing",
            "request_id": request_id,
            "reference_code": alta_data.get('reference_code'),
            "session_code": alta_data.get('session_code'),
            "status": "PROCESSING"
        }
        
    except Exception as e:
        logger.error("Failed to process cancellation request: %s", str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process cancellation request: {str(e)}"
        ) from e
    