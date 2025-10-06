from fastapi import APIRouter, HTTPException, status
from config import settings, logger
import time
from services.database_service import save_portin_request_db
from tasks.tasks import submit_to_central_node


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
        logger.error(f"Error in port-in endpoint: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process request: {str(e)}"
        )

# async def query_msisdn_details(
#     request: MsisdnQueryRequest,
#     mnp_service: MnpService = Depends()
# ):
#     """
#     REST endpoint that accepts JSON, converts to SOAP, calls WSDL service,
#     and returns JSON response
#     """
#     try:
#         response = mnp_service.handle_msisdn_query(request)
#         return response
#     except HTTPException:
#         # Re-raise HTTPExceptions as they are already properly formatted
#         raise
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e