from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date
from services.auth import verify_basic_auth
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from services.logger import logger, payload_logger, log_payload
from config import settings, get_madrid_time_iso
router = APIRouter()

# Create security instance in this file
security = HTTPBasic()

    # payload = {
    #     "request_id": mnp_request_id,
    #     "reference_code": reference_code,
    #     "response_code": response_status,
    #     "description": description or f"Status update for MNP request {mnp_request_id}",
    #     "error_fields": error_fields or [],
    #     "porting_window_date": porting_window_date or ""
    # }

# Pydantic models for validation
class ErrorField(BaseModel):
    field_name: str = Field(
        ..., 
        example="request_date", 
        description="Field name that has error | WSDL: <v1:nombre>"
    )
    error_description: Optional[str] = Field(  # Changed from field_description
        None,
        example="Field is required", 
        description="Error description for this field | WSDL: <v1:descripcion>"
    )

class BSSWebhookRequest(BaseModel):
    request_id: int = Field(..., example="12", description="Request ID")
    response_code: str = Field(..., example="ASOL", description="Response code from central node")
    description: str = Field(..., example="Request created successfully", description="Response description")
    error_fields: Optional[List[ErrorField]] = Field(default=[], description="Array of error fields")
    reference_code: str = Field(..., example="REF_123456789", description="Unique reference code")
    porting_window_date: Optional[str] = Field(..., example="2025-10-15", description="Scheduled porting date")
    # Add the missing fields that your payload includes:
    session_code: Optional[str] = Field(None, example="20", description="Session code")
    msisdn: Optional[str] = Field(None, example="621800001", description="MSISDN number")
    response_status: Optional[str] = Field(None, example="ASOL", description="Response status")

@router.post(
    '/bss-webhook', 
    status_code=status.HTTP_200_OK,
    summary="Process MNP Porting Status Update",
    description="""
    Webhook endpoint to receive and process MNP porting status updates from the central node.
    
    **Key Features:**
    - Validates incoming portability status updates
    - Logs all received payloads for audit purposes  
    - Processes response codes to determine next steps
    - Returns immediate acknowledgment to central node
    
    **Common Response Codes:**
    - `ASOL` - Request accepted successfully
    - `ACON` - Port-in request confirmed
    - `APOR` - Port-in completed
    - `AREC` - Port-in rejected
    - `4xx` - Client errors (missing fields, validation errors)
    - `5xx` - Server errors (internal system failures)
    """,
    response_description="Acknowledgement of webhook receipt",
    tags=["BSS Webhook"]
)
async def bss_webhook(request: BSSWebhookRequest):
    """
    Process MNP porting status updates from central node.
    
    This endpoint:
    1. Receives real-time updates about MNP porting requests from the central node
    2. Validates the incoming payload structure and required fields  
    3. Logs the complete payload for audit and debugging
    4. Acknowledges receipt with 200 OK response
    5. (Future) Updates internal systems with the porting status
    
    Args:
        request (BSSWebhookRequest): The portability status update containing:
            - request_id: Unique identifier for the MNP request | WSDL: Not applicable
            - response_code: Response status code | WSDL: <v1:codigoRespuesta>
            - description: Human-readable description | WSDL: <v1:descripcion>
            - error_fields: List of field-level errors (if any) | WSDL: <v1:campoErroneo>
            - reference_code: Unique request identifier | WSDL: <por:codigoReferencia>
            - porting_window_date: Scheduled porting date | WSDL: <por:fechaVentanaCambio>
    
    Returns:
        dict: Acknowledgement with processing status
        
    Raises:
        HTTPException: If payload validation fails (422)
        
    Example Request:
    ```json
    {
        "request_id": "12",
        "response_code": "ASOL",
        "description": "Request created successfully",
        "error_fields": [],
        "reference_code": "REF_123456789", 
        "porting_window_date": "2024-01-15"
    }
    ```
    """
    # Log the incoming request
    logger.info({
        "event": "bss_webhook_received",
        "request_id": request.request_id,
        "response_code": request.response_code,
        "reference_code": request.reference_code,
        "description": request.description,
        "porting_window_date": request.porting_window_date,
        "responce_status":request.response_status,
        "error_count": len(request.error_fields or [])
    })
    
    try:
        # Log complete payload for audit trail
        log_payload('MNP', 'UPDATE', 'REQUEST', request.dict())
        
        # Process based on response code
        if request.response_code.startswith('4'):
            status_nc = "REQUEST_FAILED"
        elif request.response_code.startswith('5'):
            status_nc = "SERVER_ERROR"
        elif request.response_code == 'ASOL':
            status_nc = "REQUEST_RESPONDED"
        elif request.response_code == 'ACON':
            status_nc = "PORTIN_CONFIRMED"
        elif request.response_code == 'APOR':
            status_nc = "PORTIN_COMPLETED"
        elif request.response_code == 'AREC':
            status_nc = "PORTIN_REJECTED"
        else:
            status_nc = "PENDING_CONFIRMATION"
        
        # TODO: Add business logic to process the update
        # - Update database with new status
        # - Trigger internal workflows
        # - Notify relevant systems
        
        logger.info({
            "event": "bss_webhook_processed",
            "request_id": request.request_id,
            "reference_code": request.reference_code,
            "response_code": request.response_code,
            "status_nc": status_nc,
            "status": "acknowledged"
        })
        
    except Exception as e:
        logger.error({
            "event": "bss_webhook_error",
            "reference_code": request.reference_code,
            "error": str(e)
        })
        return {
            "status": "ERROR", 
            "message": "Failed to process request",
            "reference_code": request.reference_code
        }
    
    return {
        "status": "OK", 
        "message": "Webhook processed successfully",
        "reference_code": request.reference_code,
        "timestamp": get_madrid_time_iso()
    }