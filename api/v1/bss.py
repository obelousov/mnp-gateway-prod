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

# Pydantic models for validation
class CampoErroneo(BaseModel):
    nombre: str = Field(..., example="fechaSolicitud", description="Field name that has error")
    descripcion: Optional[str] = Field(None, example="Field is required", description="Error description for this field")

class BSSWebhookRequest(BaseModel):
    codigoRespuesta: str = Field(
        ..., 
        example="ASOL", 
        description="Response code from central node. Examples: 'ASOL' (success), '400' (client error), '500' (server error)"
    )
    descripcion: str = Field(
        ..., 
        example="Solicitud creada exitosamente", 
        description="Response description or error message from central node"
    )
    campoErroneo: Optional[List[CampoErroneo]] = Field(
        default=[],
        example=[{"nombre": "fechaSolicitud", "descripcion": "Field is required"}],
        description="Array of error fields with details. Empty array for successful responses"
    )
    codigoReferencia: str = Field(
        ..., 
        example="REF_123456789", 
        description="Unique reference code for tracking the portability request"
    )
    fechaVentanaCambio: str = Field(
        ..., 
        example="2024-01-15", 
        description="Scheduled porting date in YYYY-MM-DD format"
    )

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
    - `4xx` - Client errors (missing fields, validation errors)
    - `5xx` - Server errors (internal system failures)
    """,
    response_description="Acknowledgement of webhook receipt",
    tags=["BSS Webhooks"]
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
            - codigoRespuesta: Response status code
            - descripcion: Human-readable description
            - campoErroneo: List of field-level errors (if any)
            - codigoReferencia: Unique request identifier
            - fechaVentanaCambio: Scheduled porting date
    
    Returns:
        dict: Acknowledgement with processing status
        
    Raises:
        HTTPException: If payload validation fails (422)
        
    Example Request:
    ```json
    {
        "codigoRespuesta": "ASOL",
        "descripcion": "Solicitud creada exitosamente",
        "campoErroneo": [],
        "codigoReferencia": "REF_123456789", 
        "fechaVentanaCambio": "2024-01-15"
    }
    ```
    """
    # Log the incoming request
    logger.info({
        "event": "bss_webhook_received",
        "codigoRespuesta": request.codigoRespuesta,
        "codigoReferencia": request.codigoReferencia,
        "descripcion": request.descripcion,
        "fechaVentanaCambio": request.fechaVentanaCambio,
        "error_count": len(request.campoErroneo or [])
    })
    
    try:
        # Log complete payload for audit trail
        log_payload('MNP', 'UPDATE', 'REQUEST', request.dict())
        
        # TODO: Add business logic to process the update
        # - Update database with new status
        # - Trigger internal workflows
        # - Notify relevant systems
        
        logger.info({
            "event": "bss_webhook_processed",
            "codigoReferencia": request.codigoReferencia,
            "status": "acknowledged"
        })
        
    except Exception as e:
        logger.error({
            "event": "bss_webhook_error",
            "codigoReferencia": request.codigoReferencia,
            "error": str(e)
        })
        return {
            "status": "ERROR", 
            "message": "Failed to process request",
            "reference_code": request.codigoReferencia
        }
    
    return {
        "status": "OK", 
        "message": "Webhook processed successfully",
        "reference_code": request.codigoReferencia,
        "timestamp": get_madrid_time_iso()
    }