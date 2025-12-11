from fastapi import APIRouter, Depends, HTTPException, status
from typing import Dict, Any, Optional
import logging
from pydantic import BaseModel, Field, validator
from porting.nc_msisdn_check import msisdn_status_check_nc
from services.auth import verify_basic_auth
from services.logger import logger, log_payload
from porting.nc_portin_check import portin_status_check_nc

# Set up logger
logger = logging.getLogger(__name__)

router = APIRouter()

# Request model
class MsisdnStatusRequest(BaseModel):
    msisdn: str = Field(..., min_length=9, max_length=9, 
                        description="MSISDN to check status (9 digits)",
                        example="552000023")
    reference_code: str = Field(..., min_length=23, max_length=23, 
                        description="reference_code",
                        example="29979811251210133400015")
    
    @validator('msisdn')
    def validate_msisdn(cls, v): # pylint: disable=no-self-argument
        """
        msisdn validator
        """
        # Remove any non-digit characters
        cleaned_msisdn = ''.join(filter(str.isdigit, v))
        
        if len(cleaned_msisdn) != 9:
            raise ValueError('MSISDN must be exactly 9 digits')
        
        if not cleaned_msisdn.isdigit():
            raise ValueError('MSISDN must contain only digits')
            
        return cleaned_msisdn

@router.post(
    '/portin-status', 
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_basic_auth)],
    summary="Check portin MSISDN Status",
    description="""
    Check the current status and portability information of an MSISDN with National Central.

    This endpoint:
    - Accepts MSISDN (9 digits)
    - Validates MSISDN format and performs basic cleaning
    - Queries National Central for current status via SOAP API
    - Returns comprehensive MSISDN information including operator details and portability status

    **Workflow:**
    1. MSISDN validation, cleaning and format verification
    2. SOAP request to ConsultarNumeracionPortabilidadMovil endpoint
    3. Response parsing and error handling
    4. Return structured response with MSISDN status information

    **Response Fields:**
    {
    "success": true,
    "process_type": "ALTA_PORTABILIDAD_MOVIL",
    "response_code": "0000 00000",
    "description": "La operación se ha realizado con éxito",
    "reference_code": "29900511251205163700104",
    "status": "AREC",
    "porting_date": "2025-12-15T02:00:00+01:00",
    "creation_date": "2025-12-05T16:37:11.764+01:00",
    "reject_reason": "RECH_NORES",
    "reject_date": "2025-12-09T14:00:00+01:00",
    "error_message": null
    }
    """,
    response_description="Status of MSISDN port-in status retrieved successfully",
    tags=["Spain: Portability Operations"],
    responses={
        200: {
            "description": "MSISDN port-in status retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "success": "true",
                        "process_type": "ALTA_PORTABILIDAD_MOVIL",
                        "response_code": "0000 00000",
                        "description": "La operación se ha realizado con éxito",
                        "reference_code": "29900511251205163700104",
                        "status": "AREC",
                        "porting_date": "2025-12-15T02:00:00+01:00",
                        "creation_date": "2025-12-05T16:37:11.764+01:00",
                        "reject_reason": "RECH_NORES",
                        "reject_date": "2025-12-09T14:00:00+01:00",
                        "error_message": "null"
                                        }
                }
            }
        },
        400: {
            "description": "Invalid MSISDN format or validation error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "MSISDN must be exactly 9 digits"
                    }
                }
            }
        },
        401: {
            "description": "Authentication required",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Not authenticated"
                    }
                }
            }
        },
        500: {
            "description": "Internal server error or NC connection failure", 
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Failed to retrieve MSISDN status: Connection timeout"
                    }
                }
            }
        }
    }
)
def create_portin_status_request(request: MsisdnStatusRequest) -> Dict[str, Any]:
    """
    MSISDN Status Check Endpoint
    
    Retrieves comprehensive status information for an MSISDN from National Central 
    including current operator, range ownership, and portability status.
    
    This synchronous endpoint provides real-time MSISDN verification and is essential
    for pre-portability validation and customer service inquiries.
    
    **Typical Use Cases:**
    - Portability status checks
    - Customer service number verification  

    """
    try:
        msisdn = request.msisdn
        logger.info("--- Checking portin status --- for MSISDN: %s", msisdn)
        
        # 1. Log the incoming payload
        log_payload('BSS', 'PORTIN_STATUS', 'REQUEST', str({"msisdn": msisdn}))

        # 2. Query National Central for status
        success, error_message, response_data = portin_status_check_nc(msisdn, reference_code=request.reference_code)
        
        # 3. Log the response payload
        log_payload('NC', 'PORTIN_STATUS', 'RESPONSE', str(response_data))
        
        if not success:
            # If NC call failed, return error details
            logger.error("POrtin status check failed for %s: %s", msisdn, error_message)

            return {
                "success": False,
                "msisdn": msisdn,
                "error_message": error_message
            }
        
        # 4. Return the NC response data directly (fields are already in English)
        logger.info("MSISDN status check successful for %s: %s", msisdn, (response_data or {}).get('response_code'))
        return {
            "success": True,
            **(response_data or {}),  # Unpack all the English field names directly
            "error_message": None
        }
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Validation error for MSISDN status request %s: %s", msisdn, str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid MSISDN format: {str(e)}"
        ) from e
    except Exception as e:
        logger.error("Failed to retrieve MSISDN status for %s: %s", msisdn, str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve MSISDN status: {str(e)}"
        ) from e