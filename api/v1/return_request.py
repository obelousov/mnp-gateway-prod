from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from config import settings
import time
from services.database_service import save_cancel_request_db, save_portability_request_new, check_if_cancel_request_id_in_db, check_if_cancel_request_id_in_db_online, save_cancel_request_db_online
from tasks.tasks import submit_to_central_node, submit_to_central_node_cancel
from services.logger import logger, payload_logger, log_payload
from pydantic import BaseModel, Field, validator, field_validator
import re
from typing import Optional, Union
from datetime import datetime, date
import pytz
from enum import Enum
from services.auth import verify_basic_auth
from fastapi.openapi.docs import get_swagger_ui_html
from ..core.metrics import record_port_in_success, record_port_in_error, record_port_in_processing_time
from services.database_service import save_return_request_db, check_if_cancel_return_request_in_db
from porting.spain_nc_return import submit_to_central_node_return, submit_to_central_node_cancel_return, submit_to_central_node_return_status_check
from services.database_service import save_cancel_return_request_db
from typing import Dict, Any

router = APIRouter()

# Create security instance in this file
security = HTTPBasic()

# To secure Swagger docs
@router.get("/docs", include_in_schema=False)
async def get_documentation(username: str = Depends(verify_basic_auth)):
    """Serve Swagger UI documentation with Basic Auth protection"""
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Docs")


class ReturnRequestOnline(BaseModel):
    """
    Model for number return request validation | SOAP method: `CrearSolicitudBajaNumeracionMovil`
    WSDL Reference: 'por:peticionCrearSolicitudBajaNumeracionMovil'
    """
    msisdn: str = Field(
        ...,
        description="Phone number | WSDL: `<por:MSISDN>`",
        examples=["621800000"],
        pattern="^[0-9]{9}$"
    )
    request_date: str = Field(
        ...,
        description="Date when the subscriber return is received | WSDL: `<por:fechaBajaAbonado>` in YYYY-MM-DD format",
        examples=["2025-12-25", "2025-11-25"]
    )

    @validator('request_date')
    def validate_and_format_datetime(cls, v): # pylint: disable=no-self-argument
        """Validate and format request_date to 'YYYY-MM-DD'"""
        if v is None:
            return None
            
        if isinstance(v, datetime):
            # Convert datetime object to required string format with fixed time
            return v.strftime('%Y-%m-%d')
        elif isinstance(v, str):
            # Try to parse various formats and convert to required format
            formats_to_try = [
                '%Y-%m-%dT%H:%M:%S',  # Exact required format
                '%d/%m/%Y %H:%M:%S',  # DD/MM/YYYY with time
                '%Y-%m-%d %H:%M:%S',  # ISO with space
                '%d/%m/%Y',           # Date only with slashes
                '%Y-%m-%d',           # ISO date only
            ]
            
            for fmt in formats_to_try:
                try:
                    parsed_dt = datetime.strptime(v, fmt)
                    # Always format as YYYY-MM-DDT02:00:00 regardless of input time
                    return parsed_dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
                    
            raise ValueError('Date must be in YYYY-MM-DD format or other common date formats')
        
        raise ValueError('Invalid date format')

class CampoErroneo(BaseModel):
    """Erroneous field details | WSDL: `co-v1-10:CampoErroneo`"""
    nombre: str = Field(
        ...,
        max_length=32,
        description="Nombre del campo erróneo | WSDL: `co-v1-10:nombre`",
        examples=["codigoOperadorDonante", "documentNumber", "iccid"]
    )
    descripcion: str = Field(
        ...,
        max_length=512,
        description="Descripción del error | WSDL: `co-v1-10:descripcion`",
        examples=[
            "Campo con restricción de longitud fija de 3 caracteres, se recibieron 10 caracteres",
            "Formato de fecha inválido, debe ser YYYY-MM-DDT02:00:00"
        ]
    )

class ReturnRequestResponseOnline(BaseModel):
    """ Return request response model | SOAP method: `CrearSolicitudBajaNumeracionMovilResponse`  
        WSDL Reference: 'por:respuestaCrearSolicitudBajaNumeracionMovil'
    """
    success: bool = Field(
        ...,
        examples=[True, False],
        description="Indicates if the operation was successful"
    )    
    response_code: str = Field(
        ...,
        max_length=10,
        examples=["0000 00000", "ACCS PERME", "AREC HORFV"],
        description="Código de respuesta. 10 caracteres máximo"
    )
    reference_code: Optional[str] = Field(
        ...,
        examples=["29979821251124155700001"],
        description="Código de referencia"
    )
    description: str = Field(
        ...,
        max_length=512,
        examples=["some description"],
        description="Descripcion de la respuesta. 512 caracteres máximo"
    )
    campo_erroneo: Optional[CampoErroneo] = Field(
        None,
        description="Detalles del campo erróneo si la solicitud falló | WSDL: `co-v1-10:CampoErroneo`",
        examples=[{
            "nombre": "fechaBajaAbonado",
            "descripcion": "Formato de fecha inválido, debe ser YYYY-MM-DDT02:00:00"
        }]
    )

@router.post(
    '/return-request', 
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_basic_auth)],
    response_model=ReturnRequestResponseOnline,
    summary="Submit Number Return Request",
    description="""
    Create a request to return a mobile number (repatriation).
    
    This endpoint:
    - Accepts number return requests from BSS
    - Validates and formats the request date to YYYY-MM-DDT02:00:00
    - Processes the request through Central Node
    
    **Workflow:**
    1. Request validation and date formatting
    2. Save request in DB
    3. SOAP request to CrearSolicitudBajaNumeracionMovil
    4. Response processing and return
  
    """,
    response_description="Return request processed",
    tags=["Spain: Portability Operations"],
    responses={
        202: {
            "description": "Return request accepted successfully",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "response_code": "0000 00000", 
                        "description": "Solicitud de baja procesada correctamente",
                        "campo_erroneo": None
                    }
                }
            }
        },
        400: {
            "description": "Invalid request data",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "request_date must be in YYYY-MM-DD format"
                    }
                }
            }
        },
        500: {
            "description": "Internal server error", 
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Failed to process return request: Connection error"
                    }
                }
            }
        }
    }
)
async def create_return_request_online(request: ReturnRequestOnline):
    """
    Create Number Return Request Endpoint
    
    Processes requests for returning mobile numbers (subscriber cancellations).
    
    **Validation:**
    - Validates MSISDN format (Spanish numbering plan)
    - Validates and formats request date to YYYY-MM-DD
    - Ensures session code is properly handled
    
    **Business Rules:**
    - Date is always formatted with 02:00:00 time
    - MSISDN must follow Spanish numbering plan - 9 digits
    
    **Example Request:**
    ```json
    {
        "msisdn": "34600000001",
        "request_date": "2025-12-25",
    }
    ```
    """
    try:
        logger.info("--- Processing return request --- for MSISDN: %s", request.msisdn)
        
        # Convert Pydantic model to dict
        return_data = request.dict()
        
        # 1. Log the incoming payload
        log_payload('BSS', 'RETURN_REQUEST', 'REQUEST', str(return_data))

        new_request_id = save_return_request_db(return_data)
        if not new_request_id:
           raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Failed to create retrun request record"
            )
        logger.info("Return request saved with ID: %s", new_request_id)

              
        # 3. Submit to NC and get response (you'll need to implement this function)
        success, response_code, reference_code, description = submit_to_central_node_return(new_request_id)
        
        # 4. Return the NC response
        return ReturnRequestResponseOnline(
            success=success,
            response_code=response_code or "UNKNOWN",
            reference_code=reference_code or "",
            description=description or "No response from NC",
            campo_erroneo=None
        )
        
    except ValueError as e:
        logger.error("Validation error for return request MSISDN %s: %s", request.msisdn, str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request data: {str(e)}"
        ) from e
    except Exception as e:
        logger.error("Failed to process return request for MSISDN %s: %s", request.msisdn, str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process return request: {str(e)}"
        ) from e

class ReturnCancelRequestOnline(BaseModel):
    """
    Model for number return cancel request validation | SOAP method: `CancelarSolicitudBajaNumeracionMovil`
    WSDL Reference: 'por:peticionCancelarSolicitudBajaNumeracionMovil'
    """
    reference_code: str = Field(
        ...,
        description="Return request reference code returned by NC| WSDL: `por:codigoReferencia`",
        examples=["29979811251121115100008"],
        min_length=5
    )
    cancellation_reason: str = Field(
        ...,
        description="Reason for cancelling the return request | WSDL: `<por:causaEstado>`",
        examples=["CANC_ABONA"]
    )

class ReturnCancelRequestResponseOnline(BaseModel):
    """ Return request response model | SOAP method: `CrearSolicitudBajaNumeracionMovilResponse`  
        WSDL Reference: 'por:respuestaCrearSolicitudBajaNumeracionMovil'
    """
    success: bool = Field(
        ...,
        examples=[True, False],
        description="Indicates if the operation was successful"
    )    
    response_code: str = Field(
        ...,
        max_length=10,
        examples=["0000 00000", "ACCS PERME", "AREC HORFV"],
        description="Código de respuesta. 10 caracteres máximo"
    )
    description: str = Field(
        ...,
        max_length=512,
        examples=["some description"],
        description="Descripcion de la respuesta. 512 caracteres máximo"
    )
    campo_erroneo: Optional[CampoErroneo] = Field(
        None,
        description="Detalles del campo erróneo si la solicitud falló | WSDL: `co-v1-10:CampoErroneo`",
        examples=[{
            "nombre": "fechaBajaAbonado",
            "descripcion": "Formato de fecha inválido, debe ser YYYY-MM-DDT02:00:00"
        }]
    )

@router.post(
    '/return-cancel', 
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_basic_auth)],
    response_model=ReturnCancelRequestResponseOnline,
    summary="Submit Return Cancel Request",
    description="""
    Create a request to cancel Return request (cancel repatriation).
    
    This endpoint:
    - Accepts number return requests from BSS
    - Processes the request through Central Node
    
    **Workflow:**
    1. Request validation and date formatting
    2. Save request in DB
    3. SOAP request to CancelarSolicitudBajaNumeracionMovil
    4. Response processing and return
  
    """,
    response_description="Cancel Return request processed",
    tags=["Spain: Portability Operations"],
    responses={
        202: {
            "description": "cancel Return request accepted successfully",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "response_code": "0000 00000", 
                        "description": "Solicitud de baja procesada correctamente",
                        "campo_erroneo": None
                    }
                }
            }
        },
        400: {
            "description": "Invalid request data",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "reference_code in wrong formnat"
                    }
                }
            }
        },
        500: {
            "description": "Internal server error", 
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Failed to process Cancel Return request: Connection error"
                    }
                }
            }
        }
    }
)
async def create_cancel_return_request_online(request: ReturnCancelRequestOnline):
    """
    Create Cancel Return Request Endpoint
    
    Processes requests for cancel of return number (cancle retariatin).
    
    **Validation:**
    - Validates reference_code (should present id DB)
    - Validates cancellation reason
    
    **Business Rules:**  
    
    **Example Request:**
    ```json
    {
    "reference_code": "29979811251121115100008",
    "cancellation_reason": "CANC_ABONA",
    }
    ```
    """
    try:
        reference_code = request.reference_code
        logger.info("--- Processing cancel return request --- for ref_code : %s", reference_code)
        
        # Convert Pydantic model to dict
        return_data = request.dict()
        
        res = check_if_cancel_return_request_in_db(return_data)
        logger.debug("Check if return request exists in DB result: %s", res)

        if not check_if_cancel_return_request_in_db(return_data):
            logger.warning("Return request ID %s not found for cancellation", reference_code)
            raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Return request {reference_code} not found"
        )

        # 1. Log the incoming payload
        log_payload('BSS', 'CANCEL_RETURN', 'REQUEST', str(return_data))

        new_request_id = save_cancel_return_request_db(return_data)
        if not new_request_id:
           raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Failed to save cancel return request record"
            )
        logger.info("Cancel Return request saved with ID: %s", new_request_id)

              
        # 3. Submit to NC and get response (you'll need to implement this function)
        success, response_code, description = submit_to_central_node_cancel_return(new_request_id)
        
        # 4. Return the NC response
        return ReturnCancelRequestResponseOnline(
            success=success,
            response_code=response_code or "UNKNOWN",
            description=description or "No response from NC",
            campo_erroneo=None
        )
        
    except HTTPException:
        # re-raise HTTPExceptions without modification
        raise
    except ValueError as e:
        logger.error("Validation error for cancel return request ref_code %s: %s", reference_code, str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request data: {str(e)}"
        ) from e
    except Exception as e:
        logger.error("Failed to process cancel return request for ref_code %s: %s", reference_code, str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process cancel return request: {str(e)}"
        ) from e


class ReturnStatusRequestOnline(BaseModel):
    """
    Model for checking return request status | SOAP method: `ConsultarEstadoSolicitudBajaNumeracionMovil`
    WSDL Reference: 'por:peticionConsultarEstadoSolicitudBajaNumeracionMovil'
    """
    reference_code: str = Field(
        ...,
        description="Return request reference code returned by NC| WSDL: `por:codigoReferencia`",
        examples=["29979811251121115100008"],
        min_length=5
    )

class ReturnStatusResponseOnline(BaseModel):
    """ Return request status response model | SOAP method: `ConsultarEstadoSolicitudBajaNumeracionMovilResponse`  
        WSDL Reference: 'por:respuestaConsultarEstadoSolicitudBajaNumeracionMovil'
    """
    success: bool = Field(
        ...,
        examples=[True, False],
        description="Indicates if the operation was successful"
    )    
    response_code: str = Field(
        ...,
        max_length=10,
        examples=["0000 00000", "ACCS PERME", "AREC HORFV"],
        description="Código de respuesta. 10 caracteres máximo"
    )
    response_status: str = Field(
        ...,
        max_length=50,
        examples=["APROBADO", "RECHAZADO", "PENDIENTE"],
        description="Estado actual de la solicitud de baja"
    )
    description: str = Field(
        ...,
        max_length=512,
        examples=["some description"],
        description="Descripcion de la respuesta. 512 caracteres máximo"
    )
    campo_erroneo: Optional[CampoErroneo] = Field(
        None,
        description="Detalles del campo erróneo si la solicitud falló | WSDL: `co-v1-10:CampoErroneo`",
        examples=[{
            "nombre": "codigoReferencia",
            "descripcion": "Código de referencia no encontrado"
        }]
    )

@router.post(
    '/return-status', 
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_basic_auth)],
    # response_model=ReturnStatusResponseOnline,
    summary="Check Return Request Status",
    description="""
    Check the current status of a Return request.
    
    This endpoint:
    - Accepts reference code of existing return request
    - Queries Central Node for current status
    - Returns response code, status and description
  
    **Workflow:**
    1. Request validation
    2. Check if reference code exists in DB
    3. SOAP request to ConsultarEstadoSolicitudBajaNumeracionMovil
    4. Response processing and return
  
    """,
    response_description="Return request status retrieved",
    tags=["Spain: Portability Operations"],
    responses={
        200: {
            "description": "Return request status retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "response_code": "0000 00000", 
                        "response_status": "APROBADO",
                        "description": "Solicitud de baja aprobada correctamente",
                        "campo_erroneo": None
                    }
                }
            }
        },
        400: {
            "description": "Invalid request data",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "reference_code in wrong format"
                    }
                }
            }
        },
        404: {
            "description": "Return request not found",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Return request 29979811251121115100008 not found"
                    }
                }
            }
        },
        500: {
            "description": "Internal server error", 
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Failed to retrieve return request status: Connection error"
                    }
                }
            }
        }
    }
)
async def create_return_status_request_online(request: ReturnStatusRequestOnline) -> Dict[str, Any]:
    """
    Check Return Request Status Endpoint
    
    Retrieves current status of return number request from Central Node.
    """
    try:
        reference_code = request.reference_code
        logger.info("--- Checking return request status --- for ref_code: %s", reference_code)
        
        # Convert Pydantic model to dict
        status_data = request.dict()
        
        # Check if return request exists in DB
        if not check_if_cancel_return_request_in_db(status_data):
            logger.warning("Return request ID %s not found for status check", reference_code)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Return request {reference_code} not found"
            )

        # 1. Log the incoming payload
        log_payload('BSS', 'RETURN_STATUS', 'REQUEST', str(status_data))

        # 2. Query Central Node for status
        response_dict = submit_to_central_node_return_status_check(reference_code)
        
        # 3. Log the response payload
        log_payload('NC', 'RETURN_STATUS', 'RESPONSE', str(response_dict))
        
        # 4. Return the raw NC response dictionary directly
        return convert_spanish_to_english(response_dict)
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Validation error for return status request ref_code %s: %s", reference_code, str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request data: {str(e)}"
        ) from e
    except Exception as e:
        logger.error("Failed to retrieve return request status for ref_code %s: %s", reference_code, str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve return request status: {str(e)}"
        ) from e
    
def convert_spanish_to_english(response_dict: dict) -> dict:
    """Convert Spanish field names to English"""
    mapping = {
        "codigoRespuesta": "response_code",
        "descripcion": "description",
        "codigoReferencia": "reference_code", 
        "fechaEstado": "status_date",
        "fechaCreacion": "creation_date",
        "fechaBajaAbonado": "subscriber_cancellation_date",
        "codigoOperadorReceptor": "recipient_operator_code",
        "codigoOperadorDonante": "donor_operator_code",
        "estado": "status",
        "causaEstado": "status_reason",
        "fechaVentanaCambio": "change_window_date"
    }
    
    return {mapping.get(key, key): value for key, value in response_dict.items()}