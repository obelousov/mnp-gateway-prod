from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from config import settings
import time
from services.database_service import save_portin_request_db, save_cancel_request_db, save_portability_request_new, check_if_cancel_request_id_in_db
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
from porting.spain_nc import submit_to_central_node_online

router = APIRouter()

# Create security instance in this file
security = HTTPBasic()

# To secure Swagger docs
@router.get("/docs", include_in_schema=False)
async def get_documentation(username: str = Depends(verify_basic_auth)):
    """Serve Swagger UI documentation with Basic Auth protection"""
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Docs")

@router.get(
    "/healthcheck",
    dependencies=[Depends(verify_basic_auth)],
    summary="Health Check",
    description="Check the health status of the MNP Gateway service",
    response_description="Service health status with timestamp",
    tags=["Monitoring"]
)
async def health_check_mock():
    """
    MNP Gateway Health Check
    
    Returns the current health status of the service along with timestamp
    information in the configured timezone.
    
    - **status**: Current health status (always 'healthy' if service is running)
    - **service**: Name of the API service
    - **version**: Current API version
    - **timestamp**: Current time in ISO format with timezone
    - **timezone**: Configured timezone for the service
    """
    timezone_str = settings.TIME_ZONE
    container_tz = pytz.timezone(timezone_str)
    
    # Get current time in the specified timezone
    local_time = datetime.now(container_tz)
    
    return {
        "status": "healthy", 
        "service": settings.API_TITLE,
        "version": settings.API_VERSION,
        "timestamp": local_time.isoformat(),
        "timezone": timezone_str
    }

@router.post(
    "/query-msisdn",
    summary="Query MSISDN portability details", 
    description="Accept MSISDN query from BSS, store in DB, and forward to Central Node",
    include_in_schema=False  # This hides the endpoint from Swagger)
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

class IdentificationDocument(BaseModel):
    """ Identification document data | WSDL: `<por:documentoIdentificacion>`"""
    document_type: str = Field(
        ...,
        description="Document type | WSDL: `<por:tipoDocumento>`",
        examples=["NIE", "DNI", "PASSPORT"]
    )
    document_number: str = Field(
        ...,
        description="Document number | WSDL: `<por:numeroDocumento>`",
        examples=["Y30307876", "12345678X"]
    )

    @field_validator('document_type')
    def validate_document_type(cls, v): # pylint: disable=no-self-argument
        """Validate that document_type is one of the allowed types"""
        allowed_document_types = ["NIE", "CIF", "DNI", "PASSPORT"]
        if v not in allowed_document_types:
            raise ValueError(f'document_type must be one of: {", ".join(allowed_document_types)}')
        return v
    
class PersonalData(BaseModel):
    """ Personal data | WSDL: `<por:datosPersonales>`"""
    first_name: str = Field(
        ...,
        description="First name | WSDL: `<por:nombre>`",
        examples=["Jose", "Maria"]
    )
    first_surname: Optional[str] = Field(
        None,
        description="First surname | WSDL: `<por:primerApellido>`",
        examples=["Garcia", "Lopez"]
    )
    second_surname: Optional[str] = Field(
        None,
        description="Second surname | WSDL: `<por:segundoApellido>`",
        examples=["Martinez", "Fernandez"]
    )
    nationality: Optional[str] = Field(
        None,
        description="Nationality | WSDL: `<por:nacionalidad>`",
        examples=["ES", "FR", "DE"]
    )

class Subscriber(BaseModel):
    """ Subscriber data | WSDL: `<por:abonado>`"""
    identification_document: IdentificationDocument = Field(
        ...,
        description="Identification document data"
    )
    personal_data: PersonalData = Field(
        ...,
        description="Personal data | WSDL: `<por:datosPersonales>`"
    )

class PortInRequest(BaseModel):
    """
    Pydantic class to validate Port-In request payload SOAP method: `SolicitarAltaPortabilidadMovil`
    WSDL Reference: 'por:peticionCrearSolicitudIndividualAltaPortabilidadMovil>'
    """
    country_code: str = Field(
        ...,
        description="ISO 3166-1 alpha-3 country code",
        examples=["ESP", "ITA"]
    )
    @validator('country_code')
    def validate_country_code(cls, v):  # pylint: disable=no-self-argument
        """Validate country_code
        - Must be 3 letters of ISO country code
        - MNP API supports following country codes: ESP, ITA
        """
        allowed_country_codes = ["ESP", "ITA"]
  
        # Validate the country_code value (v is the actual value, not a dict)
        if v not in allowed_country_codes:
            raise ValueError(f'country_code must be one of: {", ".join(allowed_country_codes)}')
        
        return v
    session_code: str = Field(
        ...,
        description="Unique session identifier | WSDL: `<v1:codigoSesion>`",
        examples=["13", "ABC123"]
    )
    requested_at: date = Field(
        ...,
        description="Request date | WSDL: `<por:fechaSolicitudPorAbonado>`",
        examples=["2024-01-10"]
    )
    donor_operator: str = Field(
        ...,
        description="Current operator | WSDL: `<por:codigoOperadorDonante>`",
        examples=["123"]
    )
    recipient_operator: str = Field(
        ...,
        description="New operator | WSDL: `<por:codigoOperadorReceptor>`",
        examples=["345"]
    )
    subscriber: Subscriber = Field(
        ...,
        description="Subscriber data | WSDL: `<por:abonado>`",
        example={
            "identification_document": {
                "document_type": "NIE", 
                "document_number": "Y30307876"
            },
            "personal_data": {
                "first_name": "Jose",
                "first_surname": "Garcia", 
                "second_surname": "Martinez",
                "nationality": "ES"
            }
        }
    )
    # subscriber: dict = Field(
    #     ...,
    #     description="Subscriber data | WSDL: `<por:abonado>`",
    #     example={
    #         "identification_document": {
    #             "document_type": "NIE",
    #             "document_number": "Y30307876"
    #         },
    #         "personal_data": {
    #             "name_surname": "Jose Diego"
    #         }
    #     }
    # )
    # @field_validator('msisdn')
    # def validate_msisdn(cls, v: str) -> str: # pylint: disable=no-self-argument
    #     """Validate that msisdn contains exactly 9 digits"""
    #     if not re.match(r'^\d{9}$', v):
    #         raise ValueError('msisdn must contain exactly 9 digits. Remove country code prefix if present.')
    #     return v


    @field_validator('donor_operator')
    def validate_donor_operator(cls, v: str) -> str: # pylint: disable=no-self-argument
        """Validate that donor_operator contains exactly 3 digits"""
        if not re.match(r'^\d{3}$', v):
            raise ValueError('donor_operator must contain exactly 3 digits')
        return v

    @field_validator('recipient_operator')
    def validate_recipient_operator(cls, v: str) -> str: # pylint: disable=no-self-argument
        """Validate that donor_operator contains exactly 3 digits"""
        if not re.match(r'^\d{3}$', v):
            raise ValueError('recipient_operator must contain exactly 3 digits')
        return v

    contract_number: str = Field(
        ...,
        description="Contract reference | WSDL: `<por:codigoContrato>`",
        examples=["CONTRACT_12345", "CTR_67890", "CNTR_2024_001"]
    )
    routing_number: str = Field(
        ...,
        description="Routing identifier | WSDL: `<por:NRNReceptor>`",
        examples=["NRN_RECEPTOR_001", "RN_2024001", "ROUTE_ABC123"]
    )
    desired_porting_date: Optional[date] = Field(
        None,
        description="Desired porting date (optional) | WSDL: `{fecha_ventana_optional}`",
        examples=["2025-10-20"]
    )
    iccid: Optional[str] = Field(
        None,
        description="SIM card identifier (optional) | WSDL: `{iccid_optional}`",
        examples=["89310410106543789310", "89310410106543789311"],
        min_length=20,
        max_length=20
    )
    msisdn: str = Field(
        ...,
        description="Phone number | WSDL: `<por:MSISDN>`",
        examples=["612345678"],
        pattern="^[0-9]{9}$"
    )

class PortInResponse_1(BaseModel):
    message: str = Field(..., examples=["Request accepted"])
    id: Union[str, int] = Field(..., examples=["PORT_IN_12345", 12345], description="Internal request ID")
    session_code: Union[str, int] = Field(..., examples=["13", 13], description="Original session code")
    status: str = Field(..., examples=["PROCESSING"], description="Current request status")    

class PortInResponse(BaseModel):
    id: int = Field(..., examples=[12345], description="Internal request ID")
    success: bool = Field(..., examples=[True, False])
    response_code: Optional[str] = Field(None, examples=["0000 00000", "ACCS PERME"])
    description: Optional[str] = Field(None, examples=["Operation successful", "No es posible invocar esta operación en horario inhábil"])
    reference_code: Optional[str] = Field(None, examples=["REF_12345"])

@router.post(
    '/port-in', 
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_basic_auth)],
    response_model=PortInResponse,
    summary="Submit Port-In Request",
    description="""
    Submit a new number portability request (Port-In) from the donor operator to recipient operator.
    
    This endpoint:
    - Accepts the request from BSS
    - Saves it to the database immediately  
    - Submit request to the task for processing, depending on working hours and timeband
    - Returns an immediate 202 Accepted response
    
    **Workflow:**
    1. Request validation and immediate database storage
    2. Submit request to task queue for processing initiation
    3. Async processing with Central Node
    4. Status check task initiated from central schduler (pending_requests task)
    """,
    response_description="Request accepted and queued for processing",
    tags=["Spain: Portability Operations"]
)
async def portin_request(alta_data: PortInRequest):
    """
    Port-In Number Portability Request
    
    Processes mobile number portability requests with comprehensive validation.
    
    **Key Validations:**
    - MSISDN format (Spanish numbering plan)
    - ICCID length and format
    - Document type validation
    - Date format and business logic
    
    **Background Processing:**
    - Central node communication
    - Operator coordination
    - Status tracking
    - Error handling and retries
    """
    try:
        logger.info("Processing port-in request")
    
        # Convert Pydantic model to dict for existing functions
        alta_data_dict = alta_data.dict()
        
        # Conditional payload logging
        log_payload('BSS', 'PORT_IN', 'REQUEST', str(alta_data_dict))

        # 1. & 2. Create and save the DB record
        new_request_id = save_portability_request_new(alta_data_dict, 'PORT_IN', 'ESP')
        if not new_request_id:
           raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Failed to create portability request record"
            )
        logger.info("Port-in request saved with ID: %s", new_request_id)
        # 3. Launch the background task, passing the ID of the new record
        # submit_to_central_node.delay(new_request_id) # Asynchronous version
        success, response_code, description, reference_code = submit_to_central_node_online(new_request_id)  # Synchronous version for testing

        response_data = {
        "id": new_request_id,
        "success": success,
        "response_code": response_code,
        "description": description,
        "reference_code": reference_code
    }

        # Determine appropriate status code based on success and response_code
        if success:
            return response_data  # FastAPI will use 200 by default, or you can set 202
        else:
            # Map different error types to appropriate HTTP status codes
            if response_code in ["NOT_FOUND", "VALIDATION_ERROR"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=response_data
                )
            elif response_code == "HTTP_ERROR":
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,  # or 504 Gateway Timeout
                    detail=response_data
                )
            elif response_code == "ACCS PERME":  # Outside business hours
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=response_data
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=response_data
                )

        # 4. Tell the BSS "We got it, processing now."
        return {
            "message": "Request accepted", 
            "id": new_request_id,
            "session_code": alta_data.session_code,
            "status": "PROCESSING"
        }
        
    except HTTPException:
        # Re-raise existing HTTP exceptions
        raise
        
    except ValueError as e:
        # Data validation errors
        logger.warning("Validation error: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request: {str(e)}"
        ) from e
        
    except Exception as e:
        # All other unexpected errors
        logger.error("Server error processing port-in: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error processing request"
        ) from e

class CancellationReason(str, Enum):
    """Allowed cancellation reasons"""
    SUBSCRIBER_REQUEST = "SUBSCRIBER_REQUEST"
    OPERATOR_ERROR = "OPERATOR_ERROR"
    TECHNICAL_ISSUE = "TECHNICAL_ISSUE"
    OTHER = "OTHER"

class CancelPortabilityRequest(BaseModel):
    """
    Model for portability cancellation request validation | SOAP method: `CancelarSolicitudAltaPortabilidadMovil`
    WSDL Reference: 'por:peticionConsultarProcesosPortabilidadMovil'
    """
    cancel_request_id: int = Field(
        ...,
        description="ID returned by MNP GW on initial portin request",
        examples=["12"]
        # min_length=5
    )
    reference_code: str = Field(
        ...,
        description="Portability request reference code returned by NC| WSDL: `por:codigoReferencia`",
        examples=["PORT_IN_12345", "REF_2024_001"],
        min_length=5
    )
    msisdn: str = Field(
        ...,
        description="Mobile Station International Subscriber Directory Number (phone number) | WSDL: `<por:MSISDN>`",
        examples=["34600000001", "34600000002"],
        pattern="^34[0-9]{9}$"
    )
    cancellation_reason: CancellationReason = Field(
        ...,
        description="Reason for cancelling the portability request | WSDL: `<por:causaEstado>`",
        examples=["SUBSCRIBER_REQUEST", "OPERATOR_ERROR", "TECHNICAL_ISSUE", "OTHER"]
    )
    cancellation_initiated_by_donor: bool = Field(
        ...,
        description="Indicates if cancellation is initiated by donor operator | WSDL: `<por:cancelacionIniciadaPorDonante>`",
        examples=[True, False]
    )
    session_code: Optional[str] = Field(
        None,
        description="Session identifier for tracking | WSDL: `<v1:codigoSesion>`",
        examples=["SESSION_001", "13"]
    )

class CancelPortabilityResponse(BaseModel):
    message: str = Field(..., examples=["Cancellation request accepted and queued for processing"])
    request_id: int = Field(..., examples=[12345], description="Internal cancellation request ID")
    reference_code: str = Field(..., examples=["PORT_IN_12345"], description="Original reference code")
    msisdn: str = Field(..., examples=["34600000001"], description="Phone number being cancelled")
    session_code: Optional[str] = Field(None, examples=["SESSION_001"], description="Session code if provided")
    status: str = Field(..., examples=["PROCESSING"], description="Current request status")

@router.post(
    '/cancel', 
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_basic_auth)],
    response_model=CancelPortabilityResponse,
    summary="Submit Portability Cancellation Request",
    description="""
    Cancel an existing number portability request.
    
    This endpoint:
    - Accepts cancellation requests from BSS. Request_id must correspond to existing port-in request ID
    - Immediately saves the cancellation to database
    - Queues the task for Central Node processing
    - Returns immediate 202 Accepted response
    
    **Workflow:**
    1. Request validation and immediate database storage
    2. Background processing initiation with Central Node
    3. Notification to relevant operators
    4. Status updates via separate endpoints
    
    **Note:** Only pending portability requests can be cancelled.
    """,
    response_description="Cancellation request accepted and queued for processing",
    tags=["Spain: Portability Operations"],
    responses={
        202: {
            "description": "Cancellation request accepted successfully",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Cancellation request accepted and queued for processing",
                        "request_id": 12345,
                        "reference_code": "PORT_IN_12345",
                        "msisdn": "34600000001",
                        "session_code": "SESSION_001",
                        "status": "PROCESSING"
                    }
                }
            }
        },
        400: {
            "description": "Invalid request data",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "reference_code is required"
                    }
                }
            }
        },
        404: {
            "description": "Original portability request not found",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Portability request PORT_IN_99999 not found"
                    }
                }
            }
        },
        500: {
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Failed to process cancellation request: Database connection error"
                    }
                }
            }
        }
    }
)
async def cancel_portability(request: CancelPortabilityRequest):
    """
    Cancel Portability Request Endpoint
    
    Processes cancellation requests for existing number portability operations.
    
    **Validation:**
    - Validates reference_code exists in system
    - Ensures original request is in cancellable state
    - Validates MSISDN format (Spanish numbering plan)
    - Validates cancellation reason format
    
    **Business Rules:**
    - Only pending portability requests can be cancelled
    - Donor-initiated cancellations have different workflows
    - Session code is preserved for audit tracking
    
    **Example Request:**
    ```json
    {
        "reference_code": "PORT_IN_12345",
        "msisdn": "34600000001",
        "cancellation_reason": "SUBSCRIBER_REQUEST",
        "cancellation_initiated_by_donor": false,
        "session_code": "SESSION_001"
    }
    ```
    """
    # Validate request exists FIRST (outside try-except)
    request_data = {"cancel_request_id": request.cancel_request_id}
    if not check_if_cancel_request_id_in_db(request_data):
        logger.warning("Portability request ID %s not found for cancellation", request.cancel_request_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portability request {request.cancel_request_id} not found"
        )
    try:
        logger.info("Processing cancellation request for ID: %s, reference: %s, MSISDN: %s", 
                   request.cancel_request_id, request.reference_code, request.msisdn)
        
        # Convert Pydantic model to dict for existing functions
        alta_data = request.dict()
        
        # 1. Log the incoming payload
        log_payload('BSS', 'CANCEL_PORTABILITY', 'REQUEST', str(alta_data))
        
        # 2. Save to database immediately
        request_id = save_cancel_request_db(alta_data, "CANCELLATION", "ESP")

        # 3. Submit to background task for processing
        submit_to_central_node_cancel.delay(request_id)
        
        # 3. Return immediate 202 Accepted response
        return {
            "message": "Cancellation request accepted and queued for processing",
            "request_id": request_id,
            "reference_code": request.reference_code,
            "msisdn": request.msisdn,
            "session_code": request.session_code,
            "status": "PROCESSING"
        }
        
    except Exception as e:
        logger.error("Failed to process cancellation request for MSISDN %s: %s", 
                    request.msisdn, str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process cancellation request: {str(e)}"
        ) from e