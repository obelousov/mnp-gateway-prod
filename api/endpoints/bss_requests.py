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
from porting.spain_nc import submit_to_central_node_online, submit_to_central_node_cancel_online, submit_to_central_node_cancel_online_sync 
from ..core.metrics import record_port_in_success, record_port_in_error, record_port_in_processing_time
from services.database_service import check_if_port_out_request_in_db, save_portability_request_person_legal
from porting.spain_nc import submit_to_central_node_port_out_reject, submit_to_central_node_port_out_confirm

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
    document_type: str = Field(..., description="Type of identification document (NIE, NIF, etc.)")
    document_number: str = Field(..., description="Document number")

    @validator('document_type')
    def validate_document_type(cls, v): # pylint: disable=no-self-argument
        """Validate that document_type is one of the allowed types"""
        valid_types = ["NIF", "CIF", "NIE", "PAS"]  # Add other types you support
        if v not in valid_types:
            raise ValueError(f"Document type must be one of: {', '.join(valid_types)}")
        return v

    @validator('document_number')
    def validate_document_number(cls, v, values): # pylint: disable=no-self-argument
        """Validate document_number format based on document_type"""
        if 'document_type' not in values:
            return v
            
        doc_type = values['document_type']
        v_clean = v.upper().replace(' ', '').replace('-', '')  # Clean input

        # validation_patterns = {
        #     "NIE": (r'^[XYZ]{1}[0-9]{7}[A-Z]{1}$', "NIE must be format: [X/Y/Z] + 7 digits + 1 letter (e.g., X1234567A)"),
        #     "NIF": (r'^[0-9]{8}[A-Z]{1}$', "NIF must be 8 digits + 1 letter (e.g., 12345678Z)"),
        #     # Add more patterns as needed
        # }
        validation_patterns = {
            "NIE": (
                r'^[XYZ]{1}[0-9]{7}[A-Z]{1}$',
                "NIE must be format: [X/Y/Z] + 7 digits + 1 letter (e.g., X1234567A)"
            ),
            "NIF": (
                r'^[0-9]{8}[A-Z]{1}$',
                "NIF must be 8 digits + 1 letter (e.g., 12345678Z)"
            ),
            "CIF": (
                r'^[ABCDEFGHJNPQRSUVW]{1}[0-9]{7}[0-9A-J]{1}$',
                (
                    "CIF must start with one letter (A, B, C, D, E, F, G, H, J, N, P, Q, R, S, U, V, or W), "
                    "followed by 7 digits and a final control character (digit or letter A–J). "
                    "Example: A12345678 or B1234567J"
                )
            )
        }

        if doc_type in validation_patterns:
            pattern, error_msg = validation_patterns[doc_type]
            if not re.match(pattern, v_clean):
                raise ValueError(f"{error_msg}. Got: {v}")
            
        return v_clean
    
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

class SubscriberType(str, Enum):
    PERSON = "person"
    COMPANY = "company"

class CompanyData(BaseModel):
    razon_social: str = Field(
        ...,
        description="Company legal name (razón social)",
        examples=["Empresa Ejemplo S.L."]
    )

class Subscriber(BaseModel):
    subscriber_type: SubscriberType = Field(
        ...,
        description="Type of subscriber: 'person' for individuals, 'company' for legal entities"
    )
    identification_document: IdentificationDocument
    personal_data: Optional[PersonalData] = None
    company_data: Optional[CompanyData] = None

    @validator('identification_document')
    def validate_document_matches_subscriber_type(cls, v, values):
        """Validate that document type matches subscriber type"""
        if 'subscriber_type' not in values:
            return v
            
        subscriber_type = values['subscriber_type']
        doc_type = v.document_type
        
        if subscriber_type == SubscriberType.COMPANY and doc_type not in ['CIF', 'NIF']:
            raise ValueError(f"Companies must use CIF or NIF document types, got: {doc_type}")
            
        if subscriber_type == SubscriberType.PERSON and doc_type not in ['NIE', 'PAS']:
            raise ValueError(f"Individuals must use NIE or PAS document types, got: {doc_type}")
            
        return v

    @validator('personal_data')
    def validate_personal_data(cls, v, values):
        if 'subscriber_type' not in values:
            return v
            
        if values['subscriber_type'] == SubscriberType.PERSON and not v:
            raise ValueError("personal_data is required for person subscribers")
        return v

    @validator('company_data')
    def validate_company_data(cls, v, values):
        if 'subscriber_type' not in values:
            return v
            
        if values['subscriber_type'] == SubscriberType.COMPANY and not v:
            raise ValueError("company_data is required for company subscribers")
        return v
    
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

    # session_code: str = Field(
    #     ...,
    #     description="Unique session identifier | WSDL: `<v1:codigoSesion>`",
    #     examples=["13", "ABC123"]
    # )
    requested_at: date = Field(
        ...,
        description="Request date | WSDL: `<por:fechaSolicitudPorAbonado>`",
        examples=["2024-01-10"]
    )
    donor_operator: str = Field(
        ...,
        description="Current operator | WSDL: `<por:codigoOperadorDonante>`",
        examples=["798"]
    )
    recipient_operator: str = Field(
        ...,
        description="New operator | WSDL: `<por:codigoOperadorReceptor>`",
        examples=["299"]
    )
    subscriber: Subscriber = Field(
        ...,
        description="Subscriber data | WSDL: `<por:abonado>`",
        example={
            "subscriber_type": "person",
            "identification_document": {
                "document_type": "NIE",
                "document_number": "Y3037876D"
            },
            "personal_data": {
                "first_name": "Jose",  
                "first_surname": "Alavaro",
                "second_surname": "Diego"
            }
        }
    )    # subscriber: dict = Field(

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
        examples=["299-TRAC_12"]
    )
    @validator('contract_number')
    def validate_contract_number(cls, v, values):
        if not v:
            raise ValueError("Contract number is required")
        
        contract_clean = v.strip()
        
        # Must be exactly 11 characters
        if len(contract_clean) != 11:
            raise ValueError(
                f"Contract number must be exactly 11 characters long. "
                f"Got: '{v}' (length: {len(contract_clean)})"
            )
        
        # Check if starts with recipient operator code (if available)
        if 'recipient_operator' in values and values['recipient_operator']:
            operator_code = values['recipient_operator']
            if not contract_clean.startswith(operator_code):
                raise ValueError(
                    f"Contract number must start with recipient operator code '{operator_code}'. "
                    f"Got: '{v}'"
                )
        
        return contract_clean
    routing_number: str = Field(
        ...,
        description="Routing identifier | WSDL: `<por:NRNReceptor>`",
        examples=["906299"]
    )
    @validator('routing_number')
    def validate_routing_number(cls, v):
        if not v:
            raise ValueError("Routing number (NRN) is required")
        
        routing_clean = v.strip()
        
        # Must be exactly 6 characters
        if len(routing_clean) != 6:
            raise ValueError(
                f"Routing number (NRN) must be exactly 6 characters long. "
                f"Got: '{v}' (length: {len(routing_clean)})"
            )
        
        return routing_clean
    # desired_porting_date: Optional[date] = Field(
    #     None,
    #     description="Desired porting date (optional) | WSDL: `{fecha_ventana_optional}`",
    #     examples=["2025-10-20"]
    # )
    desired_porting_date: Optional[Union[datetime, str]] = Field(
        None,
        description="Desired porting date in DD/MM/YYYY HH:MM:SS format | WSDL: `{fecha_ventana_optional}`",
        examples=["20/10/2025 02:00:00"]
    )

    @validator('desired_porting_date')
    def validate_and_format_datetime(cls, v):
        if v is None:
            return None
            
        if isinstance(v, datetime):
            # Convert datetime object to required string format
            return v.strftime('%d/%m/%Y %H:%M:%S')
        elif isinstance(v, str):
            # Try to parse various formats and convert to required format
            formats_to_try = [
                '%d/%m/%Y %H:%M:%S',  # Exact required format
                '%Y-%m-%d %H:%M:%S',  # ISO with time
                '%d/%m/%Y',           # Date only with slashes
                '%Y-%m-%d',           # ISO date only
            ]
            
            for fmt in formats_to_try:
                try:
                    parsed_dt = datetime.strptime(v, fmt)
                    return parsed_dt.strftime('%d/%m/%Y %H:%M:%S')
                except ValueError:
                    continue
                    
            raise ValueError('Date must be in DD/MM/YYYY HH:MM:SS format (e.g., "20/10/2025 02:00:00")')
        
        raise ValueError('Invalid date format')

    iccid: Optional[str] = Field(
        None,
        description="SIM card identifier (optional) | WSDL: `{iccid_optional}`",
        examples=["89214410106543789310"],
        min_length=20,
        max_length=20
    )
    @validator('iccid')
    def validate_iccid(cls, v):
        if not v:
            raise ValueError("ICCID is required")
        
        iccid_clean = v.strip()
        iccid_digits = re.sub(r'[^0-9]', '', iccid_clean)
        
        # If 20 digits, trim to 19 (remove last check digit)
        if len(iccid_digits) == 20:
            iccid_digits = iccid_digits[:19]
        
        # Ensure exactly 19 digits starting with 89
        if not re.match(r'^89[0-9]{17}$', iccid_digits):
            raise ValueError(
                f"ICCID must be exactly 19 digits starting with 89. "
                f"Got: {v} (length: {len(iccid_digits)})"
            )
        
        # Extract and validate Spain MCC (digits 3-5 should be 214 for Spain)
        mcc = iccid_digits[2:5]  # Positions 3,4,5 (0-indexed)
        if mcc != "214":
            raise ValueError(
                f"ICCID must have Spain MCC 214. "
                f"Found MCC: {mcc} in ICCID: {iccid_digits}"
            )
        
        return iccid_digits

    msisdn: str = Field(
        ...,
        description="Phone number | WSDL: `<por:MSISDN>`",
        examples=["621800000"],
        pattern="^[0-9]{9}$"
    )

class PortInResponse(BaseModel):
    id: int = Field(..., examples=[12345], description="Internal request ID")
    success: bool = Field(..., examples=[True, False])
    response_code: Optional[str] = Field(None, examples=["0000 00000", "ACCS PERME"])
    description: Optional[str] = Field(None, examples=["Operation successful", "No es posible invocar esta operación en horario inhábil"])
    reference_code: Optional[str] = Field(None, examples=["REF_12345"])
    porting_window_date: Optional[str] = Field(None, examples=["2025-11-12T02:00:00+01:00"], description="Scheduled porting date and time")

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
    - Submit request to NC
    - Returns NC response

    299 CUBEMOVIL FMVNO - 552000000 to  552009999 – NRN(906299) - recipinet operator
    798 CUBEMOVIL_FMVNO_DUMMY – 621800000 to 621899999 – NRN(704914) - donor operator
    
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
    start_time = time.time()
    try:
        logger.info("--- Processing port-in request ---")
        # logger.info("subscriber_type: %s", alta_data.subscriber.subscriber_type.value)
    
        # Convert Pydantic model to dict for existing functions
        alta_data_dict = alta_data.dict()
        
        # Conditional payload logging
        log_payload('BSS', 'PORT_IN', 'REQUEST', str(alta_data_dict))

        # 1. & 2. Create and save the DB record
        new_request_id = save_portability_request_person_legal(alta_data_dict, 'PORT_IN', 'ESP')
        if not new_request_id:
           raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Failed to create portability request record"
            )
        logger.info("Port-in request saved with ID: %s", new_request_id)
        # 3. Launch the background task, passing the ID of the new record
        # submit_to_central_node.delay(new_request_id) # Asynchronous version
        success, response_code, description, reference_code, porting_window_date = submit_to_central_node_online(new_request_id)  # Synchronous version for testing

    #     response_data = {
    #     "id": new_request_id,
    #     "success": success,
    #     "response_code": response_code,
    #     "description": description,
    #     "reference_code": reference_code
    # }

        response_data = {
            "id": new_request_id,
            "success": success,
            "reference_code":reference_code,
            "response_code": response_code,
            "description": description or f"Status update for MNP request {new_request_id}",
            # "error_fields": error_fields or [],
            "porting_window_date": porting_window_date or ""
        }

        logger.info("Port-in response: %s", response_data)
        # Determine appropriate status code based on success and response_code
        if success:
            return response_data  # 200 OK for successful operations
        else:
            # Business logic errors return 200 OK with error details
            if response_code == "ACCS PERME" or re.match(r"^AREC", (response_code or "")):
                return response_data  # 200 OK with business error details
    
        # Technical errors that should raise proper HTTP exceptions
            elif response_code in ["NOT_FOUND", "VALIDATION_ERROR"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=response_data
            )
            elif response_code == "HTTP_ERROR":
                raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=response_data
            )
            else:
                raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=response_data
            )
        # if success:
        #     return response_data  # FastAPI will use 200 by default, or you can set 202
        # else:
        #     # Map different error types to appropriate HTTP status codes
        #     if response_code in ["NOT_FOUND", "VALIDATION_ERROR"]:
        #         raise HTTPException(
        #             status_code=status.HTTP_400_BAD_REQUEST,
        #             detail=response_data
        #         )
        #     elif response_code == "HTTP_ERROR":
        #         raise HTTPException(
        #             status_code=status.HTTP_502_BAD_GATEWAY,  # or 504 Gateway Timeout
        #             detail=response_data
        #         )
        #     elif response_code == "ACCS PERME":  # Outside business hours
        #         raise HTTPException(
        #             status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        #             detail=response_data
        #         )
        #     elif re.match(r"^AREC", (response_code or "")):  # All AREC errors
        #         raise HTTPException(
        #             status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        #             detail=response_data
        #         )
        #     else:
        #         raise HTTPException(
        #             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        #             detail=response_data
        #         )

        # # 4. Tell the BSS "We got it, processing now."
        # return {
        #     "message": "Request accepted", 
        #     "id": new_request_id,
        #     "session_code": alta_data.session_code,
        #     "status": "PROCESSING"
        # }
        
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
    
    finally:
        processing_time = time.time() - start_time
        record_port_in_processing_time(processing_time)

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
    # session_code: Optional[str] = Field(
    #     None,
    #     description="Session identifier for tracking | WSDL: `<v1:codigoSesion>`",
    #     examples=["SESSION_001", "13"]
    # )

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
    include_in_schema=False,  # This hides it from Swagger
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
    

class CancellationReason_online(str, Enum):
    """Allowed cancellation reasons |  WSDL <por:causaEstado"""
    SUBSCRIBER_REQUEST = "CANC_ABONA"
    OPERATOR_ERROR = "CANC_ERROR"
    TECHNICAL_ISSUE = "CANC_TECNI"
    # OTHER = "OTHER"

class CancelPortabilityRequest_online(BaseModel):
    """
    Model for portability cancellation request validation | SOAP method: `CancelarSolicitudAltaPortabilidadMovil`
    WSDL Reference: 'por:peticionConsultarProcesosPortabilidadMovil'
    """
    # cancel_request_id: int = Field(
    #     ...,
    #     description="ID returned by MNP GW on initial portin request",
    #     examples=["12"]
    #     # min_length=5
    # )
    reference_code: str = Field(
        ...,
        description="Portability request reference code returned by NC| WSDL: `por:codigoReferencia`",
        examples=["29979811251023094300005"],
        min_length=23,
        max_length=23,
    )
    # msisdn: str = Field(
    #     ...,
    #     description="Mobile Station International Subscriber Directory Number (phone number) | WSDL: `<por:MSISDN>`",
    #     examples=["34600000001", "34600000002"],
    #     pattern="^34[0-9]{9}$"
    # )
    cancellation_reason: CancellationReason_online = Field(
        ...,
        description="Reason for cancelling the portability request | WSDL: `<por:causaEstado>`",
        examples=["CANC_ABONA", "CANC_TECNI", "CANC_ERROR"]
    )
    cancellation_initiated_by_donor: bool = Field(
        ...,
        description="Boolean indicando si la cancelacion fue iniciada por donante o por receptor | WSDL: `<por:cancelacionIniciadaPorDonante>`",
        examples=[True, False]
    )
    # session_code: Optional[str] = Field(
    #     None,
    #     description="Session identifier for tracking | WSDL: `<v1:codigoSesion>`",
    #     examples=["SESSION_001", "13"]
    # )

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
            "Formato de fecha inválido, debe ser DD/MM/YYYY HH:MM:SS"
        ]
    )

class CancelPortabilityResponse_online(BaseModel):
    """ Cancellation response model | SOAP method: `CancelarSolicitudAltaPortabilidadMovilResponse`  
        WSDL Reference: 'por:respuestaCancelarSolicitudAltaPortabilidadMovil'
    """
    # message: str = Field(..., examples=["Cancellation request accepted and queued for processing"])
    # request_id: int = Field(..., examples=[12345], description="Internal cancellation request ID")
    # reference_code: str = Field(..., examples=["PORT_IN_12345"], description="Original reference code")
    # msisdn: str = Field(..., examples=["34600000001"], description="Phone number being cancelled")
    # session_code: Optional[str] = Field(None, examples=["SESSION_001"], description="Session code if provided")
    # status: str = Field(..., examples=["PROCESSING"], description="Current request status")
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
        description="Detalles del campo erróneo si la cancelación falló | WSDL: `co-v1-10:CampoErroneo`",
        examples=[{
            "nombre": "codigoOperadorDonante",
            "descripcion": "Campo con restricción de longitud fija de 3 caracteres"
        }]
    )
    
@router.post(
    '/cancel-online', 
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_basic_auth)],
    response_model=CancelPortabilityResponse_online,
    summary="Submit Portability Cancellation Request",
    description="""
    Cancel an existing number portability request.
    
    This endpoint:
    - Accepts cancellation requests from BSS. 
    - Immediately saves the cancellation to database
    - Queues online for Central Node processing
    - Returns received response from NC
    
    **Workflow:**
    1. Request validation and immediate database storage
    
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
async def cancel_portability_online(request: CancelPortabilityRequest_online):
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
    logger.debug("Checking existence of portability request for cancellation: %s", request.reference_code)
    request_data = {"reference_code": request.reference_code}
    if not check_if_cancel_request_id_in_db_online(request_data):
        logger.warning("Portability reference code %s not found for cancellation", request.reference_code)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portability request {request.reference_code} not found"
        )
    
    try:
        logger.info("--- Processing cancellation request --- for reference: %s",
                   request.reference_code)
        
        # Convert Pydantic model to dict for existing functions
        alta_data = request.dict()
        # Convert enum to its string value
        if 'cancellation_reason' in alta_data and alta_data['cancellation_reason']:
            # alta_data['cancellation_reason'] = alta_data['cancellation_reason'].value
            alta_data['cancellation_reason'] = alta_data['cancellation_reason']

        # 1. Log the incoming payload
        log_payload('BSS', 'CANCEL_PORTABILITY', 'REQUEST', str(alta_data))
        
        # 2. Save to database immediately
        request_id = save_cancel_request_db_online(alta_data, "CANCELLATION", "ESP")

        # 3. Submit to NC and get response
        success, response_code, description = submit_to_central_node_cancel_online_sync(request_id)
        logger.debug("Success: %s", success)

        # 4. Return the NC response
        return CancelPortabilityResponse_online(
            success=success,
            response_code=response_code or "UNKNOWN",
            description=description or "No response from NC",
            campo_erroneo=None
        )
        
    except Exception as e:
        logger.error("Failed to process cancellation request for reference %s: %s", 
                    request.reference_code, str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process cancellation request: {str(e)}"
        ) from e
    

class CancellationReasonOnline(str, Enum):
    """Enumeration of valid cancellation reasons for portability requests."""
    
    RECH_IDENT = "RECH_IDENT"
    RECH_ICCID = "RECH_ICCID" 
    RECH_BNUME = "RECH_BNUME"
    RECH_FMAYO = "RECH_FMAYO"
    RECH_PERDI = "RECH_PERDI"
    RECH_NORES = "RECH_NORES"

class RejectPortOutRequest(BaseModel):
    """
    Model for reject port-out request | SOAP method: `RechazarSolicitudAltaPortabilidadMovil`
    WSDL Reference: '<por:peticionRechazarSolicitudAltaPortabilidadMovil>'
    """
    reference_code: str = Field(
        ...,
        description="Port-Out request reference code returned by NC | WSDL: `por:codigoReferencia`",
        examples=["29979811251023094300005"],
        min_length=23,
        max_length=23,
        pattern="^[0-9]{23}$"  # Ensures exactly 23 digits
    )
    cancellation_reason: CancellationReasonOnline = Field(
        ...,
        description="""Reason for cancelling the portability request | WSDL: `<por:causaEstado>`
        
**Valid Values:**
- `RECH_IDENT` - El identificador fiscal no corresponde con el MSISDN introducido
- `RECH_ICCID` - El ICC-ID no corresponde con el MSISDN introducido  
- `RECH_BNUME` - La numeracion esta en estado de baja
- `RECH_FMAYO` - Causa justificada de fuerza mayor
- `RECH_PERDI` - Tarjeta SIM denunciada al operador donante por robo o perdida
- `RECH_NORES` - Rechazo realizado por el NC actuando de oficio
""",
        examples=["RECH_IDENT", "RECH_ICCID", "RECH_BNUME"]
    )

class RejectPortOutresponse(BaseModel):
    """ Cancellation response model | SOAP method: `CancelarSolicitudAltaPortabilidadMovilResponse`  
        WSDL Reference: 'por:respuestaCancelarSolicitudAltaPortabilidadMovil'
    """
    # message: str = Field(..., examples=["Cancellation request accepted and queued for processing"])
    # request_id: int = Field(..., examples=[12345], description="Internal cancellation request ID")
    # reference_code: str = Field(..., examples=["PORT_IN_12345"], description="Original reference code")
    # msisdn: str = Field(..., examples=["34600000001"], description="Phone number being cancelled")
    # session_code: Optional[str] = Field(None, examples=["SESSION_001"], description="Session code if provided")
    # status: str = Field(..., examples=["PROCESSING"], description="Current request status")
    success: bool = Field(
        ...,
        examples=[True, False],
        description="Indicates if the operation was successful"
    )    
    response_code: str = Field(
        ...,
        max_length=20,
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
        description="Detalles del campo erróneo si la cancelación falló | WSDL: `co-v1-10:CampoErroneo`",
        examples=[{
            "nombre": "codigoOperadorDonante",
            "descripcion": "Campo con restricción de longitud fija de 3 caracteres"
        }]
    )


@router.post(
    '/port-out-reject', 
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_basic_auth)],
    response_model=CancelPortabilityResponse_online,
    summary="Submit Portability Cancellation Request",
    description="""
    Cancel an existing number portability request.
    
    This endpoint:
    - Accepts cancellation requests from BSS. 
    - Immediately saves the cancellation to database
    - Queues online for Central Node processing
    - Returns received response from NC
    
    **Workflow:**
    1. Request validation and immediate database storage
    
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
async def reject_port_out_request(request: RejectPortOutRequest):
    """
    Port-Out Reject Endpoint
    
    Processes reject of Port-Out request.
    
    **Validation:**
    - Validates reference_code exists in system
    - Validates cancellation reason format
    
    **Business Rules:**
    - Only pending portability requests can be cancelled
    
    **Example Request:**
    ```json
    {
        "reference_code": "79829911251103113000104",
        "cancellation_reason": "RECH_BNUME",
    }
    ```
    """
    # Validate request exists FIRST (outside try-except)
    reference_code = request.reference_code
    logger.debug("Checking existence of port-out request for reject: %s", reference_code)
    request_data = {"reference_code": request.reference_code}
    if not check_if_port_out_request_in_db(request_data):
        logger.warning("Reference code %s not found for Port-Out reject", reference_code)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Port-Out request {reference_code} not found"
        )
    
    try:
        logger.info("--- Processing Port-Out Reject request --- for reference: %s",
                   reference_code)
        
        # Convert Pydantic model to dict for existing functions
        alta_data = request.dict()
        # Convert enum to its string value
        alta_data['cancellation_reason'] = request.cancellation_reason.value  # Direct access with .value
        # if 'cancellation_reason' in alta_data and alta_data['cancellation_reason']:
        #     # alta_data['cancellation_reason'] = alta_data['cancellation_reason'].value
        #     alta_data['cancellation_reason'] = alta_data['cancellation_reason']

        # 1. Log the incoming payload
        # log_payload('BSS', 'PORT_OUT_REJECT', 'REQUEST', str(alta_data))
        # logger.debug("PORT_OUT_REJECT->NC:\n%s", str(alta_data))
        
        # 2. Save to database immediately
        # request_id = save_cancel_request_db_online(alta_data, "CANCELLATION", "ESP")

        # 3. Submit to NC and get response
        success, response_code, description = submit_to_central_node_port_out_reject(alta_data)
        logger.debug("Success: %s response_code %s description %s", success, response_code, description)

        # 4. Return the NC response
        return RejectPortOutresponse(
            success=success,
            response_code=response_code or "UNKNOWN",
            description=description or "No response from NC",
            campo_erroneo=None
        )
        
    except Exception as e:
        logger.error("Failed to process Port-Out Reject request for reference %s: %s", 
                    request.reference_code, str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process Port-Out Reject request: {str(e)}"
        ) from e    
    
class ConfirmPortOutRequest(BaseModel):
    """
    Model for reject port-out request | SOAP method: `RechazarSolicitudAltaPortabilidadMovil`
    WSDL Reference: '<por:peticionRechazarSolicitudAltaPortabilidadMovil>'
    """
    reference_code: str = Field(
        ...,
        description="Port-Out request reference code returned by NC | WSDL: `por:codigoReferencia`",
        examples=["29979811251023094300005"],
        min_length=23,
        max_length=23,
        pattern="^[0-9]{23}$"  # Ensures exactly 23 digits
    )

class ConfirmPortOutresponse(BaseModel):
    """ Cancellation response model | SOAP method: `CancelarSolicitudAltaPortabilidadMovilResponse`  
        WSDL Reference: 'por:respuestaCancelarSolicitudAltaPortabilidadMovil'
    """
    # message: str = Field(..., examples=["Cancellation request accepted and queued for processing"])
    # request_id: int = Field(..., examples=[12345], description="Internal cancellation request ID")
    # reference_code: str = Field(..., examples=["PORT_IN_12345"], description="Original reference code")
    # msisdn: str = Field(..., examples=["34600000001"], description="Phone number being cancelled")
    # session_code: Optional[str] = Field(None, examples=["SESSION_001"], description="Session code if provided")
    # status: str = Field(..., examples=["PROCESSING"], description="Current request status")
    success: bool = Field(
        ...,
        examples=[True, False],
        description="Indicates if the operation was successful"
    )    
    response_code: str = Field(
        ...,
        max_length=20,
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
        description="Detalles del campo erróneo si la cancelación falló | WSDL: `co-v1-10:CampoErroneo`",
        examples=[{
            "nombre": "codigoOperadorDonante",
            "descripcion": "Campo con restricción de longitud fija de 3 caracteres"
        }]
    )

@router.post(
    '/port-out-confirm', 
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_basic_auth)],
    response_model=ConfirmPortOutresponse,
    summary="Submit Port-out Confirm Request",
    description="""
    Confirm existing number port-out request.
    
    This endpoint:
    - Accepts confirmation requests from BSS. 
    - Update port-out request in portout_request table
    - Send confrim reuest to Central Node processing
    - Returns received response from NC
    - update status in portout_request table
    
    **Workflow:**
    1. Request validation and immediate database storage
    
    **Note:** Only pending portability requests can be cancelled.
    """,
    response_description="Confiorm request for port-out accepted and queued for processing",
    tags=["Spain: Portability Operations"],
    responses={
        202: {
            "description": "Confirm request accepted successfully",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Confirm request accepted and queued for processing",
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
async def confirm_port_out_request(request: ConfirmPortOutRequest):
    """
    Port-Out Confirm Endpoint
    
    Processes confirmation of Port-Out request.
    
    **Validation:**
    - Validates reference_code exists in system
    
    **Business Rules:**
    - Only pending portability requests can be cancelled
    
    **Example Request:**
    ```json
    {
        "reference_code": "79829911251103113000104"
    }
    ```
    """
    # Validate request exists FIRST (outside try-except)
    reference_code = request.reference_code
    logger.debug("Checking existence of port-out request for reject: %s", reference_code)
    request_data = {"reference_code": request.reference_code}
    if not check_if_port_out_request_in_db(request_data):
        logger.warning("Reference code %s not found for Port-Out reject", reference_code)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Port-Out request {reference_code} not found"
        )
    
    try:
        logger.info("--- Processing Port-Out Confirm request --- for reference: %s",
                   reference_code)
        
        # Convert Pydantic model to dict for existing functions
        alta_data = request.dict()
        # Convert enum to its string value
        # if 'cancellation_reason' in alta_data and alta_data['cancellation_reason']:
        #     alta_data['cancellation_reason'] = alta_data['cancellation_reason'].value

        # 1. Log the incoming payload
        # log_payload('BSS', 'PORT_OUT_REJECT', 'REQUEST', str(alta_data))
        # logger.debug("PORT_OUT_REJECT->NC:\n%s", str(alta_data))
        
        # 2. Save to database immediately
        # request_id = save_cancel_request_db_online(alta_data, "CANCELLATION", "ESP")

        # 3. Submit to NC and get response
        success, response_code, description = submit_to_central_node_port_out_confirm(alta_data)
        logger.debug("Success: %s response_code %s description %s", success, response_code, description)

        # 4. Return the NC response
        return RejectPortOutresponse(
            success=success,
            response_code=response_code or "UNKNOWN",
            description=description or "No response from NC",
            campo_erroneo=None
        )
        
    except Exception as e:
        logger.error("Failed to process Port-Out Confirm request for reference %s: %s", 
                    request.reference_code, str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process Port-Out Confirm request: {str(e)}"
        ) from e    

class PersonalDataLegal(BaseModel):
    """Personal data model for legal entities (companies)"""
    company_name: str = Field(
        ...,
        description="Legal company name | WSDL: `<v1:razonSocial>`",
        examples=["MyCompany SL", "Business Corp"]
    )
    
    # Optional fields for legal entities
    legal_representative: Optional[str] = Field(
        None,
        description="Legal representative name",
        examples=["Juan Garcia Lopez"]
    )
    
    @validator('company_name')
    def validate_company_name(cls, v): # pylint: disable=no-self-argument
        if not v or not v.strip():
            raise ValueError("Company name is required for legal entities")
        if len(v.strip()) < 2:
            raise ValueError("Company name must be at least 2 characters long")
        return v.strip()

class SubscriberLegal(BaseModel):
    """Subscriber model for legal entities"""
    subscriber_type: str = Field(
        "company",
        description="Subscriber type - must be 'company' for legal entities",
        examples=["company"]
    )
    
    identification_document: IdentificationDocument = Field(
        ...,
        description="Legal entity identification document"
    )
    
    personal_data: PersonalDataLegal = Field(
        ...,
        description="Legal entity personal data"
    )
    
    @validator('subscriber_type')
    def validate_subscriber_type(cls, v): # pylint: disable=no-self-argument
        """Ensure subscriber type is 'company' for legal entities"""
        if v != "company":
            raise ValueError('Subscriber type must be "company" for legal entities')
        return v
    
    @validator('identification_document')
    def validate_legal_document(cls, v): # pylint: disable=no-self-argument
        """Validate that legal entities use appropriate document types"""
        legal_document_types = ["CIF", "NIF", "VAT"]
        if v.document_type not in legal_document_types:
            raise ValueError(f'Legal entities must use one of: {", ".join(legal_document_types)}')
        return v

class PortInRequestLegal(PortInRequest):
    """
    Pydantic model for Legal Entity (company) Port-In requests.
    Ensures 'subscriber_type' = 'company' and 'document_type' = 'CIF' only.
    """
    
    subscriber: SubscriberLegal = Field(
        ...,
        description="Legal entity subscriber data | WSDL: `<por:abonado>`",
        example={
            "subscriber_type": "company",
            "identification_document": {
                "document_type": "CIF",
                "document_number": "A12345678"
            },
            "personal_data": {
                "company_name": "MyCompany SL"
            }
        }
    )

    # --- Contract number validation ---
    @validator('contract_number')
    def validate_contract_number_legal(cls, v, values): # pylint: disable=no-self-argument
        """Validate contract number for legal entities (companies)."""
        if not v:
            raise ValueError("Contract number is required for legal entities")

        contract_clean = v.strip()
        if len(contract_clean) < 8:
            raise ValueError(f"Contract number must be at least 8 characters long. Got: '{v}'")

        if 'recipient_operator' in values and values['recipient_operator']:
            operator_code = values['recipient_operator']
            if not contract_clean.startswith(operator_code):
                raise ValueError(
                    f"Contract number must start with recipient operator code '{operator_code}'. Got: '{v}'"
                )
        return contract_clean

    # --- Legal entity validation ---
    @validator('subscriber')
    def validate_legal_entity(cls, v): # pylint: disable=no-self-argument
        """Ensure the subscriber is a company using CIF only."""
        if v.subscriber_type != "company":
            raise ValueError('This endpoint only supports legal entities (subscriber_type must be "company").')

        doc_type = v.identification_document.document_type
        # if doc_type not in ["CIF", "NIF", "VAT"]:
        if doc_type != "CIF":
            raise ValueError(
                f'Invalid document_type "{doc_type}" for company subscriber. Legal entities must use "CIF".'
            )

        if not v.personal_data.company_name:
            raise ValueError('Company name is required for legal entity port-in requests.')

        return v

class PortInRequestLegal_02(PortInRequest):
    """
    Pydantic class to validate Port-In request payload for Legal Entities
    Extends PortInRequest with legal entity specific validations
    """
    
    # Override the subscriber field with legal entity specific validation
    subscriber: SubscriberLegal = Field(
        ...,
        description="Legal entity subscriber data | WSDL: `<por:abonado>`",
        example={
            "subscriber_type": "company",
            "identification_document": {
                "document_type": "CIF",
                "document_number": "A12345678"
            },
            "personal_data": {
                "company_name": "MyCompany SL"
            }
        }
    )
    
    # Override contract_number validation for corporate contracts
    @validator('contract_number')
    def validate_contract_number_legal(cls, v, values): # pylint: disable=no-self-argument
        """Validate contract number for legal entities (companies)."""
        if not v:
            raise ValueError("Contract number is required")
        
        contract_clean = v.strip()
        
        # For legal entities, contract might be longer than 11 characters
        # Adjust length validation if needed, or keep the same
        if len(contract_clean) < 8:  # More flexible minimum length
            raise ValueError(
                f"Contract number must be at least 8 characters long for legal entities. "
                f"Got: '{v}' (length: {len(contract_clean)})"
            )
        
        # Check if starts with recipient operator code (if available)
        if 'recipient_operator' in values and values['recipient_operator']:
            operator_code = values['recipient_operator']
            if not contract_clean.startswith(operator_code):
                raise ValueError(
                    f"Contract number must start with recipient operator code '{operator_code}'. "
                    f"Got: '{v}'"
                )
        
        return contract_clean
    
    # Add legal entity specific validation
    @validator('subscriber')
    def validate_legal_entity(cls, v): # pylint: disable=no-self-argument
        """Ensure the subscriber is actually a legal entity"""
        if v.subscriber_type != "company":
            raise ValueError('This endpoint is only for legal entities. Subscriber type must be "company"')
        
        # Define valid document types
        legal_document_types = ["CIF"]
        personal_document_types = ["NIE", "DNI", "PASSPORT"]
        
        current_doc_type = v.identification_document.document_type
        
        # Check if using personal document type for company
        if current_doc_type in personal_document_types:
            raise ValueError(
                f'Legal entities cannot use personal document type "{current_doc_type}". '
                f'Must use one of: {", ".join(legal_document_types)}'
            )
        
        # Check if using valid legal document type
        if current_doc_type not in legal_document_types:
            raise ValueError(
                f'Invalid document type "{current_doc_type}" for legal entity. '
                f'Must use one of: {", ".join(legal_document_types)}'
            )
        
        # Validate company name is provided
        if not v.personal_data.company_name:
            raise ValueError('Company name (razonSocial) is required for legal entities')
            
        return v
    
@router.post(
    '/port-in-legal', 
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_basic_auth)],
    response_model=PortInResponse,
    summary="Submit Port-In Request for Legal Entities",
    description="""
    Submit a new number portability request (Port-In) for companies and legal entities.

    This endpoint:
    - Accepts the request from BSS for legal entities (companies, organizations)
    - Saves it to the database immediately  
    - Submit request to NC with legal entity data structure
    - Returns NC response

    299 CUBEMOVIL FMVNO - 552000000 to  552009999 – NRN(906299) - recipinet operator
    798 CUBEMOVIL_FMVNO_DUMMY – 621800000 to 621899999 – NRN(704914) - donor operator
    
    **Workflow:**
    1. Request validation and immediate database storage
    2. Submit request to task queue for processing initiation
    3. Async processing with Central Node using legal entity template
    4. Status check task initiated from central schduler (pending_requests task)
    """,
    response_description="Request accepted and queued for processing",
    tags=["Spain: Portability Operations"]
)
async def portin_request_legal(alta_data: PortInRequestLegal):
    """
    Port-In Number Portability Request for Legal Entities
    
    Processes mobile number portability requests for companies and organizations with comprehensive validation.
    
    **Key Validations:**
    - MSISDN format (Spanish numbering plan)
    - ICCID length and format
    - Legal document type validation (CIF, etc.)
    - Company name validation
    - Date format and business logic
    
    **Background Processing:**
    - Central node communication using legal entity template
    - Operator coordination
    - Status tracking
    - Error handling and retries
    """
    start_time = time.time()
    try:
        logger.info("--- Processing port-in LEGAL request ---")
        logger.info("subscriber_type: %s", alta_data.subscriber.subscriber_type)
    
        # Validate that this is actually a legal entity request
        if alta_data.subscriber.subscriber_type != "company":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="This endpoint is only for legal entities (companies). Use /port-in for individual requests."
            )
        
        # Validate legal document types
        legal_document_types = ["CIF", "NIF", "VAT"]
        if alta_data.subscriber.identification_document.document_type not in legal_document_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid document type for legal entity. Must be one of: {', '.join(legal_document_types)}"
            )
        
        # Validate company name is provided
        if not alta_data.subscriber.personal_data.company_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Company name (razonSocial) is required for legal entities"
            )

        # Convert Pydantic model to dict for existing functions
        alta_data_dict = alta_data.dict()
        
        # Add legal entity flag to the data
        alta_data_dict['is_legal_entity'] = True
        
        # Conditional payload logging
        log_payload('BSS', 'PORT_IN_LEGAL', 'REQUEST', str(alta_data_dict))

        # 1. & 2. Create and save the DB record
        # new_request_id = save_portability_request_new(alta_data_dict, 'PORT_IN', 'ESP')
        new_request_id = save_portability_request_person_legal(alta_data_dict, 'PORT_IN', 'ESP')
        if not new_request_id:
           raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Failed to create portability request record"
            )
        logger.info("Port-in LEGAL request saved with ID: %s", new_request_id)
        
        # 3. Launch the background task, passing the ID of the new record
        # submit_to_central_node_legal.delay(new_request_id) # Asynchronous version for legal entities
        success, response_code, description, reference_code, porting_window_date = submit_to_central_node_online(new_request_id)  # Synchronous version for legal entities

        response_data = {
            "id": new_request_id,
            "success": success,
            "response_code": response_code,
            "description": description,
            "reference_code": reference_code,
            "porting_window_date": "" if not porting_window_date else porting_window_date
            # "entity_type": "LEGAL"
        }

        # Determine appropriate status code based on success and response_code
        if success:
            return response_data
        else:
            # Map different error types to appropriate HTTP status codes
            if response_code in ["NOT_FOUND", "VALIDATION_ERROR"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=response_data
                )
            elif response_code == "HTTP_ERROR":
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=response_data
                )
            elif response_code == "ACCS PERME":  # Outside business hours
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=response_data
                )
            elif re.match(r"^AREC", (response_code or "")):  # All AREC errors
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=response_data
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=response_data
                )
        
    except HTTPException:
        # Re-raise existing HTTP exceptions
        raise
        
    except ValueError as e:
        # Data validation errors
        logger.warning("Validation error in legal port-in: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request: {str(e)}"
        ) from e
        
    except Exception as e:
        # All other unexpected errors
        logger.error("Server error processing legal port-in: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error processing legal entity request"
        ) from e
    
    finally:
        processing_time = time.time() - start_time
        record_port_in_processing_time(processing_time)