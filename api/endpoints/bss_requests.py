from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from config import settings
import time
from services.database_service import save_portin_request_db, save_cancel_request_db, save_portability_request_new, check_if_cancel_request_id_in_db, check_if_cancel_request_id_in_db_online, save_cancel_request_db_online
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

        validation_patterns = {
            "NIE": (r'^[XYZ]{1}[0-9]{7}[A-Z]{1}$', "NIE must be format: [X/Y/Z] + 7 digits + 1 letter (e.g., X1234567A)"),
            "NIF": (r'^[0-9]{8}[A-Z]{1}$', "NIF must be 8 digits + 1 letter (e.g., 12345678Z)"),
            # Add more patterns as needed
        }

        if doc_type in validation_patterns:
            pattern, error_msg = validation_patterns[doc_type]
            if not re.match(pattern, v_clean):
                raise ValueError(f"{error_msg}. Got: {v}")
            
        return v_clean
# class IdentificationDocument(BaseModel):
#     """ Identification document data | WSDL: `<por:documentoIdentificacion>`"""
#     document_type: str = Field(
#         ...,
#         description="Document type | WSDL: `<por:tipoDocumento>`",
#         examples=["NIE", "DNI", "PASSPORT"]
#     )
#     document_number: str = Field(
#         ...,
#         description="Document number | WSDL: `<por:numeroDocumento>`",
#         examples=["Y30307876", "12345678X"]
#     )

    # @field_validator('document_type')
    # def validate_document_type(cls, v): # pylint: disable=no-self-argument
    #     """Validate that document_type is one of the allowed types"""
    #     allowed_document_types = ["NIF", "CIF", "NIE", "PAS"]
    #     if v not in allowed_document_types:
    #         raise ValueError(f'document_type must be one of: {", ".join(allowed_document_types)}')
    #     return v
    
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

# class Subscriber(BaseModel):
#     """ Subscriber data | WSDL: `<por:abonado>`"""
#     identification_document: IdentificationDocument = Field(
#         ...,
#         description="Identification document data"
#     )
#     personal_data: PersonalData = Field(
#         ...,
#         description="Personal data | WSDL: `<por:datosPersonales>`"
#     )

class SubscriberType(str, Enum):
    PERSON = "person"
    COMPANY = "company"

class CompanyData(BaseModel):
    razon_social: str = Field(
        ...,
        description="Company legal name (razón social)",
        examples=["Empresa Ejemplo S.L."]
    )

# class Subscriber(BaseModel):
#     subscriber_type: SubscriberType = Field(
#         ...,
#         description="Type of subscriber: 'person' for individuals, 'company' for legal entities"
#     )
#     identification_document: IdentificationDocument
#     personal_data: Optional[PersonalData] = None
#     company_data: Optional[CompanyData] = None  # Add this for companies

#     @validator('personal_data')
#     def validate_personal_data(cls, v, values):
#         if values.get('subscriber_type') == SubscriberType.PERSON and not v:
#             raise ValueError("personal_data is required for person subscribers")
#         return v

#     # @validator('company_data')
#     # def validate_company_data(cls, v, values):
#     #     if values.get('subscriber_type') == SubscriberType.COMPANY and not v:
#     #         raise ValueError("company_data is required for company subscribers")
#     #     return v
#     # @validator('company_data')
#     # def validate_company_data(cls, v, values):
#     #     if values.get('subscriber_type') == SubscriberType.COMPANY and not v:
#     #         raise ValueError("company_data is required for company subscribers")
#     #     return v

#     @validator('company_data')
#     def validate_company_data(cls, v, values):
#         print("Validating company_data with values:", values)
#         if 'subscriber_type' not in values:
#             return v  # Let other validators handle the missing subscriber_type
        
#         subscriber_type = values['subscriber_type']
#         if subscriber_type == SubscriberType.COMPANY and not v:
#             raise ValueError("company_data is required for company subscribers")
#         return v

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
    # @validator('company_type')
    # def validate_company_type(cls, v):  # pylint: disable=no-self-argument
    #     """Validate company_type
    #     - Must be person or company
    #     """
    #     allowed_company_types = ["person", "company"]
  
    #     # Validate the country_code value (v is the actual value, not a dict)
    #     if v not in allowed_company_types:
    #         raise ValueError(f'company_type must be one of: {", ".join(allowed_company_types)}')
        
    #     return v

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
# class CampoErroneo(BaseModel):
#     """Erroneous field details | WSDL: `co-v1-10:CampoErroneo`"""
#     nombre: str = Field(
#         ...,
#         max_length=32,
#         description="Nombre del campo erróneo | WSDL: `co-v1-10:nombre`",
#         examples=["codigoOperadorDonante", "documentNumber", "iccid"]
#     )
#     descripcion: str = Field(
#         ...,
#         max_length=512,
#         description="Descripción del error | WSDL: `co-v1-10:descripcion`",
#         examples=[
#             "Campo con restricción de longitud fija de 3 caracteres, se recibieron 10 caracteres",
#             "Formato de fecha inválido, debe ser DD/MM/YYYY HH:MM:SS"
#         ]
#     )
    
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
            alta_data['cancellation_reason'] = alta_data['cancellation_reason'].value

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