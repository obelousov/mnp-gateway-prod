from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel, Field, validator, field_validator
from typing import Optional, List, Literal
from datetime import date
from services.logger_simple import logger
from datetime import datetime, timedelta
from services.auth import verify_basic_auth
from services.italy.filename_generator import generate_mnp_filename, generate_xml_filename, generate_daily_sequence_number  # ← UPDATED IMPORT
import re
import os
import json

router = APIRouter(tags=["Italy: Portability Operations"])

# ============================================================================
# PYDANTIC MODELS FOR BSS REQUEST - ALIGNED WITH XSD ATTIVAZIONE_REC TYPE
# ============================================================================

class BSSActivationRequest(BaseModel):
    """JSON request from BSS system for Italy MNP activation (Message Type 1)
    
    Fully aligned with XSD ATTIVAZIONE_REC complexType definition.
    FILENAME fields (file_date, file_time, file_id) will be generated automatically.
    """
    
    # ATTIVAZIONE_REC FIELDS FROM XSD (exact mapping)
    
    # Fixed field from XSD: fixed="1"
    message_type_code: Literal['1'] = Field(
        '1',
        description="Message Type Code (TIPO_MESSAGGIO) - fixed to '1' per XSD"
    )
    
    # Required fields from XSD (no minOccurs="0")
    recipient_operator_code: str = Field(
        ..., 
        description="Recipient Operator Code (CODICE_OPERATORE_RECIPIENT)",
        example="LMIT"
    )
    
    donating_operator_code: str = Field(
        ..., 
        description="Donating Operator Code (CODICE_OPERATORE_DONATING)",
        example="NOVA"
    )
    
    recipient_request_code: str = Field(
        ..., 
        max_length=18,
        description="Unique request code from recipient operator (CODICE_RICHIESTA_RECIPIENT)",
        example="LYCA2510130228"
    )
    
    msisdn: str = Field(
        ..., 
        pattern=r'^393\d{8,12}$',
        description="Principal MSISDN (PHONE_TYPE) - 393 + 8-12 digits",
        example="393203004083"
    )
    
    # Required from XSD: no minOccurs="0"
    imsi: str = Field(
        ..., 
        max_length=15,
        description="Recipient's IMSI (IMSI_TYPE) - REQUIRED per XSD",
        example="222353002765232"
    )
    
    # Required from XSD: no minOccurs="0"
    credit_transfer_flag: Literal['Y', 'N'] = Field(
        ..., 
        description="Credit transfer request (FLAG_TRASFERIMENTO_CREDITO) - REQUIRED per XSD",
        example="Y"
    )
    
    # Required from XSD: no minOccurs="0"
    routing_number: str = Field(
        ..., 
        max_length=3,
        description="Routing number (ROUTING_NUMBER_TYPE) - REQUIRED per XSD",
        example="382"
    )
    
    # Required from XSD: no minOccurs="0"
    pre_validation_flag: Literal['Y', 'N'] = Field(
        ..., 
        description="Pre-validation completed (PREVALIDAZIONE) - REQUIRED per XSD",
        example="Y"
    )
    
    # Required from XSD: no minOccurs="0"
    theft_flag: Literal['Y', 'N'] = Field(
        ..., 
        description="SIM reported stolen/lost (FURTO) - REQUIRED per XSD",
        example="N"
    )
    
    # OPTIONAL FIELDS from XSD (minOccurs="0")
    group_code: Optional[str] = Field(
        None,
        max_length=12,
        description="Group code for batch processing (CODICE_GRUPPO_TYPE) - optional per XSD",
        example="GRP123456"
    )
    
    additional_msisdn_1: Optional[str] = Field(
        None,
        pattern=r'^393\d{8,12}$',
        description="Additional MSISDN 1 (ADDIZIONALE_1) - optional per XSD",
        example="393203004084"
    )
    
    additional_msisdn_2: Optional[str] = Field(
        None,
        pattern=r'^393\d{8,12}$',
        description="Additional MSISDN 2 (ADDIZIONALE_2) - optional per XSD",
        example="393203004085"
    )
    
    iccid_serial_number: Optional[str] = Field(
        None,
        max_length=19,
        description="ICCID/Serial number (ICCID_SERIAL_NUMBER_TYPE) - optional per XSD",
        example="8939079000021073033"
    )
    
    tax_code_vat: Optional[str] = Field(
        None,
        max_length=16,
        description="Tax code/VAT number (CODICE_FISCALE_PARTITA_IVA_TYPE) - optional per XSD",
        example="MRVLTZ55A41H717E"
    )
    
    payment_type: Optional[Literal['PRP', 'POP']] = Field(
        None,
        description="Service type (CODICE_PRE_POST_PAGATO_TYPE): PRP=Prepaid, POP=Postpaid - optional per XSD",
        example="PRP"
    )
    
    analog_digital_code: Optional[Literal['D', 'A']] = Field(
        None,
        description="Service technology (CODICE_ANALOGICO_DIGITALE_TYPE): D=Digital, A=Analog - optional per XSD",
        example="D"
    )
    
    cutover_date: Optional[date] = Field(
        None,
        description="Proposed cut-over date (DATA_CUT_OVER) - optional per XSD",
        example="2025-10-15"
    )
    
    customer_first_name: Optional[str] = Field(
        None,
        max_length=30,
        description="Customer first name (NOME_CLIENTE_TYPE) - optional per XSD",
        example="xxxxxx"
    )
    
    customer_last_name: Optional[str] = Field(
        None,
        max_length=50,
        description="Customer last name (COGNOME_CLIENTE_TYPE) - optional per XSD",
        example="yyyyy"
    )
    
    company_name: Optional[str] = Field(
        None,
        max_length=80,
        description="Company name (RAGIONE_SOCIALE_TYPE) - optional per XSD",
        example="Example Company SRL"
    )
    
    document_type: Optional[Literal['CI', 'PA', 'PS']] = Field(
        None,
        description="Document type (TIPO_DOCUMENTO_TYPE): CI=ID Card, PA=Driver's License, PS=Passport - optional per XSD",
        example="CI"
    )
    
    document_number: Optional[str] = Field(
        None,
        max_length=30,
        description="Document number (NUMERO_DOCUMENTO_TYPE) - optional per XSD",
        example="AB1234567"
    )
    
    virtual_recipient_operator: Optional[str] = Field(
        None,
        pattern=r'^[BCDFHILMNOPQSTVWZ]\d{3}$',
        description="Virtual Recipient Operator Code (CODICE_OPERATORE_VIRTUALE_RECIPIENT) - optional per XSD",
        example="B123"
    )
    
    virtual_donating_operator: Optional[str] = Field(
        None,
        pattern=r'^[BCDFHILMNOPQSTVWZ]\d{3}$',
        description="Virtual Donating Operator Code (CODICE_OPERATORE_VIRTUALE_DONATING) - optional per XSD",
        example="C456"
    )
    
    # Validators
    @field_validator('recipient_operator_code', 'donating_operator_code')
    @classmethod
    def validate_operator_code(cls, v: str) -> str:
        """Validate operator codes are valid OLO_TYPE values from XSD"""
        valid_operators = {
            'BLUI', 'BTIT', 'COOP', 'DMOB', 'FAST', 'H3GI', 'ILIT', 
            'IPSE', 'LMIT', 'MUND', 'NOVA', 'NPTS', 'OPIV', 'PLTN', 
            'PMOB', 'SPIT', 'TIMG', 'TIMT', 'WIND', 'WIN3', 'WLIM'
        }
        if v not in valid_operators:
            raise ValueError(f'Invalid operator code. Must be one of: {", ".join(sorted(valid_operators))}')
        return v
    
    @field_validator('recipient_request_code')
    @classmethod
    def validate_request_code(cls, v: str) -> str:
        """Validate request code doesn't contain lowercase letters"""
        if any(c.islower() for c in v):
            raise ValueError('Request code cannot contain lowercase letters')
        return v
    
    @field_validator('routing_number')
    @classmethod
    def validate_routing_number(cls, v: str) -> str:
        """Validate routing number format"""
        if not v.isdigit() or len(v) != 3:
            raise ValueError('Routing number must be 3 digits')
        return v
    
    @field_validator('additional_msisdn_1', 'additional_msisdn_2')
    @classmethod
    def validate_additional_msisdn(cls, v: Optional[str]) -> Optional[str]:
        """Validate additional MSISDNs if provided"""
        if v is None:
            return v
        if not v.startswith('393'):
            raise ValueError('Additional MSISDN must start with 393')
        if len(v) < 11 or len(v) > 15:
            raise ValueError('Additional MSISDN must be 11-15 digits (393 + 8-12 digits)')
        return v
    
    @field_validator('virtual_recipient_operator', 'virtual_donating_operator')
    @classmethod
    def validate_virtual_operator(cls, v: Optional[str]) -> Optional[str]:
        """Validate virtual operator format from XSD pattern: [BCDFHILMNOPQSTVWZ]\d{3}"""
        if v is None:
            return v
        pattern = r'^[BCDFHILMNOPQSTVWZ]\d{3}$'
        if not re.match(pattern, v):
            raise ValueError('Virtual operator must match pattern: [BCDFHILMNOPQSTVWZ] followed by 3 digits')
        return v

# ============================================================================
# RESPONSE MODELS - UPDATED TO INCLUDE GENERATED FIELDS
# ============================================================================

class BSSActivationResponse(BaseModel):
    """Response to BSS after validation including generated FILENAME fields"""
    status: Literal['accepted', 'rejected']
    message: str
    request_id: Optional[str] = None
    validation_errors: List[str] = []
    
    # Generated FILENAME fields
    sender_operator: Optional[str] = None
    recipient_operator: Optional[str] = None
    file_date: Optional[date] = None
    file_time: Optional[str] = None
    file_id: Optional[str] = None
    
    # Generated filenames
    mnp_filename: Optional[str] = None
    # xml_filename: Optional[str] = None
    
    # Other metadata
    timestamp: datetime
    
    class Config:
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
            datetime: lambda v: v.isoformat() if v else None,
        }

# ============================================================================
# ENDPOINTS
# ============================================================================

@router.post(
    "/1-activation",
    response_model=BSSActivationResponse,
    status_code=status.HTTP_200_OK,
    summary="Submit Italy MNP Activation Request (Message Type 1)",
    description="""Accepts Italy MNP activation request from BSS in JSON format for Message Type 1 (ATTIVAZIONE).
    
    **Mandatory ATTIVAZIONE fields (no minOccurs="0" in XSD):**
    - recipient_operator_code (CODICE_OPERATORE_RECIPIENT)
    - donating_operator_code (CODICE_OPERATORE_DONATING)
    - recipient_request_code (CODICE_RICHIESTA_RECIPIENT) - max 18 chars
    - msisdn (MSISDN) - 393 + 8-12 digits
    - imsi (IMSI) - max 15 chars
    - credit_transfer_flag (FLAG_TRASFERIMENTO_CREDITO) - Y/N
    - routing_number (ROUTING_NUMBER) - max 3 chars
    - pre_validation_flag (PREVALIDAZIONE) - Y/N
    - theft_flag (FURTO) - Y/N
    
    **Optional ATTIVAZIONE fields (minOccurs="0" in XSD):**
    - group_code (max 12 chars)
    - additional_msisdn_1, additional_msisdn_2 (393 + 8-12 digits)
    - iccid_serial_number (max 19 chars)
    - tax_code_vat (max 16 chars)
    - payment_type (PRP/POP)
    - analog_digital_code (D/A)
    - cutover_date (YYYY-MM-DD)
    - customer_first_name (max 30 chars)
    - customer_last_name (max 50 chars)
    - company_name (max 80 chars)
    - document_type (CI/PA/PS)
    - document_number (max 30 chars)
    - virtual_recipient_operator, virtual_donating_operator (pattern: [BCDFHILMNOPQSTVWZ] + 3 digits)
    
    **Response includes generated FILENAME fields:**
    - sender_operator (same as recipient_operator_code)
    - recipient_operator (same as donating_operator_code)
    - file_date (current date)
    - file_time (current time)
    - file_id (auto-generated daily sequence number)
    - mnp_filename (official Italy MNP format: SENDERYYYYMMDDHHMMSSRECIPIENTFILEID)
    - xml_filename (descriptive format: SENDER_RECIPIENT_TIMESTAMP_FILEID.xml)
    
    **Valid operator codes from XSD:** BLUI, BTIT, COOP, DMOB, FAST, H3GI, ILIT, 
    IPSE, LMIT, MUND, NOVA, NPTS, OPIV, PLTN, PMOB, SPIT, TIMG, TIMT, WIND, WIN3, WLIM
    
    **Returns:** Validation result with acceptance or rejection including generated FILENAME metadata.
    """
)
async def submit_activation_request(
    request: BSSActivationRequest,
    auth: str = Depends(verify_basic_auth)  # BSS authentication
) -> BSSActivationResponse:
    """
    Process Italy MNP activation request from BSS (Message Type 1).
    
    Returns generated FILENAME fields and official MNP filename in the response.
    """
    try:
        logger.info(f"Received Italy activation request (Type 1): {request.recipient_request_code}")
        
        # Generate FILENAME fields automatically
        # In your sample XML: sender = recipient_operator_code, recipient = donating_operator_code
        sender_operator = request.recipient_operator_code
        recipient_operator = request.donating_operator_code
        file_date = date.today()
        file_time = datetime.now().strftime("%H:%M:%S")
        
        # Generate file_id - use daily sequence number
        file_id = generate_daily_sequence_number(sender_operator, recipient_operator)
        
        # Generate official MNP filename
        mnp_filename = generate_mnp_filename(
            sender_operator_code=sender_operator,
            recipient_operator_code=recipient_operator,
            file_id=file_id
        )
        
        # Generate descriptive XML filename
        # xml_filename = generate_xml_filename(
        #     sender_operator=sender_operator,
        #     recipient_operator=recipient_operator,
        #     file_id=file_id
        # )
        
        # logger.info(f"Generated filenames - MNP: {mnp_filename}, XML: {xml_filename}")
        logger.info(f"Generated filenames - MNP: {mnp_filename}")
        
        # Additional business logic validation
        validation_errors = perform_business_validation(request)
        
        if validation_errors:
            logger.warning(f"Request {request.recipient_request_code} rejected: {validation_errors}")
            return BSSActivationResponse(
                status='rejected',
                message='Request validation failed',
                validation_errors=validation_errors,
                sender_operator=sender_operator,
                recipient_operator=recipient_operator,
                file_date=file_date,
                file_time=file_time,
                file_id=file_id,
                mnp_filename=mnp_filename,
                # xml_filename=xml_filename,
                timestamp=datetime.utcnow()
            )
        
        # Generate unique request ID for tracking
        request_id = generate_request_id(request)
        
        logger.info(f"Request {request.recipient_request_code} accepted, assigned ID: {request_id}")
        
        # TODO: Store to ItalyPortInRequest table and queue for XML generation
        # Include the generated FILENAME fields in the database record
        
        return BSSActivationResponse(
            status='accepted',
            message='Activation request accepted for processing',
            request_id=request_id,
            validation_errors=[],
            sender_operator=sender_operator,
            recipient_operator=recipient_operator,
            file_date=file_date,
            file_time=file_time,
            file_id=file_id,
            mnp_filename=mnp_filename,
            # xml_filename=xml_filename,
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Error processing activation request: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def perform_business_validation(request: BSSActivationRequest) -> List[str]:
    """Perform additional business logic validation specific to Message Type 1"""
    errors = []
    
    # 1. Check if MSISDN is already being processed
    if is_msisdn_in_progress(request.msisdn):
        errors.append(f"MSISDN {request.msisdn} is already being processed")
    
    # 2. Validate request code uniqueness
    if is_request_code_used(request.recipient_request_code):
        errors.append(f"Request code {request.recipient_request_code} already exists")
    
    # 3. Validate cutover date if provided (must be in future)
    if request.cutover_date and request.cutover_date <= date.today():
        errors.append(f"Cutover date {request.cutover_date} must be in the future")
    
    # 4. Check theft flag logic (from XSD/business rules)
    if request.theft_flag == 'Y' and request.pre_validation_flag != 'Y':
        errors.append("Pre-validation must be Y when theft flag is Y (XSD business rule)")
    
    # 5. Validate additional business rules
    if request.credit_transfer_flag == 'Y' and request.payment_type == 'POP':
        errors.append("Credit transfer not allowed for postpaid (POP) services")
    
    # 6. Check virtual operators consistency
    if (request.virtual_recipient_operator and not request.virtual_donating_operator) or \
       (not request.virtual_recipient_operator and request.virtual_donating_operator):
        errors.append("Both virtual recipient and donating operators must be provided or both omitted")
    
    # 7. Check document consistency
    if request.document_type and not request.document_number:
        errors.append("Document number required when document type is provided")
    if request.document_number and not request.document_type:
        errors.append("Document type required when document number is provided")
    
    return errors

def generate_request_id(request: BSSActivationRequest) -> str:
    """Generate unique request ID for tracking"""
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    return f"IT1_{request.recipient_operator_code}_{timestamp}_{request.msisdn[-6:]}"

def is_msisdn_in_progress(msisdn: str) -> bool:
    """Check if MSISDN is already being processed"""
    # TODO: Implement database check against ItalyPortInRequest table
    # For now, return False
    return False

def is_request_code_used(request_code: str) -> bool:
    """Check if request code is already used"""
    # TODO: Implement database check against ItalyPortInRequest table
    # For now, return False
    return False