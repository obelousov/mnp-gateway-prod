# api/v1/italy/1_activation.py
from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel, Field, validator, field_validator
from typing import Optional, List, Literal
from datetime import date
from services.logger_simple import logger
from datetime import datetime, timedelta
from services.auth import verify_basic_auth

router = APIRouter(tags=["Italy: Portability Operations"])

# ============================================================================
# PYDANTIC MODELS FOR BSS REQUEST
# ============================================================================

class BSSActivationRequest(BaseModel):
    """JSON request from BSS system for Italy MNP activation"""
    
    # MANDATORY FIELDS
    recipient_operator: str = Field(
        ..., 
        min_length=4, 
        max_length=4,
        description="Recipient Operator Code (4 characters)"
    )
    
    donating_operator: str = Field(
        ..., 
        min_length=4, 
        max_length=4,
        description="Donating Operator Code (4 characters)"
    )
    
    request_code: str = Field(
        ..., 
        min_length=1, 
        max_length=18,
        description="Unique request code from recipient operator (max 18 chars, no lowercase)",
        example="LYCA2510130228"
    )
    
    msisdn: str = Field(
        ..., 
        min_length=15, 
        max_length=15,
        pattern=r'^39\d{13}$',
        description="Principal MSISDN (15 digits starting with 39)"
    )
    
    imsi: str = Field(
        ..., 
        min_length=15, 
        max_length=15,
        description="Recipient's IMSI (15 digits)",
        example="222010123456789"  # Added example
    )
    
    credit_transfer_flag: Literal['Y', 'N'] = Field(
        ..., 
        description="Credit transfer request (Y=Yes, N=No)"
    )
    
    routing_number: str = Field(
        ..., 
        min_length=3, 
        max_length=3,
        description="Routing number of hosting recipient (3 digits)",
        example="382"
    )
    
    pre_validation_flag: Literal['Y', 'N'] = Field(
        ..., 
        description="Pre-validation completed (Y=Yes, N=No)"
    )
    
    theft_flag: Literal['Y', 'N'] = Field(
        ..., 
        description="SIM reported stolen/lost (Y=Yes, N=No)"
    )
    
    # OPTIONAL FIELDS
    group_code: Optional[str] = Field(
        None,
        max_length=12,
        description="Group code for batch processing (max 12 chars)"
    )
    
    additional1: Optional[str] = Field(
        None,
        min_length=15,
        max_length=15,
        pattern=r'^39\d{13}$',
        description="Additional MSISDN 1 (optional)"
    )
    
    additional2: Optional[str] = Field(
        None,
        min_length=15,
        max_length=15,
        pattern=r'^39\d{13}$',
        description="Additional MSISDN 2 (optional)"
    )
    
    iccid: Optional[str] = Field(
        None,
        max_length=19,
        description="ICCID/Serial number (max 19 chars)"
    )
    
    tax_code: Optional[str] = Field(
        None,
        max_length=16,
        description="Tax code/VAT number (max 16 chars)"
    )
    
    prepost_paid: Optional[Literal['PRP', 'POP']] = Field(
        None,
        description="Service type: PRP=Prepaid, POP=Postpaid"
    )
    
    analog_digital: Optional[Literal['D', 'A']] = Field(
        None,
        description="Service technology: D=Digital, A=Analog"
    )
    
    cutover_date: Optional[date] = Field(
        None,
        description="Proposed cut-over date (YYYY-MM-DD)"
    )
    
    customer_name: Optional[str] = Field(
        None,
        max_length=30,
        description="Customer first name (max 30 chars)"
    )
    
    customer_surname: Optional[str] = Field(
        None,
        max_length=50,
        description="Customer last name (max 50 chars)"
    )
    
    company_name: Optional[str] = Field(
        None,
        max_length=80,
        description="Company name (max 80 chars)"
    )
    
    document_type: Optional[Literal['CI', 'PA', 'PS']] = Field(
        None,
        description="Document type: CI=ID Card, PA=Driver's License, PS=Passport"
    )
    
    document_number: Optional[str] = Field(
        None,
        max_length=30,
        description="Document number (max 30 chars)"
    )
    
    virtual_recipient: Optional[str] = Field(
        None,
        min_length=4,
        max_length=4,
        description="Virtual Recipient Operator Code (4 chars)"
    )
    
    virtual_donating: Optional[str] = Field(
        None,
        min_length=4,
        max_length=4,
        description="Virtual Donating Operator Code (4 chars)"
    )
    
    # Validators
    @field_validator('request_code')
    @classmethod
    def validate_request_code(cls, v: str) -> str:
        """Validate request code doesn't contain lowercase letters"""
        if any(c.islower() for c in v):
            raise ValueError('Request code cannot contain lowercase letters')
        return v
    
    @field_validator('routing_number')
    @classmethod
    def validate_routing_number(cls, v: str) -> str:
        """Validate routing number is numeric"""
        if not v.isdigit():
            raise ValueError('Routing number must be numeric')
        return v
    
    @field_validator('imsi')
    @classmethod
    def validate_imsi(cls, v: str) -> str:
        """Validate IMSI is numeric"""
        if not v.isdigit():
            raise ValueError('IMSI must be numeric')
        if len(v) != 15:
            raise ValueError('IMSI must be 15 digits')
        return v
    
    @field_validator('additional1', 'additional2')
    @classmethod
    def validate_additional_msisdn(cls, v: Optional[str]) -> Optional[str]:
        """Validate additional MSISDNs"""
        if v is None:
            return v
        if not v.startswith('39'):
            raise ValueError('Additional MSISDN must start with 39')
        if len(v) != 15:
            raise ValueError('Additional MSISDN must be 15 digits')
        return v

# ============================================================================
# RESPONSE MODELS
# ============================================================================

class BSSActivationResponse(BaseModel):
    """Response to BSS after validation"""
    status: Literal['accepted', 'rejected']
    message: str
    request_id: Optional[str] = None
    validation_errors: List[str] = []
    timestamp: datetime

# ============================================================================
# ENDPOINTS
# ============================================================================

@router.post(
    "/1-activation",
    response_model=BSSActivationResponse,
    status_code=status.HTTP_200_OK,
    summary="Submit Italy MNP Activation Request",
    description="""Accepts Italy MNP activation request from BSS in JSON format.
    
    **Mandatory Fields:**
    - recipient_operator (4 chars)
    - donating_operator (4 chars) 
    - request_code (max 18 chars, no lowercase)
    - msisdn (15 digits starting with 39)
    - imsi (15 digits)
    - credit_transfer_flag (Y/N)
    - routing_number (3 digits)
    - pre_validation_flag (Y/N)
    - theft_flag (Y/N)
    
    **Returns:** Validation result with acceptance or rejection.
    """
)
async def submit_activation_request(
    request: BSSActivationRequest,
    auth: str = Depends(verify_basic_auth)  # BSS authentication
) -> BSSActivationResponse:
    """
    Process Italy MNP activation request from BSS.
    
    This endpoint:
    1. Validates the request against Italian MNP specifications
    2. Performs business logic validation
    3. Returns 200 OK with validation result
    4. Request is queued for further processing
    
    Note: This is phase 1 - validation only.
    Subsequent phases will handle DB storage and XML generation.
    """
    try:
        logger.info(f"Received Italy activation request from BSS: {request.request_code}")
        
        # Additional business logic validation
        validation_errors = perform_business_validation(request)
        
        if validation_errors:
            logger.warning(f"Request {request.request_code} rejected: {validation_errors}")
            return BSSActivationResponse(
                status='rejected',
                message='Request validation failed',
                validation_errors=validation_errors,
                timestamp=datetime.utcnow()
            )
        
        # Generate unique request ID for tracking
        request_id = generate_request_id(request)
        
        logger.info(f"Request {request.request_code} accepted, assigned ID: {request_id}")
        
        # TODO: In next phase, store to DB and queue for XML generation
        # For now, just return acceptance
        
        return BSSActivationResponse(
            status='accepted',
            message='Activation request accepted for processing',
            request_id=request_id,
            validation_errors=[],
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
    """Perform additional business logic validation"""
    errors = []
    
    # 1. Check if MSISDN is already being processed
    if is_msisdn_in_progress(request.msisdn):
        errors.append(f"MSISDN {request.msisdn} is already being processed")
    
    # 2. Validate request code uniqueness
    if is_request_code_used(request.request_code):
        errors.append(f"Request code {request.request_code} already exists")
    
    # 3. Validate cutover date (if provided)
    if request.cutover_date:
        if request.cutover_date < date.today():
            errors.append(f"Cutover date {request.cutover_date} is in the past")
        
        # Check if cutover is at least 2 days in the future
        min_cutover = date.today() + timedelta(days=2)
        if request.cutover_date < min_cutover:
            errors.append(f"Cutover date must be at least 2 days in the future")
    
    # 4. Validate credit transfer for prepaid
    if request.prepost_paid == 'PRP' and request.credit_transfer_flag == 'N':
        # Warning but not error - prepaid without credit transfer is allowed
        logger.info(f"Prepaid service without credit transfer: {request.msisdn}")
    
    # 5. Check theft flag logic
    if request.theft_flag == 'Y' and request.pre_validation_flag != 'Y':
        errors.append("Pre-validation must be Y when theft flag is Y")
    
    return errors

def generate_request_id(request: BSSActivationRequest) -> str:
    """Generate unique request ID for tracking"""
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    return f"IT_{request.recipient_operator}_{timestamp}_{request.msisdn[-6:]}"

def is_msisdn_in_progress(msisdn: str) -> bool:
    """Check if MSISDN is already being processed"""
    # TODO: Implement database check
    # For now, return False
    return False

def is_request_code_used(request_code: str) -> bool:
    """Check if request code is already used"""
    # TODO: Implement database check
    # For now, return False
    return False