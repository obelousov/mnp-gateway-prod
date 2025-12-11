from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Literal
from datetime import date
from services.logger_simple import logger
from datetime import datetime
from services.auth import verify_basic_auth
from services.italy.filename_generator import generate_mnp_filename, generate_daily_sequence_number, generate_recipient_request_code
import asyncio
import re
from services.italy.database_services_async import save_portin_request_minimal
from services.italy.soap_services import create_type_1_xml
# ============================================================================
# IMPORTS FOR ASYNC DATABASE
# ============================================================================
from sqlalchemy.ext.asyncio import AsyncSession
from services.italy.database_services_async import get_async_db

router = APIRouter(tags=["Italy: Portability Operations"])

# ============================================================================
# PYDANTIC MODELS (unchanged)
# ============================================================================

class BSSActivationRequest(BaseModel):
    """JSON request from BSS system for Italy MNP activation (Message Type 1)"""
    
    # Your existing model fields remain exactly the same
    message_type_code: Literal['1'] = Field('1', description="Message Type Code")
    sender_operator: str = Field(..., description="Sender Operator Code", example="LMIT")
    recipient_operator: str = Field(..., description="Recipient Operator Code", example="NOVA")
    
    donating_operator_code: str = Field(..., description="Donating Operator Code", example="NOVA")   
    recipient_operator_code: str = Field(..., description="Recipient Operator Code", example="LMIT")
    # donating_operator_code: str = Field(..., description="Donating Operator Code", example="NOVA")
    # recipient_request_code: str = Field(..., max_length=18, description="Unique request code", example="LYCA2510130228")
    msisdn: str = Field(..., pattern=r'^393\d{8,12}$', description="Principal MSISDN", example="393203004083")
    imsi: str = Field(..., max_length=15, description="Recipient's IMSI", example="222353002765232")
    credit_transfer_flag: Literal['Y', 'N'] = Field(..., description="Credit transfer request", example="Y")
    routing_number: str = Field(..., max_length=3, description="Routing number", example="382")
    pre_validation_flag: Literal['Y', 'N'] = Field(..., description="Pre-validation completed", example="Y")
    theft_flag: Literal['Y', 'N'] = Field(..., description="SIM reported stolen/lost", example="N")
    
    # Optional fields
    group_code: Optional[str] = Field(None, max_length=12, description="Group code", example="GRP123456")
    additional_msisdn_1: Optional[str] = Field(None, pattern=r'^393\d{8,12}$', description="Additional MSISDN 1", example="393203004084")
    additional_msisdn_2: Optional[str] = Field(None, pattern=r'^393\d{8,12}$', description="Additional MSISDN 2", example="393203004085")
    iccid: Optional[str] = Field(None, max_length=19, description="ICCID number", example="8939079000021073033")
    tax_code_vat: Optional[str] = Field(None, max_length=16, description="Tax code/VAT number", example="MRVLTZ55A41H717E")
    payment_type: Optional[Literal['PRP', 'POP']] = Field(None, description="Service type", example="PRP")
    analog_digital_code: Optional[Literal['D', 'A']] = Field(None, description="Service technology", example="D")
    cutover_date: Optional[date] = Field(None, description="Proposed cut-over date", example="2025-10-15")
    customer_first_name: Optional[str] = Field(None, max_length=30, description="Customer first name", example="xxxxxx")
    customer_last_name: Optional[str] = Field(None, max_length=50, description="Customer last name", example="yyyyy")
    company_name: Optional[str] = Field(None, max_length=80, description="Company name", example="Example Company SRL")
    document_type: Optional[Literal['CI', 'PA', 'PS']] = Field(None, description="Document type", example="CI")
    document_number: Optional[str] = Field(None, max_length=30, description="Document number", example="AB1234567")
    virtual_recipient_operator: Optional[str] = Field(None, pattern=r'^[BCDFHILMNOPQSTVWZ]\d{3}$', description="Virtual Recipient Operator Code", example="B123")
    virtual_donating_operator: Optional[str] = Field(None, pattern=r'^[BCDFHILMNOPQSTVWZ]\d{3}$', description="Virtual Donating Operator Code", example="C456")
    
    # Validators (unchanged)
    @field_validator('recipient_operator_code', 'donating_operator_code')
    @classmethod
    def validate_operator_code(cls, v: str) -> str:
        valid_operators = {
            'BLUI', 'BTIT', 'COOP', 'DMOB', 'FAST', 'H3GI', 'ILIT', 
            'IPSE', 'LMIT', 'MUND', 'NOVA', 'NPTS', 'OPIV', 'PLTN', 
            'PMOB', 'SPIT', 'TIMG', 'TIMT', 'WIND', 'WIN3', 'WLIM'
        }
        if v not in valid_operators:
            raise ValueError(f'Invalid operator code. Must be one of: {", ".join(sorted(valid_operators))}')
        return v
    
    # @field_validator('recipient_request_code')
    # @classmethod
    # def validate_request_code(cls, v: str) -> str:
    #     if any(c.islower() for c in v):
    #         raise ValueError('Request code cannot contain lowercase letters')
    #     return v
    
    @field_validator('routing_number')
    @classmethod
    def validate_routing_number(cls, v: str) -> str:
        if not v.isdigit() or len(v) != 3:
            raise ValueError('Routing number must be 3 digits')
        return v

# ============================================================================
# RESPONSE MODELS
# ============================================================================

class BSSActivationResponse(BaseModel):
    """Response to BSS after validation including generated FILENAME fields"""
    status: Literal['true', 'false']
    message: str
    # recipient_request_code: Optional[str] = None
    db_record_id: Optional[int] = None  # ✅ Added: Database record ID
    validation_errors: List[str] = []
    
    # Generated FILENAME fields
    recipient_request_code: Optional[str] = None
    # sender_operator: Optional[str] = None
    # recipient_operator: Optional[str] = None
    file_date: Optional[date] = None
    file_time: Optional[str] = None
    file_id: Optional[str] = None
    
    # Generated filenames
    filename: Optional[str] = None
    
    # Other metadata
    # timestamp: datetime
    
    class Config:
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
            datetime: lambda v: v.isoformat() if v else None,
        }

# ============================================================================
# ASYNC ENDPOINT WITH PROPER DATABASE HANDLING (UPDATED)
# ============================================================================

@router.post("/1-activation", response_model=BSSActivationResponse)
async def submit_activation_request(
    request: BSSActivationRequest,
    auth: str = Depends(verify_basic_auth),
    db: AsyncSession = Depends(get_async_db)
) -> BSSActivationResponse:
    """Process Italy MNP activation - Pydantic handles validation."""
    
    # 1. Generate XML with metadata
    generated = create_type_1_xml(request.model_dump())
    
    # 2. Save to database (minimal structure)
    record = await save_portin_request_minimal({
        'recipient_request_code': generated.metadata['recipient_request_code'],
        'msisdn': request.msisdn,
        'message_type_code': '1',
        'process_status': 'RECEIVED',
        'cut_over_date': request.cutover_date,
        'xml': generated.xml,
    }, db)
    
    if not record:
        raise HTTPException(status_code=500, detail="Database save failed")
    
    # 3. Return success response
    return BSSActivationResponse(
        status='true',
        message='Activation request accepted',
        recipient_request_code=generated.metadata['recipient_request_code'],
        db_record_id=record.id,
        validation_errors=[],
        file_date=generated.metadata['file_date'],
        file_time=generated.metadata['file_time'],
        file_id=generated.metadata['file_id'],
        filename=generated.metadata['mnp_filename'],
    )