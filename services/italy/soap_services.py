from datetime import datetime, date
from typing import Dict, Any

from templates.italy.soap_templates import TYPE_1_ACTIVATION
from services.logger import logger
from services.italy.filename_generator import generate_mnp_filename, generate_daily_sequence_number, generate_recipient_request_code

from datetime import datetime, date
from typing import Dict, Any, TypedDict, Optional
from dataclasses import dataclass

from templates.italy.soap_templates import TYPE_1_ACTIVATION
from services.logger import logger
from services.italy.filename_generator import (
    generate_mnp_filename, 
    generate_daily_sequence_number, 
    generate_recipient_request_code
)

# Optional: Define return type for clarity
@dataclass
class GeneratedXMLResult:
    xml: str
    metadata: Dict[str, Any]  # Generated fields for response

def create_type_1_xml(request_data: Dict[str, Any]) -> GeneratedXMLResult:
    """
    Create Type 1 (ATTIVAZIONE) XML and return with metadata.
    
    Args:
        request_data: Dictionary containing request fields
        
    Returns:
        GeneratedXMLResult with XML and metadata (file_date, file_id, etc.)
    """
    # ============ ISSUE 1: Extract sender/recipient once ============
    sender = request_data.get('sender_operator', '')
    recipient = request_data.get('recipient_operator', '')
    
    # ============ ISSUE 2: Generate all dynamic fields once ============
    file_date_obj = date.today()
    file_time_str = datetime.now().strftime("%H:%M:%S")
    file_id = generate_daily_sequence_number(sender, recipient)
    recipient_request_code = generate_recipient_request_code(sender)
    
    # ============ ISSUE 3: Handle cutover_date properly ============
    cutover_date = request_data.get('cutover_date')
    if isinstance(cutover_date, date):
        cutover_date_str = cutover_date.isoformat()
    elif cutover_date is None:
        cutover_date_str = date.today().isoformat()
    else:
        cutover_date_str = str(cutover_date)  # Already string
    
    # Prepare data for XML template
    template_data = {
        # Core identifiers
        'sender_operator': sender,
        'recipient_operator': recipient,
        'recipient_operator_code': request_data.get('recipient_operator_code', sender),
        'donating_operator_code': request_data.get('donating_operator_code', ''),
        'msisdn': request_data.get('msisdn', ''),
        
        # Customer data
        'iccid': request_data.get('iccid', ''),
        'tax_code_vat': request_data.get('tax_code_vat', ''),
        'payment_type': request_data.get('payment_type', 'PRP'),
        'analog_digital_code': request_data.get('analog_digital_code', 'D'),
        'cutover_date': cutover_date_str,  # ✅ Fixed: Use processed value
        'customer_first_name': request_data.get('customer_first_name', ''),
        'customer_last_name': request_data.get('customer_last_name', ''),
        'imsi': request_data.get('imsi', ''),
        
        # Flags
        'credit_transfer_flag': request_data.get('credit_transfer_flag', 'N'),
        'routing_number': request_data.get('routing_number', ''),
        'pre_validation_flag': request_data.get('pre_validation_flag', 'N'),
        'theft_flag': request_data.get('theft_flag', 'N'),
        
        # Generated fields
        'file_date': file_date_obj.isoformat(),  # ✅ Fixed: Use date object
        'file_time': file_time_str,
        'file_id': file_id,
        'recipient_request_code': recipient_request_code,
        'message_type_code': '1',
    }
    
    # Validate required fields
    required_fields = ['msisdn', 'cutover_date']
    missing = [f for f in required_fields if not template_data.get(f)]
    if missing:
        logger.error("Missing required fields for XML: %s", missing)
        raise ValueError(f"Missing required fields: {missing}")
    
    # Generate XML
    xml_content = TYPE_1_ACTIVATION.format(**template_data)
    
    # Generate MNP filename
    mnp_filename = generate_mnp_filename(sender, recipient, file_id)
    
    logger.debug("Created Type 1 XML for MSISDN: %s", template_data['msisdn'])
    
    # Return both XML and metadata
    return GeneratedXMLResult(
        xml=xml_content,
        metadata={
            'file_date': file_date_obj,  # Keep as date object for response
            'file_time': file_time_str,
            'file_id': file_id,
            'recipient_request_code': recipient_request_code,
            'sender_operator': sender,
            'recipient_operator': recipient,
            'mnp_filename': mnp_filename,
            'cutover_date': cutover_date_str if isinstance(cutover_date, str) else cutover_date,
        }
    )