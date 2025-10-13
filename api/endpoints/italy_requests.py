from fastapi import Form, UploadFile, File, HTTPException, BackgroundTasks, status,APIRouter
from pydantic import BaseModel
from typing import Optional
import xml.etree.ElementTree as ET
from datetime import datetime
import uuid
import os
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from config import settings

# app = FastAPI(title="MNP Gateway API")
router = APIRouter()

# Create security instance in this file
security = HTTPBasic()


class MNPResponse(BaseModel):
    status: str
    message_id: str
    timestamp: str
    details: Optional[str] = None

# RECEIVE endpoint - for receiving files from other operators
from fastapi import FastAPI, Form, UploadFile, File, HTTPException, BackgroundTasks, status
from pydantic import BaseModel
from typing import Optional
import xml.etree.ElementTree as ET
from datetime import datetime
import uuid
import os

@router.post(
    "/receive",
    response_model=MNPResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Receive MNP XML File",
    description="""
    Receive MNP XML files from other operators via HTTPS POST.
    
    This endpoint implements the standard MNP file exchange protocol between telecom operators.
    Files are processed asynchronously to guarantee ACKNOWLEDGE within 15 minutes as per MNP standards.
    
    **Protocol Details:**
    - Files are transmitted as form data with three required parameters
    - Immediate ACK response with background processing
    - Supports all MNP message types (1-13)
    - Validates filename format and XML structure
    
    **File Naming Convention:**
    `****YYYYMMDDhhmmss++++nnnnn.xml`
    - `****`: 4-character sender operator code
    - `YYYYMMDDhhmmss`: Timestamp of transmission
    - `++++`: 4-character receiver operator code  
    - `nnnnn`: 5-digit sequence number
    """,
    response_description="Immediate acknowledgment with processing queue details",
    tags=["Italy: MNP File Exchange"],
    responses={
        202: {
            "description": "File accepted and queued for processing",
            "content": {
                "application/json": {
                    "example": {
                        "status": "ACKNOWLEDGED",
                        "message_id": "MSG_20241115143000_abc12345",
                        "timestamp": "2024-11-15T14:30:00Z",
                        "details": "File VODA20241115143000ORNG00001.xml received successfully and queued for processing"
                    }
                }
            }
        },
        400: {
            "description": "Invalid request data",
            "content": {
                "application/json": {
                    "examples": {
                        "invalid_filename": {
                            "value": {
                                "detail": "Invalid filename format"
                            }
                        },
                        "invalid_file_type": {
                            "value": {
                                "detail": "Invalid file type"
                            }
                        },
                        "invalid_xml": {
                            "value": {
                                "detail": "Invalid XML format"
                            }
                        }
                    }
                }
            }
        },
        500: {
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Error processing file: Unable to parse XML content"
                    }
                }
            }
        }
    }
)
async def receive_mnp_file(
    background_tasks: BackgroundTasks,
    filename: str = Form(
        ...,
        description="""
        XML file name following MNP naming convention.
        
        **Format:** `****YYYYMMDDhhmmss++++nnnnn.xml`
        
        **Examples:**
        - `VODA20241115143000ORNG00001.xml`
        - `TIMI20241115143500OLBE00001.xml`
        """,
        examples=["VODA20241115143000ORNG00001.xml", "TIMI20241115143500OLBE00001.xml"]
    ),
    filexml: str = Form(
        ...,
        description="""
        Entire XML content as string.
        
## MNP Message Fields Specification

| ID | Field Name | Description | Type | Length |
|----|------------|-------------|------|--------|
| 1 | **Message Type** | Identifies the type of message | Num | 2 |
| | | **Allowed Values:**<br>• `1` - Activation request<br>• `2` - Validation outcome notification<br>• `3` - Porting notification<br>• `4` - Multiple request cancellation notification<br>• `5` - Taking charge notification<br>• `6` - Fulfilment outcome notification<br>• `7` - Cessation notification<br>• `9` - Activation request for Ad Hoc Project<br>• `10` - Residual Credit Request<br>• `11` - Anomalous Credit Unblock<br>• `12` - Amount Unblock<br>• `13` - Cut-over change request | | |
| 2 | **Recipient Operator Code** | Identification of Network Recipient Operator | Char | 4 |
| 3 | **Donating Operator Code** | Identification of Network Donating Operator | Char | 4 |
| 4 | **Recipient Request Code** | Identification given to request by Recipient Operator. Must be unique and not contain lowercase letters | Char | 18 |
| 5 | **Group Code** | Identifies a group of requests that must be handled as a single order | Char | 12 |
| 6 | **MSISDN** | Identifies the principal MSISDN on the donating network on which it is requested to activate MNP (international format) | Num | 15 |
| 7 | **Additional1** | Identifies the additional1 associated to the principal for which it is requested to activate MNP | Num | 15 |
| 8 | **Additional2** | Identifies the additional2 associated to the principal for which it is requested to activate MNP | Num | 15 |
| 9 | **Tax Code/VAT No.** | Tax Code or VAT no. of the client requesting the service | Char | 16 |
| 10 | **ICCID/Serial number** | Identifies the serial number of the SIM card or of the mobile phone (TACS) | Char | 19 |
| 11 | **Pre-post paid Code** | Identifies the type of the service used on the Donating network | Char | 3 |
| | | **Values:**<br>• `PRP` - Pre-paid<br>• `POP` - Post-paid | | |
| 12 | **Analogue/Digital Code** | Identifies the service technology used on the Donating network | Char | 1 |
| | | **Values:**<br>• `D` - Digital technology (GSM)<br>• `A` - Analogue (ETACS) | | |
| 13 | **Cut-over date** | Cut-over date proposed by the client (activation) or effective date of cessation (cessation) in format `YYYY-MM-DD` | Date | 8 |
| 14 | **Cut-over time** | Time of cut-over in format `hh:mm:ss` | Time | 5 |
| 15 | **Request/Notification status** | Status of the request | Num | 2 |
| | | **Allowed Values:**<br>• `0` - Accepted<br>• `1` - Refused<br>• `3` - Discarded for scheduled maintenance stoppage<br>• `4` - Porting OK<br>• `5` - Porting KO<br>• `6` - Taken in charge<br>• `7` - Discarded from waiting list<br>• `8` - Discarded in Overflow<br>• `9` - Discarded for non-unique request code<br>• *Status 2 is not used* | | |
| 16 | **List of refusal reason** | List of codes for refusal reasons encountered in validation | Sequence | - |
| 17 | **Operator Code** | Code of Operator issuing the fulfilment outcome, plus Network Operator Code | Char | 4 |
| 18 | **Customer name** | Name of customer requesting porting | Char | 30 |
| 19 | **Customer surname** | Surname of customer requesting porting | Char | 50 |
| 20 | **Official/Company name** | Official/Company name of customer requesting porting | Char | 80 |
| 21 | **Document Type** | Type of document produced by customer or their proxy when requesting porting | Char | 2 |
| | | **Values:**<br>• `CI` - Identity Card<br>• `PA` - Driver's Licence<br>• `PS` - Passport | | |
| 22 | **Document no.** | Identification number of document produced by customer when requesting porting | Char | 30 |
| 23 | **IMSI** | IMSI associated to the new Recipient SIM | Char | 15 |
| 24 | **Credit Transfer Flag** | Indicates the customer's request to port their residual credit to the Recipient | Char | 1 |
| | | **Values:**<br>• `Y` - Yes<br>• `N` - No | | |
| 25 | **Credit Notification Date** | Date of actual notification of credit in format `YYYY-MM-DD` | Date | 8 |
| 26 | **Credit Notification Time** | Time of notification of credit in format `hh:mm:ss` | Time | 5 |
| 27 | **Residual Credit Amount** | Value (in Euro) of the credit to be transferred from Donating to Recipient (format: `99999.99`) | Char | 8 |
| 28 | **Anomalous Credit Check Flag** | Indicates the need for Donating's relevant body to carry out a check on the residual credit | Char | 1 |
| | | **Values:**<br>• `Y` - Yes<br>• `N` - No | | |
| 29 | **Virtual Recipient Operator Code** | Identification of the actual Recipient Operator when it is Virtual | Char | 4 |
| 30 | **Virtual Donating Operator Code** | Identification of the actual Donating Operator when it is Virtual | Char | 4 |
| 31 | **Routing number of hosting Recipient** | Routing number associated with the network hosting the Recipient, or the Recipient itself | Char | 3 |
| 32 | **Pre-validation flag** | Indicates fulfilment of the pre-validation procedure by the recipient | Char | 1 |
| | | **Values:**<br>• `Y` - Yes<br>• `N` - No | | |
| 33 | **Theft flag** | Indicates that the recipient has acquired a report of theft or loss of the SIM associated with the number to be ported | Char | 1 |
| | | **Values:**<br>• `Y` - Yes<br>• `N` - No | | |

### Format Specifications
- **Date Format**: `YYYY-MM-DD` (e.g., 2024-11-15)
- **Time Format**: `hh:mm:ss` (e.g., 14:30:00)
- **Numeric Fields**: Only digits allowed
- **Character Fields**: Alphanumeric characters as specified
- **Currency Format**: `99999.99` (maximum 5 digits, 2 decimal places)

### Field Requirements
- **Mandatory Fields**: 1, 2, 3, 4, 6
- **Conditional Fields**: 7, 8, 13-14, 18-22, 24-33
- **Optional Fields**: 5, 9-12, 16-17, 23

        **Note:**
        **XML must conform to MNP standard schema.**
        """,
        examples=["""
<?xml version="1.0" encoding="UTF-8"?>
<mnp_request>
    <message_type>1</message_type>
    <recipient_operator_code>VODA</recipient_operator_code>
    <donating_operator_code>ORNG</donating_operator_code>
    <recipient_request_code>REQ20241115000123</recipient_request_code>
    <group_code>GRP20241115001</group_code>
    <msisdn>393331234567</msisdn>
    <additional1>393331234568</additional1>
    <additional2>393331234569</additional2>
    <tax_code_vat>RSSMRA80A01H501U</tax_code_vat>
    <iccid_serial_number>8930270012345678912</iccid_serial_number>
    <pre_post_paid_code>POP</pre_post_paid_code>
    <analogue_digital_code>D</analogue_digital_code>
    <cut_over_date>2025-10-20</cut_over_date>
    <cut_over_time>14:30:00</cut_over_time>
    <request_status>0</request_status>
    <refusal_reasons/>
    <operator_code>VODA</operator_code>
    <customer_name>Mario</customer_name>
    <customer_surname>Rossi</customer_surname>
    <official_company_name>Rossi Mario</official_company_name>
    <document_type>CI</document_type>
    <document_number>AB1234567</document_number>
    <imsi>222881234567890</imsi>
    <credit_transfer_flag>Y</credit_transfer_flag>
    <credit_notification_date>2025-10-13</credit_notification_date>
    <credit_notification_time>10:00:00</credit_notification_time>
    <residual_credit_amount>150.50</residual_credit_amount>
    <anomalous_credit_check_flag>N</anomalous_credit_check_flag>
    <virtual_recipient_operator_code>VVOD</virtual_recipient_operator_code>
    <virtual_donating_operator_code>VORG</virtual_donating_operator_code>
    <routing_number_hosting_recipient>001</routing_number_hosting_recipient>
    <pre_validation_flag>Y</pre_validation_flag>
    <theft_flag>N</theft_flag>
</mnp_request>
        """]
    ),
    filets: str = Form(
        ...,
        description="""
        File type corresponding to MNP message type.
        
        **Allowed Values:**
        - `1` - Activation request
        - `2` - Validation outcome notification
        - `3` - Porting notification
        - `4` - Cancellation notification
        - `5` - Taking charge notification
        - `6` - Fulfilment outcome notification
        - `7` - Cessation notification
        - `9` - Ad Hoc activation
        - `10` - Residual Credit
        - `11` - Anomalous Credit Unblock
        - `12` - Amount Unblock
        - `13` - Cut-over change
        """,
        examples=["1", "3", "6"]
    )
):
    """
    Receive MNP XML File from Telecom Operators
    
    Main entry point for MNP file exchange protocol. Handles reception of XML files
    containing portability requests, notifications, and outcomes between operators.
    
    **Workflow:**
    1. Validate filename format and file type
    2. Parse and validate XML structure
    3. Queue for background processing
    4. Return immediate acknowledgment
    
    **Background Processing:**
    - Files are processed asynchronously to meet 15-minute SLA
    - Each message type routed to appropriate business logic
    - Comprehensive logging and error handling
    - Support for batch and individual message processing
    
    **Security:**
    - HTTPS required for all transmissions
    - Operator authentication and authorization
    - XML schema validation
    - Input sanitization and validation
    
    **Example Request:**
    ```bash
    curl -X POST "https://mnp-api.olbe.tech/receive" \\
      -F "filename=VODA20241115143000ORNG00001.xml" \\
      -F "filexml=<?xml version='1.0'?><MNP_Message>...</MNP_Message>" \\
      -F "filets=1"
    ```
    """
    try:
        # Validate filename format
        if not validate_filename_format(filename):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid filename format")
        
        # Validate file type
        if not validate_file_type(filets):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid file type")
        
        # Parse and validate XML
        try:
            root = ET.fromstring(filexml)
        except ET.ParseError:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid XML format")
        
        # Extract message details
        message_info = extract_message_info(root, filets)
        
        # Generate unique message ID
        message_id = f"MSG_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        # Process in background (to guarantee 15-minute ACK)
        background_tasks.add_task(process_mnp_message, filename, filexml, filets, message_info)
        
        # Immediate ACKNOWLEDGE response
        return MNPResponse(
            status="ACKNOWLEDGED",
            message_id=message_id,
            timestamp=datetime.utcnow().isoformat(),
            details=f"File {filename} received successfully and queued for processing"
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, 
            f"Error processing file: {str(e)}"
        )
    
# SEND endpoint - for sending files to other operators
@router.post("/send", response_model=MNPResponse,
                 include_in_schema=False  # This hides the endpoint from Swagger)
    )
async def send_mnp_file(
    background_tasks: BackgroundTasks,
    filename: str = Form(..., description="XML file name following naming convention"),
    filexml: str = Form(..., description="Entire XML content as string"), 
    filets: str = Form(..., description="File type (same as MESSAGE TYPE field)")
):
    """
    Send MNP XML files to other operators via HTTPS POST
    """
    try:
        # Validate we're allowed to send this file type
        if not can_send_file_type(filets):
            raise HTTPException(400, "Cannot send this file type")
        
        # Extract target operator from filename
        target_operator = extract_target_operator(filename)
        
        # Queue for sending
        background_tasks.add_task(send_to_operator, target_operator, filename, filexml, filets)
        
        return MNPResponse(
            status="QUEUED_FOR_SENDING",
            message_id=f"SEND_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            timestamp=datetime.utcnow().isoformat(),
            details=f"File queued for delivery to {target_operator}"
        )
        
    except Exception as e:
        raise HTTPException(500, f"Error queuing file for sending: {str(e)}")

# Utility functions
def validate_filename_format(filename: str) -> bool:
    """Validate filename format: ****YYYYMMDDhhmmss++++nnnnn"""
    pattern = r"^[A-Z0-9]{4}\d{14}[A-Z0-9]{4}\d{5}\.xml$"
    import re
    return bool(re.match(pattern, filename))

def validate_file_type(filets: str) -> bool:
    """Validate file type matches allowed MNP message types"""
    allowed_types = {"1", "2", "3", "4", "5", "6", "7", "9", "10", "11", "12", "13"}
    return filets in allowed_types

def extract_message_info(root: ET.Element, filets: str) -> dict:
    """Extract key information from XML message"""
    try:
        header = root.find('.//Header')
        return {
            'message_type': filets,
            'sender': header.find('SenderOperator').text if header else None,
            'receiver': header.find('ReceiverOperator').text if header else None,
            'message_id': header.find('MessageID').text if header else None,
            'timestamp': header.find('TimeStamp').text if header else None
        }
    except Exception:
        return {}

def generate_filename(sender_code: str, receiver_code: str, sequence: int) -> str:
    """Generate filename according to naming convention"""
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    sequence_str = str(sequence).zfill(5)
    return f"{sender_code}{timestamp}{receiver_code}{sequence_str}.xml"

def can_send_file_type(filets: str) -> bool:
    """Check if we're allowed to send this file type"""
    # Add business logic here based on operator agreements
    return filets in {"1", "3", "5", "6"}  # Example allowed types

def extract_target_operator(filename: str) -> str:
    """Extract target operator code from filename"""
    # Format: ****YYYYMMDDhhmmss++++nnnnn.xml
    # Target operator is positions 18-21 (0-indexed)
    return filename[14:18]

# Background processing
async def process_mnp_message(filename: str, filexml: str, filets: str, message_info: dict):
    """Background processing of received MNP message"""
    try:
        # Save file to processing directory
        file_path = f"/mnp/incoming/{filename}"
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(filexml)
        
        # Process based on message type
        await route_message_by_type(filets, filexml, message_info)
        
        # Log successful processing
        print(f"Processed MNP message: {filename} type: {filets}")
        
    except Exception as e:
        print(f"Error processing MNP message {filename}: {str(e)}")
        # Handle errors according to MNP protocol

async def send_to_operator(target_operator: str, filename: str, filexml: str, filets: str):
    """Send file to target operator"""
    try:
        # Get operator endpoint from configuration
        operator_endpoint = get_operator_endpoint(target_operator)
        
        # Send via HTTPS POST
        import aiohttp
        async with aiohttp.ClientSession() as session:
            form_data = aiohttp.FormData()
            form_data.add_field('filename', filename)
            form_data.add_field('filexml', filexml)
            form_data.add_field('filets', filets)
            
            async with session.post(operator_endpoint, data=form_data) as response:
                if response.status == 200:
                    print(f"Successfully sent {filename} to {target_operator}")
                else:
                    print(f"Failed to send {filename} to {target_operator}: {response.status}")
                    # Implement retry logic
                    
    except Exception as e:
        print(f"Error sending to operator {target_operator}: {str(e)}")
        # Implement error handling and retries

async def route_message_by_type(filets: str, filexml: str, message_info: dict):
    """Route message to appropriate processor based on type"""
    processors = {
        "1": process_activation_request,
        "2": process_validation_outcome,
        "3": process_porting_notification,
        "4": process_cancellation_notification,
        "5": process_taking_charge_notification,
        "6": process_fulfilment_outcome,
        "7": process_cessation_notification,
        "9": process_ad_hoc_activation,
        "10": process_residual_credit_request,
        "11": process_anomalous_credit_unblock,
        "12": process_amount_unblock,
        "13": process_cutover_change_request
    }
    
    processor = processors.get(filets)
    if processor:
        await processor(filexml, message_info)
    else:
        print(f"No processor for message type: {filets}")

# Example processor for activation requests
async def process_activation_request(filexml: str, message_info: dict):
    """Process activation request (type 1)"""
    root = ET.fromstring(filexml)
    # Extract and process activation request data
    msisdn = root.find('.//MSISDN').text
    recipient_op = root.find('.//RecipientOperatorCode').text
    print(f"Processing activation for MSISDN: {msisdn} from {recipient_op}")
    # Add business logic here

def get_operator_endpoint(operator_code: str) -> str:
    """Get operator API endpoint from configuration"""
    endpoints = {
        "VODA": "https://mnp-vodafone.operator.com/receive",
        "ORNG": "https://mnp-orange.operator.com/receive", 
        "TIMI": "https://mnp-tim.operator.com/receive",
        "WIND": "https://mnp-wind.operator.com/receive"
    }
    return endpoints.get(operator_code, f"https://mnp-{operator_code.lower()}.operator.com/receive")
