# xsd.py (updated main script)
import xmlschema
import json
from decimal import Decimal
from typing import Dict, Any

class EnglishMNPParser:
    def __init__(self, xsd_path: str):
        self.schema = xmlschema.XMLSchema(xsd_path)
        
        # Italian to English field name mapping
        self.field_translations = {
            # FILENAME fields
            "MITTENTE": "SENDER",
            "DATA": "DATE",
            "ORA": "TIME",
            "DESTINATARIO": "RECIPIENT",
            "ID_FILE": "FILE_ID",
            
            # Common message fields
            "TIPO_MESSAGGIO": "MESSAGE_TYPE_CODE",
            "CODICE_OPERATORE_RECIPIENT": "RECIPIENT_OPERATOR_CODE",
            "CODICE_OPERATORE_DONATING": "DONATING_OPERATOR_CODE",
            "CODICE_RICHIESTA_RECIPIENT": "REQUEST_CODE_RECIPIENT",
            "CODICE_GRUPPO": "GROUP_CODE",
            "MSISDN": "PHONE_NUMBER",
            "ADDIZIONALE_1": "ADDITIONAL_1",
            "ADDIZIONALE_2": "ADDITIONAL_2",
            "ICCID_SERIAL_NUMBER": "ICCID_SERIAL_NUMBER",
            "CODICE_FISCALE_PARTITA_IVA": "TAX_CODE_VAT",
            "CODICE_PRE_POST_PAGATO": "PRE_POST_PAID_CODE",
            "CODICE_ANALOGICO_DIGITALE": "ANALOG_DIGITAL_CODE",
            "DATA_CUT_OVER": "CUT_OVER_DATE",
            "NOME_CLIENTE": "CUSTOMER_FIRST_NAME",
            "COGNOME_CLIENTE": "CUSTOMER_LAST_NAME",
            "RAGIONE_SOCIALE": "COMPANY_NAME",
            "TIPO_DOCUMENTO": "DOCUMENT_TYPE",
            "NUMERO_DOCUMENTO": "DOCUMENT_NUMBER",
            "IMSI": "IMSI",
            "FLAG_TRASFERIMENTO_CREDITO": "CREDIT_TRANSFER_FLAG",
            "CODICE_OPERATORE_VIRTUALE_RECIPIENT": "VIRTUAL_RECIPIENT_OPERATOR_CODE",
            "CODICE_OPERATORE_VIRTUALE_DONATING": "VIRTUAL_DONATING_OPERATOR_CODE",
            "ROUTING_NUMBER": "ROUTING_NUMBER",
            "PREVALIDAZIONE": "PREVALIDATION_FLAG",
            "FURTO": "THEFT_FLAG",
            "STATO_RICHIESTA_NOTIFICA": "REQUEST_STATUS_NOTIFICATION",
            "CODICE_MOTIVO_RIFIUTO": "REJECTION_REASON_CODE",
            "ORA_CUT_OVER": "CUT_OVER_TIME",
            "CODICE_OPERATORE": "OPERATOR_CODE",
            "DATA_NOTIFICA_CREDITO": "CREDIT_NOTIFICATION_DATE",
            "ORA_NOTIFICA_CREDITO": "CREDIT_NOTIFICATION_TIME",
            "IMPORTO_CREDITO_RESIDUO": "REMAINING_CREDIT_AMOUNT",
            "FLAG_VERIFICA_CREDITO_ANOMALO": "ANOMALOUS_CREDIT_CHECK_FLAG",
            
            # ACKNOWLEDGE fields
            "RISULTATO": "RESULT",
            "ERRMSG": "ERROR_MESSAGE"
        }
    
    def translate_structure(self, data):
        """Recursively translate field names in the data structure."""
        if isinstance(data, dict):
            translated = {}
            for key, value in data.items():
                # Translate the key if we have a translation
                translated_key = self.field_translations.get(key, key)
                # Recursively translate nested structures
                translated[translated_key] = self.translate_structure(value)
            return translated
        elif isinstance(data, list):
            return [self.translate_structure(item) for item in data]
        else:
            return data
    
    def decimal_to_float(self, obj):
        """Convert Decimal objects to float for JSON serialization."""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: self.decimal_to_float(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.decimal_to_float(item) for item in obj]
        else:
            return obj
    
    def parse_and_print_english(self, xml_content: str):
        """Parse XML and print with English field names."""
        try:
            # Handle encoding
            if 'ISO-8859-1' in xml_content:
                xml_content = xml_content.replace('ISO-8859-1', 'UTF-8')
            
            # Parse to dict
            data = self.schema.to_dict(xml_content)
            
            # Translate field names to English
            english_data = self.translate_structure(data)
            
            # Convert Decimal to float for JSON serialization
            english_data = self.decimal_to_float(english_data)
            
            print("=" * 60)
            print("COMPLETE PARSED DATA STRUCTURE (ENGLISH FIELD NAMES):")
            print("=" * 60)
            print(json.dumps(english_data, indent=2, ensure_ascii=False))
            print("=" * 60)
            
        except Exception as e:
            print(f"Error: {e}")
    
    def parse_to_dict(self, xml_content: str) -> Dict:
        """Parse XML and return as dictionary with English field names."""
        try:
            # Handle encoding
            if 'ISO-8859-1' in xml_content:
                xml_content = xml_content.replace('ISO-8859-1', 'UTF-8')
            
            # Parse to dict
            data = self.schema.to_dict(xml_content)
            
            # Translate field names to English
            english_data = self.translate_structure(data)
            
            # Convert Decimal to float for JSON serialization
            return self.decimal_to_float(english_data)
            
        except Exception as e:
            raise ValueError(f"Failed to parse XML: {e}")

# Usage with verification
if __name__ == "__main__":
    # SET THIS VARIABLE TO CONTROL OUTPUT
    RUN_TESTS = False  # Set to False to only show required fields
    
    # Your test XML strings
    xml_1 = """<?xml version="1.0" encoding="UTF-8"?><LISTA_MNP_RECORD><FILENAME><MITTENTE>PMOB</MITTENTE><DATA>2025-10-20</DATA><ORA>12:00:55</ORA><DESTINATARIO>LMIT</DESTINATARIO><ID_FILE>99137</ID_FILE></FILENAME><ATTIVAZIONE><TIPO_MESSAGGIO>1</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>PMOB</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>LMIT</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>1-1ZSQ06RB</CODICE_RICHIESTA_RECIPIENT><MSISDN>393508225575</MSISDN><CODICE_FISCALE_PARTITA_IVA>KMRBAU95L02Z344U</CODICE_FISCALE_PARTITA_IVA><DATA_CUT_OVER>2025-10-22</DATA_CUT_OVER><NOME_CLIENTE>ABU</NOME_CLIENTE><COGNOME_CLIENTE>KAMARA</COGNOME_CLIENTE><IMSI>222337987134047</IMSI><FLAG_TRASFERIMENTO_CREDITO>Y</FLAG_TRASFERIMENTO_CREDITO><ROUTING_NUMBER>741</ROUTING_NUMBER><PREVALIDAZIONE>Y</PREVALIDAZIONE><FURTO>N</FURTO></ATTIVAZIONE></LISTA_MNP_RECORD>"""
    
    xml_5 = """<?xml version="1.0" encoding="ISO-8859-1"?><LISTA_MNP_RECORD><FILENAME><MITTENTE>COOP</MITTENTE><DATA>2025-10-16</DATA><ORA>20:37:45</ORA><DESTINATARIO>LMIT</DESTINATARIO><ID_FILE>90199</ID_FILE></FILENAME><PRESAINCARICO><TIPO_MESSAGGIO>5</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>LMIT</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>COOP</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>LYCA2510160025</CODICE_RICHIESTA_RECIPIENT><MSISDN>393500321080</MSISDN><STATO_RICHIESTA_NOTIFICA>6</STATO_RICHIESTA_NOTIFICA></PRESAINCARICO></LISTA_MNP_RECORD>"""

    xml_2 = """<?xml version="1.0" encoding="utf-8"?><LISTA_MNP_RECORD><FILENAME><MITTENTE>PLTN</MITTENTE><DATA>2025-10-17</DATA><ORA>04:00:05</ORA><DESTINATARIO>LMIT</DESTINATARIO><ID_FILE>11002</ID_FILE></FILENAME><VALIDAZIONE><TIPO_MESSAGGIO>2</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>LMIT</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>PLTN</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>LYCA2510150233</CODICE_RICHIESTA_RECIPIENT><MSISDN>393762062545</MSISDN><STATO_RICHIESTA_NOTIFICA>0</STATO_RICHIESTA_NOTIFICA><DATA_CUT_OVER>2025-10-20</DATA_CUT_OVER><FLAG_TRASFERIMENTO_CREDITO>Y</FLAG_TRASFERIMENTO_CREDITO><CODICE_OPERATORE_VIRTUALE_DONATING>Q014</CODICE_OPERATORE_VIRTUALE_DONATING></VALIDAZIONE></LISTA_MNP_RECORD>"""

    xml_6 = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><LISTA_MNP_RECORD><FILENAME><MITTENTE>WIN3</MITTENTE><DATA>2025-10-17</DATA><ORA>05:00:30</ORA><DESTINATARIO>LMIT</DESTINATARIO><ID_FILE>84221</ID_FILE></FILENAME><ESPLETAMENTO><TIPO_MESSAGGIO>6</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>LMIT</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>WIN3</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>LYCA2510150138</CODICE_RICHIESTA_RECIPIENT><MSISDN>393778469925</MSISDN><DATA_CUT_OVER>2025-10-17</DATA_CUT_OVER><ORA_CUT_OVER>05:00:00</ORA_CUT_OVER><CODICE_OPERATORE>WIN3</CODICE_OPERATORE><STATO_RICHIESTA_NOTIFICA>4</STATO_RICHIESTA_NOTIFICA><FLAG_TRASFERIMENTO_CREDITO>Y</FLAG_TRASFERIMENTO_CREDITO><CODICE_OPERATORE_VIRTUALE_DONATING>Z003</CODICE_OPERATORE_VIRTUALE_DONATING><ROUTING_NUMBER>382</ROUTING_NUMBER></ESPLETAMENTO><ESPLETAMENTO><TIPO_MESSAGGIO>6</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>LMIT</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>WIN3</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>LYCA2510150135</CODICE_RICHIESTA_RECIPIENT><MSISDN>393385366477</MSISDN><DATA_CUT_OVER>2025-10-17</DATA_CUT_OVER><ORA_CUT_OVER>05:00:00</ORA_CUT_OVER><CODICE_OPERATORE>WIN3</CODICE_OPERATORE><STATO_RICHIESTA_NOTIFICA>4</STATO_RICHIESTA_NOTIFICA><FLAG_TRASFERIMENTO_CREDITO>Y</FLAG_TRASFERIMENTO_CREDITO><CODICE_OPERATORE_VIRTUALE_DONATING>Z003</CODICE_OPERATORE_VIRTUALE_DONATING><ROUTING_NUMBER>382</ROUTING_NUMBER></ESPLETAMENTO></LISTA_MNP_RECORD>"""

    xml_10 = """<?xml version="1.0" encoding="UTF-8"?><LISTA_MNP_RECORD><FILENAME><MITTENTE>OPIV</MITTENTE><DATA>2025-10-17</DATA><ORA>11:04:43</ORA><DESTINATARIO>LMIT</DESTINATARIO><ID_FILE>238</ID_FILE></FILENAME><TRASFERIMENTOCREDITO><TIPO_MESSAGGIO>10</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>LMIT</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>OPIV</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>LYCA2510150052</CODICE_RICHIESTA_RECIPIENT><MSISDN>393773739342</MSISDN><DATA_CUT_OVER>2025-10-17</DATA_CUT_OVER><ORA_CUT_OVER>06:04:35</ORA_CUT_OVER><FLAG_TRASFERIMENTO_CREDITO>Y</FLAG_TRASFERIMENTO_CREDITO><DATA_NOTIFICA_CREDITO>2025-10-17</DATA_NOTIFICA_CREDITO><ORA_NOTIFICA_CREDITO>11:03:23</ORA_NOTIFICA_CREDITO><IMPORTO_CREDITO_RESIDUO>1.01</IMPORTO_CREDITO_RESIDUO><CODICE_OPERATORE_VIRTUALE_DONATING>O101</CODICE_OPERATORE_VIRTUALE_DONATING><FLAG_VERIFICA_CREDITO_ANOMALO>N</FLAG_VERIFICA_CREDITO_ANOMALO></TRASFERIMENTOCREDITO></LISTA_MNP_RECORD>"""

    # Initialize parser
    parser = EnglishMNPParser('mnp_schema.xsd')
    
    if RUN_TESTS:
        # ======================================================================
        # RUN FULL TESTS (original behavior)
        # ======================================================================
        # First, test the parser's public API directly
        print("\n" + "="*80)
        print("DIRECT PARSER API TESTING")
        print("="*80)
        
        # Test 1: Test parse_to_dict method
        print("\n1. Testing parse_to_dict() method:")
        test_messages = [
            ("Type 1 (ATTIVAZIONE)", xml_1),
            ("Type 5 (PRESAINCARICO)", xml_5),
            ("Type 2 (VALIDAZIONE)", xml_2),
            ("Type 6 (ESPLETAMENTO - 2 records)", xml_6),
            ("Type 10 (TRASFERIMENTOCREDITO)", xml_10)
        ]
        
        for test_name, xml_content in test_messages:
            try:
                result = parser.parse_to_dict(xml_content)
                
                # Find the message type
                msg_type = None
                for key in result:
                    if key != 'FILENAME':
                        msg_type = key
                        break
                
                print(f"  ✓ {test_name}:")
                print(f"    - Message Type: {msg_type}")
                print(f"    - Sender: {result['FILENAME']['SENDER']}")
                print(f"    - Recipient: {result['FILENAME']['RECIPIENT']}")
                
                if msg_type and msg_type in result:
                    msg_data = result[msg_type]
                    if isinstance(msg_data, list):
                        print(f"    - Records: {len(msg_data)}")
                        if len(msg_data) > 0:
                            print(f"    - Fields in first record: {len(msg_data[0])}")
                    elif isinstance(msg_data, dict):
                        print(f"    - Fields: {len(msg_data)}")
                
            except Exception as e:
                print(f"  ✗ {test_name}: ERROR - {e}")
        
        # Test 2: Test parse_and_print_english method
        print("\n\n2. Testing parse_and_print_english() method (first message only):")
        try:
            print("\nParsing message type 1...")
            parser.parse_and_print_english(xml_1)
        except Exception as e:
            print(f"  ERROR: {e}")
        
        # Now run the comprehensive verification suite
        print("\n" + "="*80)
        print("RUNNING COMPREHENSIVE VERIFICATION SUITE")
        print("="*80)
        
        try:
            # Try to import from tests.xsd_verify (as you specified)
            from tests.xsd_verify import MNPVerifier, TEST_CASES
            
            # Create verifier instance
            verifier = MNPVerifier()
            
            # Run verification suite
            print("\nStarting verification tests...")
            results = verifier.run_verification_suite(parser, TEST_CASES)
            
            # Additional verification: Test each message individually
            print("\n" + "="*80)
            print("INDIVIDUAL MESSAGE VALIDATION")
            print("="*80)
            
            for test_name, xml_content, expected_type in TEST_CASES:
                print(f"\nValidating {test_name}...")
                success, result = verifier.verify_message(parser, xml_content, test_name)
                
                if success:
                    print(f"  ✓ VALIDATION PASSED")
                    print(f"    - Expected: {expected_type}, Got: {result['message_type']}")
                    print(f"    - Data types: {'OK' if result['data_types_valid'] else 'WARNINGS'}")
                    if not result['data_types_valid']:
                        for error in result['data_type_errors']:
                            print(f"      * {error}")
                else:
                    print(f"  ✗ VALIDATION FAILED: {result['error']}")
            
        except ImportError as e:
            print(f"\n⚠️  Verification module not found: {e}")
            print("Make sure xsd_verify.py is in the tests directory")
            
            # Fallback: Run basic tests without verification module
            print("\nRunning basic tests without verification module...")
            
            test_cases = [
                ("Message Type 1", xml_1, "ATTIVAZIONE"),
                ("Message Type 5", xml_5, "PRESAINCARICO"),
                ("Message Type 2", xml_2, "VALIDAZIONE"),
                ("Message Type 6", xml_6, "ESPLETAMENTO"),
                ("Message Type 10", xml_10, "TRASFERIMENTOCREDITO"),
            ]
            
            for test_name, xml_content, expected_type in test_cases:
                print(f"\n{test_name}:")
                try:
                    result = parser.parse_to_dict(xml_content)
                    
                    # Find actual message type
                    actual_type = None
                    for key in result:
                        if key != 'FILENAME':
                            actual_type = key
                            break
                    
                    if actual_type == expected_type:
                        print(f"  ✓ Correctly identified as {actual_type}")
                        print(f"  ✓ Sender: {result['FILENAME']['SENDER']}")
                        print(f"  ✓ Recipient: {result['FILENAME']['RECIPIENT']}")
                    else:
                        print(f"  ✗ Expected {expected_type}, got {actual_type}")
                        
                except Exception as e:
                    print(f"  ✗ Error: {e}")
        
        # Summary
        print("\n" + "="*80)
        print("PARSER TESTING COMPLETE")
        print("="*80)
        print("\nThe parser has been tested with:")
        print("- 5 different message types (1, 2, 5, 6, 10)")
        print("- Single and multiple record messages")
        print("- Different encodings (UTF-8, ISO-8859-1)")
        print("- Decimal number handling")
        print("- Field name translation (Italian → English)")
        print("\nAll tests should pass if the parser is working correctly!")
    
    else:
# ======================================================================
# MINIMAL OUTPUT - Only show required fields
# ======================================================================
        print("PARSER DEMONSTRATION - Showing required field access\n")
        result = parser.parse_to_dict(xml_5)
        # print(result)

        filename_data = result.get('FILENAME', {})
        sender = filename_data.get('SENDER', 'Not found')
        recipient = filename_data.get('RECIPIENT', 'Not found')
        file_id = filename_data.get('FILE_ID', 'Not found')
        print(f"Sender: {sender}")
        print(f"Recipient: {recipient}")
        print(f"File ID: {file_id}")

        # Exit after minimal demonstration
        # Find message type
        msg_type = None
        for key in result:
            if key != 'FILENAME':
                msg_type = key
                break

        if msg_type:
            print(f"   Message type found: {msg_type}")
            msg_data = result[msg_type]

            if isinstance(msg_data, list):
                print(f"   Multiple records: {len(msg_data)}")
                if len(msg_data) > 0:
                    first_record = msg_data[0]
                    # Show key fields
                    print(f"\n   First record key fields:")
                    key_fields = [
                        ('PHONE_NUMBER', 'Phone Number'),
                        ('REQUEST_CODE_RECIPIENT', 'Request Code'),
                        ('MESSAGE_TYPE_CODE', 'Message Type Code'),
                        ('RECIPIENT_OPERATOR_CODE', 'Recipient Operator'),
                        ('DONATING_OPERATOR_CODE', 'Donating Operator')
                    ]

                    for field_key, field_name in key_fields:
                        value = first_record.get(field_key)
                        if value is not None:
                            print(f"     {field_name}: {value}")

                    # Show conditional fields based on message type
                    if msg_type == 'ATTIVAZIONE':
                        customer_name = f"{first_record.get('CUSTOMER_FIRST_NAME', '')} {first_record.get('CUSTOMER_LAST_NAME', '')}".strip()
                        if customer_name:
                            print(f"     Customer: {customer_name}")
                        print(f"     Cut-over Date: {first_record.get('CUT_OVER_DATE')}")
                        print(f"     Credit Transfer: {first_record.get('CREDIT_TRANSFER_FLAG')}")
                    elif msg_type == 'TRASFERIMENTOCREDITO':
                        print(f"     Remaining Credit: {first_record.get('REMAINING_CREDIT_AMOUNT')}")
                        print(f"     Credit Notification: {first_record.get('CREDIT_NOTIFICATION_DATE')} {first_record.get('CREDIT_NOTIFICATION_TIME')}")

            elif isinstance(msg_data, dict):
                print(f"   Single record as dict")
                print(f"   Number of fields: {len(msg_data)}")
        
            exit()

        
        # Process each XML and show how to access fields
        xml_examples = [
            ("Message Type 1", xml_1),
            ("Message Type 5", xml_5),
            ("Message Type 2", xml_2),
            ("Message Type 6", xml_6),
            ("Message Type 10", xml_10)
        ]
        
        for msg_name, xml_content in xml_examples:
            print(f"\n{'='*60}")
            print(f"Processing {msg_name}")
            print(f"{'='*60}")
            
            try:
                # Parse the XML
                result = parser.parse_to_dict(xml_content)
                
                # ==============================================================
                # DEMONSTRATION: How to access FILENAME values
                # ==============================================================
                print("\n1. Accessing FILENAME fields:")
                
                # Method 1: Direct access
                sender = result['FILENAME']['SENDER']
                recipient = result['FILENAME']['RECIPIENT']
                file_date = result['FILENAME']['DATE']
                file_time = result['FILENAME']['TIME']
                file_id = result['FILENAME']['FILE_ID']
                
                print(f"   Direct access:")
                print(f"     Sender: {sender}")
                print(f"     Recipient: {recipient}")
                print(f"     Date: {file_date}")
                print(f"     Time: {file_time}")
                print(f"     File ID: {file_id} (type: {type(file_id).__name__})")
                
                # Method 2: Safe access with .get()
                print(f"\n   Safe access with .get():")
                filename_data = result.get('FILENAME', {})
                safe_sender = filename_data.get('SENDER', 'Not found')
                safe_recipient = filename_data.get('RECIPIENT', 'Not found')
                safe_data = filename_data.get('DATE', 'Not found')
                print(f"     Sender: {safe_sender}")
                print(f"     Recipient: {safe_recipient}")
                print(f"     Data: {safe_data}")
                
                # ==============================================================
                # DEMONSTRATION: How to access message data
                # ==============================================================
                print(f"\n2. Accessing message data:")
                
                # Find message type
                msg_type = None
                for key in result:
                    if key != 'FILENAME':
                        msg_type = key
                        break
                
                if msg_type:
                    print(f"   Message type found: {msg_type}")
                    msg_data = result[msg_type]
                    
                    if isinstance(msg_data, list):
                        print(f"   Multiple records: {len(msg_data)}")
                        if len(msg_data) > 0:
                            first_record = msg_data[0]
                            # Show key fields
                            print(f"\n   First record key fields:")
                            key_fields = [
                                ('PHONE_NUMBER', 'Phone Number'),
                                ('REQUEST_CODE_RECIPIENT', 'Request Code'),
                                ('MESSAGE_TYPE_CODE', 'Message Type Code'),
                                ('RECIPIENT_OPERATOR_CODE', 'Recipient Operator'),
                                ('DONATING_OPERATOR_CODE', 'Donating Operator')
                            ]
                            
                            for field_key, field_name in key_fields:
                                value = first_record.get(field_key)
                                if value is not None:
                                    print(f"     {field_name}: {value}")
                            
                            # Show conditional fields based on message type
                            if msg_type == 'ATTIVAZIONE':
                                customer_name = f"{first_record.get('CUSTOMER_FIRST_NAME', '')} {first_record.get('CUSTOMER_LAST_NAME', '')}".strip()
                                if customer_name:
                                    print(f"     Customer: {customer_name}")
                                print(f"     Cut-over Date: {first_record.get('CUT_OVER_DATE')}")
                                print(f"     Credit Transfer: {first_record.get('CREDIT_TRANSFER_FLAG')}")
                            
                            elif msg_type == 'TRASFERIMENTOCREDITO':
                                print(f"     Remaining Credit: {first_record.get('REMAINING_CREDIT_AMOUNT')}")
                                print(f"     Credit Notification: {first_record.get('CREDIT_NOTIFICATION_DATE')} {first_record.get('CREDIT_NOTIFICATION_TIME')}")
                    
                    elif isinstance(msg_data, dict):
                        print(f"   Single record as dict")
                        print(f"   Number of fields: {len(msg_data)}")
                
                # ==============================================================
                # DEMONSTRATION: Practical usage examples
                # ==============================================================
                print(f"\n3. Practical usage examples:")
                
                # Example 1: Create a summary
                summary = {
                    'sender': sender,
                    'recipient': recipient,
                    'file_id': file_id,
                    'message_type': msg_type,
                    'timestamp': f"{file_date} {file_time}"
                }
                print(f"   Summary dict: {summary}")
                
                # Example 2: Check for specific conditions
                if msg_type == 'ATTIVAZIONE' and result.get('ATTIVAZIONE'):
                    activation_data = result['ATTIVAZIONE']
                    if isinstance(activation_data, list) and len(activation_data) > 0:
                        record = activation_data[0]
                        if record.get('CREDIT_TRANSFER_FLAG') == 'Y':
                            print(f"   ⚠️  Note: Credit transfer requested")
                        if record.get('PREVALIDATION_FLAG') == 'Y':
                            print(f"   ⚠️  Note: Prevalidation required")
                
                # Example 3: Extract all data for processing
                print(f"\n4. Complete data extraction:")
                print(f"   Use parser.parse_to_dict(xml) to get full dictionary")
                print(f"   Then access: result['FILENAME'] for header info")
                print(f"   And: result['MESSAGE_TYPE'] for message data")
                
            except Exception as e:
                print(f"  ✗ Error processing {msg_name}: {e}")
        
        print(f"\n{'='*60}")
        print("QUICK REFERENCE - Field Access Examples")
        print(f"{'='*60}")
        print("""
# After parsing: result = parser.parse_to_dict(xml_content)

# Access FILENAME fields:
sender = result['FILENAME']['SENDER']
recipient = result['FILENAME']['RECIPIENT']
file_id = result['FILENAME']['FILE_ID']

# Safe access:
sender = result.get('FILENAME', {}).get('SENDER', 'Unknown')

# Find message type:
msg_type = [k for k in result.keys() if k != 'FILENAME'][0]

# Access message data:
if msg_type in result:
    msg_data = result[msg_type]
    if isinstance(msg_data, list) and len(msg_data) > 0:
        first_record = msg_data[0]
        msisdn = first_record.get('PHONE_NUMBER')
        request_code = first_record.get('REQUEST_CODE_RECIPIENT')
        """)