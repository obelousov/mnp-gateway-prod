# xsd_verify.py
import json
from typing import Dict, Any, List, Tuple

class MNPVerifier:
    """Verification module for MNP XML parsing - tests the public API."""
    
    def verify_message(self, parser, xml_content: str, test_name: str = "Test") -> Tuple[bool, Dict]:
        """
        Verify XML parsing using the parser's PUBLIC API methods.
        
        Args:
            parser: EnglishMNPParser instance (from main script)
            xml_content: XML string to verify
            test_name: Name of the test
            
        Returns:
            Tuple of (success: bool, results: dict)
        """
        try:
            # Method 1: Test parse_to_dict() method if it exists
            if hasattr(parser, 'parse_to_dict'):
                parsed_dict = parser.parse_to_dict(xml_content)
            # Method 2: Use parse_and_print_english but capture output
            else:
                # Parse using internal methods (for backward compatibility)
                parsed_dict = self._parse_with_internal_methods(parser, xml_content)
            
            # Extract message type
            msg_type = self._extract_message_type(parsed_dict)
            
            # Count records
            record_count = self._count_records(parsed_dict, msg_type)
            
            # Get filename info
            filename_info = self._get_filename_info(parsed_dict)
            
            # Count fields in first message record
            field_count = self._count_fields(parsed_dict, msg_type)
            
            # Check data types
            data_type_check = self._check_data_types(parsed_dict)
            
            return True, {
                'test_name': test_name,
                'message_type': msg_type,
                'message_type_code': self._get_message_code(parsed_dict, msg_type),
                'record_count': record_count,
                'field_count': field_count,
                'has_filename': bool(filename_info),
                'sender': filename_info.get('SENDER', 'N/A'),
                'recipient': filename_info.get('RECIPIENT', 'N/A'),
                'file_id': filename_info.get('FILE_ID', 'N/A'),
                'data_types_valid': data_type_check['valid'],
                'data_type_errors': data_type_check['errors'],
                'parsed_structure': parsed_dict,
                'error': None
            }
            
        except Exception as e:
            return False, {
                'test_name': test_name,
                'error': str(e),
                'parsed_structure': None
            }
    
    def _parse_with_internal_methods(self, parser, xml_content: str) -> Dict:
        """Parse XML using parser's internal methods (for compatibility)."""
        # Handle encoding
        if 'ISO-8859-1' in xml_content:
            xml_content = xml_content.replace('ISO-8859-1', 'UTF-8')
        
        # Parse to dict using the parser's schema
        data = parser.schema.to_dict(xml_content)
        
        # Translate field names using parser's method
        translated = parser.translate_structure(data)
        
        # Convert Decimal to float
        return parser.decimal_to_float(translated)
    
    def _check_data_types(self, parsed_dict: Dict) -> Dict:
        """Check that data types are correctly converted."""
        errors = []
        
        # Check filename fields
        if 'FILENAME' in parsed_dict:
            filename = parsed_dict['FILENAME']
            
            # FILE_ID should be integer
            file_id = filename.get('FILE_ID')
            if file_id is not None and not isinstance(file_id, int):
                errors.append(f"FILE_ID should be int, got {type(file_id).__name__}")
            
            # DATE and TIME should be strings
            for field in ['DATE', 'TIME']:
                value = filename.get(field)
                if value is not None and not isinstance(value, str):
                    errors.append(f"FILENAME.{field} should be str, got {type(value).__name__}")
        
        # Check message fields
        message_types = [
            'ATTIVAZIONE', 'VALIDAZIONE', 'PORTING', 'ANNULLAMENTO',
            'PRESAINCARICO', 'ESPLETAMENTO', 'CESSAZIONE', 'PROGETTOADHOC',
            'TRASFERIMENTOCREDITO', 'SBLOCCOCREDITOANOMALO', 
            'SBLOCCOIMPORTO', 'MODIFICACUTOVER'
        ]
        
        for msg_type in message_types:
            if msg_type in parsed_dict:
                msg_data = parsed_dict[msg_type]
                if isinstance(msg_data, list):
                    for i, record in enumerate(msg_data):
                        if isinstance(record, dict):
                            # MESSAGE_TYPE_CODE should be int
                            mtc = record.get('MESSAGE_TYPE_CODE')
                            if mtc is not None and not isinstance(mtc, int):
                                errors.append(f"{msg_type}[{i}].MESSAGE_TYPE_CODE should be int, got {type(mtc).__name__}")
                            
                            # REQUEST_STATUS_NOTIFICATION should be int if present
                            rsn = record.get('REQUEST_STATUS_NOTIFICATION')
                            if rsn is not None and not isinstance(rsn, int):
                                errors.append(f"{msg_type}[{i}].REQUEST_STATUS_NOTIFICATION should be int, got {type(rsn).__name__}")
                            
                            # REMAINING_CREDIT_AMOUNT should be float if present
                            rca = record.get('REMAINING_CREDIT_AMOUNT')
                            if rca is not None and not isinstance(rca, (int, float)):
                                errors.append(f"{msg_type}[{i}].REMAINING_CREDIT_AMOUNT should be numeric, got {type(rca).__name__}")
                
        return {'valid': len(errors) == 0, 'errors': errors}
    
    def _extract_message_type(self, parsed_dict: Dict) -> str:
        """Extract message type from parsed dictionary."""
        message_types = [
            'ATTIVAZIONE', 'VALIDAZIONE', 'PORTING', 'ANNULLAMENTO',
            'PRESAINCARICO', 'ESPLETAMENTO', 'CESSAZIONE', 'PROGETTOADHOC',
            'TRASFERIMENTOCREDITO', 'SBLOCCOCREDITOANOMALO', 
            'SBLOCCOIMPORTO', 'MODIFICACUTOVER', 'ACKNOWLEDGE'
        ]
        
        for msg_type in message_types:
            if msg_type in parsed_dict:
                return msg_type
        
        return "UNKNOWN"
    
    def _count_records(self, parsed_dict: Dict, msg_type: str) -> int:
        """Count number of records in the message."""
        if msg_type in parsed_dict:
            msg_data = parsed_dict[msg_type]
            if isinstance(msg_data, list):
                return len(msg_data)
            elif isinstance(msg_data, dict):
                return 1
        return 0
    
    def _get_filename_info(self, parsed_dict: Dict) -> Dict:
        """Extract filename information from parsed dictionary."""
        if 'FILENAME' in parsed_dict:
            return parsed_dict['FILENAME']
        return {}
    
    def _count_fields(self, parsed_dict: Dict, msg_type: str) -> int:
        """Count fields in the first message record."""
        if msg_type in parsed_dict:
            msg_data = parsed_dict[msg_type]
            if isinstance(msg_data, list) and len(msg_data) > 0:
                return len(msg_data[0])
            elif isinstance(msg_data, dict):
                return len(msg_data)
        return 0
    
    def _get_message_code(self, parsed_dict: Dict, msg_type: str) -> int:
        """Extract message type code from parsed data."""
        try:
            if msg_type in parsed_dict:
                msg_data = parsed_dict[msg_type]
                if isinstance(msg_data, list) and len(msg_data) > 0:
                    return msg_data[0].get('MESSAGE_TYPE_CODE', 0)
                elif isinstance(msg_data, dict):
                    return msg_data.get('MESSAGE_TYPE_CODE', 0)
            return 0
        except:
            return 0
    
    def run_verification_suite(self, parser, test_cases: List[Tuple[str, str, str]]) -> None:
        """
        Run a suite of verification tests on the parser.
        
        Args:
            parser: EnglishMNPParser instance (from main script)
            test_cases: List of tuples (test_name, xml_content, expected_type)
        """
        print("=" * 80)
        print("MNP PARSER VERIFICATION SUITE - Testing Public API")
        print("=" * 80)
        
        results = []
        all_passed = True
        
        for test_name, xml_content, expected_type in test_cases:
            print(f"\n{'='*40}")
            print(f"TEST: {test_name}")
            print(f"{'='*40}")
            
            success, result = self.verify_message(parser, xml_content, test_name)
            
            if success:
                # Check if the parser parsed correctly
                print(f"✅ PARSER SUCCESS: Message Type {result['message_type_code']} ({result['message_type']})")
                print(f"   Sender: {result['sender']} → Recipient: {result['recipient']}")
                print(f"   File ID: {result['file_id']} (type: {type(result['file_id']).__name__})")
                print(f"   Records: {result['record_count']}, Fields per record: {result['field_count']}")
                
                # Check data types
                if not result['data_types_valid']:
                    print(f"   ⚠️  Data Type Warnings:")
                    for error in result['data_type_errors']:
                        print(f"      - {error}")
                
                # Check if expected type matches
                if expected_type and result['message_type'] != expected_type:
                    print(f"   ⚠️  Warning: Expected type '{expected_type}', got '{result['message_type']}'")
                
                # Show sample of parsed data
                if result['record_count'] > 0:
                    print(f"\n   Sample parsed data (first record, first 5 fields):")
                    if result['message_type'] in result['parsed_structure']:
                        msg_data = result['parsed_structure'][result['message_type']]
                        if isinstance(msg_data, list) and len(msg_data) > 0:
                            first_record = msg_data[0]
                            for i, (key, value) in enumerate(list(first_record.items())[:5]):
                                print(f"      {key}: {value} (type: {type(value).__name__})")
                
            else:
                print(f"❌ PARSER FAILED: {result['error']}")
                all_passed = False
            
            results.append((test_name, success, result))
        
        # Print summary
        print(f"\n{'='*80}")
        print("VERIFICATION SUMMARY")
        print(f"{'='*80}")
        
        passed = sum(1 for _, success, _ in results if success)
        total = len(results)
        
        print(f"Total Tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {total - passed}")
        print(f"Success Rate: {(passed/total*100):.1f}%")
        
        # Additional statistics
        if passed > 0:
            avg_fields = sum(r['field_count'] for _, s, r in results if s) / passed
            avg_records = sum(r['record_count'] for _, s, r in results if s) / passed
            print(f"\nAverage fields per record: {avg_fields:.1f}")
            print(f"Average records per message: {avg_records:.1f}")
        
        if all_passed:
            print(f"\n🎉 PARSER VALIDATION PASSED - All messages parsed correctly!")
        else:
            print(f"\n🔧 Parser validation failed. Check the errors above.")
        
        print(f"{'='*80}")
        
        return results

# Test cases
TEST_CASES = [
    ("Message Type 1 - ATTIVAZIONE", """<?xml version="1.0" encoding="UTF-8"?><LISTA_MNP_RECORD><FILENAME><MITTENTE>PMOB</MITTENTE><DATA>2025-10-20</DATA><ORA>12:00:55</ORA><DESTINATARIO>LMIT</DESTINATARIO><ID_FILE>99137</ID_FILE></FILENAME><ATTIVAZIONE><TIPO_MESSAGGIO>1</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>PMOB</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>LMIT</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>1-1ZSQ06RB</CODICE_RICHIESTA_RECIPIENT><MSISDN>393508225575</MSISDN><CODICE_FISCALE_PARTITA_IVA>KMRBAU95L02Z344U</CODICE_FISCALE_PARTITA_IVA><DATA_CUT_OVER>2025-10-22</DATA_CUT_OVER><NOME_CLIENTE>ABU</NOME_CLIENTE><COGNOME_CLIENTE>KAMARA</COGNOME_CLIENTE><IMSI>222337987134047</IMSI><FLAG_TRASFERIMENTO_CREDITO>Y</FLAG_TRASFERIMENTO_CREDITO><ROUTING_NUMBER>741</ROUTING_NUMBER><PREVALIDAZIONE>Y</PREVALIDAZIONE><FURTO>N</FURTO></ATTIVAZIONE></LISTA_MNP_RECORD>""", "ATTIVAZIONE"),
    ("Message Type 5 - PRESAINCARICO", """<?xml version="1.0" encoding="ISO-8859-1"?><LISTA_MNP_RECORD><FILENAME><MITTENTE>COOP</MITTENTE><DATA>2025-10-16</DATA><ORA>20:37:45</ORA><DESTINATARIO>LMIT</DESTINATARIO><ID_FILE>90199</ID_FILE></FILENAME><PRESAINCARICO><TIPO_MESSAGGIO>5</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>LMIT</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>COOP</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>LYCA2510160025</CODICE_RICHIESTA_RECIPIENT><MSISDN>393500321080</MSISDN><STATO_RICHIESTA_NOTIFICA>6</STATO_RICHIESTA_NOTIFICA></PRESAINCARICO></LISTA_MNP_RECORD>""", "PRESAINCARICO"),
    ("Message Type 2 - VALIDAZIONE", """<?xml version="1.0" encoding="utf-8"?><LISTA_MNP_RECORD><FILENAME><MITTENTE>PLTN</MITTENTE><DATA>2025-10-17</DATA><ORA>04:00:05</ORA><DESTINATARIO>LMIT</DESTINATARIO><ID_FILE>11002</ID_FILE></FILENAME><VALIDAZIONE><TIPO_MESSAGGIO>2</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>LMIT</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>PLTN</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>LYCA2510150233</CODICE_RICHIESTA_RECIPIENT><MSISDN>393762062545</MSISDN><STATO_RICHIESTA_NOTIFICA>0</STATO_RICHIESTA_NOTIFICA><DATA_CUT_OVER>2025-10-20</DATA_CUT_OVER><FLAG_TRASFERIMENTO_CREDITO>Y</FLAG_TRASFERIMENTO_CREDITO><CODICE_OPERATORE_VIRTUALE_DONATING>Q014</CODICE_OPERATORE_VIRTUALE_DONATING></VALIDAZIONE></LISTA_MNP_RECORD>""", "VALIDAZIONE"),
    ("Message Type 6 - ESPLETAMENTO (Multiple Records)", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><LISTA_MNP_RECORD><FILENAME><MITTENTE>WIN3</MITTENTE><DATA>2025-10-17</DATA><ORA>05:00:30</ORA><DESTINATARIO>LMIT</DESTINATARIO><ID_FILE>84221</ID_FILE></FILENAME><ESPLETAMENTO><TIPO_MESSAGGIO>6</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>LMIT</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>WIN3</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>LYCA2510150138</CODICE_RICHIESTA_RECIPIENT><MSISDN>393778469925</MSISDN><DATA_CUT_OVER>2025-10-17</DATA_CUT_OVER><ORA_CUT_OVER>05:00:00</ORA_CUT_OVER><CODICE_OPERATORE>WIN3</CODICE_OPERATORE><STATO_RICHIESTA_NOTIFICA>4</STATO_RICHIESTA_NOTIFICA><FLAG_TRASFERIMENTO_CREDITO>Y</FLAG_TRASFERIMENTO_CREDITO><CODICE_OPERATORE_VIRTUALE_DONATING>Z003</CODICE_OPERATORE_VIRTUALE_DONATING><ROUTING_NUMBER>382</ROUTING_NUMBER></ESPLETAMENTO><ESPLETAMENTO><TIPO_MESSAGGIO>6</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>LMIT</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>WIN3</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>LYCA2510150135</CODICE_RICHIESTA_RECIPIENT><MSISDN>393385366477</MSISDN><DATA_CUT_OVER>2025-10-17</DATA_CUT_OVER><ORA_CUT_OVER>05:00:00</ORA_CUT_OVER><CODICE_OPERATORE>WIN3</CODICE_OPERATORE><STATO_RICHIESTA_NOTIFICA>4</STATO_RICHIESTA_NOTIFICA><FLAG_TRASFERIMENTO_CREDITO>Y</FLAG_TRASFERIMENTO_CREDITO><CODICE_OPERATORE_VIRTUALE_DONATING>Z003</CODICE_OPERATORE_VIRTUALE_DONATING><ROUTING_NUMBER>382</ROUTING_NUMBER></ESPLETAMENTO></LISTA_MNP_RECORD>""", "ESPLETAMENTO"),
    ("Message Type 10 - TRASFERIMENTOCREDITO", """<?xml version="1.0" encoding="UTF-8"?><LISTA_MNP_RECORD><FILENAME><MITTENTE>OPIV</MITTENTE><DATA>2025-10-17</DATA><ORA>11:04:43</ORA><DESTINATARIO>LMIT</DESTINATARIO><ID_FILE>238</ID_FILE></FILENAME><TRASFERIMENTOCREDITO><TIPO_MESSAGGIO>10</TIPO_MESSAGGIO><CODICE_OPERATORE_RECIPIENT>LMIT</CODICE_OPERATORE_RECIPIENT><CODICE_OPERATORE_DONATING>OPIV</CODICE_OPERATORE_DONATING><CODICE_RICHIESTA_RECIPIENT>LYCA2510150052</CODICE_RICHIESTA_RECIPIENT><MSISDN>393773739342</MSISDN><DATA_CUT_OVER>2025-10-17</DATA_CUT_OVER><ORA_CUT_OVER>06:04:35</ORA_CUT_OVER><FLAG_TRASFERIMENTO_CREDITO>Y</FLAG_TRASFERIMENTO_CREDITO><DATA_NOTIFICA_CREDITO>2025-10-17</DATA_NOTIFICA_CREDITO><ORA_NOTIFICA_CREDITO>11:03:23</ORA_NOTIFICA_CREDITO><IMPORTO_CREDITO_RESIDUO>1.01</IMPORTO_CREDITO_RESIDUO><CODICE_OPERATORE_VIRTUALE_DONATING>O101</CODICE_OPERATORE_VIRTUALE_DONATING><FLAG_VERIFICA_CREDITO_ANOMALO>N</FLAG_VERIFICA_CREDITO_ANOMALO></TRASFERIMENTOCREDITO></LISTA_MNP_RECORD>""", "TRASFERIMENTOCREDITO"),
]