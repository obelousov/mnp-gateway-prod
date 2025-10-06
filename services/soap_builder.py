from datetime import date, datetime
from templates.soap_templates import PORTABILITY_REQUEST_TEMPLATE

def format_date(value):
    """Format date for SOAP request"""
    if isinstance(value, (date, datetime)):
        return value.strftime('%Y-%m-%d')
    return str(value) if value is not None else ''

def build_portability_request(json_data):
    """Build SOAP request for portability"""
    # Handle optional fields
    optional_fields = {
        'fecha_ventana_optional': (
            f"<por:fechaVentanaCambio>{format_date(json_data['porting_window_date'])}</por:fechaVentanaCambio>"
            if json_data.get('porting_window_date') else ""
        ),
        'iccid_optional': (
            f"<por:ICCID>{json_data['iccid']}</por:ICCID>"
            if json_data.get('iccid') else ""
        )
    }
    
    return PORTABILITY_REQUEST_TEMPLATE.format(
        session_code=json_data.get('session_code', ''),
        request_date=format_date(json_data.get('request_date')),
        donor_operator=json_data.get('donor_operator', ''),
        recipient_operator=json_data.get('recipient_operator', ''),
        id_type=json_data.get('id_type', 'NIE'),
        id_number=json_data.get('id_number', ''),
        contract_code=json_data.get('contract_code', ''),
        nrn_receptor=json_data.get('nrn_receptor', ''),
        msisdn=json_data.get('msisdn', json_data.get('phone_number', '')),
        **optional_fields
    )

def test_build_portability_request():
    """Test function for build_portability_request"""
    
    # Test data with all fields
    test_data_full = {
        'session_code': '123SESSION123',
        'request_date': date(2025, 10, 5),
        'donor_operator': 'OPD',
        'recipient_operator': 'OPR',
        'id_type': 'NIE',
        'id_number': 'X1234567Z',
        'contract_code': 'CONTRACT001',
        'nrn_receptor': 'NRN123',
        'msisdn': '34612345678',
        'porting_window_date': date(2025, 10, 10),
        'iccid': '8931040012345678901'
    }

    print("=== TEST 1: Full Data ===")
    result1 = build_portability_request(test_data_full)
    print(result1)
    print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    test_build_portability_request()