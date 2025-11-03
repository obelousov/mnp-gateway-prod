from ast import List
# from datetime import date
# import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
from fastapi import HTTPException
from typing import Dict, List, Optional, Tuple, Any
# from db_utils import get_db_connection
from services.database_service import get_db_connection
from templates.soap_templates import PORTABILITY_REQUEST_TEMPLATE, CHECK_PORT_IN_STATUS_TEMPLATE, CANCEL_PORT_IN_REQUEST_TEMPLATE,CONSULT_PROCESS_PORT_IN,INITIATE_SESSION, CANCEL_PORT_IN_REQUEST_TEMPLATE_ONLINE
# from config import logger
from services.logger import logger, payload_logger, log_payload
from datetime import date, datetime


# Namespace definitions
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
POR_NS = "http://nc.aopm.es/v1-10/portabilidad"
V1_NS = "http://nc.aopm.es/v1-10"

# soap_utils.py
def create_soap_payload(request_data):
    """Create SOAP payload from request data"""
    # Your existing SOAP creation logic here
    soap_envelope = f"""
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
        <soapenv:Body>
            <por:altaRequest>
                <msisdn>{request_data['phone_number']}</msisdn>
                <operator>{request_data['operator']}</operator>
            </por:altaRequest>
        </soapenv:Body>
    </soapenv:Envelope>
    """
    return soap_envelope

def parse_soap_response(response_text):
    """Parse SOAP response and extract relevant data"""
    # Your existing parsing logic here
    # Example: extract session_code and status from XML
    return "SESSION_123", "ASOL"

def parse_soap_request(xml_content: str) -> dict:
    """Parse SOAP request and extract parameters"""
    namespaces = {
        'soapenv': SOAP_NS,
        'por': POR_NS,
        'v1': V1_NS
    }
    
    try:
        root = ET.fromstring(xml_content)
        
        # Extract data from request
        data = {}
        
        # Find elements using namespaces
        elements_to_extract = [
            ('codigoSesion', './/v1:codigoSesion'),
            ('fechaSolicitudPorAbonado', './/por:fechaSolicitudPorAbonado'),
            ('codigoOperadorDonante', './/por:codigoOperadorDonante'),
            ('codigoOperadorReceptor', './/por:codigoOperadorReceptor'),
            ('codigoContrato', './/por:codigoContrato'),
            ('NRNReceptor', './/por:NRNReceptor'),
            ('fechaVentanaCambio', './/por:fechaVentanaCambio'),
            ('ICCID', './/por:ICCID'),
            ('MSISDN', './/por:MSISDN'),
        ]
        
        for field_name, xpath in elements_to_extract:
            element = root.find(xpath, namespaces)
            if element is not None and element.text:
                data[field_name] = element.text
        
        # Extract document identification
        doc_tipo = root.find('.//v1:tipo', namespaces)
        doc_numero = root.find('.//v1:documento', namespaces)
        if doc_tipo is not None and doc_numero is not None:
            data['documentoIdentificacion'] = {
                'tipo': doc_tipo.text,
                'documento': doc_numero.text
            }
        
        return data
        
    except ET.ParseError:
        return {}

def json_to_soap_request(json_data: dict) -> str:
    """Convert JSON data to SOAP request XML"""
    try:
        # Create envelope with namespace declarations
        envelope = ET.Element(f"{{{SOAP_NS}}}Envelope")
        envelope.set("xmlns:soapenv", SOAP_NS)
        envelope.set("xmlns:por", POR_NS)
        envelope.set("xmlns:v1", V1_NS)

        # Create header
        header = ET.SubElement(envelope, f"{{{SOAP_NS}}}Header")

        # Create body
        body = ET.SubElement(envelope, f"{{{SOAP_NS}}}Body")

        # Create main request element
        peticion = ET.SubElement(body, f"{{{POR_NS}}}peticionCrearSolicitudIndividualAltaPortabilidadMovil")

        # Add simple fields
        simple_fields = [
            ('codigoSesion', V1_NS),
            ('fechaSolicitudPorAbonado', POR_NS),
            ('codigoOperadorDonante', POR_NS),
            ('codigoOperadorReceptor', POR_NS),
            ('codigoContrato', POR_NS),
            ('NRNReceptor', POR_NS),
            ('fechaVentanaCambio', POR_NS),
            ('ICCID', POR_NS),
            ('MSISDN', POR_NS)
        ]

        for field_name, namespace in simple_fields:
            if field_name in json_data:
                elem = ET.SubElement(peticion, f"{{{namespace}}}{field_name}")
                elem.text = str(json_data[field_name])

        # Add abonado section
        if 'abonado' in json_data:
            abonado = ET.SubElement(peticion, f"{{{POR_NS}}}abonado")

            # Add documentoIdentificacion
            if 'documentoIdentificacion' in json_data['abonado']:
                doc_id = json_data['abonado']['documentoIdentificacion']
                doc_elem = ET.SubElement(abonado, f"{{{V1_NS}}}documentoIdentificacion")

                tipo_elem = ET.SubElement(doc_elem, f"{{{V1_NS}}}tipo")
                tipo_elem.text = doc_id.get('tipo', '')

                doc_num_elem = ET.SubElement(doc_elem, f"{{{V1_NS}}}documento")
                doc_num_elem.text = doc_id.get('documento', '')

            # Add empty datosPersonales element
            ET.SubElement(abonado, f"{{{V1_NS}}}datosPersonales")

        # Convert to XML string
        rough_string = ET.tostring(envelope, encoding='unicode', method='xml')

        # Pretty print the XML
        parsed = minidom.parseString(rough_string)
        return parsed.toprettyxml(indent="  ")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error converting JSON to SOAP: {str(e)}") from e

def json_from_db_to_soap_new(json_data):
    """
    Convert JSON data from new table structure to SOAP request
    """
    logger.debug("ENTER json_from_db_to_soap_new() %s", json_data)
    # print("Received JSON data:", json_data)
    
    # Format dates properly
    def format_date(value):
        if isinstance(value, (date, datetime)):
            return value.strftime('%Y-%m-%d')
        return str(value) if value is not None else ''
      
    # Handle optional fields
    fecha_ventana_optional = ""
    if json_data.get('desired_porting_date'):  # Changed from 'porting_window_date'
        fecha_ventana_optional = f"<por:fechaVentanaCambio>{format_date(json_data['desired_porting_date'])}</por:fechaVentanaCambio>"
    
    iccid_optional = ""
    if json_data.get('iccid'):
        iccid_optional = f"<por:ICCID>{json_data['iccid']}</por:ICCID>"
    
    # Extract document data from nested structure
    # subscriber_data = json_data.get('subscriber', {})
    # doc_data = subscriber_data.get('identification_document', {})
    
    return PORTABILITY_REQUEST_TEMPLATE.format(
        session_code=json_data.get('session_code', ''),
        request_date=format_date(json_data.get('requested_at')),  # Changed from 'request_date'
        donor_operator=json_data.get('donor_operator', ''),
        recipient_operator=json_data.get('recipient_operator', ''),
        document_type=json_data.get('document_type', 'NIE'),  # Changed from 'id_type'
        document_number=json_data.get('document_number', ''),  # Changed from 'id_number'
        contract_code=json_data.get('contract_number', ''),  # Changed from 'contract_code'
        nrn_receptor=json_data.get('routing_number', ''),  # Changed from 'nrn_receptor'
        fecha_ventana_optional=fecha_ventana_optional,
        iccid_optional=iccid_optional,
        msisdn=json_data.get('msisdn', '')  # Removed phone_number fallback
    )

def json_from_db_to_soap_new_1_old(json_data, session_code):
    """
    Convert JSON data from new table structure to SOAP request
    """
    # logger.debug("ENTER json_from_db_to_soap_new_1() %s", json_data)
    logger.debug("ENTER json_from_db_to_soap_new_1()")
    # print("Received JSON data:", json_data)
    
    # Format dates properly
    def format_date(value):
        if isinstance(value, (date, datetime)):
            return value.strftime('%Y-%m-%d')
        return str(value) if value is not None else ''
      
    # Handle optional fields
    fecha_ventana_optional = ""
    if json_data.get('desired_porting_date'):  # Changed from 'porting_window_date'
        fecha_ventana_optional = f"<por:fechaVentanaCambio>{format_date(json_data['desired_porting_date'])}</por:fechaVentanaCambio>"
    
    iccid_optional = ""
    if json_data.get('iccid'):
        iccid_optional = f"<por:ICCID>{json_data['iccid']}</por:ICCID>"
    
    # Extract document data from nested structure
    # subscriber_data = json_data.get('subscriber', {})
    # doc_data = subscriber_data.get('identification_document', {})
    
    return PORTABILITY_REQUEST_TEMPLATE.format(
        session_code=session_code,
        request_date=format_date(json_data.get('requested_at')),  # Changed from 'request_date'
        donor_operator=json_data.get('donor_operator', ''),
        recipient_operator=json_data.get('recipient_operator', ''),
        document_type=json_data.get('document_type', 'NIE'),  # Changed from 'id_type'
        document_number=json_data.get('document_number', ''),  # Changed from 'id_number'
        contract_code=json_data.get('contract_number', ''),  # Changed from 'contract_code'
        nrn_receptor=json_data.get('routing_number', ''),  # Changed from 'nrn_receptor'
        fecha_ventana_optional=fecha_ventana_optional,
        iccid_optional=iccid_optional,
        msisdn=json_data.get('msisdn', '')  # Removed phone_number fallback
    )

def json_from_db_to_soap_new_1(json_data, session_code):
    """
    Convert JSON data from new table structure to SOAP request
    """
    logger.debug("ENTER json_from_db_to_soap_new() %s", json_data)
    
    def format_date(value):
        if isinstance(value, (date, datetime)):
            return value.strftime('%Y-%m-%d')
        return str(value) if value is not None else ''
      
    # Handle optional fields
    fecha_ventana_optional = ""
    if json_data.get('desired_porting_date'):
        fecha_ventana_optional = f"<por:fechaVentanaCambio>{format_date(json_data['desired_porting_date'])}</por:fechaVentanaCambio>"
    
    iccid_optional = ""
    if json_data.get('iccid'):
        iccid_optional = f"<por:ICCID>{json_data['iccid']}</por:ICCID>"
    
    # Use the actual fields from your table with fallbacks
    first_name = json_data.get('first_name', 'Test')
    first_surname = json_data.get('first_surname', 'User')
    second_surname = json_data.get('second_surname', 'Second')
    nationality = json_data.get('nationality', 'ESP')
    
    # Debug output to verify data
    print("=== PERSONAL DATA FROM DATABASE ===")
    print(f"first_name: {first_name}")
    print(f"first_surname: {first_surname}")
    print(f"second_surname: {second_surname}")
    print(f"nationality: {nationality}")
    print("===================================")
    
    result = PORTABILITY_REQUEST_TEMPLATE.format(
        session_code=session_code,
        request_date=format_date(json_data.get('requested_at')),
        donor_operator=json_data.get('donor_operator', ''),
        recipient_operator=json_data.get('recipient_operator', ''),
        document_type=json_data.get('document_type', 'NIE'),
        document_number=json_data.get('document_number', ''),
        first_name=first_name,
        first_surname=first_surname,
        second_surname=second_surname,
        nationality=nationality,
        contract_code=json_data.get('contract_number', ''),
        nrn_receptor=json_data.get('routing_number', ''),
        fecha_ventana_optional=fecha_ventana_optional,
        iccid_optional=iccid_optional,
        msisdn=json_data.get('msisdn', '')
    )
    
    return result

def json_from_db_to_soap(json_data):
    """
    Version using string formatting for the SOAP request
    """
    print ("received JSON data:", json_data)
    # Format dates properly
    def format_date(value):
        if isinstance(value, (date, datetime)):
            return value.strftime('%Y-%m-%d')
        return str(value) if value is not None else ''
      
    # Handle optional fields
    fecha_ventana_optional = ""
    if json_data.get('porting_window_date'):
        fecha_ventana_optional = f"<por:fechaVentanaCambio>{format_date(json_data['porting_window_date'])}</por:fechaVentanaCambio>"
    
    iccid_optional = ""
    if json_data.get('iccid'):
        iccid_optional = f"<por:ICCID>{json_data['iccid']}</por:ICCID>"
    
    # return soap_template.format(
    return PORTABILITY_REQUEST_TEMPLATE.format(
        session_code=json_data.get('session_code', ''),
        request_date=format_date(json_data.get('request_date')),
        donor_operator=json_data.get('donor_operator', ''),
        recipient_operator=json_data.get('recipient_operator', ''),
        id_type=json_data.get('id_type', 'NIE'),
        id_number=json_data.get('id_number', ''),
        contract_code=json_data.get('contract_code', ''),
        nrn_receptor=json_data.get('nrn_receptor', ''),
        fecha_ventana_optional=fecha_ventana_optional,
        iccid_optional=iccid_optional,
        msisdn=json_data.get('msisdn', json_data.get('phone_number', ''))
    )

# Field mapping explanation
FIELD_MAPPING = {
    'JSON Field': 'SOAP Element',
    'session_code': 'v1:codigoSesion',
    'request_date': 'por:fechaSolicitudPorAbonado', 
    'donor_operator': 'por:codigoOperadorDonante',
    'recipient_operator': 'por:codigoOperadorReceptor',
    'id_type': 'v1:tipo (inside documentoIdentificacion)',
    'id_number': 'v1:documento (inside documentoIdentificacion)',
    'contract_code': 'por:codigoContrato',
    'nrn_receptor': 'por:NRNReceptor',
    'porting_window_date': 'por:fechaVentanaCambio (optional)',
    'iccid': 'por:ICCID (optional)',
    'msisdn': 'por:MSISDN'
}

# def create_status_check_soap(mnp_request_id, session_code, msisdn):
def create_status_check_soap(mnp_request_id: int, reference_code: str, msisdn: str) -> str:
    """
    Get request data from DB based on mnp_request_idt
    """
    # print ("received mnp_id:", mnp_request_id, session_code, msisdn)
    logger.info("received mnp_id: %s, reference_code: %s, msisdn: %s", mnp_request_id, reference_code, msisdn)

    # 1. Get database connection
    # connection = get_db_connection()
    # cursor = connection.cursor(dictionary=True)
        
    # # 2. Fetch the request data from database
    # cursor.execute("SELECT session_code, msisdn FROM portability_requests WHERE id = %s", (mnp_request_id,))
    # mnp_request = cursor.fetchone()
        
    # if not mnp_request:
    #      print(f"Request {mnp_request_id} not found in database")
    #      return

    # # 3. Prepare your SOAP envelope (use your existing logic)
    # print(f"Request {mnp_request} found, preparing SOAP payload...")


    soap_template = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:por="http://nc.aopm.es/v1-10/portabilidad" xmlns:v1="http://nc.aopm.es/v1-10">
   <soapenv:Header/>
   <soapenv:Body>
      <por:peticionConsultarProcesosPortabilidadMovil>
         <v1:codigoSesion>{session_code}</v1:codigoSesion>
         <por:MSISDN>{msisdn}</por:MSISDN>
      </por:peticionConsultarProcesosPortabilidadMovil>
   </soapenv:Body>
  </soapenv:Envelope>
    """

    # return soap_template.format(
    #     session_code=mnp_request.get('session_code', ''),
    #     msisdn=mnp_request.get('msisdn')
    # )
    # return CHECK_PORT_IN_STATUS_TEMPLATE.format(
    #     session_code=mnp_request.get('session_code', ''),
    #     msisdn=mnp_request.get('msisdn')
    # )

    # return CHECK_PORT_IN_STATUS_TEMPLATE.format(
    #     session_code,msisdn
    # )
    return CHECK_PORT_IN_STATUS_TEMPLATE.format(
        reference_code=reference_code,
        msisdn=msisdn
    )


def parse_soap_response_list(soap_xml: str, requested_fields: List[str]) -> Tuple[Optional[str], ...]:
    """
    Parse SOAP response and return values as tuple for easy unpacking
    
    Args:
        soap_xml: SOAP response XML string
        requested_fields: List of field names to extract from the XML
        
    Returns:
        Tuple with extracted field values in the same order as requested_fields
    """
    result = []
    
    try:
        root = ET.fromstring(soap_xml)
        
        # Define namespace mappings for different fields
        namespace_mapping = {
            'codigoRespuesta': './/{http://nc.aopm.es/v1-10}codigoRespuesta',
            'descripcion': './/{http://nc.aopm.es/v1-10}descripcion',
            'codigoReferencia': './/{http://nc.aopm.es/v1-10/portabilidad}codigoReferencia',
            'fechaVentanaCambio': './/{http://nc.aopm.es/v1-10/portabilidad}fechaVentanaCambio',
            'codigoSesion': './/{http://nc.aopm.es/v1-10}codigoSesion',
            'campoErroneo': './/{http://nc.aopm.es/v1-10}campoErroneo',
            'nombre': './/{http://nc.aopm.es/v1-10}nombre'
        }
        
        for field in requested_fields:
            if field in namespace_mapping:
                # Use predefined XPath with namespace
                value = root.findtext(namespace_mapping[field])
            else:
                # Try generic search
                value = root.findtext(f'.//{field}')
            result.append(value)
        
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        # Fill with None values for all requested fields
        result = [None] * len(requested_fields)
    except Exception as e:
        print(f"Unexpected error: {e}")
        # Fill with None values for all requested fields
        result = [None] * len(requested_fields)
    
    # Ensure we always return the correct number of values
    if len(result) != len(requested_fields):
        result = [None] * len(requested_fields)
    
    return tuple(result)

def parse_soap_response_nested(soap_xml: str, requested_fields: List[str]) -> Tuple[Optional[str], ...]:
    """
    Parse SOAP response and return values as tuple for easy unpacking.
    Supports nested fields using '/' syntax.
    
    Args:
        soap_xml: SOAP response XML string.
        requested_fields: List of field names or paths to extract.
                         Use '/' for nested fields: 'campoErroneo/nombre'
    
    Returns:
        Tuple with extracted field values in the same order as requested_fields.
    """
    result: List[Optional[str]] = []

    try:
        root = ET.fromstring(soap_xml)
        
        for field_path in requested_fields:
            value = None
            
            if '/' in field_path:
                # Handle nested fields: 'parent/child'
                parts = field_path.split('/')
                current_element = root
                
                # Navigate through the path
                for part in parts:
                    current_element = current_element.find(f'.//{{*}}{part}')
                    if current_element is None:
                        break
                
                value = current_element.text if current_element is not None else None
            else:
                # Handle simple fields (original behavior)
                value = root.findtext(f'.//{{*}}{field_path}')
            
            # Handle empty strings and whitespace
            if value is not None:
                value = value.strip() if value.strip() else None
                
            result.append(value)

    except Exception as e:
        print(f"Error parsing SOAP XML: {e}")
        result = [None] * len(requested_fields)

    if len(result) != len(requested_fields):
        result = [None] * len(requested_fields)

    return tuple(result)


def json_from_db_to_soap_cancel(json_data):
    """
    Convert JSON data from new table structure to SOAP request
    """
    logger.debug("ENTER json_from_db_to_soap_cancel() %s", json_data)
    # print("Received JSON data:", json_data)
     
    return CANCEL_PORT_IN_REQUEST_TEMPLATE.format(
        session_code=json_data.get('session_code', ''),
        reference_code=json_data.get('reference_code'),
        cancellation_reason=json_data.get('cancellation_reason', ''),
        cancellation_initiated_by_donor=json_data.get('cancellation_initiated_by_donor', '')
    )

def json_from_db_to_soap_cancel_online(json_data,session_code):
    """
    Convert JSON data from new table structure to SOAP request
    """
    # logger.debug("ENTER json_from_db_to_soap_cancel_online() %s session_code %s", json_data, session_code)
    logger.debug("ENTER json_from_db_to_soap_cancel_online() session_code %s", session_code)
    # print("Received JSON data:", json_data)
     
    return CANCEL_PORT_IN_REQUEST_TEMPLATE_ONLINE.format(
        session_code=session_code,
        reference_code=json_data.get('reference_code'),
        cancellation_reason=json_data.get('cancellation_reason', ''),
        cancellation_initiated_by_donor=json_data.get('cancellation_initiated_by_donor', '')
    )

def create_status_check_soap_nc(mnp_request_id: int, session_code: str, msisdn: str) -> str:
    """
    Get request data from DB based on mnp_request_idt
    """
    # print ("received mnp_id:", mnp_request_id, session_code, msisdn)
    logger.info("received mnp_id: %s, session_code: %s, msisdn: %s", mnp_request_id, session_code, msisdn)

    
    return CONSULT_PROCESS_PORT_IN.format(
        session_code=session_code,
        msisdn=msisdn
    )

def create_initiate_soap(username, access_code,operator_code) -> str:
    """
    Get request data from DB based on mnp_request_idt
    """
    # print ("received mnp_id:", mnp_request_id, session_code, msisdn)
    # logger.info("received username: %s, access_code: %s, operator_code: %s", username, access_code,operator_code)

    
    return INITIATE_SESSION.format(
        username=username,
        access_code=access_code,
        operator_code=operator_code
    )

def parse_soap_response_new(soap_string: str, fields: List[str]) -> List[Any]:
    """
    Parse SOAP response and extract requested fields.
    
    Args:
        soap_string: SOAP XML response as string
        fields: List of field names to extract
        
    Returns:
        List of extracted values in the same order as fields
        
    Example:
        response_code, description, session_code = parse_soap_response(
            response.text, 
            ["codigoRespuesta", "descripcion", "codigoSesion"]
        )
    """
    try:
        # Parse the XML
        root = ET.fromstring(soap_string)
        
        # Register namespaces to handle them properly
        namespaces = {
            'S': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns2': 'http://nc.aopm.es/v1-10',
            'ns5': 'http://nc.aopm.es/v1-10/acceso'
        }
        
        # Find the SOAP Body
        body = root.find('.//S:Body', namespaces)
        if body is None:
            raise ValueError("SOAP Body not found")
        
        # Extract values for each requested field
        results = []
        for field in fields:
            value = None
            
            # Try different namespace prefixes for the field
            # Some fields are in ns2, some in ns5, etc.
            for ns_prefix in ['ns2', 'ns5', 'ns3', 'ns4', 'ns6', 'ns7', 'ns8', 'ns9', 'ns10']:
                if ns_prefix in namespaces:
                    element = body.find(f'.//{ns_prefix}:{field}', namespaces)
                    if element is not None and element.text:
                        value = element.text.strip()
                        break
            
            # If not found with namespace, try without namespace
            if value is None:
                element = body.find(f'.//{field}')
                if element is not None and element.text:
                    value = element.text.strip()
            
            results.append(value)
        
        return results
        
    except ET.ParseError as e:
        raise ValueError(f"Failed to parse XML: {e}")
    except Exception as e:
        raise ValueError(f"Error parsing SOAP response: {e}")


# from typing import Dict, Optional

def parse_soap_response_dict_flat(soap_string: str, fields: List[str]) -> Dict[str, Optional[str]]:
    """
    Parse SOAP response and return dictionary of requested fields.
    Always returns a dictionary with all requested fields (values may be None or str).
    """
    # Initialize with None values but specify the type can be str or None
    result_dict: Dict[str, Optional[str]] = {field: None for field in fields}
    
    try:
        root = ET.fromstring(soap_string)
        namespaces = {
            'S': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns2': 'http://nc.aopm.es/v1-10',
            'ns5': 'http://nc.aopm.es/v1-10/acceso'
        }
        
        body = root.find('.//S:Body', namespaces)
        if body is None:
            return result_dict
        
        for field in fields:
            value = None
            for ns_prefix in ['ns2', 'ns5', 'ns3', 'ns4', 'ns6', 'ns7', 'ns8', 'ns9', 'ns10']:
                if ns_prefix in namespaces:
                    element = body.find(f'.//{ns_prefix}:{field}', namespaces)
                    if element is not None and element.text:
                        value = element.text.strip()
                        break
            
            if value is None:
                element = body.find(f'.//{field}')
                if element is not None and element.text:
                    value = element.text.strip()
            
            result_dict[field] = value  # This should now work
        
        return result_dict
        
    except Exception as e:
        logger.error("Error parsing SOAP response: %s",{e})
        return result_dict
    
from typing import Dict, Optional, List, Union
def parse_soap_response_dict(soap_string: str, fields: List[str]) -> Dict[str, Optional[str]]:
    """
    Simple SOAP parser that handles namespaces dynamically.
    """
    result_dict: Dict[str, Optional[str]] = {field: None for field in fields}
    
    try:
        root = ET.fromstring(soap_string)
        
        # Define the namespaces we know about from your XML
        namespaces = {
            'S': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns2': 'http://nc.aopm.es/v1-10',
            'ns14': 'http://nc.aopm.es/v1-10/portabilidad'
        }
        
        body = root.find('.//S:Body', namespaces)
        if body is None:
            return result_dict
        
        # Extract main fields
        for field in fields:
            if field == "codigoRespuesta":
                element = body.find('.//ns2:codigoRespuesta', namespaces)
                if element is not None and element.text:
                    result_dict[field] = element.text.strip()
            
            elif field == "descripcion":
                element = body.find('.//ns2:descripcion', namespaces)
                if element is not None and element.text:
                    result_dict[field] = element.text.strip()
            
            elif field == "error_field":
                # Get campoErroneo/nombre
                element = body.find('.//ns2:campoErroneo/ns2:nombre', namespaces)
                if element is not None and element.text:
                    result_dict[field] = element.text.strip()
            
            elif field == "error_description":
                # Get campoErroneo/descripcion
                element = body.find('.//ns2:campoErroneo/ns2:descripcion', namespaces)
                if element is not None and element.text:
                    result_dict[field] = element.text.strip()
        
        return result_dict
        
    except Exception as e:
        logger.error(f"Error parsing SOAP response: {e}")
        return result_dict
    
def json_from_db_to_soap_online(json_data, session_code):
    """
    Convert JSON data from new table structure to SOAP request
    """
    # logger.debug("ENTER json_from_db_to_soap_new() %s", json_data)
    logger.debug("ENTER json_from_db_to_soap_new()")
    
    def format_date(value):
        if isinstance(value, (date, datetime)):
            return value.strftime('%Y-%m-%d')
        return str(value) if value is not None else ''
      
    # Handle optional fields
    fecha_ventana_optional = ""
    if json_data.get('desired_porting_date'):
        fecha_ventana_optional = f"<por:fechaVentanaCambio>{format_date(json_data['desired_porting_date'])}</por:fechaVentanaCambio>"
    
    iccid_optional = ""
    if json_data.get('iccid'):
        iccid_optional = f"<por:ICCID>{json_data['iccid']}</por:ICCID>"
    
    # Use the actual fields from your table with fallbacks
    first_name = json_data.get('first_name', 'Test')
    first_surname = json_data.get('first_surname', 'User')
    second_surname = json_data.get('second_surname', 'Second')
    nationality = json_data.get('nationality', 'ESP')
    
    # Debug output to verify data
    # print("=== PERSONAL DATA FROM DATABASE ===")
    # print(f"first_name: {first_name}")
    # print(f"first_surname: {first_surname}")
    # print(f"second_surname: {second_surname}")
    # print(f"nationality: {nationality}")
    # print("===================================")
    
    result = PORTABILITY_REQUEST_TEMPLATE.format(
        session_code=session_code,
        request_date=format_date(json_data.get('requested_at')),
        donor_operator=json_data.get('donor_operator', ''),
        recipient_operator=json_data.get('recipient_operator', ''),
        document_type=json_data.get('document_type', 'NIE'),
        document_number=json_data.get('document_number', ''),
        first_name=first_name,
        first_surname=first_surname,
        second_surname=second_surname,
        nationality=nationality,
        contract_code=json_data.get('contract_number', ''),
        nrn_receptor=json_data.get('routing_number', ''),
        fecha_ventana_optional=fecha_ventana_optional,
        iccid_optional=iccid_optional,
        msisdn=json_data.get('msisdn', '')
    )
    
    return result

def parse_soap_response_nested_multi(xml, fields, reference_code):
    """
    Parse SOAP XML and return a list of requested field values
    for the given reference_code, or None if not found.
    Works without needing lxml.
    """
    try:
        xml = xml.strip()
        root = ET.fromstring(xml)
    except Exception as e:
        print(f"Error parsing SOAP XML: {e}")
        return None

    # Find all <registro> elements, ignoring namespaces
    registros = [el for el in root.iter() if el.tag.endswith('registro')]
    if not registros:
        print("No registro elements found in XML")
        return None

    # Extract global fields (codigoRespuesta, descripcion)
    codigo_respuesta = None
    descripcion = None
    for el in root.iter():
        if el.tag.endswith('codigoRespuesta'):
            codigo_respuesta = el.text.strip() if el.text else None
        elif el.tag.endswith('descripcion'):
            descripcion = el.text.strip() if el.text else None

    for reg in registros:
        # Match reference code
        ref_el = next((el for el in reg if el.tag.endswith('codigoReferencia')), None)
        ref_value = ref_el.text.strip() if ref_el is not None and ref_el.text else None

        if ref_value == str(reference_code).strip():
            record = []
            for field in fields:
                if field == "codigoRespuesta":
                    record.append(codigo_respuesta)
                elif field == "descripcion":
                    record.append(descripcion)
                else:
                    field_el = next((el for el in reg.iter() if el.tag.endswith(field)), None)
                    record.append(field_el.text.strip() if field_el is not None and field_el.text else None)
            return record

    # Not found
    return None

from templates.soap_templates import CHECK_PORT_OUT_STATUS_TEMPLATE
def create_status_check_port_out_soap_nc(session_code: str, operator_code: str, page_count: str) -> str:
    """
    Create SOAP for Port-Out status check
    obtenerNotificacionesAltaPortabilidadMovilComoDonantePendientesConfirmarRechazar
    """
    # print ("received mnp_id:", mnp_request_id, session_code, msisdn)
    logger.debug("create_status_check_port_out_soap_nc with session_code: %s", session_code)

    
    return CHECK_PORT_OUT_STATUS_TEMPLATE.format(
        session_code=session_code,
        operator_code=operator_code,
        page_count=page_count
    )
import xml.etree.ElementTree as ET
def parse_portout_response(xml_string: str):
    """
    Parse SOAP XML with Port-Out notifications and return
    a structured English-translated response.
    """
    # 🧹 Clean XML (remove leading BOM/newlines)
    xml_string = xml_string.lstrip().replace('\ufeff', '')

    # Parse XML safely
    root = ET.fromstring(xml_string)

    # Namespace-agnostic search
    get_text = lambda element, tag: element.findtext(f".//{{*}}{tag}")

    # --- Extract response-level fields ---
    response_info = {
        "response_code": get_text(root, "codigoRespuesta"),
        "response_description": get_text(root, "descripcion"),
        "paged_request_code": get_text(root, "codigoPeticionPaginada"),
        "total_records": get_text(root, "totalRegistros"),
        "is_last_page": get_text(root, "ultimaPagina")
    }

    # --- Extract all Port-Out requests ---
    notifications = root.findall(".//{*}notificacion")
    requests = []

    for notif in notifications:
        solicitud = notif.find(".//{*}solicitud")
        if solicitud is None:
            continue

        # Helper to extract text within solicitud; bind solicitud into default arg to avoid late-binding capture
        get = lambda tag, _sol=solicitud: _sol.findtext(f".//{{*}}{tag}")

        # --- Build translated dictionary for each request ---
        request_data = {
            "notification_id": get_text(notif, "codigoNotificacion"),
            "creation_date": get_text(notif, "fechaCreacion"),
            "synchronized": get_text(notif, "sincronizada"),

            "reference_code": get("codigoReferencia"),
            "status": get("estado"),
            "state_date": get("fechaEstado"),
            "creation_date_request": get("fechaCreacion"),
            "reading_mark_date": get("fechaMarcaLectura"),
            "state_change_deadline": get("fechaLimiteCambioEstado"),
            "subscriber_request_date": get("fechaSolicitudPorAbonado"),
            "donor_operator_code": get("codigoOperadorDonante"),
            "receiver_operator_code": get("codigoOperadorReceptor"),
            "extraordinary_donor_activation": get("operadorDonanteAltaExtraordinaria"),
            "contract_code": get("codigoContrato"),
            "receiver_NRN": get("NRNReceptor"),
            "port_window_date": get("fechaVentanaCambio"),
            "port_window_by_subscriber": get("fechaVentanaCambioPorAbonado"),
            "MSISDN": get("MSISDN"),

            # --- Abonado (Subscriber) details ---
            "subscriber": {
                "id_type": solicitud.findtext(".//{*}documentoIdentificacion/{*}tipo"),
                "id_number": solicitud.findtext(".//{*}documentoIdentificacion/{*}documento"),
                "first_name": solicitud.findtext(".//{*}datosPersonales/{*}nombre"),
                "last_name_1": solicitud.findtext(".//{*}datosPersonales/{*}primerApellido"),
                "last_name_2": solicitud.findtext(".//{*}datosPersonales/{*}segundoApellido"),
            }
        }

        requests.append(request_data)

    # --- Combine everything into a clean English JSON-like dict ---
    parsed_result = {
        "response_info": response_info,
        "requests": requests
    }

    return parsed_result
