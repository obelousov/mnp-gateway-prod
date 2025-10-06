from ast import List
from datetime import date
import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
from fastapi import HTTPException
from typing import Dict, List, Optional, Tuple
# from db_utils import get_db_connection
from services.database_service import get_db_connection
from templates.soap_templates import PORTABILITY_REQUEST_TEMPLATE, CHECK_PORT_IN_STATUS_TEMPLATE


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

def create_status_check_soap(mnp_request_id):
    """
    Get request data from DB based on mnp_request_idt
    """
    print ("received mnp_id:", mnp_request_id)

    # 1. Get database connection
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
        
    # 2. Fetch the request data from database
    cursor.execute("SELECT session_code, msisdn FROM portability_requests WHERE id = %s", (mnp_request_id,))
    mnp_request = cursor.fetchone()
        
    if not mnp_request:
         print(f"Request {mnp_request_id} not found in database")
         return

    # 3. Prepare your SOAP envelope (use your existing logic)
    print(f"Request {mnp_request} found, preparing SOAP payload...")


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
    return CHECK_PORT_IN_STATUS_TEMPLATE.format(
        session_code=mnp_request.get('session_code', ''),
        msisdn=mnp_request.get('msisdn')
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
