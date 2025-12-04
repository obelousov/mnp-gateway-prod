from typing import Tuple, Optional, Dict
from porting.spain_nc import initiate_session
from services.soap_services import msisdn_status_check, parse_soap_response_list
from services.logger import logger, log_payload
from config import settings
import requests


def msisdn_status_check_nc(msisdn: str) -> Tuple[bool, Optional[str], Optional[Dict]]:
    """
    Check MSISDN status with National Central (NC)
    
    Args:
        msisdn: The phone number to check
        
    Returns:
        Tuple of (success: bool, error_message: Optional[str], response_data: Optional[Dict])
    """
    logger.debug("ENTER msisdn_status_check_NC with MSISDN: %s", msisdn)
    
    try:
        # Step 1: Get session code
        session_code = initiate_session()
        logger.debug("Session code obtained: %s", session_code)
        
        # Step 2: Create SOAP request
        soap_payload = msisdn_status_check(session_code, msisdn)
        logger.debug("Generated SOAP payload for MSISDN status check")
        logger.debug("MSISDN_CHECK->NC: %s\n", soap_payload)
        log_payload('NC', 'MSISDN_CHECK', 'REQUEST', str(soap_payload))
        
        # Step 3: Send request to NC
        logger.debug("Sending MSISDN status check request to NC")
        response = requests.post(
            settings.APIGEE_BOLETIN_URL,
            data=soap_payload,
            headers=settings.get_soap_headers('peticionConsultarNumeracionPortabilidadMovil'),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT
        )
        response.raise_for_status()
        
        # Step 4: Parse SOAP response with correct field names
        logger.debug("Received response from NC, parsing...")
        log_payload('NC', 'MSISDN_CHECK', 'RESPONSE', str(response.text))
        logger.debug("MSISDN_CHECK_RESPONSE<-NC:\n%s", str(response.text))
        
        # Parse the response based on actual SOAP structure
        field_names = [
            "codigoRespuesta", 
            "descripcion", 
            "MSISDN", 
            "codigoOperadorActual", 
            "codigoOperadorPropietarioRango",
            "involucradaProcesoPortabilidad", 
            "portada"
        ]

        parsed_tuple = parse_soap_response_list(response.text, field_names)
        parsed_dict = dict(zip(field_names, parsed_tuple))
        
        response_code = parsed_dict.get("codigoRespuesta")
        description = parsed_dict.get("descripcion")
        
        logger.debug("MSISDN status check response: code=%s, description=%s result=%s", response_code, description, parsed_dict)
        
        # Determine success based on response code
        success = (response_code == "0000 00000")
        
        # Prepare response data with English field names
        response_data = {
            'response_code': response_code,
            'description': description,
            'msisdn': parsed_dict.get('MSISDN'),
            'current_operator': parsed_dict.get('codigoOperadorActual'),
            'range_owner_operator': parsed_dict.get('codigoOperadorPropietarioRango'),
            'in_portability_process': parsed_dict.get('involucradaProcesoPortabilidad'),
            'ported': parsed_dict.get('portada')
        }
        
        return success, None, response_data
        
    except requests.exceptions.RequestException as e:
        error_msg = f"HTTP request error: {str(e)}"
        logger.error("Request error in msisdn_status_check_NC: %s", error_msg)
        return False, error_msg, None
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error("Error in msisdn_status_check_NC: %s", error_msg)
        return False, error_msg, None