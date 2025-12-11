from typing import Tuple, Optional, Dict
from porting.spain_nc import initiate_session
from services.soap_services import msisdn_status_check, parse_soap_response_list,create_status_check_soap,create_status_check_soap_nc
from services.logger import logger, log_payload
from config import settings
import requests
from services.soap_services import parse_soap_response_nested_multi


def portin_status_check_nc(msisdn: str, reference_code: str) -> Tuple[bool, Optional[str], Optional[Dict]]:
    """
    Check MSISDN portin status with National Central (NC)
    
    Args:
        msisdn: The phone number to check
        
    Returns:
        Tuple of (success: bool, error_message: Optional[str], response_data: Optional[Dict])
    """
    logger.debug("ENTER portin_status_check_NC with MSISDN: %s", msisdn)
    
    try:
        # Step 1: Get session code
        session_code = initiate_session()
        logger.debug("Session code obtained: %s", session_code)
        
        if session_code is None:
            error_msg = "Failed to obtain session code from NC"
            logger.error(error_msg)
            return False, error_msg, None
        
        # Step 2: Create SOAP request
        # soap_payload = msisdn_status_check(session_code, msisdn)
        mnp_request_id = 0  # Placeholder, replace with actual request ID if available
        # soap_payload = create_status_check_soap(mnp_request_id, session_code, msisdn)
        soap_payload = create_status_check_soap_nc(mnp_request_id, session_code, msisdn)
        logger.debug("Generated SOAP payload for MSISDN status check with refeernce_code: %s and MSISDM: %s ", reference_code, msisdn )
        log_payload('NC', 'MSISDN_CHECK', 'REQUEST', str(soap_payload))
        
        # Step 3: Send request to NC
        logger.debug("Sending MSISDN status check request to NC")
        response = requests.post(
            settings.APIGEE_PORTABILITY_URL,
            data=soap_payload,
            headers=settings.get_soap_headers('ConsultarProcesosPortabilidadMovil'),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT
        )
        response.raise_for_status()
        
        # Step 4: Parse SOAP response with correct field names
        logger.debug("Received response from NC, parsing...")
        # log_payload('NC', 'STATUS_CHECK', 'RESPONSE', str(response.text))
        # logger.debug("CHECK_RESPONSE<-NC:\n%s", str(response.text))
        
        fields = ["tipoProceso", "codigoRespuesta", "descripcion", "codigoReferencia", "estado","fechaVentanaCambio","fechaCreacion","causaRechazo","fechaRechazo"]

        result = parse_soap_response_nested_multi(str(response.text), fields, reference_code)

        if result is None:
            result = [None] * len(fields)

        # Create a dictionary using dict comprehension
        result_dict = {field: value for field, value in zip(fields, result)}
        logger.debug("ref_code: %s", reference_code)
        logger.debug("result_dict: %s", result_dict)
        

        process_type = result_dict.get("tipoProceso")
        response_code = result_dict.get("codigoRespuesta")
        description = result_dict.get("descripcion")
        reference_code_response = result_dict.get("codigoReferencia")
        estado = result_dict.get("estado")
        porting_date = result_dict.get("fechaVentanaCambio")
        creattion_date = result_dict.get("fechaCreacion")
        reject_reason = result_dict.get("causaRechazo")
        reject_date = result_dict.get("fechaRechazo")

        
        logger.debug("MSISDN status check response: code=%s, description=%s ", response_code, description)
        
        # Determine success based on response code
        success = (response_code == "0000 00000")
        
        # Prepare response data with English field names
        response_data = {
            'process_type': process_type,
            'response_code': response_code,
            'description': description,
            'reference_code': reference_code_response,
            'status': estado,
            'porting_date': porting_date,
            'creation_date': creattion_date,
            'reject_reason': reject_reason,
            'reject_date': reject_date,
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