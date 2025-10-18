from typing import List, Optional, Dict, Tuple
from mysql.connector import Error
import mysql.connector
from celery_app import app
import requests
import os
from xml.etree import ElementTree as ET
import re

from services.soap_services import parse_soap_response_list, create_status_check_soap_nc, create_initiate_soap, parse_soap_response_dict, parse_soap_response_dict_flat, json_from_db_to_soap_new, json_from_db_to_soap_new_1, json_from_db_to_soap_online
from services.time_services import calculate_countdown
from datetime import datetime, timedelta
from services.database_service import get_db_connection
from config import settings
from services.time_services import calculate_countdown_working_hours
from services.logger import logger, payload_logger, log_payload

def initiate_session():
    """
    Task to initiate a session with the Central Node API.
    """
    username = settings.APIGEE_USERNAME
    access_code = settings.APIGEE_ACCESS_CODE
    operator_code = settings.APIGEE_OPERATOR_CODE
    APIGEE_ACCESS_URL = settings.APIGEE_ACCESS_URL

    logger.info("ENTER initiate_session()")
    try:
        # Create SOAP payload for session initiation
        consultar_payload = create_initiate_soap(username, access_code, operator_code)
        
        # Conditional payload logging
        log_payload('NC', 'INITIATE_SESSION', 'REQUEST', str(consultar_payload))
        headers=settings.get_soap_headers('IniciarSesion'),
        print("Initiate session headers:", headers)

        if not APIGEE_ACCESS_URL:
            raise ValueError("WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS environment variable is not set.")
        
        response = requests.post(APIGEE_ACCESS_URL, 
                               data=consultar_payload,
                               headers=settings.get_soap_headers('IniciarSesion'),
                               timeout=settings.APIGEE_API_QUERY_TIMEOUT)
        log_payload('NC', 'INITIATE_SESSION', 'RESPONCE', str(response.text))

        result_dict = parse_soap_response_dict_flat(response.text, ["codigoRespuesta", "descripcion", "codigoSesion"])
    
        # Now these assignments are type-safe
        response_code = result_dict["codigoRespuesta"]
        description = result_dict["descripcion"]
        session_code = result_dict["codigoSesion"]

        # Check for success
        if response_code == "0000 00000" and session_code:
            return session_code
        else:
            logger.error("Failed to initiate session: %s %s",{response_code},{description})
            return None
        
    except Exception as e:
        logger.error("Error initiating session: %s", {str(e)})
        raise

# def submit_to_central_node_online(mnp_request_id):
def submit_to_central_node_online(mnp_request_id) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """
    Task to submit a porting request to the Central Node.
    This function runs synchronously.
    
    Returns:
        Tuple of (success, response_code, description, reference_code)
    """
    logger.debug("ENTER submit_to_central_node_online with req_id %s", mnp_request_id)
    APIGEE_PORTABILITY_URL = settings.APIGEE_PORTABILITY_URL
    
    # Initialize variables with default values
    success = False
    response_code = None
    description = None
    reference_code = None
    connection = None
    cursor = None
    
    try:
        # 1. Get database connection just to fetch request data
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # 2. Fetch the request data from database
        cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        mnp_request = cursor.fetchone()
        
        if not mnp_request:
            logger.error("Submit to NC: request %s not found", mnp_request_id)
            return False, "NOT_FOUND", f"Request {mnp_request_id} not found", None

        session_code = initiate_session()
        print(f"Submit to NC: Processing request - Status: {mnp_request.get('status_nc')}, Response: {mnp_request.get('response_status')}")

        # 3. Generate SOAP payload
        soap_payload = json_from_db_to_soap_online(mnp_request, session_code)
        
        logger.debug("Submit to NC: Generated SOAP Request:")
        log_payload('NC', 'PORT_IN', 'REQUEST', str(soap_payload))

        # 4. Try to send the request to Central Node
        if not APIGEE_PORTABILITY_URL:
            raise ValueError("APIGEE_PORTABILITY_URL environment variable is not set.")

        response = requests.post(
            APIGEE_PORTABILITY_URL, 
            data=soap_payload,
            headers=settings.get_soap_headers('CrearSolicitudIndividualAltaPortabilidadMovil'),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT
        )
        response.raise_for_status()

        # 5. Parse the SOAP response
        # print("Parsing SOAP response...", response.text)
        log_payload('NC', 'PORT_IN', 'RESPONSE', str(response.text))
        
        result = parse_soap_response_list(response.text, ["codigoRespuesta", "descripcion", "codigoReferencia"])
        if result and len(result) == 3:
            response_code, description, reference_code = result
        else:
            # Handle the case where parsing failed
            response_code, description, reference_code = None, None, None
            if result:
                logger.warning("Failed to parse SOAP response properly for request %s. Expected 3 values, got %s", 
                            mnp_request_id, len(result))
            else:
                logger.warning("Failed to parse SOAP response properly for request %s. Result is None", mnp_request_id)

        # Determine success based on response code
        if response_code == "0000 00000":  # Adjust this condition based on your actual success codes
            status_nc = 'SUBMITTED'
            status_bss = 'PROCESSING'
            logger.info("Success response from NC id %s response_code %s", mnp_request_id, response_code)
            success = True
        else:
            status_nc = 'ERROR RESPONSE'
            status_bss = 'REJECT_FROM_NC_SUBMITTED'
            success = False
            logger.error("Error response from NC id %s response_code %s", mnp_request_id, response_code)

        # 6. Update database with response
        update_query = """
            UPDATE portability_requests 
            SET status_nc = %s, session_code_nc = %s, status_bss = %s, response_code = %s, description = %s, updated_at = NOW() 
            WHERE id = %s
        """        
        cursor.execute(update_query, (status_nc,session_code, status_bss, response_code, description, mnp_request_id))
        connection.commit()

        return success, response_code, description, reference_code

    except requests.exceptions.RequestException as e:
        logger.error("HTTP error submitting to Central Node: %s", e)  # Remove curly braces
        error_msg = f"HTTP Error: {str(e)}"
        if hasattr(e, 'response') and e.response is not None:
            log_payload('NC', 'PORT_IN', 'RESPONSE', str(e.response.text))
            error_msg += f" - Status: {e.response.status_code}"
        
        # Update database with error
        if cursor and connection:
            try:
                update_query = """
                    UPDATE portability_requests 
                    SET status_nc = %s, description = %s, updated_at = NOW() 
                    WHERE id = %s
                """
                cursor.execute(update_query, ('ERROR', error_msg, mnp_request_id))
                connection.commit()
            except Exception as db_error:
                logger.error("Failed to update database with error: %s ",db_error)
        
        return False, "HTTP_ERROR", error_msg, None
    
    except Exception as e:
        logger.error("Unexpected error in submit_to_central_node: %s",e)
        error_msg = f"Unexpected Error: {str(e)}"
        
        # Update database with error
        if cursor and connection:
            try:
                update_query = """
                    UPDATE portability_requests 
                    SET status_nc = %s, description = %s, updated_at = NOW() 
                    WHERE id = %s
                """
                cursor.execute(update_query, ('ERROR', error_msg, mnp_request_id))
                connection.commit()
            except Exception as db_error:
                logger.error("Failed to update database with error: %s",db_error)
        
        return False, "UNKNOWN_ERROR", error_msg, None
        
    finally:
        # Clean up database connection
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()