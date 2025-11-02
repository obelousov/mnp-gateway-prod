from typing import List, Optional, Dict, Tuple
from mysql.connector import Error
import mysql.connector
from celery_app import app
import requests
import os
from xml.etree import ElementTree as ET
import re

from services.soap_services import parse_soap_response_list, create_status_check_soap_nc, create_initiate_soap, parse_soap_response_dict, parse_soap_response_dict_flat, json_from_db_to_soap_online, json_from_db_to_soap_cancel_online
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
        # log_payload('NC', 'INITIATE_SESSION', 'REQUEST', str(consultar_payload))
        headers=settings.get_soap_headers('IniciarSesion'),
        # print("Initiate session headers:", headers)

        if not APIGEE_ACCESS_URL:
            raise ValueError("WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS environment variable is not set.")
        
        response = requests.post(APIGEE_ACCESS_URL, 
                               data=consultar_payload,
                               headers=settings.get_soap_headers('IniciarSesion'),
                               timeout=settings.APIGEE_API_QUERY_TIMEOUT)
        # log_payload('NC', 'INITIATE_SESSION', 'RESPONCE', str(response.text))

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
        logger.debug("PORT_IN_REQUEST->NC:\n%s", str(soap_payload))
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
        logger.debug("PORT_IN_RESPONSE<-NC:\n%s", str(response.text))
        
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
            logger.info("Success response from NC id %s response_code %s, description %s reference_code %s", mnp_request_id, response_code, description, reference_code)
            success = True
        else:
            status_nc = 'PORT_IN_REJECTED'
            status_bss = 'REJECT_FROM_NC_SUBMITTED_TO_BSS'
            success = False
            logger.error("Error response from NC id %s response_code %s description %s", mnp_request_id, response_code, description)

        # 6. Update database with response
        update_query = """
            UPDATE portability_requests 
            SET status_nc = %s, session_code_nc = %s, status_bss = %s, response_code = %s, description = %s, reference_code = %s, updated_at = NOW() 
            WHERE id = %s
        """        
        cursor.execute(update_query, (status_nc,session_code, status_bss, response_code, description, reference_code,mnp_request_id))
        connection.commit()

        return success, response_code, description, reference_code

    except requests.exceptions.RequestException as e:
        logger.error("HTTP error submitting to Central Node: %s", e)  # Remove curly braces
        error_msg = f"HTTP Error: {str(e)}"
        if hasattr(e, 'response') and e.response is not None:
            log_payload('NC', 'PORT_IN', 'RESPONSE', str(e.response.text))
            logger.debug("PORT_IN_RESPONSE<-NC (Error):\n%s", str(e.response.text))
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

def submit_to_central_node_cancel_online(mnp_request_id):
    """
    Task to submit a cancel request to the Central Node.
    """
    logger.debug("ENTER submit_to_central_node_cancel_online with req_id %s", mnp_request_id)
    connection = None
    APIGEE_PORTABILITY_URL = settings.APIGEE_PORTABILITY_URL
    try:
        # 1. Get database connection
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # 2. Fetch the request data from database
        # cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        # current_time = datetime.now(container_tz)
        # print(f"Send Cancel to NC: current time {current_time}")
        # cursor.execute("SELECT * FROM portability_requests WHERE id = %s AND %s > scheduled_at",(mnp_request_id, current_time))
        # cursor.execute("SELECT * FROM portability_requests WHERE id = %s ",(mnp_request_id))
        cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        # cursor.execute("SELECT * FROM portability_requests WHERE id = %s AND NOW() > scheduled_at",(mnp_request_id,))
        mnp_request = cursor.fetchone()
        status_nc_old = mnp_request['status_nc'] if mnp_request else 'NOT_FOUND'
        
        if not mnp_request:
            print(f"Cancel submit to NC: request {mnp_request_id} not found or not yet scheduled")
            return f"Cancel submit to NC: request {mnp_request_id} not found or not yet scheduled"

        response_status = mnp_request['response_status']

        if response_status in ['ASOL', 'ACON', 'AREC', 'APOR', 'ACAN']:
            return f"Request {mnp_request_id} is in status {response_status}, no further submission needed"

        # 3. Prepare SOAP envelope (use your existing logic)
        session_code = initiate_session()
        logger.debug("Session_code: %s", session_code)
        print(f"Submit to NC: Processing request - Status: {mnp_request.get('status_nc')}, Response: {mnp_request.get('response_status')}")

        # Convert JSON to SOAP request
        # soap_request = json_to_soap_request(mnp_request)
        logger.debug("Cancel submit to NC: Generated SOAP Request:")
        # soap_payload = json_from_db_to_soap(mnp_request)  # function to create SOAP
        # soap_payload = json_from_db_to_soap_new(mnp_request)  # function to create SOAP
        soap_payload = json_from_db_to_soap_cancel_online(mnp_request,session_code)
        # print(soap_payload)
        # Conditional payload logging
        logger.debug("CANCEL_REQUEST->NC: %s\n", soap_payload)
        log_payload('NC', 'CANCEL', 'REQUEST', str(soap_payload))

        # 4. Try to send the request to Central Node
        # if not WSDL_SERVICE_SPAIN_MOCK_CANCEL:
        #     raise ValueError("WSDL_SERVICE_SPAIN_MOCK_CANCEL environment variable is not set.")
        if not APIGEE_PORTABILITY_URL:
            raise ValueError("APIGEE_PORTABILITY_URL environment variable is not set.")

        # current_retry = self.request.retries
        # logger.info("Attempt %d for request %s", current_retry+1, mnp_request_id)
        # print(f"Cancel submit to NC: Attempt {current_retry+1} for request {mnp_request_id}")
        
        response = requests.post(APIGEE_PORTABILITY_URL,
                               data=soap_payload,
                               headers=settings.get_soap_headers('PeticionCancelarSolicitudAltaPortabilidadMovil'),
                               timeout=settings.APIGEE_API_QUERY_TIMEOUT)
        
        response.raise_for_status()

        # 5. Parse the SOAP response (use your existing logic)
        # session_code, status = parse_soap_response_list(response.text,)
        response_code, description, reference_code = parse_soap_response_list(response.text, ["codigoRespuesta", "descripcion", "codigoReferencia"])
        print(f"Cancel to NC: Received response: response_code={response_code}, description={description}, reference_code={reference_code}")

        # Conditional payload logging
        log_payload('NC', 'CANCEL', 'RESPONSE', str(response.text))
        logger.debug("CANCEL_RESPONSE<-NC:\n%s", str(response.text))
# Received response: 
# response_code=400, description=Campos obligatorios faltantes: fechaSolicitudPorAbonado, codigoOperadorDonante, 
# codigoOperadorReceptor, codigoContrato, NRNReceptor, MSISDN, reference_code=ERROR_MISSING_FIELDS

        response_code_upper = ""
        if not response_code or not response_code.strip():
            status_nc = "PENDING_NO_RESPONSE_CODE_RECEIVED"
        else:
            response_code_upper = response_code.strip().upper()
    
        # Handle 4xx client error responses (400, 401, 404, etc.)
        if response_code_upper.startswith('4'):
            status_nc = "REQUEST_FAILED"
        # Handle 5xx server error responses
        elif response_code_upper.startswith('5'):
            status_nc = "SERVER_ERROR"
        # Handle specific success codes
        elif response_code_upper == 'ASOL':
            status_nc = "PENDING_RESPONSE"
        elif response_code_upper == 'ACAN':
            status_nc = "CANCEL_CONFIRMED"
        else:
            status_nc = "PENDING_CONFIRMATION"
        
        logger.info("Cancel: status nc changed %s status_old %s", status_nc,status_nc_old)
        # Assign status based on response_code
        if status_nc in ["REQUEST_FAILED","SERVER_ERROR","CANCEL_CONFIRMED"]:
            pass  # keep as is
        else:
            if not response_code or not response_code.strip():
                status_nc = "PENDING_NO_RESPONSE_CODE_RECEIVED"
            else:
                response_code_upper = response_code.strip().upper()
                status_nc = "PENDING_RESPONSE" if response_code_upper == 'ASOL' else "PENDING_CONFIRMATION"

        # Check if status actually changed
        status_changed = (status_nc != status_nc_old)

        logger.info("Cancel: status_changed %s status_nc %s", status_changed, status_nc)
        if status_changed:
            if status_nc == "PENDING_RESPONSE":
            # Special handling for ASOL status - reschedule at the next timeband
                _, scheduled_at = calculate_countdown(with_jitter=True)
                logger.info("Status changed to PENDING_RESPONSE (ASOL), rescheduling for %s", scheduled_at)
        
                update_query = """
                    UPDATE portability_requests 
                    SET status_nc = %s, scheduled_at = %s, response_status = %s, reference_code = %s, description = %s, updated_at = NOW() 
                    WHERE id = %s
                    """
                cursor.execute(update_query, (status_nc, scheduled_at, response_code, reference_code, description, mnp_request_id))
                connection.commit()

            if status_nc in ["REQUEST_FAILED","SERVER_ERROR","CANCEL_CONFIRMED"]:
            # Special handling for ASOL status - reschedule at the next timeband
                _, scheduled_at = calculate_countdown(with_jitter=True)
                logger.info("Status changed to %s, not rescheduled anymore", status_nc)
        
                update_query = """
                    UPDATE portability_requests 
                    SET status_nc = %s, response_status = %s, reference_code = %s, description = %s, updated_at = NOW() 
                    WHERE id = %s
                    """
                cursor.execute(update_query, (status_nc, response_code, reference_code, description, mnp_request_id))
                connection.commit()

                # callback_bss.delay(mnp_request_id, reference_code, None, response_code, description, None, None)
        else:
            # Should not come here normally, but just in case 
            logger.info("No status change for request %s", mnp_request_id)
            # initial_delta = timedelta(seconds=PENDING_REQUESTS_TIMEOUT)  # try again in 60 seconds
            # _, _, scheduled_at = calculate_countdown_working_hours(
            #             delta=initial_delta, 
            #             with_jitter=True)
            #  # Update the database with response
            # update_query = """
            #             UPDATE portability_requests 
            #             SET response_status = %s, description = %s, status_nc = %s, scheduled_at = %s, updated_at = NOW() 
            #             WHERE id = %s
            #             """
            # cursor.execute(update_query, (response_code, description, status_nc, scheduled_at, mnp_request_id))
            # connection.commit()

    except requests.exceptions.RequestException as exc:
        current_retry = self.request.retries
    
        # Convert exception to string for database storage
        error_description = str(exc)
    
        if connection is None:
            connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
    
        if current_retry < self.max_retries:
            # Still have retries left - update and retry
            print(f"Request failed, retrying ({current_retry + 1}/{self.max_retries}): {exc}")
            status_nc = "REQUEST_FAILED"
        
            update_query = """
            UPDATE portability_requests 
            SET status_nc = %s, retry_number = %s, error_description = %s, updated_at = NOW() 
            WHERE id = %s
        """
            cursor.execute(update_query, (status_nc, current_retry + 1, error_description, mnp_request_id))
            connection.commit()
        
            # Retry with exponential backoff
            # countdown = 60 * (2 ** current_retry)  # 60, 120, 240 seconds
            # countdown = 60
            # raise self.retry(exc=exc, countdown=countdown)
        else:
            # Max retries exceeded - final failure
            print(f"Max retries exceeded for request {mnp_request_id}: {exc}")
            status_nc = "MAX_RETRIES_EXCEEDED"
        
            update_query = """
                UPDATE portability_requests 
                SET status_nc = %s, retry_number = %s, error_description = %s, updated_at = NOW() 
                WHERE id = %s
            """
            cursor.execute(update_query, (status_nc, current_retry + 1, error_description, mnp_request_id))
            connection.commit()
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def submit_to_central_node_cancel_online_sync(mnp_request_id: int) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Synchronous version to submit cancellation and return immediate response
    """
    logger.debug("ENTER submit_to_central_node_cancel_online_sync with req_id %s", mnp_request_id)
    
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get request data
        cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        mnp_request = cursor.fetchone()
        
        if not mnp_request:
            logger.error("Cancellation request %s not found", mnp_request_id)
            return False, "NOT_FOUND", f"Request {mnp_request_id} not found"
        
        # Get session code
        session_code = initiate_session()
        
        # Generate SOAP payload
        soap_payload = json_from_db_to_soap_cancel_online(mnp_request, session_code)
        
        logger.debug("Submit Cancellation to NC: Generated SOAP Request")
        logger.debug("CANCEL_REQUEST->NC:\n%s",str(soap_payload))
        log_payload('NC', 'CANCEL', 'REQUEST', str(soap_payload))

        # Send to NC API
        response = requests.post(
            settings.APIGEE_PORTABILITY_URL,
            data=soap_payload,
            headers=settings.get_soap_headers('PeticionCancelarSolicitudAltaPortabilidadMovil'),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT
        )
        response.raise_for_status()

        # Parse the SOAP response
        log_payload('NC', 'CANCEL', 'RESPONSE', str(response.text))
        logger.debug("CANCEL_RESPONSE<-NC:\n%s", str(response.text))

        
        # Parse response including campoErroneo
        # response_code, description, campo_erroneo = parse_cancel_soap_response(response.text)
        response_code, description, reference_code = parse_soap_response_list(response.text, ["codigoRespuesta", "descripcion", "codigoReferencia"])
        print(f"Cancel to NC: Received response: response_code={response_code}, description={description}")
        
        # Determine success
        success = (response_code == "0000 00000")
        
        # Update database
        status_nc = 'SUBMITTED' if success else 'ERROR_CANCEL_RESPONSE'
        status_bss = f"STATUS_UPDATED_TO_{response_code}"
        update_query = """
            UPDATE portability_requests 
            SET status_nc = %s, session_code_nc = %s, response_code = %s, 
                description = %s, status_bss = %s, updated_at = NOW() 
            WHERE id = %s
        """
        cursor.execute(update_query, (status_nc, session_code, response_code, description, status_bss, mnp_request_id))
        connection.commit()

        return success, response_code, description

    except Exception as e:
        logger.error("Error in submit_to_central_node_cancel_online_sync: %s", e)
        error_msg = f"Error: {str(e)}"
        
        if cursor and connection:
            update_query = "UPDATE portability_requests SET status_nc = %s, description = %s WHERE id = %s"
            cursor.execute(update_query, ('ERROR', error_msg, mnp_request_id))
            connection.commit()
        
        return False, "PROCESSING_ERROR", error_msg, None
        
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()