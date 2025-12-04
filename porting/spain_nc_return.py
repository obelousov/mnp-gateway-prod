
from services.database_service import get_db_connection
from config import settings
from services.logger import logger, payload_logger, log_payload
from porting.spain_nc import initiate_session
from services.soap_services import soap_return_request
import requests
from services.soap_services import parse_soap_response_list, soap_cancel_return_request, soap_return_request_status_check
from services.time_services import calculate_countdown

def submit_to_central_node_return(mnp_request_id, current_retry=0, max_retries=3):
    """
    Function to submit a return request to the Central Node.
    """
    logger.debug("ENTER submit_to_central_node_return with req_id %s", mnp_request_id)
    connection = None
    APIGEE_PORTABILITY_URL = settings.APIGEE_PORTABILITY_URL
    try:
        # 1. Get database connection
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # 2. Fetch the request data from database
        cursor.execute("SELECT * FROM return_requests WHERE id = %s", (mnp_request_id,))
        mnp_request = cursor.fetchone()
        status_nc_old = mnp_request['status_nc'] if mnp_request else 'NOT_FOUND'
        request_date = mnp_request['request_date'] if mnp_request else None
        msisdn = mnp_request['msisdn'] if mnp_request else None
        status_bss = mnp_request['status_bss'] if mnp_request else None
        reference_code = mnp_request["reference_code"] if mnp_request else None
        response_code = mnp_request["response_code"] if mnp_request else None
        response_status = mnp_request["response_status"] if mnp_request else None
        response_code_old = response_code

        
        if not mnp_request:
            logger.error("Return submit to NC: request %s not found", mnp_request_id)
            return False, response_code, reference_code,  f"Return request {mnp_request_id} not found"

        response_status = mnp_request['response_status']

        # BNOT Baja notificada
        # BCAN Baja cancelada
        # BDEF Baja definitiva
        # BDET Baja detenida
        if response_status in ['BNOT', 'BCAN', 'BDEF', 'BDET']:
            logger.info("Request %s is in status %s, no further submission needed", mnp_request_id, response_status)
            return True, response_status, reference_code, f"Request already in status {response_status}"

        # 3. Prepare SOAP envelope
        session_code = initiate_session()
        logger.debug("Session_code: %s", session_code)

        # Convert to SOAP request
        logger.debug("Return submit to NC: Generating SOAP Request")
        soap_payload = soap_return_request(session_code, request_date, msisdn)
        
        # Conditional payload logging
        logger.debug("RETURN_REQUEST->NC: %s\n", soap_payload)
        log_payload('NC', 'RETURN', 'REQUEST', str(soap_payload))

        # 4. Try to send the request to Central Node
        if not APIGEE_PORTABILITY_URL:
            raise ValueError("APIGEE_PORTABILITY_URL environment variable is not set.")

        logger.info("Attempt %d for return request %s", current_retry + 1, mnp_request_id)
        
        response = requests.post(
            APIGEE_PORTABILITY_URL,
            data=soap_payload,
            headers=settings.get_soap_headers('peticionCrearSolicitudBajaNumeracionMovil'),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT
        )
        
        response.raise_for_status()

        # 5. Parse the SOAP response
        parsed = parse_soap_response_list(
            response.text, 
            ["codigoRespuesta", "descripcion", "codigoReferencia"]
        )
        # Ensure we have three values to unpack; pad with None if necessary
        if isinstance(parsed, (list, tuple)):
            parsed_list = list(parsed) + [None] * (3 - len(parsed))
            response_code, description, reference_code = parsed_list[:3]
        else:
            response_code = description = reference_code = None

        logger.info("Return to NC: Received response: response_code=%s, description=%s, reference_code=%s", 
                   response_code, description, reference_code)

        # 6. Process response code
        if not response_code or not response_code.strip():
            status_nc = "PENDING_NO_RESPONSE_CODE_RECEIVED"
        else:
            response_code_upper = response_code.strip().upper()

            # Handle 4xx client error responses
            if response_code_upper.startswith('4'):
                status_nc = "REQUEST_FAILED"
            # Handle 5xx server error responses
            elif response_code_upper.startswith('5'):
                status_nc = "SERVER_ERROR"
            # Handle success codes - contains only zeros and spaces, and has at least one zero
            elif '0' in response_code_upper and response_code_upper.replace(' ', '').replace('0', '') == '':
                status_nc = "RETURN_CONFIRMED"
            # All other non-error response codes are considered rejected returns
            else:
                status_nc = "RETURN_REJECTED"
        
        logger.info("Return: status nc changed %s status_old %s", status_nc, status_nc_old)
        
        # Check if status actually changed
        # status_changed = (status_nc != status_nc_old)
        status_changed = (response_code != response_code_old)

        logger.info("Return: status_changed %s response_code %s", status_changed, response_code)
        
        # 7. Update database based on status
        if status_changed:
            status_bss = "CHANGED_TO_" + response_code_upper   
            if status_nc == "PENDING_RESPONSE":
                # Special handling for ASOL status - reschedule at the next timeband
                _, scheduled_at = calculate_countdown(with_jitter=True)
                logger.info("Status changed to PENDING_RESPONSE (ASOL), rescheduling for %s", scheduled_at)
        
                update_query = """
                    UPDATE return_requests 
                    SET status_nc = %s, scheduled_at = %s, response_status = %s, 
                        reference_code = %s, description = %s, updated_at = NOW() 
                    WHERE id = %s
                """
                cursor.execute(update_query, (status_nc, scheduled_at, response_code, 
                                            reference_code, description, mnp_request_id))
                connection.commit()

            elif status_nc in ["REQUEST_FAILED", "SERVER_ERROR", "RETURN_CONFIRMED", "RETURN_REJECTED"]:
                logger.info("Status changed to %s, not rescheduled anymore", status_nc)
        
                update_query = """
                    UPDATE return_requests 
                    SET status_nc = %s, status_bss = %s, response_code = %s, reference_code = %s, 
                        description = %s, updated_at = NOW() 
                    WHERE id = %s
                """
                cursor.execute(update_query, (status_nc, status_bss, response_code, reference_code, 
                                            description, mnp_request_id))
                connection.commit()
        else:
            logger.info("No status change for request %s", mnp_request_id)

        return True, response_code, reference_code, description

    except requests.exceptions.RequestException as exc:
        logger.error("Request exception for return request %s: %s", mnp_request_id, str(exc))
        
        # Convert exception to string for database storage
        error_description = str(exc)
        
        # Get connection if not available
        if connection is None:
            connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
    
        if current_retry < max_retries:
            # Still have retries left - update and retry
            logger.warning("Return request failed, retrying (%d/%d): %s", 
                         current_retry + 1, max_retries, exc)
            status_nc = "REQUEST_FAILED"
        
            update_query = """
                UPDATE return_requests 
                SET status_nc = %s, retry_number = %s, error_description = %s, updated_at = NOW() 
                WHERE id = %s
            """
            cursor.execute(update_query, (status_nc, current_retry + 1, error_description, mnp_request_id))
            connection.commit()
        
            # For async retry, you would return a flag to indicate retry needed
            return False, "RETRY_NEEDED", error_description
            
        else:
            # Max retries exceeded - final failure
            logger.error("Max retries exceeded for return request %s: %s", mnp_request_id, exc)
            status_nc = "MAX_RETRIES_EXCEEDED"
        
            update_query = """
                UPDATE return_requests 
                SET status_nc = %s, retry_number = %s, error_description = %s, updated_at = NOW() 
                WHERE id = %s
            """
            cursor.execute(update_query, (status_nc, current_retry + 1, error_description, mnp_request_id))
            connection.commit()
            
            return False, "MAX_RETRIES_EXCEEDED", error_description
            
    except Exception as exc:
        logger.error("Unexpected error for return request %s: %s", mnp_request_id, str(exc))
        
        # Update database with error
        if connection is None:
            connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        update_query = """
            UPDATE return_requests 
            SET status_nc = %s, error_description = %s, updated_at = NOW() 
            WHERE id = %s
        """
        cursor.execute(update_query, ("PROCESSING_ERROR", str(exc), mnp_request_id))
        connection.commit()
        
        return False, "PROCESSING_ERROR", str(exc)
        
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def submit_to_central_node_cancel_return(mnp_request_id, current_retry=0, max_retries=3):
    """
    Function to submit a cancel return request to the Central Node.
    success, response_code, reference_code, description = submit_to_central_node_return(new_request_id)
    """
    logger.debug("ENTER submit_to_central_node_cancel_return with req_id %s", mnp_request_id)
    connection = None
    APIGEE_PORTABILITY_URL = settings.APIGEE_PORTABILITY_URL
    try:
        # 1. Get database connection
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # 2. Fetch the request data from database
        cursor.execute("SELECT * FROM return_requests WHERE id = %s", (mnp_request_id,))
        mnp_request = cursor.fetchone()
        status_nc_old = mnp_request['status_nc'] if mnp_request else 'NOT_FOUND'
        reference_code = mnp_request['reference_code'] if mnp_request else None
        cancellation_reason = mnp_request['cancellation_reason'] if mnp_request else None
        status_bss = mnp_request['status_bss'] if mnp_request else None
        response_code = mnp_request["response_code"] if mnp_request else None
        response_code_old = response_code


        
        if not mnp_request:
            logger.error("Cancel Return submit to NC: request %s not found", mnp_request_id)
            return False, "NOT_FOUND", "NOT_FOUND", f"Cancel Return request {mnp_request_id} not found"

        response_status = mnp_request['response_status']

        # BNOT Baja notificada
        # BCAN Baja cancelada
        # BDEF Baja definitiva
        # BDET Baja detenida


        if response_status in ['BNOT', 'BCAN', 'BDEF', 'BDET']:
            logger.info("Retrun Request %s is in status %s, no further submission needed", mnp_request_id, response_status)
            return False, response_status, reference_code, f"Request already in status {response_status}"

        # 3. Prepare SOAP envelope
        session_code = initiate_session()
        logger.debug("Session_code: %s", session_code)

        # Convert to SOAP request
        logger.debug("Return submit to NC: Generating SOAP Request")
        soap_payload = soap_cancel_return_request(session_code, reference_code, cancellation_reason)
        
        # Conditional payload logging
        logger.debug("CANCEL_RETURN_REQUEST->NC: %s\n", soap_payload)
        log_payload('NC', 'CANCEL_RETURN', 'REQUEST', str(soap_payload))

        # 4. Try to send the request to Central Node
        if not APIGEE_PORTABILITY_URL:
            raise ValueError("APIGEE_PORTABILITY_URL environment variable is not set.")

        logger.info("Attempt %d for return request %s", current_retry + 1, mnp_request_id)
        
        response = requests.post(
            APIGEE_PORTABILITY_URL,
            data=soap_payload,
            headers=settings.get_soap_headers('peticionCancelarSolicitudBajaNumeracionMovil'),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT
        )
        
        response.raise_for_status()

        # 5. Parse the SOAP response
        parsed = parse_soap_response_list(
            response.text, 
            ["codigoRespuesta", "descripcion", "codigoReferencia"]
        )
        # Ensure we have three values to unpack; pad with None if necessary
        if isinstance(parsed, (list, tuple)):
            parsed_list = list(parsed) + [None] * (3 - len(parsed))
            response_code, description, reference_code = parsed_list[:3]
        else:
            response_code = description = reference_code = None

        logger.info("Return to NC: Received response: response_code=%s, description=%s, reference_code=%s", 
                   response_code, description, reference_code)

        # Conditional payload logging
        log_payload('NC', 'RETURN', 'RESPONSE', str(response.text))
        logger.debug("RETURN_RESPONSE<-NC:\n%s", str(response.text))

        # 6. Process response code
        if not response_code or not response_code.strip():
            status_nc = "PENDING_NO_RESPONSE_CODE_RECEIVED"
        else:
            response_code_upper = response_code.strip().upper()

            # Handle 4xx client error responses
            if response_code_upper.startswith('4'):
                status_nc = "REQUEST_FAILED"
            # Handle 5xx server error responses
            elif response_code_upper.startswith('5'):
                status_nc = "SERVER_ERROR"
            # Handle success codes - contains only zeros and spaces, and has at least one zero
            elif '0' in response_code_upper and response_code_upper.replace(' ', '').replace('0', '') == '':
                status_nc = "RETURN_CANCEL_CONFIRMED"
            # All other non-error response codes are considered rejected returns
            else:
                status_nc = "RETURN_CANCEL_REJECTED"

        status_bss = "CHANGED_TO_" + response_code_upper        

        logger.info("Return: status nc changed %s status_old %s", status_nc, status_nc_old)
        
        # Check if status actually changed
        # status_changed = (status_nc != status_nc_old)
        status_changed = (response_code != response_code_old)

        logger.info("Return: status_changed %s status_nc %s", status_changed, status_nc)
        
        # 7. Update database based on status
        if status_changed:
            # if status_nc == "PENDING_RESPONSE":
            #     # Special handling for ASOL status - reschedule at the next timeband
            #     _, scheduled_at = calculate_countdown(with_jitter=True)
            #     logger.info("Status changed to PENDING_RESPONSE (ASOL), rescheduling for %s", scheduled_at)
        
            #     update_query = """
            #         UPDATE return_requests 
            #         SET status_nc = %s, scheduled_at = %s, response_status = %s, 
            #             description = %s, updated_at = NOW() 
            #         WHERE id = %s
            #     """
            #     cursor.execute(update_query, (status_nc, scheduled_at, response_code, 
            #                                 description, mnp_request_id))
            #     connection.commit()

            # elif status_nc in ["REQUEST_FAILED", "SERVER_ERROR", "RETURN_CONFIRMED", "RETURN_REJECTED"]:
            #     logger.info("Status changed to %s, not rescheduled anymore", status_nc)
        
                update_query = """
                    UPDATE return_requests 
                    SET status_nc = %s, status_bss = %s, response_code = %s, 
                        description = %s, updated_at = NOW() 
                    WHERE id = %s
                """
                cursor.execute(update_query, (status_nc, status_bss, response_code, 
                                            description, mnp_request_id))
                connection.commit()
        else:
            logger.info("No status change for request %s", mnp_request_id)

        return True, response_code, description

    except requests.exceptions.RequestException as exc:
        logger.error("Request exception for return request %s: %s", mnp_request_id, str(exc))
        
        # Convert exception to string for database storage
        error_description = str(exc)
        
        # Get connection if not available
        if connection is None:
            connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
    
        if current_retry < max_retries:
            # Still have retries left - update and retry
            logger.warning("Return request failed, retrying (%d/%d): %s", 
                         current_retry + 1, max_retries, exc)
            status_nc = "REQUEST_FAILED"
        
            update_query = """
                UPDATE return_requests 
                SET status_nc = %s, retry_number = %s, error_description = %s, updated_at = NOW() 
                WHERE id = %s
            """
            cursor.execute(update_query, (status_nc, current_retry + 1, error_description, mnp_request_id))
            connection.commit()
        
            # For async retry, you would return a flag to indicate retry needed
            return False, "RETRY_NEEDED", error_description
            
        else:
            # Max retries exceeded - final failure
            logger.error("Max retries exceeded for return request %s: %s", mnp_request_id, exc)
            status_nc = "MAX_RETRIES_EXCEEDED"
        
            update_query = """
                UPDATE return_requests 
                SET status_nc = %s, retry_number = %s, error_description = %s, updated_at = NOW() 
                WHERE id = %s
            """
            cursor.execute(update_query, (status_nc, current_retry + 1, error_description, mnp_request_id))
            connection.commit()
            
            return False, "MAX_RETRIES_EXCEEDED", error_description
            
    except Exception as exc:
        logger.error("Unexpected error for return request %s: %s", mnp_request_id, str(exc))
        
        # Update database with error
        if connection is None:
            connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        update_query = """
            UPDATE return_requests 
            SET status_nc = %s, error_description = %s, updated_at = NOW() 
            WHERE id = %s
        """
        cursor.execute(update_query, ("PROCESSING_ERROR", str(exc), mnp_request_id))
        connection.commit()
        
        return False, "PROCESSING_ERROR", str(exc)
        
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


# def submit_to_central_node_return_status_check(reference_code, current_retry=0, max_retries=3):
def submit_to_central_node_return_status_check(reference_code: str, current_retry: int = 0, max_retries: int = 3) -> dict:
    """
    Simplified function to check return request status from Central Node.
    Returns: response_code, description, reference_code
    """
    logger.debug("ENTER submit_to_central_node_return_status_check with reference_code %s", reference_code)
    
    APIGEE_PORTABILITY_URL = settings.APIGEE_PORTABILITY_URL
    
    try:
        # 1. Prepare SOAP envelope
        session_code = initiate_session()
        logger.debug("Session_code: %s", session_code)

        # 2. Convert to SOAP request
        logger.debug("Return status check: Generating SOAP Request")
        soap_payload = soap_return_request_status_check(session_code, reference_code)
        
        # Log the exact payload being sent
        logger.debug("FULL SOAP REQUEST:\n%s", soap_payload)
        log_payload('NC', 'STATUS_CHECK_RETURN', 'REQUEST', str(soap_payload))

        # 3. Validate URL
        if not APIGEE_PORTABILITY_URL:
            raise ValueError("APIGEE_PORTABILITY_URL environment variable is not set.")

        logger.info("Attempt %d for return status check reference_code %s", current_retry + 1, reference_code)
        
        # 4. Send request to Central Node
        response = requests.post(
            APIGEE_PORTABILITY_URL,
            data=soap_payload,
            headers=settings.get_soap_headers('peticionObtenerSolicitudAltaPortabilidadMovil'),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT
        )
              
        response.raise_for_status()

        # 5. Parse the SOAP response
        field_names = ["codigoRespuesta", "descripcion", "codigoReferencia", "fechaEstado", "fechaCreacion",
                    "fechaBajaAbonado", "codigoOperadorReceptor", "codigoOperadorDonante", "estado",
                    "causaEstado", "fechaVentanaCambio"]

        parsed_tuple = parse_soap_response_list(response.text, field_names)
        parsed_dict = dict(zip(field_names, parsed_tuple))
        
        # DEBUG: Print parsing results
        # logger.debug("Parsing Results: %s", parsed)
        # logger.debug("Parsing Type: %s", type(parsed))
        
        # # Ensure we have three values to unpack; pad with None if necessary
        # if isinstance(parsed, (list, tuple)):
        #     parsed_list = list(parsed) + [None] * (3 - len(parsed))
        #     response_code, description, returned_reference_code = parsed_list[:3]
        # else:
        #     response_code = description = returned_reference_code = None

        # # Use the returned reference_code if available, otherwise use input reference_code
        # final_reference_code = returned_reference_code or reference_code

        # logger.info("Return status check: Received response: response_code=%s, description=%s, reference_code=%s", 
        #            response_code, description, final_reference_code)

        # Conditional payload logging
        log_payload('NC', 'RETURN_STATUS_CHECK', 'RESPONSE', str(response.text))
        logger.debug("RETURN_STATUS_CHECK_RESPONSE<-NC:\n%s", str(response.text))

        return parsed_dict

    except requests.exceptions.HTTPError as e:
        # logger.error("HTTP Error for return status check reference_code %s:", reference_code)
        # logger.error("Status Code: %s", e.response.status_code if e.response else "No response")
        # logger.error("Response Headers: %s", e.response.headers if e.response else "No headers")
        # logger.error("Response Text: %s", e.response.text if e.response else "No text")
        # logger.error("Full Exception: %s", str(e))
        
        if current_retry < max_retries:
            logger.warning("Return status check failed, retrying (%d/%d): %s", 
                         current_retry + 1, max_retries, e)
            raise  # Re-raise for retry mechanism
        else:
            logger.error("Max retries exceeded for return status check reference_code %s: %s", reference_code, e)
            return "MAX_RETRIES_EXCEEDED", f"HTTP Error {e.response.status_code if e.response else 'Unknown'}: {str(e)}", reference_code
            
    except requests.exceptions.RequestException as exc:
        logger.error("Request exception for return status check reference_code %s: %s", reference_code, str(exc))
        logger.error("Exception type: %s", type(exc).__name__)
        
        if current_retry < max_retries:
            logger.warning("Return status check failed, retrying (%d/%d): %s", 
                         current_retry + 1, max_retries, exc)
            raise
        else:
            logger.error("Max retries exceeded for return status check reference_code %s: %s", reference_code, exc)
            return "MAX_RETRIES_EXCEEDED", f"Request exception: {str(exc)}", reference_code
            
    except Exception as exc:
        logger.error("Unexpected error for return status check reference_code %s: %s", reference_code, str(exc))
        logger.error("Exception type: %s", type(exc).__name__)
        import traceback
        logger.error("Traceback: %s", traceback.format_exc())
        return "PROCESSING_ERROR", f"Unexpected error: {str(exc)}", reference_code
    
def convert_spanish_to_english(response_dict: dict) -> dict:
    """Convert Spanish field names to English and add success field at the beginning"""
    mapping = {
        "codigoRespuesta": "response_code",
        "descripcion": "description",
        "codigoReferencia": "reference_code", 
        "fechaEstado": "status_date",
        "fechaCreacion": "creation_date",
        "fechaBajaAbonado": "subscriber_cancellation_date",
        "codigoOperadorReceptor": "recipient_operator_code",
        "codigoOperadorDonante": "donor_operator_code",
        "estado": "status",
        "causaEstado": "status_reason",
        "fechaVentanaCambio": "change_window_date"
    }
    
    # Convert field names
    english_dict = {mapping.get(key, key): value for key, value in response_dict.items()}
    
    # Determine success
    success = english_dict.get("response_code") == "0000 00000"
    
    # Create new dict with success first
    return {
        "success": success,
        **english_dict  # Unpack all other fields
    }