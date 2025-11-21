
from services.database_service import get_db_connection
from config import settings
from services.logger import logger, payload_logger, log_payload
from porting.spain_nc import initiate_session
from services.soap_services import soap_return_request
import requests
from services.soap_services import parse_soap_response_list, soap_cancel_return_request
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

        
        if not mnp_request:
            logger.error("Return submit to NC: request %s not found", mnp_request_id)
            return False, "NOT_FOUND", f"Return request {mnp_request_id} not found"

        response_status = mnp_request['response_status']

        if response_status in ['ASOL', 'ACON', 'AREC', 'APOR', 'ACAN']:
            logger.info("Request %s is in status %s, no further submission needed", mnp_request_id, response_status)
            return True, response_status, f"Request already in status {response_status}"

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

        # Conditional payload logging
        log_payload('NC', 'RETURN', 'RESPONSE', str(response.text))
        logger.debug("RETURN_RESPONSE<-NC:\n%s", str(response.text))

        # 6. Process response code
        response_code_upper = ""
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
        # Handle specific success codes for return requests
        elif response_code_upper != '0000 0000':
            status_nc = "RETURN_REJECTED"
        else:
            status_nc = "RETURN_COFIRMED"

        status_bss = "CHANGED_TO_" + response_code_upper
        
        logger.info("Return: status nc changed %s status_old %s", status_nc, status_nc_old)
        
        # Check if status actually changed
        status_changed = (status_nc != status_nc_old)

        logger.info("Return: status_changed %s status_nc %s", status_changed, status_nc)
        
        # 7. Update database based on status
        if status_changed:
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


def submit_to_central_node_cancel_return(mnp_request_id, current_retry=0, max_retries=3):
    """
    Function to submit a cancel return request to the Central Node.
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

        
        if not mnp_request:
            logger.error("Cancel Return submit to NC: request %s not found", mnp_request_id)
            return False, "NOT_FOUND", f"Cancel Return request {mnp_request_id} not found"

        response_status = mnp_request['response_status']

        if response_status in ['ASOL', 'ACON', 'AREC', 'APOR', 'ACAN']:
            logger.info("Request %s is in status %s, no further submission needed", mnp_request_id, response_status)
            return True, response_status, f"Request already in status {response_status}"

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
        response_code_upper = ""
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
        # Handle specific success codes for return requests
        elif response_code_upper != '0000 0000':
            status_nc = "RETURN_REJECTED"
        else:
            status_nc = "RETURN_COFIRMED"

        status_bss = "CHANGED_TO_" + response_code_upper
        
        logger.info("Return: status nc changed %s status_old %s", status_nc, status_nc_old)
        
        # Check if status actually changed
        status_changed = (status_nc != status_nc_old)

        logger.info("Return: status_changed %s status_nc %s", status_changed, status_nc)
        
        # 7. Update database based on status
        if status_changed:
            if status_nc == "PENDING_RESPONSE":
                # Special handling for ASOL status - reschedule at the next timeband
                _, scheduled_at = calculate_countdown(with_jitter=True)
                logger.info("Status changed to PENDING_RESPONSE (ASOL), rescheduling for %s", scheduled_at)
        
                update_query = """
                    UPDATE return_requests 
                    SET status_nc = %s, scheduled_at = %s, response_status = %s, 
                        description = %s, updated_at = NOW() 
                    WHERE id = %s
                """
                cursor.execute(update_query, (status_nc, scheduled_at, response_code, 
                                            description, mnp_request_id))
                connection.commit()

            elif status_nc in ["REQUEST_FAILED", "SERVER_ERROR", "RETURN_CONFIRMED", "RETURN_REJECTED"]:
                logger.info("Status changed to %s, not rescheduled anymore", status_nc)
        
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
