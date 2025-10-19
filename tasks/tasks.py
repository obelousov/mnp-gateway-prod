# tasks.py
from typing import List, Optional, Dict, Tuple
from celery_app import app
import requests
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
# from soap_utils import create_soap_payload, parse_soap_response, json_to_soap_request, json_from_db_to_soap, parse_soap_response_list, create_status_check_soap
from services.soap_services import parse_soap_response_list, create_status_check_soap, json_from_db_to_soap_new, json_from_db_to_soap_cancel,json_from_db_to_soap_online
# from time_utils import calculate_countdown
from services.time_services import calculate_countdown
from datetime import datetime, timedelta
import logging
import pytz
# from db_utils import get_db_connection
from services.database_service import get_db_connection
from config import settings
from services.time_services import calculate_countdown_working_hours
from services.logger import logger, payload_logger, log_payload
from porting.spain_nc import initiate_session

WSDL_SERVICE_SPAIN_MOCK = settings.WSDL_SERVICE_SPAIN_MOCK
WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS = settings.WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS
WSDL_SERVICE_SPAIN_MOCK_CANCEL = settings.WSDL_SERVICE_SPAIN_MOCK_CANCEL
BSS_WEBHOOK_URL = settings.BSS_WEBHOOK_URL

PENDING_REQUESTS_TIMEOUT = settings.PENDING_REQUESTS_TIMEOUT  # seconds

# Get timezone from environment or default to Europe/Madrid
timezone_str = settings.TIME_ZONE
container_tz = pytz.timezone(timezone_str)

@app.task
def print_periodic_message():
    """A simple task that prints a message with Madrid time - runs every 60 seconds via beat schedule"""
    madrid_tz = pytz.timezone('Europe/Madrid')
    current_time = datetime.now(madrid_tz).strftime('%Y-%m-%d %H:%M:%S %Z%z')
    
    message = f"Periodic task executed at {current_time}! This runs every 60 seconds in Madrid timezone."
    # full_message = f"Hello: {message}"
    # print(full_message)  # Print the complete message
    return message

@app.task(bind=True, max_retries=3)
def submit_to_central_node(self, mnp_request_id):
    """
    Task to submit a porting request to the Central Node.
    This runs in the background.
    """
    logger.debug("ENTER submit_to_central_node with req_id %s", mnp_request_id)
    connection = None
    try:
        # 1. Get database connection
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # 2. Fetch the request data from database
        # cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        current_time = datetime.now(container_tz)
        print(f"Submit to NC: current time {current_time}")
        cursor.execute("SELECT * FROM portability_requests WHERE id = %s AND %s > scheduled_at",(mnp_request_id, current_time))
        # cursor.execute("SELECT * FROM portability_requests WHERE id = %s AND NOW() > scheduled_at",(mnp_request_id,))
        mnp_request = cursor.fetchone()
        status_nc_old = mnp_request['status_nc'] if mnp_request else 'NOT_FOUND'
        
        if not mnp_request:
            print(f"Submit to NC: request {mnp_request_id} not found or not yet scheduled")
            return f"Submit to NC: request {mnp_request_id} not found or not yet scheduled"

        response_status = mnp_request['response_status']

        if response_status in ['ASOL', 'ACON', 'AREC', 'APOR', 'ACAN']:
            return f"Request {mnp_request_id} is in status {response_status}, no further submission needed"

        # 3. Prepare your SOAP envelope (use your existing logic)
        # print(f"Submit to NC: Request {mnp_request} found, preparing SOAP payload...")

        # Convert JSON to SOAP request
        # soap_request = json_to_soap_request(mnp_request)
        logger.debug("Submit to NC: Generated SOAP Request:")
        # soap_payload = json_from_db_to_soap(mnp_request)  # function to create SOAP
        soap_payload = json_from_db_to_soap_new(mnp_request)  # function to create SOAP
        # print(soap_payload)
        # Conditional payload logging
        log_payload('NC', 'PORT_IN', 'REQUEST', str(soap_payload))

        # 4. Try to send the request to Central Node
        if not WSDL_SERVICE_SPAIN_MOCK:
            raise ValueError("WSDL_SERVICE_SPAIN_MOCK environment variable is not set.")
        current_retry = self.request.retries
        logger.info("Attempt %d for request %s", current_retry+1, mnp_request_id)
        print(f"Submit to NC: Attempt {current_retry+1} for request {mnp_request_id}")

        response = requests.post(WSDL_SERVICE_SPAIN_MOCK, 
                               data=soap_payload,
                               headers=settings.get_soap_headers('IniciarSesion'),
                               timeout=settings.APIGEE_API_QUERY_TIMEOUT)
        response.raise_for_status()

        # 5. Parse the SOAP response (use your existing logic)
        # session_code, status = parse_soap_response_list(response.text,)
        response_code, description, reference_code = parse_soap_response_list(response.text, ["codigoRespuesta", "descripcion", "codigoReferencia"])
        print(f"Submit to NC: Received response: response_code={response_code}, description={description}, reference_code={reference_code}")

        # Conditional payload logging
        log_payload('NC', 'PORT_IN', 'RESPONSE', str(response.text))

        # Assign status based on response_code
        if not response_code or not response_code.strip():
            status_nc = "PENDING_NO_RESPONSE_CODE_RECEIVED"
        else:
            response_code_upper = response_code.strip().upper()
            status_nc = "PENDING_RESPONSE" if response_code_upper == 'ASOL' else "PENDING_CONFIRMATION"

        # Check if status actually changed
        status_changed = (status_nc != status_nc_old)

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
        else:
            # Should not come here normally, but just in case 
            logger.info("No status change for request %s", mnp_request_id)
            initial_delta = timedelta(seconds=PENDING_REQUESTS_TIMEOUT)  # try again in 60 seconds
            _, _, scheduled_at = calculate_countdown_working_hours(
                        delta=initial_delta, 
                        with_jitter=True)
             # Update the database with response
            update_query = """
                        UPDATE portability_requests 
                        SET response_status = %s, description = %s, status_nc = %s, scheduled_at = %s, updated_at = NOW() 
                        WHERE id = %s
                        """
            cursor.execute(update_query, (response_code, description, status_nc, scheduled_at, mnp_request_id))
            connection.commit()

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
            countdown = 60
            raise self.retry(exc=exc, countdown=countdown)
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

@app.task(bind=True, max_retries=3)
def check_status(self, mnp_request_id, session_code, msisdn,reference_code):
    """
    Task to check the status of a single MSISDN at the Central Node.
    """
    connection = None
    logger.info("ENTER check status() with req_id %s ref_code %s msisdn %s", mnp_request_id, reference_code,msisdn)
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        consultar_payload = create_status_check_soap(mnp_request_id, reference_code, msisdn)  # Check status request SOAP
        # Conditional payload logging
        log_payload('NC', 'CHECK_STATUS', 'REQUEST', str(consultar_payload))

        if not WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS:
            raise ValueError("WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS environment variable is not set.")
        
        response = requests.post(WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS, 
                               data=consultar_payload,
                               headers=settings.get_soap_headers('IniciarSesion'),
                               timeout=settings.APIGEE_API_QUERY_TIMEOUT)
        response.raise_for_status()

        # new_status = parse_soap_response(response.text)  # Parse the response
        response_code, description, _, session_code = parse_soap_response_list(response.text, ["codigoRespuesta", "descripcion", "codigoReferencia","estado"])
        print(f"Received check response: response_code={response_code}, description={description}, session_code={session_code}")
        log_payload('NC', 'CHECK_STATUS', 'RESPONSE', str(response.text))

        # If it's still pending, queue the next check during working hours
        if response_code == 'ASOL':
            # Still same status, updated scheduled_at for next check - within same timenad
            _, _, scheduled_datetime = calculate_countdown_working_hours(
                                                        delta=settings.TIME_DELTA_FOR_STATUS_CHECK, 
                                                        with_jitter=True
                                                                                    )
            # Update database with the actual scheduled time
            update_query = """
                UPDATE portability_requests 
                SET response_status = %s,
                SET scheduled_at = %s,
                updated_at = NOW() 
                WHERE id = %s
            """
            cursor.execute(update_query, (response_code,scheduled_datetime, mnp_request_id))
            connection.commit()
            return "Scheduled next check for id: %s at %s", mnp_request_id, scheduled_datetime

        if response_code in ('ACON', 'APOR', 'AREC','ACAN'):
            if response_code == 'ACON':
                status_nc = 'PORT_IN_CONFIRMED'
            elif response_code == 'APOR':
                status_nc = 'PORT_IN_COMPLETED'
            elif response_code == 'AREC':
                status_nc = 'PORT_IN_REJECTED'
            elif response_code == 'ACAN':
                status_nc = 'PORT_IN_CANCELLED'
            else:
                status_nc = 'PENDING_RESPONSE'

            print(f"Final status: response_code={response_code}, status_nc={status_nc}")
            update_query = """
                UPDATE portability_requests 
                SET response_status = %s,
                status_nc = %s,
                description = %s,
                updated_at = NOW() 
                WHERE id = %s
            """
            cursor.execute(update_query, (response_code,status_nc, description,mnp_request_id))
            connection.commit()
            callback_bss.delay(mnp_request_id, reference_code, session_code, response_code, description, None, None)
            # def callback_bss(self, mnp_request_id, reference_code, session_code, 
            # response_status, description=None, error_fields=None, porting_window_date=None):
            
            return "Final status received for id: %s, status: %s", mnp_request_id, response_code
            
            # callback_bss.delay(mnp_request_id)

    except requests.exceptions.RequestException as exc:
        print(f"Status check failed, retrying: {exc}")
        self.retry(exc=exc, countdown=120)
    except Error as e:
        print(f"Database error during status check: {e}")
        self.retry(exc=e, countdown=30)
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

@app.task(bind=True, max_retries=3)
def callback_bss(self, mnp_request_id, reference_code, session_code, response_status, description=None, error_fields=None, porting_window_date=None):
    """
    REST JSON POST to BSS Webhook with updated English field names
    
    Args:
        mnp_request_id: Unique identifier for the MNP request
        session_code: Session code for the transaction
        msisdn: Mobile number
        response_status: Status to send to webhook
        description: Optional description message
        error_fields: Optional list of error field objects
        porting_window_date: Optional porting window date
    """
    logger.debug("ENTER callback_bss() with request_id %s reference_code %s response_status %s", 
                 mnp_request_id, reference_code, response_status)
    
    # Prepare JSON payload with new English field names
    payload = {
        "request_id": mnp_request_id,
        "reference_code": reference_code,
        "response_code": response_status,
        "description": description or f"Status update for MNP request {mnp_request_id}",
        "error_fields": error_fields or [],
        "porting_window_date": porting_window_date or ""
    }
   
    
    try:
        # Send POST request
        response = requests.post(
            settings.BSS_WEBHOOK_URL,
            json=payload,
            headers=settings.get_headers_bss(),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT,
            verify=settings.SSL_VERIFICATION  # Use SSL verification setting
        )
        
        # Check if request was successful
        if response.status_code == 200:
            logger.info(
                "Webhook sent successfully for request_id %s session %s, response_code: %s", 
                mnp_request_id, session_code, response_status
            )

            # Update database with the actual scheduled time
            try: 
                update_query = """
                    UPDATE portability_requests 
                    SET status_bss = %s,
                    updated_at = NOW() 
                    WHERE id = %s
                """
                connection = get_db_connection()
                cursor = connection.cursor(dictionary=True)
                
                # Map response_code to appropriate status_bss value
                status_bss="CANCEL_REQUEST_COMPLETED" if response_status=="ACAN" else "NO_RESPONSE_ON CANCEL_RESPONSE"
                # status_bss = self._map_response_to_status(response_status)
                cursor.execute(update_query, (status_bss, mnp_request_id))
                connection.commit()
                
                logger.debug(
                    "Database updated for request %s with status_bss: %s", 
                    mnp_request_id, status_bss
                )
                return True
                
            except Exception as db_error:
                logger.error("Database update failed for request %s: %s", mnp_request_id, str(db_error))
                return False
        else:
            logger.error(
                "Webhook failed for session %s request_id: %s Status: %s, Response: %s", 
                session_code, mnp_request_id, response.status_code, response.text
            )
            return False
            
    except requests.exceptions.Timeout as exc:
        logger.error("Webhook timeout for session %s request_id %s", session_code, mnp_request_id)
        self.retry(exc=exc, countdown=120)
        return False
    except requests.exceptions.ConnectionError as exc:
        logger.error("Webhook connection error for session %s request_id %s", session_code, mnp_request_id)
        self.retry(exc=exc, countdown=120)
        return False
    except requests.exceptions.RequestException as exc:
        logger.error("Webhook error for session %s request_id %s: %s", session_code, mnp_request_id, str(exc))
        self.retry(exc=exc, countdown=120)
        return False
    finally:
        if 'connection' in locals() and connection and connection.is_connected():
            cursor.close()
            connection.close()

def _map_response_to_status(self, response_code):
    """
    Map response_code to appropriate status_bss value
    """
    status_mapping = {
        'ASOL': 'REQUEST_ACCEPTED',
        'ACON': 'PORTIN_CONFIRMED', 
        'APOR': 'PORTIN_COMPLETED',
        'AREC': 'PORTIN_REJECTED',
        '400': 'REQUEST_FAILED',
        '500': 'SERVER_ERROR'
    }
    return status_mapping.get(response_code, 'UNKNOWN_STATUS')

def _get_current_date(self):
    """
    Get current date in YYYY-MM-DD format
    """
    from datetime import datetime
    return datetime.now().strftime('%Y-%m-%d')

def callback_bss_1(self, mnp_request_id, session_code, msisdn, response_status):
    """
    REST JSON POST to BSS Webhook
    
    Args:
        mnp_request_id: Unique identifier for the MNP request
        session_code: Session code for the transaction
        msisdn: Mobile number
        response_status: Status to send to webhook
    """
    logger.debug("ENTER callback_bss() with req_id %s msisdn %s response_status %s", mnp_request_id, msisdn, response_status)
    # Prepare JSON payload
    payload = {
        "mnpRequestId": mnp_request_id,
        "sessionCode": session_code,
        "msisdn": msisdn,
        "responseStatus": response_status
        # "timestamp": self._get_current_timestamp()  # Optional: add timestamp
    }
    
    try:
        # Send POST request
        response = requests.post(
            BSS_WEBHOOK_URL,
            json=payload,
            headers=settings.get_headers_bss(),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT
        )
        
        # Check if request was successful
        if response.status_code == 200:
            logger.info("Webhook sent successfully for request_id %s session %s, status: %s", mnp_request_id,session_code, response_status)

            # Update database with the actual scheduled time
            try: 
                update_query = """
                    UPDATE portability_requests 
                    SET status_bss = %s,
                    updated_at = NOW() 
                    WHERE id = %s
                """
                connection = get_db_connection()
                cursor = connection.cursor(dictionary=True)
                cursor.execute(update_query, ("PORT_IN_COMPLETED", mnp_request_id))
                connection.commit()
                return True
            except Exception as db_error:
                logger.error("Database update failed for request %s: %s", mnp_request_id, str(db_error))
        else:
            logging.error("Webhook failed for session %s request_id: %s Status: %s, Response: %s", session_code, mnp_request_id,response.status_code, response.text)
            return False
            
    except requests.exceptions.Timeout as exc:
        logger.error("Webhook timeout for session %s request_is %s ",session_code, mnp_request_id)
        self.retry(exc=exc, countdown=120)
        return False
    except requests.exceptions.ConnectionError as exc:
        logger.error("Webhook connection error for session %s request_id %s",session_code, mnp_request_id)
        self.retry(exc=exc, countdown=120)
        return False
    except requests.exceptions.RequestException as exc:
        logger.error("Webhook error for session %s request_id %s: %s", session_code, mnp_request_id, str(exc))
        self.retry(exc=exc, countdown=120)
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

@app.task(bind=True, max_retries=3)
def submit_to_central_node_cancel(self, mnp_request_id):
    """
    Task to submit a cancel request to the Central Node.
    """
    logger.debug("ENTER submit_to_central_node_cancel with req_id %s", mnp_request_id)
    connection = None
    try:
        # 1. Get database connection
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # 2. Fetch the request data from database
        # cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        current_time = datetime.now(container_tz)
        print(f"Send Cancel to NC: current time {current_time}")
        cursor.execute("SELECT * FROM portability_requests WHERE id = %s AND %s > scheduled_at",(mnp_request_id, current_time))
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

        # Convert JSON to SOAP request
        # soap_request = json_to_soap_request(mnp_request)
        logger.debug("Cancel submit to NC: Generated SOAP Request:")
        # soap_payload = json_from_db_to_soap(mnp_request)  # function to create SOAP
        # soap_payload = json_from_db_to_soap_new(mnp_request)  # function to create SOAP
        soap_payload = json_from_db_to_soap_cancel(mnp_request)
        # print(soap_payload)
        # Conditional payload logging
        log_payload('NC', 'CANCEL', 'REQUEST', str(soap_payload))

        # 4. Try to send the request to Central Node
        if not WSDL_SERVICE_SPAIN_MOCK_CANCEL:
            raise ValueError("WSDL_SERVICE_SPAIN_MOCK_CANCEL environment variable is not set.")
        current_retry = self.request.retries
        logger.info("Attempt %d for request %s", current_retry+1, mnp_request_id)
        print(f"Cancel submit to NC: Attempt {current_retry+1} for request {mnp_request_id}")
        
        response = requests.post(WSDL_SERVICE_SPAIN_MOCK_CANCEL,
                               data=soap_payload,
                               headers=settings.get_soap_headers('IniciarSesion'),
                               timeout=settings.APIGEE_API_QUERY_TIMEOUT)
        
        response.raise_for_status()

        # 5. Parse the SOAP response (use your existing logic)
        # session_code, status = parse_soap_response_list(response.text,)
        response_code, description, reference_code = parse_soap_response_list(response.text, ["codigoRespuesta", "descripcion", "codigoReferencia"])
        print(f"Cancel to NC: Received response: response_code={response_code}, description={description}, reference_code={reference_code}")

        # Conditional payload logging
        log_payload('NC', 'CANCEL', 'RESPONSE', str(response.text))
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

                callback_bss.delay(mnp_request_id, reference_code, None, response_code, description, None, None)
        else:
            # Should not come here normally, but just in case 
            logger.info("No status change for request %s", mnp_request_id)
            initial_delta = timedelta(seconds=PENDING_REQUESTS_TIMEOUT)  # try again in 60 seconds
            _, _, scheduled_at = calculate_countdown_working_hours(
                        delta=initial_delta, 
                        with_jitter=True)
             # Update the database with response
            update_query = """
                        UPDATE portability_requests 
                        SET response_status = %s, description = %s, status_nc = %s, scheduled_at = %s, updated_at = NOW() 
                        WHERE id = %s
                        """
            cursor.execute(update_query, (response_code, description, status_nc, scheduled_at, mnp_request_id))
            connection.commit()

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
            countdown = 60
            raise self.retry(exc=exc, countdown=countdown)
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

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def submit_to_central_node_task(self, mnp_request_id: int) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """
    Celery Task: Submit a porting request to the Central Node.
    Retries on network or transient failures.
    """
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)

        # Fetch current record
        cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        mnp_request = cursor.fetchone()
        if not mnp_request:
            logger.error("Request %s not found", mnp_request_id)
            return False, "NOT_FOUND", f"Request {mnp_request_id} not found", None

        status_nc_old = mnp_request.get('status_nc', 'NOT_FOUND')

        # Build SOAP request
        session_code = initiate_session()
        soap_payload = json_from_db_to_soap_online(mnp_request, session_code)

        response = requests.post(
            settings.APIGEE_PORTABILITY_URL,
            data=soap_payload,
            headers=settings.get_soap_headers('CrearSolicitudIndividualAltaPortabilidadMovil'),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT
        )
        response.raise_for_status()

        # Parse SOAP response
        result = parse_soap_response_list(response.text, ["codigoRespuesta", "descripcion", "codigoReferencia"])
        response_code, description, reference_code = (result if result and len(result) == 3 else (None, None, None))

        # Assign status based on response_code
        if not response_code or not response_code.strip():
            status_nc = "PENDING_NO_RESPONSE_CODE_RECEIVED"
        else:
            response_code_upper = response_code.strip().upper()
            if response_code_upper == "ASOL":
                status_nc = "PENDING_RESPONSE" # ASOL received, now waiting for confirmation or rejection
            else:
                status_nc = "PENDING_CONFIRMATION_ASOL"

        # Detect if NC status actually changed
        status_changed = (status_nc != status_nc_old)

        # Update DB depending on change
        if status_changed:
            if status_nc == "PENDING_RESPONSE":
                # Special handling for ASOL – reschedule next window
                _, scheduled_at = calculate_countdown(with_jitter=True)
                logger.info("Request %s status changed to PENDING_RESPONSE, rescheduling for %s", mnp_request_id, scheduled_at)

                update_query = """
                    UPDATE portability_requests 
                    SET status_nc = %s, scheduled_at = %s, session_code_nc = %s,
                        response_status = %s, reference_code = %s, description = %s, updated_at = NOW()
                    WHERE id = %s
                """
                cursor.execute(update_query, (status_nc, scheduled_at, session_code, response_code, reference_code, description, mnp_request_id))
            else:
                # Normal status update
                update_query = """
                    UPDATE portability_requests 
                    SET status_nc = %s, session_code_nc = %s,
                        response_status = %s, reference_code = %s, description = %s, updated_at = NOW()
                    WHERE id = %s
                """
                cursor.execute(update_query, (status_nc, session_code, response_code, reference_code, description, mnp_request_id))
        else:
            # Status unchanged – just refresh metadata
            update_query = """
                UPDATE portability_requests 
                SET session_code_nc = %s, response_status = %s, reference_code = %s, description = %s, updated_at = NOW()
                WHERE id = %s
            """
            cursor.execute(update_query, (session_code, response_code, reference_code, description, mnp_request_id))

        connection.commit()

        success = response_code == "0000 00000"
        return success, response_code, description, reference_code

    except requests.exceptions.RequestException as exc:
        current_retry = self.request.retries
        error_description = str(exc)
        logger.warning("RequestException on request %s (attempt %s/%s): %s", mnp_request_id, current_retry + 1, self.max_retries, error_description)

        if connection is None:
            connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)

        if current_retry < self.max_retries:
            cursor.execute("""
                UPDATE portability_requests 
                SET status_nc = %s, retry_number = %s, error_description = %s, updated_at = NOW()
                WHERE id = %s
            """, ("REQUEST_FAILED", current_retry + 1, error_description, mnp_request_id))
            connection.commit()
            raise self.retry(exc=exc, countdown=60)
        else:
            logger.error("Max retries exceeded for request %s", mnp_request_id)
            cursor.execute("""
                UPDATE portability_requests 
                SET status_nc = %s, retry_number = %s, error_description = %s, updated_at = NOW()
                WHERE id = %s
            """, ("MAX_RETRIES_EXCEEDED", current_retry + 1, error_description, mnp_request_id))
            connection.commit()
            return False, "MAX_RETRIES_EXCEEDED", error_description, None

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
