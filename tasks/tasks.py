# tasks.py
from typing import List, Optional, Dict
from celery_app import app
import requests
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
# from soap_utils import create_soap_payload, parse_soap_response, json_to_soap_request, json_from_db_to_soap, parse_soap_response_list, create_status_check_soap
from services.soap_services import create_soap_payload, parse_soap_response, json_to_soap_request, json_from_db_to_soap, parse_soap_response_list, create_status_check_soap
# from time_utils import calculate_countdown
from services.time_services import calculate_countdown
from datetime import datetime, timedelta
import logging
import pytz
# from db_utils import get_db_connection
from services.database_service import get_db_connection
from config import logger, settings
from services.time_services import calculate_countdown_working_hours


# Load environment variables from .env file
load_dotenv()

# Your database configuration (should be in a config file)
MYSQL_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'mnp_database'),
    'port': os.getenv('DB_PORT', '3306')
}

LOG_FILE = os.getenv('LOG_FILE', 'mnp.log')  # Default log file path
LOG_INFO = os.getenv('LOG_INFO', 'INFO')

# Configure logging to both file and console
logging.basicConfig(
    level=LOG_INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),  # Container path
        logging.StreamHandler()  # Also show in docker logs
    ]
)

WSDL_SERVICE_SPAIN_MOCK = os.getenv('WSDL_SERVICE_SPAIN_MOCK')
WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS = os.getenv('WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS')

def get_db_connection_1():
    """Create and return MySQL database connection"""
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        return connection
    except Error as e:
        print(f"Database connection error: {str(e)}")
        raise

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
    connection = None
    try:
        # 1. Get database connection
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # 2. Fetch the request data from database
        cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        mnp_request = cursor.fetchone()
        
        if not mnp_request:
            print(f"Request {mnp_request_id} not found in database")
            return

        # 3. Prepare your SOAP envelope (use your existing logic)
        print(f"Request {mnp_request} found, preparing SOAP payload...")

        # Convert JSON to SOAP request
        # soap_request = json_to_soap_request(mnp_request)
        print("Generated SOAP Request:")
        soap_payload = json_from_db_to_soap(mnp_request)  # function to create SOAP
        print(soap_payload)

        # 4. Try to send the request to Central Node
        if not WSDL_SERVICE_SPAIN_MOCK:
            raise ValueError("WSDL_SERVICE_SPAIN_MOCK environment variable is not set.")
        response = requests.post(WSDL_SERVICE_SPAIN_MOCK, 
                               data=soap_payload, 
                               headers={'Content-Type': 'text/xml'}, 
                               timeout=30)
        response.raise_for_status()

        # 5. Parse the SOAP response (use your existing logic)
        # session_code, status = parse_soap_response_list(response.text,)
        response_code, description, session_code = parse_soap_response_list(response.text, ["codigoRespuesta", "descripcion", "codigoReferencia"])
        print(f"Received response: response_code={response_code}, description={description}, session_code={session_code}")

        # Assign status from response_code or description as appropriate
        status = response_code  # or use description if that's the intended status
        # status_nc="REQUEST_CONFIRMED" if response_code == 'ASOL' else "REQUEST_FAILED"
        if response_code is not None:
            status_nc = "REQUEST_CONFIRMED" if response_code.strip().upper() == 'ASOL' else "REQUEST_FAILED"
        else:
            status_nc = "REQUEST_FAILED"

        # 6. Update the database with response
        update_query = """
            UPDATE portability_requests 
            SET response_status = %s, description = %s, status_nc = %s, updated_at = NOW() 
            WHERE id = %s
        """
        cursor.execute(update_query, (response_code, description, status_nc, mnp_request_id))
        connection.commit()

        # 7. IF THE STATUS IS PENDING (e.g., 'ASOL'), QUEUE THE NEXT CHECK!
        if status == 'ASOL':
            # Schedule the next task: check status in 60 seconds.
            countdown_seconds=calculate_countdown()
            # Convert countdown to actual datetime
            scheduled_datetime = datetime.now() + timedelta(seconds=countdown_seconds)
        
            # Update database with the actual scheduled time
            update_query = """
                UPDATE portability_requests 
                SET scheduled_at = %s 
                WHERE id = %s
            """
            cursor.execute(update_query, (scheduled_datetime, mnp_request_id))
            connection.commit()
    
        # Schedule the task with countdown
            check_status.apply_async(args=[mnp_request_id], countdown=countdown_seconds)
            logging.info("Scheduled check for id: %s at %s (in %s seconds)", mnp_request_id, scheduled_datetime, countdown_seconds)
            print("Scheduled check for id: %s at %s (in %s seconds)", mnp_request_id, scheduled_datetime, countdown_seconds)

        # If it's a final status, callback the BSS immediately
        if status in ('ACON', 'APOR', 'RECHAZADO'):
            callback_bss.delay(mnp_request_id)

    except requests.exceptions.RequestException as exc:
        # If something fails, retry the task after a delay!
        print(f"Request failed, retrying: {exc}")
        self.retry(exc=exc, countdown=60)
    except Error as e:
        print(f"Database error: {e}")
        # You might want to retry on database errors too
        self.retry(exc=e, countdown=30)
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

@app.task(bind=True, max_retries=10)
def check_status(self, mnp_request_id):
    """
    Task to check the status of a single MSISDN at the Central Node.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Check if request exists and is not in final status
        cursor.execute("SELECT status FROM portability_requests WHERE id = %s", (mnp_request_id,))
        result = cursor.fetchone()
        
        if not result:
            return
        if result['status'] in ('ACON', 'APOR', 'RECHAZADO'):  # Final statuses
            return

        status_current = result['status']
        # Prepare the SOAP 'Consultar' request for this ONE MSISDN
        consultar_payload = create_status_check_soap(mnp_request_id)  # Check status request SOAP

        if not WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS:
            raise ValueError("WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS environment variable is not set.")
        response = requests.post(WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS, 
                               data=consultar_payload, 
                               headers={'Content-Type': 'text/xml'}, 
                               timeout=30)
        response.raise_for_status()

        # new_status = parse_soap_response(response.text)  # Parse the response
        response_code, description, session_code = parse_soap_response_list(response.text, ["codigoRespuesta", "descripcion", "codigoReferencia"])
        print(f"Received check response: response_code={response_code}, description={description}, session_code={session_code}")


        # Update the DB
        # update_query = "UPDATE portability_requests SET status = %s, updated_at = NOW() WHERE id = %s"
        # cursor.execute(update_query, (response_code, mnp_request_id))
        # connection.commit()

        # If it's still pending, queue the next check with exponential backoff
        if response_code == 'ASOL':
            # Still same status, updated scheduled_at for next check - within same timenad
            # countdown_seconds=calculate_countdown()
            # Convert countdown to actual datetime
            # scheduled_datetime = datetime.now() + timedelta(seconds=countdown_seconds)
            adjusted_delta, status, scheduled_datetime = calculate_countdown_working_hours(
                                                        delta=settings.TIME_DELTA_FOR_STATUS_CHECK, 
                                                        with_jitter=True
                                                                                    )      
            # Update database with the actual scheduled time
            update_query = """
                UPDATE portability_requests 
                SET status = %s,
                SET scheduled_at = %s,
                updated_at = NOW() 
                WHERE id = %s
            """
            cursor.execute(update_query, (response_code,scheduled_datetime, mnp_request_id))
            connection.commit()

            # update_query = "UPDATE portability_requests SET status = %s, updated_at = NOW() WHERE id = %s"
            # cursor.execute(update_query, (response_code, mnp_request_id))
            # connection.commit()
            # next_check_in = 60 * (self.request.retries + 1)  # 60s, 120s, 180s...
            # check_status.apply_async(args=[mnp_request_id], countdown=next_check_in)

        # If it's a final status, callback the BSS
        if response_code in ('ACON', 'APOR', 'RECHAZADO'):
            status_nc = "REQUEST_RESPONDED" if response_code in ('ACON', 'APOR', 'RECHAZADO') else "REQUEST_CONFIRMED"
            update_query = """
                UPDATE portability_requests 
                SET status = %s,
                SET status_nc = %s,
                updated_at = NOW() 
                WHERE id = %s
            """
            cursor.execute(update_query, (response_code,status_nc, mnp_request_id))
            connection.commit()

            callback_bss.delay(mnp_request_id)

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

def create_soap_payload(mnp_request):
    """Your function to create SOAP payload from request data"""
    # Implement your SOAP envelope creation logic here
    pass
