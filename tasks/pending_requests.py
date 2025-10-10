from typing import List, Optional, Dict
from celery_app import app
import requests
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from services.soap_services import create_soap_payload, parse_soap_response, json_to_soap_request, json_from_db_to_soap, parse_soap_response_list, create_status_check_soap
from services.time_services import calculate_countdown, calculate_countdown_working_hours, is_working_hours_now
from datetime import datetime, timedelta
import logging
import pytz
# from db_utils import get_db_connection
from services.database_service import get_db_connection
# from config import logger
from tasks.tasks import submit_to_central_node, check_status, callback_bss
from services.logger import logger, payload_logger, log_payload

# Load environment variables from .env file
load_dotenv()

LOG_FILE = os.getenv('LOG_FILE', 'mnp.log')  # Default log file path
LOG_INFO = os.getenv('LOG_INFO', 'INFO')

# Configure logging to both file and console
# logging.basicConfig(
#     level=LOG_INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(LOG_FILE),  # Container path
#         logging.StreamHandler()  # Also show in docker logs
#     ]
# )

WSDL_SERVICE_SPAIN_MOCK = os.getenv('WSDL_SERVICE_SPAIN_MOCK')

@app.task
def print_periodic_message():
    """A simple task that prints a message with Madrid time - runs every 60 seconds via beat schedule"""
    madrid_tz = pytz.timezone('Europe/Madrid')
    current_time = datetime.now(madrid_tz).strftime('%Y-%m-%d %H:%M:%S %Z%z')
    
    message = "Periodic task from pennding requests module"
    logger.info(message)
    # full_message = f"Hello: {message}"
    # print(full_message)  # Print the complete message
    return message


@app.task
def process_pending_requests():
    """Celery Beat task: Check for pending requests that need processing"""
    try:
        # Get requests that are due for checking
        due_requests = get_due_requests()
        
        if not due_requests:
            message = "No due requests found"
            # logging.info(message)
            logger.info(message)
            return message
        
        # logging.info("Found %d due requests", len(due_requests))
        logger.info("Found %d due requests", len(due_requests))

        processed_count = 0
        error_count = 0
        
        for request in due_requests:
            try:
                # Mark as in progress
                # mark_request_in_progress(request['id'])
                # logging.info("Processing Request ID %s", request['id'])
                logger.info("Processing Request ID %s", request['id'])
                # Process the request asynchronously
                check_single_request.delay(request['id'], request['status_nc'], request['session_code'], request['msisdn'], request['response_status'], request.get('status_bss'))
                processed_count += 1
                
            except (mysql.connector.Error, requests.exceptions.RequestException) as e:
                # logging.error("Error processing request %s: %s", request['id'], e)
                logger.error("Error processing request %s: %s", request['id'], e)
                error_count += 1
                continue
        
        # logging.info("Successfully queued %d requests, %d errors", processed_count, error_count)
        logger.info("Successfully queued %d requests, %d errors", processed_count, error_count)
        return f"Processed {processed_count} requests, {error_count} errors"
                
    except (mysql.connector.Error, requests.exceptions.RequestException) as e:
        # logging.error("Database or request error in process_pending_requests: %s", e)
        logger.error("Database or request error in process_pending_requests: %s", e)

@app.task
def check_single_request(request_id, status_nc, session_code, msisdn, response_status, status_bss):
    """Check a single MNP request and schedule next check if needed"""
    try:
        a, _, _ = calculate_countdown_working_hours(
            timedelta(minutes=0), 
            with_jitter=True)
        a_seconds = int(a.total_seconds())

        if status_nc in ["PENDING_NO_RESPONSE_CODE_RECEIVED", "PENDING_SUBMIT","PENDING_CONFIRMATION","REQUEST_FAILED"]:
            submit_to_central_node.apply_async(
                args=[request_id], 
                countdown=a_seconds
            )

        if status_nc in ["PENDING_RESPONSE","PORT_IN_CONFIRMED"]:
            check_status.apply_async(
                args=[request_id,session_code,msisdn], 
                countdown=a_seconds
            )

        if status_nc in ["REQUEST_RESPONDED","PORT_IN_COMPLETED","PORT_IN_REJECTED","PORT_IN_CANCELLED"] and status_bss in ["PROCESSING"]:
            callback_bss.apply_async(
                args=[request_id, session_code, msisdn, response_status], 
                countdown=a_seconds
            )

    except ValueError as e:
        logger.error("Value error in check_single_request for request %s: %s", request_id, {str(e)})
    except TypeError as e:
        logger.error("Type error in check_single_request for request %s: %s", request_id, {str(e)})
    except mysql.connector.Error as e:
        logger.error("Database error in check_single_request for request %s: %s", request_id, {str(e)})
    except requests.exceptions.RequestException as e:
        logger.error("Request error in check_single_request for request %s: %s", request_id, {str(e)})
    # except Exception as e:
    #     logger.error("Error in check_single_request for request %s: %s", request_id, str(e))
    #     print(f"Error in check_single_request for request {request_id}: {str(e)}")
    #     # Optionally re-raise if you want the task to fail
        # raise
    #     if status_nc == 'ASOL':
    #         # Calculate next check time
    #         countdown_seconds = calculate_countdown()
    #         next_check_time = datetime.now() + timedelta(seconds=countdown_seconds)
            
    #         # Update database with next check time
    #         update_next_check_time(request_id, next_check_time)
            
    #         logging.info("Request %s next check at %s", request_id, next_check_time)
    #     else:
    #         # Request completed, clear next check
    #         update_next_check_time(request_id, None)
    #         logging.info("Request %s completed", request_id)
            
    # except mysql.connector.Error as e:
    #     # logging.error("Database error checking request %s: %s", request_id, e)
    #     logger.error("Database error checking request %s: %s", request_id, e)
    #     # Implement retry logic here
    # except requests.exceptions.RequestException as e:
    #     # logging.error("Request error checking request %s: %s", request_id, e)
    #     logger.error("Request error checking request %s: %s", request_id, e)
    #     # Implement retry logic here

def get_due_requests():
    """Get requests that are due for checking"""
    if not is_working_hours_now():
        print("Outside working hours, no requests will be processed now.")
        logger.info("Outside working hours, no requests will be processed now.")
        return []
    # query = """
    # SELECT id, status_nc, session_code, msisdn, response_status
    # FROM portability_requests 
    # WHERE (status_nc LIKE '%PENDING%' OR status_nc LIKE '%REQUEST_FAILED%')
    # AND (scheduled_at IS NULL OR scheduled_at <= NOW())
    # AND request_type = 'port-in'
    # AND country_code = 'ESP'
    # """
    query = """
    SELECT id, status_nc, session_code, msisdn, response_status, status_bss
    FROM portability_requests 
    WHERE (
        (status_nc LIKE '%PENDING%' OR status_nc LIKE '%REQUEST_FAILED%')
        OR (status_bss LIKE '%PROCESSING%' AND status_nc LIKE '%REQUEST_RESPONDED%')
        OR (status_bss LIKE '%PROCESSING%' AND status_nc LIKE '%PORT_IN%')
    )
    AND (scheduled_at IS NULL OR scheduled_at <= NOW())
    AND request_type = 'port-in'
    AND country_code = 'ESP'
    """

    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()  # Changed from fetchone() to fetchall()
        return results  # Added return statement
    except mysql.connector.Error as e:
        # logging.error("Database error checking request %s", e)
        logger.error("Database error checking request %s", e)
        return []  # Return empty list on error
        # Implement retry logic here
    except requests.exceptions.RequestException as e:
        # logging.error("Request error checking request %s",e)
        logger.error("Request error checking request %s",e)
        return []  # Return empty list on error
        # Implement retry logic here
    # except Error as e:  # Removed requests.exceptions.RequestException as it's not relevant for DB operations
    #     print(f"Database error while fetching due requests: {e}")
    #     return []  # Return empty list on error
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_current_status(request_id: int) -> Optional[str]:
    """Get status of current request"""
    query = """
    SELECT nc_status 
    FROM portability_requests 
    WHERE id = %s
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, (request_id,))  # Note: parameter should be a tuple
        result = cursor.fetchone()
        return result['nc_status'] if result else None
    except mysql.connector.Error as e:
        # logging.error("Database error checking request %s", e)
        logger.error("Database error checking request %s", e)
        return []  # Return empty list on error
        # Implement retry logic here
    except requests.exceptions.RequestException as e:
        # logging.error("Request error checking request %s",e)
        logger.error("Request error checking request %s",e)
        return []  # Return empty list on error
        # Implement retry logic here
    # except Error as e:  # Removed requests.exceptions.RequestException as it's not relevant for DB operations
    #     print(f"Database error while fetching due requests: {e}")
    #     return []  # Return empty list on error
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def mark_request_in_progress(request_id):
    """Mark request as being processed"""
    query = """
    UPDATE portability_requests 
    SET check_status = 'IN_PROGRESS', last_checked_at = NOW() 
    WHERE id = %s
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, (request_id,))  # Note: parameter should be a tuple
        connection.commit()
    except mysql.connector.Error as e:
        # logging.error("Error marking request %s as in progress: %s", request_id, e)
        logger.error("Error marking request %s as in progress: %s", request_id, e)
        if connection:
            connection.rollback()
        raise
        # Implement retry logic here
    except requests.exceptions.RequestException as e:
        # logging.error("Request error checking request %s: %s", request_id, e)
        logger.error("Request error checking request %s: %s", request_id, e)
        # Implement retry logic here
    # except Error as e:  # Removed requests.exceptions.RequestException as it's not relevant for DB operations
    #     print(f"Database error while fetching due requests: {e}")
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
