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
from tasks.tasks import submit_to_central_node, check_status, callback_bss, submit_to_central_node_cancel
from services.logger import logger, payload_logger, log_payload
from config import settings

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
    logger.info("ENTER process_pending_requests()")
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
                logger.info("Processing Request ID %s", request['id'])
                # Process the request asynchronously
                check_single_request.delay(request['id'], request['status_nc'], 
                                           request['session_code'], request['msisdn'], 
                                           request['response_status'], request.get('status_bss'), 
                                           request.get('reference_code'), request.get('request_type'))
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
def check_single_request(request_id, status_nc, session_code, msisdn, response_status, status_bss,reference_code, request_type):
    """Check a single MNP request and schedule next check if needed"""
    logger.debug("ENTER check_single_request() with req_id: %s, status_nc %s, status_bss %s, msisdn %s, reference_code %s", request_id, status_nc, status_bss, msisdn, reference_code)
    # logger.debug("Func: check single request -- %s reference_code %s", request_id, reference_code)
    try:
        a, _, _ = calculate_countdown_working_hours(
            timedelta(minutes=0), 
            with_jitter=True)
        a_seconds = int(a.total_seconds())

        if status_nc in ["PENDING_NO_RESPONSE_CODE_RECEIVED", "PENDING_SUBMIT","PENDING_CONFIRMATION"]:
            if request_type == "CANCELLATION" and response_status not in ['ACAN',"400","404"]:
                submit_to_central_node_cancel.apply_async(
                    args=[request_id],
                    countdown=a_seconds
                )
            else:
                if response_status not in ['ASOL', 'ACON', 'AREC', 'APOR', 'ACAN']:
                    submit_to_central_node.apply_async(
                        args=[request_id],
                        countdown=a_seconds
                )

        if status_nc in ["PENDING_RESPONSE","PORT_IN_CONFIRMED","SUBMITTED"]:
            check_status.apply_async(
                args=[request_id,session_code,msisdn, reference_code], 
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

def get_due_requests():
    """Get requests that are due for checking"""

    if settings.IGNORE_WORKING_HOURS:
        # Process regardless of working hours
        pass
    else:
        if not is_working_hours_now():
            logger.info("Outside working hours, no requests will be processed now.")
            return []
        
    logger.debug("ENTER get_due_requests()")
    query = """
    SELECT id, status_nc, session_code, msisdn, response_status, status_bss, reference_code, request_type
    FROM portability_requests 
    WHERE (
        (status_nc LIKE '%PENDING%' OR status_nc LIKE '%REQUEST_FAILED%')
        OR (status_bss LIKE '%PROCESSING%' AND status_nc LIKE '%REQUEST_RESPONDED%')
        OR (status_bss LIKE '%PROCESSING%' AND status_nc LIKE '%PORT_IN%')
        OR (status_bss LIKE '%PROCESSING%' AND status_nc LIKE '%SUBMITTED%')
    )
    AND (scheduled_at IS NULL OR scheduled_at <= NOW())
    AND UPPER(request_type) IN ('CANCELLATION', 'PORT_IN')
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
