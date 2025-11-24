from services.logger_simple import log_payload, logger
from config import settings
from services.database_service import get_db_connection
from services.time_services import calculate_countdown, calculate_countdown_working_hours, is_working_hours_now
import mysql.connector
from mysql.connector import Error
import requests
from celery_app import app
from porting.spain_nc_return import submit_to_central_node_return, submit_to_central_node_cancel_return
import json

def get_return_accs_perme_requests():
    """Get requests that are due for checking"""

    if settings.IGNORE_WORKING_HOURS:
        # Process regardless of working hours
        pass
    else:
        if not is_working_hours_now():
            msg = "Outside working hours, no return requests will be processed now."
            return msg
        
    logger.debug("ENTER get_return_due_requests()")
    query = """
    SELECT *
    FROM return_requests
    WHERE (scheduled_at IS NULL OR scheduled_at <= NOW())
    AND response_code LIKE '%ACCS PERME%';
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

@app.task
def process_pending_return_requests():
    """Celery Beat task: Check for pending return requests that need processing"""
    
    processed_count = 0
    error_count = 0
    
    try:
        # Get requests that are due for checking
        due_requests = get_return_accs_perme_requests()
        
        if isinstance(due_requests, str):
            logger.info("Info message from get_return_accs_perme_requests: %s", due_requests)
            return due_requests
        
        if not due_requests:
            message = "No due return ACCS PERME requests found"
            logger.info(message)
            return message
        
        logger.info("Found %d due ACCS PERME requests", len(due_requests))

        for request in due_requests:
            try:
                request_type = request.get('request_type', 'N/A')
                request_id = request.get('id')
                response_code_old = request.get('response_code')  
                msisdn = ""
                success = False
                response_code = None
                description = None

                logger.info("Processing Return request ID %s, type %s", request_id, request_type)
                
                if request_type == 'CANCEL':
                    msisdn = request.get('msisdn', 'N/A')
                    success, response_code, description = submit_to_central_node_cancel_return(request_id)

                elif request_type == 'RETURN':
                    # Fixed: was using 'id' instead of 'request_id'
                    success, response_code, description = submit_to_central_node_return(request_id)
                
                else:
                    logger.warning("Unknown request type '%s' for request ID %s", request_type, request_id)
                    error_count += 1
                    continue

                # Check if status actually changed
                status_changed = (response_code != response_code_old)
                    
                if success and status_changed:
                    processed_count += 1
                    logger.debug("Successfully processed request ID %s", request_id)
                        
                    # Call callback function asynchronously
                    try:
                        callback_bss_return.delay(
                            request_type=request_type,  # Use actual request_type instead of hardcoded "RETURN"
                            response_code=response_code,
                            description=description,
                            msisdn=msisdn
                        )
                        logger.debug("Callback queued for request ID %s", request_id)
                    except Exception as callback_error:
                        logger.error("Failed to queue callback for request ID %s: %s", request_id, callback_error)
                        error_count += 1
                    
                elif not success:
                    logger.error("Failed to process request ID %s: %s", request_id, description)
                    error_count += 1
                else:
                    logger.debug("No status change for request ID %s (old: %s, new: %s)", 
                                request_id, response_code_old, response_code)
                        
            except KeyError as e:
                logger.error("Missing required field in request data: %s. Request: %s", e, request)
                error_count += 1
            except Exception as e:
                logger.error("Error processing request ID %s: %s", request.get('id', 'unknown'), e)
                error_count += 1

        # Final summary
        summary_message = f"Completed processing: {processed_count} successful, {error_count} errors"
        logger.info(summary_message)
        return summary_message

    except Exception as e:
        error_message = f"Error in process_pending_return_requests: {e}"
        logger.error(error_message)
        return error_message
        
@app.task(bind=True, max_retries=3)
def callback_bss_return(self, request_type, response_code, description, msisdn=""):
    """
    Updated callback function for return requests with simplified parameters
    
    Args:
        request_type: Type of request (e.g., "RETURN_CANCEL")
        response_code: codigoRespuesta (eg 0000 0000/AREC NRNRE/AREC NRNRE)
        msisdn: Mobile number
        description: Optional description message
    """
    logger.debug("ENTER callback_bss_return() with request_type %s msisdn %s response_code %s",
                 request_type, msisdn, response_code)
    
    # Prepare JSON payload with essential fields only
    payload = {
        "request_type": request_type,
        "msisdn": msisdn,
        "response_code": response_code,
        "response_status": response_code,  # Map response_code to status
        "description": description or f"Retrun cancel status update for return request {msisdn}"
    }
   
    logger.debug("Call back BSS for return request: %s", payload)
    bss_webhook_url = settings.BSS_WEBHOOK_URL
    json_payload = json.dumps(payload, ensure_ascii=False)

    connection = None
    cursor = None
    
    try:
        response = requests.post(
            settings.BSS_WEBHOOK_URL,
            data=json_payload,
            headers=settings.get_headers_bss(),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT,
            verify=settings.SSL_VERIFICATION
        )

        # Check if request was successful
        if response.status_code == 200:
            logger.info(
                "Callback BSS successful for return request: msisdn %s, response_code: %s to webhook %s", 
                msisdn, response_code, bss_webhook_url
            )

            # Update database with the actual scheduled time
            try: 
                update_query = """
                    UPDATE return_requests 
                    SET status_bss = %s,
                    updated_at = NOW() 
                    WHERE msisdn = %s AND response_code = %s
                """
                connection = get_db_connection()
                cursor = connection.cursor(dictionary=True)
                
                # Map response_code to appropriate status_bss value
                status_bss = f"CHANGED_TO_{response_code}"
                
                cursor.execute(update_query, (status_bss, msisdn, response_code))
                connection.commit()
                
                logger.debug(
                    "Database updated for return request msisdn %s with status_bss: %s", 
                    msisdn, status_bss
                )
                return True
                
            except Exception as db_error:
                logger.error("Database update failed for return request msisdn %s: %s", msisdn, str(db_error))
                return False
        else:
            logger.error(
                "Webhook failed for return request msisdn: %s Status: %s, Response: %s", 
                msisdn, response.status_code, response.text
            )
            return False
            
    except requests.exceptions.Timeout as exc:
        logger.error("Webhook timeout for return request msisdn %s", msisdn)
        self.retry(exc=exc, countdown=120)
        return False
    except requests.exceptions.ConnectionError as exc:
        logger.error("Webhook connection error for return request msisdn %s", msisdn)
        self.retry(exc=exc, countdown=120)
        return False
    except requests.exceptions.RequestException as exc:
        logger.error("Webhook error for return request msisdn %s: %s", msisdn, str(exc))
        self.retry(exc=exc, countdown=120)
        return False
    except Exception as exc:
        logger.error("Unexpected error in callback_bss_return for msisdn %s: %s", msisdn, str(exc))
        return False
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()