from fastapi import HTTPException
import mysql.connector
from mysql.connector import Error
from config import settings
from services.time_services import calculate_countdown_working_hours, normalize_datetime, parse_timestamp
from datetime import timedelta, datetime
from services.logger import logger, payload_logger, log_payload
import aiomysql
from typing import Dict, Any

async def async_get_db_connection():
    """Create and return async MySQL database connection"""
    try:
        connection = await aiomysql.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            db=settings.DB_NAME,
            autocommit=False
        )
        return connection
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}") from e

def get_db_connection():
    """Create and return MySQL database connection"""
    try:
        # connection = mysql.connector.connect(**MYSQL_CONFIG)
        connection = mysql.connector.connect(**settings.mysql_config)
        return connection
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}") from e

def save_portin_request_db(alta_data: dict):
    """
    1. Save it to the database immediately.
    """
    connection = None
    cursor = None
    try:
        # 1. & 2. Create and save the DB record
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Extract data from the request
        abonado_data = alta_data.get('abonado', {})
        doc_data = abonado_data.get('documentoIdentificacion', {})
        
        # Get MSISDN and also populate phone_number for backward compatibility
        msisdn = alta_data.get('MSISDN')
        
        # SQL insert query - updated with all fields from the new table structure
        print("Inserting new portability request into database with code:", alta_data.get('codigoSesion'))
        request_type="port-in"
        # status_bss="bss_portin_received_by_mnp"
        status_bss="PROCESSING"
        status_nc="PENDING_SUBMIT"
        # scheduled_at=calculate_countdown_working_hours(timedelta(minutes=0),with_jitter=False)
        # initial_delta = timedelta(seconds=0)
        initial_delta = timedelta(seconds=-5)  # Negative for "before"
        _, _, scheduled_at = calculate_countdown_working_hours(
            delta=initial_delta, 
            with_jitter=False)

        insert_query = """
            INSERT INTO portability_requests
            (session_code, request_date, donor_operator, recipient_operator, 
             id_type, id_number, contract_code, nrn_receptor, 
             porting_window_date, iccid, msisdn, phone_number,
             status, request_type, status_bss,status_nc,
             scheduled_at,
             requested_at, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, NOW(), NOW())
        """
        
        values = (
            alta_data.get('codigoSesion'),
            alta_data.get('fechaSolicitudPorAbonado'),
            alta_data.get('codigoOperadorDonante'),
            alta_data.get('codigoOperadorReceptor'),
            doc_data.get('tipo'),
            doc_data.get('documento'),
            alta_data.get('codigoContrato'),
            alta_data.get('NRNReceptor'),
            alta_data.get('fechaVentanaCambio'),
            alta_data.get('ICCID'),
            msisdn,  # MSISDN field
            msisdn,  # Also populate phone_number with same value
            'PENDING_SUBMIT',  # Initial status
            # alta_data.get('fechaSolicitudPorAbonado'),  # Use request date for requested_at
            request_type,
            status_bss,
            status_nc,
            scheduled_at,
            alta_data.get('fechaSolicitudPorAbonado'),  # Use request date for requested_at
        )
        
        cursor.execute(insert_query, values)
        connection.commit()
        
        # Get the ID of the newly inserted record
        new_request_id = cursor.lastrowid
        logger.info(f"Inserted new portability request with ID: {new_request_id}")
        
        return new_request_id
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Database error creating port-in request: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

    
# def save_cancel_request_db(request_data: dict):
def save_cancel_request_db(request_data: dict, request_type: str = 'CANCEL', country_code: str = "ESP") -> int:
    """
    Save cancellation request to database (synchronous)
    """
    logger.debug("ENTER save_cancel_request_db() %s", request_data)
    required_fields = ["reference_code", "cancellation_reason", "cancellation_initiated_by_donor", "msisdn"]
    for field in required_fields:
        if field not in request_data:
            raise ValueError(f"Missing required field: {field}")
    
    connection = None
    cursor = None
    try:
        # Calculate scheduled_at time
        initial_delta = timedelta(seconds=-5)
        _, _, scheduled_at = calculate_countdown_working_hours(
            delta=initial_delta, 
            with_jitter=False
        )
        # request_type="CANCEL"
        # status_bss="bss_portin_received_by_mnp"
        status_bss="PROCESSING"
        status_nc="PENDING_SUBMIT"

        # Get database connection
        connection = get_db_connection()  # Your sync connection function
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO portability_requests 
        (reference_code, msisdn, request_type, cancellation_reason, cancellation_initiated_by_donor, 
        session_code, scheduled_at, status_nc, status_bss, country_code, cancel_request_id,created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        RETURNING id
        """

        values = (
            request_data["reference_code"],
            request_data["msisdn"],
            request_type,
            request_data["cancellation_reason"],
            request_data["cancellation_initiated_by_donor"],
            request_data.get("session_code"),
            scheduled_at,
            status_nc,
            status_bss,
            country_code,
            request_data["cancel_request_id"]
            )
        # Execute and commit
        cursor.execute(insert_query, values)
        request_id = cursor.fetchone()[0]  # Get the returned ID
        connection.commit()
        
        logger.info("Saved cancellation request with ID: %s, scheduled at: %s", request_id, scheduled_at)
        return request_id
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error("Failed to save cancellation request: %s", e)
        raise
    finally:
        # Always close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def save_cancel_request_db_online(request_data: dict, request_type: str = 'CANCEL', country_code: str = "ESP") -> int:
    """
    Save cancellation request to database (synchronous)
    """
    logger.debug("ENTER save_cancel_request_db_online() %s", request_data)
    required_fields = ["reference_code", "cancellation_reason", "cancellation_initiated_by_donor"]
    for field in required_fields:
        if field not in request_data:
            raise ValueError(f"Missing required field: {field}")
    
    connection = None
    cursor = None
    try:
        # Calculate scheduled_at time
        initial_delta = timedelta(seconds=-5)
        _, _, scheduled_at = calculate_countdown_working_hours(
            delta=initial_delta, 
            with_jitter=False
        )
        status_bss = "PROCESSING"
        status_nc = "PENDING_SUBMIT"

        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # FIXED: Removed one NOW() from VALUES - 11 placeholders for 11 values
        insert_query = """
        INSERT INTO portability_requests 
        (reference_code, request_type, cancellation_reason, cancellation_initiated_by_donor, 
        scheduled_at, status_nc, status_bss, country_code, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        RETURNING id
        """

        values = (
            request_data["reference_code"],
            request_type,
            request_data["cancellation_reason"],
            request_data["cancellation_initiated_by_donor"],
            # request_data.get("session_code"),
            scheduled_at,
            status_nc,
            status_bss,
            country_code
        )
        
        # Execute and commit
        cursor.execute(insert_query, values)
        request_id = cursor.fetchone()[0]  # Get the returned ID
        connection.commit()
        
        logger.info("Saved cancellation request with ID: %s, scheduled at: %s", request_id, scheduled_at)
        return request_id
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error("Failed to save cancellation request: %s", e)
        raise
    finally:
        # Always close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# def check_if_cancel_request_id is presnt id db:
def check_if_cancel_request_id_in_db(request_data: dict) -> bool:
    """
    Check if cancel_request_id present in database (synchronous)
    Returns True if found, False if not found
    """
    logger.debug("ENTER check_if_cancel_request_id_in_db() %s", request_data)
    
    required_fields = ["cancel_request_id"]
    for field in required_fields:
        if field not in request_data:
            raise ValueError(f"Missing required field: {field}")
    
    connection = None
    cursor = None
    try:
        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor()  # No dictionary=True
        
        cancel_request_id = request_data["cancel_request_id"]
        
        # Query to check if the ID exists in portability_requests table
        query = """
            SELECT COUNT(*) as count 
            FROM portability_requests 
            WHERE id = %s
        """
        cursor.execute(query, (cancel_request_id,))
        result = cursor.fetchone()
        
        # Access tuple by index (COUNT(*) is first column)
        exists = result[0] > 0 if result else False
        
        logger.debug("Cancel request ID %s exists in DB: %s", cancel_request_id, exists)
        return exists
        
    except Exception as e:
        logger.error("Error checking cancel_request_id %s in DB: %s", 
                    request_data.get("cancel_request_id"), str(e))
        return False
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

# def check_if_cancel_request_id is presnt id db:
def check_if_cancel_request_id_in_db_online(request_data: dict) -> bool:
    """
    Check if cancel_request_id present in database (synchronous)
    Returns True if found, False if not found
    """
    logger.debug("ENTER check_if_cancel_request_id_in_db_online() %s", request_data)
    
    # Enhanced validation with better error messages
    if not request_data:
        raise ValueError("Request data is empty or None")
    
    required_fields = ["reference_code"]
    missing_fields = [field for field in required_fields if field not in request_data or not request_data[field]]
    
    if missing_fields:
        raise ValueError(f"Missing required field(s): {', '.join(missing_fields)}. Received data: {request_data}")
    
    connection = None
    cursor = None
    try:
        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor()
        
        reference_code = request_data["reference_code"]
        
        # Validate reference_code is not empty
        if not reference_code or not reference_code.strip():
            logger.error("Reference code is empty or whitespace")
            return False
        
        # Query to check if the ID exists in portability_requests table
        query = """
            SELECT COUNT(*) as count 
            FROM portability_requests 
            WHERE reference_code = %s
        """
        # logger.debug("Executing query to check reference_code: %s", query)
        cursor.execute(query, (reference_code.strip(),))
        result = cursor.fetchone()
        
        # Access tuple by index (COUNT(*) is first column)
        exists = result[0] > 0 if result else False
        
        logger.debug("Reference code '%s' exists in DB: %s", reference_code, exists)
        return exists
        
    except Exception as e:
        logger.error("Error checking reference_code '%s' in DB: %s", 
                    request_data.get("reference_code"), str(e))
        return False
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def save_portability_request_new(alta_data: dict, request_type: str = 'PORT_IN', country_code: str = "ESP") -> int:
    """
    Save portability request to optimized table structure
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Extract data from the request using NEW field names
        subscriber_data = alta_data.get('subscriber', {})
        doc_data = subscriber_data.get('identification_document', {})
        personal_data = subscriber_data.get('personal_data', {})
        first_name = personal_data.get('first_name', 'UNKNOWN_SUBSCRIBER') # Default if missing
        first_surname = personal_data.get('first_surname', 'UNKNOWN_SUBSCRIBER')
        second_surname = personal_data.get('second_surname', 'UNKNOWN_SUBSCRIBER')

        status_bss="PROCESSING"
        status_nc="PENDING_SUBMIT"

        # Calculate scheduled_at
        initial_delta = timedelta(seconds=-5)
        _, _, scheduled_at = calculate_countdown_working_hours(
            delta=initial_delta, 
            with_jitter=False
        )

        print(f"save_portability_request(): Inserting new portability request into database with session_code: {alta_data.get('session_code')}")
        logger.debug("save_portability_request(): Inserting new portability request into database with session_code: %s", alta_data.get('session_code'))
        logger.debug("save_portability_request(): Inserting new portability request: %s", alta_data)
        
        insert_query = """
        INSERT INTO portability_requests (
            session_code, donor_operator, recipient_operator,
            document_type, document_number, contract_number, routing_number,
            desired_porting_date, iccid, msisdn,
            request_type, status_bss, status_nc, scheduled_at, requested_at,
            country_code, first_name, first_surname, second_surname, nationality
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = (
            alta_data.get('session_code'),
            alta_data.get('donor_operator'),
            alta_data.get('recipient_operator'),
            doc_data.get('document_type'),
            doc_data.get('document_number'),
            alta_data.get('contract_number'),
            alta_data.get('routing_number'),
            alta_data.get('desired_porting_date'),
            alta_data.get('iccid'),
            alta_data.get('msisdn'),
            request_type,
            status_bss,
            status_nc,
            scheduled_at,
            alta_data.get('requested_at'),
            country_code,
            first_name,  # Using first_name instead of name_surname
            first_surname,  # New field
            second_surname,  # New field
            personal_data.get('nationality', 'ESP')  # Default to ESP if missing
        )

        # FIX: Remove RETURNING id and use lastrowid
        # insert_query = """
        # INSERT INTO portability_requests (
        #     session_code, donor_operator, recipient_operator,
        #     document_type, document_number, contract_number, routing_number,
        #     desired_porting_date, iccid, msisdn,
        #     request_type, status_bss, status_nc, scheduled_at, requested_at,
        #     country_code, name_surname, nationality
        # ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        # """

        # values = (
        #     alta_data.get('session_code'),
        #     alta_data.get('donor_operator'),
        #     alta_data.get('recipient_operator'),
        #     doc_data.get('document_type'),
        #     doc_data.get('document_number'),
        #     alta_data.get('contract_number'),
        #     alta_data.get('routing_number'),
        #     alta_data.get('desired_porting_date'),
        #     alta_data.get('iccid'),
        #     alta_data.get('msisdn'),
        #     request_type,
        #     status_bss,
        #     status_nc,
        #     scheduled_at,
        #     alta_data.get('requested_at'),
        #     country_code,
        #     name_surname,
        #     personal_data.get('nationality', 'ESP')
        # )

        cursor.execute(insert_query, values)
        connection.commit()
        
        # FIX: Use lastrowid (no RETURNING clause)
        new_request_id = cursor.lastrowid
        logger.info("Inserted new portability request with ID: %s", new_request_id)
        return new_request_id
        
    # except Exception as e:
    #     if connection:
    #         connection.rollback()
    #     logger.error("Database error creating port-in request: %s", str(e))
    #     raise
    # finally:
    #     if cursor:
    #         cursor.close()
    #     if connection and connection.is_connected():
    #         connection.close()

    except Error as e:
        if connection:
            connection.rollback()
    
        error_msg =""
        if e.errno == 1364:  # Field doesn't have a default value
            # Extract the field name from the error message
            error_msg = str(e)
            if "Field '" in error_msg:
                field_name = error_msg.split("Field '")[1].split("' doesn't")[0]
                logger.error("Missing required field '%s' in database insert", field_name)
                raise ValueError(f"Missing required field: {field_name}") from e
            else:
                logger.error("Missing required field: %s", error_msg)
                raise ValueError("Missing required field in database insert") from e
        else:
            logger.error("MySQL error (errno: %s): %s", e.errno, str(e))
            raise     
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def insert_portout_response_to_db(parsed_data):
    """
    Inserts parsed Port-Out response data into MySQL tables:
    - portout_metadata
    - portout_request
    using mysql.connector.

    Args:
        parsed_data (dict): Output of parse_portout_response()

    Insert each time whne new port-out response is received and total_records > 0    
    """

    try:
        # 1. Get database connection
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)

        # 2. Insert into portout_metadata
        meta = parsed_data["response_info"]

        insert_meta_sql = """
            INSERT INTO portout_metadata
                (response_code, response_description, paged_request_code,
                 total_records, is_last_page)
            VALUES (%s, %s, %s, %s, %s)
        """
        meta_values = (
            meta.get("response_code"),
            meta.get("response_description"),
            meta.get("paged_request_code"),
            meta.get("total_records"),
            1 if str(meta.get("is_last_page")).lower() in ("true", "1") else 0
        )

        cursor.execute(insert_meta_sql, meta_values)
        metadata_id = cursor.lastrowid  # link to requests

        # 3. Insert each port-out request
        status_nc = 'RECEIVED'
        status_bss = 'PENDING'
        insert_req_sql = """
            INSERT INTO portout_request (
                metadata_id, notification_id, creation_date, synchronized,
                reference_code, status, state_date, creation_date_request,
                reading_mark_date, state_change_deadline, subscriber_request_date,
                donor_operator_code, receiver_operator_code, extraordinary_donor_activation,
                contract_code, receiver_NRN, port_window_date, port_window_by_subscriber, MSISDN,
                subscriber_id_type, subscriber_id_number, subscriber_first_name,
                subscriber_last_name_1, subscriber_last_name_2, created_at, updated_at, status_nc, status_bss, subscriber_type, company_name
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, NOW(), NOW(), %s, %s, %s, %s
            )
        """

        for req in parsed_data["requests"]:
            sub = req["subscriber"]

            if check_if_port_out_request_in_db(req):
                continue

            company_name = sub.get("razon_social")
            logger.debug("Company Name: %s", company_name)
            if company_name:  # This checks for non-empty and non-None
                subscriber_type = "COMPANY"
            else:
                subscriber_type = "PERSON"
            logger.debug("Subscriber_type: %s", subscriber_type)    

            req_values = (
                metadata_id,
                req.get("notification_id"),
                normalize_datetime(req.get("creation_date")),
                1 if str(req.get("synchronized")).lower() in ("true", "1") else 0,
                req.get("reference_code"),
                req.get("status"),
                normalize_datetime(req.get("state_date")),
                normalize_datetime(req.get("creation_date_request")),
                normalize_datetime(req.get("reading_mark_date")),
                normalize_datetime(req.get("state_change_deadline")),
                normalize_datetime(req.get("subscriber_request_date")),
                req.get("donor_operator_code"),
                req.get("receiver_operator_code"),
                1 if str(req.get("extraordinary_donor_activation")).lower() in ("true", "1") else 0,
                req.get("contract_code"),
                req.get("receiver_NRN"),
                normalize_datetime(req.get("port_window_date")),
                1 if str(req.get("port_window_by_subscriber")).lower() in ("true", "1") else 0,
                req.get("MSISDN"),
                sub.get("id_type"),
                sub.get("id_number"),
                sub.get("first_name"),
                sub.get("last_name_1"),
                sub.get("last_name_2"),
                status_nc,
                status_bss,
                subscriber_type,
                company_name
            )
            cursor.execute(insert_req_sql, req_values)

        # 44. Commit all inserts
        connection.commit()
        print(f"Successfully inserted metadata_id={metadata_id} with {len(parsed_data['requests'])} requests")

    except Error as e:
        print(f" MySQL Error: {e}")
        connection.rollback()

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

# def check_if_cancel_request_id is presnt id db:
def check_if_port_out_request_in_db_1(request_data: dict) -> bool:
    """
    Check if Port-Out request present in database (synchronous)
    Returns True if found, False if not found
    """
    logger.debug("ENTER check_if_port_out_request_in_db() with data: %s", request_data)
    reference_code = request_data["reference_code"]
    logger.debug("ENTER check_if_port_out_request_in_db() %s",reference_code)
    
    # Enhanced validation with better error messages
    if not request_data:
        raise ValueError("Request data is empty or None")
    
    required_fields = ["reference_code"]
    missing_fields = [field for field in required_fields if field not in request_data or not request_data[field]]
    
    if missing_fields:
        raise ValueError(f"Missing required field(s): {', '.join(missing_fields)}. Received data: {request_data}")
    
    # Get the first request from the list
    first_request = request_data["requests"][0]
    
    # Now get reference_code from the nested request object
    reference_code = first_request.get("reference_code")
    
    if not reference_code:
        logger.error("reference_code not found in nested request object")
        return False
    
    logger.debug("Found reference_code: %s", reference_code)

    connection = None
    cursor = None
    try:
        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # reference_code = request_data["reference_code"]
        
        # Validate reference_code is not empty
        if not reference_code or not reference_code.strip():
            logger.error("Reference code is empty or whitespace")
            return False
        
        # Query to check if the ID exists in portability_requests table
        query = """
            SELECT COUNT(*) as count 
            FROM portout_request 
            WHERE reference_code = %s
        """
        # logger.debug("Executing query to check reference_code: %s", query)
        cursor.execute(query, (reference_code.strip(),))
        result = cursor.fetchone()
        
        # Access tuple by index (COUNT(*) is first column)
        exists = result[0] > 0 if result else False
        
        logger.debug("Reference code '%s' exists in portout_request table: %s", reference_code, exists)
        return exists
        
    except Exception as e:
        logger.error("Error checking reference_code '%s' in portout_requestB: %s", 
                    request_data.get("reference_code"), str(e))
        return False
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def check_if_port_out_request_in_db(request_data: dict) -> bool:
    """
    Check if a Port-Out request is present in the database.
    Accepts either:
        {"reference_code": "..."}  OR
        {"requests": [{"reference_code": "..."}]}
    Returns True if found, False otherwise.
    """
    logger.debug("ENTER check_if_port_out_request_in_db() with data: %s", request_data)

    if not request_data:
        raise ValueError("Invalid input: request_data is empty or None")

    # Determine where to extract reference_code from
    if "reference_code" in request_data:
        reference_code = request_data.get("reference_code")
    elif "requests" in request_data and request_data["requests"]:
        reference_code = request_data["requests"][0].get("reference_code")
    else:
        raise ValueError("Missing required field: reference_code")

    if not reference_code or not reference_code.strip():
        raise ValueError("reference_code is empty or invalid")

    logger.debug("Checking reference_code: %s", reference_code)

    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        query = """
            SELECT COUNT(*) 
            FROM portout_request 
            WHERE reference_code = %s
        """
        cursor.execute(query, (reference_code.strip(),))
        result = cursor.fetchone()
        exists = (result[0] if result else 0) > 0

        logger.debug("Reference code '%s' exists in portout_request: %s", reference_code, exists)
        return exists

    except Exception as e:
        logger.error("Error checking reference_code '%s' in portout_request: %s", reference_code, str(e))
        return False

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def save_portability_request_person_legal(alta_data: dict, request_type: str = 'PORT_IN', country_code: str = "ESP") -> int:
    """
    Save portability request (either person or legal entity) into portability_requests table.
    """
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        subscriber_data = alta_data.get('subscriber', {})
        doc_data = subscriber_data.get('identification_document', {})
        personal_data = subscriber_data.get('personal_data', {})

        # --- Determine entity type ---
        is_legal_entity = bool(alta_data.get('is_legal_entity', False))
        subscriber_type = subscriber_data.get('subscriber_type', 'person')
        is_legal_entity_val = 1 if (is_legal_entity or subscriber_type == 'company') else 0

        logger.debug("---- save_portability_request_person_legal(): %s", alta_data)

        # --- Handle company vs person ---
        if is_legal_entity_val:
            company_name_val = personal_data.get('company_name') or alta_data.get('company_name') or 'UNKNOWN_COMPANY'
            first_name = company_name_val
            first_surname = ''
            second_surname = ''
            name_surname = company_name_val
        else:
            first_name = personal_data.get('first_name', 'UNKNOWN_SUBSCRIBER')
            first_surname = personal_data.get('first_surname', '')
            second_surname = personal_data.get('second_surname', '')
            name_surname = f"{first_name} {first_surname} {second_surname}".strip()
            company_name_val = None

        status_bss = "PROCESSING"
        status_nc = "PENDING_SUBMIT"

        # --- Calculate scheduled_at ---
        initial_delta = timedelta(seconds=-5)
        _, _, scheduled_at = calculate_countdown_working_hours(
            delta=initial_delta,
            with_jitter=False
        )

        insert_query = """
        INSERT INTO portability_requests (
            country_code, request_type, session_code,
            donor_operator, recipient_operator,
            document_type, document_number,
            contract_number, routing_number,
            desired_porting_date, iccid, msisdn,
            status_bss, status_nc, scheduled_at, requested_at,
            first_name, first_surname, second_surname, nationality,
            subscriber_type, is_legal_entity, company_name, name_surname
        ) VALUES (
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        """

        values = (
            country_code,
            request_type,
            alta_data.get('session_code'),
            alta_data.get('donor_operator'),
            alta_data.get('recipient_operator'),
            doc_data.get('document_type'),
            doc_data.get('document_number'),
            alta_data.get('contract_number'),
            alta_data.get('routing_number'),
            alta_data.get('desired_porting_date'),
            alta_data.get('iccid'),
            alta_data.get('msisdn'),
            status_bss,
            status_nc,
            scheduled_at,
            alta_data.get('requested_at'),
            first_name,
            first_surname,
            second_surname,
            personal_data.get('nationality', 'ESP'),
            subscriber_type,
            is_legal_entity_val,
            company_name_val,
            name_surname
        )

        logger.debug(
            "Executing INSERT: company_name=%s | is_legal_entity=%s | subscriber_type=%s",
            company_name_val, is_legal_entity_val, subscriber_type
        )

        cursor.execute(insert_query, values)
        connection.commit()

        new_request_id = cursor.lastrowid
        logger.info(
            "Inserted new portability request with ID: %s, Type: %s",
            new_request_id,
            'LEGAL' if is_legal_entity_val else 'PERSONAL'
        )

        return new_request_id

    except Error as e:
        if connection:
            connection.rollback()

        if getattr(e, "errno", None) == 1364:
            msg = str(e)
            if "Field '" in msg:
                field_name = msg.split("Field '")[1].split("' doesn't")[0]
                logger.error("Missing required field '%s' in DB insert", field_name)
                raise ValueError(f"Missing required field: {field_name}") from e
            raise ValueError("Missing required field in database insert") from e
        else:
            logger.error("MySQL error (%s): %s", getattr(e, "errno", "?"), e)
            raise

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def save_return_request_db(request_data: dict, request_type: str = 'RETURN') -> int:
    """
    Save return request to database (synchronous)
    """
    logger.debug("ENTER return_request_db() %s", request_data)
    required_fields = ["request_date", "msisdn"]
    for field in required_fields:
        if field not in request_data:
            raise ValueError(f"Missing required field: {field}")
    
    connection = None
    cursor = None
    try:
        # Calculate scheduled_at time
        initial_delta = timedelta(seconds=-5)
        _, _, scheduled_at = calculate_countdown_working_hours(
            delta=initial_delta, 
            with_jitter=False
        )
        status_bss = "PROCESSING"
        status_nc = "PENDING_SUBMIT"

        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # FIXED: Removed one NOW() from VALUES - 11 placeholders for 11 values
        insert_query = """
        INSERT INTO return_requests 
        (request_type, msisdn, request_date, 
        scheduled_at, status_nc, status_bss, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        RETURNING id
        """

        values = (
            request_type,
            request_data["msisdn"],
            request_data["request_date"],
            scheduled_at,
            status_nc,
            status_bss
        )
        
        # Execute and commit
        cursor.execute(insert_query, values)
        request_id = cursor.fetchone()[0]  # Get the returned ID
        connection.commit()
        
        logger.info("Saved return request with ID: %s, scheduled at: %s", request_id, scheduled_at)
        return request_id
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error("Failed to save return request: %s", e)
        raise
    finally:
        # Always close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# def check_if_cancel_request_id is presnt id db:
def check_if_cancel_return_request_in_db(request_data: dict) -> bool:
    """
    Check if return_request_id present in database (synchronous)
    Returns True if found, False if not found
    """
    logger.debug("ENTER check_if_cancel_return_request_in_db() %s", request_data)
    
    # Enhanced validation with better error messages
    if not request_data:
        raise ValueError("Request data is empty or None")
    
    required_fields = ["reference_code"]
    missing_fields = [field for field in required_fields if field not in request_data or not request_data[field]]
    
    if missing_fields:
        raise ValueError(f"Missing required field(s): {', '.join(missing_fields)}. Received data: {request_data}")
    
    connection = None
    cursor = None
    try:
        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor()
        
        reference_code = request_data["reference_code"]
        
        # Validate reference_code is not empty
        if not reference_code or not reference_code.strip():
            logger.error("Reference code is empty or whitespace")
            return False
        
        # Query to check if the ID exists in portability_requests table
        query = """
            SELECT COUNT(*) as count 
            FROM return_requests 
            WHERE reference_code = %s
        """
        # logger.debug("Executing query to check reference_code: %s", query)
        cursor.execute(query, (reference_code.strip(),))
        result = cursor.fetchone()
        
        # Access tuple by index (COUNT(*) is first column)
        exists = result[0] > 0 if result else False
        
        logger.debug("Reference code %s found in DB True/False: %s", reference_code, exists)
        return exists
        
    except Exception as e:
        logger.error("Error checking reference_code '%s' in DB: %s", 
                    request_data.get("reference_code"), str(e))
        return False
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def save_cancel_return_request_db(request_data: dict, request_type: str = 'CANCEL') -> int:
    """
    Save cancel return request to database (synchronous)
    """
    logger.debug("ENTER save_cancel_return_request_db() %s", request_data)
    required_fields = ["reference_code", "cancellation_reason"]
    for field in required_fields:
        if field not in request_data:
            raise ValueError(f"Missing required field: {field}")
    
    connection = None
    cursor = None
    try:
        # Calculate scheduled_at time
        initial_delta = timedelta(seconds=-5)
        _, _, scheduled_at = calculate_countdown_working_hours(
            delta=initial_delta, 
            with_jitter=False
        )
        status_bss = "PROCESSING"
        status_nc = "PENDING_SUBMIT"
        reference_code = request_data["reference_code"]

        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # FIXED: Removed one NOW() from VALUES - 11 placeholders for 11 values
        insert_query = """
        INSERT INTO return_requests 
        (request_type, cancellation_reason, reference_code,
        scheduled_at, status_nc, status_bss, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        RETURNING id
        """

        values = (
            request_type,
            request_data["cancellation_reason"],
            reference_code,
            scheduled_at,
            status_nc,
            status_bss
        )
        
        logger.debug("Insert values: %s", values)
        # Execute and commit
        cursor.execute(insert_query, values)

        # Log the actual SQL that was executed
        logger.debug("Actual SQL executed: %s", cursor.statement)

        request_id = cursor.fetchone()[0]  # Get the returned ID
        connection.commit()
        
        logger.info("Saved cancel return request with ID: %s,  ref_code = %s, scheduled at: %s %s", request_id, reference_code, scheduled_at, insert_query)
        return request_id
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error("Failed to save cancel return request: %s", e)
        raise
    finally:
        # Always close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def update_return_request_with_nc_response(reference_code: str, nc_response: Dict[str, Any]) -> None:
    """
    Synchronous version for updating return_requests table
    Always updates scheduled_at for next check when status is pending
    """
    connection = None
    cursor = None

    logger.info("ENTER update_return_request_with_nc_response() ref_code nc_response: %s  %s",
                    reference_code, nc_response)

    try:
        # Extract fields from NC response
        success = nc_response.get('success')
        response_code = nc_response.get('response_code')
        description = nc_response.get('description')
        response_status = nc_response.get('status')
        cancellation_reason = nc_response.get('status_reason')
        
        # Parse timestamp fields
        status_date = parse_timestamp(nc_response.get('status_date'))
        creation_date = parse_timestamp(nc_response.get('creation_date'))
        subscriber_cancellation_date = parse_timestamp(nc_response.get('subscriber_cancellation_date'))
        change_window_date = parse_timestamp(nc_response.get('change_window_date'))
        
        recipient_operator_code = nc_response.get('recipient_operator_code')
        donor_operator_code = nc_response.get('donor_operator_code')
        
        # Determine status_nc based on response_status and success
        status_nc = "PENDING"
        status_bss = "PROCESSING"
        if success and response_status:
            status_nc = f"RETURN_{response_status}"
            status_bss = f"CHANGED_TO_{response_status}"
        elif not success:
            status_nc = "FAILED"
            status_bss = "FAILED"
        
        # Calculate scheduled_at - ALWAYS set for next check if status is not final
        if success and response_status not in ['BDEF']:  # Add other final statuses as needed
            scheduled_at = datetime.now() + timedelta(seconds=settings.TIME_DELTA_FOR_RETURN_STATUS_CHECK)
        else:
            # For final statuses, don't schedule next check
            scheduled_at = None
        
        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Always update the record with new NC response data
        update_query = """
        UPDATE return_requests 
        SET response_code = %s, 
            description = %s, 
            response_status = %s, 
            cancellation_reason = %s, 
            status_date = %s, 
            creation_date = %s, 
            subscriber_cancellation_date = %s, 
            change_window_date = %s, 
            recipient_operator_code = %s, 
            donor_operator_code = %s, 
            scheduled_at = %s, 
            status_nc = %s, 
            status_bss = %s, 
            updated_at = NOW()
        WHERE reference_code = %s
        """

        values = (
            response_code, 
            description, 
            response_status, 
            cancellation_reason, 
            status_date, 
            creation_date, 
            subscriber_cancellation_date, 
            change_window_date, 
            recipient_operator_code, 
            donor_operator_code, 
            scheduled_at,
            status_nc,
            status_bss,
            reference_code
        )
        
        # Execute the update
        cursor.execute(update_query, values)
        connection.commit()

        # Check if any rows were affected
        if cursor.rowcount == 0:
            logger.warning("No rows updated for reference_code: %s", reference_code)
        else:
            logger.info("Successfully updated return request for reference_code: %s with status: %s, scheduled_at: %s", 
                       reference_code, response_status, scheduled_at)
                
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error("Failed to update return request for reference_code %s: %s", reference_code, str(e))
        # Don't re-raise to avoid breaking the main flow
    finally:
        # Always close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()