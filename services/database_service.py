from fastapi import HTTPException
import mysql.connector
from mysql.connector import Error
from config import settings
from services.time_services import calculate_countdown_working_hours
from datetime import timedelta
from services.logger import logger, payload_logger, log_payload
import aiomysql

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
