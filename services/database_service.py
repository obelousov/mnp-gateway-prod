from fastapi import HTTPException
import mysql.connector
from mysql.connector import Error
from config import settings, logger
from services.time_services import calculate_countdown_working_hours
from datetime import timedelta

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