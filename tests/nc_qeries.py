from typing import List, Optional, Dict, Any
from celery_app import app
import requests
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from xml.etree import ElementTree as ET
import re

from services.soap_services import parse_soap_response_list, create_status_check_soap_nc, create_initiate_soap, parse_soap_response_dict, parse_soap_response_dict_flat, json_from_db_to_soap_new, json_from_db_to_soap_new_1
from services.soap_services import parse_soap_response_nested, parse_soap_response_nested_multi
# from time_utils import calculate_countdown
from services.time_services import calculate_countdown
from datetime import datetime, timedelta
import logging
import pytz
# from db_utils import get_db_connection
from services.database_service import get_db_connection
from config import settings
from services.time_services import calculate_countdown_working_hours
from services.logger_simple import logger
from services.logger import log_payload

WSDL_SERVICE_SPAIN_MOCK = settings.WSDL_SERVICE_SPAIN_MOCK
WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS = settings.WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS
WSDL_SERVICE_SPAIN_MOCK_CANCEL = settings.WSDL_SERVICE_SPAIN_MOCK_CANCEL
BSS_WEBHOOK_URL = settings.BSS_WEBHOOK_URL

PENDING_REQUESTS_TIMEOUT = settings.PENDING_REQUESTS_TIMEOUT  # seconds

# Get timezone from environment or default to Europe/Madrid
timezone_str = settings.TIME_ZONE
container_tz = pytz.timezone(timezone_str)


def check_status(mnp_request_id, session_code, msisdn):
    """
    Task to check the status of a single MSISDN at the Central Node.
    """
    connection = None
    logger.info("ENTER check status() with req_id %s session_code %s msisdn %s", mnp_request_id, session_code,msisdn)
    APIGEE_PORTABILITY_URL=settings.APIGEE_PORTABILITY_URL

    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        consultar_payload = create_status_check_soap_nc(mnp_request_id, session_code, msisdn)  # Check status request SOAP
        # Conditional payload logging
        log_payload('NC', 'CHECK_STATUS_NC', 'REQUEST', str(consultar_payload))
        headers=settings.get_soap_headers('ConsultarProcesosPortabilidadMovil')
        print("Check status headers:", headers)

        # return
    
        if not APIGEE_PORTABILITY_URL:
            raise ValueError("APIGEE_PORTABILITY_URL environment variable is not set.")
        
        response = requests.post(APIGEE_PORTABILITY_URL,
                               data=consultar_payload,
                               headers=settings.get_soap_headers('ConsultarProcesosPortabilidadMovil'),
                               timeout=settings.APIGEE_API_QUERY_TIMEOUT)
        response.raise_for_status()

        log_payload('NC', 'CHECK_STATUS', 'RESPONSE', str(response.text))

        result = parse_soap_response_dict(response.text,["codigoRespuesta", "descripcion", "error_field", "error_description"])

        print(f"NC Response Code: {result['codigoRespuesta']}")
        print(f"NC Description: {result['descripcion']}")
        print(f"NC Error Field: {result['error_field']}")
        print(f"NC Error Description: {result['error_description']}")
        
        # print(parse_consultar_procesos_response(response.text)    )

        return response.text
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
            # callback_bss.delay(mnp_request_id, reference_code, session_code, response_code, description, None, None)
            # def callback_bss(self, mnp_request_id, reference_code, session_code, 
            # response_status, description=None, error_fields=None, porting_window_date=None):
            
            return "Final status received for id: %s, status: %s", mnp_request_id, response_code
            
            # callback_bss.delay(mnp_request_id)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def initiate_session():
    """
    Task to initiate a session with the Central Node API.
    """
    username = settings.APIGEE_USERNAME
    access_code = settings.APIGEE_ACCESS_CODE
    operator_code = settings.APIGEE_OPERATOR_CODE
    APIGEE_ACCESS_URL = settings.APIGEE_ACCESS_URL

    logger.info("ENTER initiate_session()")
    try:
        # Create SOAP payload for session initiation
        consultar_payload = create_initiate_soap(username, access_code, operator_code)
        
        # Conditional payload logging
        log_payload('NC', 'INITIATE_SESSION', 'REQUEST', str(consultar_payload))
        headers=settings.get_soap_headers('IniciarSesion'),
        print("Initiate session headers:", headers)

        if not APIGEE_ACCESS_URL:
            raise ValueError("WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS environment variable is not set.")
        
        response = requests.post(APIGEE_ACCESS_URL, 
                               data=consultar_payload,
                               headers=settings.get_soap_headers('IniciarSesion'),
                               timeout=settings.APIGEE_API_QUERY_TIMEOUT)
        log_payload('NC', 'INITIATE_SESSION', 'RESPONCE', str(response.text))

        result_dict = parse_soap_response_dict_flat(response.text, ["codigoRespuesta", "descripcion", "codigoSesion"])
    
        # Now these assignments are type-safe
        response_code = result_dict["codigoRespuesta"]
        description = result_dict["descripcion"]
        session_code = result_dict["codigoSesion"]

        print(f"Response Code: {response_code}")      # 0000 00000
        print(f"Description: {description}")          # La operación se ha realizado con éxito
        print(f"Session Code: {session_code}")        # c317cddcd50a8c682d3d794663771dd8

        # Check for success
        if response_code == "0000 00000" and session_code:
            return session_code
        else:
            logger.error("Failed to initiate session: %s %s",{response_code},{description})
            return None
        
    except Exception as e:
        logger.error("Error initiating session: %s", {str(e)})
        raise

def submit_to_central_node(mnp_request_id, session_code):
    """
    Task to submit a porting request to the Central Node.
    This runs in the background.
    """
    logger.debug("ENTER submit_to_central_node with req_id %s", mnp_request_id)
    connection = None
    APIGEE_PORTABILITY_URL = settings.APIGEE_PORTABILITY_URL
    try:
        # 1. Get database connection
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # 2. Fetch the request data from database
        # cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        current_time = datetime.now(container_tz)
        print(f"Submit to NC: current time {current_time}")
        # cursor.execute("SELECT * FROM portability_requests WHERE id = %s",(mnp_request_id))
        cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        mnp_request = cursor.fetchone()
        status_nc_old = mnp_request['status_nc'] if mnp_request else 'NOT_FOUND'
        
        print(f"Submit to NC: {mnp_request['status_nc']}, {mnp_request['response_status']}")
        if not mnp_request:
            print(f"Submit to NC: request {mnp_request_id} not found or not yet scheduled")
            return f"Submit to NC: request {mnp_request_id} not found or not yet scheduled"

        response_status = mnp_request['response_status']

        # uncomment below
        # if response_status in ['ASOL', 'ACON', 'AREC', 'APOR', 'ACAN']:
        #     return f"Request {mnp_request_id} is in status {response_status}, no further submission needed"

        # Convert JSON to SOAP request
        logger.debug("Submit to NC: Generated SOAP Request:")
        # soap_payload = json_from_db_to_soap(mnp_request)  # function to create SOAP
        soap_payload = json_from_db_to_soap_new_1(mnp_request, session_code)  # function to create SOAP
        # print(soap_payload)
        # Conditional payload logging
        log_payload('NC', 'PORT_IN', 'REQUEST', str(soap_payload))

        # 4. Try to send the request to Central Node
        if not APIGEE_PORTABILITY_URL:
            raise ValueError("WSDL_SERVICE_SPAIN_MOCK environment variable is not set.")
        # current_retry = request.retries
        # logger.info("Attempt %d for request %s", current_retry+1, mnp_request_id)
        # print(f"Submit to NC: Attempt {current_retry+1} for request {mnp_request_id}")

        response = requests.post(APIGEE_PORTABILITY_URL, 
                               data=soap_payload,
                               headers=settings.get_soap_headers('CrearSolicitudIndividualAltaPortabilidadMovil'),
                               timeout=settings.APIGEE_API_QUERY_TIMEOUT)
        response.raise_for_status()

        # 5. Parse the SOAP response (use your existing logic)
        # session_code, status = parse_soap_response_list(response.text,)
        response_code, description, reference_code = parse_soap_response_list(response.text, ["codigoRespuesta", "descripcion", "codigoReferencia"])
        print(f"Submit to NC: Received response: response_code={response_code}, description={description}, reference_code={reference_code}")

        # Conditional payload logging
        log_payload('NC', 'PORT_IN', 'RESPONSE', str(response.text))
        return

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

    # except requests.exceptions.RequestException as exc:
    #     current_retry = self.request.retries
    
    #     # Convert exception to string for database storage
    #     error_description = str(exc)
    
    #     if connection is None:
    #         connection = get_db_connection()
    #     cursor = connection.cursor(dictionary=True)
    
    #     if current_retry < self.max_retries:
    #         # Still have retries left - update and retry
    #         print(f"Request failed, retrying ({current_retry + 1}/{self.max_retries}): {exc}")
    #         status_nc = "REQUEST_FAILED"
        
    #         update_query = """
    #         UPDATE portability_requests 
    #         SET status_nc = %s, retry_number = %s, error_description = %s, updated_at = NOW() 
    #         WHERE id = %s
    #     """
    #         cursor.execute(update_query, (status_nc, current_retry + 1, error_description, mnp_request_id))
    #         connection.commit()
        
    #         # Retry with exponential backoff
    #         # countdown = 60 * (2 ** current_retry)  # 60, 120, 240 seconds
    #         countdown = 60
    #         raise self.retry(exc=exc, countdown=countdown)
    #     else:
    #         # Max retries exceeded - final failure
    #         print(f"Max retries exceeded for request {mnp_request_id}: {exc}")
    #         status_nc = "MAX_RETRIES_EXCEEDED"
        
    #         update_query = """
    #             UPDATE portability_requests 
    #             SET status_nc = %s, retry_number = %s, error_description = %s, updated_at = NOW() 
    #             WHERE id = %s
    #         """
    #         cursor.execute(update_query, (status_nc, current_retry + 1, error_description, mnp_request_id))
    #         connection.commit()
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def submit_to_central_node_new(mnp_request_id, session_code):
    """
    Task to submit a porting request to the Central Node.
    This runs in the background.
    """
    logger.debug("ENTER submit_to_central_node with req_id %s", mnp_request_id)
    APIGEE_PORTABILITY_URL = settings.APIGEE_PORTABILITY_URL
    
    try:
        # 1. Get database connection just to fetch request data
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # 2. Fetch the request data from database
        cursor.execute("SELECT * FROM portability_requests WHERE id = %s", (mnp_request_id,))
        mnp_request = cursor.fetchone()
        
        if not mnp_request:
            logger.error(f"Submit to NC: request {mnp_request_id} not found")
            return f"Submit to NC: request {mnp_request_id} not found"

        print(f"Submit to NC: Processing request - Status: {mnp_request.get('status_nc')}, Response: {mnp_request.get('response_status')}")

        # 3. Generate SOAP payload
        # soap_payload = create_submit_soap(mnp_request, session_code)
        soap_payload = json_from_db_to_soap_new_1(mnp_request, session_code)  # function to create SOAP
        
        logger.debug("Submit to NC: Generated SOAP Request:")
        log_payload('NC', 'PORT_IN', 'REQUEST', str(soap_payload))
        # return

        # 4. Try to send the request to Central Node
        if not APIGEE_PORTABILITY_URL:
            raise ValueError("APIGEE_PORTABILITY_URL environment variable is not set.")

        print(f"Sending request to NC URL: {APIGEE_PORTABILITY_URL}")
        
        # return
        response = requests.post(
            APIGEE_PORTABILITY_URL, 
            data=soap_payload,
            headers=settings.get_soap_headers('CrearSolicitudIndividualAltaPortabilidadMovil'),
            timeout=settings.APIGEE_API_QUERY_TIMEOUT
        )
        response.raise_for_status()

        # 5. Parse the SOAP response
        print("Parsing SOAP response...",response.text)
        log_payload('NC', 'PORT_IN', 'RESPONSE', str(response.text))
        response_code, description, reference_code = parse_soap_response_list(
            response.text, 
            ["codigoRespuesta", "descripcion", "codigoReferencia"]
        )
        
        print(f"=== PARSED RESPONSE ===")
        print(f"Response Code: {response_code}")
        print(f"Description: {description}")
        print(f"Reference Code: {reference_code}")
        print(f"Session Code: {session_code}")
        print("=======================")

        # 6. Log the response payload
        log_payload('NC', 'PORT_IN', 'RESPONSE', str(response.text))
        
        logger.info(f"Successfully submitted request {mnp_request_id} to Central Node")
        return {
            'success': True,
            'response_code': response_code,
            'description': description,
            'reference_code': reference_code,
            'session_code': session_code,
            'raw_response': response.text
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error submitting to Central Node: {e}")
        print(f"=== HTTP ERROR ===")
        print(f"Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response Status: {e.response.status_code}")
            print(f"Response Text: {e.response.text}")
            log_payload('NC', 'PORT_IN', 'RESPONSE', str(e.response.text))
        print("=================")
        return {'success': False, 'error': f'HTTP Error: {str(e)}'}
    
    except Exception as e:
        logger.error(f"Unexpected error in submit_to_central_node: {e}")
        print(f"=== UNEXPECTED ERROR ===")
        print(f"Error: {e}")
        print("=======================")
        return {'success': False, 'error': f'Unexpected Error: {str(e)}'}
        
    finally:
        # Clean up database connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection and connection.is_connected():
            connection.close()

def parse_consultar_procesos_response_simple(soap_response):
    """
    Simple namespace-agnostic parser for ConsultarProcesosPortabilidadMovil response
    """
    result_dict = {
        'codigoRespuesta': None,
        'descripcion': None,
        'procesos': []
    }
    
    try:
        root = ET.fromstring(soap_response)
        
        # Remove namespaces for simpler parsing
        def remove_namespaces(xml_string):
            return re.sub(r' xmlns(:ns2)?="[^"]+"', '', xml_string)
        
        # Parse without namespaces
        clean_xml = remove_namespaces(soap_response)
        root = ET.fromstring(clean_xml)
        
        # Find elements without namespace concerns
        codigo_elem = root.find('.//codigoRespuesta')
        if codigo_elem is not None:
            result_dict['codigoRespuesta'] = codigo_elem.text
            
        desc_elem = root.find('.//descripcion')
        if desc_elem is not None:
            result_dict['descripcion'] = desc_elem.text
        
        # Find all registro elements
        registros = root.findall('.//registro')
        
        for registro in registros:
            proceso = {}
            
            # Extract all possible fields
            fields = [
                'tipoProceso', 'codigoReferencia', 'estado', 
                'codigoOperadorDonante', 'codigoOperadorReceptor',
                'fechaVentanaCambio', 'fechaCreacion', 'fechaMarcaLectura',
                'fechaConfirmacion', 'fechaRechazo', 'causaRechazo',
                'fechaCancelacion', 'causaCancelacion'
            ]
            
            for field in fields:
                elem = registro.find(f'.//{field}')
                if elem is not None and elem.text:
                    proceso[field] = elem.text
            
            if proceso:  # Only add if we found any data
                result_dict['procesos'].append(proceso)
            
    except ET.ParseError as e:
        logger.error(f"XML parsing error: {e}")
    except Exception as e:
        logger.error(f"Error parsing response: {e}")
    
    return result_dict

def parse_consultar_procesos_response(soap_response):
    """
    Parse ConsultarProcesosPortabilidadMovil response
    """
    result_dict = {
        'codigoRespuesta': None,
        'descripcion': None,
        'procesos': []
    }
    
    try:
        root = ET.fromstring(soap_response)
        
        # Define namespaces - use wildcard or specific ones
        namespaces = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns2': 'http://nc.aopm.es/v1-10',
            'ns9': 'http://nc.aopm.es/v1-10/portabilidad',
            'ns14': 'http://nc.aopm.es/v1-10'  # Alternative namespace seen in your responses
        }
        
        # Find the SOAP Body - try different approaches
        body = None
        for ns in ['soap', 'S']:  # Try both common SOAP namespace prefixes
            body = root.find(f'.//{{{http://schemas.xmlsoap.org/soap/envelope/}}}Body')
            if body is not None:
                break
        
        if body is None:
            # Try without namespace
            body = root.find('.//Body')
        
        if body is None:
            logger.error("SOAP Body not found in response")
            return result_dict
        
        # Extract main response fields
        codigo_elem = body.find('.//codigoRespuesta')
        if codigo_elem is not None:
            result_dict['codigoRespuesta'] = codigo_elem.text
            
        desc_elem = body.find('.//descripcion')
        if desc_elem is not None:
            result_dict['descripcion'] = desc_elem.text
        
        # Find process records - try different namespace combinations
        registros = []
        
        # Method 1: Try with explicit namespace
        registros = body.findall('.//ns9:registro', namespaces)
        if not registros:
            # Method 2: Try with wildcard namespace
            registros = body.findall('.//{http://nc.aopm.es/v1-10/portabilidad}registro')
        if not registros:
            # Method 3: Try without namespace
            registros = body.findall('.//registro')
        
        for registro in registros:
            proceso = {}
            
            # Helper function to safely extract text
            def get_text(element, xpath):
                elem = element.find(xpath)
                return elem.text if elem is not None else None
            
            # Extract fields with safe handling
            proceso['tipoProceso'] = get_text(registro, './/tipoProceso')
            proceso['codigoReferencia'] = get_text(registro, './/codigoReferencia')
            proceso['estado'] = get_text(registro, './/estado')
            proceso['codigoOperadorDonante'] = get_text(registro, './/codigoOperadorDonante')
            proceso['codigoOperadorReceptor'] = get_text(registro, './/codigoOperadorReceptor')
            proceso['fechaVentanaCambio'] = get_text(registro, './/fechaVentanaCambio')
            proceso['fechaCreacion'] = get_text(registro, './/fechaCreacion')
            
            # Optional fields
            proceso['fechaMarcaLectura'] = get_text(registro, './/fechaMarcaLectura')
            proceso['fechaConfirmacion'] = get_text(registro, './/fechaConfirmacion')
            proceso['fechaRechazo'] = get_text(registro, './/fechaRechazo')
            proceso['causaRechazo'] = get_text(registro, './/causaRechazo')
            proceso['fechaCancelacion'] = get_text(registro, './/fechaCancelacion')
            proceso['causaCancelacion'] = get_text(registro, './/causaCancelacion')
            
            # Remove None values
            proceso = {k: v for k, v in proceso.items() if v is not None}
            result_dict['procesos'].append(proceso)
            
    except ET.ParseError as e:
        logger.error(f"XML parsing error: {e}")
    except Exception as e:
        logger.error(f"Error parsing process records: {e}")
    
    return result_dict

def parse_portabilidad_xml(xml_string):
    # Define namespaces
    namespaces = {
        'S': 'http://schemas.xmlsoap.org/soap/envelope/',
        'ns2': 'http://nc.aopm.es/v1-10',
        'ns9': 'http://nc.aopm.es/v1-10/portabilidad'
    }

    root = ET.fromstring(xml_string)

    # Navigate to respuestaConsultarProcesosPortabilidadMovil
    respuesta = root.find('.//ns9:respuestaConsultarProcesosPortabilidadMovil', namespaces)
    if respuesta is None:
        raise ValueError("No respuestaConsultarProcesosPortabilidadMovil found in XML")

    # Extract general response info
    codigo_respuesta = respuesta.findtext('.//ns2:codigoRespuesta', namespaces=namespaces)
    descripcion = respuesta.findtext('.//ns2:descripcion', namespaces=namespaces)

    # Parse all registros (can be multiple)
    registros = []
    for reg in respuesta.findall('.//ns9:registro', namespaces):
        registro_data = {
            'tipoProceso': reg.findtext('.//ns2:tipoProceso', namespaces=namespaces),
            'codigoReferencia': reg.findtext('.//ns2:codigoReferencia', namespaces=namespaces),
            'valorInicial': reg.findtext('.//ns2:rangoMSISDN/ns2:valorInicial', namespaces=namespaces),
            'valorFinal': reg.findtext('.//ns2:rangoMSISDN/ns2:valorFinal', namespaces=namespaces),
            'codigoOperadorDonante': reg.findtext('.//ns2:codigoOperadorDonante', namespaces=namespaces),
            'codigoOperadorReceptor': reg.findtext('.//ns2:codigoOperadorReceptor', namespaces=namespaces),
            'estado': reg.findtext('.//ns2:estado', namespaces=namespaces),
            'fechaVentanaCambio': reg.findtext('.//ns2:fechaVentanaCambio', namespaces=namespaces),
            'fechaCreacion': reg.findtext('.//ns2:fechaCreacion', namespaces=namespaces),
            'fechaMarcaLectura': reg.findtext('.//ns2:fechaMarcaLectura', namespaces=namespaces),
            'fechaConfirmacion': reg.findtext('.//ns2:fechaConfirmacion', namespaces=namespaces),
            'fechaRechazo': reg.findtext('.//ns2:fechaRechazo', namespaces=namespaces),
            'causaRechazo': reg.findtext('.//ns2:causaRechazo', namespaces=namespaces),
            'fechaCancelacion': reg.findtext('.//ns2:fechaCancelacion', namespaces=namespaces),
            'causaCancelacion': reg.findtext('.//ns2:causaCancelacion', namespaces=namespaces)
        }
        registros.append(registro_data)

    # Return structured result
    return {
        'codigoRespuesta': codigo_respuesta,
        'descripcion': descripcion,
        'registros': registros
    }

#  this is working version to get flat dict
def parse_check_status_resonse(xml_string):
    # Define namespaces
    namespaces = {
        'S': 'http://schemas.xmlsoap.org/soap/envelope/',
        'ns2': 'http://nc.aopm.es/v1-10',
        'ns9': 'http://nc.aopm.es/v1-10/portabilidad'
    }

    root = ET.fromstring(xml_string)

    # Navigate to respuestaConsultarProcesosPortabilidadMovil
    respuesta = root.find('.//ns9:respuestaConsultarProcesosPortabilidadMovil', namespaces)
    if respuesta is None:
        raise ValueError("No respuestaConsultarProcesosPortabilidadMovil found in XML")

    # Extract general response info
    data = {
        'codigoRespuesta': respuesta.findtext('.//ns2:codigoRespuesta', namespaces=namespaces),
        'descripcion': respuesta.findtext('.//ns2:descripcion', namespaces=namespaces)
    }

    # Get first registro (or iterate if needed)
    registro = respuesta.find('.//ns9:registro', namespaces)
    if registro is not None:
        data.update({
            'tipoProceso': registro.findtext('.//ns2:tipoProceso', namespaces=namespaces),
            'codigoReferencia': registro.findtext('.//ns2:codigoReferencia', namespaces=namespaces),
            'valorInicial': registro.findtext('.//ns2:rangoMSISDN/ns2:valorInicial', namespaces=namespaces),
            'valorFinal': registro.findtext('.//ns2:rangoMSISDN/ns2:valorFinal', namespaces=namespaces),
            'codigoOperadorDonante': registro.findtext('.//ns2:codigoOperadorDonante', namespaces=namespaces),
            'codigoOperadorReceptor': registro.findtext('.//ns2:codigoOperadorReceptor', namespaces=namespaces),
            'estado': registro.findtext('.//ns2:estado', namespaces=namespaces),
            'fechaVentanaCambio': registro.findtext('.//ns2:fechaVentanaCambio', namespaces=namespaces),
            'fechaCreacion': registro.findtext('.//ns2:fechaCreacion', namespaces=namespaces),
            'fechaMarcaLectura': registro.findtext('.//ns2:fechaMarcaLectura', namespaces=namespaces),
            'fechaConfirmacion': registro.findtext('.//ns2:fechaConfirmacion', namespaces=namespaces),
            'fechaRechazo': registro.findtext('.//ns2:fechaRechazo', namespaces=namespaces),
            'causaRechazo': registro.findtext('.//ns2:causaRechazo', namespaces=namespaces),
            'fechaCancelacion': registro.findtext('.//ns2:fechaCancelacion', namespaces=namespaces),
            'causaCancelacion': registro.findtext('.//ns2:causaCancelacion', namespaces=namespaces),
        })

    return data

import xml.etree.ElementTree as ET
from typing import Any

def parse_crear_solicitud_response(xml_string: str) -> dict[str, Any]:
    """
    Parses SOAP XML for 'respuestaCrearSolicitudIndividualAltaPortabilidadMovil'.
    Works for both success and error responses.
    Returns a flat dictionary including an automatic 'status' field.
    """

    namespaces = {
        'S': 'http://schemas.xmlsoap.org/soap/envelope/',
        'ns2': 'http://nc.aopm.es/v1-10',
        'ns9': 'http://nc.aopm.es/v1-10/portabilidad'
    }

    root = ET.fromstring(xml_string)

    # Locate the response element
    response = root.find('.//ns9:respuestaCrearSolicitudIndividualAltaPortabilidadMovil', namespaces)
    if response is None:
        raise ValueError("No respuestaCrearSolicitudIndividualAltaPortabilidadMovil found in XML")

    # Core fields (always present)
    codigo_respuesta = response.findtext('.//ns2:codigoRespuesta', namespaces=namespaces)
    descripcion = response.findtext('.//ns2:descripcion', namespaces=namespaces)

    data: dict[str, Any] = {
        "codigoRespuesta": codigo_respuesta,
        "descripcion": descripcion,
    }

    # Add status (success if code starts with '0000')
    if codigo_respuesta and codigo_respuesta.strip().startswith("0000"):
        data["status"] = "success"
    else:
        data["status"] = "error"

    # Optional success fields
    codigo_ref = response.findtext('.//ns9:codigoReferencia', namespaces=namespaces)
    fecha_ventana = response.findtext('.//ns9:fechaVentanaCambio', namespaces=namespaces)
    if codigo_ref:
        data["codigoReferencia"] = codigo_ref
    if fecha_ventana:
        data["fechaVentanaCambio"] = fecha_ventana

    # Optional error fields
    campos_erroneos: list[dict[str, str]] = []
    for campo in response.findall('.//ns2:campoErroneo', namespaces):
        campos_erroneos.append({
            "nombre": campo.findtext('.//ns2:nombre', namespaces=namespaces) or "",
            "descripcionCampo": campo.findtext('.//ns2:descripcion', namespaces=namespaces) or ""
        })

    if campos_erroneos:
        data["camposErroneos"] = campos_erroneos

    return data

import xml.etree.ElementTree as ET
from typing import List, Tuple, Optional
import io

import io
import xml.etree.ElementTree as ET
from typing import List, Tuple, Optional

def parse_soap_response_list_updated(soap_xml: str, requested_fields: List[str]) -> Tuple[Optional[str], ...]:
    """
    Parse SOAP response with dynamic namespaces and return values as tuple for easy unpacking.

    Args:
        soap_xml: SOAP response XML string.
        requested_fields: List of field names to extract from the XML.

    Returns:
        Tuple with extracted field values in the same order as requested_fields.
        If a field is not found or XML parsing fails, its value will be None.
    """
    result: List[Optional[str]] = []

    try:
        # Parse XML tree directly
        root = ET.fromstring(soap_xml)
        
        for field in requested_fields:
            # Use namespace-agnostic search which works well with dynamic namespaces
            value = root.findtext(f'.//{{*}}{field}')
            
            # Handle empty strings and whitespace-only values
            if value is not None:
                value = value.strip() if value.strip() else None
                
            result.append(value)

    except ET.ParseError as e:
        print(f"XML parsing error: {e}")
        result = [None] * len(requested_fields)
    except Exception as e:
        print(f"Unexpected error parsing SOAP XML: {e}")
        result = [None] * len(requested_fields)

    # Guarantee the correct tuple length
    if len(result) != len(requested_fields):
        result = [None] * len(requested_fields)

    return tuple(result)

def parse_soap_response_nested_1(soap_xml: str, requested_fields: List[str]) -> Tuple[Optional[str], ...]:
    """
    Parse SOAP response and return values as tuple for easy unpacking.
    Supports nested fields using '/' syntax.
    
    Args:
        soap_xml: SOAP response XML string.
        requested_fields: List of field names or paths to extract.
                         Use '/' for nested fields: 'campoErroneo/nombre'
    
    Returns:
        Tuple with extracted field values in the same order as requested_fields.
    """
    result: List[Optional[str]] = []

    try:
        root = ET.fromstring(soap_xml)
        
        for field_path in requested_fields:
            value = None
            
            if '/' in field_path:
                # Handle nested fields: 'parent/child'
                parts = field_path.split('/')
                current_element = root
                
                # Navigate through the path
                for part in parts:
                    current_element = current_element.find(f'.//{{*}}{part}')
                    if current_element is None:
                        break
                
                value = current_element.text if current_element is not None else None
            else:
                # Handle simple fields (original behavior)
                value = root.findtext(f'.//{{*}}{field_path}')
            
            # Handle empty strings and whitespace
            if value is not None:
                value = value.strip() if value.strip() else None
                
            result.append(value)

    except Exception as e:
        print(f"Error parsing SOAP XML: {e}")
        result = [None] * len(requested_fields)

    if len(result) != len(requested_fields):
        result = [None] * len(requested_fields)

    return tuple(result)

import xml.etree.ElementTree as ET
from typing import List, Tuple, Optional, Union

import xml.etree.ElementTree as ET
from typing import List, Tuple, Union, Optional

import xml.etree.ElementTree as ET
from typing import List, Optional

import xml.etree.ElementTree as ET
from typing import List, Optional

def parse_soap_response_nested_2(soap_xml: str, requested_fields: List[str]) -> List[List[Optional[str]]]:
    """
    Parse SOAP response and return grouped values for each <registro> section.
    Handles both global and per-record fields.
    """
    soap_xml = soap_xml.lstrip()
    records: List[List[Optional[str]]] = []

    try:
        root = ET.fromstring(soap_xml)
        registros = root.findall(".//{*}registro")

        # Extract top-level (global) fields once
        global_values = {}
        for field in requested_fields:
            el = root.find(f".//{{*}}{field}")
            if el is not None and el.text:
                global_values[field] = el.text.strip()

        # Extract record-level values
        for reg in registros:
            record_values = []
            for field in requested_fields:
                el = reg.find(f".//{{*}}{field}")
                if el is not None and el.text:
                    record_values.append(el.text.strip())
                else:
                    # fallback to global value
                    record_values.append(global_values.get(field))
            records.append(record_values)

    except Exception as e:
        print(f"Error parsing SOAP XML: {e}")

    return records

import xml.etree.ElementTree as ET

import xml.etree.ElementTree as ET

def parse_soap_response_nested_multi_1(xml, fields, reference_code):
    """
    Parse SOAP XML and return a list of requested field values
    for the given reference_code, or None if not found.
    Works without needing lxml.
    """
    try:
        xml = xml.strip()
        root = ET.fromstring(xml)
    except Exception as e:
        print(f"Error parsing SOAP XML: {e}")
        return None

    # Find all <registro> elements, ignoring namespaces
    registros = [el for el in root.iter() if el.tag.endswith('registro')]
    if not registros:
        print("No registro elements found in XML")
        return None

    # Extract global fields (codigoRespuesta, descripcion)
    codigo_respuesta = None
    descripcion = None
    for el in root.iter():
        if el.tag.endswith('codigoRespuesta'):
            codigo_respuesta = el.text.strip() if el.text else None
        elif el.tag.endswith('descripcion'):
            descripcion = el.text.strip() if el.text else None

    for reg in registros:
        # Match reference code
        ref_el = next((el for el in reg if el.tag.endswith('codigoReferencia')), None)
        ref_value = ref_el.text.strip() if ref_el is not None and ref_el.text else None

        if ref_value == str(reference_code).strip():
            record = []
            for field in fields:
                if field == "codigoRespuesta":
                    record.append(codigo_respuesta)
                elif field == "descripcion":
                    record.append(descripcion)
                else:
                    field_el = next((el for el in reg.iter() if el.tag.endswith(field)), None)
                    record.append(field_el.text.strip() if field_el is not None and field_el.text else None)
            return record

    # Not found
    return None

def convert_for_mysql_madrid(xml_datetime: str) -> datetime:
    """Convert to Madrid time and return naive datetime for MySQL"""
    # Parse and convert to Madrid timezone
    dt = datetime.fromisoformat(xml_datetime.replace('Z', '+00:00'))
    madrid_tz = pytz.timezone("Europe/Madrid")
    dt_madrid = dt.astimezone(madrid_tz)
    
    # Remove timezone info for MySQL DATETIME
    return dt_madrid.replace(tzinfo=None)

# Usage:
porting_window_str = "2025-10-23T02:00:00+02:00"
porting_window_db = convert_for_mysql_madrid(porting_window_str)
# Result: 2025-10-23 02:00:00 (Madrid time, no timezone)

from services.soap_services import create_status_check_port_out_soap_nc
def check_status_port_out(session_code):
    """
    Task to check the status of a pending port-out request at the Central Node.
    """
    connection = None
    logger.info("ENTER check_status_port_out() with session_code %s ", session_code)
    # APIGEE_PORTABILITY_URL=settings.APIGEE_PORTABILITY_URL
    APIGEE_PORT_OUT_URL=settings.APIGEE_PORT_OUT_URL
    operator_code=settings.APIGEE_OPERATOR_CODE
    page_count = "50"

    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        consultar_payload = create_status_check_port_out_soap_nc(session_code,operator_code, page_count)  # Check status request SOAP
        # Conditional payload logging
        log_payload('NC', 'CHECK_STATUS_PORT_OUT_NC', 'REQUEST', str(consultar_payload))
        headers=settings.get_soap_headers('obtenerNotificacionesAltaPortabilidadMovilComoDonantePendientesConfirmarRechazar')
        print("Check status headers:", headers)

        # return
    
        if not APIGEE_PORT_OUT_URL:
            raise ValueError("APIGEE_PORT_OUT_URL environment variable is not set.")
        
        response = requests.post(APIGEE_PORT_OUT_URL,
                               data=consultar_payload,
                               headers=settings.get_soap_headers('obtenerNotificacionesAltaPortabilidadMovilComoDonantePendientesConfirmarRechazar'),
                               timeout=settings.APIGEE_API_QUERY_TIMEOUT)
        response.raise_for_status()

        log_payload('NC', 'CHECK_STATUS_PORT_OUT', 'RESPONSE', str(response.text))

        return response.text

        # result = parse_soap_response_nested_multi(response.text,["codigoRespuesta", "descripcion", "error_field", "error_description"])

        # fields = ["tipoProceso", "codigoRespuesta", "descripcion", "codigoReferencia", "estado","fechaVentanaCambio","fechaCreacion"]
        # reference_code = "29979811251021171100203"

        # result = parse_soap_response_nested_multi(xml_data, fields, reference_code)

        print(f"NC Response Code: {result['codigoRespuesta']}")
        print(f"NC Description: {result['descripcion']}")
        print(f"NC Error Field: {result['error_field']}")
        print(f"NC Error Description: {result['error_description']}")
        
        # print(parse_consultar_procesos_response(response.text)    )

        return response.text
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
            # callback_bss.delay(mnp_request_id, reference_code, session_code, response_code, description, None, None)
            # def callback_bss(self, mnp_request_id, reference_code, session_code, 
            # response_status, description=None, error_fields=None, porting_window_date=None):
            
            return "Final status received for id: %s, status: %s", mnp_request_id, response_code
            
            # callback_bss.delay(mnp_request_id)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

import xml.etree.ElementTree as ET

import xml.etree.ElementTree as ET

def parse_portout_response(xml_string: str):
    """
    Parse SOAP XML with Port-Out notifications and return
    a structured English-translated response.
    """
    # 🧹 Clean XML (remove leading BOM/newlines)
    xml_string = xml_string.lstrip().replace('\ufeff', '')

    # Parse XML safely
    root = ET.fromstring(xml_string)

    # Namespace-agnostic search
    get_text = lambda element, tag: element.findtext(f".//{{*}}{tag}")

    # --- Extract response-level fields ---
    response_info = {
        "response_code": get_text(root, "codigoRespuesta"),
        "response_description": get_text(root, "descripcion"),
        "paged_request_code": get_text(root, "codigoPeticionPaginada"),
        "total_records": get_text(root, "totalRegistros"),
        "is_last_page": get_text(root, "ultimaPagina")
    }

    # --- Extract all Port-Out requests ---
    notifications = root.findall(".//{*}notificacion")
    requests = []

    for notif in notifications:
        solicitud = notif.find(".//{*}solicitud")
        if solicitud is None:
            continue

        # Helper to extract text within solicitud
        get = lambda tag: solicitud.findtext(f".//{{*}}{tag}")

        # --- Build translated dictionary for each request ---
        request_data = {
            "notification_id": get_text(notif, "codigoNotificacion"),
            "creation_date": get_text(notif, "fechaCreacion"),
            "synchronized": get_text(notif, "sincronizada"),

            "reference_code": get("codigoReferencia"),
            "status": get("estado"),
            "state_date": get("fechaEstado"),
            "creation_date_request": get("fechaCreacion"),
            "reading_mark_date": get("fechaMarcaLectura"),
            "state_change_deadline": get("fechaLimiteCambioEstado"),
            "subscriber_request_date": get("fechaSolicitudPorAbonado"),
            "donor_operator_code": get("codigoOperadorDonante"),
            "receiver_operator_code": get("codigoOperadorReceptor"),
            "extraordinary_donor_activation": get("operadorDonanteAltaExtraordinaria"),
            "contract_code": get("codigoContrato"),
            "receiver_NRN": get("NRNReceptor"),
            "port_window_date": get("fechaVentanaCambio"),
            "port_window_by_subscriber": get("fechaVentanaCambioPorAbonado"),
            "MSISDN": get("MSISDN"),

            # --- Abonado (Subscriber) details ---
            "subscriber": {
                "id_type": solicitud.findtext(".//{*}documentoIdentificacion/{*}tipo"),
                "id_number": solicitud.findtext(".//{*}documentoIdentificacion/{*}documento"),
                "first_name": solicitud.findtext(".//{*}datosPersonales/{*}nombre"),
                "last_name_1": solicitud.findtext(".//{*}datosPersonales/{*}primerApellido"),
                "last_name_2": solicitud.findtext(".//{*}datosPersonales/{*}segundoApellido"),
            }
        }

        requests.append(request_data)

    # --- Combine everything into a clean English JSON-like dict ---
    parsed_result = {
        "response_info": response_info,
        "requests": requests
    }

    return parsed_result

def parse_portout_response_es(xml_string: str):
    """
    Parses a SOAP XML response containing Port-Out notifications
    and returns all response metadata + a list of detailed requests.
    """

    xml_string = xml_string.lstrip()  # removes any leading newlines or spaces
    xml_string = xml_string.replace('\ufeff', '')  # removes UTF-8 BOM if present
    root = ET.fromstring(xml_string)

    # --- Extract global response metadata ---
    response_meta = {
        "codigoRespuesta": root.findtext(".//{*}codigoRespuesta"),
        "descripcion": root.findtext(".//{*}descripcion"),
        "codigoPeticionPaginada": root.findtext(".//{*}codigoPeticionPaginada"),
        "totalRegistros": root.findtext(".//{*}totalRegistros"),
        "ultimaPagina": root.findtext(".//{*}ultimaPagina"),
    }

    # --- Extract notifications ---
    notifications = root.findall(".//{*}notificacion")
    notification_list = []

    for notif in notifications:
        notif_data: dict[str, Any] = {
            "fechaCreacion": notif.findtext(".//{*}fechaCreacion"),
            "sincronizada": notif.findtext(".//{*}sincronizada"),
            "codigoNotificacion": notif.findtext(".//{*}codigoNotificacion"),
        }

        solicitud = notif.find(".//{*}solicitud")
        if solicitud is None:
            notification_list.append(notif_data)
            continue

        def get_field(solicitud_elem, tag):
            return solicitud_elem.findtext(f".//{{*}}{tag}") if solicitud_elem is not None else None

        solicitud_data: dict[str, Union[str, dict[str, str | None], None]] = {
            "fechaCreacion": get_field(solicitud, "fechaCreacion"),
            "fechaEstado": get_field(solicitud, "fechaEstado"),
            "codigoReferencia": get_field(solicitud, "codigoReferencia"),
            "fechaMarcaLectura": get_field(solicitud, "fechaMarcaLectura"),
            "estado": get_field(solicitud, "estado"),
            "fechaLimiteCambioEstado": get_field(solicitud, "fechaLimiteCambioEstado"),
            "fechaSolicitudPorAbonado": get_field(solicitud, "fechaSolicitudPorAbonado"),
            "codigoOperadorDonante": get_field(solicitud, "codigoOperadorDonante"),
            "operadorDonanteAltaExtraordinaria": get_field(solicitud, "operadorDonanteAltaExtraordinaria"),
            "codigoOperadorReceptor": get_field(solicitud, "codigoOperadorReceptor"),
            "codigoContrato": get_field(solicitud, "codigoContrato"),
            "NRNReceptor": get_field(solicitud, "NRNReceptor"),
            "fechaVentanaCambio": get_field(solicitud, "fechaVentanaCambio"),
            "fechaVentanaCambioPorAbonado": get_field(solicitud, "fechaVentanaCambioPorAbonado"),
            "MSISDN": get_field(solicitud, "MSISDN"),
        }

        # Extract abonado details
        abonado = solicitud.find(".//{*}abonado")
        if abonado is not None:
            solicitud_data["abonado"] = {
                "tipoDocumento": abonado.findtext(".//{*}documentoIdentificacion/{*}tipo"),
                "documento": abonado.findtext(".//{*}documentoIdentificacion/{*}documento"),
                "nombre": abonado.findtext(".//{*}datosPersonales/{*}nombre"),
                "primerApellido": abonado.findtext(".//{*}datosPersonales/{*}primerApellido"),
                "segundoApellido": abonado.findtext(".//{*}datosPersonales/{*}segundoApellido"),
            }

        notif_data["solicitud"] = solicitud_data
        notification_list.append(notif_data)

    # --- Combine everything ---
    parsed_data = {
        "response": response_meta,
        "notificaciones": notification_list
    }

    return parsed_data
    
# import mysql.connector
# from mysql.connector import Error

def normalize_datetime(dt_str):
    """Convert ISO8601 datetime string (e.g. '2025-10-31T17:25:33.038+01:00')
    into MySQL-compatible format 'YYYY-MM-DD HH:MM:SS'.
    Returns None if input is invalid or empty."""
    if not dt_str:
        return None
    try:
        # Parse ISO format (handles timezone and fractional seconds)
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        # Fallback: try removing +01:00 and milliseconds manually
        cleaned = dt_str.split('+')[0].split('.')[0].replace('T', ' ')
        return cleaned
    
def insert_portout_response_to_db(parsed_data):
    """
    Inserts parsed Port-Out response data into MySQL tables:
    - portout_metadata
    - portout_request
    using mysql.connector.

    Args:
        parsed_data (dict): Output of parse_portout_response()
        db_config (dict): MySQL connection parameters, e.g.
            {
                "host": "localhost",
                "user": "root",
                "password": "mypassword",
                "database": "portability_db"
            }
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
                subscriber_last_name_1, subscriber_last_name_2, created_at, updated_at, status_nc, status_bss
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, NOW(), NOW(), %s, %s
            )
        """

        for req in parsed_data["requests"]:
            sub = req["subscriber"]

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
                status_bss
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

def callback_bss_portout(parsed_data):
    """
    REST JSON POST to BSS Webhook port-out with updated English field names
    
    """
    meta = parsed_data["response_info"]
    request_code = meta.get("paged_request_code")
    logger.debug("ENTER callback_bss_portout() with paged_request_code %s", request_code)

    for req in parsed_data["requests"]:
        sub = req["subscriber"]
        notification_id=req.get("notification_id")

        payload = {
            "notification_id": req.get("notification_id"),
            "creation_date": normalize_datetime(req.get("creation_date")),
            "synchronized": 1 if str(req.get("synchronized")).lower() in ("true", "1") else 0,
            "reference_code": req.get("reference_code"),
            "status": req.get("status"),
            "state_date": normalize_datetime(req.get("state_date")),
            "creation_date_request": normalize_datetime(req.get("creation_date_request")),
            "reading_mark_date": normalize_datetime(req.get("reading_mark_date")),
            "state_change_deadline": normalize_datetime(req.get("state_change_deadline")),
            "subscriber_request_date": normalize_datetime(req.get("subscriber_request_date")),
            "donor_operator_code": req.get("donor_operator_code"),
            "receiver_operator_code": req.get("receiver_operator_code"),
            "extraordinary_donor_activation": 1 if str(req.get("extraordinary_donor_activation")).lower() in ("true", "1") else 0,
            "contract_code": req.get("contract_code"),
            "receiver_NRN": req.get("receiver_NRN"),
            "port_window_date": normalize_datetime(req.get("port_window_date")),
            "port_window_by_subscriber": 1 if str(req.get("port_window_by_subscriber")).lower() in ("true", "1") else 0,
            "MSISDN": req.get("MSISDN"),
            "subscriber": {
                "id_type": sub.get("id_type"),
                "id_number": sub.get("id_number"),
                "first_name": sub.get("first_name"),
                "last_name_1": sub.get("last_name_1"),
                "last_name_2": sub.get("last_name_2")
            }
        }

        logger.debug("Webhook payload being sent: %s", payload)
        # print(f"Webhook payload being sent: {payload}")
        bss_webhook_port_out ="https://webhook.site/74f0037c-8b01-4319-915e-326d6094d45d"
        
        try:
            # Send POST request
            response = requests.post(
                # settings.BSS_WEBHOOK_URL,
                bss_webhook_port_out,
                json=payload,
                headers=settings.get_headers_bss(),
                timeout=settings.APIGEE_API_QUERY_TIMEOUT,
                verify=settings.SSL_VERIFICATION  # Use SSL verification setting
            )
            
            # Check if request was successful
            if response.status_code == 200:
                logger.info(
                    "Webhook sent successfully for notification_id %s",notification_id
                )

                # Update database with the actual scheduled time
                try: 
                    update_query = """
                        UPDATE portout_request 
                        SET status_bss = %s,
                        updated_at = NOW() 
                        WHERE notification_id = %s
                    """
                    connection = get_db_connection()
                    cursor = connection.cursor(dictionary=True)
                    
                    # Map response_code to appropriate status_bss value
                    # status_bss="CANCEL_REQUEST_COMPLETED" if response_status=="ACAN" else "NO_RESPONSE_ON CANCEL_RESPONSE"
                    status_bss = "PORT_OUT_REQUEST_SUBMITTED"
                    # status_bss = self._map_response_to_status(response_status)
                    cursor.execute(update_query, (status_bss, notification_id))
                    connection.commit()
                    
                    logger.debug(
                        "Database updated for notification %s with status_bss: %s", 
                        notification_id, status_bss
                    )
              
                except Exception as db_error:
                    logger.error("Database update failed for request %s: %s", mnp_request_id, str(db_error))
                    return False
            else:
                logger.error(
                    "Webhook failed for notification_id %s", 
                    notification_id
                )
            
        except requests.exceptions.Timeout as exc:
            logger.error("Webhook timeout for notification_id %s %s", notification_id, str(exc))
            # self.retry(exc=exc, countdown=120)
            return False
        except requests.exceptions.ConnectionError as exc:
            logger.error("Webhook connection error for notification_id %s %s", notification_id,str(exc))
            # self.retry(exc=exc, countdown=120)
            return False
        except requests.exceptions.RequestException as exc:
            logger.error("Webhook error for notification_id %s %s ", notification_id, str(exc))
            # self.retry(exc=exc, countdown=120)
            return False
        finally:
            if 'connection' in locals() and connection and connection.is_connected():
                cursor.close()
                connection.close()

import xml.etree.ElementTree as ET

def parse_soap_response_dict_new(xml_data: str, tags: list[str]) -> dict:
    """
    Parse a SOAP XML string and extract values for the given tag names,
    ignoring XML namespaces.

    Args:
        xml_data (str): The SOAP XML string
        tags (list[str]): List of tag names to extract

    Returns:
        dict: { tag_name: value or None }
    """
    # Remove namespace prefixes for simpler searching
    def strip_namespace(tag: str) -> str:
        if '}' in tag:
            return tag.split('}', 1)[1]
        return tag

    result = {tag: None for tag in tags}

    try:
        root = ET.fromstring(xml_data)

        # Walk the entire tree
        for elem in root.iter():
            clean_tag = strip_namespace(elem.tag)
            if clean_tag in tags:
                result[clean_tag] = (elem.text or '').strip()

    except ET.ParseError as e:
        print(f"XML Parse Error: {e}")

    return result


if __name__ == "__main__":

#     xml_data = """<?xml version='1.0' encoding='UTF-8'?><S:Envelope xmlns:S=\"http://schemas.xmlsoap.org/soap/envelope/\"><S:Header/><S:Body><ns14:respuestaRechazarSolicitudAltaPortabilidadMovil xmlns:ns17=\"http://nc.aopm.es/v1-10/extras/fichero\" xmlns:ns16=\"http://nc.aopm.es/v1-10/fichero\" xmlns:ns15=\"http://nc.aopm.es/v1-7/integracion\" xmlns:ns14=\"http://nc.aopm.es/v1-10/portabilidad\" xmlns:ns13=\"http://nc.aopm.es/v1-10/extras/portabilidad\" xmlns:ns12=\"http://nc.aopm.es/v1-10/extras/informe\" xmlns:ns11=\"http://nc.aopm.es/v1-10/extras/incidencia\" xmlns:ns10=\"http://nc.aopm.es/v1-10/extras/buzon\" xmlns:ns9=\"http://nc.aopm.es/v1-10/administracion\" xmlns:ns8=\"http://nc.aopm.es/v1-10/buzon\" xmlns:ns7=\"http://nc.aopm.es/v1-10/extras/administracion\" xmlns:ns6=\"http://nc.aopm.es/v1-10/incidencia\" xmlns:ns5=\"http://nc.aopm.es/v1-10/acceso\" xmlns:ns4=\"http://nc.aopm.es/v1-10/extras\" xmlns:ns3=\"http://nc.aopm.es/v1-10/boletin\" xmlns:ns2=\"http://nc.aopm.es/v1-10\"><ns2:codigoRespuesta>RECH TIEMP</ns2:codigoRespuesta><ns2:descripcion>\u00danicamente se puede rechazar la solicitud de alta de portabilidad m\u00f3vil durante el per\u00edodo Tv. Tv Inicio: 04/11/2025 14:00:00, Tv Fin: 04/11/2025 20:00:00.</ns2:descripcion></ns14:respuestaRechazarSolicitudAltaPortabilidadMovil></S:Body></S:Envelope>
# """
#     # response_code, description, reference_code = parse_soap_response_list(xml_data, ["codigoRespuesta", "descripcion", "codigoReferencia"])
#     result = parse_soap_response_dict(xml_data,["codigoRespuesta", "descripcion", "codigoReferencia"])

#     # print(f"Response Code: {response_code}")
#     # print(f"Description: {description}")
#     # print(f"Reference Code: {reference_code}")
#     print(f"NC Response Code: {result['codigoRespuesta']}")
#     print(f"NC Description: {result['descripcion']}")
#     print(f"NC codigoReferencia: {result['codigoReferencia']}")
#     # print(f"NC totalRegistros: {result['totalRegistros']}")
#     # print(f"NC ultimaPagina: {result['ultimaPagina']}")

#     exit()

    xml_data = """
<?xml version='1.0' encoding='UTF-8'?><S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/"><S:Header/><S:Body><ns7:respuestaObtenerNotificacionesAltaPortabilidadMovilComoDonantePendientesConfirmarRechazar xmlns:ns17="http://nc.aopm.es/v1-10/extras/fichero" xmlns:ns16="http://nc.aopm.es/v1-10/fichero" xmlns:ns15="http://nc.aopm.es/v1-7/integracion" xmlns:ns14="http://nc.aopm.es/v1-10" xmlns:ns13="http://nc.aopm.es/v1-10/extras/portabilidad" xmlns:ns12="http://nc.aopm.es/v1-10/extras/informe" xmlns:ns11="http://nc.aopm.es/v1-10/extras/incidencia" xmlns:ns10="http://nc.aopm.es/v1-10/extras/buzon" xmlns:ns9="http://nc.aopm.es/v1-10/portabilidad" xmlns:ns8="http://nc.aopm.es/v1-10/administracion" xmlns:ns7="http://nc.aopm.es/v1-10/buzon" xmlns:ns6="http://nc.aopm.es/v1-10/extras/administracion" xmlns:ns5="http://nc.aopm.es/v1-10/incidencia" xmlns:ns4="http://nc.aopm.es/v1-10/acceso" xmlns:ns3="http://nc.aopm.es/v1-10/extras" xmlns:ns2="http://nc.aopm.es/v1-10/boletin"><ns14:codigoRespuesta>0000 00000</ns14:codigoRespuesta><ns14:descripcion>La operación se ha realizado con éxito</ns14:descripcion><ns14:codigoPeticionPaginada>b63653087d60ebca0afd81001dea65e4</ns14:codigoPeticionPaginada><ns14:totalRegistros>2</ns14:totalRegistros><ns14:ultimaPagina>true</ns14:ultimaPagina><ns7:notificacion><ns14:fechaCreacion>2025-10-31T17:25:33.038+01:00</ns14:fechaCreacion><ns14:sincronizada>false</ns14:sincronizada><ns14:codigoNotificacion>431148150</ns14:codigoNotificacion><ns14:solicitud xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ns14:SolicitudIndividualAltaPortabilidadMovil"><ns14:fechaCreacion>2025-10-31T17:25:33.038+01:00</ns14:fechaCreacion><ns14:fechaEstado>2025-10-31T17:25:33.038+01:00</ns14:fechaEstado><ns14:codigoReferencia>79829911251031172500401</ns14:codigoReferencia><ns14:fechaMarcaLectura>2025-10-31T17:25:33.038+01:00</ns14:fechaMarcaLectura><ns14:estado>ASOL</ns14:estado><ns14:fechaLimiteCambioEstado>2025-11-03T14:00:00+01:00</ns14:fechaLimiteCambioEstado><ns14:fechaSolicitudPorAbonado>2025-10-31T00:00:00+01:00</ns14:fechaSolicitudPorAbonado><ns14:codigoOperadorDonante>299</ns14:codigoOperadorDonante><ns14:operadorDonanteAltaExtraordinaria>false</ns14:operadorDonanteAltaExtraordinaria><ns14:codigoOperadorReceptor>798</ns14:codigoOperadorReceptor><ns14:abonado><ns14:documentoIdentificacion><ns14:tipo>NIE</ns14:tipo><ns14:documento>Y3037876D</ns14:documento></ns14:documentoIdentificacion><ns14:datosPersonales xsi:type="ns14:DatosPersonalesAbonadoPersonaFisica"><ns14:nombre>Oleg</ns14:nombre><ns14:primerApellido>Cabrerra</ns14:primerApellido><ns14:segundoApellido>Belousov</ns14:segundoApellido></ns14:datosPersonales></ns14:abonado><ns14:codigoContrato>798-TRAC_12</ns14:codigoContrato><ns14:NRNReceptor>704914</ns14:NRNReceptor><ns14:fechaVentanaCambio>2025-11-04T02:00:00+01:00</ns14:fechaVentanaCambio><ns14:fechaVentanaCambioPorAbonado>false</ns14:fechaVentanaCambioPorAbonado><ns14:MSISDN>621800005</ns14:MSISDN></ns14:solicitud></ns7:notificacion><ns7:notificacion><ns14:fechaCreacion>2025-11-03T11:30:10.389+01:00</ns14:fechaCreacion><ns14:sincronizada>false</ns14:sincronizada><ns14:codigoNotificacion>431154450</ns14:codigoNotificacion><ns14:solicitud xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ns14:SolicitudIndividualAltaPortabilidadMovil"><ns14:fechaCreacion>2025-11-03T11:30:10.389+01:00</ns14:fechaCreacion><ns14:fechaEstado>2025-11-03T11:30:10.389+01:00</ns14:fechaEstado><ns14:codigoReferencia>79829911251103113000104</ns14:codigoReferencia><ns14:fechaMarcaLectura>2025-11-03T11:30:10.389+01:00</ns14:fechaMarcaLectura><ns14:estado>ASOL</ns14:estado><ns14:fechaLimiteCambioEstado>2025-11-03T20:00:00+01:00</ns14:fechaLimiteCambioEstado><ns14:fechaSolicitudPorAbonado>2025-11-03T00:00:00+01:00</ns14:fechaSolicitudPorAbonado><ns14:codigoOperadorDonante>299</ns14:codigoOperadorDonante><ns14:operadorDonanteAltaExtraordinaria>false</ns14:operadorDonanteAltaExtraordinaria><ns14:codigoOperadorReceptor>798</ns14:codigoOperadorReceptor><ns14:abonado><ns14:documentoIdentificacion><ns14:tipo>NIE</ns14:tipo><ns14:documento>Y3037876D</ns14:documento></ns14:documentoIdentificacion><ns14:datosPersonales xsi:type="ns14:DatosPersonalesAbonadoPersonaFisica"><ns14:nombre>Oleg</ns14:nombre><ns14:primerApellido>Cabrerra</ns14:primerApellido><ns14:segundoApellido>Belousov</ns14:segundoApellido></ns14:datosPersonales></ns14:abonado><ns14:codigoContrato>798-TRAC_15</ns14:codigoContrato><ns14:NRNReceptor>704914</ns14:NRNReceptor><ns14:fechaVentanaCambio>2025-11-05T02:00:00+01:00</ns14:fechaVentanaCambio><ns14:fechaVentanaCambioPorAbonado>false</ns14:fechaVentanaCambioPorAbonado><ns14:MSISDN>621800006</ns14:MSISDN></ns14:solicitud></ns7:notificacion></ns7:respuestaObtenerNotificacionesAltaPortabilidadMovilComoDonantePendientesConfirmarRechazar></S:Body></S:Envelope>
"""
    parsed = parse_portout_response(xml_data)

    import json
    print(parsed)
    exit()
    print(json.dumps(parsed, indent=2, ensure_ascii=False))
    insert_portout_response_to_db(parsed)
    callback_bss_portout(parsed)
    exit()

    xml_data = """
2025-10-31 19:56:32,200 - mnp_payload - INFO - NC_CANCEL_REQUEST: <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:por="http://nc.aopm.es/v1-10/portabilidad" xmlns:v1="http://nc.aopm.es/v1-10">
   <soapenv:Header/>
   <soapenv:Body>
      <por:peticionCancelarSolicitudAltaPortabilidadMovil>
         <v1:codigoSesion>e904d24522fd461dd580a7f84907244b</v1:codigoSesion>
         <por:codigoReferencia>29979811251031124400201</por:codigoReferencia>
         <por:causaEstado>CANC_ABONA</por:causaEstado>
         <por:cancelacionIniciadaPorDonante>1</por:cancelacionIniciadaPorDonante>
      </por:peticionCancelarSolicitudAltaPortabilidadMovil>
   </soapenv:Body>
</soapenv:Envelope>


"""
    cleaned_payload = xml_data.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    cleaned_payload = "".join(xml_data.split())
    print(cleaned_payload)
    exit()

    xml_data = """
<?xml version='1.0' encoding='UTF-8'?><S:Envelope xmlns:S=\"http://schemas.xmlsoap.org/soap/envelope/\"><S:Header/><S:Body><ns14:respuestaConsultarProcesosPortabilidadMovil xmlns:ns17=\"http://nc.aopm.es/v1-10/extras/fichero\" xmlns:ns16=\"http://nc.aopm.es/v1-10/fichero\" xmlns:ns15=\"http://nc.aopm.es/v1-7/integracion\" xmlns:ns14=\"http://nc.aopm.es/v1-10/portabilidad\" xmlns:ns13=\"http://nc.aopm.es/v1-10/extras/portabilidad\" xmlns:ns12=\"http://nc.aopm.es/v1-10/extras/informe\" xmlns:ns11=\"http://nc.aopm.es/v1-10/extras/incidencia\" xmlns:ns10=\"http://nc.aopm.es/v1-10/extras/buzon\" xmlns:ns9=\"http://nc.aopm.es/v1-10/administracion\" xmlns:ns8=\"http://nc.aopm.es/v1-10/buzon\" xmlns:ns7=\"http://nc.aopm.es/v1-10/extras/administracion\" xmlns:ns6=\"http://nc.aopm.es/v1-10/incidencia\" xmlns:ns5=\"http://nc.aopm.es/v1-10/acceso\" xmlns:ns4=\"http://nc.aopm.es/v1-10/extras\" xmlns:ns3=\"http://nc.aopm.es/v1-10/boletin\" xmlns:ns2=\"http://nc.aopm.es/v1-10\"><ns2:codigoRespuesta>0000 00000</ns2:codigoRespuesta><ns2:descripcion>La operación se ha realizado con éxito</ns2:descripcion><ns14:registro><ns14:tipoProceso>ALTA_PORTABILIDAD_MOVIL</ns14:tipoProceso><ns14:codigoReferencia>29979811251031102100103</ns14:codigoReferencia><ns14:rangoMSISDN><ns2:valorInicial>552000003</ns2:valorInicial><ns2:valorFinal>552000003</ns2:valorFinal></ns14:rangoMSISDN><ns14:codigoOperadorDonante>798</ns14:codigoOperadorDonante><ns14:codigoOperadorReceptor>299</ns14:codigoOperadorReceptor><ns14:estado>AREC</ns14:estado><ns14:fechaVentanaCambio>2025-11-04T02:00:00+01:00</ns14:fechaVentanaCambio><ns14:fechaCreacion>2025-10-31T10:21:59.839+01:00</ns14:fechaCreacion><ns14:fechaMarcaLectura>2025-10-31T10:21:59.839+01:00</ns14:fechaMarcaLectura><ns14:fechaRechazo>2025-10-31T14:39:30.527+01:00</ns14:fechaRechazo><ns14:causaRechazo>RECH_BNUME</ns14:causaRechazo></ns14:registro></ns14:respuestaConsultarProcesosPortabilidadMovil></S:Body></S:Envelope>
    """
    fields = ["tipoProceso", "codigoRespuesta", "descripcion", "codigoReferencia", "estado","fechaVentanaCambio","fechaCreacion","causaRechazo"]
    reference_code = "29979811251031102100103"

    result = parse_soap_response_nested_multi(xml_data, fields, reference_code)
    print(result)
    exit()
    
    print(settings.TIME_DELTA_FOR_PORT_OUT_STATUS_CHECK)
    delta = 15*60
    _, _, scheduled_datetime = calculate_countdown_working_hours(
                                                        delta=delta, 
                                                        with_jitter=True)
    print(scheduled_datetime)
    exit()

    session_code = initiate_session()
    xml_data = check_status_port_out(session_code)
    result = parse_soap_response_dict(xml_data,["codigoRespuesta", "descripcion", "codigoPeticionPaginada","totalRegistros","ultimaPagina"])

    print(f"NC Response Code: {result['codigoRespuesta']}")
    print(f"NC Description: {result['descripcion']}")
    print(f"NC codigoPeticionPaginada: {result['codigoPeticionPaginada']}")
    print(f"NC totalRegistros: {result['totalRegistros']}")
    print(f"NC ultimaPagina: {result['ultimaPagina']}")

    fields = [
    "codigoRespuesta",                    # Simple field
    "descripcion",                        # Simple field  
    "codigoPeticionPaginada",               # Nested: gets 'codigoOperadorDonante'
    "totalRegistros",           # Nested: gets the error description
    "ultimaPagina"
]
    codigoRespuesta, descripcion, codigoPeticionPaginada, totalRegistros, ultimaPagina  = parse_soap_response_nested(xml_data, fields)   

    print(f"NC Response Code: {codigoRespuesta}")
    print(f"NC Description: {descripcion}")
    print(f"NC codigoPeticionPaginada: {codigoPeticionPaginada}")
    print(f"NC totalRegistros: {totalRegistros}")
    print(f"NC ultimaPagina: {ultimaPagina}")


    # print(xml_data)
    exit()

    # xml_datetime = "2025-10-23T02:00:00+02:00"
    # print(convert_for_mysql_madrid(xml_datetime))
    # exit()
    # _, _, scheduled_datetime = calculate_countdown_working_hours(
    #                 delta=settings.TIME_DELTA_FOR_STATUS_CHECK, 
    #                 with_jitter=True
    # print(scheduled_datetime)
    TIME_DELTA_FOR_STATUS_CHECK = 120
    # _, _, scheduled_datetime = calculate_countdown_working_hours(
    #     delta=settings.TIME_DELTA_FOR_STATUS_CHECK,
    #     with_jitter=True
    # )
    print(datetime.now())
    _, _, scheduled_datetime = calculate_countdown_working_hours(
        delta=TIME_DELTA_FOR_STATUS_CHECK,
        with_jitter=False
    )

    print(scheduled_datetime)
    exit()

    a, _, _ = calculate_countdown_working_hours(
        delta=timedelta(minutes=0),
        with_jitter=True
    )
    a_seconds = int(a.total_seconds())
    print(scheduled_datetime)

    exit()
    xml = """
<?xml version='1.0' encoding='UTF-8'?><S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/"><S:Header/><S:Body><ns14:respuestaConsultarProcesosPortabilidadMovil xmlns:ns17="http://nc.aopm.es/v1-10/extras/fichero" xmlns:ns16="http://nc.aopm.es/v1-10/fichero" xmlns:ns15="http://nc.aopm.es/v1-7/integracion" xmlns:ns14="http://nc.aopm.es/v1-10/portabilidad" xmlns:ns13="http://nc.aopm.es/v1-10/extras/portabilidad" xmlns:ns12="http://nc.aopm.es/v1-10/extras/informe" xmlns:ns11="http://nc.aopm.es/v1-10/extras/incidencia" xmlns:ns10="http://nc.aopm.es/v1-10/extras/buzon" xmlns:ns9="http://nc.aopm.es/v1-10/administracion" xmlns:ns8="http://nc.aopm.es/v1-10/buzon" xmlns:ns7="http://nc.aopm.es/v1-10/extras/administracion" xmlns:ns6="http://nc.aopm.es/v1-10/incidencia" xmlns:ns5="http://nc.aopm.es/v1-10/acceso" xmlns:ns4="http://nc.aopm.es/v1-10/extras" xmlns:ns3="http://nc.aopm.es/v1-10/boletin" xmlns:ns2="http://nc.aopm.es/v1-10"><ns2:codigoRespuesta>0000 00000</ns2:codigoRespuesta><ns2:descripcion>La operación se ha realizado con éxito</ns2:descripcion><ns14:registro><ns14:tipoProceso>ALTA_PORTABILIDAD_MOVIL</ns14:tipoProceso><ns14:codigoReferencia>29979811251021171100203</ns14:codigoReferencia><ns14:rangoMSISDN><ns2:valorInicial>621800001</ns2:valorInicial><ns2:valorFinal>621800001</ns2:valorFinal></ns14:rangoMSISDN><ns14:codigoOperadorDonante>798</ns14:codigoOperadorDonante><ns14:codigoOperadorReceptor>299</ns14:codigoOperadorReceptor><ns14:estado>ASOL</ns14:estado><ns14:fechaVentanaCambio>2025-10-23T02:00:00+02:00</ns14:fechaVentanaCambio><ns14:fechaCreacion>2025-10-21T17:11:21.599+02:00</ns14:fechaCreacion><ns14:fechaMarcaLectura>2025-10-21T17:11:21.599+02:00</ns14:fechaMarcaLectura></ns14:registro><ns14:registro><ns14:tipoProceso>ALTA_PORTABILIDAD_MOVIL</ns14:tipoProceso><ns14:codigoReferencia>79829911250502103200002</ns14:codigoReferencia><ns14:rangoMSISDN><ns2:valorInicial>621800001</ns2:valorInicial><ns2:valorFinal>621800001</ns2:valorFinal></ns14:rangoMSISDN><ns14:codigoOperadorDonante>299</ns14:codigoOperadorDonante><ns14:codigoOperadorReceptor>798</ns14:codigoOperadorReceptor><ns14:estado>APOR</ns14:estado><ns14:fechaVentanaCambio>2025-05-07T02:00:00+02:00</ns14:fechaVentanaCambio><ns14:fechaCreacion>2025-05-02T10:32:02.934+02:00</ns14:fechaCreacion><ns14:fechaMarcaLectura>2025-05-02T10:32:02.934+02:00</ns14:fechaMarcaLectura><ns14:fechaConfirmacion>2025-05-02T14:15:31.096+02:00</ns14:fechaConfirmacion></ns14:registro></ns14:respuestaConsultarProcesosPortabilidadMovil></S:Body></S:Envelope>
    """
    fields = ["tipoProceso", "codigoRespuesta","descripcion","codigoReferencia", "estado"]
    # parsed = parse_soap_response_nested_multi_1(xml, fields)
    # result = parse_soap_response_nested_multi(response.text, fields, reference_code)
    # print(parsed)
    # exit()
    mnp_request_id = 68
    # session_code = "ABC123SESSION"
    msisdn = "621800001"
    session_code = initiate_session()
    xml_data = check_status(mnp_request_id, session_code, msisdn)

    # fields = ["tipoProceso", "codigoRespuesta","descripcion","codigoReferencia", "estado"]
    # parsed = parse_soap_response_nested_multi(xml_data, fields)
    # print(parsed)

    fields = ["tipoProceso", "codigoRespuesta", "descripcion", "codigoReferencia", "estado","fechaVentanaCambio","fechaCreacion"]
    reference_code = "29979811251021171100203"

    result = parse_soap_response_nested_multi(xml_data, fields, reference_code)
    print(result)

    # Create a dictionary
    result_dict = dict(zip(fields, result))

    # Access by field name
    estado = result_dict["estado"]
    codigo_referencia = result_dict["codigoReferencia"]
    descripcion = result_dict["descripcion"]

    print(f"Estado: {estado}")
    print(f"Reference: {codigo_referencia}")
    print(f"descripcion: {descripcion}")

    exit()
    # result = parse_check_status_resonse(xml_data)

    # # Print the flat dictionary
    # from pprint import pprint
    # pprint(result)

    # print("\n--- Accessing individual values ---")
    # print("Código Respuesta:", result['codigoRespuesta'])
    # print("Descripción:", result['descripcion'])

    # if 'codigoOperadorDonante' in result:
    #     print("Operador Donante:", result['codigoOperadorDonante'])
    # if 'codigoOperadorReceptor' in result:
    #     print("Operador Receptor:", result['codigoOperadorReceptor'])
    # if 'estado' in result:
    #     print("Estado del proceso:", result['estado'])
    # if 'fechaConfirmacion' in result:
    #     print("Fecha de confirmación:", result['fechaConfirmacion'])


    # --- Success XML for Solicitar---
    xml_success = """<?xml version='1.0' encoding='UTF-8'?>
    <S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">
       <S:Body>
          <ns9:respuestaCrearSolicitudIndividualAltaPortabilidadMovil 
              xmlns:ns9="http://nc.aopm.es/v1-10/portabilidad"
              xmlns:ns2="http://nc.aopm.es/v1-10">
             <ns2:codigoRespuesta>0000 00000</ns2:codigoRespuesta>
             <ns2:descripcion>La operación se ha realizada con éxito</ns2:descripcion>
             <ns9:codigoReferencia>REF123456789012345678901</ns9:codigoReferencia>
             <ns9:fechaVentanaCambio>2025-10-20T00:00:00</ns9:fechaVentanaCambio>
          </ns9:respuestaCrearSolicitudIndividualAltaPortabilidadMovil>
       </S:Body>
    </S:Envelope>"""

    # --- Error XML ---
    xml_error = """<?xml version='1.0' encoding='UTF-8'?>
    <S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">
       <S:Body>
          <ns9:respuestaCrearSolicitudIndividualAltaPortabilidadMovil 
              xmlns:ns9="http://nc.aopm.es/v1-10/portabilidad"
              xmlns:ns2="http://nc.aopm.es/v1-10">
             <ns2:codigoRespuesta>GENE INFOR</ns2:codigoRespuesta>
             <ns2:descripcion>Se detectaron campos con formato inválido en la petición</ns2:descripcion>
             <ns2:campoErroneo>
                <ns2:nombre>codigoOperadorDonante</ns2:nombre>
                <ns2:descripcion>Campo con restricción de longitud fija de 3 caracteres, se recibieron 10 caracteres</ns2:descripcion>
             </ns2:campoErroneo>
          </ns9:respuestaCrearSolicitudIndividualAltaPortabilidadMovil>
       </S:Body>
    </S:Envelope>"""

    fields = ["codigoRespuesta", "descripcion","codigoReferencia","fechaVentanaCambio"]
    response_code, description, reference_code, data_chnage  = parse_soap_response_nested(xml_success, fields)   
    print("\n--- SUCCESS RESPONSE ---")
    print(f"Response Code: {response_code}")
    print(f"Description: {description}")
    print(f"Reference Code: {reference_code}")
    print(f"Fecha Ventana Cambio: {data_chnage}")

    
    # fields = ["codigoRespuesta", "descripcion","codigoReferencia","fechaVentanaCambio"]
    fields = [
    "codigoRespuesta",                    # Simple field
    "descripcion",                        # Simple field  
    "campoErroneo/nombre",               # Nested: gets 'codigoOperadorDonante'
    "campoErroneo/descripcion"           # Nested: gets the error description
]
    response_code, description, reference_code, data_chnage  = parse_soap_response_nested(xml_error, fields)   
    print("\n--- ERROR RESPONSE ---")
    print(f"codigoRespuesta: {response_code}")
    print(f"descripcion: {description}")
    print(f"campoErroneo/nombre: {reference_code}")
    print(f"campoErroneo/descripcion: {data_chnage}")

    # print(result)  # ('ACCS PERME', 'No es posible invocar esta operación en horario inhábil')
    exit()
    print("\n--- SUCCESS RESPONSE ---")
    result = parse_crear_solicitud_response(xml_success)

    # --- Access individual values directly ---
    print("Código Respuesta:", result["codigoRespuesta"])
    print("Descripción:", result["descripcion"])
    print("Estado (status):", result["status"])


    pprint(parse_crear_solicitud_response(xml_success))

    print("\n--- ERROR RESPONSE ---")
    pprint(parse_crear_solicitud_response(xml_error))
    
    # print("\n--- Accessing individual values ---")
    # print("Código Respuesta:", result['codigoRespuesta'])
    # print("Descripción:", result['descripcion'])

    # print("\n--- ERROR RESPONSE ---")
    # pprint(parse_crear_solicitud_response(xml_error))
    # --- Consultar Procesos XML ---
    exit()


    xml_data = """<?xml version='1.0' encoding='UTF-8'?>
    <S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">
       <S:Body>
          <ns9:respuestaConsultarProcesosPortabilidadMovil 
              xmlns:ns9="http://nc.aopm.es/v1-10/portabilidad"
              xmlns:ns2="http://nc.aopm.es/v1-10">
             <ns2:codigoRespuesta>0000 00000</ns2:codigoRespuesta>
             <ns2:descripcion>La operación se ha realizado con éxito</ns2:descripcion>
             <ns9:registro>
                <ns2:tipoProceso>ALTA_PORTABILIDAD_MOVIL</ns2:tipoProceso>
                <ns2:codigoReferencia>REF123456789012345678901</ns2:codigoReferencia>
                <ns2:rangoMSISDN>
                   <ns2:valorInicial>34600000001</ns2:valorInicial>
                   <ns2:valorFinal>34600000001</ns2:valorFinal>
                </ns2:rangoMSISDN>
                <ns2:codigoOperadorDonante>TEF</ns2:codigoOperadorDonante>
                <ns2:codigoOperadorReceptor>VOD</ns2:codigoOperadorReceptor>
                <ns2:estado>ACON</ns2:estado>
                <ns2:fechaVentanaCambio>2025-10-20T00:00:00</ns2:fechaVentanaCambio>
                <ns2:fechaCreacion>2025-10-15T00:00:00</ns2:fechaCreacion>
                <ns2:fechaMarcaLectura>2025-10-16T10:30:00</ns2:fechaMarcaLectura>
                <ns2:fechaConfirmacion>2025-10-17T14:20:00</ns2:fechaConfirmacion>
                <ns2:fechaRechazo>2025-10-16T09:15:00</ns2:fechaRechazo>
                <ns2:causaRechazo>RECH_IDENT</ns2:causaRechazo>
                <ns2:fechaCancelacion>2025-10-18T16:45:00</ns2:fechaCancelacion>
                <ns2:causaCancelacion>CANC_ABONA</ns2:causaCancelacion>
             </ns9:registro>
          </ns9:respuestaConsultarProcesosPortabilidadMovil>
       </S:Body>
    </S:Envelope>"""

    result = parse_portabilidad_xml_flat(xml_data)

    # Print the flat dictionary
    from pprint import pprint
    pprint(result)

    # Example: Accessing values directly
    print("\n--- Accessing individual values ---")
    print("Código Respuesta:", result['codigoRespuesta'])
    print("Descripción:", result['descripcion'])
    print("Operador Donante:", result['codigoOperadorDonante'])
    print("Operador Receptor:", result['codigoOperadorReceptor'])
    print("Estado del proceso:", result['estado'])
    print("Fecha de confirmación:", result['fechaConfirmacion'])