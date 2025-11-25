"""
Configuration settings for MNP Gateway API.
"""
import os
from dotenv import load_dotenv # type: ignore
import logging
import pytz
from datetime import datetime
from distutils.util import strtobool

# Timezone configuration
MADRID_TZ = pytz.timezone('Europe/Madrid')

def get_madrid_time():
    """Get current time in Madrid timezone"""
    return datetime.now(MADRID_TZ)

def get_madrid_time_iso():
    """Get current time in Madrid timezone as ISO format"""
    return get_madrid_time().isoformat()

def get_madrid_time_readable():
    """Get current time in Madrid timezone as readable string"""
    return get_madrid_time().strftime("%Y-%m-%d %H:%M:%S %Z")

# Load environment variables
load_dotenv()

# # Configure logging
# logging.basicConfig(
#     level=LOG_LEVEL,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(LOG_FILE),
#         logging.StreamHandler()
#     ]
# )

# # Create a logger for this module
# logger = logging.getLogger(__name__)

SAVE_PAYLOAD_TO_LOG = int(os.getenv('SAVE_PAYLOAD_TO_LOG', '0'))
PAYLOAD_LOG_FILE = os.getenv('PAYLOAD_LOG_FILE', 'payload.log')

# # Configure PAYLOAD logger separately
# payload_logger = logging.getLogger('payload_logger')
# payload_logger.setLevel(logging.INFO)  # Payload logs are typically INFO level

# # Prevent payload logs from propagating to root logger
# payload_logger.propagate = False

# # Create formatter for payload logs
# payload_formatter = logging.Formatter('%(asctime)s - PAYLOAD - %(message)s')

# # Create file handler for payload logs
# payload_file_handler = logging.FileHandler(PAYLOAD_LOG_FILE)
# payload_file_handler.setFormatter(payload_formatter)
# payload_logger.addHandler(payload_file_handler)

# Create a settings class or object to hold all configuration
class Settings:
    """Configuration settings for the application"""
    # API Configuration
    # API_TITLE = "MNP Gateway API"
    # API_DESCRIPTION = "Gateway for handling MNP queries between BSS and Central Node"
    # API_VERSION = "1.0.0"
    # API_V1_PREFIX = "/api/v1"
    API_TITLE = os.getenv('API_TITLE', 'MNP Gateway API')
    API_DESCRIPTION = os.getenv('API_DESCRIPTION', 'Gateway for handling MNP queries between BSS and Central Node')
    API_VERSION = os.getenv('API_VERSION', '1.0.0')
    API_PREFIX = os.getenv('API_PREFIX', '/api/v1')
    API_PREFIX_V2 = os.getenv('API_PREFIX_V2', '/api/v2')
    API_VERSION_V2 = os.getenv('API_VERSION_V2', '2.0.0')

    # Server Configuration
    HOST = "0.0.0.0"
    PORT = 8000
    
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_USER = os.getenv('DB_USER', 'root')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    DB_NAME = os.getenv('DB_NAME', 'mnp_database')
    DB_PORT = int(os.getenv('DB_PORT', "3306"))

    # SOAP Service Configuration
    #SOAP_URL = os.getenv(
    # 'SOAP_URL', 'http://webservices.oorsprong.org/websamples.countryinfo/CountryInfoService.wso')
    WSDL_SERVICE_URL = os.getenv('WSDL_SERVICE_URL')
    # SOAP Action headers
    SOAP_ACTION = "http://www.oorsprong.org/websamples.countryinfo/CapitalCity"
    SOAP_ACTION_SOLICITUD = "crearSolicitud"
    # Namespace definitions
    SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
    POR_NS = "http://nc.aopm.es/v1-10/portabilidad"
    V1_NS = "http://nc.aopm.es/v1-10"

    # working hours swrrings
    MORNING_WINDOW_START = int(os.getenv('MORNING_WINDOW_START', "8"))
    MORNING_WINDOW_END = int(os.getenv('MORNING_WINDOW_END', "14")  )
    AFTERNOON_WINDOW_START = int(os.getenv('AFTERNOON_WINDOW_START', "14"))
    AFTERNOON_WINDOW_END = int(os.getenv('AFTERNOON_WINDOW_END', "20"))

    # Jitter configuration - spread tasks over this many minutes
    JITTER_WINDOW_MINUTES = int(os.getenv('JITTER_WINDOW_MINUTES', '30'))  # Spread over 30 minutes
    JITTER_WINDOW_SECONDS = int(os.getenv('JITTER_WINDOW_SECONDS', '60'))  # Spread over 1 minute

    TIME_DELTA_FOR_STATUS_CHECK = int(os.getenv('TIME_DELTA_FOR_STATUS_CHECK', '15'))  # 15 minutes
    TIME_DELTA_FOR_PORT_OUT_STATUS_CHECK = int(os.getenv('TIME_DELTA_FOR_PORT_OUT_STATUS_CHECK', '15'))
    TIME_DELTA_FOR_RETURN_STATUS_CHECK = int(os.getenv('TIME_DELTA_FOR_RETURN_STATUS_CHECK', '15'))
    TIME_ZONE = os.getenv('TIME_ZONE', 'Europe/Madrid')

    WSDL_SERVICE_SPAIN_MOCK = os.getenv('WSDL_SERVICE_SPAIN_MOCK', '')
    WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS = os.getenv('WSDL_SERVICES_SPAIN_MOCK_CHECK_STATUS', '')
    WSDL_SERVICE_SPAIN_MOCK_CANCEL = os.getenv('WSDL_SERVICE_SPAIN_MOCK_CANCEL', '')
    BSS_WEBHOOK_URL = os.getenv('BSS_WEBHOOK_URL', '')
    BSS_WEBHOOK_PORT_OUT_URL = os.getenv('BSS_WEBHOOK_PORT_OUT_URL', '')
    BSS_WEBHOOK_URL_RETURN = os.getenv('BSS_WEBHOOK_URL_RETURN', '')

    SSL_VERIFICATION = os.getenv('SSL_VERIFICATION', '0').lower() in ('1', 'true', 'yes', 'on')
    
    APIGEE_API_KEY = os.getenv('APIGEE_API_KEY', '')
    APIGEE_API_QUERY_TIMEOUT = int(os.getenv('APIGEE_API_QUERY_TIMEOUT', '10'))  # seconds
    APIGEE_USERNAME = os.getenv('APIGEE_API_USERNAME', '')
    APIGEE_ACCESS_CODE = os.getenv('APIGEE_API_ACCESS_CODE', '')
    APIGEE_OPERATOR_CODE = os.getenv('APIGEE_API_OPERATOR_CODE', '')
    APIGEE_ACCESS_URL = os.getenv('APIGEE_ACCESS_URL', '')
    APIGEE_PORTABILITY_URL = os.getenv('APIGEE_PORTABILITY_URL', '')
    APIGEE_PORT_OUT_URL = os.getenv('APIGEE_PORT_OUT_URL', '')
    PAGE_COUNT_PORT_OUT = os.getenv('PAGE_COUNT_PORT_OUT', '')

    PENDING_REQUESTS_TIMEOUT = float(os.getenv('PENDING_REQUESTS_TIMEOUT', '60.0'))  # seconds
   
   # Logging Configuration
    LOG_FILE = os.getenv('LOG_FILE', 'mnp.log')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    SAVE_PAYLOAD_TO_LOG = int(os.getenv('SAVE_PAYLOAD_TO_LOG', '0'))
    PAYLOAD_LOG_FILE = os.getenv('PAYLOAD_LOG_FILE', 'payload.log')

    IGNORE_WORKING_HOURS = int(os.getenv('IGNORE_WORKING_HOURS', '0'))  # Default to 1 (Falsoe)

    SWAGGER_USERNAME = os.getenv('SWAGGER_USERNAME', 'admin')
    SWAGGER_PASSWORD = os.getenv('SWAGGER_PASSWORD', 'secret@123')

    API_USERNAME = os.getenv('API_USERNAME', 'apiuser')
    API_PASSWORD = os.getenv('API_PASSWORD', 'api@password')

    SERVICE_NAME = os.getenv('SERVICE_NAME', 'mnp-gateway')
    ARTIFACT_ID = os.getenv('ARTIFACT_ID', 'mnp-gateway:1.0.0')

    ENABLE_FILE_LOGGING = os.getenv('ENABLE_FILE_LOGGING', 'true').lower() == 'true'
    ENABLE_STDOUT_LOGGING = os.getenv('ENABLE_STDOUT_LOGGING', 'true').lower() == 'true'
    APP_LOG_FILE = os.getenv('APP_LOG_FILE', '/var/log/mnp.log')
    PAYLOAD_LOG_FILE = os.getenv('PAYLOAD_LOG_FILE', '/var/log/payload.log')
    SAVE_PAYLOAD_TO_LOG = int(os.getenv('SAVE_PAYLOAD_TO_LOG', '3'))

    # Database configuration as dict (for existing db_utils compatibility)
    @property
    def mysql_config(self) -> dict:
        """
        Get database configuration as dictionary.
        Returns:
            dict: Database configuration with host, user, password, 
                  database name, and port
        """
        return {
            'host': self.DB_HOST,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD,
            'database': self.DB_NAME,
            'port': self.DB_PORT
        }

    def get_soap_headers(self, soap_action: str = 'IniciarSesion'):
        """
        Get SOAP headers for API requests
        Args: soap_action: The SOAP action to use
        Returns: dict: Headers for SOAP requests
        """
        return {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': soap_action,
            'apikey': self.APIGEE_API_KEY
        }
    def get_headers_bss(self):
        """
        Get JSON headers for callback requests to BSS
        Returns: dict: Headers for BSS callbacks
        """
        return {
        'Content-Type': 'application/json',
        'User-Agent': 'MNP-GW/1.0'
        }

# Create a single settings instance
settings = Settings()
