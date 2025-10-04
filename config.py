"""
Configuration settings for MNP Gateway API.
"""
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()
# Logging Configuration
LOG_FILE = os.getenv('LOG_FILE', 'mnp.log')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Configure logging
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Create a logger for this module
logger = logging.getLogger(__name__)

# Create a settings class or object to hold all configuration
class Settings:
    # API Configuration
    API_TITLE = "MNP Gateway API"
    API_DESCRIPTION = "Gateway for handling MNP queries between BSS and Central Node"
    API_VERSION = "1.0.0"
    API_V1_PREFIX = "/api/v1"
    
    # Server Configuration
    HOST = "0.0.0.0"
    PORT = 8000
    
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_USER = os.getenv('DB_USER', 'root')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    DB_NAME = os.getenv('DB_NAME', 'mnp_database')
    DB_PORT = int(os.getenv('DB_PORT', 3306))
    
    # SOAP Service Configuration
    SOAP_URL = os.getenv('SOAP_URL', 'http://webservices.oorsprong.org/websamples.countryinfo/CountryInfoService.wso')
    WSDL_SERVICE_URL = os.getenv('WSDL_SERVICE_URL')
    
    # SOAP Action headers
    SOAP_ACTION = "http://www.oorsprong.org/websamples.countryinfo/CapitalCity"
    SOAP_ACTION_SOLICITUD = "crearSolicitud"
    
    # Namespace definitions
    SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
    POR_NS = "http://nc.aopm.es/v1-10/portabilidad"
    V1_NS = "http://nc.aopm.es/v1-10"
    
    # Database configuration as dict (for existing db_utils compatibility)
    @property
    def MYSQL_CONFIG(self):
        return {
            'host': self.DB_HOST,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD,
            'database': self.DB_NAME,
            'port': self.DB_PORT
        }

# Create a single settings instance
settings = Settings()
