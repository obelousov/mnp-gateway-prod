import logging
from config import settings

class LoggerService:
    def __init__(self):
        self.setup_application_logger()
        self.setup_payload_logger()
    
    def setup_application_logger(self):
        """Configure basic application logger"""
        self.logger = logging.getLogger('mnp_gateway')
        
        # Convert string log level to logging constant
        log_level_str = getattr(settings, 'LOG_LEVEL', 'INFO')
        level_mapping = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        log_level = level_mapping.get(log_level_str, logging.INFO)
        self.logger.setLevel(log_level)
        self.logger.propagate = False
        
        # Clear existing handlers
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Create basic formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Add file handler
        app_log_file = getattr(settings, 'APP_LOG_FILE', 'logs/mnp.log')
        file_handler = logging.FileHandler(app_log_file)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Add stdout handler if needed
        if getattr(settings, 'ENABLE_STDOUT_LOGGING', True):
            stdout_handler = logging.StreamHandler()
            stdout_handler.setFormatter(formatter)
            self.logger.addHandler(stdout_handler)
    
    def setup_payload_logger(self):
        """Configure basic payload logger"""
        self.payload_logger = logging.getLogger('mnp_payload')
        self.payload_logger.setLevel(logging.INFO)
        self.payload_logger.propagate = False
        
        # Clear existing handlers
        if self.payload_logger.handlers:
            self.payload_logger.handlers.clear()
        
        # Create basic formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Add file handler
        payload_log_file = getattr(settings, 'PAYLOAD_LOG_FILE', 'logs/payload.log')
        file_handler = logging.FileHandler(payload_log_file)
        file_handler.setFormatter(formatter)
        self.payload_logger.addHandler(file_handler)
        
        # Add stdout handler if needed
        if getattr(settings, 'ENABLE_STDOUT_LOGGING', True):
            stdout_handler = logging.StreamHandler()
            stdout_handler.setFormatter(formatter)
            self.payload_logger.addHandler(stdout_handler)
    
    def should_log_payload(self, service_type: str) -> bool:
        """Check if payload should be logged based on configuration"""
        save_payload = getattr(settings, 'SAVE_PAYLOAD_TO_LOG', 3)
        if service_type == 'NC' and save_payload in [1, 3]:
            return True
        if service_type == 'BSS' and save_payload in [2, 3]:
            return True
        return False
    
    def log_payload(self, service_type: str, operation: str, direction: str, payload: str):
        """Simple payload logging method"""
        if not self.should_log_payload(service_type):
            return
        
        # Simple log format: SERVICE_OPERATION_DIRECTION: payload
        log_message = f"{service_type}_{operation}_{direction}: {payload}"
        self.payload_logger.info(log_message)

# Create singleton instance
logger_service = LoggerService()

# Export both loggers
logger = logger_service.logger
log_payload = logger_service.log_payload