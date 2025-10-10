import logging
from config import settings
import os

class LoggerService:
    """Centralized logging service for the application"""
    def __init__(self):
        self.setup_regular_logger()
        self.setup_payload_logger()
    
    def setup_regular_logger(self):
        """Configure regular application logger"""
        # Configure the root logger first
        logging.basicConfig(
            level=settings.LOG_LEVEL,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(settings.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        
        # Create our main application logger
        self.logger = logging.getLogger('mnp_gateway')
        # Don't set level here as it inherits from basicConfig
    
    def setup_payload_logger(self):
        """Configure payload-specific logger"""
        # Create a completely separate logger for payloads
        self.payload_logger = logging.getLogger('mnp_payload')
        self.payload_logger.setLevel(logging.INFO)
        
        # CRITICAL: Prevent propagation to avoid duplicate logs
        self.payload_logger.propagate = False
        
        # Clear any existing handlers to avoid duplicates
        if self.payload_logger.handlers:
            self.payload_logger.handlers.clear()
        
        # Create formatter for payload logs
        payload_formatter = logging.Formatter('%(asctime)s - PAYLOAD - %(message)s')
        
        # Create file handler for payload logs
        payload_file_handler = logging.FileHandler(settings.PAYLOAD_LOG_FILE)
        payload_file_handler.setFormatter(payload_formatter)
        self.payload_logger.addHandler(payload_file_handler)
           
    def should_log_payload(self, service_type: str) -> bool:
        """Check if payload should be logged based on configuration"""
        if service_type == 'NC' and settings.SAVE_PAYLOAD_TO_LOG in [1, 3]:
            return True
        if service_type == 'BSS' and settings.SAVE_PAYLOAD_TO_LOG in [2, 3]:
            return True
        return False
    
    def log_payload(self, service_type: str, operation: str, direction: str, payload: str):
        """Unified payload logging method"""
        if not self.should_log_payload(service_type):
            return
        
        log_message = f"{service_type}_{operation}_{direction}: {payload}"
        self.payload_logger.info(log_message)

# Create singleton instance
logger_service = LoggerService()

# Export the loggers for easy importing
logger = logger_service.logger
payload_logger = logger_service.payload_logger
log_payload = logger_service.log_payload
