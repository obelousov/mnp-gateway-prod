import logging
import json
import os
import threading
from datetime import datetime, timezone
import sys
from config import settings

class LoggerService:
    """Centralized logging service compliant with the required JSON format"""
    def __init__(self):
        self.setup_environment_config()
        self.setup_regular_logger()
        self.setup_payload_logger()
        self.suppress_celery_logs()
    
    def setup_environment_config(self):
        """Read configuration from environment variables"""
        self.service_name = os.getenv('SERVICE_NAME', 'mnp-gateway')
        self.service_name = settings.SERVICE_NAME
        self.artifact_id = settings.ARTIFACT_ID
        self.enable_file_logging = settings.ENABLE_FILE_LOGGING
        self.enable_stdout_logging = settings.ENABLE_STDOUT_LOGGING
        self.app_log_file = settings.APP_LOG_FILE
        self.payload_log_file = settings.PAYLOAD_LOG_FILE
        
        # Handle payload configuration
        save_payload = settings.SAVE_PAYLOAD_TO_LOG
        try:
            self.save_payload_to_log = int(save_payload)
        except ValueError:
            self.save_payload_to_log = 3
        
        # Log level mapping to numeric values
        self.level_values = {
            'TRACE': 10000,
            'DEBUG': 20000,
            'INFO': 30000,
            'WARNING': 40000,
            'ERROR': 50000,
            'CRITICAL': 60000
        }
    
    def setup_regular_logger(self):
        """Configure regular application logger with required JSON format"""
        # Create our main application logger
        self.logger = logging.getLogger('mnp_gateway')
        
        # Convert string log level to logging constant
        log_level_str = os.getenv('LOG_LEVEL', 'INFO')
        level_mapping = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        log_level = level_mapping.get(log_level_str, logging.INFO)
        self.logger.setLevel(log_level)
        
        # Clear any existing handlers
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Create JSON formatter with required fields
        class RequiredJSONFormatter(logging.Formatter):
            def __init__(self, service_name, artifact_id, level_values):
                super().__init__()
                self.service_name = service_name
                self.artifact_id = artifact_id
                self.level_values = level_values
            
            def format(self, record):
                # Get numeric level value
                level_value = self.level_values.get(record.levelname, 30000)
                
                # Build tags array with module, function, line
                tags = [
                    f"module:{record.module}",
                    f"function:{record.funcName}",
                    f"line:{record.lineno}"
                ]
                
                # Create log record with required fields
                log_record = {
                    "@timestamp": self.formatTime(record),
                    "@version": "1",
                    "log_type": "LOG",
                    "log_level": record.levelname,
                    "level_value": level_value,
                    "logger_name": record.name,
                    "service_name": self.service_name,
                    "artifact_id": self.artifact_id,
                    "trace_token": getattr(record, 'trace_token', 'undefined'),
                    "thread_name": record.threadName,
                    "message": record.getMessage(),
                    "tags": tags
                }
                
                # Add stack_trace if exception present
                if record.exc_info:
                    log_record["stack_trace"] = self.formatException(record.exc_info)
                
                # Convert to single-line JSON string
                return json.dumps(log_record, ensure_ascii=False)
            
            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
                return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        # Create formatter
        formatter = RequiredJSONFormatter(self.service_name, self.artifact_id, self.level_values)
        
        handlers = []
        
        # Add stdout handler if enabled
        if self.enable_stdout_logging:
            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setFormatter(formatter)
            stdout_handler.addFilter(lambda record: not record.name.startswith('celery'))
            handlers.append(stdout_handler)
        
        # Add file handler if enabled
        if self.enable_file_logging:
            file_handler = logging.FileHandler(self.app_log_file)
            file_handler.setFormatter(formatter)
            file_handler.addFilter(lambda record: not record.name.startswith('celery'))
            handlers.append(file_handler)
        
        # Add all configured handlers to our logger
        for handler in handlers:
            self.logger.addHandler(handler)
        
        # Prevent propagation to root logger
        self.logger.propagate = False
        
        if not handlers:
            self.logger.addHandler(logging.NullHandler())
    
    def setup_payload_logger(self):
        """Configure payload-specific logger with required JSON format"""
        # Create a completely separate logger for payloads
        self.payload_logger = logging.getLogger('mnp_payload')
        self.payload_logger.setLevel(logging.INFO)
        
        # Prevent propagation to avoid duplicate logs
        self.payload_logger.propagate = False
        
        # Clear any existing handlers to avoid duplicates
        if self.payload_logger.handlers:
            self.payload_logger.handlers.clear()
        
        # Create JSON formatter for payload logs
        class PayloadJSONFormatter(logging.Formatter):
            def __init__(self, service_name, artifact_id, level_values):
                super().__init__()
                self.service_name = service_name
                self.artifact_id = artifact_id
                self.level_values = level_values
            
            def format(self, record):
                # Extract service_type, operation, direction from message
                message_parts = record.getMessage().split(':', 1)
                if len(message_parts) == 2:
                    prefix, payload = message_parts
                    service_parts = prefix.split('_')
                    if len(service_parts) >= 3:
                        service_type = service_parts[0]
                        operation = service_parts[1]
                        direction = service_parts[2]
                    else:
                        service_type = operation = direction = "unknown"
                else:
                    service_type = operation = direction = "unknown"
                    payload = record.getMessage()
                
                # Build tags for payload logs
                tags = [
                    f"service_type:{service_type}",
                    f"operation:{operation}",
                    f"direction:{direction}",
                    f"module:{record.module}",
                    f"function:{record.funcName}",
                    f"line:{record.lineno}"
                ]
                
                # Create payload log record with required fields
                log_record = {
                    "@timestamp": self.formatTime(record),
                    "@version": "1",
                    "log_type": "LOG",
                    "log_level": record.levelname,
                    "level_value": self.level_values.get(record.levelname, 30000),
                    "logger_name": record.name,
                    "service_name": self.service_name,
                    "artifact_id": self.artifact_id,
                    "trace_token": getattr(record, 'trace_token', 'undefined'),
                    "thread_name": record.threadName,
                    "message": f"{service_type} {operation} {direction}",
                    "payload_data": payload.strip(),
                    "tags": tags
                }
                
                return json.dumps(log_record, ensure_ascii=False)
            
            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
                return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        # Create formatter for payload logs
        payload_formatter = PayloadJSONFormatter(self.service_name, self.artifact_id, self.level_values)
        
        handlers = []
        
        # Add stdout handler if enabled
        if self.enable_stdout_logging:
            payload_stdout_handler = logging.StreamHandler(sys.stdout)
            payload_stdout_handler.setFormatter(payload_formatter)
            handlers.append(payload_stdout_handler)
        
        # Add file handler if enabled (separate file for payloads)
        if self.enable_file_logging:
            payload_file_handler = logging.FileHandler(self.payload_log_file)
            payload_file_handler.setFormatter(payload_formatter)
            handlers.append(payload_file_handler)
        
        # Add all configured handlers to payload logger
        for handler in handlers:
            self.payload_logger.addHandler(handler)
        
        if not handlers:
            self.payload_logger.addHandler(logging.NullHandler())
    
    def suppress_celery_logs(self):
        """Suppress Celery logs from our log files"""
        celery_loggers = [
            'celery', 'celery.utils', 'celery.worker', 'celery.app', 
            'celery.task', 'celery.result', 'celery.bootsteps',
            'celery.pool', 'celery.backends', 'celery.events', 'kombu'
        ]
        
        for logger_name in celery_loggers:
            celery_logger = logging.getLogger(logger_name)
            celery_logger.setLevel(logging.WARNING)
            celery_logger.propagate = False
    
    def should_log_payload(self, service_type: str) -> bool:
        """Check if payload should be logged based on configuration"""
        if service_type == 'NC' and self.save_payload_to_log in [1, 3]:
            return True
        if service_type == 'BSS' and self.save_payload_to_log in [2, 3]:
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