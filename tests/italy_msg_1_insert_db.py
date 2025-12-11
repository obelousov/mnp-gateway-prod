from datetime import datetime
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from models.models import ItalyPortInRequest  # Fixed import based on previous discussion
from config import settings
from services.logger_simple import logger
from typing import Optional
from services.italy.database_services import save_portin_request
import random

# Database connection manager class
class DatabaseManager:
    """Manages database engine and sessions without global variables"""
    _instance = None
    _engine = None
    _SessionLocal = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def get_engine(self):
        """Get or create database engine with connection pooling"""
        if self._engine is None:
            DATABASE_URL = f"{settings.DB_DRIVER}://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{int(settings.DB_PORT)}/{settings.DB_NAME}"
            
            self._engine = create_engine(
                DATABASE_URL,
                pool_pre_ping=True,          # Verify connections before use
                pool_size=5,                 # Number of connections to keep open
                max_overflow=10,             # Max connections beyond pool_size
                pool_recycle=3600,           # Recycle connections after 1 hour
                echo=False,                  # Set to True for SQL debugging
                connect_args={
                    'connect_timeout': 10    # Connection timeout in seconds
                }
            )
            
            self._SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self._engine
            )
        
        return self._engine
    
    def get_session(self):
        """Get a new database session from the connection pool"""
        if self._SessionLocal is None:
            self.get_engine()  # Initialize if needed
        if self._SessionLocal is None:
            raise RuntimeError("Database session factory (_SessionLocal) is not initialized.")
        return self._SessionLocal()
    
    def close(self):
        """Close all database connections (call on shutdown)"""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._SessionLocal = None
            logger.info("Database engine closed")

_db_manager = DatabaseManager()

def get_database_engine():
    """Get or create database engine with connection pooling"""
    return _db_manager.get_engine()

def get_db_session():
    """Get a new database session from the connection pool"""
    return _db_manager.get_session()

def close_database_engine():
    """Close all database connections (call on shutdown)"""
    _db_manager.close()

# Context manager for automatic session handling
class DatabaseSession:
    """Context manager for database sessions with auto-commit/rollback"""
    def __init__(self):
        self.session = None
    
    def __enter__(self):
        self.session = get_db_session()
        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session is None:
            return
        try:
            if exc_type:
                self.session.rollback()
                logger.warning("Database session rolled back due to exception")
            else:
                self.session.commit()
        except SQLAlchemyError as e:
            logger.error("Error during session cleanup: %s", e)
            self.session.rollback()
        finally:
            self.session.close()

def save_portin_request_1(alta_data: dict) -> Optional[int]:
    """
    Save port-in request to database using SQLAlchemy ORM.
    
    Args:
        alta_data: Dictionary containing the port-in request data
    
    Returns:
        int: ID of the newly inserted record, or None if failed
    """
    try:
        current_datetime = datetime.now()
        
        # Create the ORM object
        portin_request = ItalyPortInRequest(
            # Direct mappings
            sender_operator=alta_data.get('sender_operator', ''),
            recipient_operator=alta_data.get('recipient_operator', ''),
            recipient_operator_code=alta_data.get('recipient_operator_code', ''),
            donating_operator_code=alta_data.get('donating_operator_code', ''),
            recipient_request_code=alta_data.get('recipient_request_code', ''),
            phone_number=alta_data.get('msisdn', ''),  # Map msisdn to phone_number
            iccid_serial_number=alta_data.get('iccid_serial_number'),
            tax_code_vat=alta_data.get('tax_code_vat'),
            payment_type=alta_data.get('payment_type'),
            analog_digital_code=alta_data.get('analog_digital_code'),
            
            # Convert string date to Date object for cut_over_date
            cut_over_date=datetime.strptime(
                alta_data.get('cutover_date', ''), '%Y-%m-%d'
            ).date() if alta_data.get('cutover_date') else None,
            
            customer_first_name=alta_data.get('customer_first_name'),
            customer_last_name=alta_data.get('customer_last_name'),
            imsi=alta_data.get('imsi'),
            credit_transfer_flag=alta_data.get('credit_transfer_flag'),
            routing_number=alta_data.get('routing_number'),
            pre_validation_flag=alta_data.get('pre_validation_flag'),
            theft_flag=alta_data.get('theft_flag'),
            
            # Generated fields
            file_date=current_datetime.date(),
            file_time=current_datetime.strftime('%H:%M:%S'),
            file_id=f"FILE_{current_datetime.strftime('%Y%m%d_%H%M%S')}",
            message_type_code='PI',
            status='RECEIVED'
        )
        
        # Validate required fields
        required_fields = ['recipient_request_code', 'phone_number', 'cut_over_date']
        for field in required_fields:
            if getattr(portin_request, field) is None:
                raise ValueError(f"Required field '{field}' is missing or invalid")
        
        # Save using context manager
        with DatabaseSession() as session:
            session.add(portin_request)
            session.commit()
            session.refresh(portin_request)

            # Store the ID before session closes
            inserted_id = portin_request.id
        
            logger.info("Inserted new Italy port-in request with ID: %s", portin_request.id)
            logger.info("Recipient request code: %s", portin_request.recipient_request_code)
        return inserted_id
        
    except (SQLAlchemyError, IntegrityError, ValueError) as e:
        logger.error("Database error creating Italy port-in request: %s", str(e))
        # Don't re-raise, return None instead for graceful handling
        return None

def generate_recipient_request_code(
    recipient_operator_code: str,
    custom_timestamp: Optional[datetime] = None
) -> str:
    """
    Generate a recipient request code according to Italy MNP XSD specification.
    
    Format: [OPERATOR][YYMMDDHHMMSS][SEQUENCE]
    Total length: 4 (operator) + 12 (timestamp) + 2 (sequence) = 18 characters max
    
    Args:
        recipient_operator_code: 4-character operator code (e.g., 'LMIT', 'NOVA')
        custom_timestamp: Optional datetime to use (defaults to current UTC time)
    
    Returns:
        str: 18-character recipient request code
    
    Example:
        generate_recipient_request_code('LMIT') → 'LMIT25120621190342'
        (LMIT + 25-12-06 21:19:03 + sequence 42)
    """
    # Validate operator code
    if not recipient_operator_code or len(recipient_operator_code) != 4:
        raise ValueError(f"recipient_operator_code must be exactly 4 characters, got: {recipient_operator_code}")
    
    # Use provided timestamp or current UTC time
    timestamp = custom_timestamp or datetime.utcnow()
    
    # Format: YYMMDDHHMMSS (12 characters)
    timestamp_str = timestamp.strftime('%y%m%d%H%M%S')  # %y = 2-digit year
    
    # Generate 2-digit random sequence (00-99)
    # You could use database sequence instead for true uniqueness
    sequence = f"{random.randint(0, 99):02d}"
    
    # Combine: 4 + 12 + 2 = 18 characters total
    request_code = f"{recipient_operator_code}{timestamp_str}{sequence}"
    
    # Double-check length (should always be 18)
    if len(request_code) != 18:
        raise ValueError(f"Generated request code length invalid: {len(request_code)} characters")
    
    return request_code


if __name__ == "__main__":
    # Test with sample data
    print(generate_recipient_request_code('LMIT'))
    exit()

    sample_data = {
        "sender_operator": "LMIT",
        "recipient_operator": "NOVA",
        "recipient_operator_code": "LMIT",
        "donating_operator_code": "NOVA",
        "recipient_request_code": "LYCA25101302284",
        "msisdn": "393203004083",
        "iccid_serial_number": "8939079000021073033",
        "tax_code_vat": "MRVLTZ55A41H717E",
        "payment_type": "PRP",
        "analog_digital_code": "D",
        "cutover_date": "2025-12-15",
        "customer_first_name": "Test",
        "customer_last_name": "User",
        "imsi": "222353002765232",
        "credit_transfer_flag": "Y",
        "routing_number": "382",
        "pre_validation_flag": "Y",
        "theft_flag": "N"
    }
    
    # Test the optimized version
    request_id = save_portin_request(sample_data)
    if request_id:
        print(f"✅ Success! Inserted record with ID: {request_id}")
    else:
        print("❌ Failed to insert record")