from sqlalchemy import Column, BigInteger, String, Integer, Boolean, DateTime, text, ForeignKey, TIMESTAMP, Enum, Text, Index, SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import Date,JSON

Base = declarative_base()

class PortoutMetadata(Base):
    __tablename__ = 'portout_metadata'
    __table_args__ = (
        Index('idx_response_code', 'response_code'),
    )
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    response_code = Column(String(50))
    response_description = Column(String(255))
    paged_request_code = Column(String(100))
    total_records = Column(Integer)
    is_last_page = Column(Boolean)
    received_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))
    
    # Relationship
    requests = relationship("PortoutRequest", back_populates="portout_metadata")

class PortoutRequest(Base):
    __tablename__ = 'portout_request'
    __table_args__ = (
        Index('idx_metadata_id', 'metadata_id'),
        Index('idx_msisdn', 'MSISDN'),
        Index('idx_reference_code', 'reference_code'),
    )
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    metadata_id = Column(BigInteger, ForeignKey('portout_metadata.id'), nullable=False)
    notification_id = Column(String(50))
    creation_date = Column(DateTime)
    synchronized = Column(Boolean)
    reference_code = Column(String(50))
    status_bss = Column(String(50))
    submitted_to_bss = Column(Boolean, server_default=text('0'), comment='0 - not submitted, 1 - submitted')
    status_nc = Column(String(50))
    status = Column(String(10))
    response_code = Column(String(10))
    confirm_reject = Column(SmallInteger, comment='confirm=1, reject=2 for offline confirmation to NC')
    cancellation_reason = Column(String(100), comment='eg CANC_ABONA')
    description = Column(String(200), comment='description with return NC on reject or confirm')
    state_date = Column(DateTime)
    creation_date_request = Column(DateTime)
    reading_mark_date = Column(DateTime)
    state_change_deadline = Column(DateTime)
    subscriber_request_date = Column(DateTime)
    donor_operator_code = Column(String(10))
    receiver_operator_code = Column(String(10))
    extraordinary_donor_activation = Column(Boolean)
    contract_code = Column(String(100))
    receiver_NRN = Column(String(50))
    port_window_date = Column(DateTime)
    port_window_by_subscriber = Column(Boolean)
    MSISDN = Column(String(20))
    subscriber_type = Column(String(20), server_default=text("'person'"), comment='person or legal')
    subscriber_id_type = Column(String(10))
    subscriber_id_number = Column(String(30))
    subscriber_first_name = Column(String(100))
    subscriber_last_name_1 = Column(String(100))
    subscriber_last_name_2 = Column(String(100))
    company_name = Column(String(100))
    scheduled_at = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    msisdn_single = Column(JSON, comment='List of individual MSISDNs as strings, e.g., ["621800007"]')
    msisdn_ranges = Column(JSON, comment='List of MSISDN range objects, e.g., [{"initial_value": "621800011", "final_value": "621800012"}]')
    
    # Relationship
    portout_metadata = relationship("PortoutMetadata", back_populates="requests")

class PortabilityRequests(Base):
    __tablename__ = 'portability_requests'
    __table_args__ = (
        Index('idx_country_type', 'country_code', 'request_type'),
        Index('idx_status_composite', 'status_nc', 'status_bss', 'scheduled_at'),
        Index('idx_msisdn', 'msisdn'),
        Index('idx_reference_code', 'reference_code'),
        Index('idx_session_code', 'session_code'),
        Index('idx_donor_recipient', 'donor_operator', 'recipient_operator'),
        Index('idx_created_at', 'created_at'),
        Index('idx_scheduled_status', 'scheduled_at', 'status_nc'),
        Index('idx_completion', 'completed_at', 'country_code'),
        Index('idx_document', 'document_type', 'document_number'),
        {'comment': 'Mobile number portability requests (IN/OUT/CANCEL/MULTISIM)'}
    )
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    country_code = Column(String(3), nullable=False, server_default=text("'ESP'"))
    request_type = Column(Enum('PORT_IN', 'PORT_OUT', 'CANCELLATION', 'MULTISIM', 'EXTENSION', name='request_type_enum'), nullable=False)
    cancel_request_id = Column(BigInteger, nullable=False, server_default=text('0'), comment='id of portin request')
    reference_code = Column(String(100), server_default=text("''"), comment='Unique reference from NC system')
    session_code = Column(String(100), comment='Session identifier from BSS')
    session_code_nc = Column(String(100), comment='Session identifier from NC')
    request_code = Column(String(100), comment='Internal request identifier')
    status_bss = Column(String(100), server_default=text("'PROCESSING'"), comment='BSS system status')
    status_nc = Column(String(50), server_default=text("'PENDING'"), comment='NC system status')
    response_code = Column(String(20), server_default=text("''"), comment='coddigoRespuesta')
    response_status = Column(String(20), server_default=text("''"), comment='Latest response code from NC')
    reject_code = Column(String(10), server_default=text("''"), comment='reject status eg RECH_BNUME RECH_PERDI RECH_IDENT RECH_ICCID')
    description = Column(String(1000))
    retry_count = Column(Integer, server_default=text('0'))
    last_error = Column(Text, comment='Last error message')
    error_description = Column(String(255))
    msisdn = Column(String(15), nullable=False, server_default=text("''"), comment='Phone number')
    iccid = Column(String(22), comment='SIM card number')
    subscriber_type = Column(Enum('person', 'company', name='subscriber_type_enum'), comment='person/company')
    document_type = Column(Enum('NIF', 'CIF', 'NIE', 'PAS', name='document_type_enum'), nullable=False)
    document_number = Column(String(50), nullable=False, server_default=text("''"))
    first_name = Column(String(100))
    first_surname = Column(String(100))
    second_surname = Column(String(100))
    nationality = Column(String(64), server_default=text("'España'"))
    name_surname = Column(String(200), nullable=False, server_default=text("''"))
    contract_number = Column(String(100), comment='Subscriber contract number')
    donor_operator = Column(String(50), nullable=False, server_default=text("''"), comment='Current operator')
    recipient_operator = Column(String(50), nullable=False, server_default=text("''"), comment='New operator')
    routing_number = Column(String(20), server_default=text("''"), comment='Routing information')
    desired_porting_date = Column(String(20))
    porting_window = Column(DateTime, comment='Scheduled porting window date/time from NC')
    reason_code = Column(String(50), comment='Reason for port-out')
    cancellation_reason = Column(String(150))
    cancellation_initiated_by_donor = Column(String(150))
    requested_at = Column(TIMESTAMP, comment='When customer requested')
    scheduled_at = Column(TIMESTAMP, comment='When to process next')
    completed_at = Column(TIMESTAMP, comment='When request completed')
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    is_legal_entity = Column(Boolean, nullable=False, server_default=text('0'), comment='Flag indicating if this is a legal entity (1) or individual (0)')
    company_name = Column(String(255), comment='Company name for legal entities')

class ReturnRequests(Base):
    __tablename__ = 'return_requests'
    __table_args__ = (
        Index('idx_return_status_scheduled', 'status_nc', 'scheduled_at'),  # For job scheduling
        Index('idx_return_msisdn', 'msisdn'),  # For customer lookups
        Index('idx_return_reference_code', 'reference_code'),  # For NC reference lookups
        {'comment': 'Mobile number Return requests'}
    )
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    request_type = Column(String(20), nullable=False, server_default=text("'RETURN'"), comment='Request type: RETURN')
    request_date = Column(Date, nullable=True, comment='Request date from API in YYYY-MM-DD format')
    msisdn = Column(String(15), nullable=True, comment='Phone number (9 digits without country code)')
    cancellation_reason = Column(String(150))
    reference_code = Column(String(100), comment='CodigoReferencia - Unique reference from NC system')
    status_bss = Column(String(50), server_default=text("'PROCESSING'"), comment='BSS system status: PROCESSING, COMPLETED, FAILED')
    status_nc = Column(String(50), server_default=text("'PENDING'"), comment='NC system status: PENDING, PENDING_RESPONSE, COMPLETED, FAILED')
    response_code = Column(String(20), comment='codigoRespuesta from NC response')
    response_status = Column(String(20), comment='estado - latest response status from NC')
    reject_code = Column(String(20), comment='reject status eg RECH_BNUME RECH_PERDI RECH_IDENT RECH_ICCID')
    description = Column(Text, comment='Response description from NC')
    retry_count = Column(Integer, server_default=text('0'), comment='Number of retry attempts')
    last_error = Column(Text, comment='Last error message')
    error_description = Column(Text, comment='Detailed error description')
    requested_at = Column(TIMESTAMP, comment='When customer submitted the request')
    scheduled_at = Column(TIMESTAMP, comment='When to process next (for retries)')
    completed_at = Column(TIMESTAMP, comment='When request completed successfully')
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    # New fields from NC response
    status_date = Column(TIMESTAMP, comment='statusDate from NC response - status timestamp')
    creation_date = Column(TIMESTAMP, comment='creationDate from NC response - request creation timestamp')
    subscriber_cancellation_date = Column(TIMESTAMP, comment='subscriberCancellationDate from NC response')
    recipient_operator_code = Column(String(10), comment='recipientOperatorCode from NC response')
    donor_operator_code = Column(String(10), comment='donorOperatorCode from NC response')
    change_window_date = Column(TIMESTAMP, comment='changeWindowDate from NC response - porting date')

class ItalyPortInRequest(Base):
    """Table to store Italy MNP port-in request information - message type 1 (ATTIVAZIONE)"""
    __tablename__ = 'italy_port_in_requests'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # File metadata (from FILENAME section)
    sender_operator = Column(String(50), nullable=False)
    file_date = Column(Date, nullable=False)
    file_time = Column(String(8), nullable=False)
    recipient_operator = Column(String(50), nullable=False)
    file_id = Column(String(50), nullable=False)
    
    # Message type info
    message_type_code = Column(String(2), nullable=False)
    recipient_operator_code = Column(String(50), nullable=False)
    donating_operator_code = Column(String(50), nullable=False)
    recipient_request_code = Column(String(50), nullable=False, unique=True)
    
    # Customer/Porting info
    phone_number = Column(String(20), nullable=False)
    iccid_serial_number = Column(String(20))
    tax_code_vat = Column(String(50))
    payment_type = Column(String(3))
    analog_digital_code = Column(String(1))
    cut_over_date = Column(Date, nullable=False)
    customer_first_name = Column(String(100))
    customer_last_name = Column(String(100))
    imsi = Column(String(20))
    
    # Flags and additional info
    credit_transfer_flag = Column(String(1), default='N')
    routing_number = Column(String(10))
    pre_validation_flag = Column(String(1), default='N')
    theft_flag = Column(String(1), default='N')
    
    # Status tracking - MySQL native timestamps
    status = Column(String(20), default='PENDING')
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'), nullable=False)
    updated_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), nullable=False)
    
    # Scheduled processing time
    scheduled_at = Column(DateTime, comment='When this request is scheduled for processing')

class ItalyAllPortRequests(Base):
    """Only 7 essential columns + XML"""
    __tablename__ = 'italy_port_requests'
    
    id = Column(Integer, primary_key=True)
    recipient_request_code = Column(String(50), unique=True, index=True)
    msisdn = Column(String(20), index=True)
    message_type_code = Column(String(2), index=True)
    process_status = Column(String(50), default='RECEIVED', index=True)
    cut_over_date = Column(Date, index=True)
    xml = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'), index=True)
    updated_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    
    # Relationships
    status_history = relationship(
        "ItalyPortInStatusHistory", 
        back_populates="request",
        cascade="all, delete-orphan",
        lazy="dynamic"
    )
    
    scheduled_actions = relationship(
        "ItalyPortInScheduledAction", 
        back_populates="request",
        cascade="all, delete-orphan",
        lazy="dynamic"
    )
    
    # Composite indexes for common queries
    __table_args__ = (
        Index('idx_request_msisdn_status', 'msisdn', 'process_status'),
        Index('idx_request_type_status', 'message_type_code', 'process_status'),
        Index('idx_request_cutover_status', 'cut_over_date', 'process_status'),
        {
            'mysql_engine': 'InnoDB',
            'mysql_charset': 'utf8mb4',
            'comment': 'Main table for Italy MNP porting requests'
        }
    )


class ItalyPortInStatusHistory(Base):
    __tablename__ = "italy_port_in_status_history"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key to main request table
    portin_request_id = Column(
        Integer, 
        ForeignKey("italy_port_requests.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Consistent with main table
    message_type_code = Column(  # Changed from 'message_type'
        String(2),  # Changed from String(5) to match main table
        nullable=False,
        comment='Message type code (1-13) when status changed'
    )
    
    # field for clarity
    status_reason = Column(
        String(100),
        nullable=True,
        comment='Why status changed: timeout, manual, system, etc.'
    )
    
    old_status = Column(String(50))
    new_status = Column(String(50), nullable=False)
    
    payload = Column(
        JSON,
        nullable=True,
        comment='Additional context: error details, response XML, etc.'
    )
    
    created_at = Column(
        TIMESTAMP, 
        server_default=text('CURRENT_TIMESTAMP'),
        nullable=False,
        index=True
    )
    
    # Relationship is back-populated
    request = relationship(
        "ItalyAllPortRequests",
        back_populates="status_history"
    )
    
    __table_args__ = (
        # Composite indexes for common queries
        Index('idx_status_history_request_status', 'portin_request_id', 'created_at'),
        # index for status queries
        Index('idx_status_history_new_status', 'new_status', 'created_at'),
        {
            'mysql_engine': 'InnoDB',
            'mysql_charset': 'utf8mb4',
            'comment': 'Audit trail of all status changes for porting requests'
        }
    )

class ItalyPortInScheduledAction(Base):
    __tablename__ = "italy_port_in_scheduled_actions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key to main request table
    portin_request_id = Column(
        Integer, 
        ForeignKey("italy_port_requests.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    action_type = Column(
        String(30), 
        nullable=False,
        comment='SEND_MSG2, CHECK_STATUS, RETRY, NOTIFY_BSS, CLEANUP'
    )
    
    scheduled_at = Column(
        DateTime, 
        nullable=False,
        index=True,
        comment='When to execute (UTC)'
    )
    
    executed_at = Column(
        DateTime, 
        index=True,
        nullable=True  # Should be nullable for pending actions
    )
    
    # Status field with defined states
    status = Column(
        String(20),
        server_default="PENDING", 
        nullable=False,
        comment='PENDING, EXECUTING, COMPLETED, FAILED, CANCELLED'
    )
    
    retry_count = Column(Integer, default=0)
    last_error = Column(Text)
    
    # Consistent lengths   
    depends_on_message_type = Column(
        String(2),  # Changed from String(5) to match message_type_code
        comment='Wait for this message type (1-13) before executing'
    )
    
    depends_on_status = Column(
        String(50),
        comment='Wait for this status before executing'
    )
    
    expires_at = Column(
        DateTime,
        comment='Hard stop - never execute after this (UTC)'
    )
    
    created_at = Column(
        TIMESTAMP, 
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP')
    )
    
    # Relationship is back-populated
    request = relationship(
        "ItalyAllPortRequests",
        back_populates="scheduled_actions"
    )
    
    __table_args__ = (
        # Indexes for common queries 
        Index('idx_scheduled_due', 'status', 'scheduled_at'),
        Index('idx_scheduled_dependencies', 'depends_on_message_type', 'depends_on_status'),
        # Index for cleanup queries
        Index('idx_scheduled_expired', 'status', 'expires_at'),
        {
            'mysql_engine': 'InnoDB',
            'mysql_charset': 'utf8mb4',
            'comment': 'Future actions scheduled for porting requests'
        }
    )