# api/core/metrics.py
from prometheus_client import Counter, Histogram, Gauge

# HTTP Metrics
REQUEST_COUNT = Counter(
    'mnp_http_requests_total', 
    'Total HTTP Requests', 
    ['method', 'endpoint', 'status_code']
)

REQUEST_LATENCY = Histogram(
    'mnp_http_request_duration_seconds', 
    'HTTP request latency', 
    ['method', 'endpoint']
)

ACTIVE_REQUESTS = Gauge(
    'mnp_http_requests_active', 
    'Active HTTP requests'
)

# Business Logic Metrics
PORT_IN_REQUESTS = Counter(
    'mnp_port_in_requests_total',
    'Total port-in requests',
    ['status']  # success, validation_error, business_error, server_error
)

PORT_IN_PROCESSING_TIME = Histogram(
    'mnp_port_in_processing_seconds',
    'Port-in request processing time'
)

PORT_OUT_REQUESTS = Counter(
    'mnp_port_out_requests_total',
    'Total port-out requests',
    ['status']
)

CANCEL_REQUESTS = Counter(
    'mnp_cancel_requests_total',
    'Total cancellation requests',
    ['status']
)

# System Metrics
DATABASE_CONNECTIONS = Gauge(
    'mnp_database_connections_active',
    'Active database connections'
)

CELERY_TASKS = Counter(
    'mnp_celery_tasks_total',
    'Total Celery tasks',
    ['task_name', 'status']
)

# Error Metrics
ERROR_COUNT = Counter(
    'mnp_errors_total',
    'Total errors by type',
    ['error_type', 'endpoint']
)

def record_port_in_success():
    PORT_IN_REQUESTS.labels(status="success").inc()

def record_port_in_error(error_type: str):
    PORT_IN_REQUESTS.labels(status=error_type).inc()

def record_port_in_processing_time(processing_time: float):
    PORT_IN_PROCESSING_TIME.observe(processing_time)

def record_error(error_type: str, endpoint: str = "unknown"):
    ERROR_COUNT.labels(error_type=error_type, endpoint=endpoint).inc()