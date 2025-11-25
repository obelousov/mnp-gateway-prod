# celery.py (in project's root directory)
from celery import Celery # type: ignore
from dotenv import load_dotenv
import os
from config import settings

# Load environment variables from .env file
load_dotenv()

# Get Redis URL from environment variables
redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
PENDING_REQUESTS_TIMEOUT = float(os.getenv('PENDING_REQUESTS_TIMEOUT', '60.0'))
TIME_DELTA_FOR_PORT_OUT_STATUS_CHECK = settings.TIME_DELTA_FOR_PORT_OUT_STATUS_CHECK
TIME_DELTA_FOR_RETURN_STATUS_CHECK = settings.TIME_DELTA_FOR_RETURN_STATUS_CHECK

# Create the Celery instance
app = Celery('mnp_worker',
             broker=redis_url,
             backend=redis_url,
            #  include=['tasks'])
            #  include=['tasks', 'tasks_pending_requests'])  # ← ADD BOTH MODULES HERE
            include=['tasks.tasks', 'tasks.pending_requests'])

# Optional configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    # timezone='Europe/Madrid',  # ← Changed to Madrid timezone
    # enable_utc=False,  # ← Set to False since we're using specific timezone
)

app.conf.timezone = 'Europe/Madrid'
app.conf.enable_utc = False

# Beat Schedule Configuration
app.conf.beat_schedule = {
    # Run a task every 60 seconds that prints a message
    # 'print-message-every-60-seconds': {
    #     'task': 'tasks.print_periodic_message',
    #     'schedule': 60.0,  # Every 60 seconds
    # },
    # 'print-message-every-60-seconds-from-pending_tasks': {
    #     'task': 'tasks_pending_requests.print_periodic_message',
    #     'schedule': 30.0,  # Every 60 seconds
    # },
    'process-pending-requests-every-60-seconds': {
        # 'task': 'tasks_pending_requests.process_pending_requests',
        'task': 'tasks.pending_requests.process_pending_requests',
        'schedule': PENDING_REQUESTS_TIMEOUT, 
    },
    'process-check-port-out': {
        # 'task': 'tasks_pending_requests.process_pending_requests',
        'task': 'tasks.tasks.check_status_port_out',
        'schedule': TIME_DELTA_FOR_PORT_OUT_STATUS_CHECK, 
    },
    'process-check-return': {
        # 'task': 'tasks_pending_requests.process_pending_requests',
        'task': 'tasks.tasks.process_pending_return_status_checks',
        'schedule': TIME_DELTA_FOR_RETURN_STATUS_CHECK, 
    },
}

# This allows you to run this module directly for debugging
if __name__ == '__main__':
    app.start()