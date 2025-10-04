from datetime import datetime, timedelta
import logging, os
from dotenv import load_dotenv
import random

# Configuration
MORNING_WINDOW_START = 8
MORNING_WINDOW_END = 14
AFTERNOON_WINDOW_START = 14
AFTERNOON_WINDOW_END = 20


# Load environment variables from .env file
load_dotenv()

LOG_FILE = os.getenv('LOG_FILE', 'mnp.log')  # Default log file path
LOG_INFO = os.getenv('LOG_INFO', 'INFO')

MORNING_WINDOW_START = int(os.getenv('MORNING_WINDOW_START', "8"))
MORNING_WINDOW_END = int(os.getenv('MORNING_WINDOW_END', "14")  )
AFTERNOON_WINDOW_START = int(os.getenv('AFTERNOON_WINDOW_START', "14"))
AFTERNOON_WINDOW_END = int(os.getenv('AFTERNOON_WINDOW_END', "20"))

# Jitter configuration - spread tasks over this many minutes
JITTER_WINDOW_MINUTES = int(os.getenv('JITTER_WINDOW_MINUTES', '30'))  # Spread over 30 minutes

# Configure logging to both file and console
logging.basicConfig(
    level=LOG_INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),  # Container path
        logging.StreamHandler()  # Also show in docker logs
    ]
)

def parse_holidays(holidays_str):
    """Parse holidays from string format YYYY-MM-DD,YYYY-MM-DD"""
    holidays = []
    if holidays_str:
        for date_str in holidays_str.split(','):
            try:
                year, month, day = map(int, date_str.strip().split('-'))
                holidays.append(datetime(year, month, day).date())
            except ValueError:
                logging.warning("Invalid holiday date format: %s", date_str)
    return holidays

NATIONAL_HOLIDAYS = parse_holidays(os.getenv('NATIONAL_HOLIDAYS', ''))

def is_holiday(date):
    """Check if a given date is a holiday or weekend."""
    return date.weekday() >= 5 or date.date() in NATIONAL_HOLIDAYS  # 5=Saturday, 6=Sunday

def get_next_working_day(date):
    """Get the next working day (skip weekends and holidays)."""
    next_day = date + timedelta(days=1)
    while is_holiday(next_day):
        next_day += timedelta(days=1)
    return next_day

def calculate_countdown(with_jitter=True):
    """
    Calculate countdown seconds with holiday awareness.
    """
    now = datetime.now()
    current_hour = now.hour
    
    # Check if today is a holiday/weekend
    if is_holiday(now):
        logging.info("Current day is holiday/weekend, targeting next working day")
        next_working_day = get_next_working_day(now)
        target_time = datetime.combine(next_working_day.date(), datetime.min.time()).replace(
            hour=MORNING_WINDOW_START, minute=0, second=0
        )
        time_window = "next working day"
    
    # Normal business day logic
    elif MORNING_WINDOW_START <= current_hour < MORNING_WINDOW_END:
        # Morning → Check afternoon same day (if still business hours)
        target_afternoon = datetime.combine(now.date(), datetime.min.time()).replace(
            hour=AFTERNOON_WINDOW_START, minute=0, second=0
        )
        
        # If afternoon target is still today and not a holiday, use it
        if target_afternoon > now and not is_holiday(target_afternoon):
            target_time = target_afternoon
            time_window = "afternoon"
        else:
            # Otherwise target next working morning
            next_working_day = get_next_working_day(now)
            target_time = datetime.combine(next_working_day.date(), datetime.min.time()).replace(
                hour=MORNING_WINDOW_START, minute=0, second=0
            )
            time_window = "next working morning"
    
    elif AFTERNOON_WINDOW_START <= current_hour < AFTERNOON_WINDOW_END:
        # Afternoon → Check next working morning
        next_working_day = get_next_working_day(now)
        target_time = datetime.combine(next_working_day.date(), datetime.min.time()).replace(
            hour=MORNING_WINDOW_START, minute=0, second=0
        )
        time_window = "next working morning"
    
    else:
        # Outside hours → Check next working morning
        next_working_day = get_next_working_day(now)
        target_time = datetime.combine(next_working_day.date(), datetime.min.time()).replace(
            hour=MORNING_WINDOW_START, minute=0, second=0
        )
        time_window = "next working morning"
    
    logging.info("Targeting %s check at %s", time_window, target_time)
    
    # Calculate base countdown
    time_difference = target_time - now
    base_countdown = max(60, int(time_difference.total_seconds()))
    
    # Add jitter to spread the load
    if with_jitter:
        jitter_seconds = random.randint(0, JITTER_WINDOW_MINUTES * 60)
        final_countdown = base_countdown + jitter_seconds
        actual_execution_time = now + timedelta(seconds=final_countdown)
        
        logging.info("Countdown: target_time=%s, base=%ss, jitter=+%ss, total=%ss, actual_execution=%s", 
                    target_time, base_countdown, jitter_seconds, final_countdown, actual_execution_time)
        return final_countdown
    else:
        logging.info("Countdown: target_time=%s, base=%ss (no jitter)", 
                    target_time, base_countdown)
        return base_countdown


if __name__ == "__main__":
    calculate_countdown()
#     uvicorn.run(app, host="0.0.0.0", port=8000)