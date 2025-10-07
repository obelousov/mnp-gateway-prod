from datetime import datetime, timedelta
import logging, os
from dotenv import load_dotenv
import random
from config import logger, settings

# Load environment variables from .env file
# load_dotenv()

# LOG_FILE = os.getenv('LOG_FILE', 'mnp.log')  # Default log file path
# LOG_INFO = os.getenv('LOG_INFO', 'INFO')

# MORNING_WINDOW_START = int(os.getenv('MORNING_WINDOW_START', "8"))
# MORNING_WINDOW_END = int(os.getenv('MORNING_WINDOW_END', "14")  )
# AFTERNOON_WINDOW_START = int(os.getenv('AFTERNOON_WINDOW_START', "14"))
# AFTERNOON_WINDOW_END = int(os.getenv('AFTERNOON_WINDOW_END', "20"))

# # Jitter configuration - spread tasks over this many minutes
# JITTER_WINDOW_MINUTES = int(os.getenv('JITTER_WINDOW_MINUTES', '30'))  # Spread over 30 minutes
# JITTER_WINDOW_SECONDS = int(os.getenv('JITTER_WINDOW_SECONDS', '60'))  # Spread over 1 minute

# # Configure logging to both file and console
# logging.basicConfig(
#     level=LOG_INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(LOG_FILE),  # Container path
#         logging.StreamHandler()  # Also show in docker logs
#     ]
# )

MORNING_WINDOW_START = settings.MORNING_WINDOW_START
MORNING_WINDOW_END = settings.MORNING_WINDOW_END
AFTERNOON_WINDOW_START = settings.AFTERNOON_WINDOW_START
AFTERNOON_WINDOW_END = settings.AFTERNOON_WINDOW_END
JITTER_WINDOW_MINUTES = settings.JITTER_WINDOW_MINUTES
JITTER_WINDOW_SECONDS = settings.JITTER_WINDOW_SECONDS


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

def calculate_countdown_working_hours(delta, with_jitter=True):
    """
    Calculate countdown status and return appropriate delta (with jitter) if within working hours,
    otherwise calculate for next timeband (next business date).
    
    Args:
        delta: timedelta object representing the initial countdown
        with_jitter: Whether to add random jitter
    
    Returns:
        tuple: (adjusted_delta, status, scheduled_datetime)
    """
    now = datetime.now()
    scheduled_datetime = now + delta
    # current_hour = now.hour
    
    # Check if scheduled time falls within working hours and not on holiday
    def is_valid_business_time(target_dt):
        if is_holiday(target_dt):
            return False
        
        target_hour = target_dt.hour
        is_morning_window = MORNING_WINDOW_START <= target_hour < MORNING_WINDOW_END
        is_afternoon_window = AFTERNOON_WINDOW_START <= target_hour < AFTERNOON_WINDOW_END
        
        return is_morning_window or is_afternoon_window
    
    # Check if the originally scheduled datetime is valid
    if is_valid_business_time(scheduled_datetime):
        status = "WITHIN_WORKING_HOURS"
        target_time = scheduled_datetime
        time_window = "current_schedule"
    else:
        status = "NEXT_TIMEBAND"
        with_jitter=True
        
        # Find the next valid business time
        candidate_date = scheduled_datetime.date()
        
        # Keep looking until we find a valid business day
        while True:
            # Try morning window first
            morning_time = datetime.combine(candidate_date, datetime.min.time()).replace(
                hour=MORNING_WINDOW_START, minute=0, second=0
            )
            
            if morning_time > now and not is_holiday(morning_time):
                target_time = morning_time
                time_window = "next_morning_window"
                break
            
            # Try afternoon window if morning is not valid or already passed
            afternoon_time = datetime.combine(candidate_date, datetime.min.time()).replace(
                hour=AFTERNOON_WINDOW_START, minute=0, second=0
            )
            
            if afternoon_time > now and not is_holiday(afternoon_time):
                target_time = afternoon_time
                time_window = "next_afternoon_window"
                break
            
            # Move to next day
            candidate_date = get_next_working_day(datetime.combine(candidate_date, datetime.min.time())).date()
    
    # Calculate base countdown
    time_difference = target_time - now
    base_countdown = max(60, int(time_difference.total_seconds()))
    
    # Add jitter if requested
    if with_jitter:
        jitter_seconds = random.randint(0, JITTER_WINDOW_SECONDS)
        final_countdown = base_countdown + jitter_seconds
        actual_execution_time = now + timedelta(seconds=final_countdown)
        
        logger.info("Countdown Status: %s | Base: %ss, Jitter: +%ss, Total: %ss | Window: %s | Execution: %s", 
                    status, base_countdown, jitter_seconds, final_countdown, time_window, actual_execution_time)
        
        return timedelta(seconds=final_countdown), status, actual_execution_time
    else:
        logger.info("Countdown Status:: %s | Base: %ss (no jitter) | Window: %s | Execution: %s", 
                    status, base_countdown, time_window, target_time)
        
        return timedelta(seconds=base_countdown), status, target_time

def schedule_task_with_countdown(initial_countdown_seconds):
    """
    Example usage of the calculate_countdown_status function.
    """
    initial_delta = timedelta(seconds=initial_countdown_seconds)
    
    adjusted_delta, status, scheduled_time = calculate_countdown_working_hours(
        delta=initial_delta, 
        with_jitter=True
    )
    
    print(f"Status: {status}")
    print(f"Original delta: {initial_delta}")
    print(f"Adjusted delta: {adjusted_delta}")
    print(f"Scheduled execution: {scheduled_time}")
    
    return adjusted_delta, status, scheduled_time

if __name__ == "__main__":
    # calculate_countdown()
    # calculate_countdown_working_hours(timedelta(minutes=1))
    calculate_countdown_working_hours(timedelta(minutes=0),with_jitter=False)
#     uvicorn.run(app, host="0.0.0.0", port=8000)