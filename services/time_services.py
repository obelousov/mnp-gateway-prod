from datetime import datetime, timedelta
import logging, os
from dotenv import load_dotenv
import random
from config import settings
from services.logger import logger, payload_logger # Use the centralized logger
import pytz

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
    Returns: (countdown_seconds, scheduled_at_datetime)
    """
    now = datetime.now()
    
    # If ignoring working hours, return immediate execution
    if settings.IGNORE_WORKING_HOURS:
        base_countdown = 60  # 1 minute minimum
        with_jitter = False  # No jitter needed for immediate execution
        if with_jitter:
            jitter_seconds = random.randint(0, JITTER_WINDOW_MINUTES * 60)
            final_countdown = base_countdown + jitter_seconds
            actual_execution_time = now + timedelta(seconds=final_countdown)
            
            logging.info("Calc Countdown: IGNORE_WORKING_HOURS | base=%ss, jitter=+%ss, total=%ss, execution=%s", 
                        base_countdown, jitter_seconds, final_countdown, actual_execution_time)
            return final_countdown, actual_execution_time
        else:
            actual_execution_time = now + timedelta(seconds=base_countdown)
            logging.info("Calc Countdown: IGNORE_WORKING_HOURS | base=%ss (no jitter), execution=%s", 
                        base_countdown, actual_execution_time)
            return base_countdown, actual_execution_time
    
    # Original business hour logic (only executed when IGNORE_WORKING_HOURS is False)
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
        
        logging.info("Calc Countdown: target_time=%s, base=%ss, jitter=+%ss, total=%ss, actual_execution=%s", 
                    target_time, base_countdown, jitter_seconds, final_countdown, actual_execution_time)
        return final_countdown, actual_execution_time
    else:
        logging.info("Calc Countdown: target_time=%s, base=%ss (no jitter)", 
                    target_time, base_countdown)
        actual_execution_time = now + timedelta(seconds=base_countdown)
        return base_countdown, actual_execution_time
    
def calculate_countdown_working(with_jitter=True):
    """
    Calculate countdown seconds with holiday awareness.
    Returns: (countdown_seconds, scheduled_at_datetime)
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
        return final_countdown, actual_execution_time
    else:
        logging.info("Countdown: target_time=%s, base=%ss (no jitter)", 
                    target_time, base_countdown)
        actual_execution_time = now + timedelta(seconds=base_countdown)
        return base_countdown, actual_execution_time

def calculate_countdown_1(with_jitter=True):
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

    # Handle both int (seconds) and timedelta inputs
    if isinstance(delta, int):
        delta = timedelta(seconds=delta)
    elif not isinstance(delta, timedelta):
        raise TypeError(f"delta must be timedelta or int, got {type(delta)}")

    # delta = timedelta(seconds=delta)
    scheduled_datetime = now + delta
    print(settings.IGNORE_WORKING_HOURS)
    
    # Check if scheduled time falls within working hours and not on holiday
    def is_valid_business_time(target_dt):
        if is_holiday(target_dt):
            return False
        
        target_hour = target_dt.hour
        is_morning_window = MORNING_WINDOW_START <= target_hour < MORNING_WINDOW_END
        is_afternoon_window = AFTERNOON_WINDOW_START <= target_hour < AFTERNOON_WINDOW_END
        
        return is_morning_window or is_afternoon_window
    
    # Use settings.IGNORE_WORKING_HOURS to bypass business hour validation
    if settings.IGNORE_WORKING_HOURS:
        status = "IGNORE_WORKING_HOURS"
        target_time = scheduled_datetime
        time_window = "ignore_working_hours"
        with_jitter = False  # No jitter needed for immediate execution
    else:
        # Check if the originally scheduled datetime is valid
        if is_valid_business_time(scheduled_datetime):
            status = "WITHIN_WORKING_HOURS"
            target_time = scheduled_datetime
            time_window = "current_schedule"
        else:
            status = "NEXT_TIMEBAND"
            with_jitter = True  # Force jitter for next timeband
            
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
        
        # logger.info("Working Hrs Countdown: %s | Window: %s | Execution: %s", 
        #             status, time_window, actual_execution_time)

        # return timedelta(seconds=final_countdown), status, actual_execution_time
        return timedelta(seconds=final_countdown), status, actual_execution_time.replace(microsecond=0)
    else:
        logger.info("Working Hrs Countdown: %s | Window: %s | Execution: %s", 
                    status, time_window, target_time)
        
        # return timedelta(seconds=base_countdown), status, target_time
        return timedelta(seconds=base_countdown), status, target_time.replace(microsecond=0)
    
def calculate_countdown_working_hours_old(delta, with_jitter=True):
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
        
        # logger.info("Countdown Status: %s | Base: %ss, Jitter: +%ss, Total: %ss | Window: %s | Execution: %s", 
        #             status, base_countdown, jitter_seconds, final_countdown, time_window, actual_execution_time)
        logger.info("Countdown: %s | Window: %s | Execution: %s", 
                    status, time_window, actual_execution_time)

        return timedelta(seconds=final_countdown), status, actual_execution_time
    else:
        # logger.info("Countdown Status:: %s | Base: %ss (no jitter) | Window: %s | Execution: %s", 
        #             status, base_countdown, time_window, target_time)
        logger.info("Countdown: %s | Window: %s | Execution: %s", 
                    status, time_window, target_time)
        
        return timedelta(seconds=base_countdown), status, target_time

from typing import Optional

def is_working_hours_now(check_time: Optional[datetime] = None) -> bool:
    """
    Check if the given time (or current time) is within working hours
    Returns True if within working hours, False otherwise
    """
    if check_time is None:
        check_time = datetime.now()
    
    # Use the same time window constants from your existing function
    current_hour = check_time.hour
    
    # Check if today is a holiday/weekend
    if is_holiday(check_time):
        return False
    
    # Check if within morning or afternoon working windows
    if (MORNING_WINDOW_START <= current_hour < MORNING_WINDOW_END or
        AFTERNOON_WINDOW_START <= current_hour < AFTERNOON_WINDOW_END):
        return True
    
    return False

def normalize_datetime(dt_str):
    """Convert ISO8601 datetime string (e.g. '2025-10-31T17:25:33.038+01:00')
    into MySQL-compatible format 'YYYY-MM-DD HH:MM:SS'.
    Returns None if input is invalid or empty."""
    if not dt_str:
        return None
    try:
        # Parse ISO format (handles timezone and fractional seconds)
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        # Fallback: try removing +01:00 and milliseconds manually
        cleaned = dt_str.split('+')[0].split('.')[0].replace('T', ' ')
        return cleaned

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

def convert_for_mysql_env_tz(xml_datetime: str) -> datetime:
    """Convert to Madrid time and return naive datetime for MySQL"""
    # Parse and convert to Madrid timezone
    dt = datetime.fromisoformat(xml_datetime.replace('Z', '+00:00'))
    madrid_tz = pytz.timezone("Europe/Madrid")
    dt_madrid = dt.astimezone(madrid_tz)
    
    # Remove timezone info for MySQL DATETIME
    return dt_madrid.replace(tzinfo=None)

# Usage:
porting_window_str = "2025-10-23T02:00:00+02:00"
porting_window_db = convert_for_mysql_env_tz(porting_window_str)
# Result: 2025-10-23 02:00:00 (Madrid time, no timezone)


if __name__ == "__main__":
    # print (calculate_countdown())
    # print("end of time_services")

    # countdown_seconds, scheduled_at = calculate_countdown(with_jitter=True)

    # print(f"Task will run in {countdown_seconds} seconds")
    # print(f"Scheduled execution time: {scheduled_at}")

    # calculate_countdown_working_hours(timedelta(minutes=1))
    # calculate_countdown_working_hours(timedelta(minutes=0),with_jitter=True)
    # print("Is working hours now?", is_working_hours_now())
#     uvicorn.run(app, host="0.0.0.0", port=8000)

    a, b, scheduled_at = calculate_countdown_working_hours(
            timedelta(minutes=0), 
            with_jitter=True)
    a_seconds = int(a.total_seconds())
    print("Final scheduled at:a", a, "a_sec", a_seconds, " b: " , b, " c: ", scheduled_at)