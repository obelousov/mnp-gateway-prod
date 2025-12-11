from datetime import datetime, time, timedelta
import random
import os
import pytz
from config import settings
# from dotenv import load_dotenv

# ROOT_DIR = Path(__file__).resolve().parents[2]  # go up to project root
# load_dotenv(ROOT_DIR / ".env")

def ita_calculate_countdown_working_hours(
    message_number: int,
    delta,
    with_jitter: bool = True,
    jitter_window_seconds: int = 60,
    ignore_working_hours: bool = False
) -> datetime:
    """Calculate the countdown time considering Italy working hours."""
    italy_tz = pytz.timezone('Europe/Rome')
    now = datetime.now(italy_tz)

    delta = delta if isinstance(delta, timedelta) else timedelta(seconds=delta)

    if ignore_working_hours:
        result = now + delta
        if with_jitter:
            result += timedelta(seconds=random.randint(0, jitter_window_seconds))
        return result.replace(microsecond=0)

    schedule = settings.ITA_MSG_SCHEDULES.get(message_number)
    if not schedule:
        # Fallback if no schedule found
        result = now + delta
        if with_jitter:
            result += timedelta(seconds=random.randint(0, jitter_window_seconds))
        return result.replace(microsecond=0)

    scheduled_days = schedule["days"]
    start_time = time.fromisoformat(schedule["start_time"])
    stop_time = time.fromisoformat(schedule["stop_time"])
    # Proper localization helper
    def localize(date_obj, time_obj):
        return italy_tz.localize(datetime.combine(date_obj, time_obj))

    # Check if dt lies within schedule window
    def is_in_schedule(dt: datetime) -> bool:
        day = dt.strftime('%a')
        if day not in scheduled_days:
            return False

        # Normal window
        if stop_time > start_time:
            return start_time <= dt.time() <= stop_time

        # Overnight window (e.g., 21:00 → 00:00)
        return dt.time() >= start_time or dt.time() <= stop_time

    target = now + delta

    # Case 1: Already inside a valid window
    if is_in_schedule(target):
        result = target
        if with_jitter:
            jittered = target + timedelta(seconds=random.randint(0, jitter_window_seconds))
            if is_in_schedule(jittered):
                result = jittered
        return result.replace(microsecond=0)

    # Case 2: Find next valid window
    target_date = target.date()

    for day_offset in range(7):
        check_date = target_date + timedelta(days=day_offset)
        check_day = check_date.strftime("%a")

        if check_day not in scheduled_days:
            continue

        # Normal window
        if stop_time > start_time:
            window_start = localize(check_date, start_time)

            if day_offset == 0 and target < window_start:
                result = window_start
            else:
                result = window_start

        # Overnight window
        else:
            window_start = localize(check_date, start_time)
            window_end = localize(check_date + timedelta(days=1), stop_time)

            if day_offset == 0 and window_start <= target <= window_end:
                result = target
            else:
                result = window_start

        # Add jitter safely
        if with_jitter:
            jittered = result + timedelta(seconds=random.randint(0, jitter_window_seconds))
            if is_in_schedule(jittered):
                result = jittered

        return result.replace(microsecond=0)

    # Fallback (should never happen)
    result = now + delta
    if with_jitter:
        result += timedelta(seconds=random.randint(0, jitter_window_seconds))
    return result.replace(microsecond=0)


# Test the function
if __name__ == "__main__":
       
    ITA_VENDORS_LIST = settings.ITA_VENDORS_LIST
    print("Vendors List:", ITA_VENDORS_LIST)
    for each in ITA_VENDORS_LIST:
        print(f"{each}")
    exit()
    current_time = datetime.now(pytz.timezone('Europe/Rome'))
    print(f"Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Today is: {current_time.strftime('%A')}")
    print()
    
    test_cases = [
        (1, timedelta(hours=0), "MSG1 (10:00-19:00)"),
        (2, timedelta(hours=0), "MSG2 (04:00-10:00)"),
        (3, timedelta(hours=0), "MSG3 (10:00-19:00)"),
        (5, timedelta(hours=0), "MSG5 (21:00-00:00)"),
        (6, timedelta(hours=0), "MSG6 (04:30-19:00)"),
        (7, timedelta(hours=0), "MSG7 (24/7)"),
        (8, timedelta(hours=0), "MSG8 (24/7)"),
        (9, timedelta(hours=0), "MSG9 (10:00-19:00)"),
        (10, timedelta(hours=0), "MSG10 (10:00-19:00)"),
        (11, timedelta(hours=0), "MSG10 (10:00-19:00)"),
        (12, timedelta(hours=0), "MSG10 (10:00-19:00)"),
    ]


    for msg_num, time_delta, description in test_cases:
        scheduled = ita_calculate_countdown_working_hours(
            msg_num, time_delta, True, 300
        )
        
        # Get schedule info
        days = os.getenv(f"ITA_MSG{msg_num}_DAYS", "").split(',')
        start = os.getenv(f"ITA_MSG{msg_num}_START_TIME", "")
        stop = os.getenv(f"ITA_MSG{msg_num}_STOP_TIME", "")
        
        print(f"{description}:")
        print(f"  Schedule: {days} {start}-{stop}")
        print(f"  Scheduled at: {scheduled.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        # Check if scheduled time is within window
        def check_in_window(dt):
            """
            Docstring for check_in_window
            
            :param dt: Description
            """
            day = dt.strftime('%a')
            if day not in days:
                return False
            t = dt.time()
            start_t = time.fromisoformat(start)
            stop_t = time.fromisoformat(stop)
            if stop_t > start_t:
                return start_t <= t <= stop_t
            else:
                return t >= start_t or t <= stop_t
        
        if check_in_window(scheduled):
            print(f"  ✓ Within schedule")
        else:
            print(f"  ✗ ERROR: Outside schedule!")
        print()