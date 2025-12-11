# test_quick.py
#!/usr/bin/env python3
"""
Quick test for Italy time service.
"""
import os
import sys
from datetime import datetime, time

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.italy.time_services import time_service, ItalySchedulingSettings
from config import settings

def quick_test():
    """Quick test of the main functionality."""
    print("Quick Test of Italy Time Service")
    print("="*40)
    
    # 1. Test Madrid time functions
    print("\n1. Madrid Time Functions:")
    print(f"   Current Madrid time: {settings.get_madrid_time_readable()}")
    print(f"   ISO format: {settings.get_madrid_time_iso()}")
    
    # 2. Test Italy scheduling (with test data)
    print("\n2. Italy Scheduling Test:")
    
    # Add a test schedule
    from services.italy.time_services import MessageSchedule, Direction
    test_schedule = MessageSchedule(
        message_type="1",
        direction=Direction.IN,
        days="Mon,Tue,Wed,Thu,Fri",
        start_time="09:00",
        stop_time="17:00"
    )
    
    # Add to time service
    time_service.italy_scheduling.schedules["MSG1"] = test_schedule
    
    # Test current time
    can_send_now = settings.is_italy_message_allowed("1")
    print(f"   Can send MSG1 now: {can_send_now}")
    
    # 3. Test specific scenarios
    print("\n3. Test Specific Scenarios:")
    
    # Test Monday 10:00 (should be allowed)
    from datetime import datetime
    import pytz
    
    italy_tz = pytz.timezone('Europe/Rome')
    monday_10am = italy_tz.localize(datetime(2024, 1, 1, 10, 0, 0))  # Monday
    monday_result = time_service.is_italy_message_allowed("1", monday_10am)
    print(f"   Monday 10:00: {'Allowed' if monday_result else 'Not allowed'} (expected: Allowed)")
    
    # Test Sunday 10:00 (should NOT be allowed)
    sunday_10am = italy_tz.localize(datetime(2024, 1, 7, 10, 0, 0))  # Sunday
    sunday_result = time_service.is_italy_message_allowed("1", sunday_10am)
    print(f"   Sunday 10:00: {'Allowed' if sunday_result else 'Not allowed'} (expected: Not allowed)")
    
    # 4. Test message not configured
    print("\n4. Test Message Not Configured:")
    not_configured_result = time_service.is_italy_message_allowed("99")  # MSG99 not configured
    print(f"   MSG99 (not configured): {'Allowed' if not_configured_result else 'Not allowed'} (expected: Allowed)")
    
    print("\n" + "="*40)
    print("Quick test completed!")

if __name__ == "__main__":
    quick_test()