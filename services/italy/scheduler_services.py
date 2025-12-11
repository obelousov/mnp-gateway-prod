# services/italy/scheduler_services.py
# services/italy/scheduler_services.py
from datetime import datetime, timedelta, time as dt_time, timezone
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
from sqlalchemy.ext.asyncio import AsyncSession
from models.models import ItalyPortInScheduledAction
from services.logger_simple import logger
from config import settings

MADRID_TZ = ZoneInfo(settings.TIME_ZONE)
UTC = timezone.utc

class ItalyScheduler:
    """Smart scheduler that respects Italy MNP time windows."""
    
    def __init__(self):
        self.italy_schedules = settings.italy_scheduling
    
    async def schedule_for_message_type(
        self,
        portin_request_id: int,
        message_type: str,
        action_type: str,
        db: AsyncSession,
        base_time: Optional[datetime] = None,
        depends_on_message_type: Optional[str] = None,
        depends_on_status: Optional[str] = None,
    ) -> bool:
        """
        Schedule an action at the next allowed time for this message type.
        """
        if base_time is None:
            base_time = datetime.now(MADRID_TZ)
        
        # Find next allowed time for this message type
        next_allowed = self._calculate_next_allowed_time(message_type, base_time)
        
        return await _create_scheduled_action(
            portin_request_id=portin_request_id,
            action_type=action_type,
            scheduled_at=next_allowed.astimezone(UTC),
            depends_on_message_type=depends_on_message_type,
            depends_on_status=depends_on_status,
            db=db
        )
    
    def _calculate_next_allowed_time(
        self, 
        message_type: str, 
        base_time: datetime
    ) -> datetime:
        """
        Calculate the next allowed time to send this message type.
        Returns datetime in Madrid timezone.
        """
        schedule = self.italy_schedules.get_schedule(message_type)
        
        # If no schedule restrictions, send immediately
        if not schedule:
            return base_time
        
        # Get day name (Mon, Tue, etc.)
        day_name = base_time.strftime("%a")  # Returns "Mon", "Tue", etc.
        current_time = base_time.time()
        
        # Check if current time is within allowed window
        if self._is_within_window(schedule, day_name, current_time):
            return base_time
        
        # Find next allowed window
        return self._find_next_window(schedule, base_time)
    
    def _is_within_window(
        self, 
        schedule: MessageSchedule, 
        day_name: str, 
        check_time: dt_time
    ) -> bool:
        """Check if a specific time is within the allowed window."""
        if day_name not in schedule.days:
            return False
        
        if schedule.stop_time > schedule.start_time:
            # Normal window: 10:00-19:00
            return schedule.start_time <= check_time <= schedule.stop_time
        else:
            # Overnight window: 21:00-00:00
            return check_time >= schedule.start_time or check_time <= schedule.stop_time
    
    def _find_next_window(
        self, 
        schedule: MessageSchedule, 
        base_time: datetime
    ) -> datetime:
        """
        Find the next allowed window starting from base_time.
        """
        current_day = base_time.date()
        days_to_check = 7  # Check up to a week ahead
        
        for day_offset in range(days_to_check):
            check_date = current_day + timedelta(days=day_offset)
            day_name = check_date.strftime("%a")
            
            if day_name in schedule.days:
                # Found an allowed day, now find the start time
                if day_offset == 0:
                    # Today, but past window. Use tomorrow's start time.
                    next_start = datetime.combine(
                        check_date + timedelta(days=1),
                        schedule.start_time,
                        tzinfo=MADRID_TZ
                    )
                else:
                    # Future day, use its start time
                    next_start = datetime.combine(
                        check_date,
                        schedule.start_time,
                        tzinfo=MADRID_TZ
                    )
                
                return next_start
        
        # Fallback: return base_time + 1 hour if no window found (shouldn't happen)
        return base_time + timedelta(hours=1)
    
    # Updated scheduling methods using message-specific windows
    async def schedule_x_day_actions(
        self,
        portin_request_id: int,
        db: AsyncSession
    ):
        """Schedule X Day actions respecting Italy windows."""
        now_madrid = datetime.now(MADRID_TZ)
        
        # MSG1: Portability Activation Request (10:00-19:00 Mon-Fri)
        await self.schedule_for_message_type(
            portin_request_id=portin_request_id,
            message_type="1",
            action_type="SEND_MSG1",
            db=db,
            base_time=now_madrid,
            depends_on_status="RECEIVED"
        )
        
        # MSG5: Activation Acknowledge (21:00-00:00 Mon-Fri) - next day
        msg5_time = now_madrid + timedelta(days=1)
        await self.schedule_for_message_type(
            portin_request_id=portin_request_id,
            message_type="5",
            action_type="EXPECT_MSG5",
            db=db,
            base_time=msg5_time,
            depends_on_message_type="1",
            depends_on_status="ACK_RECEIVED"
        )
    
    async def schedule_validation_actions(
        self,
        portin_request_id: int,
        db: AsyncSession
    ):
        """Schedule validation (MSG2: 04:00-10:00 Mon-Fri)."""
        now_madrid = datetime.now(MADRID_TZ)
        
        await self.schedule_for_message_type(
            portin_request_id=portin_request_id,
            message_type="2",
            action_type="EXPECT_MSG2",
            db=db,
            base_time=now_madrid + timedelta(days=1),  # Next day
            depends_on_message_type="5",
            depends_on_status="PROCESSED"
        )
    
    async def schedule_porting_notification(
        self,
        portin_request_id: int,
        db: AsyncSession
    ):
        """Schedule porting notification (MSG3: 10:00-19:00 Mon-Fri)."""
        now_madrid = datetime.now(MADRID_TZ)
        
        await self.schedule_for_message_type(
            portin_request_id=portin_request_id,
            message_type="3",
            action_type="SEND_MSG3",
            db=db,
            base_time=now_madrid + timedelta(days=1),
            depends_on_message_type="2",
            depends_on_status="VALIDATION_PASSED"
        )

# Global scheduler instance
italy_scheduler = ItalyScheduler()