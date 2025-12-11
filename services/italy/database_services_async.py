# services/italy/database_services_async.py (ULTRA-MINIMAL)
from datetime import datetime, date
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from models.models import ItalyAllPortRequests
from config import settings
from services.logger_simple import logger
from typing import Dict, Optional

# Database engine singleton
_engine = None
_AsyncSessionLocal = None

async def get_async_db() -> AsyncSession:
    """FastAPI dependency - simplified"""
    global _engine, _AsyncSessionLocal
    
    if _engine is None:
        driver = settings.DB_DRIVER.replace('mysql+pymysql', 'mysql+aiomysql')
        DATABASE_URL = f"{driver}://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        
        _engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
        _AsyncSessionLocal = async_sessionmaker(_engine, expire_on_commit=False)
    
    if _AsyncSessionLocal is None:
        raise RuntimeError("AsyncSessionLocal is not initialized. Check database configuration.")
    session = _AsyncSessionLocal()
    try:
        yield session
    finally:
        await session.close()

async def save_portin_request_minimal(data: Dict, db: AsyncSession) -> Optional[ItalyAllPortRequests]:
    """Only essential save function"""
    try:
        # Handle date
        cut_over_date = data.get('cut_over_date')
        if isinstance(cut_over_date, str):
            try:
                cut_over_date = datetime.strptime(cut_over_date, '%Y-%m-%d').date()
            except ValueError:
                cut_over_date = None
        
        # Create record
        record = ItalyAllPortRequests(
            recipient_request_code=data['recipient_request_code'],
            msisdn=data['msisdn'],
            message_type_code=data['message_type_code'],
            process_status=data.get('process_status', 'RECEIVED'),
            cut_over_date=cut_over_date,
            xml=data['xml'],
        )
        
        db.add(record)
        await db.commit()
        await db.refresh(record)
        return record
        
    except Exception as e:
        logger.error(f"Save failed: {e}")
        await db.rollback()
        return None