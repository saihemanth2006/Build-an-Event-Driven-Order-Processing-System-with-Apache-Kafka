# Notification Service - Notification Logic and Database Operations

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, DateTime, TEXT, Boolean, select
from config import db_config, app_config

logger = logging.getLogger(__name__)

# Create async engine
engine = create_async_engine(
    db_config.connection_string,
    echo=False,
    pool_size=20,
    max_overflow=0,
    pool_pre_ping=True,
    pool_recycle=3600
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

Base = declarative_base()

class ProcessedEvent(Base):
    """Processed events table (idempotency)"""
    __tablename__ = "processed_events"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    consumer_id = Column(String(100), nullable=False, index=True)
    event_id = Column(String(36), nullable=False, index=True)
    event_type = Column(String(100), nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow, nullable=False)

class DeadLetterEvent(Base):
    """Dead Letter Queue table"""
    __tablename__ = "dead_letter_events"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    consumer_id = Column(String(100), nullable=False, index=True)
    event_id = Column(String(36), nullable=False, index=True)
    event_type = Column(String(100), nullable=False)
    payload = Column(String(5000), nullable=False)  # Store as string for simplicity
    error_reason = Column(TEXT, nullable=False)
    retry_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime, nullable=True)

class NotificationLogic:
    """
    Notification processing with idempotency
    
    Sends notifications idempotently:
    - OrderCreated: Initial order confirmation
    - InventoryUpdated: Order processing confirmation
    - OrderFailed: Inventory failure notification
    """
    
    @staticmethod
    async def process_order_created_notification_idempotently(
        session: AsyncSession,
        order_id: str,
        event_id: str,
        user_email: str,
        items: list
    ) -> tuple[bool, Optional[str]]:
        """Process OrderCreated notification idempotently"""
        try:
            # Check if already processed (IDEMPOTENCY)
            if await NotificationLogic.is_event_processed(
                session,
                consumer_id=app_config.consumer_id,
                event_id=event_id
            ):
                logger.warning(
                    f"Notification for event {event_id} already sent. "
                    f"Skipping idempotently."
                )
                return (True, None)
            
            # Send notification (simulated by logging)
            logger.info(
                f"📧 NOTIFICATION: Order Confirmation\n"
                f"   Order ID: {order_id}\n"
                f"   Status: Order received and pending processing\n"
                f"   Items: {len(items)} item(s)\n"
                f"   Customer Email: {user_email}"
            )
            
            # Mark as processed
            await NotificationLogic.mark_event_processed(
                session,
                consumer_id=app_config.consumer_id,
                event_id=event_id,
                event_type="OrderCreated"
            )
            
            await session.commit()
            return (True, None)
            
        except Exception as e:
            logger.error(f"Error processing OrderCreated notification: {e}")
            await session.rollback()
            return (False, str(e))
    
    @staticmethod
    async def process_inventory_updated_notification_idempotently(
        session: AsyncSession,
        order_id: str,
        event_id: str,
        user_email: str,
        items: list
    ) -> tuple[bool, Optional[str]]:
        """Process InventoryUpdated notification idempotently"""
        try:
            # Check if already processed (IDEMPOTENCY)
            if await NotificationLogic.is_event_processed(
                session,
                consumer_id=app_config.consumer_id,
                event_id=event_id
            ):
                logger.warning(
                    f"Notification for event {event_id} already sent. "
                    f"Skipping idempotently."
                )
                return (True, None)
            
            # Send notification (simulated by logging)
            logger.info(
                f"📧 NOTIFICATION: Order Confirmed & Processing\n"
                f"   Order ID: {order_id}\n"
                f"   Status: Inventory reserved and order is being processed\n"
                f"   Items: {len(items)} item(s)\n"
                f"   Customer Email: {user_email}\n"
                f"   Expected Delivery: 3-5 business days"
            )
            
            # Mark as processed
            await NotificationLogic.mark_event_processed(
                session,
                consumer_id=app_config.consumer_id,
                event_id=event_id,
                event_type="InventoryUpdated"
            )
            
            await session.commit()
            return (True, None)
            
        except Exception as e:
            logger.error(f"Error processing InventoryUpdated notification: {e}")
            await session.rollback()
            return (False, str(e))
    
    @staticmethod
    async def process_order_failed_notification_idempotently(
        session: AsyncSession,
        order_id: str,
        event_id: str,
        user_email: str,
        failure_reason: str
    ) -> tuple[bool, Optional[str]]:
        """Process OrderFailed notification idempotently"""
        try:
            # Check if already processed (IDEMPOTENCY)
            if await NotificationLogic.is_event_processed(
                session,
                consumer_id=app_config.consumer_id,
                event_id=event_id
            ):
                logger.warning(
                    f"Notification for event {event_id} already sent. "
                    f"Skipping idempotently."
                )
                return (True, None)
            
            # Send notification (simulated by logging)
            logger.warning(
                f"📧 NOTIFICATION: Order Failed\n"
                f"   Order ID: {order_id}\n"
                f"   Status: Order processing failed\n"
                f"   Reason: {failure_reason}\n"
                f"   Customer Email: {user_email}\n"
                f"   Action: Please contact support or place a new order"
            )
            
            # Mark as processed
            await NotificationLogic.mark_event_processed(
                session,
                consumer_id=app_config.consumer_id,
                event_id=event_id,
                event_type="OrderFailed"
            )
            
            await session.commit()
            return (True, None)
            
        except Exception as e:
            logger.error(f"Error processing OrderFailed notification: {e}")
            await session.rollback()
            return (False, str(e))
    
    @staticmethod
    async def is_event_processed(
        session: AsyncSession,
        consumer_id: str,
        event_id: str
    ) -> bool:
        """Check if notification has been sent (IDEMPOTENCY)"""
        try:
            result = await session.execute(
                select(ProcessedEvent).where(
                    (ProcessedEvent.consumer_id == consumer_id) &
                    (ProcessedEvent.event_id == event_id)
                )
            )
            return result.scalar_one_or_none() is not None
        except Exception as e:
            logger.error(f"Error checking if event processed: {e}")
            return False
    
    @staticmethod
    async def mark_event_processed(
        session: AsyncSession,
        consumer_id: str,
        event_id: str,
        event_type: str
    ) -> ProcessedEvent:
        """Mark an event as processed"""
        try:
            processed = ProcessedEvent(
                consumer_id=consumer_id,
                event_id=event_id,
                event_type=event_type
            )
            session.add(processed)
            await session.flush()
            return processed
        except Exception as e:
            logger.error(f"Error marking event as processed: {e}")
            raise
    
    @staticmethod
    async def add_to_dlq(
        session: AsyncSession,
        consumer_id: str,
        event_id: str,
        event_type: str,
        payload: str,
        error_reason: str,
        retry_count: int = 0
    ) -> DeadLetterEvent:
        """Add notification to Dead Letter Queue"""
        try:
            dlq_event = DeadLetterEvent(
                consumer_id=consumer_id,
                event_id=event_id,
                event_type=event_type,
                payload=payload,
                error_reason=error_reason,
                retry_count=retry_count
            )
            session.add(dlq_event)
            await session.commit()
            logger.error(
                f"Notification {event_id} added to DLQ. Reason: {error_reason}"
            )
            return dlq_event
        except Exception as e:
            logger.error(f"Error adding notification to DLQ: {e}")
            raise
