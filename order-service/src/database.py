# Order Service - Database Connection and Models

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, JSON, Enum, DateTime, DECIMAL, TEXT, Boolean, Integer, TIMESTAMP, Index, func
from sqlalchemy.sql import select, update
import enum

from config import db_config

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

# Create session maker
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

Base = declarative_base()

class OrderStatus(str, enum.Enum):
    """Order status enumeration"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class Order(Base):
    """Order model - stores order information"""
    __tablename__ = "orders"
    
    id = Column(String(36), primary_key=True)
    user_id = Column(String(36), nullable=False, index=True)
    items = Column(JSON, nullable=False)  # Array of {sku, quantity, price}
    status = Column(Enum(OrderStatus), default=OrderStatus.PENDING, nullable=False, index=True)
    total_amount = Column(DECIMAL(10, 2), default=0.00)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

class OutboxEvent(Base):
    """Transactional Outbox pattern - ensures atomicity of DB and event publishing"""
    __tablename__ = "outbox_events"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    aggregate_type = Column(String(50), nullable=False)  # e.g., "Order"
    aggregate_id = Column(String(36), nullable=False, index=True)  # e.g., order_id
    event_type = Column(String(100), nullable=False, index=True)  # e.g., "OrderCreated"
    payload = Column(JSON, nullable=False)  # Complete event data
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    processed = Column(Boolean, default=False, nullable=False, index=True)
    processed_at = Column(DateTime, nullable=True)
    retry_count = Column(Integer, default=0)
    error_message = Column(TEXT, nullable=True)

class ProcessedEvent(Base):
    """Idempotency tracking - prevents duplicate event processing"""
    __tablename__ = "processed_events"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    consumer_id = Column(String(100), nullable=False, index=True)
    event_id = Column(String(36), nullable=False, index=True)
    event_type = Column(String(100), nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow, nullable=False)

class OrderEvent(Base):
    """Event audit log for order events"""
    __tablename__ = "order_events"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(36), nullable=False, index=True)
    event_type = Column(String(100), nullable=False, index=True)
    event_data = Column(JSON, nullable=False)
    service_name = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

# Database helper functions
async def get_session() -> AsyncSession:
    """Get a new database session"""
    async with AsyncSessionLocal() as session:
        yield session

async def create_tables():
    """Create all tables"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created successfully")

async def drop_tables():
    """Drop all tables (for testing)"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    logger.info("Database tables dropped")

class OrderRepository:
    """Repository for Order operations"""
    
    @staticmethod
    async def create_order(
        session: AsyncSession,
        user_id: str,
        items: List[Dict[str, Any]]
    ) -> Order:
        """Create a new order"""
        order_id = str(uuid.uuid4())
        
        # Calculate total amount
        total = sum(item.get('price', 0) * item.get('quantity', 0) for item in items)
        
        order = Order(
            id=order_id,
            user_id=user_id,
            items=items,
            status=OrderStatus.PENDING,
            total_amount=total
        )
        
        session.add(order)
        await session.flush()  # Flush to get the order in context
        
        logger.info(f"Order created: {order_id} for user: {user_id}")
        return order
    
    @staticmethod
    async def get_order(session: AsyncSession, order_id: str) -> Optional[Order]:
        """Get order by ID"""
        result = await session.execute(
            select(Order).where(Order.id == order_id)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def update_order_status(
        session: AsyncSession,
        order_id: str,
        status: OrderStatus
    ) -> bool:
        """Update order status"""
        result = await session.execute(
            update(Order)
            .where(Order.id == order_id)
            .values(status=status, updated_at=datetime.utcnow())
        )
        await session.flush()
        logger.info(f"Order {order_id} status updated to {status}")
        return result.rowcount > 0
    
    @staticmethod
    async def list_orders(
        session: AsyncSession,
        user_id: str,
        limit: int = 50,
        offset: int = 0
    ) -> List[Order]:
        """List orders for a user"""
        result = await session.execute(
            select(Order)
            .where(Order.user_id == user_id)
            .order_by(Order.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return result.scalars().all()

class OutboxRepository:
    """Repository for Outbox operations"""
    
    @staticmethod
    async def add_event(
        session: AsyncSession,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: Dict[str, Any]
    ) -> OutboxEvent:
        """Add event to outbox (Transactional Outbox pattern)"""
        event = OutboxEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            processed=False
        )
        session.add(event)
        await session.flush()
        logger.info(f"Event added to outbox: {event_type} for {aggregate_id}")
        return event
    
    @staticmethod
    async def get_unprocessed_events(
        session: AsyncSession,
        batch_size: int = 100
    ) -> List[OutboxEvent]:
        """Get unprocessed events from outbox"""
        result = await session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.processed == False)
            .order_by(OutboxEvent.created_at.asc())
            .limit(batch_size)
        )
        return result.scalars().all()
    
    @staticmethod
    async def mark_event_processed(
        session: AsyncSession,
        event_id: int,
        processed_at: datetime = None
    ) -> bool:
        """Mark event as processed"""
        result = await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                processed=True,
                processed_at=processed_at or datetime.utcnow()
            )
        )
        await session.flush()
        return result.rowcount > 0
    
    @staticmethod
    async def update_event_retry(
        session: AsyncSession,
        event_id: int,
        retry_count: int,
        error_message: str = None
    ) -> bool:
        """Update event retry information"""
        result = await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                retry_count=retry_count,
                error_message=error_message
            )
        )
        await session.flush()
        return result.rowcount > 0

class ProcessedEventRepository:
    """Repository for Processed Events (Idempotency)"""
    
    @staticmethod
    async def mark_processed(
        session: AsyncSession,
        consumer_id: str,
        event_id: str,
        event_type: str
    ) -> ProcessedEvent:
        """Mark an event as processed"""
        processed = ProcessedEvent(
            consumer_id=consumer_id,
            event_id=event_id,
            event_type=event_type
        )
        session.add(processed)
        await session.flush()
        return processed
    
    @staticmethod
    async def is_processed(
        session: AsyncSession,
        consumer_id: str,
        event_id: str
    ) -> bool:
        """Check if event has been processed"""
        result = await session.execute(
            select(ProcessedEvent)
            .where(
                (ProcessedEvent.consumer_id == consumer_id) &
                (ProcessedEvent.event_id == event_id)
            )
        )
        return result.scalar_one_or_none() is not None
