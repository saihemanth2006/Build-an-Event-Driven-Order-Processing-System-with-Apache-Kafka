# Inventory Service - Inventory Logic and Database Operations

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, JSON, DateTime, Integer, DECIMAL, TEXT, Boolean, select, update
from config import db_config, app_config

logger = logging.getLogger(__name__)

# Create async engine for inventory DB (same as order DB)
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

class Inventory(Base):
    """Inventory model"""
    __tablename__ = "inventory"
    
    sku = Column(String(50), primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(TEXT, nullable=True)
    stock = Column(Integer, nullable=False, default=0)
    price = Column(DECIMAL(10, 2), nullable=False)
    reserved_stock = Column(Integer, default=0)
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ProcessedEvent(Base):
    """Processed events table (same as in order DB)"""
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
    payload = Column(JSON, nullable=False)
    error_reason = Column(TEXT, nullable=False)
    retry_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime, nullable=True)

class InventoryLogic:
    """
    Idempotent inventory operations
    
    Ensures that processing the same event multiple times does not cause
    incorrect inventory changes (e.g., stock deducted twice for same order)
    """
    
    @staticmethod
    async def process_order_created_event_idempotently(
        session: AsyncSession,
        order_id: str,
        event_id: str,
        items: List[Dict[str, Any]]
    ) -> tuple[bool, Optional[str]]:
        """
        Process OrderCreated event with idempotency
        
        Returns:
            (success: bool, error_message: Optional[str])
        """
        try:
            # Step 1: Check if event has already been processed (IDEMPOTENCY)
            already_processed = await InventoryLogic.is_event_processed(
                session,
                consumer_id=app_config.consumer_id,
                event_id=event_id
            )
            
            if already_processed:
                logger.warning(
                    f"Event {event_id} for order {order_id} already processed. "
                    f"Skipping idempotently."
                )
                return (True, None)  # Success - already processed
            
            # Step 2: Check inventory availability
            inventory_check = await InventoryLogic.check_inventory_availability(
                session,
                items=items
            )
            
            if not inventory_check["available"]:
                error_msg = f"Insufficient stock for items: {json.dumps(inventory_check['unavailable_items'])}"
                logger.error(f"[Order {order_id}] {error_msg}")
                return (False, error_msg)
            
            # Step 3: Deduct inventory atomically
            deducted = await InventoryLogic.deduct_inventory(
                session,
                items=items,
                order_id=order_id
            )
            
            if not deducted:
                error_msg = "Failed to deduct inventory"
                logger.error(f"[Order {order_id}] {error_msg}")
                return (False, error_msg)
            
            # Step 4: Mark event as processed (IDEMPOTENCY)
            await InventoryLogic.mark_event_processed(
                session,
                consumer_id=app_config.consumer_id,
                event_id=event_id,
                event_type="OrderCreated"
            )
            
            await session.commit()
            
            logger.info(
                f"[Order {order_id}] Inventory deducted successfully. "
                f"Items: {json.dumps([{k: v for k, v in item.items() if k != 'price'} for item in items])}"
            )
            return (True, None)
            
        except Exception as e:
            logger.error(f"Error processing order {order_id}: {e}")
            await session.rollback()
            return (False, str(e))
    
    @staticmethod
    async def check_inventory_availability(
        session: AsyncSession,
        items: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Check if all items are available in sufficient quantity
        
        Returns:
            {"available": bool, "unavailable_items": [...]}
        """
        unavailable_items = []
        
        for item in items:
            sku = item["sku"]
            quantity = item["quantity"]
            
            result = await session.execute(
                select(Inventory).where(Inventory.sku == sku)
            )
            inventory_item = result.scalar_one_or_none()
            
            if not inventory_item:
                unavailable_items.append({
                    "sku": sku,
                    "reason": "SKU not found"
                })
            elif inventory_item.stock < quantity:
                unavailable_items.append({
                    "sku": sku,
                    "requested": quantity,
                    "available": inventory_item.stock,
                    "reason": "Insufficient stock"
                })
        
        return {
            "available": len(unavailable_items) == 0,
            "unavailable_items": unavailable_items
        }
    
    @staticmethod
    async def deduct_inventory(
        session: AsyncSession,
        items: List[Dict[str, Any]],
        order_id: str
    ) -> bool:
        """
        Deduct inventory for all items
        
        This is done atomically - if any item fails, all changes are rolled back
        """
        try:
            for item in items:
                sku = item["sku"]
                quantity = item["quantity"]
                
                # Update inventory
                result = await session.execute(
                    update(Inventory)
                    .where(Inventory.sku == sku)
                    .values(
                        stock=Inventory.stock - quantity,
                        reserved_stock=Inventory.reserved_stock + quantity,
                        last_updated_at=datetime.utcnow()
                    )
                )
                
                if result.rowcount == 0:
                    logger.error(f"Failed to deduct inventory for SKU {sku}")
                    return False
                
                logger.debug(f"Deducted {quantity} units of {sku} for order {order_id}")
            
            await session.flush()
            return True
            
        except Exception as e:
            logger.error(f"Error deducting inventory: {e}")
            return False
    
    @staticmethod
    async def is_event_processed(
        session: AsyncSession,
        consumer_id: str,
        event_id: str
    ) -> bool:
        """Check if an event has already been processed (IDEMPOTENCY)"""
        result = await session.execute(
            select(ProcessedEvent).where(
                (ProcessedEvent.consumer_id == consumer_id) &
                (ProcessedEvent.event_id == event_id)
            )
        )
        return result.scalar_one_or_none() is not None
    
    @staticmethod
    async def mark_event_processed(
        session: AsyncSession,
        consumer_id: str,
        event_id: str,
        event_type: str
    ) -> ProcessedEvent:
        """Mark an event as processed"""
        processed_event = ProcessedEvent(
            consumer_id=consumer_id,
            event_id=event_id,
            event_type=event_type
        )
        session.add(processed_event)
        await session.flush()
        logger.debug(f"Event {event_id} marked as processed")
        return processed_event
    
    @staticmethod
    async def add_to_dlq(
        session: AsyncSession,
        consumer_id: str,
        event_id: str,
        event_type: str,
        payload: Dict[str, Any],
        error_reason: str,
        retry_count: int = 0
    ) -> DeadLetterEvent:
        """Add an event to the Dead Letter Queue"""
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
            f"Event {event_id} added to DLQ. Reason: {error_reason}"
        )
        return dlq_event
    
    @staticmethod
    async def get_inventory_by_sku(
        session: AsyncSession,
        sku: str
    ) -> Optional[Inventory]:
        """Get inventory item by SKU"""
        result = await session.execute(
            select(Inventory).where(Inventory.sku == sku)
        )
        return result.scalar_one_or_none()
