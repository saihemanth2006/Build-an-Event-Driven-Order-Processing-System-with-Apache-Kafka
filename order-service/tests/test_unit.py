# Order Service - Unit Tests

import pytest
import asyncio
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

import sys
sys.path.insert(0, '../src')

from database import (
    Base, Order, OrderStatus, OutboxEvent, ProcessedEvent,
    OrderRepository, OutboxRepository, ProcessedEventRepository
)

# Test database setup
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

@pytest.fixture
async def test_db():
    """Create test database"""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
    )
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    AsyncTestingSessionLocal = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    yield AsyncTestingSessionLocal()
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

class TestOrderRepository:
    """Test Order repository operations"""
    
    @pytest.mark.asyncio
    async def test_create_order(self, test_db):
        """Test order creation"""
        user_id = "550e8400-e29b-41d4-a716-446655440000"
        items = [
            {"sku": "SKU-001", "quantity": 2, "price": 99.99}
        ]
        
        order = await OrderRepository.create_order(test_db, user_id, items)
        
        assert order.id is not None
        assert order.user_id == user_id
        assert order.status == OrderStatus.PENDING
        assert order.total_amount == 199.98
    
    @pytest.mark.asyncio
    async def test_get_order(self, test_db):
        """Test order retrieval"""
        user_id = "550e8400-e29b-41d4-a716-446655440001"
        items = [{"sku": "SKU-002", "quantity": 1, "price": 49.99}]
        
        created_order = await OrderRepository.create_order(test_db, user_id, items)
        retrieved_order = await OrderRepository.get_order(test_db, created_order.id)
        
        assert retrieved_order is not None
        assert retrieved_order.id == created_order.id
        assert retrieved_order.user_id == user_id
    
    @pytest.mark.asyncio
    async def test_update_order_status(self, test_db):
        """Test order status update"""
        user_id = "550e8400-e29b-41d4-a716-446655440002"
        items = [{"sku": "SKU-003", "quantity": 1, "price": 29.99}]
        
        order = await OrderRepository.create_order(test_db, user_id, items)
        
        # Update status
        result = await OrderRepository.update_order_status(
            test_db, order.id, OrderStatus.PROCESSING
        )
        
        assert result is True
        
        # Verify update
        updated_order = await OrderRepository.get_order(test_db, order.id)
        assert updated_order.status == OrderStatus.PROCESSING

class TestOutboxRepository:
    """Test Outbox repository operations"""
    
    @pytest.mark.asyncio
    async def test_add_event(self, test_db):
        """Test adding event to outbox"""
        event_data = {
            "order_id": "test-order-123",
            "user_id": "test-user-456",
            "items": []
        }
        
        event = await OutboxRepository.add_event(
            test_db,
            aggregate_type="Order",
            aggregate_id="test-order-123",
            event_type="OrderCreated",
            payload=event_data
        )
        
        assert event.id is not None
        assert event.aggregate_type == "Order"
        assert event.event_type == "OrderCreated"
        assert event.processed is False
    
    @pytest.mark.asyncio
    async def test_get_unprocessed_events(self, test_db):
        """Test retrieving unprocessed events"""
        # Add multiple events
        for i in range(3):
            await OutboxRepository.add_event(
                test_db,
                aggregate_type="Order",
                aggregate_id=f"order-{i}",
                event_type="OrderCreated",
                payload={"order_id": f"order-{i}"}
            )
        
        events = await OutboxRepository.get_unprocessed_events(test_db, batch_size=10)
        
        assert len(events) == 3
        assert all(not event.processed for event in events)

class TestProcessedEventRepository:
    """Test Processed event repository (idempotency)"""
    
    @pytest.mark.asyncio
    async def test_mark_and_check_processed(self, test_db):
        """Test event processing tracking"""
        consumer_id = "test-consumer"
        event_id = "test-event-123"
        
        # Initially not processed
        is_processed = await ProcessedEventRepository.is_processed(
            test_db, consumer_id, event_id
        )
        assert is_processed is False
        
        # Mark as processed
        await ProcessedEventRepository.mark_processed(
            test_db, consumer_id, event_id, "OrderCreated"
        )
        
        # Now should be processed
        is_processed = await ProcessedEventRepository.is_processed(
            test_db, consumer_id, event_id
        )
        assert is_processed is True

class TestOrderValidation:
    """Test order validation logic"""
    
    def test_order_item_validation(self):
        """Test order item validation"""
        from api import OrderItem
        
        # Valid item
        item = OrderItem(sku="SKU-001", quantity=2, price=99.99)
        assert item.sku == "SKU-001"
        assert item.quantity == 2
        
        # Invalid quantity
        with pytest.raises(ValueError):
            OrderItem(sku="SKU-001", quantity=0, price=99.99)
        
        # Invalid price
        with pytest.raises(ValueError):
            OrderItem(sku="SKU-001", quantity=1, price=0)

# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
