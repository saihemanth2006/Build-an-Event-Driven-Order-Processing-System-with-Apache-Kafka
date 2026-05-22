# Inventory Service - Kafka Consumer

import logging
import asyncio
import json
from typing import Optional, Dict, Any
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from config import kafka_config, app_config
from inventory_logic import AsyncSessionLocal, InventoryLogic
import sys

logger = logging.getLogger(__name__)

class InventoryConsumer:
    """
    Kafka consumer for processing order events
    
    Responsibilities:
    1. Consume OrderCreated events from Kafka
    2. Process events idempotently (no duplicate inventory deductions)
    3. Update order status in order service DB
    4. Publish InventoryUpdated or OrderFailed events
    5. Move failed messages to DLQ after retries
    """
    
    def __init__(self):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_running = False
    
    async def start(self):
        """Start the Kafka consumer"""
        try:
            logger.info("Starting Inventory Consumer...")
            
            # Create consumer
            self.consumer = AIOKafkaConsumer(
                kafka_config.order_topic,
                bootstrap_servers=kafka_config.bootstrap_servers,
                group_id=kafka_config.consumer_group,
                session_timeout_ms=kafka_config.session_timeout_ms,
                max_poll_records=kafka_config.max_poll_records,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            # Create producer for publishing results
            self.producer = AIOKafkaProducer(
                bootstrap_servers=kafka_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=app_config.max_retries
            )
            
            await self.consumer.start()
            await self.producer.start()
            
            self.is_running = True
            logger.info(
                f"Inventory Consumer started. "
                f"Consumer Group: {kafka_config.consumer_group}, "
                f"Topic: {kafka_config.order_topic}"
            )
            
            # Start message processing loop
            asyncio.create_task(self._process_messages())
            
        except Exception as e:
            logger.error(f"Failed to start Inventory Consumer: {e}")
            raise
    
    async def stop(self):
        """Stop the Kafka consumer"""
        self.is_running = False
        
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        
        logger.info("Inventory Consumer stopped")
    
    async def _process_messages(self):
        """
        Main message processing loop
        
        Workflow:
        1. Consume message from Kafka
        2. Check if already processed (idempotency)
        3. Try to deduct inventory
        4. On success: publish InventoryUpdated, update order status
        5. On failure: publish OrderFailed
        6. On permanent failure: move to DLQ
        """
        logger.info("Message processing loop started")
        
        while self.is_running:
            try:
                async for message in self.consumer:
                    try:
                        await self._handle_message(message)
                        
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        # Continue processing next message
                        continue
                        
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(5)
                continue
    
    async def _handle_message(self, message):
        """Handle a single message"""
        try:
            event_data = message.value
            event_id = event_data.get("event_id")
            event_type = event_data.get("event_type")
            order_data = event_data.get("data", {})
            
            order_id = order_data.get("order_id")
            items = order_data.get("items", [])
            
            logger.info(
                f"Processing event: {event_type} (ID: {event_id}, Order: {order_id})"
            )
            
            async with AsyncSessionLocal() as session:
                # Process order with idempotency
                success, error_msg = await InventoryLogic.process_order_created_event_idempotently(
                    session,
                    order_id=order_id,
                    event_id=event_id,
                    items=items
                )
                
                if success:
                    # Inventory deducted successfully
                    logger.info(f"Order {order_id} processed successfully")
                    
                    # Publish InventoryUpdated event
                    await self._publish_inventory_updated_event(
                        order_id=order_id,
                        event_id=event_id,
                        items=items
                    )
                    
                    # Update order status to PROCESSING
                    await self._update_order_status(
                        order_id=order_id,
                        status="PROCESSING"
                    )
                    
                else:
                    # Inventory deduction failed
                    logger.warning(f"Failed to deduct inventory for order {order_id}: {error_msg}")
                    
                    # Publish OrderFailed event
                    await self._publish_order_failed_event(
                        order_id=order_id,
                        event_id=event_id,
                        reason=error_msg
                    )
                    
                    # Update order status to FAILED
                    await self._update_order_status(
                        order_id=order_id,
                        status="FAILED"
                    )
                    
                    # Add to DLQ for manual review
                    async with AsyncSessionLocal() as dlq_session:
                        await InventoryLogic.add_to_dlq(
                            dlq_session,
                            consumer_id=app_config.consumer_id,
                            event_id=event_id,
                            event_type=event_type,
                            payload=event_data,
                            error_reason=error_msg,
                            retry_count=0
                        )
                
                # Commit offset only after successful processing
                await self.consumer.commit()
                logger.info(f"Event {event_id} processing completed. Offset committed.")
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def _publish_inventory_updated_event(
        self,
        order_id: str,
        event_id: str,
        items: list
    ):
        """Publish InventoryUpdated event"""
        try:
            event = {
                "event_id": f"{event_id}_inventory_updated",
                "event_type": "InventoryUpdated",
                "timestamp": json.loads(json.dumps({}), strict=False) if False else None,
                "version": "1.0",
                "data": {
                    "order_id": order_id,
                    "items": items
                }
            }
            
            await self.producer.send_and_wait(
                kafka_config.inventory_update_topic,
                value=event,
                key=order_id.encode('utf-8')
            )
            
            logger.info(f"InventoryUpdated event published for order {order_id}")
            
        except Exception as e:
            logger.error(f"Failed to publish InventoryUpdated event: {e}")
    
    async def _publish_order_failed_event(
        self,
        order_id: str,
        event_id: str,
        reason: str
    ):
        """Publish OrderFailed event"""
        try:
            event = {
                "event_id": f"{event_id}_failed",
                "event_type": "OrderFailed",
                "timestamp": None,
                "version": "1.0",
                "data": {
                    "order_id": order_id,
                    "failure_reason": reason
                }
            }
            
            await self.producer.send_and_wait(
                kafka_config.failed_orders_topic,
                value=event,
                key=order_id.encode('utf-8')
            )
            
            logger.warning(f"OrderFailed event published for order {order_id}")
            
        except Exception as e:
            logger.error(f"Failed to publish OrderFailed event: {e}")
    
    async def _update_order_status(
        self,
        order_id: str,
        status: str
    ):
        """Update order status in order service database"""
        try:
            from sqlalchemy import text
            
            async with AsyncSessionLocal() as session:
                await session.execute(
                    text(
                        "UPDATE orders SET status = :status, updated_at = NOW() WHERE id = :order_id"
                    ),
                    {"status": status, "order_id": order_id}
                )
                await session.commit()
                
                logger.info(f"Order {order_id} status updated to {status}")
                
        except Exception as e:
            logger.error(f"Failed to update order status: {e}")

# Global consumer instance
inventory_consumer = InventoryConsumer()

async def get_consumer() -> InventoryConsumer:
    """Get consumer instance"""
    return inventory_consumer
