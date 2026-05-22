# Notification Service - Kafka Consumer

import logging
import asyncio
import json
from typing import Optional
from aiokafka import AIOKafkaConsumer
from config import kafka_config, app_config
from notification_logic import AsyncSessionLocal, NotificationLogic

logger = logging.getLogger(__name__)

class NotificationConsumer:
    """
    Kafka consumer for notification events
    
    Consumes from multiple topics:
    1. order-events (OrderCreated)
    2. inventory-events (InventoryUpdated)
    3. failed-orders (OrderFailed)
    
    Sends idempotent notifications for each event type
    """
    
    def __init__(self):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.is_running = False
    
    async def start(self):
        """Start the Kafka consumer"""
        try:
            logger.info("Starting Notification Consumer...")
            
            # Create consumer that subscribes to multiple topics
            self.consumer = AIOKafkaConsumer(
                kafka_config.order_topic,
                kafka_config.inventory_update_topic,
                kafka_config.failed_orders_topic,
                bootstrap_servers=kafka_config.bootstrap_servers,
                group_id=kafka_config.consumer_group,
                session_timeout_ms=kafka_config.session_timeout_ms,
                max_poll_records=kafka_config.max_poll_records,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            await self.consumer.start()
            self.is_running = True
            
            logger.info(
                f"Notification Consumer started. "
                f"Consumer Group: {kafka_config.consumer_group}\n"
                f"Topics: {kafka_config.order_topic}, "
                f"{kafka_config.inventory_update_topic}, "
                f"{kafka_config.failed_orders_topic}"
            )
            
            # Start message processing loop
            asyncio.create_task(self._process_messages())
            
        except Exception as e:
            logger.error(f"Failed to start Notification Consumer: {e}")
            raise
    
    async def stop(self):
        """Stop the Kafka consumer"""
        self.is_running = False
        
        if self.consumer:
            await self.consumer.stop()
        
        logger.info("Notification Consumer stopped")
    
    async def _process_messages(self):
        """Main message processing loop"""
        logger.info("Message processing loop started")
        
        while self.is_running:
            try:
                async for message in self.consumer:
                    try:
                        await self._handle_message(message)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        continue
                        
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(5)
                continue
    
    async def _handle_message(self, message):
        """Handle a single notification message"""
        try:
            event_data = message.value
            event_type = event_data.get("event_type")
            event_id = event_data.get("event_id")
            order_data = event_data.get("data", {})
            
            logger.info(
                f"Processing notification event: {event_type} (ID: {event_id})"
            )
            
            async with AsyncSessionLocal() as session:
                success = False
                error_msg = None
                
                # Route to appropriate notification handler
                if event_type == "OrderCreated":
                    order_id = order_data.get("order_id")
                    items = order_data.get("items", [])
                    user_email = f"user+{order_data.get('user_id', 'unknown')[:8]}@example.com"
                    
                    success, error_msg = await NotificationLogic.process_order_created_notification_idempotently(
                        session,
                        order_id=order_id,
                        event_id=event_id,
                        user_email=user_email,
                        items=items
                    )
                    
                elif event_type == "InventoryUpdated":
                    order_id = order_data.get("order_id")
                    items = order_data.get("items", [])
                    user_email = f"user+{order_id[:8]}@example.com"
                    
                    success, error_msg = await NotificationLogic.process_inventory_updated_notification_idempotently(
                        session,
                        order_id=order_id,
                        event_id=event_id,
                        user_email=user_email,
                        items=items
                    )
                    
                elif event_type == "OrderFailed":
                    order_id = order_data.get("order_id")
                    failure_reason = order_data.get("failure_reason", "Unknown")
                    user_email = f"user+{order_id[:8]}@example.com"
                    
                    success, error_msg = await NotificationLogic.process_order_failed_notification_idempotently(
                        session,
                        order_id=order_id,
                        event_id=event_id,
                        user_email=user_email,
                        failure_reason=failure_reason
                    )
                    
                else:
                    logger.warning(f"Unknown event type: {event_type}")
                    success = True  # Skip unknown types
                
                if success:
                    # Commit offset only after successful processing
                    await self.consumer.commit()
                    logger.info(f"Notification for event {event_id} processed successfully. Offset committed.")
                    
                else:
                    # Move to DLQ after max retries would have been exceeded
                    logger.error(f"Failed to process notification: {error_msg}")
                    
                    async with AsyncSessionLocal() as dlq_session:
                        await NotificationLogic.add_to_dlq(
                            dlq_session,
                            consumer_id=app_config.consumer_id,
                            event_id=event_id,
                            event_type=event_type,
                            payload=json.dumps(event_data),
                            error_reason=error_msg,
                            retry_count=0
                        )
                    
                    # Still commit offset to avoid infinite retries
                    await self.consumer.commit()
                    
        except Exception as e:
            logger.error(f"Error handling message: {e}")

# Global consumer instance
notification_consumer = NotificationConsumer()

async def get_consumer() -> NotificationConsumer:
    """Get consumer instance"""
    return notification_consumer
