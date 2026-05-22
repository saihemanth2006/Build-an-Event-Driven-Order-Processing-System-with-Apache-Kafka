# Order Service - Outbox Publisher (Background Task)

import logging
import asyncio
from typing import Optional
from datetime import datetime
import time
from config import app_config, kafka_config
from database import AsyncSessionLocal, OutboxRepository, OutboxEvent
from kafka_producer import kafka_producer

logger = logging.getLogger(__name__)

class OutboxPublisher:
    """
    Implements the Transactional Outbox pattern.
    
    This component:
    1. Polls the outbox_events table periodically
    2. Fetches unprocessed events
    3. Publishes them to Kafka
    4. Marks them as processed
    
    This ensures atomicity between database updates and event publishing.
    If the service crashes after writing to outbox but before publishing to Kafka,
    the event will be republished on restart.
    """
    
    def __init__(self):
        self.is_running = False
        self.poll_interval = app_config.outbox_poll_interval
        self.batch_size = app_config.outbox_batch_size
    
    async def start(self):
        """Start the outbox publisher background task"""
        self.is_running = True
        logger.info("Starting Outbox Publisher...")
        
        # Create background task
        asyncio.create_task(self._poll_and_publish())
    
    async def stop(self):
        """Stop the outbox publisher"""
        self.is_running = False
        logger.info("Stopping Outbox Publisher...")
    
    async def _poll_and_publish(self):
        """
        Main polling loop - continuously fetches and publishes events
        """
        await asyncio.sleep(5)  # Wait for Kafka producer to initialize
        
        while self.is_running:
            try:
                async with AsyncSessionLocal() as session:
                    # Fetch unprocessed events from outbox
                    events = await OutboxRepository.get_unprocessed_events(
                        session,
                        batch_size=self.batch_size
                    )
                    
                    if events:
                        logger.info(f"Found {len(events)} unprocessed events in outbox")
                        
                        for event in events:
                            await self._publish_event(session, event)
                        
                        await session.commit()
                
            except Exception as e:
                logger.error(f"Error in outbox polling: {e}")
                await asyncio.sleep(self.poll_interval)
                continue
            
            # Wait before next poll
            await asyncio.sleep(self.poll_interval)
    
    async def _publish_event(self, session, event: OutboxEvent):
        """
        Publish a single event to Kafka with retry logic
        """
        retry_count = 0
        max_retries = app_config.max_retries
        backoff_base = app_config.retry_backoff_base
        
        while retry_count < max_retries:
            try:
                # Determine topic based on event type
                topic = self._get_topic_for_event(event.event_type)
                
                # Publish to Kafka
                await kafka_producer.send_event(
                    topic=topic,
                    event_type=event.event_type,
                    event_data=event.payload,
                    event_id=event.aggregate_id
                )
                
                # Mark as processed
                await OutboxRepository.mark_event_processed(
                    session,
                    event.id,
                    datetime.utcnow()
                )
                
                logger.info(
                    f"Event {event.event_type} (ID: {event.id}, Aggregate: {event.aggregate_id}) "
                    f"published successfully"
                )
                return
                
            except Exception as e:
                retry_count += 1
                error_msg = str(e)
                
                logger.warning(
                    f"Failed to publish event {event.id} (attempt {retry_count}/{max_retries}): {error_msg}"
                )
                
                # Update retry count and error message
                await OutboxRepository.update_event_retry(
                    session,
                    event.id,
                    retry_count,
                    error_msg
                )
                
                if retry_count < max_retries:
                    # Exponential backoff: wait 2^retry_count seconds
                    wait_time = backoff_base ** retry_count
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"Event {event.id} failed after {max_retries} retries. "
                        f"Manual intervention may be required."
                    )
    
    def _get_topic_for_event(self, event_type: str) -> str:
        """Map event type to Kafka topic"""
        topic_mapping = {
            "OrderCreated": kafka_config.order_topic,
            "OrderFailed": kafka_config.failed_orders_topic,
            "OrderCompleted": kafka_config.order_topic,
        }
        return topic_mapping.get(event_type, kafka_config.order_topic)

# Global outbox publisher instance
outbox_publisher = OutboxPublisher()

async def get_outbox_publisher() -> OutboxPublisher:
    """Get outbox publisher instance"""
    return outbox_publisher
