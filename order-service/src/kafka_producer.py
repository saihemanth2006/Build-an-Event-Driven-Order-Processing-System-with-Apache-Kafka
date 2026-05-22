# Order Service - Kafka Producer

import logging
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime
import json
from aiokafka import AIOKafkaProducer
from config import kafka_config, app_config

logger = logging.getLogger(__name__)

class KafkaProducer:
    """Kafka producer for publishing order events"""
    
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_connected = False
    
    async def start(self):
        """Start Kafka producer"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=kafka_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                max_batch_size=kafka_config.batch_size,
                linger_ms=kafka_config.linger_ms,
                acks='all',  # Wait for all replicas to acknowledge
                retries=app_config.max_retries,
                retry_backoff_ms=100
            )
            await self.producer.start()
            self.is_connected = True
            logger.info(f"Kafka Producer connected to {kafka_config.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to start Kafka Producer: {e}")
            raise
    
    async def stop(self):
        """Stop Kafka producer"""
        if self.producer:
            await self.producer.stop()
            self.is_connected = False
            logger.info("Kafka Producer stopped")
    
    async def send_event(
        self,
        topic: str,
        event_type: str,
        event_data: Dict[str, Any],
        event_id: str
    ) -> str:
        """
        Send event to Kafka with retry logic
        
        Args:
            topic: Kafka topic name
            event_type: Type of event (e.g., OrderCreated)
            event_data: Event payload
            event_id: Unique event ID for tracking
            
        Returns:
            Partition and offset information
        """
        if not self.is_connected:
            raise RuntimeError("Kafka Producer is not connected")
        
        # Create event wrapper with metadata
        event_message = {
            "event_id": event_id,
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0",
            "data": event_data
        }
        
        try:
            # Send to Kafka
            future = await self.producer.send_and_wait(
                topic=topic,
                value=event_message,
                key=str(event_data.get('order_id', event_id)).encode('utf-8')
            )
            
            logger.info(
                f"Event {event_type} (ID: {event_id}) sent to topic {topic}. "
                f"Partition: {future.partition}, Offset: {future.offset}"
            )
            
            return f"partition_{future.partition}_offset_{future.offset}"
            
        except Exception as e:
            logger.error(f"Failed to send event {event_id} to topic {topic}: {e}")
            raise

# Global producer instance
kafka_producer = KafkaProducer()

async def get_kafka_producer() -> KafkaProducer:
    """Get Kafka producer instance"""
    return kafka_producer
