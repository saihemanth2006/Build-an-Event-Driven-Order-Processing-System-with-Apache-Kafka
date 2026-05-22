# Notification Service - Main Application

import logging
import asyncio
import signal
import sys
from config import app_config
from kafka_consumer import notification_consumer

# Configure logging
logging.basicConfig(
    level=getattr(logging, app_config.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NotificationService:
    """Main notification service orchestrator"""
    
    def __init__(self):
        self.running = False
    
    async def start(self):
        """Start the notification service"""
        logger.info("=" * 60)
        logger.info(f"Starting {app_config.app_name} v{app_config.version}")
        logger.info("=" * 60)
        
        try:
            await notification_consumer.start()
            self.running = True
            logger.info("✓ Notification Consumer started successfully")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Failed to start service: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the notification service"""
        logger.info("=" * 60)
        logger.info("Shutting down...")
        logger.info("=" * 60)
        
        try:
            await notification_consumer.stop()
            self.running = False
            logger.info("✓ Notification Consumer stopped")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    async def run(self):
        """Run the service"""
        await self.start()
        
        # Keep the service running
        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            await self.stop()

def handle_signal(signum, frame):
    """Handle system signals"""
    logger.info(f"Received signal {signum}")
    sys.exit(0)

async def main():
    """Main entry point"""
    service = NotificationService()
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    
    try:
        await service.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
