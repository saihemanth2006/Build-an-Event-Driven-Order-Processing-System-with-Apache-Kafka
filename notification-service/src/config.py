# Notification Service - Configuration

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class DatabaseConfig:
    """Database configuration"""
    host: str = os.getenv("MYSQL_HOST", "mysql")
    port: int = int(os.getenv("MYSQL_PORT", 3306))
    user: str = os.getenv("MYSQL_USER", "orderuser")
    password: str = os.getenv("MYSQL_PASSWORD", "orderpass123")
    database: str = os.getenv("MYSQL_DATABASE", "orderdb")
    
    @property
    def connection_string(self) -> str:
        return f"mysql+asyncmy://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class KafkaConfig:
    """Kafka configuration"""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    order_topic: str = os.getenv("ORDER_TOPIC", "order-events")
    inventory_update_topic: str = os.getenv("INVENTORY_UPDATE_TOPIC", "inventory-events")
    failed_orders_topic: str = os.getenv("FAILED_ORDERS_TOPIC", "failed-orders")
    dlq_topic: str = os.getenv("DLQ_TOPIC", "dlq-events")
    consumer_group: str = os.getenv("NOTIFICATION_CONSUMER_GROUP", "notification-service")
    session_timeout_ms: int = int(os.getenv("KAFKA_SESSION_TIMEOUT", 30000))
    max_poll_records: int = int(os.getenv("KAFKA_MAX_POLL_RECORDS", 10))

@dataclass
class AppConfig:
    """Application configuration"""
    app_name: str = "Notification Service"
    version: str = "1.0.0"
    debug: bool = os.getenv("DEBUG", "False").lower() == "true"
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    consumer_id: str = "notification-service"
    max_retries: int = int(os.getenv("MAX_RETRIES", 3))
    retry_backoff_base: float = float(os.getenv("RETRY_BACKOFF_BASE", 2.0))

# Load configurations
db_config = DatabaseConfig()
kafka_config = KafkaConfig()
app_config = AppConfig()
