-- ============================================
-- DATABASE INITIALIZATION SCRIPT
-- ============================================
-- Creates all necessary tables for the Event-Driven Order Processing System

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS orderdb;
USE orderdb;

-- ============================================
-- TABLE: orders
-- Purpose: Stores order details
-- ============================================
CREATE TABLE IF NOT EXISTS orders (
  id VARCHAR(36) PRIMARY KEY COMMENT 'UUID of the order',
  user_id VARCHAR(36) NOT NULL COMMENT 'UUID of the user placing the order',
  items JSON NOT NULL COMMENT 'Array of items with {sku, quantity, price}',
  status ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED') DEFAULT 'PENDING' COMMENT 'Current status of the order',
  total_amount DECIMAL(10, 2) DEFAULT 0.00 COMMENT 'Total order amount',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'When order was created',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update time',
  INDEX idx_user_id (user_id),
  INDEX idx_status (status),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Stores all order information for the order service';

-- ============================================
-- TABLE: inventory
-- Purpose: Stores product and stock information
-- ============================================
CREATE TABLE IF NOT EXISTS inventory (
  sku VARCHAR(50) PRIMARY KEY COMMENT 'Stock Keeping Unit - unique product identifier',
  name VARCHAR(255) NOT NULL COMMENT 'Product name',
  description TEXT COMMENT 'Product description',
  stock INT NOT NULL DEFAULT 0 COMMENT 'Current stock quantity',
  price DECIMAL(10, 2) NOT NULL COMMENT 'Unit price',
  reserved_stock INT DEFAULT 0 COMMENT 'Stock reserved by pending orders',
  last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update time',
  INDEX idx_stock (stock)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Stores product and inventory information for inventory service';

-- ============================================
-- TABLE: outbox_events (Transactional Outbox Pattern)
-- Purpose: Ensures atomicity between DB changes and event publishing
-- ============================================
CREATE TABLE IF NOT EXISTS outbox_events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique event ID',
  aggregate_type VARCHAR(50) NOT NULL COMMENT 'Type of aggregate (e.g., Order)',
  aggregate_id VARCHAR(36) NOT NULL COMMENT 'ID of the aggregate (e.g., order_id)',
  event_type VARCHAR(100) NOT NULL COMMENT 'Type of event (e.g., OrderCreated)',
  payload JSON NOT NULL COMMENT 'Complete event data as JSON',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'When event was created',
  processed BOOLEAN DEFAULT FALSE COMMENT 'Whether event has been published to Kafka',
  processed_at TIMESTAMP NULL COMMENT 'When event was published to Kafka',
  retry_count INT DEFAULT 0 COMMENT 'Number of retry attempts',
  error_message TEXT COMMENT 'Error message if publishing failed',
  INDEX idx_processed (processed),
  INDEX idx_aggregate_id (aggregate_id),
  INDEX idx_event_type (event_type),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Outbox pattern implementation for transactional event publishing';

-- ============================================
-- TABLE: processed_events (Idempotency)
-- Purpose: Tracks processed events to ensure idempotency
-- ============================================
CREATE TABLE IF NOT EXISTS processed_events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique ID',
  consumer_id VARCHAR(100) NOT NULL COMMENT 'Consumer group/service ID',
  event_id VARCHAR(36) NOT NULL COMMENT 'Unique event ID from Kafka',
  event_type VARCHAR(100) NOT NULL COMMENT 'Type of event processed',
  processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'When the event was processed',
  UNIQUE KEY unique_consumer_event (consumer_id, event_id),
  INDEX idx_event_id (event_id),
  INDEX idx_consumer_id (consumer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Idempotency tracking - prevents duplicate processing of events';

-- ============================================
-- TABLE: dead_letter_events (DLQ)
-- Purpose: Stores messages that failed to process
-- ============================================
CREATE TABLE IF NOT EXISTS dead_letter_events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique ID',
  consumer_id VARCHAR(100) NOT NULL COMMENT 'Consumer that failed to process',
  event_id VARCHAR(36) NOT NULL COMMENT 'Event ID that failed',
  event_type VARCHAR(100) NOT NULL COMMENT 'Type of event',
  payload JSON NOT NULL COMMENT 'Event payload',
  error_reason TEXT NOT NULL COMMENT 'Reason why processing failed',
  retry_count INT DEFAULT 0 COMMENT 'Number of retries attempted',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'When DLQ entry was created',
  resolved BOOLEAN DEFAULT FALSE COMMENT 'Whether this DLQ message was resolved',
  resolved_at TIMESTAMP NULL COMMENT 'When the DLQ message was resolved',
  INDEX idx_consumer_id (consumer_id),
  INDEX idx_resolved (resolved),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Dead Letter Queue for failed message processing';

-- ============================================
-- TABLE: order_events (Event Store)
-- Purpose: Log all events related to orders for auditing
-- ============================================
CREATE TABLE IF NOT EXISTS order_events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique event ID',
  order_id VARCHAR(36) NOT NULL COMMENT 'Order ID',
  event_type VARCHAR(100) NOT NULL COMMENT 'Type of event',
  event_data JSON NOT NULL COMMENT 'Event details',
  service_name VARCHAR(100) NOT NULL COMMENT 'Service that emitted the event',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'When event occurred',
  INDEX idx_order_id (order_id),
  INDEX idx_event_type (event_type),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Audit log of all order-related events for debugging and compliance';

-- ============================================
-- SEED DATA FOR INVENTORY
-- ============================================
INSERT INTO inventory (sku, name, description, stock, price) VALUES
('SKU-001', 'Laptop Pro', 'High-performance laptop with 16GB RAM', 50, 1299.99),
('SKU-002', 'USB-C Cable', 'Durable USB-C charging cable', 500, 19.99),
('SKU-003', 'Wireless Mouse', 'Ergonomic wireless mouse with 2.4GHz connection', 200, 49.99),
('SKU-004', 'Monitor 27inch', '27-inch 4K UHD monitor with USB-C', 75, 399.99),
('SKU-005', 'Keyboard Mechanical', 'RGB mechanical keyboard with Cherry MX switches', 150, 129.99)
ON DUPLICATE KEY UPDATE 
  name = VALUES(name),
  description = VALUES(description),
  stock = VALUES(stock),
  price = VALUES(price);

-- ============================================
-- Create User for application access
-- ============================================
GRANT ALL PRIVILEGES ON orderdb.* TO 'orderuser'@'%' IDENTIFIED BY 'orderpass123';
FLUSH PRIVILEGES;

-- ============================================
-- SUMMARY OF CREATED TABLES
-- ============================================
-- 1. orders: Main orders table
-- 2. inventory: Product and stock data
-- 3. outbox_events: Transactional outbox for event publishing
-- 4. processed_events: Idempotency tracking
-- 5. dead_letter_events: DLQ for failed messages
-- 6. order_events: Event audit log
