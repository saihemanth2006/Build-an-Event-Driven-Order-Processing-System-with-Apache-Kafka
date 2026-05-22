# Event-Driven Order Processing System with Apache Kafka

A production-ready, scalable event-driven microservices architecture for order processing using Apache Kafka, demonstrating enterprise-grade patterns and best practices.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Running the System](#running-the-system)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Key Concepts](#key-concepts)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

This project implements a complete event-driven order processing system with:

- **Asynchronous Communication**: Services communicate via Apache Kafka events
- **Transactional Outbox Pattern**: Ensures atomicity between database operations and event publishing
- **Idempotent Consumers**: Prevents duplicate processing and data inconsistencies
- **Dead-Letter Queues (DLQ)**: Handles failed messages for manual inspection
- **Distributed Architecture**: Independently scalable microservices
- **Production-Ready**: Comprehensive error handling, logging, and monitoring

### 🎓 Perfect For:

- Learning event-driven architecture
- Interview preparation for distributed systems roles
- Building microservices at scale
- Understanding asynchronous message processing

---

## 🏗️ Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    CLIENTS / API CLIENTS                      │
└─────────────────┬───────────────────────────────────────────┘
                  │ HTTP POST /api/orders
                  │
┌─────────────────▼───────────────────────────────────────────┐
│              ORDER SERVICE (Producer)                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  REST API | Transactional Outbox | Outbox Publisher │   │
│  └──────────────────┬───────────────────────────────────┘   │
│                     │ Publishes OrderCreated events          │
└─────────────────────┼───────────────────────────────────────┘
                      │
                      ▼ Kafka Topic: order-events
        ┌─────────────────────────────────┐
        │      APACHE KAFKA BROKER        │
        │  Zookeeper-managed, Replicated  │
        └─────────────────────────────────┘
          ▲                             ▲
          │                             │
          │ Consumes                    │ Consumes
          │ OrderCreated                │ OrderCreated
          │                             │
┌─────────┴─────────────┐      ┌───────┴─────────────┐
│ INVENTORY SERVICE     │      │ NOTIFICATION SERVICE │
│ (Consumer 1)          │      │ (Consumer 2)         │
│                       │      │                      │
│ • Check stock         │      │ • Send confirmations │
│ • Deduct inventory    │      │ • Send updates       │
│ • Idempotency        │      │ • Log notifications  │
│ • Publish events      │      │ • Handle DLQ         │
└─────────┬─────────────┘      └───────┬──────────────┘
          │ Publishes                   │ Consumes
          │ InventoryUpdated/Failed     │
          ▼                              ▼
        MySQL Database (Shared State)
```

### Data Flow

```
1. User creates order via REST API
2. Order Service saves to MySQL (status=PENDING)
3. OrderCreated event added to outbox table (same transaction)
4. Outbox Publisher polls and publishes to Kafka
5. Inventory Service consumes and reserves stock
6. InventoryUpdated event published
7. Order status updated to PROCESSING
8. Notification Service sends confirmation
9. On failure → OrderFailed event + DLQ
```

---

## 📦 Prerequisites

### Required

- Docker & Docker Compose (v20.10+)
- Python 3.11+ (for local development)
- 4GB RAM minimum, 8GB recommended
- Ports available: 2181, 3306, 8000, 9092

### Software Dependencies

- Apache Kafka 7.5.0
- MySQL 8.0
- Zookeeper 7.5.0
- FastAPI, SQLAlchemy, aiokafka (see requirements.txt)

---

## 🚀 Installation & Setup

### Step 1: Clone Repository

```bash
git clone https://github.com/yourusername/event-driven-order-processing.git
cd event-driven-order-processing
```

### Step 2: Environment Configuration

```bash
# Copy example environment file
cp .env.example .env

# Edit if needed (defaults work for docker-compose)
# nano .env
```

### Step 3: Build Docker Images

```bash
# Build all services
docker-compose build

# Or rebuild without cache
docker-compose build --no-cache
```

### Step 4: Start the System

```bash
# Start all services
docker-compose up -d

# Verify all services are healthy
docker-compose ps
```

### Step 5: Verify Setup

```bash
# Check Order Service health
curl http://localhost:8000/health

# Check Kafka connectivity
docker-compose exec kafka kafka-broker-api-versions.sh --bootstrap-server kafka:9092

# Check MySQL connectivity
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "SHOW TABLES;"
```

---

## 🎬 Running the System

### Create Your First Order

```bash
# Generate UUIDs for testing
USER_ID="550e8400-e29b-41d4-a716-446655440000"

# Create order
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d "{
    \"user_id\": \"$USER_ID\",
    \"items\": [
      {\"sku\": \"SKU-001\", \"quantity\": 1, \"price\": 1299.99},
      {\"sku\": \"SKU-002\", \"quantity\": 3, \"price\": 19.99}
      ]
  }"

# Response should have status 202 Accepted
```

### Monitor the Event Flow

```bash
# Terminal 1: Monitor Order Service logs
docker-compose logs -f order-service

# Terminal 2: Monitor Inventory Service logs
docker-compose logs -f inventory-service

# Terminal 3: Monitor Notification Service logs
docker-compose logs -f notification-service

# Terminal 4: Watch Kafka events
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic order-events \
  --from-beginning
```

### Retrieve Order Status

```bash
# Replace ORDER_ID from the create response
curl http://localhost:8000/api/orders/{ORDER_ID}
```

### List User Orders

```bash
curl "http://localhost:8000/api/orders?user_id=$USER_ID"
```

---

## 🧪 Testing

### Run Unit Tests

```bash
# Order Service tests
docker-compose run --rm order-service pytest tests/ -v

# Inventory Service tests
docker-compose run --rm inventory-service pytest tests/ -v

# Notification Service tests
docker-compose run --rm notification-service pytest tests/ -v
```

### Run Integration Tests

```bash
# Full end-to-end test
bash scripts/integration-test.sh
```

### Manual Testing with Postman/Insomnia

1. Import API_DOCS.md into your REST client
2. Set base URL to http://localhost:8000
3. Create orders and monitor status changes

### Database Queries

```bash
# Check created orders
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "
  SELECT id, user_id, status, total_amount, created_at FROM orders;
"

# Check outbox events (unprocessed)
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "
  SELECT id, aggregate_id, event_type, processed FROM outbox_events;
"

# Check processed events (idempotency)
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "
  SELECT consumer_id, event_id, event_type FROM processed_events;
"

# Check dead letter queue (failed messages)
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "
  SELECT event_id, event_type, error_reason, created_at FROM dead_letter_events;
"

# Check inventory
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "
  SELECT sku, name, stock, reserved_stock FROM inventory;
"
```

---

## 📁 Project Structure

```
event-driven-order-processing/
│
├── docker-compose.yml                 # Orchestrates all services
├── .env                               # Environment variables
├── .env.example                       # Example environment file
│
├── sql/
│   └── init.sql                       # Database schema and seed data
│
├── order-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── src/
│   │   ├── main.py                    # FastAPI application
│   │   ├── config.py                  # Configuration management
│   │   ├── database.py                # ORM models and repos
│   │   ├── api.py                     # REST endpoints
│   │   ├── kafka_producer.py          # Kafka producer
│   │   └── outbox_publisher.py        # Transactional Outbox publisher
│   └── tests/
│       ├── test_unit.py
│       └── test_integration.py
│
├── inventory-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── src/
│   │   ├── main.py                    # Service entry point
│   │   ├── config.py                  # Configuration
│   │   ├── inventory_logic.py         # Core business logic
│   │   ├── kafka_consumer.py          # Event consumer
│   │   └── database.py                # Database operations
│   └── tests/
│       └── test_unit.py
│
├── notification-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── src/
│   │   ├── main.py                    # Service entry point
│   │   ├── config.py                  # Configuration
│   │   ├── notification_logic.py      # Notification processing
│   │   └── kafka_consumer.py          # Multi-topic consumer
│   └── tests/
│       └── test_unit.py
│
├── README.md                          # This file
├── API_DOCS.md                        # Complete API reference
└── COMPREHENSIVE_NOTES.md             # Deep knowledge documentation
```

---

## 🎓 Key Concepts

### 1. Transactional Outbox Pattern

Ensures atomicity between database changes and event publishing:

- Save entity and event to database in single transaction
- Separate process polls and publishes events to Kafka
- If service crashes before publishing, events are republished on restart

### 2. Idempotent Consumers

Processes messages safely even if received multiple times:

- Track processed event IDs in `processed_events` table
- Check before processing to avoid duplicates
- Same order effect regardless of replay count

### 3. Dead-Letter Queues (DLQ)

Handles messages that permanently fail:

- Move unprocessable messages to DLQ table
- Prevents blocking of main processing flow
- Enable manual inspection and recovery

### 4. Event Schema Versioning

Events include version information for compatibility:

- Easy evolution of event formats
- Backward/forward compatibility
- Version handling in consumers

### 5. Distributed Tracing

- Correlation IDs track requests across services
- Structured logging for debugging
- Timestamps on all operations

See COMPREHENSIVE_NOTES.md for deeper explanations!

---

## 🛑 Stopping the System

```bash
# Stop all services (keep volumes)
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v

# Stop specific service
docker-compose stop order-service

# Restart specific service
docker-compose restart inventory-service
```

---

## 🐛 Troubleshooting

### Services won't start

```bash
# Check logs
docker-compose logs

# Rebuild images
docker-compose build --no-cache

# Full reset
docker-compose down -v && docker-compose up -d
```

### Kafka connectivity issues

```bash
# Verify Kafka is healthy
docker-compose exec kafka kafka-broker-api-versions.sh --bootstrap-server kafka:9092

# Check topics exist
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

### MySQL connection errors

```bash
# Verify MySQL is running
docker-compose exec mysql mysql -u orderuser -porderpass123 -e "SELECT 1;"

# Check database exists
docker-compose exec mysql mysql -u orderuser -porderpass123 -e "SHOW DATABASES;"

# Check tables
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "SHOW TABLES;"
```

### Order not processing

1. Check order status: `GET /api/orders/{order_id}`
2. Monitor logs: `docker-compose logs -f`
3. Check inventory: Database query above
4. Check DLQ for failed messages
5. Verify Kafka connectivity

### Performance issues

```bash
# Check resource usage
docker stats

# Increase resource limits in docker-compose.yml
# Monitor Kafka broker health
# Check database indexes
# Review application logs
```

---

## 📊 Monitoring & Observability

### Structured Logging

All services produce JSON logs with:

- Timestamp
- Service name
- Log level
- Correlation ID
- Detailed context

### Key Metrics to Monitor

- Order processing latency
- Kafka consumer lag
- Database query performance
- DLQ message count
- Service health status

### Health Endpoints

```
GET /health                    # Generic health
GET /api/orders/health         # Order service health
```

---

## 🔒 Security Considerations

For production deployment:

1. Use strong database passwords
2. Enable Kafka authentication & encryption
3. Implement API authentication (JWT/OAuth2)
4. Use HTTPS for external APIs
5. Implement rate limiting
6. Add API key management
7. Encrypt sensitive data at rest
8. Enable database SSL connections
9. Regular security audits
10. Container image scanning

---

## 📈 Scaling Considerations

### Horizontal Scaling

- Scale Inventory Service: Add replicas with same consumer group
- Scale Notification Service: Add replicas with same consumer group
- Scale Order Service: Use load balancer (sticky sessions not needed)
- Scale Kafka: Add brokers and increase replication

### Vertical Scaling

- Increase connection pools
- Increase database resource allocation
- Increase container memory/CPU limits

### Optimization

- Add database indexes
- Implement caching layer
- Use message compression
- Batch event processing
- Connection pooling tuning

---

## 📚 Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [FastAPI Guide](https://fastapi.tiangolo.com/)
- [SQLAlchemy ORM Guide](https://docs.sqlalchemy.org/)
- [Event Sourcing Pattern](https://martinfowler.com/eaaDev/EventSourcing.html)
- [Microservices Patterns](https://microservices.io/patterns/index.html)

---

## 📝 License

This project is open source and available under the MIT License.

---

## 👥 Contributing

Contributions are welcome! Please follow:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

---

## 📞 Support

For issues, questions, or suggestions:

1. Check README.md and API_DOCS.md
2. Review logs: `docker-compose logs`
3. Check COMPREHENSIVE_NOTES.md for concepts
4. Create an issue with detailed description

---

**Built with ❤️ for learning event-driven architecture**
