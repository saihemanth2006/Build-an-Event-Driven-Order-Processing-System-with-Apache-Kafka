# Event-Driven Order Processing System with Apache Kafka

This is a project I built to learn about microservices and event-driven architecture. It's basically an order processing system where different services communicate with each other using Apache Kafka instead of calling each other directly.

## Table of Contents

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

## Overview

So basically, I built this system with three microservices:

- **Order Service**: This is where users create orders. It's a REST API that takes order data and publishes events.
- **Inventory Service**: This listens to order events and checks if we have enough stock. If we do, it deducts from inventory.
- **Notification Service**: This listens to different events and sends notifications (simulated with logs).
- **Kafka**: Acts as a message broker so services don't have to talk to each other directly.
- **MySQL**: Stores all the data.

The cool part is that I implemented some important patterns:

- Transactional Outbox Pattern - makes sure events don't get lost
- Idempotent Consumers - so if an event is processed twice, nothing bad happens
- Dead Letter Queues - for messages that can't be processed
- Proper error handling and retries

---

## Architecture

Ok so here's how it all works together. I set it up with Docker Compose so everything runs in containers.

```
Clients create orders via REST API
              |
              v
        Order Service (FastAPI)
        - Receives order request
        - Saves to database
        - Publishes OrderCreated event to Kafka
              |
              v
         Apache Kafka
        (Message Broker)
              |
      --------|--------
      |               |
      v               v
Inventory Service  Notification Service
- Checks stock     - Sends messages
- Deducts items    - Listens to multiple events
- Publishes events - Logs everything
      |               |
      --------|--------
              v
          MySQL Database
```

### How it works step by step:

1. Someone creates an order through the REST API
2. Order Service saves the order and also saves an event to the "outbox" table in the same database transaction
3. A background process in Order Service polls the outbox table and publishes events to Kafka
4. Inventory Service reads the OrderCreated event
5. It checks if we have enough stock, then deducts from inventory
6. Publishes an InventoryUpdated event back to Kafka
7. Order status gets updated
8. Notification Service sees all the events and logs them out
9. If something fails, the message goes to a dead letter queue so we can investigate later

---

## Prerequisites

Before running this, you'll need:

- Docker and Docker Compose (v20.10 or newer)
- Python 3.11+ if you want to run things locally (but honestly just use Docker)
- At least 4GB of RAM, 8GB is better
- These ports need to be available: 2181, 3306, 8000, 9092

### What's installed in the Docker containers:

- Apache Kafka 7.5.0
- MySQL 8.0
- Zookeeper 7.5.0
- FastAPI, SQLAlchemy, aiokafka (Python libraries)

---

## Installation & Setup

Ok so here's how to get it running:

### Step 1: Clone the repo

```bash
git clone https://github.com/yourusername/event-driven-order-processing.git
cd event-driven-order-processing
```

### Step 2: Set up environment variables

```bash
# Copy the example env file
cp .env.example .env

# You can edit it if you want, but the defaults work fine with Docker Compose
```

### Step 3: Build the Docker images

```bash
# Build all the services
docker-compose build

# Or without cache if something is weird
docker-compose build --no-cache
```

### Step 4: Start everything

```bash
# Spin up all services
docker-compose up -d

# Check if everything started ok
docker-compose ps
```

### Step 5: Verify it's actually working

```bash
# Test the Order Service
curl http://localhost:8000/health

# Check Kafka
docker-compose exec kafka kafka-broker-api-versions.sh --bootstrap-server kafka:9092

# Check MySQL
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "SHOW TABLES;"
```

---

## Running the System

### Create an order

```bash
# Generate a UUID for the user
USER_ID="550e8400-e29b-41d4-a716-446655440000"

# Create an order with some items
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d "{
    \"user_id\": \"$USER_ID\",
    \"items\": [
      {\"sku\": \"SKU-001\", \"quantity\": 1, \"price\": 1299.99},
      {\"sku\": \"SKU-002\", \"quantity\": 3, \"price\": 19.99}
      ]
  }"

# Should get back 202 Accepted
```

### Watch it happen in real-time

Open multiple terminals and run these to watch the events flow through:

```bash
# Terminal 1: Order Service logs
docker-compose logs -f order-service

# Terminal 2: Inventory Service logs
docker-compose logs -f inventory-service

# Terminal 3: Notification Service logs
docker-compose logs -f notification-service

# Terminal 4: Kafka events in real-time
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic order-events \
  --from-beginning
```

### Check your order status

```bash
# Replace ORDER_ID with the ID from the create response
curl http://localhost:8000/api/orders/{ORDER_ID}
```

### Get all orders from a user

```bash
curl "http://localhost:8000/api/orders?user_id=$USER_ID"
```

---

## Testing

### Run the unit tests

```bash
# Test Order Service
docker-compose run --rm order-service pytest tests/ -v

# Test Inventory Service
docker-compose run --rm inventory-service pytest tests/ -v

# Test Notification Service
docker-compose run --rm notification-service pytest tests/ -v
```

### Manual testing

Honestly I just tested this by hand using curl and checking the database. You can use Postman or Insomnia if you want:

1. Open API_DOCS.md and use it as reference
2. Set base URL to http://localhost:8000
3. Create some orders and watch the status change

### Check the database directly

```bash
# See all orders
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "
  SELECT id, user_id, status, total_amount, created_at FROM orders;
"

# Check outbox (events that haven't been published yet)
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "
  SELECT id, aggregate_id, event_type, processed FROM outbox_events;
"

# Check processed events (for idempotency tracking)
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "
  SELECT consumer_id, event_id, event_type FROM processed_events;
"

# Check dead letter queue (failed messages)
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "
  SELECT event_id, event_type, error_reason, created_at FROM dead_letter_events;
"

# Check inventory levels
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "
  SELECT sku, name, stock, reserved_stock FROM inventory;
"
```

---

## Project Structure

Here's what I created:

```
event-driven-order-processing/
│
├── docker-compose.yml                 # Runs all the services
├── .env                               # Configuration vars
├── .env.example                       # Example config
│
├── sql/
│   └── init.sql                       # Database setup and sample data
│
├── order-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── src/
│   │   ├── main.py                    # FastAPI app
│   │   ├── config.py                  # Settings
│   │   ├── database.py                # Database stuff
│   │   ├── api.py                     # REST endpoints
│   │   ├── kafka_producer.py          # Publishes events
│   │   └── outbox_publisher.py        # Polls and publishes outbox
│   └── tests/
│       └── test_unit.py
│
├── inventory-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── src/
│   │   ├── main.py                    # Entry point
│   │   ├── config.py                  # Settings
│   │   ├── inventory_logic.py         # Business logic
│   │   ├── kafka_consumer.py          # Event listener
│   │   └── database.py                # Database queries
│   └── tests/
│       └── test_unit.py
│
├── notification-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── src/
│   │   ├── main.py                    # Entry point
│   │   ├── config.py                  # Settings
│   │   ├── notification_logic.py      # Sends notifications
│   │   └── kafka_consumer.py          # Listens to multiple topics
│   └── tests/
│       └── test_unit.py
│
├── README.md                          # This file
└── API_DOCS.md                        # API reference
```

---

## Key Concepts I Used

### Transactional Outbox Pattern

This is probably the most important pattern here. The problem is that if you save data to the database and then publish an event to Kafka, what happens if your service crashes between those two operations? The event never gets published but the data is saved.

The solution: do both in one database transaction. Save the entity AND save an event to an "outbox" table at the same time. Then have a separate process poll that outbox table and publish events. If the service crashes, when it restarts, it can still publish the events from the outbox table.

### Idempotent Consumers

Kafka gives "at-least-once" delivery guarantee. That means if your consumer crashes after processing but before committing the offset, it might process the same message twice.

To handle this, I track which events have been processed using a `processed_events` table with a unique constraint on (consumer_id, event_id). Before processing, I check if this event was already processed. If it was, I skip it. This way, processing the same event multiple times has the same effect as processing it once.

### Dead Letter Queues

If something goes wrong when processing an event (like invalid data), you can't just crash the service. That blocks everything else from processing.

Instead, when something fails, I move it to a dead letter queue table in the database. The message is out of the way, the consumer keeps running, and someone can look at it later to figure out what went wrong.

### Event Schema Versioning

I included a version field in events so I can change the event format later without breaking old consumers.

### Distributed Tracing

I use correlation IDs to track a request as it flows through multiple services. Makes debugging way easier.

---

## Stopping the System

```bash
# Stop everything but keep the data
docker-compose down

# Stop and delete everything (fresh start)
docker-compose down -v

# Just stop one service
docker-compose stop order-service

# Restart a service
docker-compose restart inventory-service
```

---

## Troubleshooting

### Services won't start

```bash
# Check what went wrong
docker-compose logs

# Try rebuilding everything
docker-compose build --no-cache

# Nuclear option: delete everything and start over
docker-compose down -v && docker-compose up -d
```

### Kafka isn't working

```bash
# Check Kafka health
docker-compose exec kafka kafka-broker-api-versions.sh --bootstrap-server kafka:9092

# List the topics
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

### MySQL errors

```bash
# Check if MySQL is running
docker-compose exec mysql mysql -u orderuser -porderpass123 -e "SELECT 1;"

# List databases
docker-compose exec mysql mysql -u orderuser -porderpass123 -e "SHOW DATABASES;"

# List tables
docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "SHOW TABLES;"
```

### Order isn't processing

1. Check order status: `GET /api/orders/{order_id}`
2. Look at logs: `docker-compose logs -f`
3. Check if inventory is available
4. Look at the dead letter queue for errors
5. Make sure Kafka is actually running

### Running slow

```bash
# Check CPU and memory usage
docker stats

# Increase container resources in docker-compose.yml if needed
# Check database indexes
# Look at the logs for any slow queries
```

---

## Monitoring

All the services log stuff in JSON format so you can easily parse it. The logs include:

- Timestamp
- Service name
- Log level
- Some kind of ID to track the request
- What actually happened

### Things to watch out for:

- How long does an order take to process
- Is Kafka getting behind on consuming messages
- Are the database queries fast
- How many messages are in the dead letter queue
- Is the service healthy

### Health checks

```
GET /health                    # Is it alive
GET /api/orders/health         # Is Order Service ok
```

---

## Security

If this were production, I'd do:

1. Better passwords (not "orderpass123")
2. Turn on Kafka authentication and encryption
3. Add API authentication (like JWT)
4. Use HTTPS
5. Rate limit the API
6. Add API keys
7. Encrypt sensitive data in the database
8. Use SSL for database connections
9. Regular security audits
10. Scan Docker images for vulnerabilities

---

## Scaling

If this got popular and I needed to handle more orders:

### Horizontal scaling (add more servers):

- Run multiple instances of Inventory Service (they share a consumer group so they automatically load balance)
- Same for Notification Service
- Run multiple Order Service instances behind a load balancer
- Add more Kafka brokers and increase replication

### Vertical scaling (make servers more powerful):

- Increase connection pools
- Give database more RAM
- More CPU for containers

### Make it faster:

- Add more database indexes
- Add a cache layer
- Compress Kafka messages
- Process events in batches
- Tune database connection pooling

---

## Resources

If you want to learn more about this stuff:

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [FastAPI Guide](https://fastapi.tiangolo.com/)
- [SQLAlchemy ORM Guide](https://docs.sqlalchemy.org/)
- [Event Sourcing Pattern](https://martinfowler.com/eaaDev/EventSourcing.html)
- [Microservices Patterns](https://microservices.io/patterns/index.html)

---

## License

MIT License - do whatever you want with this code

---

## Contributing

If you want to contribute or fork this:

1. Fork it
2. Create a branch for your feature
3. Make your changes
4. Add some tests
5. Send a pull request

---

## Questions?

If something doesn't work:

1. Check README.md (this file)
2. Check API_DOCS.md
3. Look at the logs
4. Create an issue with details

Built for learning about event-driven architecture!
