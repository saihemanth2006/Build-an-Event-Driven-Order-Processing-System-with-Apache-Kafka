# API Documentation
## Event-Driven Order Processing System - Order Service

### Base URL
```
http://localhost:8000
```

### API Version
```
1.0.0
```

---

## Endpoints

### 1. Create Order
**POST** `/api/orders`

Create a new order in the system. Returns 202 ACCEPTED if successful.

#### Request Body
```json
{
  "user_id": "550e8400-e29b-41d4-a716-446655440000",
  "items": [
    {
      "sku": "SKU-001",
      "quantity": 2,
      "price": 1299.99
    },
    {
      "sku": "SKU-002",
      "quantity": 5,
      "price": 19.99
    }
  ]
}
```

#### Parameters
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| user_id | UUID | Yes | Unique identifier of the user placing the order |
| items | Array | Yes | List of items being ordered (minimum 1 item) |
| items[].sku | String | Yes | Stock Keeping Unit (product identifier) |
| items[].quantity | Integer | Yes | Quantity to order (must be > 0) |
| items[].price | Float | Yes | Unit price of the item (must be > 0) |

#### Response (202 Accepted)
```json
{
  "order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "PENDING",
  "total_amount": 2699.93,
  "created_at": "2024-01-15T10:30:45.123Z"
}
```

#### Response (400 Bad Request)
```json
{
  "error": "Validation Error",
  "detail": "At least one item is required"
}
```

#### Example cURL
```bash
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "550e8400-e29b-41d4-a716-446655440000",
    "items": [
      {
        "sku": "SKU-001",
        "quantity": 2,
        "price": 1299.99
      }
    ]
  }'
```

---

### 2. Get Order
**GET** `/api/orders/{order_id}`

Retrieve details of a specific order.

#### Path Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| order_id | UUID | Yes | The unique identifier of the order |

#### Response (200 OK)
```json
{
  "order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "user_id": "550e8400-e29b-41d4-a716-446655440000",
  "items": [
    {
      "sku": "SKU-001",
      "quantity": 2,
      "price": 1299.99
    },
    {
      "sku": "SKU-002",
      "quantity": 5,
      "price": 19.99
    }
  ],
  "status": "PROCESSING",
  "total_amount": 2699.93,
  "created_at": "2024-01-15T10:30:45.123Z",
  "updated_at": "2024-01-15T10:31:22.456Z"
}
```

#### Status Values
- `PENDING`: Order created, awaiting inventory check
- `PROCESSING`: Inventory verified and reserved
- `COMPLETED`: Order successfully processed
- `FAILED`: Order failed due to insufficient inventory

#### Response (404 Not Found)
```json
{
  "error": "Not Found",
  "detail": "Order f47ac10b-58cc-4372-a567-0e02b2c3d479 not found"
}
```

#### Example cURL
```bash
curl -X GET http://localhost:8000/api/orders/f47ac10b-58cc-4372-a567-0e02b2c3d479
```

---

### 3. List Orders
**GET** `/api/orders?user_id={user_id}&limit={limit}&offset={offset}`

List all orders for a specific user with pagination.

#### Query Parameters
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| user_id | UUID | Yes | - | The user to list orders for |
| limit | Integer | No | 50 | Max number of orders (1-100) |
| offset | Integer | No | 0 | Pagination offset |

#### Response (200 OK)
```json
[
  {
    "order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "user_id": "550e8400-e29b-41d4-a716-446655440000",
    "items": [...],
    "status": "PROCESSING",
    "total_amount": 2699.93,
    "created_at": "2024-01-15T10:30:45.123Z",
    "updated_at": "2024-01-15T10:31:22.456Z"
  },
  {
    "order_id": "550e8400-e29b-41d4-a716-446655440099",
    "user_id": "550e8400-e29b-41d4-a716-446655440000",
    "items": [...],
    "status": "COMPLETED",
    "total_amount": 149.99,
    "created_at": "2024-01-14T08:15:30.789Z",
    "updated_at": "2024-01-14T08:20:15.012Z"
  }
]
```

#### Example cURL
```bash
curl -X GET "http://localhost:8000/api/orders?user_id=550e8400-e29b-41d4-a716-446655440000&limit=20&offset=0"
```

---

### 4. Health Check
**GET** `/health`

Check if the Order Service is running and healthy.

#### Response (200 OK)
```json
{
  "status": "healthy",
  "service": "Order Service"
}
```

---

## Event Flow

### 1. Order Creation Flow
```
1. Client calls POST /api/orders
   ↓
2. Order Service creates order (status=PENDING)
   ↓
3. Order Service saves order to database
   ↓
4. Order Service adds OrderCreated event to Outbox table (same transaction)
   ↓
5. Outbox Publisher polls and finds unprocessed event
   ↓
6. Publishes OrderCreated event to Kafka topic: "order-events"
   ↓
7. Inventory Service consumes OrderCreated event
   ↓
8. Inventory Service deducts stock (idempotently)
   ↓
9. Inventory Service publishes InventoryUpdated event
   ↓
10. Order Service updates order status to PROCESSING
    ↓
11. Notification Service sends notification to customer
```

### 2. Failure Flow
```
1. If Inventory Service detects insufficient stock:
   ↓
2. Publishes OrderFailed event
   ↓
3. Order Service updates order status to FAILED
   ↓
4. Notification Service sends failure notification
   ↓
5. Event moved to Dead Letter Queue (DLQ) for review
```

---

## Error Codes

| Code | Message | Description |
|------|---------|-------------|
| 200 | OK | Successful request |
| 202 | Accepted | Order accepted for processing |
| 400 | Bad Request | Invalid input parameters |
| 404 | Not Found | Order/resource not found |
| 500 | Internal Server Error | Server-side error |

---

## Data Models

### Order Object
```json
{
  "order_id": "UUID",
  "user_id": "UUID",
  "items": [
    {
      "sku": "string",
      "quantity": "integer",
      "price": "float"
    }
  ],
  "status": "PENDING | PROCESSING | COMPLETED | FAILED",
  "total_amount": "float",
  "created_at": "ISO8601 datetime",
  "updated_at": "ISO8601 datetime"
}
```

### Event Object (Kafka)
```json
{
  "event_id": "string",
  "event_type": "OrderCreated | InventoryUpdated | OrderFailed",
  "timestamp": "ISO8601 datetime",
  "version": "1.0",
  "data": {
    "order_id": "UUID",
    "user_id": "UUID",
    "items": [...],
    "total_amount": "float"
  }
}
```

---

## Testing

### Test Order Creation
```bash
# Create order
ORDER_RESPONSE=$(curl -s -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "550e8400-e29b-41d4-a716-446655440000",
    "items": [
      {"sku": "SKU-001", "quantity": 2, "price": 1299.99}
    ]
  }')

ORDER_ID=$(echo $ORDER_RESPONSE | jq -r '.order_id')

# Wait for processing
sleep 2

# Retrieve order
curl -s -X GET http://localhost:8000/api/orders/$ORDER_ID | jq .
```

### Monitor Kafka Events
```bash
# List Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092

# Consume from order-events topic
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic order-events --from-beginning
```

### Check Database
```bash
# Connect to MySQL
docker exec -it mysql mysql -u orderuser -porderpass123 -D orderdb

# View orders
SELECT * FROM orders;

# View outbox events
SELECT * FROM outbox_events WHERE processed = 0;

# View processed events
SELECT * FROM processed_events;
```

---

## Rate Limiting
Currently no rate limiting is implemented. Add rate limiting middleware for production use.

## Authentication
Currently no authentication is implemented. Add JWT or OAuth2 for production use.

## Pagination
All list endpoints support cursor-based pagination using `limit` and `offset` parameters.

---

## Support & Documentation
For more information, see README.md and COMPREHENSIVE_NOTES.md
