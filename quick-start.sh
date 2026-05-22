#!/bin/bash

# Event-Driven Order Processing System - Quick Start Script
# This script automates the setup and testing of the system

set -e

echo "================================="
echo "Event-Driven Order Processing"
echo "Quick Start Setup"
echo "================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_section() {
    echo -e "\n${BLUE}>>> $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Step 1: Check prerequisites
print_section "Checking Prerequisites"

if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker."
    exit 1
fi
print_success "Docker installed"

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose not found. Please install Docker Compose."
    exit 1
fi
print_success "Docker Compose installed"

# Step 2: Copy environment file
print_section "Setting up Environment"

if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        print_success "Created .env from .env.example"
    else
        echo "❌ .env.example not found"
        exit 1
    fi
else
    print_info ".env already exists"
fi

# Step 3: Build images
print_section "Building Docker Images"

docker-compose build --no-cache
print_success "All images built"

# Step 4: Start services
print_section "Starting Services"

docker-compose up -d
print_success "Services started in background"

# Step 5: Wait for services to be healthy
print_section "Waiting for Services to be Healthy"

echo "Waiting for Order Service..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if curl -s http://localhost:8000/health > /dev/null; then
        print_success "Order Service is healthy"
        break
    fi
    attempt=$((attempt + 1))
    echo -n "."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "❌ Order Service failed to start"
    docker-compose logs order-service
    exit 1
fi

echo ""
print_success "All services are healthy"

# Step 6: Display credentials and endpoints
print_section "System Information"

echo "API Endpoint: http://localhost:8000"
echo "API Docs: http://localhost:8000/api/docs"
echo ""
echo "Database:"
echo "  Host: localhost:3306"
echo "  User: orderuser"
echo "  Password: orderpass123"
echo "  Database: orderdb"
echo ""
echo "Kafka:"
echo "  Bootstrap Servers: kafka:9092"
echo "  Topics: order-events, inventory-events, failed-orders, dlq-events"

# Step 7: Create test order
print_section "Creating Test Order"

# Generate a UUID for user_id
USER_ID="550e8400-e29b-41d4-a716-446655440000"

# Create order
RESPONSE=$(curl -s -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d "{
    \"user_id\": \"$USER_ID\",
    \"items\": [
      {\"sku\": \"SKU-001\", \"quantity\": 1, \"price\": 1299.99},
      {\"sku\": \"SKU-002\", \"quantity\": 3, \"price\": 19.99}
    ]
  }")

ORDER_ID=$(echo $RESPONSE | grep -o '"order_id":"[^"]*' | cut -d'"' -f4)

if [ -n "$ORDER_ID" ]; then
    print_success "Order created: $ORDER_ID"
    echo ""
    echo "Response:"
    echo $RESPONSE | jq . 2>/dev/null || echo $RESPONSE
else
    echo "❌ Failed to create order"
    echo $RESPONSE
    exit 1
fi

# Step 8: Display next steps
print_section "Next Steps"

echo ""
echo "1. View order status:"
echo "   curl http://localhost:8000/api/orders/$ORDER_ID"
echo ""
echo "2. List user orders:"
echo "   curl \"http://localhost:8000/api/orders?user_id=$USER_ID\""
echo ""
echo "3. Watch logs:"
echo "   docker-compose logs -f"
echo ""
echo "4. View API documentation:"
echo "   Open http://localhost:8000/api/docs in your browser"
echo ""
echo "5. Stop services:"
echo "   docker-compose down"
echo ""
echo "6. Stop and remove volumes:"
echo "   docker-compose down -v"
echo ""

# Display documentation
print_section "Documentation"

echo "Read the following for more information:"
echo "  - README.md: Setup and operations guide"
echo "  - API_DOCS.md: Complete API reference"
echo "  - COMPREHENSIVE_NOTES.md: Deep knowledge base"
echo "  - PROJECT_SUMMARY.md: Project completion details"
echo ""

print_success "Setup Complete!"
echo ""
echo "The system is now running and ready to process orders."
echo "Monitor the event flow with: docker-compose logs -f"
