# Event-Driven Order Processing System - Makefile
# Convenience commands for development and operations

.PHONY: help build up down logs logs-order logs-inventory logs-notification \
        test test-order test-inventory test-notification clean \
        ps health db-shell kafka-topics api-docs

help:
	@echo "Event-Driven Order Processing System - Available Commands"
	@echo ""
	@echo "Setup & Running:"
	@echo "  make build              - Build all Docker images"
	@echo "  make up                 - Start all services"
	@echo "  make down               - Stop all services"
	@echo "  make clean              - Stop services and remove volumes"
	@echo ""
	@echo "Monitoring:"
	@echo "  make ps                 - Show running containers"
	@echo "  make logs               - Watch all logs"
	@echo "  make logs-order         - Watch Order Service logs"
	@echo "  make logs-inventory     - Watch Inventory Service logs"
	@echo "  make logs-notification  - Watch Notification Service logs"
	@echo ""
	@echo "Health & Testing:"
	@echo "  make health             - Check service health"
	@echo "  make test               - Run all tests"
	@echo "  make test-order         - Test Order Service"
	@echo "  make test-inventory     - Test Inventory Service"
	@echo "  make test-notification  - Test Notification Service"
	@echo ""
	@echo "Database & Message Queue:"
	@echo "  make db-shell           - Connect to MySQL"
	@echo "  make kafka-topics       - List Kafka topics"
	@echo "  make kafka-console      - Consume from order-events"
	@echo ""
	@echo "Development:"
	@echo "  make api-docs           - Open API documentation"
	@echo "  make create-order       - Create a test order"
	@echo "  make list-orders        - List orders"

# Setup Commands
build:
	@echo "Building Docker images..."
	docker-compose build

up:
	@echo "Starting services..."
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 5
	@make health

down:
	@echo "Stopping services..."
	docker-compose down

clean:
	@echo "Stopping services and removing volumes..."
	docker-compose down -v
	@echo "Clean complete"

# Monitoring Commands
ps:
	@docker-compose ps

logs:
	@docker-compose logs -f

logs-order:
	@docker-compose logs -f order-service

logs-inventory:
	@docker-compose logs -f inventory-service

logs-notification:
	@docker-compose logs -f notification-service

# Health Check
health:
	@echo "Checking Order Service health..."
	@curl -s http://localhost:8000/health | jq . || echo "Order Service not responding"
	@echo ""
	@echo "Container Status:"
	@docker-compose ps

# Testing
test:
	@echo "Running all tests..."
	docker-compose run --rm order-service pytest tests/ -v
	docker-compose run --rm inventory-service pytest tests/ -v
	docker-compose run --rm notification-service pytest tests/ -v

test-order:
	@echo "Testing Order Service..."
	docker-compose run --rm order-service pytest tests/ -v

test-inventory:
	@echo "Testing Inventory Service..."
	docker-compose run --rm inventory-service pytest tests/ -v

test-notification:
	@echo "Testing Notification Service..."
	docker-compose run --rm notification-service pytest tests/ -v

# Database Commands
db-shell:
	@echo "Connecting to MySQL..."
	docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb

db-orders:
	@echo "Viewing orders..."
	docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "SELECT id, user_id, status, total_amount, created_at FROM orders LIMIT 10;"

db-inventory:
	@echo "Viewing inventory..."
	docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "SELECT sku, name, stock, reserved_stock FROM inventory;"

db-outbox:
	@echo "Viewing unprocessed outbox events..."
	docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "SELECT id, aggregate_id, event_type, processed FROM outbox_events LIMIT 10;"

db-dlq:
	@echo "Viewing dead letter events..."
	docker-compose exec mysql mysql -u orderuser -porderpass123 -D orderdb -e "SELECT event_id, event_type, error_reason, created_at FROM dead_letter_events LIMIT 10;"

# Kafka Commands
kafka-topics:
	@echo "Available Kafka topics:"
	@docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:9092

kafka-console:
	@echo "Consuming from order-events topic..."
	docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic order-events --from-beginning

# API Commands
api-docs:
	@echo "Opening API documentation..."
	@if command -v open >/dev/null 2>&1; then \
		open http://localhost:8000/api/docs; \
	elif command -v xdg-open >/dev/null 2>&1; then \
		xdg-open http://localhost:8000/api/docs; \
	elif command -v start >/dev/null 2>&1; then \
		start http://localhost:8000/api/docs; \
	else \
		echo "Visit http://localhost:8000/api/docs in your browser"; \
	fi

create-order:
	@echo "Creating test order..."
	@curl -X POST http://localhost:8000/api/orders \
	  -H "Content-Type: application/json" \
	  -d '{"user_id": "550e8400-e29b-41d4-a716-446655440000", "items": [{"sku": "SKU-001", "quantity": 1, "price": 1299.99}]}' | jq .

list-orders:
	@echo "Listing orders..."
	@curl -s "http://localhost:8000/api/orders?user_id=550e8400-e29b-41d4-a716-446655440000" | jq .

# Development
restart: down up

rebuild: clean build up

logs-all: up logs

dev: build up api-docs logs

# Print help by default
.DEFAULT_GOAL := help
