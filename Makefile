# Quant Backtest - Makefile
# Convenience commands for development and database management

.PHONY: help up down logs db-init db-reset db-loaddata db-loaddata-file db-status db-fresh

# Default target
help:
	@echo "Quant Backtest - Available Commands"
	@echo ""
	@echo "Docker:"
	@echo "  make up              Start all services"
	@echo "  make down            Stop all services"
	@echo "  make logs            View logs (follow mode)"
	@echo ""
	@echo "Database:"
	@echo "  make db-init         Initialize database schema"
	@echo "  make db-reset        Reset database (drop all tables)"
	@echo "  make db-loaddata     Load sample data (15 stocks)"
	@echo "  make db-status       Show database status"
	@echo "  make db-fresh        Reset + load sample data"
	@echo ""
	@echo "  make db-loaddata-file FILE=/path/to/data.db"
	@echo "                       Load data from external SQLite file"
	@echo ""
	@echo "Example:"
	@echo "  make db-loaddata-file FILE=/Users/dan/Code/q/trading_data/a_stock_2024.db"

# Docker commands
up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

# Database commands
db-init:
	docker-compose exec api python cmd.py db init

db-reset:
	docker-compose exec api python cmd.py db reset --force

db-loaddata:
	docker-compose exec api python cmd.py db loaddata

db-status:
	docker-compose exec api python cmd.py db status

# Load external data file
# Usage: make db-loaddata-file FILE=/path/to/data.db
db-loaddata-file:
	@test -n "$(FILE)" || (echo "Error: FILE is required. Usage: make db-loaddata-file FILE=/path/to/data.db" && exit 1)
	@test -f "$(FILE)" || (echo "Error: File not found: $(FILE)" && exit 1)
	@echo "Copying $(FILE) to container..."
	docker cp "$(FILE)" quant_api:/tmp/data.db
	@echo "Loading data..."
	docker-compose exec api python cmd.py db loaddata --source /tmp/data.db
	@echo "Cleaning up..."
	docker-compose exec api rm /tmp/data.db
	@echo "Done!"

# Combined commands
db-fresh: db-reset db-loaddata
	@echo "Database reset and sample data loaded!"
