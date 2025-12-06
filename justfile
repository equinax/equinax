# Quant Backtest - Development Commands
# Usage: just <command> [args...]
# Run `just` or `just --list` to see all available commands

set dotenv-load := true
set positional-arguments := true

# Default recipe - show help
default:
    @just --list --unsorted

# ==============================================================================
# Core Commands
# ==============================================================================

# Start all services
up:
    docker compose up -d

# Stop all services
down:
    docker compose down

# Restart all services
restart:
    docker compose restart

# Show service status
status:
    docker compose ps

# First-time project setup
setup: _ensure-env up
    @echo "Waiting for services to be healthy..."
    @sleep 5
    just db-setup
    @echo ""
    @echo "Setup complete! Access the app at http://localhost:3000"

# ==============================================================================
# Database Commands
# ==============================================================================

# Initialize database (migrate + seed all)
[group('db')]
db-setup: db-migrate seed-all

# Run database migrations
[group('db')]
db-migrate:
    docker compose exec api alembic upgrade head

# Create a new migration
[group('db')]
db-migrate-new message:
    docker compose exec api alembic revision --autogenerate -m "{{message}}"

# Rollback one migration
[group('db')]
db-migrate-down:
    docker compose exec api alembic downgrade -1

# Show migration status
[group('db')]
db-migrate-status:
    @echo "=== Current Version ==="
    docker compose exec api alembic current
    @echo ""
    @echo "=== Migration History ==="
    docker compose exec api alembic history -v

# Reset database (destructive!)
[group('db')]
[confirm("This will DELETE ALL DATA. Continue?")]
db-reset:
    docker compose exec api python cmd.py db reset --force
    just seed-all

# Show database statistics
[group('db')]
db-status:
    docker compose exec api python cmd.py db status

# Open database console (psql)
[group('db')]
db-console:
    docker compose exec db psql -U quant -d quantdb

# ==============================================================================
# Seed Commands (Data Import)
# ==============================================================================

# Import all seed data (user + strategy + stocks)
[group('seed')]
seed-all: seed-user seed-strategy seed-stocks

# Create default user
[group('seed')]
seed-user:
    docker compose exec api python cmd.py seed user

# Load default strategies
[group('seed')]
seed-strategy:
    docker compose exec api python cmd.py seed strategy

# Load sample stock data (built-in 15 stocks)
[group('seed')]
seed-stocks:
    docker compose exec api python cmd.py seed stocks

# Load stock data from external SQLite file
[group('seed')]
seed-stocks-file file:
    @test -f "{{file}}" || (echo "Error: File not found: {{file}}" && exit 1)
    @echo "Copying {{file}} to container..."
    docker cp "{{file}}" quant_api:/tmp/data.db
    @echo "Loading data..."
    docker compose exec api python cmd.py seed stocks --source /tmp/data.db
    @echo "Cleaning up..."
    docker compose exec api rm /tmp/data.db
    @echo "Done!"

# Clear all stock data (for switching data source)
[group('seed')]
[confirm("This will clear all stock data. Continue?")]
seed-stocks-clear:
    docker compose exec api python cmd.py seed stocks --clear

# ==============================================================================
# Development Commands
# ==============================================================================

# View logs (all services or specific service)
[group('dev')]
dev-logs *args='':
    docker compose logs -f {{args}}

# View API logs only
[group('dev')]
dev-logs-api:
    docker compose logs -f api

# View worker logs only
[group('dev')]
dev-logs-worker:
    docker compose logs -f worker

# Open shell in API container
[group('dev')]
dev-shell:
    docker compose exec api bash

# Open Python REPL with app context
[group('dev')]
dev-python:
    docker compose exec api python

# Run linter (ruff check)
[group('dev')]
dev-lint:
    docker compose exec api ruff check app/

# Run formatter (ruff format)
[group('dev')]
dev-format:
    docker compose exec api ruff format app/

# Generate API client for frontend
[group('dev')]
dev-api-gen:
    cd frontend && pnpm run api:generate

# ==============================================================================
# Test Commands
# ==============================================================================

# Run backend tests
[group('test')]
test-backend *args='':
    docker compose exec api pytest {{args}}

# Run backend tests with coverage
[group('test')]
test-coverage:
    docker compose exec api pytest --cov=app --cov-report=html

# ==============================================================================
# Internal Helpers
# ==============================================================================

# Ensure .env file exists
_ensure-env:
    @test -f .env || (echo "Creating .env from .env.docker..." && cp .env.docker .env)
