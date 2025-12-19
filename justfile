# Quant Backtest - Development Commands
# Usage: just <command> [args...]
# Run `just` or `just --list` to see all available commands

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

# Destroy all Docker containers and images (DANGEROUS!)
[confirm("This will DELETE all Docker containers and images for this project. Continue?")]
destroy:
    @echo "Stopping all containers..."
    -docker compose down -v --remove-orphans
    @echo "Removing project images..."
    -docker rmi $(docker images -q 'v1-*' 2>/dev/null) 2>/dev/null || true
    -docker rmi $(docker images -q '*quant*' 2>/dev/null) 2>/dev/null || true
    @echo "Cleaning up dangling images..."
    -docker image prune -f
    @echo "Done! All project containers and images have been removed."

# Restart all services
restart:
    docker compose restart

# Show service status
status:
    docker compose ps

# First-time project setup (Docker mode)
setup: _ensure-env up
    @echo "Waiting for services to be healthy..."
    @sleep 5
    just db-migrate
    just data-init
    @echo ""
    @echo "Setup complete! Access the app at http://localhost:3000"
    @echo "API Docs: http://localhost:8000/api/docs"

# ==============================================================================
# Database Commands
# ==============================================================================

# Run database migrations + TimescaleDB setup
[group('db')]
db-migrate:
    docker compose exec api alembic upgrade head
    @echo "Setting up TimescaleDB hypertables and continuous aggregates..."
    docker cp backend/alembic/scripts/timescaledb_setup.sql quant_db:/tmp/timescaledb_setup.sql
    docker compose exec db psql -U quant -d quantdb -f /tmp/timescaledb_setup.sql

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

# Open database console (psql)
[group('db')]
db-console:
    docker compose exec db psql -U quant -d quantdb

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

# Install/update frontend dependencies (syncs package.json to container)
[group('dev')]
fe-install *args='':
    docker compose exec -e CI=true frontend pnpm install {{args}}

# Add a frontend dependency
[group('dev')]
fe-add *packages:
    docker compose exec -e CI=true frontend pnpm add {{packages}}

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
# Data Management Commands (Docker-based)
# ==============================================================================

# Initialize database with fixtures (user + market data + strategies, ~30s)
[group('data')]
data-init:
    docker compose exec api python -m scripts.data_cli init

# Show comprehensive data status (PostgreSQL + cache)
[group('data')]
data-status:
    docker compose exec api python -m scripts.data_cli status

# Create default system user
[group('data')]
data-seed-user:
    docker compose exec api python -m scripts.data_cli seed-user

# Load default strategies
[group('data')]
data-seed-strategy:
    docker compose exec api python -m scripts.data_cli seed-strategy

# Reset database (drop all tables + run migrations)
[group('data')]
[confirm("This will DELETE ALL DATA. Continue?")]
data-db-reset:
    docker compose exec api python -m scripts.data_cli db-reset --force

# Refresh TimescaleDB continuous aggregates
[group('data')]
data-db-refresh:
    docker compose exec api python -m scripts.data_cli db-refresh

# Download data from external sources to data/cache/
# Examples:
#   just data-download                      # Download recent 30 days (default)
#   just data-download indices              # Download index constituents
#   just data-download northbound           # Download northbound holdings
#   just data-download institutional        # Download institutional holdings
#   just data-download market_cap           # Download market cap data
[group('data')]
data-download *args='--recent 30':
    docker compose exec api python -m scripts.data_cli download {{args}}

# Load data from data/cache/ to PostgreSQL
# Examples:
#   just data-load                          # Load all data
#   just data-load stocks --full            # Load all stock data
#   just data-load --years 2024             # Load data for 2024 only
#   just data-load market_cap --years 2024  # Load market cap for 2024
#   just data-load northbound --full        # Load all northbound data
#   just data-load institutional            # Load institutional holdings
[group('data')]
data-load *args='':
    docker compose exec api python -m scripts.data_cli load {{args}}

# Incremental update (download + import today's data)
[group('data')]
data-update:
    docker compose exec api python -m scripts.data_cli update

# Copy SQLite files between directories
# Example: just data-copy-cache --source /path/to/source
[group('data')]
data-copy-cache *args='':
    docker compose exec api python -m scripts.data_cli copy-cache {{args}}

# Generate fixture data from cache dataset
[group('data')]
data-generate-fixtures *args='':
    docker compose exec api python -m scripts.data_cli generate-fixtures {{args}}

# Download fixture data from AKShare
# Examples:
#   just data-download-fixtures                    # Download all (market_cap, northbound, institutional)
#   just data-download-fixtures market_cap         # Download market cap only
#   just data-download-fixtures northbound         # Download northbound only
#   just data-download-fixtures institutional      # Download institutional only
[group('data')]
data-download-fixtures *args='all':
    docker compose exec api python -m scripts.download_fixtures {{args}}

# ==============================================================================
# Internal Helpers
# ==============================================================================

# Ensure backend .env file exists for Docker
_ensure-env:
    @test -f backend/.env.docker || (echo "Error: backend/.env.docker not found" && exit 1)
    @test -f frontend/.env || (echo "Creating frontend/.env..." && cp frontend/.env.example frontend/.env)
