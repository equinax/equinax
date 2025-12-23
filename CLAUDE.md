# Claude Code Project Context

## Project Overview

A Chinese stock quantitative trading platform with:
- **Backend**: FastAPI + PostgreSQL (TimescaleDB) + Redis + ARQ workers
- **Frontend**: React + Vite + TypeScript + TailwindCSS + shadcn/ui
- **Data Pipeline**: trading_data directory with akshare-based data downloaders

## Docker Environment

This project runs in Docker. **DO NOT** use `.venv` or local Python environments.

### Running Backend Commands

All backend Python commands should be run inside the `quant_api` container:

```bash
# General pattern
docker compose exec api python -m <module>

# Examples:
docker compose exec api python -m scripts.import_sw_industry
docker compose exec api python -m scripts.import_sw_industry --system em
docker compose exec api python -m scripts.migrate_all_data --help
docker compose exec api alembic upgrade head
```

### Database Access

Database runs in `quant_db` container, exposed on port `54321`:

```bash
# From host machine
PGPASSWORD=quant_dev_password psql -h localhost -p 54321 -U quant -d quantdb

# Or use docker exec
docker compose exec db psql -U quant -d quantdb
```

### Service URLs

| Service  | Container       | Host Port | Description            |
|----------|-----------------|-----------|------------------------|
| API      | quant_api       | 8000      | FastAPI backend        |
| Frontend | quant_frontend  | 3000      | React dev server       |
| Database | quant_db        | 54321     | TimescaleDB/PostgreSQL |
| Redis    | quant_redis     | 6379      | Task queue & cache     |
| Metabase | quant_metabase  | 7600      | Data exploration UI    |

### Common Docker Commands

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f api
docker compose logs -f worker

# Restart a service
docker compose restart api

# Run one-off command
docker compose exec api python -m scripts.import_sw_industry

# Rebuild after Dockerfile changes
docker compose build api
docker compose up -d api
```

## Data Sources

- **trading_data** (`/Users/dan/Code/q/trading_data`): Local SQLite cache with:
  - Daily K-line data
  - Industry classification (SW申万 + EM东财)
  - Stock profiles
  - Market cap data

Data is downloaded via akshare and cached in SQLite, then imported to PostgreSQL.

## Key Directories

```
v1/
├── backend/
│   ├── app/           # FastAPI application
│   ├── scripts/       # Data import/migration scripts
│   ├── workers/       # ARQ background workers
│   └── data/          # Cache and fixtures
├── frontend/
│   ├── src/
│   │   ├── api/       # Generated API hooks (orval)
│   │   ├── components/
│   │   └── pages/
│   └── orval.config.ts
└── docker-compose.yml
```

## Frontend API Generation

API types are auto-generated from OpenAPI spec:

```bash
# From frontend directory
pnpm orval
```

This reads from `http://localhost:8000/openapi.json` and generates typed hooks.

## Industry Classification Systems

| System | Levels | Count | Description |
|--------|--------|-------|-------------|
| SW (申万) | L1/L2/L3 | 31/124/258 | Hierarchical structure |
| EM (东财) | L1 only | 86 | Flat structure |

Import commands:
```bash
docker compose exec api python -m scripts.import_sw_industry           # All
docker compose exec api python -m scripts.import_sw_industry --system sw  # SW only
docker compose exec api python -m scripts.import_sw_industry --system em  # EM only
```
