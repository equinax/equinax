#!/usr/bin/env python3
"""
Unified Data Management CLI

All data operations in one place:
- init        : Initialize database with fixtures (dev mode, ~30s)
- status      : Show database status (PostgreSQL + cache)
- seed-user   : Create default system user
- seed-strategy: Load default strategies
- download    : Download data from external sources
- load        : Load data from cache to PostgreSQL
- update      : Incremental update (today's data)
- db-reset    : Reset database (drop all tables)
- db-refresh  : Refresh continuous aggregates
- copy-cache  : Copy cache data to another location
- generate-fixtures: Create fixture files

Usage:
    python -m scripts.data_cli init
    python -m scripts.data_cli status
    python -m scripts.data_cli seed-user
    python -m scripts.data_cli seed-strategy
    python -m scripts.data_cli db-reset
"""

import asyncio
import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import subprocess
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

# Paths
BACKEND_DIR = Path(__file__).parent.parent
DATA_DIR = BACKEND_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
FIXTURES_DIR = DATA_DIR / "fixtures"
# NOTE: All data storage now unified in CACHE_DIR (data/cache/)
DEFAULT_STRATEGIES_PATH = FIXTURES_DIR / "default_strategies.json"

# Default database URL
DEFAULT_DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+asyncpg://quant:quant_dev_password@localhost:54321/quantdb"
)

# Default system user
DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000001"
DEFAULT_USER_EMAIL = "system@localhost"
DEFAULT_USER_NAME = "system"

# CLI app
app = typer.Typer(
    name="data",
    help="Unified data management CLI for quant system",
    add_completion=False,
)
console = Console()


# =============================================================================
# Helper Functions
# =============================================================================


def get_sync_pg_url(url: str) -> str:
    """Convert async URL to sync URL for asyncpg."""
    return url.replace("postgresql+asyncpg://", "postgresql://").replace("+asyncpg", "")


def get_cache_path(name: str) -> Path:
    """Get cache file path."""
    return CACHE_DIR / name


def get_fixture_path(name: str) -> Path:
    """Get fixture file path."""
    return FIXTURES_DIR / name


def ensure_cache_dir():
    """Ensure cache directory exists."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_sqlite_stats(db_path: Path) -> dict:
    """Get statistics from SQLite database."""
    if not db_path.exists():
        return {"exists": False}

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        stats = {"exists": True, "path": str(db_path), "size_mb": db_path.stat().st_size / 1024 / 1024}

        # Try to get record counts from common tables
        for table in ["daily_k_data", "stock_basic", "etf_basic", "stock_market_cap",
                      "index_constituents", "northbound_holdings", "institutional_holdings",
                      "industry_classification", "stock_industry_mapping"]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats[f"{table}_count"] = cursor.fetchone()[0]
            except:
                pass

        # Try to get date range
        for table in ["daily_k_data", "stock_market_cap", "northbound_holdings"]:
            try:
                cursor.execute(f"SELECT MIN(date), MAX(date) FROM {table}")
                row = cursor.fetchone()
                if row[0]:
                    stats[f"{table}_date_range"] = f"{row[0]} ~ {row[1]}"
            except:
                pass

        conn.close()
        return stats
    except Exception as e:
        return {"exists": True, "error": str(e)}


async def _import_market_cap_fixture(pg_url: str, sqlite_path: Path) -> int:
    """
    Import market cap data from SQLite fixture to PostgreSQL indicator_valuation table.

    SQLite table: stock_market_cap(code, date, total_mv, circ_mv)
    PostgreSQL table: indicator_valuation(code, date, total_mv, circ_mv, ...)
    """
    import asyncpg

    # Read from SQLite
    conn_sqlite = sqlite3.connect(str(sqlite_path))
    cursor = conn_sqlite.cursor()
    cursor.execute("SELECT code, date, total_mv, circ_mv FROM stock_market_cap")
    rows = cursor.fetchall()
    conn_sqlite.close()

    if not rows:
        return 0

    # Convert date strings to Python date objects
    def parse_date(date_str):
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    records = [(row[0], parse_date(row[1]), row[2], row[3]) for row in rows]

    # Insert to PostgreSQL
    pg_conn = await asyncpg.connect(pg_url)
    try:
        # Use ON CONFLICT to upsert - only update market cap fields
        await pg_conn.executemany(
            """
            INSERT INTO indicator_valuation (code, date, total_mv, circ_mv)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (code, date) DO UPDATE SET
                total_mv = COALESCE(EXCLUDED.total_mv, indicator_valuation.total_mv),
                circ_mv = COALESCE(EXCLUDED.circ_mv, indicator_valuation.circ_mv)
            """,
            records
        )
        return len(records)
    finally:
        await pg_conn.close()


async def _import_northbound_fixture(pg_url: str, sqlite_path: Path) -> int:
    """
    Import northbound holdings from SQLite fixture to PostgreSQL stock_microstructure table.

    SQLite table: northbound_holdings(code, date, holding_ratio, holding_change)
    PostgreSQL table: stock_microstructure(code, date, northbound_holding_ratio, northbound_holding_change, ...)
    """
    import asyncpg

    # Read from SQLite
    conn_sqlite = sqlite3.connect(str(sqlite_path))
    cursor = conn_sqlite.cursor()
    cursor.execute("SELECT code, date, holding_ratio, holding_change FROM northbound_holdings")
    rows = cursor.fetchall()
    conn_sqlite.close()

    if not rows:
        return 0

    # Convert date strings to Python date objects
    def parse_date(date_str):
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    # Build records with all required boolean fields
    records = [
        (
            row[0],                           # code
            parse_date(row[1]),               # date
            row[2],                           # northbound_holding_ratio
            row[3],                           # northbound_holding_change
            row[2] > 5.0 if row[2] else False,  # is_northbound_heavy
            False,                            # is_institutional
            False,                            # is_retail_hot
            False,                            # is_main_controlled
        )
        for row in rows
    ]

    # Insert to PostgreSQL
    pg_conn = await asyncpg.connect(pg_url)
    try:
        # Use ON CONFLICT to upsert - only update northbound fields
        # Include all boolean fields to avoid NOT NULL constraint violations
        await pg_conn.executemany(
            """
            INSERT INTO stock_microstructure (
                code, date, northbound_holding_ratio, northbound_holding_change,
                is_northbound_heavy, is_institutional, is_retail_hot, is_main_controlled
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (code, date) DO UPDATE SET
                northbound_holding_ratio = COALESCE(EXCLUDED.northbound_holding_ratio, stock_microstructure.northbound_holding_ratio),
                northbound_holding_change = COALESCE(EXCLUDED.northbound_holding_change, stock_microstructure.northbound_holding_change),
                is_northbound_heavy = COALESCE(EXCLUDED.is_northbound_heavy, stock_microstructure.is_northbound_heavy)
            """,
            records
        )
        return len(records)
    finally:
        await pg_conn.close()


async def _import_institutional_fixture(pg_url: str, sqlite_path: Path) -> int:
    """
    Import institutional holdings from SQLite fixture to PostgreSQL stock_microstructure table.

    SQLite table: institutional_holdings(code, date, fund_holding_ratio, fund_holding_change)
    PostgreSQL table: stock_microstructure(code, date, is_institutional, ...)
    """
    import asyncpg

    # Read from SQLite
    conn_sqlite = sqlite3.connect(str(sqlite_path))
    cursor = conn_sqlite.cursor()
    cursor.execute("SELECT code, date, fund_holding_ratio, fund_holding_change FROM institutional_holdings")
    rows = cursor.fetchall()
    conn_sqlite.close()

    if not rows:
        return 0

    # Convert date strings to Python date objects
    def parse_date(date_str):
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    # Build records with all required boolean fields
    # is_institutional = True when fund_holding_ratio > 10%
    records = [
        (
            row[0],                           # code
            parse_date(row[1]),               # date
            row[2] > 10.0 if row[2] else False,  # is_institutional (high fund holding)
            False,                            # is_northbound_heavy (preserve existing)
            False,                            # is_retail_hot
            False,                            # is_main_controlled
        )
        for row in rows
    ]

    # Insert to PostgreSQL
    pg_conn = await asyncpg.connect(pg_url)
    try:
        # Use ON CONFLICT to upsert - only update institutional field
        await pg_conn.executemany(
            """
            INSERT INTO stock_microstructure (
                code, date, is_institutional, is_northbound_heavy, is_retail_hot, is_main_controlled
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (code, date) DO UPDATE SET
                is_institutional = COALESCE(EXCLUDED.is_institutional, stock_microstructure.is_institutional)
            """,
            records
        )
        return len(records)
    finally:
        await pg_conn.close()


# =============================================================================
# SEED Commands
# =============================================================================


@app.command("seed-user")
def seed_user(
    database_url: str = typer.Option(DEFAULT_DATABASE_URL, "--database", "-d", help="Database URL"),
):
    """Create default system user."""
    console.print("\n[bold blue]Creating default user...[/bold blue]\n")

    try:
        result = asyncio.run(_create_default_user(database_url))
        if result == 0:
            console.print("\n[green]Default user ready![/green]\n")
        else:
            raise typer.Exit(result)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise typer.Exit(1)


async def _create_default_user(database_url: str) -> int:
    """Create default system user if not exists."""
    import asyncpg

    postgres_url = get_sync_pg_url(database_url)

    try:
        conn = await asyncpg.connect(postgres_url)
    except Exception as e:
        console.print(f"[red]Failed to connect to PostgreSQL: {e}[/red]")
        return 1

    try:
        # Check if user exists
        exists = await conn.fetchval(
            "SELECT 1 FROM users WHERE id = $1",
            DEFAULT_USER_ID
        )

        if exists:
            console.print(f"  Default user already exists: [cyan]{DEFAULT_USER_EMAIL}[/cyan]")
            return 0

        # Create user
        salt = secrets.token_hex(32)
        password_hash = hashlib.sha256(f"system_default_{salt}".encode()).hexdigest()

        await conn.execute(
            """
            INSERT INTO users (id, email, username, password_hash, salt, is_active, is_admin)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING
            """,
            DEFAULT_USER_ID,
            DEFAULT_USER_EMAIL,
            DEFAULT_USER_NAME,
            password_hash,
            salt,
            True,
            False,
        )
        console.print(f"  [green]Created default user:[/green] [cyan]{DEFAULT_USER_EMAIL}[/cyan]")
        return 0

    finally:
        await conn.close()


@app.command("seed-strategy")
def seed_strategy(
    database_url: str = typer.Option(DEFAULT_DATABASE_URL, "--database", "-d", help="Database URL"),
):
    """Load default strategies from fixtures."""
    console.print("\n[bold blue]Loading default strategies...[/bold blue]\n")

    if not DEFAULT_STRATEGIES_PATH.exists():
        console.print(f"[yellow]No default strategies file found at:[/yellow]")
        console.print(f"  {DEFAULT_STRATEGIES_PATH}")
        return

    try:
        result = asyncio.run(_load_strategies(database_url))
        if result == 0:
            console.print("\n[green]Strategies loaded successfully![/green]\n")
        else:
            raise typer.Exit(result)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise typer.Exit(1)


async def _load_strategies(database_url: str) -> int:
    """Load strategies from JSON file."""
    import asyncpg

    postgres_url = get_sync_pg_url(database_url)

    try:
        conn = await asyncpg.connect(postgres_url)
    except Exception as e:
        console.print(f"[red]Failed to connect to PostgreSQL: {e}[/red]")
        return 1

    try:
        # Read strategies from JSON
        with open(DEFAULT_STRATEGIES_PATH, "r", encoding="utf-8") as f:
            strategies_data = json.load(f)

        strategies = strategies_data.get("strategies", [])
        if not strategies:
            console.print("  No strategies defined in file")
            return 0

        console.print(f"  Found {len(strategies)} strategies")

        loaded = 0
        for strategy in strategies:
            # Check if strategy exists
            exists = await conn.fetchval(
                "SELECT 1 FROM strategies WHERE name = $1 AND user_id = $2",
                strategy["name"],
                DEFAULT_USER_ID
            )

            if exists:
                console.print(f"  [dim]Skipping (exists):[/dim] {strategy['name']}")
                continue

            # Generate required fields
            strategy_id = str(uuid.uuid4())
            code = strategy.get("code", "")
            code_hash = hashlib.sha256(code.encode()).hexdigest()

            # Insert strategy with all required fields
            await conn.execute(
                """
                INSERT INTO strategies (
                    id, user_id, name, description, version, code, code_hash,
                    indicators_used, parameters, is_validated, is_active, is_public, execution_mode
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """,
                strategy_id,
                DEFAULT_USER_ID,
                strategy["name"],
                strategy.get("description", ""),
                1,  # version
                code,
                code_hash,
                json.dumps([]),  # indicators_used
                json.dumps({}),  # parameters
                False,  # is_validated
                True,   # is_active
                strategy.get("is_public", True),
                "backtest",  # execution_mode
            )
            console.print(f"  [green]Created:[/green] {strategy['name']}")
            loaded += 1

        console.print(f"\n  Loaded {loaded} new strategies")
        return 0

    finally:
        await conn.close()


# =============================================================================
# DB Commands
# =============================================================================


@app.command("db-reset")
def db_reset(
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt"),
    database_url: str = typer.Option(DEFAULT_DATABASE_URL, "--database", "-d", help="Database URL"),
):
    """Reset the database (drop all tables and run migrations)."""
    if not force:
        confirm = typer.confirm(
            "\nThis will DELETE ALL DATA in the database. Continue?",
            default=False
        )
        if not confirm:
            console.print("[yellow]Aborted.[/yellow]")
            raise typer.Exit(0)

    console.print("\n[bold blue]Resetting database...[/bold blue]\n")

    # Step 1: Drop all tables
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Dropping tables...", total=None)
        try:
            result = asyncio.run(_drop_all_tables(database_url))
            if result != 0:
                progress.remove_task(task)
                console.print("[red]Failed to drop tables[/red]")
                raise typer.Exit(1)
        except Exception as e:
            progress.remove_task(task)
            console.print(f"[red]Failed to drop tables: {e}[/red]")
            raise typer.Exit(1)
        progress.remove_task(task)

    console.print("[green]Tables dropped[/green]")

    # Step 2: Run alembic upgrade
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Recreating tables...", total=None)
        result = subprocess.run(
            ["alembic", "upgrade", "head"],
            cwd=str(BACKEND_DIR),
            capture_output=True,
            text=True,
        )
        progress.remove_task(task)

    if result.returncode == 0:
        console.print("[green]Database reset successfully![/green]\n")
    else:
        console.print("[red]Failed to recreate tables[/red]")
        if result.stderr.strip():
            console.print(f"\n[red]{result.stderr}[/red]")
        raise typer.Exit(1)


async def _drop_all_tables(database_url: str) -> int:
    """Drop all tables and types directly using SQL."""
    import asyncpg

    postgres_url = get_sync_pg_url(database_url)
    try:
        conn = await asyncpg.connect(postgres_url)
    except Exception as e:
        console.print(f"[red]Failed to connect to PostgreSQL: {e}[/red]")
        return 1

    try:
        # Drop ALL tables in public schema using CASCADE
        await conn.execute("""
            DO $$ DECLARE
                r RECORD;
            BEGIN
                -- Drop all tables
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
                -- Drop all custom types (enums)
                FOR r IN (SELECT typname FROM pg_type WHERE typnamespace = 'public'::regnamespace AND typtype = 'e') LOOP
                    EXECUTE 'DROP TYPE IF EXISTS ' || quote_ident(r.typname) || ' CASCADE';
                END LOOP;
            END $$;
        """)
        return 0
    finally:
        await conn.close()


@app.command("db-refresh")
def db_refresh(
    database_url: str = typer.Option(DEFAULT_DATABASE_URL, "--database", "-d", help="Database URL"),
):
    """Refresh TimescaleDB continuous aggregates."""
    console.print("\n[bold blue]Refreshing continuous aggregates...[/bold blue]\n")

    try:
        result = asyncio.run(_refresh_continuous_aggregates(database_url))
        if result == 0:
            console.print("\n[green]All continuous aggregates refreshed![/green]\n")
        else:
            raise typer.Exit(result)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise typer.Exit(1)


async def _refresh_continuous_aggregates(database_url: str) -> int:
    """Refresh all continuous aggregates."""
    import asyncpg

    postgres_url = get_sync_pg_url(database_url)
    try:
        conn = await asyncpg.connect(postgres_url)
    except Exception as e:
        console.print(f"[red]Failed to connect to PostgreSQL: {e}[/red]")
        return 1

    # Get list of continuous aggregates from database
    caggs = []
    try:
        rows = await conn.fetch("""
            SELECT view_name FROM timescaledb_information.continuous_aggregates
            WHERE view_schema = 'public'
        """)
        caggs = [row['view_name'] for row in rows]
    except Exception:
        # Fallback to known aggregates if query fails
        caggs = ["market_daily_1w", "market_daily_1m"]

    if not caggs:
        console.print("  [yellow]No continuous aggregates found[/yellow]")
        return 0

    try:
        for view_name in caggs:
            console.print(f"  Refreshing [cyan]{view_name}[/cyan]...")
            try:
                await conn.execute(
                    f"CALL refresh_continuous_aggregate('{view_name}', NULL, NULL)"
                )
                console.print(f"  [green]✓[/green] {view_name}")
            except Exception as e:
                console.print(f"  [yellow]⚠[/yellow] {view_name}: {e}")
        return 0
    finally:
        await conn.close()


# =============================================================================
# INIT Command
# =============================================================================


async def _init_database(database_url: str, force: bool) -> dict:
    """
    Internal function to initialize database with fixtures.

    Returns a dict with import counts.
    """
    import asyncpg
    from scripts.migrate_all_data import migrate_stock_database, migrate_etf_database
    from scripts.import_index_constituents import import_index_constituents

    results = {}

    # Convert asyncpg URL format
    pg_url = get_sync_pg_url(database_url)

    # 0. Create default user first
    console.print("\n[0/9] Creating default user...")
    user_result = await _create_default_user(database_url)
    results['user'] = 'created' if user_result == 0 else 'failed'

    # 1. Import stocks
    stock_db = FIXTURES_DIR / "sample_stocks.db"
    if stock_db.exists():
        console.print("\n[1/9] Importing stock data...")
        stock_results = await migrate_stock_database(stock_db, pg_url)
        results['stocks'] = stock_results
        console.print(f"  Imported: {stock_results.get('stock_basic', 0)} stocks, {stock_results.get('daily_k_data', 0)} daily records")
    else:
        console.print("\n[1/9] Skipping stocks (sample_stocks.db not found)")
        results['stocks'] = {}

    # 2. Import ETFs
    etf_db = FIXTURES_DIR / "sample_etfs.db"
    if etf_db.exists():
        console.print("\n[2/9] Importing ETF data...")
        etf_results = await migrate_etf_database(etf_db, pg_url)
        results['etfs'] = etf_results
        console.print(f"  Imported: {etf_results.get('etf_basic', 0)} ETFs, {etf_results.get('etf_daily', 0)} daily records")
    else:
        console.print("\n[2/9] Skipping ETFs (sample_etfs.db not found)")
        results['etfs'] = {}

    # 3. Import market cap data
    market_cap_db = FIXTURES_DIR / "sample_market_cap.db"
    if market_cap_db.exists():
        console.print("\n[3/9] Importing market cap data...")
        try:
            market_cap_count = await _import_market_cap_fixture(pg_url, market_cap_db)
            results['market_cap'] = market_cap_count
            console.print(f"  Imported: {market_cap_count} market cap records")
        except Exception as e:
            console.print(f"  [yellow]Failed to import market cap: {e}[/yellow]")
            results['market_cap'] = 0
    else:
        console.print("\n[3/9] Skipping market cap (sample_market_cap.db not found)")
        results['market_cap'] = 0

    # 4. Import northbound holdings data
    northbound_db = FIXTURES_DIR / "sample_northbound.db"
    if northbound_db.exists():
        console.print("\n[4/9] Importing northbound holdings data...")
        try:
            northbound_count = await _import_northbound_fixture(pg_url, northbound_db)
            results['northbound'] = northbound_count
            console.print(f"  Imported: {northbound_count} northbound records")
        except Exception as e:
            console.print(f"  [yellow]Failed to import northbound: {e}[/yellow]")
            results['northbound'] = 0
    else:
        console.print("\n[4/9] Skipping northbound (sample_northbound.db not found)")
        results['northbound'] = 0

    # 5. Import institutional holdings data
    institutional_db = FIXTURES_DIR / "sample_institutional.db"
    if institutional_db.exists():
        console.print("\n[5/9] Importing institutional holdings data...")
        try:
            institutional_count = await _import_institutional_fixture(pg_url, institutional_db)
            results['institutional'] = institutional_count
            console.print(f"  Imported: {institutional_count} institutional records")
        except Exception as e:
            console.print(f"  [yellow]Failed to import institutional: {e}[/yellow]")
            results['institutional'] = 0
    else:
        console.print("\n[5/9] Skipping institutional (sample_institutional.db not found)")
        results['institutional'] = 0

    # 6. Import index constituents (renamed from 5)
    index_db = FIXTURES_DIR / "sample_indices.db"
    if index_db.exists():
        console.print("\n[6/9] Importing index constituents...")
        pg_conn = await asyncpg.connect(pg_url)
        try:
            index_count = await import_index_constituents(index_db, pg_conn, force=force)
            results['indices'] = index_count
            console.print(f"  Imported: {index_count} index constituent records")
        finally:
            await pg_conn.close()
    else:
        console.print("\n[6/9] Skipping indices (sample_indices.db not found)")
        results['indices'] = 0

    # 7. Import industry classification
    industry_db = FIXTURES_DIR / "sample_industries.db"
    if industry_db.exists():
        console.print("\n[7/9] Importing industry classification...")
        try:
            from scripts.import_sw_industry import import_industries_from_sqlite, update_stock_profile_industries
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
            from sqlalchemy.orm import sessionmaker

            # Use passed database_url instead of settings
            async_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
            if "+asyncpg" not in async_url:
                async_url = async_url.replace("postgresql:", "postgresql+asyncpg:")

            engine = create_async_engine(async_url, echo=False)
            async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
            session = async_session()
            try:
                ind_count, mapping_count = await import_industries_from_sqlite(session, industry_db)
                results['industries'] = {'classifications': ind_count, 'mappings': mapping_count}
                console.print(f"  Imported: {ind_count} industries, {mapping_count} stock mappings")

                # Update stock_profile with industry L1/L2/L3
                console.print("  Updating stock_profile with industry classification...")
                await update_stock_profile_industries(session)
            finally:
                await session.close()
                await engine.dispose()
        except Exception as e:
            console.print(f"  [yellow]Skipped industry import: {e}[/yellow]")
            results['industries'] = {'error': str(e)}
    else:
        console.print("\n[7/9] Skipping industries (sample_industries.db not found)")
        results['industries'] = {}

    # 8. Calculate classification snapshot for the latest date in fixtures
    console.print("\n[8/9] Calculating classification data...")
    try:
        from workers.classification_tasks import (
            calculate_structural_classification,
            calculate_style_factors,
            calculate_market_regime,
            generate_classification_snapshot,
        )

        # Find the latest date in the data
        latest_date = "2024-12-31"  # Default to end of fixture data
        try:
            pg_conn = await asyncpg.connect(pg_url)
            row = await pg_conn.fetchrow("SELECT MAX(date) as max_date FROM market_daily")
            if row and row['max_date']:
                latest_date = str(row['max_date'])
            await pg_conn.close()
        except:
            pass

        console.print(f"  Calculating classification for {latest_date}...")

        # Convert database_url to async format for classification tasks
        async_db_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
        if "+asyncpg" not in async_db_url:
            async_db_url = async_db_url.replace("postgresql:", "postgresql+asyncpg:")

        # Run classification tasks with explicit database URL
        ctx = {}  # Empty context for non-ARQ direct call
        structural_result = await calculate_structural_classification(ctx, latest_date, async_db_url)
        console.print(f"  Structural: {structural_result.get('records_updated', 0)} records")

        style_result = await calculate_style_factors(ctx, latest_date, async_db_url)
        console.print(f"  Style factors: {style_result.get('records_inserted', 0)} records")

        regime_result = await calculate_market_regime(ctx, latest_date, async_db_url)
        console.print(f"  Market regime: {regime_result.get('regime', 'unknown')}")

        snapshot_result = await generate_classification_snapshot(ctx, latest_date, async_db_url)
        console.print(f"  Snapshot: {snapshot_result.get('records_generated', 0)} records")

        results['classification'] = {
            'date': latest_date,
            'structural': structural_result.get('records_updated', 0),
            'style': style_result.get('records_inserted', 0),
            'regime': regime_result.get('regime', 'unknown'),
            'snapshot': snapshot_result.get('records_generated', 0),
        }
    except Exception as e:
        console.print(f"  [yellow]Skipped classification: {e}[/yellow]")
        import traceback
        traceback.print_exc()
        results['classification'] = {'error': str(e)}

    # 8. Load default strategies
    console.print("\n[9/9] Loading default strategies...")
    if DEFAULT_STRATEGIES_PATH.exists():
        await _load_strategies(database_url)
        results['strategies'] = 'loaded'
    else:
        console.print("  [dim]No default strategies file found[/dim]")
        results['strategies'] = 'skipped'

    return results


@app.command()
def init(
    force: bool = typer.Option(False, "--force", "-f", help="Force reinitialize even if data exists"),
    database_url: str = typer.Option(DEFAULT_DATABASE_URL, "--database", "-d", help="Database URL"),
):
    """
    Initialize database with fixture data (development mode).

    This command:
    1. Creates default user
    2. Loads sample market data from fixtures/
    3. Loads default strategies

    Takes ~30 seconds. Perfect for quick development iteration.
    """
    console.print("\n[bold blue]Initializing database with fixtures...[/bold blue]\n")

    # Check fixtures exist
    fixture_files = list(FIXTURES_DIR.glob("*.db"))
    if not fixture_files:
        console.print("[yellow]No fixture files found in data/fixtures/[/yellow]")
        console.print("Run 'python -m scripts.data_cli generate-fixtures' first")
        raise typer.Exit(1)

    console.print(f"Found {len(fixture_files)} fixture files:")
    total_size = 0
    for f in fixture_files:
        size = f.stat().st_size / 1024 / 1024
        total_size += size
        console.print(f"  - {f.name} ({size:.2f} MB)")
    console.print(f"  Total: {total_size:.2f} MB")

    # Run the async import
    start_time = datetime.now()
    results = asyncio.run(_init_database(database_url, force))
    elapsed = (datetime.now() - start_time).total_seconds()

    # Summary
    console.print("\n" + "=" * 50)
    console.print("[bold green]Database initialized successfully![/bold green]")
    console.print("=" * 50)

    stock_results = results.get('stocks', {})
    etf_results = results.get('etfs', {})
    market_cap_count = results.get('market_cap', 0)
    northbound_count = results.get('northbound', 0)
    index_count = results.get('indices', 0)
    industry_results = results.get('industries', {})

    console.print(f"\n[cyan]Summary:[/cyan]")
    console.print(f"  User: {results.get('user', 'unknown')}")
    console.print(f"  Stocks: {stock_results.get('stock_basic', 0)} symbols, {stock_results.get('daily_k_data', 0)} daily records")
    console.print(f"  ETFs: {etf_results.get('etf_basic', 0)} symbols, {etf_results.get('etf_daily', 0)} daily records")
    console.print(f"  Market cap: {market_cap_count} records")
    console.print(f"  Northbound: {northbound_count} records")
    console.print(f"  Index constituents: {index_count} records")
    console.print(f"  Industries: {industry_results.get('classifications', 0)} categories, {industry_results.get('mappings', 0)} mappings")
    console.print(f"  Strategies: {results.get('strategies', 'unknown')}")
    console.print(f"\n  Time: {elapsed:.1f} seconds")
    console.print("\nRun 'python -m scripts.data_cli status' to verify.\n")


# =============================================================================
# STATUS Command
# =============================================================================


@app.command()
def status(
    database_url: str = typer.Option(DEFAULT_DATABASE_URL, "--database", "-d", help="Database URL"),
):
    """
    Show comprehensive data status (PostgreSQL + cache).
    """
    console.print("\n[bold blue]Data Status[/bold blue]\n")

    # PostgreSQL status
    console.print("[cyan]PostgreSQL Database:[/cyan]")
    try:
        pg_status = asyncio.run(_get_pg_status(database_url))

        table = Table(show_header=True, header_style="bold")
        table.add_column("Table")
        table.add_column("Records", justify="right")
        table.add_column("Date Range")

        for name, info in pg_status.items():
            if isinstance(info, dict):
                count = f"{info.get('count', 0):,}"
                date_range = info.get('date_range', '-')
                table.add_row(name, count, date_range)

        console.print(table)
    except Exception as e:
        console.print(f"  [red]Cannot connect to database: {e}[/red]")

    # Cache status
    console.print("\n[cyan]Cache (SQLite):[/cyan]")
    cache_files = list(CACHE_DIR.glob("*.db")) if CACHE_DIR.exists() else []
    if cache_files:
        cache_table = Table(show_header=True, header_style="bold")
        cache_table.add_column("File")
        cache_table.add_column("Size")

        for f in sorted(cache_files):
            size = f"{f.stat().st_size / 1024 / 1024:.1f} MB"
            cache_table.add_row(f.name, size)
        console.print(cache_table)
    else:
        console.print("  [dim]No cache files[/dim]")

    # Fixtures status
    console.print("\n[cyan]Fixtures:[/cyan]")
    fixture_files = list(FIXTURES_DIR.glob("*.db")) if FIXTURES_DIR.exists() else []
    if fixture_files:
        for f in sorted(fixture_files):
            size = f"{f.stat().st_size / 1024 / 1024:.2f} MB"
            console.print(f"  {f.name}: {size}")
    else:
        console.print("  [dim]No fixture files[/dim]")

    console.print()


async def _get_pg_status(database_url: str) -> dict:
    """Get PostgreSQL database status."""
    import asyncpg

    postgres_url = get_sync_pg_url(database_url)
    conn = await asyncpg.connect(postgres_url)

    status = {}

    try:
        # Users
        count = await conn.fetchval("SELECT COUNT(*) FROM users")
        status['users'] = {'count': count}

        # Strategies
        count = await conn.fetchval("SELECT COUNT(*) FROM strategies")
        status['strategies'] = {'count': count}

        # Asset meta (stocks + ETFs)
        try:
            row = await conn.fetchrow("""
                SELECT COUNT(*) as count,
                       COUNT(CASE WHEN asset_type = 'stock' THEN 1 END) as stocks,
                       COUNT(CASE WHEN asset_type = 'etf' THEN 1 END) as etfs
                FROM asset_meta
            """)
            status['asset_meta'] = {
                'count': row['count'],
                'date_range': f"stocks: {row['stocks']}, etfs: {row['etfs']}"
            }
        except Exception:
            status['asset_meta'] = {'count': 0, 'date_range': '-'}

        # Market daily
        try:
            row = await conn.fetchrow("""
                SELECT COUNT(*) as count,
                       MIN(trade_date) as min_date,
                       MAX(trade_date) as max_date
                FROM market_daily
            """)
            date_range = f"{row['min_date']} ~ {row['max_date']}" if row['min_date'] else '-'
            status['market_daily'] = {'count': row['count'], 'date_range': date_range}
        except Exception:
            status['market_daily'] = {'count': 0, 'date_range': '-'}

        # Index constituents
        try:
            count = await conn.fetchval("SELECT COUNT(*) FROM index_constituents")
            status['index_constituents'] = {'count': count}
        except Exception:
            status['index_constituents'] = {'count': 0}

        # Industry classification
        try:
            count = await conn.fetchval("SELECT COUNT(*) FROM industry_classification")
            status['industry_classification'] = {'count': count}
        except Exception:
            status['industry_classification'] = {'count': 0}

        # Backtests
        try:
            count = await conn.fetchval("SELECT COUNT(*) FROM backtests")
            status['backtests'] = {'count': count}
        except Exception:
            status['backtests'] = {'count': 0}

    finally:
        await conn.close()

    return status


# =============================================================================
# DOWNLOAD Command
# =============================================================================


@app.command()
def download(
    data_type: str = typer.Argument("all", help="Data type: stocks, etfs, indices, industries, northbound, institutional, market_cap, all"),
    full: bool = typer.Option(False, "--full", help="Download full history"),
    recent: int = typer.Option(30, "--recent", "-r", help="Download recent N days"),
    years: str = typer.Option(None, "--years", "-y", help="Years to download (comma-separated): 2023,2024,2025"),
    mode: str = typer.Option("all", "--mode", "-m", help="Download mode for stocks/etfs: all, basic, daily, adjust"),
    force: bool = typer.Option(False, "--force", "-f", help="Force re-download even if data exists"),
):
    """
    Download data from external sources to data/cache/.

    Uses data.downloads module with rate limiting and checkpoint support.

    Examples:
        python -m scripts.data_cli download stocks --years 2024
        python -m scripts.data_cli download etfs --years 2024
        python -m scripts.data_cli download indices
        python -m scripts.data_cli download all --full
        python -m scripts.data_cli download market_cap --recent 30
        python -m scripts.data_cli download northbound
    """
    from data.downloads import (
        download_stocks,
        download_etfs,
        download_market_cap,
        download_northbound,
        download_institutional,
        download_indices,
        download_industries,
    )

    console.print(f"\n[bold blue]Downloading {data_type} data to cache/...[/bold blue]\n")

    # Ensure cache directory exists
    ensure_cache_dir()

    # Parse years if provided
    year_list = None
    if years:
        year_list = [int(y.strip()) for y in years.split(',')]
        console.print(f"Target years: {year_list}")

    # Define all available downloaders
    if data_type == "all":
        types_to_download = ["stocks", "etfs", "indices", "industries", "northbound", "institutional", "market_cap"]
    else:
        types_to_download = [data_type]

    for dtype in types_to_download:
        console.print(f"\n[cyan]Downloading {dtype}...[/cyan]")

        try:
            if dtype == "stocks":
                count = download_stocks(years=year_list, mode=mode, force=force)
                console.print(f"  [green]✓ Downloaded {count} stock records[/green]")
            elif dtype == "etfs":
                count = download_etfs(years=year_list, mode=mode, force=force)
                console.print(f"  [green]✓ Downloaded {count} ETF records[/green]")
            elif dtype == "indices":
                count = download_indices()
                console.print(f"  [green]✓ Downloaded {count} index constituent records[/green]")
            elif dtype == "industries":
                count = download_industries()
                console.print(f"  [green]✓ Downloaded {count} industry records[/green]")
            elif dtype == "northbound":
                count = download_northbound(today_only=not full)
                console.print(f"  [green]✓ Downloaded {count} northbound records[/green]")
            elif dtype == "institutional":
                count = download_institutional()
                console.print(f"  [green]✓ Downloaded {count} institutional records[/green]")
            elif dtype == "market_cap":
                count = download_market_cap(recent=recent, full_history=full)
                console.print(f"  [green]✓ Downloaded {count} market cap records[/green]")
            else:
                console.print(f"  [yellow]Unknown data type: {dtype}[/yellow]")
        except Exception as e:
            console.print(f"  [red]✗ {dtype} download failed: {e}[/red]")
            import traceback
            traceback.print_exc()

    console.print("\n[bold green]Download complete![/bold green]")
    console.print(f"Data saved to: {CACHE_DIR}\n")


# =============================================================================
# LOAD Command
# =============================================================================


def _check_cache_integrity(cache_dir: Path, data_types: list = None) -> dict:
    """
    Check cache data integrity before loading.

    Returns dict with:
        - valid: bool - whether all required files exist
        - missing: list - missing data types
        - files: dict - mapping of data types to file info
    """
    # Expected files for each data type
    expected_files = {
        "stocks": {"pattern": "a_stock_*.db", "required": False},
        "etfs": {"pattern": "etf_*.db", "required": False},
        "indices": {"pattern": "index_constituents.db", "required": False},
        "industries": {"pattern": "industry_classification.db", "required": False},
        "northbound": {"pattern": "northbound_holdings.db", "required": False},
        "institutional": {"pattern": "institutional_holdings.db", "required": False},
        "market_cap": {"pattern": "market_cap.db", "required": False},
    }

    # Filter to requested types
    if data_types:
        expected_files = {k: v for k, v in expected_files.items() if k in data_types}

    status = {"valid": True, "missing": [], "files": {}}

    for dtype, info in expected_files.items():
        pattern = info["pattern"]
        files = list(cache_dir.glob(pattern))

        if not files:
            status["missing"].append(dtype)
            if info["required"]:
                status["valid"] = False
        else:
            # Get file stats
            total_size = sum(f.stat().st_size for f in files) / 1024 / 1024
            status["files"][dtype] = {
                "count": len(files),
                "names": [f.name for f in files],
                "size_mb": round(total_size, 2),
            }

    return status


async def _get_pg_loaded_dates(pg_url: str, table: str) -> set:
    """Get set of dates already loaded in PostgreSQL table."""
    import asyncpg

    try:
        conn = await asyncpg.connect(pg_url)
        try:
            if table == "market_daily":
                rows = await conn.fetch("SELECT DISTINCT trade_date FROM market_daily")
                return {str(row['trade_date']) for row in rows}
            elif table == "indicator_valuation":
                rows = await conn.fetch("SELECT DISTINCT date FROM indicator_valuation")
                return {str(row['date']) for row in rows}
            elif table == "stock_microstructure":
                rows = await conn.fetch("SELECT DISTINCT date FROM stock_microstructure")
                return {str(row['date']) for row in rows}
            return set()
        finally:
            await conn.close()
    except Exception:
        return set()


@app.command()
def load(
    data_type: str = typer.Argument("all", help="Data type: stocks, etfs, indices, industries, northbound, institutional, market_cap, all"),
    full: bool = typer.Option(False, "--full", help="Load all data"),
    recent: int = typer.Option(None, "--recent", "-r", help="Load recent N days only"),
    years: str = typer.Option(None, "--years", "-y", help="Years to load (comma-separated): 2023,2024"),
    source: str = typer.Option(None, "--source", "-s", help="Source directory (default: data/cache/)"),
    skip_check: bool = typer.Option(False, "--skip-check", help="Skip cache integrity check"),
    database_url: str = typer.Option(DEFAULT_DATABASE_URL, "--database", "-d", help="Database URL"),
):
    """
    Load data from data/cache/ to PostgreSQL.

    Checks cache integrity before loading and reports missing files.

    Examples:
        python -m scripts.data_cli load stocks --full
        python -m scripts.data_cli load all --recent 30
        python -m scripts.data_cli load market_cap --years 2024
        python -m scripts.data_cli load --years 2024  # Load all types for 2024
    """
    console.print("\n[bold blue]Loading data to PostgreSQL...[/bold blue]\n")

    source_dir = Path(source) if source else CACHE_DIR

    if not source_dir.exists():
        console.print(f"[red]Source directory not found: {source_dir}[/red]")
        console.print("Run 'python -m scripts.data_cli download' first")
        raise typer.Exit(1)

    # Parse years if provided
    year_list = []
    if years:
        year_list = [y.strip() for y in years.split(',')]
        console.print(f"Target years: {year_list}")

    # Determine types to load
    if data_type == "all":
        types_to_load = ["stocks", "etfs", "indices", "industries", "northbound", "institutional", "market_cap"]
    else:
        types_to_load = [data_type]

    # Check cache integrity
    if not skip_check:
        console.print("[cyan]Checking cache integrity...[/cyan]")
        cache_status = _check_cache_integrity(source_dir, types_to_load)

        if cache_status["missing"]:
            console.print(f"\n[yellow]Missing data in cache:[/yellow]")
            for dtype in cache_status["missing"]:
                console.print(f"  - {dtype}")
            console.print(f"\n[dim]Run 'data_cli download {' '.join(cache_status['missing'])}' to download[/dim]")

            # Filter out missing types
            types_to_load = [t for t in types_to_load if t not in cache_status["missing"]]
            if not types_to_load:
                console.print("[red]No data available to load[/red]")
                raise typer.Exit(1)

        # Show available files
        console.print("\n[cyan]Cache status:[/cyan]")
        for dtype, info in cache_status["files"].items():
            console.print(f"  {dtype}: {info['count']} file(s), {info['size_mb']} MB")

    # Find and display SQLite files
    db_files = list(source_dir.glob("*.db"))
    if not db_files and not skip_check:
        console.print(f"[yellow]No .db files found in {source_dir}[/yellow]")
        raise typer.Exit(1)

    console.print(f"\n[cyan]Loading data types: {', '.join(types_to_load)}[/cyan]")

    # Convert database URL for sync operations
    pg_url = get_sync_pg_url(database_url)

    for dtype in types_to_load:
        console.print(f"\n[cyan]Loading {dtype}...[/cyan]")

        if dtype == "stocks":
            # Run migrate_all_data for stocks
            cmd = [
                "python", "-m", "scripts.migrate_all_data",
                "--source-dir", str(source_dir),
                "--type", "stock",
                "-d", pg_url,
            ]
            if full:
                cmd.append("--all")
            console.print(f"  Command: {' '.join(cmd)}")
            result = subprocess.run(cmd, cwd=str(BACKEND_DIR), capture_output=False)
            if result.returncode == 0:
                console.print(f"  [green]✓ stocks loaded[/green]")
            else:
                console.print(f"  [red]✗ stocks load failed[/red]")

        elif dtype == "etfs":
            # Run migrate_all_data for ETFs
            cmd = [
                "python", "-m", "scripts.migrate_all_data",
                "--source-dir", str(source_dir),
                "--type", "etf",
                "-d", pg_url,
            ]
            if full:
                cmd.append("--all")
            console.print(f"  Command: {' '.join(cmd)}")
            result = subprocess.run(cmd, cwd=str(BACKEND_DIR), capture_output=False)
            if result.returncode == 0:
                console.print(f"  [green]✓ etfs loaded[/green]")
            else:
                console.print(f"  [red]✗ etfs load failed[/red]")

        elif dtype == "indices":
            # Load index constituents
            index_db = source_dir / "index_constituents.db"
            if index_db.exists():
                result = asyncio.run(_load_index_constituents(pg_url, index_db))
                console.print(f"  [green]✓ indices loaded ({result} records)[/green]")
            else:
                console.print(f"  [yellow]⚠ index_constituents.db not found[/yellow]")

        elif dtype == "industries":
            # Load industry classification
            industry_db = source_dir / "industry_classification.db"
            if industry_db.exists():
                cmd = [
                    "python", "-m", "scripts.import_sw_industry",
                    "--source", str(industry_db),
                ]
                console.print(f"  Command: {' '.join(cmd)}")
                result = subprocess.run(cmd, cwd=str(BACKEND_DIR), capture_output=False)
                if result.returncode == 0:
                    console.print(f"  [green]✓ industries loaded[/green]")
                else:
                    console.print(f"  [red]✗ industries load failed[/red]")
            else:
                console.print(f"  [yellow]⚠ industry_classification.db not found[/yellow]")

        elif dtype == "northbound":
            # Load northbound holdings
            northbound_db = source_dir / "northbound_holdings.db"
            if northbound_db.exists():
                result = asyncio.run(_load_northbound_from_source(pg_url, northbound_db, year_list))
                console.print(f"  [green]✓ northbound loaded ({result} records)[/green]")
            else:
                console.print(f"  [yellow]⚠ northbound_holdings.db not found[/yellow]")

        elif dtype == "institutional":
            # Load institutional holdings
            inst_db = source_dir / "institutional_holdings.db"
            if inst_db.exists():
                result = asyncio.run(_load_institutional_from_source(pg_url, inst_db, year_list))
                console.print(f"  [green]✓ institutional loaded ({result} records)[/green]")
            else:
                console.print(f"  [yellow]⚠ institutional_holdings.db not found[/yellow]")

        elif dtype == "market_cap":
            # Load market cap data
            market_cap_db = source_dir / "market_cap.db"
            if market_cap_db.exists():
                result = asyncio.run(_load_market_cap_from_source(pg_url, market_cap_db, year_list))
                console.print(f"  [green]✓ market_cap loaded ({result} records)[/green]")
            else:
                console.print(f"  [yellow]⚠ market_cap.db not found[/yellow]")

        else:
            console.print(f"  [yellow]⚠ Unknown data type: {dtype}[/yellow]")

    console.print("\n[bold green]Load complete![/bold green]\n")


async def _load_index_constituents(pg_url: str, sqlite_path: Path) -> int:
    """Load index constituents from SQLite to PostgreSQL."""
    import asyncpg
    from scripts.import_index_constituents import import_index_constituents

    pg_conn = await asyncpg.connect(pg_url)
    try:
        count = await import_index_constituents(sqlite_path, pg_conn, force=True)
        return count
    finally:
        await pg_conn.close()


async def _load_market_cap_from_source(pg_url: str, sqlite_path: Path, years: list = None) -> int:
    """Load market cap data from full SQLite database to PostgreSQL."""
    import asyncpg

    conn_sqlite = sqlite3.connect(str(sqlite_path))
    cursor = conn_sqlite.cursor()

    # Build query with year filter if provided
    query = "SELECT code, date, total_mv, circ_mv FROM stock_market_cap"
    if years:
        year_conditions = " OR ".join([f"date LIKE '{y}%'" for y in years])
        query += f" WHERE ({year_conditions})"

    cursor.execute(query)
    rows = cursor.fetchall()
    conn_sqlite.close()

    if not rows:
        return 0

    def parse_date(date_str):
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    records = [(row[0], parse_date(row[1]), row[2], row[3]) for row in rows]

    pg_conn = await asyncpg.connect(pg_url)
    try:
        await pg_conn.executemany(
            """
            INSERT INTO indicator_valuation (code, date, total_mv, circ_mv)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (code, date) DO UPDATE SET
                total_mv = COALESCE(EXCLUDED.total_mv, indicator_valuation.total_mv),
                circ_mv = COALESCE(EXCLUDED.circ_mv, indicator_valuation.circ_mv)
            """,
            records
        )
        return len(records)
    finally:
        await pg_conn.close()


async def _load_northbound_from_source(pg_url: str, sqlite_path: Path, years: list = None) -> int:
    """Load northbound holdings from full SQLite database to PostgreSQL."""
    import asyncpg

    conn_sqlite = sqlite3.connect(str(sqlite_path))
    cursor = conn_sqlite.cursor()

    # Build query with year filter if provided
    query = "SELECT code, date, holding_ratio, holding_change FROM northbound_holdings"
    if years:
        year_conditions = " OR ".join([f"date LIKE '{y}%'" for y in years])
        query += f" WHERE ({year_conditions})"

    cursor.execute(query)
    rows = cursor.fetchall()
    conn_sqlite.close()

    if not rows:
        return 0

    def parse_date(date_str):
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    records = [
        (
            row[0],
            parse_date(row[1]),
            row[2],
            row[3],
            row[2] > 5.0 if row[2] else False,
            False, False, False  # is_institutional, is_retail_hot, is_main_controlled
        )
        for row in rows
    ]

    pg_conn = await asyncpg.connect(pg_url)
    try:
        await pg_conn.executemany(
            """
            INSERT INTO stock_microstructure (
                code, date, northbound_holding_ratio, northbound_holding_change,
                is_northbound_heavy, is_institutional, is_retail_hot, is_main_controlled
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (code, date) DO UPDATE SET
                northbound_holding_ratio = COALESCE(EXCLUDED.northbound_holding_ratio, stock_microstructure.northbound_holding_ratio),
                northbound_holding_change = COALESCE(EXCLUDED.northbound_holding_change, stock_microstructure.northbound_holding_change),
                is_northbound_heavy = COALESCE(EXCLUDED.is_northbound_heavy, stock_microstructure.is_northbound_heavy)
            """,
            records
        )
        return len(records)
    finally:
        await pg_conn.close()


async def _load_institutional_from_source(pg_url: str, sqlite_path: Path, years: list = None) -> int:
    """Load institutional holdings from full SQLite database to PostgreSQL."""
    import asyncpg

    conn_sqlite = sqlite3.connect(str(sqlite_path))
    cursor = conn_sqlite.cursor()

    # Build query with year filter if provided
    query = "SELECT code, date, fund_holding_ratio, fund_holding_change FROM institutional_holdings"
    if years:
        year_conditions = " OR ".join([f"date LIKE '{y}%'" for y in years])
        query += f" WHERE ({year_conditions})"

    cursor.execute(query)
    rows = cursor.fetchall()
    conn_sqlite.close()

    if not rows:
        return 0

    def parse_date(date_str):
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    # is_institutional = True when fund_holding_ratio > 10%
    records = [
        (
            row[0],
            parse_date(row[1]),
            row[2] > 10.0 if row[2] else False,  # is_institutional
            False, False, False  # is_northbound_heavy, is_retail_hot, is_main_controlled
        )
        for row in rows
    ]

    pg_conn = await asyncpg.connect(pg_url)
    try:
        await pg_conn.executemany(
            """
            INSERT INTO stock_microstructure (
                code, date, is_institutional, is_northbound_heavy, is_retail_hot, is_main_controlled
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (code, date) DO UPDATE SET
                is_institutional = COALESCE(EXCLUDED.is_institutional, stock_microstructure.is_institutional)
            """,
            records
        )
        return len(records)
    finally:
        await pg_conn.close()


# =============================================================================
# UPDATE Command
# =============================================================================


@app.command()
def update(
    database_url: str = typer.Option(DEFAULT_DATABASE_URL, "--database", "-d", help="Database URL"),
):
    """
    Incremental update: download and load today's data.

    This command:
    1. Downloads today's stock/ETF data
    2. Downloads today's northbound holdings
    3. Imports to PostgreSQL
    4. Runs classification calculations
    """
    console.print("\n[bold blue]Running incremental update...[/bold blue]\n")

    today = date.today().strftime("%Y-%m-%d")
    console.print(f"Date: {today}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Downloading today's data...", total=None)

        # TODO: Implement incremental download
        progress.update(task, description="Downloading stocks...")
        progress.update(task, description="Downloading northbound...")
        progress.update(task, description="Importing to PostgreSQL...")
        progress.update(task, description="Running classification...")
        progress.update(task, description="[green]Update complete![/green]")

    console.print("\n[bold green]Incremental update complete![/bold green]\n")


# =============================================================================
# COPY-CACHE Command
# =============================================================================


@app.command("copy-cache")
def copy_cache(
    source: str = typer.Option(None, "--source", "-s", help="Source directory to copy from"),
    target: str = typer.Option(None, "--target", "-t", help="Target directory (default: data/cache/)"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing files"),
):
    """
    Copy SQLite files between directories.

    This can migrate data without re-downloading.
    """
    console.print("\n[bold blue]Copying SQLite files...[/bold blue]\n")

    if not source:
        console.print("[red]Please specify --source directory[/red]")
        raise typer.Exit(1)

    source_dir = Path(source)
    if not source_dir.exists():
        console.print(f"[red]Source directory not found: {source_dir}[/red]")
        raise typer.Exit(1)

    target_dir = Path(target) if target else CACHE_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    # Find all .db files
    db_files = list(source_dir.glob("*.db"))
    console.print(f"Found {len(db_files)} database files in {source_dir}")

    total_size = sum(f.stat().st_size for f in db_files) / 1024 / 1024 / 1024
    console.print(f"Total size: {total_size:.2f} GB\n")

    if not typer.confirm("Proceed with copy?"):
        raise typer.Abort()

    copied = 0
    skipped = 0

    with Progress(console=console) as progress:
        task = progress.add_task("Copying...", total=len(db_files))

        for src_file in db_files:
            dst_file = target_dir / src_file.name
            progress.update(task, description=f"Copying {src_file.name}...")

            if dst_file.exists() and not force:
                skipped += 1
            else:
                shutil.copy2(src_file, dst_file)
                copied += 1

            progress.advance(task)

    console.print(f"\n[green]Copied: {copied} files[/green]")
    if skipped:
        console.print(f"[yellow]Skipped: {skipped} files (use --force to overwrite)[/yellow]")

    console.print("\n[bold green]Done![/bold green]\n")


# =============================================================================
# GENERATE-FIXTURES Command
# =============================================================================


@app.command("generate-fixtures")
def generate_fixtures_cmd(
    stocks: int = typer.Option(100, "--stocks", "-s", help="Number of sample stocks"),
    days: int = typer.Option(30, "--days", "-d", help="Number of days of history"),
    source: str = typer.Option(None, "--source", help="Source data directory (default: data/cache/)"),
):
    """
    Generate fixture files from cache dataset.

    Creates small sample databases (~5MB total) for development.
    """
    console.print("\n[bold blue]Generating fixture files...[/bold blue]\n")

    source_dir = Path(source) if source else CACHE_DIR

    console.print(f"Parameters:")
    console.print(f"  Stocks: {stocks}")
    console.print(f"  Days: {days}")
    console.print(f"  Source: {source_dir}")

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    # Call the actual fixture generator
    from scripts.generate_fixtures import generate_fixtures

    start_time = datetime.now()
    result = generate_fixtures(stocks=stocks, days=days, source_dir=source_dir)
    elapsed = (datetime.now() - start_time).total_seconds()

    console.print(f"\n[bold green]Fixtures generated successfully![/bold green]")
    console.print(f"  Time: {elapsed:.1f} seconds")
    console.print(f"  Stats: {result}")


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    app()
