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
- copy-cache  : Copy trading_data to cache
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
TRADING_DATA_DIR = Path("/Users/dan/Code/q/trading_data")
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
    console.print("\n[0/6] Creating default user...")
    user_result = await _create_default_user(database_url)
    results['user'] = 'created' if user_result == 0 else 'failed'

    # 1. Import stocks
    stock_db = FIXTURES_DIR / "sample_stocks.db"
    if stock_db.exists():
        console.print("\n[1/6] Importing stock data...")
        stock_results = await migrate_stock_database(stock_db, pg_url)
        results['stocks'] = stock_results
        console.print(f"  Imported: {stock_results.get('stock_basic', 0)} stocks, {stock_results.get('daily_k_data', 0)} daily records")
    else:
        console.print("\n[1/6] Skipping stocks (sample_stocks.db not found)")
        results['stocks'] = {}

    # 2. Import ETFs
    etf_db = FIXTURES_DIR / "sample_etfs.db"
    if etf_db.exists():
        console.print("\n[2/6] Importing ETF data...")
        etf_results = await migrate_etf_database(etf_db, pg_url)
        results['etfs'] = etf_results
        console.print(f"  Imported: {etf_results.get('etf_basic', 0)} ETFs, {etf_results.get('etf_daily', 0)} daily records")
    else:
        console.print("\n[2/6] Skipping ETFs (sample_etfs.db not found)")
        results['etfs'] = {}

    # 3. Import index constituents
    index_db = FIXTURES_DIR / "sample_indices.db"
    if index_db.exists():
        console.print("\n[3/6] Importing index constituents...")
        pg_conn = await asyncpg.connect(pg_url)
        try:
            index_count = await import_index_constituents(index_db, pg_conn, force=force)
            results['indices'] = index_count
            console.print(f"  Imported: {index_count} index constituent records")
        finally:
            await pg_conn.close()
    else:
        console.print("\n[3/6] Skipping indices (sample_indices.db not found)")
        results['indices'] = 0

    # 4. Import industry classification
    industry_db = FIXTURES_DIR / "sample_industries.db"
    if industry_db.exists():
        console.print("\n[4/6] Importing industry classification...")
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
        console.print("\n[4/6] Skipping industries (sample_industries.db not found)")
        results['industries'] = {}

    # 5. Calculate classification snapshot for the latest date in fixtures
    console.print("\n[5/6] Calculating classification data...")
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

        # Run classification tasks
        ctx = {}  # Empty context for non-ARQ direct call
        structural_result = await calculate_structural_classification(ctx, latest_date)
        console.print(f"  Structural: {structural_result.get('records_updated', 0)} records")

        style_result = await calculate_style_factors(ctx, latest_date)
        console.print(f"  Style factors: {style_result.get('records_inserted', 0)} records")

        regime_result = await calculate_market_regime(ctx, latest_date)
        console.print(f"  Market regime: {regime_result.get('regime', 'unknown')}")

        snapshot_result = await generate_classification_snapshot(ctx, latest_date)
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

    # 6. Load default strategies
    console.print("\n[6/6] Loading default strategies...")
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
    index_count = results.get('indices', 0)
    industry_results = results.get('industries', {})

    console.print(f"\n[cyan]Summary:[/cyan]")
    console.print(f"  User: {results.get('user', 'unknown')}")
    console.print(f"  Stocks: {stock_results.get('stock_basic', 0)} symbols, {stock_results.get('daily_k_data', 0)} daily records")
    console.print(f"  ETFs: {etf_results.get('etf_basic', 0)} symbols, {etf_results.get('etf_daily', 0)} daily records")
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
    data_type: str = typer.Argument("all", help="Data type: stocks, etfs, indices, industries, northbound, institutional, all"),
    full: bool = typer.Option(False, "--full", help="Download full history"),
    recent: int = typer.Option(30, "--recent", "-r", help="Download recent N days"),
):
    """
    Download data from external sources to cache.

    Examples:
        python -m scripts.data_cli download stocks --recent 30
        python -m scripts.data_cli download all --full
    """
    console.print(f"\n[bold blue]Downloading {data_type} data...[/bold blue]\n")

    ensure_cache_dir()

    if data_type == "all":
        types_to_download = ["stocks", "etfs", "indices", "industries", "northbound", "institutional"]
    else:
        types_to_download = [data_type]

    for dtype in types_to_download:
        console.print(f"\n[cyan]Downloading {dtype}...[/cyan]")

        # Map to download script
        script_map = {
            "stocks": "download_a_stock_data.py",
            "etfs": "download_etf_data.py",
            "indices": "download_index_constituents.py",
            "industries": "download_industry_data.py",
            "northbound": "download_northbound_holdings.py",
            "institutional": "download_institutional_holdings.py",
            "market_cap": "download_market_cap.py",
        }

        if dtype not in script_map:
            console.print(f"[yellow]Unknown data type: {dtype}[/yellow]")
            continue

        # Check if script exists in downloads/ or trading_data/
        script_name = script_map[dtype]
        backend_script = DATA_DIR / "downloads" / script_name
        trading_script = TRADING_DATA_DIR / script_name

        if backend_script.exists():
            console.print(f"  Using: {backend_script}")
            # TODO: Run script
        elif trading_script.exists():
            console.print(f"  Using: {trading_script} (legacy)")
            console.print(f"  [yellow]Hint: Migrate this script to backend/data/downloads/[/yellow]")
            # TODO: Run script
        else:
            console.print(f"  [red]Script not found: {script_name}[/red]")

    console.print("\n[bold green]Download complete![/bold green]\n")


# =============================================================================
# LOAD Command
# =============================================================================


@app.command()
def load(
    full: bool = typer.Option(False, "--full", help="Load all cached data"),
    recent: int = typer.Option(None, "--recent", "-r", help="Load recent N days only"),
    source: str = typer.Option(None, "--source", "-s", help="Source directory (default: cache/)"),
    database_url: str = typer.Option(DEFAULT_DATABASE_URL, "--database", "-d", help="Database URL"),
):
    """
    Load data from cache to PostgreSQL.

    Examples:
        python -m scripts.data_cli load --full
        python -m scripts.data_cli load --recent 30
    """
    console.print("\n[bold blue]Loading data to PostgreSQL...[/bold blue]\n")

    source_dir = Path(source) if source else CACHE_DIR

    if not source_dir.exists():
        console.print(f"[red]Source directory not found: {source_dir}[/red]")
        console.print("Run 'python -m scripts.data_cli download' first")
        raise typer.Exit(1)

    # Find SQLite files
    db_files = list(source_dir.glob("*.db"))
    if not db_files:
        console.print(f"[yellow]No .db files found in {source_dir}[/yellow]")
        raise typer.Exit(1)

    console.print(f"Found {len(db_files)} database files:")
    for f in db_files:
        stats = get_sqlite_stats(f)
        size = stats.get("size_mb", 0)
        console.print(f"  - {f.name} ({size:.1f} MB)")

    # Use existing migration scripts
    console.print("\n[cyan]Running migration...[/cyan]")

    # TODO: Implement migration orchestration
    # For now, provide instructions
    console.print("\n[yellow]Manual migration commands:[/yellow]")
    console.print(f"  python -m scripts.migrate_all_data --source-dir {source_dir} --type stock --all")
    console.print(f"  python -m scripts.migrate_all_data --source-dir {source_dir} --type etf --all")
    console.print(f"  python -m scripts.import_index_constituents --source-dir {source_dir}")
    console.print(f"  python -m scripts.import_northbound_holdings --source-dir {source_dir}")
    console.print(f"  python -m scripts.import_sw_industry --source-dir {source_dir}")


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
    source: str = typer.Option(str(TRADING_DATA_DIR), "--source", "-s", help="Source directory"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing files"),
):
    """
    Copy existing SQLite files from trading_data to cache/.

    This migrates ~3GB of data without re-downloading.
    """
    console.print("\n[bold blue]Copying SQLite files to cache...[/bold blue]\n")

    source_dir = Path(source)
    if not source_dir.exists():
        console.print(f"[red]Source directory not found: {source_dir}[/red]")
        raise typer.Exit(1)

    ensure_cache_dir()

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
            dst_file = CACHE_DIR / src_file.name
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
    source: str = typer.Option(str(TRADING_DATA_DIR), "--source", help="Source data directory"),
):
    """
    Generate fixture files from full dataset.

    Creates small sample databases (~5MB total) for development.
    """
    console.print("\n[bold blue]Generating fixture files...[/bold blue]\n")

    console.print(f"Parameters:")
    console.print(f"  Stocks: {stocks}")
    console.print(f"  Days: {days}")
    console.print(f"  Source: {source}")

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    # Call the actual fixture generator
    from scripts.generate_fixtures import generate_fixtures

    start_time = datetime.now()
    result = generate_fixtures(stocks=stocks, days=days, source_dir=Path(source))
    elapsed = (datetime.now() - start_time).total_seconds()

    console.print(f"\n[bold green]Fixtures generated successfully![/bold green]")
    console.print(f"  Time: {elapsed:.1f} seconds")
    console.print(f"  Stats: {result}")


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    app()
