"""Seed data CLI commands."""

import asyncio
import hashlib
import json
import os
import secrets
import sqlite3
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

app = typer.Typer(help="Seed data commands")
console = Console()

# Default paths
CLI_DIR = Path(__file__).parent
BACKEND_DIR = CLI_DIR.parent.parent
EXAMPLES_DIR = BACKEND_DIR / "examples" / "data"
SAMPLE_DATA_PATH = EXAMPLES_DIR / "sample_data.db"
DEFAULT_STRATEGIES_PATH = EXAMPLES_DIR / "default_strategies.json"

# Default system user
DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000001"
DEFAULT_USER_EMAIL = "system@localhost"
DEFAULT_USER_NAME = "system"


def get_database_url() -> str:
    """Get PostgreSQL connection URL from environment."""
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://quant:quant_dev_password@localhost:5432/quantdb"
    )
    # Convert async URL to sync for asyncpg
    return url.replace("+asyncpg", "").replace("postgresql+asyncpg", "postgresql")


@app.command()
def user():
    """Create default system user."""
    console.print("\n[bold blue]Creating default user...[/bold blue]\n")

    try:
        result = asyncio.run(_create_default_user())
        if result == 0:
            console.print("\n[green]Default user ready![/green]")
        else:
            raise typer.Exit(result)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise typer.Exit(1)


async def _create_default_user() -> int:
    """Create default system user if not exists."""
    import asyncpg

    postgres_url = get_database_url()

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


@app.command()
def strategy():
    """Load default strategies."""
    console.print("\n[bold blue]Loading default strategies...[/bold blue]\n")

    if not DEFAULT_STRATEGIES_PATH.exists():
        console.print(f"[yellow]No default strategies file found at:[/yellow]")
        console.print(f"  {DEFAULT_STRATEGIES_PATH}")
        console.print("\n[dim]You can create one to add default strategies.[/dim]")
        return

    try:
        result = asyncio.run(_load_strategies())
        if result == 0:
            console.print("\n[green]Strategies loaded successfully![/green]")
        else:
            raise typer.Exit(result)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise typer.Exit(1)


async def _load_strategies() -> int:
    """Load strategies from JSON file."""
    import asyncpg

    postgres_url = get_database_url()

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


@app.command()
def stocks(
    source: Optional[Path] = typer.Option(
        None, "--source", "-s",
        help="Path to SQLite data source (default: bundled sample data)"
    ),
    clear: bool = typer.Option(
        False, "--clear", "-c",
        help="Clear existing stock data before loading (or just clear if no source)"
    ),
):
    """Load stock data from SQLite source into PostgreSQL."""
    # If only --clear is specified without source, just clear the data
    if clear and source is None:
        console.print("\n[bold blue]Clearing stock data...[/bold blue]\n")
        try:
            result = asyncio.run(_clear_stock_data())
            if result == 0:
                console.print("\n[green]Stock data cleared![/green]")
            else:
                raise typer.Exit(result)
        except typer.Exit:
            raise
        except Exception as e:
            console.print(f"\n[red]Error: {e}[/red]")
            raise typer.Exit(1)
        return

    # Determine source path
    source_path = source if source else SAMPLE_DATA_PATH

    if not source_path.exists():
        console.print(f"[red]Error: Source file not found: {source_path}[/red]")
        raise typer.Exit(1)

    console.print("\n[bold blue]Loading stock data...[/bold blue]")
    console.print(f"Source: [cyan]{source_path}[/cyan]")

    # Get file size
    file_size = source_path.stat().st_size
    if file_size > 1024 * 1024:
        console.print(f"Size: [cyan]{file_size / (1024*1024):.1f} MB[/cyan]")
    else:
        console.print(f"Size: [cyan]{file_size / 1024:.1f} KB[/cyan]")

    # Run async migration
    try:
        result = asyncio.run(_load_stock_data(source_path, clear))
        if result == 0:
            console.print("\n[green]Stock data loaded successfully![/green]")
        else:
            raise typer.Exit(result)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise typer.Exit(1)


async def _clear_stock_data() -> int:
    """Clear all stock data from PostgreSQL."""
    import asyncpg

    postgres_url = get_database_url()

    try:
        conn = await asyncpg.connect(postgres_url)
    except Exception as e:
        console.print(f"[red]Failed to connect to PostgreSQL: {e}[/red]")
        return 1

    try:
        # Clear in order due to foreign key constraints
        console.print("  Clearing technical_indicators...")
        await conn.execute("DELETE FROM technical_indicators")

        console.print("  Clearing fundamental_indicators...")
        await conn.execute("DELETE FROM fundamental_indicators")

        console.print("  Clearing adjust_factor...")
        await conn.execute("DELETE FROM adjust_factor")

        console.print("  Clearing daily_k_data...")
        await conn.execute("DELETE FROM daily_k_data")

        console.print("  Clearing stock_basic...")
        await conn.execute("DELETE FROM stock_basic")

        return 0

    finally:
        await conn.close()


async def _load_stock_data(source_path: Path, clear: bool) -> int:
    """Load stock data from SQLite to PostgreSQL."""
    import asyncpg

    postgres_url = get_database_url()

    # Connect to SQLite
    console.print("\nConnecting to databases...")
    sqlite_conn = sqlite3.connect(str(source_path))
    sqlite_conn.row_factory = sqlite3.Row

    try:
        pg_conn = await asyncpg.connect(postgres_url)
    except Exception as e:
        console.print(f"[red]Failed to connect to PostgreSQL: {e}[/red]")
        console.print("\n[yellow]Make sure PostgreSQL is running:[/yellow]")
        console.print("  docker compose up -d db")
        return 1

    try:
        # Clear existing data if requested
        if clear:
            console.print("\nClearing existing data...")
            await pg_conn.execute("DELETE FROM adjust_factor")
            await pg_conn.execute("DELETE FROM daily_k_data")
            await pg_conn.execute("DELETE FROM stock_basic")
            console.print("[green]Existing data cleared[/green]")

        start_time = datetime.now()

        # Migrate tables
        stock_count = await _migrate_stock_basic(sqlite_conn, pg_conn)
        kdata_count = await _migrate_daily_k_data(sqlite_conn, pg_conn)
        adjust_count = await _migrate_adjust_factor(sqlite_conn, pg_conn)

        elapsed = datetime.now() - start_time

        # Summary table
        console.print()
        table = Table(title="Migration Summary")
        table.add_column("Table", style="cyan")
        table.add_column("Records", justify="right", style="green")
        table.add_row("stock_basic", f"{stock_count:,}")
        table.add_row("daily_k_data", f"{kdata_count:,}")
        table.add_row("adjust_factor", f"{adjust_count:,}")
        table.add_row("", "")
        table.add_row("[bold]Total time[/bold]", f"[bold]{elapsed}[/bold]")
        console.print(table)

        # Refresh continuous aggregates after data load
        if kdata_count > 0:
            console.print("\n[bold]Refreshing continuous aggregates...[/bold]")
            await _refresh_continuous_aggregates(pg_conn)

        return 0

    finally:
        sqlite_conn.close()
        await pg_conn.close()


def _parse_date(val) -> Optional[date]:
    """Parse date string to date object."""
    if val is None or val == "":
        return None
    if isinstance(val, date):
        return val
    try:
        return datetime.strptime(str(val), "%Y-%m-%d").date()
    except:
        return None


def _safe_decimal(val) -> Optional[Decimal]:
    """Convert value to Decimal safely."""
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except:
        return None


def _safe_int(val) -> Optional[int]:
    """Convert value to int safely."""
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except:
        return None


# Continuous aggregates to refresh after data import
CONTINUOUS_AGGREGATES = [
    "daily_k_weekly",
    "daily_k_monthly",
    "market_daily_stats",
    "tech_indicators_weekly",
]


async def _refresh_continuous_aggregates(pg_conn) -> None:
    """Refresh all TimescaleDB continuous aggregates."""
    for view_name in CONTINUOUS_AGGREGATES:
        console.print(f"  Refreshing [cyan]{view_name}[/cyan]...")
        try:
            await pg_conn.execute(
                f"CALL refresh_continuous_aggregate('{view_name}', NULL, NULL)"
            )
            console.print(f"  [green]✓[/green] {view_name}")
        except Exception as e:
            console.print(f"  [yellow]⚠[/yellow] {view_name}: {e}")


async def _migrate_stock_basic(sqlite_conn: sqlite3.Connection, pg_conn) -> int:
    """Migrate stock_basic table."""
    console.print("\nMigrating stock_basic...")

    cursor = sqlite_conn.execute("SELECT * FROM stock_basic")
    rows = cursor.fetchall()

    if not rows:
        console.print("  No data in stock_basic")
        return 0

    columns = [desc[0] for desc in cursor.description]
    console.print(f"  Found {len(rows):,} records")

    records = []
    for row in rows:
        record = dict(zip(columns, row))
        code = record.get("code", "")
        exchange = "sh" if code.startswith("sh.") else "sz" if code.startswith("sz.") else None

        records.append((
            code,
            record.get("code_name"),
            _parse_date(record.get("ipo_date")),
            _parse_date(record.get("out_date")),
            record.get("type"),
            record.get("status"),
            exchange,
            None,  # sector
            None,  # industry
        ))

    await pg_conn.executemany(
        """
        INSERT INTO stock_basic (code, code_name, ipo_date, out_date, stock_type, status, exchange, sector, industry)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (code) DO UPDATE SET
            code_name = EXCLUDED.code_name,
            updated_at = CURRENT_TIMESTAMP
        """,
        records,
    )

    console.print(f"  [green]Migrated {len(records):,} records[/green]")
    return len(records)


async def _migrate_daily_k_data(sqlite_conn: sqlite3.Connection, pg_conn) -> int:
    """Migrate daily_k_data table in batches."""
    console.print("\nMigrating daily_k_data...")

    BATCH_SIZE = 10000

    cursor = sqlite_conn.execute("SELECT COUNT(*) FROM daily_k_data")
    total = cursor.fetchone()[0]
    console.print(f"  Total records: {total:,}")

    cursor = sqlite_conn.execute("SELECT * FROM daily_k_data")
    columns = [desc[0] for desc in cursor.description]

    migrated = 0
    batch = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"  Progress: 0/{total:,} (0%)", total=None)

        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                record = dict(zip(columns, row))
                batch.append((
                    _parse_date(record.get("date")),
                    record.get("code"),
                    _safe_decimal(record.get("open")),
                    _safe_decimal(record.get("high")),
                    _safe_decimal(record.get("low")),
                    _safe_decimal(record.get("close")),
                    _safe_decimal(record.get("preclose")),
                    _safe_int(record.get("volume")),
                    _safe_decimal(record.get("amount")),
                    _safe_decimal(record.get("turn")),
                    _safe_int(record.get("tradestatus")),
                    _safe_decimal(record.get("pctChg")),
                    _safe_decimal(record.get("peTTM")),
                    _safe_decimal(record.get("pbMRQ")),
                    _safe_decimal(record.get("psTTM")),
                    _safe_decimal(record.get("pcfNcfTTM")),
                    _safe_int(record.get("isST")),
                ))

            await pg_conn.executemany(
                """
                INSERT INTO daily_k_data (
                    date, code, open, high, low, close, preclose, volume, amount,
                    turn, trade_status, pct_chg, pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm, is_st
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                ON CONFLICT (code, date) DO NOTHING
                """,
                batch,
            )

            migrated += len(batch)
            pct = (migrated / total) * 100
            progress.update(task, description=f"  Progress: {migrated:,}/{total:,} ({pct:.1f}%)")
            batch = []

    console.print(f"  [green]Migrated {migrated:,} records[/green]")
    return migrated


async def _migrate_adjust_factor(sqlite_conn: sqlite3.Connection, pg_conn) -> int:
    """Migrate adjust_factor table."""
    console.print("\nMigrating adjust_factor...")

    cursor = sqlite_conn.execute("SELECT * FROM adjust_factor")
    rows = cursor.fetchall()

    if not rows:
        console.print("  No data in adjust_factor")
        return 0

    columns = [desc[0] for desc in cursor.description]
    console.print(f"  Found {len(rows):,} records")

    records = []
    for row in rows:
        record = dict(zip(columns, row))
        records.append((
            record.get("code"),
            _parse_date(record.get("dividOperateDate")),
            _safe_decimal(record.get("foreAdjustFactor")),
            _safe_decimal(record.get("backAdjustFactor")),
            _safe_decimal(record.get("adjustFactor")),
        ))

    await pg_conn.executemany(
        """
        INSERT INTO adjust_factor (code, divid_operate_date, fore_adjust_factor, back_adjust_factor, adjust_factor)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (code, divid_operate_date) DO NOTHING
        """,
        records,
    )

    console.print(f"  [green]Migrated {len(records):,} records[/green]")
    return len(records)


@app.command()
def all():
    """Load all seed data (user + strategy + stocks)."""
    console.print("\n[bold blue]Loading all seed data...[/bold blue]")

    # 1. Create default user
    console.print("\n" + "=" * 50)
    console.print("[bold]Step 1: Creating default user[/bold]")
    console.print("=" * 50)
    try:
        result = asyncio.run(_create_default_user())
        if result != 0:
            raise typer.Exit(result)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Error creating user: {e}[/red]")
        raise typer.Exit(1)

    # 2. Load strategies
    console.print("\n" + "=" * 50)
    console.print("[bold]Step 2: Loading default strategies[/bold]")
    console.print("=" * 50)
    if DEFAULT_STRATEGIES_PATH.exists():
        try:
            result = asyncio.run(_load_strategies())
            if result != 0:
                raise typer.Exit(result)
        except typer.Exit:
            raise
        except Exception as e:
            console.print(f"[red]Error loading strategies: {e}[/red]")
            raise typer.Exit(1)
    else:
        console.print("  [dim]No default strategies file found, skipping[/dim]")

    # 3. Load stock data
    console.print("\n" + "=" * 50)
    console.print("[bold]Step 3: Loading stock data[/bold]")
    console.print("=" * 50)
    if SAMPLE_DATA_PATH.exists():
        try:
            result = asyncio.run(_load_stock_data(SAMPLE_DATA_PATH, False))
            if result != 0:
                raise typer.Exit(result)
        except typer.Exit:
            raise
        except Exception as e:
            console.print(f"[red]Error loading stock data: {e}[/red]")
            raise typer.Exit(1)
    else:
        console.print(f"  [yellow]Sample data not found:[/yellow] {SAMPLE_DATA_PATH}")
        console.print("  [dim]Skipping stock data loading[/dim]")

    console.print("\n" + "=" * 50)
    console.print("[bold green]All seed data loaded successfully![/bold green]")
    console.print("=" * 50 + "\n")
