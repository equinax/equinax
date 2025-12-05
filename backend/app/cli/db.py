"""Database management CLI commands."""

import asyncio
import os
import sqlite3
import subprocess
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

app = typer.Typer(help="Database management commands")
console = Console()

# Default paths
CLI_DIR = Path(__file__).parent
BACKEND_DIR = CLI_DIR.parent.parent
SAMPLE_DATA_PATH = BACKEND_DIR / "examples" / "data" / "sample_data.db"
ALEMBIC_DIR = BACKEND_DIR / "alembic"


def get_database_url() -> str:
    """Get PostgreSQL connection URL from environment."""
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://quant:quant_dev_password@localhost:5432/quantdb"
    )
    # Convert async URL to sync for asyncpg
    return url.replace("+asyncpg", "").replace("postgresql+asyncpg", "postgresql")


def run_alembic(command: list[str]) -> tuple[int, str, str]:
    """Run an alembic command and return exit code, stdout, stderr."""
    env = os.environ.copy()
    result = subprocess.run(
        ["alembic"] + command,
        cwd=str(BACKEND_DIR),
        capture_output=True,
        text=True,
        env=env
    )
    return result.returncode, result.stdout, result.stderr


@app.command()
def init():
    """Initialize the database schema (run migrations)."""
    console.print("\n[bold blue]Initializing database...[/bold blue]\n")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Running migrations...", total=None)

        exit_code, stdout, stderr = run_alembic(["upgrade", "head"])

        progress.remove_task(task)

    if exit_code == 0:
        console.print("[green]Database initialized successfully![/green]")
        if stdout.strip():
            console.print(f"\n{stdout}")
    else:
        console.print("[red]Failed to initialize database[/red]")
        if stderr.strip():
            console.print(f"\n[red]{stderr}[/red]")
        raise typer.Exit(1)


@app.command()
def reset(
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt")
):
    """Reset the database (drop all tables and reinitialize)."""
    if not force:
        confirm = typer.confirm(
            "\n⚠️  This will DELETE ALL DATA in the database. Continue?",
            default=False
        )
        if not confirm:
            console.print("[yellow]Aborted.[/yellow]")
            raise typer.Exit(0)

    console.print("\n[bold blue]Resetting database...[/bold blue]\n")

    # Step 1: Drop all tables directly (more robust than alembic downgrade)
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Dropping tables...", total=None)
        try:
            result = asyncio.run(_drop_all_tables())
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

    # Step 2: Upgrade to head
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Recreating tables...", total=None)
        exit_code, stdout, stderr = run_alembic(["upgrade", "head"])
        progress.remove_task(task)

    if exit_code == 0:
        console.print("[green]Database reset successfully![/green]")
    else:
        console.print("[red]Failed to recreate tables[/red]")
        if stderr.strip():
            console.print(f"\n[red]{stderr}[/red]")
        raise typer.Exit(1)


async def _drop_all_tables() -> int:
    """Drop all tables and types directly using SQL."""
    import asyncpg

    postgres_url = get_database_url()
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


@app.command()
def loaddata(
    source: Optional[Path] = typer.Option(
        None, "--source", "-s",
        help="Path to SQLite data source (default: bundled sample data)"
    ),
    clear: bool = typer.Option(
        False, "--clear", "-c",
        help="Clear existing stock data before loading"
    ),
):
    """Load stock data from SQLite source into PostgreSQL."""
    import asyncpg

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
        result = asyncio.run(_load_data_async(source_path, clear))
        if result == 0:
            console.print("\n[green]Data loaded successfully![/green]")
        else:
            raise typer.Exit(result)
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise typer.Exit(1)


async def _load_data_async(source_path: Path, clear: bool) -> int:
    """Async function to load data from SQLite to PostgreSQL."""
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
        console.print("  docker-compose up -d db")
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

        # Ensure default user exists
        await _ensure_default_user(pg_conn)

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

        return 0

    finally:
        sqlite_conn.close()
        await pg_conn.close()


async def _ensure_default_user(pg_conn) -> None:
    """Create default system user if not exists."""
    import hashlib
    import secrets

    # Default system user UUID
    default_user_id = "00000000-0000-0000-0000-000000000001"

    # Check if user exists
    exists = await pg_conn.fetchval(
        "SELECT 1 FROM users WHERE id = $1",
        default_user_id
    )

    if not exists:
        console.print("\nCreating default system user...")
        # Generate a random password hash (user won't actually log in with this)
        salt = secrets.token_hex(32)
        password_hash = hashlib.sha256(f"system_default_{salt}".encode()).hexdigest()

        await pg_conn.execute(
            """
            INSERT INTO users (id, email, username, password_hash, salt, is_active, is_admin)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING
            """,
            default_user_id,
            "system@localhost",
            "system",
            password_hash,
            salt,
            True,
            False,
        )
        console.print("  [green]Default user created[/green]")


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
def status():
    """Show database status and record counts."""
    import asyncpg

    console.print("\n[bold blue]Database Status[/bold blue]\n")

    async def get_status():
        postgres_url = get_database_url()
        try:
            conn = await asyncpg.connect(postgres_url)
        except Exception as e:
            console.print(f"[red]Cannot connect to database: {e}[/red]")
            return 1

        try:
            # Get record counts
            stock_count = await conn.fetchval("SELECT COUNT(*) FROM stock_basic")
            kdata_count = await conn.fetchval("SELECT COUNT(*) FROM daily_k_data")
            adjust_count = await conn.fetchval("SELECT COUNT(*) FROM adjust_factor")

            # Get date range for k-data
            date_range = await conn.fetchrow(
                "SELECT MIN(date) as min_date, MAX(date) as max_date FROM daily_k_data"
            )

            table = Table(title="Record Counts")
            table.add_column("Table", style="cyan")
            table.add_column("Count", justify="right", style="green")
            table.add_row("stock_basic", f"{stock_count:,}")
            table.add_row("daily_k_data", f"{kdata_count:,}")
            table.add_row("adjust_factor", f"{adjust_count:,}")
            console.print(table)

            if date_range and date_range['min_date']:
                console.print(f"\nData range: [cyan]{date_range['min_date']}[/cyan] to [cyan]{date_range['max_date']}[/cyan]")

            return 0
        finally:
            await conn.close()

    try:
        result = asyncio.run(get_status())
        if result != 0:
            raise typer.Exit(result)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
