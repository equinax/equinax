"""Database management CLI commands."""

import asyncio
import os
import subprocess
from pathlib import Path

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

app = typer.Typer(help="Database management commands")
console = Console()

# Default paths
CLI_DIR = Path(__file__).parent
BACKEND_DIR = CLI_DIR.parent.parent
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
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            strategy_count = await conn.fetchval("SELECT COUNT(*) FROM strategies")

            # Get date range for k-data
            date_range = await conn.fetchrow(
                "SELECT MIN(date) as min_date, MAX(date) as max_date FROM daily_k_data"
            )

            table = Table(title="Record Counts")
            table.add_column("Table", style="cyan")
            table.add_column("Count", justify="right", style="green")
            table.add_row("users", f"{user_count:,}")
            table.add_row("strategies", f"{strategy_count:,}")
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
