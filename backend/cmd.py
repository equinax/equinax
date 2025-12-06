#!/usr/bin/env python3
"""
Quant Backtest CLI

Database management and utility commands for the Quant Backtest system.

Usage:
    python cmd.py db reset          # Reset database
    python cmd.py db status         # Show database status

    python cmd.py seed user         # Create default user
    python cmd.py seed strategy     # Load default strategies
    python cmd.py seed stocks       # Load stock data
    python cmd.py seed all          # Load all seed data
"""

import typer
from app.cli import db, seed

app = typer.Typer(
    name="cmd",
    help="Quant Backtest CLI - Database management and utilities",
    add_completion=False,
)

# Add sub-commands
app.add_typer(db.app, name="db", help="Database management commands")
app.add_typer(seed.app, name="seed", help="Seed data commands")


if __name__ == "__main__":
    app()
