#!/usr/bin/env python3
"""
Quant Backtest CLI

Database management and utility commands for the Quant Backtest system.

Usage:
    python cmd.py db init           # Initialize database
    python cmd.py db reset          # Reset database
    python cmd.py db loaddata       # Load sample data
    python cmd.py db status         # Show database status
"""

import typer
from app.cli import db

app = typer.Typer(
    name="cmd",
    help="Quant Backtest CLI - Database management and utilities",
    add_completion=False,
)

# Add sub-commands
app.add_typer(db.app, name="db", help="Database management commands")


if __name__ == "__main__":
    app()
