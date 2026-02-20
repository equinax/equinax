"""Data Map API endpoints.

Provides endpoints for visualizing data coverage across all tables,
detecting gaps, and triggering targeted backfills.
"""

from datetime import date, timedelta
from typing import List, Optional, Dict
import logging

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================
# Data table registry — defines which tables
# the data map tracks and how to query them
# ============================================

# Each entry: (table_name, date_column, code_column_or_None, display_name, asset_scope)
DATA_TABLES = [
    ("market_daily", "date", "code", "行情数据", "all"),
    ("indicator_valuation", "date", "code", "估值指标", "stock"),
    # ("indicator_etf", "date", "code", "ETF指标", "etf"),  # Hidden: ETF指标暂不展示
    ("moneyflow_daily", "date", "code", "资金流向", "stock"),
    ("limit_list_daily", "date", "code", "涨跌停", "stock"),
    ("adjust_factor", "divid_operate_date", "code", "复权因子", "stock_etf"),
    ("stock_style_exposure", "date", "code", "风格因子", "stock"),
    # ("stock_microstructure", "date", "code", "微观结构", "stock"),  # Hidden: 微观结构暂不展示
    # ("technical_indicators", "date", "code", "技术指标", "all"),  # Hidden: 空表，指标实时计算中
    ("market_regime", "date", None, "市场环境", "market"),
]


# ============================================
# Pydantic Schemas
# ============================================


class TableCoverage(BaseModel):
    """Coverage info for a single data table."""

    table: str = Field(description="Table name")
    display_name: str = Field(description="Chinese display name")
    asset_scope: str = Field(description="Which asset types: all, stock, etf, stock_etf, market")
    row_count: int = Field(description="Total row count")
    symbol_count: int = Field(description="Distinct symbols (0 for market-level tables)")
    earliest_date: Optional[str] = None
    latest_date: Optional[str] = None
    total_dates: int = Field(description="Number of distinct dates with data", default=0)
    status: str = Field(description="OK, Empty, Stale, Gap")
    gap_days: int = Field(description="Number of detected gap trading days", default=0)
    staleness_days: int = Field(
        description="How many trading days behind the latest data date", default=0
    )


class CoverageResponse(BaseModel):
    """Full coverage overview for all tracked tables."""

    tables: List[TableCoverage]
    reference_date: str = Field(description="The latest trading day we compare against")
    total_gap_days: int = Field(description="Sum of gap days across all tables")


class HeatmapCell(BaseModel):
    """One cell in the date × table heatmap."""

    count: int = Field(description="Number of rows for this date+table")


class HeatmapRow(BaseModel):
    """One date row in the heatmap."""

    date: str
    cells: Dict[str, int] = Field(description="Map of table_name -> row count for this date")


class HeatmapResponse(BaseModel):
    """Heatmap matrix data: dates × tables."""

    tables: List[str] = Field(description="Ordered table names (columns)")
    table_labels: Dict[str, str] = Field(description="table_name -> display_name")
    rows: List[HeatmapRow] = Field(description="Date rows, newest first")
    expected_counts: Dict[str, int] = Field(
        description="Expected row count per table for a 'full' day (for coloring)"
    )


class BackfillRequest(BaseModel):
    """Request to backfill a specific table for a date range."""

    table: str = Field(description="Table to backfill")
    start_date: str = Field(description="Start date YYYY-MM-DD")
    end_date: str = Field(description="End date YYYY-MM-DD")


class BackfillResponse(BaseModel):
    """Response after triggering a backfill."""

    job_id: str
    status: str
    message: str


class GapDetail(BaseModel):
    """Detail about a detected gap."""

    table: str
    display_name: str
    missing_dates: List[str]


class GapResponse(BaseModel):
    """Detailed gap analysis for a table."""

    table: str
    display_name: str
    reference_dates: int = Field(description="Total trading dates in range")
    covered_dates: int = Field(description="Dates with data")
    missing_dates: List[str] = Field(description="List of missing dates")


# ============================================
# API Endpoints
# ============================================


@router.get("/coverage", response_model=CoverageResponse)
async def get_data_coverage(
    db: AsyncSession = Depends(get_db),
):
    """
    Get coverage summary for all tracked data tables.

    Returns per-table: row count, symbol count, date range, gap info, staleness.
    Uses pg_class for fast approximate row counts, exact min/max/distinct queries
    are lightweight since they leverage indexes.
    """
    import asyncio
    from workers.trading_days import get_latest_trading_day, get_trading_days_between

    reference_day = get_latest_trading_day()
    reference_str = reference_day.strftime("%Y-%m-%d")

    # Get approximate row counts using TimescaleDB's approximate_row_count()
    # for hypertables, and pg_class for regular tables.
    table_names_list = [t[0] for t in DATA_TABLES]

    # First try TimescaleDB hypertable stats (instant, no table scan)
    hyper_q = text("""
        SELECT hypertable_name, approximate_row_count(format('%I', hypertable_name)::regclass)
        FROM timescaledb_information.hypertables
        WHERE hypertable_name = ANY(:tables)
    """)
    hyper_result = await db.execute(hyper_q, {"tables": table_names_list})
    approx_counts = {row[0]: row[1] for row in hyper_result}

    # For tables not found as hypertables, use pg_class
    missing_tables = [t for t in table_names_list if t not in approx_counts]
    if missing_tables:
        pg_q = text("""
            SELECT relname, GREATEST(reltuples::bigint, 0) as approx_rows
            FROM pg_class
            WHERE relname = ANY(:tables)
        """)
        pg_result = await db.execute(pg_q, {"tables": missing_tables})
        for row in pg_result:
            approx_counts[row.relname] = row.approx_rows

    async def query_table(tbl_name, date_col, code_col, display, scope):
        try:
            approx_rows = max(0, approx_counts.get(tbl_name, 0))

            if approx_rows == 0:
                return TableCoverage(
                    table=tbl_name,
                    display_name=display,
                    asset_scope=scope,
                    row_count=0,
                    symbol_count=0,
                    total_dates=0,
                    status="Empty",
                    gap_days=0,
                    staleness_days=0,
                )

            # MIN/MAX uses index skip-scan — instant even on 100M+ rows
            range_q = text(f"""
                SELECT MIN({date_col})::text as earliest, MAX({date_col})::text as latest
                FROM {tbl_name}
            """)
            result = await db.execute(range_q)
            row = result.first()

            earliest = row.earliest if row and row.earliest else None
            latest = row.latest if row and row.latest else None

            if not latest or not earliest:
                return TableCoverage(
                    table=tbl_name,
                    display_name=display,
                    asset_scope=scope,
                    row_count=approx_rows,
                    symbol_count=0,
                    total_dates=0,
                    status="Empty",
                    gap_days=0,
                    staleness_days=0,
                )

            # Estimate symbol count from a single date (fast — scans ~5K rows)
            symbol_count = 0
            if code_col:
                latest_as_date = date.fromisoformat(latest)
                sym_q = text(f"""
                    SELECT COUNT(DISTINCT {code_col}) as cnt
                    FROM {tbl_name}
                    WHERE {date_col} = :latest
                """)
                sym_result = await db.execute(sym_q, {"latest": latest_as_date})
                symbol_count = sym_result.scalar() or 0

            # Estimate date count: approx_rows / symbols_per_day
            date_count = int(approx_rows / symbol_count) if symbol_count > 0 else approx_rows

            latest_date = date.fromisoformat(latest)
            staleness_list = get_trading_days_between(latest_date, reference_day)
            staleness = len(staleness_list)

            if staleness > 5:
                table_status = "Stale"
            else:
                table_status = "OK"

            earliest_date = date.fromisoformat(earliest)
            expected_trading_days = get_trading_days_between(
                earliest_date - timedelta(days=1), reference_day
            )
            gap_days = max(0, len(expected_trading_days) - date_count)
            if gap_days > 10:
                table_status = "Gap"

            return TableCoverage(
                table=tbl_name,
                display_name=display,
                asset_scope=scope,
                row_count=approx_rows,
                symbol_count=symbol_count,
                earliest_date=earliest,
                latest_date=latest,
                total_dates=date_count,
                status=table_status,
                gap_days=gap_days,
                staleness_days=staleness,
            )
        except Exception as e:
            logger.warning(f"Coverage query failed for {tbl_name}: {e}")
            await db.rollback()
            return TableCoverage(
                table=tbl_name,
                display_name=display,
                asset_scope=scope,
                row_count=0,
                symbol_count=0,
                total_dates=0,
                status=f"Error: {str(e)[:80]}",
                gap_days=0,
                staleness_days=0,
            )

    # Query all tables (sequentially since we share a single db session)
    tables: List[TableCoverage] = []
    for tbl_name, date_col, code_col, display, scope in DATA_TABLES:
        tc = await query_table(tbl_name, date_col, code_col, display, scope)
        tables.append(tc)

    total_gap = sum(t.gap_days for t in tables if t.row_count > 0)

    return CoverageResponse(
        tables=tables,
        reference_date=reference_str,
        total_gap_days=total_gap,
    )


@router.get("/heatmap", response_model=HeatmapResponse)
async def get_data_heatmap(
    days: int = Query(default=60, ge=7, le=4100, description="Number of calendar days to show"),
    start_date: Optional[str] = Query(
        default=None, description="Start date YYYY-MM-DD (overrides days)"
    ),
    end_date: Optional[str] = Query(
        default=None, description="End date YYYY-MM-DD (overrides days)"
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Get date × table heatmap matrix.

    For each date in the range, returns the row count per table.
    Used to render the coverage heatmap on the frontend.
    """
    if start_date and end_date:
        q_start = date.fromisoformat(start_date)
        q_end = date.fromisoformat(end_date)
    else:
        q_end = date.today()
        q_start = q_end - timedelta(days=days)

    table_names = []
    table_labels = {}

    date_table_counts: Dict[str, Dict[str, int]] = {}

    for tbl_name, date_col, code_col, display, scope in DATA_TABLES:
        table_names.append(tbl_name)
        table_labels[tbl_name] = display

        try:
            q = text(f"""
                SELECT {date_col}::text as d, COUNT(*) as cnt
                FROM {tbl_name}
                WHERE {date_col} >= :start AND {date_col} <= :end
                GROUP BY {date_col}
                ORDER BY {date_col}
            """)
            result = await db.execute(q, {"start": q_start, "end": q_end})

            for row in result:
                d = row.d
                if d not in date_table_counts:
                    date_table_counts[d] = {}
                date_table_counts[d][tbl_name] = row.cnt
        except Exception as e:
            logger.warning(f"Heatmap query failed for {tbl_name}: {e}")
            pass

    # Build expected counts (approximate) — use the max count seen for each table
    expected_counts: Dict[str, int] = {}
    for tbl_name in table_names:
        max_cnt = 0
        for d_counts in date_table_counts.values():
            if tbl_name in d_counts and d_counts[tbl_name] > max_cnt:
                max_cnt = d_counts[tbl_name]
        expected_counts[tbl_name] = max_cnt

    # Build rows sorted by date descending
    rows = []
    for d in sorted(date_table_counts.keys(), reverse=True):
        cells = {}
        for tbl_name in table_names:
            cells[tbl_name] = date_table_counts[d].get(tbl_name, 0)
        rows.append(HeatmapRow(date=d, cells=cells))

    return HeatmapResponse(
        tables=table_names,
        table_labels=table_labels,
        rows=rows,
        expected_counts=expected_counts,
    )


@router.get("/gaps/{table}", response_model=GapResponse)
async def get_table_gaps(
    table: str,
    days: int = Query(default=90, ge=7, le=4100, description="Calendar days to check"),
    start_date: Optional[str] = Query(
        default=None, description="Start date YYYY-MM-DD (overrides days)"
    ),
    end_date: Optional[str] = Query(
        default=None, description="End date YYYY-MM-DD (overrides days)"
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed gap analysis for a specific table.

    Returns all missing trading dates for the given table in the specified range.
    """
    from workers.trading_days import get_trading_days_between

    # Validate table name
    table_info = None
    for tbl_name, date_col, code_col, display, scope in DATA_TABLES:
        if tbl_name == table:
            table_info = (tbl_name, date_col, code_col, display, scope)
            break

    if table_info is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown table: {table}. Valid: {[t[0] for t in DATA_TABLES]}",
        )

    tbl_name, date_col, code_col, display, scope = table_info
    if start_date and end_date:
        q_start = date.fromisoformat(start_date)
        q_end = date.fromisoformat(end_date)
    else:
        q_end = date.today()
        q_start = q_end - timedelta(days=days)

    # Get all trading days in range (between is exclusive start, inclusive end)
    trading_days = get_trading_days_between(q_start - timedelta(days=1), q_end)
    trading_day_set = {d.strftime("%Y-%m-%d") for d in trading_days}

    # Get dates that exist in the table
    q = text(f"""
        SELECT DISTINCT {date_col}::text as d
        FROM {tbl_name}
        WHERE {date_col} >= :start AND {date_col} <= :end
    """)
    result = await db.execute(
        q,
        {"start": q_start, "end": q_end},
    )
    existing_dates = {row.d for row in result}

    # Missing = trading days that don't appear in the table
    missing = sorted(trading_day_set - existing_dates, reverse=True)

    return GapResponse(
        table=tbl_name,
        display_name=display,
        reference_dates=len(trading_days),
        covered_dates=len(existing_dates),
        missing_dates=missing,
    )


@router.post("/backfill", response_model=BackfillResponse)
async def trigger_backfill(
    request: BackfillRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger a targeted backfill for a specific table and date range.

    Enqueues a backfill job to the ARQ worker.
    """
    from uuid import uuid4
    from app.core.arq import get_arq_pool
    from app.db.models.sync import SyncHistory

    # Validate table name
    valid_tables = [t[0] for t in DATA_TABLES]
    if request.table not in valid_tables:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown table: {request.table}. Valid: {valid_tables}",
        )

    # Validate dates
    try:
        start = date.fromisoformat(request.start_date)
        end = date.fromisoformat(request.end_date)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD.",
        )

    if start > end:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date must be <= end_date",
        )

    if (end - start).days > 365:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Maximum backfill range is 365 days",
        )

    job_id = str(uuid4())

    # Create sync history record
    sync_record = SyncHistory(
        id=job_id,
        sync_type=f"backfill:{request.table}",
        status="queued",
        triggered_by="api",
    )
    db.add(sync_record)
    await db.commit()

    # Enqueue to ARQ worker
    try:
        arq_pool = await get_arq_pool()
        await arq_pool.enqueue_job(
            "targeted_backfill",
            job_id,
            request.table,
            request.start_date,
            request.end_date,
        )
    except Exception as e:
        sync_record.status = "failed"
        sync_record.error_message = f"Failed to enqueue: {str(e)}"
        await db.commit()
        return BackfillResponse(
            job_id=job_id,
            status="failed",
            message=f"Failed to enqueue backfill job: {str(e)}",
        )

    return BackfillResponse(
        job_id=job_id,
        status="queued",
        message=f"Backfill queued: {request.table} from {request.start_date} to {request.end_date}",
    )


# ============================================
# Market Daily breakdown — stock vs ETF vs index
# ============================================


class MarketDailyBreakdown(BaseModel):
    """Breakdown of market_daily by asset type."""

    asset_type: str
    row_count: int
    symbol_count: int
    earliest_date: Optional[str] = None
    latest_date: Optional[str] = None


class MarketDailyBreakdownResponse(BaseModel):
    """market_daily breakdown by asset type."""

    breakdowns: List[MarketDailyBreakdown]


@router.get("/market-daily-breakdown", response_model=MarketDailyBreakdownResponse)
async def get_market_daily_breakdown(
    db: AsyncSession = Depends(get_db),
):
    """
    Get market_daily breakdown by asset type (stock, ETF, index).

    Uses a fast approach: queries distinct codes from a recent single date
    to classify by asset type, then estimates row counts proportionally
    from the total approximate_row_count.
    """
    # Get total approx row count and a recent date with data
    total_q = text("""
        SELECT approximate_row_count('market_daily'::regclass) as total_rows
    """)
    total_result = await db.execute(total_q)
    total_rows = total_result.scalar() or 0

    date_range_q = text("""
        SELECT MIN(date)::text as earliest, MAX(date)::text as latest
        FROM market_daily
    """)
    date_range_result = await db.execute(date_range_q)
    date_range = date_range_result.first()
    global_earliest = date_range.earliest if date_range else None
    global_latest = date_range.latest if date_range else None

    # Get symbol counts per type from a single recent date (fast — ~5K rows max)
    type_filters = {
        "Stock": """
            (code LIKE 'sh.6%%' OR code LIKE 'sz.0%%' OR code LIKE 'sz.3%%' OR code LIKE 'bj.%%')
            AND code NOT LIKE 'sh.000%%' AND code NOT LIKE 'sz.399%%'
        """,
        "Index": "code LIKE 'sh.000%%' OR code LIKE 'sz.399%%'",
        "ETF": "code LIKE 'sh.5%%' OR code LIKE 'sz.1%%'",
    }

    breakdowns = []
    symbol_counts = {}
    total_symbols = 0

    if global_latest:
        latest_as_date = date.fromisoformat(global_latest)
        for asset_type, where_clause in type_filters.items():
            try:
                q = text(f"""
                    SELECT COUNT(DISTINCT code) as symbol_count
                    FROM market_daily
                    WHERE date = :latest AND ({where_clause})
                """)
                result = await db.execute(q, {"latest": latest_as_date})
                cnt = result.scalar() or 0
                symbol_counts[asset_type] = cnt
                total_symbols += cnt
            except Exception:
                symbol_counts[asset_type] = 0

    # Estimate row counts proportionally from symbol distribution
    for asset_type in type_filters:
        sc = symbol_counts.get(asset_type, 0)
        estimated_rows = int(total_rows * sc / total_symbols) if total_symbols > 0 else 0
        breakdowns.append(
            MarketDailyBreakdown(
                asset_type=asset_type,
                row_count=estimated_rows,
                symbol_count=sc,
                earliest_date=global_earliest,
                latest_date=global_latest,
            )
        )

    return MarketDailyBreakdownResponse(breakdowns=breakdowns)
