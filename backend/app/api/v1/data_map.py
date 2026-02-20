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

# Each entry: (table_key, physical_table, date_column, code_column_or_None, display_name, asset_scope, where_filter_or_None)
# table_key is the unique identifier used in API responses (may differ from physical table for virtual splits)
# where_filter is an optional SQL WHERE clause fragment to scope rows (e.g. for splitting market_daily by asset type)

_STOCK_FILTER = """
    (code LIKE 'sh.6%%' OR code LIKE 'sz.0%%' OR code LIKE 'sz.3%%' OR code LIKE 'bj.%%')
    AND code NOT LIKE 'sh.000%%' AND code NOT LIKE 'sz.399%%'
"""
_INDEX_FILTER = "code LIKE 'sh.000%%' OR code LIKE 'sz.399%%'"
_ETF_FILTER = "code LIKE 'sh.5%%' OR code LIKE 'sz.1%%'"

DATA_TABLES = [
    ("market_daily_stock", "market_daily", "date", "code", "股票行情", "stock", _STOCK_FILTER),
    ("market_daily_etf", "market_daily", "date", "code", "ETF行情", "etf", _ETF_FILTER),
    ("market_daily_index", "market_daily", "date", "code", "指数行情", "index", _INDEX_FILTER),
    ("indicator_valuation", "indicator_valuation", "date", "code", "估值指标", "stock", None),
    # ("indicator_etf", "indicator_etf", "date", "code", "ETF指标", "etf", None),  # Hidden
    ("moneyflow_daily", "moneyflow_daily", "date", "code", "资金流向", "stock", None),
    ("limit_list_daily", "limit_list_daily", "date", "code", "涨跌停", "stock", None),
    ("adjust_factor", "adjust_factor", "divid_operate_date", "code", "复权因子", "stock_etf", None),
    ("stock_style_exposure", "stock_style_exposure", "date", "code", "风格因子", "stock", None),
    # ("stock_microstructure", ..., None),  # Hidden
    # ("technical_indicators", ..., None),  # Hidden
    ("market_regime", "market_regime", "date", None, "市场环境", "market", None),
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


class SparseDateInfo(BaseModel):
    date: str
    actual: int
    expected: int


class GapResponse(BaseModel):
    """Detailed gap analysis for a table."""

    table: str
    display_name: str
    reference_dates: int = Field(description="Total trading dates in range")
    covered_dates: int = Field(description="Dates with data")
    missing_dates: List[str] = Field(description="List of missing dates")
    sparse_dates: List[SparseDateInfo] = Field(
        default_factory=list,
        description="Dates with data < 95% expected",
    )


# ============================================
# API Endpoints
# ============================================


def _build_where(date_col: str, where_filter: Optional[str] = None, extra: str = "") -> str:
    """Build WHERE clause combining date filter, asset type filter, and optional extra conditions."""
    parts = [extra] if extra else []
    if where_filter:
        parts.append(f"({where_filter})")
    return (" AND ".join(parts)) if parts else "TRUE"


async def _get_trading_dates(db: AsyncSession, start: date, end: date) -> List[date]:
    """Query trading_calendar for open trading days in [start, end]."""
    result = await db.execute(
        text("""
            SELECT cal_date FROM trading_calendar
            WHERE is_open = 1 AND cal_date >= :start AND cal_date <= :end
            ORDER BY cal_date
        """),
        {"start": start, "end": end},
    )
    return [row[0] for row in result]


async def _get_latest_trading_date(db: AsyncSession) -> date:
    """Get the most recent trading day from trading_calendar that is <= today."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    china_tz = ZoneInfo("Asia/Shanghai")
    now = datetime.now(china_tz)
    today = now.date()

    # If before 17:00 China time, use yesterday as cutoff (intraday data incomplete)
    cutoff = today if now.hour >= 17 else today - timedelta(days=1)

    result = await db.execute(
        text("""
            SELECT cal_date FROM trading_calendar
            WHERE is_open = 1 AND cal_date <= :cutoff
            ORDER BY cal_date DESC LIMIT 1
        """),
        {"cutoff": cutoff},
    )
    row = result.first()
    if row:
        return row[0]
    return cutoff


async def _get_expected_asset_count(db: AsyncSession, asset_type: str, target_date: date) -> int:
    result = await db.execute(
        text("""
            SELECT COUNT(*) FROM asset_meta
            WHERE asset_type = :atype
              AND list_date <= :d
              AND (delist_date IS NULL OR delist_date > :d)
        """),
        {"atype": asset_type, "d": target_date},
    )
    return result.scalar() or 0


@router.get("/coverage", response_model=CoverageResponse)
async def get_data_coverage(
    db: AsyncSession = Depends(get_db),
):
    reference_day = await _get_latest_trading_date(db)
    reference_str = reference_day.strftime("%Y-%m-%d")

    physical_tables = list({t[1] for t in DATA_TABLES})

    hyper_q = text("""
        SELECT hypertable_name, approximate_row_count(format('%I', hypertable_name)::regclass)
        FROM timescaledb_information.hypertables
        WHERE hypertable_name = ANY(:tables)
    """)
    hyper_result = await db.execute(hyper_q, {"tables": physical_tables})
    approx_counts = {row[0]: row[1] for row in hyper_result}

    missing_tables = [t for t in physical_tables if t not in approx_counts]
    if missing_tables:
        pg_q = text("""
            SELECT relname, GREATEST(reltuples::bigint, 0) as approx_rows
            FROM pg_class
            WHERE relname = ANY(:tables)
        """)
        pg_result = await db.execute(pg_q, {"tables": missing_tables})
        for row in pg_result:
            approx_counts[row.relname] = row.approx_rows

    async def query_table(table_key, phys_table, date_col, code_col, display, scope, where_filter):
        try:
            approx_rows = max(0, approx_counts.get(phys_table, 0))

            if approx_rows == 0:
                return TableCoverage(
                    table=table_key,
                    display_name=display,
                    asset_scope=scope,
                    row_count=0,
                    symbol_count=0,
                    total_dates=0,
                    status="Empty",
                    gap_days=0,
                    staleness_days=0,
                )

            filter_clause = _build_where(date_col, where_filter)
            range_q = text(f"""
                SELECT MIN({date_col})::text as earliest, MAX({date_col})::text as latest
                FROM {phys_table}
                WHERE {filter_clause}
            """)
            result = await db.execute(range_q)
            row = result.first()

            earliest = row.earliest if row and row.earliest else None
            latest = row.latest if row and row.latest else None

            if not latest or not earliest:
                return TableCoverage(
                    table=table_key,
                    display_name=display,
                    asset_scope=scope,
                    row_count=0,
                    symbol_count=0,
                    total_dates=0,
                    status="Empty",
                    gap_days=0,
                    staleness_days=0,
                )

            symbol_count = 0
            if code_col:
                asset_type = VIRTUAL_TABLE_ASSET_MAP.get(table_key)
                if asset_type:
                    latest_as_date = date.fromisoformat(latest)
                    symbol_count = await _get_expected_asset_count(db, asset_type, latest_as_date)
                else:
                    latest_as_date = date.fromisoformat(latest)
                    sym_q = text(f"""
                        SELECT COUNT(DISTINCT {code_col}) as cnt
                        FROM {phys_table}
                        WHERE {date_col} = :latest AND {filter_clause}
                    """)
                    sym_result = await db.execute(sym_q, {"latest": latest_as_date})
                    symbol_count = sym_result.scalar() or 0

            date_count = (
                int(approx_rows / symbol_count) if symbol_count > 0 and not where_filter else 0
            )
            if where_filter or symbol_count == 0:
                distinct_q = text(f"""
                    SELECT COUNT(DISTINCT {date_col}) as cnt
                    FROM {phys_table}
                    WHERE {filter_clause}
                """)
                date_count = (await db.execute(distinct_q)).scalar() or 0

            latest_date = date.fromisoformat(latest)
            staleness_dates = await _get_trading_dates(db, latest_date, reference_day)
            staleness = len(staleness_dates)

            table_status = "Stale" if staleness > 5 else "OK"

            earliest_date = date.fromisoformat(earliest)
            expected_trading_days = await _get_trading_dates(db, earliest_date, reference_day)
            gap_days = max(0, len(expected_trading_days) - date_count)
            if gap_days > 10:
                table_status = "Gap"

            row_count = approx_rows
            if where_filter:
                row_count = symbol_count * date_count if symbol_count > 0 else 0

            return TableCoverage(
                table=table_key,
                display_name=display,
                asset_scope=scope,
                row_count=row_count,
                symbol_count=symbol_count,
                earliest_date=earliest,
                latest_date=latest,
                total_dates=date_count,
                status=table_status,
                gap_days=gap_days,
                staleness_days=staleness,
            )
        except Exception as e:
            logger.warning(f"Coverage query failed for {table_key}: {e}")
            await db.rollback()
            return TableCoverage(
                table=table_key,
                display_name=display,
                asset_scope=scope,
                row_count=0,
                symbol_count=0,
                total_dates=0,
                status=f"Error: {str(e)[:80]}",
                gap_days=0,
                staleness_days=0,
            )

    tables: List[TableCoverage] = []
    for table_key, phys_table, date_col, code_col, display, scope, where_filter in DATA_TABLES:
        tc = await query_table(
            table_key, phys_table, date_col, code_col, display, scope, where_filter
        )
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
    if start_date and end_date:
        q_start = date.fromisoformat(start_date)
        q_end = date.fromisoformat(end_date)
    else:
        q_end = date.today()
        q_start = q_end - timedelta(days=days)

    trading_dates = await _get_trading_dates(db, q_start, q_end)
    trading_date_set = {d.isoformat() for d in trading_dates}

    table_keys = []
    table_labels = {}

    date_table_counts: Dict[str, Dict[str, int]] = {}
    for d_str in trading_date_set:
        date_table_counts[d_str] = {}

    for table_key, phys_table, date_col, code_col, display, scope, where_filter in DATA_TABLES:
        table_keys.append(table_key)
        table_labels[table_key] = display

        try:
            filter_clause = _build_where(
                date_col, where_filter, f"{date_col} >= :start AND {date_col} <= :end"
            )
            q = text(f"""
                SELECT {date_col}::text as d, COUNT(*) as cnt
                FROM {phys_table}
                WHERE {filter_clause}
                GROUP BY {date_col}
                ORDER BY {date_col}
            """)
            result = await db.execute(q, {"start": q_start, "end": q_end})

            for row in result:
                d = row.d
                if d in trading_date_set:
                    date_table_counts[d][table_key] = row.cnt
        except Exception as e:
            logger.warning(f"Heatmap query failed for {table_key}: {e}")

    expected_counts: Dict[str, int] = {}
    representative_date = trading_dates[-1] if trading_dates else q_end
    for tk in table_keys:
        asset_type = VIRTUAL_TABLE_ASSET_MAP.get(tk)
        if asset_type:
            expected_counts[tk] = await _get_expected_asset_count(
                db, asset_type, representative_date
            )
        else:
            max_cnt = 0
            for d_counts in date_table_counts.values():
                if tk in d_counts and d_counts[tk] > max_cnt:
                    max_cnt = d_counts[tk]
            expected_counts[tk] = max_cnt

    rows = []
    for d in sorted(date_table_counts.keys(), reverse=True):
        cells = {}
        for tk in table_keys:
            cells[tk] = date_table_counts[d].get(tk, 0)
        rows.append(HeatmapRow(date=d, cells=cells))

    return HeatmapResponse(
        tables=table_keys,
        table_labels=table_labels,
        rows=rows,
        expected_counts=expected_counts,
    )


def _find_table(table_key: str):
    for entry in DATA_TABLES:
        if entry[0] == table_key:
            return entry
    return None


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
    entry = _find_table(table)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown table: {table}. Valid: {[t[0] for t in DATA_TABLES]}",
        )

    table_key, phys_table, date_col, code_col, display, scope, where_filter = entry
    if start_date and end_date:
        q_start = date.fromisoformat(start_date)
        q_end = date.fromisoformat(end_date)
    else:
        q_end = date.today()
        q_start = q_end - timedelta(days=days)

    trading_days = await _get_trading_dates(db, q_start, q_end)
    trading_day_set = {d.strftime("%Y-%m-%d") for d in trading_days}

    asset_type = VIRTUAL_TABLE_ASSET_MAP.get(table_key)

    filter_clause = _build_where(
        date_col, where_filter, f"{date_col} >= :start AND {date_col} <= :end"
    )

    if asset_type and code_col:
        count_q = text(f"""
            SELECT {date_col}::text as d, COUNT(*) as cnt
            FROM {phys_table}
            WHERE {filter_clause}
            GROUP BY {date_col}
        """)
        result = await db.execute(count_q, {"start": q_start, "end": q_end})
        date_counts = {row.d: row.cnt for row in result}
        existing_dates = set(date_counts.keys())

        expected_q = text("""
            SELECT d::text, COUNT(*) as cnt
            FROM unnest(CAST(:dates AS date[])) AS d
            JOIN asset_meta am ON am.asset_type = :atype
              AND am.list_date <= d
              AND (am.delist_date IS NULL OR am.delist_date > d)
            GROUP BY d
        """)
        dates_with_data = [td for td in trading_days if td.strftime("%Y-%m-%d") in date_counts]
        expected_map: Dict[str, int] = {}
        if dates_with_data:
            exp_result = await db.execute(
                expected_q,
                {
                    "dates": dates_with_data,
                    "atype": asset_type,
                },
            )
            expected_map = {row[0]: row[1] for row in exp_result}

        sparse_dates = []
        for td in dates_with_data:
            d_str = td.strftime("%Y-%m-%d")
            actual = date_counts[d_str]
            expected = expected_map.get(d_str, 0)
            if expected > 0 and actual / expected < 0.95:
                sparse_dates.append(SparseDateInfo(date=d_str, actual=actual, expected=expected))
        sparse_dates.sort(key=lambda x: x.date, reverse=True)
    else:
        q = text(f"""
            SELECT DISTINCT {date_col}::text as d
            FROM {phys_table}
            WHERE {filter_clause}
        """)
        result = await db.execute(q, {"start": q_start, "end": q_end})
        existing_dates = {row.d for row in result}
        sparse_dates = []

    missing = sorted(trading_day_set - existing_dates, reverse=True)

    return GapResponse(
        table=table_key,
        display_name=display,
        reference_dates=len(trading_days),
        covered_dates=len(existing_dates & trading_day_set),
        missing_dates=missing,
        sparse_dates=sparse_dates,
    )


@router.post("/backfill", response_model=BackfillResponse)
async def trigger_backfill(
    request: BackfillRequest,
    db: AsyncSession = Depends(get_db),
):
    from uuid import uuid4
    from app.core.arq import get_arq_pool
    from app.db.models.sync import SyncHistory

    entry = _find_table(request.table)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown table: {request.table}. Valid: {[t[0] for t in DATA_TABLES]}",
        )

    phys_table = entry[1]

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

    sync_record = SyncHistory(
        id=job_id,
        sync_type=f"backfill:{request.table}",
        status="queued",
        triggered_by="api",
    )
    db.add(sync_record)
    await db.commit()

    try:
        arq_pool = await get_arq_pool()
        await arq_pool.enqueue_job(
            "targeted_backfill",
            job_id,
            phys_table,
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


# ============================================
# Date Detail & Single-Date Backfill
# ============================================

VIRTUAL_TABLE_ASSET_MAP = {
    "market_daily_stock": "STOCK",
    "market_daily_etf": "ETF",
    "market_daily_index": "INDEX",
}

ASSET_TYPE_TO_SYNC = {
    "STOCK": "stock",
    "ETF": "etf",
    "INDEX": "index",
}


class AssetTypeBreakdown(BaseModel):
    asset_type: str
    actual: int
    expected: int


class DateDetailResponse(BaseModel):
    date: str
    table: str
    total_actual: int
    total_expected: int
    breakdown: List[AssetTypeBreakdown]


class DateBackfillRequest(BaseModel):
    table: str
    date: str
    asset_types: List[str]


class DateBackfillResponse(BaseModel):
    date: str
    results: Dict[str, int]
    errors: List[str] = Field(default_factory=list)


class SparseBackfillRequest(BaseModel):
    table: str = Field(description="Virtual table key, e.g. market_daily_stock")
    dates: List[str] = Field(description="Sparse dates to backfill (YYYY-MM-DD)")


class SparseBackfillDateResult(BaseModel):
    date: str
    before: int
    after: int
    expected: int
    status: str = Field(description="improved | best_effort | unchanged | error")
    message: str = ""


class SparseBackfillResponse(BaseModel):
    table: str
    results: List[SparseBackfillDateResult]
    total_improved: int = 0
    errors: List[str] = Field(default_factory=list)


@router.get("/date-detail/{table}", response_model=DateDetailResponse)
async def get_date_detail(
    table: str,
    date_str: str = Query(..., alias="date", description="Date YYYY-MM-DD"),
    db: AsyncSession = Depends(get_db),
):
    entry = _find_table(table)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown table: {table}",
        )

    table_key, phys_table, date_col, code_col, _, scope, where_filter = entry
    target_date = date.fromisoformat(date_str)

    asset_type = VIRTUAL_TABLE_ASSET_MAP.get(table_key)
    if asset_type:
        filter_clause = _build_where(date_col, where_filter, f"{date_col} = :d")
        actual_q = text(f"SELECT COUNT(*) FROM {phys_table} WHERE {filter_clause}")
        actual = (await db.execute(actual_q, {"d": target_date})).scalar() or 0

        expected = await _get_expected_asset_count(db, asset_type, target_date)

        return DateDetailResponse(
            date=date_str,
            table=table_key,
            total_actual=actual,
            total_expected=expected,
            breakdown=[AssetTypeBreakdown(asset_type=asset_type, actual=actual, expected=expected)],
        )

    if code_col is None:
        total_q = text(f"SELECT COUNT(*) FROM {phys_table} WHERE {date_col} = :d")
        total = (await db.execute(total_q, {"d": target_date})).scalar() or 0
        return DateDetailResponse(
            date=date_str,
            table=table_key,
            total_actual=total,
            total_expected=0,
            breakdown=[],
        )

    total_q = text(f"SELECT COUNT(*) FROM {phys_table} WHERE {date_col} = :d")
    total = (await db.execute(total_q, {"d": target_date})).scalar() or 0
    return DateDetailResponse(
        date=date_str,
        table=table_key,
        total_actual=total,
        total_expected=0,
        breakdown=[],
    )


@router.post("/date-backfill", response_model=DateBackfillResponse)
async def trigger_date_backfill(
    request: DateBackfillRequest,
    db: AsyncSession = Depends(get_db),
):
    entry = _find_table(request.table)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown table: {request.table}",
        )

    phys_table = entry[1]
    if phys_table != "market_daily":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Single-date backfill currently only supports market_daily variants",
        )

    try:
        target_date = date.fromisoformat(request.date)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD.",
        )

    valid_types = {"STOCK", "ETF", "INDEX"}
    requested = [t.upper() for t in request.asset_types]
    invalid = set(requested) - valid_types
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid asset_types: {invalid}. Valid: {valid_types}",
        )

    sync_types = [ASSET_TYPE_TO_SYNC[t] for t in requested]

    from workers.source_sync import sync_daily_data_with_source, sync_index_backfill

    try:
        if "INDEX" in requested and len(requested) == 1:
            result = await sync_index_backfill(db, target_date, target_date)
            results = {"INDEX": result.get("total_records", 0)}
            errors = result.get("errors", [])
        elif "INDEX" in requested:
            non_index_types = [ASSET_TYPE_TO_SYNC[t] for t in requested if t != "INDEX"]
            main_result = await sync_daily_data_with_source(
                db, target_date, asset_types=non_index_types
            )
            idx_result = await sync_index_backfill(db, target_date, target_date)

            results = {}
            if "STOCK" in requested:
                results["STOCK"] = main_result.get("stock_count", 0)
            if "ETF" in requested:
                results["ETF"] = main_result.get("etf_count", 0)
            results["INDEX"] = idx_result.get("total_records", 0)
            errors = main_result.get("errors", []) + idx_result.get("errors", [])
        else:
            main_result = await sync_daily_data_with_source(db, target_date, asset_types=sync_types)
            results = {}
            if "STOCK" in requested:
                results["STOCK"] = main_result.get("stock_count", 0)
            if "ETF" in requested:
                results["ETF"] = main_result.get("etf_count", 0)
            errors = main_result.get("errors", [])

    except Exception as e:
        logger.error(f"Date backfill failed for {request.date}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Backfill failed: {str(e)}",
        )

    return DateBackfillResponse(
        date=request.date,
        results=results,
        errors=errors,
    )


@router.post("/sparse-backfill", response_model=SparseBackfillResponse)
async def trigger_sparse_backfill(
    request: SparseBackfillRequest,
    db: AsyncSession = Depends(get_db),
):
    """Backfill sparse dates and re-check counts.

    For each date, re-runs sync_daily_data_with_source (upsert, safe for
    re-runs). After backfill, re-queries actual count and compares to expected.
    If actual improved but still < 95% expected, marks as ``best_effort``
    (likely due to suspended stocks on that day).
    """
    entry = _find_table(request.table)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown table: {request.table}",
        )

    table_key, phys_table, date_col, code_col, _, scope, where_filter = entry
    if phys_table != "market_daily":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sparse backfill currently only supports market_daily variants",
        )

    asset_type = VIRTUAL_TABLE_ASSET_MAP.get(table_key)
    if not asset_type:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Table {table_key} is not a virtual market_daily table",
        )

    sync_type = ASSET_TYPE_TO_SYNC[asset_type]

    if len(request.dates) > 60:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Maximum 60 dates per request",
        )

    from workers.source_sync import sync_daily_data_with_source, sync_index_backfill

    results: List[SparseBackfillDateResult] = []
    all_errors: List[str] = []
    total_improved = 0

    parsed_dates: List[date] = []
    for ds in request.dates:
        try:
            parsed_dates.append(date.fromisoformat(ds))
        except ValueError:
            results.append(
                SparseBackfillDateResult(
                    date=ds,
                    before=0,
                    after=0,
                    expected=0,
                    status="error",
                    message=f"Invalid date: {ds}",
                )
            )

    if not parsed_dates:
        return SparseBackfillResponse(
            table=request.table,
            results=results,
            total_improved=0,
            errors=all_errors,
        )

    if asset_type == "INDEX":
        filter_clause = _build_where(date_col, where_filter, f"{date_col} = :d")
        count_q = text(f"SELECT COUNT(*) FROM {phys_table} WHERE {filter_clause}")

        before_counts = {}
        for d in parsed_dates:
            before_counts[d] = (await db.execute(count_q, {"d": d})).scalar() or 0

        try:
            idx_result = await sync_index_backfill(db, min(parsed_dates), max(parsed_dates))
            backfill_errors = idx_result.get("errors", [])
            all_errors.extend(backfill_errors)
        except Exception as e:
            logger.error(f"Index sparse backfill failed: {e}")
            for d in parsed_dates:
                results.append(
                    SparseBackfillDateResult(
                        date=d.isoformat(),
                        before=before_counts[d],
                        after=before_counts[d],
                        expected=0,
                        status="error",
                        message=str(e)[:200],
                    )
                )
            return SparseBackfillResponse(
                table=request.table,
                results=results,
                total_improved=0,
                errors=[str(e)[:200]],
            )

        for d in parsed_dates:
            after_count = (await db.execute(count_q, {"d": d})).scalar() or 0
            expected = await _get_expected_asset_count(db, asset_type, d)
            before_count = before_counts[d]

            ratio = after_count / expected if expected > 0 else 1.0
            if ratio >= 0.95:
                s = "improved"
                total_improved += 1
            elif after_count > before_count:
                s = "best_effort"
                total_improved += 1
            else:
                s = "unchanged"

            msg = ""
            if s == "best_effort":
                gap = expected - after_count
                msg = f"已补全(部分停牌, 缺{gap}条)"
            elif s == "improved":
                msg = f"已补全 {after_count}/{expected}"

            results.append(
                SparseBackfillDateResult(
                    date=d.isoformat(),
                    before=before_count,
                    after=after_count,
                    expected=expected,
                    status=s,
                    message=msg,
                )
            )

        return SparseBackfillResponse(
            table=request.table,
            results=results,
            total_improved=total_improved,
            errors=all_errors,
        )

    for date_str in request.dates:
        try:
            target_date = date.fromisoformat(date_str)
        except ValueError:
            results.append(
                SparseBackfillDateResult(
                    date=date_str,
                    before=0,
                    after=0,
                    expected=0,
                    status="error",
                    message=f"Invalid date: {date_str}",
                )
            )
            continue

        filter_clause = _build_where(date_col, where_filter, f"{date_col} = :d")
        before_q = text(f"SELECT COUNT(*) FROM {phys_table} WHERE {filter_clause}")
        before_count = (await db.execute(before_q, {"d": target_date})).scalar() or 0

        try:
            await sync_daily_data_with_source(db, target_date, asset_types=[sync_type])
        except Exception as e:
            logger.error(f"Sparse backfill failed for {date_str}: {e}")
            results.append(
                SparseBackfillDateResult(
                    date=date_str,
                    before=before_count,
                    after=before_count,
                    expected=0,
                    status="error",
                    message=str(e)[:200],
                )
            )
            all_errors.append(f"{date_str}: {str(e)[:100]}")
            continue

        after_count = (await db.execute(before_q, {"d": target_date})).scalar() or 0
        expected = await _get_expected_asset_count(db, asset_type, target_date)

        ratio = after_count / expected if expected > 0 else 1.0
        if ratio >= 0.95:
            s = "improved"
            total_improved += 1
        elif after_count > before_count:
            s = "best_effort"
            total_improved += 1
        else:
            s = "unchanged"

        msg = ""
        if s == "best_effort":
            gap = expected - after_count
            msg = f"已补全(部分停牌, 缺{gap}条)"
        elif s == "improved":
            msg = f"已补全 {after_count}/{expected}"

        results.append(
            SparseBackfillDateResult(
                date=date_str,
                before=before_count,
                after=after_count,
                expected=expected,
                status=s,
                message=msg,
            )
        )

    return SparseBackfillResponse(
        table=request.table,
        results=results,
        total_improved=total_improved,
        errors=all_errors,
    )


# ============================================
# Asset Meta Refresh
# ============================================


class AssetMetaRefreshResponse(BaseModel):
    stocks_upserted: int = 0
    indices_upserted: int = 0
    etfs_upserted: int = 0
    errors: List[str] = Field(default_factory=list)


def _classify_board(ts_code: str, market: str = "") -> str:
    code = ts_code.split(".")[0] if "." in ts_code else ts_code
    if code.startswith("688"):
        return "科创板"
    if code.startswith("3"):
        return "创业板"
    if code.startswith("4") or code.startswith("8"):
        return "北交所"
    return "主板"


@router.post("/refresh-asset-meta", response_model=AssetMetaRefreshResponse)
async def refresh_asset_meta(
    db: AsyncSession = Depends(get_db),
):
    import os
    import tushare as ts

    api_key = os.environ.get("TUSHARE_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="TUSHARE_API_KEY not set")

    ts.set_token(api_key)
    pro = ts.pro_api()
    errors: List[str] = []

    upsert_sql = text("""
        INSERT INTO asset_meta (code, name, asset_type, exchange, list_date, delist_date, status, category)
        VALUES (:code, :name, :asset_type, :exchange, :list_date, :delist_date, :status, :category)
        ON CONFLICT (code)
        DO UPDATE SET
            name = EXCLUDED.name,
            list_date = COALESCE(EXCLUDED.list_date, asset_meta.list_date),
            delist_date = EXCLUDED.delist_date,
            status = EXCLUDED.status,
            category = COALESCE(EXCLUDED.category, asset_meta.category),
            updated_at = NOW()
    """)

    from workers.data_sources.base import convert_tushare_code_to_standard

    stocks_count = 0
    try:
        df = pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,list_date,market,is_hs",
        )
        if df is not None and not df.empty:
            rows = []
            for _, r in df.iterrows():
                tc = str(r["ts_code"])
                std_code = convert_tushare_code_to_standard(tc)
                exchange = std_code.split(".")[0]
                ld = None
                if r.get("list_date") and str(r["list_date"]) not in ("", "nan", "None"):
                    try:
                        from datetime import datetime as dt

                        ld = dt.strptime(str(r["list_date"]), "%Y%m%d").date()
                    except (ValueError, TypeError):
                        pass
                rows.append(
                    {
                        "code": std_code,
                        "name": str(r["name"]),
                        "asset_type": "STOCK",
                        "exchange": exchange,
                        "list_date": ld,
                        "delist_date": None,
                        "status": 1,
                        "category": _classify_board(tc, str(r.get("market", ""))),
                    }
                )
            await db.execute(upsert_sql, rows)
            stocks_count = len(rows)
            logger.info(f"Refreshed {stocks_count} stocks from TuShare stock_basic")

        df_d = pro.stock_basic(
            exchange="", list_status="D", fields="ts_code,symbol,name,list_date,delist_date"
        )
        if df_d is not None and not df_d.empty:
            rows_d = []
            for _, r in df_d.iterrows():
                tc = str(r["ts_code"])
                std_code = convert_tushare_code_to_standard(tc)
                exchange = std_code.split(".")[0]
                ld = dd = None
                for field, target in [("list_date", "ld"), ("delist_date", "dd")]:
                    val = r.get(field)
                    if val and str(val) not in ("", "nan", "None"):
                        try:
                            from datetime import datetime as dt

                            parsed = dt.strptime(str(val), "%Y%m%d").date()
                            if field == "list_date":
                                ld = parsed
                            else:
                                dd = parsed
                        except (ValueError, TypeError):
                            pass
                rows_d.append(
                    {
                        "code": std_code,
                        "name": str(r["name"]),
                        "asset_type": "STOCK",
                        "exchange": exchange,
                        "list_date": ld,
                        "delist_date": dd,
                        "status": 0,
                        "category": _classify_board(tc),
                    }
                )
            await db.execute(upsert_sql, rows_d)
            stocks_count += len(rows_d)
    except Exception as e:
        logger.error(f"Failed to refresh stocks: {e}")
        errors.append(f"Stocks: {str(e)[:200]}")

    indices_count = 0
    try:
        for market in ["SSE", "SZSE"]:
            df_idx = pro.index_basic(market=market, fields="ts_code,name,list_date,exp_date")
            if df_idx is not None and not df_idx.empty:
                rows_idx = []
                for _, r in df_idx.iterrows():
                    tc = str(r["ts_code"])
                    std_code = convert_tushare_code_to_standard(tc)
                    exchange = std_code.split(".")[0]
                    ld = None
                    if r.get("list_date") and str(r["list_date"]) not in ("", "nan", "None"):
                        try:
                            from datetime import datetime as dt

                            ld = dt.strptime(str(r["list_date"]), "%Y%m%d").date()
                        except (ValueError, TypeError):
                            pass
                    rows_idx.append(
                        {
                            "code": std_code,
                            "name": str(r["name"]),
                            "asset_type": "INDEX",
                            "exchange": exchange,
                            "list_date": ld,
                            "delist_date": None,
                            "status": 1,
                            "category": "INDEX",
                        }
                    )
                await db.execute(upsert_sql, rows_idx)
                indices_count += len(rows_idx)
        logger.info(f"Refreshed {indices_count} indices from TuShare index_basic")
    except Exception as e:
        logger.error(f"Failed to refresh indices: {e}")
        errors.append(f"Indices: {str(e)[:200]}")

    etfs_count = 0
    try:
        df_etf = pro.fund_basic(
            market="E", status="L", fields="ts_code,name,list_date,delist_date,fund_type"
        )
        if df_etf is not None and not df_etf.empty:
            rows_etf = []
            for _, r in df_etf.iterrows():
                tc = str(r["ts_code"])
                std_code = convert_tushare_code_to_standard(tc)
                exchange = std_code.split(".")[0]
                ld = dd = None
                for field in ["list_date", "delist_date"]:
                    val = r.get(field)
                    if val and str(val) not in ("", "nan", "None"):
                        try:
                            from datetime import datetime as dt

                            parsed = dt.strptime(str(val), "%Y%m%d").date()
                            if field == "list_date":
                                ld = parsed
                            else:
                                dd = parsed
                        except (ValueError, TypeError):
                            pass
                rows_etf.append(
                    {
                        "code": std_code,
                        "name": str(r["name"]),
                        "asset_type": "ETF",
                        "exchange": exchange,
                        "list_date": ld,
                        "delist_date": dd,
                        "status": 1,
                        "category": str(r.get("fund_type", "ETF")),
                    }
                )
            await db.execute(upsert_sql, rows_etf)
            etfs_count = len(rows_etf)

        df_etf_d = pro.fund_basic(
            market="E", status="D", fields="ts_code,name,list_date,delist_date,fund_type"
        )
        if df_etf_d is not None and not df_etf_d.empty:
            rows_etf_d = []
            for _, r in df_etf_d.iterrows():
                tc = str(r["ts_code"])
                std_code = convert_tushare_code_to_standard(tc)
                exchange = std_code.split(".")[0]
                ld = dd = None
                for field in ["list_date", "delist_date"]:
                    val = r.get(field)
                    if val and str(val) not in ("", "nan", "None"):
                        try:
                            from datetime import datetime as dt

                            parsed = dt.strptime(str(val), "%Y%m%d").date()
                            if field == "list_date":
                                ld = parsed
                            else:
                                dd = parsed
                        except (ValueError, TypeError):
                            pass
                rows_etf_d.append(
                    {
                        "code": std_code,
                        "name": str(r["name"]),
                        "asset_type": "ETF",
                        "exchange": exchange,
                        "list_date": ld,
                        "delist_date": dd,
                        "status": 0,
                        "category": str(r.get("fund_type", "ETF")),
                    }
                )
            await db.execute(upsert_sql, rows_etf_d)
            etfs_count += len(rows_etf_d)
        logger.info(f"Refreshed {etfs_count} ETFs from TuShare fund_basic")
    except Exception as e:
        logger.error(f"Failed to refresh ETFs: {e}")
        errors.append(f"ETFs: {str(e)[:200]}")

    await db.commit()

    return AssetMetaRefreshResponse(
        stocks_upserted=stocks_count,
        indices_upserted=indices_count,
        etfs_upserted=etfs_count,
        errors=errors,
    )
