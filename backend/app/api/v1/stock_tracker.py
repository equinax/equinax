import asyncio
import uuid
from datetime import date, datetime, time
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func, and_, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.stock_tracker import (
    StockTrack,
    TrackDailyEntry,
    DailySketchPoints,
    DailySituationScore,
    TradeOperation,
    BaostockMinuteCache,
)
from app.db.models.asset import MarketDaily
from app.services.baostock_service import fetch_minute_data_from_baostock, store_minute_data

router = APIRouter()


class StockTrackCreate(BaseModel):
    ts_code: str = Field(..., min_length=1, max_length=20)
    stock_name: Optional[str] = None
    watch_reason: Optional[str] = None
    sector: Optional[str] = None
    tags: Optional[List[str]] = None


class StockTrackRead(BaseModel):
    id: str
    ts_code: str
    stock_name: Optional[str]
    watch_reason: Optional[str]
    sector: Optional[str]
    tags: Optional[List[str]]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TrackDailyEntryRead(BaseModel):
    id: str
    track_id: str
    trade_date: date
    ts_code: str
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    pre_close: Optional[float]
    volume: Optional[float]
    amount: Optional[float]
    pct_chg: Optional[float]
    pattern: Optional[str]
    notes: Optional[str]
    mood: Optional[str]
    is_draft: bool = False
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TimelineDayRead(BaseModel):
    """A trading day in the timeline - may or may not have an entry."""

    trade_date: date
    ts_code: str
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    pre_close: Optional[float] = None
    volume: Optional[float] = None
    amount: Optional[float] = None
    pct_chg: Optional[float] = None
    entry_id: Optional[str] = None
    track_id: Optional[str] = None
    pattern: Optional[str] = None
    notes: Optional[str] = None
    mood: Optional[str] = None
    has_entry: bool = False
    is_draft: bool = False
    key_points: Optional[Dict[str, Any]] = None
    scores_summary: Optional[Dict[str, int]] = None


class TrackDailyEntryUpdate(BaseModel):
    notes: Optional[str] = None
    mood: Optional[str] = None
    pattern: Optional[str] = None


class SketchPointsUpsert(BaseModel):
    key_points: Dict[str, Any]


class SketchPointsRead(BaseModel):
    id: str
    entry_id: str
    key_points: Dict[str, Any]
    auto_pattern: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class SituationScoreUpsert(BaseModel):
    scores: Dict[str, Any]


class SituationScoreRead(BaseModel):
    id: str
    entry_id: str
    scores: Dict[str, Any]
    total_score: int
    market_label: Optional[str]
    sector_label: Optional[str]
    stock_label: Optional[str]
    decision: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TradeOperationCreate(BaseModel):
    op_type: str = Field(..., description="buy, sell, add, or reduce")
    op_time: Optional[str] = None
    price: Optional[float] = None
    quantity: Optional[int] = None
    emotion: Optional[str] = None
    notes: Optional[str] = None


class TradeOperationRead(BaseModel):
    id: str
    entry_id: str
    op_type: str
    op_time: Optional[str]
    price: Optional[float]
    quantity: Optional[int]
    emotion: Optional[str]
    notes: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TradeOperationUpdate(BaseModel):
    op_type: Optional[str] = None
    op_time: Optional[str] = None
    price: Optional[float] = None
    quantity: Optional[int] = None
    emotion: Optional[str] = None
    notes: Optional[str] = None


def _track_to_response(t: StockTrack) -> StockTrackRead:
    return StockTrackRead(
        id=str(t.id),
        ts_code=t.ts_code,
        stock_name=t.stock_name,
        watch_reason=t.watch_reason,
        sector=t.sector,
        tags=t.tags,
        is_active=t.is_active,
        created_at=t.created_at,
        updated_at=t.updated_at,
    )


def _entry_to_response(e: TrackDailyEntry) -> TrackDailyEntryRead:
    return TrackDailyEntryRead(
        id=str(e.id),
        track_id=str(e.track_id),
        trade_date=e.trade_date,
        ts_code=e.ts_code,
        open=float(e.open) if e.open is not None else None,
        high=float(e.high) if e.high is not None else None,
        low=float(e.low) if e.low is not None else None,
        close=float(e.close) if e.close is not None else None,
        pre_close=float(e.pre_close) if e.pre_close is not None else None,
        volume=float(e.volume) if e.volume is not None else None,
        amount=float(e.amount) if e.amount is not None else None,
        pct_chg=float(e.pct_chg) if e.pct_chg is not None else None,
        pattern=e.pattern,
        notes=e.notes,
        mood=e.mood,
        is_draft=e.is_draft,
        created_at=e.created_at,
        updated_at=e.updated_at,
    )


def _compute_score_labels(scores: Dict[str, Any]) -> tuple:
    market_sub = sum(scores.get("market", {}).values())
    sector_sub = sum(scores.get("sector", {}).values())
    stock_sub = sum(scores.get("stock", {}).values())
    total = market_sub + sector_sub + stock_sub

    if market_sub >= 20:
        market_label = "偏多"
    elif market_sub >= 10:
        market_label = "震荡"
    else:
        market_label = "偏空"

    if sector_sub >= 20:
        sector_label = "强"
    elif sector_sub >= 10:
        sector_label = "一般"
    else:
        sector_label = "弱"

    if stock_sub >= 30:
        stock_label = "强"
    elif stock_sub >= 15:
        stock_label = "震荡"
    else:
        stock_label = "弱"

    if total >= 70:
        decision = "做多日"
    elif total >= 40:
        decision = "震荡日"
    else:
        decision = "回撤日"

    return total, market_label, sector_label, stock_label, decision


# --- Track endpoints ---


@router.get("/tracks", response_model=List[StockTrackRead])
async def list_tracks(
    active_only: bool = Query(default=True),
    db: AsyncSession = Depends(get_db),
):
    query = select(StockTrack).order_by(StockTrack.created_at.desc())
    if active_only:
        query = query.where(StockTrack.is_active == True)  # noqa: E712
    result = await db.execute(query)
    tracks = result.scalars().all()
    return [_track_to_response(t) for t in tracks]


@router.post("/tracks", response_model=StockTrackRead, status_code=status.HTTP_201_CREATED)
async def create_track(
    body: StockTrackCreate,
    db: AsyncSession = Depends(get_db),
):
    existing = await db.execute(select(StockTrack).where(StockTrack.ts_code == body.ts_code))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail=f"Track for {body.ts_code} already exists")

    track = StockTrack(
        ts_code=body.ts_code,
        stock_name=body.stock_name,
        watch_reason=body.watch_reason,
        sector=body.sector,
        tags=body.tags,
    )
    db.add(track)
    await db.commit()
    await db.refresh(track)
    return _track_to_response(track)


@router.delete("/tracks/{ts_code}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_track(
    ts_code: str,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(StockTrack).where(StockTrack.ts_code == ts_code))
    track = result.scalar_one_or_none()
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")
    await db.delete(track)
    await db.commit()


@router.get("/tracks/{ts_code}/timeline", response_model=List[TimelineDayRead])
async def get_timeline(
    ts_code: str,
    days: int = Query(default=20, ge=1, le=120),
    before: Optional[date] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(StockTrack).where(StockTrack.ts_code == ts_code))
    track = result.scalar_one_or_none()
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")

    market_query = (
        select(MarketDaily)
        .where(MarketDaily.code == ts_code)
        .order_by(MarketDaily.date.desc())
        .limit(days)
    )
    if before:
        market_query = market_query.where(MarketDaily.date < before)

    market_result = await db.execute(market_query)
    market_rows = market_result.scalars().all()

    if not market_rows:
        return []

    trade_dates = [row.date for row in market_rows]
    entry_result = await db.execute(
        select(TrackDailyEntry).where(
            TrackDailyEntry.track_id == track.id,
            TrackDailyEntry.trade_date.in_(trade_dates),
        )
    )
    entries_by_date = {e.trade_date: e for e in entry_result.scalars().all()}

    entry_ids = [e.id for e in entries_by_date.values()]
    sketch_by_entry: Dict[uuid.UUID, DailySketchPoints] = {}
    scores_by_entry: Dict[uuid.UUID, DailySituationScore] = {}

    if entry_ids:
        sketch_result = await db.execute(
            select(DailySketchPoints).where(DailySketchPoints.entry_id.in_(entry_ids))
        )
        sketch_by_entry = {s.entry_id: s for s in sketch_result.scalars().all()}

        scores_result = await db.execute(
            select(DailySituationScore).where(DailySituationScore.entry_id.in_(entry_ids))
        )
        scores_by_entry = {s.entry_id: s for s in scores_result.scalars().all()}

    timeline: List[TimelineDayRead] = []
    for row in market_rows:
        entry = entries_by_date.get(row.date)

        kp = None
        ss = None
        if entry:
            sketch = sketch_by_entry.get(entry.id)
            if sketch:
                kp = sketch.key_points

            score = scores_by_entry.get(entry.id)
            if score:
                ss = {
                    "market": sum(score.scores.get("market", {}).values()),
                    "sector": sum(score.scores.get("sector", {}).values()),
                    "stock": sum(score.scores.get("stock", {}).values()),
                }

        timeline.append(
            TimelineDayRead(
                trade_date=row.date,
                ts_code=ts_code,
                open=float(row.open) if row.open is not None else None,
                high=float(row.high) if row.high is not None else None,
                low=float(row.low) if row.low is not None else None,
                close=float(row.close) if row.close is not None else None,
                pre_close=float(row.preclose) if row.preclose is not None else None,
                volume=float(row.volume) if row.volume is not None else None,
                amount=float(row.amount) if row.amount is not None else None,
                pct_chg=float(row.pct_chg) if row.pct_chg is not None else None,
                entry_id=str(entry.id) if entry else None,
                track_id=str(track.id) if entry else None,
                pattern=entry.pattern if entry else None,
                notes=entry.notes if entry else None,
                mood=entry.mood if entry else None,
                has_entry=entry is not None,
                is_draft=entry.is_draft if entry else False,
                key_points=kp,
                scores_summary=ss,
            )
        )

    return timeline


@router.post("/tracks/{ts_code}/sync-daily", response_model=Dict[str, Any])
async def sync_daily(
    ts_code: str,
    days: int = Query(default=60, ge=1, le=365),
    before: Optional[date] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(StockTrack).where(StockTrack.ts_code == ts_code))
    track = result.scalar_one_or_none()
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")

    market_query = (
        select(MarketDaily)
        .where(MarketDaily.code == ts_code)
        .order_by(MarketDaily.date.desc())
        .limit(days)
    )
    if before:
        market_query = market_query.where(MarketDaily.date < before)
    market_result = await db.execute(market_query)
    market_rows = market_result.scalars().all()

    created = 0
    updated = 0
    for row in market_rows:
        existing = await db.execute(
            select(TrackDailyEntry).where(
                TrackDailyEntry.track_id == track.id,
                TrackDailyEntry.trade_date == row.date,
            )
        )
        entry = existing.scalar_one_or_none()
        if entry:
            entry.open = row.open
            entry.high = row.high
            entry.low = row.low
            entry.close = row.close
            entry.pre_close = row.preclose
            entry.volume = row.volume
            entry.amount = row.amount
            entry.pct_chg = row.pct_chg
            entry.is_draft = False
            updated += 1
        else:
            entry = TrackDailyEntry(
                track_id=track.id,
                trade_date=row.date,
                ts_code=ts_code,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                pre_close=row.preclose,
                volume=row.volume,
                amount=row.amount,
                pct_chg=row.pct_chg,
            )
            db.add(entry)
            created += 1

    await db.commit()
    return {"created": created, "updated": updated, "total_market_rows": len(market_rows)}


@router.post("/tracks/{ts_code}/entries/create-draft", response_model=TrackDailyEntryRead)
async def create_draft_entry(
    ts_code: str,
    trade_date: date = Query(...),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(StockTrack).where(StockTrack.ts_code == ts_code))
    track = result.scalar_one_or_none()
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")

    existing = await db.execute(
        select(TrackDailyEntry).where(
            TrackDailyEntry.track_id == track.id,
            TrackDailyEntry.trade_date == trade_date,
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="该日期已有记录")

    entry = TrackDailyEntry(
        track_id=track.id,
        trade_date=trade_date,
        ts_code=ts_code,
        is_draft=True,
    )
    db.add(entry)
    await db.commit()
    await db.refresh(entry)

    return _entry_to_response(entry)


@router.post("/tracks/{ts_code}/entries/create-for-date", response_model=TrackDailyEntryRead)
async def create_entry_for_date(
    ts_code: str,
    trade_date: date = Query(...),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(StockTrack).where(StockTrack.ts_code == ts_code))
    track = result.scalar_one_or_none()
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")

    existing = await db.execute(
        select(TrackDailyEntry).where(
            TrackDailyEntry.track_id == track.id,
            TrackDailyEntry.trade_date == trade_date,
        )
    )
    entry = existing.scalar_one_or_none()
    if entry:
        return TrackDailyEntryRead(
            id=str(entry.id),
            track_id=str(entry.track_id),
            trade_date=entry.trade_date,
            ts_code=entry.ts_code,
            open=float(entry.open) if entry.open is not None else None,
            high=float(entry.high) if entry.high is not None else None,
            low=float(entry.low) if entry.low is not None else None,
            close=float(entry.close) if entry.close is not None else None,
            pre_close=float(entry.pre_close) if entry.pre_close is not None else None,
            volume=float(entry.volume) if entry.volume is not None else None,
            amount=float(entry.amount) if entry.amount is not None else None,
            pct_chg=float(entry.pct_chg) if entry.pct_chg is not None else None,
            pattern=entry.pattern,
            notes=entry.notes,
            mood=entry.mood,
            is_draft=entry.is_draft,
            created_at=entry.created_at,
            updated_at=entry.updated_at,
        )

    market_result = await db.execute(
        select(MarketDaily).where(
            MarketDaily.code == ts_code,
            MarketDaily.date == trade_date,
        )
    )
    market_row = market_result.scalar_one_or_none()
    if not market_row:
        raise HTTPException(
            status_code=404,
            detail=f"No market data found for {ts_code} on {trade_date}",
        )

    entry = TrackDailyEntry(
        track_id=track.id,
        trade_date=trade_date,
        ts_code=ts_code,
        open=market_row.open,
        high=market_row.high,
        low=market_row.low,
        close=market_row.close,
        pre_close=market_row.preclose,
        volume=market_row.volume,
        amount=market_row.amount,
        pct_chg=market_row.pct_chg,
    )
    db.add(entry)
    await db.commit()
    await db.refresh(entry)

    return TrackDailyEntryRead(
        id=str(entry.id),
        track_id=str(entry.track_id),
        trade_date=entry.trade_date,
        ts_code=entry.ts_code,
        open=float(entry.open) if entry.open is not None else None,
        high=float(entry.high) if entry.high is not None else None,
        low=float(entry.low) if entry.low is not None else None,
        close=float(entry.close) if entry.close is not None else None,
        pre_close=float(entry.pre_close) if entry.pre_close is not None else None,
        volume=float(entry.volume) if entry.volume is not None else None,
        amount=float(entry.amount) if entry.amount is not None else None,
        pct_chg=float(entry.pct_chg) if entry.pct_chg is not None else None,
        pattern=entry.pattern,
        notes=entry.notes,
        mood=entry.mood,
        is_draft=entry.is_draft,
        created_at=entry.created_at,
        updated_at=entry.updated_at,
    )


# --- Entry endpoints ---


def _parse_entry_id(entry_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(entry_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid entry ID format")


async def _get_entry(entry_id: str, db: AsyncSession) -> TrackDailyEntry:
    eid = _parse_entry_id(entry_id)
    result = await db.execute(select(TrackDailyEntry).where(TrackDailyEntry.id == eid))
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found")
    return entry


@router.get("/entries/{entry_id}", response_model=TrackDailyEntryRead)
async def get_entry(
    entry_id: str,
    db: AsyncSession = Depends(get_db),
):
    entry = await _get_entry(entry_id, db)
    return _entry_to_response(entry)


@router.patch("/entries/{entry_id}", response_model=TrackDailyEntryRead)
async def update_entry(
    entry_id: str,
    body: TrackDailyEntryUpdate,
    db: AsyncSession = Depends(get_db),
):
    entry = await _get_entry(entry_id, db)
    if body.notes is not None:
        entry.notes = body.notes
    if body.mood is not None:
        entry.mood = body.mood
    if body.pattern is not None:
        entry.pattern = body.pattern
    await db.commit()
    await db.refresh(entry)
    return _entry_to_response(entry)


# --- Sketch endpoints ---


@router.get("/entries/{entry_id}/sketch", response_model=Optional[SketchPointsRead])
async def get_sketch(
    entry_id: str,
    db: AsyncSession = Depends(get_db),
):
    entry = await _get_entry(entry_id, db)
    result = await db.execute(
        select(DailySketchPoints).where(DailySketchPoints.entry_id == entry.id)
    )
    sketch = result.scalar_one_or_none()
    if not sketch:
        return None
    return SketchPointsRead(
        id=str(sketch.id),
        entry_id=str(sketch.entry_id),
        key_points=sketch.key_points,
        auto_pattern=sketch.auto_pattern,
        created_at=sketch.created_at,
        updated_at=sketch.updated_at,
    )


@router.put("/entries/{entry_id}/sketch", response_model=SketchPointsRead)
async def upsert_sketch(
    entry_id: str,
    body: SketchPointsUpsert,
    db: AsyncSession = Depends(get_db),
):
    entry = await _get_entry(entry_id, db)

    auto_pattern = None
    try:
        from app.services.pattern_recognition import recognize_pattern

        auto_pattern = recognize_pattern(
            body.key_points,
            real_open=float(entry.open) if entry.open is not None else None,
            real_high=float(entry.high) if entry.high is not None else None,
            real_low=float(entry.low) if entry.low is not None else None,
            real_close=float(entry.close) if entry.close is not None else None,
        )
    except Exception:
        pass

    result = await db.execute(
        select(DailySketchPoints).where(DailySketchPoints.entry_id == entry.id)
    )
    sketch = result.scalar_one_or_none()

    if sketch:
        sketch.key_points = body.key_points
        sketch.auto_pattern = auto_pattern
    else:
        sketch = DailySketchPoints(
            entry_id=entry.id,
            key_points=body.key_points,
            auto_pattern=auto_pattern,
        )
        db.add(sketch)

    if auto_pattern:
        entry.pattern = auto_pattern

    await db.commit()
    await db.refresh(sketch)
    return SketchPointsRead(
        id=str(sketch.id),
        entry_id=str(sketch.entry_id),
        key_points=sketch.key_points,
        auto_pattern=sketch.auto_pattern,
        created_at=sketch.created_at,
        updated_at=sketch.updated_at,
    )


# --- Score endpoints ---


@router.get("/entries/{entry_id}/scores", response_model=Optional[SituationScoreRead])
async def get_scores(
    entry_id: str,
    db: AsyncSession = Depends(get_db),
):
    entry = await _get_entry(entry_id, db)
    result = await db.execute(
        select(DailySituationScore).where(DailySituationScore.entry_id == entry.id)
    )
    score = result.scalar_one_or_none()
    if not score:
        return None
    return SituationScoreRead(
        id=str(score.id),
        entry_id=str(score.entry_id),
        scores=score.scores,
        total_score=score.total_score,
        market_label=score.market_label,
        sector_label=score.sector_label,
        stock_label=score.stock_label,
        decision=score.decision,
        created_at=score.created_at,
        updated_at=score.updated_at,
    )


@router.put("/entries/{entry_id}/scores", response_model=SituationScoreRead)
async def upsert_scores(
    entry_id: str,
    body: SituationScoreUpsert,
    db: AsyncSession = Depends(get_db),
):
    entry = await _get_entry(entry_id, db)
    total, market_label, sector_label, stock_label, decision = _compute_score_labels(body.scores)

    result = await db.execute(
        select(DailySituationScore).where(DailySituationScore.entry_id == entry.id)
    )
    score = result.scalar_one_or_none()

    if score:
        score.scores = body.scores
        score.total_score = total
        score.market_label = market_label
        score.sector_label = sector_label
        score.stock_label = stock_label
        score.decision = decision
    else:
        score = DailySituationScore(
            entry_id=entry.id,
            scores=body.scores,
            total_score=total,
            market_label=market_label,
            sector_label=sector_label,
            stock_label=stock_label,
            decision=decision,
        )
        db.add(score)

    await db.commit()
    await db.refresh(score)
    return SituationScoreRead(
        id=str(score.id),
        entry_id=str(score.entry_id),
        scores=score.scores,
        total_score=score.total_score,
        market_label=score.market_label,
        sector_label=score.sector_label,
        stock_label=score.stock_label,
        decision=score.decision,
        created_at=score.created_at,
        updated_at=score.updated_at,
    )


# --- Operation endpoints ---


@router.get("/entries/{entry_id}/operations", response_model=List[TradeOperationRead])
async def list_operations(
    entry_id: str,
    db: AsyncSession = Depends(get_db),
):
    entry = await _get_entry(entry_id, db)
    result = await db.execute(
        select(TradeOperation)
        .where(TradeOperation.entry_id == entry.id)
        .order_by(TradeOperation.op_time.asc().nullslast())
    )
    ops = result.scalars().all()
    return [
        TradeOperationRead(
            id=str(op.id),
            entry_id=str(op.entry_id),
            op_type=op.op_type,
            op_time=op.op_time.strftime("%H:%M") if op.op_time else None,
            price=float(op.price) if op.price is not None else None,
            quantity=op.quantity,
            emotion=op.emotion,
            notes=op.notes,
            created_at=op.created_at,
            updated_at=op.updated_at,
        )
        for op in ops
    ]


@router.post(
    "/entries/{entry_id}/operations",
    response_model=TradeOperationRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_operation(
    entry_id: str,
    body: TradeOperationCreate,
    db: AsyncSession = Depends(get_db),
):
    entry = await _get_entry(entry_id, db)

    valid_types = {"buy", "sell", "add", "reduce"}
    if body.op_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"op_type must be one of {valid_types}")

    op_time_parsed = None
    if body.op_time:
        try:
            parts = body.op_time.split(":")
            op_time_parsed = time(int(parts[0]), int(parts[1]))
        except (ValueError, IndexError):
            raise HTTPException(status_code=400, detail="op_time must be HH:MM format")

    op = TradeOperation(
        entry_id=entry.id,
        op_type=body.op_type,
        op_time=op_time_parsed,
        price=body.price,
        quantity=body.quantity,
        emotion=body.emotion,
        notes=body.notes,
    )
    db.add(op)
    await db.commit()
    await db.refresh(op)
    return TradeOperationRead(
        id=str(op.id),
        entry_id=str(op.entry_id),
        op_type=op.op_type,
        op_time=op.op_time.strftime("%H:%M") if op.op_time else None,
        price=float(op.price) if op.price is not None else None,
        quantity=op.quantity,
        emotion=op.emotion,
        notes=op.notes,
        created_at=op.created_at,
        updated_at=op.updated_at,
    )


def _parse_op_id(op_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(op_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid operation ID format")


@router.patch("/operations/{op_id}", response_model=TradeOperationRead)
async def update_operation(
    op_id: str,
    body: TradeOperationUpdate,
    db: AsyncSession = Depends(get_db),
):
    oid = _parse_op_id(op_id)
    result = await db.execute(select(TradeOperation).where(TradeOperation.id == oid))
    op = result.scalar_one_or_none()
    if not op:
        raise HTTPException(status_code=404, detail="Operation not found")

    if body.op_type is not None:
        valid_types = {"buy", "sell", "add", "reduce"}
        if body.op_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"op_type must be one of {valid_types}")
        op.op_type = body.op_type
    if body.op_time is not None:
        try:
            parts = body.op_time.split(":")
            op.op_time = time(int(parts[0]), int(parts[1]))
        except (ValueError, IndexError):
            raise HTTPException(status_code=400, detail="op_time must be HH:MM format")
    if body.price is not None:
        op.price = body.price
    if body.quantity is not None:
        op.quantity = body.quantity
    if body.emotion is not None:
        op.emotion = body.emotion
    if body.notes is not None:
        op.notes = body.notes

    await db.commit()
    await db.refresh(op)
    return TradeOperationRead(
        id=str(op.id),
        entry_id=str(op.entry_id),
        op_type=op.op_type,
        op_time=op.op_time.strftime("%H:%M") if op.op_time else None,
        price=float(op.price) if op.price is not None else None,
        quantity=op.quantity,
        emotion=op.emotion,
        notes=op.notes,
        created_at=op.created_at,
        updated_at=op.updated_at,
    )


@router.delete("/operations/{op_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_operation(
    op_id: str,
    db: AsyncSession = Depends(get_db),
):
    oid = _parse_op_id(op_id)
    result = await db.execute(select(TradeOperation).where(TradeOperation.id == oid))
    op = result.scalar_one_or_none()
    if not op:
        raise HTTPException(status_code=404, detail="Operation not found")
    await db.delete(op)
    await db.commit()


# --- Minute data endpoint ---


@router.get("/entries/{entry_id}/minute-data")
async def get_minute_data(
    entry_id: str,
    freq: str = Query(default="5", description="Frequency: 5, 15, 30, 60"),
    db: AsyncSession = Depends(get_db),
):
    entry = await _get_entry(entry_id, db)

    result = await db.execute(
        select(BaostockMinuteCache).where(
            BaostockMinuteCache.ts_code == entry.ts_code,
            BaostockMinuteCache.trade_date == entry.trade_date,
            BaostockMinuteCache.frequency == freq,
        )
    )
    cache = result.scalar_one_or_none()

    # Invalidate stale cache for today — intraday data keeps updating
    is_today = entry.trade_date == date.today()
    if cache and is_today:
        await db.delete(cache)
        await db.flush()
        cache = None

    if cache:
        return {
            "ts_code": cache.ts_code,
            "trade_date": str(cache.trade_date),
            "frequency": cache.frequency,
            "candles": cache.candles,
            "fetched_at": cache.fetched_at,
        }

    # baostock uses blocking I/O (bs.login/query/logout) — must offload to thread
    try:
        candles = await asyncio.to_thread(
            fetch_minute_data_from_baostock, entry.ts_code, entry.trade_date, freq
        )
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Minute data service unavailable",
        )

    if candles:
        stored = await store_minute_data(db, entry.ts_code, entry.trade_date, freq, candles)
        await db.commit()
        await db.refresh(stored)
        return {
            "ts_code": stored.ts_code,
            "trade_date": str(stored.trade_date),
            "frequency": stored.frequency,
            "candles": stored.candles,
            "fetched_at": stored.fetched_at,
        }

    return {
        "ts_code": entry.ts_code,
        "trade_date": str(entry.trade_date),
        "frequency": freq,
        "candles": [],
        "fetched_at": None,
    }
