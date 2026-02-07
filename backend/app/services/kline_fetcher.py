"""K线数据按需拉取服务 - 前端滚动时自动拉取缺失历史数据"""

import logging
from datetime import date
from decimal import Decimal
from typing import List, Optional, Set

from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.asset import AssetMeta, AssetType, MarketDaily

logger = logging.getLogger(__name__)


class KlineFetcher:
    """
    K线数据拉取器

    支持按需拉取单只资产的历史K线数据。
    """

    def __init__(self):
        self._tushare = None

    def _get_tushare(self):
        """懒加载 TuShare 数据源"""
        if self._tushare is None:
            from workers.data_sources import get_data_source

            self._tushare = get_data_source()
        return self._tushare

    async def get_existing_dates(
        self, db: AsyncSession, code: str, start_date: date, end_date: date
    ) -> Set[date]:
        """
        获取数据库中已有的交易日期

        Args:
            db: 数据库会话
            code: 资产代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            已存在的日期集合
        """
        result = await db.execute(
            select(MarketDaily.date).where(
                and_(
                    MarketDaily.code == code,
                    MarketDaily.date >= start_date,
                    MarketDaily.date <= end_date,
                )
            )
        )
        return {row[0] for row in result.all()}

    async def get_asset_type(self, db: AsyncSession, code: str) -> Optional[AssetType]:
        """获取资产类型"""
        result = await db.execute(select(AssetMeta.asset_type).where(AssetMeta.code == code))
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return row if isinstance(row, AssetType) else AssetType(row)

    def _convert_to_tushare_code(self, code: str) -> str:
        """
        将标准代码转换为 TuShare 格式

        标准格式: sh.600000, sz.000001
        TuShare: 600000.SH, 000001.SZ
        """
        if code.startswith("sh."):
            return code[3:] + ".SH"
        elif code.startswith("sz."):
            return code[3:] + ".SZ"
        elif code.startswith("bj."):
            return code[3:] + ".BJ"
        return code

    def _convert_from_tushare_code(self, ts_code: str) -> str:
        """
        将 TuShare 代码转换为标准格式

        TuShare: 600000.SH, 000001.SZ
        标准格式: sh.600000, sz.000001
        """
        if ts_code.endswith(".SH"):
            return "sh." + ts_code[:-3]
        elif ts_code.endswith(".SZ"):
            return "sz." + ts_code[:-3]
        elif ts_code.endswith(".BJ"):
            return "bj." + ts_code[:-3]
        return ts_code

    async def fetch_and_store_kline(
        self,
        db: AsyncSession,
        code: str,
        start_date: date,
        end_date: date,
    ) -> int:
        """
        拉取并存储 K 线数据

        1. 查询数据库中已有的日期
        2. 获取交易日历
        3. 计算缺失的交易日
        4. 从 TuShare 拉取缺失数据
        5. 存入数据库

        Args:
            db: 数据库会话
            code: 资产代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            新增的记录数
        """
        # 1. 获取资产类型
        asset_type = await self.get_asset_type(db, code)
        if asset_type is None:
            logger.warning(f"Asset {code} not found in database")
            return 0

        # 2. 获取已有数据的日期
        existing_dates = await self.get_existing_dates(db, code, start_date, end_date)
        logger.info(f"Found {len(existing_dates)} existing dates for {code}")

        # 3. 获取交易日历
        tushare = self._get_tushare()
        trading_days = set(tushare.get_trading_days(start_date, end_date))
        logger.info(f"Trading days in range: {len(trading_days)}")

        # 4. 计算缺失日期
        missing_dates = trading_days - existing_dates
        if not missing_dates:
            logger.info(f"No missing dates for {code}")
            return 0

        logger.info(f"Missing {len(missing_dates)} dates for {code}")

        # 5. 拉取缺失数据
        records = await self._fetch_kline_from_tushare(code, asset_type, missing_dates)

        if not records:
            logger.info(f"No data fetched for {code}")
            return 0

        # 6. 存入数据库
        inserted_count = await self._store_kline_records(db, records)
        logger.info(f"Inserted {inserted_count} records for {code}")

        return inserted_count

    async def _fetch_kline_from_tushare(
        self, code: str, asset_type: AssetType, missing_dates: Set[date]
    ) -> List[dict]:
        """
        从 TuShare 拉取 K 线数据

        根据资产类型调用不同的 API。
        """
        import time

        import pandas as pd
        import tushare as ts

        tushare = self._get_tushare()
        ts_code = self._convert_to_tushare_code(code)

        # 按日期范围查询（而不是逐日查询，更高效）
        sorted_dates = sorted(missing_dates)
        start_date = sorted_dates[0]
        end_date = sorted_dates[-1]

        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        logger.info(f"Fetching {code} ({ts_code}) from {start_str} to {end_str}")

        records = []
        pro = ts.pro_api()

        try:
            if asset_type == AssetType.STOCK:
                # 股票: pro.daily(ts_code=xxx, start_date=xxx, end_date=xxx)
                df = pro.daily(ts_code=ts_code, start_date=start_str, end_date=end_str)
            elif asset_type == AssetType.ETF:
                # ETF: pro.fund_daily(ts_code=xxx, start_date=xxx, end_date=xxx)
                df = pro.fund_daily(ts_code=ts_code, start_date=start_str, end_date=end_str)
            elif asset_type == AssetType.INDEX:
                # 指数: pro.index_daily(ts_code=xxx, start_date=xxx, end_date=xxx)
                df = pro.index_daily(ts_code=ts_code, start_date=start_str, end_date=end_str)
            else:
                logger.warning(f"Unsupported asset type: {asset_type}")
                return []

            if df is None or df.empty:
                logger.info(f"No data returned for {code}")
                return []

            logger.info(f"Fetched {len(df)} rows from TuShare for {code}")

            # 转换为记录
            for _, row in df.iterrows():
                ts = pd.Timestamp(str(row["trade_date"]))
                trade_date = ts.to_pydatetime().date()

                # 只保留缺失的日期
                if trade_date not in missing_dates:
                    continue

                vol_raw = self._safe_int(row.get("vol"))
                amt_raw = self._safe_decimal(row.get("amount"))

                record = {
                    "code": code,
                    "date": trade_date,
                    "open": self._safe_decimal(row.get("open")),
                    "high": self._safe_decimal(row.get("high")),
                    "low": self._safe_decimal(row.get("low")),
                    "close": self._safe_decimal(row.get("close")),
                    "preclose": self._safe_decimal(row.get("pre_close")),
                    "volume": vol_raw * 100 if vol_raw is not None else None,
                    "amount": amt_raw * 1000 if amt_raw is not None else None,
                    "pct_chg": self._safe_decimal(row.get("pct_chg")),
                    "turn": None,
                }
                records.append(record)

            # 尝试获取换手率 (daily_basic)
            if asset_type == AssetType.STOCK and records:
                time.sleep(0.2)  # Rate limit
                try:
                    basic_df = pro.daily_basic(
                        ts_code=ts_code, start_date=start_str, end_date=end_str
                    )
                    if basic_df is not None and not basic_df.empty:
                        turn_map = {}
                        for _, brow in basic_df.iterrows():
                            bts = pd.Timestamp(str(brow["trade_date"]))
                            turn_map[bts.to_pydatetime().date()] = self._safe_decimal(
                                brow.get("turnover_rate")
                            )

                        for record in records:
                            if record["date"] in turn_map:
                                record["turn"] = turn_map[record["date"]]
                except Exception as e:
                    logger.warning(f"Failed to fetch turnover for {code}: {e}")

        except Exception as e:
            logger.error(f"Error fetching {code} from TuShare: {e}")
            return []

        return records

    async def _store_kline_records(self, db: AsyncSession, records: List[dict]) -> int:
        """
        存储 K 线记录到数据库

        使用 upsert (ON CONFLICT DO UPDATE) 避免重复插入。
        """
        if not records:
            return 0

        stmt = pg_insert(MarketDaily).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["code", "date"],
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "preclose": stmt.excluded.preclose,
                "volume": stmt.excluded.volume,
                "amount": stmt.excluded.amount,
                "pct_chg": stmt.excluded.pct_chg,
                "turn": stmt.excluded.turn,
            },
        )

        await db.execute(stmt)
        await db.commit()

        return len(records)

    def _safe_decimal(self, value) -> Optional[Decimal]:
        """安全转换为 Decimal"""
        import pandas as pd

        if pd.isna(value) or value == "" or value == "-":
            return None
        try:
            return Decimal(str(value))
        except:
            return None

    def _safe_int(self, value) -> Optional[int]:
        """安全转换为 int"""
        import pandas as pd

        if pd.isna(value) or value == "" or value == "-":
            return None
        try:
            f = float(value)
            if pd.isna(f):
                return None
            return int(f)
        except:
            return None


# 单例
_kline_fetcher: Optional[KlineFetcher] = None


def get_kline_fetcher() -> KlineFetcher:
    """获取 K线拉取器单例"""
    global _kline_fetcher
    if _kline_fetcher is None:
        _kline_fetcher = KlineFetcher()
    return _kline_fetcher
