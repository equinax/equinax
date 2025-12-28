#!/usr/bin/env python3
"""
Run missing sync steps: pct_chg calculation and adjust factor sync.
"""
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+asyncpg://quant:quant_dev_password@localhost:5432/quantdb"
)

async def main():
    engine = create_async_engine(DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    from workers.batch_sync import calculate_index_pct_chg, sync_adjust_factors

    async with async_session() as session:
        # Step 1: Calculate index pct_chg
        print("=" * 60)
        print("Step 1: Calculating index pct_chg...")
        print("=" * 60)
        count = await calculate_index_pct_chg(session)
        await session.commit()
        print(f"Updated {count} index records with pct_chg")

        # Step 2: Sync adjust factors
        print("\n" + "=" * 60)
        print("Step 2: Syncing adjust factors...")
        print("=" * 60)
        result = await sync_adjust_factors(session)
        await session.commit()
        print(f"Adjust factor sync result: {result}")

    await engine.dispose()
    print("\nDone!")

if __name__ == "__main__":
    asyncio.run(main())
