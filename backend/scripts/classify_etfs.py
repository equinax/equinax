"""Classify all ETFs and persist results to database.

This script uses the ETFClassifier to classify all ETFs in the database
and stores the results in etf_profile table.

Usage:
    docker compose exec api python -m scripts.classify_etfs

    # Dry run - show what would be done without saving
    docker compose exec api python -m scripts.classify_etfs --dry-run

    # Export fixture data after classification
    docker compose exec api python -m scripts.classify_etfs --export-fixture
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings
from app.db.models.asset import AssetMeta, AssetType, ETFType
from app.db.models.profile import ETFProfile, ClassificationSource, ClassificationConfidence
from app.services.alpha_radar.etf_classifier import ETFClassifier, ClassificationResult


# Mapping from classifier category to ETFType enum
CATEGORY_TO_ETF_TYPE = {
    "broad": ETFType.BROAD_BASED,
    "sector": ETFType.SECTOR,
    "theme": ETFType.THEME,
    "cross_border": ETFType.CROSS_BORDER,
    "commodity": ETFType.COMMODITY,
    "bond": ETFType.BOND,
    "other": None,  # Keep as NULL for unclassified
}


async def get_async_session():
    """Create async database session."""
    engine = create_async_engine(
        settings.database_url.replace("postgresql://", "postgresql+asyncpg://"),
        echo=False,
    )
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return async_session(), engine


async def classify_all_etfs(
    session: AsyncSession,
    dry_run: bool = False,
    only_empty: bool = True
) -> dict:
    """Classify all ETFs and update database.

    Args:
        session: Async database session
        dry_run: If True, don't save to database
        only_empty: If True, only classify ETFs with empty etf_type

    Returns:
        Statistics dict with counts per category
    """
    china_tz = ZoneInfo("Asia/Shanghai")
    now = datetime.now(china_tz)

    print("\n=== Classifying ETFs ===")
    print(f"Timestamp: {now.isoformat()}")
    print(f"Dry run: {dry_run}")
    print(f"Only empty: {only_empty}")

    # Get all ETFs from asset_meta
    query = select(AssetMeta).where(AssetMeta.asset_type == AssetType.ETF)
    result = await session.execute(query)
    all_etfs = result.scalars().all()

    print(f"\nFound {len(all_etfs)} ETFs in database")

    # Statistics
    stats = {
        "total": len(all_etfs),
        "classified": 0,
        "skipped": 0,
        "categories": {},
        "confidence": {"HIGH": 0, "MEDIUM": 0, "LOW": 0},
    }

    classifications = []

    for etf in all_etfs:
        # Get or create ETFProfile
        profile_query = select(ETFProfile).where(ETFProfile.code == etf.code)
        profile_result = await session.execute(profile_query)
        profile = profile_result.scalar_one_or_none()

        # Skip if already classified (unless only_empty is False)
        if only_empty and profile and profile.etf_type:
            stats["skipped"] += 1
            continue

        # Classify
        result = ETFClassifier.classify_with_confidence(etf.name)
        etf_type = CATEGORY_TO_ETF_TYPE.get(result.category)

        # Update statistics
        stats["classified"] += 1
        stats["categories"][result.category] = stats["categories"].get(result.category, 0) + 1
        stats["confidence"][result.confidence] += 1

        # Collect for fixture export
        classifications.append({
            "code": etf.code,
            "name": etf.name,
            "etf_type": etf_type.value if etf_type else None,
            "etf_sub_category": result.sub_category,
            "confidence": result.confidence,
            "source": ClassificationSource.AUTO.value,
        })

        if not dry_run:
            if profile:
                # Update existing profile
                await session.execute(
                    update(ETFProfile)
                    .where(ETFProfile.code == etf.code)
                    .values(
                        etf_type=etf_type.value if etf_type else None,
                        etf_sub_category=result.sub_category,
                        classification_source=ClassificationSource.AUTO.value,
                        classification_confidence=result.confidence,
                        classified_at=now,
                    )
                )
            else:
                # Create new profile
                new_profile = ETFProfile(
                    code=etf.code,
                    etf_type=etf_type.value if etf_type else None,
                    etf_sub_category=result.sub_category,
                    classification_source=ClassificationSource.AUTO.value,
                    classification_confidence=result.confidence,
                    classified_at=now,
                )
                session.add(new_profile)

    if not dry_run:
        await session.commit()

    # Print summary
    print("\n=== Classification Results ===")
    print(f"Total ETFs: {stats['total']}")
    print(f"Classified: {stats['classified']}")
    print(f"Skipped (already classified): {stats['skipped']}")

    print("\n--- By Category ---")
    for category, count in sorted(stats["categories"].items(), key=lambda x: -x[1]):
        label = ETFClassifier.get_category_label(category)
        print(f"  {label} ({category}): {count}")

    print("\n--- By Confidence ---")
    for confidence, count in stats["confidence"].items():
        print(f"  {confidence}: {count}")

    return {"stats": stats, "classifications": classifications}


async def export_fixture(classifications: list, output_path: Optional[Path] = None):
    """Export classification results to fixture file.

    Args:
        classifications: List of classification dicts
        output_path: Output file path (default: data/fixtures/etf_classifications.json)
    """
    if output_path is None:
        output_path = Path(__file__).parent.parent / "data" / "fixtures" / "etf_classifications.json"

    # Ensure directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    china_tz = ZoneInfo("Asia/Shanghai")
    now = datetime.now(china_tz)

    fixture_data = {
        "version": "1.0",
        "generated_at": now.isoformat(),
        "classifier_version": "1.0",
        "total_count": len(classifications),
        "classifications": classifications,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(fixture_data, f, ensure_ascii=False, indent=2)

    print(f"\n=== Exported Fixture ===")
    print(f"File: {output_path}")
    print(f"Count: {len(classifications)}")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Classify ETFs and persist to database")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    parser.add_argument("--all", action="store_true", help="Reclassify all ETFs (not just empty)")
    parser.add_argument("--export-fixture", action="store_true", help="Export fixture after classification")
    args = parser.parse_args()

    session, engine = await get_async_session()

    try:
        result = await classify_all_etfs(
            session,
            dry_run=args.dry_run,
            only_empty=not args.all,
        )

        if args.export_fixture:
            await export_fixture(result["classifications"])

    finally:
        await session.close()
        await engine.dispose()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
