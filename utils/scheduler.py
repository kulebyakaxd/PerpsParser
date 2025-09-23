import asyncio
from typing import Sequence

from parsers import HyperliquidParser, LighterParser, PacificaSDKParser, AsterParser, ExtendedParser
from database import DatabaseManager


async def refresh_all_once() -> None:
    db = DatabaseManager()
    parsers = [
        ("hyperliquid", HyperliquidParser(db)),
        ("lighter", LighterParser(db)),
        ("pacifica", PacificaSDKParser(db)),
        ("aster", AsterParser(db)),
        ("extended", ExtendedParser(db)),
    ]
    try:
        for name, parser in parsers:
            try:
                await parser.get_pairs_with_prices()
                close = getattr(parser, 'close', None)
                if close and asyncio.iscoroutinefunction(close):
                    await close()
            except Exception:
                pass
        db.maintenance_snapshot(valid_exchanges=["hyperliquid", "lighter", "pacifica", "aster", "extended"])
        db.calculate_price_differences()
    finally:
        # nothing persistent
        pass


async def periodic_refresh(interval_seconds: int = 300) -> None:
    while True:
        await refresh_all_once()
        await asyncio.sleep(interval_seconds)



