"""
Парсер для Extended Exchange (Starknet):
Источник: https://starknet.app.extended.exchange/api/v1/info/markets

Возвращаем пары в формате [{"symbol": str, "price": float}] и сохраняем в БД.
"""
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional

from database import DatabaseManager
from utils.telegram_notifier import get_notifier


class ExtendedParser:
    def __init__(self, db_manager: DatabaseManager = None):
        self.base_url = "https://starknet.app.extended.exchange/api/v1"
        self.session: Optional[aiohttp.ClientSession] = None
        self.db_manager = db_manager or DatabaseManager()

    async def initialize(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    async def get_pairs_with_prices(self) -> List[Dict[str, Any]]:
        await self.initialize()
        assert self.session is not None

        try:
            markets = await self._fetch_markets()
            pairs: List[Dict[str, Any]] = []
            valid_symbols: List[str] = []

            if isinstance(markets, list):
                for m in markets:
                    try:
                        # Популярные поля в подобных API: symbol/name + цена last/mark/mid
                        symbol = None
                        price_val: Optional[float] = None

                        if isinstance(m, dict):
                            symbol = self._extract_symbol(m)
                            price_val = self._extract_price(m)

                        if symbol and price_val is not None and price_val > 0:
                            norm_symbol = self._normalize_symbol(symbol)
                            pairs.append({"symbol": norm_symbol, "price": float(price_val)})
                            valid_symbols.append(norm_symbol)
                    except Exception:
                        continue

            if pairs:
                saved = self.db_manager.save_trading_pairs("extended", pairs)
                try:
                    if valid_symbols:
                        self.db_manager.sync_exchange_snapshot("extended", valid_symbols)
                except Exception:
                    pass
                get_notifier().log(f"💾 Сохранено {saved} пар Extended в базу данных")
            else:
                get_notifier().log("⚠️ Extended вернул пустой список рынков или цен")

            return pairs
        except Exception as e:
            get_notifier().log(f"❌ Ошибка Extended: {e}")
            return []

    async def _fetch_markets(self) -> Any:
        url = f"{self.base_url}/info/markets"
        data = await self._get_json(url)
        # Extended API: {"status": "OK", "data": [...]} — разворачиваем список
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            return data["data"]
        return data

    async def _get_json(self, url: str) -> Any:
        assert self.session is not None
        try:
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
                text = await resp.text()
                get_notifier().log(f"Extended GET {url} -> {resp.status}: {text}")
                return None
        except Exception as e:
            get_notifier().log(f"Ошибка запроса {url}: {e}")
            return None

    def _extract_symbol(self, data: Dict[str, Any]) -> Optional[str]:
        try:
            for key in ("symbol", "name", "uiName", "pair", "market", "ticker"):
                val = data.get(key)
                if isinstance(val, str) and val.strip():
                    return val
            # Иногда символ может быть в вложенном объекте
            base = data.get("base") or data.get("baseAsset")
            quote = data.get("quote") or data.get("quoteAsset")
            if isinstance(base, str) and isinstance(quote, str):
                return f"{base}{quote}"
            return None
        except Exception:
            return None

    def _extract_price(self, data: Dict[str, Any]) -> Optional[float]:
        try:
            for key in ("price", "last", "lastPrice", "mark", "mid", "oracle"):
                if key in data and data[key] is not None:
                    val = float(data[key])
                    if val > 0:
                        return val
            # Возможная структура: data["prices"]["mark"] и т.п.
            prices = data.get("prices")
            if isinstance(prices, dict):
                for key in ("mark", "mid", "last", "price"):
                    v = prices.get(key)
                    if v is not None:
                        val = float(v)
                        if val > 0:
                            return val
            # Структура Extended: data["marketStats"]["markPrice"|"lastPrice"|"indexPrice"|"bidPrice"|"askPrice"]
            market_stats = data.get("marketStats")
            if isinstance(market_stats, dict):
                for key in ("markPrice", "lastPrice", "indexPrice"):
                    v = market_stats.get(key)
                    if v is not None:
                        val = float(v)
                        if val > 0:
                            return val
                bid = market_stats.get("bidPrice")
                ask = market_stats.get("askPrice")
                try:
                    if bid is not None and ask is not None:
                        bid_f = float(bid)
                        ask_f = float(ask)
                        if bid_f > 0 and ask_f > 0:
                            return (bid_f + ask_f) / 2
                except Exception:
                    pass
            return None
        except Exception:
            return None

    def _normalize_symbol(self, symbol: str) -> str:
        s = symbol.strip().upper()
        # Удаляем стандартные суффиксы разделённые '-' или '/' (например, ENA-USD -> ENA)
        for sep in ("-", "/"):
            if sep in s:
                parts = s.split(sep)
                if len(parts) == 2 and parts[1] in ("USD", "USDT", "USDC"):
                    return parts[0]
        # Если просто заканчивается на USD/USDT/USDC без разделителя
        for quote in ("USD", "USDT", "USDC"):
            if s.endswith(quote) and len(s) > len(quote):
                return s[: -len(quote)]
        return s


async def main():
    parser = ExtendedParser()
    try:
        pairs = await parser.get_pairs_with_prices()
        print(f"Всего пар: {len(pairs)}")
        for i, p in enumerate(pairs[:10], 1):
            print(f"{i:2d}. {p['symbol']:20s} - ${p['price']:>12.6f}")
    finally:
        await parser.close()


if __name__ == "__main__":
    asyncio.run(main())


