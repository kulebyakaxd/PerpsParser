"""
Парсер для получения данных о торговых парах с Aster (Asterdex)
Пытается использовать публичные эндпоинты. При необходимости может добавлять авторизацию.

Переменные окружения (опционально):
  - ASTER_API_KEY
  - ASTER_SECRET_KEY
  - ASTER_BASE_URL (по умолчанию "https://www.asterdex.com")
  - ASTER_MARKETS_PATH (по умолчанию "/fapi/v1/astherusExchangeInfo?showall=false")
  - ASTER_TICKERS_24HR_PATH (по умолчанию "/fapi/v1/ticker/24hr")
"""
import asyncio
import aiohttp
import os
import hmac
import hashlib
import time
from typing import List, Dict, Any, Optional

from database import DatabaseManager


ASTER_API_KEY = os.getenv("ASTER_API_KEY")
ASTER_SECRET_KEY = os.getenv("ASTER_SECRET_KEY")
ASTER_BASE_URL = os.getenv("ASTER_BASE_URL", "https://www.asterdex.com")
ASTER_MARKETS_PATH = os.getenv("ASTER_MARKETS_PATH", "/fapi/v1/astherusExchangeInfo?showall=false")
ASTER_TICKERS_24HR_PATH = os.getenv("ASTER_TICKERS_24HR_PATH", "/fapi/v1/ticker/24hr")
BINANCE_EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"
BINANCE_TICKER_PRICE_PATH = "/fapi/v1/ticker/price"
BINANCE_BOOK_TICKER_PATH = "/fapi/v1/ticker/bookTicker"


class AsterParser:
    def __init__(self, db_manager: DatabaseManager = None):
        self.base_url = ASTER_BASE_URL.rstrip("/")
        self.markets_path = ASTER_MARKETS_PATH
        self.tickers_24hr_path = ASTER_TICKERS_24HR_PATH
        self.api_key: Optional[str] = ASTER_API_KEY
        self.secret_key: Optional[str] = ASTER_SECRET_KEY
        self.session: Optional[aiohttp.ClientSession] = None
        self.db_manager = db_manager or DatabaseManager()

    async def initialize(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    async def get_pairs_with_prices(self) -> List[Dict[str, Any]]:
        """
        Получает список всех торговых пар с их текущими ценами и сохраняет в БД.
        Возвращает: [{"symbol": str, "price": float}]
        """
        await self.initialize()
        assert self.session is not None

        try:
            markets = await self._fetch_markets()
            tickers = await self._fetch_tickers()

            prices_by_symbol: Dict[str, float] = {}
            # Индексация тикеров
            if isinstance(tickers, dict):
                for k, v in tickers.items():
                    price = self._extract_price(v)
                    if price is not None:
                        prices_by_symbol[str(k).upper()] = price
            elif isinstance(tickers, list):
                for t in tickers:
                    symbol = self._extract_symbol(t)
                    price = self._extract_price(t)
                    if symbol and price is not None:
                        prices_by_symbol[symbol.upper()] = price

            pairs: List[Dict[str, Any]] = []
            valid_symbols: List[str] = []
            # Если markets есть (exchangeInfo), фильтруем только TRADING + PERPETUAL и только те, у кого есть тикер
            filtered_markets: List[Any] = []
            if isinstance(markets, list) and markets and isinstance(markets[0], dict):
                for m in markets:
                    try:
                        if m.get("status") == "TRADING" and m.get("contractType") == "PERPETUAL":
                            sym = str(m.get("symbol", "")).upper()
                            if sym and sym in prices_by_symbol:
                                filtered_markets.append(m)
                    except Exception:
                        continue
            # Если нет нормализованных markets — используем только тикеры
            source_iterable = filtered_markets if filtered_markets else list(prices_by_symbol.keys())

            if isinstance(source_iterable, dict):
                iterable = source_iterable.values()
            else:
                iterable = source_iterable

            for item in iterable:
                raw_symbol = self._extract_symbol(item) if not isinstance(item, str) else item
                if not raw_symbol:
                    continue
                # Цену ищем по исходному символу (обычно вида BTCUSDT)
                price = prices_by_symbol.get(str(raw_symbol).upper())
                if price is None:
                    continue
                # Нормализуем символ: убираем суффикс USDT
                norm_symbol = self._normalize_symbol(str(raw_symbol).upper())
                pairs.append({"symbol": norm_symbol, "price": price})
                valid_symbols.append(norm_symbol)

            if pairs:
                saved = self.db_manager.save_trading_pairs("aster", pairs)
                try:
                    if valid_symbols:
                        self.db_manager.sync_exchange_snapshot("aster", valid_symbols)
                except Exception:
                    pass
                print(f"💾 Сохранено {saved} пар Aster в базу данных")
            else:
                print("⚠️ Aster вернул пустой список рынков или цен")

            return pairs
        except Exception as e:
            print(f"❌ Ошибка Aster: {e}")
            return []

    async def _fetch_markets(self) -> Any:
        # Primary: Aster dex-specific markets with showall=false
        url_primary = f"{self.base_url}{self.markets_path}"
        data = await self._get_json(url_primary)
        # Normalize for downstream: prefer symbols array from the wrapper object
        if isinstance(data, dict) and isinstance(data.get("symbols"), list):
            return data["symbols"]
        # If not symbols field, try legacy exchangeInfo format
        if isinstance(data, dict) and isinstance(data.get("symbols"), list):
            return data["symbols"]
        if data:
            return data
        # Fallback to Binance Futures-compatible exchangeInfo
        url_fallback = f"{self.base_url}{BINANCE_EXCHANGE_INFO_PATH}"
        data2 = await self._get_json(url_fallback)
        if isinstance(data2, dict) and isinstance(data2.get("symbols"), list):
            return data2["symbols"]
        return data2

    async def _fetch_tickers(self) -> Any:
        # Primary path: 24hr tickers on asterdex
        url_primary = f"{self.base_url}{self.tickers_24hr_path}"
        data = await self._get_json(url_primary)
        if data:
            return data
        # Fallback 1: Binance Futures-compatible ticker price list
        url_price = f"{self.base_url}{BINANCE_TICKER_PRICE_PATH}"
        data2 = await self._get_json(url_price)
        if data2:
            return data2
        # Fallback 2: bookTicker -> compute mid price if possible
        url_book = f"{self.base_url}{BINANCE_BOOK_TICKER_PATH}"
        data3 = await self._get_json(url_book)
        if isinstance(data3, list):
            # Convert to simple symbol/price entries using mid of bid/ask
            converted: List[Dict[str, Any]] = []
            for t in data3:
                try:
                    sym = self._extract_symbol(t)
                    bid = float(t.get("bidPrice")) if isinstance(t, dict) and t.get("bidPrice") is not None else None
                    ask = float(t.get("askPrice")) if isinstance(t, dict) and t.get("askPrice") is not None else None
                    if sym and bid is not None and ask is not None and bid > 0 and ask > 0:
                        converted.append({"symbol": sym, "price": (bid + ask) / 2})
                except Exception:
                    continue
            if converted:
                return converted
        return data3

    async def _get_json(self, url: str) -> Any:
        assert self.session is not None
        headers = self._build_headers("GET", url)
        # Первая попытка: без авторизации, если ключи не заданы
        try:
            async with self.session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                # Если нужен ключ — попробуем ещё раз с авторизацией Bearer
                if resp.status in (401, 403) and self.api_key:
                    bearer_headers = dict(headers)
                    bearer_headers["Authorization"] = f"Bearer {self.api_key}"
                    async with self.session.get(url, headers=bearer_headers) as resp2:
                        if resp2.status == 200:
                            return await resp2.json()
                        text = await resp2.text()
                        print(f"Aster GET {url} -> {resp2.status}: {text}")
                        return None
                text = await resp.text()
                print(f"Aster GET {url} -> {resp.status}: {text}")
                return None
        except Exception as e:
            print(f"Ошибка запроса {url}: {e}")
            return None

    def _build_headers(self, method: str, url: str, body: str = "") -> Dict[str, str]:
        headers: Dict[str, str] = {
            "Accept": "application/json",
        }
        # Если задан secret, попробуем HMAC-подпись: ts + method + path + body
        if self.api_key and self.secret_key:
            try:
                timestamp = str(int(time.time() * 1000))
                path = url.replace(self.base_url, "")
                payload = f"{timestamp}{method.upper()}{path}{body}"
                signature = hmac.new(
                    self.secret_key.encode("utf-8"),
                    payload.encode("utf-8"),
                    hashlib.sha256
                ).hexdigest()
                headers.update({
                    "X-API-KEY": self.api_key,
                    "X-API-TIMESTAMP": timestamp,
                    "X-API-SIGNATURE": signature,
                })
            except Exception:
                # Молча игнорируем, т.к. публичные эндпоинты могут не требовать подпись
                pass
        return headers

    def _extract_symbol(self, data: Any) -> Optional[str]:
        try:
            if isinstance(data, str):
                return data
            if isinstance(data, dict):
                for key in ("symbol", "pair", "name", "market", "ticker"):
                    val = data.get(key)
                    if isinstance(val, str) and val.strip():
                        return val.strip()
            return None
        except Exception:
            return None

    def _extract_price(self, data: Any) -> Optional[float]:
        try:
            if isinstance(data, (int, float)):
                return float(data)
            if isinstance(data, dict):
                for key in ("price", "last", "lastPrice", "mark", "mid", "oracle"):
                    val = data.get(key)
                    if val is None:
                        continue
                    fval = float(val)
                    if fval > 0:
                        return fval
            return None
        except Exception:
            return None

    def _normalize_symbol(self, symbol: str) -> str:
        # Если символ оканчивается на USDT — удаляем суффикс. Например, BTCUSDT -> BTC
        if symbol.upper().endswith("USDT") and len(symbol) > 4:
            return symbol[:-4]
        return symbol


async def main():
    parser = AsterParser()
    try:
        pairs = await parser.get_pairs_with_prices()
        print(f"Всего пар: {len(pairs)}")
        for i, p in enumerate(pairs[:10], 1):
            print(f"{i:2d}. {p['symbol']:20s} - ${p['price']:>12.6f}")
    finally:
        await parser.close()


if __name__ == "__main__":
    asyncio.run(main())


