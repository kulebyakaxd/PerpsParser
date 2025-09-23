"""
Парсер Pacifica на базе официального SDK (async)
"""
import asyncio
from typing import List, Dict, Any
import os

# Добавляем путь проекта для локальных импортов
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import DatabaseManager
from utils.telegram_notifier import get_notifier

# Публичный ключ (опционально) можно задать через .env
PACIFICA_PUBLIC_KEY = os.getenv('PACIFICA_PUBLIC_KEY')


class PacificaSDKParser:
    def __init__(self, db_manager: DatabaseManager = None, public_key: str | None = None):
        self.db_manager = db_manager or DatabaseManager()
        self.public_key = public_key or PACIFICA_PUBLIC_KEY
        self.info_client = None

    async def _ensure_client(self):
        if self.info_client is not None:
            return
        try:
            from pacifica_sdk.async_.info import Info
        except ImportError as e:
            raise RuntimeError("Pacifica SDK не установлен. Установите пакет 'pacifica-sdk'.") from e
        # Для публичных эндпоинтов public_key не обязателен
        self.info_client = Info(public_key=self.public_key)

    async def get_pairs_with_prices(self) -> List[Dict[str, Any]]:
        await self._ensure_client()
        assert self.info_client is not None

        # Загружаем рынки и цены параллельно
        markets_task = asyncio.create_task(self.info_client.get_market_info())
        prices_task = asyncio.create_task(self.info_client.get_prices())
        markets, prices = await asyncio.gather(markets_task, prices_task)

        # Индексируем цены по символу
        prices_by_symbol: Dict[str, float] = {}
        if isinstance(prices, list):
            for p in prices:
                try:
                    sym = getattr(p, 'symbol', None) if not isinstance(p, dict) else p.get('symbol')
                    # Поля: mark/mid/oracle — берём mark как ближайшее к тик-котировке
                    val = None
                    if not isinstance(p, dict):
                        val = getattr(p, 'mark', None) or getattr(p, 'mid', None) or getattr(p, 'oracle', None)
                    else:
                        val = p.get('mark') or p.get('mid') or p.get('oracle')
                    if sym and val is not None:
                        prices_by_symbol[sym] = float(val)
                except Exception:
                    continue

        pairs: List[Dict[str, Any]] = []
        if isinstance(markets, list):
            for m in markets:
                try:
                    symbol = getattr(m, 'symbol', None) if not isinstance(m, dict) else m.get('symbol')
                    if not symbol:
                        continue
                    price = prices_by_symbol.get(symbol)
                    if price is not None:
                        pairs.append({"symbol": symbol, "price": price})
                except Exception:
                    continue

        if pairs:
            saved = self.db_manager.save_trading_pairs("pacifica", pairs)
            get_notifier().log(f"💾 Сохранено {saved} пар Pacifica (SDK) в базу данных")
        else:
            get_notifier().log("⚠️ SDK вернул пустой список рынков или цен")
        return pairs

    async def close(self):
        if self.info_client and hasattr(self.info_client, 'close'):
            await self.info_client.close()


async def main():
    parser = PacificaSDKParser()
    try:
        pairs = await parser.get_pairs_with_prices()
        get_notifier().log(f"Всего пар: {len(pairs)}")
        for i, p in enumerate(pairs[:10], 1):
            get_notifier().log(f"{i:2d}. {p['symbol']:20s} - ${p['price']:>12.6f}")
    finally:
        await parser.close()


if __name__ == "__main__":
    asyncio.run(main())
