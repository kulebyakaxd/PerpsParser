"""
Парсер для получения данных о торговых парах с Lighter
Использует прямые HTTP запросы к API
"""
import asyncio
import aiohttp
import requests
from typing import List, Dict, Any
from database import DatabaseManager
from utils.telegram_notifier import get_notifier


class LighterParser:
    def __init__(self, db_manager: DatabaseManager = None):
        self.base_url = "https://mainnet.zklighter.elliot.ai/api/v1"
        self.session = None
        self.db_manager = db_manager or DatabaseManager()
    
    async def initialize(self):
        """Инициализация HTTP сессии"""
        if self.session is None:
            self.session = aiohttp.ClientSession()
    
    async def close(self):
        """Закрытие HTTP сессии"""
        if self.session:
            await self.session.close()
    
    async def get_pairs_with_prices(self) -> List[Dict[str, Any]]:
        """
        Получает список всех торговых пар с их текущими ценами
        
        Returns:
            List[Dict[str, Any]]: Список словарей в формате {"symbol": str, "price": float}
        """
        try:
            await self.initialize()
            
            # Получаем данные о всех рынках с ценами
            order_book_data = await self._get_order_book_details()
            if not order_book_data:
                return []
            
            # Извлекаем пары и цены из данных
            pairs = []
            valid_symbols: List[str] = []
            for market in order_book_data:
                try:
                    symbol = str(market.get('symbol', '')).strip().upper()
                    price = market.get('last_trade_price')
                    if not symbol:
                        continue
                    if price is None:
                        continue
                    price = float(price)
                    if price <= 0:
                        continue
                    pairs.append({
                        "symbol": symbol,
                        "price": price
                    })
                    valid_symbols.append(symbol)
                except (ValueError, TypeError):
                    continue
            
            # Сохраняем в базу данных
            if pairs:
                saved_count = self.db_manager.save_trading_pairs("lighter", pairs)
                # Синхронизируем снапшот: удаляем отсутствующие символы и некорректные
                try:
                    self.db_manager.sync_exchange_snapshot("lighter", valid_symbols)
                except Exception:
                    pass
                get_notifier().log(f"💾 Сохранено {saved_count} пар Lighter в базу данных")
            
            return pairs
                
        except Exception as e:
            get_notifier().log(f"❌ Общая ошибка: {e}")
            return []
    
    async def _get_order_book_details(self) -> List[Dict[str, Any]]:
        """Получает детали ордербука со всеми рынками и ценами"""
        try:
            url = f"{self.base_url}/orderBookDetails"
            get_notifier().log(f"Запрос к: {url}")
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('code') == 200:
                        order_book_details = data.get('order_book_details', [])
                        get_notifier().log(f"✅ Получено {len(order_book_details)} рынков")
                        return order_book_details
                    else:
                        get_notifier().log(f"❌ Ошибка API: {data.get('message', 'Unknown error')}")
                        return []
                else:
                    get_notifier().log(f"❌ Ошибка {response.status} при получении данных")
                    return []
        except Exception as e:
            get_notifier().log(f"❌ Ошибка при получении данных: {e}")
            return []
    
    def get_pairs_with_prices_sync(self) -> List[Dict[str, Any]]:
        """
        Синхронная версия получения пар с ценами
        """
        try:
            # Получаем данные о всех рынках с ценами
            order_book_data = self._get_order_book_details_sync()
            if not order_book_data:
                return []
            
            # Извлекаем пары и цены из данных
            pairs = []
            valid_symbols: List[str] = []
            for market in order_book_data:
                try:
                    symbol = str(market.get('symbol', '')).strip().upper()
                    price = market.get('last_trade_price')
                    if not symbol:
                        continue
                    if price is None:
                        continue
                    price = float(price)
                    if price <= 0:
                        continue
                    pairs.append({
                        "symbol": symbol,
                        "price": price
                    })
                    valid_symbols.append(symbol)
                except (ValueError, TypeError):
                    continue
            
            # Сохраняем в базу данных
            if pairs:
                saved_count = self.db_manager.save_trading_pairs("lighter", pairs)
                try:
                    self.db_manager.sync_exchange_snapshot("lighter", valid_symbols)
                except Exception:
                    pass
                print(f"💾 Сохранено {saved_count} пар Lighter в базу данных")
            
            return pairs
            
        except Exception as e:
            print(f"❌ Общая ошибка: {e}")
            return []
    
    def _get_order_book_details_sync(self) -> List[Dict[str, Any]]:
        """Синхронная версия получения деталей ордербука"""
        try:
            url = f"{self.base_url}/orderBookDetails"
            get_notifier().log(f"Запрос к: {url}")
            
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 200:
                    order_book_details = data.get('order_book_details', [])
                    get_notifier().log(f"✅ Получено {len(order_book_details)} рынков")
                    return order_book_details
                else:
                    get_notifier().log(f"❌ Ошибка API: {data.get('message', 'Unknown error')}")
                    return []
            else:
                get_notifier().log(f"❌ Ошибка {response.status_code} при получении данных")
                return []
        except Exception as e:
            get_notifier().log(f"❌ Ошибка при получении данных: {e}")
            return []


async def main():
    """Пример использования парсера"""
    parser = LighterParser()
    
    try:
        get_notifier().log("Получение данных с Lighter...")
        pairs = await parser.get_pairs_with_prices()
        
        if pairs:
            get_notifier().log(f"Найдено {len(pairs)} торговых пар:")
            for i, pair in enumerate(pairs[:10], 1):  # Показываем первые 10 пар
                get_notifier().log(f"{i:2d}. {pair['symbol']:20s} - ${pair['price']:>12.6f}")
        else:
            get_notifier().log("Не удалось получить данные")
        
        return pairs
        
    finally:
        await parser.close()


if __name__ == "__main__":
    # Запуск асинхронной версии
    pairs = asyncio.run(main())
