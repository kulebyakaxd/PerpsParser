"""
Парсер для получения данных о торговых парах с Hyperliquid
Использует прямые HTTP запросы к API
"""
import asyncio
import aiohttp
import requests
from typing import List, Dict, Any
from database import DatabaseManager


class HyperliquidParser:
    def __init__(self, db_manager: DatabaseManager = None):
        self.base_url = "https://api.hyperliquid.xyz"
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
            
            # Сначала получаем метаинформацию о торговых парах
            meta_data = await self._get_meta_info()
            if not meta_data:
                return []
            
            # Получаем цены для всех пар
            prices_data = await self._get_all_mids()
            if not prices_data:
                return []
            
            # Объединяем данные
            pairs = []
            universe = meta_data.get('universe', [])
            
            for asset in universe:
                symbol = asset.get('name', '')
                if symbol and symbol in prices_data:
                    try:
                        price = float(prices_data[symbol])
                        pairs.append({
                            "symbol": symbol,
                            "price": price
                        })
                    except (ValueError, TypeError) as e:
                        print(f"Ошибка при обработке цены для {symbol}: {e}")
                        continue
            
            # Сохраняем в базу данных
            if pairs:
                saved_count = self.db_manager.save_trading_pairs("hyperliquid", pairs)
                print(f"💾 Сохранено {saved_count} пар Hyperliquid в базу данных")
            
            return pairs
                
        except Exception as e:
            print(f"❌ Общая ошибка: {e}")
            return []
    
    async def _get_meta_info(self) -> Dict[str, Any]:
        """Получает метаинформацию о торговых парах"""
        try:
            url = f"{self.base_url}/info"
            payload = {"type": "meta"}
            
            async with self.session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ Получена метаинформация: {len(data.get('universe', []))} активов")
                    return data
                else:
                    print(f"❌ Ошибка {response.status} при получении метаинформации")
                    return {}
        except Exception as e:
            print(f"❌ Ошибка при получении метаинформации: {e}")
            return {}
    
    async def _get_all_mids(self) -> Dict[str, Any]:
        """Получает средние цены для всех активов"""
        try:
            url = f"{self.base_url}/info"
            payload = {"type": "allMids"}
            
            async with self.session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ Получены цены для {len(data)} активов")
                    return data
                else:
                    print(f"❌ Ошибка {response.status} при получении цен")
                    return {}
        except Exception as e:
            print(f"❌ Ошибка при получении цен: {e}")
            return {}
    
    def _extract_pairs_from_data(self, data: Any) -> List[Dict[str, Any]]:
        """
        Извлекает пары и цены из данных API
        """
        pairs = []
        
        try:
            # Если данные - это словарь с парами
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, (int, float)) and value > 0:
                        pairs.append({
                            "symbol": str(key),
                            "price": float(value)
                        })
                    elif isinstance(value, dict) and 'price' in value:
                        pairs.append({
                            "symbol": str(key),
                            "price": float(value['price'])
                        })
            
            # Если данные - это список
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        if 'symbol' in item and 'price' in item:
                            pairs.append({
                                "symbol": str(item['symbol']),
                                "price": float(item['price'])
                            })
                        elif 'name' in item and 'price' in item:
                            pairs.append({
                                "symbol": str(item['name']),
                                "price": float(item['price'])
                            })
            
            print(f"Извлечено {len(pairs)} пар из данных")
            return pairs
            
        except Exception as e:
            print(f"Ошибка при извлечении пар: {e}")
            return []
    
    def get_pairs_with_prices_sync(self) -> List[Dict[str, Any]]:
        """
        Синхронная версия получения пар с ценами
        """
        try:
            # Получаем метаинформацию
            meta_data = self._get_meta_info_sync()
            if not meta_data:
                return []
            
            # Получаем цены
            prices_data = self._get_all_mids_sync()
            if not prices_data:
                return []
            
            # Объединяем данные
            pairs = []
            universe = meta_data.get('universe', [])
            
            for asset in universe:
                symbol = asset.get('name', '')
                if symbol and symbol in prices_data:
                    try:
                        price = float(prices_data[symbol])
                        pairs.append({
                            "symbol": symbol,
                            "price": price
                        })
                    except (ValueError, TypeError) as e:
                        print(f"Ошибка при обработке цены для {symbol}: {e}")
                        continue
            
            return pairs
            
        except Exception as e:
            print(f"❌ Общая ошибка: {e}")
            return []
    
    def _get_meta_info_sync(self) -> Dict[str, Any]:
        """Синхронная версия получения метаинформации"""
        try:
            url = f"{self.base_url}/info"
            payload = {"type": "meta"}
            
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Получена метаинформация: {len(data.get('universe', []))} активов")
                return data
            else:
                print(f"❌ Ошибка {response.status_code} при получении метаинформации")
                return {}
        except Exception as e:
            print(f"❌ Ошибка при получении метаинформации: {e}")
            return {}
    
    def _get_all_mids_sync(self) -> Dict[str, Any]:
        """Синхронная версия получения цен"""
        try:
            url = f"{self.base_url}/info"
            payload = {"type": "allMids"}
            
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Получены цены для {len(data)} активов")
                return data
            else:
                print(f"❌ Ошибка {response.status_code} при получении цен")
                return {}
        except Exception as e:
            print(f"❌ Ошибка при получении цен: {e}")
            return {}


async def main():
    """Пример использования парсера"""
    parser = HyperliquidParser()
    
    try:
        print("Получение данных с Hyperliquid...")
        pairs = await parser.get_pairs_with_prices()
        
        if pairs:
            print(f"Найдено {len(pairs)} торговых пар:")
            for i, pair in enumerate(pairs[:10], 1):  # Показываем первые 10 пар
                print(f"{i:2d}. {pair['symbol']:20s} - ${pair['price']:>12.6f}")
        else:
            print("Не удалось получить данные")
        
        return pairs
        
    finally:
        await parser.close()


if __name__ == "__main__":
    # Запуск асинхронной версии
    pairs = asyncio.run(main())
