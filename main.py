"""
Основной файл для запуска парсера торговых пар
"""
import asyncio

# Загрузка переменных окружения из .env (если есть) ДО импортов парсеров,
# чтобы настройки (например, ASTER_BASE_URL) применялись при импортировании модулей
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from parsers import HyperliquidParser, LighterParser, PacificaSDKParser, AsterParser, ExtendedParser
from database import DatabaseManager


async def main():
    """Основная функция для получения данных с различных бирж"""
    print("=== Парсер торговых пар ===")
    print("Получает данные с Hyperliquid, Lighter, Pacifica, Aster и Extended")
    
    all_pairs = []
    
    # Парсеры
    parsers = [
        ("Hyperliquid", HyperliquidParser()),
        ("Lighter", LighterParser()),
        ("Pacifica (SDK)", PacificaSDKParser()),
        ("Aster", AsterParser()),
        ("Extended", ExtendedParser()),
    ]
    
    try:
        for name, parser in parsers:
            print(f"\n=== Получение данных с {name} ===")
            
            try:
                pairs = await parser.get_pairs_with_prices()
                
                if pairs:
                    print(f"✅ Успешно получено {len(pairs)} торговых пар с {name}")
                    all_pairs.extend(pairs)
                    
                    # Выводим первые 5 пар для проверки
                    print(f"Первые 5 пар с {name}:")
                    for i, pair in enumerate(pairs[:5], 1):
                        print(f"  {i}. {pair['symbol']:20s} - ${pair['price']:>12.6f}")
                else:
                    print(f"❌ Не удалось получить данные с {name}")
                    
            except Exception as e:
                print(f"❌ Ошибка при получении данных с {name}: {e}")
            
            finally:
                close = getattr(parser, 'close', None)
                if close and asyncio.iscoroutinefunction(close):
                    await close()
        
        # Обслуживание БД: оставляем один актуальный снэпшот на (symbol, exchange)
        db = DatabaseManager()
        db.maintenance_snapshot(valid_exchanges=["hyperliquid", "lighter", "pacifica", "aster", "extended"])
        
        # Общая статистика
        if all_pairs:
            print(f"\n✅ Всего получено {len(all_pairs)} торговых пар")
        else:
            print("❌ Не удалось получить данные ни с одной биржи")
            
    except Exception as e:
        print(f"❌ Общая ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
