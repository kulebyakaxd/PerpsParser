"""
Основной файл для запуска парсера торговых пар
"""
import asyncio
import os
import certifi

# Загрузка переменных окружения из .env (если есть) ДО импортов парсеров,
# чтобы настройки (например, ASTER_BASE_URL) применялись при импортировании модулей
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# Ensure SSL verification uses certifi bundle (fixes SSL issues on macOS)
os.environ.setdefault("SSL_CERT_FILE", certifi.where())
os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

from parsers import HyperliquidParser, LighterParser, PacificaSDKParser, AsterParser, ExtendedParser
from utils.telegram_notifier import get_notifier
from database import DatabaseManager
from utils.telegram_bot import run_bot
from utils.single_instance import acquire_pid_lock


async def main():
    """Основная функция для получения данных с различных бирж"""
    notifier = get_notifier()
    notifier.log("=== Парсер торговых пар ===")
    notifier.log("Получает данные с Hyperliquid, Lighter, Pacifica, Aster и Extended")
    
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
            notifier.log(f"\n=== Получение данных с {name} ===")
            
            try:
                pairs = await parser.get_pairs_with_prices()
                
                if pairs:
                    notifier.log(f"✅ Успешно получено {len(pairs)} торговых пар с {name}")
                    all_pairs.extend(pairs)
                    
                    # Выводим первые 5 пар для проверки
                    notifier.log(f"Первые 5 пар с {name}:")
                    for i, pair in enumerate(pairs[:5], 1):
                        notifier.log(f"  {i}. {pair['symbol']:20s} - ${pair['price']:>12.6f}")
                else:
                    notifier.log(f"❌ Не удалось получить данные с {name}")
                    
            except Exception as e:
                notifier.log(f"❌ Ошибка при получении данных с {name}: {e}")
            
            finally:
                close = getattr(parser, 'close', None)
                if close and asyncio.iscoroutinefunction(close):
                    await close()
        
        # Обслуживание БД: оставляем один актуальный снэпшот на (symbol, exchange)
        db = DatabaseManager()
        db.maintenance_snapshot(valid_exchanges=["hyperliquid", "lighter", "pacifica", "aster", "extended"])
        
        # Общая статистика
        if all_pairs:
            notifier.log(f"\n✅ Всего получено {len(all_pairs)} торговых пар")
        else:
            notifier.log("❌ Не удалось получить данные ни с одной биржи")
            
    except Exception as e:
        notifier = get_notifier()
        notifier.log(f"❌ Общая ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Ensure single instance
    try:
        acquire_pid_lock()
    except RuntimeError as e:
        print(str(e))
        raise SystemExit(1)
    # One-file launch: start Telegram bot (it runs the 5-minute refresh internally)
    asyncio.run(run_bot())
