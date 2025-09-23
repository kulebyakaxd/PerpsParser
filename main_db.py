"""
Основной файл для работы с парсерами и базой данных
"""
import asyncio
from database import DatabaseManager
from parsers import HyperliquidParser, LighterParser
from utils.telegram_notifier import get_notifier


async def main():
    """Основная функция для получения данных и сравнения цен"""
    notifier = get_notifier()
    notifier.log("=== Парсер торговых пар с базой данных ===")
    
    # Инициализируем базу данных
    db = DatabaseManager()
    
    # Создаем парсеры с общим менеджером базы данных
    hyperliquid_parser = HyperliquidParser(db)
    lighter_parser = LighterParser(db)
    
    try:
        # Получаем данные с Hyperliquid
        notifier.log("\n🔄 Получение данных с Hyperliquid...")
        hyperliquid_pairs = await hyperliquid_parser.get_pairs_with_prices()
        
        if hyperliquid_pairs:
            notifier.log(f"✅ Hyperliquid: получено {len(hyperliquid_pairs)} пар")
        else:
            notifier.log("❌ Не удалось получить данные с Hyperliquid")
        
        # Получаем данные с Lighter
        notifier.log("\n🔄 Получение данных с Lighter...")
        lighter_pairs = await lighter_parser.get_pairs_with_prices()
        
        if lighter_pairs:
            notifier.log(f"✅ Lighter: получено {len(lighter_pairs)} пар")
        else:
            notifier.log("❌ Не удалось получить данные с Lighter")
        
        # Вычисляем различия в ценах
        notifier.log("\n🔄 Вычисление различий в ценах...")
        price_differences = db.calculate_price_differences()
        
        if price_differences:
            notifier.log(f"✅ Найдено {len(price_differences)} сравнений цен")
            
            # Показываем топ-5 различий
            notifier.log("\n📊 Топ-5 самых больших различий в ценах:")
            top_differences = db.get_top_differences(5)
            
            for i, diff in enumerate(top_differences, 1):
                notifier.log(
                    f"{i}. {diff['symbol']:15s} | "
                    f"{diff['exchange1']:12s}: ${diff['price1']:>12.6f} | "
                    f"{diff['exchange2']:12s}: ${diff['price2']:>12.6f} | "
                    f"Разница: ${diff['price_difference']:>12.6f} "
                    f"({diff['percentage_difference']:>6.2f}%)"
                )
        else:
            notifier.log("❌ Не найдено общих символов для сравнения")
        
        # Показываем статистику по биржам
        notifier.log("\n📈 Статистика по биржам:")
        stats = db.get_exchange_stats()
        for exchange, data in stats.items():
            notifier.log(f"  {exchange:12s}: {data['pair_count']:3d} пар")
        
        # Показываем последние цены
        notifier.log("\n💰 Последние цены (первые 10 записей):")
        latest_prices = db.get_latest_prices()
        for i, price_data in enumerate(latest_prices[:10], 1):
            notifier.log(
                f"{i:2d}. {price_data['symbol']:15s} | "
                f"{price_data['exchange']:12s}: ${price_data['price']:>12.6f}"
            )
        
        if len(latest_prices) > 10:
            notifier.log(f"    ... и еще {len(latest_prices) - 10} записей")
            
    except Exception as e:
        notifier = get_notifier()
        notifier.log(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Закрываем соединения
        await hyperliquid_parser.close()
        await lighter_parser.close()


if __name__ == "__main__":
    asyncio.run(main())
