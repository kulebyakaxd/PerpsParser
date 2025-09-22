"""
Основной файл для работы с парсерами и базой данных
"""
import asyncio
from database import DatabaseManager
from parsers import HyperliquidParser, LighterParser


async def main():
    """Основная функция для получения данных и сравнения цен"""
    print("=== Парсер торговых пар с базой данных ===")
    
    # Инициализируем базу данных
    db = DatabaseManager()
    
    # Создаем парсеры с общим менеджером базы данных
    hyperliquid_parser = HyperliquidParser(db)
    lighter_parser = LighterParser(db)
    
    try:
        # Получаем данные с Hyperliquid
        print("\n🔄 Получение данных с Hyperliquid...")
        hyperliquid_pairs = await hyperliquid_parser.get_pairs_with_prices()
        
        if hyperliquid_pairs:
            print(f"✅ Hyperliquid: получено {len(hyperliquid_pairs)} пар")
        else:
            print("❌ Не удалось получить данные с Hyperliquid")
        
        # Получаем данные с Lighter
        print("\n🔄 Получение данных с Lighter...")
        lighter_pairs = await lighter_parser.get_pairs_with_prices()
        
        if lighter_pairs:
            print(f"✅ Lighter: получено {len(lighter_pairs)} пар")
        else:
            print("❌ Не удалось получить данные с Lighter")
        
        # Вычисляем различия в ценах
        print("\n🔄 Вычисление различий в ценах...")
        price_differences = db.calculate_price_differences()
        
        if price_differences:
            print(f"✅ Найдено {len(price_differences)} сравнений цен")
            
            # Показываем топ-5 различий
            print("\n📊 Топ-5 самых больших различий в ценах:")
            top_differences = db.get_top_differences(5)
            
            for i, diff in enumerate(top_differences, 1):
                print(f"{i}. {diff['symbol']:15s} | "
                      f"{diff['exchange1']:12s}: ${diff['price1']:>12.6f} | "
                      f"{diff['exchange2']:12s}: ${diff['price2']:>12.6f} | "
                      f"Разница: ${diff['price_difference']:>12.6f} "
                      f"({diff['percentage_difference']:>6.2f}%)")
        else:
            print("❌ Не найдено общих символов для сравнения")
        
        # Показываем статистику по биржам
        print("\n📈 Статистика по биржам:")
        stats = db.get_exchange_stats()
        for exchange, data in stats.items():
            print(f"  {exchange:12s}: {data['pair_count']:3d} пар")
        
        # Показываем последние цены
        print("\n💰 Последние цены (первые 10 записей):")
        latest_prices = db.get_latest_prices()
        for i, price_data in enumerate(latest_prices[:10], 1):
            print(f"{i:2d}. {price_data['symbol']:15s} | "
                  f"{price_data['exchange']:12s}: ${price_data['price']:>12.6f}")
        
        if len(latest_prices) > 10:
            print(f"    ... и еще {len(latest_prices) - 10} записей")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Закрываем соединения
        await hyperliquid_parser.close()
        await lighter_parser.close()


if __name__ == "__main__":
    asyncio.run(main())
