"""
Утилита для просмотра данных из базы данных
"""
from database import DatabaseManager


def show_latest_prices(exchange=None, limit=20):
    """Показывает последние цены"""
    db = DatabaseManager()
    prices = db.get_latest_prices(exchange)
    
    if not prices:
        print("Нет данных в базе")
        return
    
    print(f"\n💰 Последние цены{' (' + exchange + ')' if exchange else ''}:")
    print("-" * 80)
    print(f"{'№':<3} {'Символ':<15} {'Биржа':<12} {'Цена':<15} {'Время'}")
    print("-" * 80)
    
    for i, price_data in enumerate(prices[:limit], 1):
        print(f"{i:<3} {price_data['symbol']:<15} {price_data['exchange']:<12} "
              f"${price_data['price']:<14.6f} {price_data['timestamp']}")
    
    if len(prices) > limit:
        print(f"... и еще {len(prices) - limit} записей")


def show_top_differences(limit=10):
    """Показывает топ различий в ценах"""
    db = DatabaseManager()
    differences = db.get_top_differences(limit)
    
    if not differences:
        print("Нет данных о различиях в ценах")
        return
    
    print(f"\n📊 Топ-{limit} самых больших различий в ценах:")
    print("-" * 100)
    print(f"{'№':<3} {'Символ':<15} {'Биржа 1':<12} {'Цена 1':<15} {'Биржа 2':<12} {'Цена 2':<15} {'Разница':<15} {'%'}")
    print("-" * 100)
    
    for i, diff in enumerate(differences, 1):
        print(f"{i:<3} {diff['symbol']:<15} {diff['exchange1']:<12} "
              f"${diff['price1']:<14.6f} {diff['exchange2']:<12} "
              f"${diff['price2']:<14.6f} ${diff['price_difference']:<14.6f} "
              f"{diff['percentage_difference']:<6.2f}%")


def show_exchange_stats():
    """Показывает статистику по биржам"""
    db = DatabaseManager()
    stats = db.get_exchange_stats()
    
    if not stats:
        print("Нет данных в базе")
        return
    
    print(f"\n📈 Статистика по биржам:")
    print("-" * 40)
    print(f"{'Биржа':<15} {'Количество пар'}")
    print("-" * 40)
    
    total_pairs = 0
    for exchange, data in stats.items():
        print(f"{exchange:<15} {data['pair_count']:<15}")
        total_pairs += data['pair_count']
    
    print("-" * 40)
    print(f"{'Всего':<15} {total_pairs:<15}")


def show_common_symbols():
    """Показывает общие символы между биржами"""
    db = DatabaseManager()
    prices = db.get_latest_prices()
    
    # Группируем по символам
    symbols = {}
    for price in prices:
        symbol = price['symbol']
        if symbol not in symbols:
            symbols[symbol] = []
        symbols[symbol].append(price)
    
    # Находим символы, которые есть на нескольких биржах
    common_symbols = {symbol: exchanges for symbol, exchanges in symbols.items() 
                     if len(exchanges) > 1}
    
    if not common_symbols:
        print("Нет общих символов между биржами")
        return
    
    print(f"\n🔗 Общие символы между биржами ({len(common_symbols)} символов):")
    print("-" * 80)
    print(f"{'Символ':<15} {'Биржи':<30} {'Цены'}")
    print("-" * 80)
    
    for symbol, exchanges in sorted(common_symbols.items()):
        exchange_names = [ex['exchange'] for ex in exchanges]
        prices_str = " | ".join([f"{ex['exchange']}: ${ex['price']:.6f}" for ex in exchanges])
        print(f"{symbol:<15} {', '.join(exchange_names):<30} {prices_str}")


def main():
    """Главное меню"""
    while True:
        print("\n" + "="*60)
        print("           ПРОСМОТР ДАННЫХ ТОРГОВЫХ ПАР")
        print("="*60)
        print("1. Показать последние цены")
        print("2. Показать последние цены по бирже")
        print("3. Показать топ различий в ценах")
        print("4. Показать статистику по биржам")
        print("5. Показать общие символы")
        print("6. Очистить старые данные")
        print("0. Выход")
        print("-"*60)
        
        choice = input("Выберите опцию (0-6): ").strip()
        
        if choice == "0":
            print("До свидания!")
            break
        elif choice == "1":
            limit = input("Количество записей (по умолчанию 20): ").strip()
            limit = int(limit) if limit.isdigit() else 20
            show_latest_prices(limit=limit)
        elif choice == "2":
            exchange = input("Введите название биржи (hyperliquid/lighter): ").strip()
            if exchange in ['hyperliquid', 'lighter']:
                limit = input("Количество записей (по умолчанию 20): ").strip()
                limit = int(limit) if limit.isdigit() else 20
                show_latest_prices(exchange=exchange, limit=limit)
            else:
                print("Неверное название биржи")
        elif choice == "3":
            limit = input("Количество записей (по умолчанию 10): ").strip()
            limit = int(limit) if limit.isdigit() else 10
            show_top_differences(limit=limit)
        elif choice == "4":
            show_exchange_stats()
        elif choice == "5":
            show_common_symbols()
        elif choice == "6":
            days = input("Удалить данные старше N дней (по умолчанию 7): ").strip()
            days = int(days) if days.isdigit() else 7
            db = DatabaseManager()
            db.clear_old_data(days)
        else:
            print("Неверный выбор, попробуйте снова")
        
        input("\nНажмите Enter для продолжения...")


if __name__ == "__main__":
    main()
