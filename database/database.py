"""
Модуль для работы с базой данных SQLite
"""
import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Any, Optional


class DatabaseManager:
    def __init__(self, db_path: str = "data/trading_pairs.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных и создание таблиц"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Создаем таблицу для торговых пар
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trading_pairs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    price REAL NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, exchange, timestamp)
                )
            ''')
            
            # Создаем таблицу для сравнения цен
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_comparisons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    exchange1 TEXT NOT NULL,
                    price1 REAL NOT NULL,
                    exchange2 TEXT NOT NULL,
                    price2 REAL NOT NULL,
                    price_difference REAL NOT NULL,
                    percentage_difference REAL NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Создаем индексы для быстрого поиска
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_exchange ON trading_pairs(symbol, exchange)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON trading_pairs(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_diff ON price_comparisons(price_difference)')
            
            conn.commit()
    
    def save_trading_pairs(self, exchange: str, pairs: List[Dict[str, Any]]) -> int:
        """
        Сохраняет торговые пары в базу данных
        
        Args:
            exchange: Название биржи (hyperliquid, lighter, pacifica)
            pairs: Список пар в формате [{"symbol": str, "price": float}]
        
        Returns:
            Количество сохраненных записей
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            saved_count = 0
            for pair in pairs:
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO trading_pairs (symbol, exchange, price, timestamp)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        pair['symbol'],
                        exchange,
                        pair['price'],
                        datetime.now()
                    ))
                    saved_count += 1
                except sqlite3.Error as e:
                    print(f"Ошибка при сохранении пары {pair['symbol']}: {e}")
                    continue
            
            conn.commit()
            return saved_count
    
    def get_latest_prices(self, exchange: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает последние цены для всех или конкретной биржи
        
        Args:
            exchange: Название биржи (опционально)
        
        Returns:
            Список последних цен
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if exchange:
                cursor.execute('''
                    SELECT symbol, exchange, price, timestamp
                    FROM trading_pairs
                    WHERE exchange = ?
                    AND timestamp = (
                        SELECT MAX(timestamp)
                        FROM trading_pairs t2
                        WHERE t2.symbol = trading_pairs.symbol
                        AND t2.exchange = trading_pairs.exchange
                    )
                    ORDER BY symbol
                ''', (exchange,))
            else:
                cursor.execute('''
                    SELECT symbol, exchange, price, timestamp
                    FROM trading_pairs
                    WHERE timestamp = (
                        SELECT MAX(timestamp)
                        FROM trading_pairs t2
                        WHERE t2.symbol = trading_pairs.symbol
                        AND t2.exchange = trading_pairs.exchange
                    )
                    ORDER BY exchange, symbol
                ''')
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'symbol': row[0],
                    'exchange': row[1],
                    'price': row[2],
                    'timestamp': row[3]
                })
            
            return results
    
    def calculate_price_differences(self) -> List[Dict[str, Any]]:
        """
        Вычисляет разности цен между биржами для одинаковых символов
        
        Returns:
            Список сравнений цен
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Получаем последние цены для каждой биржи
            cursor.execute('''
                WITH latest_prices AS (
                    SELECT symbol, exchange, price, timestamp,
                           ROW_NUMBER() OVER (PARTITION BY symbol, exchange ORDER BY timestamp DESC) as rn
                    FROM trading_pairs
                )
                SELECT 
                    p1.symbol,
                    p1.exchange as exchange1,
                    p1.price as price1,
                    p2.exchange as exchange2,
                    p2.price as price2,
                    ABS(p1.price - p2.price) as price_difference,
                    ABS(p1.price - p2.price) / ((p1.price + p2.price) / 2) * 100 as percentage_difference
                FROM latest_prices p1
                JOIN latest_prices p2 ON p1.symbol = p2.symbol AND p1.exchange < p2.exchange
                WHERE p1.rn = 1 AND p2.rn = 1
                ORDER BY price_difference DESC
            ''')
            
            results = []
            for row in cursor.fetchall():
                comparison = {
                    'symbol': row[0],
                    'exchange1': row[1],
                    'price1': row[2],
                    'exchange2': row[3],
                    'price2': row[4],
                    'price_difference': row[5],
                    'percentage_difference': row[6]
                }
                results.append(comparison)
                
                # Сохраняем сравнение в базу
                cursor.execute('''
                    INSERT INTO price_comparisons 
                    (symbol, exchange1, price1, exchange2, price2, price_difference, percentage_difference)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    comparison['symbol'],
                    comparison['exchange1'],
                    comparison['price1'],
                    comparison['exchange2'],
                    comparison['price2'],
                    comparison['price_difference'],
                    comparison['percentage_difference']
                ))
            
            conn.commit()
            return results
    
    def get_top_differences(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Получает топ различий в ценах
        
        Args:
            limit: Количество записей для возврата
        
        Returns:
            Список топ различий
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT symbol, exchange1, price1, exchange2, price2, 
                       price_difference, percentage_difference, timestamp
                FROM price_comparisons
                ORDER BY price_difference DESC
                LIMIT ?
            ''', (limit,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'symbol': row[0],
                    'exchange1': row[1],
                    'price1': row[2],
                    'exchange2': row[3],
                    'price2': row[4],
                    'price_difference': row[5],
                    'percentage_difference': row[6],
                    'timestamp': row[7]
                })
            
            return results
    
    def get_exchange_stats(self) -> Dict[str, Any]:
        """
        Получает статистику по биржам
        
        Returns:
            Словарь со статистикой
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Количество пар по биржам
            cursor.execute('''
                SELECT exchange, COUNT(DISTINCT symbol) as pair_count
                FROM trading_pairs
                WHERE timestamp = (
                    SELECT MAX(timestamp)
                    FROM trading_pairs t2
                    WHERE t2.symbol = trading_pairs.symbol
                    AND t2.exchange = trading_pairs.exchange
                )
                GROUP BY exchange
            ''')
            
            exchange_stats = {}
            for row in cursor.fetchall():
                exchange_stats[row[0]] = {
                    'pair_count': row[1]
                }
            
            return exchange_stats
    
    def clear_old_data(self, days: int = 7):
        """
        Удаляет старые данные
        
        Args:
            days: Количество дней для хранения данных
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                DELETE FROM trading_pairs
                WHERE timestamp < datetime('now', '-{} days')
            '''.format(days))
            
            cursor.execute('''
                DELETE FROM price_comparisons
                WHERE timestamp < datetime('now', '-{} days')
            '''.format(days))
            
            conn.commit()
            print(f"Удалены данные старше {days} дней")


def main():
    """Тестирование базы данных"""
    db = DatabaseManager()
    
    # Тестовые данные
    test_pairs = [
        {"symbol": "BTC", "price": 50000.0},
        {"symbol": "ETH", "price": 3000.0}
    ]
    
    # Сохраняем тестовые данные
    saved = db.save_trading_pairs("test_exchange", test_pairs)
    print(f"Сохранено {saved} пар")
    
    # Получаем данные
    latest = db.get_latest_prices()
    print(f"Получено {len(latest)} записей")
    
    # Статистика
    stats = db.get_exchange_stats()
    print(f"Статистика: {stats}")


if __name__ == "__main__":
    main()
