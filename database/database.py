"""
Модуль для работы с базой данных SQLite
"""
import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import math


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

            # Уникальный индекс на текущий снэпшот
            try:
                cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS ux_trading_pairs_latest ON trading_pairs(symbol, exchange)')
            except sqlite3.IntegrityError:
                pass

            # Дедупликация price_comparisons и уникальный индекс
            cursor.execute('''
                DELETE FROM price_comparisons
                WHERE rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM price_comparisons
                    GROUP BY symbol, exchange1, exchange2
                )
            ''')
            try:
                cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS ux_price_comp ON price_comparisons(symbol, exchange1, exchange2)')
            except sqlite3.IntegrityError:
                pass
            
            conn.commit()
    
    def maintenance_snapshot(self, valid_exchanges: Optional[List[str]] = None):
        """Очищает невалидные биржи, записи с price<=0 и оставляет только последние записи по (symbol, exchange)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if valid_exchanges:
                placeholders = ','.join('?' for _ in valid_exchanges)
                cursor.execute(f"DELETE FROM trading_pairs WHERE exchange NOT IN ({placeholders})", valid_exchanges)
            # Удаляем нулевые/некорректные цены
            cursor.execute('DELETE FROM trading_pairs WHERE price IS NULL OR price<=0 OR NOT (price<1000000000000.0)')
            # Удаляем все записи, которые не являются последними по времени для своей пары
            cursor.execute('''
                DELETE FROM trading_pairs AS tp
                WHERE EXISTS (
                    SELECT 1 FROM trading_pairs t2
                    WHERE t2.symbol = tp.symbol
                      AND t2.exchange = tp.exchange
                      AND t2.timestamp > tp.timestamp
                )
            ''')
            conn.commit()

    def sync_exchange_snapshot(self, exchange: str, valid_symbols: List[str]):
        """Удаляет из trading_pairs все записи указанной биржи, символы которых не присутствуют в текущем списке."""
        if not valid_symbols:
            return
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            placeholders = ','.join('?' for _ in valid_symbols)
            cursor.execute(f"DELETE FROM trading_pairs WHERE exchange = ? AND symbol NOT IN ({placeholders})", [exchange, *valid_symbols])
            # Заодно почистим нулевые цены на этой бирже
            cursor.execute("DELETE FROM trading_pairs WHERE exchange = ? AND (price IS NULL OR price<=0)", (exchange,))
            conn.commit()
    
    def save_trading_pairs(self, exchange: str, pairs: List[Dict[str, Any]]) -> int:
        """
        Сохраняет торговые пары в базу данных, поддерживая один актуальный снэпшот на (symbol, exchange)
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            saved_count = 0
            now_ts = datetime.now()
            for pair in pairs:
                try:
                    symbol = str(pair['symbol']).strip().upper()
                    price_val = float(pair['price'])
                    if not symbol or not math.isfinite(price_val) or price_val <= 0:
                        continue
                    cursor.execute('''
                        INSERT INTO trading_pairs (symbol, exchange, price, timestamp)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(symbol, exchange) DO UPDATE SET
                            price=excluded.price,
                            timestamp=excluded.timestamp
                    ''', (
                        symbol,
                        exchange,
                        price_val,
                        now_ts
                    ))
                    saved_count += 1
                except (ValueError, TypeError):
                    continue
                except sqlite3.Error as e:
                    print(f"Ошибка при сохранении пары {pair.get('symbol')}: {e}")
                    continue
            
            conn.commit()
            return saved_count
    
    def get_latest_prices(self, exchange: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает текущие цены. Так как у нас один снэпшот на (symbol, exchange), запросы проще.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if exchange:
                cursor.execute('''
                    SELECT symbol, exchange, price, timestamp
                    FROM trading_pairs
                    WHERE exchange = ?
                    ORDER BY symbol
                ''', (exchange,))
            else:
                cursor.execute('''
                    SELECT symbol, exchange, price, timestamp
                    FROM trading_pairs
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
        Возвращаемый и записанный в БД набор — актуальный снимок. Старые записи очищаются.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Полностью очищаем предыдущие сравнения, чтобы в таблице был только актуальный снимок
            cursor.execute('DELETE FROM price_comparisons')
            
            # Так как trading_pairs теперь хранит один снэпшот на (symbol, exchange), джоин простой
            cursor.execute('''
                SELECT 
                    p1.symbol,
                    p1.exchange as exchange1,
                    p1.price as price1,
                    p2.exchange as exchange2,
                    p2.price as price2,
                    ABS(p1.price - p2.price) as price_difference,
                    ABS(p1.price - p2.price) / ((p1.price + p2.price) / 2) * 100 as percentage_difference
                FROM trading_pairs p1
                JOIN trading_pairs p2 ON p1.symbol = p2.symbol AND p1.exchange < p2.exchange
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
                
                cursor.execute('''
                    INSERT INTO price_comparisons 
                    (symbol, exchange1, price1, exchange2, price2, price_difference, percentage_difference)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, exchange1, exchange2) DO UPDATE SET
                        price1=excluded.price1,
                        price2=excluded.price2,
                        price_difference=excluded.price_difference,
                        percentage_difference=excluded.percentage_difference,
                        timestamp=CURRENT_TIMESTAMP
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
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT exchange, COUNT(DISTINCT symbol) as pair_count
                FROM trading_pairs
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
    db = DatabaseManager()
    db.maintenance_snapshot(valid_exchanges=["hyperliquid", "lighter", "pacifica"])  # cleanup
    print('Объём trading_pairs:', len(db.get_latest_prices()))

if __name__ == "__main__":
    main()
