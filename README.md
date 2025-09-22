# PerpsParser

Парсер для получения данных о торговых парах с различных бирж деривативов с сохранением в базу данных SQLite.

## Возможности

- ✅ **Hyperliquid**: 211 торговых пар
- ✅ **Lighter**: 84 торговые пары  
- ✅ **База данных SQLite**: Автоматическое сохранение и сравнение цен
- ✅ **Сравнение цен**: Находит различия между биржами
- ✅ **Топ различий**: Показывает 5 самых больших разниц в ценах
- ✅ **Статистика**: Подробная аналитика по биржам

## Установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

## Использование

### Основной парсер с базой данных

```bash
python main_db.py
```

### Просмотр данных

```bash
python utils/view_data.py
```

### Отдельные парсеры

```python
from parsers import HyperliquidParser, LighterParser
import asyncio

async def get_data():
    # Hyperliquid
    hyperliquid_parser = HyperliquidParser()
    hyperliquid_pairs = await hyperliquid_parser.get_pairs_with_prices()
    
    # Lighter  
    lighter_parser = LighterParser()
    lighter_pairs = await lighter_parser.get_pairs_with_prices()
    
    return hyperliquid_pairs, lighter_pairs

# Запуск
pairs = asyncio.run(get_data())
```

## Структура базы данных

### Таблица `trading_pairs`
- `id` - Уникальный идентификатор
- `symbol` - Символ торговой пары
- `exchange` - Название биржи
- `price` - Цена
- `timestamp` - Время получения данных

### Таблица `price_comparisons`
- `id` - Уникальный идентификатор
- `symbol` - Символ торговой пары
- `exchange1`, `exchange2` - Сравниваемые биржи
- `price1`, `price2` - Цены на биржах
- `price_difference` - Абсолютная разница в цене
- `percentage_difference` - Процентная разница
- `timestamp` - Время сравнения

## Структура проекта

```
PerpsParser/
├── parsers/                 # Парсеры для различных бирж
│   ├── __init__.py
│   ├── hyperliquid_parser.py
│   └── lighter_parser.py
├── database/               # Работа с базой данных
│   ├── __init__.py
│   └── database.py
├── utils/                  # Утилиты
│   ├── __init__.py
│   └── view_data.py
├── data/                   # Данные и файлы БД
│   └── trading_pairs.db
├── main.py                 # Основной файл для Hyperliquid
├── main_db.py             # Основной файл с базой данных
├── requirements.txt
└── README.md
```

## Файлы

- `main_db.py` - Основной файл с базой данных
- `main.py` - Основной файл для Hyperliquid
- `parsers/` - Пакет парсеров для различных бирж
- `database/` - Пакет для работы с SQLite
- `utils/` - Утилиты для просмотра данных
- `data/` - Данные и файлы базы данных

## Примеры использования

### Получение топ-5 различий в ценах
```python
from database import DatabaseManager

db = DatabaseManager()
top_differences = db.get_top_differences(5)
for diff in top_differences:
    print(f"{diff['symbol']}: {diff['exchange1']} ${diff['price1']} vs {diff['exchange2']} ${diff['price2']}")
```

### Статистика по биржам
```python
from database import DatabaseManager

db = DatabaseManager()
stats = db.get_exchange_stats()
print(f"Hyperliquid: {stats['hyperliquid']['pair_count']} пар")
print(f"Lighter: {stats['lighter']['pair_count']} пар")
```
