"""
Утилита для отправки сообщений в Telegram бота.

Использует переменную окружения TELEGRAM_API (токен вида 12345:ABC...).
Опционально использует TELEGRAM_CHAT_ID для явного чата. Если chat_id не
передан при инициализации и нет TELEGRAM_CHAT_ID, сообщения не будут отправляться.

Поддерживает простой синхронный метод log(...) и асинхронный send(...).
"""
import os
import asyncio
from typing import Optional

import aiohttp


class TelegramNotifier:
    def __init__(self, chat_id: Optional[str] = None) -> None:
        # Поддержка нескольких возможных имен переменных окружения для удобства
        token_candidates = [
            os.getenv("TELEGRAM_API", ""),
            os.getenv("TELEGRAM_BOT_TOKEN", ""),
            os.getenv("TELEGRAM_TOKEN", ""),
            os.getenv("BOT_TOKEN", ""),
        ]
        chat_id_candidates = [
            (chat_id or ""),
            os.getenv("TELEGRAM_CHAT_ID", ""),
            os.getenv("TELEGRAM_USER_ID", ""),
            os.getenv("TELEGRAM_TO", ""),
            os.getenv("CHAT_ID", ""),
        ]
        self.token = next((t.strip() for t in token_candidates if t and t.strip()), "")
        cid = next((c.strip() for c in chat_id_candidates if c and c.strip()), "")
        self.chat_id = cid or None
        self.base_url = f"https://api.telegram.org/bot{self.token}" if self.token else None

    def is_configured(self) -> bool:
        # Не отправляем сообщения в каналы (chat_id начинается с -100)
        if self.chat_id and str(self.chat_id).startswith("-100"):
            return False
        return bool(self.base_url and self.chat_id)

    async def send(self, text: str) -> bool:
        """Асинхронно отправляет сообщение. Возвращает True при успехе."""
        if not self.is_configured():
            return False
        assert self.base_url is not None and self.chat_id is not None
        url = f"{self.base_url}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=15) as resp:
                    return resp.status == 200
        except Exception:
            return False

    def log(self, text: str) -> None:
        """Синхронный помощник: печатает в консоль и пытается отправить в Telegram."""
        print(text)
        # Не блокируем поток, запускаем fire-and-forget если уже есть цикл, иначе создаём временный
        if not self.is_configured():
            return
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.send(text))
            else:
                loop.run_until_complete(self.send(text))
        except RuntimeError:
            # Если нет активного цикла (или в чужом треде) — создаём свой
            try:
                asyncio.run(self.send(text))
            except Exception:
                pass


# Глобальный удобный инстанс для простого импорта
_global_notifier: Optional[TelegramNotifier] = None


def get_notifier(chat_id: Optional[str] = None) -> TelegramNotifier:
    global _global_notifier
    if _global_notifier is None or (chat_id and _global_notifier.chat_id != chat_id):
        _global_notifier = TelegramNotifier(chat_id)
    return _global_notifier


