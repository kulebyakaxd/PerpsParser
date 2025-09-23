"""
Public Telegram bot that:
- Lets users select exchanges via inline keyboard (English labels)
- Shows Top-10 spreads (percentage) among selected exchanges
- Data refresh happens independently every 5 minutes (separate scheduler)

Environment:
- TELEGRAM_API / TELEGRAM_BOT_TOKEN / TELEGRAM_TOKEN / BOT_TOKEN
"""
import os
import asyncio
from typing import List
import contextlib

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from database import DatabaseManager
from utils.scheduler import periodic_refresh


EXCHANGES_ALL = ["hyperliquid", "lighter", "pacifica", "aster", "extended"]


def _get_bot_token() -> str:
    for k in ["TELEGRAM_API", "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "BOT_TOKEN"]:
        v = os.getenv(k)
        if v and v.strip():
            return v.strip()
    return ""


def _keyboard(selected: List[str]) -> InlineKeyboardMarkup:
    rows = []
    for ex in EXCHANGES_ALL:
        flag = "✅" if ex in selected else "☑️"
        rows.append([InlineKeyboardButton(f"{flag} {ex.capitalize()}", callback_data=f"toggle:{ex}")])
    # Interval row: 1, 5, 10, 15, 30, 60
    rows.append([
        InlineKeyboardButton("1m", callback_data="interval:1"),
        InlineKeyboardButton("5m", callback_data="interval:5"),
        InlineKeyboardButton("10m", callback_data="interval:10"),
        InlineKeyboardButton("15m", callback_data="interval:15"),
        InlineKeyboardButton("30m", callback_data="interval:30"),
        InlineKeyboardButton("60m", callback_data="interval:60"),
    ])
    rows.append([InlineKeyboardButton("Show Top-10", callback_data="show")])
    return InlineKeyboardMarkup(rows)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id:
        return
    db = DatabaseManager()
    selected = db.get_user_exchanges(user_id)
    interval = db.get_user_interval(user_id)
    await update.message.reply_text(
        f"Select at least 2 exchanges and interval (current: {interval} min):",
        reply_markup=_keyboard(selected),
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Commands:\n"\
        "/start - Configure exchanges\n"\
        "/top - Show Top-10 spreads for your selection\n"\
        "/settings - Configure exchanges and update interval"
    )


async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id:
        return
    db = DatabaseManager()
    exchanges = db.get_user_exchanges(user_id)
    if len(exchanges) < 2:
        await update.message.reply_text("Please select at least 2 exchanges via /start.")
        return
    items = db.get_top_differences_filtered(exchanges, limit=10)
    if not items:
        await update.message.reply_text("No data found. Please wait for the next refresh.")
        return
    text_lines = ["Top-10 spreads (%):"]
    for i, diff in enumerate(items, 1):
        pct = diff['percentage_difference']
        text_lines.append(
            f"{i}. {diff['symbol']} | {diff['exchange1']}: ${diff['price1']:.6f} | "
            f"{diff['exchange2']}: ${diff['price2']:.6f} | Δ%: {pct:.2f}"
        )
    await update.message.reply_text("\n".join(text_lines))


async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show settings UI (same as /start)."""
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id:
        return
    db = DatabaseManager()
    selected = db.get_user_exchanges(user_id)
    interval = db.get_user_interval(user_id)
    await update.message.reply_text(
        f"Select at least 2 exchanges and interval (current: {interval} min):",
        reply_markup=_keyboard(selected),
    )


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query:
        return
    query = update.callback_query
    user = update.effective_user
    user_id = user.id if user else None
    if not user_id:
        await query.answer()
        return
    db = DatabaseManager()
    data = query.data or ""
    if data.startswith("toggle:"):
        ex = data.split(":", 1)[1]
        selected = set(db.get_user_exchanges(user_id))
        if ex in selected:
            selected.remove(ex)
        else:
            selected.add(ex)
        db.set_user_exchanges(user_id, sorted(selected))
        await query.edit_message_reply_markup(reply_markup=_keyboard(list(selected)))
        await query.answer("Updated")
        return
    if data.startswith("interval:"):
        try:
            minutes = int(data.split(":", 1)[1])
        except Exception:
            minutes = 5
        db.set_user_interval(user_id, minutes)
        await query.answer(f"Interval set to {minutes} min")
        return
    if data == "show":
        exchanges = db.get_user_exchanges(user_id)
        if len(exchanges) < 2:
            await query.answer("Select at least 2 exchanges.", show_alert=True)
            return
        items = db.get_top_differences_filtered(exchanges, limit=10)
        if not items:
            await query.answer("No data yet. Please wait.", show_alert=True)
            return
        text_lines = ["Top-10 spreads (%):"]
        for i, diff in enumerate(items, 1):
            pct = diff['percentage_difference']
            text_lines.append(
                f"{i}. {diff['symbol']} | {diff['exchange1']}: ${diff['price1']:.6f} | "
                f"{diff['exchange2']}: ${diff['price2']:.6f} | Δ%: {pct:.2f}"
            )
        await query.answer()
        await query.message.reply_text("\n".join(text_lines))


async def run_bot() -> None:
    token = _get_bot_token()
    if not token:
        raise RuntimeError("Telegram bot token is not configured.")
    application = Application.builder().token(token).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("top", top_cmd))
    application.add_handler(CommandHandler("settings", settings_cmd))
    application.add_handler(CallbackQueryHandler(on_callback))

    # Per-user scheduled push via JobQueue
    async def push_top(context: ContextTypes.DEFAULT_TYPE):
        user_id = context.job.chat_id
        db = DatabaseManager()
        exchanges = db.get_user_exchanges(user_id)
        if len(exchanges) < 2:
            return
        items = db.get_top_differences_filtered(exchanges, limit=10)
        if not items:
            return
        text_lines = ["Top-10 spreads (%):"]
        for i, diff in enumerate(items, 1):
            pct = diff['percentage_difference']
            text_lines.append(
                f"{i}. {diff['symbol']} | {diff['exchange1']}: ${diff['price1']:.6f} | "
                f"{diff['exchange2']}: ${diff['price2']:.6f} | Δ%: {pct:.2f}"
            )
        try:
            await context.bot.send_message(chat_id=user_id, text="\n".join(text_lines))
        except Exception:
            pass

    # Helpers to (re)schedule on /start and interval changes
    async def schedule_for_user(user_id: int):
        db = DatabaseManager()
        minutes = db.get_user_interval(user_id)
        # Guard: JobQueue may be None if extras not installed
        jq = getattr(application, 'job_queue', None)
        if jq is None:
            return
        # Remove previous jobs
        for job in jq.get_jobs_by_name(str(user_id)):
            job.schedule_removal()
        jq.run_repeating(push_top, interval=minutes * 60, chat_id=user_id, name=str(user_id))

    # Hook into /start and /settings to (re)schedule
    async def post_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user:
            await schedule_for_user(update.effective_user.id)
    application.add_handler(CommandHandler("start", post_start), group=1)
    async def post_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user:
            await schedule_for_user(update.effective_user.id)
    application.add_handler(CommandHandler("settings", post_settings), group=1)
    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    # Start global periodic refresh task within bot process to avoid second runner
    loop = asyncio.get_running_loop()
    refresh_task = loop.create_task(periodic_refresh(300))
    # Keep running until cancelled
    try:
        while True:
            await asyncio.sleep(60)
    finally:
        refresh_task.cancel()
        with contextlib.suppress(Exception):
            await refresh_task
        await application.updater.stop()
        await application.stop()
        await application.shutdown()


if __name__ == "__main__":
    asyncio.run(run_bot())



