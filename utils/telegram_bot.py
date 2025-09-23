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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters

from database import DatabaseManager
from utils.scheduler import periodic_refresh


EXCHANGES_ALL = ["hyperliquid", "lighter", "pacifica", "aster", "extended"]


def _format_top_spreads(items: list) -> str:
    lines = ["📊 Top‑10 spreads"]
    for i, diff in enumerate(items, 1):
        pct = diff['percentage_difference']
        symbol = diff['symbol']
        ex1 = str(diff['exchange1']).capitalize()
        ex2 = str(diff['exchange2']).capitalize()
        price1 = diff['price1']
        price2 = diff['price2']
        lines.append(
            f"{i}. {symbol} — Δ {pct:.2f}% | {ex1} ${price1:.4f} • {ex2} ${price2:.4f}"
        )
    return "\n".join(lines)


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
    # Welcome and quick how-to (EN) + author mention
    if update.message:
        await update.message.reply_text(
            "<b>Soft made by</b> <a href=\"https://t.me/xartmoves\">@xartmoves</a>\n\n"
            "👋 <b>Welcome!</b>\n\n"
            "This bot aggregates prices from Hyperliquid, Lighter, Pacifica, Aster and Extended\n"
            "and shows the Top‑10 spreads (%).\n\n"
            "How to use:\n"
            "1) Open /start and select at least 2 exchanges;\n"
            "2) Choose update interval (1–60 minutes);\n"
            "3) Tap \"Show Top-10\" or use /top.\n\n"
            "Commands:\n"
            "• /start — configure exchanges and interval\n"
            "• /top — show Top‑10 spreads\n"
            "• /settings — open settings\n"
            "• /help — help",
            parse_mode=ParseMode.HTML,
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("Top"), KeyboardButton("Settings")]],
                resize_keyboard=True,
            ),
        )
    await update.message.reply_text(
        f"Select at least 2 exchanges and interval (current: {interval} min):",
        reply_markup=_keyboard(selected),
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            "<b>Soft made by</b> <a href=\"https://t.me/xartmoves\">@xartmoves</a>\n\n"
            "ℹ️ <b>Help</b>\n\n"
            "This bot shows the Top‑10 spreads (%) among your selected exchanges.\n\n"
            "Commands:\n"
            "• /start — configure exchanges and interval\n"
            "• /top — show Top‑10 spreads\n"
            "• /settings — open settings\n\n"
            "Tips: select at least 2 exchanges and an interval between 1–60 minutes.",
            parse_mode=ParseMode.HTML,
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
    await update.message.reply_text(_format_top_spreads(items))


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
        await query.answer()
        await query.message.reply_text(_format_top_spreads(items))


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
    # Reply keyboard buttons without slash
    application.add_handler(MessageHandler(filters.Regex(r"(?i)^top$"), top_cmd))
    application.add_handler(MessageHandler(filters.Regex(r"(?i)^settings$"), settings_cmd))

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
        try:
            await context.bot.send_message(chat_id=user_id, text=_format_top_spreads(items))
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



