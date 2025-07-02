# main.py
#
# Telegram “RFQ → Quote → Trade” bot
# FastAPI webhook -- works on Render / Fly / anywhere that gives you a URL.
#
# Requirements (see requirements.txt):
#   fastapi, uvicorn, httpx, sqlmodel==0.0.16, python-telegram-bot==22.2, aiosqlite
#
# ────────────────────────────────────────────────────────────────────────────
import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

import httpx
from fastapi import FastAPI, Request
from sqlmodel import SQLModel, Field, create_engine, Session, select
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
    constants,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

##############################################################################
#  ░█▀▀░█▀█░█▀▀░█░█░█░░              CONFIGURATION
##############################################################################
BOT_TOKEN: str = os.environ["BOT_TOKEN"]          # Telegram Bot token
WEBHOOK_URL: str = os.environ["WEBHOOK_URL"]      # e.g. https://my-bot.onrender.com/telegram
TRADER_CHAT_ID: int = int(os.environ["TRADER_CHAT_ID"])  # chat-id (or group) where the trader sits
DB_URL: str = os.environ.get("DATABASE_URL", "sqlite:///./db.sqlite3")

##############################################################################
#  ░█▄█░█▀█░█▀▀░▀█▀░█▀█░█▀█          DATABASE
##############################################################################
class Ticket(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    # business data
    side: str                      # "buy" / "sell"
    qty: float
    symbol: str = "SOL"

    # quoting
    price: Optional[float] = None
    price_expiry: Optional[datetime] = None

    # Telegram message bookkeeping
    client_msg_id: Optional[int] = None
    trader_msg_id: Optional[int] = None

    status: str = "waiting"        # waiting | priced | traded
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Event(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ticket_id: int = Field(index=True)
    ts: datetime = Field(default_factory=datetime.utcnow)
    kind: str                      # rfq_created | quoted | refreshed | trade_done | quote_expired
    payload: Dict[str, Any] = Field(sa_column_kwargs={"type_": "JSON"})


engine = create_engine(DB_URL, echo=False)
SQLModel.metadata.create_all(engine)

# We’ll turn off “expire_on_commit” so ORM instances stay attached after commit
SessionLocal = Session.bind_for(engine, expire_on_commit=False)

##############################################################################
#  ░█▀█░█▀█░█▄░█░█▀█░█▀█           TEXT / MARKUP HELPERS
##############################################################################
def fmt_waiting(t: Ticket) -> str:
    return (
        "🆕  Ticket #{id}\n"
        "────────────────────\n"
        "SIDE    :  {side}\n"
        "QTY     :  {qty} {sym}\n"
        "────────────────────\n"
        "STATUS  :  Waiting for price…\n\n"
        "🔄 Refresh to update."
    ).format(id=t.id, side=t.side.upper(), qty=t.qty, sym=t.symbol)


def fmt_priced(t: Ticket) -> str:
    secs_left = int((t.price_expiry - datetime.utcnow()).total_seconds())
    return (
        "🆕  Ticket #{id}\n"
        "────────────────────\n"
        "SIDE    :  {side}\n"
        "QTY     :  {qty} {sym}\n"
        "────────────────────\n"
        "Price: {px}  (valid {ttl}s)\n\n"
        "🔄 Refresh to update\n"
        "✅ Press Accept to trade."
    ).format(id=t.id, side=t.side.upper(), qty=t.qty, sym=t.symbol, px=t.price, ttl=secs_left)


def fmt_traded(t: Ticket) -> str:
    return (
        "✅  Ticket #{id}\n"
        "────────────────────\n"
        "SIDE    :  {side}\n"
        "QTY     :  {qty} {sym}\n"
        "────────────────────\n"
        "Price: {px}   (done)\n\n"
        "🎉 Trade executed."
    ).format(id=t.id, side=t.side.upper(), qty=t.qty, sym=t.symbol, px=t.price)


#
# inline-keyboard helpers ----------------------------------------------------
#
def kb_open(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{tid}")]])


def kb_priced(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Accept", callback_data=f"accept:{tid}"),
                InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{tid}"),
            ]
        ]
    )


def kb_traded() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([])  # no buttons

##############################################################################
#  ░█▀▀░█▀█░█░█░█▀▀░█▀▀            UTILITIES
##############################################################################
def db_ticket(tid: int) -> Ticket | None:
    with SessionLocal() as db:
        return db.exec(select(Ticket).where(Ticket.id == tid)).first()


def log_event(tid: int, kind: str, payload: Dict[str, Any] | None = None) -> None:
    with SessionLocal() as db:
        db.add(Event(ticket_id=tid, kind=kind, payload=payload or {}))
        db.commit()


def ticket_expired(t: Ticket) -> bool:
    return t.price_expiry is not None and t.price_expiry < datetime.utcnow()


##############################################################################
#  ░█▀█░█▀█░█░█░█▀▀░█░█            TELEGRAM COMMAND HANDLERS
##############################################################################
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hi!  /rfq <side> <qty> — create a new RFQ\n"
        "Trader: /quote <id> <price> <ttl_sec> — send a quote"
    )


async def cmd_rfq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Client: /rfq buy 10 or /rfq sell 5"""
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /rfq <buy/sell> <qty>")
        return

    side, qty_txt = context.args
    side = side.lower()
    if side not in ("buy", "sell"):
        await update.message.reply_text("side must be buy|sell")
        return
    try:
        qty = float(qty_txt)
    except ValueError:
        await update.message.reply_text("qty must be number")
        return

    # create ticket
    with SessionLocal() as db:
        t = Ticket(side=side, qty=qty)
        db.add(t)
        db.commit()  # generates id
        db.refresh(t)

        # send client message
        cmsg = await update.message.reply_text(fmt_waiting(t), reply_markup=kb_open(t.id), parse_mode=constants.ParseMode.HTML)
        t.client_msg_id = cmsg.message_id

        # send trader notice
        tmsg = await context.bot.send_message(
            TRADER_CHAT_ID,
            f"🔔 New RFQ #{t.id}  {side.upper()} {qty} {t.symbol}",
        )
        t.trader_msg_id = tmsg.message_id

        db.commit()

    log_event(t.id, "rfq_created", {})
    logging.info("RFQ %s created", t.id)


async def cmd_quote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Trader: /quote <ticket_id> <price> <ttl_sec>"""
    if len(context.args) != 3:
        await update.message.reply_text("Usage: /quote <id> <price> <ttl_sec>")
        return
    tid, px_txt, ttl_txt = context.args
    try:
        tid = int(tid)
        px = float(px_txt)
        ttl = int(ttl_txt)
    except ValueError:
        await update.message.reply_text("id=int, price=float, ttl=int")
        return

    with SessionLocal() as db:
        t = db_ticket(tid)
        if not t:
            await update.message.reply_text("Ticket not found")
            return
        if t.status == "traded":
            await update.message.reply_text("Already traded")
            return

        t.price = px
        t.price_expiry = datetime.utcnow() + timedelta(seconds=ttl)
        t.status = "priced"
        t.updated_at = datetime.utcnow()
        db.add(t)
        db.commit()

        # edit client message
        try:
            await context.bot.edit_message_text(
                fmt_priced(t),
                chat_id=update.effective_chat.id if update.effective_chat else None,
                message_id=t.client_msg_id,
                reply_markup=kb_priced(t.id),
                parse_mode=constants.ParseMode.HTML,
            )
        except Exception as e:
            logging.warning("edit client msg failed: %s", e)

        # update trader msg
        try:
            await context.bot.edit_message_text(
                f"💰 Quoted #{t.id} @ {px} (valid {ttl}s)",
                chat_id=TRADER_CHAT_ID,
                message_id=t.trader_msg_id,
            )
        except Exception:
            pass

    log_event(t.id, "quoted", {"px": px, "ttl": ttl})
    logging.info("Ticket %s quoted", t.id)


##############################################################################
#  ░█▀█░█▀█░█▀▄░█▀█░█░█            CALLBACK BUTTONS
##############################################################################
async def cb_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE, tid: int):
    query: CallbackQuery = update.callback_query
    await query.answer()

    with SessionLocal() as db:
        t = db_ticket(tid)
        if not t:
            await query.edit_message_text("Ticket not found")
            return

        # If quote expired, revert to waiting & ping trader
        if t.status == "priced" and ticket_expired(t):
            t.status = "waiting"
            t.price = None
            t.price_expiry = None
            db.add(t)
            db.commit()
            await context.bot.send_message(TRADER_CHAT_ID, f"⏰ Quote for #{t.id} expired – please re-price.")

        # choose correct text + kb
        if t.status == "waiting":
            text, kb = fmt_waiting(t), kb_open(t.id)
        elif t.status == "priced":
            text, kb = fmt_priced(t), kb_priced(t.id)
        else:  # traded
            text, kb = fmt_traded(t), kb_traded()

        try:
            await query.edit_message_text(text, reply_markup=kb)
        except Exception:
            # "message is not modified" → ignore
            pass

        log_event(t.id, "refreshed", {})


async def cb_accept(update: Update, context: ContextTypes.DEFAULT_TYPE, tid: int):
    query: CallbackQuery = update.callback_query
    await query.answer()

    with SessionLocal() as db:
        t = db_ticket(tid)
        if not t:
            await query.edit_message_text("Ticket not found")
            return
        if t.status != "priced":
            await query.answer("No active quote.", show_alert=True)
            return
        if ticket_expired(t):
            await query.answer("Quote expired. Press Refresh.", show_alert=True)
            return

        # mark traded
        t.status = "traded"
        t.updated_at = datetime.utcnow()
        db.add(t)
        db.commit()

        await query.edit_message_text(fmt_traded(t), reply_markup=kb_traded(), parse_mode=constants.ParseMode.HTML)

        # Notify trader
        await context.bot.send_message(
            TRADER_CHAT_ID,
            f"✅  Trade done   #{t.id}   {t.side.upper()} {t.qty} {t.symbol} @ {t.price}",
        )

    log_event(t.id, "trade_done", {})
    logging.info("Ticket %s traded", t.id)


async def cb_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delegates callback queries to appropriate handler."""
    data = update.callback_query.data
    if data.startswith("refresh:"):
        await cb_refresh(update, context, int(data.split(":")[1]))
    elif data.startswith("accept:"):
        await cb_accept(update, context, int(data.split(":")[1]))


##############################################################################
#  ░█▀▀░█░█░▀█▀░█▀▀░█▀▄            FASTAPI WEBHOOK
##############################################################################
app = FastAPI()
ptb_app: Application  # will be built in startup event


@app.on_event("startup")
async def on_startup():
    global ptb_app
    logging.basicConfig(level=logging.INFO)

    ptb_app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .build()
    )

    ptb_app.add_handler(CommandHandler("start", cmd_start))
    ptb_app.add_handler(CommandHandler("rfq", cmd_rfq))
    ptb_app.add_handler(CommandHandler("quote", cmd_quote))
    ptb_app.add_handler(CallbackQueryHandler(cb_router))

    # set webhook once
    async with httpx.AsyncClient() as cli:
        await cli.post(f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook", json={"url": WEBHOOK_URL})
    logging.info("Webhook set to %s", WEBHOOK_URL)

    # run telegram application in the background
    await ptb_app.initialize()
    await ptb_app.start()


@app.on_event("shutdown")
async def on_shutdown():
    await ptb_app.stop()
    await ptb_app.shutdown()


@app.post("/telegram")
async def telegram_webhook(req: Request):
    raw_update = await req.json()
    await ptb_app.process_update(Update.de_json(raw_update, ptb_app.bot))
    return "ok"


@app.get("/")
async def root():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}


# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # local dev:  uvicorn main:app --reload --port 8000
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
