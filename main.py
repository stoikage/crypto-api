"""
binance-fastapi + RFQ Telegram bot  (python-telegram-bot 22  •  SQLite on /db)

ENV VARS ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN      BotFather token
WEBHOOK_SECRET      random string (same in setWebhook & header check)
ADMIN_CHAT_ID       numeric ID of the traders group
ALLOWED_SYMBOLS     comma list, e.g. "BTC,ETH,SOL"
RFQ_TTL             seconds before un-quoted RFQ expires   (default 120)
MAX_VALIDITY        max seconds a quote can be valid       (default 120)
DB_PATH             optional, default "/db/rfq.sqlite"
PUBLIC_BASE_URL     optional override for public URL (useful locally)
"""

import asyncio, json, logging, os
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Optional

import httpx
import numpy as np
from fastapi import FastAPI, HTTPException, Request, Response
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler,
    CommandHandler, ContextTypes, Defaults
)
from sqlmodel import SQLModel, Field, Session, create_engine

# ───────────────────── CONFIG ────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)

BOT_TOKEN       = os.environ["TELEGRAM_TOKEN"]
WEBHOOK_SECRET  = os.environ["WEBHOOK_SECRET"]
ADMIN_CHAT_ID   = int(os.environ["ADMIN_CHAT_ID"])
ALLOWED_SYMBOLS = {s.strip().upper() for s in os.environ["ALLOWED_SYMBOLS"].split(",")}

RFQ_TTL      = int(os.getenv("RFQ_TTL", "120"))
MAX_VALIDITY = int(os.getenv("MAX_VALIDITY", "120"))

PUBLIC_BASE_URL = (
    os.getenv("PUBLIC_BASE_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or (f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}"
        if os.getenv("RENDER_EXTERNAL_HOSTNAME") else None)
)
if not PUBLIC_BASE_URL:
    raise RuntimeError("Set PUBLIC_BASE_URL or run inside Render")
PUBLIC_BASE_URL = PUBLIC_BASE_URL.rstrip("/")

DB_PATH = os.getenv("DB_PATH", "/db/rfq.sqlite")

# ───────────────────── DATABASE ──────────────────────────────────────────
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    echo=False,
    connect_args={"check_same_thread": False},
)

class Ticket(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    side: str
    symbol: str
    qty: float
    client_chat_id: int
    client_msg_id: Optional[int] = None
    trader_msg_id: Optional[int] = None
    status: str = "open"              # open | quoted | traded
    price: Optional[float] = None
    valid_until: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class TicketEvent(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ticket_id: int = Field(index=True)
    event: str
    payload: Optional[str] = None
    ts: datetime = Field(default_factory=datetime.utcnow)

def init_db() -> None:
    SQLModel.metadata.create_all(engine)

def db() -> Session:
    # keep loaded attributes alive after commit/close
    return Session(engine, expire_on_commit=False)

def log_event(ticket_id: int, event: str, payload: dict | None = None) -> None:
    with db() as s:
        s.add(
            TicketEvent(
                ticket_id=ticket_id,
                event=event,
                payload=json.dumps(payload) if payload else None,
            )
        )
        s.commit()

# ───────────────────── TELEGRAM BOT ──────────────────────────────────────
defaults = Defaults(parse_mode=ParseMode.HTML)
ptb: Application = ApplicationBuilder().token(BOT_TOKEN).defaults(defaults).build()

# ─────────── helpers ─────────────────────────────────────────────────────
def fmt_waiting(t: Ticket) -> str:
    return (
        f"🆕  Ticket #{t.id}\n"
        f"────────────────────\n"
        f"SIDE    :  {t.side.upper()}\n"
        f"QTY     :  {t.qty} {t.symbol}\n"
        f"────────────────────\n"
        f"STATUS  :  Waiting for price…\n\n"
        f"🔄 Refresh to update."
    )

def fmt_quoted(t: Ticket, remain: int) -> str:
    remain = max(0, remain)
    return (
        f"🆕  Ticket #{t.id}\n"
        f"────────────────────\n"
        f"SIDE    :  {t.side.upper()}\n"
        f"QTY     :  {t.qty} {t.symbol}\n"
        f"────────────────────\n"
        f"Price   :  {t.price} (valid {remain}s)\n\n"
        f"Press ✅ Accept to trade.\n\n"
        f"🔄 Refresh to update."
    )

def fmt_traded(t: Ticket) -> str:
    return (
        f"✅ TRADE DONE\n"
        f"Ticket #{t.id} | {t.side.upper()} {t.qty} {t.symbol}\n"
        f"Price   :  {t.price}"
    )

def kb_open(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup.from_button(
        InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{tid}")
    )

def kb_quoted(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Accept",   callback_data=f"accept:{tid}"),
            InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{tid}"),
        ]
    ])

# ─────────── expiry task ────────────────────────────────────────────────
async def expire_worker(ticket_id: int, delay: int) -> None:
    await asyncio.sleep(delay)
    with db() as s:
        t: Ticket | None = s.get(Ticket, ticket_id)
        if not t or t.status != "quoted":
            return
        if t.valid_until and t.valid_until > datetime.utcnow():
            return

        # copy fields we still need
        client_chat_id = t.client_chat_id
        client_msg_id  = t.client_msg_id

        t.status = "open"
        t.price = None
        t.valid_until = None
        t.updated_at = datetime.utcnow()
        s.add(t)
        s.commit()

    if client_msg_id:
        await ptb.bot.edit_message_text(
            chat_id=client_chat_id,
            message_id=client_msg_id,
            text=fmt_waiting(t),
            reply_markup=kb_open(ticket_id),
        )
    await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=(
            f"⏳ RFQ #{ticket_id} expired without acceptance.\n"
            f"Please re-price:\n<code>/quote {ticket_id} price secs</code>"
        ),
    )
    log_event(ticket_id, "expired", {})

# ─────────── /rfq (client) ───────────────────────────────────────────────
async def cmd_rfq(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id == ADMIN_CHAT_ID:
        await update.message.reply_text("Traders can’t issue RFQs here.")
        return
    try:
        side, qty, symbol = (
            ctx.args[0].lower(),
            float(ctx.args[1]),
            ctx.args[2].upper(),
        )
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /rfq buy 10 SOL")
        return
    if side not in {"buy", "sell"} or symbol not in ALLOWED_SYMBOLS:
        await update.message.reply_text("Unsupported side or symbol.")
        return

    with db() as s:
        t = Ticket(
            side=side,
            symbol=symbol,
            qty=qty,
            client_chat_id=update.effective_chat.id,
        )
        s.add(t)
        s.commit()
        s.refresh(t)          # ensure ID populated

        # we need ID immediately for messages; keep copy outside
        tid = t.id

    client_msg = await update.message.reply_text(
        fmt_waiting(t),
        reply_markup=kb_open(tid),
    )
    trader_msg = await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=(
            f"📥 <b>RFQ #{tid}</b>\n"
            f"{update.effective_chat.title or update.effective_chat.id} wants "
            f"{side.upper()} {qty} {symbol}\n\n"
            f"<code>/quote {tid} price secs</code>"
        ),
    )

    with db() as s:
        t = s.get(Ticket, tid)
        t.client_msg_id = client_msg.message_id
        t.trader_msg_id = trader_msg.message_id
        s.add(t)
        s.commit()

    log_event(tid, "rfq_created", {})
    asyncio.create_task(expire_worker(tid, RFQ_TTL))

# ─────────── /quote (trader) ─────────────────────────────────────────────
async def cmd_quote(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_CHAT_ID:
        await update.message.reply_text("Only the trader group can quote.")
        return
    try:
        ticket_id = int(ctx.args[0])
        price = float(ctx.args[1])
        secs = int(ctx.args[2])
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /quote <id> <price> <secs>")
        return
    if secs > MAX_VALIDITY:
        await update.message.reply_text(f"Max validity is {MAX_VALIDITY}s.")
        return

    with db() as s:
        t: Ticket | None = s.get(Ticket, ticket_id)
        if not t:
            await update.message.reply_text("Ticket not found.")
            return
        if t.status == "traded":
            await update.message.reply_text("Ticket already traded.")
            return

        t.price = price
        t.status = "quoted"
        t.valid_until = datetime.utcnow() + timedelta(seconds=secs)
        t.updated_at = datetime.utcnow()
        s.add(t)
        s.commit()

        # copies for use after session
        client_chat_id, client_msg_id = t.client_chat_id, t.client_msg_id
        trader_msg_id                 = t.trader_msg_id

    await ptb.bot.edit_message_text(
        chat_id=ADMIN_CHAT_ID,
        message_id=trader_msg_id,
        text=(
            f"✅ RFQ #{ticket_id} priced {price} "
            f"(valid {secs}s) by @{update.effective_user.username}"
        ),
    )
    await ptb.bot.edit_message_text(
        chat_id=client_chat_id,
        message_id=client_msg_id,
        text=fmt_quoted(t, secs),
        reply_markup=kb_quoted(ticket_id),
    )

    log_event(ticket_id, "quoted",
              {"trader": update.effective_user.username, "price": price, "secs": secs})
    asyncio.create_task(expire_worker(ticket_id, secs))

# ─────────── Refresh & Accept callbacks ──────────────────────────────────
async def cb_refresh(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tid = int(query.data.split(":")[1])

    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not t:
            await query.edit_message_text("❌ Ticket not found.")
            return

        status = t.status
        valid_until = t.valid_until
        remain = max(0, int((valid_until - datetime.utcnow()).total_seconds())) if valid_until else 0

    if status == "quoted" and valid_until > datetime.utcnow():
        await query.edit_message_text(fmt_quoted(t, remain), reply_markup=kb_quoted(t.id))
    elif status == "traded":
        await query.edit_message_text(fmt_traded(t))
    else:
        await query.edit_message_text(fmt_waiting(t), reply_markup=kb_open(t.id))

async def cb_accept(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tid = int(query.data.split(":")[1])

    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not t or t.status != "quoted":
            await query.edit_message_text("❌ Quote no longer valid.")
            return
        if t.valid_until < datetime.utcnow():
            await query.edit_message_text("❌ Quote expired.")
            return

        t.status = "traded"
        t.updated_at = datetime.utcnow()
        s.add(t)
        s.commit()

        # copies for messages after session
        msg = fmt_traded(t)

    await query.edit_message_text(msg)
    await ptb.bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg)
    log_event(tid, "traded", {"user": query.from_user.username})

# ─────────── register handlers ───────────────────────────────────────────
ptb.add_handler(CommandHandler("rfq",   cmd_rfq))
ptb.add_handler(CommandHandler("quote", cmd_quote))
ptb.add_handler(CallbackQueryHandler(cb_refresh, pattern=r"^refresh:\d+$"))
ptb.add_handler(CallbackQueryHandler(cb_accept,  pattern=r"^accept:\d+$"))

# ───────────────────── FASTAPI / WEBHOOK ─────────────────────────────────
app = FastAPI()

@app.on_event("startup")
async def _startup() -> None:
    init_db()
    await ptb.initialize()                      # ① init first
    url = f"{PUBLIC_BASE_URL}/telegram"
    await ptb.bot.set_webhook(url, secret_token=WEBHOOK_SECRET,
                              drop_pending_updates=True)
    logging.info("Webhook set to %s", url)
    asyncio.create_task(ptb.start())            # ② then run

@app.on_event("shutdown")
async def _shutdown() -> None:
    await ptb.stop()

@app.post("/telegram")
async def telegram_webhook(req: Request):
    if req.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(401, "bad secret")
    update = Update.de_json(await req.json(), ptb.bot)
    await ptb.process_update(update)
    return Response(status_code=HTTPStatus.OK)

# ───────────────────── Health & Binance helpers (unchanged) ─────────────
@app.get("/")
def health(): return {"status": "alive"}

@app.get("/price/{symbol}")
async def price(symbol: str):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol.upper()}"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Binance ticker unavailable")
    j = r.json()
    return {"symbol": j["symbol"], "price": float(j["price"])}

@app.get("/funding/{symbol}")
async def funding(symbol: str):
    url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol.upper()}"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Binance funding unavailable")
    j = r.json()
    return {
        "symbol": j["symbol"],
        "markPrice": float(j["markPrice"]),
        "lastFundingRate": float(j["lastFundingRate"]),
        "nextFundingTime": int(j["nextFundingTime"]),
    }

@app.get("/rv/{symbol}")
async def realized_vol(symbol: str):
    url = (f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}"
           f"&interval=1d&limit=31")
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Failed to fetch price history")
    closes = [float(item[4]) for item in r.json()]
    if len(closes) < 31:
        raise HTTPException(400, "Insufficient data")
    returns = np.diff(np.log(closes))
    vol = np.std(returns) * np.sqrt(365)
    return {"symbol": symbol.upper(), "realized_vol_%": round(vol * 100, 2)}

def _clearing(book: list[list[float]], qty: float):
    filled, cost = 0.0, 0.0
    for price, size in book:
        take = min(qty - filled, size)
        cost += take * price
        filled += take
        if filled >= qty:
            break
    if filled == 0:
        return {"error": "no liquidity"}
    avg = cost / filled
    return {
        "filled": filled,
        "avg_price": round(avg, 6),
        "total_cost": round(cost, 2),
        "partial": filled < qty,
    }

@app.get("/clearing/spot/{symbol}")
async def clearing_spot(symbol: str, quantity: float):
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}&limit=1000"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Failed to fetch orderbook")
    data = r.json()
    bids = [[float(p), float(q)] for p, q in data["bids"]]
    asks = [[float(p), float(q)] for p, q in data["asks"]]
    return {
        "symbol": symbol.upper(),
        "venue": "spot",
        "quantity": quantity,
        "bid": _clearing(bids, quantity),
        "ask": _clearing(asks, quantity),
    }

@app.get("/clearing/perp/{symbol}")
async def clearing_perp(symbol: str, quantity: float):
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol.upper()}&limit=1000"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Failed to fetch perp orderbook")
    data = r.json()
    bids = [[float(p), float(q)] for p, q in data["bids"]]
    asks = [[float(p), float(q)] for p, q in data["asks"]]
    return {
        "symbol": symbol.upper(),
        "venue": "perp",
        "quantity": quantity,
        "bid": _clearing(bids, quantity),
        "ask": _clearing(asks, quantity),
    }
