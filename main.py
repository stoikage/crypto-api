"""
binance-fastapi + RFQ Telegram bot  (python-telegram-bot 22  •  SQLite on /db)

ENV VARS …
"""
import asyncio, json, logging, os
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Optional

import httpx, numpy as np
from fastapi import FastAPI, HTTPException, Request, Response
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler,
    CommandHandler, ContextTypes, Defaults,
)
from sqlmodel import SQLModel, Field, Session, create_engine

# ───────────────────────── CONFIG ────────────────────────────
logging.basicConfig(level=logging.INFO)

BOT_TOKEN       = os.environ["TELEGRAM_TOKEN"]
WEBHOOK_SECRET  = os.environ["WEBHOOK_SECRET"]
ADMIN_CHAT_ID   = int(os.environ["ADMIN_CHAT_ID"])
ALLOWED_SYMBOLS = {s.strip().upper()
                   for s in os.environ["ALLOWED_SYMBOLS"].split(",")}

RFQ_TTL      = int(os.getenv("RFQ_TTL", "120"))
MAX_VALIDITY = int(os.getenv("MAX_VALIDITY", "120"))

PUBLIC_BASE_URL = (
    os.getenv("PUBLIC_BASE_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or (f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}"
        if os.getenv("RENDER_EXTERNAL_HOSTNAME") else None)
)
if not PUBLIC_BASE_URL:
    raise RuntimeError("Cannot discover PUBLIC_BASE_URL")
PUBLIC_BASE_URL = PUBLIC_BASE_URL.rstrip("/")

DB_PATH = os.getenv("DB_PATH", "/db/rfq.sqlite")

# ───────────────────────── DATABASE ──────────────────────────
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
    status: str = "open"                 # open | quoted | traded
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

def init_db(): SQLModel.metadata.create_all(engine)
def db() -> Session: return Session(engine)

def log_event(tid: int, ev: str, pl: dict | None = None):
    with db() as s:
        s.add(TicketEvent(ticket_id=tid, event=ev,
                          payload=json.dumps(pl) if pl else None))
        s.commit()

# ───────────────────────── BOT SET-UP ────────────────────────
defaults = Defaults(parse_mode=ParseMode.HTML)
ptb = ApplicationBuilder().token(BOT_TOKEN).defaults(defaults).build()

# ────────── Formatting helpers ───────────────────────────────
def fmt_waiting(t: Ticket) -> str:
    return (
        f"🆕  Ticket #{t.id}\n"
        "────────────────────\n"
        f"SIDE    :  {t.side.upper()}\n"
        f"QTY     :  {t.qty} {t.symbol}\n"
        "────────────────────\n"
        "STATUS  :  Waiting for price…\n\n"
        "🔄 Refresh to update."
    )

def fmt_quoted(t: Ticket, remain: int) -> str:
    return (
        f"🆕  Ticket #{t.id}\n"
        "────────────────────\n"
        f"SIDE    :  {t.side.upper()}\n"
        f"QTY     :  {t.qty} {t.symbol}\n"
        "────────────────────\n"
        f"Price   :  {t.price} (valid {remain}s)\n\n"
        "Press ✅ Accept to trade.\n\n"
        "🔄 Refresh to update."
    )

def _client_name(chat_id: int) -> str:
    # try to fetch from bot cache; fall back to chat-id
    chat = ptb.bot.chat_data.get(chat_id, {})
    return chat.get("title") or str(chat_id)

def fmt_traded(t: Ticket) -> str:
    client = _client_name(t.client_chat_id)
    proceeds = round((t.price or 0) * t.qty, 2)
    trade_date = datetime.utcnow().strftime("%Y-%m-%d")

    buyer = client if t.side == "buy" else "GSR"
    seller = "GSR" if t.side == "buy" else client

    return (
        "✅ TRADE DONE\n"
        f"Ticket #{t.id} |\n"
        f"Trade Date : {trade_date}\n"
        f"Buyer      : {buyer}\n"
        f"Seller     : {seller}\n"
        f"Cross      : {t.symbol}/USD\n"
        f"Side       : {t.side.upper()}\n"
        f"Price      : {t.price}\n"
        f"Quantity   : {t.qty}\n"
        f"Proceeds   : {proceeds}\n"
        "Trade Type : Spot\n"
        "Settlement : T+0\n"
        "—\n"
        "Thanks for the trade\n"
        "GSR OTC Trading Team"
    )

def kb_open(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup.from_button(
        InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{tid}")
    )

def kb_quoted(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Accept",  callback_data=f"accept:{tid}"),
            InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{tid}"),
        ]
    ])

# ────────── Expiry background task ───────────────────────────
async def expire_worker(tid: int, delay: int):
    await asyncio.sleep(delay)
    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not t or t.status != "quoted": return
        if t.valid_until and t.valid_until > datetime.utcnow(): return

        t.status, t.price, t.valid_until = "open", None, None
        t.updated_at = datetime.utcnow()
        s.add(t); s.commit()

    if t.client_msg_id:
        await ptb.bot.edit_message_text(
            chat_id=t.client_chat_id, message_id=t.client_msg_id,
            text=fmt_waiting(t), reply_markup=kb_open(t.id)
        )
    await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=(f"⏳ RFQ #{tid} expired. Please re-price:\n"
              f"<code>/quote {tid} price secs</code>")
    )
    log_event(tid, "expired", {})

# ────────── /rfq (client) ────────────────────────────────────
async def cmd_rfq(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id == ADMIN_CHAT_ID:
        await update.message.reply_text("Traders can’t issue RFQs here.")
        return
    try:
        side, qty, symbol = ctx.args[0].lower(), float(ctx.args[1]), ctx.args[2].upper()
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /rfq buy 10 SOL")
        return
    if side not in {"buy", "sell"} or symbol not in ALLOWED_SYMBOLS:
        await update.message.reply_text("Unsupported side or symbol.")
        return

    with db() as s:
        t = Ticket(side=side, symbol=symbol, qty=qty,
                   client_chat_id=update.effective_chat.id)
        s.add(t); s.commit(); s.refresh(t)

    c_msg = await update.message.reply_text(fmt_waiting(t), reply_markup=kb_open(t.id))
    t_msg = await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=(f"📥 <b>RFQ #{t.id}</b>\n{update.effective_chat.title or update.effective_chat.id} "
              f"wants {side.upper()} {qty} {symbol}\n\n"
              f"<code>/quote {t.id} price secs</code>")
    )
    with db() as s:
        t.client_msg_id, t.trader_msg_id = c_msg.message_id, t_msg.message_id
        s.add(t); s.commit()

    log_event(t.id, "rfq_created", {})
    asyncio.create_task(expire_worker(t.id, RFQ_TTL))

# ────────── /quote (trader) ──────────────────────────────────
async def cmd_quote(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_CHAT_ID:
        await update.message.reply_text("Only the trader group can quote.")
        return
    try:
        tid, price, secs = int(ctx.args[0]), float(ctx.args[1]), int(ctx.args[2])
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /quote <id> <price> <secs>")
        return
    if secs > MAX_VALIDITY:
        await update.message.reply_text(f"Max validity is {MAX_VALIDITY}s.")
        return

    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not t:         await update.message.reply_text("Ticket not found."); return
        if t.status == "traded": await update.message.reply_text("Ticket already traded."); return
        t.price, t.status = price, "quoted"
        t.valid_until = datetime.utcnow() + timedelta(seconds=secs)
        t.updated_at = datetime.utcnow()
        s.add(t); s.commit()

    await ptb.bot.edit_message_text(
        chat_id=ADMIN_CHAT_ID, message_id=t.trader_msg_id,
        text=(f"✅ RFQ #{tid} priced {price} (valid {secs}s) "
              f"by @{update.effective_user.username}")
    )
    await ptb.bot.edit_message_text(
        chat_id=t.client_chat_id, message_id=t.client_msg_id,
        text=fmt_quoted(t, secs), reply_markup=kb_quoted(t.id)
    )
    log_event(tid, "quoted", {"trader": update.effective_user.username,
                              "price": price, "secs": secs})
    asyncio.create_task(expire_worker(tid, secs))

# ────────── Callbacks: refresh / accept ──────────────────────
async def cb_refresh(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    tid = int(q.data.split(":")[1])

    with db() as s: t: Ticket | None = s.get(Ticket, tid)
    if not t: await q.edit_message_text("❌ Ticket not found."); return

    if t.status == "quoted" and t.valid_until > datetime.utcnow():
        remain = max(0, int((t.valid_until - datetime.utcnow()).total_seconds()))
        await q.edit_message_text(fmt_quoted(t, remain), reply_markup=kb_quoted(t.id))
    elif t.status == "traded":
        await q.edit_message_text(fmt_traded(t))
    else:
        await q.edit_message_text(fmt_waiting(t), reply_markup=kb_open(t.id))

async def cb_accept(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    tid = int(q.data.split(":")[1])

    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not t or t.status != "quoted":
            await q.edit_message_text("❌ Quote no longer valid."); return
        if t.valid_until < datetime.utcnow():
            await q.edit_message_text("❌ Quote expired."); return
        t.status, t.updated_at = "traded", datetime.utcnow()
        s.add(t); s.commit()

    await q.edit_message_text(fmt_traded(t))
    await ptb.bot.send_message(chat_id=ADMIN_CHAT_ID, text=fmt_traded(t))
    log_event(tid, "traded", {"user": q.from_user.username})

# ────────── handlers ─────────────────────────────────────────
ptb.add_handler(CommandHandler("rfq",   cmd_rfq))
ptb.add_handler(CommandHandler("quote", cmd_quote))
ptb.add_handler(CallbackQueryHandler(cb_refresh, pattern=r"^refresh:\d+$"))
ptb.add_handler(CallbackQueryHandler(cb_accept,  pattern=r"^accept:\d+$"))

# ────────── FASTAPI & webhook ────────────────────────────────
app = FastAPI()

@app.on_event("startup")
async def _startup():
    init_db()
    url = f"{PUBLIC_BASE_URL}/telegram"
    await ptb.bot.set_webhook(url, secret_token=WEBHOOK_SECRET,
                              drop_pending_updates=True)
    logging.info("Webhook set to %s", url)
    await ptb.initialize()
    asyncio.create_task(ptb.start())

@app.on_event("shutdown")
async def _shutdown(): await ptb.stop()

@app.post("/telegram")
async def telegram_webhook(req: Request):
    if req.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(401, "bad secret")
    update = Update.de_json(await req.json(), ptb.bot)
    await ptb.process_update(update)
    return Response(status_code=HTTPStatus.OK)

# ────────── health + Binance helper routes (unchanged) ──────
@app.get("/")
def health(): return {"status": "alive"}

# … remaining /price, /funding, /rv, /clearing endpoints unchanged …


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
