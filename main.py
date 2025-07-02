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

import asyncio, json, logging, math, os
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Optional

import httpx
import numpy as np
from fastapi import FastAPI, HTTPException, Request, Response
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler,
    ContextTypes, Defaults,
)
from sqlmodel import SQLModel, Field, Session, create_engine

# ───────────────────── CONFIG ────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

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
    raise RuntimeError("Cannot discover PUBLIC_BASE_URL")
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
    status: str = "open"   # open | quoted | traded
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
    return Session(engine)

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

# ─────────── formatting helpers ─────────────────────────────────────────
def _client_name(chat_id: int) -> str:
    """Cheap client id → ‘name’; falls back to id if we can’t fetch."""
    try:
        chat = asyncio.run(ptb.bot.get_chat(chat_id))
        return chat.title or (chat.username or str(chat.id))
    except Exception:
        return str(chat_id)

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
    proceeds = round((t.price or 0) * t.qty, 6)
    client   = _client_name(t.client_chat_id)
    buyer    = "GSR" if t.side == "sell" else client
    seller   = client if t.side == "sell" else "GSR"
    trade_d  = datetime.utcnow().date().isoformat()
    return (
        f"✅ TRADE DONE\n"
        f"Ticket #{t.id} |\n"
        f"Trade Date : {trade_d}\n"
        f"Buyer      : {buyer}\n"
        f"Seller     : {seller}\n"
        f"Cross      : {t.symbol}/USD\n"
        f"Side       : {t.side.upper()}\n"
        f"Price      : {t.price}\n"
        f"Quantity   : {t.qty}\n"
        f"Proceeds   : {proceeds}\n"
        f"Trade Type : Spot\n"
        f"Settlement : T+0\n"
        f"—\n"
        f"Thanks for the trade\n"
        f"GSR OTC Trading Team"
    )

# ─────────── keyboards ──────────────────────────────────────────────────
def kb_open(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup.from_button(
        InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{tid}")
    )

def kb_quote_actions(tid: int) -> InlineKeyboardMarkup:
    """
    Row 1:   QUOTE
    Row 2:   Bid +10bps | Ask +10bps
    Row 3:   Bid +20bps | Ask +20bps
    """
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("💰 QUOTE", callback_data=f"autoquote:quote:{tid}")],
            [
                InlineKeyboardButton("Bid +10bps", callback_data=f"autoquote:bid10:{tid}"),
                InlineKeyboardButton("Ask +10bps", callback_data=f"autoquote:ask10:{tid}"),
            ],
            [
                InlineKeyboardButton("Bid +20bps", callback_data=f"autoquote:bid20:{tid}"),
                InlineKeyboardButton("Ask +20bps", callback_data=f"autoquote:ask20:{tid}"),
            ],
        ]
    )

# ─────────── Binance helper to compute clearing snapshot ────────────────
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
    return {"avg_price": round(avg, 6), "filled": filled}

async def get_snapshot(symbol: str, qty: float) -> dict:
    """Return {'mid': mid, 'bid': bid_avg, 'ask': ask_avg} for perp book."""
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol.upper()}USDT&limit=1000"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Failed to fetch orderbook")
    data = r.json()
    bids = [[float(p), float(q)] for p, q in data["bids"]]
    asks = [[float(p), float(q)] for p, q in data["asks"]]
    best_bid, best_ask = bids[0][0], asks[0][0]
    mid = round((best_bid + best_ask) / 2, 6)
    bid_avg = _clearing(bids, qty)["avg_price"]
    ask_avg = _clearing(asks, qty)["avg_price"]
    return {"mid": mid, "bid": bid_avg, "ask": ask_avg}

# ─────────── expiry task ────────────────────────────────────────────────
async def expire_worker(ticket_id: int, delay: int) -> None:
    await asyncio.sleep(delay)
    with db() as s:
        t: Ticket | None = s.get(Ticket, ticket_id)
        if not t or t.status != "quoted":
            return
        if t.valid_until and t.valid_until > datetime.utcnow():
            return
        t.status = "open"
        t.price = None
        t.valid_until = None
        t.updated_at = datetime.utcnow()
        s.add(t)
        s.commit()
    if t.client_msg_id:
        await ptb.bot.edit_message_text(
            chat_id=t.client_chat_id,
            message_id=t.client_msg_id,
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
        s.refresh(t)

    client_msg = await update.message.reply_text(
        fmt_waiting(t),
        reply_markup=kb_open(t.id),
    )

    # --------------------------------------------------------------
    # build trader message with snapshot
    # --------------------------------------------------------------
    try:
        snap = await get_snapshot(symbol, qty)
        bid_bps = round((snap["bid"] / snap["mid"] - 1) * 10_000, 1)
        ask_bps = round((snap["ask"] / snap["mid"] - 1) * 10_000, 1)
        trader_text = (
            f"📥 <b>RFQ #{t.id}</b>\n"
            f"{_client_name(t.client_chat_id)} wants {side.upper()} {qty} {symbol}\n\n"
            f"MID: {snap['mid']}\n"
            f"BID: {snap['bid']} ({bid_bps:+} bps)\n"
            f"ASK: {snap['ask']} ({ask_bps:+} bps)"
        )
    except Exception as e:
        logging.warning("snapshot fail: %s", e)
        trader_text = (
            f"📥 <b>RFQ #{t.id}</b>\n"
            f"{_client_name(t.client_chat_id)} wants {side.upper()} {qty} {symbol}\n\n"
            f"<code>/quote {t.id} price secs</code>"
        )

    trader_msg = await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=trader_text,
        reply_markup=kb_quote_actions(t.id),
    )

    with db() as s:
        t.client_msg_id = client_msg.message_id
        t.trader_msg_id = trader_msg.message_id
        s.add(t)
        s.commit()

    log_event(t.id, "rfq_created", {})
    asyncio.create_task(expire_worker(t.id, RFQ_TTL))

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
        # copy fields before session closes
        client_chat_id, client_msg_id = t.client_chat_id, t.client_msg_id
        trader_msg_id = t.trader_msg_id
        side, qty, sym = t.side, t.qty, t.symbol

    await ptb.bot.edit_message_text(
        chat_id=ADMIN_CHAT_ID,
        message_id=trader_msg_id,
        text=(
            f"✅ RFQ #{ticket_id} priced {price} "
            f"(valid {secs}s) by @{update.effective_user.username}"
        ),
    )

    remain = secs
    await ptb.bot.edit_message_text(
        chat_id=client_chat_id,
        message_id=client_msg_id,
        text=fmt_quoted(t, remain),
        reply_markup=kb_quote_actions(ticket_id),
    )

    log_event(ticket_id, "quoted", {"trader": update.effective_user.username, "price": price, "secs": secs})
    asyncio.create_task(expire_worker(ticket_id, secs))

# ─────────── Auto-quote callback  ────────────────────────────────────────
async def cb_autoquote(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    action, tid_s = q.data.split(":")[1:]
    tid = int(tid_s)

    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not t:
            await q.edit_message_text("❌ Ticket not found.")
            return
        if t.status == "traded":
            await q.edit_message_text("❌ Ticket already traded.")
            return
        side, qty, sym = t.side, t.qty, t.symbol

    # snapshot
    try:
        snap = await get_snapshot(sym, qty)
    except HTTPException as e:
        await q.edit_message_text(f"⚠️ Snapshot error: {e.detail}")
        return

    base = snap["ask"] if side == "buy" else snap["bid"]
    bump_bps = {"bid10": 10, "bid20": 20, "ask10": 10, "ask20": 20}.get(action, 0)
    if action.startswith("bid"):
        # bid side → shade inside (subtract bps)
        price = base * (1 - bump_bps / 10_000)
    elif action.startswith("ask"):
        # ask side → widen (add bps)
        price = base * (1 + bump_bps / 10_000)
    else:  # plain QUOTE button
        price = base
    price = round(price, 6)

    secs = 90
    # reuse /quote flow internally
    quote_cmd = f"/quote {tid} {price} {secs}"
    await ctx.application.process_update(
        Update.de_json(
            {
                "update_id": 0,
                "message": {
                    "chat": {"id": ADMIN_CHAT_ID, "type": "group"},
                    "from":  {"id": q.from_user.id, "username": q.from_user.username},
                    "text":  quote_cmd,
                    "entities": [{"offset": 0, "length": 6, "type": "bot_command"}],
                },
            },
            ctx.bot,
        )
    )
    await q.edit_message_text("✅ Auto-quote sent.")

# ─────────── Refresh & Accept callbacks ──────────────────────────────────
async def cb_refresh(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    tid = int(q.data.split(":")[1])
    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not t:
            await q.edit_message_text("❌ Ticket not found.")
            return

    if t.status == "quoted" and t.valid_until > datetime.utcnow():
        remain = max(0, int((t.valid_until - datetime.utcnow()).total_seconds()))
        await q.edit_message_text(fmt_quoted(t, remain), reply_markup=kb_quote_actions(t.id))
    elif t.status == "traded":
        await q.edit_message_text(fmt_traded(t))
    else:
        await q.edit_message_text(fmt_waiting(t), reply_markup=kb_open(t.id))

async def cb_accept(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    tid = int(q.data.split(":")[1])
    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not t or t.status != "quoted":
            await q.edit_message_text("❌ Quote no longer valid.")
            return
        if t.valid_until < datetime.utcnow():
            await q.edit_message_text("❌ Quote expired.")
            return
        t.status = "traded"
        t.updated_at = datetime.utcnow()
        s.add(t)
        s.commit()
        # copy for out-of-session access
        client_msg_id, client_chat_id = t.client_msg_id, t.client_chat_id

    await q.edit_message_text(fmt_traded(t))
    await ptb.bot.send_message(chat_id=ADMIN_CHAT_ID, text=fmt_traded(t))
    log_event(tid, "traded", {"user": q.from_user.username})

# ─────────── register handlers ───────────────────────────────────────────
ptb.add_handler(CommandHandler("rfq",   cmd_rfq))
ptb.add_handler(CommandHandler("quote", cmd_quote))
ptb.add_handler(CallbackQueryHandler(cb_refresh,    pattern=r"^refresh:\d+$"))
ptb.add_handler(CallbackQueryHandler(cb_accept,     pattern=r"^accept:\d+$"))
ptb.add_handler(CallbackQueryHandler(cb_autoquote,
    pattern=r"^autoquote:(quote|bid10|bid20|ask10|ask20):\d+$"))

# ───────────────────── FASTAPI / WEBHOOK ─────────────────────────────────
app = FastAPI()

@app.on_event("startup")
async def _startup() -> None:
    init_db()
    url = f"{PUBLIC_BASE_URL}/telegram"
    await ptb.bot.set_webhook(url, secret_token=WEBHOOK_SECRET, drop_pending_updates=True)
    logging.info("Webhook set to %s", url)
    await ptb.initialize()
    asyncio.create_task(ptb.start())

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

# ───────────────────── Health + public endpoints (unchanged) ────────────
@app.get("/")
def health(): return {"status": "alive"}

@app.get("/price/{symbol}")
async def price(symbol: str):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol.upper()}USDT"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Binance ticker unavailable")
    j = r.json()
    return {"symbol": j["symbol"], "price": float(j["price"])}

@app.get("/funding/{symbol}")
async def funding(symbol: str):
    url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol.upper()}USDT"
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
    url = (f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}USDT"
           f"&interval=1d&limit=31")
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Failed to fetch price history")
    closes = [float(item[4]) for item in r.json()]
    if len(closes) < 31:
        raise HTTPException(400, "Insufficient data")
    returns = np.diff(np.log(closes))
    vol = np.std(returns) * math.sqrt(365)
    return {"symbol": symbol.upper(), "realized_vol_%": round(vol * 100, 2)}

@app.get("/clearing/spot/{symbol}")
async def clearing_spot(symbol: str, quantity: float):
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}USDT&limit=1000"
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
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol.upper()}USDT&limit=1000"
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
