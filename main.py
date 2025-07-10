"""
binance-fastapi + RFQ Telegram bot             (python-telegram-bot 22 • SQLite)

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

import httpx, numpy as np
from fastapi import FastAPI, HTTPException, Request, Response
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, ApplicationBuilder,
    CallbackQueryHandler, CommandHandler,
    ContextTypes, Defaults,
)
from sqlmodel import SQLModel, Field, Session, create_engine

# ───────────────────── CONFIG ────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)

BOT_TOKEN       = os.environ["TELEGRAM_TOKEN"]
WEBHOOK_SECRET  = os.environ["WEBHOOK_SECRET"]
ADMIN_CHAT_ID   = int(os.environ["ADMIN_CHAT_ID"])
ALLOWED_SYMBOLS = {s.strip().upper() for s in os.environ["ALLOWED_SYMBOLS"].split(",")}

RFQ_TTL      = int(os.getenv("RFQ_TTL",  "120"))
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
    status: str = "open"           # open | quoted | traded
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

def log_event(tid: int, event: str, payload: dict | None = None):
    with db() as s:
        s.add(TicketEvent(
            ticket_id=tid, event=event,
            payload=json.dumps(payload) if payload else None))
        s.commit()

# ───────────────────── TELEGRAM BOT ──────────────────────────────────────
defaults = Defaults(parse_mode=ParseMode.HTML)
ptb: Application = ApplicationBuilder().token(BOT_TOKEN).defaults(defaults).build()

# ─────────── keyboards & formatting ──────────────────────────────────────
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

def kb_quote_actions(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("QUOTE", callback_data=f"quote:{tid}")],
        [
            InlineKeyboardButton("Bid –10 bps", callback_data=f"bid10:{tid}"),
            InlineKeyboardButton("Ask +10 bps", callback_data=f"ask10:{tid}")
        ],
        [
            InlineKeyboardButton("Bid –20 bps", callback_data=f"bid20:{tid}"),
            InlineKeyboardButton("Ask +20 bps", callback_data=f"ask20:{tid}")
        ],
    ])

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
    proceeds = round(t.qty * t.price, 2)
    today = datetime.utcnow().date().isoformat()
    buyer, seller = ("GSR", f"Chat {t.client_chat_id}") if t.side == "buy" else (f"Chat {t.client_chat_id}", "GSR")
    return (
        "✅ TRADE DONE\n"
        f"Ticket #{t.id}\n"
        "────────────────────\n"
        f"Trade Date :  {today}\n"
        f"Buyer      :  {buyer}\n"
        f"Seller     :  {seller}\n"
        f"Cross      :  {t.symbol}/USD\n"
        f"Side       :  {t.side.upper()}\n"
        f"Price      :  {t.price}\n"
        f"Quantity   :  {t.qty}\n"
        f"Proceeds   :  {proceeds}\n"
        f"Trade Type :  Spot\n"
        f"Settlement :  T+0\n"
        "────────────────────\n"
        "Thanks for the trade\n"
        "GSR OTC Trading Team"
    )

# ─────────── Helpers for market data ─────────────────────────────────────
async def _binance_mid(symbol: str) -> float:
    """
    True mid-price = (best bid + best ask) / 2, taken from the top of the book.
    """
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol.upper()}USDT&limit=5"
    async with httpx.AsyncClient() as c:
        r = await c.get(url, timeout=5)
    if r.status_code != 200:
        raise RuntimeError("Binance depth unavailable")
    data = r.json()
    best_bid = float(data["bids"][0][0])
    best_ask = float(data["asks"][0][0])
    return (best_bid + best_ask) / 2


async def _clearing_avg(symbol: str, side: str, qty: float) -> float:
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol.upper()}USDT&limit=1000"
    async with httpx.AsyncClient() as c:
        r = await c.get(url, timeout=5)
    if r.status_code != 200:
        raise RuntimeError("Binance depth unavailable")
    data = r.json()
    book = data["asks"] if side == "buy" else data["bids"]
    filled, cost = 0.0, 0.0
    for p, q in book:
        p, q = float(p), float(q)
        take = min(qty - filled, q)
        cost += take * p
        filled += take
        if filled >= qty:
            break
    if filled == 0:
        raise RuntimeError("No liquidity")
    return cost / filled

# ─────────── expiry task ────────────────────────────────────────────────
async def expire_worker(tid: int, delay: int):
    await asyncio.sleep(delay)
    with db() as s:
        t = s.get(Ticket, tid)
        if not t or t.status != "quoted":
            return
        if t.valid_until and t.valid_until > datetime.utcnow():
            return
        t.status, t.price, t.valid_until = "open", None, None
        t.updated_at = datetime.utcnow()
        s.add(t);  s.commit()
        client_chat_id, client_msg_id = t.client_chat_id, t.client_msg_id

    await ptb.bot.edit_message_text(
        chat_id=client_chat_id, message_id=client_msg_id,
        text=fmt_waiting(t), reply_markup=kb_open(tid)
    )
    await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=f"⏳ RFQ #{tid} expired. Please re-price:\n<code>/quote {tid} price secs</code>"
    )
    log_event(tid, "expired", {})

# ─────────── /rfq (client) ───────────────────────────────────────────────
async def cmd_rfq(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_chat.id == ADMIN_CHAT_ID:
        await upd.message.reply_text("Traders can’t issue RFQs here.")
        return
    try:
        side, qty, symbol = upd.message.text.split()[1:4]
        side = side.lower();  qty = float(qty);  symbol = symbol.upper()
    except Exception:
        await upd.message.reply_text("Usage: /rfq buy 10 SOL")
        return
    if side not in {"buy", "sell"} or symbol not in ALLOWED_SYMBOLS:
        await upd.message.reply_text("Unsupported side or symbol.")
        return

    # create ticket
    with db() as s:
        t = Ticket(side=side, symbol=symbol, qty=qty, client_chat_id=upd.effective_chat.id)
        s.add(t); s.commit(); s.refresh(t)
        tid = t.id

    # send client msg
    cmsg = await upd.message.reply_text(fmt_waiting(t), reply_markup=kb_open(tid))

    # compute market data for trader pad
    mid  = await _binance_mid(symbol)
    avg_bid = await _clearing_avg(symbol, "sell", qty)
    avg_ask = await _clearing_avg(symbol, "buy",  qty)
    bps_bid = round((avg_bid/mid - 1)*10000, 1)
    bps_ask = round((avg_ask/mid - 1)*10000, 1)

    tr_msg = await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=(
            f"📥 <b>RFQ #{tid}</b>\n"
            f"CLIENT wants {side.upper()} {qty} {symbol}\n"
            f"MID : {mid}\n"
            f"BID : {avg_bid} ({bps_bid:+} bps)\n"
            f"ASK : {avg_ask} ({bps_ask:+} bps)"
        ),
        reply_markup=kb_quote_actions(tid)
    )

    # update ticket with message IDs
    with db() as s:
        t = s.get(Ticket, tid)
        t.client_msg_id, t.trader_msg_id = cmsg.message_id, tr_msg.message_id
        s.add(t); s.commit()
    log_event(tid, "rfq_created", {})
    asyncio.create_task(expire_worker(tid, RFQ_TTL))

# ─────────── /quote (manual trader command) ─────────────────────────────
async def cmd_quote(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_chat.id != ADMIN_CHAT_ID:
        await upd.message.reply_text("Only the trader group can quote.")
        return
    try:
        tid, price, secs = int(ctx.args[0]), float(ctx.args[1]), int(ctx.args[2])
    except (IndexError, ValueError):
        await upd.message.reply_text("Usage: /quote <id> <price> <secs>")
        return
    if secs > MAX_VALIDITY:
        await upd.message.reply_text(f"Max validity {MAX_VALIDITY}s.")
        return
    await apply_quote(
        tid=tid, price=price, secs=secs,
        trader_username=upd.effective_user.username or ""
    )

# ─────────── Auto-quote callback (buttons) ───────────────────────────────
async def cb_autoquote(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query;  await q.answer()
    action, tid_str = q.data.split(":");  tid = int(tid_str)

    with db() as s:
        t = s.get(Ticket, tid)
        if not t or t.status == "traded":
            await q.edit_message_text("❌ Ticket closed.");  return
        qty, symbol, side = t.qty, t.symbol, t.side

    mid  = await _binance_mid(symbol)
    if action == "quote":          # bigger side
        price = await _clearing_avg(symbol, "buy" if side=="buy" else "sell", qty)
    else:
        kind, bps = action[:3], int(action[3:])
        avg_side  = "sell" if kind == "bid" else "buy"
        avg       = await _clearing_avg(symbol, avg_side, qty)
        sign      = -1 if kind == "bid" else +1        # bids are negative bps
        price     = round(avg * (1 + sign*bps/10000), 6)

    await apply_quote(
        tid=tid, price=price, secs=90,
        trader_username=q.from_user.username or ""
    )

async def apply_quote(tid: int, price: float, secs: int, trader_username: str):
    valid_until = datetime.utcnow() + timedelta(seconds=secs)

    # DB update
    with db() as s:
        t = s.get(Ticket, tid)
        if not t or t.status == "traded":
            return
        t.status, t.price, t.valid_until, t.updated_at = "quoted", price, valid_until, datetime.utcnow()
        s.add(t); s.commit()
        client_chat_id, client_msg_id = t.client_chat_id, t.client_msg_id
        trader_msg_id                 = t.trader_msg_id

    # edit trader pad (buttons stay)
    await ptb.bot.edit_message_text(
        chat_id=ADMIN_CHAT_ID, message_id=trader_msg_id,
        text=(f"✅ RFQ #{tid} priced {price} "
              f"(valid {secs}s) by @{trader_username}"),
        reply_markup=kb_quote_actions(tid)
    )
    # edit client view
    await ptb.bot.edit_message_text(
        chat_id=client_chat_id, message_id=client_msg_id,
        text=fmt_quoted(t, secs), reply_markup=kb_quoted(tid)
    )
    log_event(tid, "quoted", {"trader": trader_username, "price": price, "secs": secs})
    asyncio.create_task(expire_worker(tid, secs))

# ─────────── Refresh & Accept callbacks ──────────────────────────────────
async def cb_refresh(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query;  await q.answer()
    tid = int(q.data.split(":")[1])

    with db() as s:
        t = s.get(Ticket, tid)
        if not t:
            await q.edit_message_text("❌ Ticket not found.");  return

        if t.status == "quoted" and t.valid_until > datetime.utcnow():
            remain = int((t.valid_until - datetime.utcnow()).total_seconds())
            await q.edit_message_text(fmt_quoted(t, remain), reply_markup=kb_quoted(tid))
        elif t.status == "traded":
            await q.edit_message_text(fmt_traded(t))
        else:
            await q.edit_message_text(fmt_waiting(t), reply_markup=kb_open(tid))

async def cb_accept(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query;  await q.answer()
    tid = int(q.data.split(":")[1])

    with db() as s:
        t = s.get(Ticket, tid)
        if not t or t.status != "quoted":
            await q.edit_message_text("❌ Quote no longer valid.");  return
        if t.valid_until < datetime.utcnow():
            await q.edit_message_text("❌ Quote expired.");  return
        t.status, t.updated_at = "traded", datetime.utcnow()
        s.add(t); s.commit()
        trader_msg_id = t.trader_msg_id

    await q.edit_message_text(fmt_traded(t))
    await ptb.bot.send_message(chat_id=ADMIN_CHAT_ID, text=fmt_traded(t))
    log_event(tid, "traded", {"user": q.from_user.username or ""})

# ─────────── register handlers ───────────────────────────────────────────
ptb.add_handler(CommandHandler("rfq",   cmd_rfq))
ptb.add_handler(CommandHandler("quote", cmd_quote))
ptb.add_handler(CallbackQueryHandler(cb_autoquote, pattern=r"^(quote|bid10|ask10|bid20|ask20):\d+$"))
ptb.add_handler(CallbackQueryHandler(cb_refresh,   pattern=r"^refresh:\d+$"))
ptb.add_handler(CallbackQueryHandler(cb_accept,    pattern=r"^accept:\d+$"))

# ───────────────────── FASTAPI / WEBHOOK ─────────────────────────────────
app = FastAPI()

@app.on_event("startup")
async def _startup():
    init_db()
    url = f"{PUBLIC_BASE_URL}/telegram"
    await ptb.bot.set_webhook(url, secret_token=WEBHOOK_SECRET, drop_pending_updates=True)
    logging.info("Webhook set to %s", url)
    await ptb.initialize()
    asyncio.create_task(ptb.start())

@app.on_event("shutdown")
async def _shutdown():
    await ptb.stop()

@app.post("/telegram")
async def telegram_webhook(req: Request):
    if req.headers.get("X-telegram-bot-api-secret-token") != WEBHOOK_SECRET:
        raise HTTPException(401, "bad secret")
    upd = Update.de_json(await req.json(), ptb.bot)
    await ptb.process_update(upd)
    return Response(status_code=HTTPStatus.OK)

# ───────────────────── Health & Data helpers ────────────────────────────
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
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}USDT&interval=1d&limit=31"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Failed to fetch price history")
    closes = [float(item[4]) for item in r.json()]
    if len(closes) < 31:
        raise HTTPException(400, "Insufficient data")
    returns = np.diff(np.log(closes))
    vol = np.std(returns) * np.sqrt(365)
    return {"symbol": symbol.upper(), "realized_vol_%": round(vol*100, 2)}

# ─────────── Clearing price helpers (spot & perp) ───────────────────────
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

@app.get("/liquidity/spot/{symbol}")
async def liquidity_info_spot(symbol: str, quantity: float):
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}USDT&limit=1000"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Failed to fetch orderbook")
    data = r.json()

    bids = [[float(p), float(q)] for p, q in data["bids"]]
    asks = [[float(p), float(q)] for p, q in data["asks"]]
    
    if not bids or not asks:
        raise HTTPException(400, "Orderbook too thin")

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2

    def get_depth_price(book: list[list[float]]) -> float:
        filled = 0.0
        for price, size in book:
            filled += size
            if filled >= quantity:
                return price
        raise HTTPException(400, "Orderbook too thin")

    def _clearing(book: list[list[float]], qty: float):
        filled, cost = 0.0, 0.0
        for price, size in book:
            take = min(qty - filled, size)
            cost += take * price
            filled += take
            if filled >= qty:
                break
        if filled == 0:
            raise HTTPException(400, "Orderbook too thin")
        avg = cost / filled
        return avg

    try:
        bid_depth_price = get_depth_price(bids)
        ask_depth_price = get_depth_price(asks)
        bid_clearing = _clearing(bids, quantity)
        ask_clearing = _clearing(asks, quantity)
    except HTTPException as e:
        raise e

    def bps(p): return (p - mid) / mid * 10000

    return {
        "symbol": symbol.upper(),
        "venue": "spot",
        "quantity": quantity,
        "mid": mid,
        "bid": {
            "depth_price": bid_depth_price,
            "spread": bid_depth_price - mid,
            "bps": bps(bid_depth_price),
            "clearing_price": bid_clearing,
            "clearing_bps": bps(bid_clearing),
        },
        "ask": {
            "depth_price": ask_depth_price,
            "spread": ask_depth_price - mid,
            "bps": bps(ask_depth_price),
            "clearing_price": ask_clearing,
            "clearing_bps": bps(ask_clearing),
        }
    }

@app.get("/liquidity/spot/bybit/{symbol}")
async def liquidity_info_bybit_spot(symbol: str, quantity: float):
    symbol_pair = symbol.upper() + "USDT"
    url = f"https://api.bybit.com/v5/market/orderbook?category=spot&symbol={symbol_pair}&limit=200"

    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Bybit orderbook unavailable")

    data = r.json()
    result = data.get("result", {})
    bids = [[float(p), float(q)] for p, q in result.get("b", [])]
    asks = [[float(p), float(q)] for p, q in result.get("a", [])]

    if not bids or not asks:
        raise HTTPException(400, "Orderbook too thin")

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2

    def get_depth_price(book: list[list[float]]) -> float:
        filled = 0.0
        for price, size in book:
            filled += size
            if filled >= quantity:
                return price
        raise HTTPException(400, "Orderbook too thin")

    def _clearing(book: list[list[float]], qty: float):
        filled, cost = 0.0, 0.0
        for price, size in book:
            take = min(qty - filled, size)
            cost += take * price
            filled += take
            if filled >= qty:
                break
        if filled == 0:
            raise HTTPException(400, "Orderbook too thin")
        return cost / filled

    def bps(p): return (p - mid) / mid * 10000

    bid_depth_price = get_depth_price(bids)
    ask_depth_price = get_depth_price(asks)
    bid_clearing = _clearing(bids, quantity)
    ask_clearing = _clearing(asks, quantity)

    return {
        "symbol": symbol.upper(),
        "venue": "bybit",
        "quantity": quantity,
        "mid": mid,
        "bid": {
            "depth_price": bid_depth_price,
            "spread": bid_depth_price - mid,
            "bps": bps(bid_depth_price),
            "clearing_price": bid_clearing,
            "clearing_bps": bps(bid_clearing),
        },
        "ask": {
            "depth_price": ask_depth_price,
            "spread": ask_depth_price - mid,
            "bps": bps(ask_depth_price),
            "clearing_price": ask_clearing,
            "clearing_bps": bps(ask_clearing),
        }
    }

@app.get("/price/bybit/{symbol}")
async def price_bybit(symbol: str):
    symbol_pair = symbol.upper() + "USDT"
    url = f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={symbol_pair}"

    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Bybit price unavailable")

    data = r.json()
    result = data.get("result", {}).get("list", [])
    if not result or "lastPrice" not in result[0]:
        raise HTTPException(400, "Invalid symbol or no data")

    return {
        "symbol": result[0]["symbol"],
        "price": float(result[0]["lastPrice"]),
    }

@app.get("/liquidity/spot/okx/{symbol}")
async def liquidity_info_okx_spot(symbol: str, quantity: float):
    inst_id = f"{symbol.upper()}-USDT"
    url = f"https://www.okx.com/api/v5/market/books?instId={inst_id}&sz=400"

    headers = {
        "User-Agent": "okx-liquidity-fetcher/1.0"
    }

    async with httpx.AsyncClient() as c:
        r = await c.get(url, headers=headers)

    data = r.json()
    if r.status_code != 200 or data.get("code") != "0":
        raise HTTPException(502, "OKX orderbook unavailable")

    book_data = data.get("data", [{}])[0]
    bids = [[float(level[0]), float(level[1])] for level in book_data.get("bids", [])]
    asks = [[float(level[0]), float(level[1])] for level in book_data.get("asks", [])]

    if not bids or not asks:
        raise HTTPException(400, "Orderbook too thin")

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2

    def get_depth_price(book: list[list[float]]) -> float:
        filled = 0.0
        for price, size in book:
            filled += size
            if filled >= quantity:
                return price
        raise HTTPException(400, "Orderbook too thin")

    def _clearing(book: list[list[float]], qty: float):
        filled, cost = 0.0, 0.0
        for price, size in book:
            take = min(qty - filled, size)
            cost += take * price
            filled += take
            if filled >= qty:
                break
        if filled == 0:
            raise HTTPException(400, "Orderbook too thin")
        return cost / filled

    def bps(p): return (p - mid) / mid * 10000

    bid_depth_price = get_depth_price(bids)
    ask_depth_price = get_depth_price(asks)
    bid_clearing = _clearing(bids, quantity)
    ask_clearing = _clearing(asks, quantity)

    return {
        "symbol": symbol.upper(),
        "venue": "okx",
        "quantity": quantity,
        "mid": mid,
        "bid": {
            "depth_price": bid_depth_price,
            "spread": bid_depth_price - mid,
            "bps": bps(bid_depth_price),
            "clearing_price": bid_clearing,
            "clearing_bps": bps(bid_clearing),
        },
        "ask": {
            "depth_price": ask_depth_price,
            "spread": ask_depth_price - mid,
            "bps": bps(ask_depth_price),
            "clearing_price": ask_clearing,
            "clearing_bps": bps(ask_clearing),
        }
    }


@app.get("/price/okx/{symbol}")
async def price_okx(symbol: str):
    inst_id = f"{symbol.upper()}-USDT"
    url = f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"

    headers = {
        "User-Agent": "okx-liquidity-fetcher/1.0"
    }

    async with httpx.AsyncClient() as c:
        r = await c.get(url, headers=headers)

    data = r.json()
    if r.status_code != 200 or data.get("code") != "0":
        raise HTTPException(502, "OKX price unavailable")

    result = data.get("data", [{}])[0]
    if "last" not in result:
        raise HTTPException(400, "Invalid symbol or no price found")

    return {
        "symbol": result["instId"],
        "price": float(result["last"]),
    }


@app.get("/price/equity/{symbol}")
async def price_equity(symbol: str):
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
    if not POLYGON_API_KEY:
        raise HTTPException(status_code=500, detail="Polygon API key missing.")

    url = f"https://api.polygon.io/v2/last/trade/stocks/{symbol.upper()}?apiKey={POLYGON_API_KEY}"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)

    if r.status_code != 200:
        raise HTTPException(status_code=502, detail="Polygon API request failed.")

    data = r.json()
    try:
        result = data["results"]
        return {
            "symbol": symbol.upper(),
            "price": result["p"],
            "timestamp": result["t"]
        }
    except KeyError:
        raise HTTPException(status_code=400, detail=f"Invalid response from Polygon for symbol '{symbol}'.")
