"""
binance-fastapi + RFQ Telegram bot
──────────────────────────────────
( python-telegram-bot 22  •  SQLite on /db )

ENV VARS ────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN      BotFather token
WEBHOOK_SECRET      random string (same in setWebhook & header check)
ADMIN_CHAT_ID       numeric ID of the traders group
ALLOWED_SYMBOLS     comma list, e.g. "BTC,ETH,SOL"
RFQ_TTL             seconds before un-quoted RFQ expires   (default 120)
MAX_VALIDITY        max seconds a quote can be valid       (default 120)
DB_PATH             optional, default "/db/rfq.sqlite"
PUBLIC_BASE_URL     optional public URL (auto discovered on Render)
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
    Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler,
    ContextTypes, Defaults,
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
    side: str                      # buy | sell
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

def log_event(ticket_id: int, event: str, payload: dict | None = None) -> None:
    with db() as s:
        s.add(TicketEvent(
            ticket_id=ticket_id,
            event=event,
            payload=json.dumps(payload) if payload else None,
        ))
        s.commit()

# ───────────────────── TELEGRAM BOT ──────────────────────────────────────
defaults = Defaults(parse_mode=ParseMode.HTML)
ptb: Application = ApplicationBuilder().token(BOT_TOKEN).defaults(defaults).build()

# ─────────── formatting helpers ─────────────────────────────────────────
def fmt_waiting(t: dict) -> str:
    return (
        f"🆕  Ticket #{t['id']}\n"
        f"────────────────────\n"
        f"SIDE    :  {t['side'].upper()}\n"
        f"QTY     :  {t['qty']} {t['symbol']}\n"
        f"────────────────────\n"
        f"STATUS  :  Waiting for price…\n\n"
        f"🔄 Refresh to update."
    )

def fmt_quoted(t: dict, remain: int) -> str:
    return (
        f"🆕  Ticket #{t['id']}\n"
        f"────────────────────\n"
        f"SIDE    :  {t['side'].upper()}\n"
        f"QTY     :  {t['qty']} {t['symbol']}\n"
        f"────────────────────\n"
        f"Price   :  {t['price']} (valid {remain}s)\n\n"
        f"Press ✅ Accept to trade.\n\n"
        f"🔄 Refresh to update."
    )

def fmt_traded(t: dict) -> str:
    proceeds = t['price'] * t['qty']
    return (
        "✅ TRADE DONE\n"
        f"Ticket  : #{t['id']}\n"
        f"Date    : {datetime.utcnow():%Y-%m-%d}\n"
        f"Buyer   : GSR\n"
        f"Seller  : CLIENT\n"
        f"Cross   : {t['symbol']}/USD   (Spot)\n"
        f"Side    : {t['side'].upper()}\n"
        f"Price   : {t['price']}\n"
        f"Qty     : {t['qty']}\n"
        f"Proceeds: {proceeds:,.2f}\n"
        f"Settle  : T+0\n"
        "—\nThanks for the trade!\nGSR OTC Trading Team"
    )

# ─────────── inline keyboards ───────────────────────────────────────────
def kb_open(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup.from_button(
        InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{tid}")
    )

def kb_quoted(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Accept",   callback_data=f"accept:{tid}")],
        [InlineKeyboardButton("🔄 Refresh",  callback_data=f"refresh:{tid}")]
    ])

def kb_market(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("QUOTE",            callback_data=f"autoquote:{tid}:quote")],
        [InlineKeyboardButton("Bid +10 bps",      callback_data=f"autoquote:{tid}:bid10"),
         InlineKeyboardButton("Ask +10 bps",      callback_data=f"autoquote:{tid}:ask10")],
        [InlineKeyboardButton("Bid +20 bps",      callback_data=f"autoquote:{tid}:bid20"),
         InlineKeyboardButton("Ask +20 bps",      callback_data=f"autoquote:{tid}:ask20")],
    ])

# ─────────── misc helpers ───────────────────────────────────────────────
def detach(t: Ticket) -> dict:
    """Return plain-dict copy (so we can use it after session closes)."""
    return {
        "id": t.id, "side": t.side, "symbol": t.symbol, "qty": t.qty,
        "client_chat_id": t.client_chat_id, "client_msg_id": t.client_msg_id,
        "trader_msg_id": t.trader_msg_id, "price": t.price,
        "valid_until": t.valid_until,
    }

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

async def market_snapshot(symbol: str, qty: float) -> dict:
    """mid / bid / ask averages + bps."""
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol.upper()}USDT&limit=1000"
    async with httpx.AsyncClient(timeout=4.0) as c:
        r = await c.get(url)
    r.raise_for_status()
    data = r.json()
    bids = [[float(p), float(q)] for p, q in data["bids"]]
    asks = [[float(p), float(q)] for p, q in data["asks"]]

    bid_avg = _clearing(bids, qty)["avg_price"]
    ask_avg = _clearing(asks, qty)["avg_price"]
    mid     = (bid_avg + ask_avg) / 2
    bid_bps = round((bid_avg / mid - 1) * 10_000)
    ask_bps = round((ask_avg / mid - 1) * 10_000)
    return dict(mid=mid, bid=bid_avg, ask=ask_avg,
                bid_bps=bid_bps, ask_bps=ask_bps)

# ─────────── expiry task ────────────────────────────────────────────────
async def expire_worker(tid: int, delay: int) -> None:
    await asyncio.sleep(delay)
    with db() as s:
        t = s.get(Ticket, tid)
        if not t or t.status != "quoted":
            return
        if t.valid_until and t.valid_until > datetime.utcnow():
            return

        t.status = "open"
        t.price = None
        t.valid_until = None
        t.updated_at = datetime.utcnow()
        s.commit()
        t_det = detach(t)

    # notify chats
    await ptb.bot.edit_message_text(
        chat_id=t_det["client_chat_id"],
        message_id=t_det["client_msg_id"],
        text=fmt_waiting(t_det),
        reply_markup=kb_open(t_det["id"]),
    )
    # ping traders
    await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=(f"⏳ RFQ #{tid} expired without acceptance.\n"
              f"Please re-price:\n<code>/quote {tid} price secs</code>"),
    )
    log_event(tid, "expired", {})

# ─────────── shared quote helper ─────────────────────────────────────────
async def quote_and_publish(
    tid: int, price: float, secs: int, actor: str
) -> None:
    with db() as s:
        t = s.get(Ticket, tid)
        if not t or t.status == "traded":
            raise ValueError("ticket not open")
        t.price = price
        t.status = "quoted"
        t.valid_until = datetime.utcnow() + timedelta(seconds=secs)
        t.updated_at = datetime.utcnow()
        s.commit()
        s.refresh(t)
        t_det = detach(t)

    # update chats
    await ptb.bot.edit_message_text(
        chat_id=ADMIN_CHAT_ID,
        message_id=t_det["trader_msg_id"],
        text=(f"✅ RFQ #{tid} priced {price} "
              f"(valid {secs}s) by {actor}"),
    )
    await ptb.bot.edit_message_text(
        chat_id=t_det["client_chat_id"],
        message_id=t_det["client_msg_id"],
        text=fmt_quoted(t_det, secs),
        reply_markup=kb_quoted(tid),
    )
    log_event(tid, "quoted", {"actor": actor, "price": price, "secs": secs})
    asyncio.create_task(expire_worker(tid, secs))

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
        t = Ticket(side=side, symbol=symbol, qty=qty,
                   client_chat_id=update.effective_chat.id)
        s.add(t)
        s.commit()
        s.refresh(t)
        t_det = detach(t)

    # client placeholder
    client_msg = await update.message.reply_text(
        fmt_waiting(t_det),
        reply_markup=kb_open(t_det["id"]),
    )
    # trader market snapshot
    try:
        snap = await market_snapshot(symbol, qty)
        trader_text = (
            f"📥 <b>RFQ #{t_det['id']}</b>\n"
            f"CLIENT wants {side.upper()} {qty} {symbol}\n\n"
            f"MID : {snap['mid']}\n"
            f"BID : {snap['bid']} ({snap['bid_bps']:+} bps)\n"
            f"ASK : {snap['ask']} ({snap['ask_bps']:+} bps)"
        )
    except Exception as e:
        logging.warning("snapshot failed: %s", e)
        trader_text = (
            f"📥 <b>RFQ #{t_det['id']}</b>\n"
            f"CLIENT wants {side.upper()} {qty} {symbol}\n\n"
            "⚠️  Market snapshot unavailable.\n"
            "<code>/quote {id} price secs</code>"
        )

    trader_msg = await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=trader_text,
        reply_markup=kb_market(t_det["id"]),
    )

    with db() as s:
        t = s.get(Ticket, t_det["id"])
        t.client_msg_id = client_msg.message_id
        t.trader_msg_id = trader_msg.message_id
        s.commit()

    log_event(t_det["id"], "rfq_created", {})
    asyncio.create_task(expire_worker(t_det["id"], RFQ_TTL))

# ─────────── /quote (manual) ─────────────────────────────────────────────
async def cmd_quote(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_CHAT_ID:
        await update.message.reply_text("Only the trader group can quote.")
        return
    try:
        tid = int(ctx.args[0]); price = float(ctx.args[1]); secs = int(ctx.args[2])
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /quote <id> <price> <secs>")
        return
    if secs > MAX_VALIDITY:
        await update.message.reply_text(f"Max validity is {MAX_VALIDITY}s.")
        return
    try:
        await quote_and_publish(tid, price, secs, actor=f"@{update.effective_user.username}")
    except ValueError as err:
        await update.message.reply_text(str(err))

# ─────────── auto-quote callbacks ────────────────────────────────────────
async def cb_autoquote(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    tid, action = q.data.split(":")[1:3]

    with db() as s:
        t = s.get(Ticket, int(tid))
        if not t or t.status != "open":
            await q.edit_message_text("❌ Ticket no longer open.")
            return
        t_det = detach(t)

    try:
        snap = await market_snapshot(t_det["symbol"], t_det["qty"])
    except Exception:
        await q.edit_message_text("⚠️ Couldn’t fetch market depth.")
        return

    base = snap["ask"] if t_det["side"] == "buy" else snap["bid"]
    bump_bps = 0
    if action.endswith("10"): bump_bps = 10
    if action.endswith("20"): bump_bps = 20
    price = round(base * (1 + bump_bps / 10_000), 6)

    await quote_and_publish(int(tid), price, 90, actor="AutoQuote")

# ─────────── Refresh & Accept callbacks ──────────────────────────────────
async def cb_refresh(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    tid = int(q.data.split(":")[1])

    with db() as s:
        t = s.get(Ticket, tid)
        if not t:
            await q.edit_message_text("❌ Ticket not found."); return
        t_det = detach(t)

    if t_det["status"] == "quoted" and t_det["valid_until"] > datetime.utcnow():
        remain = int((t_det["valid_until"] - datetime.utcnow()).total_seconds())
        await q.edit_message_text(fmt_quoted(t_det, remain),
                                  reply_markup=kb_quoted(tid))
    elif t_det["status"] == "traded":
        await q.edit_message_text(fmt_traded(t_det))
    else:
        await q.edit_message_text(fmt_waiting(t_det), reply_markup=kb_open(tid))

async def cb_accept(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    tid = int(q.data.split(":")[1])

    with db() as s:
        t = s.get(Ticket, tid)
        if not t or t.status != "quoted":
            await q.edit_message_text("❌ Quote no longer valid."); return
        if t.valid_until < datetime.utcnow():
            await q.edit_message_text("❌ Quote expired."); return
        t.status = "traded"; t.updated_at = datetime.utcnow(); s.commit()
        t_det = detach(t)

    await q.edit_message_text(fmt_traded(t_det))
    await ptb.bot.send_message(chat_id=ADMIN_CHAT_ID, text=fmt_traded(t_det))
    log_event(tid, "traded", {"user": q.from_user.username})

# ─────────── register handlers ───────────────────────────────────────────
ptb.add_handler(CommandHandler("rfq",   cmd_rfq))
ptb.add_handler(CommandHandler("quote", cmd_quote))
ptb.add_handler(CallbackQueryHandler(cb_autoquote,
        pattern=r"^autoquote:\d+:(?:quote|bid10|ask10|bid20|ask20)$"))
ptb.add_handler(CallbackQueryHandler(cb_refresh, pattern=r"^refresh:\d+$"))
ptb.add_handler(CallbackQueryHandler(cb_accept,  pattern=r"^accept:\d+$"))

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

# ───────────────────── Health & price utilities (unchanged) ─────────────
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
    vol = np.std(returns) * np.sqrt(365)
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
        "symbol": symbol.upper(), "venue": "spot", "quantity": quantity,
        "bid": _clearing(bids, quantity), "ask": _clearing(asks, quantity),
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
        "symbol": symbol.upper(), "venue": "perp", "quantity": quantity,
        "bid": _clearing(bids, quantity), "ask": _clearing(asks, quantity),
    }
