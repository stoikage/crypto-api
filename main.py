"""
binance-fastapi + RFQ Telegram bot
(PTB 22 • SQLite)

⬇ ENV VARS … (unchanged, trimmed for brevity)
"""
import asyncio, json, logging, math, os
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

# ───── CONFIG ───────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

BOT_TOKEN      = os.environ["TELEGRAM_TOKEN"]
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
ADMIN_CHAT_ID  = int(os.environ["ADMIN_CHAT_ID"])
ALLOWED_SYMBOLS = {s.strip().upper() for s in os.environ["ALLOWED_SYMBOLS"].split(",")}

RFQ_TTL      = int(os.getenv("RFQ_TTL", "120"))
MAX_VALIDITY = int(os.getenv("MAX_VALIDITY", "120"))

PUBLIC_BASE_URL = (
    os.getenv("PUBLIC_BASE_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or (f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}"
        if os.getenv("RENDER_EXTERNAL_HOSTNAME") else None)
).rstrip("/")

DB_PATH = os.getenv("DB_PATH", "/db/rfq.sqlite")

# ───── DB ───────────────────────────────────────────────────────────────
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})

class Ticket(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    side: str
    symbol: str
    qty: float
    client_chat_id: int
    client_msg_id: Optional[int] = None
    trader_msg_id: Optional[int] = None
    status: str = "open"     # open | quoted | traded
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

def log_event(tid: int, event: str, payload: dict | None = None):
    with db() as s:
        s.add(TicketEvent(ticket_id=tid, event=event,
                          payload=json.dumps(payload) if payload else None))
        s.commit()

# ───── BOT ──────────────────────────────────────────────────────────────
ptb = (
    ApplicationBuilder()
    .token(BOT_TOKEN)
    .defaults(Defaults(parse_mode=ParseMode.HTML))
    .build()
)

# ───── helpers / formatting ────────────────────────────────────────────
def _client_name(chat_id: int) -> str:
    # Avoid an async call — just show the numeric id if we can’t cache names easily
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
    buyer, seller = ("GSR", client) if t.side == "sell" else (client, "GSR")
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

# ───── keyboards ────────────────────────────────────────────────────────
def kb_open(tid: int):
    return InlineKeyboardMarkup.from_button(
        InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{tid}")
    )

def kb_quote_actions(tid: int):
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("💰 QUOTE",   callback_data=f"autoquote:quote:{tid}")],
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

# ───── Binance snapshot (unchanged) ─────────────────────────────────────
def _clearing(book, qty):
    filled = cost = 0.0
    for p, sz in book:
        take = min(qty - filled, sz)
        filled += take
        cost   += take * p
        if filled >= qty:
            break
    return {"avg_price": round(cost / filled, 6)} if filled else {"error": "no liq"}

async def get_snapshot(symbol: str, qty: float):
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol.upper()}USDT&limit=1000"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    r.raise_for_status()
    data = r.json()
    bids = [[float(p), float(q)] for p, q in data["bids"]]
    asks = [[float(p), float(q)] for p, q in data["asks"]]
    mid       = round((bids[0][0] + asks[0][0]) / 2, 6)
    bid_avg   = _clearing(bids, qty)["avg_price"]
    ask_avg   = _clearing(asks, qty)["avg_price"]
    return {"mid": mid, "bid": bid_avg, "ask": ask_avg}

# ───── expiry worker ────────────────────────────────────────────────────
async def expire_worker(tid: int, delay: int):
    await asyncio.sleep(delay)
    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not (t and t.status == "quoted" and
                (not t.valid_until or t.valid_until <= datetime.utcnow())):
            return
        t.status = "open"; t.price = None; t.valid_until = None
        t.updated_at = datetime.utcnow(); s.add(t); s.commit()
        client_msg_id = t.client_msg_id; client_chat_id = t.client_chat_id
    if client_msg_id:
        await ptb.bot.edit_message_text(
            chat_id=client_chat_id, message_id=client_msg_id,
            text=fmt_waiting(t), reply_markup=kb_open(tid)
        )
    await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=(f"⏳ RFQ #{tid} expired without acceptance.\n"
              f"Please re-price: <code>/quote {tid} price secs</code>")
    )
    log_event(tid, "expired", {})

# ───── core quote-apply helper (shared by /quote & auto-buttons) ────────
async def apply_quote(tid: int, price: float, secs: int, trader_username: str):
    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not t or t.status == "traded":
            return False
        t.price = price
        t.status = "quoted"
        t.valid_until = datetime.utcnow() + timedelta(seconds=secs)
        t.updated_at  = datetime.utcnow()
        s.add(t); s.commit()
        # copy for out-of-session use
        client_chat_id, client_msg_id, trader_msg_id = \
            t.client_chat_id, t.client_msg_id, t.trader_msg_id
    # notify chats
    await ptb.bot.edit_message_text(
        chat_id=ADMIN_CHAT_ID, message_id=trader_msg_id,
        text=(f"✅ RFQ #{tid} priced {price} "
              f"(valid {secs}s) by @{trader_username}")
    )
    await ptb.bot.edit_message_text(
        chat_id=client_chat_id, message_id=client_msg_id,
        text=fmt_quoted(t, secs), reply_markup=kb_quote_actions(tid)
    )
    log_event(tid, "quoted",
              {"trader": trader_username, "price": price, "secs": secs})
    asyncio.create_task(expire_worker(tid, secs))
    return True

# ───── /rfq (client) ────────────────────────────────────────────────────
async def cmd_rfq(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_chat.id == ADMIN_CHAT_ID:
        await upd.message.reply_text("Traders can’t issue RFQs here."); return
    try:
        side, qty, symbol = ctx.args[0].lower(), float(ctx.args[1]), ctx.args[2].upper()
    except (IndexError, ValueError):
        await upd.message.reply_text("Usage: /rfq buy 10 SOL"); return
    if side not in {"buy", "sell"} or symbol not in ALLOWED_SYMBOLS:
        await upd.message.reply_text("Unsupported side or symbol."); return

    with db() as s:
        t = Ticket(side=side, symbol=symbol, qty=qty,
                   client_chat_id=upd.effective_chat.id)
        s.add(t); s.commit(); s.refresh(t)
        tid = t.id   # cache before leaving the session

    client_msg = await upd.message.reply_text(fmt_waiting(t), reply_markup=kb_open(tid))

    # grab snapshot for traders
    try:
        snap = await get_snapshot(symbol, qty)
        bid_bps = round((snap["bid"]/snap["mid"] - 1) * 10_000, 1)
        ask_bps = round((snap["ask"]/snap["mid"] - 1) * 10_000, 1)
        trader_txt = (
            f"📥 <b>RFQ #{tid}</b>\n"
            f"{_client_name(upd.effective_chat.id)} wants {side.upper()} {qty} {symbol}\n\n"
            f"MID: {snap['mid']}\n"
            f"BID: {snap['bid']} ({bid_bps:+} bps)\n"
            f"ASK: {snap['ask']} ({ask_bps:+} bps)"
        )
    except Exception as e:
        logging.warning("snapshot error: %s", e)
        trader_txt = (
            f"📥 <b>RFQ #{tid}</b>\n"
            f"{_client_name(upd.effective_chat.id)} wants {side.upper()} {qty} {symbol}\n\n"
            f"<code>/quote {tid} price secs</code>"
        )

    trader_msg = await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID, text=trader_txt, reply_markup=kb_quote_actions(tid)
    )

    # persist message IDs
    with db() as s:
        t = s.get(Ticket, tid)
        t.client_msg_id = client_msg.message_id
        t.trader_msg_id = trader_msg.message_id
        s.add(t); s.commit()

    log_event(tid, "rfq_created", {})
    asyncio.create_task(expire_worker(tid, RFQ_TTL))

# ───── /quote (manual) ──────────────────────────────────────────────────
async def cmd_quote(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_chat.id != ADMIN_CHAT_ID:
        await upd.message.reply_text("Only the trader group can quote."); return
    try:
        tid, price, secs = int(ctx.args[0]), float(ctx.args[1]), int(ctx.args[2])
    except (IndexError, ValueError):
        await upd.message.reply_text("Usage: /quote <id> <price> <secs>"); return
    if secs > MAX_VALIDITY:
        await upd.message.reply_text(f"Max validity is {MAX_VALIDITY}s."); return

    ok = await apply_quote(tid, price, secs, upd.effective_user.username)
    if not ok:
        await upd.message.reply_text("Ticket not found or already traded.")

# ───── auto-quote callback ──────────────────────────────────────────────
async def cb_autoquote(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await q.answer()
    act, tid = q.data.split(":")[1:]
    tid = int(tid)

    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not t or t.status == "traded":
            await q.edit_message_text("❌ Ticket not found or traded."); return
        side, qty, sym = t.side, t.qty, t.symbol

    try:
        snap = await get_snapshot(sym, qty)
    except Exception as e:
        await q.edit_message_text(f"Snapshot error: {e}"); return

    base = snap["ask"] if side == "buy" else snap["bid"]
    bump = {"bid10": 10, "bid20": 20, "ask10": 10, "ask20": 20}.get(act, 0)

    price = base * (1 - bump/10_000) if act.startswith("bid") else \
            base * (1 + bump/10_000)
    price = round(price, 6)
    secs  = 90

    await apply_quote(tid, price, secs, q.from_user.username)
    await q.edit_message_text("✅ Auto-quote sent.")

# ───── refresh / accept callbacks ───────────────────────────────────────
async def cb_refresh(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await q.answer()
    tid = int(q.data.split(":")[1])
    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
    if not t:
        await q.edit_message_text("❌ Ticket not found."); return
    if t.status == "quoted" and t.valid_until > datetime.utcnow():
        remain = (t.valid_until - datetime.utcnow()).total_seconds()
        await q.edit_message_text(fmt_quoted(t, round(remain)),
                                  reply_markup=kb_quote_actions(tid))
    elif t.status == "traded":
        await q.edit_message_text(fmt_traded(t))
    else:
        await q.edit_message_text(fmt_waiting(t), reply_markup=kb_open(tid))

async def cb_accept(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await q.answer()
    tid = int(q.data.split(":")[1])
    with db() as s:
        t: Ticket | None = s.get(Ticket, tid)
        if not (t and t.status == "quoted" and t.valid_until > datetime.utcnow()):
            await q.edit_message_text("❌ Quote no longer valid."); return
        t.status = "traded"; t.updated_at = datetime.utcnow()
        s.add(t); s.commit()
    await q.edit_message_text(fmt_traded(t))
    await ptb.bot.send_message(chat_id=ADMIN_CHAT_ID, text=fmt_traded(t))
    log_event(tid, "traded", {"user": q.from_user.username})

# ───── handlers ─────────────────────────────────────────────────────────
ptb.add_handler(CommandHandler("rfq",   cmd_rfq))
ptb.add_handler(CommandHandler("quote", cmd_quote))
ptb.add_handler(CallbackQueryHandler(cb_refresh,    r"^refresh:\d+$"))
ptb.add_handler(CallbackQueryHandler(cb_accept,     r"^accept:\d+$"))
ptb.add_handler(CallbackQueryHandler(cb_autoquote,
    r"^autoquote:(quote|bid10|bid20|ask10|ask20):\d+$"))

# ───── FastAPI / webhook glue ───────────────────────────────────────────
app = FastAPI()

@app.on_event("startup")
async def _startup():
    init_db()
    url = f"{PUBLIC_BASE_URL}/telegram"
    await ptb.bot.set_webhook(url, secret_token=WEBHOOK_SECRET, drop_pending_updates=True)
    logging.info("Webhook set to %s", url)
    await ptb.initialize(); asyncio.create_task(ptb.start())

@app.on_event("shutdown")
async def _shutdown(): await ptb.stop()

@app.post("/telegram")
async def telegram(req: Request):
    if req.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(401, "bad secret")
    await ptb.process_update(Update.de_json(await req.json(), ptb.bot))
    return Response(status_code=HTTPStatus.OK)

@app.get("/")
def health(): return {"status": "alive"}

# —— price / funding / rv / clearing endpoints (unchanged) ——
# … (same as previous file)

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
