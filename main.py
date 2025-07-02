"""
binance-fastapi + RFQ Telegram bot (persistent)

ENV VARS (Render → Settings → Environment)
------------------------------------------
TELEGRAM_TOKEN      BotFather token
WEBHOOK_SECRET      random string (same in setWebhook & header check)
ADMIN_CHAT_ID       numeric ID of the traders group
ALLOWED_SYMBOLS     comma list, e.g. "BTC,ETH,SOL"
RFQ_TTL             seconds before an un-quoted RFQ expires   (default 120)
MAX_VALIDITY        max seconds a quote can be valid          (default 120)
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
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from sqlmodel import SQLModel, Field, Session, create_engine

# ─────────────────── CONFIG ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)

BOT_TOKEN       = os.environ["TELEGRAM_TOKEN"]
WEBHOOK_SECRET  = os.environ["WEBHOOK_SECRET"]
ADMIN_CHAT_ID   = int(os.environ["ADMIN_CHAT_ID"])
ALLOWED_SYMBOLS = {
    s.strip().upper() for s in os.environ["ALLOWED_SYMBOLS"].split(",")
}

RFQ_TTL      = int(os.getenv("RFQ_TTL", "120"))
MAX_VALIDITY = int(os.getenv("MAX_VALIDITY", "120"))

# Public URL discovery for setWebhook
PUBLIC_BASE_URL = (
    os.getenv("PUBLIC_BASE_URL")            # optional override
    or os.getenv("RENDER_EXTERNAL_URL")     # Render always sets this
)

if not PUBLIC_BASE_URL:
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME")
    if host:
        PUBLIC_BASE_URL = f"https://{host}"
    else:
        raise RuntimeError(
            "Cannot determine public URL. "
            "Set PUBLIC_BASE_URL in the environment."
        )

PUBLIC_BASE_URL = PUBLIC_BASE_URL.rstrip("/")

DB_PATH = os.getenv("DB_PATH", "/db/rfq.sqlite")

# ─────────────────── DATABASE (SQLite) ───────────────────────────────────
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    echo=False,
    connect_args={"check_same_thread": False},  # async-friendly
)


class Ticket(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    side: str
    symbol: str
    qty: float
    client_chat_id: int
    client_msg_id: Optional[int] = None
    trader_msg_id: Optional[int] = None
    status: str = "open"          # open | quoted | expired | cancelled
    price: Optional[float] = None
    valid_until: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class TicketEvent(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ticket_id: int = Field(index=True)
    event: str                    # rfq_created | quoted | expired | cancelled
    payload: Optional[str] = None # JSON string
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


# ─────────────────── TELEGRAM BOT ────────────────────────────────────────
ptb: Application = (
    ApplicationBuilder()
    .token(BOT_TOKEN)
    .parse_mode(ParseMode.HTML)   # ← enum instead of "HTML"
    .build()
)


async def expire_worker(ticket_id: int, delay: int) -> None:
    await asyncio.sleep(delay)
    with db() as s:
        t: Ticket | None = s.get(Ticket, ticket_id)
        if not t or t.status != "quoted":
            return
        if t.valid_until and t.valid_until > datetime.utcnow():
            return
        t.status = "expired"
        t.updated_at = datetime.utcnow()
        s.add(t)
        s.commit()
    # notify chats
    if t.trader_msg_id:
        await ptb.bot.edit_message_text(
            chat_id=ADMIN_CHAT_ID,
            message_id=t.trader_msg_id,
            text=f"❌ RFQ #{ticket_id} expired.",
        )
    if t.client_msg_id:
        await ptb.bot.edit_message_text(
            chat_id=t.client_chat_id,
            message_id=t.client_msg_id,
            text=f"❌ Quote for ticket #{ticket_id} expired.",
        )
    log_event(ticket_id, "expired", {})


# ─────────── /rfq (client) ───────────
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

    log_event(
        t.id,
        "rfq_created",
        {"group": update.effective_chat.title or update.effective_chat.id},
    )

    client_msg = await update.message.reply_text(
        f"🆕 Ticket <b>#{t.id}</b> | {side.upper()} {qty} {symbol}\n"
        f"⏳ Waiting for price…"
    )
    trader_msg = await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=(
            f"📥 <b>RFQ #{t.id}</b>\n"
            f"{update.effective_chat.title or update.effective_chat.id} wants "
            f"{side.upper()} {qty} {symbol}\n\n"
            f"Respond with:\n/quote {t.id} <price> <secs>"
        ),
    )

    with db() as s:
        t.client_msg_id = client_msg.message_id
        t.trader_msg_id = trader_msg.message_id
        s.add(t)
        s.commit()

    asyncio.create_task(expire_worker(t.id, RFQ_TTL))


# ─────────── /quote (trader) ───────────
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
        if t.status != "open":
            await update.message.reply_text(f"Ticket is already {t.status}.")
            return
        t.price = price
        t.status = "quoted"
        t.valid_until = datetime.utcnow() + timedelta(seconds=secs)
        t.updated_at = datetime.utcnow()
        s.add(t)
        s.commit()

    log_event(
        ticket_id,
        "quoted",
        {
            "trader": update.effective_user.username,
            "price": price,
            "secs": secs,
        },
    )

    await ptb.bot.edit_message_text(
        chat_id=ADMIN_CHAT_ID,
        message_id=t.trader_msg_id,
        text=f"✅ RFQ #{ticket_id} priced {price} (valid {secs}s) by @{update.effective_user.username}",
    )
    await ptb.bot.edit_message_text(
        chat_id=t.client_chat_id,
        message_id=t.client_msg_id,
        text=(
            f"💰 Ticket <b>#{ticket_id}</b> | {t.side.upper()} {t.qty} {t.symbol}\n"
            f"Price: <b>{price}</b> (valid {secs}s)\n"
            f"🔄 Refresh to update."
        ),
    )

    asyncio.create_task(expire_worker(ticket_id, secs))


ptb.add_handler(CommandHandler("rfq", cmd_rfq))
ptb.add_handler(CommandHandler("quote", cmd_quote))

# ─────────────────── FASTAPI APP / WEBHOOK ───────────────────────────────
app = FastAPI()


@app.on_event("startup")
async def _startup() -> None:
    init_db()
    url = f"{PUBLIC_BASE_URL}/telegram"
    await ptb.bot.set_webhook(
        url, secret_token=WEBHOOK_SECRET, drop_pending_updates=True
    )
    logging.info("Webhook set to %s", url)
    asyncio.create_task(ptb.start())


@app.on_event("shutdown")
async def _shutdown() -> None:
    await ptb.stop()


@app.post("/telegram")
async def telegram_webhook(req: Request):
    if (
        req.headers.get("X-Telegram-Bot-Api-Secret-Token")
        != WEBHOOK_SECRET
    ):
        raise HTTPException(status_code=401, detail="bad secret")
    update = Update.de_json(await req.json(), ptb.bot)
    await ptb.process_update(update)
    return Response(status_code=HTTPStatus.OK)


# ─────────────────── EXISTING BINANCE ENDPOINTS ─────────────────────────
@app.get("/")
def health():
    return {"status": "alive"}


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
    url = (
        f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}"
        f"&interval=1d&limit=31"
    )
    async with httpx.AsyncClient() as c:
        r = await c.get(url)
    if r.status_code != 200:
        raise HTTPException(502, "Failed to fetch price history")
    data = r.json()
    closes = [float(item[4]) for item in data]
    if len(closes) < 31:
        raise HTTPException(400, "Insufficient data")
    returns = np.diff(np.log(closes))
    vol = np.std(returns) * np.sqrt(365)
    return {"symbol": symbol.upper(), "realized_vol_%": round(vol * 100, 2)}


def clearing(order_book: list[list[float]], qty: float):
    filled, cost = 0.0, 0.0
    for price, size in order_book:
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
    url = (
        f"https://api.binance.com/api/v3/depth?"
        f"symbol={symbol.upper()}&limit=1000"
    )
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
        "bid": clearing(bids, quantity),
        "ask": clearing(asks, quantity),
    }


@app.get("/clearing/perp/{symbol}")
async def clearing_perp(symbol: str, quantity: float):
    url = (
        f"https://fapi.binance.com/fapi/v1/depth?"
        f"symbol={symbol.upper()}&limit=1000"
    )
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
        "bid": clearing(bids, quantity),
        "ask": clearing(asks, quantity),
    }
