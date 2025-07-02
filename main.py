"""
binance-fastapi + RFQ Telegram bot
---------------------------------
* FastAPI endpoints (health, price, funding, RV, clearing) – unchanged.
* Telegram webhook (/rfq, /quote) – uses python-telegram-bot v22.
* SQLite on the Render Disk (/data) via SQLModel for persistence.

ENV VARS REQUIRED
-----------------
TELEGRAM_TOKEN   – BotFather token
WEBHOOK_SECRET   – header secret for setWebhook
ADMIN_CHAT_ID    – numeric chat-id of the traders group
ALLOWED_SYMBOLS  – comma list, e.g. "BTC,ETH,SOL"
RFQ_TTL          – seconds before an un-quoted RFQ expires   (default 120)
MAX_VALIDITY     – max seconds a quote can be valid          (default 120)
DB_PATH          – optional, defaults to "/data/rfq.sqlite"
"""

import asyncio, json, logging, os
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Optional

import httpx
import numpy as np
from fastapi import FastAPI, HTTPException, Request, Response
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from sqlmodel import SQLModel, Field, Session, create_engine, select

# ---------------------------------------------------------------------#
# 0.  CONFIG / CONSTANTS                                               #
# ---------------------------------------------------------------------#
logging.basicConfig(level=logging.INFO)

BOT_TOKEN      = os.environ["TELEGRAM_TOKEN"]
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
ADMIN_CHAT_ID  = int(os.environ["ADMIN_CHAT_ID"])
ALLOWED_SYMBOLS = {s.strip().upper() for s in os.environ["ALLOWED_SYMBOLS"].split(",")}

RFQ_TTL      = int(os.getenv("RFQ_TTL", "120"))
MAX_VALIDITY = int(os.getenv("MAX_VALIDITY", "120"))

HOST  = os.getenv("RENDER_EXTERNAL_HOSTNAME", "localhost:10000")
DB_PATH = os.getenv("DB_PATH", "/data/rfq.sqlite")

# ---------------------------------------------------------------------#
# 1.  DATABASE (SQLITE on /data)                                       #
# ---------------------------------------------------------------------#
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    echo=False,
    connect_args={"check_same_thread": False},  # allows threads / asyncio
)


class Ticket(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    side: str
    symbol: str
    qty: float
    client_chat_id: int
    client_msg_id: Optional[int] = None
    trader_msg_id: Optional[int] = None
    status: str = "open"  # open | quoted | expired | cancelled
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


def db_session() -> Session:
    # simple helper so we don’t repeat Session(engine) everywhere
    return Session(engine)


def log_event(ticket_id: int, event: str, payload: dict | None = None) -> None:
    with db_session() as s:
        s.add(TicketEvent(ticket_id=ticket_id,
                          event=event,
                          payload=json.dumps(payload) if payload else None))
        s.commit()


# ---------------------------------------------------------------------#
# 2.  TELEGRAM BOT SET-UP                                              #
# ---------------------------------------------------------------------#
ptb: Application = (
    ApplicationBuilder()
    .token(BOT_TOKEN)
    .parse_mode("HTML")
    .build()
)


async def expire_ticket_worker(ticket_id: int, delay: int) -> None:
    """Marks a ticket expired after *delay* seconds (if still valid)."""
    await asyncio.sleep(delay)
    with db_session() as s:
        t: Ticket | None = s.get(Ticket, ticket_id)
        if not t or t.status != "quoted":
            return
        if t.valid_until and t.valid_until > datetime.utcnow():
            return  # still valid
        t.status = "expired"
        t.updated_at = datetime.utcnow()
        s.add(t); s.commit()

        # notify chats
        if t.trader_msg_id:
            await ptb.bot.edit_message_text(
                chat_id=ADMIN_CHAT_ID,
                message_id=t.trader_msg_id,
                text=f"❌ RFQ #{ticket_id} expired."
            )
        if t.client_msg_id:
            await ptb.bot.edit_message_text(
                chat_id=t.client_chat_id,
                message_id=t.client_msg_id,
                text=f"❌ Quote for ticket #{ticket_id} expired."
            )
        log_event(ticket_id, "expired", {})


# ------------------------------------------------------------------#
# 2a.  /rfq  (client groups)                                        #
# ------------------------------------------------------------------#
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

    # create ticket in DB
    with db_session() as s:
        t = Ticket(side=side, symbol=symbol, qty=qty,
                   client_chat_id=update.effective_chat.id)
        s.add(t); s.commit(); s.refresh(t)
        ticket_id = t.id

    log_event(ticket_id, "rfq_created",
              {"group": update.effective_chat.title or update.effective_chat.id})

    # send messages
    client_msg = await update.message.reply_text(
        f"🆕 Ticket <b>#{ticket_id}</b> | {side.upper()} {qty} {symbol}\n"
        f"⏳ Waiting for price…"
    )
    trader_msg = await ptb.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=(
            f"📥 <b>RFQ #{ticket_id}</b>\n"
            f"{update.effective_chat.title or update.effective_chat.id} wants "
            f"{side.upper()} {qty} {symbol}\n\n"
            f"Respond with:\n/quote {ticket_id} <price> <secs>"
        )
    )

    # store message-ids
    with db_session() as s:
        t = s.get(Ticket, ticket_id)
        t.client_msg_id = client_msg.message_id
        t.trader_msg_id = trader_msg.message_id
        s.add(t); s.commit()

    # schedule auto-expiry if nobody quotes
    asyncio.create_task(expire_ticket_worker(ticket_id, RFQ_TTL))


# ------------------------------------------------------------------#
# 2b.  /quote  (trader group)                                       #
# ------------------------------------------------------------------#
async def cmd_quote(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_CHAT_ID:
        await update.message.reply_text("Only the trader group can quote.")
        return

    # parse
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

    # fetch & update
    with db_session() as s:
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
        s.add(t); s.commit()

    log_event(ticket_id, "quoted",
              {"trader": update.effective_user.username, "price": price, "secs": secs})

    # edit both chats
    await ptb.bot.edit_message_text(
        chat_id=ADMIN_CHAT_ID,
        message_id=t.trader_msg_id,
        text=f"✅ RFQ #{ticket_id} priced {price} (valid {secs}s) by @{update.effective_user.username}"
    )
    await ptb.bot.edit_message_text(
        chat_id=t.client_chat_id,
        message_id=t.client_msg_id,
        text=(
            f"💰 Ticket <b>#{ticket_id}</b> | {t.side.upper()} {t.qty} {t.symbol}\n"
            f"Price: <b>{price}</b> (valid {secs}s)\n"
            f"🔄 Refresh to update."
        )
    )

    # schedule expiry
    asyncio.create_task(expire_ticket_worker(ticket_id, secs))


# Register handlers
ptb.add_handler(CommandHandler("rfq", cmd_rfq))
ptb.add_handler(CommandHandler("quote", cmd_quote))

# ---------------------------------------------------------------------#
# 3.  FASTAPI + WEBHOOK ENDPOINT                                       #
# ---------------------------------------------------------------------#
app = FastAPI()


@app.on_event("startup")
async def _startup() -> None:
    init_db()
    url = f"https://{HOST}/telegram"
    await ptb.bot.set_webhook(url, secret_token=WEBHOOK_SECRET,
                              drop_pending_updates=True)
    logging.info("Webhook set to %s", url)
    asyncio.create_task(ptb.start())


@app.on_event("shutdown")
async def _shutdown() -> None:
    await ptb.stop()


@app.post("/telegram")
async def telegram_webhook(req: Request):
    if req.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="bad secret")
    update = Update.de_json(await req.json(), ptb.bot)
    await ptb.process_update(update)
    return Response(status_code=HTTPStatus.OK)


# ---------------------------------------------------------------------#
# 4.  EXISTING BINANCE ENDPOINTS (unchanged from your repo)            #
# ---------------------------------------------------------------------#
@app.get("/")
def health():
    return {"status": "alive"}


@app.get("/price/{symbol}")
async def get_price(symbol: str):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol.upper()}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            return {"error": "Failed to fetch price", "code": resp.status_code}
        data = resp.json()
        return {"symbol": data["symbol"], "price": float(data["price"])}


@app.get("/funding/{symbol}")
async def get_funding(symbol: str):
    url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol.upper()}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            return {"error": "Failed to fetch funding", "code": resp.status_code}
        data = resp.json()
        return {
            "symbol": data["symbol"],
            "markPrice": float(data["markPrice"]),
            "lastFundingRate": float(data["lastFundingRate"]),
            "nextFundingTime": int(data["nextFundingTime"]),
        }


@app.get("/rv/{symbol}")
async def get_realized_vol(symbol: str):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}&interval=1d&limit=31"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            return {"error": "Failed to fetch historical prices", "code": resp.status_code}
        data = resp.json()

    closes = [float(entry[4]) for entry in data]  # entry[4] = close price
    if len(closes) < 31:
        return {"error": "Insufficient data"}

    returns = np.diff(np.log(closes))
    vol = np.std(returns) * np.sqrt(365)
    return {"symbol": symbol.upper(), "realized_vol": round(vol * 100, 2)}


def compute_clearing_price(order_book: list[list[float]], quantity: float) -> dict:
    filled, total_cost = 0.0, 0.0
    for price, size in order_book:
        take = min(quantity - filled, size)
        total_cost += take * price
        filled += take
        if filled >= quantity:
            break
    if filled == 0:
        return {"error": "No liquidity", "filled": 0}
    avg_price = total_cost / filled
    if filled < quantity:
        return {
            "error": "Insufficient liquidity",
            "filled": filled,
            "requested": quantity,
            "avg_price": round(avg_price, 6),
            "total_cost": round(total_cost, 2),
        }
    return {"filled": filled, "avg_price": round(avg_price, 6), "total_cost": round(total_cost, 2)}


@app.get("/clearing/spot/{symbol}")
async def clearing_spot(symbol: str, quantity: float):
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}&limit=1000"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            return {"error": "Failed to fetch spot order book"}
        data = resp.json()

    bids = [[float(p), float(q)] for p, q in data["bids"]]
    asks = [[float(p), float(q)] for p, q in data["asks"]]
    return {
        "symbol": symbol.upper(),
        "venue": "spot",
        "quantity": quantity,
        "bid": compute_clearing_price(bids, quantity),
        "ask": compute_clearing_price(asks, quantity),
    }


@app.get("/clearing/perp/{symbol}")
async def clearing_perp(symbol: str, quantity: float):
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol.upper()}&limit=1000"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            return {"error": "Failed to fetch perp order book"}
        data = resp.json()

    bids = [[float(p), float(q)] for p, q in data["bids"]]
    asks = [[float(p), float(q)] for p, q in data["asks"]]
    return {
        "symbol": symbol.upper(),
        "venue": "perp",
        "quantity": quantity,
        "bid": compute_clearing_price(bids, quantity),
        "ask": compute_clearing_price(asks, quantity),
    }
