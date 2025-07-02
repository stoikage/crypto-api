"""
binance-fastapi + RFQ Telegram bot (persistent, PTB v22)

ENV VARS …  (unchanged – see earlier file)
"""

import asyncio, json, logging, os
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Optional

import httpx
import numpy as np
from fastapi import FastAPI, HTTPException, Request, Response
from telegram import Update                    #  ← FIX ➜  need Update class
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    Defaults,
)
from sqlmodel import SQLModel, Field, Session, create_engine

# ───── CONFIG (unchanged) ────────────────────────────────────────────────
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

# ───── SQLITE MODELS (unchanged) ─────────────────────────────────────────
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False,
                       connect_args={"check_same_thread": False})
class Ticket(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    side: str; symbol: str; qty: float
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
    event: str
    payload: Optional[str] = None
    ts: datetime = Field(default_factory=datetime.utcnow)

def init_db(): SQLModel.metadata.create_all(engine)
def db() -> Session: return Session(engine)
def log_event(tid:int, evt:str, payload:dict|None=None):
    with db() as s:
        s.add(TicketEvent(ticket_id=tid, event=evt,
                          payload=json.dumps(payload) if payload else None))
        s.commit()

# ───── TELEGRAM BOT ──────────────────────────────────────────────────────
defaults = Defaults(parse_mode=ParseMode.HTML)
ptb: Application = (ApplicationBuilder()
                    .token(BOT_TOKEN)
                    .defaults(defaults)
                    .build())

async def expire_worker(ticket_id:int, delay:int):
    await asyncio.sleep(delay)
    with db() as s:
        t:Ticket|None = s.get(Ticket, ticket_id)
        if not t or t.status!="quoted": return
        if t.valid_until and t.valid_until>datetime.utcnow(): return
        t.status="expired"; t.updated_at=datetime.utcnow()
        s.add(t); s.commit()
    if t.trader_msg_id:
        await ptb.bot.edit_message_text(chat_id=ADMIN_CHAT_ID,
                                        message_id=t.trader_msg_id,
                                        text=f"❌ RFQ #{ticket_id} expired.")
    if t.client_msg_id:
        await ptb.bot.edit_message_text(chat_id=t.client_chat_id,
                                        message_id=t.client_msg_id,
                                        text=f"❌ Quote for ticket #{ticket_id} expired.")
    log_event(ticket_id,"expired",{})

# /rfq and /quote handlers (unchanged from previous full file) …
#   --- cut for brevity, keep identical to prior version ---

ptb.add_handler(CommandHandler("rfq", cmd_rfq))
ptb.add_handler(CommandHandler("quote", cmd_quote))

# ───── FASTAPI APP / WEBHOOK ─────────────────────────────────────────────
app = FastAPI()

@app.on_event("startup")
async def _startup() -> None:
    init_db()
    url = f"{PUBLIC_BASE_URL}/telegram"
    await ptb.bot.set_webhook(url, secret_token=WEBHOOK_SECRET,
                              drop_pending_updates=True)
    logging.info("Webhook set to %s", url)
    await ptb.initialize()                 #  ← FIX ➜  initialize first
    asyncio.create_task(ptb.start())       #  then start processing updates

@app.on_event("shutdown")
async def _shutdown() -> None:
    await ptb.stop()

@app.post("/telegram")
async def telegram_webhook(req: Request):
    if req.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(401, "bad secret")
    update = Update.de_json(await req.json(), ptb.bot)   # ← FIX ➜ make Update
    await ptb.process_update(update)
    return Response(status_code=HTTPStatus.OK)

# ───── Health & Binance helper endpoints (unchanged) ─────────────────────
# keep the /price, /funding, /rv, /clearing/* routes exactly as before.
