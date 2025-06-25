from fastapi import FastAPI
import httpx
import numpy as np

app = FastAPI()


@app.get("/")
def health():
    return {"status": "alive"}


@app.get("/price/{symbol}")
async def get_price(symbol: str):
    """
    Get latest Binance spot price
    """
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol.upper()}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            return {"error": "Failed to fetch price", "code": resp.status_code}
        data = resp.json()
        return {
            "symbol": data["symbol"],
            "price": float(data["price"])
        }


@app.get("/funding/{symbol}")
async def get_funding(symbol: str):
    """
    Get latest funding rate, mark price, and next funding time from Binance Perps
    """
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
            "nextFundingTime": int(data["nextFundingTime"])
        }


@app.get("/rv/{symbol}")
async def get_realized_vol(symbol: str):
    """
    Get 30-day annualized realized volatility from Binance spot
    """
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
    return {
        "symbol": symbol.upper(),
        "realized_vol": round(vol * 100, 2)  # percent
    }


def compute_clearing_price(order_book: list[list[float]], quantity: float) -> dict:
    """
    Generic VWAP calculation over any standardized order book
    """
    filled = 0
    total_cost = 0

    for price, size in order_book:
        take = min(quantity - filled, size)
        total_cost += take * price
        filled += take
        if filled >= quantity:
            break

    if filled == 0:
        return {"error": "No liquidity at any price", "filled": 0}

    avg_price = total_cost / filled

    if filled < quantity:
        return {
            "error": "Insufficient liquidity",
            "filled": filled,
            "requested": quantity,
            "avg_price": round(avg_price, 6),
            "total_cost": round(total_cost, 2)
        }

    return {
        "filled": filled,
        "avg_price": round(avg_price, 6),
        "total_cost": round(total_cost, 2)
    }


@app.get("/clearing/spot/{symbol}")
async def clearing_spot(symbol: str, quantity: float):
    """
    Compute bid/ask VWAP clearing price using Binance Spot depth
    """
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
        "ask": compute_clearing_price(asks, quantity)
    }

@app.get("/clearing/perp/{symbol}")
async def clearing_perp(symbol: str, quantity: float):
    """
    Compute bid/ask VWAP clearing price using Binance Perps depth
    """
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
        "ask": compute_clearing_price(asks, quantity)
    }
