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
    Get the latest spot price for a symbol (e.g., BTCUSDT)
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
    Get latest funding rate, mark price, and next funding time from Binance Futures
    """
    symbol = symbol.upper()
    url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"
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
    Calculate annualized 30-day realized volatility from daily closing prices.
    """
    symbol = symbol.upper()
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1d&limit=31"

    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            return {"error": "Failed to fetch historical prices", "code": resp.status_code}
        data = resp.json()

    # Extract closing prices
    closes = [float(entry[4]) for entry in data]  # entry[4] = close
    if len(closes) < 31:
        return {"error": "Insufficient data"}

    # Compute log daily returns
    returns = np.diff(np.log(closes))

    # Realized volatility = std dev of daily returns × sqrt(365)
    vol = np.std(returns) * np.sqrt(365)

    return {
        "symbol": symbol,
        "realized_vol": round(vol * 100, 2)  # return as %
    }
