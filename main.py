from fastapi import FastAPI
import httpx

app = FastAPI()

@app.get("/")
def health():
    return {"status": "alive"}

@app.get("/price/{symbol}")
async def get_price(symbol: str):
    """
    Example: /price/BTCUSDT
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
    Example: /funding/BTCUSDT (will fetch BTCUSDT perpetual funding rate)
    """
    # Convert symbol to futures format: BTCUSDT -> perpetual
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
