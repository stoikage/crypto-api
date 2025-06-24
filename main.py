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
