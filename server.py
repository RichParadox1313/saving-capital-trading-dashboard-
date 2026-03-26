#!/usr/bin/env python3
"""
Saving Capital Market Intelligence Dashboard — Backend Server
Serves the dashboard HTML and handles all API calls server-side.
"""

import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
import anthropic
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ─── Config ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
PORT              = int(os.environ.get("PORT", 8000))

app = FastAPI(title="Saving Capital Dashboard")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Cache ────────────────────────────────────────────────────────────────────
price_cache      = {"data": {}, "ts": 0}
analysis_cache   = {}   # key: asset_id + date
PRICE_TTL        = 300  # 5 min
ANALYSIS_TTL     = 86400  # 24 hr

REQUEST_TIMEOUT  = aiohttp.ClientTimeout(total=15)
USER_AGENT       = "SavingCapitalDashboard/1.0"

# ─── Assets ───────────────────────────────────────────────────────────────────
YAHOO_ASSETS = [
    {"id": "GC=F",      "name": "Gold",          "sym": "XAUUSD", "tab": "gold",  "desc": "Safe haven · Inflation hedge"},
    {"id": "EURUSD=X",  "name": "EUR/USD",        "sym": "EURUSD", "tab": "forex", "desc": "Euro vs Dollar"},
    {"id": "GBPUSD=X",  "name": "GBP/USD",        "sym": "GBPUSD", "tab": "forex", "desc": "Cable"},
    {"id": "USDJPY=X",  "name": "USD/JPY",        "sym": "USDJPY", "tab": "forex", "desc": "Dollar vs Yen"},
    {"id": "DX-Y.NYB",  "name": "DXY Index",      "sym": "DXY",    "tab": "forex", "desc": "Dollar strength"},
    {"id": "AUDUSD=X",  "name": "AUD/USD",        "sym": "AUDUSD", "tab": "forex", "desc": "Aussie Dollar"},
    {"id": "USDCAD=X",  "name": "USD/CAD",        "sym": "USDCAD", "tab": "forex", "desc": "Loonie"},
    {"id": "USDCHF=X",  "name": "USD/CHF",        "sym": "USDCHF", "tab": "forex", "desc": "Swissie"},
    {"id": "NZDUSD=X",  "name": "NZD/USD",        "sym": "NZDUSD", "tab": "forex", "desc": "Kiwi Dollar"},
    {"id": "CL=F",      "name": "WTI Crude Oil",  "sym": "WTI",    "tab": "oil",   "desc": "US benchmark crude"},
    {"id": "BZ=F",      "name": "Brent Crude",    "sym": "BRENT",  "tab": "oil",   "desc": "Global benchmark"},
    {"id": "SI=F",      "name": "Silver",          "sym": "XAGUSD", "tab": "oil",   "desc": "Precious · Industrial"},
    {"id": "HG=F",      "name": "Copper",          "sym": "COPPER", "tab": "oil",   "desc": "Global growth proxy"},
    {"id": "NG=F",      "name": "Natural Gas",     "sym": "NATGAS", "tab": "oil",   "desc": "Energy commodity"},
    {"id": "PL=F",      "name": "Platinum",        "sym": "XPTUSD", "tab": "oil",   "desc": "Precious · Auto"},
    {"id": "PA=F",      "name": "Palladium",       "sym": "XPDUSD", "tab": "oil",   "desc": "Catalytic converter"},
]

CRYPTO_ASSETS = [
    {"id": "bitcoin",            "name": "Bitcoin",          "sym": "BTC",    "tab": "crypto"},
    {"id": "ethereum",           "name": "Ethereum",         "sym": "ETH",    "tab": "crypto"},
    {"id": "ripple",             "name": "XRP",              "sym": "XRP",    "tab": "crypto"},
    {"id": "solana",             "name": "Solana",           "sym": "SOL",    "tab": "crypto"},
    {"id": "binancecoin",        "name": "BNB",              "sym": "BNB",    "tab": "crypto"},
    {"id": "dogecoin",           "name": "Dogecoin",         "sym": "DOGE",   "tab": "crypto"},
    {"id": "cardano",            "name": "Cardano",          "sym": "ADA",    "tab": "crypto"},
    {"id": "avalanche-2",        "name": "Avalanche",        "sym": "AVAX",   "tab": "crypto"},
    {"id": "chainlink",          "name": "Chainlink",        "sym": "LINK",   "tab": "crypto"},
    {"id": "polkadot",           "name": "Polkadot",         "sym": "DOT",    "tab": "crypto"},
    {"id": "the-open-network",   "name": "Toncoin",          "sym": "TON",    "tab": "crypto"},
    {"id": "shiba-inu",          "name": "Shiba Inu",        "sym": "SHIB",   "tab": "crypto"},
    {"id": "litecoin",           "name": "Litecoin",         "sym": "LTC",    "tab": "crypto"},
    {"id": "tron",               "name": "TRON",             "sym": "TRX",    "tab": "crypto"},
    {"id": "matic-network",      "name": "Polygon",          "sym": "POL",    "tab": "crypto"},
    {"id": "uniswap",            "name": "Uniswap",          "sym": "UNI",    "tab": "crypto"},
    {"id": "stellar",            "name": "Stellar",          "sym": "XLM",    "tab": "crypto"},
    {"id": "near",               "name": "Near Protocol",    "sym": "NEAR",   "tab": "crypto"},
    {"id": "arbitrum",           "name": "Arbitrum",         "sym": "ARB",    "tab": "crypto"},
    {"id": "aptos",              "name": "Aptos",            "sym": "APT",    "tab": "crypto"},
    {"id": "internet-computer",  "name": "Internet Computer","sym": "ICP",    "tab": "crypto"},
    {"id": "filecoin",           "name": "Filecoin",         "sym": "FIL",    "tab": "crypto"},
    {"id": "render-token",       "name": "Render",           "sym": "RENDER", "tab": "crypto"},
    {"id": "injective-protocol", "name": "Injective",        "sym": "INJ",    "tab": "crypto"},
    {"id": "monero",             "name": "Monero",           "sym": "XMR",    "tab": "crypto"},
]

ALL_ASSETS = YAHOO_ASSETS + CRYPTO_ASSETS

# ─── Price Fetching ───────────────────────────────────────────────────────────
async def fetch_yahoo(session: aiohttp.ClientSession, asset_id: str) -> dict:
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{asset_id}"
        params = {"interval": "1d", "range": "10d"}
        async with session.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT) as r:
            if r.status == 200:
                data   = await r.json()
                result = data.get("chart", {}).get("result", [])
                if result:
                    closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
                    closes = [c for c in closes if c is not None]
                    if len(closes) >= 2:
                        cur  = closes[-1]
                        prev = closes[-2]
                        w    = closes[-5] if len(closes) >= 5 else prev
                        return {
                            "price":     round(cur, 6),
                            "change":    round(((cur - prev) / prev) * 100, 3),
                            "change5d":  round(((cur - w) / w) * 100, 3),
                        }
    except Exception as e:
        print(f"Yahoo {asset_id}: {e}")
    return {}

async def fetch_coingecko(ids: list[str]) -> dict:
    try:
        params = {
            "ids": ",".join(ids),
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_7d_change": "true",
            "include_market_cap": "true",
        }
        headers = {}
        if COINGECKO_API_KEY:
            headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params=params, headers=headers, timeout=REQUEST_TIMEOUT
            ) as r:
                if r.status == 200:
                    return await r.json()
    except Exception as e:
        print(f"CoinGecko: {e}")
    return {}

async def load_all_prices() -> dict:
    result = {}
    async with aiohttp.ClientSession() as session:
        tasks  = [fetch_yahoo(session, a["id"]) for a in YAHOO_ASSETS]
        prices = await asyncio.gather(*tasks, return_exceptions=True)
    for asset, price in zip(YAHOO_ASSETS, prices):
        if isinstance(price, dict) and price:
            result[asset["id"]] = {**asset, **price}

    cg_data = await fetch_coingecko([a["id"] for a in CRYPTO_ASSETS])
    for asset in CRYPTO_ASSETS:
        d = cg_data.get(asset["id"], {})
        if d.get("usd"):
            result[asset["id"]] = {
                **asset,
                "price":    round(d["usd"], 8),
                "change":   round(d.get("usd_24h_change", 0) or 0, 3),
                "change5d": round(d.get("usd_7d_change", 0) or 0, 3),
                "mcap":     d.get("usd_market_cap"),
            }
    return result

# ─── Market Phase Detection ───────────────────────────────────────────────────
def detect_phase(prices: dict) -> dict:
    changes = [p["change"] for p in prices.values() if "change" in p]
    if not changes:
        return {"phase": "Unknown", "regime": "Unknown", "risk": "Unknown", "bullPct": 50}
    avg   = sum(changes) / len(changes)
    bull  = sum(1 for c in changes if c > 0.5)
    bear  = sum(1 for c in changes if c < -0.5)
    ratio = bull / len(changes)
    bullPct = round((bull / len(changes)) * 100)

    if avg > 2 and ratio > .7:
        phase, regime, risk = "Bull Run", "Risk-On", "Elevated"
    elif avg > 0.5 and ratio > .55:
        phase, regime, risk = "Uptrend", "Risk-On", "Moderate"
    elif avg < -2 and ratio < .3:
        phase, regime, risk = "Bear Market", "Risk-Off", "Defensive"
    elif avg < -0.5 and ratio < .45:
        phase, regime, risk = "Downtrend", "Risk-Off", "Cautious"
    else:
        phase, regime, risk = "Consolidation", "Neutral", "Balanced"

    return {"phase": phase, "regime": regime, "risk": risk, "bullPct": bullPct, "bull": bull, "bear": bear, "neut": len(changes) - bull - bear}

def detect_asset_phase(change: float, change5d: float) -> str:
    if change > 3 and change5d > 5:   return "Strong Uptrend / Momentum Phase"
    if change > 1 and change5d > 2:   return "Bullish Continuation"
    if change > 0 and change5d < -2:  return "Rebound within Downtrend"
    if change < -3 and change5d < -5: return "Strong Downtrend / Distribution"
    if change < -1 and change5d < -2: return "Bearish Continuation"
    if change < 0 and change5d > 2:   return "Pullback within Uptrend"
    if abs(change) < 0.5:             return "Tight Consolidation / Coiling"
    return "Neutral Drift"

def fmt_price(p: float) -> str:
    if p is None: return "N/A"
    if p > 10000: return f"${p:,.0f}"
    if p > 100:   return f"${p:,.2f}"
    if p > 1:     return f"${p:.4f}"
    return f"${p:.8f}"

def fmt_chg(c: float) -> str:
    if c is None: return "0.00%"
    return f"{'+' if c >= 0 else ''}{c:.2f}%"

# ─── API Endpoints ────────────────────────────────────────────────────────────

@app.get("/api/prices")
async def get_prices():
    now = time.time()
    if now - price_cache["ts"] < PRICE_TTL and price_cache["data"]:
        return JSONResponse(price_cache["data"])
    data = await load_all_prices()
    price_cache["data"] = data
    price_cache["ts"]   = now
    return JSONResponse(data)

@app.get("/api/phase")
async def get_phase():
    prices = price_cache["data"] or await load_all_prices()
    return JSONResponse(detect_phase(prices))

class AnalysisRequest(BaseModel):
    asset_id: str

@app.post("/api/analysis")
async def get_analysis(req: AnalysisRequest):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY not configured")

    today     = datetime.utcnow().strftime("%Y-%m-%d")
    cache_key = f"{req.asset_id}_{today}"

    # Return cached analysis if fresh
    if cache_key in analysis_cache:
        cached = analysis_cache[cache_key]
        if time.time() - cached["ts"] < ANALYSIS_TTL:
            return JSONResponse(cached["data"])

    # Get prices
    prices = price_cache["data"]
    if not prices:
        prices = await load_all_prices()

    asset_data = prices.get(req.asset_id)
    if not asset_data:
        raise HTTPException(404, f"Asset {req.asset_id} not found")

    # Get global phase
    phase_data   = detect_phase(prices)
    asset_phase  = detect_asset_phase(asset_data.get("change", 0), asset_data.get("change5d", 0))
    price_str    = fmt_price(asset_data.get("price"))
    chg_str      = fmt_chg(asset_data.get("change"))
    chg5_str     = fmt_chg(asset_data.get("change5d"))

    prompt = f"""You are a Managing Director and Senior Macro Analyst at Goldman Sachs Global Investment Research, with 20 years covering global markets. You write institutional research notes for hedge funds, sovereign wealth funds, and family offices.

LIVE MARKET DATA — {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}:
Asset: {asset_data['name']} ({asset_data['sym']})
Current Price: {price_str}
24h Performance: {chg_str}
5-Day Performance: {chg5_str}
Asset-Specific Phase: {asset_phase}

GLOBAL MARKET ENVIRONMENT:
Market Phase: {phase_data['phase']}
Market Regime: {phase_data['regime']}
Risk Appetite: {phase_data['risk']}
Breadth: {phase_data['bullPct']}% of tracked assets positive on the day

Write a Goldman Sachs-calibre institutional research note for {asset_data['name']}.
Requirements:
- Use precise GS language: "we believe", "in our view", "we maintain", "our base case", "risks are skewed to the", "conviction is high/medium/low"
- Reference the actual live price {price_str} and asset phase {asset_phase} explicitly in the executive summary
- Be specific about price levels, ranges, and catalysts — not vague generalities
- Reflect the current global regime ({phase_data['regime']}, {phase_data['risk']} risk appetite)
- Write with the precision and authority of a GS Research note

Return ONLY a valid JSON object — no markdown, no code blocks, no explanation:
{{
  "exec": "2-3 sentence executive summary. Must reference live price {price_str} and phase {asset_phase}. State a clear directional view with GS language.",
  "shortTerm": "3-4 sentences on 1-7 day tactical outlook. Reference specific price levels near {price_str}. Include momentum, nearest catalysts, and key levels to watch.",
  "longTerm": "3-4 sentences on 1-3 month strategic view. Cover structural drivers, macro tailwinds/headwinds, and positioning. Reference {phase_data['phase']} market context.",
  "narrative": "3-4 sentences on dominant macro narrative. How does {asset_data['name']} fit within current {phase_data['regime']} regime? What is the structural story driving this asset?",
  "drivers": [
    "Key catalyst or structural driver — specific and quantified where possible",
    "Second driver — with risk/reward context and conviction level",
    "Macro or technical driver — specific price level or threshold",
    "Key risk factor or tail risk — what could invalidate the thesis",
    "Positioning or flow dynamic — institutional behaviour or sentiment"
  ],
  "positioning": "2-3 sentences on recommended positioning. Include conviction level, approach, and key risk to the thesis. Written as a formal GS recommendation.",
  "assetPhase": "{asset_phase}",
  "globalPhase": "{phase_data['phase']}",
  "generatedAt": "{datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}"
}}"""

    try:
        client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message  = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1400,
            messages=[{"role": "user", "content": prompt}]
        )
        text     = message.content[0].text.strip()
        clean    = text.replace("```json", "").replace("```", "").strip()
        parsed   = json.loads(clean)

        analysis_cache[cache_key] = {"data": parsed, "ts": time.time()}
        return JSONResponse(parsed)

    except json.JSONDecodeError as e:
        raise HTTPException(500, f"Failed to parse AI response: {e}")
    except Exception as e:
        raise HTTPException(500, f"Analysis error: {e}")

# ─── Serve Dashboard HTML ─────────────────────────────────────────────────────
@app.get("/")
async def serve_dashboard():
    return FileResponse(Path(__file__).parent / "dashboard.html")

# ─── Run ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
