#!/usr/bin/env python3
"""
Saving Capital Market Intelligence Dashboard — Backend Server
"""

import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
import anthropic
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from pydantic import BaseModel

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
PORT              = int(os.environ.get("PORT", 8000))
DASHBOARD_PATH    = Path(__file__).parent / "dashboard.html"

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

price_cache    = {"data": {}, "ts": 0}
analysis_cache = {}
PRICE_TTL      = 300
ANALYSIS_TTL   = 86400
TIMEOUT        = aiohttp.ClientTimeout(total=15)
UA             = "SavingCapitalDashboard/1.0"

YAHOO_ASSETS = [
    {"id": "GC=F",     "name": "Gold",         "sym": "XAUUSD"},
    {"id": "EURUSD=X", "name": "EUR/USD",       "sym": "EURUSD"},
    {"id": "GBPUSD=X", "name": "GBP/USD",       "sym": "GBPUSD"},
    {"id": "USDJPY=X", "name": "USD/JPY",       "sym": "USDJPY"},
    {"id": "DX-Y.NYB", "name": "DXY Index",     "sym": "DXY"},
    {"id": "AUDUSD=X", "name": "AUD/USD",       "sym": "AUDUSD"},
    {"id": "USDCAD=X", "name": "USD/CAD",       "sym": "USDCAD"},
    {"id": "USDCHF=X", "name": "USD/CHF",       "sym": "USDCHF"},
    {"id": "NZDUSD=X", "name": "NZD/USD",       "sym": "NZDUSD"},
    {"id": "CL=F",     "name": "WTI Crude Oil", "sym": "WTI"},
    {"id": "BZ=F",     "name": "Brent Crude",   "sym": "BRENT"},
    {"id": "SI=F",     "name": "Silver",        "sym": "XAGUSD"},
    {"id": "HG=F",     "name": "Copper",        "sym": "COPPER"},
    {"id": "NG=F",     "name": "Natural Gas",   "sym": "NATGAS"},
    {"id": "PL=F",     "name": "Platinum",      "sym": "XPTUSD"},
    {"id": "PA=F",     "name": "Palladium",     "sym": "XPDUSD"},
]

CRYPTO_ASSETS = [
    {"id": "bitcoin",            "name": "Bitcoin",           "sym": "BTC"},
    {"id": "ethereum",           "name": "Ethereum",          "sym": "ETH"},
    {"id": "ripple",             "name": "XRP",               "sym": "XRP"},
    {"id": "solana",             "name": "Solana",            "sym": "SOL"},
    {"id": "binancecoin",        "name": "BNB",               "sym": "BNB"},
    {"id": "dogecoin",           "name": "Dogecoin",          "sym": "DOGE"},
    {"id": "cardano",            "name": "Cardano",           "sym": "ADA"},
    {"id": "avalanche-2",        "name": "Avalanche",         "sym": "AVAX"},
    {"id": "chainlink",          "name": "Chainlink",         "sym": "LINK"},
    {"id": "polkadot",           "name": "Polkadot",          "sym": "DOT"},
    {"id": "the-open-network",   "name": "Toncoin",           "sym": "TON"},
    {"id": "shiba-inu",          "name": "Shiba Inu",         "sym": "SHIB"},
    {"id": "litecoin",           "name": "Litecoin",          "sym": "LTC"},
    {"id": "tron",               "name": "TRON",              "sym": "TRX"},
    {"id": "matic-network",      "name": "Polygon",           "sym": "POL"},
    {"id": "uniswap",            "name": "Uniswap",           "sym": "UNI"},
    {"id": "stellar",            "name": "Stellar",           "sym": "XLM"},
    {"id": "near",               "name": "Near Protocol",     "sym": "NEAR"},
    {"id": "arbitrum",           "name": "Arbitrum",          "sym": "ARB"},
    {"id": "aptos",              "name": "Aptos",             "sym": "APT"},
    {"id": "internet-computer",  "name": "ICP",               "sym": "ICP"},
    {"id": "filecoin",           "name": "Filecoin",          "sym": "FIL"},
    {"id": "render-token",       "name": "Render",            "sym": "RENDER"},
    {"id": "injective-protocol", "name": "Injective",         "sym": "INJ"},
    {"id": "monero",             "name": "Monero",            "sym": "XMR"},
]

# ─── Price Fetching ───────────────────────────────────────────────────────────

async def fetch_yahoo(session, asset_id: str) -> dict:
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{asset_id}"
        async with session.get(url, params={"interval": "1d", "range": "10d"},
                               headers={"User-Agent": UA}, timeout=TIMEOUT) as r:
            if r.status == 200:
                data   = await r.json()
                result = data.get("chart", {}).get("result", [])
                if result:
                    closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
                    closes = [c for c in closes if c is not None]
                    if len(closes) >= 2:
                        cur, prev = closes[-1], closes[-2]
                        w = closes[-5] if len(closes) >= 5 else prev
                        return {
                            "price":    round(cur, 6),
                            "change":   round(((cur - prev) / prev) * 100, 3),
                            "change5d": round(((cur - w) / w) * 100, 3),
                            "closes":   [round(c, 6) for c in closes[-20:]],
                        }
    except Exception as e:
        print(f"Yahoo {asset_id}: {e}")
    return {}

async def fetch_coingecko(ids: list) -> dict:
    try:
        params = {
            "ids": ",".join(ids),
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_7d_change": "true",
            "include_market_cap": "true",
        }
        headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.coingecko.com/api/v3/simple/price",
                                   params=params, headers=headers, timeout=TIMEOUT) as r:
                if r.status == 200:
                    return await r.json()
    except Exception as e:
        print(f"CoinGecko: {e}")
    return {}

async def load_all_prices() -> dict:
    result = {}
    async with aiohttp.ClientSession() as session:
        tasks  = [fetch_yahoo(session, a["id"]) for a in YAHOO_ASSETS]
        datas  = await asyncio.gather(*tasks, return_exceptions=True)
    for asset, data in zip(YAHOO_ASSETS, datas):
        if isinstance(data, dict) and data:
            result[asset["id"]] = {**asset, **data}

    cg = await fetch_coingecko([a["id"] for a in CRYPTO_ASSETS])
    for asset in CRYPTO_ASSETS:
        d = cg.get(asset["id"], {})
        if d.get("usd"):
            result[asset["id"]] = {
                **asset,
                "price":    round(d["usd"], 8),
                "change":   round(d.get("usd_24h_change", 0) or 0, 3),
                "change5d": round(d.get("usd_7d_change", 0) or 0, 3),
                "mcap":     d.get("usd_market_cap"),
                "closes":   [],
            }
    return result

# ─── Phase Detection ──────────────────────────────────────────────────────────

def detect_phase(prices: dict) -> dict:
    changes = [p["change"] for p in prices.values() if "change" in p]
    if not changes:
        return {"phase": "Unknown", "regime": "Neutral", "risk": "Balanced", "bullPct": 50, "bull": 0, "bear": 0, "neut": 0}
    avg    = sum(changes) / len(changes)
    bull   = sum(1 for c in changes if c > 0.5)
    bear   = sum(1 for c in changes if c < -0.5)
    ratio  = bull / len(changes)
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
    return {"phase": phase, "regime": regime, "risk": risk,
            "bullPct": bullPct, "bull": bull, "bear": bear,
            "neut": len(changes) - bull - bear}

def detect_asset_phase(change: float, change5d: float) -> str:
    if change > 3 and change5d > 5:   return "Strong Uptrend"
    if change > 1 and change5d > 2:   return "Bullish Continuation"
    if change > 0 and change5d < -2:  return "Rebound in Downtrend"
    if change < -3 and change5d < -5: return "Strong Downtrend"
    if change < -1 and change5d < -2: return "Bearish Continuation"
    if change < 0 and change5d > 2:   return "Pullback in Uptrend"
    if abs(change) < 0.5:             return "Tight Consolidation"
    return "Neutral Drift"

def fmt_price(p) -> str:
    if not p: return "N/A"
    if p > 10000: return f"${p:,.0f}"
    if p > 100:   return f"${p:,.2f}"
    if p > 1:     return f"${p:.4f}"
    return f"${p:.8f}"

def fmt_chg(c) -> str:
    if c is None: return "0.00%"
    return f"{'+' if c >= 0 else ''}{c:.2f}%"

# ─── API Routes ───────────────────────────────────────────────────────────────

@app.get("/api/prices")
async def api_prices():
    now = time.time()
    if now - price_cache["ts"] < PRICE_TTL and price_cache["data"]:
        return JSONResponse(price_cache["data"])
    data = await load_all_prices()
    price_cache["data"] = data
    price_cache["ts"]   = now
    return JSONResponse(data)

@app.get("/api/phase")
async def api_phase():
    prices = price_cache["data"] if price_cache["data"] else await load_all_prices()
    return JSONResponse(detect_phase(prices))

class AnalysisRequest(BaseModel):
    asset_id: str

@app.post("/api/analysis")
async def api_analysis(req: AnalysisRequest):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY not set in environment variables")

    today     = datetime.utcnow().strftime("%Y-%m-%d")
    cache_key = f"{req.asset_id}_{today}"

    if cache_key in analysis_cache:
        cached = analysis_cache[cache_key]
        if time.time() - cached["ts"] < ANALYSIS_TTL:
            return JSONResponse(cached["data"])

    prices = price_cache["data"] if price_cache["data"] else await load_all_prices()
    asset  = prices.get(req.asset_id)
    if not asset:
        raise HTTPException(404, f"Asset {req.asset_id} not found in price data")

    phase_data  = detect_phase(prices)
    asset_phase = detect_asset_phase(asset.get("change", 0), asset.get("change5d", 0))
    price_str   = fmt_price(asset.get("price"))
    chg_str     = fmt_chg(asset.get("change"))
    chg5_str    = fmt_chg(asset.get("change5d"))
    name        = asset.get("name", req.asset_id)
    sym         = asset.get("sym", "")

    prompt = f"""You are a Managing Director and Senior Macro Analyst at Goldman Sachs Global Investment Research, with 20 years covering global markets. You write institutional research notes for hedge funds, sovereign wealth funds, and family offices.

LIVE MARKET DATA — {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}:
Asset: {name} ({sym})
Current Price: {price_str}
24h Performance: {chg_str}
5-Day Performance: {chg5_str}
Asset Phase: {asset_phase}

GLOBAL MARKET ENVIRONMENT:
Phase: {phase_data['phase']} | Regime: {phase_data['regime']} | Risk Appetite: {phase_data['risk']}
Breadth: {phase_data['bullPct']}% of tracked assets positive today

Write a Goldman Sachs-calibre institutional research note. Requirements:
- Use GS language: "we believe", "in our view", "our base case", "risks are skewed", "conviction is high/medium/low"
- Reference the live price {price_str} explicitly
- Be specific about price levels and catalysts
- Reflect the current {phase_data['regime']} regime

Respond ONLY with valid JSON, no markdown, no extra text:
{{"exec":"2-3 sentence executive summary referencing live price {price_str} and phase {asset_phase}. State directional view with GS language.","shortTerm":"3-4 sentences on 1-7 day tactical outlook with specific levels near {price_str}.","longTerm":"3-4 sentences on 1-3 month strategic view covering macro fundamentals and {phase_data['phase']} context.","narrative":"3-4 sentences on dominant macro narrative and how {name} fits within {phase_data['regime']} regime.","drivers":["Catalyst 1 — specific and quantified","Catalyst 2 — with risk/reward context","Macro driver — specific threshold or level","Key risk factor — what invalidates the thesis","Positioning dynamic — institutional behaviour"],"positioning":"2-3 sentences on recommended positioning with conviction level and key risk.","assetPhase":"{asset_phase}","globalPhase":"{phase_data['phase']}","generatedAt":"{datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}"}}"""

    try:
        client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1400,
            messages=[{"role": "user", "content": prompt}]
        )
        text   = message.content[0].text.strip()
        clean  = text.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(clean)
        analysis_cache[cache_key] = {"data": parsed, "ts": time.time()}
        return JSONResponse(parsed)
    except json.JSONDecodeError as e:
        raise HTTPException(500, f"Failed to parse AI response: {e}")
    except Exception as e:
        raise HTTPException(500, f"Analysis error: {e}")

# ─── Serve Dashboard (MUST be last — catches all non-API routes) ──────────────

@app.get("/{full_path:path}")
async def serve_dashboard(full_path: str):
    return FileResponse(DASHBOARD_PATH)

# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
