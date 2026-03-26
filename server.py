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
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
PORT              = int(os.environ.get("PORT", 8000))
DASHBOARD_PATH    = Path(__file__).parent / "dashboard.html"

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

price_cache    = {"data": {}, "ts": 0}
analysis_cache = {}
PRICE_TTL      = 300    # 5 min cache
ANALYSIS_TTL   = 21600  # 6 hr cache (matches refresh schedule)

TIMEOUT = aiohttp.ClientTimeout(total=20)
UA      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"

YAHOO_ASSETS = [
    {"id": "GC=F",     "name": "Gold",          "sym": "XAUUSD", "desc": "Safe haven · Inflation hedge"},
    {"id": "CL=F",     "name": "WTI Crude Oil",  "sym": "WTI",    "desc": "US benchmark crude"},
    {"id": "BZ=F",     "name": "Brent Crude",    "sym": "BRENT",  "desc": "Global benchmark"},
    {"id": "SI=F",     "name": "Silver",          "sym": "XAGUSD", "desc": "Precious · Industrial"},
    {"id": "HG=F",     "name": "Copper",          "sym": "COPPER", "desc": "Global growth proxy"},
    {"id": "NG=F",     "name": "Natural Gas",     "sym": "NATGAS", "desc": "Energy commodity"},
    {"id": "PL=F",     "name": "Platinum",        "sym": "XPTUSD", "desc": "Precious metals"},
    {"id": "PA=F",     "name": "Palladium",       "sym": "XPDUSD", "desc": "Industrial precious"},
    {"id": "EURUSD=X", "name": "EUR/USD",         "sym": "EURUSD", "desc": "Euro vs Dollar"},
    {"id": "GBPUSD=X", "name": "GBP/USD",         "sym": "GBPUSD", "desc": "Cable"},
    {"id": "USDJPY=X", "name": "USD/JPY",         "sym": "USDJPY", "desc": "Dollar vs Yen"},
    {"id": "DX-Y.NYB", "name": "DXY Index",       "sym": "DXY",    "desc": "Dollar strength"},
    {"id": "AUDUSD=X", "name": "AUD/USD",         "sym": "AUDUSD", "desc": "Aussie Dollar"},
    {"id": "USDCAD=X", "name": "USD/CAD",         "sym": "USDCAD", "desc": "Loonie"},
    {"id": "USDCHF=X", "name": "USD/CHF",         "sym": "USDCHF", "desc": "Swissie"},
    {"id": "NZDUSD=X", "name": "NZD/USD",         "sym": "NZDUSD", "desc": "Kiwi Dollar"},
]

CRYPTO_ASSETS = [
    {"id": "bitcoin",            "name": "Bitcoin",          "sym": "BTC"},
    {"id": "ethereum",           "name": "Ethereum",         "sym": "ETH"},
    {"id": "ripple",             "name": "XRP",              "sym": "XRP"},
    {"id": "solana",             "name": "Solana",           "sym": "SOL"},
    {"id": "binancecoin",        "name": "BNB",              "sym": "BNB"},
    {"id": "dogecoin",           "name": "Dogecoin",         "sym": "DOGE"},
    {"id": "cardano",            "name": "Cardano",          "sym": "ADA"},
    {"id": "avalanche-2",        "name": "Avalanche",        "sym": "AVAX"},
    {"id": "chainlink",          "name": "Chainlink",        "sym": "LINK"},
    {"id": "polkadot",           "name": "Polkadot",         "sym": "DOT"},
    {"id": "the-open-network",   "name": "Toncoin",          "sym": "TON"},
    {"id": "shiba-inu",          "name": "Shiba Inu",        "sym": "SHIB"},
    {"id": "litecoin",           "name": "Litecoin",         "sym": "LTC"},
    {"id": "tron",               "name": "TRON",             "sym": "TRX"},
    {"id": "matic-network",      "name": "Polygon",          "sym": "POL"},
    {"id": "uniswap",            "name": "Uniswap",          "sym": "UNI"},
    {"id": "stellar",            "name": "Stellar",          "sym": "XLM"},
    {"id": "near",               "name": "Near Protocol",    "sym": "NEAR"},
    {"id": "arbitrum",           "name": "Arbitrum",         "sym": "ARB"},
    {"id": "aptos",              "name": "Aptos",            "sym": "APT"},
    {"id": "internet-computer",  "name": "ICP",              "sym": "ICP"},
    {"id": "filecoin",           "name": "Filecoin",         "sym": "FIL"},
    {"id": "render-token",       "name": "Render",           "sym": "RENDER"},
    {"id": "injective-protocol", "name": "Injective",        "sym": "INJ"},
    {"id": "monero",             "name": "Monero",           "sym": "XMR"},
]

# ─── Yahoo Finance Fetcher ────────────────────────────────────────────────────

async def fetch_yahoo(session: aiohttp.ClientSession, asset_id: str) -> dict:
    """Try both Yahoo endpoints with fallback."""
    endpoints = [
        f"https://query1.finance.yahoo.com/v8/finance/chart/{asset_id}",
        f"https://query2.finance.yahoo.com/v8/finance/chart/{asset_id}",
    ]
    headers = {
        "User-Agent": UA,
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://finance.yahoo.com",
        "Referer": "https://finance.yahoo.com/",
    }
    for url in endpoints:
        try:
            async with session.get(
                url,
                params={"interval": "1d", "range": "20d"},
                headers=headers,
                timeout=TIMEOUT,
            ) as r:
                if r.status != 200:
                    continue
                data   = await r.json(content_type=None)
                result = data.get("chart", {}).get("result", [])
                if not result:
                    continue
                quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
                closes = [c for c in (quotes.get("close") or []) if c is not None]
                if len(closes) < 2:
                    continue
                cur   = closes[-1]
                prev  = closes[-2]
                w5    = closes[-5] if len(closes) >= 5 else prev
                return {
                    "price":    round(cur, 6),
                    "change":   round(((cur - prev) / prev) * 100, 3),
                    "change5d": round(((cur - w5) / w5) * 100, 3),
                    "closes":   [round(c, 6) for c in closes[-20:]],
                }
        except Exception as e:
            print(f"  Yahoo {asset_id} @ {url[:40]}: {e}")
    print(f"  Yahoo FAILED for {asset_id}")
    return {}

# ─── CoinGecko Fetcher ────────────────────────────────────────────────────────

async def fetch_coingecko_prices(ids: list) -> dict:
    """Fetch current prices + 24h/7d change for all crypto assets."""
    headers = {}
    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY
    try:
        params = {
            "ids": ",".join(ids),
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_7d_change": "true",
            "include_market_cap": "true",
        }
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params=params, headers=headers, timeout=TIMEOUT,
            ) as r:
                if r.status == 200:
                    return await r.json()
    except Exception as e:
        print(f"CoinGecko prices: {e}")
    return {}

async def fetch_coingecko_sparkline(coin_id: str) -> list:
    """Fetch 14-day hourly closes for sparkline chart."""
    headers = {}
    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
                params={"vs_currency": "usd", "days": "14", "interval": "daily"},
                headers=headers,
                timeout=TIMEOUT,
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    prices = data.get("prices", [])
                    # Return just the close prices (index 1 in [timestamp, price])
                    return [round(p[1], 8) for p in prices if p and len(p) > 1]
    except Exception as e:
        print(f"CoinGecko sparkline {coin_id}: {e}")
    return []

# ─── Load All Prices ──────────────────────────────────────────────────────────

async def load_all_prices() -> dict:
    result = {}
    print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Loading all prices...")

    # ── Yahoo Finance (commodities + forex) ──────────────────────────────────
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_yahoo(session, a["id"]) for a in YAHOO_ASSETS]
        datas = await asyncio.gather(*tasks, return_exceptions=True)

    yahoo_ok = 0
    for asset, data in zip(YAHOO_ASSETS, datas):
        if isinstance(data, dict) and data.get("price"):
            result[asset["id"]] = {**asset, **data}
            yahoo_ok += 1
        else:
            # Store asset metadata with empty price so UI still shows the card
            result[asset["id"]] = {**asset, "price": None, "change": None, "change5d": None, "closes": []}

    print(f"  Yahoo: {yahoo_ok}/{len(YAHOO_ASSETS)} assets loaded")

    # ── CoinGecko (crypto prices) ─────────────────────────────────────────────
    cg_ids  = [a["id"] for a in CRYPTO_ASSETS]
    cg_data = await fetch_coingecko_prices(cg_ids)

    crypto_ok = 0
    for asset in CRYPTO_ASSETS:
        d = cg_data.get(asset["id"], {})
        if d.get("usd"):
            result[asset["id"]] = {
                **asset,
                "price":    round(d["usd"], 8),
                "change":   round(d.get("usd_24h_change", 0) or 0, 3),
                "change5d": round(d.get("usd_7d_change", 0) or 0, 3),
                "mcap":     d.get("usd_market_cap"),
                "closes":   [],  # populated below
            }
            crypto_ok += 1
        else:
            result[asset["id"]] = {**asset, "price": None, "change": None, "change5d": None, "closes": []}

    print(f"  CoinGecko: {crypto_ok}/{len(CRYPTO_ASSETS)} assets loaded")

    # ── CoinGecko sparklines (fetch in batches to respect rate limits) ────────
    # Only fetch top 10 by market cap to stay within free tier limits
    top_crypto = ["bitcoin", "ethereum", "ripple", "solana", "binancecoin",
                  "dogecoin", "cardano", "avalanche-2", "chainlink", "polkadot"]

    for coin_id in top_crypto:
        if result.get(coin_id, {}).get("price"):
            closes = await fetch_coingecko_sparkline(coin_id)
            if closes:
                result[coin_id]["closes"] = closes
            await asyncio.sleep(0.5)  # polite rate limiting

    print(f"  Sparklines loaded for top 10 crypto")
    print(f"  Total assets with prices: {sum(1 for v in result.values() if v.get('price'))}/43")
    return result

# ─── Market Phase Detection ───────────────────────────────────────────────────

def detect_phase(prices: dict) -> dict:
    changes = [p["change"] for p in prices.values() if p.get("change") is not None]
    if not changes:
        return {"phase": "Unknown", "regime": "Neutral", "risk": "Balanced",
                "bullPct": 50, "bull": 0, "bear": 0, "neut": 0, "avg": "0.00"}

    avg     = sum(changes) / len(changes)
    bull    = sum(1 for c in changes if c > 0.5)
    bear    = sum(1 for c in changes if c < -0.5)
    ratio   = bull / len(changes)
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

    return {
        "phase": phase, "regime": regime, "risk": risk,
        "bullPct": bullPct, "bull": bull, "bear": bear,
        "neut": len(changes) - bull - bear,
        "avg": f"{avg:+.2f}",
    }

def detect_asset_phase(change: float, change5d: float) -> str:
    if change is None or change5d is None: return "Insufficient Data"
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
    if c is None: return "N/A"
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
    data = price_cache["data"] if price_cache["data"] else await load_all_prices()
    return JSONResponse(detect_phase(data))

@app.get("/api/health")
async def health():
    loaded = sum(1 for v in price_cache["data"].values() if v.get("price")) if price_cache["data"] else 0
    return {"status": "ok", "assets_loaded": loaded, "cache_age_seconds": round(time.time() - price_cache["ts"])}

class AnalysisRequest(BaseModel):
    asset_id: str

@app.post("/api/analysis")
async def api_analysis(req: AnalysisRequest):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY not configured")

    today     = datetime.utcnow().strftime("%Y-%m-%d")
    cache_key = f"{req.asset_id}_{today}"

    if cache_key in analysis_cache:
        cached = analysis_cache[cache_key]
        if time.time() - cached["ts"] < ANALYSIS_TTL:
            return JSONResponse(cached["data"])

    prices = price_cache["data"] if price_cache["data"] else await load_all_prices()
    asset  = prices.get(req.asset_id)
    if not asset:
        raise HTTPException(404, f"Asset {req.asset_id} not found")
    if not asset.get("price"):
        raise HTTPException(503, f"Price data unavailable for {req.asset_id}")

    phase      = detect_phase(prices)
    a_phase    = detect_asset_phase(asset.get("change"), asset.get("change5d"))
    price_str  = fmt_price(asset.get("price"))
    chg_str    = fmt_chg(asset.get("change"))
    chg5_str   = fmt_chg(asset.get("change5d"))

    prompt = f"""You are a senior quantitative analyst synthesising the analytical frameworks of the world's top hedge funds and trading firms. Your analysis integrates:
- BRIDGEWATER ASSOCIATES: Ray Dalio's all-weather macro framework, debt cycle analysis, risk parity principles
- RENAISSANCE TECHNOLOGIES: Statistical momentum, mean-reversion signals, quantitative pattern recognition
- CITADEL: Multi-strategy risk-adjusted positioning, volatility regime classification, position sizing
- TWO SIGMA: Machine learning regime detection, factor decomposition (momentum, carry, value, quality)
- GOLDMAN SACHS: Top-down macro research, institutional flow analysis, fundamental valuation

LIVE MARKET DATA — {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}:
Asset: {asset['name']} ({asset['sym']})
Current Price: {price_str}
24h Performance: {chg_str}
5-Day Performance: {chg5_str}
Asset-Specific Phase: {a_phase}

GLOBAL MARKET ENVIRONMENT:
Phase: {phase['phase']} | Regime: {phase['regime']} | Risk Appetite: {phase['risk']}
Market Breadth: {phase['bullPct']}% of 43 assets positive | Avg 24h move: {phase['avg']}%

Apply all five frameworks to generate a comprehensive Wall Street institutional research note.
Reference the actual live price {price_str} explicitly. Be specific about levels, not vague.

Respond ONLY with valid JSON, no markdown, no extra text:
{{"quant":{{"momentum":"[Long/Short/Neutral] — [1 sentence momentum signal]","meanReversion":"[Overbought/Oversold/Neutral] — [1 sentence]","macroRegime":"[Risk-On/Risk-Off/Neutral] — [1 sentence]","volRegime":"[Low/Medium/High/Extreme] Vol — [1 sentence]","conviction":"[High/Medium/Low]","score":"[number -10 to +10 e.g. +6.5]"}},"exec":"3 sentences. Reference {price_str}, phase {a_phase}, state directional view blending Bridgewater macro and RenTech quant.","shortTerm":"3-4 sentences tactical 1-7 day. Specific price levels near {price_str}. Momentum signals and vol regime.","longTerm":"3-4 sentences strategic 1-3 month. Bridgewater debt cycle + Two Sigma factor exposures.","narrative":"3-4 sentences. Dalio economic machine on {asset['name']}. {phase['phase']} phase and {phase['regime']} regime impact. Goldman institutional flows.","drivers":["Bridgewater Macro Factor: specific driver with cycle context","RenTech Momentum Signal: quantitative momentum reading with direction","Citadel Risk Framework: risk/reward with specific price levels","Two Sigma Factor Model: dominant factor — momentum/carry/value/quality","Goldman Flow Analysis: institutional positioning dynamic"],"positioning":"3 sentences. Citadel-style conviction, specific entry zone near {price_str}, risk management, what invalidates thesis.","assetPhase":"{a_phase}","globalPhase":"{phase['phase']}","generatedAt":"{datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}"}}"""

    try:
        client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1600,
            messages=[{"role": "user", "content": prompt}]
        )
        text   = message.content[0].text.strip()
        clean  = text.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(clean)
        analysis_cache[cache_key] = {"data": parsed, "ts": time.time()}
        return JSONResponse(parsed)
    except json.JSONDecodeError as e:
        raise HTTPException(500, f"AI response parse error: {e}")
    except Exception as e:
        raise HTTPException(500, f"Analysis error: {e}")

# ─── Serve Dashboard ──────────────────────────────────────────────────────────

@app.get("/{full_path:path}")
async def serve_dashboard(full_path: str):
    return FileResponse(DASHBOARD_PATH)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
