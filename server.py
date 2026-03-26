#!/usr/bin/env python3
"""
Saving Capital Market Intelligence Dashboard — Backend Server
"""

import asyncio
import json
import os
import time
import re
import xml.etree.ElementTree as ET
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
TWELVE_DATA_KEY   = os.environ.get("TWELVE_DATA_KEY", "")   # free at twelvedata.com
PORT              = int(os.environ.get("PORT", 8000))
DASHBOARD_PATH    = Path(__file__).parent / "dashboard.html"

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

price_cache    = {"data": {}, "ts": 0}
news_cache     = {"data": [], "ts": 0}
analysis_cache = {}

PRICE_TTL    = 300      # 5 min
NEWS_TTL     = 600      # 10 min
ANALYSIS_TTL = 21600    # 6 hr

TIMEOUT = aiohttp.ClientTimeout(total=20)
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"

# ─── Asset Definitions ────────────────────────────────────────────────────────

CRYPTO_ASSETS = [
    {"id": "bitcoin",            "name": "Bitcoin",       "sym": "BTC"},
    {"id": "ethereum",           "name": "Ethereum",      "sym": "ETH"},
    {"id": "ripple",             "name": "XRP",           "sym": "XRP"},
    {"id": "solana",             "name": "Solana",        "sym": "SOL"},
    {"id": "binancecoin",        "name": "BNB",           "sym": "BNB"},
    {"id": "dogecoin",           "name": "Dogecoin",      "sym": "DOGE"},
    {"id": "cardano",            "name": "Cardano",       "sym": "ADA"},
    {"id": "avalanche-2",        "name": "Avalanche",     "sym": "AVAX"},
    {"id": "chainlink",          "name": "Chainlink",     "sym": "LINK"},
    {"id": "polkadot",           "name": "Polkadot",      "sym": "DOT"},
    {"id": "the-open-network",   "name": "Toncoin",       "sym": "TON"},
    {"id": "shiba-inu",          "name": "Shiba Inu",     "sym": "SHIB"},
    {"id": "litecoin",           "name": "Litecoin",      "sym": "LTC"},
    {"id": "tron",               "name": "TRON",          "sym": "TRX"},
    {"id": "matic-network",      "name": "Polygon",       "sym": "POL"},
    {"id": "uniswap",            "name": "Uniswap",       "sym": "UNI"},
    {"id": "stellar",            "name": "Stellar",       "sym": "XLM"},
    {"id": "near",               "name": "Near Protocol", "sym": "NEAR"},
    {"id": "arbitrum",           "name": "Arbitrum",      "sym": "ARB"},
    {"id": "aptos",              "name": "Aptos",         "sym": "APT"},
    {"id": "internet-computer",  "name": "ICP",           "sym": "ICP"},
    {"id": "filecoin",           "name": "Filecoin",      "sym": "FIL"},
    {"id": "render-token",       "name": "Render",        "sym": "RENDER"},
    {"id": "injective-protocol", "name": "Injective",     "sym": "INJ"},
    {"id": "monero",             "name": "Monero",        "sym": "XMR"},
]

# Twelve Data symbols for forex + commodities
TWELVE_ASSETS = [
    {"id": "EURUSD", "name": "EUR/USD",       "sym": "EURUSD", "tab": "forex",  "desc": "Euro vs Dollar"},
    {"id": "GBPUSD", "name": "GBP/USD",       "sym": "GBPUSD", "tab": "forex",  "desc": "Cable"},
    {"id": "USDJPY", "name": "USD/JPY",       "sym": "USDJPY", "tab": "forex",  "desc": "Dollar vs Yen"},
    {"id": "AUDUSD", "name": "AUD/USD",       "sym": "AUDUSD", "tab": "forex",  "desc": "Aussie Dollar"},
    {"id": "USDCAD", "name": "USD/CAD",       "sym": "USDCAD", "tab": "forex",  "desc": "Loonie"},
    {"id": "USDCHF", "name": "USD/CHF",       "sym": "USDCHF", "tab": "forex",  "desc": "Swissie"},
    {"id": "NZDUSD", "name": "NZD/USD",       "sym": "NZDUSD", "tab": "forex",  "desc": "Kiwi Dollar"},
    {"id": "XAU/USD","name": "Gold",          "sym": "XAUUSD", "tab": "oil",    "desc": "Safe haven · Inflation hedge"},
    {"id": "XAG/USD","name": "Silver",        "sym": "XAGUSD", "tab": "oil",    "desc": "Precious · Industrial"},
    {"id": "WTI/USD","name": "WTI Crude Oil", "sym": "WTI",    "tab": "oil",    "desc": "US benchmark crude"},
    {"id": "BRENT/USD","name":"Brent Crude",  "sym": "BRENT",  "tab": "oil",    "desc": "Global benchmark"},
    {"id": "XCU/USD","name": "Copper",        "sym": "COPPER", "tab": "oil",    "desc": "Global growth proxy"},
    {"id": "XPT/USD","name": "Platinum",      "sym": "XPTUSD", "tab": "oil",    "desc": "Precious metals"},
    {"id": "NATGAS/USD","name":"Natural Gas", "sym": "NATGAS", "tab": "oil",    "desc": "Energy commodity"},
]

# DXY via Yahoo as fallback (no Twelve Data support)
DXY_ASSET = {"id": "DX-Y.NYB", "name": "DXY Index", "sym": "DXY", "tab": "forex", "desc": "Dollar strength index"}

# ─── Crypto Prices (CoinGecko) ────────────────────────────────────────────────

async def fetch_crypto_prices() -> dict:
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
    ids = ",".join(a["id"] for a in CRYPTO_ASSETS)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": ids, "vs_currencies": "usd",
                        "include_24hr_change": "true",
                        "include_7d_change": "true",
                        "include_market_cap": "true"},
                headers=headers, timeout=TIMEOUT,
            ) as r:
                if r.status == 200:
                    return await r.json()
    except Exception as e:
        print(f"CoinGecko error: {e}")
    return {}

async def fetch_crypto_sparkline(coin_id: str) -> list:
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
                params={"vs_currency": "usd", "days": "7", "interval": "daily"},
                headers=headers, timeout=TIMEOUT,
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return [round(p[1], 8) for p in data.get("prices", []) if p]
    except Exception as e:
        print(f"CoinGecko sparkline {coin_id}: {e}")
    return []

# ─── Forex + Commodities (Twelve Data free tier) ─────────────────────────────

async def fetch_twelve_quote(session: aiohttp.ClientSession, symbol: str) -> dict:
    """Fetch real-time quote from Twelve Data."""
    if not TWELVE_DATA_KEY:
        return {}
    try:
        async with session.get(
            "https://api.twelvedata.com/quote",
            params={"symbol": symbol, "apikey": TWELVE_DATA_KEY},
            timeout=TIMEOUT,
        ) as r:
            if r.status == 200:
                d = await r.json()
                if d.get("status") == "error":
                    return {}
                price = float(d.get("close") or d.get("price") or 0)
                prev  = float(d.get("previous_close") or price)
                change = ((price - prev) / prev * 100) if prev else 0
                # Get 5d change from percent_change field or calculate
                pct5d = float(d.get("percent_change", 0) or 0)
                return {
                    "price":    round(price, 6),
                    "change":   round(change, 3),
                    "change5d": round(pct5d, 3),
                    "closes":   [],
                }
    except Exception as e:
        print(f"Twelve Data {symbol}: {e}")
    return {}

async def fetch_twelve_timeseries(session: aiohttp.ClientSession, symbol: str) -> list:
    """Fetch 20-day daily series for sparkline."""
    if not TWELVE_DATA_KEY:
        return []
    try:
        async with session.get(
            "https://api.twelvedata.com/time_series",
            params={"symbol": symbol, "interval": "1day",
                    "outputsize": "20", "apikey": TWELVE_DATA_KEY},
            timeout=TIMEOUT,
        ) as r:
            if r.status == 200:
                d = await r.json()
                if d.get("status") == "error":
                    return []
                values = d.get("values", [])
                closes = [round(float(v["close"]), 6) for v in reversed(values) if v.get("close")]
                return closes
    except Exception as e:
        print(f"Twelve timeseries {symbol}: {e}")
    return []

# ─── Yahoo Finance fallback ───────────────────────────────────────────────────

async def fetch_yahoo(session: aiohttp.ClientSession, yahoo_id: str) -> dict:
    headers = {
        "User-Agent": UA,
        "Accept": "application/json,text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    for base in ["https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com"]:
        try:
            async with session.get(
                f"{base}/v8/finance/chart/{yahoo_id}",
                params={"interval": "1d", "range": "20d"},
                headers=headers, timeout=TIMEOUT,
            ) as r:
                if r.status != 200:
                    continue
                data   = await r.json(content_type=None)
                result = data.get("chart", {}).get("result", [])
                if not result:
                    continue
                closes = [c for c in (result[0].get("indicators", {})
                          .get("quote", [{}])[0].get("close") or []) if c]
                if len(closes) < 2:
                    continue
                cur, prev = closes[-1], closes[-2]
                w5 = closes[-5] if len(closes) >= 5 else prev
                return {
                    "price":    round(cur, 6),
                    "change":   round(((cur - prev) / prev) * 100, 3),
                    "change5d": round(((cur - w5) / w5) * 100, 3),
                    "closes":   [round(c, 6) for c in closes[-20:]],
                }
        except Exception as e:
            print(f"Yahoo {yahoo_id}: {e}")
    return {}

# ─── Master Price Loader ──────────────────────────────────────────────────────

async def load_all_prices() -> dict:
    result = {}
    ts = datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] Loading prices...")

    # 1. Crypto via CoinGecko
    cg = await fetch_crypto_prices()
    crypto_ok = 0
    for a in CRYPTO_ASSETS:
        d = cg.get(a["id"], {})
        if d.get("usd"):
            result[a["id"]] = {
                **a,
                "price":    round(d["usd"], 8),
                "change":   round(d.get("usd_24h_change", 0) or 0, 3),
                "change5d": round(d.get("usd_7d_change",  0) or 0, 3),
                "mcap":     d.get("usd_market_cap"),
                "closes":   [],
            }
            crypto_ok += 1
    print(f"  Crypto: {crypto_ok}/{len(CRYPTO_ASSETS)}")

    # 2. Crypto sparklines for top 8
    for coin_id in ["bitcoin","ethereum","ripple","solana","binancecoin","dogecoin","cardano","avalanche-2"]:
        if result.get(coin_id, {}).get("price"):
            cl = await fetch_crypto_sparkline(coin_id)
            if cl:
                result[coin_id]["closes"] = cl
            await asyncio.sleep(0.4)

    # 3. Forex + Commodities — try Twelve Data first, Yahoo as fallback
    async with aiohttp.ClientSession() as session:
        forex_ok = 0
        if TWELVE_DATA_KEY:
            # Twelve Data: batch quotes
            tasks = [fetch_twelve_quote(session, a["id"]) for a in TWELVE_ASSETS]
            datas = await asyncio.gather(*tasks, return_exceptions=True)
            # Timeseries in small batches (respect free tier rate limit)
            ts_tasks = [fetch_twelve_timeseries(session, a["id"]) for a in TWELVE_ASSETS[:8]]
            ts_datas = await asyncio.gather(*ts_tasks, return_exceptions=True)
            for i, (a, d) in enumerate(zip(TWELVE_ASSETS, datas)):
                if isinstance(d, dict) and d.get("price"):
                    closes = ts_datas[i] if i < len(ts_datas) and isinstance(ts_datas[i], list) else []
                    result[a["sym"]] = {**a, "id": a["sym"], **d, "closes": closes}
                    forex_ok += 1
                else:
                    result[a["sym"]] = {**a, "id": a["sym"], "price": None, "change": None, "change5d": None, "closes": []}
        else:
            # No Twelve Data key — fall back entirely to Yahoo Finance
            yahoo_map = {
                "EURUSD": "EURUSD=X", "GBPUSD": "GBPUSD=X", "USDJPY": "USDJPY=X",
                "AUDUSD": "AUDUSD=X", "USDCAD": "USDCAD=X", "USDCHF": "USDCHF=X",
                "NZDUSD": "NZDUSD=X",
                "XAU/USD": "GC=F",  "XAG/USD": "SI=F",    "WTI/USD": "CL=F",
                "BRENT/USD": "BZ=F","XCU/USD": "HG=F",    "XPT/USD": "PL=F",
                "NATGAS/USD": "NG=F",
            }
            tasks = [fetch_yahoo(session, yahoo_map.get(a["id"], a["id"])) for a in TWELVE_ASSETS]
            datas = await asyncio.gather(*tasks, return_exceptions=True)
            for a, d in zip(TWELVE_ASSETS, datas):
                if isinstance(d, dict) and d.get("price"):
                    result[a["sym"]] = {**a, "id": a["sym"], **d}
                    forex_ok += 1
                else:
                    result[a["sym"]] = {**a, "id": a["sym"], "price": None, "change": None, "change5d": None, "closes": []}

        # DXY always via Yahoo
        dxy = await fetch_yahoo(session, "DX-Y.NYB")
        if dxy.get("price"):
            result["DX-Y.NYB"] = {**DXY_ASSET, **dxy}
        else:
            result["DX-Y.NYB"] = {**DXY_ASSET, "price": None, "change": None, "change5d": None, "closes": []}

        print(f"  Forex/Commodities: {forex_ok}/{len(TWELVE_ASSETS)}")

    loaded = sum(1 for v in result.values() if v.get("price"))
    print(f"  Total loaded: {loaded}/43")
    return result

# ─── Market Phase ─────────────────────────────────────────────────────────────

def detect_phase(prices: dict) -> dict:
    changes = [p["change"] for p in prices.values() if p.get("change") is not None]
    if not changes:
        return {"phase":"Unknown","regime":"Neutral","risk":"Balanced","bullPct":50,"bull":0,"bear":0,"neut":0,"avg":"0.00"}
    avg  = sum(changes) / len(changes)
    bull = sum(1 for c in changes if c > 0.5)
    bear = sum(1 for c in changes if c < -0.5)
    rat  = bull / len(changes)
    bpct = round(rat * 100)
    if avg > 2 and rat > .7:   phase,regime,risk = "Bull Run","Risk-On","Elevated"
    elif avg > 0.5 and rat>.55: phase,regime,risk = "Uptrend","Risk-On","Moderate"
    elif avg <-2 and rat < .3:  phase,regime,risk = "Bear Market","Risk-Off","Defensive"
    elif avg <-0.5 and rat<.45: phase,regime,risk = "Downtrend","Risk-Off","Cautious"
    else:                        phase,regime,risk = "Consolidation","Neutral","Balanced"
    return {"phase":phase,"regime":regime,"risk":risk,"bullPct":bpct,
            "bull":bull,"bear":bear,"neut":len(changes)-bull-bear,"avg":f"{avg:+.2f}"}

def detect_asset_phase(c, c5) -> str:
    if c is None or c5 is None: return "Insufficient Data"
    if c > 3 and c5 > 5:   return "Strong Uptrend"
    if c > 1 and c5 > 2:   return "Bullish Continuation"
    if c > 0 and c5 <-2:   return "Rebound in Downtrend"
    if c <-3 and c5 <-5:   return "Strong Downtrend"
    if c <-1 and c5 <-2:   return "Bearish Continuation"
    if c < 0 and c5 > 2:   return "Pullback in Uptrend"
    if abs(c) < 0.5:        return "Tight Consolidation"
    return "Neutral Drift"

def fmt_p(p) -> str:
    if not p: return "N/A"
    if p > 10000: return f"${p:,.0f}"
    if p > 100:   return f"${p:,.2f}"
    if p > 1:     return f"${p:.4f}"
    return f"${p:.8f}"

def fmt_c(c) -> str:
    if c is None: return "N/A"
    return f"{'+' if c>=0 else ''}{c:.2f}%"

# ─── News Fetcher (server-side proxy) ─────────────────────────────────────────

NEWS_FEEDS = [
    {"url": "https://cointelegraph.com/rss",                          "type": "crypto"},
    {"url": "https://decrypt.co/feed",                                 "type": "crypto"},
    {"url": "https://www.theblock.co/rss.xml",                         "type": "crypto"},
    {"url": "https://oilprice.com/rss/main",                          "type": "commodity"},
    {"url": "https://www.kitco.com/rss/news.xml",                     "type": "commodity"},
    {"url": "https://www.forexlive.com/feed/news",                    "type": "forex"},
    {"url": "https://feeds.reuters.com/reuters/businessNews",         "type": "macro"},
    {"url": "https://feeds.feedburner.com/zerohedge/feed",            "type": "macro"},
]

def clean_html(text: str) -> str:
    return re.sub(r'<[^>]+>', '', text or '').strip()

async def fetch_rss(session: aiohttp.ClientSession, feed: dict) -> list:
    try:
        async with session.get(
            feed["url"],
            headers={"User-Agent": UA},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return []
            raw = await r.read()
        root = ET.fromstring(raw)
        items = []
        for item in root.iter("item"):
            title = clean_html(item.findtext("title", ""))
            link  = (item.findtext("link") or "").strip()
            if title and link and len(title) > 15:
                items.append({"title": title, "link": link, "type": feed["type"]})
            if len(items) >= 6:
                break
        return items
    except Exception as e:
        print(f"RSS {feed['url'][:40]}: {e}")
    return []

async def load_news() -> list:
    async with aiohttp.ClientSession() as session:
        tasks   = [fetch_rss(session, f) for f in NEWS_FEEDS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    items = []
    for r in results:
        if isinstance(r, list):
            items.extend(r)
    import random
    random.shuffle(items)
    return items

# ─── API Endpoints ────────────────────────────────────────────────────────────

@app.get("/api/prices")
async def api_prices():
    now = time.time()
    if now - price_cache["ts"] < PRICE_TTL and price_cache["data"]:
        return JSONResponse(price_cache["data"])
    data = await load_all_prices()
    price_cache.update({"data": data, "ts": time.time()})
    return JSONResponse(data)

@app.get("/api/phase")
async def api_phase():
    d = price_cache["data"] or await load_all_prices()
    return JSONResponse(detect_phase(d))

@app.get("/api/news")
async def api_news():
    now = time.time()
    if now - news_cache["ts"] < NEWS_TTL and news_cache["data"]:
        return JSONResponse(news_cache["data"])
    items = await load_news()
    news_cache.update({"data": items, "ts": time.time()})
    return JSONResponse(items)

@app.get("/api/health")
async def health():
    loaded = sum(1 for v in price_cache["data"].values() if v.get("price")) if price_cache["data"] else 0
    return {"status": "ok", "assets_loaded": loaded,
            "has_twelve_data": bool(TWELVE_DATA_KEY),
            "cache_age_s": round(time.time() - price_cache["ts"])}

class AnalysisRequest(BaseModel):
    asset_id: str

@app.post("/api/analysis")
async def api_analysis(req: AnalysisRequest):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY not configured")
    today     = datetime.utcnow().strftime("%Y-%m-%d")
    cache_key = f"{req.asset_id}_{today}"
    if cache_key in analysis_cache:
        c = analysis_cache[cache_key]
        if time.time() - c["ts"] < ANALYSIS_TTL:
            return JSONResponse(c["data"])
    prices = price_cache["data"] or await load_all_prices()
    asset  = prices.get(req.asset_id)
    if not asset:
        raise HTTPException(404, f"Asset {req.asset_id} not found")
    if not asset.get("price"):
        raise HTTPException(503, f"Price unavailable for {req.asset_id}")
    phase   = detect_phase(prices)
    aphase  = detect_asset_phase(asset.get("change"), asset.get("change5d"))
    ps, cs, c5s = fmt_p(asset.get("price")), fmt_c(asset.get("change")), fmt_c(asset.get("change5d"))

    prompt = f"""You are a senior quantitative analyst synthesising the frameworks of the world's top hedge funds:
- All-weather macro framework, debt cycle analysis, risk parity principles
- Statistical momentum, mean-reversion signals, quantitative pattern recognition
- Multi-strategy risk-adjusted positioning, volatility regime classification
- Factor decomposition (momentum, carry, value, quality), regime detection
- Top-down macro research, institutional flow analysis

LIVE DATA — {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}:
Asset: {asset['name']} ({asset['sym']})
Price: {ps} | 24h: {cs} | 5d: {c5s} | Phase: {aphase}
Global: {phase['phase']} | {phase['regime']} | {phase['risk']} risk | {phase['bullPct']}% assets positive | avg {phase['avg']}%

Return ONLY valid JSON:
{{"quant":{{"momentum":"[Long/Short/Neutral] — signal","meanReversion":"[Overbought/Oversold/Neutral] — signal","macroRegime":"[Risk-On/Risk-Off/Neutral] — context","volRegime":"[Low/Medium/High/Extreme] Vol","conviction":"[High/Medium/Low]","score":"[-10 to +10]"}},"exec":"3 sentences. Reference {ps}, phase {aphase}. Clear directional view.","shortTerm":"3-4 sentences. Specific levels near {ps}. Momentum and vol regime.","longTerm":"3-4 sentences. Macro structural drivers. Factor exposures.","narrative":"3-4 sentences. How {phase['phase']} phase and {phase['regime']} regime affect {asset['name']}. Institutional flows.","drivers":["Macro Factor: driver with cycle context","Momentum Signal: quantitative reading","Risk Framework: risk/reward with levels","Factor Model: dominant factor","Flow Analysis: institutional positioning"],"positioning":"3 sentences. Conviction level, entry zone near {ps}, risk management, what invalidates thesis.","assetPhase":"{aphase}","globalPhase":"{phase['phase']}","generatedAt":"{datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}"}}"""

    try:
        client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        msg     = client.messages.create(model="claude-sonnet-4-20250514", max_tokens=1600,
                                          messages=[{"role":"user","content":prompt}])
        parsed  = json.loads(msg.content[0].text.strip().replace("```json","").replace("```","").strip())
        analysis_cache[cache_key] = {"data": parsed, "ts": time.time()}
        return JSONResponse(parsed)
    except Exception as e:
        raise HTTPException(500, f"Analysis error: {e}")

@app.get("/{full_path:path}")
async def serve(full_path: str):
    return FileResponse(DASHBOARD_PATH)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
