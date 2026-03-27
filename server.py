#!/usr/bin/env python3
"""
Saving Capital Market Intelligence Dashboard — Backend Server
Reliable price sources — no extra API keys required beyond Anthropic
"""

import asyncio, json, os, re, time, xml.etree.ElementTree as ET, random
from datetime import datetime
from pathlib import Path

import aiohttp, anthropic
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

@app.on_event("startup")
async def startup_event():
    print("[STARTUP] Pre-loading all prices...")
    try:
        data = await load_all_prices()
        price_cache.update({"data": data, "ts": time.time()})
        loaded = sum(1 for v in data.values() if v.get("price"))
        print(f"[STARTUP] Done — {loaded}/43 assets loaded")
    except Exception as e:
        print(f"[STARTUP] Price pre-load failed: {e}")

price_cache    = {"data": {}, "ts": 0.0}
news_cache     = {"data": [], "ts": 0.0}
analysis_cache = {}

PRICE_TTL    = 300
NEWS_TTL     = 600
ANALYSIS_TTL = 21600

# Full browser headers — makes Yahoo Finance respond correctly from server
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}

YAHOO_HEADERS = {
    **BROWSER_HEADERS,
    "Referer": "https://finance.yahoo.com/",
    "Origin": "https://finance.yahoo.com",
}

TIMEOUT_FAST = aiohttp.ClientTimeout(total=12)
TIMEOUT_SLOW = aiohttp.ClientTimeout(total=20)

# ─── Asset Definitions ────────────────────────────────────────────────────────

CRYPTO_ASSETS = [
    {"id":"bitcoin",            "name":"Bitcoin",       "sym":"BTC",    "tab":"crypto"},
    {"id":"ethereum",           "name":"Ethereum",      "sym":"ETH",    "tab":"crypto"},
    {"id":"ripple",             "name":"XRP",           "sym":"XRP",    "tab":"crypto"},
    {"id":"solana",             "name":"Solana",        "sym":"SOL",    "tab":"crypto"},
    {"id":"binancecoin",        "name":"BNB",           "sym":"BNB",    "tab":"crypto"},
    {"id":"dogecoin",           "name":"Dogecoin",      "sym":"DOGE",   "tab":"crypto"},
    {"id":"cardano",            "name":"Cardano",       "sym":"ADA",    "tab":"crypto"},
    {"id":"avalanche-2",        "name":"Avalanche",     "sym":"AVAX",   "tab":"crypto"},
    {"id":"chainlink",          "name":"Chainlink",     "sym":"LINK",   "tab":"crypto"},
    {"id":"polkadot",           "name":"Polkadot",      "sym":"DOT",    "tab":"crypto"},
    {"id":"the-open-network",   "name":"Toncoin",       "sym":"TON",    "tab":"crypto"},
    {"id":"shiba-inu",          "name":"Shiba Inu",     "sym":"SHIB",   "tab":"crypto"},
    {"id":"litecoin",           "name":"Litecoin",      "sym":"LTC",    "tab":"crypto"},
    {"id":"tron",               "name":"TRON",          "sym":"TRX",    "tab":"crypto"},
    {"id":"polygon-ecosystem-token",      "name":"Polygon",       "sym":"POL",    "tab":"crypto"},
    {"id":"uniswap",            "name":"Uniswap",       "sym":"UNI",    "tab":"crypto"},
    {"id":"stellar",            "name":"Stellar",       "sym":"XLM",    "tab":"crypto"},
    {"id":"near",               "name":"Near Protocol", "sym":"NEAR",   "tab":"crypto"},
    {"id":"arbitrum",           "name":"Arbitrum",      "sym":"ARB",    "tab":"crypto"},
    {"id":"aptos",              "name":"Aptos",         "sym":"APT",    "tab":"crypto"},
    {"id":"internet-computer",  "name":"ICP",           "sym":"ICP",    "tab":"crypto"},
    {"id":"filecoin",           "name":"Filecoin",      "sym":"FIL",    "tab":"crypto"},
    {"id":"render-token",       "name":"Render",        "sym":"RENDER", "tab":"crypto"},
    {"id":"injective-protocol", "name":"Injective",     "sym":"INJ",    "tab":"crypto"},
    {"id":"monero",             "name":"Monero",        "sym":"XMR",    "tab":"crypto"},
]

# Yahoo Finance symbols for forex + commodities
YAHOO_ASSETS = [
    # ── Commodities ───────────────────────────────────────────────────────────
    {"yahoo":"GC=F",      "id":"XAUUSD",  "name":"Gold",         "sym":"XAUUSD",  "tab":"oil",   "desc":"Safe haven · Inflation hedge"},
    {"yahoo":"SI=F",      "id":"XAGUSD",  "name":"Silver",       "sym":"XAGUSD",  "tab":"oil",   "desc":"Precious · Industrial"},
    {"yahoo":"CL=F",      "id":"WTI",     "name":"WTI Crude",    "sym":"WTI",     "tab":"oil",   "desc":"US benchmark crude"},
    {"yahoo":"BZ=F",      "id":"BRENT",   "name":"Brent Crude",  "sym":"BRENT",   "tab":"oil",   "desc":"Global benchmark"},
    {"yahoo":"HG=F",      "id":"COPPER",  "name":"Copper",       "sym":"COPPER",  "tab":"oil",   "desc":"Global growth proxy"},
    {"yahoo":"NG=F",      "id":"NATGAS",  "name":"Natural Gas",  "sym":"NATGAS",  "tab":"oil",   "desc":"Energy commodity"},
    {"yahoo":"PL=F",      "id":"XPTUSD",  "name":"Platinum",     "sym":"XPTUSD",  "tab":"oil",   "desc":"Precious metals"},
    # ── Majors ────────────────────────────────────────────────────────────────
    {"yahoo":"DX-Y.NYB",  "id":"DXY",     "name":"DXY Index",    "sym":"DXY",     "tab":"forex", "desc":"US Dollar Index"},
    {"yahoo":"EURUSD=X",  "id":"EURUSD",  "name":"EUR/USD",      "sym":"EURUSD",  "tab":"forex", "desc":"Euro vs Dollar"},
    {"yahoo":"GBPUSD=X",  "id":"GBPUSD",  "name":"GBP/USD",      "sym":"GBPUSD",  "tab":"forex", "desc":"Cable"},
    {"yahoo":"USDJPY=X",  "id":"USDJPY",  "name":"USD/JPY",      "sym":"USDJPY",  "tab":"forex", "desc":"Dollar vs Yen"},
    {"yahoo":"AUDUSD=X",  "id":"AUDUSD",  "name":"AUD/USD",      "sym":"AUDUSD",  "tab":"forex", "desc":"Aussie Dollar"},
    {"yahoo":"USDCAD=X",  "id":"USDCAD",  "name":"USD/CAD",      "sym":"USDCAD",  "tab":"forex", "desc":"Loonie"},
    {"yahoo":"USDCHF=X",  "id":"USDCHF",  "name":"USD/CHF",      "sym":"USDCHF",  "tab":"forex", "desc":"Swissie"},
    {"yahoo":"NZDUSD=X",  "id":"NZDUSD",  "name":"NZD/USD",      "sym":"NZDUSD",  "tab":"forex", "desc":"Kiwi Dollar"},
    # ── Minors (EUR crosses) ──────────────────────────────────────────────────
    {"yahoo":"EURGBP=X",  "id":"EURGBP",  "name":"EUR/GBP",      "sym":"EURGBP",  "tab":"forex", "desc":"Euro vs Pound"},
    {"yahoo":"EURJPY=X",  "id":"EURJPY",  "name":"EUR/JPY",      "sym":"EURJPY",  "tab":"forex", "desc":"Euro vs Yen"},
    {"yahoo":"EURAUD=X",  "id":"EURAUD",  "name":"EUR/AUD",      "sym":"EURAUD",  "tab":"forex", "desc":"Euro vs Aussie"},
    {"yahoo":"EURCAD=X",  "id":"EURCAD",  "name":"EUR/CAD",      "sym":"EURCAD",  "tab":"forex", "desc":"Euro vs Loonie"},
    {"yahoo":"EURCHF=X",  "id":"EURCHF",  "name":"EUR/CHF",      "sym":"EURCHF",  "tab":"forex", "desc":"Euro vs Swissie"},
    {"yahoo":"EURNZD=X",  "id":"EURNZD",  "name":"EUR/NZD",      "sym":"EURNZD",  "tab":"forex", "desc":"Euro vs Kiwi"},
    # ── Minors (GBP crosses) ──────────────────────────────────────────────────
    {"yahoo":"GBPJPY=X",  "id":"GBPJPY",  "name":"GBP/JPY",      "sym":"GBPJPY",  "tab":"forex", "desc":"Pound vs Yen"},
    {"yahoo":"GBPAUD=X",  "id":"GBPAUD",  "name":"GBP/AUD",      "sym":"GBPAUD",  "tab":"forex", "desc":"Pound vs Aussie"},
    {"yahoo":"GBPCAD=X",  "id":"GBPCAD",  "name":"GBP/CAD",      "sym":"GBPCAD",  "tab":"forex", "desc":"Pound vs Loonie"},
    {"yahoo":"GBPCHF=X",  "id":"GBPCHF",  "name":"GBP/CHF",      "sym":"GBPCHF",  "tab":"forex", "desc":"Pound vs Swissie"},
    {"yahoo":"GBPNZD=X",  "id":"GBPNZD",  "name":"GBP/NZD",      "sym":"GBPNZD",  "tab":"forex", "desc":"Pound vs Kiwi"},
    # ── Minors (JPY crosses) ──────────────────────────────────────────────────
    {"yahoo":"AUDJPY=X",  "id":"AUDJPY",  "name":"AUD/JPY",      "sym":"AUDJPY",  "tab":"forex", "desc":"Aussie vs Yen"},
    {"yahoo":"CADJPY=X",  "id":"CADJPY",  "name":"CAD/JPY",      "sym":"CADJPY",  "tab":"forex", "desc":"Loonie vs Yen"},
    {"yahoo":"CHFJPY=X",  "id":"CHFJPY",  "name":"CHF/JPY",      "sym":"CHFJPY",  "tab":"forex", "desc":"Swissie vs Yen"},
    {"yahoo":"NZDJPY=X",  "id":"NZDJPY",  "name":"NZD/JPY",      "sym":"NZDJPY",  "tab":"forex", "desc":"Kiwi vs Yen"},
    # ── Minors (AUD/NZD/CAD crosses) ─────────────────────────────────────────
    {"yahoo":"AUDCAD=X",  "id":"AUDCAD",  "name":"AUD/CAD",      "sym":"AUDCAD",  "tab":"forex", "desc":"Aussie vs Loonie"},
    {"yahoo":"AUDCHF=X",  "id":"AUDCHF",  "name":"AUD/CHF",      "sym":"AUDCHF",  "tab":"forex", "desc":"Aussie vs Swissie"},
    {"yahoo":"AUDNZD=X",  "id":"AUDNZD",  "name":"AUD/NZD",      "sym":"AUDNZD",  "tab":"forex", "desc":"Aussie vs Kiwi"},
    {"yahoo":"NZDCAD=X",  "id":"NZDCAD",  "name":"NZD/CAD",      "sym":"NZDCAD",  "tab":"forex", "desc":"Kiwi vs Loonie"},
    {"yahoo":"NZDCHF=X",  "id":"NZDCHF",  "name":"NZD/CHF",      "sym":"NZDCHF",  "tab":"forex", "desc":"Kiwi vs Swissie"},
    {"yahoo":"CADCHF=X",  "id":"CADCHF",  "name":"CAD/CHF",      "sym":"CADCHF",  "tab":"forex", "desc":"Loonie vs Swissie"},
    # ── Exotics ───────────────────────────────────────────────────────────────
    {"yahoo":"USDTRY=X",  "id":"USDTRY",  "name":"USD/TRY",      "sym":"USDTRY",  "tab":"forex", "desc":"Dollar vs Turkish Lira"},
    {"yahoo":"USDZAR=X",  "id":"USDZAR",  "name":"USD/ZAR",      "sym":"USDZAR",  "tab":"forex", "desc":"Dollar vs Rand"},
    {"yahoo":"USDMXN=X",  "id":"USDMXN",  "name":"USD/MXN",      "sym":"USDMXN",  "tab":"forex", "desc":"Dollar vs Peso"},
    {"yahoo":"USDSEK=X",  "id":"USDSEK",  "name":"USD/SEK",      "sym":"USDSEK",  "tab":"forex", "desc":"Dollar vs Swedish Krona"},
    {"yahoo":"USDNOK=X",  "id":"USDNOK",  "name":"USD/NOK",      "sym":"USDNOK",  "tab":"forex", "desc":"Dollar vs Norwegian Krone"},
    {"yahoo":"USDDKK=X",  "id":"USDDKK",  "name":"USD/DKK",      "sym":"USDDKK",  "tab":"forex", "desc":"Dollar vs Danish Krone"},
    {"yahoo":"USDSGD=X",  "id":"USDSGD",  "name":"USD/SGD",      "sym":"USDSGD",  "tab":"forex", "desc":"Dollar vs Singapore Dollar"},
    {"yahoo":"USDHKD=X",  "id":"USDHKD",  "name":"USD/HKD",      "sym":"USDHKD",  "tab":"forex", "desc":"Dollar vs HK Dollar"},
    {"yahoo":"USDINR=X",  "id":"USDINR",  "name":"USD/INR",      "sym":"USDINR",  "tab":"forex", "desc":"Dollar vs Indian Rupee"},
    {"yahoo":"USDAED=X",  "id":"USDAED",  "name":"USD/AED",      "sym":"USDAED",  "tab":"forex", "desc":"Dollar vs UAE Dirham"},
    {"yahoo":"USDSAR=X",  "id":"USDSAR",  "name":"USD/SAR",      "sym":"USDSAR",  "tab":"forex", "desc":"Dollar vs Saudi Riyal"},
]

# ─── CoinGecko ────────────────────────────────────────────────────────────────

async def fetch_coingecko() -> dict:
    """Use /coins/markets — always returns 7d change reliably."""
    ids = ",".join(a["id"] for a in CRYPTO_ASSETS)
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}

    for attempt in range(3):
        try:
            await asyncio.sleep(attempt * 3)
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    "https://api.coingecko.com/api/v3/coins/markets",
                    params={
                        "vs_currency": "usd",
                        "ids": ids,
                        "order": "market_cap_desc",
                        "per_page": "250",
                        "page": "1",
                        "sparkline": "false",
                        "price_change_percentage": "24h,7d",
                    },
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        # Convert to dict keyed by id
                        result = {}
                        for coin in data:
                            cid = coin.get("id")
                            if not cid: continue
                            result[cid] = {
                                "usd":            coin.get("current_price"),
                                "usd_24h_change": coin.get("price_change_percentage_24h") or 0,
                                "usd_7d_change":  coin.get("price_change_percentage_7d_in_currency") or
                                                  coin.get("price_change_percentage_7d") or 0,
                                "usd_market_cap": coin.get("market_cap"),
                            }
                        print(f"  CoinGecko /markets OK: {len(result)} assets (attempt {attempt+1})")
                        return result
                    elif r.status == 429:
                        print(f"  CoinGecko rate limited, waiting 15s...")
                        await asyncio.sleep(15)
                    else:
                        print(f"  CoinGecko HTTP {r.status} (attempt {attempt+1})")
        except Exception as e:
            print(f"  CoinGecko attempt {attempt+1}: {e}")

    print("  CoinGecko all attempts failed")
    return {}

async def fetch_cg_sparkline(coin_id: str, session: aiohttp.ClientSession) -> list:
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
    try:
        async with session.get(
            f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
            params={"vs_currency": "usd", "days": "7"},
            headers=headers, timeout=TIMEOUT_FAST,
        ) as r:
            if r.status == 200:
                d = await r.json()
                prices = d.get("prices", [])
                return [round(p[1], 8) for p in prices[::4] if p]  # sample to ~40 pts
    except Exception as e:
        print(f"  Sparkline {coin_id}: {e}")
    return []

# ─── Yahoo Finance ────────────────────────────────────────────────────────────

async def yahoo_get_crumb(session: aiohttp.ClientSession) -> str:
    """Get Yahoo Finance crumb token for authenticated requests."""
    try:
        async with session.get(
            "https://query1.finance.yahoo.com/v1/test/getcrumb",
            headers=YAHOO_HEADERS, timeout=TIMEOUT_FAST,
        ) as r:
            if r.status == 200:
                return await r.text()
    except Exception:
        pass
    return ""

async def fetch_yahoo_chart(session: aiohttp.ClientSession, symbol: str) -> dict:
    """Fetch Yahoo Finance chart data with full browser simulation."""
    for base in ["https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com"]:
        try:
            async with session.get(
                f"{base}/v8/finance/chart/{symbol}",
                params={"interval": "1d", "range": "20d", "includePrePost": "false"},
                headers=YAHOO_HEADERS,
                timeout=TIMEOUT_SLOW,
            ) as r:
                if r.status != 200:
                    print(f"    Yahoo {symbol}: HTTP {r.status}")
                    continue
                data = await r.json(content_type=None)
                result = data.get("chart", {}).get("result")
                if not result:
                    continue
                closes = [c for c in (result[0].get("indicators", {})
                          .get("quote", [{}])[0].get("close") or []) if c]
                if len(closes) < 2:
                    continue
                cur, prev = closes[-1], closes[-2]
                w5 = closes[-5] if len(closes) >= 5 else closes[0]
                print(f"    Yahoo {symbol}: ${cur:,.4f}")
                return {
                    "price":    round(cur, 6),
                    "change":   round(((cur - prev) / prev) * 100, 3),
                    "change5d": round(((cur - w5) / w5) * 100, 3),
                    "closes":   [round(c, 6) for c in closes[-20:]],
                }
        except Exception as e:
            print(f"    Yahoo {symbol} error: {e}")
    return {}

# ─── Master Price Loader ──────────────────────────────────────────────────────

async def load_all_prices() -> dict:
    result = {}
    print(f"\n[{datetime.utcnow().strftime('%H:%M:%S')}] === Loading all prices ===")

    # 1. Crypto — CoinGecko (reliable, no issues)
    cg = await fetch_coingecko()
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
        else:
            result[a["id"]] = {**a, "price": None, "change": None, "change5d": None, "closes": []}
    print(f"  Crypto prices: {crypto_ok}/{len(CRYPTO_ASSETS)} loaded")

    # 2. Crypto sparklines — top 8 only (rate limit friendly)
    top8 = ["bitcoin","ethereum","ripple","solana","binancecoin","dogecoin","cardano","avalanche-2"]
    async with aiohttp.ClientSession() as s:
        for cid in top8:
            if result.get(cid, {}).get("price"):
                cl = await fetch_cg_sparkline(cid, s)
                if cl:
                    result[cid]["closes"] = cl
                await asyncio.sleep(0.35)

    # 3. Forex + Commodities — Yahoo Finance
    print("  Fetching Yahoo Finance (forex + commodities)...")
    # Use a single session with cookie jar to maintain session
    jar = aiohttp.CookieJar(unsafe=True)
    connector = aiohttp.TCPConnector(ssl=False, limit=5)
    async with aiohttp.ClientSession(cookie_jar=jar, connector=connector) as session:
        # Warm up the session with a visit to Yahoo Finance first
        try:
            async with session.get(
                "https://finance.yahoo.com",
                headers=BROWSER_HEADERS,
                timeout=TIMEOUT_FAST,
            ) as r:
                print(f"  Yahoo warmup: HTTP {r.status}")
        except Exception as e:
            print(f"  Yahoo warmup failed: {e}")

        # Now fetch all assets
        yahoo_ok = 0
        for a in YAHOO_ASSETS:
            d = await fetch_yahoo_chart(session, a["yahoo"])
            if d.get("price"):
                result[a["id"]] = {**a, **d}
                yahoo_ok += 1
            else:
                result[a["id"]] = {**a, "price": None, "change": None, "change5d": None, "closes": []}
            await asyncio.sleep(0.3)  # polite delay

    print(f"  Forex/Commodities: {yahoo_ok}/{len(YAHOO_ASSETS)} loaded")
    total = sum(1 for v in result.values() if v.get("price"))
    print(f"  === Total: {total}/43 assets loaded ===\n")
    return result



# ─── Real-Time Forex (open.er-api.com — free, no key) ────────────────────────

FOREX_PAIRS = {
    # id -> (base, quote, multiplier)
    "EURUSD": ("EUR","USD",1), "GBPUSD": ("GBP","USD",1),
    "USDJPY": ("USD","JPY",1), "AUDUSD": ("AUD","USD",1),
    "USDCAD": ("USD","CAD",1), "USDCHF": ("USD","CHF",1),
    "NZDUSD": ("NZD","USD",1), "EURGBP": ("EUR","GBP",1),
    "EURJPY": ("EUR","JPY",1), "EURAUD": ("EUR","AUD",1),
    "EURCAD": ("EUR","CAD",1), "EURCHF": ("EUR","CHF",1),
    "EURNZD": ("EUR","NZD",1), "GBPJPY": ("GBP","JPY",1),
    "GBPAUD": ("GBP","AUD",1), "GBPCAD": ("GBP","CAD",1),
    "GBPCHF": ("GBP","CHF",1), "GBPNZD": ("GBP","NZD",1),
    "AUDJPY": ("AUD","JPY",1), "CADJPY": ("CAD","JPY",1),
    "CHFJPY": ("CHF","JPY",1), "NZDJPY": ("NZD","JPY",1),
    "AUDCAD": ("AUD","CAD",1), "AUDCHF": ("AUD","CHF",1),
    "AUDNZD": ("AUD","NZD",1), "NZDCAD": ("NZD","CAD",1),
    "NZDCHF": ("NZD","CHF",1), "CADCHF": ("CAD","CHF",1),
    "USDTRY": ("USD","TRY",1), "USDZAR": ("USD","ZAR",1),
    "USDMXN": ("USD","MXN",1), "USDSEK": ("USD","SEK",1),
    "USDNOK": ("USD","NOK",1), "USDDKK": ("USD","DKK",1),
    "USDSGD": ("USD","SGD",1), "USDHKD": ("USD","HKD",1),
    "USDINR": ("USD","INR",1), "USDAED": ("USD","AED",1),
    "USDSAR": ("USD","SAR",1),
}

_forex_rates_cache = {"data": {}, "ts": 0.0}

async def fetch_forex_rates() -> dict:
    """Fetch all forex rates from open.er-api.com — free, no key, updates every minute."""
    now = time.time()
    if now - _forex_rates_cache["ts"] < 60 and _forex_rates_cache["data"]:
        return _forex_rates_cache["data"]
    bases = set(b for b,q,m in FOREX_PAIRS.values())
    all_rates = {}
    try:
        async with aiohttp.ClientSession() as s:
            for base in bases:
                async with s.get(
                    f"https://open.er-api.com/v6/latest/{base}",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        if d.get("result") == "success":
                            for quote, rate in d.get("rates", {}).items():
                                all_rates[f"{base}{quote}"] = rate
                await asyncio.sleep(0.2)
        _forex_rates_cache.update({"data": all_rates, "ts": time.time()})
        print(f"  Forex rates: {len(all_rates)} pairs loaded")
    except Exception as e:
        print(f"  Forex rates error: {e}")
    return all_rates

async def get_forex_live() -> dict:
    """Return real-time forex prices for all pairs."""
    rates = await fetch_forex_rates()
    result = {}
    for asset_id, (base, quote, _) in FOREX_PAIRS.items():
        key = f"{base}{quote}"
        rate = rates.get(key)
        if not rate:
            continue
        cached = price_cache["data"].get(asset_id, {})
        old    = cached.get("price")
        chg    = ((rate - old) / old * 100) if old and old > 0 else cached.get("change", 0)
        result[asset_id] = {
            "price":  round(rate, 6),
            "change": round(chg, 4),
        }
        if asset_id in price_cache["data"]:
            price_cache["data"][asset_id]["price"]  = round(rate, 6)
            price_cache["data"][asset_id]["change"] = round(chg, 4)
    return result

# ─── Real-Time Commodities (Yahoo Finance v7/spark) ───────────────────────────

COMMODITY_YAHOO = {
    "XAUUSD": "GC=F", "XAGUSD": "SI=F", "WTI": "CL=F",
    "BRENT": "BZ=F",  "COPPER": "HG=F", "NATGAS": "NG=F", "XPTUSD": "PL=F",
    "DXY": "DX-Y.NYB",
}

async def fetch_commodity_live() -> dict:
    """Fetch commodity spot prices using Yahoo Finance spark endpoint."""
    symbols = ",".join(COMMODITY_YAHOO.values())
    result  = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
        "Accept": "application/json",
        "Referer": "https://finance.yahoo.com",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://query1.finance.yahoo.com/v7/finance/spark",
                params={"symbols": symbols, "range": "1d", "interval": "5m"},
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status != 200:
                    print(f"  Yahoo spark HTTP {r.status}")
                    return {}
                data = await r.json(content_type=None)

        spark = data.get("spark", {}).get("result") or []
        for item in spark:
            sym    = item.get("symbol","")
            asset_id = next((k for k,v in COMMODITY_YAHOO.items() if v==sym), None)
            if not asset_id: continue
            closes = item.get("response",[{}])[0].get("indicators",{}).get("quote",[{}])[0].get("close",[])
            closes = [c for c in closes if c]
            if len(closes) < 2: continue
            price = closes[-1]
            open_ = closes[0]
            chg   = ((price - open_) / open_ * 100) if open_ else 0
            result[asset_id] = {"price": round(price,4), "change": round(chg,3)}
            if asset_id in price_cache["data"]:
                price_cache["data"][asset_id]["price"]  = round(price,4)
                price_cache["data"][asset_id]["change"] = round(chg,3)
        print(f"  Commodities: {len(result)}/{len(COMMODITY_YAHOO)} loaded")
    except Exception as e:
        print(f"  Commodity live error: {e}")
    return result

# ─── Phase Detection ──────────────────────────────────────────────────────────

def detect_phase(prices: dict) -> dict:
    changes = [p["change"] for p in prices.values() if p.get("change") is not None]
    if not changes:
        return {"phase":"Unknown","regime":"Neutral","risk":"Balanced","bullPct":50,"bull":0,"bear":0,"neut":0,"avg":"0.00"}
    avg  = sum(changes) / len(changes)
    bull = sum(1 for c in changes if c > 0.5)
    bear = sum(1 for c in changes if c < -0.5)
    rat  = bull / len(changes)
    bpct = round(rat * 100)
    if avg > 2 and rat > .7:    phase,regime,risk = "Bull Run",   "Risk-On",  "Elevated"
    elif avg > 0.5 and rat>.55: phase,regime,risk = "Uptrend",    "Risk-On",  "Moderate"
    elif avg < -2 and rat < .3: phase,regime,risk = "Bear Market","Risk-Off", "Defensive"
    elif avg <-0.5 and rat<.45: phase,regime,risk = "Downtrend",  "Risk-Off", "Cautious"
    else:                        phase,regime,risk = "Consolidation","Neutral","Balanced"
    return {"phase":phase,"regime":regime,"risk":risk,"bullPct":bpct,
            "bull":bull,"bear":bear,"neut":len(changes)-bull-bear,"avg":f"{avg:+.2f}"}

def asset_phase(c, c5) -> str:
    if c is None or c5 is None: return "No Data"
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

# ─── News (server-side RSS proxy) ─────────────────────────────────────────────

NEWS_FEEDS = [
    {"url":"https://cointelegraph.com/rss",                          "type":"crypto"},
    {"url":"https://decrypt.co/feed",                                "type":"crypto"},
    {"url":"https://www.theblock.co/rss.xml",                        "type":"crypto"},
    {"url":"https://oilprice.com/rss/main",                          "type":"commodity"},
    {"url":"https://www.kitco.com/rss/news.xml",                     "type":"commodity"},
    {"url":"https://www.forexlive.com/feed/news",                    "type":"forex"},
    {"url":"https://feeds.reuters.com/reuters/businessNews",         "type":"macro"},
    {"url":"https://feeds.feedburner.com/zerohedge/feed",            "type":"macro"},
    {"url":"https://www.investing.com/rss/news.rss",                 "type":"macro"},
]

def strip_html(t: str) -> str:
    return re.sub(r'<[^>]+>', '', t or '').strip()

async def fetch_one_feed(session: aiohttp.ClientSession, feed: dict) -> list:
    try:
        async with session.get(
            feed["url"],
            headers={"User-Agent": "Mozilla/5.0 (compatible; RSS reader)"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return []
            raw = await r.read()
        root = ET.fromstring(raw)
        items = []
        for el in root.iter("item"):
            title = strip_html(el.findtext("title",""))
            link  = (el.findtext("link") or "").strip()
            if title and link and len(title) > 15:
                items.append({"title": title[:120], "link": link, "type": feed["type"]})
            if len(items) >= 7:
                break
        return items
    except Exception as e:
        print(f"  RSS {feed['url'][:45]}: {e}")
    return []

async def load_news() -> list:
    async with aiohttp.ClientSession() as s:
        results = await asyncio.gather(*[fetch_one_feed(s, f) for f in NEWS_FEEDS], return_exceptions=True)
    items = []
    for r in results:
        if isinstance(r, list):
            items.extend(r)
    print(f"  News: {len(items)} items loaded from {len(NEWS_FEEDS)} feeds")
    random.shuffle(items)
    return items

# ─── API Routes ───────────────────────────────────────────────────────────────


@app.get("/api/forex/live")
async def api_forex_live():
    """Real-time forex + commodity prices — no API key required."""
    forex = await get_forex_live()
    comms = await fetch_commodity_live()
    result = {**forex, **comms}
    if not result:
        # Absolute fallback — return whatever is cached
        for k,v in price_cache["data"].items():
            if v.get("tab") in ("forex","oil") and v.get("price"):
                result[k] = {"price":v["price"],"change":v.get("change",0),"change5d":v.get("change5d",0)}
    return JSONResponse(result)

@app.get("/api/crypto/live")@app.get("/api/crypto/live")
async def api_crypto_live():
    """Fast endpoint for real-time crypto. Uses /coins/markets for reliable 7d data."""
    ids = ",".join(a["id"] for a in CRYPTO_ASSETS)
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
    result = {}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={
                    "vs_currency":                "usd",
                    "ids":                        ids,
                    "order":                      "market_cap_desc",
                    "per_page":                   "250",
                    "page":                       "1",
                    "sparkline":                  "false",
                    "price_change_percentage":    "24h,7d",
                },
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    for coin in data:
                        cid = coin.get("id")
                        price = coin.get("current_price")
                        if not cid or not price:
                            continue
                        chg24  = coin.get("price_change_percentage_24h") or 0
                        chg7d  = coin.get("price_change_percentage_7d_in_currency") or                                  coin.get("price_change_percentage_7d") or 0
                        result[cid] = {
                            "price":    round(float(price), 8),
                            "change":   round(float(chg24), 3),
                            "change5d": round(float(chg7d), 3),
                            "mcap":     coin.get("market_cap"),
                        }
                else:
                    print(f"  /coins/markets HTTP {r.status}")
    except Exception as e:
        print(f"  /coins/markets error: {e}")
        # Fallback to simple/price
        cg = await fetch_coingecko()
        for a in CRYPTO_ASSETS:
            d = cg.get(a["id"], {})
            if d.get("usd"):
                result[a["id"]] = {
                    "price":    round(d["usd"], 8),
                    "change":   round(d.get("usd_24h_change", 0) or 0, 3),
                    "change5d": round(d.get("usd_7d_change",  0) or 0, 3),
                    "mcap":     d.get("usd_market_cap"),
                }

    # Sync into main cache
    if result and price_cache["data"]:
        for cid, data in result.items():
            if cid in price_cache["data"]:
                price_cache["data"][cid].update(data)

    print(f"  /crypto/live: {len(result)} assets returned")
    return JSONResponse(result)

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
    d = price_cache["data"] if price_cache["data"] else await load_all_prices()
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
    return {"status":"ok", "assets_with_price": loaded,
            "cache_age_seconds": round(time.time() - price_cache["ts"]),
            "news_items": len(news_cache["data"])}

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
    # Always ensure prices are loaded
    if not price_cache["data"] or time.time() - price_cache["ts"] > PRICE_TTL:
        data = await load_all_prices()
        price_cache.update({"data": data, "ts": time.time()})

    prices = price_cache["data"]
    a = prices.get(req.asset_id)

    # If still no price, try one more direct fetch
    if not a or not a.get("price"):
        print(f"[ANALYSIS] Price missing for {req.asset_id}, retrying...")
        data = await load_all_prices()
        price_cache.update({"data": data, "ts": time.time()})
        prices = price_cache["data"]
        a = prices.get(req.asset_id)

    if not a:
        raise HTTPException(404, f"Asset not found: {req.asset_id}")
    if not a.get("price"):
        raise HTTPException(503, f"Price still unavailable for {req.asset_id} after retry")
    ph   = detect_phase(prices)
    aph  = asset_phase(a.get("change"), a.get("change5d"))
    ps,cs,c5s = fmt_p(a.get("price")), fmt_c(a.get("change")), fmt_c(a.get("change5d"))
    prompt = f"""You are a senior quantitative analyst combining frameworks from the world's top hedge funds and quant trading desks including macro debt-cycle analysis, statistical momentum and mean-reversion signals, multi-strategy risk-adjusted positioning, factor decomposition (momentum, carry, value, quality), and institutional flow analysis.

LIVE DATA — {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}:
Asset: {a['name']} ({a['sym']}) | Price: {ps} | 24h: {cs} | 5d: {c5s} | Phase: {aph}
Global: {ph['phase']} | {ph['regime']} | {ph['risk']} risk | {ph['bullPct']}% positive | avg {ph['avg']}%

Return ONLY valid JSON no markdown:
{{"quant":{{"momentum":"[Long/Short/Neutral] — signal","meanReversion":"[Overbought/Oversold/Neutral] — signal","macroRegime":"[Risk-On/Risk-Off/Neutral] — context","volRegime":"[Low/Medium/High/Extreme] Vol","conviction":"[High/Medium/Low]","score":"[-10 to +10]"}},"exec":"3 sentences referencing {ps}, phase {aph}, clear directional view.","shortTerm":"3-4 sentences with specific levels near {ps}.","longTerm":"3-4 sentences macro structural view.","narrative":"3-4 sentences on {ph['phase']} phase and {ph['regime']} regime impact.","drivers":["Macro Factor: driver","Momentum: signal","Risk: levels","Factor Model: exposure","Flow: positioning"],"positioning":"3 sentences with conviction, entry zone near {ps}, what invalidates thesis.","assetPhase":"{aph}","globalPhase":"{ph['phase']}","generatedAt":"{datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}"}}"""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    last_error = None
    for attempt in range(4):
        try:
            if attempt > 0:
                wait = [0, 10, 20, 40][attempt]
                print(f"  Anthropic retry {attempt}/3 after {wait}s...")
                await asyncio.sleep(wait)
            msg    = client.messages.create(model="claude-sonnet-4-20250514", max_tokens=1600,
                                             messages=[{"role":"user","content":prompt}])
            parsed = json.loads(msg.content[0].text.strip().replace("```json","").replace("```","").strip())
            analysis_cache[cache_key] = {"data":parsed,"ts":time.time()}
            return JSONResponse(parsed)
        except anthropic.APIStatusError as e:
            last_error = e
            if e.status_code == 529:
                print(f"  Anthropic overloaded (529) — attempt {attempt+1}/4")
                continue
            raise HTTPException(500, f"Analysis error: {e}")
        except Exception as e:
            raise HTTPException(500, f"Analysis error: {e}")
    raise HTTPException(503, f"Anthropic API overloaded — please try again in a moment")

@app.get("/{full_path:path}")
async def serve(full_path: str):
    return FileResponse(DASHBOARD_PATH)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
