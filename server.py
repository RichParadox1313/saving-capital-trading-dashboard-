#!/usr/bin/env python3
"""
Saving Capital Market Intelligence Dashboard — Backend Server
Reliable price sources — no extra API keys required beyond Anthropic
"""

import asyncio, json, os, re, time, xml.etree.ElementTree as ET, random
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp, anthropic
from fastapi import FastAPI, HTTPException, Request
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
    {"id":"sui",                 "name":"Sui",           "sym":"SUI",    "tab":"crypto"},
    {"id":"pepe",                "name":"Pepe",          "sym":"PEPE",   "tab":"crypto"},
    {"id":"fetch-ai",            "name":"Fetch.ai",      "sym":"FET",    "tab":"crypto"},
    {"id":"sei-network",         "name":"Sei",           "sym":"SEI",    "tab":"crypto"},
    {"id":"bittensor",           "name":"Bittensor",     "sym":"TAO",    "tab":"crypto"},
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
    # ── Major Indexes ──────────────────────────────────────────────────────────
    {"yahoo":"^GSPC",     "id":"SPX",     "name":"S&P 500",      "sym":"SPX",     "tab":"stocks", "desc":"US Large Cap Index"},
    {"yahoo":"^DJI",      "id":"DJI",     "name":"Dow Jones",    "sym":"DJI",     "tab":"stocks", "desc":"US Blue Chip Index"},
    {"yahoo":"^IXIC",     "id":"NASDAQ",  "name":"NASDAQ",       "sym":"NASDAQ",  "tab":"stocks", "desc":"US Tech Index"},
    {"yahoo":"^RUT",      "id":"RUT",     "name":"Russell 2000", "sym":"RUT",     "tab":"stocks", "desc":"US Small Cap Index"},
    {"yahoo":"^VIX",      "id":"VIX",     "name":"VIX",          "sym":"VIX",     "tab":"stocks", "desc":"Volatility Index"},
    {"yahoo":"^FTSE",     "id":"FTSE",    "name":"FTSE 100",     "sym":"FTSE",    "tab":"stocks", "desc":"UK Index"},
    {"yahoo":"^GDAXI",    "id":"DAX",     "name":"DAX",          "sym":"DAX",     "tab":"stocks", "desc":"German Index"},
    {"yahoo":"^FCHI",     "id":"CAC",     "name":"CAC 40",       "sym":"CAC",     "tab":"stocks", "desc":"French Index"},
    {"yahoo":"^N225",     "id":"NIKKEI",  "name":"Nikkei 225",   "sym":"NIKKEI",  "tab":"stocks", "desc":"Japan Index"},
    {"yahoo":"^HSI",      "id":"HSI",     "name":"Hang Seng",    "sym":"HSI",     "tab":"stocks", "desc":"Hong Kong Index"},
    {"yahoo":"000001.SS", "id":"SSE",     "name":"Shanghai",     "sym":"SSE",     "tab":"stocks", "desc":"China Index"},
    # ── Top 30 Stocks by Market Cap ────────────────────────────────────────────
    {"yahoo":"AAPL",      "id":"AAPL",    "name":"Apple",        "sym":"AAPL",    "tab":"stocks", "desc":"Consumer Tech"},
    {"yahoo":"NVDA",      "id":"NVDA",    "name":"NVIDIA",       "sym":"NVDA",    "tab":"stocks", "desc":"AI & Semiconductors"},
    {"yahoo":"MSFT",      "id":"MSFT",    "name":"Microsoft",    "sym":"MSFT",    "tab":"stocks", "desc":"Cloud & Software"},
    {"yahoo":"GOOGL",     "id":"GOOGL",   "name":"Alphabet",     "sym":"GOOGL",   "tab":"stocks", "desc":"Search & AI"},
    {"yahoo":"AMZN",      "id":"AMZN",    "name":"Amazon",       "sym":"AMZN",    "tab":"stocks", "desc":"E-Commerce & Cloud"},
    {"yahoo":"META",      "id":"META",    "name":"Meta",         "sym":"META",    "tab":"stocks", "desc":"Social Media & AI"},
    {"yahoo":"TSLA",      "id":"TSLA",    "name":"Tesla",        "sym":"TSLA",    "tab":"stocks", "desc":"EV & Energy"},
    {"yahoo":"BRK-B",     "id":"BRK",     "name":"Berkshire",    "sym":"BRK",     "tab":"stocks", "desc":"Conglomerate"},
    {"yahoo":"TSM",       "id":"TSM",     "name":"TSMC",         "sym":"TSM",     "tab":"stocks", "desc":"Semiconductors"},
    {"yahoo":"LLY",       "id":"LLY",     "name":"Eli Lilly",    "sym":"LLY",     "tab":"stocks", "desc":"Pharmaceuticals"},
    {"yahoo":"JPM",       "id":"JPM",     "name":"JPMorgan",     "sym":"JPM",     "tab":"stocks", "desc":"Banking"},
    {"yahoo":"V",         "id":"V",       "name":"Visa",         "sym":"V",       "tab":"stocks", "desc":"Payments"},
    {"yahoo":"XOM",       "id":"XOM",     "name":"ExxonMobil",   "sym":"XOM",     "tab":"stocks", "desc":"Oil & Gas"},
    {"yahoo":"UNH",       "id":"UNH",     "name":"UnitedHealth", "sym":"UNH",     "tab":"stocks", "desc":"Healthcare"},
    {"yahoo":"MA",        "id":"MA",      "name":"Mastercard",   "sym":"MA",      "tab":"stocks", "desc":"Payments"},
    {"yahoo":"JNJ",       "id":"JNJ",     "name":"Johnson & J",  "sym":"JNJ",     "tab":"stocks", "desc":"Healthcare"},
    {"yahoo":"AVGO",      "id":"AVGO",    "name":"Broadcom",     "sym":"AVGO",    "tab":"stocks", "desc":"Semiconductors"},
    {"yahoo":"WMT",       "id":"WMT",     "name":"Walmart",      "sym":"WMT",     "tab":"stocks", "desc":"Retail"},
    {"yahoo":"PG",        "id":"PG",      "name":"Procter & G",  "sym":"PG",      "tab":"stocks", "desc":"Consumer Goods"},
    {"yahoo":"HD",        "id":"HD",      "name":"Home Depot",   "sym":"HD",      "tab":"stocks", "desc":"Home Improvement"},
    {"yahoo":"ORCL",      "id":"ORCL",    "name":"Oracle",       "sym":"ORCL",    "tab":"stocks", "desc":"Enterprise Software"},
    {"yahoo":"COST",      "id":"COST",    "name":"Costco",       "sym":"COST",    "tab":"stocks", "desc":"Wholesale Retail"},
    {"yahoo":"NFLX",      "id":"NFLX",    "name":"Netflix",      "sym":"NFLX",    "tab":"stocks", "desc":"Streaming"},
    {"yahoo":"AMD",       "id":"AMD",     "name":"AMD",          "sym":"AMD",     "tab":"stocks", "desc":"Semiconductors"},
    {"yahoo":"ADBE",      "id":"ADBE",    "name":"Adobe",        "sym":"ADBE",    "tab":"stocks", "desc":"Creative Software"},
    {"yahoo":"CRM",       "id":"CRM",     "name":"Salesforce",   "sym":"CRM",     "tab":"stocks", "desc":"CRM Software"},
    {"yahoo":"BAC",       "id":"BAC",     "name":"Bank of Am",   "sym":"BAC",     "tab":"stocks", "desc":"Banking"},
    {"yahoo":"PEP",       "id":"PEP",     "name":"PepsiCo",      "sym":"PEP",     "tab":"stocks", "desc":"Beverages"},
    {"yahoo":"KO",        "id":"KO",      "name":"Coca-Cola",    "sym":"KO",      "tab":"stocks", "desc":"Beverages"},
    {"yahoo":"BABA",      "id":"BABA",    "name":"Alibaba",      "sym":"BABA",    "tab":"stocks", "desc":"China E-Commerce"},
]

# ─── CoinGecko ────────────────────────────────────────────────────────────────

async def fetch_coingecko() -> dict:
    """Use /coins/markets — always returns 7d change reliably."""
    _ids = [a["id"] for a in CRYPTO_ASSETS]
    # Always request both Polygon IDs — CoinGecko switches between them
    ids = ",".join(_ids)
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
                        "sparkline": "true",
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
                            spark_prices = (coin.get("sparkline_in_7d") or {}).get("price") or []
                            # Sample to 20 points max
                            if len(spark_prices) > 20:
                                step = len(spark_prices) // 20
                                spark_prices = spark_prices[::step][:20]
                            result[cid] = {
                                "usd":            coin.get("current_price"),
                                "usd_24h_change": coin.get("price_change_percentage_24h") or 0,
                                "usd_7d_change":  coin.get("price_change_percentage_7d_in_currency") or
                                                  coin.get("price_change_percentage_7d") or 0,
                                "usd_market_cap": coin.get("market_cap"),
                                "sparkline":      [float(f"{p:.8g}") for p in spark_prices if p is not None],
                            }

                        print(f"  CoinGecko /markets OK: {len(result)} assets (attempt {attempt+1})")

                        pol = result.get("pol-polygon-ecosystem-token")
                        if pol:
                            print(f"  Polygon: price={pol.get('usd')} sparkline_pts={len(pol.get('sparkline',[]))}")
                        else:
                            print(f"  Polygon NOT in result")
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
                    "closes":   [float(f"{c:.8g}") for c in closes[-20:] if c is not None],
                }
        except Exception as e:
            print(f"    Yahoo {symbol} error: {e}")
    return {}

# ─── Master Price Loader ──────────────────────────────────────────────────────

async def load_all_prices() -> dict:
    result = {}
    print(f"\n[{datetime.utcnow().strftime('%H:%M:%S')}] === Loading all prices ===")

    # 1. Crypto — CoinGecko (price + 24h change)
    cg = await fetch_coingecko()
    crypto_ok = 0
    for a in CRYPTO_ASSETS:
        d = cg.get(a["id"]) or {}
        if d.get("usd"):
            result[a["id"]] = {
                **a,
                "price":    round(d["usd"], 8),
                "change":   round(d.get("usd_24h_change", 0) or 0, 3),
                "change5d": round(d.get("usd_7d_change",  0) or 0, 3),
                "mcap":     d.get("usd_market_cap"),
                "closes":   d.get("sparkline", []),
            }
            crypto_ok += 1
        else:
            result[a["id"]] = {**a, "price": None, "change": None, "change5d": None, "closes": []}
    print(f"  Crypto prices: {crypto_ok}/{len(CRYPTO_ASSETS)} loaded")

    # 2. Polygon — Binance klines (multiple endpoints) with CoinGecko market_chart fallback
    pol_price, pol_change, pol_closes, pol_change5d = 0, 0, [], 0
    async with aiohttp.ClientSession() as _bs:
        # Try multiple Binance API endpoints (Railway may block some)
        for binance_base in ["https://api.binance.com", "https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com"]:
            try:
                async with _bs.get(f"{binance_base}/api/v3/ticker/24hr", params={"symbol":"POLUSDT"}, timeout=aiohttp.ClientTimeout(total=6)) as r:
                    if r.status == 200:
                        t = await r.json()
                        pol_price = float(t.get("lastPrice", 0))
                        pol_change = float(t.get("priceChangePercent", 0))
                async with _bs.get(f"{binance_base}/api/v3/klines", params={"symbol":"POLUSDT","interval":"8h","limit":"42"}, timeout=aiohttp.ClientTimeout(total=6)) as r:
                    if r.status == 200:
                        klines = await r.json()
                        closes = [float(f"{float(k[4]):.8g}") for k in klines if k[4]]
                        if closes:
                            pol_closes = closes[-20:]
                            w5 = closes[-30] if len(closes) >= 30 else closes[0]
                            pol_change5d = round(((closes[-1] - w5) / w5) * 100, 3)
                if pol_closes:
                    print(f"  Polygon via {binance_base}: ${pol_price} closes={len(pol_closes)}")
                    break
            except Exception as e:
                print(f"  Polygon {binance_base} failed: {e}")

        # Fallback: CoinGecko market_chart (dedicated endpoint, not bulk — works on free tier)
        if not pol_closes:
            print("  Polygon Binance all failed — trying CoinGecko market_chart...")
            cg_headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
            for cg_id in ("matic-network", "pol-polygon-ecosystem-token"):
                try:
                    async with _bs.get(
                        f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart",
                        params={"vs_currency":"usd","days":"7"},
                        headers=cg_headers,
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as r:
                        if r.status == 200:
                            mc = await r.json()
                            raw = mc.get("prices", [])
                            if raw:
                                step = max(1, len(raw) // 20)
                                pol_closes = [float(f"{p[1]:.8g}") for p in raw[::step][:20] if p[1]]
                                if pol_closes:
                                    if not pol_price: pol_price = pol_closes[-1]
                                    w5 = pol_closes[0]
                                    pol_change5d = round(((pol_closes[-1]-w5)/w5)*100, 3)
                                    print(f"  Polygon CoinGecko market_chart ({cg_id}): {len(pol_closes)} pts")
                                    break
                except Exception as e:
                    print(f"  Polygon CoinGecko {cg_id}: {e}")

    result["pol-polygon-ecosystem-token"] = {
        "id":"pol-polygon-ecosystem-token","name":"Polygon","sym":"POL","tab":"crypto",
        "price":    round(pol_price, 8) if pol_price else None,
        "change":   round(pol_change, 3),
        "change5d": pol_change5d,
        "mcap":     None,
        "closes":   pol_closes,
    }
    print(f"  Polygon final: price={pol_price} closes={len(pol_closes)} change5d={pol_change5d}")

    # 3. Forex + Commodities — Yahoo Finance
    print("  Fetching Yahoo Finance (forex + commodities)...")
    jar = aiohttp.CookieJar(unsafe=True)
    connector = aiohttp.TCPConnector(ssl=False, limit=5)
    async with aiohttp.ClientSession(cookie_jar=jar, connector=connector) as session:
        # Warm up the session
        try:
            async with session.get(
                "https://finance.yahoo.com",
                headers=BROWSER_HEADERS,
                timeout=TIMEOUT_FAST,
            ) as r:
                print(f"  Yahoo warmup: HTTP {r.status}")
        except Exception as e:
            print(f"  Yahoo warmup failed: {e}")

        # Now fetch all other assets
        yahoo_ok = 0
        for a in YAHOO_ASSETS:
            d = await fetch_yahoo_chart(session, a["yahoo"])
            if d.get("price"):
                result[a["id"]] = {**a, **d}
                yahoo_ok += 1
            else:
                result[a["id"]] = {**a, "price": None, "change": None, "change5d": None, "closes": []}
            await asyncio.sleep(0.3)

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
            if v.get("tab") in ("forex","oil","stocks") and v.get("price"):
                result[k] = {"price":v["price"],"change":v.get("change",0),"change5d":v.get("change5d",0)}
    return JSONResponse(result)

@app.get("/api/crypto/live")
async def api_crypto_live():
    """Fast endpoint for real-time crypto. Uses /coins/markets for reliable 7d data."""
    _ids = [a["id"] for a in CRYPTO_ASSETS]
    ids = ",".join(_ids)
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
                        chg7d  = coin.get("price_change_percentage_7d_in_currency") or \
                                 coin.get("price_change_percentage_7d") or 0
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

@app.get("/api/polygon/sparkline")
async def polygon_sparkline():
    """Server-side proxy for Polygon sparkline — tries Binance then CoinGecko."""
    async with aiohttp.ClientSession() as s:
        # Try multiple Binance endpoints
        for base in ["https://api.binance.com", "https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com"]:
            try:
                async with s.get(f"{base}/api/v3/klines", params={"symbol":"POLUSDT","interval":"8h","limit":"42"}, timeout=aiohttp.ClientTimeout(total=6)) as r:
                    if r.status == 200:
                        klines = await r.json()
                        closes = [float(f"{float(k[4]):.8g}") for k in klines if k[4]]
                        if closes:
                            w5 = closes[-30] if len(closes) >= 30 else closes[0]
                            chg5d = round(((closes[-1]-w5)/w5)*100, 3)
                            return JSONResponse({"closes":closes[-20:],"change5d":chg5d,"price":closes[-1]})
            except Exception:
                pass
        # CoinGecko market_chart fallback
        cg_headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
        for cg_id in ("matic-network", "pol-polygon-ecosystem-token"):
            try:
                async with s.get(
                    f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart",
                    params={"vs_currency":"usd","days":"7"},
                    headers=cg_headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status == 200:
                        mc = await r.json()
                        raw = mc.get("prices", [])
                        if raw:
                            step = max(1, len(raw)//20)
                            closes = [float(f"{p[1]:.8g}") for p in raw[::step][:20] if p[1]]
                            if closes:
                                w5 = closes[0]
                                chg5d = round(((closes[-1]-w5)/w5)*100, 3)
                                return JSONResponse({"closes":closes,"change5d":chg5d,"price":closes[-1]})
            except Exception:
                pass
    raise HTTPException(502, "All Polygon data sources failed")

class AnalysisRequest(BaseModel):
    asset_id: str
    lang: str = "en"  # en, bg, he

@app.post("/api/analysis")
async def api_analysis(req: AnalysisRequest):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY not configured")
    today     = datetime.utcnow().strftime("%Y-%m-%d")
    lang      = req.lang if req.lang in ("en","bg","he") else "en"
    cache_key = f"{req.asset_id}_{today}_{lang}"
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
    lang_instruction = ""
    if lang == "bg":
        lang_instruction = "IMPORTANT: Write ALL text fields in Bulgarian (Български). Only keep financial symbols, numbers, and technical indicators in English."
    elif lang == "he":
        lang_instruction = "IMPORTANT: Write ALL text fields in Hebrew (עברית). Only keep financial symbols, numbers, and technical indicators in English."

    prompt = f"""You are a senior quantitative analyst combining frameworks from the world's top hedge funds and quant trading desks including macro debt-cycle analysis, statistical momentum and mean-reversion signals, multi-strategy risk-adjusted positioning, factor decomposition (momentum, carry, value, quality), and institutional flow analysis.

{lang_instruction}

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



# ─── MetaAPI MT5 Connection ───────────────────────────────────────────────────
# MetaAPI cloud service connects to MT5 remotely — no local install needed
# Clients: get your MetaAPI token at metaapi.cloud (free tier available)

META_API_TOKEN = os.environ.get("META_API_TOKEN", "")  # Your MetaAPI token from metaapi.cloud

class MT5ConnectRequest(BaseModel):
    accountId:  str         # MetaAPI account ID (after provisioning)
    login:      str         # MT5 account number
    password:   str         # MT5 password
    server:     str         # MT5 broker server e.g. "ICMarkets-Live"
    broker:     str = ""    # Broker name (display only)
    name:       str = ""    # Client display name

@app.post("/api/mt5/connect")
async def mt5_connect(req: MT5ConnectRequest, request: Request):
    """Provision a MetaAPI MT5 account and sync trades."""
    if not META_API_TOKEN:
        raise HTTPException(500, "META_API_TOKEN not set — add it to Railway environment variables")
    print(f"  MT5 connect: login={req.login} server={req.server} broker={req.broker}")

    headers = {
        "auth-token":   META_API_TOKEN,
        "Content-Type": "application/json",
    }

    # Step 1: Create MetaAPI account
    payload = {
        "login":          req.login,
        "password":       req.password,
        "name":           req.name or f"SC-{req.login}",
        "server":         req.server,
        "platform":       "mt5",
        "magic":          0,
        "application":    "MetaApi",
        "type":           "cloud",
    }

    try:
        import ssl as _ssl
        _ssl_ctx = _ssl.create_default_context()
        _ssl_ctx.check_hostname = False
        _ssl_ctx.verify_mode = _ssl.CERT_NONE
        _connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
        async with aiohttp.ClientSession(connector=_connector) as s:
            # Create account
            async with s.post(
                "https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                body = await r.text()
                if r.status not in (200, 201):
                    print(f"  MT5 connect MetaAPI error: HTTP {r.status} — {body[:500]}")
                    raise HTTPException(400, f"MetaAPI HTTP {r.status}: {body[:300]}")
                data = await r.json()
                account_id = data.get("id")

            if not account_id:
                raise HTTPException(500, "No account ID returned from MetaAPI")

            # Step 2: Wait for connection (poll up to 90s)
            for _ in range(18):
                await asyncio.sleep(5)
                async with s.get(
                    f"https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts/{account_id}",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status == 200:
                        info = await r.json()
                        state = info.get("state","")
                        if state in ("DEPLOYED", "CONNECTED"):
                            break

            # Step 3: Pull trade history
            async with s.get(
                f"https://mt-client-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts/{account_id}/history-deals/time/{{}}/{{}}"
                .format(
                    int((datetime.utcnow() - timedelta(days=30)).timestamp() * 1000),
                    int(datetime.utcnow().timestamp() * 1000),
                ),
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                deals = await r.json() if r.status == 200 else []

            # Step 4: Convert and store trades
            trades = load_journal()
            existing_ids = {t.get("id") for t in trades}
            new_trades = []

            for deal in (deals if isinstance(deals, list) else []):
                deal_id = f"mt5_{deal.get('id',deal.get('ticket',''))}"
                if deal_id in existing_ids:
                    continue
                dtype = deal.get("type","")
                if dtype not in ("DEAL_TYPE_BUY","DEAL_TYPE_SELL"):
                    continue
                direction = "LONG" if dtype == "DEAL_TYPE_BUY" else "SHORT"
                entry_type = deal.get("entryType","")
                if entry_type in ("DEAL_ENTRY_OUT","DEAL_ENTRY_OUT_BY"):
                    direction = "SHORT" if dtype == "DEAL_TYPE_BUY" else "LONG"
                pnl = round((deal.get("profit",0) or 0) + (deal.get("swap",0) or 0) + (deal.get("commission",0) or 0), 2)
                t_str = deal.get("time","")
                date_str = t_str[:10] if t_str else datetime.utcnow().strftime("%Y-%m-%d")
                trade = {
                    "id":        deal_id,
                    "date":      date_str,
                    "asset":     deal.get("symbol",""),
                    "symbol":    deal.get("symbol",""),
                    "direction": direction,
                    "entry":     deal.get("price"),
                    "exit":      None,
                    "size":      deal.get("volume"),
                    "pnl":       pnl,
                    "status":    "OPEN" if entry_type == "DEAL_ENTRY_IN" else "CLOSED",
                    "strategy":  "MT5 Auto",
                    "notes":     f"{req.broker} | Account: {req.login} | {deal.get('comment','')}".strip(" |"),
                    "createdAt": datetime.utcnow().isoformat(),
                }
                new_trades.append(trade)

            # Also pull open positions
            async with s.get(
                f"https://mt-client-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts/{account_id}/positions",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                positions = await r.json() if r.status == 200 else []

            for pos in (positions if isinstance(positions, list) else []):
                pos_id = f"pos_{pos.get('id',pos.get('ticket',''))}"
                ptype = pos.get("type","")
                direction = "LONG" if ptype == "POSITION_TYPE_BUY" else "SHORT"
                pnl = round((pos.get("profit",0) or 0) + (pos.get("swap",0) or 0), 2)
                t_str = pos.get("time","")
                date_str = t_str[:10] if t_str else datetime.utcnow().strftime("%Y-%m-%d")
                trade = {
                    "id":        pos_id,
                    "date":      date_str,
                    "asset":     pos.get("symbol",""),
                    "symbol":    pos.get("symbol",""),
                    "direction": direction,
                    "entry":     pos.get("openPrice"),
                    "exit":      pos.get("currentPrice"),
                    "size":      pos.get("volume"),
                    "pnl":       pnl,
                    "status":    "OPEN",
                    "strategy":  "MT5 Live",
                    "notes":     f"{req.broker} | Account: {req.login}",
                    "createdAt": datetime.utcnow().isoformat(),
                }
                # Update existing open position or add new
                existing = next((i for i,t in enumerate(trades) if t.get("id")==pos_id), None)
                if existing is not None:
                    trades[existing] = trade
                else:
                    new_trades.append(trade)

            all_trades = new_trades + trades
            save_journal(all_trades)

            return JSONResponse({
                "ok":        True,
                "accountId": account_id,
                "synced":    len(new_trades),
                "message":   f"Connected! Synced {len(new_trades)} trades from {req.broker or req.server}",
            })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Connection error: {e}")

@app.get("/api/mt5/sync/{account_id}")
async def mt5_sync(account_id: str):
    """Re-sync trades for an already connected MetaAPI account."""
    if not META_API_TOKEN:
        raise HTTPException(500, "META_API_TOKEN not configured")
    headers = {"auth-token": META_API_TOKEN, "Content-Type": "application/json"}
    try:
        import ssl as _ssl2
        _ssl_ctx2 = _ssl2.create_default_context()
        _ssl_ctx2.check_hostname = False
        _ssl_ctx2.verify_mode = _ssl2.CERT_NONE
        _conn2 = aiohttp.TCPConnector(ssl=_ssl_ctx2)

        from_ms = int((datetime.utcnow() - timedelta(days=90)).timestamp() * 1000)
        to_ms   = int(datetime.utcnow().timestamp() * 1000)

        async with aiohttp.ClientSession(connector=_conn2) as s:
            # Pull closed deal history
            async with s.get(
                f"https://mt-client-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts/{account_id}/history-deals/time/{from_ms}/{to_ms}",
                headers=headers, timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                deals = await r.json() if r.status == 200 else []
                if r.status != 200:
                    print(f"  MT5 sync deals HTTP {r.status}: {await r.text()}")

            # Pull open positions
            async with s.get(
                f"https://mt-client-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts/{account_id}/positions",
                headers=headers, timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                positions = await r.json() if r.status == 200 else []

        trades = load_journal()
        existing_ids = {t.get("id") for t in trades}
        new_count = 0

        # Process deals — both entries and exits
        # MetaAPI returns individual deal legs. We record each deal as a trade row.
        print(f"  MT5 sync: {len(deals) if isinstance(deals,list) else 0} raw deals, {len(positions) if isinstance(positions,list) else 0} positions")
        for deal in (deals if isinstance(deals, list) else []):
            deal_id = f"mt5_{deal.get('id', deal.get('ticket',''))}"
            dtype   = deal.get("type","")
            # Only process buy/sell deals
            if dtype not in ("DEAL_TYPE_BUY","DEAL_TYPE_SELL"): continue
            if deal_id in existing_ids: continue

            entry_type = deal.get("entryType","")
            # DEAL_ENTRY_IN  = opening a position
            # DEAL_ENTRY_OUT = closing a position (has the actual P&L)
            # DEAL_ENTRY_OUT_BY = closed by opposite position
            is_close = entry_type in ("DEAL_ENTRY_OUT","DEAL_ENTRY_OUT_BY")
            direction = "LONG" if dtype == "DEAL_TYPE_BUY" else "SHORT"
            # For closing deals, the direction is the direction being closed
            # i.e. BUY to close means was SHORT, SELL to close means was LONG
            if is_close:
                direction = "SHORT" if dtype == "DEAL_TYPE_BUY" else "LONG"

            sym    = deal.get("symbol","")
            vol    = deal.get("volume",0) or 0
            price  = deal.get("price",0) or 0
            profit = round((deal.get("profit",0) or 0) + (deal.get("swap",0) or 0) + (deal.get("commission",0) or 0), 2)
            t_time = deal.get("time","") or deal.get("brokerTime","")
            status = "CLOSED" if is_close else "OPEN"

            trades.append({
                "id": deal_id, "asset": sym, "symbol": sym, "direction": direction,
                "entry": price, "exit": None if not is_close else price,
                "size": vol, "pnl": profit,
                "date": t_time[:10] if t_time else "",
                "status": status, "strategy": "MT5 Auto",
                "notes": entry_type, "tags": [], "source": "mt5",
                "createdAt": datetime.utcnow().isoformat(),
            })
            existing_ids.add(deal_id)
            new_count += 1

        # Update open positions pnl
        for pos in (positions if isinstance(positions, list) else []):
            pos_id = f"pos_{pos.get('id', pos.get('ticket',''))}"
            pnl = round((pos.get("profit",0) or 0) + (pos.get("swap",0) or 0), 2)
            if pos_id not in existing_ids:
                sym = pos.get("symbol","")
                dtype = pos.get("type","")
                direction = "LONG" if dtype == "POSITION_TYPE_BUY" else "SHORT"
                trades.append({
                    "id": pos_id, "asset": sym, "symbol": sym, "direction": direction,
                    "entry": pos.get("openPrice",0), "exit": pos.get("currentPrice"),
                    "size": pos.get("volume",0), "pnl": pnl,
                    "date": (pos.get("time","") or "")[:10],
                    "status": "OPEN", "strategy": "MT5 Live",
                    "notes": "", "tags": [], "source": "mt5",
                    "createdAt": datetime.utcnow().isoformat(),
                })
                new_count += 1
            else:
                for t in trades:
                    if t.get("id") == pos_id:
                        t["exit"] = pos.get("currentPrice")
                        t["pnl"]  = pnl

        save_journal(trades)
        print(f"  MT5 sync: {new_count} new trades, {len(positions)} open positions")
        return JSONResponse({"ok": True, "new_trades": new_count, "positions": len(positions)})
    except Exception as e:
        print(f"  MT5 sync error: {e}")
        raise HTTPException(500, f"Sync error: {e}")


# ─── Trading Journal ──────────────────────────────────────────────────────────
import json as _json
from pathlib import Path as _Path

# Railway persistent storage: mount a Volume at /data in Railway dashboard
# Without a volume, falls back to /tmp (survives restarts but not redeploys)
_DATA_DIR = _Path("/data") if _Path("/data").exists() else _Path("/tmp")
JOURNAL_FILE = _DATA_DIR / "sc_journal.json"
print(f"  Journal storage: {JOURNAL_FILE}")

JOURNAL_KEY = os.environ.get("JOURNAL_API_KEY", "")

def check_journal_key(request: Request) -> bool:
    """Validate X-Journal-Key header from MT5 EA or return True if no key set."""
    if not JOURNAL_KEY:
        return True  # no key configured — open access
    return request.headers.get("X-Journal-Key","") == JOURNAL_KEY

def load_journal() -> list:
    try:
        if JOURNAL_FILE.exists():
            return _json.loads(JOURNAL_FILE.read_text())
    except Exception as e:
        print(f"Journal load error: {e}")
    return []

def save_journal(trades: list):
    try:
        JOURNAL_FILE.write_text(_json.dumps(trades, indent=2))
    except Exception as e:
        print(f"Journal save error: {e}")

class TradeEntry(BaseModel):
    id: str
    date: str
    asset: str
    direction: str   # LONG / SHORT
    entry: float
    exit: float | None = None
    size: float
    pnl: float | None = None
    status: str      # OPEN / CLOSED
    strategy: str = ""
    notes: str = ""
    createdAt: str

@app.get("/api/journal")
async def get_journal():
    return JSONResponse(load_journal())

@app.post("/api/journal")
async def add_trade(trade: TradeEntry, request: Request):
    if not check_journal_key(request):
        raise HTTPException(401, "Invalid journal API key")
    trades = load_journal()
    # Update existing trade if same ID (e.g. position update from MT5)
    existing = next((i for i,t in enumerate(trades) if t.get("id")==trade.id), None)
    if existing is not None:
        trades[existing] = trade.dict()
    else:
        trades.insert(0, trade.dict())
    save_journal(trades)
    return JSONResponse({"ok": True})

@app.put("/api/journal/{trade_id}")
async def update_trade(trade_id: str, trade: TradeEntry):
    trades = load_journal()
    for i, t in enumerate(trades):
        if t["id"] == trade_id:
            trades[i] = trade.dict()
            save_journal(trades)
            return JSONResponse({"ok": True})
    raise HTTPException(404, "Trade not found")

class TradeAnalysisRequest(BaseModel):
    trade: dict

@app.post("/api/journal/analyse")
async def analyse_trade(req: TradeAnalysisRequest):
    """Analyse a single trade using the same quant frameworks as market intelligence."""
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY not configured")

    trade = req.trade
    asset   = trade.get("asset", "Unknown")
    direction = trade.get("direction", "LONG")
    entry   = trade.get("entry")
    exit_p  = trade.get("exit")
    sl      = trade.get("sl")
    tp      = trade.get("tp")
    size    = trade.get("size")
    pnl     = trade.get("pnl")
    rr      = trade.get("rr")
    strategy = trade.get("strategy", "")
    notes   = trade.get("notes", "")
    psych_tags = trade.get("psychTags", [])
    date    = trade.get("date", "")
    status  = trade.get("status", "CLOSED")

    # Get current live prices for context
    if not price_cache["data"] or time.time() - price_cache["ts"] > PRICE_TTL:
        data = await load_all_prices()
        price_cache.update({"data": data, "ts": time.time()})

    prices = price_cache["data"]
    ph = detect_phase(prices)

    # Format price context
    price_lines = ["CURRENT LIVE MARKET PRICES:"]
    for pid, pdata in list(prices.items())[:12]:
        if pdata.get("price"):
            price_lines.append(f"  {pdata.get('name', pid)}: ${pdata['price']:,.4g}  {pdata.get('change', 0):+.2f}%")
    price_ctx = "\n".join(price_lines)

    # Build detailed trade context
    pnl_str = f"${pnl:+.2f}" if pnl is not None else "Open"
    rr_str  = f"1:{rr:.2f}" if rr else ("N/A (no SL/TP set)" if not sl else "N/A")
    sl_str  = f"${float(sl):,.4g}" if sl else "Not set"
    tp_str  = f"${float(tp):,.4g}" if tp else "Not set"
    entry_str = f"${float(entry):,.4g}" if entry else "N/A"
    exit_str  = f"${float(exit_p):,.4g}" if exit_p else "Still open"
    size_str  = f"${float(size):,.0f}" if size else "N/A"
    psych_str = ", ".join(psych_tags) if psych_tags else "None tagged"

    prompt = f"""You are a senior quantitative analyst and trading coach at Saving Capital, a professional trading academy.
Analyse this trade using the same institutional frameworks (Bridgewater macro, RenTech momentum, Citadel risk management, Two Sigma factor models) used in our market intelligence system.

{price_ctx}

GLOBAL MARKET REGIME: {ph['phase']} | {ph['regime']} | {ph['risk']} risk
{ph['bullPct']}% of tracked assets positive | Average move: {ph['avg']}%

TRADE TO ANALYSE:
Asset: {asset}
Direction: {direction}
Date: {date}
Status: {status}
Entry: {entry_str}
Exit: {exit_str}
Stop Loss: {sl_str}
Take Profit: {tp_str}
Position Size: {size_str}
P&L: {pnl_str}
Risk:Reward Ratio: {rr_str}
Strategy/Setup: {strategy or 'Not specified'}
Notes: {notes or 'None'}
Psychology Tags: {psych_str}

Return ONLY valid JSON (no markdown, no backticks):
{{
  "verdict": "2-3 sentence overall assessment of this trade — was it a good trade regardless of outcome? Reference the specific price levels.",
  "verdict_positive": true or false (true if trade was well-executed even if it lost, false if it was a bad trade even if it won),
  "strengths": "What the trader did well — specific to this trade (entry timing, risk management, setup selection, discipline). Be specific, mention actual prices.",
  "weaknesses": "What could be improved — concrete and actionable. If SL/TP not set, flag it. If psychology tags suggest emotional trading, address it directly.",
  "market_context": "What the market was doing at {date} for {asset} based on the regime data and current price context. How did macro conditions affect this trade?",
  "risk_management": "Assessment of the risk management: position sizing relative to account, SL placement, RR ratio quality. Be direct — good RR or not?",
  "psychology": "{f'The trader tagged: {psych_str}. ' if psych_tags else ''}Assess the psychological state during this trade and how it likely affected decision-making.",
  "lesson": "Single most important lesson from this trade — one punchy sentence that the trader should remember.",
  "generated_at": "{datetime.utcnow().strftime('%d %b %Y %H:%M UTC')}"
}}"""

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    for attempt in range(3):
        try:
            if attempt > 0:
                await asyncio.sleep(10 * attempt)
            msg = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1200,
                messages=[{"role": "user", "content": prompt}]
            )
            text = msg.content[0].text.strip()
            text = text.replace("```json", "").replace("```", "").strip()
            result = json.loads(text)
            return JSONResponse(result)
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < 2:
                continue
            raise HTTPException(500, f"AI error: {e}")
        except json.JSONDecodeError as e:
            raise HTTPException(500, f"JSON parse error: {e}")
        except Exception as e:
            raise HTTPException(500, f"Analysis error: {e}")
    raise HTTPException(503, "Service overloaded, try again")


@app.delete("/api/journal/{trade_id}")
async def delete_trade(trade_id: str):
    trades = load_journal()
    trades = [t for t in trades if t["id"] != trade_id]
    save_journal(trades)
    return JSONResponse({"ok": True})

# ─── Backtesting Data API ─────────────────────────────────────────────────────

# Map dashboard asset IDs to Yahoo Finance symbols
BACKTEST_YAHOO_MAP = {a["id"]: a["yahoo"] for a in YAHOO_ASSETS}

# Map dashboard crypto IDs to Binance symbols
BACKTEST_BINANCE_MAP = {
    "bitcoin":"BTCUSDT","ethereum":"ETHUSDT","ripple":"XRPUSDT",
    "solana":"SOLUSDT","binancecoin":"BNBUSDT","dogecoin":"DOGEUSDT",
    "cardano":"ADAUSDT","avalanche-2":"AVAXUSDT","chainlink":"LINKUSDT",
    "polkadot":"DOTUSDT","the-open-network":"TONUSDT","shiba-inu":"SHIBUSDT",
    "litecoin":"LTCUSDT","tron":"TRXUSDT","pol-polygon-ecosystem-token":"POLUSDT",
    "uniswap":"UNIUSDT","stellar":"XLMUSDT","near":"NEARUSDT",
    "arbitrum":"ARBUSDT","aptos":"APTUSDT","internet-computer":"ICPUSDT",
    "filecoin":"FILUSDT","render-token":"RENDERUSDT","injective-protocol":"INJUSDT",
    "monero":"XMRUSDT","sui":"SUIUSDT","pepe":"PEPEUSDT",
    "fetch-ai":"FETUSDT","sei-network":"SEIUSDT","bittensor":"TAOUSDT",
}

BINANCE_INTERVAL_MAP = {"M1":"1m","M5":"5m","M15":"15m","M30":"30m","H1":"1h","H4":"4h","D1":"1d","W1":"1w"}
YAHOO_INTERVAL_MAP  = {"M1":"1m","M5":"5m","M15":"15m","M30":"30m","H1":"1h","H4":"1h","D1":"1d","W1":"1wk"}

@app.get("/api/backtest/data")
async def backtest_data(symbol: str, interval: str = "H1", from_date: str = "", to_date: str = "", cgid: str = "", src: str = ""):
    """
    Fetch OHLCV candle data for backtesting.
    - Crypto (BTCUSDT etc): called from browser via Binance directly — this endpoint used as fallback only
    - Non-crypto: called from browser with src=stooq, server proxies Stooq CSV
    """
    import time as _time
    sym = symbol.strip().upper()
    tf  = interval.strip().upper()

    # Date range
    now_ts  = int(_time.time())
    to_ts   = int(datetime.strptime(to_date,   "%Y-%m-%d").timestamp()) if to_date   else now_ts
    from_ts = int(datetime.strptime(from_date, "%Y-%m-%d").timestamp()) if from_date else to_ts - 730*86400

    # ── Stooq path (called from browser for forex/stocks/indexes/commodities) ──
    if src == "stooq" or (sym not in {
        "BTCUSDT","ETHUSDT","XRPUSDT","SOLUSDT","BNBUSDT","DOGEUSDT","ADAUSDT",
        "AVAXUSDT","LINKUSDT","DOTUSDT","TONUSDT","SHIBUSDT","LTCUSDT","TRXUSDT",
        "POLUSDT","UNIUSDT","XLMUSDT","NEARUSDT","ARBUSDT","APTUSDT","SUIUSDT",
        "PEPEUSDT","ICPUSDT","XMRUSDT","FILUSDT","RENDERUSDT","INJUSDT",
    } and not cgid):
        # symbol here is already the Stooq symbol (e.g. eurusd, xauusd, ^spx, msft.us)
        # passed directly from btFetchStooq via btAsset.st
        st_sym = sym.lower()
        d1 = datetime.utcfromtimestamp(from_ts).strftime("%Y%m%d")
        d2 = datetime.utcfromtimestamp(to_ts).strftime("%Y%m%d")
        url = f"https://stooq.com/q/d/l/?s={st_sym}&d1={d1}&d2={d2}&i=d"
        candles = []
        try:
            jar = aiohttp.CookieJar(unsafe=True)
            async with aiohttp.ClientSession(cookie_jar=jar) as s:
                async with s.get(url, headers=YAHOO_HEADERS, timeout=TIMEOUT_SLOW) as r:
                    if r.status != 200:
                        return JSONResponse({"error": f"Stooq HTTP {r.status} for {st_sym}"}, status_code=502)
                    text = await r.text()
                    lines = text.strip().split("\n")
                    if len(lines) < 2 or "No data" in text or text.startswith("<"):
                        return JSONResponse({"error": f"No Stooq data for {st_sym}"}, status_code=404)
                    for line in lines[1:]:
                        cols = line.strip().split(",")
                        if len(cols) < 5: continue
                        date_str, o, h, l, c = cols[0], cols[1], cols[2], cols[3], cols[4]
                        v = cols[5] if len(cols) > 5 else "0"
                        if not date_str or not c or c in ("null","N/A",""): continue
                        try:
                            ts = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())
                            candles.append({
                                "time": ts, "open": float(o), "high": float(h),
                                "low": float(l), "close": float(c), "volume": float(v or 0),
                            })
                        except: continue
        except Exception as e:
            return JSONResponse({"error": f"Stooq error: {e}"}, status_code=500)

        if not candles:
            return JSONResponse({"error": f"No data returned from Stooq for {st_sym}"}, status_code=404)
        candles.sort(key=lambda x: x["time"])
        print(f"  BT Stooq {st_sym}: {len(candles)} candles")
        return JSONResponse({"source": "stooq", "symbol": sym, "interval": tf, "candles": candles[-5000:]})

    # ── Binance path (fallback if browser Binance call failed) ───────────────
    BINANCE_SYMBOLS = {
        "BTCUSDT","ETHUSDT","XRPUSDT","SOLUSDT","BNBUSDT","DOGEUSDT","ADAUSDT",
        "AVAXUSDT","LINKUSDT","DOTUSDT","TONUSDT","SHIBUSDT","LTCUSDT","TRXUSDT",
        "POLUSDT","UNIUSDT","XLMUSDT","NEARUSDT","ARBUSDT","APTUSDT","SUIUSDT",
        "PEPEUSDT","ICPUSDT","XMRUSDT","FILUSDT","RENDERUSDT","INJUSDT",
    }
    SYM_TO_CGID = {
        "BTCUSDT":"bitcoin","ETHUSDT":"ethereum","XRPUSDT":"ripple","SOLUSDT":"solana",
        "BNBUSDT":"binancecoin","DOGEUSDT":"dogecoin","ADAUSDT":"cardano","AVAXUSDT":"avalanche-2",
        "LINKUSDT":"chainlink","DOTUSDT":"polkadot","TONUSDT":"the-open-network","SHIBUSDT":"shiba-inu",
        "LTCUSDT":"litecoin","TRXUSDT":"tron","POLUSDT":"pol-polygon-ecosystem-token","UNIUSDT":"uniswap",
        "XLMUSDT":"stellar","NEARUSDT":"near","ARBUSDT":"arbitrum","APTUSDT":"aptos","SUIUSDT":"sui",
        "PEPEUSDT":"pepe","ICPUSDT":"internet-computer","XMRUSDT":"monero","FILUSDT":"filecoin",
    }
    cg_id = cgid.strip().lower() or SYM_TO_CGID.get(sym, "")

    TF_MAX_DAYS = {"M1":7,"M5":30,"M15":90,"M30":180,"H1":730,"H4":1460,"D1":3650,"W1":3650}
    max_days = TF_MAX_DAYS.get(tf, 730)
    if (to_ts - from_ts) > max_days * 86400:
        from_ts = to_ts - max_days * 86400

    B_INT = {"M1":"1m","M5":"5m","M15":"15m","M30":"30m","H1":"1h","H4":"4h","D1":"1d","W1":"1w"}
    candles = []

    # Try Binance
    if sym in BINANCE_SYMBOLS:
        try:
            from_ms = from_ts * 1000
            to_ms   = to_ts   * 1000
            b_int   = B_INT.get(tf, "1h")
            async with aiohttp.ClientSession() as s:
                while from_ms < to_ms and len(candles) < 5000:
                    async with s.get(
                        "https://api.binance.com/api/v3/klines",
                        params={"symbol":sym,"interval":b_int,"startTime":from_ms,"endTime":to_ms,"limit":1000},
                        timeout=aiohttp.ClientTimeout(total=12),
                    ) as r:
                        text = await r.text()
                        if r.status != 200 or not text or text.lstrip().startswith("<"): break
                        rows = json.loads(text)
                        if not rows or not isinstance(rows, list): break
                        for k in rows:
                            candles.append({"time":int(k[0])//1000,"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),"close":float(k[4]),"volume":float(k[5])})
                        if len(rows) < 1000: break
                        from_ms = int(rows[-1][0]) + 1
            if candles:
                print(f"  BT Binance {sym}: {len(candles)} candles")
                return JSONResponse({"source":"binance","symbol":sym,"interval":tf,"candles":candles[-5000:]})
        except Exception as e:
            print(f"  BT Binance failed: {e}")

    # Try CoinGecko fallback
    if cg_id:
        try:
            days = max(30, (to_ts - from_ts) // 86400)
            days_p = "max" if days > 365 else str(days + 5)
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.coingecko.com/api/v3/coins/{cg_id}/ohlc",
                    params={"vs_currency":"usd","days":days_p},
                    headers={"Accept":"application/json","User-Agent":USER_AGENT},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as r:
                    if r.status == 200:
                        text = await r.text()
                        if text and not text.lstrip().startswith("<"):
                            rows = json.loads(text)
                            if isinstance(rows, list):
                                for row in rows:
                                    if len(row) < 5: continue
                                    ts = int(row[0]) // 1000
                                    if ts < from_ts or ts > to_ts: continue
                                    candles.append({"time":ts,"open":float(row[1]),"high":float(row[2]),"low":float(row[3]),"close":float(row[4]),"volume":0.0})
                                if candles:
                                    print(f"  BT CoinGecko {cg_id}: {len(candles)} candles")
                                    return JSONResponse({"source":"coingecko","symbol":sym,"interval":tf,"candles":candles[-5000:]})
        except Exception as e:
            print(f"  BT CoinGecko failed: {e}")

    return JSONResponse({"error": f"No data found for {sym}"}, status_code=404)



@app.get("/api/journal/debug")
async def journal_debug():
    """Show journal storage info for debugging."""
    trades = load_journal()
    return JSONResponse({
        "file": str(JOURNAL_FILE),
        "exists": JOURNAL_FILE.exists(),
        "trade_count": len(trades),
        "trades_sample": trades[:3],
        "data_dir_exists": _Path("/data").exists(),
        "tmp_journal": (_Path("/tmp") / "sc_journal.json").exists(),
    })

@app.get("/api/mt5/debug/{account_id}")
async def mt5_debug(account_id: str):
    """Return raw MetaAPI data for debugging."""
    if not META_API_TOKEN:
        return JSONResponse({"error": "no token"})
    headers = {"auth-token": META_API_TOKEN}
    import ssl as _ssl3
    ctx = _ssl3.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=_ssl3.CERT_NONE
    from_ms = int((datetime.utcnow() - timedelta(days=90)).timestamp() * 1000)
    to_ms   = int(datetime.utcnow().timestamp() * 1000)
    result = {}
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ctx)) as s:
        # Account info
        async with s.get(f"https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts/{account_id}",
                         headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            result["account"] = await r.json() if r.status==200 else {"error": r.status}
        # Deals
        async with s.get(f"https://mt-client-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts/{account_id}/history-deals/time/{from_ms}/{to_ms}",
                         headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
            deals = await r.json() if r.status==200 else []
            result["deals_count"] = len(deals) if isinstance(deals,list) else 0
            result["deals_sample"] = deals[:3] if isinstance(deals,list) else deals
            result["deals_http"] = r.status
        # Positions
        async with s.get(f"https://mt-client-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts/{account_id}/positions",
                         headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            pos = await r.json() if r.status==200 else []
            result["positions_count"] = len(pos) if isinstance(pos,list) else 0
            result["positions"] = pos
            result["positions_http"] = r.status
    return JSONResponse(result)

@app.get("/img/{filename}")
async def serve_img(filename: str):
    import re
    if not re.match(r'^[\w\-]+\.(jpg|png|webp)$', filename):
        raise HTTPException(404, "Not found")
    p = Path(__file__).parent / filename
    if not p.exists():
        raise HTTPException(404, "Image not found")
    return FileResponse(p, headers={"Cache-Control": "public, max-age=86400"})

@app.get("/{full_path:path}")
async def serve(full_path: str):
    return FileResponse(DASHBOARD_PATH)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
