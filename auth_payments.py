#!/usr/bin/env python3
"""
Saving Capital — Auth, Payments & Admin module
Drop this file next to server.py and add the import line shown at the bottom.
"""

import asyncio, hashlib, hmac, json, os, re, secrets, time
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
from fastapi import Depends, FastAPI, HTTPException, Request, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ─── Config (set these as Railway env vars) ────────────────────────────────────
JWT_SECRET        = os.environ.get("JWT_SECRET", "CHANGE_ME_IN_RAILWAY")
ADMIN_EMAIL       = os.environ.get("ADMIN_EMAIL", "admin@savingcapital.com")
ADMIN_PASSWORD    = os.environ.get("ADMIN_PASSWORD", "CHANGE_ME_IN_RAILWAY")
NOWPAYMENTS_KEY   = os.environ.get("NOWPAYMENTS_API_KEY", "")
NOWPAYMENTS_IPN   = os.environ.get("NOWPAYMENTS_IPN_SECRET", "")
DATABASE_URL      = os.environ.get("DATABASE_URL", "")  # Railway provides this automatically
APP_URL           = os.environ.get("APP_URL", "https://your-app.railway.app")

# ─── Plan definitions ──────────────────────────────────────────────────────────
PLANS = {
    "basic": {"name": "Basic", "price_usd": 19.00, "features": ["markets"]},
    "pro":   {"name": "Pro",   "price_usd": 49.00, "features": ["markets", "quant", "journal"]},
}

# ─── Simple JWT (no external library needed) ───────────────────────────────────
def _b64url(data: bytes) -> str:
    import base64
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

def _b64url_decode(s: str) -> bytes:
    import base64
    pad = 4 - len(s) % 4
    return base64.urlsafe_b64decode(s + "=" * (pad % 4))

def create_jwt(payload: dict, expires_hours: int = 24 * 30) -> str:
    import json, hmac, hashlib
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload["exp"] = int(time.time()) + expires_hours * 3600
    body = _b64url(json.dumps(payload).encode())
    sig = _b64url(hmac.new(JWT_SECRET.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest())
    return f"{header}.{body}.{sig}"

def verify_jwt(token: str) -> Optional[dict]:
    try:
        import json, hmac, hashlib
        header, body, sig = token.split(".")
        expected = _b64url(hmac.new(JWT_SECRET.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest())
        if not secrets.compare_digest(sig, expected):
            return None
        payload = json.loads(_b64url_decode(body))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None

# ─── Password hashing (no bcrypt needed) ───────────────────────────────────────
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260000)
    return f"{salt}:{h.hex()}"

def verify_password(password: str, stored: str) -> bool:
    try:
        salt, h = stored.split(":", 1)
        check = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260000)
        return secrets.compare_digest(check.hex(), h)
    except Exception:
        return False

# ─── Database (asyncpg) ────────────────────────────────────────────────────────
_db_pool = None

async def get_db():
    global _db_pool
    if _db_pool is None:
        try:
            import asyncpg
            _db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
            await _init_db()
            print("[DB] Connected to PostgreSQL")
        except Exception as e:
            print(f"[DB] Connection failed: {e}")
            raise HTTPException(500, "Database unavailable")
    return _db_pool

async def _init_db():
    pool = _db_pool
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id          SERIAL PRIMARY KEY,
                email       TEXT UNIQUE NOT NULL,
                password    TEXT NOT NULL,
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                banned      BOOLEAN DEFAULT FALSE,
                ban_reason  TEXT
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
                id          SERIAL PRIMARY KEY,
                user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
                plan        TEXT NOT NULL DEFAULT 'basic',
                status      TEXT NOT NULL DEFAULT 'pending',
                started_at  TIMESTAMPTZ,
                expires_at  TIMESTAMPTZ,
                UNIQUE(user_id)
            );
            CREATE TABLE IF NOT EXISTS payments (
                id              SERIAL PRIMARY KEY,
                user_id         INTEGER REFERENCES users(id) ON DELETE CASCADE,
                order_id        TEXT UNIQUE NOT NULL,
                nowpayments_id  TEXT,
                plan            TEXT NOT NULL,
                amount_usd      NUMERIC(10,2),
                pay_currency    TEXT,
                pay_amount      NUMERIC(20,8),
                status          TEXT DEFAULT 'pending',
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                confirmed_at    TIMESTAMPTZ
            );
        """)
    print("[DB] Tables ready")

# ─── Auth helpers ──────────────────────────────────────────────────────────────
async def get_current_user(request: Request) -> Optional[dict]:
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token:
        token = request.cookies.get("sc_token", "")
    if not token:
        return None
    payload = verify_jwt(token)
    if not payload:
        return None
    pool = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT u.id, u.email, u.banned, s.plan, s.status, s.expires_at "
            "FROM users u LEFT JOIN subscriptions s ON s.user_id = u.id "
            "WHERE u.id = $1", payload["user_id"]
        )
    if not row:
        return None
    return dict(row)

async def require_auth(request: Request) -> dict:
    user = await get_current_user(request)
    if not user:
        raise HTTPException(401, "Not authenticated")
    if user["banned"]:
        raise HTTPException(403, "Account suspended. Contact support.")
    return user

async def require_active_sub(request: Request) -> dict:
    user = await require_auth(request)
    expires = user.get("expires_at")
    if user.get("status") != "active" or (expires and expires < datetime.now(timezone.utc)):
        raise HTTPException(402, "Subscription required")
    return user

async def require_pro(request: Request) -> dict:
    user = await require_active_sub(request)
    if user.get("plan") != "pro":
        raise HTTPException(403, "Pro subscription required")
    return user

async def require_admin(request: Request):
    token = request.headers.get("X-Admin-Token", "")
    expected = hash_password.__module__  # dummy — use env var check below
    admin_token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    payload = verify_jwt(admin_token)
    if not payload or not payload.get("is_admin"):
        raise HTTPException(403, "Admin access required")

# ─── Pydantic models ───────────────────────────────────────────────────────────
class SignupBody(BaseModel):
    email: str
    password: str

class LoginBody(BaseModel):
    email: str
    password: str

class CreatePaymentBody(BaseModel):
    plan: str  # "basic" or "pro"
    pay_currency: str = "usdttrc20"  # default USDT

class AdminBanBody(BaseModel):
    user_id: int
    reason: str = ""

class AdminPlanBody(BaseModel):
    user_id: int
    plan: str
    months: int = 1

# ─── Auth endpoints ────────────────────────────────────────────────────────────
async def signup(body: SignupBody):
    email = body.email.lower().strip()
    if not re.match(r"^[^@]+@[^@]+\.[^@]+$", email):
        raise HTTPException(400, "Invalid email address")
    if len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    pool = await get_db()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT id FROM users WHERE email=$1", email)
        if existing:
            raise HTTPException(409, "Email already registered")
        user_id = await conn.fetchval(
            "INSERT INTO users (email, password) VALUES ($1, $2) RETURNING id",
            email, hash_password(body.password)
        )
        await conn.execute(
            "INSERT INTO subscriptions (user_id, plan, status) VALUES ($1, 'basic', 'pending')",
            user_id
        )
    token = create_jwt({"user_id": user_id, "email": email})
    return JSONResponse({"token": token, "email": email, "plan": "basic", "status": "pending"})

async def login(body: LoginBody):
    email = body.email.lower().strip()
    pool = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT u.id, u.email, u.password, u.banned, s.plan, s.status, s.expires_at "
            "FROM users u LEFT JOIN subscriptions s ON s.user_id=u.id WHERE u.email=$1", email
        )
    if not row or not verify_password(body.password, row["password"]):
        raise HTTPException(401, "Invalid email or password")
    if row["banned"]:
        raise HTTPException(403, "Account suspended. Contact support.")
    token = create_jwt({"user_id": row["id"], "email": email})
    expires = row["expires_at"]
    active = row["status"] == "active" and (not expires or expires > datetime.now(timezone.utc))
    return JSONResponse({
        "token": token,
        "email": email,
        "plan": row["plan"] or "basic",
        "status": "active" if active else row["status"] or "pending"
    })

async def admin_login(body: LoginBody):
    if body.email != ADMIN_EMAIL or body.password != ADMIN_PASSWORD:
        raise HTTPException(401, "Invalid admin credentials")
    token = create_jwt({"is_admin": True, "email": body.email}, expires_hours=8)
    return JSONResponse({"token": token})

async def me(request: Request):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"authenticated": False})
    expires = user.get("expires_at")
    active = user.get("status") == "active" and (not expires or expires > datetime.now(timezone.utc))
    return JSONResponse({
        "authenticated": True,
        "email": user["email"],
        "plan": user["plan"] or "basic",
        "status": "active" if active else (user["status"] or "pending"),
        "banned": user["banned"],
    })

# ─── Payment endpoints ─────────────────────────────────────────────────────────
async def create_payment(body: CreatePaymentBody, request: Request):
    user = await require_auth(request)
    if body.plan not in PLANS:
        raise HTTPException(400, "Invalid plan")
    plan = PLANS[body.plan]
    order_id = f"sc_{user['id']}_{body.plan}_{int(time.time())}"

    if not NOWPAYMENTS_KEY:
        raise HTTPException(500, "Payment processor not configured")

    try:
        async with aiohttp.ClientSession() as s:
            resp = await s.post(
                "https://api.nowpayments.io/v1/payment",
                json={
                    "price_amount":    plan["price_usd"],
                    "price_currency":  "usd",
                    "pay_currency":    body.pay_currency,
                    "order_id":        order_id,
                    "order_description": f"Saving Capital {plan['name']} — 1 month",
                    "ipn_callback_url": f"{APP_URL}/payments/webhook",
                    "success_url":     f"{APP_URL}/?payment=success",
                    "cancel_url":      f"{APP_URL}/?payment=cancel",
                },
                headers={"x-api-key": NOWPAYMENTS_KEY, "Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=15)
            )
            data = await resp.json()
    except Exception as e:
        raise HTTPException(502, f"Payment provider error: {e}")

    if "payment_id" not in data:
        raise HTTPException(502, f"NOWPayments error: {data.get('message', 'Unknown error')}")

    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO payments (user_id, order_id, nowpayments_id, plan, amount_usd, pay_currency, status) "
            "VALUES ($1,$2,$3,$4,$5,$6,'pending')",
            user["id"], order_id, str(data["payment_id"]),
            body.plan, plan["price_usd"], body.pay_currency
        )

    return JSONResponse({
        "payment_id":      data["payment_id"],
        "pay_address":     data.get("pay_address"),
        "pay_amount":      data.get("pay_amount"),
        "pay_currency":    data.get("pay_currency"),
        "price_amount":    plan["price_usd"],
        "order_id":        order_id,
        "invoice_url":     data.get("invoice_url", ""),
    })

async def payment_webhook(request: Request):
    body_bytes = await request.body()
    # Verify NOWPayments signature
    if NOWPAYMENTS_IPN:
        sig = request.headers.get("x-nowpayments-sig", "")
        import hmac as _hmac, hashlib as _hs, json as _json
        sorted_body = json.dumps(json.loads(body_bytes), sort_keys=True, separators=(",", ":"))
        expected = _hmac.new(NOWPAYMENTS_IPN.encode(), sorted_body.encode(), _hs.sha512).hexdigest()
        if not secrets.compare_digest(sig, expected):
            raise HTTPException(400, "Invalid signature")

    data = json.loads(body_bytes)
    status = data.get("payment_status", "")
    order_id = data.get("order_id", "")
    nowpayments_id = str(data.get("payment_id", ""))

    pool = await get_db()
    async with pool.acquire() as conn:
        payment = await conn.fetchrow(
            "SELECT * FROM payments WHERE order_id=$1 OR nowpayments_id=$2",
            order_id, nowpayments_id
        )
        if not payment:
            return JSONResponse({"ok": True})

        await conn.execute(
            "UPDATE payments SET status=$1 WHERE id=$2",
            status, payment["id"]
        )

        if status in ("finished", "confirmed", "partially_paid"):
            plan = payment["plan"]
            user_id = payment["user_id"]
            now = datetime.now(timezone.utc)
            expires = now + timedelta(days=31)
            await conn.execute(
                "UPDATE payments SET confirmed_at=$1 WHERE id=$2",
                now, payment["id"]
            )
            await conn.execute(
                """INSERT INTO subscriptions (user_id, plan, status, started_at, expires_at)
                   VALUES ($1,$2,'active',$3,$4)
                   ON CONFLICT (user_id) DO UPDATE
                   SET plan=$2, status='active', started_at=$3, expires_at=$4""",
                user_id, plan, now, expires
            )
            print(f"[PAYMENT] User {user_id} activated {plan} until {expires}")

    return JSONResponse({"ok": True})

async def payment_status(order_id: str, request: Request):
    user = await require_auth(request)
    pool = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, confirmed_at, plan FROM payments WHERE order_id=$1 AND user_id=$2",
            order_id, user["id"]
        )
    if not row:
        raise HTTPException(404, "Payment not found")
    return JSONResponse(dict(row))

# ─── Admin endpoints ───────────────────────────────────────────────────────────
async def admin_get_users(request: Request):
    await require_admin(request)
    pool = await get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT u.id, u.email, u.created_at, u.banned, u.ban_reason, "
            "s.plan, s.status, s.expires_at, "
            "(SELECT COUNT(*) FROM payments p WHERE p.user_id=u.id AND p.status IN ('finished','confirmed')) as paid_count, "
            "(SELECT COALESCE(SUM(amount_usd),0) FROM payments p WHERE p.user_id=u.id AND p.status IN ('finished','confirmed')) as total_paid "
            "FROM users u LEFT JOIN subscriptions s ON s.user_id=u.id "
            "ORDER BY u.created_at DESC"
        )
    users = []
    for r in rows:
        d = dict(r)
        if d.get("created_at"): d["created_at"] = d["created_at"].isoformat()
        if d.get("expires_at"): d["expires_at"] = d["expires_at"].isoformat()
        d["total_paid"] = float(d.get("total_paid") or 0)
        users.append(d)
    return JSONResponse(users)

async def admin_get_stats(request: Request):
    await require_admin(request)
    pool = await get_db()
    async with pool.acquire() as conn:
        total_users   = await conn.fetchval("SELECT COUNT(*) FROM users")
        active_subs   = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE status='active' AND expires_at > NOW()")
        basic_count   = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE plan='basic' AND status='active' AND expires_at > NOW()")
        pro_count     = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE plan='pro' AND status='active' AND expires_at > NOW()")
        total_revenue = await conn.fetchval("SELECT COALESCE(SUM(amount_usd),0) FROM payments WHERE status IN ('finished','confirmed')")
        month_revenue = await conn.fetchval(
            "SELECT COALESCE(SUM(amount_usd),0) FROM payments WHERE status IN ('finished','confirmed') AND created_at > NOW()-INTERVAL '30 days'"
        )
        banned_count  = await conn.fetchval("SELECT COUNT(*) FROM users WHERE banned=TRUE")
    return JSONResponse({
        "total_users":    total_users,
        "active_subs":    active_subs,
        "basic_active":   basic_count,
        "pro_active":     pro_count,
        "total_revenue":  float(total_revenue or 0),
        "month_revenue":  float(month_revenue or 0),
        "banned_users":   banned_count,
    })

async def admin_ban_user(body: AdminBanBody, request: Request):
    await require_admin(request)
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET banned=TRUE, ban_reason=$1 WHERE id=$2",
            body.reason, body.user_id
        )
        await conn.execute(
            "UPDATE subscriptions SET status='suspended' WHERE user_id=$1",
            body.user_id
        )
    return JSONResponse({"ok": True})

async def admin_unban_user(body: AdminBanBody, request: Request):
    await require_admin(request)
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET banned=FALSE, ban_reason=NULL WHERE id=$1",
            body.user_id
        )
        # Restore active sub if payment exists
        await conn.execute(
            "UPDATE subscriptions SET status='active' WHERE user_id=$1 AND expires_at > NOW()",
            body.user_id
        )
    return JSONResponse({"ok": True})

async def admin_set_plan(body: AdminPlanBody, request: Request):
    await require_admin(request)
    if body.plan not in PLANS:
        raise HTTPException(400, "Invalid plan")
    pool = await get_db()
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=31 * body.months)
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO subscriptions (user_id, plan, status, started_at, expires_at)
               VALUES ($1,$2,'active',$3,$4)
               ON CONFLICT (user_id) DO UPDATE SET plan=$2, status='active', started_at=$3, expires_at=$4""",
            body.user_id, body.plan, now, expires
        )
        # Record a manual payment
        await conn.execute(
            "INSERT INTO payments (user_id, order_id, plan, amount_usd, status, confirmed_at) "
            "VALUES ($1,$2,$3,$4,'manual',NOW())",
            body.user_id, f"manual_{body.user_id}_{int(time.time())}", body.plan,
            PLANS[body.plan]["price_usd"] * body.months
        )
    return JSONResponse({"ok": True})

# ─── Route registration helper ─────────────────────────────────────────────────
def register_auth_routes(app: FastAPI):
    """Call this from server.py: register_auth_routes(app)"""
    from fastapi import Body
    app.post("/auth/signup")(signup)
    app.post("/auth/login")(login)
    app.post("/auth/admin/login")(admin_login)
    app.get("/auth/me")(me)
    app.post("/payments/create")(create_payment)
    app.post("/payments/webhook")(payment_webhook)
    app.get("/payments/status/{order_id}")(payment_status)
    app.get("/admin/users")(admin_get_users)
    app.get("/admin/stats")(admin_get_stats)
    app.post("/admin/ban")(admin_ban_user)
    app.post("/admin/unban")(admin_unban_user)
    app.post("/admin/set-plan")(admin_set_plan)
    print("[AUTH] Auth, payment & admin routes registered")
