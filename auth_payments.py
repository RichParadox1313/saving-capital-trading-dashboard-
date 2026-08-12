#!/usr/bin/env python3
"""
Saving Capital — Auth, Payments & Admin (Production-hardened)
"""

import asyncio, base64, hashlib, hmac as _hmac_mod, json, logging, os, re, secrets, time
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

log = logging.getLogger("sc_auth")
logging.basicConfig(level=logging.INFO)

# ─── Config ────────────────────────────────────────────────────────────────────
JWT_SECRET       = os.environ.get("JWT_SECRET", "")
ADMIN_EMAIL      = os.environ.get("ADMIN_EMAIL", "").lower().strip()
ADMIN_PASSWORD   = os.environ.get("ADMIN_PASSWORD", "")
NOWPAYMENTS_KEY  = os.environ.get("NOWPAYMENTS_API_KEY", "")
NOWPAYMENTS_IPN  = os.environ.get("NOWPAYMENTS_IPN_SECRET", "")
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
APP_URL          = os.environ.get("APP_URL", "https://your-app.railway.app").rstrip("/")

if not JWT_SECRET:
    raise RuntimeError("JWT_SECRET env var is not set. Add it in Railway → Variables.")

# ─── Plans ─────────────────────────────────────────────────────────────────────
PLANS = {
    "basic": {"name": "Basic", "price_usd": 19.00, "features": ["markets"]},
    "pro":   {"name": "Pro",   "price_usd": 49.00, "features": ["markets", "quant", "journal"]},
}

# ─── Rate limiting ─────────────────────────────────────────────────────────────
_rate_store: dict = {}
_rate_lock = asyncio.Lock()

async def _check_rate(ip: str, bucket: str, limit: int, window: int) -> bool:
    key = f"{bucket}:{ip}"
    now = time.time()
    async with _rate_lock:
        entry = _rate_store.get(key)
        if entry:
            if entry.get("lockout_until", 0) > now:
                return False
            if now - entry["window_start"] > window:
                _rate_store[key] = {"count": 1, "window_start": now, "lockout_until": 0}
                return True
            if entry["count"] >= limit:
                entry["lockout_until"] = now + 900  # 15-min lockout after abuse
                return False
            entry["count"] += 1
        else:
            _rate_store[key] = {"count": 1, "window_start": now, "lockout_until": 0}
    return True

def _get_ip(request: Request) -> str:
    fwd = request.headers.get("X-Forwarded-For", "")
    return fwd.split(",")[0].strip() or (request.client.host if request.client else "unknown")

# ─── JWT ───────────────────────────────────────────────────────────────────────
def _b64url_enc(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

def _b64url_dec(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))

def create_jwt(payload: dict, expires_hours: int = 24 * 30) -> str:
    header = _b64url_enc(json.dumps({"alg":"HS256","typ":"JWT"}, separators=(",",":")).encode())
    body   = _b64url_enc(json.dumps({**payload, "exp": int(time.time()) + expires_hours*3600, "iat": int(time.time())}, separators=(",",":")).encode())
    sig    = _b64url_enc(_hmac_mod.new(JWT_SECRET.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest())
    return f"{header}.{body}.{sig}"

def verify_jwt(token: str) -> Optional[dict]:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header, body, sig = parts
        expected = _b64url_enc(_hmac_mod.new(JWT_SECRET.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest())
        if not secrets.compare_digest(sig, expected):
            return None
        payload = json.loads(_b64url_dec(body))
        return payload if payload.get("exp", 0) > time.time() else None
    except Exception:
        return None

# ─── Password hashing ──────────────────────────────────────────────────────────
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 320000)
    return f"v2:{salt}:{h.hex()}"

def verify_password(password: str, stored: str) -> bool:
    try:
        if stored.startswith("v2:"):
            _, salt, h = stored.split(":", 2)
            iters = 320000
        else:
            salt, h = stored.split(":", 1)
            iters = 260000
        check = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), iters)
        return secrets.compare_digest(check.hex(), h)
    except Exception:
        return False

# ─── Database pool ─────────────────────────────────────────────────────────────
_db_pool   = None
_db_lock   = asyncio.Lock()

async def get_db():
    global _db_pool
    if _db_pool is not None:
        return _db_pool
    async with _db_lock:
        if _db_pool is not None:
            return _db_pool
        if not DATABASE_URL:
            raise HTTPException(500, "DATABASE_URL not configured — add PostgreSQL in Railway")
        try:
            import asyncpg
            _db_pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=3,
                max_size=20,
                max_inactive_connection_lifetime=300,
                command_timeout=30,
                statement_cache_size=0,   # required for Railway's PgBouncer
            )
            await _init_schema()
            log.info("[DB] Pool ready (min=3, max=20)")
        except Exception as e:
            log.error(f"[DB] Failed: {e}")
            raise HTTPException(500, "Database unavailable")
    return _db_pool

async def _init_schema():
    async with _db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id          SERIAL PRIMARY KEY,
                email       TEXT UNIQUE NOT NULL,
                password    TEXT NOT NULL,
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                last_login  TIMESTAMPTZ,
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
            CREATE INDEX IF NOT EXISTS idx_users_email       ON users(email);
            CREATE INDEX IF NOT EXISTS idx_subs_user        ON subscriptions(user_id);
            CREATE INDEX IF NOT EXISTS idx_subs_status_exp  ON subscriptions(status, expires_at);
            CREATE INDEX IF NOT EXISTS idx_pay_order        ON payments(order_id);
            CREATE INDEX IF NOT EXISTS idx_pay_user         ON payments(user_id);
            CREATE INDEX IF NOT EXISTS idx_pay_np           ON payments(nowpayments_id);
        """)
    log.info("[DB] Schema and indexes ready")

async def close_db():
    global _db_pool
    if _db_pool:
        await _db_pool.close()
        _db_pool = None

# ─── Auth helpers ──────────────────────────────────────────────────────────────
def _is_active(user: dict) -> bool:
    exp = user.get("expires_at")
    return user.get("status") == "active" and (exp is None or exp > datetime.now(timezone.utc))

async def get_current_user(request: Request) -> Optional[dict]:
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token:
        token = request.cookies.get("sc_token", "")
    if not token:
        return None
    payload = verify_jwt(token)
    if not payload:
        return None
    if payload.get("is_owner"):
        return {
            "id": 0, "email": payload.get("email", ADMIN_EMAIL), "banned": False,
            "plan": "pro", "status": "active", "expires_at": None, "is_owner": True,
        }
    if "user_id" not in payload:
        return None
    try:
        pool = await get_db()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT u.id, u.email, u.banned, s.plan, s.status, s.expires_at "
                "FROM users u LEFT JOIN subscriptions s ON s.user_id=u.id WHERE u.id=$1",
                payload["user_id"]
            )
        return dict(row) if row else None
    except Exception as e:
        log.error(f"[AUTH] get_current_user: {e}")
        return None

async def require_auth(request: Request) -> dict:
    user = await get_current_user(request)
    if not user:
        raise HTTPException(401, "Not authenticated")
    if user["banned"]:
        raise HTTPException(403, "Account suspended. Contact support.")
    return user

async def require_admin(request: Request):
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    payload = verify_jwt(token)
    if not payload or not payload.get("is_admin"):
        raise HTTPException(403, "Admin access required")
    ip = _get_ip(request)
    if not await _check_rate(ip, "admin_api", 300, 3600):
        raise HTTPException(429, "Rate limit exceeded")

# ─── Models ────────────────────────────────────────────────────────────────────
class SignupBody(BaseModel):
    email: str
    password: str

class LoginBody(BaseModel):
    email: str
    password: str

class CreatePaymentBody(BaseModel):
    plan: str
    pay_currency: str = "usdttrc20"

class AdminBanBody(BaseModel):
    user_id: int
    reason: str = ""

class AdminPlanBody(BaseModel):
    user_id: int
    plan: str
    months: int = 1

# ─── Auth endpoints ────────────────────────────────────────────────────────────
async def signup(body: SignupBody, request: Request):
    ip = _get_ip(request)
    if not await _check_rate(ip, "signup", 5, 3600):
        raise HTTPException(429, "Too many signup attempts. Try again in 1 hour.")

    email = body.email.lower().strip()
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$", email):
        raise HTTPException(400, "Invalid email address")
    if len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    if len(body.password) > 128:
        raise HTTPException(400, "Password too long")

    pool = await get_db()
    async with pool.acquire() as conn:
        if await conn.fetchval("SELECT id FROM users WHERE email=$1", email):
            raise HTTPException(409, "An account with this email already exists")
        user_id = await conn.fetchval(
            "INSERT INTO users (email, password) VALUES ($1,$2) RETURNING id",
            email, hash_password(body.password)
        )
        await conn.execute(
            "INSERT INTO subscriptions (user_id, plan, status) VALUES ($1,'basic','pending')",
            user_id
        )

    log.info(f"[SIGNUP] #{user_id} {email}")
    token = create_jwt({"user_id": user_id, "email": email})
    return JSONResponse({"token": token, "email": email, "plan": "basic", "status": "pending"})

async def login(body: LoginBody, request: Request):
    ip = _get_ip(request)
    if not await _check_rate(ip, "login", 10, 900):
        raise HTTPException(429, "Too many login attempts. Try again in 15 minutes.")

    email = body.email.lower().strip()

    # Owner login — same credentials as the admin panel, grants full Pro access
    # to the dashboard itself without needing a DB user row.
    if ADMIN_EMAIL and email == ADMIN_EMAIL and body.password == ADMIN_PASSWORD:
        token = create_jwt({"is_owner": True, "email": email})
        log.info(f"[OWNER] Login from {ip}")
        return JSONResponse({"token": token, "email": email, "plan": "pro", "status": "active", "is_owner": True})

    pool  = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT u.id, u.email, u.password, u.banned, s.plan, s.status, s.expires_at "
            "FROM users u LEFT JOIN subscriptions s ON s.user_id=u.id WHERE u.email=$1", email
        )

    # Always verify password even on miss — prevents email enumeration
    ok = verify_password(body.password, row["password"] if row else "v2:x:x")
    if not row or not ok:
        raise HTTPException(401, "Invalid email or password")
    if row["banned"]:
        raise HTTPException(403, "Account suspended. Contact support.")

    asyncio.create_task(_touch_login(row["id"]))
    token  = create_jwt({"user_id": row["id"], "email": email})
    active = _is_active(dict(row))
    log.info(f"[LOGIN] #{row['id']} {email}")
    return JSONResponse({
        "token":  token,
        "email":  email,
        "plan":   row["plan"] or "basic",
        "status": "active" if active else (row["status"] or "pending"),
    })

async def _touch_login(user_id: int):
    try:
        pool = await get_db()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET last_login=NOW() WHERE id=$1", user_id)
    except Exception:
        pass

async def admin_login(body: LoginBody, request: Request):
    ip = _get_ip(request)
    if not await _check_rate(ip, "admin_login", 5, 1800):
        raise HTTPException(429, "Too many admin login attempts. Locked for 30 minutes.")
    if body.email.lower().strip() != ADMIN_EMAIL or body.password != ADMIN_PASSWORD:
        log.warning(f"[ADMIN] Failed login from {ip}")
        raise HTTPException(401, "Invalid admin credentials")
    token = create_jwt({"is_admin": True, "email": body.email}, expires_hours=8)
    log.info(f"[ADMIN] Login from {ip}")
    return JSONResponse({"token": token})

async def me(request: Request):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"authenticated": False})
    active = _is_active(user)
    return JSONResponse({
        "authenticated": True,
        "email":  user["email"],
        "plan":   user["plan"] or "basic",
        "status": "active" if active else (user["status"] or "pending"),
        "banned": user["banned"],
        "is_owner": user.get("is_owner", False),
    })

# ─── Payment endpoints ─────────────────────────────────────────────────────────
async def create_payment(body: CreatePaymentBody, request: Request):
    user = await require_auth(request)
    if not await _check_rate(_get_ip(request), "payment", 3, 3600):
        raise HTTPException(429, "Too many payment requests. Try again later.")
    if body.plan not in PLANS:
        raise HTTPException(400, "Invalid plan")
    if not NOWPAYMENTS_KEY:
        raise HTTPException(503, "Payment processor not configured")

    plan     = PLANS[body.plan]
    order_id = f"sc_{user['id']}_{body.plan}_{secrets.token_hex(6)}"

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as s:
            r    = await s.post(
                "https://api.nowpayments.io/v1/payment",
                json={
                    "price_amount":      plan["price_usd"],
                    "price_currency":    "usd",
                    "pay_currency":      body.pay_currency,
                    "order_id":          order_id,
                    "order_description": f"Saving Capital {plan['name']} — 1 month",
                    "ipn_callback_url":  f"{APP_URL}/payments/webhook",
                    "success_url":       f"{APP_URL}/?payment=success",
                    "cancel_url":        f"{APP_URL}/?payment=cancel",
                },
                headers={"x-api-key": NOWPAYMENTS_KEY, "Content-Type": "application/json"},
            )
            data = await r.json()
    except asyncio.TimeoutError:
        raise HTTPException(504, "Payment provider timed out — please try again")
    except Exception as e:
        log.error(f"[PAYMENT] NOWPayments error: {e}")
        raise HTTPException(502, "Payment provider unreachable")

    if "payment_id" not in data:
        log.error(f"[PAYMENT] Bad response: {data}")
        raise HTTPException(502, f"Payment error: {data.get('message','Unknown')}")

    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO payments (user_id,order_id,nowpayments_id,plan,amount_usd,pay_currency,status) "
            "VALUES ($1,$2,$3,$4,$5,$6,'pending')",
            user["id"], order_id, str(data["payment_id"]),
            body.plan, plan["price_usd"], body.pay_currency
        )

    log.info(f"[PAYMENT] Created {order_id} user=#{user['id']} plan={body.plan}")
    return JSONResponse({
        "payment_id":   data["payment_id"],
        "pay_address":  data.get("pay_address"),
        "pay_amount":   data.get("pay_amount"),
        "pay_currency": data.get("pay_currency"),
        "price_amount": plan["price_usd"],
        "order_id":     order_id,
        "invoice_url":  data.get("invoice_url", ""),
    })

async def payment_webhook(request: Request):
    body_bytes = await request.body()

    # Fail closed: an unsigned webhook must never be trusted to grant a paid
    # subscription. Any user who knows their own order_id (they always do —
    # /payments/create returns it) could otherwise forge "finished" for free.
    if not NOWPAYMENTS_IPN:
        log.error("[WEBHOOK] NOWPAYMENTS_IPN_SECRET not configured — rejecting webhook")
        raise HTTPException(503, "Webhook verification not configured")

    sig = request.headers.get("x-nowpayments-sig", "")
    try:
        parsed      = json.loads(body_bytes)
        sorted_body = json.dumps(parsed, sort_keys=True, separators=(",",":"))
        expected    = _hmac_mod.new(NOWPAYMENTS_IPN.encode(), sorted_body.encode(), hashlib.sha512).hexdigest()
        if not secrets.compare_digest(sig.lower(), expected.lower()):
            log.warning("[WEBHOOK] Invalid signature")
            raise HTTPException(400, "Invalid signature")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, "Malformed payload")

    try:
        data = json.loads(body_bytes)
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    status         = data.get("payment_status", "")
    order_id       = data.get("order_id", "")
    nowpayments_id = str(data.get("payment_id", ""))

    pool = await get_db()
    async with pool.acquire() as conn:
        payment = await conn.fetchrow(
            "SELECT * FROM payments WHERE order_id=$1 OR nowpayments_id=$2",
            order_id, nowpayments_id
        )
        if not payment:
            return JSONResponse({"ok": True})

        await conn.execute("UPDATE payments SET status=$1 WHERE id=$2", status, payment["id"])

        if status in ("finished", "confirmed", "partially_paid"):
            now     = datetime.now(timezone.utc)
            expires = now + timedelta(days=31)
            await conn.execute("UPDATE payments SET confirmed_at=$1 WHERE id=$2", now, payment["id"])
            await conn.execute(
                "INSERT INTO subscriptions (user_id,plan,status,started_at,expires_at) VALUES ($1,$2,'active',$3,$4) "
                "ON CONFLICT (user_id) DO UPDATE SET plan=$2,status='active',started_at=$3,expires_at=$4",
                payment["user_id"], payment["plan"], now, expires
            )
            log.info(f"[PAYMENT] Confirmed user=#{payment['user_id']} plan={payment['plan']} exp={expires.date()}")

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
    d = dict(row)
    if d.get("confirmed_at"):
        d["confirmed_at"] = d["confirmed_at"].isoformat()
    return JSONResponse(d)

# ─── Admin endpoints ───────────────────────────────────────────────────────────
async def admin_get_users(request: Request):
    await require_admin(request)
    pool = await get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT u.id, u.email, u.created_at, u.last_login, u.banned, u.ban_reason, "
            "s.plan, s.status, s.expires_at, "
            "(SELECT COUNT(*) FROM payments p WHERE p.user_id=u.id AND p.status IN ('finished','confirmed','manual')) AS paid_count, "
            "(SELECT COALESCE(SUM(amount_usd),0) FROM payments p WHERE p.user_id=u.id AND p.status IN ('finished','confirmed','manual')) AS total_paid "
            "FROM users u LEFT JOIN subscriptions s ON s.user_id=u.id ORDER BY u.created_at DESC LIMIT 1000"
        )
    result = []
    for r in rows:
        d = dict(r)
        for k in ("created_at","last_login","expires_at"):
            if d.get(k): d[k] = d[k].isoformat()
        d["total_paid"] = float(d.get("total_paid") or 0)
        result.append(d)
    return JSONResponse(result)

async def admin_get_stats(request: Request):
    await require_admin(request)
    pool = await get_db()
    async with pool.acquire() as conn:
        s = await conn.fetchrow(
            "SELECT "
            "(SELECT COUNT(*) FROM users) AS total_users, "
            "(SELECT COUNT(*) FROM subscriptions WHERE status='active' AND expires_at>NOW()) AS active_subs, "
            "(SELECT COUNT(*) FROM subscriptions WHERE plan='basic' AND status='active' AND expires_at>NOW()) AS basic_active, "
            "(SELECT COUNT(*) FROM subscriptions WHERE plan='pro'   AND status='active' AND expires_at>NOW()) AS pro_active, "
            "(SELECT COALESCE(SUM(amount_usd),0) FROM payments WHERE status IN ('finished','confirmed','manual')) AS total_revenue, "
            "(SELECT COALESCE(SUM(amount_usd),0) FROM payments WHERE status IN ('finished','confirmed','manual') AND created_at>NOW()-INTERVAL '30 days') AS month_revenue, "
            "(SELECT COUNT(*) FROM users WHERE banned=TRUE) AS banned_users, "
            "(SELECT COUNT(*) FROM payments WHERE status='pending' AND created_at>NOW()-INTERVAL '2 hours') AS pending_payments"
        )
    d = dict(s)
    d["total_revenue"] = float(d["total_revenue"] or 0)
    d["month_revenue"] = float(d["month_revenue"] or 0)
    return JSONResponse(d)

async def admin_ban_user(body: AdminBanBody, request: Request):
    await require_admin(request)
    pool = await get_db()
    async with pool.acquire() as conn:
        if not await conn.fetchval("UPDATE users SET banned=TRUE,ban_reason=$1 WHERE id=$2 RETURNING id", body.reason, body.user_id):
            raise HTTPException(404, "User not found")
        await conn.execute("UPDATE subscriptions SET status='suspended' WHERE user_id=$1", body.user_id)
    log.info(f"[ADMIN] Banned #{body.user_id}")
    return JSONResponse({"ok": True})

async def admin_unban_user(body: AdminBanBody, request: Request):
    await require_admin(request)
    pool = await get_db()
    async with pool.acquire() as conn:
        if not await conn.fetchval("UPDATE users SET banned=FALSE,ban_reason=NULL WHERE id=$1 RETURNING id", body.user_id):
            raise HTTPException(404, "User not found")
        await conn.execute("UPDATE subscriptions SET status='active' WHERE user_id=$1 AND expires_at>NOW()", body.user_id)
    log.info(f"[ADMIN] Unbanned #{body.user_id}")
    return JSONResponse({"ok": True})

async def admin_set_plan(body: AdminPlanBody, request: Request):
    await require_admin(request)
    if body.plan not in PLANS:
        raise HTTPException(400, "Invalid plan")
    if not 1 <= body.months <= 24:
        raise HTTPException(400, "months must be 1–24")
    pool    = await get_db()
    now     = datetime.now(timezone.utc)
    expires = now + timedelta(days=31 * body.months)
    async with pool.acquire() as conn:
        if not await conn.fetchval("SELECT id FROM users WHERE id=$1", body.user_id):
            raise HTTPException(404, "User not found")
        await conn.execute(
            "INSERT INTO subscriptions (user_id,plan,status,started_at,expires_at) VALUES ($1,$2,'active',$3,$4) "
            "ON CONFLICT (user_id) DO UPDATE SET plan=$2,status='active',started_at=$3,expires_at=$4",
            body.user_id, body.plan, now, expires
        )
        await conn.execute(
            "INSERT INTO payments (user_id,order_id,plan,amount_usd,status,confirmed_at) VALUES ($1,$2,$3,$4,'manual',NOW())",
            body.user_id, f"manual_{body.user_id}_{secrets.token_hex(4)}", body.plan,
            PLANS[body.plan]["price_usd"] * body.months
        )
    log.info(f"[ADMIN] Granted {body.plan} x{body.months}mo to #{body.user_id}")
    return JSONResponse({"ok": True})

async def admin_delete_user(request: Request):
    await require_admin(request)
    data    = await request.json()
    user_id = data.get("user_id")
    if not user_id:
        raise HTTPException(400, "user_id required")
    pool = await get_db()
    async with pool.acquire() as conn:
        if not await conn.fetchval("DELETE FROM users WHERE id=$1 RETURNING id", user_id):
            raise HTTPException(404, "User not found")
    log.info(f"[ADMIN] Deleted #{user_id}")
    return JSONResponse({"ok": True})

async def auth_health(request: Request):
    try:
        pool = await get_db()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return JSONResponse({"db":"ok","pool_size": pool.get_size(), "pool_max": pool.get_max_size()})
    except Exception as e:
        return JSONResponse({"db":"error","error":str(e)}, status_code=503)

# ─── Route registration ────────────────────────────────────────────────────────
def register_auth_routes(app: FastAPI):
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
    app.delete("/admin/user")(admin_delete_user)
    app.get("/auth/health")(auth_health)
    log.info("[AUTH] Production-hardened routes registered")
