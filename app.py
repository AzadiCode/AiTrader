# -*- coding: utf-8 -*-
"""
تحلیلگر طلا (XAUUSD) — ربات تلگرام + مینی‌اپ
فایل‌ها: app.py (بک‌اند+ربات) · smc.py (موتور تحلیل) · index.html (مینی‌اپ)
اجرا: gunicorn app:app --workers 1 --threads 8 --timeout 120     یا   python app.py

اصل کار: تشخیص ساختار (سوینگ، BOS/CHoCH، OB، FVG، لیکوییدیتی، پریمیوم/دیسکانت، گرید کانفلوئنس) کاملاً در کد انجام می‌شه؛
مدل هوش مصنوعی فقط نتیجه‌ی آماده رو روایت می‌کنه (عدد و سطح نمی‌سازه).

Environment الزامی:  MOTHER_TOKEN  BASE_URL  ADMIN_IDS
اختیاری: MONGO_URI SECRET_KEY ALLOWED_IDS ADMIN_WEB_KEY
         AI_BASE_URL AI_API_KEY AI_MODEL   (برای روایت متنی؛ نبودشون = فقط تحلیل کدی)
         TD_API_KEY (Twelve Data، منبع دقیق‌تر)  DATA_SYMBOL (پیش‌فرض یاهو: GC=F)
"""
import os, re, json, time, hmac, hashlib, logging, threading, functools
from urllib.parse import parse_qsl
from datetime import datetime, timezone

import requests
from flask import Flask, g, jsonify, request, send_from_directory
from pymongo import MongoClient
from bson import ObjectId
from bson.errors import InvalidId

import smc

# ───────────────────────── تنظیمات و دیتابیس ─────────────────────────
BOT_TOKEN = os.environ.get("MOTHER_TOKEN") or os.environ["BOT_TOKEN"]
BASE_URL = os.environ["BASE_URL"].rstrip("/")
MONGO_URI = os.environ.get("MONGO_URI", "").strip()
SECRET_KEY = os.environ.get("SECRET_KEY", BOT_TOKEN)
ADMIN_IDS = {int(x) for x in re.findall(r"\d+", os.environ.get("ADMIN_IDS", ""))}
ALLOWED = ADMIN_IDS | {int(x) for x in re.findall(r"\d+", os.environ.get("ALLOWED_IDS", ""))}
ADMIN_WEB_KEY = os.environ.get("ADMIN_WEB_KEY", "").strip()
AI_BASE_URL = re.sub(r"/chat/completions/?$", "", os.environ.get("AI_BASE_URL", "").strip().rstrip("/"))
AI_API_KEY, AI_MODEL = os.environ.get("AI_API_KEY", ""), os.environ.get("AI_MODEL", "")
AI_ON = bool(AI_BASE_URL and AI_API_KEY and AI_MODEL)
TD_KEY = os.environ.get("TD_API_KEY", "").strip()
SYMBOL = os.environ.get("DATA_SYMBOL", "GC=F")
WEBHOOK_SECRET = hashlib.sha256(("gs" + SECRET_KEY).encode()).hexdigest()[:32]

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("goldsmc")
app = Flask(__name__)

if MONGO_URI:
    db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000).get_database("goldsmc")
else:
    import mongomock
    db = mongomock.MongoClient().get_database("goldsmc")
    log.warning("MONGO_URI تنظیم نشده؛ داده‌ها موقتی‌ان")
for col, key in ((db.analyses, [("uid", 1), ("t", -1)]),):
    try:
        col.create_index(key)
    except Exception as e:
        log.warning("index failed: %s", e)
if not ALLOWED:
    log.warning("ADMIN_IDS خالیه؛ هیچ‌کس اجازه‌ی استفاده نداره")


def now():
    return datetime.now(timezone.utc)


# ───────────────────────── احراز هویت مینی‌اپ (مثل نمونه) ─────────────────────────
def verify_init_data(init_data):
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        got = pairs.pop("hash", None)
        if not got:
            return None
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(hmac.new(key, check.encode(), hashlib.sha256).hexdigest(), got):
            return None
        if time.time() - int(pairs.get("auth_date", 0)) > 86400:
            return None
        return json.loads(pairs["user"])
    except Exception:
        return None


def request_user():
    u = verify_init_data(request.headers.get("X-Init-Data", ""))
    if u:
        return u
    k = request.headers.get("X-Admin-Key", "")
    if ADMIN_WEB_KEY and ADMIN_IDS and k and hmac.compare_digest(k.encode(), ADMIN_WEB_KEY.encode()):
        return {"id": min(ADMIN_IDS), "first_name": "Admin"}
    return None


def authed(fn):
    @functools.wraps(fn)
    def w(*a, **k):
        u = request_user()
        if not u:
            return jsonify(error="unauthorized"), 401
        if u["id"] not in ALLOWED:
            return jsonify(error="دسترسی نداری"), 403
        g.user = u
        return fn(u["id"], *a, **k)
    return w


_rl = {}


def rate_ok(key, n, window):
    t = time.time()
    q = [x for x in _rl.get(key, []) if t - x < window]
    ok = len(q) < n
    if ok:
        q.append(t)
    _rl[key] = q
    return ok


# ───────────────────────── دیتای بازار ─────────────────────────
TF_SEC = {"M15": 900, "H1": 3600, "H4": 14400, "D1": 86400}
_cache = {}


def _yahoo(interval, rng):
    r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{SYMBOL}", params={"interval": interval, "range": rng},
                     headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    r.raise_for_status()
    res = r.json()["chart"]["result"][0]
    q = res["indicators"]["quote"][0]
    return [{"t": t, "o": o, "h": h, "l": l, "c": c} for t, o, h, l, c in zip(res["timestamp"], q["open"], q["high"], q["low"], q["close"])
            if None not in (o, h, l, c)]


def _twelve(interval, size=500):
    r = requests.get("https://api.twelvedata.com/time_series", params={"symbol": "XAU/USD", "interval": interval, "outputsize": size,
                                                                       "apikey": TD_KEY, "timezone": "UTC"}, timeout=15)
    v = r.json().get("values")
    if not v:
        raise RuntimeError("Twelve Data: " + str(r.json())[:200])
    out = []
    for x in reversed(v):
        dt = datetime.strptime(x["datetime"][:19], "%Y-%m-%d %H:%M:%S" if len(x["datetime"]) > 10 else "%Y-%m-%d")
        out.append({"t": int(dt.replace(tzinfo=timezone.utc).timestamp()), "o": float(x["open"]), "h": float(x["high"]), "l": float(x["low"]), "c": float(x["close"])})
    return out


def _resample(c, step):
    out = {}
    for x in c:
        k = x["t"] // step * step
        b = out.get(k)
        if not b:
            out[k] = {"t": k, "o": x["o"], "h": x["h"], "l": x["l"], "c": x["c"]}
        else:
            b["h"], b["l"], b["c"] = max(b["h"], x["h"]), min(b["l"], x["l"]), x["c"]
    return [out[k] for k in sorted(out)]


def candles(tf):
    hit = _cache.get(tf)
    if hit and time.time() - hit[0] < 60:
        return hit[1]
    if TD_KEY:
        c = _twelve({"M15": "15min", "H1": "1h", "H4": "4h", "D1": "1day"}[tf])
    elif tf == "H4":
        c = _resample(candles("H1"), 14400)
    else:
        c = _yahoo(*{"M15": ("15m", "30d"), "H1": ("60m", "90d"), "D1": ("1d", "2y")}[tf])
    closed = [x for x in c if x["t"] + TF_SEC[tf] <= time.time() + 5]        # کندل در حال تشکیل وارد تحلیل نمی‌شه (ضد repaint)
    out = closed + [x for x in c if x not in closed][-1:]                    # ولی آخرین قیمت زنده برای نمایش می‌مونه
    _cache[tf] = (time.time(), out)
    return out


def analyze_now(mode):
    tfs = smc.MODES[mode]
    data = {}
    for tf in tfs:
        c = candles(tf)
        data[tf] = c[:-1] if c and c[-1]["t"] + TF_SEC[tf] > time.time() + 5 else c
    if any(len(v) < 60 for v in data.values()):
        raise RuntimeError("دیتای کافی از منبع نیومد")
    return smc.run(data, mode)


# ───────────────────────── هوش مصنوعی (فقط روایت) ─────────────────────────
AI_SYSTEM = """You are a gold (XAUUSD) market analyst writing in Persian for a Telegram mini app.
You receive a JSON produced by a deterministic Smart-Money/price-action engine. Narrate it; NEVER invent levels, numbers or setups not present in the JSON.
Write 4–7 short lines: (1) higher-timeframe bias, (2) where price sits (premium/discount), (3) best setup with entry/SL/TP/RR and why its grade, (4) what would invalidate it, (5) one caution.
Keep technical terms (BOS, CHoCH, OB, FVG) in English. Plain text, no markdown headers. End with: «این تحلیل آموزشی است و توصیه‌ی معاملاتی نیست.»"""


def brief(res):
    return json.dumps({k: res[k] for k in ("price", "mode", "tfs", "bias", "kz", "pd", "days", "setups", "last_events")}, ensure_ascii=False)


def narrate(res):
    r = requests.post(f"{AI_BASE_URL}/chat/completions", headers={"Authorization": f"Bearer {AI_API_KEY}"}, timeout=(10, 90),
                      json={"model": AI_MODEL, "temperature": 0.3, "max_tokens": 900,
                            "messages": [{"role": "system", "content": AI_SYSTEM}, {"role": "user", "content": brief(res)}]})
    if r.status_code >= 400:
        raise RuntimeError(f"AI HTTP {r.status_code}: {r.text[:200]}")
    return (r.json()["choices"][0]["message"]["content"] or "").strip()


def save(uid, res):
    r = db.analyses.insert_one({"uid": uid, "t": now(), "mode": res["mode"], "price": res["price"], "res": res})
    return str(r.inserted_id)


def get_an(uid, aid):
    try:
        return db.analyses.find_one({"_id": ObjectId(aid), "uid": uid})
    except (InvalidId, TypeError):
        return None


# ───────────────────────── API مینی‌اپ ─────────────────────────
@app.get("/")
def index():
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "index.html")


@app.get("/healthz")
def healthz():
    return jsonify(ok=True)


@app.after_request
def _sec(r):
    r.headers.setdefault("X-Content-Type-Options", "nosniff")
    if request.path.startswith("/api/"):
        r.headers["Cache-Control"] = "no-store"
    return r


@app.get("/api/me")
@authed
def api_me(uid):
    u = db.users.find_one({"_id": uid}) or {}
    hist = [{"id": str(a["_id"]), "t": a["t"].isoformat(), "mode": a["mode"], "price": a["price"],
             "grade": (a["res"]["setups"] or [{}])[0].get("grade"), "bias": a["res"]["bias"]}
            for a in db.analyses.find({"uid": uid}).sort("t", -1).limit(12)]
    return jsonify(name=g.user.get("first_name", ""), mode=u.get("mode", "scalp"), history=hist, ai=AI_ON)


@app.post("/api/analyze")
@authed
def api_analyze(uid):
    mode = (request.get_json(silent=True) or {}).get("mode")
    mode = mode if mode in smc.MODES else "scalp"
    if not rate_ok(f"an{uid}", 8, 60):
        return jsonify(error="کمی صبر کن"), 429
    db.users.update_one({"_id": uid}, {"$set": {"mode": mode}}, upsert=True)
    try:
        res = analyze_now(mode)
    except Exception as e:
        log.exception("analyze")
        return jsonify(error="خطا در دریافت دیتا: " + str(e)[:150]), 502
    return jsonify(id=save(uid, res), res=res)


@app.get("/api/analyses/<aid>")
@authed
def api_get(uid, aid):
    a = get_an(uid, aid)
    if not a:
        return jsonify(error="not found"), 404
    return jsonify(id=aid, res=a["res"], ai=a.get("ai"))


@app.post("/api/analyses/<aid>/narrate")
@authed
def api_narrate(uid, aid):
    a = get_an(uid, aid)
    if not a:
        return jsonify(error="not found"), 404
    if not AI_ON:
        return jsonify(error="هوش مصنوعی تنظیم نشده (AI_BASE_URL / AI_API_KEY / AI_MODEL)"), 400
    if a.get("ai"):
        return jsonify(text=a["ai"])
    if not rate_ok(f"ai{uid}", 5, 60):
        return jsonify(error="کمی صبر کن"), 429
    try:
        text = narrate(a["res"])
    except Exception as e:
        return jsonify(error=str(e)[:200]), 502
    db.analyses.update_one({"_id": a["_id"]}, {"$set": {"ai": text}})
    return jsonify(text=text)


# ───────────────────────── ربات تلگرام ─────────────────────────
def tg(method, **data):
    try:
        return requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=data, timeout=20).json()
    except Exception as e:
        log.warning("tg %s: %s", method, e)
        return {"ok": False}


def summary(res):
    bias = {1: "صعودی 🟢", -1: "نزولی 🔴", 0: "نامشخص ⚪️"}[res["bias"]]
    tb = res["tfs"][0]
    L = [f"🥇 <b>XAUUSD</b>  {res['price']}", f"ساختار {tb}: {bias}"]
    if res["pd"]:
        L.append(f"موقعیت قیمت در رنج {res['tfs'][1]}: {int(res['pd']['pos'] * 100)}٪ (۵۰٪ = تعادل)")
    for s in res["setups"][:2]:
        side = "خرید" if s["d"] == 1 else "فروش"
        L.append(f"\n<b>{s['grade']}</b> · {side} · OB {s['tf']} ({s['lo']} – {s['hi']}){' ← قیمت داخل ناحیه' if s['inzone'] else ''}\n"
                 f"ورود {s['entry']} · حد ضرر {s['sl']} · هدف {s['tp'] or '—'} · RR {s['rr'] or '—'}\n" + "، ".join(s["why"]))
    if not res["setups"]:
        L.append("\nستاپ معتبری الان نیست.")
    return "\n".join(L)


def open_btn():
    return {"inline_keyboard": [[{"text": "باز کردن نمودار و تحلیل کامل", "web_app": {"url": BASE_URL}}]]}


def handle_msg(msg):
    uid, text = msg["from"]["id"], (msg.get("text") or "").strip()
    if uid not in ALLOWED:
        return tg("sendMessage", chat_id=uid, text="دسترسی نداری.")
    if text.startswith("/start"):
        return tg("sendMessage", chat_id=uid, reply_markup=open_btn(),
                  text="سلام! من تحلیلگر ساختار بازار طلا هستم.\nساختار، OB، FVG و لیکوییدیتی رو با کد تشخیص می‌دم، نه با حدس مدل.\n\n/gold — تحلیل سریع اسکالپ (H4 ← H1 ← M15)\n/swing — تحلیل سوئینگ (D1 ← H4 ← H1)")
    if msg.get("photo"):
        return tg("sendMessage", chat_id=uid, text="عکس چارت دقیق خونده نمی‌شه؛ من خودم دیتای کندل رو می‌گیرم و دقیق‌تر تحلیل می‌کنم. /gold رو بزن.")
    mode = "swing" if text.startswith("/swing") else "scalp" if text.startswith(("/gold", "/scalp")) else None
    if not mode:
        return
    if not rate_ok(f"an{uid}", 8, 60):
        return tg("sendMessage", chat_id=uid, text="کمی صبر کن.")
    st = (tg("sendMessage", chat_id=uid, text="⏳ در حال تحلیل…").get("result") or {}).get("message_id")
    try:
        res = analyze_now(mode)
        aid = save(uid, res)
        out = summary(res)
        if AI_ON:
            try:
                txt = narrate(res)
                db.analyses.update_one({"_id": ObjectId(aid)}, {"$set": {"ai": txt}})
                out += "\n\n" + re.sub(r"[<>&]", " ", txt)
            except Exception as e:
                log.warning("narrate: %s", e)
        tg("sendMessage", chat_id=uid, text=out[:4000], parse_mode="HTML", reply_markup=open_btn())
    except Exception as e:
        log.exception("bot analyze")
        tg("sendMessage", chat_id=uid, text="⚠️ خطا: " + str(e)[:200])
    finally:
        if st:
            tg("deleteMessage", chat_id=uid, message_id=st)


@app.post("/mother")
def hook():
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return "forbidden", 403
    msg = (request.get_json(silent=True) or {}).get("message")
    if msg and msg.get("chat", {}).get("type") == "private" and (msg.get("from") or {}).get("id"):
        threading.Thread(target=lambda: handle_msg(msg), daemon=True).start()
    return "ok"


def setup_bot():
    r = tg("setWebhook", url=f"{BASE_URL}/mother", secret_token=WEBHOOK_SECRET, allowed_updates=["message"])
    tg("setChatMenuButton", menu_button={"type": "web_app", "text": "تحلیل طلا", "web_app": {"url": BASE_URL}})
    tg("setMyCommands", commands=[{"command": "start", "description": "شروع"}, {"command": "gold", "description": "تحلیل اسکالپ"},
                                  {"command": "swing", "description": "تحلیل سوئینگ"}])
    log.info("webhook: %s", r)


setup_bot()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), threaded=True)
