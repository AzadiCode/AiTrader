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
         دیتا پیش‌فرض: Dukascopy (بدون کلید)؛ اختیاری: TD_API_KEY (Twelve Data) / OANDA_TOKEN   DATA_SOURCE=auto|dukascopy|twelve|yahoo|binance  BINANCE_SYMBOL  DATA_SYMBOL (فال‌بک یاهو)
"""
import os, re, json, time, hmac, hashlib, logging, threading, functools, lzma, struct
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo
from urllib.parse import parse_qsl
from datetime import datetime, timezone, timedelta

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
TD_TTL = int(os.environ.get("TD_TTL", "20"))
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
# زنجیرهٔ منابع (اولی که جواب بده): Twelve Data (اگه کلید باشه) ← Binance XAUUSDT ← Yahoo XAUUSD=X ← Yahoo DATA_SYMBOL
# DATA_SOURCE=auto|dukascopy|oanda|twelve|yahoo|binance   ·   OANDA_TOKEN + OANDA_ENV=practice|live   ·   BINANCE_SYMBOL (پیش‌فرض XAUUSDT)
TF_SEC = {"M1": 60, "M5": 300, "M15": 900, "H1": 3600, "H4": 14400, "D1": 86400}
TF_BIN = {"M1": "1m", "M5": "5m", "M15": "15m", "H1": "1h", "H4": "4h", "D1": "1d"}
TF_TD = {"M1": "1min", "M5": "5min", "M15": "15min", "H1": "1h", "H4": "4h", "D1": "1day"}
TF_YH = {"M1": ("1m", "7d"), "M5": ("5m", "30d"), "M15": ("15m", "30d"), "H1": ("60m", "180d"), "D1": ("1d", "3y")}
OANDA_TOKEN = os.environ.get("OANDA_TOKEN", "").strip()
OANDA_HOST = "https://api-fxtrade.oanda.com" if os.environ.get("OANDA_ENV", "practice").lower() == "live" else "https://api-fxpractice.oanda.com"
TF_OA = {"M1": "M1", "M5": "M5", "M15": "M15", "H1": "H1", "H4": "H4", "D1": "D"}
SRC = os.environ.get("DATA_SOURCE", "auto").lower()
BIN_SYMBOL = os.environ.get("BINANCE_SYMBOL", "XAUUSDT")
HTTP = requests.Session()
HTTP.headers["User-Agent"] = "Mozilla/5.0"
_cache, _locks, _bad, SRC_USED = {}, {k: threading.Lock() for k in TF_SEC}, {}, {}
_an_cache = {}


def _oanda(tf):
    """XAU_USD اسپات از OANDA (قیمت mid)؛ همون فید چارت OANDA توی تردینگ‌ویو. توکن دمو/practice رایگانه."""
    r = HTTP.get(f"{OANDA_HOST}/v3/instruments/XAU_USD/candles", params={"granularity": TF_OA[tf], "count": 2500, "price": "M"},
                 headers={"Authorization": "Bearer " + OANDA_TOKEN}, timeout=10)
    if r.status_code >= 400:
        raise RuntimeError(f"OANDA {r.status_code}: {r.text[:120]}")
    out = []
    for k in r.json()["candles"]:
        m = k["mid"]
        dt = datetime.strptime(k["time"][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        out.append({"t": int(dt.timestamp()), "o": float(m["o"]), "h": float(m["h"]), "l": float(m["l"]), "c": float(m["c"])})
    return out


NY = ZoneInfo("America/New_York")
DUKA = "https://datafeed.dukascopy.com/datafeed/XAUUSD"
_bi5c, _yh = {}, {}
# تایم‌فریم ← تعداد روز (بدون شنبه) از کندل M1 ساخته می‌شه؛ همه‌ی تایم‌فریم‌ها از یه پایه‌ی M1 می‌آن و فایل‌ها بینشون کش مشترکه
DUKA_DAYS = {"M1": 2, "M5": 5, "M15": 10, "H1": 20, "H4": 40, "D1": 90}


def _p_candles(raw, ts):
    """رکورد ۲۴ بایتی: ثانیه، open، close، low، high، حجم"""
    return [{"t": ts + s, "o": o / 1000, "h": h / 1000, "l": l / 1000, "c": c / 1000}
            for s, o, c, l, h, _v in struct.iter_unpack(">IIIIIf", raw[:len(raw) // 24 * 24]) if o and h and l and c]


def _p_ticks(raw, ts):
    """رکورد ۲۰ بایتی تیک: ms، ask، bid، حجم‌ها ← کندل M1 از قیمت bid"""
    m = {}
    for ms, _ask, bid, _a, _b in struct.iter_unpack(">IIIff", raw[:len(raw) // 20 * 20]):
        if not bid:
            continue
        k, pr = ms // 60000, bid / 1000
        r = m.get(k)
        if r is None:
            m[k] = [pr, pr, pr, pr]
        else:
            r[1], r[2], r[3] = max(r[1], pr), min(r[2], pr), pr
    return [{"t": ts + k * 60, "o": v[0], "h": v[1], "l": v[2], "c": v[3]} for k, v in sorted(m.items())]


def _dl(path, parse, ts, cur):
    """دانلود تحمل‌خطا: ۴۰۴/۵۰۳/هر خطا = فایل خالی (کل منبع نمی‌افته). گذشته ۶ ساعت کش، ساعت/روز جاری ۲۰ ثانیه."""
    hit = _bi5c.get(path)
    if hit and time.time() - hit[0] < hit[2]:
        return hit[1]
    rows, ttl = [], 20
    try:
        for attempt in (0, 1):
            r = HTTP.get(f"{DUKA}/{path}", timeout=12)
            if r.status_code == 503 and attempt == 0:
                time.sleep(.5)
                continue
            break
        if r.status_code == 404:
            ttl = 20 if cur else 21600
        elif r.status_code < 400:
            if r.content:
                rows = parse(lzma.decompress(r.content), ts)
            ttl = 20 if cur else 21600
        else:
            log.info("dukascopy %s -> %s", path, r.status_code)
    except Exception as e:
        log.info("dukascopy %s: %s", path, str(e)[:100])
    _bi5c[path] = (time.time(), rows, ttl)
    return rows


def _duka_base(days):
    now_d, jobs, d, n = datetime.now(timezone.utc), [], datetime.now(timezone.utc), 0
    today = now_d.date()
    while n < days:
        if d.weekday() != 5:                                        # شنبه بازار بسته‌ست
            n += 1
            ds = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())
            if d.date() >= today - timedelta(days=1):                # دیروز/امروز: فایل روزانه هنوز آماده نیست ← تیک ساعتی
                for h in range(24 if d.date() < today else now_d.hour + 1):
                    jobs.append((f"{d.year}/{d.month - 1:02d}/{d.day:02d}/{h:02d}h_ticks.bi5", _p_ticks, ds + h * 3600, ds + h * 3600 > time.time() - 3 * 3600))
            else:
                jobs.append((f"{d.year}/{d.month - 1:02d}/{d.day:02d}/BID_candles_min_1.bi5", _p_candles, ds, False))
        d -= timedelta(days=1)
    with ThreadPoolExecutor(6) as ex:
        return [x for rows in ex.map(lambda j: _dl(*j), jobs) for x in rows]


def _yh_tail(sym):
    hit = _yh.get(sym)
    if hit and time.time() - hit[0] < 5:
        return hit[1]
    try:
        v = _yahoo("M1", sym)
    except Exception as e:
        log.info("yahoo tail %s: %s", sym, str(e)[:100])
        v = []
    _yh[sym] = (time.time(), v)
    return v


def _stitch(B):
    """فایل Dukascopy همیشه کمی عقبه (تا آخرین ساعت کامل). دنباله‌ی M1 رو از یاهو (اسپات، وگرنه فیوچرز) می‌چسبونیم و جوش رو روی آخرین کلوز تراز می‌کنیم."""
    if not B or B[-1]["t"] >= time.time() - 180:
        return B
    for sym in ("XAUUSD=X", SYMBOL):
        Y = _yh_tail(sym)
        tail = [y for y in Y if y["t"] > B[-1]["t"]]
        if tail:
            ref = next((y for y in Y if y["t"] == B[-1]["t"]), None)
            sh = B[-1]["c"] - (ref["c"] if ref else tail[0]["o"])
            return B + [{"t": y["t"], "o": y["o"] + sh, "h": y["h"] + sh, "l": y["l"] + sh, "c": y["c"] + sh} for y in tail]
    return B


def _ny_resample(c, step):
    """H4/D1 هم‌تراز با بروکرها: شروع روز = ۱۷:۰۰ نیویورک (با DST)."""
    out = {}
    for x in c:
        off = datetime.fromtimestamp(x["t"], NY).utcoffset().total_seconds()
        k = int((x["t"] + off - 61200) // step * step)
        b = out.get(k)
        if not b:
            out[k] = {"t": int(k + 61200 - off), "o": x["o"], "h": x["h"], "l": x["l"], "c": x["c"]}
        else:
            b["h"], b["l"], b["c"] = max(b["h"], x["h"]), min(b["l"], x["l"]), x["c"]
    return [out[k] for k in sorted(out)]


def _duka(tf):
    B = clean(_stitch(sorted(_duka_base(DUKA_DAYS[tf]), key=lambda x: x["t"])))
    if tf == "M1":
        return B
    return _resample(B, TF_SEC[tf]) if tf in ("M5", "M15", "H1") else _ny_resample(B, TF_SEC[tf])


def _binance(tf):
    err = None
    for url, sym, lim in (("https://fapi.binance.com/fapi/v1/klines", BIN_SYMBOL, 1500),
                          ("https://data-api.binance.vision/api/v3/klines", "PAXGUSDT", 1000)):
        try:
            r = HTTP.get(url, params={"symbol": sym, "interval": TF_BIN[tf], "limit": lim}, timeout=10)
            r.raise_for_status()
            return [{"t": k[0] // 1000, "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4])} for k in r.json()]
        except Exception as e:
            err = e
    raise err


_td = {}


def _td_get(tf, n):
    r = HTTP.get("https://api.twelvedata.com/time_series", params={"symbol": "XAU/USD", "interval": TF_TD[tf], "outputsize": n,
                                                                   "apikey": TD_KEY, "timezone": "UTC"}, timeout=15)
    v = r.json().get("values")
    if not v:
        raise RuntimeError("Twelve Data: " + str(r.json())[:200])
    out = []
    for x in reversed(v):
        dt = datetime.strptime(x["datetime"][:19], "%Y-%m-%d %H:%M:%S" if len(x["datetime"]) > 10 else "%Y-%m-%d")
        out.append({"t": int(dt.replace(tzinfo=timezone.utc).timestamp()), "o": float(x["open"]), "h": float(x["high"]), "l": float(x["low"]), "c": float(x["close"])})
    return out


def _twelve(tf):
    """هر درخواست ۱ کردیت می‌خوره (رایگان: ۸ در دقیقه / ۸۰۰ در روز). پس تاریخچه هر ۱۵ دقیقه یک‌بار؛ بین‌اش فقط ۳ کندل آخر."""
    st = _td.get(tf)
    if st and time.time() - st[0] < 900:
        d = {x["t"]: x for x in st[1]}
        d.update({x["t"]: x for x in _td_get(tf, 3)})
        out = [d[k] for k in sorted(d)][-2500:]
        _td[tf] = (st[0], out)
    else:
        out = _td_get(tf, 2000)
        _td[tf] = (time.time(), out)
    return out


def _yahoo(tf, sym):
    if tf == "H4":
        return _resample(_yahoo("H1", sym), 14400)
    iv, rng = TF_YH[tf]
    r = HTTP.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}", params={"interval": iv, "range": rng}, timeout=15)
    r.raise_for_status()
    res = r.json()["chart"]["result"][0]
    q = res["indicators"]["quote"][0]
    return [{"t": t, "o": o, "h": h, "l": l, "c": c} for t, o, h, l, c in zip(res["timestamp"], q["open"], q["high"], q["low"], q["close"])
            if None not in (o, h, l, c)]


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


def _closed(t):
    """بازار طلا: جمعه ~۲۲:۰۰ UTC تا یکشنبه ~۲۲:۰۰ UTC بسته‌ست."""
    d = datetime.fromtimestamp(t, timezone.utc)
    return d.weekday() == 5 or (d.weekday() == 4 and d.hour >= 22) or (d.weekday() == 6 and d.hour < 22)


def clean(c):
    """مرتب‌سازی، حذف تکراری، اصلاح های/لو، و حذف کندل‌های تخت (O=H=L=C) آخر هفته که بعضی منابع پر می‌کنن."""
    d = {}
    for x in c:
        h, l = max(x["h"], x["o"], x["c"]), min(x["l"], x["o"], x["c"])
        if h == l and _closed(x["t"]):
            continue
        d[x["t"]] = {"t": x["t"], "o": round(x["o"], 2), "c": round(x["c"], 2), "h": round(h, 2), "l": round(l, 2)}
    return [d[k] for k in sorted(d)]


def _chain():
    """ترتیب کیفیت: (OANDA/Twelve اگه کلید باشه) ← Dukascopy (بدون اکانت) ← Yahoo اسپات ← Binance (پرپچوال کم‌عمق، شدوی M1 ضعیف) ← Yahoo فیوچرز."""
    ch = []
    if OANDA_TOKEN and SRC in ("auto", "oanda"):
        ch.append(("oanda", _oanda))
    if TD_KEY and SRC in ("auto", "twelve"):
        ch.append(("twelve", _twelve))
    if SRC == "dukascopy":                      # از IP بعضی هاست‌ها (مثل Render) ۵۰۳ می‌ده؛ فقط با DATA_SOURCE=dukascopy
        ch.append(("dukascopy", _duka))
    if SRC in ("auto", "yahoo"):
        ch.append(("yahoo-spot", lambda tf: _yahoo(tf, "XAUUSD=X")))
    if SRC in ("auto", "binance"):
        ch.append(("binance", _binance))
    if SRC in ("auto", "yahoo"):
        ch.append(("yahoo-fut", lambda tf: _yahoo(tf, SYMBOL)))
    return ch


def _fetch(tf):
    for name, fn in _chain():
        if _bad.get((name, tf), 0) > time.time():
            continue
        try:
            c = clean(fn(tf))
            if len(c) >= 60:
                SRC_USED[tf] = name
                return c
            raise RuntimeError("دیتای ناکافی")
        except Exception as e:
            log.warning("source %s/%s: %s", name, tf, str(e)[:150])
            _bad[(name, tf)] = time.time() + 90
    raise RuntimeError("هیچ‌کدوم از منابع دیتا جواب ندادن")


def candles(tf):
    """لیست کندل‌ها (آخری ممکنه در حال تشکیل باشه). کش کوتاه + قفل برای هر تایم‌فریم تا درخواست‌های هم‌زمان یکی بشن."""
    ttl = {"twelve": TD_TTL, "oanda": 1}.get(SRC_USED.get(tf)) or ((4 if tf in ("M1", "M5") else 10 if tf in ("M15", "H1") else 40) if SRC_USED.get(tf) == "dukascopy" else 3)
    hit = _cache.get(tf)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    with _locks[tf]:
        hit = _cache.get(tf)
        if hit and time.time() - hit[0] < ttl:
            return hit[1]
        try:
            c = _fetch(tf)
        except Exception:
            if hit:
                return hit[1]
            raise
        _cache[tf] = (time.time(), c)
        return c


def analyze_now(mode):
    data = {}
    for tf in smc.MODES[mode]:
        c = candles(tf)
        if c and c[-1]["t"] + TF_SEC[tf] > time.time() + 2:     # کندل در حال تشکیل وارد تحلیل نمی‌شه (ضد repaint)
            c = c[:-1]
        data[tf] = c[-900:]
    if any(len(v) < 60 for v in data.values()):
        raise RuntimeError("دیتای کافی از منبع نیومد")
    key = (mode,) + tuple(v[-1]["t"] for v in data.values())
    if key in _an_cache:
        return _an_cache[key]
    res = smc.run(data, mode)
    if len(_an_cache) > 12:
        _an_cache.clear()
    _an_cache[key] = res
    return res


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


def diag_text(tf="M5"):
    L = [f"منبع‌ها برای {tf}:"]
    for name, fn in _chain():
        t = time.time()
        try:
            c = clean(fn(tf))
            flat = sum(1 for x in c[-200:] if x["h"] == x["l"])
            L.append(f"✅ {name}: {len(c)} کندل · آخرین {datetime.fromtimestamp(c[-1]['t'], timezone.utc):%m-%d %H:%M} · {c[-1]['c']} · کندل بدون شدو در ۲۰۰ تای آخر: {flat} · {int((time.time() - t) * 1000)}ms")
        except Exception as e:
            L.append(f"❌ {name}: {str(e)[:160]}")
    L.append(f"کلید Twelve Data: {'هست' if TD_KEY else 'نیست'}")
    return "\n".join(L)


@app.get("/api/diag")
def api_diag():
    k = request.args.get("key", "")
    if not (ADMIN_WEB_KEY and hmac.compare_digest(k.encode(), ADMIN_WEB_KEY.encode())):
        return "forbidden", 403
    return diag_text(request.args.get("tf", "M5") if request.args.get("tf") in TF_SEC else "M5"), 200, {"Content-Type": "text/plain; charset=utf-8"}


@app.get("/api/candles")
@authed
def api_candles(uid):
    tf = request.args.get("tf", "")
    if tf not in TF_SEC:
        return jsonify(error="bad tf"), 400
    if not rate_ok(f"cd{uid}", 90, 60):
        return jsonify(error="کمی صبر کن"), 429
    try:
        c = candles(tf)
    except Exception as e:
        return jsonify(error="خطا در دریافت دیتا: " + str(e)[:150]), 502
    since = request.args.get("since", type=int)
    c = [x for x in c if x["t"] >= since] if since else c[-min(request.args.get("limit", 1500, type=int), 3000):]
    age = int(time.time() - c[-1]["t"]) if c else None
    return jsonify(tf=tf, src=SRC_USED.get(tf), age=age, closed=_closed(time.time()), c=[[x["t"], x["o"], x["h"], x["l"], x["c"]] for x in c])


@app.post("/api/analyze")
@authed
def api_analyze(uid):
    body = request.get_json(silent=True) or {}
    mode = body.get("mode") if body.get("mode") in smc.MODES else "scalp"
    live = bool(body.get("live"))                      # رفرش خودکار: ذخیره نمی‌شه و تاریخچه رو شلوغ نمی‌کنه
    if not rate_ok(f"{'lv' if live else 'an'}{uid}", 6 if live else 8, 60):
        return jsonify(error="کمی صبر کن"), 429
    if not live:
        db.users.update_one({"_id": uid}, {"$set": {"mode": mode}}, upsert=True)
    try:
        res = analyze_now(mode)
    except Exception as e:
        log.exception("analyze")
        return jsonify(error="خطا در دریافت دیتا: " + str(e)[:150]), 502
    return jsonify(id=None if live else save(uid, res), res=res)


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
                  text="سلام! من تحلیلگر ساختار بازار طلا هستم.\nساختار، OB، FVG و لیکوییدیتی رو با کد تشخیص می‌دم، نه با حدس مدل.\n\n/fast — اسکالپ سریع (H1 ← M5 ← M1)\n/gold — اسکالپ (H4 ← H1 ← M15)\n/swing — تحلیل سوئینگ (D1 ← H4 ← H1)")
    if msg.get("photo"):
        return tg("sendMessage", chat_id=uid, text="عکس چارت دقیق خونده نمی‌شه؛ من خودم دیتای کندل رو می‌گیرم و دقیق‌تر تحلیل می‌کنم. /gold رو بزن.")
    if text.startswith("/diag") and uid in ADMIN_IDS:
        arg = text.split()[1].upper() if len(text.split()) > 1 else "M5"
        return tg("sendMessage", chat_id=uid, text=diag_text(arg if arg in TF_SEC else "M5"))
    mode = "swing" if text.startswith("/swing") else "fast" if text.startswith("/fast") else "scalp" if text.startswith(("/gold", "/scalp")) else None
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
    tg("setMyCommands", commands=[{"command": "start", "description": "شروع"}, {"command": "fast", "description": "اسکالپ سریع M1/M5"}, {"command": "gold", "description": "تحلیل اسکالپ"},
                                  {"command": "swing", "description": "تحلیل سوئینگ"}])
    log.info("webhook: %s", r)


setup_bot()


def _warm():                                    # پیش‌گرم کردن کش: اولین باز شدن مینی‌اپ منتظر دانلود فایل‌ها نمی‌مونه
    for tf in ("M15", "H1", "M5", "M1", "H4", "D1"):
        try:
            candles(tf)
        except Exception as e:
            log.warning("warm %s: %s", tf, str(e)[:120])


threading.Thread(target=_warm, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), threaded=True)
