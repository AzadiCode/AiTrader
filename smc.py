# -*- coding: utf-8 -*-
"""
موتور تشخیص ساختار SMC / پرایس‌اکشن — پایتون خالص، بدون وابستگی.
کندل‌ها: لیست دیکشنری {t,o,h,l,c} به ترتیب زمانی.
اصل ضد-repaint: هر سوینگ دو ایندکس داره: i (لحظهٔ وقوع) و conf (لحظهٔ تأیید = i + lookback)؛
شکست‌ها فقط بعد از conf حساب می‌شن، پس بک‌تست دروغ نمی‌گه.
"""
from datetime import datetime, timezone

MODES = {"scalp": ("H4", "H1", "M15"), "swing": ("D1", "H4", "H1")}   # (بایاس، ناحیهٔ ورود POI، تریگر)


def atr(c, n=14):
    out, s = [], 0.0
    for i, x in enumerate(c):
        p = c[i - 1]["c"] if i else x["o"]
        tr = max(x["h"] - x["l"], abs(x["h"] - p), abs(x["l"] - p))
        if i < n:
            s += tr
            out.append(s / (i + 1))
        else:
            out.append((out[-1] * (n - 1) + tr) / n)
    return out


def swings(c, lb=3):
    out = []
    for i in range(lb, len(c) - lb):
        w = c[i - lb:i + lb + 1]
        if c[i]["h"] == max(x["h"] for x in w) and sum(x["h"] == c[i]["h"] for x in w) == 1:
            out.append({"i": i, "k": "H", "p": c[i]["h"], "conf": i + lb})
        if c[i]["l"] == min(x["l"] for x in w) and sum(x["l"] == c[i]["l"] for x in w) == 1:
            out.append({"i": i, "k": "L", "p": c[i]["l"], "conf": i + lb})
    return out


def structure(c, sw):
    """BOS = شکست در جهت روند، CHoCH = شکست خلاف روند؛ فقط با کلوز."""
    ev, trend, hi, lo = [], 0, None, None
    by = {}
    for s in sw:
        by.setdefault(s["conf"], []).append(s)
    for i, x in enumerate(c):
        for s in by.get(i, []):
            if s["k"] == "H":
                hi = s
            else:
                lo = s
        if hi and x["c"] > hi["p"]:
            ev.append({"type": "BOS" if trend >= 0 else "CHoCH", "dir": 1, "lvl": hi["p"], "from": hi["i"], "i": i})
            trend, hi = 1, None
        elif lo and x["c"] < lo["p"]:
            ev.append({"type": "BOS" if trend <= 0 else "CHoCH", "dir": -1, "lvl": lo["p"], "from": lo["i"], "i": i})
            trend, lo = -1, None
    return ev


def fvgs(c, a, min_atr=0.25):
    out = []
    for i in range(2, len(c)):
        p, n = c[i - 2], c[i]
        if n["l"] > p["h"]:
            d, lo, hi = 1, p["h"], n["l"]
        elif n["h"] < p["l"]:
            d, lo, hi = -1, n["h"], p["l"]
        else:
            continue
        if hi - lo < min_atr * a[i]:
            continue
        fill = 0.0
        for j in range(i + 1, len(c)):
            x = c[j]
            if (d == 1 and x["c"] < lo) or (d == -1 and x["c"] > hi):
                fill = 1.0
                break
            pen = (hi - x["l"]) if d == 1 else (x["h"] - lo)
            fill = max(fill, min(1.0, max(0.0, pen) / (hi - lo)))
        out.append({"i": i, "d": d, "lo": lo, "hi": hi, "fill": round(fill, 2)})
    return out


def order_blocks(c, a, ev, fv, impulse=1.5):
    """آخرین کندل مخالف قبل از حرکتی که ساختار رو شکسته. شرط کیفیت: حرکت ≥ impulse×ATR.
    وضعیت: fresh / tested (سایه وارد شد) / mitigated (کامل وارد شد) / broken (کلوز بیرون)."""
    out, seen = [], set()
    for e in ev:
        d, s, b = e["dir"], e["from"], e["i"]
        k = min(range(s, b + 1), key=lambda j: c[j]["l"]) if d == 1 else max(range(s, b + 1), key=lambda j: c[j]["h"])
        ob = next((j for j in range(k, max(k - 4, -1), -1) if (c[j]["c"] < c[j]["o"]) == (d == 1) and c[j]["c"] != c[j]["o"]), None)
        if ob is None or ob in seen:
            continue
        move = (max(c[j]["h"] for j in range(k, b + 1)) - c[k]["l"]) if d == 1 else (c[k]["h"] - min(c[j]["l"] for j in range(k, b + 1)))
        if move < impulse * a[b]:
            continue
        seen.add(ob)
        lo, hi, st = c[ob]["l"], c[ob]["h"], "fresh"
        for j in range(b + 1, len(c)):
            x = c[j]
            if d == 1:
                if x["c"] < lo:
                    st = "broken"
                    break
                if x["l"] <= lo:
                    st = "mitigated"
                elif x["l"] <= hi and st == "fresh":
                    st = "tested"
            else:
                if x["c"] > hi:
                    st = "broken"
                    break
                if x["h"] >= hi:
                    st = "mitigated"
                elif x["h"] >= lo and st == "fresh":
                    st = "tested"
        out.append({"i": ob, "d": d, "lo": lo, "hi": hi, "st": st, "k": k, "b": b, "ev": e["type"],
                    "fvg": any(f["d"] == d and k <= f["i"] <= b + 1 for f in fv)})
    return out


def sweeps(c, sw):
    """سوییپ: سایه از سوینگ رد می‌شه ولی کلوز برمی‌گرده داخل. k=L یعنی لیکوییدیتی فروش (کف) جارو شد."""
    out = []
    for s in sw:
        for j in range(s["conf"] + 1, len(c)):
            x = c[j]
            if s["k"] == "L" and x["l"] < s["p"]:
                if x["c"] > s["p"]:
                    out.append({"k": "L", "p": s["p"], "i": j, "ext": x["l"], "si": s["i"]})
                break
            if s["k"] == "H" and x["h"] > s["p"]:
                if x["c"] < s["p"]:
                    out.append({"k": "H", "p": s["p"], "i": j, "ext": x["h"], "si": s["i"]})
                break
    return out


def liquidity(c, a, sw, tol=0.15):
    """Equal Highs/Lows: حداقل دو سوینگ با اختلاف کمتر از tol×ATR؛ تا وقتی کلوزِ بیرون نیومده منتظر (resting) می‌مونه."""
    out, used = [], set()
    for kind in "HL":
        s = [x for x in sw if x["k"] == kind]
        for x in s:
            if x["i"] in used:
                continue
            g = [y for y in s if abs(y["p"] - x["p"]) <= tol * a[min(x["i"], len(a) - 1)]]
            if len(g) < 2:
                continue
            for y in g:
                used.add(y["i"])
            p = max(y["p"] for y in g) if kind == "H" else min(y["p"] for y in g)
            st, last = "resting", max(y["conf"] for y in g)
            for j in range(last + 1, len(c)):
                if (kind == "H" and c[j]["h"] > p) or (kind == "L" and c[j]["l"] < p):
                    st = "swept" if ((kind == "H" and c[j]["c"] < p) or (kind == "L" and c[j]["c"] > p)) else "taken"
                    break
            out.append({"k": kind, "p": p, "n": len(g), "i": min(y["i"] for y in g), "st": st})
    return out


def analyze(c, lb=3):
    a = atr(c)
    sw = swings(c, lb)
    ev = structure(c, sw)
    fv = fvgs(c, a)
    hs, ls = [s for s in sw if s["k"] == "H"], [s for s in sw if s["k"] == "L"]
    trend = ev[-1]["dir"] if ev else 0
    pd = None
    if hs and ls and hs[-1]["p"] > ls[-1]["p"]:
        hi, lo = hs[-1]["p"], ls[-1]["p"]
        r = hi - lo
        ote = (hi - 0.79 * r, hi - 0.62 * r) if trend == 1 else (lo + 0.62 * r, lo + 0.79 * r)
        pd = {"hi": hi, "lo": lo, "eq": (hi + lo) / 2, "pos": round((c[-1]["c"] - lo) / r, 2), "ote": ote}
    return {"atr": a[-1], "sw": sw, "ev": ev, "fvg": fv, "ob": order_blocks(c, a, ev, fv), "liq": liquidity(c, a, sw),
            "sweeps": sweeps(c, sw), "trend": trend, "pd": pd}


def killzone(ts=None):
    h = (datetime.fromtimestamp(ts, timezone.utc) if ts else datetime.now(timezone.utc)).hour
    return "london" if 7 <= h < 10 else "newyork" if 12 <= h < 16 else None


def day_levels(c):
    days = {}
    for x in c:
        d = int(x["t"] // 86400)
        r = days.setdefault(d, [x["h"], x["l"]])
        r[0], r[1] = max(r[0], x["h"]), min(r[1], x["l"])
    ks = sorted(days)
    return {"pdh": days[ks[-2]][0], "pdl": days[ks[-2]][1]} if len(ks) > 1 else None


def grade(s):
    return "A+" if s >= 80 else "A" if s >= 65 else "B" if s >= 50 else "C"


def run(data, mode="scalp"):
    tb, tp, te = MODES[mode]
    B, P, E = analyze(data[tb]), analyze(data[tp]), analyze(data[te])
    ce = data[te]
    price, bias, kz = ce[-1]["c"], B["trend"], killzone()
    highs = sorted({s["p"] for A in (B, P, E) for s in A["sw"] if s["k"] == "H"} | {l["p"] for A in (P, E) for l in A["liq"] if l["k"] == "H" and l["st"] == "resting"})
    lows = sorted({s["p"] for A in (B, P, E) for s in A["sw"] if s["k"] == "L"} | {l["p"] for A in (P, E) for l in A["liq"] if l["k"] == "L" and l["st"] == "resting"})
    setups = []
    for tf, A in ((tp, P), (te, E)):
        for o in A["ob"]:
            if o["st"] not in ("fresh", "tested"):
                continue
            d, sc, why = o["d"], 0, []
            mid = (o["lo"] + o["hi"]) / 2
            if bias == d:
                sc += 25
                why.append(f"هم‌جهت با ساختار {tb}")
            elif bias:
                why.append(f"خلاف ساختار {tb}")
            pd = P["pd"]
            if pd and ((d == 1 and mid < pd["eq"]) or (d == -1 and mid > pd["eq"])):
                sc += 20
                why.append(("دیسکانت" if d == 1 else "پریمیوم") + f" در {tp}")
            sw_ = [s for s in A["sweeps"] if (s["k"] == "L") == (d == 1) and o["k"] - 20 <= s["i"] <= o["b"]]
            if sw_:
                sc += 20
                why.append("سوییپ لیکوییدیتی قبل از ناحیه")
            if o["fvg"]:
                sc += 15
                why.append("FVG همراه")
            sc += 10 if o["st"] == "fresh" else 5
            if any(e["type"] == "CHoCH" and e["dir"] == d and e["i"] >= len(ce) - 40 for e in E["ev"]) and o["lo"] - E["atr"] <= price <= o["hi"] + E["atr"]:
                sc += 15
                why.append(f"CHoCH در {te} داخل ناحیه")
            if kz:
                sc += 5
                why.append("کیلزون " + kz)
            at = A["atr"]
            entry = mid
            sl = (min([o["lo"]] + [s["ext"] for s in sw_]) - 0.1 * at) if d == 1 else (max([o["hi"]] + [s["ext"] for s in sw_]) + 0.1 * at)
            risk = abs(entry - sl)
            cand = [p for p in highs if p > entry] if d == 1 else [p for p in reversed(lows) if p < entry]
            tp_ = next((p for p in cand if abs(p - entry) >= 2 * risk), cand[-1] if cand else None)
            rr = round(abs(tp_ - entry) / risk, 1) if tp_ and risk else None
            sc = min(sc, 100)
            setups.append({"tf": tf, "d": d, "lo": round(o["lo"], 2), "hi": round(o["hi"], 2), "st": o["st"], "score": sc, "grade": grade(sc),
                           "why": why, "entry": round(entry, 2), "sl": round(sl, 2), "tp": round(tp_, 2) if tp_ else None, "rr": rr,
                           "inzone": o["lo"] <= price <= o["hi"]})
    setups.sort(key=lambda s: (-s["score"], abs(s["entry"] - price)))
    charts = {}
    for tf, A in ((tb, B), (tp, P), (te, E)):
        cs = data[tf]
        n = min(120, len(cs))
        off = len(cs) - n
        f = lambda i: max(0, i - off)
        charts[tf] = {
            "c": [[x["t"], x["o"], x["h"], x["l"], x["c"]] for x in cs[-n:]],
            "ob": [{"i": f(o["i"]), "d": o["d"], "lo": o["lo"], "hi": o["hi"], "st": o["st"]} for o in A["ob"] if o["st"] != "broken" and o["b"] >= off][-6:],
            "fvg": [{"i": f(g["i"] - 2), "d": g["d"], "lo": g["lo"], "hi": g["hi"]} for g in A["fvg"] if g["fill"] < 1 and g["i"] >= off][-6:],
            "liq": [{"k": l["k"], "p": l["p"], "i": f(l["i"]), "st": l["st"]} for l in A["liq"] if l["st"] != "taken"][-4:],
            "ev": [{"t": e["type"], "d": e["dir"], "lvl": e["lvl"], "a": f(e["from"]), "b": e["i"] - off} for e in A["ev"] if e["i"] >= off][-8:],
            "pd": A["pd"] and {"hi": A["pd"]["hi"], "lo": A["pd"]["lo"], "eq": A["pd"]["eq"]},
        }
    return {"price": round(price, 2), "mode": mode, "tfs": [tb, tp, te], "bias": bias, "kz": kz, "pd": P["pd"] and {"pos": P["pd"]["pos"]},
            "days": day_levels(ce), "setups": setups[:4], "charts": charts,
            "last_events": [{"tf": tf, "t": e["type"], "d": e["dir"], "lvl": e["lvl"]} for tf, A in ((tb, B), (tp, P), (te, E)) for e in A["ev"][-2:]]}
