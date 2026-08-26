import asyncio
import html
import json
import logging
import math
import os
import signal
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse


# =========================================================
# الإعدادات
# =========================================================

BINANCE_BASE = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "8080"))
TZ = ZoneInfo(os.getenv("TZ", "Asia/Riyadh"))

SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "15"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "12"))
DEEP_CANDIDATES = int(os.getenv("DEEP_CANDIDATES", "180"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "750000"))

EARLY_SCORE = float(os.getenv("EARLY_SCORE", "68"))
CONFIRMED_SCORE = float(os.getenv("CONFIRMED_SCORE", "78"))
EXPLOSION_SCORE = float(os.getenv("EXPLOSION_SCORE", "86"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "180"))

DB_PATH = os.getenv("DB_PATH", "data/early_explosion.db")
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"

TIMEFRAMES = {"15m": 15, "1h": 60, "4h": 240}
KLINE_LIMIT = 80

# --- Anti-late / 4H zone engine ---
ZONE_NEAR_ATR4 = float(os.getenv("ZONE_NEAR_ATR4", "0.30"))
MAX_CHASE_ATR15 = float(os.getenv("MAX_CHASE_ATR15", "0.65"))
REJECTION_CONFIRM = float(os.getenv("REJECTION_CONFIRM", "60"))
REJECTION_STRONG = float(os.getenv("REJECTION_STRONG", "72"))
FLOW_CONFIRM_5M = float(os.getenv("FLOW_CONFIRM_5M", "64"))
FLOW_CONFIRM_15M = float(os.getenv("FLOW_CONFIRM_15M", "60"))
REVERSAL_FLOW_MIN = float(os.getenv("REVERSAL_FLOW_MIN", "57"))
REVERSAL_EDGE_MIN = float(os.getenv("REVERSAL_EDGE_MIN", "8"))
REVERSAL_TRIGGER_FLOW_MIN = float(os.getenv("REVERSAL_TRIGGER_FLOW_MIN", "54"))
REVERSAL_MAX_CHASE_ATR15 = float(os.getenv("REVERSAL_MAX_CHASE_ATR15", "0.45"))
REVERSAL_WATCH_MAX_ATR15 = float(os.getenv("REVERSAL_WATCH_MAX_ATR15", "1.20"))
MAX_ENTRY_ZONE_ATR15 = float(os.getenv("MAX_ENTRY_ZONE_ATR15", "0.34"))
STOP_PAD_ATR4 = float(os.getenv("STOP_PAD_ATR4", "0.12"))

# --- v4 SELL quality calibration (stats 17) ---
# BUY is intentionally left almost unchanged because BUY EARLY/EXPLOSION were the strongest groups.
SELL_EARLY_ALERTS = os.getenv("SELL_EARLY_ALERTS", "false").lower() == "true"
SELL_CONFIRMED_REJECTION_MIN = float(os.getenv("SELL_CONFIRMED_REJECTION_MIN", "65"))
SELL_CONFIRMED_FLOW_MIN = float(os.getenv("SELL_CONFIRMED_FLOW_MIN", "58"))
SELL_CONFIRMED_TRIGGER_MIN = float(os.getenv("SELL_CONFIRMED_TRIGGER_MIN", "57"))
SELL_CONFIRMED_ZONE_MAX_ATR4 = float(os.getenv("SELL_CONFIRMED_ZONE_MAX_ATR4", "0.22"))

Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("early-explosion")


# =========================================================
# أدوات حسابية
# =========================================================

def clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


def pct_change(a: float, b: float) -> float:
    return safe_div(a - b, abs(b), 0.0) * 100.0


def ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (length + 1.0)
    out = values[0]
    for v in values[1:]:
        out = alpha * v + (1 - alpha) * out
    return out


def atr(rows: list[list[Any]], length: int = 14) -> float:
    if len(rows) < 2:
        return 0.0
    trs = []
    for i in range(1, len(rows)):
        high = float(rows[i][2])
        low = float(rows[i][3])
        prev_close = float(rows[i - 1][4])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs[-length:]) / max(1, min(length, len(trs)))


def fmt_price(x: float) -> str:
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:,.4f}".rstrip("0").rstrip(".")
    if x >= 0.01:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def now_local() -> datetime:
    return datetime.now(TZ)


# =========================================================
# نماذج
# =========================================================

@dataclass
class Signal:
    symbol: str
    direction: str
    stage: str
    score: float
    explosion_score: float
    entry_score: float
    safety_score: float
    scores_by_tf: dict[str, float]
    price: float
    entry_low: float
    entry_high: float
    stop: float
    tp1: float
    tp2: float
    tp3: float
    rr1: float
    rr2: float
    rr3: float
    recipe: list[str]
    details: dict[str, Any]


# =========================================================
# قاعدة البيانات
# =========================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    current_stage TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    opened_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    early_at TEXT,
    confirmed_at TEXT,
    explosion_at TEXT,
    early_price REAL,
    confirmed_price REAL,
    explosion_price REAL,
    score REAL,
    explosion_score REAL,
    entry_score REAL,
    safety_score REAL,
    score_15m REAL,
    score_1h REAL,
    score_4h REAL,
    entry_low REAL,
    entry_high REAL,
    stop REAL,
    tp1 REAL,
    tp2 REAL,
    tp3 REAL,
    rr1 REAL,
    rr2 REAL,
    rr3 REAL,
    recipe TEXT,
    details_json TEXT,
    entered_at TEXT,
    entered_price REAL,
    tp1_at TEXT,
    tp2_at TEXT,
    tp3_at TEXT,
    stop_at TEXT,
    mfe_pct REAL DEFAULT 0,
    mae_pct REAL DEFAULT 0,
    best_price REAL,
    worst_price REAL,
    closed_at TEXT,
    outcome TEXT
);

CREATE INDEX IF NOT EXISTS idx_opp_open
ON opportunities(status, symbol, direction);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    scan_number INTEGER,
    symbols_total INTEGER,
    candidates_total INTEGER,
    analyzed_total INTEGER,
    alerts_sent INTEGER,
    scan_seconds REAL,
    error TEXT
);
"""


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()


async def get_open_opportunity(symbol: str, direction: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """SELECT * FROM opportunities
               WHERE symbol=? AND direction=? AND status='OPEN'
               ORDER BY id DESC LIMIT 1""",
            (symbol, direction),
        )
        return await cur.fetchone()


async def close_opposite_on_reversal(sig: Signal) -> None:
    if not bool(sig.details.get("is_live_reversal")):
        return
    opposite="SELL" if sig.direction=="BUY" else "BUY"
    ts=now_local().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE opportunities SET status='CLOSED',closed_at=?,updated_at=?,outcome='REVERSED'
               WHERE symbol=? AND direction=? AND status='OPEN'""",
            (ts,ts,sig.symbol,opposite),
        )
        await db.commit()


async def save_signal(sig: Signal) -> tuple[int, bool]:
    existing = await get_open_opportunity(sig.symbol, sig.direction)
    ts = now_local().isoformat()
    stage_col = {
        "EARLY": ("early_at", "early_price"),
        "CONFIRMED": ("confirmed_at", "confirmed_price"),
        "EXPLOSION": ("explosion_at", "explosion_price"),
    }[sig.stage]

    async with aiosqlite.connect(DB_PATH) as db:
        if not existing:
            values = {
                "early_at": None, "confirmed_at": None, "explosion_at": None,
                "early_price": None, "confirmed_price": None, "explosion_price": None,
            }
            values[stage_col[0]] = ts
            values[stage_col[1]] = sig.price
            cur = await db.execute(
                """INSERT INTO opportunities (
                    symbol,direction,current_stage,status,opened_at,updated_at,
                    early_at,confirmed_at,explosion_at,
                    early_price,confirmed_price,explosion_price,
                    score,explosion_score,entry_score,safety_score,
                    score_15m,score_1h,score_4h,
                    entry_low,entry_high,stop,tp1,tp2,tp3,rr1,rr2,rr3,
                    recipe,details_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    sig.symbol,sig.direction,sig.stage,"OPEN",ts,ts,
                    values["early_at"],values["confirmed_at"],values["explosion_at"],
                    values["early_price"],values["confirmed_price"],values["explosion_price"],
                    sig.score,sig.explosion_score,sig.entry_score,sig.safety_score,
                    sig.scores_by_tf.get("15m",0),sig.scores_by_tf.get("1h",0),sig.scores_by_tf.get("4h",0),
                    sig.entry_low,sig.entry_high,sig.stop,sig.tp1,sig.tp2,sig.tp3,
                    sig.rr1,sig.rr2,sig.rr3,json.dumps(sig.recipe),json.dumps(sig.details),
                ),
            )
            await db.commit()
            return cur.lastrowid, True

        stages = {"EARLY": 1, "CONFIRMED": 2, "EXPLOSION": 3}
        if stages[sig.stage] <= stages[existing["current_stage"]]:
            return int(existing["id"]), False

        await db.execute(
            f"""UPDATE opportunities SET
                current_stage=?,updated_at=?,
                {stage_col[0]}=?,{stage_col[1]}=?,
                score=?,explosion_score=?,entry_score=?,safety_score=?,
                score_15m=?,score_1h=?,score_4h=?,
                entry_low=?,entry_high=?,stop=?,tp1=?,tp2=?,tp3=?,
                rr1=?,rr2=?,rr3=?,recipe=?,details_json=?
                WHERE id=?""",
            (
                sig.stage,ts,ts,sig.price,
                sig.score,sig.explosion_score,sig.entry_score,sig.safety_score,
                sig.scores_by_tf.get("15m",0),sig.scores_by_tf.get("1h",0),sig.scores_by_tf.get("4h",0),
                sig.entry_low,sig.entry_high,sig.stop,sig.tp1,sig.tp2,sig.tp3,
                sig.rr1,sig.rr2,sig.rr3,json.dumps(sig.recipe),json.dumps(sig.details),
                existing["id"],
            ),
        )
        await db.commit()
        return int(existing["id"]), True


async def record_checkpoint(scan_no, symbols, candidates, analyzed, alerts, seconds, error=None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO checkpoints
            (created_at,scan_number,symbols_total,candidates_total,analyzed_total,alerts_sent,scan_seconds,error)
            VALUES (?,?,?,?,?,?,?,?)""",
            (now_local().isoformat(), scan_no, symbols, candidates, analyzed, alerts, seconds, error),
        )
        await db.commit()


# =========================================================
# عميل Binance
# =========================================================

class BinanceClient:
    def __init__(self):
        self.session: aiohttp.ClientSession | None = None
        self.sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=20)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        if self.session:
            await self.session.close()

    async def get(self, path: str, params=None):
        assert self.session
        async with self.sem:
            for attempt in range(4):
                try:
                    async with self.session.get(BINANCE_BASE + path, params=params) as r:
                        if r.status in (418, 429):
                            await asyncio.sleep(2 ** attempt + 1)
                            continue
                        r.raise_for_status()
                        return await r.json()
                except Exception:
                    if attempt == 3:
                        raise
                    await asyncio.sleep(1.5 ** attempt)
        raise RuntimeError("Binance request failed")

    async def symbols(self) -> list[str]:
        data = await self.get("/fapi/v1/exchangeInfo")
        return [
            s["symbol"] for s in data["symbols"]
            if s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
        ]

    async def tickers(self):
        return await self.get("/fapi/v1/ticker/24hr")

    async def klines(self, symbol: str, interval: str):
        return await self.get("/fapi/v1/klines", {
            "symbol": symbol, "interval": interval, "limit": KLINE_LIMIT
        })

    async def oi(self, symbol: str):
        return await self.get("/fapi/v1/openInterest", {"symbol": symbol})

    async def oi_hist(self, symbol: str, period: str):
        return await self.get("/futures/data/openInterestHist", {
            "symbol": symbol, "period": period, "limit": 8
        })

    async def depth(self, symbol: str):
        return await self.get("/fapi/v1/depth", {"symbol": symbol, "limit": 100})

    async def premium(self, symbol: str):
        return await self.get("/fapi/v1/premiumIndex", {"symbol": symbol})


# =========================================================
# التحليل
# =========================================================

def candle_features(rows: list[list[Any]], direction: str) -> dict[str, float]:
    closes = [float(x[4]) for x in rows]
    highs = [float(x[2]) for x in rows]
    lows = [float(x[3]) for x in rows]
    volumes = [float(x[5]) for x in rows]
    taker_buy = [float(x[9]) for x in rows]

    price = closes[-1]
    a = atr(rows)
    avg_vol = sum(volumes[-21:-1]) / max(1, len(volumes[-21:-1]))
    vol_ratio = safe_div(volumes[-1], avg_vol, 1.0)

    deltas = [(2 * tb - v) for tb, v in zip(taker_buy, volumes)]
    delta_now = sum(deltas[-3:])
    delta_prev = sum(deltas[-6:-3])
    delta_accel = pct_change(delta_now, delta_prev) if delta_prev else 0.0
    cvd = sum(deltas[-20:])
    cvd_prev = sum(deltas[-25:-5])

    recent_range = max(highs[-6:]) - min(lows[-6:])
    range_atr = safe_div(recent_range, a, 0.0)
    compression = clamp((2.5 - range_atr) / 2.0 * 100)

    ema9 = ema(closes[-30:], 9)
    ema21 = ema(closes[-50:], 21)
    trend = pct_change(ema9, ema21)

    breakout_up = price > max(highs[-8:-1])
    breakout_down = price < min(lows[-8:-1])
    near_up = safe_div(max(highs[-8:-1]) - price, a, 99) < 0.35
    near_down = safe_div(price - min(lows[-8:-1]), a, 99) < 0.35

    price_move = pct_change(closes[-1], closes[-4])
    cvd_div = (cvd > cvd_prev and abs(price_move) < 0.8) if direction == "BUY" else (cvd < cvd_prev and abs(price_move) < 0.8)

    signed_delta = delta_now if direction == "BUY" else -delta_now
    signed_cvd = cvd if direction == "BUY" else -cvd
    signed_trend = trend if direction == "BUY" else -trend
    breakout = breakout_up if direction == "BUY" else breakout_down
    near_break = near_up if direction == "BUY" else near_down

    return {
        "price": price,
        "atr": a,
        "vol_ratio": vol_ratio,
        "delta_strength": clamp(50 + safe_div(signed_delta, max(sum(volumes[-3:]), 1), 0) * 250),
        "delta_accel": clamp(50 + (delta_accel if direction == "BUY" else -delta_accel) * 0.35),
        "cvd_strength": clamp(50 + safe_div(signed_cvd, max(sum(volumes[-20:]), 1), 0) * 300),
        "cvd_divergence": 100.0 if cvd_div else 35.0,
        "compression": compression,
        "volume": clamp((vol_ratio - 0.7) * 65),
        "trend": clamp(50 + signed_trend * 20),
        "breakout": 100.0 if breakout else (72.0 if near_break else 25.0),
        "is_breakout": breakout,
        "near_break": near_break,
        "swing_low": min(lows[-12:]),
        "swing_high": max(highs[-12:]),
    }




def htf_zone_context(rows: list[list[Any]], direction: str) -> dict[str, float | str | bool]:
    """حدد أقرب منطقة طلب/عرض من بنية 4H بدل مطاردة السعر."""
    highs=[float(x[2]) for x in rows]
    lows=[float(x[3]) for x in rows]
    opens=[float(x[1]) for x in rows]
    closes=[float(x[4]) for x in rows]
    price=closes[-1]
    a=max(atr(rows), price*0.002)
    start=max(2, len(rows)-42)
    end=len(rows)-2
    candidates=[]
    for i in range(start,end):
        if direction=="BUY":
            pivot=lows[i] <= lows[i-1] and lows[i] <= lows[i+1]
            if not pivot: continue
            base_top=max(opens[i], closes[i]) + 0.10*a
            zlow=lows[i]-0.06*a
            zhigh=min(base_top, zlow+0.62*a)
            if zhigh < zlow: zhigh=zlow+0.35*a
            displacement=(max(closes[i+1:min(len(rows),i+7)], default=closes[i])-lows[i])/a
            if zlow <= price+0.40*a:
                dist=0 if zlow <= price <= zhigh else min(abs(price-zlow),abs(price-zhigh))/a
                candidates.append((dist-0.01*(i-start), i,zlow,zhigh,displacement))
        else:
            pivot=highs[i] >= highs[i-1] and highs[i] >= highs[i+1]
            if not pivot: continue
            base_bottom=min(opens[i], closes[i]) - 0.10*a
            zhigh=highs[i]+0.06*a
            zlow=max(base_bottom, zhigh-0.62*a)
            if zhigh < zlow: zlow=zhigh-0.35*a
            displacement=(highs[i]-min(closes[i+1:min(len(rows),i+7)], default=closes[i]))/a
            if zhigh >= price-0.40*a:
                dist=0 if zlow <= price <= zhigh else min(abs(price-zlow),abs(price-zhigh))/a
                candidates.append((dist-0.01*(i-start), i,zlow,zhigh,displacement))
    if candidates:
        _, pi, zlow, zhigh, displacement=min(candidates,key=lambda x:x[0])
    else:
        if direction=="BUY":
            pi=min(range(max(0,len(rows)-20),len(rows)-1), key=lambda i:lows[i])
            zlow=lows[pi]-0.06*a; zhigh=zlow+0.52*a
            displacement=(max(closes[pi+1:], default=price)-lows[pi])/a
        else:
            pi=max(range(max(0,len(rows)-20),len(rows)-1), key=lambda i:highs[i])
            zhigh=highs[pi]+0.06*a; zlow=zhigh-0.52*a
            displacement=(highs[pi]-min(closes[pi+1:], default=price))/a
    in_zone=zlow <= price <= zhigh
    if in_zone: distance=0.0
    elif price < zlow: distance=(zlow-price)/a
    else: distance=(price-zhigh)/a
    e20=ema(closes[-60:],20); e50=ema(closes[-70:],50)
    trend_raw=pct_change(e20,e50)
    signed=trend_raw if direction=="BUY" else -trend_raw
    structure=clamp(50+signed*18)
    strength=clamp(45 + min(displacement,2.5)*18 + (8 if in_zone else 0))
    return {
        "zone_low":zlow,"zone_high":zhigh,"distance_atr4":distance,"in_zone":in_zone,
        "atr4":a,"pivot_index":pi,"zone_strength":strength,"structure_score":structure,
        "trend_label":("صاعد" if trend_raw>0.12 else "هابط" if trend_raw<-0.12 else "متوازن"),
        "displacement_atr":displacement,
    }

def rejection_features(rows: list[list[Any]], direction: str) -> dict[str, float | bool]:
    """شموع رفض/سحب سيولة على 5M أو 15M بدون انتظار إغلاق 4H."""
    if len(rows)<8:
        return {"score":0.0,"sweep":False,"wick":0.0,"reclaim":False}
    o,h,l,c=map(float,(rows[-1][1],rows[-1][2],rows[-1][3],rows[-1][4]))
    rng=max(h-l,1e-12); body=abs(c-o)
    upper=h-max(o,c); lower=min(o,c)-l
    prev_high=max(float(x[2]) for x in rows[-7:-1])
    prev_low=min(float(x[3]) for x in rows[-7:-1])
    if direction=="BUY":
        wick=lower/rng
        sweep=l<prev_low and c>prev_low
        reclaim=c >= l + 0.62*rng
        body_dir=c>o
    else:
        wick=upper/rng
        sweep=h>prev_high and c<prev_high
        reclaim=c <= h - 0.62*rng
        body_dir=c<o
    score=35 + wick*45 + (24 if sweep else 0) + (14 if reclaim else 0) + (7 if body_dir else 0)
    if body/rng < 0.18 and wick>0.55: score += 7
    return {"score":clamp(score),"sweep":sweep,"wick":wick*100,"reclaim":reclaim,"body_dir":body_dir}

def micro_structure_features(rows: list[list[Any]], direction: str) -> dict[str, float | bool]:
    """قراءة حية للبنية الدقيقة: BOS/CHOCH + استرداد + اندفاع، لكل فريم بشكل مستقل."""
    if len(rows) < 12:
        return {"bos": False, "choch": False, "reclaim": False, "momentum": 0.0, "trigger": 0.0, "invalidation": 0.0}
    o=[float(x[1]) for x in rows]; h=[float(x[2]) for x in rows]; l=[float(x[3]) for x in rows]; c=[float(x[4]) for x in rows]
    a=max(atr(rows), abs(c[-1])*1e-6)
    prev_hi=max(h[-8:-1]); prev_lo=min(l[-8:-1])
    fast=ema(c[-24:],5); slow=ema(c[-30:],13)
    slope=pct_change(fast, slow)
    if direction=="BUY":
        bos=c[-1] > prev_hi
        choch=(c[-2] <= prev_hi and c[-1] > prev_hi) or (l[-1] < prev_lo and c[-1] > prev_lo)
        reclaim=c[-1] > max(o[-1], (h[-1]+l[-1])*0.5)
        impulse=(c[-1]-o[-1])/a
        momentum=clamp(50 + slope*24 + impulse*18 + (12 if bos else 0) + (10 if choch else 0))
        trigger=max(prev_lo, min(c[-1], prev_hi)) if (bos or choch) else max(l[-3:])
        invalidation=min(l[-6:])
    else:
        bos=c[-1] < prev_lo
        choch=(c[-2] >= prev_lo and c[-1] < prev_lo) or (h[-1] > prev_hi and c[-1] < prev_hi)
        reclaim=c[-1] < min(o[-1], (h[-1]+l[-1])*0.5)
        impulse=(o[-1]-c[-1])/a
        momentum=clamp(50 - slope*24 + impulse*18 + (12 if bos else 0) + (10 if choch else 0))
        trigger=min(prev_hi, max(c[-1], prev_lo)) if (bos or choch) else min(h[-3:])
        invalidation=max(h[-6:])
    return {"bos":bos,"choch":choch,"reclaim":reclaim,"momentum":momentum,"trigger":trigger,"invalidation":invalidation}


def professional_trade_plan(direction: str, price: float, zone: dict, a15: float, micro5: dict, micro15: dict):
    """دخول ضيق حول مستوى الاسترداد/إعادة الاختبار، ووقف خلف نقطة إبطال حقيقية لا خلف رقم ثابت فقط."""
    zlow=float(zone["zone_low"]); zhigh=float(zone["zone_high"]); a4=float(zone["atr4"])
    trigger_candidates=[float(x.get("trigger",0) or 0) for x in (micro5,micro15)]
    trigger_candidates=[x for x in trigger_candidates if x>0]
    trigger=sum(trigger_candidates)/len(trigger_candidates) if trigger_candidates else price
    # لا نطارد السعر؛ نثبت نقطة الدخول داخل منطقة 4H وحول مستوى الزناد الحي.
    center=min(max(trigger,zlow),zhigh)
    half=min(MAX_ENTRY_ZONE_ATR15*a15, max(0.10*a15, (zhigh-zlow)*0.14))
    entry_low=max(zlow, center-half)
    entry_high=min(zhigh, center+half)
    if entry_high <= entry_low:
        entry_low,entry_high=zlow,zhigh
    mid=(entry_low+entry_high)/2
    inv5=float(micro5.get("invalidation",0) or 0); inv15=float(micro15.get("invalidation",0) or 0)
    pad=max(STOP_PAD_ATR4*a4, 0.35*a15)
    if direction=="BUY":
        structural=min([x for x in (zlow,inv5,inv15) if x>0])
        stop=structural-pad
        risk=max(mid-stop,0.60*a15)
        tp1,tp2,tp3=mid+risk,mid+2*risk,mid+3*risk
    else:
        structural=max([x for x in (zhigh,inv5,inv15) if x>0])
        stop=structural+pad
        risk=max(stop-mid,0.60*a15)
        tp1,tp2,tp3=mid-risk,mid-2*risk,mid-3*risk
    rr=lambda tp: abs(tp-mid)/max(abs(mid-stop),1e-12)
    return entry_low,entry_high,stop,tp1,tp2,tp3,rr(tp1),rr(tp2),rr(tp3)

def zone_trade_plan(direction: str, price: float, zone: dict, a15: float, swing_low: float, swing_high: float):
    zlow=float(zone["zone_low"]); zhigh=float(zone["zone_high"]); a4=float(zone["atr4"])
    pad=max(a15*0.32, (zhigh-zlow)*0.18)
    if direction=="BUY":
        entry_low=max(zlow, price-pad)
        entry_high=min(zhigh, price+pad) if price<=zhigh else zhigh
        if entry_high<entry_low: entry_low,entry_high=zlow,zhigh
        stop=min(zlow-max(0.18*a4,0.65*a15), swing_low-0.12*a15)
    else:
        entry_low=max(zlow, price-pad) if price>=zlow else zlow
        entry_high=min(zhigh, price+pad)
        if entry_high<entry_low: entry_low,entry_high=zlow,zhigh
        stop=max(zhigh+max(0.18*a4,0.65*a15), swing_high+0.12*a15)
    mid=(entry_low+entry_high)/2
    risk=max(abs(mid-stop),0.70*a15)
    if direction=="BUY": tp1,tp2,tp3=mid+risk,mid+2*risk,mid+3*risk
    else: tp1,tp2,tp3=mid-risk,mid-2*risk,mid-3*risk
    rr=lambda tp: abs(tp-mid)/max(abs(mid-stop),1e-12)
    return entry_low,entry_high,stop,tp1,tp2,tp3,rr(tp1),rr(tp2),rr(tp3)

def orderbook_features(depth: dict, direction: str) -> dict[str, float]:
    bids = [(float(p), float(q)) for p, q in depth.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in depth.get("asks", [])]
    bid_notional = sum(p*q for p, q in bids[:30])
    ask_notional = sum(p*q for p, q in asks[:30])
    imbalance = safe_div(bid_notional - ask_notional, bid_notional + ask_notional, 0.0)
    signed = imbalance if direction == "BUY" else -imbalance

    # تركيز أوامر كبيرة: مؤشر Iceberg/Absorption احتمالي وليس إثباتًا.
    bid_sizes = [q for _, q in bids[:50]]
    ask_sizes = [q for _, q in asks[:50]]
    side = bid_sizes if direction == "BUY" else ask_sizes
    opp = ask_sizes if direction == "BUY" else bid_sizes
    side_peak = max(side, default=0)
    side_avg = sum(side)/max(1,len(side))
    opp_avg = sum(opp)/max(1,len(opp))
    wall = safe_div(side_peak, side_avg, 0)

    return {
        "imbalance_raw": imbalance,
        "imbalance": clamp(50 + signed * 180),
        "absorption": clamp(35 + max(0, wall - 2) * 12),
        "iceberg": clamp(30 + max(0, wall - 3) * 14),
        "spoof_risk": clamp(max(0, wall - 8) * 14 + (20 if side_avg > opp_avg*4 else 0)),
    }


def oi_features(hist: list[dict], direction: str) -> dict[str, float]:
    if len(hist) < 2:
        return {"oi_change": 0, "oi_score": 50}
    vals = [float(x.get("sumOpenInterest", 0)) for x in hist]
    change = pct_change(vals[-1], vals[-4] if len(vals) >= 4 else vals[0])
    # ارتفاع OI مفيد للاتجاهين؛ اتجاه السعر والدلتا يحددان BUY/SELL.
    return {"oi_change": change, "oi_score": clamp(50 + change * 18)}


def funding_features(data: dict, direction: str) -> dict[str, float]:
    funding = float(data.get("lastFundingRate", 0)) * 100
    # التمويل عامل مساعد: التمويل السلبي يدعم BUY، والموجب يدعم SELL.
    signed = -funding if direction == "BUY" else funding
    return {"funding": funding, "funding_score": clamp(50 + signed * 900)}


def combine_score(c: dict, ob: dict, oi: dict, fund: dict, tf: str) -> float:
    weights = {
        "15m": (0.20,0.13,0.10,0.13,0.12,0.12,0.08,0.08,0.04),
        "1h":  (0.17,0.12,0.10,0.11,0.13,0.12,0.12,0.09,0.04),
        "4h":  (0.14,0.10,0.09,0.09,0.14,0.10,0.17,0.13,0.04),
    }[tf]
    vals = [
        c["delta_strength"], c["cvd_strength"], c["cvd_divergence"],
        ob["imbalance"], oi["oi_score"], c["volume"],
        c["trend"], c["breakout"], fund["funding_score"],
    ]
    score = sum(w*v for w,v in zip(weights, vals))
    score -= ob["spoof_risk"] * 0.10
    return clamp(score)


def build_trade_plan(direction: str, price: float, a: float, swing_low: float, swing_high: float, stage: str):
    zone = max(a * (0.18 if stage == "EXPLOSION" else 0.28), price * 0.0008)
    if direction == "BUY":
        entry_low, entry_high = price - zone, price + zone * 0.25
        structural = min(swing_low, entry_low - a * 0.75)
        stop = structural - a * 0.15
        risk = max(((entry_low + entry_high)/2) - stop, a * 0.65)
        mid = (entry_low + entry_high)/2
        tp1, tp2, tp3 = mid+risk, mid+2*risk, mid+3*risk
    else:
        entry_low, entry_high = price - zone * 0.25, price + zone
        structural = max(swing_high, entry_high + a * 0.75)
        stop = structural + a * 0.15
        risk = max(stop - ((entry_low + entry_high)/2), a * 0.65)
        mid = (entry_low + entry_high)/2
        tp1, tp2, tp3 = mid-risk, mid-2*risk, mid-3*risk

    rr = lambda tp: abs(tp-mid)/max(abs(mid-stop), 1e-12)
    return entry_low, entry_high, stop, tp1, tp2, tp3, rr(tp1), rr(tp2), rr(tp3)


def choose_stage(scores: dict[str,float], f15: dict, f1: dict, f4: dict) -> str | None:
    s15, s1, s4 = scores["15m"], scores["1h"], scores["4h"]
    breakout = f15["is_breakout"] or f1["is_breakout"]
    volume_expansion = max(f15["vol_ratio"], f1["vol_ratio"]) >= 1.35

    explosion = 0.45*s15 + 0.40*s1 + 0.15*s4
    confirmed = 0.35*s15 + 0.50*s1 + 0.15*s4
    early = 0.60*s15 + 0.25*s1 + 0.15*s4

    if explosion >= EXPLOSION_SCORE and breakout and volume_expansion:
        return "EXPLOSION"
    if confirmed >= CONFIRMED_SCORE and s15 >= 65 and s1 >= 72:
        return "CONFIRMED"
    if early >= EARLY_SCORE and s15 >= EARLY_SCORE:
        return "EARLY"
    return None


# =========================================================
# تيليجرام
# =========================================================

async def send_telegram(session: aiohttp.ClientSession, text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram variables are missing")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with session.post(url, json=payload, timeout=20) as r:
            body = await r.text()
            if r.status != 200:
                log.error("Telegram %s: %s", r.status, body)
                return False
            return True
    except Exception:
        log.exception("Telegram send failed")
        return False


def signal_message(sig: Signal) -> str:
    reversal=bool(sig.details.get("is_live_reversal"))
    stage_title={
        "EARLY":"🟢 تحول حي — انتظار إعادة الاختبار" if reversal else "🟡 داخل/قرب منطقة 4H — مراقبة",
        "CONFIRMED":"🔄 انقلاب حي — دخول بعد التأكيد" if reversal else "🔵 تأكيد رفض من منطقة 4H",
        "EXPLOSION":"🔥 انقلاب مؤكد — دخول الآن" if reversal else "🔥 دخول مؤكد من منطقة 4H",
    }[sig.stage]
    side="شراء" if sig.direction=="BUY" else "بيع"
    z=sig.details.get("zone_4h",{})
    r5=sig.details.get("rejection_5m",{})
    r15=sig.details.get("rejection_15m",{})
    m5=sig.details.get("micro_5m",{})
    m15=sig.details.get("micro_15m",{})
    checks="\n".join(f"✅ {html.escape(x)}" for x in sig.recipe[:7])
    action={
        "EARLY":"👀 الحالة: تحول الاتجاه مؤكد مبدئيًا — لا نطارد؛ ننتظر إعادة اختبار منطقة الانقلاب" if reversal else "👀 الحالة: السعر عند المنطقة — ننتظر رفض/تدفق، لا مطاردة",
        "CONFIRMED":"📍 الحالة: دخول من المنطقة بعد تأكيد 5M/15M",
        "EXPLOSION":"⚡ الحالة: دخول الآن؛ الإشارة خرجت قبل امتداد الحركة",
    }[sig.stage]
    tv=f"https://www.tradingview.com/chart/?symbol=BINANCE:{sig.symbol}.P"
    bn=f"https://www.binance.com/en/futures/{sig.symbol}"
    ts=now_local().strftime("%d-%m-%Y %H:%M:%S")
    return f"""<b>{stage_title} — {side}</b>

💰 العملة: <b>#{sig.symbol}.P</b>
🧭 التحليل الرئيسي: <b>4H</b> | الزناد: <b>5M / 15M</b>
💵 السعر: <b>{fmt_price(sig.price)}</b>

🏦 منطقة {side} / الانقلاب: <b>{fmt_price(float(z.get('zone_low',sig.entry_low)))} – {fmt_price(float(z.get('zone_high',sig.entry_high)))}</b>
📏 البعد عن المنطقة: <b>{float(z.get('distance_atr4',0)):.2f} ATR4</b>
🧱 قوة المنطقة: <b>{float(z.get('zone_strength',0)):.1f}%</b>
📈 اتجاه بنية 4H: <b>{html.escape(str(z.get('trend_label','-')))}</b>

🕯 رفض 5M: <b>{float(r5.get('score',0)):.1f}%</b> | رفض 15M: <b>{float(r15.get('score',0)):.1f}%</b>
🧩 بنية 5M: <b>{'BOS/CHOCH' if (m5.get('bos') or m5.get('choch')) else 'تراقب'}</b> | 15M: <b>{'BOS/CHOCH' if (m15.get('bos') or m15.get('choch')) else 'تراقب'}</b>
⚡ درجة التدفق الحي: <b>{float(sig.details.get('flow_score',sig.explosion_score)):.1f}%</b>
🎯 جودة الدخول: <b>{sig.entry_score:.1f}%</b>
🛡️ الأمان: <b>{sig.safety_score:.1f}%</b>

🎯 منطقة الدخول: <b>{fmt_price(sig.entry_low)} – {fmt_price(sig.entry_high)}</b>
🛑 وقف الخسارة: <b>{fmt_price(sig.stop)}</b>
✅ TP1: <b>{fmt_price(sig.tp1)}</b> ({sig.rr1:.1f}R)
✅ TP2: <b>{fmt_price(sig.tp2)}</b> ({sig.rr2:.1f}R)
✅ TP3: <b>{fmt_price(sig.tp3)}</b> ({sig.rr3:.1f}R)

{checks}

{action}

🕒 {ts} (السعودية)
🔗 <a href="{bn}">Binance</a> | <a href="{tv}">TradingView</a>

⚠️ خطة إحصائية مقترحة وليست ضمانًا أو تنفيذًا تلقائيًا."""


# =========================================================
# محرك المتابعة
# =========================================================

class Engine:
    def __init__(self):
        self.client = BinanceClient()
        self.telegram_session: aiohttp.ClientSession | None = None
        self.running = True
        self.scan_no = 0
        self.last_scan = None
        self.last_error = None
        self.symbol_count = 0
        self.candidate_count = 0
        self.alert_count = 0

    async def start(self):
        await init_db()
        await self.client.start()
        self.telegram_session = aiohttp.ClientSession()
        if SEND_STARTUP_MESSAGE:
            await send_telegram(
                self.telegram_session,
                "✅ <b>Ahmed Early Explosion Trader بدأ العمل</b>\n\n"
                "التحليل الرئيسي: 4H | الزناد: 5M / 15M\n"
                "المنطق: 4H للموقع + 5M/15M للزناد + انقلاب حي BUY↔SELL + وقف بنيوي\n"
                "🧠 v4: BUY محفوظ + فلترة SELL CONFIRMED + انقلاب حي BUY↔SELL\n"
                "⚠️ لا ينفذ صفقات تلقائيًا."
            )
        asyncio.create_task(self.loop())
        asyncio.create_task(self.track_open_positions())

    async def close(self):
        self.running = False
        await self.client.close()
        if self.telegram_session:
            await self.telegram_session.close()

    async def loop(self):
        while self.running:
            started = time.monotonic()
            self.scan_no += 1
            alerts = 0
            analyzed = 0
            error = None
            try:
                alerts, analyzed = await self.scan()
                self.last_error = None
            except Exception as e:
                error = repr(e)
                self.last_error = error
                log.exception("Scan failed")
            elapsed = time.monotonic() - started
            self.last_scan = now_local().isoformat()
            await record_checkpoint(
                self.scan_no, self.symbol_count, self.candidate_count,
                analyzed, alerts, elapsed, error
            )
            log.info(
                "scan=%s symbols=%s candidates=%s analyzed=%s alerts=%s seconds=%.1f",
                self.scan_no, self.symbol_count, self.candidate_count,
                analyzed, alerts, elapsed
            )
            await asyncio.sleep(max(5, SCAN_SECONDS - elapsed))

    async def scan(self) -> tuple[int,int]:
        symbols = await self.client.symbols()
        self.symbol_count = len(symbols)
        tickers = await self.client.tickers()
        allowed = set(symbols)

        ranked = []
        for t in tickers:
            s = t.get("symbol")
            if s not in allowed:
                continue
            qv = float(t.get("quoteVolume", 0))
            if qv >= MIN_QUOTE_VOLUME:
                ranked.append((qv, s))
        ranked.sort(reverse=True)

        # كل العقود تُفحص بالحجم والسيولة اليومية؛ التحليل العميق للأعلى سيولة.
        candidates = [s for _,s in ranked[:DEEP_CANDIDATES]]
        self.candidate_count = len(candidates)

        tasks = [self.analyze_symbol(s) for s in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        alerts = 0
        analyzed = 0
        for result in results:
            if isinstance(result, Exception):
                log.debug("symbol analysis failed: %r", result)
                continue
            analyzed += 1
            for sig in result:
                await close_opposite_on_reversal(sig)
                _, changed = await save_signal(sig)
                if changed:
                    ok = await send_telegram(self.telegram_session, signal_message(sig))
                    alerts += int(ok)
                    self.alert_count += int(ok)
        return alerts, analyzed

    async def analyze_symbol(self, symbol: str) -> list[Signal]:
        k5,k15,k1,k4,oi5,oi15,oi1,depth,premium = await asyncio.gather(
            self.client.klines(symbol,"5m"),
            self.client.klines(symbol,"15m"),
            self.client.klines(symbol,"1h"),
            self.client.klines(symbol,"4h"),
            self.client.oi_hist(symbol,"5m"),
            self.client.oi_hist(symbol,"15m"),
            self.client.oi_hist(symbol,"1h"),
            self.client.depth(symbol),
            self.client.premium(symbol),
        )
        # مهم: نقرأ السيناريو المفتوح قبل بناء المرشحين. النسخة السابقة كانت لا ترى
        # الانقلاب إذا ابتعد السعر عن منطقة 4H المعاكسة، وهذا سبب تفويت BUY بعد SELL.
        open_buy, open_sell = await asyncio.gather(
            get_open_opportunity(symbol,"BUY"),
            get_open_opportunity(symbol,"SELL"),
        )
        open_by_dir={"BUY":open_buy,"SELL":open_sell}

        candidates=[]
        raw={}
        for direction in ("BUY","SELL"):
            opposite = "SELL" if direction=="BUY" else "BUY"
            opposite_open = open_by_dir.get(opposite)

            c5=candle_features(k5,direction); c15=candle_features(k15,direction)
            c1=candle_features(k1,direction); c4=candle_features(k4,direction)
            ob=orderbook_features(depth,direction)
            o5=oi_features(oi5,direction); o15=oi_features(oi15,direction); o1=oi_features(oi1,direction)
            fund=funding_features(premium,direction)
            s5=combine_score(c5,ob,o5,fund,"15m")
            s15=combine_score(c15,ob,o15,fund,"15m")
            s1=combine_score(c1,ob,o1,fund,"1h")
            s4=combine_score(c4,ob,o1,fund,"4h")
            zone=htf_zone_context(k4,direction)
            r5=rejection_features(k5,direction); r15=rejection_features(k15,direction)
            m5=micro_structure_features(k5,direction); m15=micro_structure_features(k15,direction)

            rejection5=float(r5["score"]); rejection15=float(r15["score"])
            rejection=0.58*rejection5+0.42*rejection15
            flow=0.38*s5+0.27*s15+0.15*float(m5["momentum"])+0.12*float(m15["momentum"])+0.08*ob["imbalance"]
            flow_confirm=(flow>=REVERSAL_FLOW_MIN and (s5>=55 or s15>=57) and (ob["imbalance"]>=52 or c5["cvd_strength"]>=55))
            reversal_flow=0.40*s5+0.30*s15+0.15*float(m5["momentum"])+0.15*float(m15["momentum"])
            structure_live=bool(m5["bos"] or m5["choch"] or m15["bos"] or m15["choch"])
            reclaim_live=bool(m5["reclaim"] or m15["reclaim"] or r5["reclaim"] or r15["reclaim"])
            strong_reject=(rejection5>=REJECTION_STRONG or rejection15>=REJECTION_STRONG)
            reject_confirm=(rejection5>=REJECTION_CONFIRM or rejection15>=REJECTION_CONFIRM)

            # --- فشل السيناريو المعاكس / انقلاب حي ---
            failure_level=None
            failure_reclaimed=False
            prior_zone={}
            if opposite_open:
                try:
                    prior_details=json.loads(opposite_open["details_json"] or "{}")
                    prior_zone=prior_details.get("zone_4h",{}) or {}
                except Exception:
                    prior_zone={}
                a15=max(c15["atr"],1e-12)
                if direction=="BUY":
                    failure_level=max(float(prior_zone.get("zone_high",0) or 0), float(opposite_open["entry_high"] or 0))
                    failure_reclaimed = failure_level>0 and c15["price"] > failure_level + 0.04*a15
                else:
                    vals=[x for x in (float(prior_zone.get("zone_low",0) or 0), float(opposite_open["entry_low"] or 0)) if x>0]
                    failure_level=min(vals) if vals else None
                    failure_reclaimed = bool(failure_level and c15["price"] < failure_level - 0.04*a15)

            reversal_context=bool(
                opposite_open and failure_reclaimed and structure_live and
                reversal_flow>=REVERSAL_TRIGGER_FLOW_MIN and
                (reclaim_live or max(float(m5["momentum"]),float(m15["momentum"]))>=60)
            )

            # 4H للموقع الطبيعي. أما عند فشل صفقة معاكسة فلا نشترط الوصول إلى
            # منطقة 4H جديدة؛ نستخدم مستوى فشل السيناريو السابق كمنطقة انقلاب/إعادة اختبار.
            effective_zone=zone
            if reversal_context and failure_level:
                a15=max(c15["atr"],1e-12)
                a4=float(zone["atr4"])
                if direction=="BUY":
                    rz_low=failure_level-0.10*a15; rz_high=failure_level+0.14*a15
                else:
                    rz_low=failure_level-0.14*a15; rz_high=failure_level+0.10*a15
                effective_zone=dict(zone)
                effective_zone.update({
                    "zone_low":rz_low,"zone_high":rz_high,
                    "in_zone":rz_low<=c15["price"]<=rz_high,
                    "distance_atr4":abs(c15["price"]-failure_level)/max(a4,1e-12),
                    "zone_strength":max(72.0,float(zone["zone_strength"])),
                    "zone_kind":"REVERSAL_RETEST",
                    "failure_level":failure_level,
                })

            dist=float(effective_zone["distance_atr4"]); near_zone=dist<=ZONE_NEAR_ATR4; in_zone=bool(effective_zone["in_zone"])
            if direction=="BUY": chase=(c15["price"]-float(effective_zone["zone_high"]))/max(c15["atr"],1e-12)
            else: chase=(float(effective_zone["zone_low"])-c15["price"])/max(c15["atr"],1e-12)

            # المسار الطبيعي: لا مطاردة ولا إشارة بعيدة عن منطقة 4H.
            # مسار الانقلاب: نسمح بإشعار تحول حتى لو ابتعد قليلًا، لكن الدخول يبقى على إعادة الاختبار.
            if not reversal_context:
                if chase>MAX_CHASE_ATR15 and not in_zone:
                    continue
                if not near_zone:
                    continue
            else:
                if max(0.0,chase)>REVERSAL_WATCH_MAX_ATR15:
                    continue

            htf_penalty=max(0.0,48.0-s4)*0.45 + max(0.0,48.0-float(zone["structure_score"]))*0.30
            live_reversal = reversal_context or (structure_live and reclaim_live and flow_confirm)

            if reversal_context:
                # قريب من مستوى الفشل = دخول مؤكد. إذا تحرك بعيدًا نرسل التحول لكن ننتظر retest.
                if max(0.0,chase)<=0.18 and reversal_flow>=62 and (m5["bos"] or m15["bos"]):
                    stage="EXPLOSION"
                elif max(0.0,chase)<=REVERSAL_MAX_CHASE_ATR15:
                    stage="CONFIRMED"
                else:
                    stage="EARLY"
            elif strong_reject and live_reversal:
                stage="EXPLOSION"
            elif (reject_confirm and (flow_confirm or structure_live)) or (live_reversal and dist<=0.20):
                stage="CONFIRMED"
            elif (in_zone or dist<=0.12) and max(s5,s15,float(m5["momentum"]))>=55:
                stage="EARLY"
            else:
                continue

            # stats(17): SELL EARLY produced very little favorable excursion, while SELL CONFIRMED
            # was massively over-represented.  Do not weaken BUY.  For normal SELL we now require
            # actual buyer failure (rejection + micro structure + reclaim/flow) before calling it confirmed.
            # Live reversal SELL keeps its dedicated reversal rules above.
            if direction == "SELL" and not reversal_context:
                if stage == "EARLY" and not SELL_EARLY_ALERTS:
                    continue
                if stage == "CONFIRMED":
                    buyer_failure = (
                        rejection >= SELL_CONFIRMED_REJECTION_MIN
                        and structure_live
                        and (reclaim_live or flow >= SELL_CONFIRMED_FLOW_MIN)
                        and max(s5, s15, float(m5["momentum"])) >= SELL_CONFIRMED_TRIGGER_MIN
                        and (in_zone or dist <= SELL_CONFIRMED_ZONE_MAX_ATR4)
                    )
                    if not buyer_failure:
                        continue
                    recipe.insert(0, "فشل المشترين مؤكد قبل اعتماد SELL")

            explosion_score=clamp(0.30*s5+0.22*s15+0.16*float(m5["momentum"])+0.12*float(m15["momentum"])+0.10*s1+0.10*s4)
            entry_score=clamp(0.28*float(effective_zone["zone_strength"])+0.22*rejection+0.20*max(flow,reversal_flow)+0.15*float(m5["momentum"])+0.15*ob["imbalance"]-max(0,chase)*20)
            safety_score=clamp(0.22*s4+0.18*float(zone["structure_score"])+0.16*s1+0.15*(100-ob["spoof_risk"])+0.10*o15["oi_score"]+0.07*fund["funding_score"]+0.12*float(m15["momentum"])-htf_penalty)
            score=clamp((explosion_score+entry_score+safety_score)/3)

            recipe=[]
            if reversal_context:
                recipe.append(f"🔄 فشل سيناريو {('بيع' if direction=='BUY' else 'شراء')} سابق وتحولت السيطرة")
                recipe.append("تم كسر/استرداد مستوى إبطال السيناريو السابق")
                recipe.append("منطقة الدخول الجديدة = إعادة اختبار مستوى الانقلاب")
            else:
                recipe.append(f"تحليل 4H: منطقة {('طلب/شراء' if direction=='BUY' else 'عرض/بيع')} قبل الزناد")
                recipe.append("4H سياق وموقع وليس قفل اتجاه")
            if in_zone: recipe.append("السعر داخل منطقة الدخول")
            else: recipe.append(f"السعر يبعد {dist:.2f} ATR4 عن منطقة الدخول")
            if bool(r5["sweep"]) or bool(r15["sweep"]): recipe.append("سحب سيولة ثم استرداد/رفض")
            if structure_live: recipe.append("BOS/CHOCH حي على 5M/15M")
            if reclaim_live: recipe.append("استرداد مستوى دقيق بعد الرفض")
            if flow_confirm or reversal_flow>=REVERSAL_TRIGGER_FLOW_MIN: recipe.append("Delta/CVD والزخم يؤكد تحول السيطرة")
            if ob["absorption"]>=60: recipe.append("امتصاص محتمل عند المنطقة")
            if max(0,chase)<=REVERSAL_MAX_CHASE_ATR15: recipe.append("Anti-Late: الدخول غير مطارد")
            elif reversal_context: recipe.append("السعر ابتعد؛ انتظار إعادة اختبار منطقة الانقلاب")

            plan=professional_trade_plan(direction,c15["price"],effective_zone,max(c15["atr"],1e-12),m5,m15)
            details={
                "zone_4h":effective_zone,"original_zone_4h":zone,
                "rejection_5m":r5,"rejection_15m":r15,"micro_5m":m5,"micro_15m":m15,
                "chase_atr15":chase,"flow_score":max(flow,reversal_flow),"orderbook":ob,"oi_5m":o5,"oi_15m":o15,"oi_1h":o1,"funding":fund,
                "features_5m":c5,"features_15m":c15,"features_1h":c1,"features_4h":c4,
                "is_live_reversal":bool(reversal_context),"reversal_context":bool(reversal_context),
                "failure_level":failure_level,"failure_reclaimed":failure_reclaimed,
            }
            sig=Signal(symbol=symbol,direction=direction,stage=stage,score=score,
                explosion_score=explosion_score,entry_score=entry_score,safety_score=safety_score,
                scores_by_tf={"5m":s5,"15m":s15,"1h":s1,"4h":s4},price=c15["price"],
                entry_low=plan[0],entry_high=plan[1],stop=plan[2],tp1=plan[3],tp2=plan[4],tp3=plan[5],
                rr1=plan[6],rr2=plan[7],rr3=plan[8],recipe=recipe,details=details)
            candidates.append(sig)
            raw[direction]={"flow":flow,"reversal_flow":reversal_flow,"live_reversal":live_reversal,
                            "reversal_context":reversal_context,"structure_live":structure_live,
                            "reclaim_live":reclaim_live,"score":score,"entry":entry_score}

        if not candidates:
            return []

        # انقلاب مرتبط بصفقة سابقة له أولوية على مرشح عادي معاكس.
        reversals=[x for x in candidates if x.details.get("reversal_context")]
        if reversals:
            reversals.sort(key=lambda x:(x.stage=="EXPLOSION",x.stage=="CONFIRMED",x.entry_score+x.explosion_score),reverse=True)
            return [reversals[0]]

        bydir={x.direction:x for x in candidates}
        if "BUY" in bydir and "SELL" in bydir:
            buy,sell=bydir["BUY"],bydir["SELL"]
            edge=(buy.entry_score+buy.explosion_score)-(sell.entry_score+sell.explosion_score)
            if raw["BUY"]["live_reversal"] and edge>=REVERSAL_EDGE_MIN:
                buy.details["is_live_reversal"]=True
                buy.recipe.insert(0,"🔄 فشل السيناريو البيعي وتحولت السيطرة إلى شراء")
                return [buy]
            if raw["SELL"]["live_reversal"] and edge<=-REVERSAL_EDGE_MIN:
                sell.details["is_live_reversal"]=True
                sell.recipe.insert(0,"🔄 فشل السيناريو الشرائي وتحولت السيطرة إلى بيع")
                return [sell]
            candidates.sort(key=lambda x:(x.entry_score+x.explosion_score,x.safety_score),reverse=True)
            return [candidates[0]]

        return [candidates[0]]

    async def track_open_positions(self):
        while self.running:
            try:
                async with aiosqlite.connect(DB_PATH) as db:
                    db.row_factory = aiosqlite.Row
                    rows = await (await db.execute(
                        "SELECT * FROM opportunities WHERE status='OPEN' ORDER BY id DESC LIMIT 500"
                    )).fetchall()
                if not rows:
                    await asyncio.sleep(60)
                    continue

                prices = {
                    x["symbol"]: float(x["price"])
                    for x in await self.client.get("/fapi/v1/ticker/price")
                    if "symbol" in x and "price" in x
                }
                async with aiosqlite.connect(DB_PATH) as db:
                    for r in rows:
                        p = prices.get(r["symbol"])
                        if not p:
                            continue
                        direction = r["direction"]
                        mid = (r["entry_low"] + r["entry_high"]) / 2
                        entered = r["entered_at"] is not None
                        in_zone = r["entry_low"] <= p <= r["entry_high"]

                        if not entered and in_zone:
                            await db.execute(
                                "UPDATE opportunities SET entered_at=?,entered_price=?,best_price=?,worst_price=? WHERE id=?",
                                (now_local().isoformat(), p, p, p, r["id"])
                            )
                            entered = True

                        if not entered:
                            continue

                        best = r["best_price"] if r["best_price"] is not None else p
                        worst = r["worst_price"] if r["worst_price"] is not None else p
                        best = max(best,p) if direction=="BUY" else min(best,p)
                        worst = min(worst,p) if direction=="BUY" else max(worst,p)
                        mfe = pct_change(best, mid) * (1 if direction=="BUY" else -1)
                        mae = pct_change(worst, mid) * (-1 if direction=="BUY" else 1)

                        updates = {"best_price":best, "worst_price":worst, "mfe_pct":max(0,mfe), "mae_pct":max(0,mae)}
                        hit_stop = p <= r["stop"] if direction=="BUY" else p >= r["stop"]
                        hit1 = p >= r["tp1"] if direction=="BUY" else p <= r["tp1"]
                        hit2 = p >= r["tp2"] if direction=="BUY" else p <= r["tp2"]
                        hit3 = p >= r["tp3"] if direction=="BUY" else p <= r["tp3"]
                        ts = now_local().isoformat()

                        if hit1 and not r["tp1_at"]: updates["tp1_at"] = ts
                        if hit2 and not r["tp2_at"]: updates["tp2_at"] = ts
                        if hit3 and not r["tp3_at"]:
                            updates.update({"tp3_at":ts,"closed_at":ts,"status":"CLOSED","outcome":"TP3"})
                        elif hit_stop and not r["stop_at"]:
                            outcome = "SL_AFTER_TP" if (r["tp1_at"] or hit1) else "SL"
                            updates.update({"stop_at":ts,"closed_at":ts,"status":"CLOSED","outcome":outcome})

                        set_sql = ", ".join(f"{k}=?" for k in updates)
                        await db.execute(
                            f"UPDATE opportunities SET {set_sql},updated_at=? WHERE id=?",
                            (*updates.values(), ts, r["id"])
                        )
                    await db.commit()
            except Exception:
                log.exception("Position tracker failed")
            await asyncio.sleep(60)


engine = Engine()


# =========================================================
# واجهة المتابعة
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await engine.start()
    yield
    await engine.close()

app = FastAPI(title="Ahmed Early Explosion Trader", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "ok": engine.last_error is None,
        "service": "Ahmed Early Explosion Trader",
        "last_scan": engine.last_scan,
        "last_error": engine.last_error,
        "scan_number": engine.scan_no,
        "symbols": engine.symbol_count,
        "candidates": engine.candidate_count,
        "alerts_sent_since_start": engine.alert_count,
        "time": now_local().isoformat(),
    }


@app.get("/opportunities")
async def opportunities(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute(
            "SELECT * FROM opportunities ORDER BY id DESC LIMIT ?", (limit,)
        )).fetchall()
    return [dict(r) for r in rows]


@app.get("/checkpoints")
async def checkpoints(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute(
            "SELECT * FROM checkpoints ORDER BY id DESC LIMIT ?", (limit,)
        )).fetchall()
    return [dict(r) for r in rows]


@app.get("/stats")
async def stats():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        overall = await (await db.execute("""
            SELECT COUNT(*) total,
                   SUM(status='OPEN') open_count,
                   SUM(outcome='TP3') tp3,
                   SUM(outcome='SL') sl,
                   AVG(mfe_pct) avg_mfe,
                   AVG(mae_pct) avg_mae
            FROM opportunities
        """)).fetchone()
        by_group = await (await db.execute("""
            SELECT direction,current_stage,COUNT(*) cases,
                   SUM(outcome='TP3') tp3,
                   SUM(outcome='SL') sl,
                   AVG(mfe_pct) avg_mfe,
                   AVG(mae_pct) avg_mae
            FROM opportunities
            GROUP BY direction,current_stage
        """)).fetchall()
    return {"overall": dict(overall), "groups": [dict(x) for x in by_group]}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    h = await health()
    s = await stats()
    status = "يعمل ✅" if h["ok"] else "يوجد خطأ ⚠️"
    return f"""<!doctype html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ahmed Early Explosion Trader</title>
<style>
body{{font-family:Arial;background:#0b1020;color:#eef2ff;margin:0;padding:24px}}
.wrap{{max-width:1000px;margin:auto}}
.card{{background:#151c33;border:1px solid #2a3558;border-radius:16px;padding:18px;margin:12px 0}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}}
.k{{font-size:13px;color:#aab4d6}} .v{{font-size:24px;font-weight:bold;margin-top:6px}}
a{{color:#8bb8ff}} code{{color:#f6c86f}}
</style></head>
<body><div class="wrap">
<h1>Ahmed Early Explosion Trader</h1>
<div class="card"><b>الحالة: {status}</b><br>آخر فحص: {h["last_scan"] or "لم يبدأ"}</div>
<div class="grid">
<div class="card"><div class="k">رقم الفحص</div><div class="v">{h["scan_number"]}</div></div>
<div class="card"><div class="k">العقود</div><div class="v">{h["symbols"]}</div></div>
<div class="card"><div class="k">المرشحون العميقون</div><div class="v">{h["candidates"]}</div></div>
<div class="card"><div class="k">التنبيهات</div><div class="v">{h["alerts_sent_since_start"]}</div></div>
<div class="card"><div class="k">الفرص الكلية</div><div class="v">{s["overall"].get("total") or 0}</div></div>
<div class="card"><div class="k">الفرص المفتوحة</div><div class="v">{s["overall"].get("open_count") or 0}</div></div>
</div>
<div class="card">
<h3>الروابط</h3>
<a href="/health">Health</a> ·
<a href="/opportunities">Opportunities</a> ·
<a href="/stats">Stats</a> ·
<a href="/checkpoints">Checkpoints</a>
</div>
<div class="card">التحليل الرئيسي: <code>4H</code> · الزناد الداخلي: <code>5M / 15M</code><br>
لا ينفذ البوت صفقات تلقائيًا، وجميع الخطط إحصائية مقترحة.</div>
</div></body></html>"""


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
