
import asyncio
import html
import json
import logging
import math
import os
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse


# =========================================================
# Railway settings
# =========================================================

BINANCE_BASE = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

PORT = int(os.getenv("PORT", "8080"))
TZ = ZoneInfo(os.getenv("TZ", "Asia/Riyadh"))

SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "15"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "16"))
RADAR_POOL = int(os.getenv("RADAR_POOL", "180"))
DEEP_CANDIDATES = int(os.getenv("DEEP_CANDIDATES", "60"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "750000"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "2"))
TELEGRAM_MIN_INTERVAL_SECONDS = float(os.getenv("TELEGRAM_MIN_INTERVAL_SECONDS", "2.5"))

# Source mapping requested by the user.
SOURCE_1H = os.getenv("DELTA_SOURCE_1H", "5m")
SOURCE_4H = os.getenv("DELTA_SOURCE_4H", "15m")

NORMAL_1H_BARS = int(os.getenv("NORMAL_1H_CONFIRM_BARS", "3"))
FAST_1H_BARS = int(os.getenv("FAST_1H_CONFIRM_BARS", "2"))
NORMAL_4H_BARS = int(os.getenv("NORMAL_4H_CONFIRM_BARS", "2"))
FAST_4H_BARS = int(os.getenv("FAST_4H_CONFIRM_BARS", "1"))

# Flexible relative abnormal-candle logic.
ABNORMAL_LOOKBACK = int(os.getenv("ABNORMAL_LOOKBACK", "20"))
ABNORMAL_BODY_PERCENTILE = float(os.getenv("ABNORMAL_BODY_PERCENTILE", "0.80"))
ABNORMAL_VOLUME_PERCENTILE = float(os.getenv("ABNORMAL_VOLUME_PERCENTILE", "0.75"))
ABNORMAL_RANGE_PERCENTILE = float(os.getenv("ABNORMAL_RANGE_PERCENTILE", "0.80"))
ABNORMAL_CLOSE_LOCATION = float(os.getenv("ABNORMAL_CLOSE_LOCATION", "0.68"))
ABNORMAL_MIN_COMPONENTS = int(os.getenv("ABNORMAL_MIN_COMPONENTS", "2"))

# Ordinary progressive path.
NORMAL_MIN_DELTA_PCT = float(os.getenv("NORMAL_MIN_DELTA_PCT", "8"))
NORMAL_MIN_DIRECTIONAL_BARS = int(os.getenv("NORMAL_MIN_DIRECTIONAL_BARS", "2"))
FAST_MIN_DELTA_PCT = float(os.getenv("FAST_MIN_DELTA_PCT", "18"))
MIN_DELTA_SLOPE = float(os.getenv("MIN_DELTA_SLOPE", "2.0"))

# Flexible scoring.
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "52"))
MIN_DIRECTION_GAP = float(os.getenv("MIN_DIRECTION_GAP", "8"))
MAX_1H_SIGNAL_EXTENSION_ATR = float(os.getenv("MAX_1H_SIGNAL_EXTENSION_ATR", "0.65"))
MAX_4H_SIGNAL_EXTENSION_ATR = float(os.getenv("MAX_4H_SIGNAL_EXTENSION_ATR", "0.80"))
MAX_1H_AGE_MINUTES = int(os.getenv("MAX_1H_AGE_MINUTES", "45"))
MAX_4H_AGE_MINUTES = int(os.getenv("MAX_4H_AGE_MINUTES", "180"))

DELTA_LEN = int(os.getenv("DELTA_LEN", "14"))
DELTA_MULT = float(os.getenv("DELTA_MULT", "2.0"))
DELTA_MIN_VOL = float(os.getenv("DELTA_MIN_VOL", "1.10"))

COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "90"))
BINANCE_RETRIES = int(os.getenv("BINANCE_RETRIES", "4"))
SYMBOL_TIMEOUT = float(os.getenv("SYMBOL_TIMEOUT", "15"))
SCAN_TIMEOUT = float(os.getenv("SCAN_TIMEOUT", "60"))
EXCHANGE_CACHE_SECONDS = int(os.getenv("EXCHANGE_CACHE_SECONDS", "3600"))
PRICE_CACHE_SECONDS = int(os.getenv("PRICE_CACHE_SECONDS", "30"))

SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
SEND_TEST_MESSAGE = os.getenv("SEND_TEST_MESSAGE", "true").lower() == "true"
ENABLE_MANUAL_TEST_ENDPOINT = os.getenv("ENABLE_MANUAL_TEST_ENDPOINT", "true").lower() == "true"

DB_PATH = os.getenv("DB_PATH", "data/delta_early_predictor.db")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("delta-early-predictor")


# =========================================================
# Helpers
# =========================================================

def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


def pct_change(a: float, b: float) -> float:
    return safe_div(a - b, abs(b), 0.0) * 100.0


def now_local() -> datetime:
    return datetime.now(TZ)


def fmt_price(value: float) -> str:
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:.5f}".rstrip("0").rstrip(".")
    if value >= 0.01:
        return f"{value:.7f}".rstrip("0").rstrip(".")
    return f"{value:.10f}".rstrip("0").rstrip(".")


def fmt_compact(value: float) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.2f}"


def percentile_rank(value: float, sample: list[float]) -> float:
    if not sample:
        return 0.5
    return sum(item <= value for item in sample) / len(sample)


def ema(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (length + 1.0)
    output = [values[0]]
    for value in values[1:]:
        output.append(alpha * value + (1.0 - alpha) * output[-1])
    return output


def atr(rows: list[list[Any]], length: int = 14) -> float:
    if len(rows) < 2:
        return 0.0
    values = []
    for index in range(1, len(rows)):
        high = float(rows[index][2])
        low = float(rows[index][3])
        previous_close = float(rows[index - 1][4])
        values.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    return sum(values[-length:]) / max(1, min(length, len(values)))


def unpack(rows: list[list[Any]]) -> dict[str, list[float]]:
    return {
        "o": [float(row[1]) for row in rows],
        "h": [float(row[2]) for row in rows],
        "l": [float(row[3]) for row in rows],
        "c": [float(row[4]) for row in rows],
        "v": [float(row[5]) for row in rows],
        "q": [float(row[7]) for row in rows],
        "n": [float(row[8]) for row in rows],
        "tb": [float(row[9]) for row in rows],
        "tq": [float(row[10]) for row in rows],
    }


def candle_delta(row: list[Any]) -> float:
    volume = float(row[5])
    taker_buy = float(row[9])
    return (2.0 * taker_buy) - volume


def candle_delta_pct(row: list[Any]) -> float:
    volume = float(row[5])
    return safe_div(candle_delta(row), max(volume, 1e-12), 0.0) * 100.0


def directional_candle(row: list[Any], direction: str) -> bool:
    open_price = float(row[1])
    close = float(row[4])
    return close > open_price if direction == "BUY" else close < open_price


def close_location(row: list[Any], direction: str) -> float:
    high = float(row[2])
    low = float(row[3])
    close = float(row[4])
    position = safe_div(close - low, max(high - low, 1e-12), 0.5)
    return position if direction == "BUY" else 1.0 - position


def abnormal_candle(rows: list[list[Any]], index: int, direction: str) -> tuple[bool, dict[str, Any]]:
    if index < ABNORMAL_LOOKBACK:
        return False, {}

    row = rows[index]
    history = rows[index - ABNORMAL_LOOKBACK:index]

    body = abs(float(row[4]) - float(row[1]))
    candle_range = float(row[2]) - float(row[3])
    volume = float(row[5])
    delta_pct = candle_delta_pct(row)

    history_bodies = [abs(float(item[4]) - float(item[1])) for item in history]
    history_ranges = [float(item[2]) - float(item[3]) for item in history]
    history_volumes = [float(item[5]) for item in history]

    body_rank = percentile_rank(body, history_bodies)
    range_rank = percentile_rank(candle_range, history_ranges)
    volume_rank = percentile_rank(volume, history_volumes)
    close_loc = close_location(row, direction)
    direction_ok = directional_candle(row, direction)
    delta_ok = delta_pct >= FAST_MIN_DELTA_PCT if direction == "BUY" else delta_pct <= -FAST_MIN_DELTA_PCT

    previous_high = max(float(item[2]) for item in history[-8:])
    previous_low = min(float(item[3]) for item in history[-8:])
    breakout = float(row[4]) > previous_high if direction == "BUY" else float(row[4]) < previous_low

    components = [
        body_rank >= ABNORMAL_BODY_PERCENTILE,
        range_rank >= ABNORMAL_RANGE_PERCENTILE,
        volume_rank >= ABNORMAL_VOLUME_PERCENTILE,
        close_loc >= ABNORMAL_CLOSE_LOCATION,
        breakout,
        delta_ok,
    ]
    count = sum(components)

    # The direction and delta must agree; only the remaining relative components are flexible.
    active = direction_ok and delta_ok and count >= ABNORMAL_MIN_COMPONENTS + 1

    return active, {
        "body_rank": body_rank,
        "range_rank": range_rank,
        "volume_rank": volume_rank,
        "close_location": close_loc,
        "breakout": breakout,
        "delta_pct": delta_pct,
        "components": count,
    }


def target_candle_age_minutes(target_tf: str, now_ms: int) -> float:
    seconds = now_ms / 1000
    if target_tf == "1h":
        boundary = 3600
    else:
        boundary = 14400
    return (seconds % boundary) / 60.0


def aggregate_target_open(rows: list[list[Any]], target_tf: str) -> float:
    if not rows:
        return 0.0
    target_ms = 3600_000 if target_tf == "1h" else 14_400_000
    current_open = int(rows[-1][0])
    target_start = current_open - (current_open % target_ms)
    matching = [row for row in rows if int(row[0]) >= target_start]
    return float(matching[0][1]) if matching else float(rows[-1][1])


def extension_atr(rows: list[list[Any]], target_tf: str, direction: str) -> float:
    price = float(rows[-1][4])
    target_open = aggregate_target_open(rows, target_tf)
    source_atr = atr(rows, 14)
    bars_per_target = 12 if target_tf == "1h" else 16
    estimated_target_atr = max(source_atr * math.sqrt(bars_per_target), 1e-12)
    signed_move = price - target_open if direction == "BUY" else target_open - price
    return max(0.0, signed_move / estimated_target_atr)


def delta_path(rows: list[list[Any]], bar_count: int, direction: str) -> dict[str, Any]:
    closed = rows[:-1] if len(rows) > 1 else rows
    selected = closed[-bar_count:]
    if len(selected) < bar_count:
        return {}

    signed = [
        candle_delta_pct(row) if direction == "BUY" else -candle_delta_pct(row)
        for row in selected
    ]
    directional = [directional_candle(row, direction) for row in selected]

    slope = signed[-1] - signed[0] if len(signed) > 1 else signed[-1]
    positive_count = sum(value >= NORMAL_MIN_DELTA_PCT for value in signed)
    average_delta = sum(signed) / len(signed)
    increasing_steps = sum(signed[i] >= signed[i - 1] for i in range(1, len(signed)))

    return {
        "signed_values": signed,
        "positive_count": positive_count,
        "directional_count": sum(directional),
        "average_delta": average_delta,
        "slope": slope,
        "increasing_steps": increasing_steps,
    }


def ma_alignment(rows: list[list[Any]], direction: str) -> bool:
    closes = [float(row[4]) for row in rows]
    if len(closes) < 55:
        return False
    e9 = ema(closes, 9)[-1]
    e21 = ema(closes, 21)[-1]
    e50 = ema(closes, 50)[-1]
    return e9 > e21 > e50 if direction == "BUY" else e9 < e21 < e50


def volume_spike(rows: list[list[Any]]) -> bool:
    if len(rows) < 21:
        return False
    current = float(rows[-2][5])
    average = sum(float(row[5]) for row in rows[-21:-2]) / 19
    return current >= average * DELTA_MIN_VOL


def score_prediction(
    rows: list[list[Any]],
    target_tf: str,
    direction: str,
    path_type: str,
    path: dict[str, Any],
    abnormal: dict[str, Any] | None,
    book_score: float,
    oi_change: float,
) -> tuple[float, list[str]]:
    score = 45.0
    recipe = ["DeltaIndicator"]

    if path_type == "NORMAL":
        score += min(18, max(0, path.get("average_delta", 0)) * 0.45)
        score += min(10, max(0, path.get("slope", 0)) * 0.60)
        score += path.get("directional_count", 0) * 2.0
    elif path_type == "FAST_TWO":
        score += 14
        score += min(12, max(0, path.get("average_delta", 0)) * 0.35)
        recipe.append("AbnormalCandles")
    else:
        score += 17
        if abnormal:
            score += abnormal.get("components", 0) * 1.5
            score += max(0, abnormal.get("delta_pct", 0) if direction == "BUY" else -abnormal.get("delta_pct", 0)) * 0.15
        recipe.append("AbnormalCandle")

    if ma_alignment(rows, direction):
        score += 5
        recipe.append("MAAlignment")
    if volume_spike(rows):
        score += 5
        recipe.append("VolumeSpike")
    if book_score >= 55:
        score += 4
        recipe.append("OrderBook")
    if oi_change > 0:
        score += min(4, oi_change * 4)
        recipe.append("OI")

    return clamp(score), recipe


# =========================================================
# Signal model and database
# =========================================================

@dataclass
class Signal:
    symbol: str
    target_tf: str
    source_tf: str
    direction: str
    price: float
    score: float
    path_type: str
    extension: float
    age_minutes: float
    oi_value: float
    oi_change: float
    funding: float
    recipe: list[str]
    trigger_open_time: int


SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    target_tf TEXT NOT NULL,
    source_tf TEXT NOT NULL,
    direction TEXT NOT NULL,
    trigger_open_time INTEGER NOT NULL,
    price REAL NOT NULL,
    score REAL NOT NULL,
    path_type TEXT NOT NULL,
    extension REAL,
    age_minutes REAL,
    oi_value REAL,
    oi_change REAL,
    funding REAL,
    recipe_json TEXT,
    status TEXT DEFAULT 'OPEN',
    result_5m INTEGER,
    result_15m INTEGER,
    result_30m INTEGER,
    result_60m INTEGER,
    checked_5m INTEGER DEFAULT 0,
    checked_15m INTEGER DEFAULT 0,
    checked_30m INTEGER DEFAULT 0,
    checked_60m INTEGER DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_unique
ON alerts(symbol,target_tf,direction,trigger_open_time,path_type);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    scan_number INTEGER,
    symbols_total INTEGER,
    candidates_total INTEGER,
    analyzed_total INTEGER,
    opportunities_total INTEGER,
    alerts_sent INTEGER,
    scan_seconds REAL,
    error TEXT
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()


async def insert_alert(signal: Signal) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        # Prevent repeated alerts for the same symbol/timeframe/direction during cooldown.
        cutoff = now_local().timestamp() - COOLDOWN_MINUTES * 60
        row = await (await db.execute(
            """SELECT created_at FROM alerts
               WHERE symbol=? AND target_tf=? AND direction=?
               ORDER BY id DESC LIMIT 1""",
            (signal.symbol, signal.target_tf, signal.direction),
        )).fetchone()
        if row:
            try:
                if datetime.fromisoformat(row[0]).timestamp() >= cutoff:
                    return False
            except (TypeError, ValueError):
                pass
        try:
            await db.execute(
                """INSERT INTO alerts (
                    created_at,symbol,target_tf,source_tf,direction,trigger_open_time,
                    price,score,path_type,extension,age_minutes,oi_value,oi_change,funding,recipe_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    now_local().isoformat(),
                    signal.symbol,
                    signal.target_tf,
                    signal.source_tf,
                    signal.direction,
                    signal.trigger_open_time,
                    signal.price,
                    signal.score,
                    signal.path_type,
                    signal.extension,
                    signal.age_minutes,
                    signal.oi_value,
                    signal.oi_change,
                    signal.funding,
                    json.dumps(signal.recipe, ensure_ascii=False),
                ),
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def historical_stats(signal: Signal) -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                """SELECT * FROM alerts
                   WHERE target_tf=? AND direction=? AND path_type=?
                   AND id NOT IN (SELECT MAX(id) FROM alerts)
                   ORDER BY id DESC LIMIT 500""",
                (signal.target_tf, signal.direction, signal.path_type),
            )
        ).fetchall()

    cases = len(rows)
    result = {"cases": cases, "historical": None, "5": None, "15": None, "30": None, "60": None}
    if not rows:
        return result

    horizon_rates = {}
    for horizon in ("5", "15", "30", "60"):
        values = [row[f"result_{horizon}m"] for row in rows if row[f"checked_{horizon}m"]]
        horizon_rates[horizon] = 100 * sum(values) / len(values) if values else None

    checked_60 = [row["result_60m"] for row in rows if row["checked_60m"]]
    result["historical"] = 100 * sum(checked_60) / len(checked_60) if checked_60 else None
    result.update(horizon_rates)
    return result


async def record_checkpoint(scan_no, symbols, candidates, analyzed, opportunities, alerts, seconds, error=None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO checkpoints
            (created_at,scan_number,symbols_total,candidates_total,analyzed_total,
             opportunities_total,alerts_sent,scan_seconds,error)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                now_local().isoformat(), scan_no, symbols, candidates, analyzed,
                opportunities, alerts, seconds, error,
            ),
        )
        await db.commit()


# =========================================================
# Binance client
# =========================================================

class BinanceClient:
    def __init__(self):
        self.session: aiohttp.ClientSession | None = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self.symbol_cache: tuple[float, list[str]] | None = None
        self.price_cache: tuple[float, dict[str, float]] | None = None

    async def start(self):
        connector = aiohttp.TCPConnector(limit=max(40, MAX_CONCURRENCY * 3), ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20, connect=8),
            connector=connector,
            headers={"User-Agent": "Ahmed-Delta-Early-Predictor/1.0"},
        )

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def get(self, path: str, params=None):
        assert self.session is not None
        last_error = None
        async with self.semaphore:
            for attempt in range(BINANCE_RETRIES):
                try:
                    async with self.session.get(BINANCE_BASE + path, params=params) as response:
                        if response.status in (418, 429):
                            retry_after_raw = response.headers.get("Retry-After", "")
                            try:
                                retry_after = float(retry_after_raw)
                            except (TypeError, ValueError):
                                retry_after = min(2 ** attempt + 1, 12)
                            last_error = RuntimeError(
                                f"Binance rate limit HTTP {response.status}; retry_after={retry_after}"
                            )
                            await asyncio.sleep(min(max(retry_after, 1.0), 20.0))
                            continue
                        response.raise_for_status()
                        data = await response.json(content_type=None)
                        if data is None:
                            raise RuntimeError("Binance returned null")
                        return data
                except Exception as error:
                    last_error = error
                    if attempt < BINANCE_RETRIES - 1:
                        await asyncio.sleep(min(1.5 ** attempt, 8))
        raise RuntimeError(f"Binance request failed {path}: {last_error!r}")

    async def symbols(self) -> list[str]:
        if self.symbol_cache and time.time() - self.symbol_cache[0] < EXCHANGE_CACHE_SECONDS:
            return self.symbol_cache[1]
        data = await self.get("/fapi/v1/exchangeInfo")
        symbols = [
            item["symbol"]
            for item in data.get("symbols", [])
            if item.get("status") == "TRADING"
            and item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
        ]
        if not symbols:
            raise RuntimeError("No USDT perpetual symbols")
        self.symbol_cache = (time.time(), symbols)
        return symbols

    async def tickers(self):
        data = await self.get("/fapi/v1/ticker/24hr")
        if not isinstance(data, list):
            raise RuntimeError("Invalid tickers")
        return data

    async def klines(self, symbol: str, interval: str, limit: int = 80):
        data = await self.get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        if not isinstance(data, list) or len(data) < 30:
            raise RuntimeError(f"Invalid klines {symbol} {interval}")
        return data

    async def oi_hist(self, symbol: str, period: str):
        data = await self.get(
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": 5},
        )
        return data if isinstance(data, list) else []

    async def premium(self, symbol: str):
        data = await self.get("/fapi/v1/premiumIndex", {"symbol": symbol})
        return data if isinstance(data, dict) else {}

    async def depth(self, symbol: str):
        data = await self.get("/fapi/v1/depth", {"symbol": symbol, "limit": 20})
        return data if isinstance(data, dict) else {}

    async def prices(self) -> dict[str, float]:
        if self.price_cache and time.time() - self.price_cache[0] < PRICE_CACHE_SECONDS:
            return self.price_cache[1]

        try:
            data = await self.get("/fapi/v1/ticker/price")
            prices = {
                item["symbol"]: float(item["price"])
                for item in data
                if item.get("symbol") and item.get("price")
            }
            if prices:
                self.price_cache = (time.time(), prices)
                return prices
        except Exception:
            if self.price_cache:
                log.warning("Using cached Binance prices after temporary request failure")
                return self.price_cache[1]
            raise

        if self.price_cache:
            return self.price_cache[1]
        return {}


def orderbook_score(depth: dict, direction: str) -> float:
    bids = sum(float(price) * float(quantity) for price, quantity in depth.get("bids", []))
    asks = sum(float(price) * float(quantity) for price, quantity in depth.get("asks", []))
    imbalance = safe_div(bids - asks, bids + asks, 0.0)
    signed = imbalance if direction == "BUY" else -imbalance
    return clamp(50 + signed * 150)


def oi_values(rows: list[dict]) -> tuple[float, float]:
    if not rows:
        return 0.0, 0.0
    values = [float(item.get("sumOpenInterest", 0) or 0) for item in rows]
    current = values[-1]
    change = pct_change(current, values[-2]) if len(values) >= 2 else 0.0
    return current, change


# =========================================================
# Engine
# =========================================================

class Engine:
    def __init__(self):
        self.client = BinanceClient()
        self.telegram_session: aiohttp.ClientSession | None = None
        self.running = True
        self.scan_no = 0
        self.symbol_count = 0
        self.candidate_count = 0
        self.alert_count = 0
        self.last_scan = None
        self.last_error = None
        self.fast_state: dict[str, dict[str, float]] = {}
        self.telegram_lock = asyncio.Lock()
        self.last_telegram_send_at = 0.0

    async def start(self):
        await init_db()
        await self.client.start()
        self.telegram_session = aiohttp.ClientSession()

        if SEND_STARTUP_MESSAGE:
            await self.send_telegram(
                "✅ <b>Ahmed Delta Early Predictor v1.2 FINAL بدأ العمل</b>\n\n"
                "🎯 1H يُتوقع من 5M\n"
                "🎯 4H يُتوقع من 15M\n"
                "✅ 3 شموع عادية أو شمعتان قويتان أو شمعة غير طبيعية\n"
                "🔒 فلتر يمنع الإشارة المتأخرة\n"
                "⚠️ لا ينفذ صفقات تلقائيًا."
            )

        if SEND_TEST_MESSAGE:
            await self.send_telegram(
                "🧪 <b>رسالة اختبار ناجحة</b>\n\n"
                "✅ Telegram متصل\n"
                "✅ Railway يعمل\n"
                "✅ قاعدة البيانات جاهزة\n"
                "✅ محرك التوقع المبكر جاهز"
            )

        asyncio.create_task(self.loop())
        asyncio.create_task(self.evaluate_alerts())

    async def close(self):
        self.running = False
        await self.client.close()
        if self.telegram_session and not self.telegram_session.closed:
            await self.telegram_session.close()

    async def send_telegram(self, text: str) -> bool:
        if not BOT_TOKEN or not CHAT_ID or not self.telegram_session:
            log.warning("Telegram variables missing")
            return False

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        async with self.telegram_lock:
            for attempt in range(5):
                elapsed = time.monotonic() - self.last_telegram_send_at
                wait_before = TELEGRAM_MIN_INTERVAL_SECONDS - elapsed
                if wait_before > 0:
                    await asyncio.sleep(wait_before)

                try:
                    async with self.telegram_session.post(
                        url,
                        json=payload,
                        timeout=20,
                    ) as response:
                        body = await response.text()

                        if response.status == 200:
                            self.last_telegram_send_at = time.monotonic()
                            return True

                        if response.status == 429:
                            retry_after = 5.0
                            try:
                                data = json.loads(body)
                                retry_after = float(
                                    data.get("parameters", {}).get("retry_after", retry_after)
                                )
                            except (json.JSONDecodeError, TypeError, ValueError):
                                pass

                            log.warning(
                                "Telegram rate limited; waiting %.1f seconds",
                                retry_after,
                            )
                            await asyncio.sleep(min(max(retry_after, 1.0), 60.0))
                            continue

                        log.error("Telegram HTTP %s: %s", response.status, body)

                except Exception as error:
                    log.warning(
                        "Telegram attempt %s failed: %s",
                        attempt + 1,
                        error,
                    )

                await asyncio.sleep(min(2 ** attempt, 15))

        return False

    def radar_score(self, ticker: dict) -> tuple[float, dict[str, float]]:
        symbol = ticker.get("symbol", "")
        price = float(ticker.get("lastPrice", 0) or 0)
        volume = float(ticker.get("quoteVolume", 0) or 0)
        trades = float(ticker.get("count", 0) or 0)
        change = abs(float(ticker.get("priceChangePercent", 0) or 0))
        previous = self.fast_state.get(symbol)

        price_accel = volume_accel = trade_accel = 0.0
        if previous:
            price_accel = abs(pct_change(price, previous["price"]))
            volume_accel = max(0, pct_change(volume, previous["volume"]))
            trade_accel = max(0, pct_change(trades, previous["trades"]))

        liquidity = clamp((math.log10(max(volume, 1)) - 5.0) * 20)
        score = clamp(
            price_accel * 700
            + volume_accel * 4
            + trade_accel * 3
            + liquidity * 0.25
            + change * 1.2
        )
        return score, {"price": price, "volume": volume, "trades": trades}

    async def loop(self):
        while self.running:
            started = time.monotonic()
            self.scan_no += 1
            analyzed = opportunities = alerts = 0
            error = None
            try:
                alerts, analyzed, opportunities = await asyncio.wait_for(
                    self.scan(),
                    timeout=SCAN_TIMEOUT,
                )
                self.last_error = None
            except Exception as exception:
                error = repr(exception)
                self.last_error = error
                log.exception("Scan failed")

            elapsed = time.monotonic() - started
            self.last_scan = now_local().isoformat()
            await record_checkpoint(
                self.scan_no, self.symbol_count, self.candidate_count,
                analyzed, opportunities, alerts, elapsed, error,
            )
            log.info(
                "scan=%s symbols=%s candidates=%s analyzed=%s opportunities=%s alerts=%s seconds=%.1f",
                self.scan_no, self.symbol_count, self.candidate_count,
                analyzed, opportunities, alerts, elapsed,
            )
            await asyncio.sleep(max(3, SCAN_SECONDS - elapsed))

    async def scan(self):
        symbols, tickers = await asyncio.gather(
            self.client.symbols(),
            self.client.tickers(),
        )
        self.symbol_count = len(symbols)
        allowed = set(symbols)
        ranked = []

        for ticker in tickers:
            symbol = ticker.get("symbol")
            if symbol not in allowed:
                continue
            quote_volume = float(ticker.get("quoteVolume", 0) or 0)
            if quote_volume < MIN_QUOTE_VOLUME:
                continue
            score, state = self.radar_score(ticker)
            self.fast_state[symbol] = state
            ranked.append((score, quote_volume, symbol))

        ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
        candidates = [item[2] for item in ranked[:RADAR_POOL]][:DEEP_CANDIDATES]
        self.candidate_count = len(candidates)

        async def guarded(symbol: str):
            try:
                return await asyncio.wait_for(
                    self.analyze_symbol(symbol),
                    timeout=SYMBOL_TIMEOUT,
                )
            except Exception as error:
                log.debug("Analysis failed %s: %r", symbol, error)
                return []

        groups = await asyncio.gather(*(guarded(symbol) for symbol in candidates))
        signals = [signal for group in groups for signal in group]
        signals.sort(key=lambda item: (item.score, -item.extension), reverse=True)

        alerts = 0
        sent_symbols = set()
        for signal in signals:
            if alerts >= MAX_ALERTS_PER_SCAN:
                break
            if signal.symbol in sent_symbols:
                continue
            if not await insert_alert(signal):
                continue
            stats = await historical_stats(signal)
            if await self.send_telegram(self.message(signal, stats)):
                alerts += 1
                self.alert_count += 1
                sent_symbols.add(signal.symbol)

        return alerts, len(groups), len(signals)

    async def analyze_symbol(self, symbol: str) -> list[Signal]:
        k5, k15, oi5, oi15, premium, depth = await asyncio.gather(
            self.client.klines(symbol, SOURCE_1H, 80),
            self.client.klines(symbol, SOURCE_4H, 80),
            self.client.oi_hist(symbol, "5m"),
            self.client.oi_hist(symbol, "15m"),
            self.client.premium(symbol),
            self.client.depth(symbol),
        )

        funding = float(premium.get("lastFundingRate", 0) or 0) * 100
        signals = []

        for target_tf, rows, normal_bars, fast_bars, oi_rows in (
            ("1h", k5, NORMAL_1H_BARS, FAST_1H_BARS, oi5),
            ("4h", k15, NORMAL_4H_BARS, FAST_4H_BARS, oi15),
        ):
            source_tf = SOURCE_1H if target_tf == "1h" else SOURCE_4H
            oi_value, oi_change = oi_values(oi_rows)
            age = target_candle_age_minutes(target_tf, int(time.time() * 1000))
            max_age = MAX_1H_AGE_MINUTES if target_tf == "1h" else MAX_4H_AGE_MINUTES

            if age > max_age:
                continue

            for direction in ("BUY", "SELL"):
                path_type = None
                path = {}
                abnormal_details = None

                normal = delta_path(rows, normal_bars, direction)
                normal_valid = (
                    normal
                    and normal["positive_count"] >= NORMAL_MIN_DIRECTIONAL_BARS
                    and normal["directional_count"] >= NORMAL_MIN_DIRECTIONAL_BARS
                    and normal["average_delta"] >= NORMAL_MIN_DELTA_PCT
                    and normal["slope"] >= MIN_DELTA_SLOPE
                )

                fast = delta_path(rows, fast_bars, direction)
                abnormal_count = 0
                abnormal_candidates = []
                closed_last_index = len(rows) - 2
                for offset in range(fast_bars):
                    index = closed_last_index - offset
                    active, details = abnormal_candle(rows, index, direction)
                    if active:
                        abnormal_count += 1
                        abnormal_candidates.append(details)

                fast_two_valid = (
                    fast_bars >= 2
                    and fast
                    and abnormal_count >= fast_bars
                    and fast["average_delta"] >= FAST_MIN_DELTA_PCT
                )

                single_active, single_details = abnormal_candle(rows, closed_last_index, direction)

                if normal_valid:
                    path_type = "NORMAL"
                    path = normal
                if fast_two_valid:
                    path_type = "FAST_TWO"
                    path = fast
                    abnormal_details = abnormal_candidates[-1] if abnormal_candidates else None
                if single_active:
                    # For 4H this is explicitly allowed as one abnormal 15M candle.
                    # For 1H it is also an optional second path, as requested later.
                    path_type = "ABNORMAL_ONE"
                    path = fast or normal
                    abnormal_details = single_details

                if not path_type:
                    continue

                extension = extension_atr(rows, target_tf, direction)
                max_extension = (
                    MAX_1H_SIGNAL_EXTENSION_ATR
                    if target_tf == "1h"
                    else MAX_4H_SIGNAL_EXTENSION_ATR
                )
                if extension > max_extension:
                    continue

                book = orderbook_score(depth, direction)
                score, recipe = score_prediction(
                    rows, target_tf, direction, path_type, path,
                    abnormal_details, book, oi_change,
                )

                opposite_direction = "SELL" if direction == "BUY" else "BUY"
                opposite_path = delta_path(rows, normal_bars, opposite_direction)
                opposite_strength = opposite_path.get("average_delta", 0) if opposite_path else 0
                if score < MIN_SIGNAL_SCORE or score - opposite_strength < MIN_DIRECTION_GAP:
                    continue

                price = float(rows[-1][4])
                signals.append(Signal(
                    symbol=symbol,
                    target_tf=target_tf,
                    source_tf=source_tf,
                    direction=direction,
                    price=price,
                    score=score,
                    path_type=path_type,
                    extension=extension,
                    age_minutes=age,
                    oi_value=oi_value,
                    oi_change=oi_change,
                    funding=funding,
                    recipe=recipe,
                    trigger_open_time=int(rows[-2][0]),
                ))

        # Keep one best direction per symbol.
        if not signals:
            return []
        signals.sort(key=lambda item: (item.score, -item.extension), reverse=True)
        if len(signals) > 1 and signals[0].direction != signals[1].direction:
            if signals[0].score - signals[1].score < MIN_DIRECTION_GAP:
                return []
        return [signals[0]]

    def message(self, signal: Signal, stats: dict[str, Any]) -> str:
        side = signal.direction
        historical = "يتعلم" if stats["historical"] is None else f"{stats['historical']:.1f}%"
        cases = stats["cases"]
        learning_weight = min(70, max(0, int(cases / 2.5)))

        def horizon(name: str) -> str:
            value = stats[name]
            return "يتعلم" if value is None else f"{value:.1f}%"

        recipe = " + ".join(signal.recipe[:6])
        tv = f"https://www.tradingview.com/chart/?symbol=BINANCE:{signal.symbol}.P"
        bn = f"https://www.binance.com/en/futures/{signal.symbol}"
        timestamp = now_local().strftime("%d-%m-%Y %H:%M:%S")

        early_label = (
            "🔵 <b>إشارة مبكرة جدًا</b>"
            if signal.path_type in ("FAST_TWO", "ABNORMAL_ONE")
            else "🟡 <b>إشارة مبكرة</b>"
        )

        return f"""⚡ <b>DELTA {side}</b>

{early_label}

💰 العملة: <b>#{signal.symbol}.P</b>
⏰ الفريم: <b>{signal.target_tf.upper()}</b>
💵 السعر: <b>{fmt_price(signal.price)}</b>
📊 الدرجة: <b>{signal.score:.1f}%</b>
🧠 الاحتمال التاريخي: <b>{historical}</b>
🧪 حالات مشابهة: <b>{cases}</b>
⚙️ وزن التعلم: <b>{learning_weight}%</b>

⏱ 5 دقائق: <b>{horizon("5")}</b>
⏱ 15 دقيقة: <b>{horizon("15")}</b>
⏱ 30 دقيقة: <b>{horizon("30")}</b>
⏱ 60 دقيقة: <b>{horizon("60")}</b>

⚡ Delta المؤشر: <b>{side}</b>
📈 OI: <b>{fmt_compact(signal.oi_value)}</b>
💸 Funding: <b>{signal.funding:+.5f}%</b>
🧬 الوصفة: <b>{html.escape(recipe)}</b>

🕒 {timestamp} (السعودية)
🔗 <a href="{bn}">Binance</a> | <a href="{tv}">TradingView</a>

⚠️ احتمال إحصائي وليس ضمانًا"""

    async def evaluate_alerts(self):
        horizons = (5, 15, 30, 60)
        while self.running:
            try:
                prices = await self.client.prices()
                now_ts = now_local().timestamp()

                async with aiosqlite.connect(DB_PATH) as db:
                    db.row_factory = aiosqlite.Row
                    rows = await (
                        await db.execute(
                            "SELECT * FROM alerts WHERE checked_60m=0 ORDER BY id ASC LIMIT 1000"
                        )
                    ).fetchall()

                    for row in rows:
                        price = prices.get(row["symbol"])
                        if price is None:
                            continue
                        created = datetime.fromisoformat(row["created_at"]).timestamp()
                        age_minutes = (now_ts - created) / 60
                        direction = row["direction"]
                        entry = float(row["price"])

                        for horizon in horizons:
                            checked_key = f"checked_{horizon}m"
                            result_key = f"result_{horizon}m"
                            if row[checked_key] or age_minutes < horizon:
                                continue

                            move = pct_change(price, entry)
                            signed_move = move if direction == "BUY" else -move
                            # A modest positive move counts as a hit. This is learning metadata,
                            # not a guarantee or a trade execution rule.
                            threshold = {5: 0.15, 15: 0.25, 30: 0.35, 60: 0.50}[horizon]
                            result = int(signed_move >= threshold)

                            await db.execute(
                                f"UPDATE alerts SET {checked_key}=1,{result_key}=? WHERE id=?",
                                (result, row["id"]),
                            )
                    await db.commit()
            except Exception as error:
                log.warning("Evaluation worker skipped this cycle: %s", error)

            await asyncio.sleep(90)


engine = Engine()


# =========================================================
# FastAPI
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await engine.start()
    yield
    await engine.close()


app = FastAPI(title="Ahmed Delta Early Predictor v1.2 FINAL", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "ok": engine.last_error is None,
        "service": "Ahmed Delta Early Predictor v1.2 FINAL",
        "last_scan": engine.last_scan,
        "last_error": engine.last_error,
        "scan_number": engine.scan_no,
        "symbols": engine.symbol_count,
        "candidates": engine.candidate_count,
        "alerts_since_start": engine.alert_count,
        "mapping": {"1H": SOURCE_1H, "4H": SOURCE_4H},
        "time": now_local().isoformat(),
    }


@app.get("/test-telegram")
async def test_telegram():
    if not ENABLE_MANUAL_TEST_ENDPOINT:
        return JSONResponse({"ok": False, "error": "disabled"}, status_code=403)
    ok = await engine.send_telegram(
        "🧪 <b>اختبار يدوي ناجح</b>\n\n"
        "✅ Ahmed Delta Early Predictor متصل\n"
        f"🕒 {now_local().strftime('%d-%m-%Y %H:%M:%S')} (السعودية)"
    )
    return {"ok": ok}


@app.get("/alerts")
async def alerts(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM alerts ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/stats")
async def stats():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        overall = await (
            await db.execute(
                """SELECT COUNT(*) total,
                          SUM(checked_60m) evaluated_60m,
                          SUM(result_60m) successes_60m
                   FROM alerts"""
            )
        ).fetchone()
        groups = await (
            await db.execute(
                """SELECT target_tf,direction,path_type,COUNT(*) cases,
                          SUM(checked_60m) evaluated_60m,
                          SUM(result_60m) successes_60m
                   FROM alerts
                   GROUP BY target_tf,direction,path_type
                   ORDER BY cases DESC"""
            )
        ).fetchall()
    return {"overall": dict(overall), "groups": [dict(row) for row in groups]}


@app.get("/learning/checkpoints")
async def learning_checkpoints(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM checkpoints ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return f"""<!doctype html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ahmed Delta Early Predictor v1.2 FINAL</title>
<style>
body{{font-family:Arial;background:#0b1020;color:#eef2ff;margin:0;padding:24px}}
.wrap{{max-width:1050px;margin:auto}}
.card{{background:#151d34;border:1px solid #2b3658;border-radius:16px;padding:18px;margin:12px 0}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}}
.v{{font-size:25px;font-weight:bold;margin-top:6px}} a{{color:#8cb7ff}}
</style>
</head>
<body><div class="wrap">
<h1>Ahmed Delta Early Predictor v1.2 FINAL</h1>
<div class="card">الحالة: {'✅ يعمل' if engine.last_error is None else '⚠️ يوجد خطأ'}<br>آخر فحص: {engine.last_scan or 'لم يبدأ'}</div>
<div class="grid">
<div class="card">العقود<div class="v">{engine.symbol_count}</div></div>
<div class="card">المرشحون<div class="v">{engine.candidate_count}</div></div>
<div class="card">التنبيهات<div class="v">{engine.alert_count}</div></div>
<div class="card">رقم الفحص<div class="v">{engine.scan_no}</div></div>
</div>
<div class="card">1H ← {SOURCE_1H.upper()} &nbsp; | &nbsp; 4H ← {SOURCE_4H.upper()}</div>
<div class="card">
<a href="/health">Health</a> ·
<a href="/test-telegram">Test Telegram</a> ·
<a href="/alerts">Alerts</a> ·
<a href="/stats">Stats</a> ·
<a href="/learning/checkpoints">Checkpoints</a>
</div>
</div></body></html>"""


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
