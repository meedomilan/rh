
import asyncio
import html
import json
import logging
import math
import os
import sqlite3
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

BIG_CANDLE_BODY_ATR = float(os.getenv("BIG_CANDLE_BODY_ATR", "0.65"))
BIG_CANDLE_VOLUME_MULT = float(os.getenv("BIG_CANDLE_VOLUME_MULT", "1.40"))
VERY_BIG_CANDLE_BODY_ATR = float(os.getenv("VERY_BIG_CANDLE_BODY_ATR", "0.90"))
VERY_BIG_CANDLE_VOLUME_MULT = float(os.getenv("VERY_BIG_CANDLE_VOLUME_MULT", "1.80"))

REQUIRE_FIXED_ABNORMAL_FILTER = (
    os.getenv("REQUIRE_FIXED_ABNORMAL_FILTER", "true").lower() == "true"
)
ABNORMAL_RELATIVE_FILTER = (
    os.getenv("ABNORMAL_RELATIVE_FILTER", "true").lower() == "true"
)

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
MAX_1H_AGE_MINUTES = int(os.getenv("MAX_1H_AGE_MINUTES", "30"))
MAX_4H_AGE_MINUTES = int(os.getenv("MAX_4H_AGE_MINUTES", "120"))

REQUIRE_LAST_CANDLE_DIRECTION = (
    os.getenv("REQUIRE_LAST_CANDLE_DIRECTION", "true").lower() == "true"
)
MAX_OPPOSITE_CANDLE_DELTA_PCT = float(
    os.getenv("MAX_OPPOSITE_CANDLE_DELTA_PCT", "12")
)
ALLOW_SMALL_OPPOSITE_CANDLE = (
    os.getenv("ALLOW_SMALL_OPPOSITE_CANDLE", "true").lower() == "true"
)
MAX_SMALL_OPPOSITE_BODY_ATR = float(
    os.getenv("MAX_SMALL_OPPOSITE_BODY_ATR", "0.25")
)

ENABLE_4H_1H_CONFLICT_FILTER = (
    os.getenv("ENABLE_4H_1H_CONFLICT_FILTER", "true").lower() == "true"
)
LOWER_TF_STRONG_SCORE = float(os.getenv("LOWER_TF_STRONG_SCORE", "80"))
CONFLICT_WARN_FACTORS = int(os.getenv("CONFLICT_WARN_FACTORS", "2"))
CONFLICT_BLOCK_FACTORS = int(os.getenv("CONFLICT_BLOCK_FACTORS", "4"))
CONFLICT_SCORE_PENALTY = float(os.getenv("CONFLICT_SCORE_PENALTY", "8"))
BOS_LOOKBACK = int(os.getenv("BOS_LOOKBACK", "12"))
LIQUIDITY_SCORE_MIN = float(os.getenv("LIQUIDITY_SCORE_MIN", "50"))

ENABLE_COIN_TREND_FILTER = os.getenv("ENABLE_COIN_TREND_FILTER", "true").lower() == "true"
COIN_TREND_BLOCK_SCORE = float(os.getenv("COIN_TREND_BLOCK_SCORE", "42"))
COIN_TREND_WARN_SCORE = float(os.getenv("COIN_TREND_WARN_SCORE", "58"))
ENTRY_MIN_QUALITY = float(os.getenv("ENTRY_MIN_QUALITY", "55"))
ENTRY_MAX_DISTANCE_ATR = float(os.getenv("ENTRY_MAX_DISTANCE_ATR", "0.90"))

STOP_ATR_MULT = float(os.getenv("STOP_ATR_MULT", "0.90"))
TP1_R = float(os.getenv("TP1_R", "1.0"))
TP2_R = float(os.getenv("TP2_R", "2.0"))
TP3_R = float(os.getenv("TP3_R", "3.0"))

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


def candle_body_atr(rows: list[list[Any]], index: int) -> float:
    current_atr = atr(rows[: index + 1], 14)
    body = abs(float(rows[index][4]) - float(rows[index][1]))
    return safe_div(body, current_atr, 0.0)


def candle_volume_ratio(rows: list[list[Any]], index: int, length: int = 20) -> float:
    start = max(0, index - length)
    history = rows[start:index]
    if not history:
        return 1.0
    average = sum(float(row[5]) for row in history) / len(history)
    return safe_div(float(rows[index][5]), average, 1.0)


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

    body_atr = candle_body_atr(rows, index)
    volume_ratio = candle_volume_ratio(rows, index)

    previous_high = max(float(item[2]) for item in history[-8:])
    previous_low = min(float(item[3]) for item in history[-8:])
    breakout = float(row[4]) > previous_high if direction == "BUY" else float(row[4]) < previous_low

    relative_components = [
        body_rank >= ABNORMAL_BODY_PERCENTILE,
        range_rank >= ABNORMAL_RANGE_PERCENTILE,
        volume_rank >= ABNORMAL_VOLUME_PERCENTILE,
        close_loc >= ABNORMAL_CLOSE_LOCATION,
        breakout,
        delta_ok,
    ]
    relative_count = sum(relative_components)
    relative_ok = relative_count >= ABNORMAL_MIN_COMPONENTS + 1

    fixed_ok = (
        body_atr >= VERY_BIG_CANDLE_BODY_ATR
        and volume_ratio >= VERY_BIG_CANDLE_VOLUME_MULT
    )

    if REQUIRE_FIXED_ABNORMAL_FILTER and ABNORMAL_RELATIVE_FILTER:
        active = direction_ok and delta_ok and fixed_ok and relative_ok
    elif REQUIRE_FIXED_ABNORMAL_FILTER:
        active = direction_ok and delta_ok and fixed_ok
    else:
        active = direction_ok and delta_ok and relative_ok

    return active, {
        "body_rank": body_rank,
        "range_rank": range_rank,
        "volume_rank": volume_rank,
        "close_location": close_loc,
        "breakout": breakout,
        "delta_pct": delta_pct,
        "components": relative_count,
        "body_atr": body_atr,
        "volume_ratio": volume_ratio,
        "fixed_ok": fixed_ok,
        "relative_ok": relative_ok,
    }


def strong_two_candles(
    rows: list[list[Any]],
    end_index: int,
    direction: str,
) -> tuple[bool, list[dict[str, float]]]:
    indexes = [end_index - 1, end_index]
    if indexes[0] < 20:
        return False, []

    details = []
    for index in indexes:
        body_atr = candle_body_atr(rows, index)
        volume_ratio = candle_volume_ratio(rows, index)
        delta_pct = candle_delta_pct(rows[index])
        direction_ok = directional_candle(rows[index], direction)
        signed_delta = delta_pct if direction == "BUY" else -delta_pct
        close_ok = close_location(rows[index], direction) >= 0.60

        details.append({
            "body_atr": body_atr,
            "volume_ratio": volume_ratio,
            "signed_delta": signed_delta,
        })

        if not (
            direction_ok
            and body_atr >= BIG_CANDLE_BODY_ATR
            and volume_ratio >= BIG_CANDLE_VOLUME_MULT
            and signed_delta >= FAST_MIN_DELTA_PCT
            and close_ok
        ):
            return False, details

    return True, details


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


def last_candle_allows_signal(
    rows: list[list[Any]],
    direction: str,
) -> tuple[bool, dict[str, float | bool]]:
    """Check the last CLOSED source candle before sending an alert."""
    if len(rows) < 3:
        return False, {}

    index = len(rows) - 2
    row = rows[index]
    direction_ok = directional_candle(row, direction)
    raw_delta = candle_delta_pct(row)
    signed_delta = raw_delta if direction == "BUY" else -raw_delta
    body_atr = candle_body_atr(rows, index)

    if direction_ok:
        return True, {
            "direction_ok": True,
            "signed_delta": signed_delta,
            "body_atr": body_atr,
            "small_opposite": False,
        }

    opposite_delta_strength = max(0.0, -signed_delta)
    small_opposite = (
        ALLOW_SMALL_OPPOSITE_CANDLE
        and body_atr <= MAX_SMALL_OPPOSITE_BODY_ATR
        and opposite_delta_strength < MAX_OPPOSITE_CANDLE_DELTA_PCT
    )

    if not REQUIRE_LAST_CANDLE_DIRECTION:
        allowed = opposite_delta_strength < MAX_OPPOSITE_CANDLE_DELTA_PCT
    else:
        allowed = small_opposite

    return allowed, {
        "direction_ok": False,
        "signed_delta": signed_delta,
        "body_atr": body_atr,
        "small_opposite": small_opposite,
    }


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


def lower_tf_direction_score(rows: list[list[Any]], direction: str) -> float:
    if len(rows) < 55:
        return 50.0
    closes = [float(row[4]) for row in rows[:-1]]
    current = rows[-2]
    e9 = ema(closes, 9)[-1]
    e21 = ema(closes, 21)[-1]
    e50 = ema(closes, 50)[-1]
    delta_pct = candle_delta_pct(current)
    signed_delta = delta_pct if direction == "BUY" else -delta_pct
    score = 50.0
    if direction == "BUY":
        score += 10 if e9 > e21 else -10
        score += 8 if e21 > e50 else -8
        score += 6 if float(current[4]) > e21 else -6
    else:
        score += 10 if e9 < e21 else -10
        score += 8 if e21 < e50 else -8
        score += 6 if float(current[4]) < e21 else -6
    score += clamp(signed_delta, -20, 20) * 0.8
    score += 6 if directional_candle(current, direction) else -6
    score += (close_location(current, direction) - 0.5) * 12
    score += min(max(candle_volume_ratio(rows, len(rows) - 2) - 1.0, 0.0) * 5, 6)
    return clamp(score)


def bos_signal(rows: list[list[Any]], direction: str) -> bool:
    if len(rows) < BOS_LOOKBACK + 3:
        return False
    closed = rows[:-1]
    current_close = float(closed[-1][4])
    history = closed[-BOS_LOOKBACK - 1:-1]
    if direction == "BUY":
        return current_close > max(float(row[2]) for row in history)
    return current_close < min(float(row[3]) for row in history)


def cvd_direction(rows: list[list[Any]], direction: str, length: int = 8) -> bool:
    if len(rows) < length + 2:
        return False
    closed = rows[:-1]
    recent = sum(candle_delta(row) for row in closed[-length:])
    previous = sum(candle_delta(row) for row in closed[-2 * length:-length]) if len(closed) >= 2 * length else 0.0
    change = recent - previous
    return change > 0 if direction == "BUY" else change < 0


def directional_volume_spike(rows: list[list[Any]], direction: str) -> bool:
    if len(rows) < 22:
        return False
    index = len(rows) - 2
    return directional_candle(rows[index], direction) and candle_volume_ratio(rows, index) >= 1.4


def build_trade_plan(target_rows: list[list[Any]], direction: str, price: float) -> dict[str, float]:
    # Fast source finds the signal; target timeframe controls stop/targets.
    target_atr = atr(target_rows, 14)
    risk = max(target_atr * STOP_ATR_MULT, price * 0.001)
    zone = target_atr * 0.08
    if direction == "BUY":
        return {
            "entry_low": price - zone,
            "entry_high": price + zone,
            "stop": price - risk,
            "tp1": price + risk * TP1_R,
            "tp2": price + risk * TP2_R,
            "tp3": price + risk * TP3_R,
        }
    return {
        "entry_low": price - zone,
        "entry_high": price + zone,
        "stop": price + risk,
        "tp1": price - risk * TP1_R,
        "tp2": price - risk * TP2_R,
        "tp3": price - risk * TP3_R,
    }



def coin_trend_analysis(
    direction: str,
    target_tf: str,
    rows_1h: list[list[Any]],
    rows_4h: list[list[Any]],
) -> dict[str, Any]:
    """Analyze the coin itself before allowing BUY/SELL."""
    score_1h = lower_tf_direction_score(rows_1h, direction)
    score_4h = lower_tf_direction_score(rows_4h, direction)

    if target_tf == "1h":
        combined = 0.65 * score_1h + 0.35 * score_4h
    else:
        combined = 0.40 * score_1h + 0.60 * score_4h

    notes = []
    if score_1h >= 65:
        notes.append(f"1H يدعم {direction} ({score_1h:.0f}%)")
    elif score_1h < 45:
        notes.append(f"1H يعاكس {direction} ({score_1h:.0f}%)")

    if score_4h >= 65:
        notes.append(f"4H يدعم {direction} ({score_4h:.0f}%)")
    elif score_4h < 45:
        notes.append(f"4H يعاكس {direction} ({score_4h:.0f}%)")

    return {
        "score_1h": score_1h,
        "score_4h": score_4h,
        "combined": clamp(combined),
        "blocked": combined < COIN_TREND_BLOCK_SCORE,
        "warned": combined < COIN_TREND_WARN_SCORE,
        "notes": notes,
    }


def adaptive_entry_candidate(rows: list[list[Any]], direction: str, tf: str, current_price: float) -> dict[str, Any]:
    """Score a lower timeframe and derive a pullback/retest entry zone."""
    if len(rows) < 55:
        return {"tf": tf, "quality": 0.0, "entry_ref": current_price, "distance_atr": 99.0}

    closes = [float(row[4]) for row in rows[:-1]]
    e9 = ema(closes, 9)[-1]
    e21 = ema(closes, 21)[-1]
    e50 = ema(closes, 50)[-1]
    lower_atr = atr(rows, 14)
    base_quality = lower_tf_direction_score(rows, direction)

    # Structure/flow additions.
    structure = 0.0
    if direction == "BUY":
        structure += 6 if e9 > e21 else -4
        structure += 6 if e21 > e50 else -4
    else:
        structure += 6 if e9 < e21 else -4
        structure += 6 if e21 < e50 else -4

    if cvd_direction(rows, direction):
        structure += 5
    if directional_volume_spike(rows, direction):
        structure += 5
    if bos_signal(rows, direction):
        structure += 5

    quality = clamp(base_quality + structure)

    # Use EMA21 as the preferred retest point when not too far from live price.
    if lower_atr <= 0:
        lower_atr = max(current_price * 0.002, 1e-12)

    if direction == "BUY":
        entry_ref = min(current_price, e21)
    else:
        entry_ref = max(current_price, e21)

    distance_atr = abs(current_price - entry_ref) / lower_atr
    if distance_atr > ENTRY_MAX_DISTANCE_ATR:
        # Do not ask the user to wait for a point unrealistically far away.
        entry_ref = current_price
        distance_atr = 0.0
        quality -= 5

    zone_half = lower_atr * 0.10
    return {
        "tf": tf,
        "quality": clamp(quality),
        "entry_ref": entry_ref,
        "entry_low": entry_ref - zone_half,
        "entry_high": entry_ref + zone_half,
        "distance_atr": distance_atr,
    }


def choose_adaptive_entry(
    target_tf: str,
    direction: str,
    current_price: float,
    frames: dict[str, list[list[Any]]],
) -> dict[str, Any]:
    # 1H: prefer 5M, but 3M/15M can win when their setup is cleaner.
    # 4H: prefer 15M, but 5M/30M/1H can win when stronger.
    preferred = (
        ["5m", "3m", "15m"]
        if target_tf == "1h"
        else ["15m", "5m", "30m", "1h"]
    )
    candidates = [
        adaptive_entry_candidate(frames[tf], direction, tf, current_price)
        for tf in preferred
        if tf in frames and frames[tf]
    ]
    if not candidates:
        return {
            "tf": "5m" if target_tf == "1h" else "15m",
            "quality": 0.0,
            "entry_ref": current_price,
            "entry_low": current_price,
            "entry_high": current_price,
            "distance_atr": 0.0,
        }

    candidates.sort(
        key=lambda x: (
            x["quality"],
            -x["distance_atr"],
            1 if x["tf"] == ("5m" if target_tf == "1h" else "15m") else 0,
        ),
        reverse=True,
    )
    return candidates[0]


def build_trade_plan_from_entry(
    target_rows: list[list[Any]],
    direction: str,
    entry_ref: float,
    entry_low: float,
    entry_high: float,
) -> dict[str, float]:
    target_atr = atr(target_rows, 14)
    risk = max(target_atr * STOP_ATR_MULT, entry_ref * 0.001)

    if direction == "BUY":
        return {
            "entry_low": entry_low,
            "entry_high": entry_high,
            "stop": entry_ref - risk,
            "tp1": entry_ref + risk * TP1_R,
            "tp2": entry_ref + risk * TP2_R,
            "tp3": entry_ref + risk * TP3_R,
        }
    return {
        "entry_low": entry_low,
        "entry_high": entry_high,
        "stop": entry_ref + risk,
        "tp1": entry_ref - risk * TP1_R,
        "tp2": entry_ref - risk * TP2_R,
        "tp3": entry_ref - risk * TP3_R,
    }


def conflict_analysis_4h(rows_1h: list[list[Any]], signal_direction: str, oi_change: float) -> dict[str, Any]:
    opposite = "BUY" if signal_direction == "SELL" else "SELL"
    notes = []
    factors = 0
    lower_score = lower_tf_direction_score(rows_1h, opposite)

    if lower_score >= LOWER_TF_STRONG_SCORE:
        factors += 1
        notes.append(f"1H {opposite} قوي ({lower_score:.0f}%)")
    if cvd_direction(rows_1h, opposite):
        factors += 1
        notes.append("CVD على 1H يعاكس الإشارة")
    if oi_change > 0 and lower_score >= 65:
        factors += 1
        notes.append("OI يرتفع مع الاتجاه المعاكس")
    if directional_volume_spike(rows_1h, opposite):
        factors += 1
        notes.append("Volume Spike معاكس على 1H")
    if bos_signal(rows_1h, opposite):
        factors += 1
        notes.append("BOS معاكس على 1H")

    return {
        "factors": factors,
        "notes": notes,
        "blocked": factors >= CONFLICT_BLOCK_FACTORS,
        "warned": factors >= CONFLICT_WARN_FACTORS,
        "lower_score": lower_score,
    }


def agreement_scores(
    target_tf: str,
    direction: str,
    rows_source: list[list[Any]],
    rows_1h: list[list[Any]],
    oi_change: float,
    book_score: float,
    funding: float,
    conflict: dict[str, Any],
) -> tuple[float, float, float]:
    same_score = lower_tf_direction_score(
        rows_1h if target_tf == "4h" else rows_source,
        direction,
    )
    timeframe_agreement = clamp(same_score - conflict.get("factors", 0) * 10 + (10 if target_tf == "4h" else 0))

    liquidity = 45.0
    liquidity += (book_score - 50) * 0.35
    liquidity += clamp(oi_change, -2, 2) * 5
    liquidity += 8 if cvd_direction(rows_source, direction) else -5
    liquidity += 7 if directional_volume_spike(rows_source, direction) else 0
    liquidity += 3 if ((direction == "BUY" and funding <= 0) or (direction == "SELL" and funding >= 0)) else -1
    liquidity_agreement = clamp(liquidity)

    decision_confidence = clamp(
        0.45 * timeframe_agreement
        + 0.40 * liquidity_agreement
        + 0.15 * (100 - conflict.get("factors", 0) * 18)
    )
    return timeframe_agreement, liquidity_agreement, decision_confidence


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
        recipe.append("TwoStrongCandles")
    else:
        score += 17
        if abnormal:
            score += abnormal.get("components", 0) * 1.5
            score += max(0, abnormal.get("delta_pct", 0) if direction == "BUY" else -abnormal.get("delta_pct", 0)) * 0.15
        recipe.append("ExceptionalCandle")

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

STAGE_NAMES = {
    "NORMAL": ("EARLY", "🟢 إشارة مبكرة"),
    "FAST_TWO": ("CONFIRMED", "🔵 إشارة مبكرة قوية"),
    "ABNORMAL_ONE": ("EXPLOSION", "🔥 إشارة مبكرة جدًا"),
}


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
    quote_volume_24h: float
    recipe: list[str]
    trigger_open_time: int
    timeframe_agreement: float
    liquidity_agreement: float
    decision_confidence: float
    conflict_factors: int
    conflict_notes: list[str]
    coin_trend_score: float
    entry_tf: str
    entry_quality: float
    entry_low: float
    entry_high: float
    stop: float
    tp1: float
    tp2: float
    tp3: float


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
    quote_volume_24h REAL,
    recipe_json TEXT,
    timeframe_agreement REAL,
    liquidity_agreement REAL,
    decision_confidence REAL,
    conflict_factors INTEGER DEFAULT 0,
    conflict_notes_json TEXT,
    coin_trend_score REAL,
    entry_tf TEXT,
    entry_quality REAL,
    entry_low REAL,
    entry_high REAL,
    stop REAL,
    tp1 REAL,
    tp2 REAL,
    tp3 REAL,
    status TEXT DEFAULT 'OPEN',
    result_5m INTEGER,
    result_15m INTEGER,
    result_30m INTEGER,
    result_60m INTEGER,
    checked_5m INTEGER DEFAULT 0,
    checked_15m INTEGER DEFAULT 0,
    checked_30m INTEGER DEFAULT 0,
    checked_60m INTEGER DEFAULT 0,
    result_120m INTEGER,
    result_240m INTEGER,
    result_480m INTEGER,
    result_720m INTEGER,
    checked_120m INTEGER DEFAULT 0,
    checked_240m INTEGER DEFAULT 0,
    checked_480m INTEGER DEFAULT 0,
    checked_720m INTEGER DEFAULT 0,
    tp1_hit INTEGER DEFAULT 0,
    tp2_hit INTEGER DEFAULT 0,
    tp3_hit INTEGER DEFAULT 0,
    stop_hit INTEGER DEFAULT 0,
    first_outcome TEXT,
    first_outcome_at TEXT,
    tp1_after_stop INTEGER DEFAULT 0,
    tp2_after_stop INTEGER DEFAULT 0,
    tp3_after_stop INTEGER DEFAULT 0,
    max_beyond_stop_r REAL DEFAULT 0
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

        columns = {
            row[1]
            for row in await (await db.execute("PRAGMA table_info(alerts)")).fetchall()
        }
        migrations = {
            "quote_volume_24h": "REAL",
            "timeframe_agreement": "REAL",
            "liquidity_agreement": "REAL",
            "decision_confidence": "REAL",
            "conflict_factors": "INTEGER DEFAULT 0",
            "conflict_notes_json": "TEXT",
            "coin_trend_score": "REAL",
            "entry_tf": "TEXT",
            "entry_quality": "REAL",
            "max_beyond_stop_r": "REAL DEFAULT 0",
            "entry_low": "REAL",
            "entry_high": "REAL",
            "stop": "REAL",
            "tp1": "REAL",
            "tp2": "REAL",
            "tp3": "REAL",
            "result_120m": "INTEGER",
            "result_240m": "INTEGER",
            "result_480m": "INTEGER",
            "result_720m": "INTEGER",
            "checked_120m": "INTEGER DEFAULT 0",
            "checked_240m": "INTEGER DEFAULT 0",
            "checked_480m": "INTEGER DEFAULT 0",
            "checked_720m": "INTEGER DEFAULT 0",
            "tp1_hit": "INTEGER DEFAULT 0",
            "tp2_hit": "INTEGER DEFAULT 0",
            "tp3_hit": "INTEGER DEFAULT 0",
            "stop_hit": "INTEGER DEFAULT 0",
            "first_outcome": "TEXT",
            "first_outcome_at": "TEXT",
            "tp1_after_stop": "INTEGER DEFAULT 0",
            "tp2_after_stop": "INTEGER DEFAULT 0",
            "tp3_after_stop": "INTEGER DEFAULT 0",
        }
        for name, definition in migrations.items():
            if name not in columns:
                await db.execute(f"ALTER TABLE alerts ADD COLUMN {name} {definition}")

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
                    price,score,path_type,extension,age_minutes,oi_value,oi_change,funding,quote_volume_24h,recipe_json,
                    timeframe_agreement,liquidity_agreement,decision_confidence,
                    conflict_factors,conflict_notes_json,coin_trend_score,entry_tf,entry_quality,
                    entry_low,entry_high,stop,tp1,tp2,tp3
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
                    signal.quote_volume_24h,
                    json.dumps(signal.recipe, ensure_ascii=False),
                    signal.timeframe_agreement,
                    signal.liquidity_agreement,
                    signal.decision_confidence,
                    signal.conflict_factors,
                    json.dumps(signal.conflict_notes, ensure_ascii=False),
                    signal.coin_trend_score,
                    signal.entry_tf,
                    signal.entry_quality,
                    signal.entry_low,
                    signal.entry_high,
                    signal.stop,
                    signal.tp1,
                    signal.tp2,
                    signal.tp3,
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
                   ORDER BY id DESC""",
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
        self.symbol_timeouts_last_scan = 0
        self.scan_timeouts_total = 0
        self.rejection_stats = {
            "last_candle": 0,
            "no_path": 0,
            "extension": 0,
            "score": 0,
            "direction_gap": 0,
            "conflict": 0,
            "liquidity": 0,
            "age": 0,
            "duplicate": 0,
        }
        self.telegram_lock = asyncio.Lock()
        self.last_telegram_send_at = 0.0

    async def start(self):
        await init_db()
        await self.client.start()
        self.telegram_session = aiohttp.ClientSession()

        if SEND_STARTUP_MESSAGE:
            await self.send_telegram(
                "✅ <b>Ahmed Delta Early Predictor v2.9 STAGE + FIRST OUTCOME STATS بدأ العمل</b>\n\n"
                "🎯 1H يُتوقع من 5M\n"
                "🎯 4H يُتوقع من 15M\n"
                "✅ 3 شموع متدرجة أو شمعتان قويتان أو شمعة استثنائية\n"
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
                    timeout=max(SCAN_TIMEOUT, 180),
                )
                self.last_error = None
            except asyncio.TimeoutError:
                self.scan_timeouts_total += 1
                error = "scan hard timeout"
                # Keep the service healthy: skip this cycle and continue.
                self.last_error = None
                log.warning("Scan hard timeout; skipped cycle and continuing")
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
                "scan=%s symbols=%s candidates=%s analyzed=%s opportunities=%s alerts=%s "
                "rejected[last=%s no_path=%s extension=%s score=%s gap=%s conflict=%s "
                "liquidity=%s age=%s duplicate=%s] symbol_timeouts=%s seconds=%.1f",
                self.scan_no,
                self.symbol_count,
                self.candidate_count,
                analyzed,
                opportunities,
                alerts,
                self.rejection_stats["last_candle"],
                self.rejection_stats["no_path"],
                self.rejection_stats["extension"],
                self.rejection_stats["score"],
                self.rejection_stats["direction_gap"],
                self.rejection_stats["conflict"],
                self.rejection_stats["liquidity"],
                self.rejection_stats["age"],
                self.rejection_stats["duplicate"],
                self.symbol_timeouts_last_scan,
                elapsed,
            )
            await asyncio.sleep(max(3, SCAN_SECONDS - elapsed))

    async def scan(self):
        self.symbol_timeouts_last_scan = 0
        self.rejection_stats = {
            "last_candle": 0,
            "no_path": 0,
            "extension": 0,
            "score": 0,
            "direction_gap": 0,
            "conflict": 0,
            "liquidity": 0,
            "age": 0,
            "duplicate": 0,
        }

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
        selected = ranked[:RADAR_POOL][:DEEP_CANDIDATES]
        candidates = [item[2] for item in selected]
        candidate_volume_24h = {item[2]: item[1] for item in selected}
        self.candidate_count = len(candidates)

        async def guarded(symbol: str):
            try:
                return await asyncio.wait_for(
                    self.analyze_symbol(symbol, candidate_volume_24h.get(symbol, 0.0)),
                    timeout=SYMBOL_TIMEOUT,
                )
            except asyncio.TimeoutError:
                self.symbol_timeouts_last_scan += 1
                log.warning("Analysis timeout %s — skipped and scan continues", symbol)
                return []
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
                self.rejection_stats["duplicate"] += 1
                continue
            stats = await historical_stats(signal)
            if await self.send_telegram(self.message(signal, stats)):
                alerts += 1
                self.alert_count += 1
                sent_symbols.add(signal.symbol)

        return alerts, len(groups), len(signals)

    async def analyze_symbol(self, symbol: str, quote_volume_24h: float = 0.0) -> list[Signal]:
        k3, k5, k15, k30, k1h, k4h, oi5, oi15, premium, depth = await asyncio.gather(
            self.client.klines(symbol, "3m", 100),
            self.client.klines(symbol, SOURCE_1H, 100),
            self.client.klines(symbol, SOURCE_4H, 100),
            self.client.klines(symbol, "30m", 100),
            self.client.klines(symbol, "1h", 100),
            self.client.klines(symbol, "4h", 100),
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
                self.rejection_stats["age"] += 1
                continue

            for direction in ("BUY", "SELL"):
                path_type = None
                path = {}
                abnormal_details = None

                last_candle_ok, last_candle_details = last_candle_allows_signal(
                    rows,
                    direction,
                )
                if not last_candle_ok:
                    self.rejection_stats["last_candle"] += 1
                    continue

                normal = delta_path(rows, normal_bars, direction)
                normal_valid = (
                    normal
                    and normal["positive_count"] >= NORMAL_MIN_DIRECTIONAL_BARS
                    and normal["directional_count"] >= NORMAL_MIN_DIRECTIONAL_BARS
                    and normal["average_delta"] >= NORMAL_MIN_DELTA_PCT
                    and normal["slope"] >= MIN_DELTA_SLOPE
                )

                fast = delta_path(rows, max(2, fast_bars), direction)
                closed_last_index = len(rows) - 2

                # Path 3: one exceptional candle. Highest priority.
                single_active, single_details = abnormal_candle(
                    rows,
                    closed_last_index,
                    direction,
                )

                # Path 2: two strong candles.
                two_active, two_details = strong_two_candles(
                    rows,
                    closed_last_index,
                    direction,
                )

                # Path 1: three progressive candles.
                if normal_valid:
                    path_type = "NORMAL"
                    path = normal

                if two_active:
                    path_type = "FAST_TWO"
                    path = fast
                    abnormal_details = {
                        "body_atr": sum(item["body_atr"] for item in two_details) / len(two_details),
                        "volume_ratio": sum(item["volume_ratio"] for item in two_details) / len(two_details),
                        "components": 2,
                    }

                if single_active:
                    path_type = "ABNORMAL_ONE"
                    path = fast or normal
                    abnormal_details = single_details

                if not path_type:
                    self.rejection_stats["no_path"] += 1
                    continue

                extension = extension_atr(rows, target_tf, direction)
                max_extension = (
                    MAX_1H_SIGNAL_EXTENSION_ATR
                    if target_tf == "1h"
                    else MAX_4H_SIGNAL_EXTENSION_ATR
                )
                if extension > max_extension:
                    self.rejection_stats["extension"] += 1
                    continue

                book = orderbook_score(depth, direction)
                score, recipe = score_prediction(
                    rows, target_tf, direction, path_type, path,
                    abnormal_details, book, oi_change,
                )

                if last_candle_details.get("direction_ok"):
                    recipe.append("LastCandleAligned")
                elif last_candle_details.get("small_opposite"):
                    recipe.append("SmallPullbackAllowed")

                opposite_direction = "SELL" if direction == "BUY" else "BUY"
                opposite_path = delta_path(rows, normal_bars, opposite_direction)
                opposite_strength = opposite_path.get("average_delta", 0) if opposite_path else 0
                if score < MIN_SIGNAL_SCORE:
                    self.rejection_stats["score"] += 1
                    continue

                if score - opposite_strength < MIN_DIRECTION_GAP:
                    self.rejection_stats["direction_gap"] += 1
                    continue

                conflict = {
                    "factors": 0,
                    "notes": [],
                    "blocked": False,
                    "warned": False,
                    "lower_score": 50.0,
                }
                if target_tf == "4h" and ENABLE_4H_1H_CONFLICT_FILTER:
                    conflict = conflict_analysis_4h(k1h, direction, oi_change)
                    if conflict["blocked"]:
                        self.rejection_stats["conflict"] += 1
                        log.info(
                            "blocked 4H conflict %s %s factors=%s",
                            symbol,
                            direction,
                            conflict["factors"],
                        )
                        continue
                    if conflict["warned"]:
                        score = max(0.0, score - CONFLICT_SCORE_PENALTY)
                        recipe.append("LowerTFConflictWarning")

                trend = coin_trend_analysis(direction, target_tf, k1h, k4h)
                if ENABLE_COIN_TREND_FILTER and trend["blocked"]:
                    log.info(
                        "blocked coin trend %s %s target=%s score=%.1f",
                        symbol, direction, target_tf, trend["combined"],
                    )
                    continue
                if trend["warned"]:
                    recipe.append("CoinTrendWeak")
                else:
                    recipe.append("CoinTrendAligned")

                timeframe_agreement, liquidity_agreement, decision_confidence = agreement_scores(
                    target_tf,
                    direction,
                    rows,
                    k1h,
                    oi_change,
                    book,
                    funding,
                    conflict,
                )
                decision_confidence = clamp(
                    decision_confidence * 0.80 + trend["combined"] * 0.20
                )
                if liquidity_agreement < LIQUIDITY_SCORE_MIN:
                    self.rejection_stats["liquidity"] += 1
                    recipe.append("WeakLiquidityAgreement")

                price = float(rows[-1][4])
                target_rows = k1h if target_tf == "1h" else k4h
                entry_pick = choose_adaptive_entry(
                    target_tf,
                    direction,
                    price,
                    {
                        "3m": k3,
                        "5m": k5,
                        "15m": k15,
                        "30m": k30,
                        "1h": k1h,
                    },
                )
                if entry_pick["quality"] < ENTRY_MIN_QUALITY:
                    recipe.append("EntryQualityWeak")

                plan = build_trade_plan_from_entry(
                    target_rows,
                    direction,
                    entry_pick["entry_ref"],
                    entry_pick["entry_low"],
                    entry_pick["entry_high"],
                )

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
                    quote_volume_24h=quote_volume_24h,
                    recipe=recipe,
                    trigger_open_time=int(rows[-2][0]),
                    timeframe_agreement=timeframe_agreement,
                    liquidity_agreement=liquidity_agreement,
                    decision_confidence=decision_confidence,
                    conflict_factors=conflict["factors"],
                    conflict_notes=conflict["notes"],
                    coin_trend_score=trend["combined"],
                    entry_tf=entry_pick["tf"],
                    entry_quality=entry_pick["quality"],
                    entry_low=plan["entry_low"],
                    entry_high=plan["entry_high"],
                    stop=plan["stop"],
                    tp1=plan["tp1"],
                    tp2=plan["tp2"],
                    tp3=plan["tp3"],
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

        path_label = {
            "NORMAL": "🟢 إشارة مبكرة",
            "FAST_TWO": "🔵 إشارة مبكرة قوية",
            "ABNORMAL_ONE": "🔥 إشارة مبكرة جدًا",
        }.get(signal.path_type, "🟡 إشارة مبكرة")

        recipe = " + ".join(signal.recipe[:8])
        tv = f"https://www.tradingview.com/chart/?symbol=BINANCE:{signal.symbol}.P"
        bn = f"https://www.binance.com/en/futures/{signal.symbol}"
        timestamp = now_local().strftime("%d-%m-%Y %H:%M:%S")

        timeframe_icon = "✅" if signal.timeframe_agreement >= 70 else "⚠️"
        liquidity_icon = "✅" if signal.liquidity_agreement >= 70 else "⚠️"

        conflict_lines = ""
        if signal.conflict_factors:
            notes = "\n".join(f"⚠️ {html.escape(note)}" for note in signal.conflict_notes[:4])
            conflict_lines = f"\n\n⚠️ <b>تعارض جزئي مع الفريم الأدنى</b>\n{notes}"
        elif signal.target_tf == "4h":
            conflict_lines = "\n\n✅ لا يوجد تعارض قوي مع 1H"

        return f"""⚡ <b>DELTA {side}</b>

<b>{path_label}</b>

💰 العملة: <b>#{signal.symbol}.P</b>
⏰ الفريم: <b>{signal.target_tf.upper()}</b>
💵 السعر: <b>{fmt_price(signal.price)}</b>
🚦 المرحلة: <b>{STAGE_NAMES.get(signal.path_type, (signal.path_type, "⚪ غير مصنف"))[1]}</b> — <b>{STAGE_NAMES.get(signal.path_type, (signal.path_type, "⚪ غير مصنف"))[0]}</b>
📊 الدرجة: <b>{signal.score:.1f}%</b>

🧭 توافق الفريمات: <b>{signal.timeframe_agreement:.1f}%</b> {timeframe_icon}
🧠 توافق السيولة: <b>{signal.liquidity_agreement:.1f}%</b> {liquidity_icon}
📈 اتجاه العملة: <b>{signal.coin_trend_score:.1f}%</b>
🎯 ثقة القرار: <b>{signal.decision_confidence:.1f}%</b>{conflict_lines}

🔎 فريم الدخول المختار: <b>{signal.entry_tf.upper()}</b>
💎 جودة نقطة الدخول: <b>{signal.entry_quality:.1f}%</b>
🎯 الدخول: <b>{fmt_price(signal.entry_low)} – {fmt_price(signal.entry_high)}</b>
🛑 وقف الخسارة: <b>{fmt_price(signal.stop)}</b>
✅ TP1: <b>{fmt_price(signal.tp1)}</b> ({TP1_R:.1f}R)
✅ TP2: <b>{fmt_price(signal.tp2)}</b> ({TP2_R:.1f}R)
✅ TP3: <b>{fmt_price(signal.tp3)}</b> ({TP3_R:.1f}R)

🧠 الاحتمال التاريخي: <b>{historical}</b>
🧪 حالات مشابهة: <b>{cases}</b>
⚙️ وزن التعلم: <b>{learning_weight}%</b>

⏱ 5 دقائق: <b>{horizon("5")}</b>
⏱ 15 دقيقة: <b>{horizon("15")}</b>
⏱ 30 دقيقة: <b>{horizon("30")}</b>
⏱ 60 دقيقة: <b>{horizon("60")}</b>

⚡ Delta المؤشر: <b>{side}</b>
📈 OI: <b>{signal.oi_value:,.2f}</b>
💰 حجم التداول 24H: <b>{signal.quote_volume_24h:,.0f} USDT</b>
💸 Funding: <b>{signal.funding:+.5f}%</b>
🧬 الوصفة: <b>{html.escape(recipe)}</b>

🕒 {timestamp} (السعودية)
🔗 <a href="{bn}">Binance</a> | <a href="{tv}">TradingView</a>

⚠️ احتمال إحصائي وليس ضمانًا"""

    async def evaluate_alerts(self):
        horizons = (5, 15, 30, 60, 120, 240, 480, 720)
        while self.running:
            try:
                prices = await self.client.prices()
                now_ts = now_local().timestamp()

                async with aiosqlite.connect(DB_PATH) as db:
                    db.row_factory = aiosqlite.Row
                    rows = await (
                        await db.execute(
                            "SELECT * FROM alerts WHERE checked_720m=0 OR first_outcome IS NULL ORDER BY id ASC LIMIT 1500"
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

                        # Track TP/SL from stored trade plan.
                        tp1, tp2, tp3, stop = row["tp1"], row["tp2"], row["tp3"], row["stop"]
                        if all(v is not None for v in (tp1, tp2, tp3, stop)):
                            if direction == "BUY":
                                hit_tp1 = price >= float(tp1)
                                hit_tp2 = price >= float(tp2)
                                hit_tp3 = price >= float(tp3)
                                hit_stop = price <= float(stop)
                            else:
                                hit_tp1 = price <= float(tp1)
                                hit_tp2 = price <= float(tp2)
                                hit_tp3 = price <= float(tp3)
                                hit_stop = price >= float(stop)

                            set_parts, params = [], []
                            if hit_tp1 and not row["tp1_hit"]:
                                set_parts.append("tp1_hit=1")
                            if hit_tp2 and not row["tp2_hit"]:
                                set_parts.append("tp2_hit=1")
                            if hit_tp3 and not row["tp3_hit"]:
                                set_parts.append("tp3_hit=1")
                            if hit_stop and not row["stop_hit"]:
                                set_parts.append("stop_hit=1")

                            if row["first_outcome"] is None:
                                outcome = None
                                if hit_tp1 and not hit_stop:
                                    outcome = "TP1"
                                elif hit_stop and not hit_tp1:
                                    outcome = "STOP"
                                elif hit_tp1 and hit_stop:
                                    outcome = "AMBIGUOUS"
                                if outcome:
                                    set_parts.extend(["first_outcome=?", "first_outcome_at=?"])
                                    params.extend([outcome, now_local().isoformat()])

                            stop_was_first = (row["first_outcome"] == "STOP") or (
                                row["first_outcome"] is None and hit_stop and not hit_tp1
                            )

                            # Measure how far price went beyond Stop in R after Stop was first.
                            if stop_was_first:
                                risk = abs(float(stop) - entry)
                                if risk > 0:
                                    if direction == "BUY":
                                        beyond_r = max(0.0, (float(stop) - price) / risk)
                                    else:
                                        beyond_r = max(0.0, (price - float(stop)) / risk)
                                    current_max = float(row["max_beyond_stop_r"] or 0)
                                    if beyond_r > current_max:
                                        set_parts.append("max_beyond_stop_r=?")
                                        params.append(beyond_r)

                            if stop_was_first:
                                if hit_tp1 and not row["tp1_after_stop"]:
                                    set_parts.append("tp1_after_stop=1")
                                if hit_tp2 and not row["tp2_after_stop"]:
                                    set_parts.append("tp2_after_stop=1")
                                if hit_tp3 and not row["tp3_after_stop"]:
                                    set_parts.append("tp3_after_stop=1")

                            if set_parts:
                                params.append(row["id"])
                                await db.execute(
                                    f"UPDATE alerts SET {','.join(set_parts)} WHERE id=?",
                                    tuple(params),
                                )

                        for horizon in horizons:
                            checked_key = f"checked_{horizon}m"
                            result_key = f"result_{horizon}m"
                            if row[checked_key] or age_minutes < horizon:
                                continue

                            move = pct_change(price, entry)
                            signed_move = move if direction == "BUY" else -move
                            # A modest positive move counts as a hit. This is learning metadata,
                            # not a guarantee or a trade execution rule.
                            threshold = {
                                5: 0.15, 15: 0.25, 30: 0.35, 60: 0.50,
                                120: 0.70, 240: 1.00, 480: 1.40, 720: 1.80,
                            }[horizon]
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


app = FastAPI(title="Ahmed Delta Early Predictor v2.9 STAGE + FIRST OUTCOME STATS", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "ok": engine.last_error is None,
        "service": "Ahmed Delta Early Predictor v2.9 STAGE + FIRST OUTCOME STATS",
        "last_scan": engine.last_scan,
        "last_error": engine.last_error,
        "scan_number": engine.scan_no,
        "symbols": engine.symbol_count,
        "candidates": engine.candidate_count,
        "alerts_since_start": engine.alert_count,
        "symbol_timeouts_last_scan": engine.symbol_timeouts_last_scan,
        "scan_timeouts_total": engine.scan_timeouts_total,
        "rejection_stats_last_scan": engine.rejection_stats,
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
    limit = max(1, min(limit, 1000000))
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
        overall = await (await db.execute(
            """SELECT COUNT(*) total,
                SUM(checked_15m) evaluated_15m, SUM(result_15m) successes_15m,
                SUM(checked_30m) evaluated_30m, SUM(result_30m) successes_30m,
                SUM(checked_60m) evaluated_1h, SUM(result_60m) successes_1h,
                SUM(checked_120m) evaluated_2h, SUM(result_120m) successes_2h,
                SUM(checked_240m) evaluated_4h, SUM(result_240m) successes_4h,
                SUM(checked_480m) evaluated_8h, SUM(result_480m) successes_8h,
                SUM(checked_720m) evaluated_12h, SUM(result_720m) successes_12h,
                SUM(tp1_hit) tp1_hits, SUM(tp2_hit) tp2_hits, SUM(tp3_hit) tp3_hits,
                SUM(stop_hit) stop_hits,
                SUM(CASE WHEN first_outcome='TP1' THEN 1 ELSE 0 END) tp1_before_stop,
                SUM(CASE WHEN first_outcome='STOP' THEN 1 ELSE 0 END) stop_before_tp1,
                SUM(CASE WHEN first_outcome='AMBIGUOUS' THEN 1 ELSE 0 END) ambiguous,
                SUM(tp1_after_stop) tp1_after_stop,
                SUM(tp2_after_stop) tp2_after_stop,
                SUM(tp3_after_stop) tp3_after_stop,
                AVG(score) avg_score,
                AVG(timeframe_agreement) avg_timeframe_agreement,
                AVG(liquidity_agreement) avg_liquidity_agreement,
                AVG(decision_confidence) avg_decision_confidence,
                AVG(coin_trend_score) avg_coin_trend_score,
                AVG(entry_quality) avg_entry_quality,
                AVG(oi_change) avg_oi_change,
                AVG(quote_volume_24h) avg_quote_volume_24h,
                AVG(CASE WHEN first_outcome='STOP' THEN max_beyond_stop_r END) avg_beyond_stop_r,
                MAX(max_beyond_stop_r) max_beyond_stop_r
             FROM alerts"""
        )).fetchone()

        groups = await (await db.execute(
            """SELECT target_tf,direction,path_type,COUNT(*) cases,
                SUM(checked_15m) evaluated_15m, SUM(result_15m) successes_15m,
                SUM(checked_30m) evaluated_30m, SUM(result_30m) successes_30m,
                SUM(checked_60m) evaluated_1h, SUM(result_60m) successes_1h,
                SUM(checked_120m) evaluated_2h, SUM(result_120m) successes_2h,
                SUM(checked_240m) evaluated_4h, SUM(result_240m) successes_4h,
                SUM(checked_480m) evaluated_8h, SUM(result_480m) successes_8h,
                SUM(checked_720m) evaluated_12h, SUM(result_720m) successes_12h,
                SUM(tp1_hit) tp1_hits, SUM(tp2_hit) tp2_hits, SUM(tp3_hit) tp3_hits,
                SUM(stop_hit) stop_hits,
                SUM(CASE WHEN first_outcome='TP1' THEN 1 ELSE 0 END) tp1_before_stop,
                SUM(CASE WHEN first_outcome='STOP' THEN 1 ELSE 0 END) stop_before_tp1,
                SUM(CASE WHEN first_outcome='AMBIGUOUS' THEN 1 ELSE 0 END) ambiguous,
                SUM(tp1_after_stop) tp1_after_stop,
                SUM(tp2_after_stop) tp2_after_stop,
                SUM(tp3_after_stop) tp3_after_stop,
                AVG(score) avg_score,
                AVG(timeframe_agreement) avg_timeframe_agreement,
                AVG(liquidity_agreement) avg_liquidity_agreement,
                AVG(decision_confidence) avg_decision_confidence,
                AVG(coin_trend_score) avg_coin_trend_score,
                AVG(entry_quality) avg_entry_quality,
                AVG(oi_change) avg_oi_change,
                AVG(quote_volume_24h) avg_quote_volume_24h,
                AVG(CASE WHEN first_outcome='STOP' THEN max_beyond_stop_r END) avg_beyond_stop_r,
                MAX(max_beyond_stop_r) max_beyond_stop_r
             FROM alerts
             GROUP BY target_tf,direction,path_type
             ORDER BY cases DESC"""
        )).fetchall()

    def enrich(row):
        d = dict(row)
        decided = (d.get("tp1_before_stop") or 0) + (d.get("stop_before_tp1") or 0)
        d["trade_win_rate_pct"] = (
            round((d.get("tp1_before_stop") or 0) * 100 / decided, 2)
            if decided else None
        )
        stop_first = d.get("stop_before_tp1") or 0
        d["tp1_after_stop_pct"] = round((d.get("tp1_after_stop") or 0) * 100 / stop_first, 2) if stop_first else None
        d["tp2_after_stop_pct"] = round((d.get("tp2_after_stop") or 0) * 100 / stop_first, 2) if stop_first else None
        d["tp3_after_stop_pct"] = round((d.get("tp3_after_stop") or 0) * 100 / stop_first, 2) if stop_first else None

        # Wilson lower bound ranks large/reliable samples above tiny lucky samples.
        wins = d.get("tp1_before_stop") or 0
        losses = d.get("stop_before_tp1") or 0
        n = wins + losses
        if n:
            z = 1.96
            phat = wins / n
            denom = 1 + z*z/n
            centre = phat + z*z/(2*n)
            margin = z * ((phat*(1-phat)/n + z*z/(4*n*n)) ** 0.5)
            d["reliability_rank_pct"] = round(100 * (centre - margin) / denom, 2)
        else:
            d["reliability_rank_pct"] = None

        for key in (
            "avg_score", "avg_timeframe_agreement", "avg_liquidity_agreement",
            "avg_decision_confidence", "avg_coin_trend_score", "avg_entry_quality",
            "avg_oi_change", "avg_quote_volume_24h", "avg_beyond_stop_r", "max_beyond_stop_r",
        ):
            if d.get(key) is not None:
                d[key] = round(float(d[key]), 3)

        for label in ("15m", "30m", "1h", "2h", "4h", "8h", "12h"):
            e, s = d.get(f"evaluated_{label}") or 0, d.get(f"successes_{label}") or 0
            d[f"success_rate_{label}_pct"] = round(s * 100 / e, 2) if e else None
        return d

    return {
        "overall": enrich(overall),
        "groups": sorted(
            [enrich(row) for row in groups],
            key=lambda x: (x.get("reliability_rank_pct") or -1, x.get("cases") or 0),
            reverse=True,
        ),
        "recommended_windows": {
            "1h": ["15m", "30m", "1h", "2h", "4h"],
            "4h": ["1h", "2h", "4h", "8h", "12h"]
        },
        "trade_result_definition": "trade_win_rate_pct = TP1 before Stop among decided trades",
        "stop_recovery_definition": "after_stop = Stop was hit first, then price later reached the original target"
    }


@app.get("/learning/checkpoints")
async def learning_checkpoints(limit: int = 100):
    limit = max(1, min(limit, 1000000))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM checkpoints ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        ).fetchall()
    return [dict(row) for row in rows]



@app.get("/stage-stats")
async def stage_stats():
    """Stage × direction × timeframe, including which arrived first."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cols={r[1] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()}
        def sm(c,a): return f"SUM(COALESCE({c},0)) AS {a}" if c in cols else f"0 AS {a}"
        def av(c,a): return f"AVG({c}) AS {a}" if c in cols else f"NULL AS {a}"
        select=[
            "direction","target_tf","path_type","COUNT(*) AS cases",
            sm("tp1_hit","tp1_hits"),sm("tp2_hit","tp2_hits"),sm("tp3_hit","tp3_hits"),sm("stop_hit","stop_hits"),
            sm("tp1_before_stop","tp1_before_stop"),sm("stop_before_tp1","stop_before_tp1"),
            sm("tp1_after_stop","tp1_after_stop"),sm("tp2_after_stop","tp2_after_stop"),sm("tp3_after_stop","tp3_after_stop"),
            av("score","avg_score"),av("timeframe_agreement","avg_timeframe_agreement"),
            av("liquidity_agreement","avg_liquidity_agreement"),av("decision_confidence","avg_decision_confidence"),
            av("coin_trend_score","avg_coin_trend_score"),av("entry_quality","avg_entry_quality"),
            av("oi_change","avg_oi_change"),av("quote_volume_24h","avg_quote_volume_24h"),
            av("max_beyond_stop_r","avg_beyond_stop_r"),
        ]
        rows=[dict(r) for r in conn.execute(
            f"SELECT {', '.join(select)} FROM alerts GROUP BY direction,target_tf,path_type"
        ).fetchall()]
        names={"NORMAL":("EARLY","🟢 إشارة مبكرة"),"FAST_TWO":("CONFIRMED","🔵 إشارة مبكرة قوية"),"ABNORMAL_ONE":("EXPLOSION","🔥 إشارة مبكرة جدًا")}
        for d in rows:
            d["stage"],d["stage_ar"]=names.get(d.get("path_type"),(d.get("path_type") or "UNKNOWN","⚪ غير مصنف"))
            w=int(d.get("tp1_before_stop") or 0); l=int(d.get("stop_before_tp1") or 0); n=w+l
            d["decided"]=n
            d["tp1_first_pct"]=round(100*w/n,2) if n else None
            d["stop_first_pct"]=round(100*l/n,2) if n else None
            d["first_winner"]="TP1" if n and w>l else "STOP" if n and l>w else "TIE" if n else None
            for k in ("tp1_after_stop","tp2_after_stop","tp3_after_stop"):
                d[k+"_pct"]=round(100*(d.get(k) or 0)/l,2) if l else None
            if n:
                z=1.96; p=w/n; den=1+z*z/n
                cen=p+z*z/(2*n); mar=z*((p*(1-p)/n+z*z/(4*n*n))**0.5)
                d["reliability_pct"]=round(100*(cen-mar)/den,2)
            else: d["reliability_pct"]=None
            for k,v in list(d.items()):
                if k.startswith("avg_") and v is not None: d[k]=round(float(v),3)
        rows.sort(key=lambda x:(x.get("reliability_pct") or -1,x.get("cases") or 0),reverse=True)
        return {"groups":rows,"definition":"TP1-first means TP1 reached before Stop; STOP-first means Stop reached before TP1"}

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return """<!doctype html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ahmed Delta Early Predictor v2.8</title>
<style>
:root{--bg:#081019;--card:#111d2a;--line:#25384b;--text:#f5f7fb;--muted:#9bb3ca;--good:#59d98e;--warn:#f7c65d;--bad:#ff7070;--blue:#6db7ff}
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;background:var(--bg);color:var(--text);margin:0;padding:18px}
.wrap{max-width:1500px;margin:auto}
h1{font-size:30px;margin:6px 0}.sub{color:var(--muted);margin-bottom:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin:12px 0 22px}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:15px}
.card .n{font-size:28px;font-weight:800;margin-top:8px}.card .l{color:var(--muted)}
.best{border:1px solid #9a7528;background:linear-gradient(135deg,#172032,#241c0d)}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:15px;overflow:hidden;font-size:13px}
th,td{padding:10px 8px;border-bottom:1px solid var(--line);text-align:center;white-space:nowrap}
th{background:#16263a;position:sticky;top:0;z-index:2}
tr:hover{background:#142233}.scroll{overflow:auto;border-radius:15px;border:1px solid var(--line)}
.good{color:var(--good);font-weight:700}.warn{color:var(--warn)}.bad{color:var(--bad)}
.path{font-weight:800}.normal{color:#62d98e}.fast{color:#62b7ff}.abn{color:#ff8c42}
a{color:var(--blue)}.links{margin:12px 0}.small{font-size:12px;color:var(--muted)}
</style>
</head>
<body><div class="wrap">
<h1>Ahmed Delta Early Predictor 📊</h1>
<div class="sub">v2.9 — أسماء المراحل + مين وصل أولًا + إحصائيات تفصيلية بلا حد</div>
<div class="links"><a href="/stats">JSON Stats</a> · <a href="/alerts">Alerts</a> · <a href="/health">Health</a></div>

<div id="summary" class="grid"></div>
<div id="best"></div>

<h2>🏆 ترتيب الإشارات من الأقوى للأضعف</h2>
<div class="small">الترتيب يعتمد على Wilson reliability وليس النسبة الخام فقط، حتى لا تتصدر عينة صغيرة بالصدفة.</div>
<div class="scroll"><table>
<thead><tr>
<th>الترتيب</th><th>العلامة</th><th>الفريم</th><th>الاتجاه</th><th>الحالات</th>
<th>TP1 قبل SL</th><th>SL قبل TP1</th><th>نجاح الصفقة</th><th>ثقة العينة</th>
<th>TP2</th><th>TP3</th><th>SL→TP1</th><th>SL→TP2</th><th>SL→TP3</th>
<th>Avg Score</th><th>توافق الفريمات</th><th>السيولة</th><th>اتجاه العملة</th>
<th>ثقة القرار</th><th>جودة الدخول</th><th>Avg OI Δ</th><th>Avg Vol 24H</th>
<th>Avg تجاوز SL (R)</th><th>Max تجاوز SL (R)</th>
<th>15m</th><th>30m</th><th>1H</th><th>2H</th><th>4H</th><th>8H</th><th>12H</th>
</tr></thead><tbody id="rows"></tbody>
</table></div>
</div>


<h2>🚦 إحصائيات المراحل — مين وصل أولًا؟</h2>
<div class="small">كل مرحلة مفصولة حسب BUY/SELL و1H/4H. الأساس: TP1 قبل الوقف أم الوقف قبل TP1.</div>
<div class="scroll"><table>
<thead><tr><th>#</th><th>اسم التنبيه</th><th>المرحلة</th><th>الفريم</th><th>الاتجاه</th><th>الحالات</th><th>المحسومة</th>
<th>TP1 أولًا</th><th>% TP1 أولًا</th><th>STOP أولًا</th><th>% STOP أولًا</th><th>الفائز</th><th>ثقة العينة</th>
<th>TP1</th><th>TP2</th><th>TP3</th><th>SL→TP1</th><th>SL→TP2</th><th>SL→TP3</th>
<th>Score</th><th>توافق الفريمات</th><th>السيولة</th><th>اتجاه العملة</th><th>ثقة القرار</th><th>جودة الدخول</th><th>تجاوز SL (R)</th>
</tr></thead><tbody id="stageRows"></tbody></table></div>
<script>
const fmt=(x,d=1)=> x==null?'—':Number(x).toFixed(d);
const pct=x=>x==null?'—':fmt(x,1)+'%';
const pathInfo=p=> p==='NORMAL'?['🟢 إشارة مبكرة','normal']:p==='FAST_TWO'?['🔵 إشارة مبكرة قوية','fast']:['🔥 إشارة مبكرة جدًا','abn'];
async function load(){
  const r=await fetch('/stats'); const d=await r.json(); const o=d.overall||{};
  const cards=[
    ['كل التنبيهات',o.total],['تم تقييم 1H',o.evaluated_1h],['TP1',o.tp1_hits],
    ['TP2',o.tp2_hits],['TP3',o.tp3_hits],['ضرب Stop',o.stop_hits],
    ['TP1 قبل Stop',pct(o.trade_win_rate_pct)],['Stop ثم TP1',pct(o.tp1_after_stop_pct)],
    ['Stop ثم TP2',pct(o.tp2_after_stop_pct)],['Stop ثم TP3',pct(o.tp3_after_stop_pct)]
  ];
  document.getElementById('summary').innerHTML=cards.map(c=>`<div class="card"><div class="l">${c[0]}</div><div class="n">${c[1]??'—'}</div></div>`).join('');
  const gs=d.groups||[];
  if(gs.length){
    const b=gs[0], pi=pathInfo(b.path_type);
    document.getElementById('best').innerHTML=`<div class="card best"><div class="l">🏆 أقوى إشارة إحصائيًا الآن</div><div class="n ${pi[1]}">${pi[0]} — ${b.direction} — ${String(b.target_tf).toUpperCase()}</div><div>نجاح TP1 قبل Stop: <b>${pct(b.trade_win_rate_pct)}</b> · الحالات: <b>${b.cases}</b> · ثقة العينة: <b>${pct(b.reliability_rank_pct)}</b></div></div>`;
  }
  document.getElementById('rows').innerHTML=gs.map((g,i)=>{
    const pi=pathInfo(g.path_type), win=g.trade_win_rate_pct;
    const cls=win>=55?'good':win>=45?'warn':'bad';
    return `<tr>
      <td>${i+1}</td><td class="path ${pi[1]}">${pi[0]}</td><td>${String(g.target_tf).toUpperCase()}</td><td>${g.direction}</td><td>${g.cases}</td>
      <td>${g.tp1_before_stop??0}</td><td>${g.stop_before_tp1??0}</td><td class="${cls}">${pct(win)}</td><td>${pct(g.reliability_rank_pct)}</td>
      <td>${g.tp2_hits??0}</td><td>${g.tp3_hits??0}</td><td>${pct(g.tp1_after_stop_pct)}</td><td>${pct(g.tp2_after_stop_pct)}</td><td>${pct(g.tp3_after_stop_pct)}</td>
      <td>${fmt(g.avg_score)}</td><td>${pct(g.avg_timeframe_agreement)}</td><td>${pct(g.avg_liquidity_agreement)}</td><td>${pct(g.avg_coin_trend_score)}</td>
      <td>${pct(g.avg_decision_confidence)}</td><td>${pct(g.avg_entry_quality)}</td><td>${fmt(g.avg_oi_change,3)}</td><td>${g.avg_quote_volume_24h==null?'—':Math.round(g.avg_quote_volume_24h).toLocaleString()}</td>
      <td>${fmt(g.avg_beyond_stop_r,2)}R</td><td>${fmt(g.max_beyond_stop_r,2)}R</td>
      <td>${pct(g.success_rate_15m_pct)}</td><td>${pct(g.success_rate_30m_pct)}</td><td>${pct(g.success_rate_1h_pct)}</td><td>${pct(g.success_rate_2h_pct)}</td><td>${pct(g.success_rate_4h_pct)}</td><td>${pct(g.success_rate_8h_pct)}</td><td>${pct(g.success_rate_12h_pct)}</td>
    </tr>`
  }).join('');
}

async function loadStages(){
 const r=await fetch('/stage-stats'); const d=await r.json();
 document.getElementById('stageRows').innerHTML=(d.groups||[]).map((g,i)=>{
  const cls=(g.tp1_first_pct??0)>=55?'good':(g.tp1_first_pct??0)>=45?'warn':'bad';
  const winner=g.first_winner==='TP1'?'✅ TP1':g.first_winner==='STOP'?'❌ STOP':'➖ تعادل';
  return `<tr><td>${i+1}</td><td class="path">${g.stage_ar}</td><td>${g.stage}</td><td>${String(g.target_tf).toUpperCase()}</td><td>${g.direction}</td><td>${g.cases}</td><td>${g.decided}</td>
  <td>${g.tp1_before_stop}</td><td class="${cls}">${pct(g.tp1_first_pct)}</td><td>${g.stop_before_tp1}</td><td>${pct(g.stop_first_pct)}</td><td>${winner}</td><td>${pct(g.reliability_pct)}</td>
  <td>${g.tp1_hits}</td><td>${g.tp2_hits}</td><td>${g.tp3_hits}</td><td>${pct(g.tp1_after_stop_pct)}</td><td>${pct(g.tp2_after_stop_pct)}</td><td>${pct(g.tp3_after_stop_pct)}</td>
  <td>${fmt(g.avg_score)}</td><td>${pct(g.avg_timeframe_agreement)}</td><td>${pct(g.avg_liquidity_agreement)}</td><td>${pct(g.avg_coin_trend_score)}</td>
  <td>${pct(g.avg_decision_confidence)}</td><td>${pct(g.avg_entry_quality)}</td><td>${fmt(g.avg_beyond_stop_r,2)}R</td></tr>`;
 }).join('');
}
load(); loadStages(); setInterval(()=>{load();loadStages();},30000);

</script>
</body></html>"""


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
