import asyncio
import json
import logging
import math
import os
import signal
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Deque, Dict, List, Optional, Tuple
from urllib.parse import quote

import aiohttp
import numpy as np
from zoneinfo import ZoneInfo

BINANCE_REST = os.getenv("BINANCE_REST", "https://fapi.binance.com")
BINANCE_WS = os.getenv("BINANCE_WS", "wss://fstream.binance.com")
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
TIMEFRAMES = [x.strip() for x in os.getenv("TIMEFRAMES", "15m,1h,4h,1d,1w").split(",") if x.strip()]
EARLY_SCORE = float(os.getenv("EARLY_SCORE", "58"))
GOLDEN_SCORE = float(os.getenv("GOLDEN_SCORE", "78"))
EARLY_MTF_COUNT = int(os.getenv("EARLY_MTF_COUNT", "1"))
REQUIRE_GOLD_MTF_COUNT = int(os.getenv("REQUIRE_GOLD_MTF_COUNT", "3"))
REQUIRE_GOLD_BREAKOUT = os.getenv("REQUIRE_GOLD_BREAKOUT", "true").lower() == "true"
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
WS_STREAMS_PER_CONNECTION = int(os.getenv("WS_STREAMS_PER_CONNECTION", "180"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "0"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "260"))
REST_CONCURRENCY = int(os.getenv("REST_CONCURRENCY", "12"))
SAUDI_TZ = ZoneInfo("Asia/Riyadh")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("ahmed-scanner")

TF_LABEL = {"15m": "15M", "1h": "1H", "4h": "4H", "1d": "D", "1w": "W"}

@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    closed: bool = False

@dataclass
class SignalState:
    candle_open_time: int = 0
    buy_seen: bool = False
    sell_seen: bool = False
    buy_was_active: bool = False
    sell_was_active: bool = False

@dataclass
class MarketState:
    candles: Dict[Tuple[str, str], Deque[Candle]] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=HISTORY_LIMIT)))
    signals: Dict[Tuple[str, str], SignalState] = field(default_factory=lambda: defaultdict(SignalState))
    ready: set = field(default_factory=set)

state = MarketState()
shutdown_event = asyncio.Event()
telegram_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=5000)


def ema(values: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(values), np.nan, dtype=float)
    if len(values) < length:
        return out
    seed = float(np.mean(values[:length]))
    out[length - 1] = seed
    alpha = 2.0 / (length + 1.0)
    for i in range(length, len(values)):
        out[i] = alpha * values[i] + (1.0 - alpha) * out[i - 1]
    return out


def sma(values: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(values), np.nan, dtype=float)
    if len(values) < length:
        return out
    c = np.cumsum(np.insert(values, 0, 0.0))
    out[length - 1:] = (c[length:] - c[:-length]) / length
    return out


def rsi(values: np.ndarray, length: int = 14) -> np.ndarray:
    out = np.full(len(values), np.nan, dtype=float)
    if len(values) <= length:
        return out
    diff = np.diff(values)
    gains = np.where(diff > 0, diff, 0.0)
    losses = np.where(diff < 0, -diff, 0.0)
    avg_gain = np.mean(gains[:length])
    avg_loss = np.mean(losses[:length])
    out[length] = 100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    for i in range(length + 1, len(values)):
        avg_gain = (avg_gain * (length - 1) + gains[i - 1]) / length
        avg_loss = (avg_loss * (length - 1) + losses[i - 1]) / length
        out[i] = 100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    return out


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int = 14) -> np.ndarray:
    tr = np.empty(len(close), dtype=float)
    tr[0] = high[0] - low[0]
    tr[1:] = np.maximum(high[1:] - low[1:], np.maximum(np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])))
    out = np.full(len(close), np.nan, dtype=float)
    if len(close) < length:
        return out
    out[length - 1] = np.mean(tr[:length])
    for i in range(length, len(close)):
        out[i] = (out[i - 1] * (length - 1) + tr[i]) / length
    return out


def arrays(candles: Deque[Candle]):
    c = list(candles)
    return (
        np.array([x.open for x in c], dtype=float),
        np.array([x.high for x in c], dtype=float),
        np.array([x.low for x in c], dtype=float),
        np.array([x.close for x in c], dtype=float),
        np.array([x.volume for x in c], dtype=float),
    )


def score_calc(candles: Deque[Candle]) -> Optional[float]:
    if len(candles) < 205:
        return None
    o, h, l, c, v = arrays(candles)
    e20, e50, e200 = ema(c, 20), ema(c, 50), ema(c, 200)
    rs = rsi(c, 14)
    mfast, mslow = ema(c, 12), ema(c, 26)
    macd = mfast - mslow
    sig = ema(macd[~np.isnan(macd)], 9)
    fullsig = np.full(len(c), np.nan)
    first = np.where(~np.isnan(macd))[0][0]
    fullsig[first:first + len(sig)] = sig
    hist = macd - fullsig
    at = atr(h, l, c, 14)
    av = sma(v, 20)
    i = len(c) - 1
    needed = [e20[i], e50[i], e200[i], rs[i], macd[i], fullsig[i], hist[i], hist[i-1], at[i], av[i]]
    if any(np.isnan(x) for x in needed):
        return None
    vol_ratio = v[i] / av[i] if av[i] > 0 else 1.0
    body_ratio = abs(c[i] - o[i]) / at[i] if at[i] > 0 else 0.0
    rng = max(h[i] - l[i], 1e-12)
    close_loc = (c[i] - l[i]) / rng
    s = 50.0
    s += 7 if c[i] > e20[i] else -7
    s += 8 if e20[i] > e50[i] else -8
    s += 10 if e50[i] > e200[i] else -10
    s += 6 if c[i] > e200[i] else -6
    s += 7 if macd[i] > fullsig[i] else -7
    s += 5 if hist[i] > 0 else -5
    s += 4 if hist[i] > hist[i-1] else -4
    s += 5 if rs[i] > 55 else (-5 if rs[i] < 45 else 0)
    s += 4 if c[i] > o[i] and vol_ratio >= 1.2 else (-4 if c[i] < o[i] and vol_ratio >= 1.2 else 0)
    s += 3 if c[i] > o[i] and body_ratio >= 0.6 else (-3 if c[i] < o[i] and body_ratio >= 0.6 else 0)
    s += 3 if close_loc >= 0.7 else (-3 if close_loc <= 0.3 else 0)
    return max(0.0, min(100.0, s))


def momentum_scores(candles: Deque[Candle]) -> Optional[Tuple[float, float, bool, bool]]:
    if len(candles) < 205:
        return None
    o, h, l, c, v = arrays(candles)
    i = len(c) - 1
    e25, e50, e200 = ema(c, 25), ema(c, 50), ema(c, 200)
    mfast, mslow = ema(c, 12), ema(c, 26)
    macd = mfast - mslow
    sig_short = ema(macd[~np.isnan(macd)], 9)
    sig = np.full(len(c), np.nan)
    first = np.where(~np.isnan(macd))[0][0]
    sig[first:first + len(sig_short)] = sig_short
    hist = macd - sig
    av = sma(v, 20)
    at = atr(h, l, c, 14)
    vals = [e25[i], e50[i], e200[i], macd[i], sig[i], hist[i], hist[i-1], av[i], at[i]]
    if any(np.isnan(x) for x in vals):
        return None
    rng = max(h[i] - l[i], 1e-12)
    close_pos = max(0.0, min(1.0, (c[i] - l[i]) / rng))
    buy_pct = close_pos * 100.0
    sell_pct = 100.0 - buy_pct
    vol_ratio = v[i] / av[i] if av[i] > 0 else 1.0
    body_ratio = abs(c[i] - o[i]) / at[i] if at[i] > 0 else 0.0
    prev_high = np.max(h[max(0, i-10):i])
    prev_low = np.min(l[max(0, i-10):i])
    bull_breakout = c[i] > prev_high and c[i] > o[i]
    bear_breakout = c[i] < prev_low and c[i] < o[i]

    bull = 0.0
    bull += 8 if c[i] > o[i] else 0
    bull += 7 if close_pos >= .75 else (4 if close_pos >= .60 else 0)
    bull += 13 if vol_ratio >= 2 else (10 if vol_ratio >= 1.5 else (5 if vol_ratio >= 1.2 else 0))
    bull += 10 if body_ratio >= 1 else (7 if body_ratio >= .6 else 0)
    bull += 8 if macd[i] > sig[i] else 0
    bull += 7 if hist[i] > 0 else 0
    bull += 5 if hist[i] > hist[i-1] else 0
    bull += 5 if c[i] > e25[i] else 0
    bull += 6 if c[i] > e50[i] else 0
    bull += 7 if c[i] > e200[i] else 0
    bull += 4 if e25[i] > e50[i] else 0
    bull += 5 if e50[i] > e200[i] else 0
    bull += 8 if buy_pct >= 70 else (5 if buy_pct >= 60 else (3 if buy_pct >= 55 else 0))
    bull += 7 if bull_breakout else 0

    bear = 0.0
    bear += 8 if c[i] < o[i] else 0
    bear += 7 if close_pos <= .25 else (4 if close_pos <= .40 else 0)
    bear += 13 if vol_ratio >= 2 else (10 if vol_ratio >= 1.5 else (5 if vol_ratio >= 1.2 else 0))
    bear += 10 if body_ratio >= 1 else (7 if body_ratio >= .6 else 0)
    bear += 8 if macd[i] < sig[i] else 0
    bear += 7 if hist[i] < 0 else 0
    bear += 5 if hist[i] < hist[i-1] else 0
    bear += 5 if c[i] < e25[i] else 0
    bear += 6 if c[i] < e50[i] else 0
    bear += 7 if c[i] < e200[i] else 0
    bear += 4 if e25[i] < e50[i] else 0
    bear += 5 if e50[i] < e200[i] else 0
    bear += 8 if sell_pct >= 70 else (5 if sell_pct >= 60 else (3 if sell_pct >= 55 else 0))
    bear += 7 if bear_breakout else 0
    return min(100.0, bull), min(100.0, bear), bull_breakout, bear_breakout


def strength_text(score: float) -> str:
    if score >= 80:
        return "قوية"
    if score >= 65:
        return "متوسطة"
    return "مبكرة"


def format_price(price: float) -> str:
    if price >= 1000: return f"{price:,.2f}"
    if price >= 1: return f"{price:.4f}".rstrip("0").rstrip(".")
    return f"{price:.8f}".rstrip("0").rstrip(".")


def links(symbol: str, timeframe: str) -> str:
    b = f"https://www.binance.com/en/futures/{symbol}"
    tv_symbol = quote(f"BINANCE:{symbol}.P", safe="")
    interval = {"15m":"15", "1h":"60", "4h":"240", "1d":"D", "1w":"W"}.get(timeframe, "15")
    t = f"https://www.tradingview.com/chart/?symbol={tv_symbol}&interval={interval}"
    return f'<a href="{b}">Binance Futures</a> | <a href="{t}">TradingView</a>'


def signal_message(symbol: str, timeframe: str, price: float, side: str, score: float, event_ms: int) -> str:
    buy = side == "BUY"
    title = "🚨 إشارة مبكرة - فرصة شراء جديدة!" if buy else "🚨 إشارة مبكرة - فرصة بيع جديدة!"
    type_line = "🟢 النوع: علامة مبكرة للشراء (Buy Signal)" if buy else "🔴 النوع: علامة مبكرة للبيع (Sell Signal)"
    dt = datetime.fromtimestamp(event_ms / 1000, SAUDI_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"{title}\n"
        "-----------------------------------\n"
        f"💰 العملة: #{symbol}.P\n"
        f"⏰ الفريم: {TF_LABEL.get(timeframe, timeframe)}\n"
        f"💲 السعر الحالي: {format_price(price)}\n\n"
        f"{type_line}\n"
        f"⚡️ وقت الظهور: {dt} (توقيت السعودية)\n"
        "⏳ حالة الشمعة: قيد التكوين ⚠️\n"
        f"🔥 قوة الإشارة: {strength_text(score)} ({score:.1f}%)\n\n"
        f"🔗 {links(symbol, timeframe)}"
    )


def cancel_message(symbol: str, timeframe: str, price: float, side: str, event_ms: int) -> str:
    dt = datetime.fromtimestamp(event_ms / 1000, SAUDI_TZ).strftime("%Y-%m-%d %H:%M:%S")
    side_text = "الشراء" if side == "BUY" else "البيع"
    return (
        "⚠️ تنبيه: انطفاء واختفاء العلامة!\n"
        "-----------------------------------\n"
        f"💰 العملة: #{symbol}.P\n"
        f"⏰ الفريم: {TF_LABEL.get(timeframe, timeframe)}\n"
        f"💲 السعر الحالي: {format_price(price)}\n\n"
        f"🔄 الحالة: انطفأت علامة {side_text} (تم إلغاء الإشارة السابقة)\n"
        f"⚡️ وقت التنبيه: {dt} (توقيت السعودية)\n\n"
        f"🔗 {links(symbol, timeframe)}"
    )


async def enqueue(msg: str):
    try:
        telegram_queue.put_nowait(msg)
    except asyncio.QueueFull:
        log.error("Telegram queue full; dropping alert")


async def telegram_worker(session: aiohttp.ClientSession):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    while not shutdown_event.is_set():
        msg = await telegram_queue.get()
        for attempt in range(6):
            try:
                async with session.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=20) as r:
                    data = await r.json(content_type=None)
                    if r.status == 200 and data.get("ok"):
                        break
                    retry_after = data.get("parameters", {}).get("retry_after")
                    if retry_after:
                        await asyncio.sleep(float(retry_after) + 0.2)
                    else:
                        log.warning("Telegram error %s: %s", r.status, data)
                        await asyncio.sleep(min(2 ** attempt, 30))
            except Exception as exc:
                log.warning("Telegram send failed: %s", exc)
                await asyncio.sleep(min(2 ** attempt, 30))
        telegram_queue.task_done()
        await asyncio.sleep(0.06)


async def fetch_symbols(session: aiohttp.ClientSession) -> List[str]:
    async with session.get(f"{BINANCE_REST}/fapi/v1/exchangeInfo", timeout=30) as r:
        r.raise_for_status()
        data = await r.json()
    symbols = [s["symbol"] for s in data["symbols"] if s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"]
    symbols.sort()
    return symbols[:MAX_SYMBOLS] if MAX_SYMBOLS > 0 else symbols


async def fetch_history(session: aiohttp.ClientSession, sem: asyncio.Semaphore, symbol: str, tf: str):
    params = {"symbol": symbol, "interval": tf, "limit": HISTORY_LIMIT}
    async with sem:
        for attempt in range(8):
            try:
                async with session.get(f"{BINANCE_REST}/fapi/v1/klines", params=params, timeout=30) as r:
                    if r.status in (418, 429):
                        await asyncio.sleep(5 + attempt * 3)
                        continue
                    r.raise_for_status()
                    rows = await r.json()
                dq = state.candles[(symbol, tf)]
                dq.clear()
                for x in rows:
                    dq.append(Candle(int(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), int(x[6]), True))
                state.ready.add((symbol, tf))
                return
            except Exception as exc:
                if attempt == 7:
                    log.error("History failed %s %s: %s", symbol, tf, exc)
                await asyncio.sleep(min(1.5 ** attempt, 20))


async def initialize_history(session: aiohttp.ClientSession, symbols: List[str]):
    sem = asyncio.Semaphore(REST_CONCURRENCY)
    tasks = [fetch_history(session, sem, s, tf) for s in symbols for tf in TIMEFRAMES]
    done = 0
    for fut in asyncio.as_completed(tasks):
        await fut
        done += 1
        if done % 100 == 0:
            log.info("History initialized %s/%s", done, len(tasks))


def update_candle(symbol: str, tf: str, k: dict):
    dq = state.candles[(symbol, tf)]
    candle = Candle(int(k["t"]), float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"]), int(k["T"]), bool(k["x"]))
    if dq and dq[-1].open_time == candle.open_time:
        dq[-1] = candle
    else:
        dq.append(candle)


def mtf_counts(symbol: str, current_tf: str) -> Optional[Tuple[int, int]]:
    tfs = [current_tf, "1h", "4h", "1d"]
    scores = []
    for tf in tfs:
        sc = score_calc(state.candles[(symbol, tf)])
        if sc is None:
            return None
        scores.append(sc)
    return sum(x >= 58 for x in scores), sum(x <= 42 for x in scores)


async def evaluate(symbol: str, tf: str, event_ms: int):
    if not all((symbol, x) in state.ready for x in {tf, "1h", "4h", "1d"}):
        return
    dq = state.candles[(symbol, tf)]
    scores = momentum_scores(dq)
    counts = mtf_counts(symbol, tf)
    if not scores or not counts or not dq:
        return
    bull, bear, bull_breakout, bear_breakout = scores
    bull_count, bear_count = counts
    effective = min(EARLY_SCORE, GOLDEN_SCORE - 1.0)
    gold_buy = bull >= GOLDEN_SCORE and bull > bear and bull_count >= REQUIRE_GOLD_MTF_COUNT and (bull_breakout or not REQUIRE_GOLD_BREAKOUT)
    gold_sell = bear >= GOLDEN_SCORE and bear > bull and bear_count >= REQUIRE_GOLD_MTF_COUNT and (bear_breakout or not REQUIRE_GOLD_BREAKOUT)
    buy_active = bull >= effective and bull > bear and bull_count >= EARLY_MTF_COUNT and not gold_buy
    sell_active = bear >= effective and bear > bull and bear_count >= EARLY_MTF_COUNT and not gold_sell

    sig = state.signals[(symbol, tf)]
    current_open = dq[-1].open_time
    if sig.candle_open_time != current_open:
        sig.candle_open_time = current_open
        sig.buy_seen = sig.sell_seen = False
        sig.buy_was_active = sig.sell_was_active = False

    buy_appeared = buy_active and not sig.buy_seen
    sell_appeared = sell_active and not sig.sell_seen
    buy_cancelled = sig.buy_was_active and not buy_active and not gold_buy
    sell_cancelled = sig.sell_was_active and not sell_active and not gold_sell

    if buy_appeared:
        sig.buy_seen = True
        await enqueue(signal_message(symbol, tf, dq[-1].close, "BUY", bull, event_ms))
    if sell_appeared:
        sig.sell_seen = True
        await enqueue(signal_message(symbol, tf, dq[-1].close, "SELL", bear, event_ms))
    if buy_cancelled:
        await enqueue(cancel_message(symbol, tf, dq[-1].close, "BUY", event_ms))
    if sell_cancelled:
        await enqueue(cancel_message(symbol, tf, dq[-1].close, "SELL", event_ms))

    sig.buy_was_active = buy_active
    sig.sell_was_active = sell_active


async def ws_shard(session: aiohttp.ClientSession, streams: List[str], shard_id: int):
    url = f"{BINANCE_WS}/stream?streams={'/'.join(streams)}"
    backoff = 1
    while not shutdown_event.is_set():
        try:
            async with session.ws_connect(url, heartbeat=120, receive_timeout=180, autoclose=True, max_msg_size=2_000_000) as ws:
                log.info("WS shard %s connected (%s streams)", shard_id, len(streams))
                backoff = 1
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        payload = json.loads(msg.data)
                        data = payload.get("data", {})
                        if data.get("e") != "kline":
                            continue
                        symbol = data["s"]
                        k = data["k"]
                        tf = k["i"]
                        update_candle(symbol, tf, k)
                        await evaluate(symbol, tf, int(data.get("E", k["T"])))
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("WS shard %s disconnected: %s", shard_id, exc)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)


async def main():
    timeout = aiohttp.ClientTimeout(total=45)
    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tg_task = asyncio.create_task(telegram_worker(session))
        symbols = await fetch_symbols(session)
        log.info("Monitoring %s USDT perpetual futures symbols on %s", len(symbols), TIMEFRAMES)
        if SEND_STARTUP_MESSAGE:
            await enqueue(
                "✅ تم تشغيل Ahmed Early Signal Scanner\n"
                f"💰 العملات: {len(symbols)} عقد USDT دائم\n"
                f"⏰ الفريمات: {', '.join(TF_LABEL.get(x,x) for x in TIMEFRAMES)}\n"
                "📡 الحالة: فحص مباشر أثناء تكوّن الشمعة"
            )
        await initialize_history(session, symbols)
        streams = [f"{s.lower()}@kline_{tf}" for s in symbols for tf in TIMEFRAMES]
        shards = [streams[i:i + WS_STREAMS_PER_CONNECTION] for i in range(0, len(streams), WS_STREAMS_PER_CONNECTION)]
        log.info("Starting %s websocket shards", len(shards))
        ws_tasks = [asyncio.create_task(ws_shard(session, shard, idx + 1)) for idx, shard in enumerate(shards)]
        await shutdown_event.wait()
        for task in ws_tasks:
            task.cancel()
        await asyncio.gather(*ws_tasks, return_exceptions=True)
        tg_task.cancel()
        await asyncio.gather(tg_task, return_exceptions=True)


def request_shutdown(*_):
    shutdown_event.set()


if __name__ == "__main__":
    for sig_name in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig_name, request_shutdown)
    asyncio.run(main())
