# Ahmed Candlestick Alert Bot

بوت مستقل لنماذج الشموع اليابانية BUY/SELL على Binance USDT-M Futures.

## الفريمات
- اكتشاف النموذج الرئيسي: 15m, 1h, 4h, 1d
- تأكيد الدخول داخل المنطقة: 3m, 5m, 15m
- التنبيه يرسل فور اكتشاف إغلاق شمعة تأكيد جديدة، بدون انتظار شمعة إضافية.

## المنطق
1. يكتشف نموذج شموع رئيسي قوي.
2. ينشئ منطقة شراء/بيع من نطاق النموذج.
3. يحتفظ بالفرصة ويراقب السعر.
4. عند وجود السعر في/قرب المنطقة وإغلاق نموذج تأكيد على 3m/5m/15m، يرسل Telegram.
5. يحسب SL من قاع/قمة النموذج + هامش ATR، وTP1/TP2/TP3 = 1R/2R/3R.
6. يستمر بتتبع الإشارة بعد SL لمعرفة: الرجوع للدخول، TP1/TP2/TP3 بعد الوقف، ومدة كل مرحلة.
7. Dashboard مفصل حسب النموذج والاتجاه والفريم.

## النماذج
Bullish/Bearish Engulfing, Hammer/Hanging Man, Inverted Hammer/Shooting Star,
Morning/Evening Star, Piercing Line/Dark Cloud Cover, Tweezer Bottom/Top,
Bullish/Bearish Harami, Dragonfly/Gravestone Doji, Three White Soldiers/Three Black Crows,
Three Inside Up/Down, Three Outside Up/Down, Bullish/Bearish Kicker.

## التشغيل
```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN="..."
export TELEGRAM_CHAT_ID="..."
python app.py
```

Dashboard:
`http://SERVER:PORT/`

Health:
`/health`

JSON stats:
`/api/stats`

> لا تضع التوكن داخل الكود عند رفع المشروع إلى GitHub. استخدم Environment Variables.


## Strict candle-close timing (v1.2)
- لا يستخدم الشمعة المفتوحة نهائيًا.
- يعتمد Binance closeTime نفسه وليس raw[:-1] فقط.
- لا يرسل إشارات تاريخية عند Restart/Deploy.
- نموذج الدخول يجب أن يكون قد أغلق فعلًا على 3m/5m/15m وبعد إغلاق النموذج الرئيسي.
- CLOSE_GRACE_SECONDS=120 نافذة التقاط الإغلاق الحديث، وليست انتظارًا إضافيًا.


Dashboard aliases: `/` and `/stats`

## v1.6 STRICT MAIN CLOSE GATE
- Main models are detected only on 15m / 1h / 4h / 1d after the main candle closes.
- 3m / 5m / 15m are confirmation/entry frames only.
- The confirmation candle must close at the same exact boundary as the main candle.
- A 1H signal cannot fire at 04:46; it may fire only immediately after an hourly close (e.g. 05:00), provided a matching 3m/5m/15m confirmation also closed on that boundary.
- Main setups are one-shot and are not kept alive into the next open main candle.


## v1.11 WEEKLY (1W)
- Added 1W as a MAIN candlestick-pattern timeframe.
- MAIN timeframes are now: 15m / 1h / 4h / 1d / 1w.
- Weekly candles are aligned to Binance weekly boundaries: Monday 00:00 UTC open and Sunday 23:59:59.999 UTC close (Monday 02:59:59.999 KSA).
- Weekly patterns are evaluated only after the full 1W candle closes.
- Entry confirmation remains 3m / 5m / 15m after the weekly close, using the same validation/anti-stale rules as v1.10.
- /scanner-status now includes 1w status and next weekly close.
- Startup does not backfill an old weekly alert; it waits for the next actual weekly close after deployment.

## v1.12 — Compact candle volume in Telegram alerts
- Telegram entry alerts now include the main pattern candle's Binance quote volume.
- Compact display examples: 550K, 4M, 1B.
- Uses Binance kline quote-asset volume (USDT notional for USDT-M pairs), not the raw base-coin quantity.


## v1.13 — 24H USDT volume in Telegram alerts
- Replaced main-pattern candle volume in the Telegram message with the symbol rolling 24-hour Binance Futures quote volume.
- Uses `/fapi/v1/ticker/24hr?symbol=...` and the `quoteVolume` field (USDT for USDT-M pairs).
- Compact display examples: 550K, 4M, 1B.
