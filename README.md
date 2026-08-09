# Ahmed Delta Early Predictor v1

نسخة جديدة مستقلة تحافظ على مضمون رسالة DELTA السابقة، وتغير فقط توقيت الاكتشاف.

## الفكرة

- توقع DELTA على 1H من شموع 5M.
- توقع DELTA على 4H من شموع 15M.
- 1H: ثلاث شموع تدريجية، أو شمعتان قويتان، أو شمعة غير طبيعية.
- 4H: شمعتان تدريجيتان، أو شمعة 15M غير طبيعية.
- الشمعة غير الطبيعية نسبية مقارنة بآخر 20 شمعة، وليست رقم ATR/Volume ثابتًا.
- رفض الإشارة بعد امتداد الحركة أو تأخر عمر شمعة الهدف.

## التنبيه

لم يتغير مضمون الرسالة:
- الدرجة
- الاحتمال التاريخي
- الحالات المشابهة
- وزن التعلم
- 5/15/30/60 دقيقة
- Delta
- OI
- Funding
- الوصفة
- الروابط والتوقيت

## الروابط

- `/`
- `/health`
- `/test-telegram`
- `/alerts`
- `/stats`
- `/learning/checkpoints`

## ملاحظة

الاحتمالات تبدأ بـ “يتعلم” حتى تتجمع حالات فعلية. البوت لا ينفذ صفقات تلقائيًا ولا يضمن النتائج.


## v1.1 STABLE fixes

- Telegram messages are serialized and spaced to avoid HTTP 429.
- Telegram Retry-After is respected automatically.
- Default maximum alerts per scan reduced from 5 to 2.
- Binance rate-limit errors now preserve a real diagnostic instead of `None`.
- Price snapshots are cached for 30 seconds.
- The evaluation worker reuses the last valid price snapshot during temporary Binance failures.
- Temporary evaluation failures no longer print a full traceback every cycle.


## v1.2 FINAL

تمت إضافة وسم واضح داخل نفس رسالة DELTA:

- 🟡 إشارة مبكرة للمسار التدريجي.
- 🔵 إشارة مبكرة جدًا للمسار السريع أو الشمعة غير الطبيعية.

لم يتغير أي حقل آخر في رسالة التنبيه.


## v1.3 THREE PATHS

المشروع يعتمد الآن على ثلاثة مسارات فقط:

1. المسار التدريجي:
   - 3 شموع متدرجة.
   - Delta تتزايد.
   - بدون اشتراط شمعة ضخمة.

2. مسار الشمعتين:
   - شمعتان قويتان.
   - جسم كل شمعة 0.65 ATR أو أكثر.
   - حجم كل شمعة 1.40× المتوسط أو أكثر.

3. المسار الاستثنائي:
   - شمعة واحدة فقط.
   - جسم 0.90 ATR أو أكثر.
   - حجم 1.80× المتوسط أو أكثر.
   - Delta قوية وإغلاق قوي.
   - يجب أن تكون مختلفة نسبيًا عن آخر 20 شمعة.

فلتر التأخير:
- 1H: أقصى عمر 30 دقيقة وامتداد 0.45 ATR.
- 4H: أقصى عمر 120 دقيقة وامتداد 0.60 ATR.


## v1.4 LAST CANDLE FILTER

- BUY لا يُرسل إذا كانت آخر شمعة مصدر مغلقة هبوطًا بقوة.
- SELL لا يُرسل إذا كانت آخر شمعة مصدر مغلقة صعودًا بقوة.
- يُسمح بتصحيح صغير فقط إذا كان جسمه لا يتجاوز 0.25 ATR.
- إذا كانت دلتا الشمعة الأخيرة عكس الاتجاه بقوة 12% أو أكثر، يُلغى التنبيه.

Railway:
REQUIRE_LAST_CANDLE_DIRECTION=true
MAX_OPPOSITE_CANDLE_DELTA_PCT=12
ALLOW_SMALL_OPPOSITE_CANDLE=true
MAX_SMALL_OPPOSITE_BODY_ATR=0.25


## v2.0 CONFLICT AWARE

- قبل تنبيه 4H يتم فحص انعكاس 1H.
- عوامل التعارض: اتجاه 1H، CVD، OI، Volume Spike، BOS.
- 4 عوامل أو أكثر: حجب التنبيه.
- عاملان أو ثلاثة: تحذير وتخفيض الدرجة.
- يظهر توافق الفريمات، توافق السيولة، ثقة القرار، الدخول، الوقف، والأهداف.
- قاعدة البيانات القديمة تُحدّث تلقائيًا دون حذف التعلم.


## v2.1 REJECTION STATS

يظهر في كل دورة سبب رفض الفرص: last_candle, no_path, extension, score, direction_gap, conflict, liquidity, age, duplicate. ويظهر نفس الملخص في /health.


## v2.2 TARGET ATR + ADVANCED STATS
- اكتشاف 1H من 5M واكتشاف 4H من 15M كما هو.
- وقف وأهداف 1H من ATR فريم 1H، و4H من ATR فريم 4H.
- إحصائيات 15m/30m/1H/2H/4H/8H/12H.
- تتبع TP1/TP2/TP3/Stop وTP1-before-Stop.
- التقسيم حسب الفريم والاتجاه والمسار.
- ترقية تلقائية لقاعدة البيانات القديمة بدون حذف التعلم.


## v2.3 TIMEOUT FIX + 24H VOLUME
- إضافة حجم التداول 24H بالـ USDT داخل كل تنبيه.
- Timeout لعملة واحدة لا يوقف دورة الفحص؛ يتم تجاوز العملة فقط.
- عداد symbol_timeouts_last_scan داخل /health.
- عداد scan_timeouts_total داخل /health.
- الـ hard scan timeout أصبح بحد أدنى 180 ثانية حتى لا يقطع الدورة بسبب Telegram retry/backoff.
- حجم 24H محفوظ في قاعدة البيانات لكل تنبيه جديد.


## v2.4 STOP RECOVERY STATS
- tp1_after_stop / tp2_after_stop / tp3_after_stop
- tp1_after_stop_pct / tp2_after_stop_pct / tp3_after_stop_pct
- تقيس: ضرب الوقف أولًا ثم رجوع السعر إلى الهدف الأصلي.
- لم يتم توسيع الوقف؛ نجمع الدليل أولًا.


## v2.5 SIMILAR CASES 5000
- رفع الحد الأقصى للحالات المشابهة من 500 إلى 5000.
- لا تغيير على منطق الإشارة أو الوقف أو الأهداف.
- إحصائية Stop Recovery من v2.4 باقية كما هي.


## v2.6 SIMILAR CASES 10000
- رفع الحد الأقصى للحالات المشابهة من 5000 إلى 10000.
- لا تغيير على شروط الإشارة أو الوقف أو الأهداف أو الإحصائيات.
