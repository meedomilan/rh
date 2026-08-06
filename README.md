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
