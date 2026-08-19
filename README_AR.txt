Ahmed Early Explosion Trader — 4H Zones + Anti-Late

التعديل الرئيسي:
- 4H هو فريم تحليل بنية العملة وتحديد منطقة شراء/بيع.
- 5M و15M زناد داخلي مبكر للتأكيد، ولا ننتظر إغلاق 4H.
- شموع رفض + Sweep/Reclaim + Delta/CVD + Order Book تستخدم لتأكيد المنطقة.
- إذا كان التدفق قويًا جدًا داخل المنطقة يمكن التأكيد بدون انتظار شمعة رفض كاملة.
- Anti-Late يمنع إرسال إشارة إذا تحرك السعر بعيدًا عن المنطقة قبل التنبيه.
- وقف الخسارة خلف منطقة 4H بهامش بنيوي بدل وقف قريب من السعر.
- إذا ظهر BUY وSELL معًا، يرسل الأعلى جودة فقط.

إعدادات البيئة الاختيارية:
SCAN_SECONDS=15
DEEP_CANDIDATES=180
MIN_QUOTE_VOLUME_USDT=750000
ZONE_NEAR_ATR4=0.30
MAX_CHASE_ATR15=0.65
REJECTION_CONFIRM=60
REJECTION_STRONG=72
FLOW_CONFIRM_5M=64
FLOW_CONFIRM_15M=60

متغيرات التليجرام:
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
TZ=Asia/Riyadh

تشغيل:
uvicorn app:app --host 0.0.0.0 --port $PORT
