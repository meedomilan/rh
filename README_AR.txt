نسخة إصلاح Railway
===================
سبب الخطأ السابق: Railway كان يشغّل python /app/main.py بينما الحزمة كانت تحتوي app.py داخل مجلد فرعي.

هذه النسخة تحتوي الملفات في جذر ZIP مباشرة:
- main.py
- requirements.txt
- Procfile

أمر التشغيل:
python main.py

يمكن أيضًا استخدام:
uvicorn main:app --host 0.0.0.0 --port $PORT
