import os, time, math, sqlite3, threading, html
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import requests
from flask import Flask, jsonify, render_template_string

BASE="https://fapi.binance.com"
TOKEN=os.getenv("TELEGRAM_BOT_TOKEN","")
CHAT=os.getenv("TELEGRAM_CHAT_ID","")
PORT=int(os.getenv("PORT","8080"))
SCAN_SECONDS=int(os.getenv("SCAN_SECONDS","15"))
MIN_QUOTE_VOLUME=float(os.getenv("MIN_QUOTE_VOLUME","750000"))
MAX_SYMBOLS=int(os.getenv("MAX_SYMBOLS","0"))
# لا نعالج شمعة قديمة عند تشغيل/إعادة نشر البوت. التنبيه يجب أن يرتبط بإغلاق حديث فقط.
CLOSE_GRACE_SECONDS=int(os.getenv("CLOSE_GRACE_SECONDS","120"))
MAIN_TFS=["15m","1h","4h","1d"]
ENTRY_TFS=["3m","5m","15m"]
DB="candles.db"
app=Flask(__name__)
S=requests.Session()
S.headers.update({"User-Agent":"Ahmed-Candlestick-Bot/1.0"})
ksa=timezone(timedelta(hours=3))
last_main={}
last_entry={}
setups={}
lock=threading.Lock()

def db():
    c=sqlite3.connect(DB, timeout=30)
    c.row_factory=sqlite3.Row
    return c

def init_db():
    c=db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS signals(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT, side TEXT, main_pattern TEXT, main_tf TEXT,
      confirm_pattern TEXT, entry_tf TEXT,
      detected_at INTEGER, entry_at INTEGER, entry REAL, zone_low REAL, zone_high REAL,
      sl REAL, tp1 REAL, tp2 REAL, tp3 REAL, risk REAL,
      sl_at INTEGER, tp1_at INTEGER, tp2_at INTEGER, tp3_at INTEGER,
      returned_entry_at INTEGER, post_sl_tp1_at INTEGER, post_sl_tp2_at INTEGER, post_sl_tp3_at INTEGER,
      mfe REAL DEFAULT 0, mae REAL DEFAULT 0, status TEXT DEFAULT 'OPEN'
    );
    CREATE INDEX IF NOT EXISTS ix_signals_symbol ON signals(symbol);
    CREATE INDEX IF NOT EXISTS ix_signals_pattern ON signals(main_pattern,side);
    """)
    c.commit(); c.close()

def klines(symbol, interval, limit=120):
    r=S.get(BASE+"/fapi/v1/klines",params={"symbol":symbol,"interval":interval,"limit":limit},timeout=8)
    r.raise_for_status()
    out=[]
    for x in r.json():
        out.append({"ot":x[0],"o":float(x[1]),"h":float(x[2]),"l":float(x[3]),"c":float(x[4]),"v":float(x[5]),"ct":x[6]})
    return out

def binance_time_ms():
    try:
        return int(S.get(BASE+"/fapi/v1/time",timeout=5).json()["serverTime"])
    except Exception:
        return int(time.time()*1000)

def latest_closed(cs, now_ms=None):
    """Return candles Binance has actually closed; never infer closure by list position."""
    now_ms = now_ms or binance_time_ms()
    return [x for x in cs if int(x["ct"]) < now_ms]

def is_fresh_close(c, now_ms=None):
    now_ms = now_ms or binance_time_ms()
    age = now_ms - int(c["ct"])
    return 0 <= age <= CLOSE_GRACE_SECONDS*1000

def body(c): return abs(c["c"]-c["o"])
def rng(c): return max(c["h"]-c["l"],1e-12)
def bull(c): return c["c"]>c["o"]
def bear(c): return c["c"]<c["o"]
def upper(c): return c["h"]-max(c["o"],c["c"])
def lower(c): return min(c["o"],c["c"])-c["l"]

def atr(cs,n=14):
    if len(cs)<n+1:return rng(cs[-1])
    tr=[]
    for i in range(-n,0):
        p=cs[i-1]["c"]; x=cs[i]
        tr.append(max(x["h"]-x["l"],abs(x["h"]-p),abs(x["l"]-p)))
    return sum(tr)/len(tr)

def patterns(cs):
    # Uses CLOSED candles only. Returns (name, side, strength 0..100).
    if len(cs)<5:return []
    a,b,c=cs[-3],cs[-2],cs[-1]
    out=[]
    # engulfing
    if bear(b) and bull(c) and c["o"]<=b["c"] and c["c"]>=b["o"]: out.append(("Bullish Engulfing","BUY",88))
    if bull(b) and bear(c) and c["o"]>=b["c"] and c["c"]<=b["o"]: out.append(("Bearish Engulfing","SELL",88))
    # hammer family
    if lower(c)>=2*max(body(c),rng(c)*.08) and upper(c)<=max(body(c),rng(c)*.25):
        out.append(("Hammer","BUY",78))
    if upper(c)>=2*max(body(c),rng(c)*.08) and lower(c)<=max(body(c),rng(c)*.25):
        out.append(("Shooting Star","SELL",78))
    # inverted hammer (context-free candidate)
    if upper(c)>=2*max(body(c),rng(c)*.08) and bull(c): out.append(("Inverted Hammer","BUY",70))
    if lower(c)>=2*max(body(c),rng(c)*.08) and bear(c): out.append(("Hanging Man","SELL",70))
    # harami
    if bear(b) and bull(c) and max(c["o"],c["c"])<b["o"] and min(c["o"],c["c"])>b["c"]: out.append(("Bullish Harami","BUY",74))
    if bull(b) and bear(c) and max(c["o"],c["c"])<b["c"] and min(c["o"],c["c"])>b["o"]: out.append(("Bearish Harami","SELL",74))
    # piercing / dark cloud
    mid=(b["o"]+b["c"])/2
    if bear(b) and bull(c) and c["o"]<=b["c"] and c["c"]>mid and c["c"]<b["o"]: out.append(("Piercing Line","BUY",80))
    if bull(b) and bear(c) and c["o"]>=b["c"] and c["c"]<mid and c["c"]>b["o"]: out.append(("Dark Cloud Cover","SELL",80))
    # stars
    small=body(b)<=rng(b)*.35
    if bear(a) and small and bull(c) and c["c"]>(a["o"]+a["c"])/2: out.append(("Morning Star","BUY",90))
    if bull(a) and small and bear(c) and c["c"]<(a["o"]+a["c"])/2: out.append(("Evening Star","SELL",90))
    # tweezers (ATR-ish tolerance)
    tol=max(rng(c),rng(b))*.08
    if abs(b["l"]-c["l"])<=tol and bear(b) and bull(c): out.append(("Tweezer Bottom","BUY",76))
    if abs(b["h"]-c["h"])<=tol and bull(b) and bear(c): out.append(("Tweezer Top","SELL",76))
    # doji
    if body(c)<=rng(c)*.12 and lower(c)>=rng(c)*.55: out.append(("Dragonfly Doji","BUY",72))
    if body(c)<=rng(c)*.12 and upper(c)>=rng(c)*.55: out.append(("Gravestone Doji","SELL",72))
    # 3 soldiers/crows
    x,y,z=cs[-3:]
    if all(bull(q) for q in (x,y,z)) and x["c"]<y["c"]<z["c"]: out.append(("Three White Soldiers","BUY",92))
    if all(bear(q) for q in (x,y,z)) and x["c"]>y["c"]>z["c"]: out.append(("Three Black Crows","SELL",92))
    # three inside
    if bear(a) and bull(b) and max(b["o"],b["c"])<a["o"] and min(b["o"],b["c"])>a["c"] and bull(c) and c["c"]>a["o"]:
        out.append(("Three Inside Up","BUY",86))
    if bull(a) and bear(b) and max(b["o"],b["c"])<a["c"] and min(b["o"],b["c"])>a["o"] and bear(c) and c["c"]<a["o"]:
        out.append(("Three Inside Down","SELL",86))
    # three outside
    if bear(a) and bull(b) and b["o"]<=a["c"] and b["c"]>=a["o"] and bull(c) and c["c"]>b["c"]:
        out.append(("Three Outside Up","BUY",90))
    if bull(a) and bear(b) and b["o"]>=a["c"] and b["c"]<=a["o"] and bear(c) and c["c"]<b["c"]:
        out.append(("Three Outside Down","SELL",90))
    # kicker approximation
    if bear(b) and bull(c) and c["o"]>=b["o"] and body(c)>=rng(c)*.65: out.append(("Bullish Kicker","BUY",90))
    if bull(b) and bear(c) and c["o"]<=b["o"] and body(c)>=rng(c)*.65: out.append(("Bearish Kicker","SELL",90))
    # dedupe strongest
    d={}
    for n,s,q in out:
        if (n,s) not in d or q>d[(n,s)]: d[(n,s)]=q
    return [(n,s,q) for (n,s),q in d.items()]

def symbols():
    ex=S.get(BASE+"/fapi/v1/exchangeInfo",timeout=10).json()
    tick=S.get(BASE+"/fapi/v1/ticker/24hr",timeout=10).json()
    vols={x["symbol"]:float(x.get("quoteVolume",0)) for x in tick}
    sy=[x["symbol"] for x in ex["symbols"] if x["status"]=="TRADING" and x.get("quoteAsset")=="USDT" and x.get("contractType")=="PERPETUAL" and vols.get(x["symbol"],0)>=MIN_QUOTE_VOLUME]
    sy.sort(key=lambda x:vols.get(x,0),reverse=True)
    return sy[:MAX_SYMBOLS] if MAX_SYMBOLS>0 else sy

def fmt(x):
    if x>=1000:return f"{x:.2f}"
    if x>=1:return f"{x:.5f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")

def links(symbol):
    tv=f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P"
    bn=f"https://www.binance.com/en/futures/{symbol}"
    return tv,bn

def send(msg, symbol):
    if not TOKEN or not CHAT:
        print(msg); return
    tv,bn=links(symbol)
    msg += f'\n\n🔗 <a href="{tv}">TradingView</a> | <a href="{bn}">Binance</a>'
    try:
        S.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
               json={"chat_id":CHAT,"text":msg,"parse_mode":"HTML","disable_web_page_preview":True},timeout=8)
    except Exception as e: print("telegram",e)

def make_setup(symbol,tf,pname,side,strength,cs):
    c=cs[-1]; A=atr(cs)
    lo=min(x["l"] for x in cs[-3:]); hi=max(x["h"] for x in cs[-3:])
    # compact zone around pattern body/range, not whole distant swing
    if side=="BUY":
        zl=min(c["l"],c["o"],c["c"]); zh=max(c["o"],c["c"])
        invalid=lo-.15*A
    else:
        zl=min(c["o"],c["c"]); zh=max(c["h"],c["o"],c["c"])
        invalid=hi+.15*A
    key=(symbol,tf,pname,side,c["ct"])
    setups[key]={"symbol":symbol,"main_tf":tf,"main_pattern":pname,"side":side,"strength":strength,
                 "zone_low":min(zl,zh),"zone_high":max(zl,zh),"invalid":invalid,"created":time.time(),"main_close":c["ct"]}

def maybe_entry(st, etf, cs, pname, side):
    if side!=st["side"]: return
    c=cs[-1]; px=c["c"]; zl,zh=st["zone_low"],st["zone_high"]
    # الدخول يجب أن يكون داخل منطقة النموذج نفسها؛ لا نطارد السعر خارجها.
    if not (zl <= px <= zh): return
    ded=("ALERT",st["symbol"],st["main_tf"],st["main_pattern"],etf,c["ct"])
    if ded in last_entry:return
    last_entry[ded]=1
    entry=px
    A=atr(cs)
    if side=="BUY":
        sl=min(st["invalid"],min(x["l"] for x in cs[-5:])-.10*A)
        risk=entry-sl
        if risk<=0:return
        t1,t2,t3=entry+risk,entry+2*risk,entry+3*risk
        emoji="🟢"; title="دخول شراء مؤكد — نموذج شموع"; zone="منطقة الشراء"
    else:
        sl=max(st["invalid"],max(x["h"] for x in cs[-5:])+.10*A)
        risk=sl-entry
        if risk<=0:return
        t1,t2,t3=entry-risk,entry-2*risk,entry-3*risk
        # لا نرسل خطة بيع إذا كان حساب R ينتج هدفًا صفريًا/سالبًا.
        if t3 <= 0: return
        emoji="🔴"; title="دخول بيع مؤكد — نموذج شموع"; zone="منطقة البيع"
    now=int(time.time()*1000)
    con=db(); cur=con.execute("""INSERT INTO signals(symbol,side,main_pattern,main_tf,confirm_pattern,entry_tf,detected_at,entry_at,entry,zone_low,zone_high,sl,tp1,tp2,tp3,risk)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(st["symbol"],side,st["main_pattern"],st["main_tf"],pname,etf,st["main_close"],now,entry,zl,zh,sl,t1,t2,t3,risk))
    con.commit(); con.close()
    sym=st["symbol"]+".P"
    tv,bn=links(st["symbol"])
    symbol_link=f'<a href="{tv}">#{html.escape(sym)}</a>'
    msg=f"""{emoji} <b>{title}</b>

💰 {symbol_link} | ⏰ <b>{st["main_tf"].upper()}</b>
🕯 النموذج: <b>{html.escape(st["main_pattern"])}</b> | قوة <b>{st["strength"]}%</b>
⚡ تأكيد الدخول: <b>{html.escape(pname)}</b> | <b>{etf.upper()}</b>
📍 {zone}: <b>{fmt(zl)} – {fmt(zh)}</b>

🎯 الدخول: <b>{fmt(entry)}</b>
🛑 الوقف: <b>{fmt(sl)}</b>
✅ TP1: <b>{fmt(t1)}</b> | TP2: <b>{fmt(t2)}</b> | TP3: <b>{fmt(t3)}</b>

🕒 {datetime.now(ksa).strftime("%d-%m-%Y %H:%M:%S")} (السعودية)

⚠️ تنبيه إحصائي وليس ضمانًا"""
    send(msg,st["symbol"])
    # remove sibling setups same symbol/side to reduce duplicate spam
    with lock:
        for k in list(setups):
            if setups[k]["symbol"]==st["symbol"] and setups[k]["side"]==side:
                setups.pop(k,None)

def scan_loop():
    """Strict close-driven scanner.

    - A main model is created only from a candle whose Binance closeTime has passed.
    - The candle must have closed recently, so deploy/restart never fires historical alerts.
    - Entry confirmation is also from a freshly CLOSED 3m/5m/15m candle.
    - Confirmation must close after the main model candle.
    """
    while True:
        try:
            now_ms=binance_time_ms()
            syms=symbols()
            for symbol in syms:
                # MAIN: 15m / 1h / 4h / 1d — closed candle only
                for tf in MAIN_TFS:
                    try:
                        raw=klines(symbol,tf,80)
                        closed=latest_closed(raw,now_ms)
                        if not closed: continue
                        c=closed[-1]
                        cid=int(c["ct"])
                        key=(symbol,tf,cid)
                        if key in last_main: continue
                        # mark it seen even if old: no historical/restart alert
                        last_main[key]=1
                        if not is_fresh_close(c,now_ms):
                            continue
                        for p,side,strength in patterns(closed):
                            make_setup(symbol,tf,p,side,strength,closed)
                    except Exception as e:
                        print("main",symbol,tf,e)

                # ENTRY: only a freshly CLOSED 3m / 5m / 15m confirmation
                active=[v for v in list(setups.values()) if v["symbol"]==symbol and time.time()-v["created"]<7*86400]
                if active:
                    for etf in ENTRY_TFS:
                        try:
                            raw=klines(symbol,etf,80)
                            closed=latest_closed(raw,now_ms)
                            if not closed: continue
                            c=closed[-1]
                            cid=int(c["ct"])
                            ekey=(symbol,etf,cid)
                            if ekey in last_entry:
                                continue
                            # Mark this close seen once; patterns on this exact candle only.
                            last_entry[ekey]=1
                            if not is_fresh_close(c,now_ms):
                                continue
                            pats=patterns(closed)
                            for p,side,q in pats:
                                for st in list(active):
                                    if cid <= int(st["main_close"]):
                                        continue
                                    maybe_entry(st,etf,closed,p,side)
                        except Exception as e:
                            print("entry",symbol,etf,e)
            with lock:
                for k in list(setups):
                    if time.time()-setups[k]["created"]>=7*86400:
                        setups.pop(k,None)
        except Exception as e:
            print("scan",e)
        time.sleep(SCAN_SECONDS)

def tracker_loop():
    while True:
        try:
            con=db(); rows=con.execute("SELECT * FROM signals WHERE status='OPEN' OR (sl_at IS NOT NULL AND post_sl_tp3_at IS NULL)").fetchall()
            for r in rows:
                try:
                    p=float(S.get(BASE+"/fapi/v1/ticker/price",params={"symbol":r["symbol"]},timeout=5).json()["price"])
                    now=int(time.time()*1000); side=r["side"]; entry=r["entry"]; risk=r["risk"]
                    mfe=max(r["mfe"] or 0, (p-entry) if side=="BUY" else (entry-p))
                    mae=max(r["mae"] or 0, (entry-p) if side=="BUY" else (p-entry))
                    upd={"mfe":mfe,"mae":mae}
                    hit=lambda level: p>=level if side=="BUY" else p<=level
                    slhit=lambda: p<=r["sl"] if side=="BUY" else p>=r["sl"]
                    if not r["sl_at"] and slhit(): upd["sl_at"]=now
                    if not r["tp1_at"] and hit(r["tp1"]): upd["tp1_at"]=now
                    if not r["tp2_at"] and hit(r["tp2"]): upd["tp2_at"]=now
                    if not r["tp3_at"] and hit(r["tp3"]): upd["tp3_at"]=now
                    sl_at=upd.get("sl_at") or r["sl_at"]
                    if sl_at:
                        # after SL, continue tracking recovery and targets
                        reentered=(p>=entry if side=="BUY" else p<=entry)
                        if not r["returned_entry_at"] and reentered: upd["returned_entry_at"]=now
                        if not r["post_sl_tp1_at"] and hit(r["tp1"]): upd["post_sl_tp1_at"]=now
                        if not r["post_sl_tp2_at"] and hit(r["tp2"]): upd["post_sl_tp2_at"]=now
                        if not r["post_sl_tp3_at"] and hit(r["tp3"]): upd["post_sl_tp3_at"]=now
                    if upd.get("tp3_at") and not sl_at: upd["status"]="TP3"
                    elif sl_at: upd["status"]="SL_TRACKING"
                    sets=",".join(f"{k}=?" for k in upd); vals=list(upd.values())+[r["id"]]
                    con.execute(f"UPDATE signals SET {sets} WHERE id=?",vals)
                except Exception as e: print("track",r["symbol"],e)
            con.commit(); con.close()
        except Exception as e: print("tracker",e)
        time.sleep(10)

def mins(a,b):
    if not a or not b:return None
    return round((a-b)/60000,1)

def stats_data():
    con=db(); rows=con.execute("SELECT * FROM signals ORDER BY id DESC").fetchall(); con.close()
    groups=defaultdict(list)
    for r in rows: groups[(r["side"],r["main_pattern"],r["main_tf"])].append(r)
    stats=[]
    for (side,p,tf),rs in groups.items():
        n=len(rs)
        def pct(field): return round(100*sum(1 for x in rs if x[field])/n,1) if n else 0
        slfirst=sum(1 for x in rs if x["sl_at"] and (not x["tp1_at"] or x["sl_at"]<x["tp1_at"]))
        def avg_time(field,base="entry_at"):
            v=[mins(x[field],x[base]) for x in rs if x[field] and x[base]]
            return round(sum(v)/len(v),1) if v else None
        stats.append({"side":side,"pattern":p,"tf":tf,"signals":n,"tp1":pct("tp1_at"),"tp2":pct("tp2_at"),"tp3":pct("tp3_at"),
          "sl_first":round(100*slfirst/n,1),"returned_after_sl":pct("returned_entry_at"),
          "post_sl_tp1":pct("post_sl_tp1_at"),"post_sl_tp2":pct("post_sl_tp2_at"),"post_sl_tp3":pct("post_sl_tp3_at"),
          "t_tp1":avg_time("tp1_at"),"t_tp2":avg_time("tp2_at"),"t_tp3":avg_time("tp3_at"),
          "t_return_after_sl":avg_time("returned_entry_at","sl_at"),"t_post_sl_tp1":avg_time("post_sl_tp1_at","sl_at"),
          "mfe":round(sum((x["mfe"] or 0)/(x["risk"] or 1) for x in rs)/n,2),
          "mae":round(sum((x["mae"] or 0)/(x["risk"] or 1) for x in rs)/n,2)})
    return sorted(stats,key=lambda x:x["signals"],reverse=True), [dict(x) for x in rows[:100]]

TPL="""<!doctype html><html lang="ar" dir="rtl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>نماذج الشموع - الإحصائيات</title><style>
body{background:#07110f;color:#eef7f2;font-family:system-ui;margin:0;padding:18px}.wrap{max-width:1500px;margin:auto}
h1{color:#65ef73}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin:16px 0}
.card,table{background:#0d1b18;border:1px solid #24483e;border-radius:14px}.card{padding:15px}.n{font-size:28px;font-weight:800;color:#6ef27b}
table{width:100%;border-collapse:collapse;overflow:hidden;font-size:13px}th,td{padding:9px;border-bottom:1px solid #1e3831;text-align:center}th{color:#8ff59a;position:sticky;top:0;background:#10231e}
.buy{color:#63f277}.sell{color:#ff6c6c}.scroll{overflow:auto;max-height:70vh}.muted{color:#9ab2aa}
</style></head><body><div class="wrap"><h1>📊 Dashboard نماذج الشموع BUY / SELL</h1>
<div class="cards"><div class="card">إجمالي الإشارات<div class="n">{{total}}</div></div><div class="card">نماذج/فريمات<div class="n">{{groups}}</div></div><div class="card">فرص قيد المراقبة<div class="n">{{active}}</div></div></div>
<div class="scroll"><table><thead><tr><th>الاتجاه</th><th>النموذج</th><th>الفريم</th><th>الإشارات</th><th>TP1%</th><th>TP2%</th><th>TP3%</th><th>SL أولاً%</th><th>رجع بعد SL%</th><th>TP1 بعد SL%</th><th>TP2 بعد SL%</th><th>TP3 بعد SL%</th><th>وقت TP1 د</th><th>TP2 د</th><th>TP3 د</th><th>رجوع بعد SL د</th><th>TP1 بعد SL د</th><th>MFE R</th><th>MAE R</th></tr></thead><tbody>
{% for s in stats %}<tr><td class="{{'buy' if s.side=='BUY' else 'sell'}}">{{s.side}}</td><td>{{s.pattern}}</td><td>{{s.tf}}</td><td>{{s.signals}}</td><td>{{s.tp1}}</td><td>{{s.tp2}}</td><td>{{s.tp3}}</td><td>{{s.sl_first}}</td><td>{{s.returned_after_sl}}</td><td>{{s.post_sl_tp1}}</td><td>{{s.post_sl_tp2}}</td><td>{{s.post_sl_tp3}}</td><td>{{s.t_tp1 or '-'}}</td><td>{{s.t_tp2 or '-'}}</td><td>{{s.t_tp3 or '-'}}</td><td>{{s.t_return_after_sl or '-'}}</td><td>{{s.t_post_sl_tp1 or '-'}}</td><td>{{s.mfe}}</td><td>{{s.mae}}</td></tr>{% endfor %}
</tbody></table></div><p class="muted">يستمر تتبع الإشارة بعد ضرب الوقف لقياس الرجوع والوصول اللاحق للأهداف.</p></div></body></html>"""

@app.route("/")
def home():
    st,rows=stats_data()
    return render_template_string(TPL,stats=st,total=len(rows),groups=len(st),active=len(setups))
@app.route("/api/stats")
def api_stats():
    st,rows=stats_data(); return jsonify({"stats":st,"recent":rows,"active_setups":len(setups)})
@app.route("/health")
def health(): return jsonify({"ok":True,"active_setups":len(setups)})

if __name__=="__main__":
    init_db()
    threading.Thread(target=scan_loop,daemon=True).start()
    threading.Thread(target=tracker_loop,daemon=True).start()
    app.run(host="0.0.0.0",port=PORT)
