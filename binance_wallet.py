import os,time,hmac,hashlib,sqlite3,logging,asyncio
from urllib.parse import urlencode
from datetime import datetime,timezone,timedelta
import requests
from telegram import InlineKeyboardButton,InlineKeyboardMarkup

log=logging.getLogger("binance_wallet")
DB=os.getenv("BINANCE_DB_PATH","binance_wallet.sqlite3")
def load_keys():
 # Read credentials from binance_keys.txt in the repository root.
 values={}
 try:
  with open(os.getenv("BINANCE_KEYS_FILE","binance_keys.txt"),"r",encoding="utf-8") as f:
   for line in f:
    line=line.strip()
    if not line or line.startswith("#") or "=" not in line:continue
    k,v=line.split("=",1);values[k.strip()]=v.strip().strip('"').strip("'")
 except FileNotFoundError:
  log.warning("binance_keys.txt not found; falling back to environment variables")
 return values

_FILE_KEYS=load_keys()
KEY=os.getenv("BINANCE_API_KEY") or _FILE_KEYS.get("API_KEY","")
SECRET=os.getenv("BINANCE_API_SECRET") or _FILE_KEYS.get("API_SECRET","")
BASE=os.getenv("BINANCE_API_BASE","https://api.binance.com").rstrip("/")
ALLOWED={int(x.strip()) for x in os.getenv("BINANCE_ALLOWED_USER_IDS","").split(",") if x.strip()}
INTERVAL=max(60,int(os.getenv("BINANCE_SYNC_SECONDS","300")))
S=requests.Session()
if KEY:S.headers.update({"X-MBX-APIKEY":KEY})
PRICES={}

def conn():
 c=sqlite3.connect(DB,timeout=30);c.row_factory=sqlite3.Row;return c

def init_db():
 with conn() as c:
  c.executescript("""
  CREATE TABLE IF NOT EXISTS bw_snapshots(ts INTEGER,wallet TEXT,asset TEXT,amount TEXT,usd REAL,UNIQUE(ts,wallet,asset));
  CREATE TABLE IF NOT EXISTS bw_events(event_key TEXT PRIMARY KEY,kind TEXT,asset TEXT,amount TEXT,status TEXT,ts INTEGER,source TEXT,details TEXT);
  CREATE TABLE IF NOT EXISTS bw_income(event_key TEXT PRIMARY KEY,market TEXT,kind TEXT,asset TEXT,amount TEXT,ts INTEGER,details TEXT);
  CREATE TABLE IF NOT EXISTS bw_state(name TEXT PRIMARY KEY,value TEXT);
  CREATE TABLE IF NOT EXISTS bw_notified(event_key TEXT PRIMARY KEY,sent_at INTEGER);
  CREATE INDEX IF NOT EXISTS bw_snap_ts ON bw_snapshots(ts);
  CREATE INDEX IF NOT EXISTS bw_event_ts ON bw_events(ts);
  """)

def api(method,path,params=None,base=None,signed=True):
 if signed and (not KEY or not SECRET): raise RuntimeError("أضف BINANCE_API_KEY وBINANCE_API_SECRET")
 p=dict(params or {})
 if signed:
  p["timestamp"]=int(time.time()*1000);p.setdefault("recvWindow",10000)
  q=urlencode(p,doseq=True);p["signature"]=hmac.new(SECRET.encode(),q.encode(),hashlib.sha256).hexdigest()
 for n in range(4):
  try:
   r=S.request(method,(base or BASE)+path,params=p if method=="GET" else None,data=p if method!="GET" else None,timeout=25)
   if r.status_code in (418,429):time.sleep(min(60,2**(n+2)));continue
   r.raise_for_status();return r.json()
  except requests.RequestException:
   if n==3:raise
   time.sleep(2**n)
 raise RuntimeError("Binance API rate limit")

def usd(asset,qty):
 q=float(qty)
 if asset in ("USDT","USDC","FDUSD","BUSD","TUSD","USDP"):return q
 return next((q*PRICES[asset+x] for x in ("USDT","USDC") if asset+x in PRICES),None)

def save(wallet,items,ts):
 out=[]
 for x in items or []:
  a=x.get("asset") or x.get("coin")
  if not a:continue
  q=sum(float(x.get(k,0) or 0) for k in ("free","locked","freeze","balance","totalAmount","amount"))
  if not q:continue
  v=usd(a,q)
  with conn() as c:c.execute("INSERT OR REPLACE INTO bw_snapshots VALUES(?,?,?,?,?)",(ts,wallet,a,str(q),v))
  out.append((a,q,v))
 return out

def sync_balances():
 global PRICES
 PRICES={x["symbol"]:float(x["price"]) for x in api("GET","/api/v3/ticker/price",signed=False)}
 ts=int(time.time());out={}
 out["Spot"]=save("Spot",api("GET","/api/v3/account").get("balances",[]),ts)
 out["Funding"]=save("Funding",api("POST","/sapi/v1/asset/get-funding-asset",{}),ts)
 for name,path in (("Earn Flexible","/sapi/v1/simple-earn/flexible/position"),("Earn Locked","/sapi/v1/simple-earn/locked/position")):
  try:out[name]=save(name,api("GET",path,{"current":1,"size":100}).get("rows",[]),ts)
  except Exception as e:log.warning("%s unavailable: %s",name,e);out[name]=[]
 for name,host,path in (("USD-M Futures","https://fapi.binance.com","/fapi/v3/balance"),("COIN-M Futures","https://dapi.binance.com","/dapi/v1/balance")):
  try:out[name]=save(name,api("GET",path,{},host),ts)
  except Exception as e:log.warning("%s unavailable: %s",name,e);out[name]=[]
 with conn() as c:c.execute("INSERT OR REPLACE INTO bw_state VALUES('last_sync',?)",(str(ts),))
 return out

def put_event(key,kind,x,source,ts):
 with conn() as c:
  cur=c.execute("INSERT OR IGNORE INTO bw_events VALUES(?,?,?,?,?,?,?,?)",(key,kind,x.get("coin",x.get("asset")),str(x.get("amount")),str(x.get("status","completed")),int(ts or 0),source,str(x)))
  return cur.rowcount==1

def sync_flows():
 now=int(time.time()*1000)
 with conn() as c:r=c.execute("SELECT value FROM bw_state WHERE name='flow_cursor'").fetchone()
 start=int(r["value"]) if r else now-7*86400000
 fresh=[]
 for kind,path in (("deposit","/sapi/v1/capital/deposit/hisrec"),("withdrawal","/sapi/v1/capital/withdraw/history")):
  try:
   for x in api("GET",path,{"startTime":start,"endTime":now,"limit":1000}):
    ts=x.get("insertTime",x.get("applyTime",0));tid=x.get("id",x.get("withdrawOrderId",x.get("txId")))
    key=f"{kind}:{tid}:{x.get('coin')}:{x.get('amount')}:{ts}"
    if put_event(key,kind,x,"Capital",ts):fresh.append((key,kind,x))
  except Exception:log.exception("sync %s",kind)
 # Internal transfers are classified separately and never counted as income/expense.
 for typ in ("MAIN_FUNDING","FUNDING_MAIN","MAIN_UMFUTURE","UMFUTURE_MAIN","MAIN_CMFUTURE","CMFUTURE_MAIN"):
  try:
   data=api("GET","/sapi/v1/asset/transfer",{"type":typ,"startTime":start,"endTime":now,"current":1,"size":100})
   for x in data.get("rows",[]):
    ts=x.get("timestamp",0);key=f"transfer:{typ}:{x.get('tranId')}"
    if put_event(key,"transfer",x,typ,ts):fresh.append((key,"transfer",x))
  except Exception as e:log.debug("transfer %s unavailable: %s",typ,e)
 with conn() as c:c.execute("INSERT OR REPLACE INTO bw_state VALUES('flow_cursor',?)",(str(now-60000),))
 return fresh

def sync_income():
 now=int(time.time()*1000)
 with conn() as c:r=c.execute("SELECT value FROM bw_state WHERE name='income_cursor'").fetchone()
 start=int(r["value"]) if r else now-90*86400000
 for market,host,path in (("USD-M","https://fapi.binance.com","/fapi/v1/income"),("COIN-M","https://dapi.binance.com","/dapi/v1/income")):
  try:
   with conn() as c:
    for x in api("GET",path,{"startTime":start,"endTime":now,"limit":1000},host):
     key=f"{market}:{x.get('incomeType')}:{x.get('tranId')}:{x.get('time')}"
     c.execute("INSERT OR IGNORE INTO bw_income VALUES(?,?,?,?,?,?,?)",(key,market,x.get("incomeType"),x.get("asset"),x.get("income"),x.get("time"),str(x)))
  except Exception:log.exception("income %s",market)
 with conn() as c:c.execute("INSERT OR REPLACE INTO bw_state VALUES('income_cursor',?)",(str(now-60000),))

def latest():
 with conn() as c:
  r=c.execute("SELECT MAX(ts) ts FROM bw_snapshots").fetchone()
  if not r["ts"]:return 0,[]
  rows=c.execute("SELECT wallet,asset,amount,usd FROM bw_snapshots WHERE ts=?",(r["ts"],)).fetchall()
 return sum(float(x["usd"]) for x in rows if x["usd"] is not None),rows

def local(ms):
 return datetime.fromtimestamp(int(ms)/1000,timezone.utc).astimezone(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M") if ms else "غير معروف"

def menu():
 return InlineKeyboardMarkup([
 [InlineKeyboardButton("💰 رصيد Binance الحالي",callback_data="bn_balance")],
 [InlineKeyboardButton("📊 الأرباح والخسائر",callback_data="bn_pnl")],
 [InlineKeyboardButton("📥 الإيداعات",callback_data="bn_deposits"),InlineKeyboardButton("📤 السحوبات",callback_data="bn_withdrawals")],
 [InlineKeyboardButton("🔄 التحويلات",callback_data="bn_transfers")],
 [InlineKeyboardButton("📈 الإحصائيات",callback_data="bn_stats")],
 [InlineKeyboardButton("📅 التقارير",callback_data="bn_reports"),InlineKeyboardButton("🪙 توزيع العملات",callback_data="bn_coins")],
 [InlineKeyboardButton("🔄 مزامنة الآن",callback_data="bn_refresh")],
 [InlineKeyboardButton("⬅️ رجوع",callback_data="back_money")]])

def authorized(update):
 return bool(update.effective_user and ALLOWED and update.effective_user.id in ALLOWED)

async def open_menu(update,context):
 q=update.callback_query
 if not authorized(update):await q.answer("غير مصرح",show_alert=True);return
 await q.answer();await q.edit_message_text("🟡 مراقبة Binance — قراءة فقط\nاختر القسم:",reply_markup=menu())

async def callback(update,context):
 q=update.callback_query
 if not authorized(update):await q.answer("غير مصرح",show_alert=True);return
 await q.answer();k=q.data
 if k=="bn_refresh":
  await q.edit_message_text("⏳ جاري تحديث بيانات Binance...")
  try:
   await asyncio.to_thread(sync_balances);events=await asyncio.to_thread(sync_flows);await asyncio.to_thread(sync_income)
   for ek,kind,x in events:
    with conn() as c:sent=c.execute("SELECT 1 FROM bw_notified WHERE event_key=?",(ek,)).fetchone()
    if not sent:
     msg=f"🔔 Binance {kind}\n{x.get('coin',x.get('asset',''))} {x.get('amount','')}\nالحالة: {x.get('status','غير محدد')}\n{local(x.get('insertTime',x.get('applyTime',x.get('timestamp',0))))}"
     for uid in ALLOWED:
      try:await context.bot.send_message(uid,msg)
      except Exception:log.exception("Telegram notify")
     with conn() as c:c.execute("INSERT OR IGNORE INTO bw_notified VALUES(?,?)",(ek,int(time.time())))
   await q.edit_message_text("✅ تمت المزامنة",reply_markup=menu())
  except Exception as e:
   log.exception("manual sync")
   await q.edit_message_text(f"فشل التحديث: {str(e)[:250]}",reply_markup=menu())
  return
 total,rows=latest()
 if k=="bn_balance":
  text=f"💰 القيمة المسعّرة: ${total:,.2f}\n" + ("\n".join(f"{x['wallet']} · {x['asset']}: {float(x['amount']):.8g} · "+(f"${float(x['usd']):,.2f}" if x['usd'] is not None else "غير مسعّر") for x in rows[:35]) if rows else "لا توجد لقطة. اضغط مزامنة الآن.")
 elif k=="bn_pnl":
  with conn() as c:inc=c.execute("SELECT market,kind,asset,amount,ts FROM bw_income ORDER BY ts DESC LIMIT 12").fetchall()
  text=f"📊 قيمة الأصول: ${total:,.2f}\nصافي الربح الشامل: غير مكتمل حالياً.\nSpot يحتاج سجل الصفقات وتكلفة الاقتناء.\nFutures income محدود تاريخياً (عادةً آخر 3 أشهر).\nالأرباح غير المحققة تحتاج تقييم المراكز المفتوحة.\nالإيداعات ليست أرباحاً والسحوبات ليست خسائر والتحويلات الداخلية ليست تدفقات خارجية."
  if inc:text+="\n\nآخر قيود Futures:\n"+"\n".join(f"{x['market']} {x['kind']}: {x['amount']} {x['asset']} · {local(x['ts'])}" for x in inc)
 elif k in ("bn_deposits","bn_withdrawals","bn_transfers"):
  kind={"bn_deposits":"deposit","bn_withdrawals":"withdrawal","bn_transfers":"transfer"}[k]
  with conn() as c:ev=c.execute("SELECT asset,amount,status,ts,source FROM bw_events WHERE kind=? ORDER BY ts DESC LIMIT 20",(kind,)).fetchall()
  text="لا توجد عمليات مسجلة." if not ev else "\n".join(f"{x['asset']} {x['amount']} · {x['status']} · {local(x['ts'])} · {x['source']}" for x in ev)
 elif k=="bn_coins":
  sums={}
  for x in rows:
   if x["usd"] is not None:sums[x["asset"]]=sums.get(x["asset"],0)+float(x["usd"])
  text="🪙 توزيع العملات (القيم التي أمكن تسعيرها):\n"+"\n".join(f"{a}: ${v:,.2f}" for a,v in sorted(sums.items(),key=lambda z:z[1],reverse=True)[:25])
 elif k=="bn_stats":
  with conn() as c:r=c.execute("SELECT COUNT(DISTINCT ts) n,MIN(ts) first,MAX(ts) last FROM bw_snapshots").fetchone()
  text=f"📈 لقطات محفوظة: {r['n']}\nأول لقطة: {local(r['first']*1000) if r['first'] else 'لا يوجد'}\nآخر لقطة: {local(r['last']*1000) if r['last'] else 'لا يوجد'}"
 else:
  with conn() as c:n=c.execute("SELECT COUNT(DISTINCT ts) n FROM bw_snapshots").fetchone()["n"]
  text=f"📅 التقارير تعتمد على اللقطات المحفوظة منذ تشغيل المزامنة. عدد اللقطات: {n}. لا تتوفر مقارنة تاريخية موثوقة قبل أول لقطة."
 await q.edit_message_text(text[:4000],reply_markup=menu())

async def background(app):
 while True:
  try:
   if KEY and SECRET:
    await asyncio.to_thread(sync_balances);events=await asyncio.to_thread(sync_flows);await asyncio.to_thread(sync_income)
    for ek,kind,x in events:
     with conn() as c:sent=c.execute("SELECT 1 FROM bw_notified WHERE event_key=?",(ek,)).fetchone()
     if not sent:
      for uid in ALLOWED:
       await app.bot.send_message(uid,f"🔔 Binance {kind}: {x.get('coin',x.get('asset',''))} {x.get('amount','')} · {x.get('status','')}")
      with conn() as c:c.execute("INSERT OR IGNORE INTO bw_notified VALUES(?,?)",(ek,int(time.time())))
  except Exception:log.exception("background Binance sync")
  await asyncio.sleep(INTERVAL)

async def post_init(app):
 init_db()
 if KEY and SECRET and ALLOWED:app.create_task(background(app),name="binance-wallet-sync")
