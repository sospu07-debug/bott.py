import os,time,hmac,hashlib,logging,asyncio,sys,sqlite3
from urllib.parse import urlencode
from datetime import datetime,timezone,timedelta
import requests
from telegram import InlineKeyboardButton,InlineKeyboardMarkup

log=logging.getLogger("binance_wallet")
TURSO_URL=os.getenv("TURSO_HTTP_URL","")
TURSO_TOKEN=os.getenv("TURSO_TOKEN","")
LOCAL_DB_PATH=os.getenv("BINANCE_LOCAL_DB_PATH","/tmp/binance_wallet_fallback.db")
DB_MODE="cloud"
_LOCAL_CONNECTION=None

class CloudRow(dict):
 def __getitem__(self,key):
  if isinstance(key,int):return list(self.values())[key]
  return super().__getitem__(key)

class CloudCursor:
 def __init__(self,rows=None,rowcount=0):
  self.rows=rows or [];self.rowcount=rowcount
 def fetchone(self):return self.rows[0] if self.rows else None
 def fetchall(self):return self.rows

def _local_conn():
 global _LOCAL_CONNECTION
 if _LOCAL_CONNECTION is None:
  _LOCAL_CONNECTION=sqlite3.connect(LOCAL_DB_PATH,timeout=30,check_same_thread=False,isolation_level=None)
  _LOCAL_CONNECTION.row_factory=sqlite3.Row
  _LOCAL_CONNECTION.execute("PRAGMA busy_timeout=30000")
 return _LOCAL_CONNECTION

def _switch_to_local(reason):
 global DB_MODE
 if DB_MODE!="local":
  DB_MODE="local"
  log.exception("Cloud Binance database unavailable; switching to temporary server storage: %s",reason)
 with _local_conn() as db:
  db.executescript("""
  CREATE TABLE IF NOT EXISTS bw_snapshots(ts INTEGER,wallet TEXT,asset TEXT,amount TEXT,usd REAL,UNIQUE(ts,wallet,asset));
  CREATE TABLE IF NOT EXISTS bw_events(event_key TEXT PRIMARY KEY,kind TEXT,asset TEXT,amount TEXT,status TEXT,ts INTEGER,source TEXT,details TEXT);
  CREATE TABLE IF NOT EXISTS bw_income(event_key TEXT PRIMARY KEY,market TEXT,kind TEXT,asset TEXT,amount TEXT,ts INTEGER,details TEXT);
  CREATE TABLE IF NOT EXISTS bw_state(name TEXT PRIMARY KEY,value TEXT);
  CREATE TABLE IF NOT EXISTS bw_notified(event_key TEXT PRIMARY KEY,sent_at INTEGER);
  CREATE TABLE IF NOT EXISTS bw_portfolio(ts INTEGER PRIMARY KEY,total_usd REAL NOT NULL);
  CREATE INDEX IF NOT EXISTS bw_snap_ts ON bw_snapshots(ts);
  CREATE INDEX IF NOT EXISTS bw_event_ts ON bw_events(ts);
  """)

class CloudConnection:
 def __enter__(self):return self
 def __exit__(self,*args):return False
 def execute(self,sql,params=()):
  global DB_MODE
  if DB_MODE=="local":
   return _local_conn().execute(sql,params)
  try:
   return cloud_execute(sql,params)
  except Exception as e:
   _switch_to_local(e)
   return _local_conn().execute(sql,params)
 def executescript(self,sql):
  for statement in sql.split(";"):
   if statement.strip():self.execute(statement,())

def conn():
 if DB_MODE=="local":return _local_conn()
 return CloudConnection()

def cloud_execute(sql,params=()):
 config=sys.modules.get("__main__") or sys.modules.get("bot")
 url=TURSO_URL or getattr(config,"TURSO_HTTP_URL","")
 token=TURSO_TOKEN or getattr(config,"TURSO_TOKEN","")
 if not url or not token:raise RuntimeError("إعدادات قاعدة البيانات السحابية غير موجودة")
 args=[]
 for p in params or ():
  if p is None:args.append({"type":"null","value":None})
  elif isinstance(p,bool):args.append({"type":"integer","value":"1" if p else "0"})
  elif isinstance(p,int):args.append({"type":"integer","value":str(p)})
  elif isinstance(p,float):args.append({"type":"float","value":p})
  else:args.append({"type":"text","value":str(p)})
 stmt={"sql":sql}
 if args:stmt["args"]=args
 response=requests.post(url.rstrip("/")+"/v2/pipeline",
  json={"requests":[{"type":"execute","stmt":stmt},{"type":"close"}]},
  headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"},timeout=20)
 response.raise_for_status()
 data=response.json()
 if "results" not in data:raise RuntimeError("استجابة قاعدة البيانات السحابية غير صحيحة")
 first=data["results"][0]
 if first.get("type")=="error":raise RuntimeError(first.get("error",{}).get("message","Turso error"))
 result=first.get("response",{}).get("result",{})
 cols=[c.get("name") for c in result.get("cols",[])]
 rows=[CloudRow(zip(cols,[cell.get("value") for cell in row])) for row in result.get("rows",[])]
 return CloudCursor(rows,result.get("affected_row_count",result.get("rows_affected",0)))

def load_keys():
 # Read the repository's .env.example file (or a path supplied by BINANCE_ENV_FILE).
 values={}
 path=os.getenv("BINANCE_ENV_FILE",".env.example")
 try:
  with open(path,"r",encoding="utf-8") as f:
   for line in f:
    line=line.strip()
    if not line or line.startswith("#") or "=" not in line:continue
    k,v=line.split("=",1);values[k.strip()]=v.strip().strip('"').strip("'")
 except FileNotFoundError:
  log.warning("%s not found; falling back to process environment",path)
 return values

_FILE_KEYS=load_keys()
KEY=os.getenv("BINANCE_API_KEY") or _FILE_KEYS.get("BINANCE_API_KEY","")
SECRET=os.getenv("BINANCE_API_SECRET") or _FILE_KEYS.get("BINANCE_API_SECRET","")
BASE=(os.getenv("BINANCE_API_BASE") or _FILE_KEYS.get("BINANCE_API_BASE") or "https://api.binance.com").rstrip("/")
_allowed=os.getenv("BINANCE_ALLOWED_USER_IDS") or _FILE_KEYS.get("BINANCE_ALLOWED_USER_IDS","")
ALLOWED={int(x.strip()) for x in _allowed.split(",") if x.strip()}
INTERVAL=max(60,int(os.getenv("BINANCE_SYNC_SECONDS","60")))
FLOW_INTERVAL=max(60,int(os.getenv("BINANCE_FLOW_SYNC_SECONDS","60")))
BALANCE_CHANGES=[]
S=requests.Session()
if KEY:S.headers.update({"X-MBX-APIKEY":KEY})
PRICES={}

def init_db():
 global DB_MODE
 schema="""
 CREATE TABLE IF NOT EXISTS bw_snapshots(ts INTEGER,wallet TEXT,asset TEXT,amount TEXT,usd REAL,UNIQUE(ts,wallet,asset));
 CREATE TABLE IF NOT EXISTS bw_events(event_key TEXT PRIMARY KEY,kind TEXT,asset TEXT,amount TEXT,status TEXT,ts INTEGER,source TEXT,details TEXT);
 CREATE TABLE IF NOT EXISTS bw_income(event_key TEXT PRIMARY KEY,market TEXT,kind TEXT,asset TEXT,amount TEXT,ts INTEGER,details TEXT);
 CREATE TABLE IF NOT EXISTS bw_state(name TEXT PRIMARY KEY,value TEXT);
 CREATE TABLE IF NOT EXISTS bw_notified(event_key TEXT PRIMARY KEY,sent_at INTEGER);
 CREATE TABLE IF NOT EXISTS bw_portfolio(ts INTEGER PRIMARY KEY,total_usd REAL NOT NULL);
 CREATE INDEX IF NOT EXISTS bw_snap_ts ON bw_snapshots(ts);
 CREATE INDEX IF NOT EXISTS bw_event_ts ON bw_events(ts);
 """
 try:
  with conn() as c:c.executescript(schema)
  # تأكد أن الجداول الأساسية موجودة وقابلة للقراءة والكتابة.
  with conn() as c:
   c.execute("SELECT COUNT(*) FROM bw_snapshots").fetchone()
   c.execute("SELECT COUNT(*) FROM bw_state").fetchone()
  log.info("Binance database ready (%s)",DB_MODE)
 except Exception as e:
  _switch_to_local(e)
  with _local_conn() as c:c.executescript(schema)
  log.warning("Binance is using temporary local database: %s",LOCAL_DB_PATH)

def api(method,path,params=None,base=None,signed=True):
 if signed and (not KEY or not SECRET): raise RuntimeError("أضف BINANCE_API_KEY وBINANCE_API_SECRET")
 p=dict(params or {})
 if signed:
  p["timestamp"]=int(time.time()*1000);p.setdefault("recvWindow",10000)
  q=urlencode(p,doseq=True);p["signature"]=hmac.new(SECRET.encode(),q.encode(),hashlib.sha256).hexdigest()
 root=(base or BASE).rstrip("/")
 if root=="https://api.binance.com":
  hosts=[root,"https://api-gcp.binance.com","https://api1.binance.com","https://api2.binance.com","https://api3.binance.com","https://api4.binance.com"]
 elif root=="https://fapi.binance.com":
  hosts=[root,"https://fapi1.binance.com"]
 elif root=="https://dapi.binance.com":
  hosts=[root,"https://dapi1.binance.com"]
 else:hosts=[root]
 last=None
 for host in hosts:
  for attempt in range(2):
   try:
    r=S.request(method,host+path,params=p if method=="GET" else None,data=p if method!="GET" else None,timeout=12)
    if r.status_code in (418,429):
     last=requests.HTTPError(f"Binance rate limit {r.status_code}: {r.text[:200]}",response=r)
     time.sleep(min(10,2**(attempt+1)));continue
    if r.status_code in (403,451):
     last=requests.HTTPError(f"Binance HTTP {r.status_code} at {host}: {r.text[:200]}",response=r)
     break
    r.raise_for_status();return r.json()
   except requests.RequestException as e:
    last=e
    if getattr(getattr(e,"response",None),"status_code",None) in (403,451):break
    if attempt==0:time.sleep(1)
 if last:raise last
 raise RuntimeError("تعذر الاتصال بعناوين Binance البديلة")

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
 global PRICES,BALANCE_CHANGES
 # Keep the last successful snapshot so alerts reflect amount changes, not price movement.
 with conn() as c:
  prev_ts=c.execute("SELECT MAX(ts) FROM bw_snapshots").fetchone()[0]
  previous={}
  if prev_ts:
   for x in c.execute("SELECT wallet,asset,amount FROM bw_snapshots WHERE ts=?",(prev_ts,)).fetchall():
    previous[(x["wallet"],x["asset"])]=float(x["amount"])
 PRICES={x["symbol"]:float(x["price"]) for x in api("GET","/api/v3/ticker/price",signed=False)}
 ts=int(time.time());out={};available_wallets={"Spot","Funding"}
 out["Spot"]=save("Spot",api("GET","/api/v3/account").get("balances",[]),ts)
 out["Funding"]=save("Funding",api("POST","/sapi/v1/asset/get-funding-asset",{}),ts)
 for name,path in (("Earn Flexible","/sapi/v1/simple-earn/flexible/position"),("Earn Locked","/sapi/v1/simple-earn/locked/position")):
  try:out[name]=save(name,api("GET",path,{"current":1,"size":100}).get("rows",[]),ts);available_wallets.add(name)
  except Exception as e:log.warning("%s unavailable: %s",name,e);out[name]=[]
 for name,host,path in (("USD-M Futures","https://fapi.binance.com","/fapi/v3/balance"),("COIN-M Futures","https://dapi.binance.com","/dapi/v1/balance")):
  try:out[name]=save(name,api("GET",path,{},host),ts);available_wallets.add(name)
  except Exception as e:log.warning("%s unavailable: %s",name,e);out[name]=[]
 current={}
 for wallet,items in out.items():
  for asset,amount,value in items:current[(wallet,asset)]=amount
 with conn() as c:
  initialized=c.execute("SELECT value FROM bw_state WHERE name='balance_initialized'").fetchone()
  if initialized:
   changes=[]
   for key in set(previous)|set(current):
    if key[0] not in available_wallets:continue
    before=previous.get(key,0.0);after=current.get(key,0.0);delta=after-before
    if abs(delta)>1e-12:changes.append((key[0],key[1],before,after,delta))
   BALANCE_CHANGES=changes
  else:
   BALANCE_CHANGES=[]
   c.execute("INSERT OR REPLACE INTO bw_state VALUES('balance_initialized','1')")
  c.execute("INSERT OR REPLACE INTO bw_state VALUES('last_sync',?)",(str(ts),))
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

def save_portfolio_value():
 total,rows=latest()
 if not rows:return None
 ts=int(time.time())
 with conn() as db:
  db.execute("INSERT OR REPLACE INTO bw_portfolio VALUES(?,?)",(ts,total))
 return total

def pnl_report():
 # Futures trading PnL only; transfers, deposits and withdrawals are excluded.
 with conn() as db:
  row=db.execute(
   "SELECT COALESCE(SUM(CAST(amount AS REAL)),0) AS net "
   "FROM bw_income WHERE kind IN "
   "('REALIZED_PNL','COMMISSION','FUNDING_FEE','INSURANCE_CLEAR')"
  ).fetchone()
  first=db.execute("SELECT total_usd FROM bw_portfolio ORDER BY ts ASC LIMIT 1").fetchone()
 net=float(row["net"] or 0)
 initial=float(first["total_usd"] or 0) if first else 0.0
 pct=(net/initial*100.0) if initial>0 else 0.0
 sign="+" if pct>=0 else ""
 return f"{sign}{pct:.2f}%"

def local(ms):
 return datetime.fromtimestamp(int(ms)/1000,timezone.utc).astimezone(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M") if ms else "غير معروف"

def menu():
 return InlineKeyboardMarkup([
 [InlineKeyboardButton("الرصيد",callback_data="bn_balance")],
 [InlineKeyboardButton("🔄 تحديث الآن",callback_data="bn_refresh")],
 [InlineKeyboardButton("الأرباح والخسائر",callback_data="bn_pnl")],
 [InlineKeyboardButton("رجوع",callback_data="back_main")]])

def authorized(update):
 return bool(update.effective_user and ALLOWED and update.effective_user.id in ALLOWED)

async def open_menu(update,context):
 q=update.callback_query
 if not authorized(update):await q.answer("غير مصرح",show_alert=True);return
 await q.answer();await q.edit_message_text("Binance\nاختر القسم:",reply_markup=menu())

async def callback(update,context):
 q=update.callback_query
 if not authorized(update):await q.answer("غير مصرح",show_alert=True);return
 await q.answer();k=q.data
 if k=="bn_refresh":
  await q.edit_message_text("⏳ جاري تحديث بيانات Binance...")
  try:
   await asyncio.to_thread(sync_balances);events=await asyncio.to_thread(sync_flows);await asyncio.to_thread(sync_income)
   await asyncio.to_thread(save_portfolio_value)
   for ek,kind,x in events:
    with conn() as c:sent=c.execute("SELECT 1 FROM bw_notified WHERE event_key=?",(ek,)).fetchone()
    if not sent:
     msg=f"🔔 Binance {kind}\n{x.get('coin',x.get('asset',''))} {x.get('amount','')}\nالحالة: {x.get('status','غير محدد')}\n{local(x.get('insertTime',x.get('applyTime',x.get('timestamp',0))))}"
     for uid in ALLOWED:
      try:await context.bot.send_message(uid,msg)
      except Exception:log.exception("Telegram notify")
     with conn() as c:c.execute("INSERT OR IGNORE INTO bw_notified VALUES(?,?)",(ek,int(time.time())))
   await q.edit_message_text("✅ تمت المزامنة",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 تحديث الآن",callback_data="bn_refresh")],[InlineKeyboardButton("عرض الرصيد",callback_data="bn_balance")],[InlineKeyboardButton("رجوع",callback_data="back_main")]]))
  except Exception as e:
   log.exception("manual sync")
   await q.edit_message_text(f"فشل التحديث: {str(e)[:250]}",reply_markup=menu())
  return
 total,rows=latest()
 if k=="bn_balance":
  text=f"💰 القيمة المسعّرة: ${total:,.2f}\n" + ("\n".join(f"{x['wallet']} · {x['asset']}: {float(x['amount']):.8g} · "+(f"${float(x['usd']):,.2f}" if x['usd'] is not None else "غير مسعّر") for x in rows[:35]) if rows else "لا توجد لقطة محفوظة. اضغط زر تحديث الآن.")
 elif k=="bn_pnl":
  text=pnl_report()
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
 last_flow=0
 while True:
  try:
   if KEY and SECRET:
    await asyncio.to_thread(sync_balances)
    await asyncio.to_thread(save_portfolio_value)
    for wallet,asset,before,after,delta in BALANCE_CHANGES:
     direction="زيادة" if delta>0 else "نقصان"
     msg=(f"🔔 تغير رصيد Binance\\nالمحفظة: {wallet}\\nالعملة: {asset}\\n"
          f"التغيير: {direction} {abs(delta):.10g} {asset}\\n"
          f"السابق: {before:.10g}\\nالحالي: {after:.10g}")
     for uid in ALLOWED:
      try:await app.bot.send_message(uid,msg)
      except Exception:log.exception("balance notification")
    now=time.time()
    if now-last_flow>=FLOW_INTERVAL:
     events=await asyncio.to_thread(sync_flows);await asyncio.to_thread(sync_income);last_flow=now
     for ek,kind,x in events:
      with conn() as c:sent=c.execute("SELECT 1 FROM bw_notified WHERE event_key=?",(ek,)).fetchone()
      if not sent:
       msg=f"🔔 Binance {kind}\\n{x.get('coin',x.get('asset',''))} {x.get('amount','')}\\nالحالة: {x.get('status','غير محدد')}\\n{local(x.get('insertTime',x.get('applyTime',x.get('timestamp',0))))}"
       for uid in ALLOWED:
        try:await app.bot.send_message(uid,msg)
        except Exception:log.exception("Telegram notify")
       with conn() as c:c.execute("INSERT OR IGNORE INTO bw_notified VALUES(?,?)",(ek,int(time.time())))
  except Exception:log.exception("background Binance sync")
  await asyncio.sleep(INTERVAL)

async def post_init(app):
 init_db()
 if KEY and SECRET and ALLOWED:app.create_task(background(app),name="binance-wallet-sync")
