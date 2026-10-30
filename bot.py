import os
import sys
import threading
import traceback
import calendar
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:
    plt = None

import requests as http_requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)

#
# ============================================================
# خادم إبقاء التطبيق يعمل
# ============================================================

def _keep_alive():
    port = int(os.environ.get("PORT", 10000))

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"alive")

        def log_message(self, *a):
            pass

    HTTPServer(("0.0.0.0", port), H).serve_forever()


threading.Thread(target=_keep_alive, daemon=True).start()
print("Dummy server started", flush=True)


# ============================================================
# الإعدادات
# ============================================================

TURSO_HTTP_URL = "https://botyanal-sospu07-debug.aws-ap-northeast-1.turso.io/v2/pipeline"

TURSO_TOKEN = os.environ.get("TURSO_TOKEN", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
RATE = 0.75
AMMAN_TZ = timezone(timedelta(hours=3))


def now_amman():
    return datetime.now(AMMAN_TZ)


PRAYERS = ["Fajr", "Dhuhr", "Asr", "Maghrib", "Isha"]

PRAYERS_AR = {
    "Fajr": "الفجر",
    "Dhuhr": "الظهر",
    "Asr": "العصر",
    "Maghrib": "المغرب",
    "Isha": "العشاء",
}


# ============================================================
# عميل Turso عبر HTTP Pipeline
# ============================================================

def _build_args(params):
    args = []

    for p in params:
        if isinstance(p, bool):
            args.append({
                "type": "integer",
                "value": "1" if p else "0"
            })
        elif isinstance(p, int):
            args.append({
                "type": "integer",
                "value": str(p)
            })
        elif isinstance(p, float):
            args.append({
                "type": "float",
                "value": p
            })
        elif p is None:
            args.append({
                "type": "null",
                "value": None
            })
        else:
            args.append({
                "type": "text",
                "value": str(p)
            })

    return args


def _parse_rows(response):
    result = response.get("result", {})
    rows_raw = result.get("rows", [])
    rows = []

    for row in rows_raw:
        parsed = []

        for cell in row:
            v = cell.get("value")
            t = cell.get("type")

            if v is None:
                parsed.append(None)
                continue

            if t == "integer":
                try:
                    parsed.append(int(v))
                    continue
                except Exception:
                    pass

            elif t == "float":
                try:
                    parsed.append(float(v))
                    continue
                except Exception:
                    pass

            parsed.append(v)

        rows.append(parsed)

    return rows


def db_execute(sql, params=None):
    headers = {
        "Authorization": "Bearer " + TURSO_TOKEN,
        "Content-Type": "application/json",
    }

    if params:
        stmt = {
            "sql": sql,
            "args": _build_args(params)
        }
    else:
        stmt = {"sql": sql}

    payload = {
        "requests": [
            {"type": "execute", "stmt": stmt},
            {"type": "close"},
        ]
    }

    r = http_requests.post(
        TURSO_HTTP_URL,
        json=payload,
        headers=headers,
        timeout=30
    )

    if r.status_code != 200:
        raise Exception(
            "Turso HTTP {}: {}".format(
                r.status_code, r.text[:200]
            )
        )

    data = r.json()

    if "results" not in data:
        raise Exception(
            "Turso bad response: " + str(data)[:300]
        )

    first = data["results"][0]

    if first.get("type") == "error":
        msg = first.get("error", {}).get("message", "unknown")
        raise Exception("Turso error: " + str(msg))

    response = first.get("response", {})

    if response.get("type") == "execute":
        return _parse_rows(response)

    return []


def db_execute_many(statements):
    headers = {
        "Authorization": "Bearer " + TURSO_TOKEN,
        "Content-Type": "application/json",
    }

    reqs = []

    for sql, params in statements:
        if params:
            reqs.append({
                "type": "execute",
                "stmt": {
                    "sql": sql,
                    "args": _build_args(params)
                }
            })
        else:
            reqs.append({
                "type": "execute",
                "stmt": {"sql": sql}
            })

    reqs.append({"type": "close"})

    r = http_requests.post(
        TURSO_HTTP_URL,
        json={"requests": reqs},
        headers=headers,
        timeout=30
    )

    if r.status_code != 200:
        raise Exception(
            "Turso HTTP {}: {}".format(
                r.status_code, r.text[:200]
            )
        )

    return r.json()


def init_db():
    statements = [
        (
            "CREATE TABLE IF NOT EXISTS transactions ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "user_id INTEGER,"
            "category TEXT,"
            "amount REAL,"
            "reason TEXT,"
            "timestamp TEXT)",
            None
        ),
        (
            "CREATE TABLE IF NOT EXISTS notes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "user_id INTEGER,"
            "text TEXT,"
            "timestamp TEXT)",
            None
        ),
        (
            "CREATE TABLE IF NOT EXISTS prayers ("
            "user_id INTEGER,"
            "date TEXT,"
            "prayer TEXT,"
            "PRIMARY KEY (user_id, date, prayer))",
            None
        ),
        (
            "CREATE TABLE IF NOT EXISTS body_log ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "user_id INTEGER,"
            "weight REAL,"
            "height REAL,"
            "date TEXT,"
            "timestamp TEXT)",
            None
        ),
        (
            "CREATE TABLE IF NOT EXISTS habits ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "user_id INTEGER,"
            "kind TEXT,"
            "name TEXT,"
            "start_ts TEXT,"
            "timestamp TEXT)",
            None
        ),
        (
            "CREATE TABLE IF NOT EXISTS debts (id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,direction TEXT NOT NULL,person TEXT NOT NULL,amount REAL NOT NULL,currency TEXT NOT NULL,debt_date TEXT NOT NULL,notes TEXT,created_at TEXT NOT NULL)",
            None
        ),
        (
            "CREATE TABLE IF NOT EXISTS debt_payments (id INTEGER PRIMARY KEY AUTOINCREMENT,debt_id INTEGER NOT NULL,user_id INTEGER NOT NULL,amount REAL NOT NULL,payment_date TEXT NOT NULL,notes TEXT)",
            None
        ),
    ]

    db_execute_many(statements)


# ============================================================
# المصاري
# ============================================================

def add_transaction(user_id, category, amount, reason):
    db_execute(
        "INSERT INTO transactions "
        "(user_id, category, amount, reason, timestamp) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            int(user_id),
            category,
            float(amount),
            str(reason),
            now_amman().isoformat()
        ]
    )


def get_transaction_by_id(tx_id, user_id):
    rows = db_execute(
        "SELECT id, category, amount, reason, timestamp "
        "FROM transactions WHERE id = ? AND user_id = ?",
        [int(tx_id), int(user_id)]
    )

    if rows:
        return rows[0]

    return None


def update_transaction(
    tx_id,
    user_id,
    category=None,
    amount=None,
    reason=None
):
    tx = get_transaction_by_id(tx_id, user_id)

    if not tx:
        return False

    _, old_cat, old_amt, old_reason, _ = tx

    new_cat = category if category is not None else old_cat
    new_amt = amount if amount is not None else old_amt
    new_reason = reason if reason is not None else old_reason

    db_execute(
        "UPDATE transactions "
        "SET category = ?, amount = ?, reason = ? "
        "WHERE id = ? AND user_id = ?",
        [
            new_cat,
            float(new_amt),
            str(new_reason),
            int(tx_id),
            int(user_id)
        ]
    )

    return True


def delete_transaction(tx_id, user_id):
    db_execute(
        "DELETE FROM transactions WHERE id = ? AND user_id = ?",
        [int(tx_id), int(user_id)]
    )


def get_balance(user_id, category):
    rows = db_execute(
        "SELECT COALESCE(SUM(amount), 0) "
        "FROM transactions WHERE user_id = ? AND category = ?",
        [int(user_id), category]
    )

    if rows and rows[0]:
        try:
            return float(rows[0][0])
        except (ValueError, TypeError):
            return 0

    return 0


def get_all_balances(user_id):
    return {
        "cash": get_balance(user_id, "cash"),
        "bank": get_balance(user_id, "bank"),
        "dollar": get_balance(user_id, "dollar"),
    }


def get_transactions(user_id, category=None, date_filter=None):
    query = (
        "SELECT id, category, amount, reason, timestamp "
        "FROM transactions WHERE user_id = ?"
    )

    params = [int(user_id)]

    if category:
        query += " AND category = ?"
        params.append(category)

    if date_filter:
        query += " AND timestamp LIKE ?"
        params.append(date_filter + "%")

    query += " ORDER BY timestamp ASC"

    return db_execute(query, params)


def get_months(user_id):
    rows = db_execute(
        "SELECT DISTINCT substr(timestamp, 1, 7) as m "
        "FROM transactions WHERE user_id = ? ORDER BY m DESC",
        [int(user_id)]
    )

    return [row[0] for row in rows if row[0]]


def get_days(user_id, month):
    rows = db_execute(
        "SELECT DISTINCT substr(timestamp, 1, 10) as d "
        "FROM transactions "
        "WHERE user_id = ? AND timestamp LIKE ? "
        "ORDER BY d DESC",
        [int(user_id), month + "%"]
    )

    return [row[0] for row in rows if row[0]]



# ==================== الإحصائيات المالية ====================

def _period_dates(kind):
    today = now_amman().date()
    if kind == "daily":
        start = today
        end = start + timedelta(days=1)
    elif kind == "weekly":
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=7)
    elif kind == "yearly":
        start = today.replace(month=1, day=1)
        end = start.replace(year=start.year + 1)
    else:
        start = today.replace(day=1)
        end = start.replace(year=start.year + (start.month == 12), month=1 if start.month == 12 else start.month + 1)
    return start, end


def _previous_dates(kind, start):
    if kind == "daily":
        return start - timedelta(days=1), start
    if kind == "weekly":
        return start - timedelta(days=7), start
    if kind == "yearly":
        return start.replace(year=start.year - 1), start
    previous_end = start
    previous_start = (previous_end - timedelta(days=1)).replace(day=1)
    return previous_start, previous_end


def get_financial_stats(user_id, start, end):
    rows = db_execute(
        "SELECT category, amount, reason, timestamp FROM transactions "
        "WHERE user_id = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
        [int(user_id), start.isoformat(), end.isoformat()]
    )
    out = {"income_jod": 0.0, "expense_jod": 0.0, "income_usd": 0.0,
           "expense_usd": 0.0, "categories": {}, "days": {}}
    for category, amount, reason, stamp in rows:
        amount = float(amount or 0)
        currency = "usd" if category == "dollar" else "jod"
        day = str(stamp)[:10]
        bucket = out["days"].setdefault(day, {"income_jod": 0.0, "expense_jod": 0.0, "income_usd": 0.0, "expense_usd": 0.0})
        if amount >= 0:
            out["income_" + currency] += amount
            bucket["income_" + currency] += amount
        else:
            value = abs(amount)
            out["expense_" + currency] += value
            bucket["expense_" + currency] += value
            label = {"cash": "كاش", "bank": "بنك", "dollar": "دولار"}.get(category, str(category))
            out["categories"][label] = out["categories"].get(label, 0.0) + value
    return out


def _stats_text(stats, label):
    msg = [
        "الإحصائيات " + label, "-------------------",
        "JOD | الدخل: {:.2f} | المصروفات: {:.2f} | الصافي: {:.2f}".format(
            stats["income_jod"], stats["expense_jod"], stats["income_jod"] - stats["expense_jod"]),
        "USD | الدخل: {:.2f} | المصروفات: {:.2f} | الصافي: {:.2f}".format(
            stats["income_usd"], stats["expense_usd"], stats["income_usd"] - stats["expense_usd"]),
        "", "توزيع المصروفات حسب وسيلة الدفع:"
    ]
    if stats["categories"]:
        for name, amount in sorted(stats["categories"].items()):
            msg.append("{}: {:.2f}".format(name, amount))
        msg.append("تنبيه: التصنيف القديم يمثل وسيلة الدفع وليس تصنيفًا تفصيليًا.")
    else:
        msg.append("لا توجد مصروفات مسجلة.")
    msg += ["", "مخطط الدخل والمصروفات اليومي:"]
    for day, vals in sorted(stats["days"].items())[-14:]:
        msg.append("{} | JOD دخل {:.2f} مصروف {:.2f} | USD دخل {:.2f} مصروف {:.2f}".format(
            day[5:], vals["income_jod"], vals["expense_jod"], vals["income_usd"], vals["expense_usd"]))
    return "\n".join(msg)


async def show_financial_stats(query, user_id, kind="monthly"):
    labels = {"daily": "اليومية", "weekly": "الأسبوعية", "monthly": "الشهرية", "yearly": "السنوية"}
    start, end = _period_dates(kind)
    prev_start, prev_end = _previous_dates(kind, start)
    current = get_financial_stats(user_id, start, end)
    previous = get_financial_stats(user_id, prev_start, prev_end)
    msg = _stats_text(current, labels[kind])
    msg += "\n\nمقارنة بالفترة السابقة:\n"
    for key, label in (("income_jod", "دخل JOD"), ("expense_jod", "مصروفات JOD"),
                       ("income_usd", "دخل USD"), ("expense_usd", "مصروفات USD")):
        msg += "{}: {:+.2f}\n".format(label, current[key] - previous[key])
    kb = [
        [InlineKeyboardButton("يومي", callback_data="stats_daily"), InlineKeyboardButton("أسبوعي", callback_data="stats_weekly")],
        [InlineKeyboardButton("شهري", callback_data="stats_monthly"), InlineKeyboardButton("سنوي", callback_data="stats_yearly")],
        [InlineKeyboardButton("رجوع", callback_data="menu_money")]
    ]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb))
    # صورة رسم بياني اختيارية؛ يبقى العرض النصي متاحًا إذا لم تكن matplotlib مثبتة.
    if plt is not None and current["days"]:
        try:
            dates = sorted(current["days"])
            income_jod = [current["days"][d]["income_jod"] for d in dates]
            expense_jod = [current["days"][d]["expense_jod"] for d in dates]
            income_usd = [current["days"][d]["income_usd"] for d in dates]
            expense_usd = [current["days"][d]["expense_usd"] for d in dates]
            fig, ax = plt.subplots(figsize=(10, 4))
            positions = list(range(len(dates)))
            width = 0.38
            ax.bar([x - 1.5 * width for x in positions], income_jod, width, label="Income JOD")
            ax.bar([x - 0.5 * width for x in positions], expense_jod, width, label="Expenses JOD")
            ax.bar([x + 0.5 * width for x in positions], income_usd, width, label="Income USD")
            ax.bar([x + 1.5 * width for x in positions], expense_usd, width, label="Expenses USD")
            ax.set_xticks(positions)
            ax.set_xticklabels([d[5:] for d in dates], rotation=45, ha="right")
            ax.set_title("Daily income and expenses")
            ax.set_ylabel("Amount (JOD and USD shown separately)")
            ax.legend()
            ax.grid(axis="y", alpha=0.25)
            fig.tight_layout()
            image_buffer = BytesIO()
            fig.savefig(image_buffer, format="png", dpi=140)
            plt.close(fig)
            image_buffer.seek(0)
            await query.message.reply_photo(photo=image_buffer, caption="الرسم البياني للفترة المختارة")
        except Exception as chart_error:
            print("statistics chart error:", repr(chart_error), flush=True)


# ==================== إدارة الديون ====================

def create_debt(user_id, direction, person, amount, currency, debt_date, notes=""):
    amount = float(amount)
    if amount <= 0 or currency not in ("JOD", "USD"):
        raise ValueError("المبلغ أو العملة غير صالحين")
    db_execute(
        "INSERT INTO debts (user_id,direction,person,amount,currency,debt_date,notes,created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        [int(user_id), direction, str(person), amount, currency, debt_date, str(notes), now_amman().isoformat()])


def get_debt_by_id(debt_id, user_id):
    rows = db_execute(
        "SELECT d.id,d.direction,d.person,d.amount,d.currency,d.debt_date,d.notes,"
        "COALESCE((SELECT SUM(p.amount) FROM debt_payments p WHERE p.debt_id=d.id AND p.user_id=d.user_id),0) "
        "FROM debts d WHERE d.id=? AND d.user_id=?", [int(debt_id), int(user_id)])
    return rows[0] if rows else None


def get_debts(user_id, direction=None):
    sql = ("SELECT d.id,d.direction,d.person,d.amount,d.currency,d.debt_date,d.notes,"
           "COALESCE((SELECT SUM(p.amount) FROM debt_payments p WHERE p.debt_id=d.id AND p.user_id=d.user_id),0) "
           "FROM debts d WHERE d.user_id=?")
    params = [int(user_id)]
    if direction:
        sql += " AND d.direction=?"
        params.append(direction)
    sql += " ORDER BY d.debt_date DESC,d.id DESC"
    return db_execute(sql, params)


def get_debt_payments(debt_id, user_id):
    return db_execute("SELECT id,amount,payment_date,notes FROM debt_payments "
                      "WHERE debt_id=? AND user_id=? ORDER BY payment_date DESC,id DESC",
                      [int(debt_id), int(user_id)])


def add_debt_payment(debt_id, user_id, amount):
    debt = get_debt_by_id(debt_id, user_id)
    amount = float(amount)
    if not debt:
        raise ValueError("الدين غير موجود")
    remaining = float(debt[3]) - float(debt[7])
    if amount <= 0 or amount > remaining + 0.000001:
        raise ValueError("الدفعة يجب أن تكون موجبة ولا تتجاوز المبلغ المتبقي")
    db_execute("INSERT INTO debt_payments (debt_id,user_id,amount,payment_date,notes) VALUES (?,?,?,?,?)",
               [int(debt_id), int(user_id), amount, now_amman().isoformat(), ""])


def update_debt(debt_id, user_id, field, value):
    if field not in ("person", "amount", "currency", "debt_date", "notes", "direction"):
        raise ValueError("حقل غير صالح")
    debt = get_debt_by_id(debt_id, user_id)
    if not debt:
        raise ValueError("الدين غير موجود")
    if field == "amount":
        value = float(value)
        if value <= 0 or value + 0.000001 < float(debt[7]):
            raise ValueError("المبلغ الجديد أقل من المبالغ المدفوعة")
    if field == "currency":
        if value not in ("JOD", "USD") or float(debt[7]) > 0:
            raise ValueError("لا يمكن تغيير العملة بعد تسجيل دفعات")
    if field == "direction" and value not in ("owed_to_me", "i_owe"):
        raise ValueError("نوع الدين غير صالح")
    db_execute("UPDATE debts SET " + field + "=? WHERE id=? AND user_id=?",
               [value, int(debt_id), int(user_id)])


def delete_debt(debt_id, user_id):
    db_execute_many([
        ("DELETE FROM debt_payments WHERE debt_id=? AND user_id=?", [int(debt_id), int(user_id)]),
        ("DELETE FROM debts WHERE id=? AND user_id=?", [int(debt_id), int(user_id)])])


def _debt_status(total, paid):
    if paid <= 0:
        return "غير مسدد"
    return "مسدد" if paid + 0.000001 >= total else "مسدد جزئيًا"


async def show_debts_menu(query, user_id):
    owed, owe = {"JOD": 0.0, "USD": 0.0}, {"JOD": 0.0, "USD": 0.0}
    for row in get_debts(user_id):
        target = owed if row[1] == "owed_to_me" else owe
        target[row[4]] += max(0.0, float(row[3]) - float(row[7]))
    msg = ("إدارة الديون 💰\n-------------------\nلي عند الآخرين: {:.2f} JOD | {:.2f} USD\n"
           "عليّ للآخرين: {:.2f} JOD | {:.2f} USD").format(
               owed["JOD"], owed["USD"], owe["JOD"], owe["USD"])
    kb = [[InlineKeyboardButton("إضافة دين", callback_data="debt_add")],
          [InlineKeyboardButton("ديون لي عند أشخاص", callback_data="debt_list_owed")],
          [InlineKeyboardButton("ديون عليّ", callback_data="debt_list_owe")],
          [InlineKeyboardButton("رجوع", callback_data="menu_money")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb))


async def show_debt_list(query, user_id, direction):
    rows = get_debts(user_id, direction)
    if not rows:
        await query.edit_message_text("لا توجد ديون.", reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("رجوع", callback_data="debts_menu")]]))
        return
    kb = []
    for row in rows:
        remaining = max(0.0, float(row[3]) - float(row[7]))
        label = "{} | {:.2f} {} | {}".format(row[2], remaining, row[4], _debt_status(float(row[3]), float(row[7])))
        kb.append([InlineKeyboardButton(label[:60], callback_data="debt_view_" + str(row[0]))])
    kb.append([InlineKeyboardButton("رجوع", callback_data="debts_menu")])
    await query.edit_message_text("قائمة الديون:", reply_markup=InlineKeyboardMarkup(kb))


async def show_debt_detail(query, user_id, debt_id):
    d = get_debt_by_id(debt_id, user_id)
    if not d:
        await query.edit_message_text("الدين غير موجود.")
        return
    paid = float(d[7])
    remaining = max(0.0, float(d[3]) - paid)
    msg = ("الشخص: {}\nالنوع: {}\nالأصل: {:.2f} {}\nالمدفوع: {:.2f} {}\nالمتبقي: {:.2f} {}\n"
           "الحالة: {}\nالتاريخ: {}\nملاحظات: {}").format(
        d[2], "لي عنده" if d[1] == "owed_to_me" else "عليّ له",
        float(d[3]), d[4], paid, d[4], remaining, d[4],
        _debt_status(float(d[3]), paid), d[5], d[6] or "—")
    payments = get_debt_payments(debt_id, user_id)
    if payments:
        msg += "\n\nسجل الدفعات:"
        for p in payments[:10]:
            msg += "\n• {:.2f} {} | {}".format(float(p[1]), d[4], str(p[2])[:10])
    kb = []
    if remaining > 0:
        kb.append([InlineKeyboardButton("تسجيل دفعة", callback_data="debt_pay_" + str(debt_id))])
    kb += [[InlineKeyboardButton("تعديل", callback_data="debt_edit_" + str(debt_id))],
           [InlineKeyboardButton("حذف", callback_data="debt_delete_" + str(debt_id))],
           [InlineKeyboardButton("رجوع", callback_data="debt_list_owed" if d[1] == "owed_to_me" else "debt_list_owe")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb))


async def debt_name_input(update, context):
    value = update.message.text.strip()
    if not value:
        await update.message.reply_text("اكتب اسم الشخص:")
        return DEBT_NAME
    context.user_data["debt_person"] = value
    await update.message.reply_text("اكتب المبلغ:")
    return DEBT_AMOUNT


async def debt_amount_input(update, context):
    try:
        value = float(update.message.text.strip().replace(",", "."))
        if value <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("اكتب مبلغًا صحيحًا أكبر من صفر:")
        return DEBT_AMOUNT
    context.user_data["debt_amount"] = value
    kb = [[InlineKeyboardButton("JOD دينار أردني", callback_data="debt_currency_JOD")],
          [InlineKeyboardButton("USD دولار أمريكي", callback_data="debt_currency_USD")]]
    await update.message.reply_text("اختر العملة:", reply_markup=InlineKeyboardMarkup(kb))
    return DEBT_CURRENCY


async def debt_notes_input(update, context):
    return await _save_debt(update, context, update.message.text.strip())


async def debt_date_input(update, context):
    value = update.message.text.strip()
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("أدخل التاريخ بصيغة YYYY-MM-DD:")
        return DEBT_DATE
    context.user_data["debt_date"] = value
    await update.message.reply_text("اكتب ملاحظات أو اضغط تخطي:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("تخطي", callback_data="debt_skip_notes")]]))
    return DEBT_NOTES


async def _save_debt(update, context, notes):
    try:
        create_debt(update.effective_user.id, context.user_data["debt_direction"],
                    context.user_data["debt_person"], context.user_data["debt_amount"],
                    context.user_data["debt_currency"], context.user_data["debt_date"], notes)
        await update.effective_message.reply_text("تم تسجيل الدين.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("إدارة الديون", callback_data="debts_menu")]]))
    except Exception as e:
        print("debt save error:", repr(e), flush=True)
        await update.effective_message.reply_text("تعذر حفظ الدين. تحقق من البيانات وحاول مجددًا.")
    return ConversationHandler.END


async def debt_payment_input(update, context):
    debt_id = context.user_data.get("debt_payment_id")
    try:
        amount = float(update.message.text.strip().replace(",", "."))
        add_debt_payment(debt_id, update.effective_user.id, amount)
    except Exception as e:
        await update.message.reply_text(str(e) or "تعذر تسجيل الدفعة:")
        return DEBT_PAYMENT_AMOUNT
    await update.message.reply_text("تم تسجيل الدفعة وتحديث المتبقي.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("عرض الدين", callback_data="debt_view_" + str(debt_id))]]))
    return ConversationHandler.END


async def debt_edit_input(update, context):
    debt_id = context.user_data.get("debt_edit_id")
    field = context.user_data.get("debt_edit_field")
    value = update.message.text.strip()
    try:
        if field == "debt_date":
            datetime.strptime(value, "%Y-%m-%d")
        if field == "amount":
            value = float(value.replace(",", "."))
        update_debt(debt_id, update.effective_user.id, field, value)
    except Exception as e:
        await update.message.reply_text(str(e) or "تعذر تعديل الدين:")
        return DEBT_EDIT_VALUE
    await update.message.reply_text("تم تعديل الدين.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("عرض الدين", callback_data="debt_view_" + str(debt_id))]]))
    return ConversationHandler.END


# ============================================================
# الملاحظات
# ============================================================

def add_note(user_id, text):
    db_execute(
        "INSERT INTO notes (user_id, text, timestamp) "
        "VALUES (?, ?, ?)",
        [
            int(user_id),
            str(text),
            now_amman().isoformat()
        ]
    )


def get_notes(user_id, limit=20):
    return db_execute(
        "SELECT id, text, timestamp FROM notes "
        "WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
        [int(user_id), int(limit)]
    )


def delete_note(note_id, user_id):
    db_execute(
        "DELETE FROM notes WHERE id = ? AND user_id = ?",
        [int(note_id), int(user_id)]
    )


def get_note_by_id(note_id, user_id):
    rows = db_execute(
        "SELECT id, text, timestamp FROM notes "
        "WHERE id = ? AND user_id = ?",
        [int(note_id), int(user_id)]
    )

    if rows:
        return rows[0]

    return None


# ============================================================
# الصلاة
# ============================================================

def log_prayer(user_id, date, prayer):
    try:
        db_execute(
            "INSERT OR IGNORE INTO prayers "
            "(user_id, date, prayer) VALUES (?, ?, ?)",
            [int(user_id), date, prayer]
        )
    except Exception as e:
        print(">>> log_prayer error:", repr(e), flush=True)


def unlog_prayer(user_id, date, prayer):
    db_execute(
        "DELETE FROM prayers "
        "WHERE user_id = ? AND date = ? AND prayer = ?",
        [int(user_id), date, prayer]
    )


def get_today_prayers(user_id):
    today = now_amman().strftime("%Y-%m-%d")

    rows = db_execute(
        "SELECT prayer FROM prayers "
        "WHERE user_id = ? AND date = ?",
        [int(user_id), today]
    )

    return [row[0] for row in rows]


def get_prayer_months(user_id):
    rows = db_execute(
        "SELECT DISTINCT date FROM prayers "
        "WHERE user_id = ? ORDER BY date DESC",
        [int(user_id)]
    )

    dates = [row[0] for row in rows if row[0]]
    months = []

    for d in dates:
        m = d[:7]

        if m not in months:
            months.append(m)

    return months


def get_prayer_days(user_id, month):
    rows = db_execute(
        "SELECT DISTINCT date FROM prayers "
        "WHERE user_id = ? AND date LIKE ? "
        "ORDER BY date DESC",
        [int(user_id), month + "%"]
    )

    return [row[0] for row in rows if row[0]]


def get_prayers_by_date(user_id, date):
    rows = db_execute(
        "SELECT prayer FROM prayers "
        "WHERE user_id = ? AND date = ?",
        [int(user_id), date]
    )

    return [row[0] for row in rows]


# ============================================================
# الوزن والطول
# ============================================================

def add_body_log(user_id, weight, height):
    today = now_amman().strftime("%Y-%m-%d")

    db_execute_many([
        (
            "DELETE FROM body_log WHERE user_id = ? AND date = ?",
            [int(user_id), today]
        ),
        (
            "INSERT INTO body_log "
            "(user_id, weight, height, date, timestamp) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                int(user_id),
                float(weight),
                float(height),
                today,
                now_amman().isoformat()
            ]
        ),
    ])


def get_body_logs(user_id, limit=30):
    return db_execute(
        "SELECT id, weight, height, date, timestamp "
        "FROM body_log WHERE user_id = ? "
        "ORDER BY date DESC LIMIT ?",
        [int(user_id), int(limit)]
    )


def get_today_body(user_id):
    today = now_amman().strftime("%Y-%m-%d")

    rows = db_execute(
        "SELECT id, weight, height, date FROM body_log "
        "WHERE user_id = ? AND date = ?",
        [int(user_id), today]
    )

    if rows:
        return rows[0]

    return None


# ============================================================
# العادات
# ============================================================

def add_habit(user_id, kind, name, start_ts):
    db_execute(
        "INSERT INTO habits "
        "(user_id, kind, name, start_ts, timestamp) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            int(user_id),
            kind,
            str(name),
            start_ts,
            now_amman().isoformat()
        ]
    )


def get_habits(user_id):
    return db_execute(
        "SELECT id, kind, name, start_ts FROM habits "
        "WHERE user_id = ? ORDER BY id DESC",
        [int(user_id)]
    )


def get_habit_by_id(habit_id, user_id):
    rows = db_execute(
        "SELECT id, kind, name, start_ts FROM habits "
        "WHERE id = ? AND user_id = ?",
        [int(habit_id), int(user_id)]
    )

    if rows:
        return rows[0]

    return None


def delete_habit(habit_id, user_id):
    db_execute(
        "DELETE FROM habits WHERE id = ? AND user_id = ?",
        [int(habit_id), int(user_id)]
    )


def reset_habit_start(habit_id, user_id, start_ts):
    db_execute(
        "UPDATE habits SET start_ts = ? "
        "WHERE id = ? AND user_id = ?",
        [start_ts, int(habit_id), int(user_id)]
    )


def format_duration(start_ts_str):
    try:
        if "T" in start_ts_str:
            start = datetime.fromisoformat(start_ts_str)

            if start.tzinfo is None:
                start = start.replace(tzinfo=AMMAN_TZ)
            else:
                start = start.astimezone(AMMAN_TZ)

        else:
            start = datetime.strptime(
                start_ts_str,
                "%Y-%m-%d %H:%M"
            )
            start = start.replace(tzinfo=AMMAN_TZ)

    except Exception:
        return "غير معروف"

    now = now_amman()
    delta = now - start

    if delta.total_seconds() < 0:
        return "لم يبدأ بعد"

    total_seconds = int(delta.total_seconds())

    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    parts = []

    if days > 0:
        parts.append("{} يوم".format(days))

    if hours > 0:
        parts.append("{} ساعة".format(hours))

    if minutes > 0:
        parts.append("{} دقيقة".format(minutes))

    parts.append("{} ثانية".format(seconds))

    return "، ".join(parts)


KIND_AR = {
    "commit": "التزام",
    "quit": "قطع"
}


# ============================================================
# حالات المحادثة
# ============================================================

(
    CHOOSE_ACTION,
    ENTER_AMOUNT,
    ENTER_REASON,
    EDIT_CHOOSE_FIELD,
    EDIT_ENTER_VALUE,
    ENTER_NOTE,
    BODY_WEIGHT,
    BODY_HEIGHT,
    HABIT_NAME,
    HABIT_CUSTOM_TIME,
    HABIT_YEAR,
    HABIT_MONTH,
    HABIT_DAY,
    HABIT_HOUR,
    DEBT_TYPE, DEBT_NAME, DEBT_AMOUNT, DEBT_CURRENCY, DEBT_DATE, DEBT_NOTES,
    DEBT_PAYMENT_AMOUNT, DEBT_EDIT_FIELD, DEBT_EDIT_VALUE
) = range(23)


CAT_NAMES = {
    "cash": "كاش",
    "bank": "بنك",
    "dollar": "دولار"
}


MONTH_NAMES = {
    "01": "كانون الثاني",
    "02": "شباط",
    "03": "آذار",
    "04": "نيسان",
    "05": "أيار",
    "06": "حزيران",
    "07": "تموز",
    "08": "آب",
    "09": "أيلول",
    "10": "تشرين الأول",
    "11": "تشرين الثاني",
    "12": "كانون الأول"
}


# ============================================================
# أزرار اختيار تاريخ العادة
# ============================================================

def habit_year_keyboard():
    current_year = now_amman().year
    buttons = []

    for year in range(current_year, current_year + 4):
        buttons.append([
            InlineKeyboardButton(
                str(year),
                callback_data="hy_" + str(year)
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "إلغاء",
            callback_data="menu_habits"
        )
    ])

    return InlineKeyboardMarkup(buttons)


def habit_month_keyboard():
    buttons = []
    row = []

    for month in range(1, 13):
        month_str = "{:02d}".format(month)

        row.append(
            InlineKeyboardButton(
                MONTH_NAMES[month_str],
                callback_data="hm_" + month_str
            )
        )

        if len(row) == 3:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="habit_back_year"
        ),
        InlineKeyboardButton(
            "إلغاء",
            callback_data="menu_habits"
        )
    ])

    return InlineKeyboardMarkup(buttons)


def habit_day_keyboard(year, month):
    days_count = calendar.monthrange(year, month)[1]

    buttons = []
    row = []

    for day in range(1, days_count + 1):
        row.append(
            InlineKeyboardButton(
                str(day),
                callback_data="hd_" + str(day)
            )
        )

        if len(row) == 7:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="habit_back_month"
        ),
        InlineKeyboardButton(
            "إلغاء",
            callback_data="menu_habits"
        )
    ])

    return InlineKeyboardMarkup(buttons)


def habit_hour_keyboard():
    buttons = []
    row = []

    for hour in range(24):
        row.append(
            InlineKeyboardButton(
                "{:02d}:00".format(hour),
                callback_data="hh_" + str(hour)
            )
        )

        if len(row) == 4:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="habit_back_day"
        ),
        InlineKeyboardButton(
            "إلغاء",
            callback_data="menu_habits"
        )
    ])

    return InlineKeyboardMarkup(buttons)


# ============================================================
# تنسيق المبالغ
# ============================================================

def format_balances(user_id):
    b = get_all_balances(user_id)

    cash = b["cash"]
    bank = b["bank"]
    usd = b["dollar"]

    usd_in_jod = usd * RATE
    total_jod = cash + bank + usd_in_jod
    total_usd = total_jod / RATE if RATE else 0

    return (
        "المبالغ الحالية\n"
        "-------------------\n"
        "كاش: {:.2f}\n".format(cash) +
        "بنك: {:.2f}\n".format(bank) +
        "دولار: {:.2f} ({:.2f} دينار)\n".format(
            usd, usd_in_jod
        ) +
        "-------------------\n"
        "الإجمالي بالدينار: {:.2f}\n".format(total_jod) +
        "الإجمالي بالدولار: {:.2f}".format(total_usd)
    )


def build_csv(user_id, date_filter=None):
    txs = get_transactions(
        user_id,
        date_filter=date_filter
    )

    lines = ["date,time,category,amount,reason"]

    for tx in txs:
        tx_id, cat, amt, reason, ts = tx

        date = str(ts)[:10]
        time_value = str(ts)[11:16]
        reason_clean = str(reason).replace(",", " ")

        lines.append(
            "{},{},{},{:.2f},{}".format(
                date,
                time_value,
                cat,
                float(amt),
                reason_clean
            )
        )

    return "\n".join(lines)


async def send_csv(update, context, date_filter, filename):
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id

    csv_data = build_csv(user_id, date_filter)

    if len(csv_data.split("\n")) <= 1:
        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="log_months"
            )
        ]]

        await query.edit_message_text(
            "لا يوجد عمليات في هذه الفترة.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    file_bytes = csv_data.encode("utf-8")

    await query.message.reply_document(
        document=file_bytes,
        filename=filename
    )


# ============================================================
# القوائم الرئيسية
# ============================================================

def main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("فلوس", callback_data="menu_money")],
        [InlineKeyboardButton("الإحصائيات 📊", callback_data="stats_menu")],
        [InlineKeyboardButton("الديون 💰", callback_data="debts_menu")],
        [InlineKeyboardButton("ملاحظات", callback_data="menu_notes")],
        [InlineKeyboardButton("صلاة", callback_data="menu_prayer")],
        [InlineKeyboardButton("عاداتي", callback_data="menu_habits")],
        [InlineKeyboardButton("الوزن والطول", callback_data="menu_body")],
    ])


async def start(update, context):
    await update.message.reply_text(
        "القائمة الرئيسية",
        reply_markup=main_keyboard()
    )


async def money_menu(update, context):
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    msg = format_balances(user_id)

    kb = [
        [
            InlineKeyboardButton("بنك", callback_data="cat_bank"),
            InlineKeyboardButton("كاش", callback_data="cat_cash"),
            InlineKeyboardButton("دولار", callback_data="cat_dollar"),
        ],
        [InlineKeyboardButton("سجل", callback_data="log_months")],
        [InlineKeyboardButton("الإحصائيات 📊", callback_data="stats_menu")],
        [InlineKeyboardButton("إدارة الديون 💰", callback_data="debts_menu")],
        [InlineKeyboardButton("رجوع", callback_data="back_main")],
    ]

    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def notes_menu(update, context):
    query = update.callback_query
    await query.answer()

    kb = [
        [InlineKeyboardButton("إضافة ملاحظة", callback_data="note_add")],
        [InlineKeyboardButton("عرض الملاحظات", callback_data="note_list")],
        [InlineKeyboardButton("رجوع", callback_data="back_main")],
    ]

    await query.edit_message_text(
        "الملاحظات:",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def prayer_menu(update, context):
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    today = now_amman().strftime("%Y-%m-%d")
    done = get_today_prayers(user_id)

    msg = (
        "الصلوات\n"
        "التاريخ: " + today + "\n"
        "-------------------\n"
    )

    for p in PRAYERS:
        status = "تم" if p in done else "لم"
        msg += PRAYERS_AR[p] + ": " + status + "\n"

    buttons = []

    for p in PRAYERS:
        mark = " ✓" if p in done else ""

        buttons.append([
            InlineKeyboardButton(
                PRAYERS_AR[p] + mark,
                callback_data="pr_" + p
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "سجل الصلاة",
            callback_data="plog_months"
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="back_main"
        )
    ])

    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def habits_menu(update, context):
    query = update.callback_query
    await query.answer()

    kb = [
        [InlineKeyboardButton("إضافة التزام", callback_data="habit_commit")],
        [InlineKeyboardButton("إضافة قطع", callback_data="habit_quit")],
        [InlineKeyboardButton("عاداتي الحالية", callback_data="habit_list")],
        [InlineKeyboardButton("رجوع", callback_data="back_main")],
    ]

    await query.edit_message_text(
        "عاداتي:",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def habits_choose_time(update, context, kind):
    query = update.callback_query

    context.user_data["habit_kind"] = kind

    kind_ar = KIND_AR[kind]

    kb = [
        [InlineKeyboardButton("من الآن", callback_data="habit_now")],
        [InlineKeyboardButton("من وقت محدد", callback_data="habit_custom")],
        [InlineKeyboardButton("رجوع", callback_data="menu_habits")],
    ]

    await query.edit_message_text(
        kind_ar + "\n\nمتى بدأ؟",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def show_habits_list(query, user_id):
    habits = get_habits(user_id)

    if not habits:
        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="menu_habits"
            )
        ]]

        await query.edit_message_text(
            "لا يوجد عادات مسجلة.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    buttons = []

    for h in habits:
        h_id, kind, name, start_ts = h

        kind_ar = KIND_AR.get(kind, kind)
        label = kind_ar + " | " + str(name)[:25]

        buttons.append([
            InlineKeyboardButton(
                label,
                callback_data="habit_v_" + str(h_id)
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="menu_habits"
        )
    ])

    await query.edit_message_text(
        "عاداتك:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def show_habit_detail(query, user_id, habit_id):
    h = get_habit_by_id(habit_id, user_id)

    if not h:
        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="menu_habits"
            )
        ]]

        await query.edit_message_text(
            "العادة غير موجودة.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    h_id, kind, name, start_ts = h

    kind_ar = KIND_AR.get(kind, kind)
    duration = format_duration(str(start_ts))
    start_display = str(start_ts)[:16].replace("T", " ")

    msg = (
        kind_ar + ": " + str(name) + "\n"
        "البداية: " + start_display + "\n"
        "-------------------\n"
        "المدة: " + duration
    )

    kb = [
        [InlineKeyboardButton("تحديث", callback_data="habit_r_" + str(h_id))],
        [InlineKeyboardButton("حذف", callback_data="habit_d_" + str(h_id))],
        [InlineKeyboardButton("رجوع", callback_data="habit_list")],
    ]

    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def body_menu(update, context):
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    today = get_today_body(user_id)

    if today:
        _, w, h, date = today

        msg = (
            "الوزن والطول\n"
            "-------------------\n"
            "اليوم: " + str(date) + "\n"
            "الوزن: {:.2f} kg\n".format(float(w)) +
            "الطول: {:.2f} cm".format(float(h))
        )

    else:
        msg = (
            "الوزن والطول\n"
            "-------------------\n"
            "لم تسجل اليوم."
        )

    kb = [
        [InlineKeyboardButton("تسجيل اليوم", callback_data="body_add")],
        [InlineKeyboardButton("السجل", callback_data="body_list")],
        [InlineKeyboardButton("رجوع", callback_data="back_main")],
    ]

    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def body_list(query, user_id):
    logs = get_body_logs(user_id, limit=30)

    if not logs:
        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="menu_body"
            )
        ]]

        await query.edit_message_text(
            "لا يوجد سجل.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    msg = (
        "سجل الوزن والطول\n"
        "-------------------\n"
    )

    for log in logs:
        log_id, w, h, date, ts = log

        msg += str(date) + " | {:.1f} kg | {:.1f} cm\n".format(
            float(w), float(h)
        )

    kb = [[
        InlineKeyboardButton(
            "رجوع",
            callback_data="menu_body"
        )
    ]]

    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(kb)
    )


# ============================================================
# المعالج الرئيسي للأزرار
# ============================================================

async def category_selected(update, context):
    query = update.callback_query
    await query.answer()

    data = query.data

    # القوائم الرئيسية

    if data == "menu_money":
        await money_menu(update, context)
        return ConversationHandler.END

    if data == "menu_notes":
        await notes_menu(update, context)
        return ConversationHandler.END

    if data == "menu_prayer":
        await prayer_menu(update, context)
        return ConversationHandler.END

    if data == "menu_habits":
        await habits_menu(update, context)
        return ConversationHandler.END

    if data == "menu_body":
        await body_menu(update, context)
        return ConversationHandler.END

    if data == "back_main":
        await query.edit_message_text(
            "القائمة الرئيسية",
            reply_markup=main_keyboard()
        )
        return ConversationHandler.END

    if data == "back_money":
        await money_menu(update, context)
        return ConversationHandler.END


    # الإحصائيات
    if data == "stats_menu":
        await show_financial_stats(query, update.effective_user.id, "monthly")
        return ConversationHandler.END
    if data.startswith("stats_"):
        kind = data.replace("stats_", "")
        if kind in ("daily", "weekly", "monthly", "yearly"):
            await show_financial_stats(query, update.effective_user.id, kind)
        return ConversationHandler.END

    # الديون
    if data == "debts_menu":
        await show_debts_menu(query, update.effective_user.id)
        return ConversationHandler.END
    if data == "debt_add":
        kb = [[InlineKeyboardButton("لي دين عند شخص", callback_data="debt_type_owed_to_me")],
              [InlineKeyboardButton("عليّ دين لشخص", callback_data="debt_type_i_owe")],
              [InlineKeyboardButton("إلغاء", callback_data="debts_menu")]]
        await query.edit_message_text("اختر نوع الدين:", reply_markup=InlineKeyboardMarkup(kb))
        return DEBT_TYPE
    if data.startswith("debt_type_"):
        direction = data.replace("debt_type_", "")
        if direction not in ("owed_to_me", "i_owe"):
            return ConversationHandler.END
        context.user_data["debt_direction"] = direction
        await query.edit_message_text("اكتب اسم الشخص:")
        return DEBT_NAME
    if data.startswith("debt_currency_"):
        currency = data.replace("debt_currency_", "")
        if currency not in ("JOD", "USD"):
            return DEBT_CURRENCY
        context.user_data["debt_currency"] = currency
        today = now_amman().strftime("%Y-%m-%d")
        kb = [[InlineKeyboardButton("اليوم " + today, callback_data="debt_date_today")]]
        await query.edit_message_text("أدخل تاريخ الدين بصيغة YYYY-MM-DD أو اختر اليوم:", reply_markup=InlineKeyboardMarkup(kb))
        return DEBT_DATE
    if data == "debt_date_today":
        context.user_data["debt_date"] = now_amman().strftime("%Y-%m-%d")
        await query.edit_message_text("اكتب ملاحظات اختيارية أو اضغط تخطي:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("تخطي", callback_data="debt_skip_notes")]]))
        return DEBT_NOTES
    if data == "debt_skip_notes":
        return await _save_debt(update, context, "")
    if data == "debt_list_owed":
        await show_debt_list(query, update.effective_user.id, "owed_to_me")
        return ConversationHandler.END
    if data == "debt_list_owe":
        await show_debt_list(query, update.effective_user.id, "i_owe")
        return ConversationHandler.END
    if data.startswith("debt_view_"):
        await show_debt_detail(query, update.effective_user.id, int(data.replace("debt_view_", "")))
        return ConversationHandler.END
    if data.startswith("debt_pay_"):
        debt_id = int(data.replace("debt_pay_", ""))
        if not get_debt_by_id(debt_id, update.effective_user.id):
            await query.edit_message_text("الدين غير موجود.")
            return ConversationHandler.END
        context.user_data["debt_payment_id"] = debt_id
        await query.edit_message_text("اكتب قيمة الدفعة:")
        return DEBT_PAYMENT_AMOUNT
    if data.startswith("debt_delete_"):
        debt_id = int(data.replace("debt_delete_", ""))
        delete_debt(debt_id, update.effective_user.id)
        await query.edit_message_text("تم حذف الدين وسجل دفعاته.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("إدارة الديون", callback_data="debts_menu")]]))
        return ConversationHandler.END
    if data.startswith("debt_edit_"):
        debt_id = int(data.replace("debt_edit_", ""))
        debt = get_debt_by_id(debt_id, update.effective_user.id)
        if not debt:
            await query.edit_message_text("الدين غير موجود.")
            return ConversationHandler.END
        context.user_data["debt_edit_id"] = debt_id
        kb = [[InlineKeyboardButton("اسم الشخص", callback_data="debt_edit_field_person")],
              [InlineKeyboardButton("المبلغ", callback_data="debt_edit_field_amount")],
              [InlineKeyboardButton("العملة", callback_data="debt_edit_currency")],
              [InlineKeyboardButton("نوع الدين", callback_data="debt_edit_direction")],
              [InlineKeyboardButton("التاريخ", callback_data="debt_edit_field_debt_date")],
              [InlineKeyboardButton("الملاحظات", callback_data="debt_edit_field_notes")],
              [InlineKeyboardButton("رجوع", callback_data="debt_view_" + str(debt_id))]]
        await query.edit_message_text("اختر الحقل:", reply_markup=InlineKeyboardMarkup(kb))
        return DEBT_EDIT_FIELD
    if data.startswith("debt_edit_field_"):
        field = data.replace("debt_edit_field_", "")
        if field not in ("person", "amount", "debt_date", "notes"):
            return DEBT_EDIT_FIELD
        context.user_data["debt_edit_field"] = field
        prompts = {"person": "اكتب الاسم الجديد:", "amount": "اكتب المبلغ الأصلي الجديد:",
                   "debt_date": "اكتب التاريخ YYYY-MM-DD:", "notes": "اكتب الملاحظات الجديدة:"}
        await query.edit_message_text(prompts[field])
        return DEBT_EDIT_VALUE
    if data == "debt_edit_currency":
        kb = [[InlineKeyboardButton("JOD", callback_data="debt_edit_set_currency_JOD"),
               InlineKeyboardButton("USD", callback_data="debt_edit_set_currency_USD")]]
        await query.edit_message_text("اختر العملة (لا يمكن تغييرها بعد وجود دفعات):", reply_markup=InlineKeyboardMarkup(kb))
        return DEBT_EDIT_FIELD
    if data.startswith("debt_edit_set_currency_"):
        try:
            update_debt(context.user_data["debt_edit_id"], update.effective_user.id,
                        "currency", data.rsplit("_", 1)[1])
        except Exception as e:
            await query.answer(str(e)[:180], show_alert=True)
            return DEBT_EDIT_FIELD
        await query.edit_message_text("تم تعديل العملة.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("عرض الدين", callback_data="debt_view_" + str(context.user_data["debt_edit_id"]))]]))
        return ConversationHandler.END
    if data == "debt_edit_direction":
        kb = [[InlineKeyboardButton("لي عند شخص", callback_data="debt_edit_set_direction_owed_to_me")],
              [InlineKeyboardButton("عليّ لشخص", callback_data="debt_edit_set_direction_i_owe")]]
        await query.edit_message_text("اختر نوع الدين:", reply_markup=InlineKeyboardMarkup(kb))
        return DEBT_EDIT_FIELD
    if data.startswith("debt_edit_set_direction_"):
        update_debt(context.user_data["debt_edit_id"], update.effective_user.id,
                    "direction", data.replace("debt_edit_set_direction_", ""))
        await query.edit_message_text("تم تعديل نوع الدين.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("عرض الدين", callback_data="debt_view_" + str(context.user_data["debt_edit_id"]))]]))
        return ConversationHandler.END


    # ========================================================
    # العادات
    # ========================================================

    if data == "habit_commit":
        await habits_choose_time(update, context, "commit")
        return ConversationHandler.END

    if data == "habit_quit":
        await habits_choose_time(update, context, "quit")
        return ConversationHandler.END

    if data == "habit_list":
        await show_habits_list(
            query,
            update.effective_user.id
        )
        return ConversationHandler.END

    if data == "habit_now":
        context.user_data["habit_custom_dt"] = None

        await query.edit_message_text(
            "اكتب اسم العادة:"
        )

        return HABIT_NAME

    if data == "habit_custom":
        context.user_data.pop("habit_custom_dt", None)
        context.user_data.pop("habit_year", None)
        context.user_data.pop("habit_month", None)
        context.user_data.pop("habit_day", None)

        await query.edit_message_text(
            "اختر سنة بداية العادة:",
            reply_markup=habit_year_keyboard()
        )

        return HABIT_YEAR

    # اختيار السنة

    if data.startswith("hy_"):
        year = int(data.replace("hy_", ""))

        current_year = now_amman().year

        if year < current_year or year > current_year + 3:
            await query.answer(
                "السنة غير صالحة.",
                show_alert=True
            )
            return HABIT_YEAR

        context.user_data["habit_year"] = year

        await query.edit_message_text(
            "السنة المختارة: {}\n\nاختر الشهر:".format(year),
            reply_markup=habit_month_keyboard()
        )

        return HABIT_MONTH

    # اختيار الشهر

    if data.startswith("hm_"):
        month = int(data.replace("hm_", ""))

        if month < 1 or month > 12:
            await query.answer(
                "الشهر غير صالح.",
                show_alert=True
            )
            return HABIT_MONTH

        year = context.user_data.get("habit_year")

        if not year:
            await query.edit_message_text(
                "حدث خطأ. ابدأ من جديد."
            )
            return ConversationHandler.END

        context.user_data["habit_month"] = month

        month_name = MONTH_NAMES["{:02d}".format(month)]

        await query.edit_message_text(
            "السنة: {}\nالشهر: {}\n\nاختر اليوم:".format(
                year,
                month_name
            ),
            reply_markup=habit_day_keyboard(year, month)
        )

        return HABIT_DAY

    # اختيار اليوم

    if data.startswith("hd_"):
        day = int(data.replace("hd_", ""))

        year = context.user_data.get("habit_year")
        month = context.user_data.get("habit_month")

        if not year or not month:
            await query.edit_message_text(
                "حدث خطأ. ابدأ من جديد."
            )
            return ConversationHandler.END

        days_count = calendar.monthrange(year, month)[1]

        if day < 1 or day > days_count:
            await query.answer(
                "اليوم غير صالح.",
                show_alert=True
            )
            return HABIT_DAY

        context.user_data["habit_day"] = day

        await query.edit_message_text(
            "التاريخ المختار: {:04d}-{:02d}-{:02d}\n\n"
            "اختر الساعة:".format(year, month, day),
            reply_markup=habit_hour_keyboard()
        )

        return HABIT_HOUR

    # اختيار الساعة

    if data.startswith("hh_"):
        hour = int(data.replace("hh_", ""))

        if hour < 0 or hour > 23:
            await query.answer(
                "الساعة غير صالحة.",
                show_alert=True
            )
            return HABIT_HOUR

        year = context.user_data.get("habit_year")
        month = context.user_data.get("habit_month")
        day = context.user_data.get("habit_day")

        if year is None or month is None or day is None:
            await query.edit_message_text(
                "حدث خطأ. ابدأ من جديد."
            )
            return ConversationHandler.END

        selected_dt = datetime(
            year,
            month,
            day,
            hour,
            0,
            tzinfo=AMMAN_TZ
        )

        context.user_data["habit_custom_dt"] = (
            selected_dt.isoformat()
        )

        await query.edit_message_text(
            "تم اختيار وقت البداية:\n"
            "-------------------\n"
            "{}\n\n"
            "اكتب اسم العادة:".format(
                selected_dt.strftime("%Y-%m-%d %H:%M")
            )
        )

        return HABIT_NAME

    # الرجوع بين خطوات اختيار التاريخ

    if data == "habit_back_year":
        await query.edit_message_text(
            "اختر سنة بداية العادة:",
            reply_markup=habit_year_keyboard()
        )

        return HABIT_YEAR

    if data == "habit_back_month":
        await query.edit_message_text(
            "اختر الشهر:",
            reply_markup=habit_month_keyboard()
        )

        return HABIT_MONTH

    if data == "habit_back_day":
        year = context.user_data.get("habit_year")
        month = context.user_data.get("habit_month")

        if not year or not month:
            await query.edit_message_text(
                "حدث خطأ. ابدأ من جديد."
            )
            return ConversationHandler.END

        await query.edit_message_text(
            "اختر اليوم:",
            reply_markup=habit_day_keyboard(year, month)
        )

        return HABIT_DAY

    if data.startswith("habit_v_"):
        h_id = int(data.replace("habit_v_", ""))

        await show_habit_detail(
            query,
            update.effective_user.id,
            h_id
        )

        return ConversationHandler.END

    if data.startswith("habit_r_"):
        h_id = int(data.replace("habit_r_", ""))

        reset_habit_start(
            h_id,
            update.effective_user.id,
            now_amman().isoformat()
        )

        await show_habit_detail(
            query,
            update.effective_user.id,
            h_id
        )

        return ConversationHandler.END

    if data.startswith("habit_d_"):
        h_id = int(data.replace("habit_d_", ""))

        delete_habit(
            h_id,
            update.effective_user.id
        )

        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="habit_list"
            )
        ]]

        await query.edit_message_text(
            "تم حذف العادة.",
            reply_markup=InlineKeyboardMarkup(kb)
        )

        return ConversationHandler.END

    # ========================================================
    # الصلاة
    # ========================================================

    if data.startswith("pr_"):
        prayer = data.replace("pr_", "")
        user_id = update.effective_user.id

        today = now_amman().strftime("%Y-%m-%d")
        done = get_today_prayers(user_id)

        if prayer in done:
            unlog_prayer(user_id, today, prayer)
        else:
            log_prayer(user_id, today, prayer)

        await prayer_menu(update, context)

        return ConversationHandler.END

    if data == "plog_months":
        await show_prayer_months(
            query,
            update.effective_user.id
        )
        return ConversationHandler.END

    if data.startswith("pmonth_"):
        month = data.replace("pmonth_", "")

        await show_prayer_days(
            query,
            update.effective_user.id,
            month
        )

        return ConversationHandler.END

    if data.startswith("pday_"):
        date = data.replace("pday_", "")

        await show_prayer_day(
            query,
            update.effective_user.id,
            date
        )

        return ConversationHandler.END

    # ========================================================
    # الملاحظات
    # ========================================================

    if data == "note_add":
        await query.edit_message_text(
            "اكتب الملاحظة:"
        )
        return ENTER_NOTE

    if data == "note_list":
        await show_notes_list(
            query,
            update.effective_user.id
        )
        return ConversationHandler.END

    if data.startswith("note_v_"):
        note_id = int(data.replace("note_v_", ""))

        await show_note_detail(
            query,
            update.effective_user.id,
            note_id
        )

        return ConversationHandler.END

    if data.startswith("note_d_"):
        note_id = int(data.replace("note_d_", ""))

        delete_note(
            note_id,
            update.effective_user.id
        )

        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="note_list"
            )
        ]]

        await query.edit_message_text(
            "تم حذف الملاحظة.",
            reply_markup=InlineKeyboardMarkup(kb)
        )

        return ConversationHandler.END

    # ========================================================
    # الوزن والطول
    # ========================================================

    if data == "body_add":
        await query.edit_message_text(
            "اكتب وزنك (كغ):"
        )
        return BODY_WEIGHT

    if data == "body_list":
        await body_list(
            query,
            update.effective_user.id
        )
        return ConversationHandler.END

    # ========================================================
    # سجل العمليات
    # ========================================================

    if data == "log_months":
        await show_months(
            query,
            update.effective_user.id
        )
        return ConversationHandler.END

    if data.startswith("month_"):
        month = data.replace("month_", "")

        await show_days(
            query,
            update.effective_user.id,
            month
        )

        return ConversationHandler.END

    if data.startswith("mfile_"):
        month = data.replace("mfile_", "")

        await send_csv(
            update,
            context,
            month,
            "transactions_" + month + ".csv"
        )

        return ConversationHandler.END

    if data.startswith("day_"):
        date = data.replace("day_", "")

        await show_day_transactions(
            query,
            update.effective_user.id,
            date
        )

        return ConversationHandler.END

    if data.startswith("dfile_"):
        date = data.replace("dfile_", "")

        await send_csv(
            update,
            context,
            date,
            "transactions_" + date + ".csv"
        )

        return ConversationHandler.END

    if data.startswith("tx_"):
        tx_id = int(data.replace("tx_", ""))

        await show_transaction_detail(
            query,
            update.effective_user.id,
            tx_id
        )

        return ConversationHandler.END

    if data.startswith("del_"):
        tx_id = int(data.replace("del_", ""))

        tx = get_transaction_by_id(
            tx_id,
            update.effective_user.id
        )

        if tx:
            delete_transaction(
                tx_id,
                update.effective_user.id
            )

            date = str(tx[4])[:10]

            kb = [[
                InlineKeyboardButton(
                    "رجوع",
                    callback_data="day_" + date
                )
            ]]

            await query.edit_message_text(
                "تم حذف العملية بنجاح.",
                reply_markup=InlineKeyboardMarkup(kb)
            )

        else:
            kb = [[
                InlineKeyboardButton(
                    "رجوع",
                    callback_data="menu_money"
                )
            ]]

            await query.edit_message_text(
                "العملية غير موجودة.",
                reply_markup=InlineKeyboardMarkup(kb)
            )

        return ConversationHandler.END

    if data.startswith("edit_"):
        tx_id = int(data.replace("edit_", ""))

        context.user_data["edit_tx_id"] = tx_id

        tx = get_transaction_by_id(
            tx_id,
            update.effective_user.id
        )

        if not tx:
            await query.edit_message_text(
                "العملية غير موجودة."
            )
            return ConversationHandler.END

        _, cat, amt, reason, ts = tx

        kb = [
            [InlineKeyboardButton("الفئة", callback_data="edfield_category")],
            [InlineKeyboardButton("المبلغ", callback_data="edfield_amount")],
            [InlineKeyboardButton("الوصف", callback_data="edfield_reason")],
            [InlineKeyboardButton("رجوع", callback_data="tx_" + str(tx_id))],
        ]

        await query.edit_message_text(
            "تعديل العملية\n"
            "-------------------\n"
            "الفئة الحالية: " + CAT_NAMES.get(cat, cat) + "\n"
            "المبلغ الحالي: {:.2f}\n".format(float(amt)) +
            "الوصف الحالي: " + str(reason) + "\n\n"
            "اختر الحقل للتعديل:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

        return EDIT_CHOOSE_FIELD

    if data.startswith("cat_"):
        category = data.replace("cat_", "")

        context.user_data["category"] = category
        context.user_data["user_id"] = update.effective_user.id

        kb = [
            [
                InlineKeyboardButton("إضافة", callback_data="act_add"),
                InlineKeyboardButton("خصم", callback_data="act_sub"),
            ],
            [InlineKeyboardButton("رجوع", callback_data="menu_money")],
        ]

        await query.edit_message_text(
            "الفئة: " + CAT_NAMES[category] + "\n\nاختر العملية:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

        return CHOOSE_ACTION

    if data.startswith("act_"):
        action = data.replace("act_", "")

        context.user_data["action"] = action

        category = context.user_data["category"]

        act_name = "إضافة" if action == "add" else "خصم"

        await query.edit_message_text(
            act_name + " على " + CAT_NAMES[category] + "\n\nاكتب المبلغ:"
        )

        return ENTER_AMOUNT

    return ConversationHandler.END


# ============================================================
# إدخال العمليات المالية
# ============================================================

async def enter_amount(update, context):
    text = update.message.text.strip()

    try:
        amount = float(text)

        if amount <= 0:
            raise ValueError

    except ValueError:
        await update.message.reply_text(
            "اكتب رقم صحيح موجب:"
        )
        return ENTER_AMOUNT

    context.user_data["amount"] = amount

    kb = [[
        InlineKeyboardButton(
            "سكيب",
            callback_data="skip_reason"
        )
    ]]

    await update.message.reply_text(
        "اكتب وصف العملية أو اضغط سكيب:",
        reply_markup=InlineKeyboardMarkup(kb)
    )

    return ENTER_REASON


async def enter_reason(update, context):
    if update.callback_query:
        await update.callback_query.answer()
        reason = "بدون وصف"
    else:
        reason = update.message.text.strip() or "بدون وصف"

    await save_and_show(
        update,
        context,
        reason
    )

    return ConversationHandler.END


async def save_and_show(update, context, reason):
    user_id = context.user_data["user_id"]
    category = context.user_data["category"]
    action = context.user_data["action"]
    amount = context.user_data["amount"]

    signed = amount if action == "add" else -amount

    add_transaction(
        user_id,
        category,
        signed,
        reason
    )

    act_name = "إضافة" if action == "add" else "خصم"

    msg = (
        "تم التسجيل\n"
        "-------------------\n"
        "الفئة: " + CAT_NAMES[category] + "\n"
        "العملية: " + act_name + "\n"
        "المبلغ: {:.2f}\n".format(amount) +
        "الوصف: " + str(reason) + "\n"
        "-------------------\n"
        + format_balances(user_id)
    )

    kb = [[
        InlineKeyboardButton(
            "رجوع",
            callback_data="menu_money"
        )
    ]]

    reply_markup = InlineKeyboardMarkup(kb)

    if update.callback_query:
        await update.callback_query.edit_message_text(
            msg,
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            msg,
            reply_markup=reply_markup
        )


# ============================================================
# إدخال الملاحظات
# ============================================================

async def enter_note(update, context):
    text = update.message.text.strip()

    if not text:
        await update.message.reply_text(
            "اكتب نص الملاحظة:"
        )
        return ENTER_NOTE

    add_note(
        update.effective_user.id,
        text
    )

    kb = [[
        InlineKeyboardButton(
            "رجوع",
            callback_data="menu_notes"
        )
    ]]

    await update.message.reply_text(
        "تم حفظ الملاحظة.",
        reply_markup=InlineKeyboardMarkup(kb)
    )

    return ConversationHandler.END


# ============================================================
# إدخال الوزن والطول
# ============================================================

async def body_weight(update, context):
    text = update.message.text.strip()

    try:
        w = float(text)

        if w <= 0 or w > 500:
            raise ValueError

    except ValueError:
        await update.message.reply_text(
            "اكتب وزن صحيح بالكيلوغرام:"
        )
        return BODY_WEIGHT

    context.user_data["body_weight"] = w

    await update.message.reply_text(
        "اكتب طولك بالسنتيمتر:"
    )

    return BODY_HEIGHT


async def body_height(update, context):
    text = update.message.text.strip()

    try:
        h = float(text)

        if h <= 0 or h > 300:
            raise ValueError

    except ValueError:
        await update.message.reply_text(
            "اكتب طول صحيح بالسنتيمتر:"
        )
        return BODY_HEIGHT

    w = context.user_data.get("body_weight")

    if w is None:
        await update.message.reply_text(
            "حدث خطأ، اكتب /start"
        )
        return ConversationHandler.END

    add_body_log(
        update.effective_user.id,
        w,
        h
    )

    kb = [[
        InlineKeyboardButton(
            "رجوع",
            callback_data="menu_body"
        )
    ]]

    await update.message.reply_text(
        "تم الحفظ\n"
        "-------------------\n"
        "الوزن: {:.2f} kg\n".format(w) +
        "الطول: {:.2f} cm".format(h),
        reply_markup=InlineKeyboardMarkup(kb)
    )

    return ConversationHandler.END


# ============================================================
# حفظ العادة
# ============================================================

async def habit_name(update, context):
    text = update.message.text.strip()

    if not text:
        await update.message.reply_text(
            "اكتب اسم العادة:"
        )
        return HABIT_NAME

    kind = context.user_data.get(
        "habit_kind",
        "commit"
    )

    custom_dt = context.user_data.get(
        "habit_custom_dt"
    )

    if custom_dt:
        start_ts = custom_dt
    else:
        start_ts = now_amman().isoformat()

    # منع اختيار تاريخ مستقبلي بالخطأ لا يمنع التسجيل،
    # وسيظهر في تفاصيل العادة أنها لم تبدأ بعد.

    add_habit(
        update.effective_user.id,
        kind,
        text,
        start_ts
    )

    context.user_data.pop("habit_custom_dt", None)
    context.user_data.pop("habit_year", None)
    context.user_data.pop("habit_month", None)
    context.user_data.pop("habit_day", None)

    kb = [[
        InlineKeyboardButton(
            "رجوع",
            callback_data="menu_habits"
        )
    ]]

    await update.message.reply_text(
        "تم حفظ " + KIND_AR.get(kind, kind) + ": " + text,
        reply_markup=InlineKeyboardMarkup(kb)
    )

    return ConversationHandler.END


# ============================================================
# عرض الملاحظات
# ============================================================

async def show_notes_list(query, user_id):
    notes = get_notes(user_id, limit=20)

    if not notes:
        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="menu_notes"
            )
        ]]

        await query.edit_message_text(
            "لا يوجد ملاحظات.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    buttons = []

    for note in notes:
        note_id, text, ts = note

        short = str(text)[:30]

        if len(str(text)) > 30:
            short += "..."

        label = short + " | " + str(ts)[:10]

        buttons.append([
            InlineKeyboardButton(
                label,
                callback_data="note_v_" + str(note_id)
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="menu_notes"
        )
    ])

    await query.edit_message_text(
        "ملاحظاتك (آخر 20):",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def show_note_detail(query, user_id, note_id):
    note = get_note_by_id(note_id, user_id)

    if not note:
        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="menu_notes"
            )
        ]]

        await query.edit_message_text(
            "الملاحظة غير موجودة.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    _, text, ts = note

    msg = (
        "الملاحظة\n"
        "-------------------\n"
        + str(text) + "\n"
        "-------------------\n"
        "التاريخ: " + str(ts)[:10] + "\n"
        "الوقت: " + str(ts)[11:16]
    )

    kb = [
        [InlineKeyboardButton("حذف", callback_data="note_d_" + str(note_id))],
        [InlineKeyboardButton("رجوع", callback_data="note_list")],
    ]

    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(kb)
    )


# ============================================================
# سجل العمليات المالية
# ============================================================

async def show_months(query, user_id):
    months = get_months(user_id)

    if not months:
        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="menu_money"
            )
        ]]

        await query.edit_message_text(
            "لا يوجد عمليات مسجلة.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    buttons = []

    for m in months:
        year, mon = m.split("-")

        label = MONTH_NAMES.get(mon, mon) + " " + year

        buttons.append([
            InlineKeyboardButton(
                label,
                callback_data="month_" + m
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="menu_money"
        )
    ])

    await query.edit_message_text(
        "السجل\nاختر الشهر:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def show_days(query, user_id, month):
    days = get_days(user_id, month)

    year, mon = month.split("-")
    label = MONTH_NAMES.get(mon, mon) + " " + year

    if not days:
        kb = [
            [InlineKeyboardButton("تحميل ملف الشهر", callback_data="mfile_" + month)],
            [InlineKeyboardButton("رجوع", callback_data="log_months")],
        ]

        await query.edit_message_text(
            label + "\n\nلا يوجد عمليات.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    buttons = []

    for d in days:
        buttons.append([
            InlineKeyboardButton(
                "يوم " + d.split("-")[2],
                callback_data="day_" + d
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "تحميل ملف الشهر",
            callback_data="mfile_" + month
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="log_months"
        )
    ])

    await query.edit_message_text(
        label + "\nاختر اليوم:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def show_day_transactions(query, user_id, date):
    txs = get_transactions(
        user_id,
        date_filter=date
    )

    if not txs:
        kb = [
            [InlineKeyboardButton("تحميل ملف اليوم", callback_data="dfile_" + date)],
            [InlineKeyboardButton("رجوع", callback_data="month_" + date[:7])],
        ]

        await query.edit_message_text(
            date + "\n\nلا يوجد عمليات.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    tc = 0
    tb = 0
    td = 0

    for tx in txs:
        _, cat, amt, _, _ = tx

        amt = float(amt)

        if cat == "cash":
            tc += amt
        elif cat == "bank":
            tb += amt
        elif cat == "dollar":
            td += amt

    usd_jod = td * RATE
    total_jod = tc + tb + usd_jod
    total_usd = total_jod / RATE if RATE else 0

    header = (
        "سجل يوم " + date + "\n"
        "-------------------\n"
        "كاش: {:.2f}\n".format(tc) +
        "بنك: {:.2f}\n".format(tb) +
        "دولار: {:.2f} ({:.2f} دينار)\n".format(td, usd_jod) +
        "-------------------\n"
        "الإجمالي بالدينار: {:.2f}\n".format(total_jod) +
        "الإجمالي بالدولار: {:.2f}\n".format(total_usd) +
        "-------------------\n"
        "اضغط على العملية لتعديلها أو حذفها:"
    )

    buttons = []

    for tx in txs:
        tx_id, cat, amt, reason, ts = tx

        amt = float(amt)

        sign = "+" if amt > 0 else "-"

        rs = str(reason)[:15]

        if len(str(reason)) > 15:
            rs += "..."

        label = (
            sign + " " +
            CAT_NAMES.get(cat, cat) +
            " | {:.2f}".format(abs(amt)) +
            " | " + rs +
            " | " + str(ts)[11:16]
        )

        buttons.append([
            InlineKeyboardButton(
                label,
                callback_data="tx_" + str(tx_id)
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "تحميل ملف اليوم",
            callback_data="dfile_" + date
        )
    ])

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="month_" + date[:7]
        )
    ])

    await query.edit_message_text(
        header,
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def show_transaction_detail(query, user_id, tx_id):
    tx = get_transaction_by_id(tx_id, user_id)

    if not tx:
        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="menu_money"
            )
        ]]

        await query.edit_message_text(
            "العملية غير موجودة.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    _, cat, amt, reason, ts = tx

    amt = float(amt)
    ts = str(ts)

    sign = "+" if amt > 0 else "-"

    msg = (
        "تفاصيل العملية\n"
        "-------------------\n"
        "الفئة: " + CAT_NAMES.get(cat, cat) + "\n"
        "المبلغ: " + sign + "{:.2f}\n".format(abs(amt)) +
        "الوصف: " + str(reason) + "\n"
        "التاريخ: " + ts[:10] + "\n"
        "الوقت: " + ts[11:16] + "\n"
        "-------------------\n"
        "اختر إجراء:"
    )

    kb = [
        [InlineKeyboardButton("تعديل", callback_data="edit_" + str(tx_id))],
        [InlineKeyboardButton("حذف", callback_data="del_" + str(tx_id))],
        [InlineKeyboardButton("رجوع", callback_data="day_" + ts[:10])],
    ]

    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(kb)
    )


# ============================================================
# سجل الصلاة
# ============================================================

async def show_prayer_months(query, user_id):
    months = get_prayer_months(user_id)

    if not months:
        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="menu_prayer"
            )
        ]]

        await query.edit_message_text(
            "لا يوجد سجل صلاة.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    buttons = []

    for m in months:
        year, mon = m.split("-")

        label = MONTH_NAMES.get(mon, mon) + " " + year

        buttons.append([
            InlineKeyboardButton(
                label,
                callback_data="pmonth_" + m
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="menu_prayer"
        )
    ])

    await query.edit_message_text(
        "سجل الصلاة\nاختر الشهر:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def show_prayer_days(query, user_id, month):
    days = get_prayer_days(user_id, month)

    year, mon = month.split("-")
    label = MONTH_NAMES.get(mon, mon) + " " + year

    if not days:
        kb = [[
            InlineKeyboardButton(
                "رجوع",
                callback_data="plog_months"
            )
        ]]

        await query.edit_message_text(
            label + "\n\nلا يوجد صلوات.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    buttons = []

    for d in days:
        buttons.append([
            InlineKeyboardButton(
                "يوم " + d.split("-")[2],
                callback_data="pday_" + d
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "رجوع",
            callback_data="plog_months"
        )
    ])

    await query.edit_message_text(
        label + "\nاختر اليوم:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def show_prayer_day(query, user_id, date):
    done = get_prayers_by_date(user_id, date)

    msg = (
        "سجل يوم " + date + "\n"
        "-------------------\n"
    )

    for p in PRAYERS:
        status = "تم" if p in done else "لم"
        msg += PRAYERS_AR[p] + ": " + status + "\n"

    kb = [[
        InlineKeyboardButton(
            "رجوع",
            callback_data="pmonth_" + date[:7]
        )
    ]]

    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(kb)
    )


# ============================================================
# تعديل العمليات المالية
# ============================================================

async def edit_choose_field(update, context):
    query = update.callback_query
    await query.answer()

    data = query.data

    tx_id = context.user_data.get("edit_tx_id")

    if not tx_id:
        await query.edit_message_text("حدث خطأ.")
        return ConversationHandler.END

    user_id = update.effective_user.id

    tx = get_transaction_by_id(tx_id, user_id)

    if not tx:
        await query.edit_message_text(
            "العملية غير موجودة."
        )
        return ConversationHandler.END

    if data == "tx_" + str(tx_id):
        await show_transaction_detail(
            query,
            user_id,
            tx_id
        )
        return ConversationHandler.END

    if data == "edfield_category":
        _, cat, _, _, _ = tx

        cash_label = "كاش"
        bank_label = "بنك"
        dollar_label = "دولار"

        if cat == "cash":
            cash_label += " (الحالي)"

        if cat == "bank":
            bank_label += " (الحالي)"

        if cat == "dollar":
            dollar_label += " (الحالي)"

        kb = [
            [
                InlineKeyboardButton(cash_label, callback_data="edcat_cash"),
                InlineKeyboardButton(bank_label, callback_data="edcat_bank"),
                InlineKeyboardButton(dollar_label, callback_data="edcat_dollar"),
            ],
            [InlineKeyboardButton("رجوع", callback_data="edit_" + str(tx_id))],
        ]

        await query.edit_message_text(
            "اختر الفئة الجديدة:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

        return EDIT_CHOOSE_FIELD

    if data == "edfield_amount":
        context.user_data["edit_field"] = "amount"

        await query.edit_message_text(
            "اكتب المبلغ الجديد (رقم فقط):"
        )

        return EDIT_ENTER_VALUE

    if data == "edfield_reason":
        context.user_data["edit_field"] = "reason"

        await query.edit_message_text(
            "اكتب الوصف الجديد:"
        )

        return EDIT_ENTER_VALUE

    if data.startswith("edcat_"):
        new_cat = data.replace("edcat_", "")

        update_transaction(
            tx_id,
            user_id,
            category=new_cat
        )

        kb = [[
            InlineKeyboardButton(
                "رجوع للعملية",
                callback_data="tx_" + str(tx_id)
            )
        ]]

        await query.edit_message_text(
            "تم تعديل الفئة إلى: " + CAT_NAMES[new_cat],
            reply_markup=InlineKeyboardMarkup(kb)
        )

        return ConversationHandler.END

    return ConversationHandler.END


async def edit_enter_value(update, context):
    user_id = update.effective_user.id

    tx_id = context.user_data.get("edit_tx_id")
    field = context.user_data.get("edit_field")

    if not tx_id or not field:
        await update.message.reply_text(
            "حدث خطأ، اكتب /start"
        )
        return ConversationHandler.END

    tx = get_transaction_by_id(tx_id, user_id)

    if not tx:
        await update.message.reply_text(
            "العملية غير موجودة."
        )
        return ConversationHandler.END

    text = update.message.text.strip()

    if field == "amount":
        try:
            new_amount = float(text)

            if new_amount <= 0:
                raise ValueError

        except ValueError:
            await update.message.reply_text(
                "اكتب رقم صحيح موجب:"
            )
            return EDIT_ENTER_VALUE

        old_amt = float(tx[2])

        signed = new_amount if old_amt >= 0 else -new_amount

        update_transaction(
            tx_id,
            user_id,
            amount=signed
        )

        confirm = "تم تعديل المبلغ إلى: {:.2f}".format(
            new_amount
        )

    elif field == "reason":
        new_reason = text or "بدون وصف"

        update_transaction(
            tx_id,
            user_id,
            reason=new_reason
        )

        confirm = "تم تعديل الوصف إلى: " + new_reason

    else:
        confirm = "حدث خطأ."

    kb = [[
        InlineKeyboardButton(
            "رجوع للعملية",
            callback_data="tx_" + str(tx_id)
        )
    ]]

    await update.message.reply_text(
        confirm,
        reply_markup=InlineKeyboardMarkup(kb)
    )

    return ConversationHandler.END


# ============================================================
# إلغاء المحادثة
# ============================================================

async def cancel(update, context):
    await update.message.reply_text(
        "تم الإلغاء. اكتب /start للبدء."
    )

    return ConversationHandler.END


# ============================================================
# تشغيل البوت
# ============================================================

def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError(
            "TELEGRAM_TOKEN غير موجود في متغيرات البيئة."
        )

    if not TURSO_TOKEN:
        raise RuntimeError(
            "TURSO_TOKEN غير موجود في متغيرات البيئة."
        )

    print(">>> STEP 1: init_db", flush=True)

    init_db()

    print(">>> STEP 2: building app", flush=True)

    app = Application.builder().token(
        TELEGRAM_TOKEN
    ).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),

            CallbackQueryHandler(
                money_menu,
                pattern="^menu_money$"
            ),

            CallbackQueryHandler(
                notes_menu,
                pattern="^menu_notes$"
            ),

            CallbackQueryHandler(
                prayer_menu,
                pattern="^menu_prayer$"
            ),

            CallbackQueryHandler(
                habits_menu,
                pattern="^menu_habits$"
            ),

            CallbackQueryHandler(
                body_menu,
                pattern="^menu_body$"
            ),

            CallbackQueryHandler(category_selected),

            CallbackQueryHandler(
                edit_choose_field,
                pattern="^(edit_|edfield_|edcat_|tx_)"
            ),
        ],

        states={
            CHOOSE_ACTION: [
                CallbackQueryHandler(category_selected)
            ],

            ENTER_AMOUNT: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    enter_amount
                )
            ],

            ENTER_REASON: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    enter_reason
                ),
                CallbackQueryHandler(
                    enter_reason,
                    pattern="^skip_reason$"
                ),
            ],

            EDIT_CHOOSE_FIELD: [
                CallbackQueryHandler(edit_choose_field)
            ],

            EDIT_ENTER_VALUE: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    edit_enter_value
                )
            ],

            ENTER_NOTE: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    enter_note
                )
            ],

            BODY_WEIGHT: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    body_weight
                )
            ],

            BODY_HEIGHT: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    body_height
                )
            ],

            HABIT_NAME: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    habit_name
                )
            ],

            HABIT_CUSTOM_TIME: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    habit_name
                )
            ],

            HABIT_YEAR: [
                CallbackQueryHandler(category_selected)
            ],

            HABIT_MONTH: [
                CallbackQueryHandler(category_selected)
            ],

            HABIT_DAY: [
                CallbackQueryHandler(category_selected)
            ],

            HABIT_HOUR: [
                CallbackQueryHandler(category_selected)
            ],

            DEBT_TYPE: [CallbackQueryHandler(category_selected)],
            DEBT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_name_input)],
            DEBT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_amount_input)],
            DEBT_CURRENCY: [CallbackQueryHandler(category_selected)],
            DEBT_DATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debt_date_input),
                CallbackQueryHandler(category_selected)
            ],
            DEBT_NOTES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debt_notes_input),
                CallbackQueryHandler(category_selected)
            ],
            DEBT_PAYMENT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_payment_input)],
            DEBT_EDIT_FIELD: [CallbackQueryHandler(category_selected)],
            DEBT_EDIT_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, debt_edit_input)],

        },

        fallbacks=[
            CommandHandler("cancel", cancel)
        ],

        per_user=True,
        per_chat=True,
    )

    app.add_handler(conv)

    print(">>> STEP 3: starting polling", flush=True)

    app.run_polling(
        drop_pending_updates=True
    )


if __name__ == "__main__":
    try:
        main()

    except Exception as e:
        print(
            ">>> FATAL ERROR:",
            e,
            flush=True
        )

        traceback.print_exc()

        sys.exit(1)