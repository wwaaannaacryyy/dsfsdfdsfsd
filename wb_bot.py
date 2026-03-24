import logging
import sqlite3
import json
import uuid
import ssl
from datetime import datetime
from typing import Optional

import certifi
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import SSLError, RequestException
from urllib3.util.retry import Retry

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, BotCommand
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── curl_cffi — главное исправление SSL ──
try:
    from curl_cffi import requests as cf_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    print(
        "⚠️  curl_cffi не установлен! Авторизация WB будет падать с SSL-ошибкой.\n"
        "    Установите:  pip install curl_cffi"
    )

# ─────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────
TELEGRAM_BOT_TOKEN = "8277535481:AAFA9ihu7rSaszbos8G65sAd2rBG1Pp1dd0"
WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjYwMzAydjEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjEsImVudCI6MSwiZXhwIjoxNzkwMDQ5NDkxLCJpZCI6IjAxOWQxYjZiLTAzN2QtN2RlYS1hYmZkLWZkZGUyZjRjY2Y1NCIsImlpZCI6MjU1NjAzMjMsIm9pZCI6NTQwMDg5LCJzIjoxMDQwLCJzaWQiOiI4ZDcyOGI2Yi04MGZiLTRhYzctOTdjZS05YWVmNmNhZjg0YmUiLCJ0IjpmYWxzZSwidWlkIjoyNTU2MDMyM30.Ib_moeMpzG0AWtbEIQXjv1f8be_xnbq8m_Ar8TESOlhg0vn7AGTA6YdjYdW76P-yjRDCsAD2dKYDGG81hopBOQ"

POLLING_INTERVAL_NORMAL     = 60
POLLING_INTERVAL_AGGRESSIVE = 5

# Прокси (если бот на VPS с датацентровым IP, WB может блокировать).
# Раскомментируйте и укажите резидентский прокси:
# PROXY_URL = "socks5://user:pass@proxy-host:port"
# PROXY_URL = "http://user:pass@proxy-host:port"
PROXY_URL = None  # None = без прокси

# ─────────────────────────────────────────
# WB API URLs
# ─────────────────────────────────────────
WB_COMMON_API   = "https://common-api.wildberries.ru"
WB_SUPPLIES_API = "https://supplies-api.wildberries.ru"

COEF_URL       = f"{WB_COMMON_API}/api/tariffs/v1/acceptance/coefficients"
WAREHOUSES_URL = f"{WB_SUPPLIES_API}/api/v1/warehouses"
OPTIONS_URL    = f"{WB_SUPPLIES_API}/api/v1/acceptance/options"
SUPPLIES_URL   = f"{WB_SUPPLIES_API}/api/v1/supplies"

# ─────────────────────────────────────────
# STATES
# ─────────────────────────────────────────
STATE_API_KEY        = "api_key"
STATE_BARCODES       = "task_barcodes"
STATE_WAREHOUSE      = "task_warehouse"
STATE_ADD_BARCODE    = "add_barcode_to"
STATE_PHONE          = "wb_phone"
STATE_SMS_CODE       = "wb_sms_code"
STATE_SUPPLY_ID      = "wb_supply_id"

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    filename="wb_bot.log"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────
def init_db():
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id INTEGER PRIMARY KEY,
            wb_api_key TEXT,
            aggressive_mode INTEGER DEFAULT 0,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS watch_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER,
            task_type TEXT,
            barcodes TEXT,
            target_warehouse_id INTEGER,
            target_warehouse_name TEXT,
            source_warehouse_id INTEGER,
            source_warehouse_name TEXT,
            max_coefficient INTEGER DEFAULT 1,
            quantity INTEGER DEFAULT 1,
            active INTEGER DEFAULT 1,
            created_at TEXT,
            last_triggered TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS booking_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER,
            task_id INTEGER,
            warehouse_name TEXT,
            coefficient REAL,
            date TEXT,
            status TEXT,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS wb_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER NOT NULL,
            phone TEXT NOT NULL,
            device_id TEXT NOT NULL,
            access_token TEXT,
            cookies TEXT,
            seller_token TEXT,
            is_active INTEGER DEFAULT 1,
            last_check TEXT,
            created_at TEXT,
            UNIQUE(tg_id, phone)
        )
    """)
    try:
        c.execute("ALTER TABLE watch_tasks ADD COLUMN supply_id TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE watch_tasks ADD COLUMN wb_account_phone TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE watch_tasks ADD COLUMN auto_book INTEGER DEFAULT 0")
    except Exception:
        pass
    conn.commit()
    conn.close()


def get_user(tg_id: int) -> Optional[dict]:
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("SELECT tg_id, wb_api_key, aggressive_mode FROM users WHERE tg_id=?", (tg_id,))
    row = c.fetchone()
    conn.close()
    return {"tg_id": row[0], "wb_api_key": row[1], "aggressive_mode": row[2]} if row else None


def save_user(tg_id: int, api_key: str):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO users (tg_id, wb_api_key, aggressive_mode, created_at)
        VALUES (?, ?, COALESCE((SELECT aggressive_mode FROM users WHERE tg_id=?), 0), ?)
    """, (tg_id, api_key, tg_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def get_tasks(tg_id: int, active_only=True) -> list:
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    q = "SELECT * FROM watch_tasks WHERE tg_id=?"
    if active_only:
        q += " AND active=1"
    c.execute(q, [tg_id])
    rows = c.fetchall()
    conn.close()
    tasks = []
    for r in rows:
        tasks.append({
            "id": r[0], "tg_id": r[1], "task_type": r[2],
            "barcodes": json.loads(r[3]) if r[3] else [],
            "target_warehouse_id": r[4], "target_warehouse_name": r[5],
            "source_warehouse_id": r[6], "source_warehouse_name": r[7],
            "max_coefficient": r[8], "quantity": r[9], "active": r[10],
            "created_at": r[11], "last_triggered": r[12],
            "supply_id": r[13] if len(r) > 13 else None,
            "wb_account_phone": r[14] if len(r) > 14 else None,
            "auto_book": r[15] if len(r) > 15 else 0,
        })
    return tasks


def add_task(tg_id, task_type, barcodes, target_id, target_name,
             source_id=None, source_name=None, max_coef=1, quantity=1) -> int:
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO watch_tasks
            (tg_id, task_type, barcodes, target_warehouse_id, target_warehouse_name,
             source_warehouse_id, source_warehouse_name, max_coefficient, quantity, active, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,1,?)
    """, (tg_id, task_type, json.dumps(barcodes), target_id, target_name,
          source_id, source_name, max_coef, quantity, datetime.now().isoformat()))
    task_id = c.lastrowid
    conn.commit()
    conn.close()
    return task_id


def deactivate_task(task_id: int):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("UPDATE watch_tasks SET active=0 WHERE id=?", (task_id,))
    conn.commit()
    conn.close()


def add_barcode_to_task(task_id: int, tg_id: int, barcode: str) -> tuple:
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("SELECT barcodes FROM watch_tasks WHERE id=? AND tg_id=?", (task_id, tg_id))
    row = c.fetchone()
    if not row:
        conn.close()
        return False, 0
    barcodes = json.loads(row[0]) if row[0] else []
    if barcode not in barcodes:
        barcodes.append(barcode)
    c.execute("UPDATE watch_tasks SET barcodes=? WHERE id=?", (json.dumps(barcodes), task_id))
    conn.commit()
    conn.close()
    return True, len(barcodes)


def log_booking(tg_id, task_id, wh_name, coef, date, status):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO booking_log (tg_id, task_id, warehouse_name, coefficient, date, status, created_at)
        VALUES (?,?,?,?,?,?,?)
    """, (tg_id, task_id, wh_name, coef, date, status, datetime.now().isoformat()))
    conn.commit()
    conn.close()


# ─── WB ACCOUNTS DB ─────────────────────
def save_wb_account(tg_id, phone, device_id, access_token=None, cookies=None, seller_token=None):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO wb_accounts (tg_id, phone, device_id, access_token, cookies,
                                 seller_token, is_active, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(tg_id, phone) DO UPDATE SET
            access_token=excluded.access_token,
            cookies=excluded.cookies,
            seller_token=excluded.seller_token,
            is_active=1,
            last_check=excluded.created_at
    """, (tg_id, phone, device_id, access_token,
          json.dumps(cookies or {}), seller_token, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def get_wb_accounts(tg_id):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        SELECT id, phone, access_token, cookies, seller_token, is_active, last_check, device_id
        FROM wb_accounts WHERE tg_id=? ORDER BY created_at DESC
    """, (tg_id,))
    rows = c.fetchall()
    conn.close()
    return [
        {"id": r[0], "phone": r[1], "access_token": r[2],
         "cookies": json.loads(r[3]) if r[3] else {},
         "seller_token": r[4], "is_active": r[5], "last_check": r[6], "device_id": r[7]}
        for r in rows
    ]


def get_wb_account_by_phone(tg_id, phone):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        SELECT id, phone, access_token, cookies, seller_token, is_active, last_check, device_id
        FROM wb_accounts WHERE tg_id=? AND phone=?
    """, (tg_id, phone))
    r = c.fetchone()
    conn.close()
    if not r:
        return None
    return {"id": r[0], "phone": r[1], "access_token": r[2],
            "cookies": json.loads(r[3]) if r[3] else {},
            "seller_token": r[4], "is_active": r[5], "last_check": r[6], "device_id": r[7]}


def delete_wb_account(tg_id, phone):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("DELETE FROM wb_accounts WHERE tg_id=? AND phone=?", (tg_id, phone))
    conn.commit()
    conn.close()


def update_account_tokens(tg_id, phone, access_token=None, cookies=None, seller_token=None):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("""
        UPDATE wb_accounts SET access_token=?, cookies=?, seller_token=?, last_check=?
        WHERE tg_id=? AND phone=?
    """, (access_token, json.dumps(cookies or {}), seller_token,
          datetime.now().isoformat(), tg_id, phone))
    conn.commit()
    conn.close()


def mark_account_invalid(tg_id, phone):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("UPDATE wb_accounts SET is_active=0 WHERE tg_id=? AND phone=?", (tg_id, phone))
    conn.commit()
    conn.close()


# ─────────────────────────────────────────
# WB API CLIENT (для коэффициентов — API-ключ)
# ─────────────────────────────────────────
class WBClient:
    def __init__(self, api_key: str):
        self.headers = {"Authorization": api_key, "Content-Type": "application/json"}

    def get_warehouses(self) -> list:
        try:
            r = requests.get(WAREHOUSES_URL, headers=self.headers, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"get_warehouses: {e}")
            return []

    def get_coefficients(self, warehouse_ids=None) -> list:
        try:
            params = {}
            if warehouse_ids:
                params["warehouseIDs"] = ",".join(map(str, warehouse_ids))
            r = requests.get(COEF_URL, headers=self.headers, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"get_coefficients: {e}")
            return []

    def find_warehouse_by_name(self, name_part: str):
        for wh in self.get_warehouses():
            if name_part.lower() in wh.get("name", "").lower():
                return wh
        return None

    def book_supply_slot(self, supply_id, warehouse_id, slot_date, account=None):
        """Бронирует дату поставки. Использует curl_cffi если есть."""
        try:
            headers = {
                "Content-Type": "application/json",
                "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/124.0.0.0 Safari/537.36"),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "ru-RU,ru;q=0.9",
                "Origin": "https://seller.wildberries.ru",
                "Referer": "https://seller.wildberries.ru/",
            }
            cookies = {}
            if account:
                if account.get("access_token"):
                    headers["Authorization"] = account["access_token"]
                cookies = account.get("cookies", {})
            else:
                headers["Authorization"] = self.headers.get("Authorization", "")

            payload = {
                "supplyId": supply_id,
                "warehouseId": warehouse_id,
                "date": f"{slot_date}T00:00:00.000Z",
            }
            url = ("https://seller-supply.wildberries.ru/ns/sm-supply/"
                   "supply-manager/api/v1/supply/booking")
            proxies = {"https": PROXY_URL, "http": PROXY_URL} if PROXY_URL else None

            if HAS_CURL_CFFI:
                r = cf_requests.post(
                    url, json=payload, headers=headers,
                    cookies=cookies, impersonate="chrome124",
                    timeout=20, proxies=proxies
                )
            else:
                r = requests.post(
                    url, json=payload, headers=headers,
                    cookies=cookies, timeout=15, proxies=proxies
                )

            logger.info("book_supply_slot supply=%s wh=%s date=%s status=%s body=%s",
                        supply_id, warehouse_id, slot_date, r.status_code, r.text[:300])

            if r.status_code in (200, 201):
                return True, "Поставка забронирована ✅"
            data = r.json() if r.text else {}
            err = data.get("errorText") or data.get("detail") or r.text[:200]
            return False, f"Ошибка: {err}"
        except Exception as e:
            logger.error(f"book_supply_slot: {e}")
            return False, str(e)


# ─────────────────────────────────────────
# WB AUTH — ИСПРАВЛЕННАЯ ВЕРСИЯ (curl_cffi)
# ─────────────────────────────────────────
WB_PASSPORT_API = "https://passport.wildberries.ru"


class WBAuth:
    """
    Авторизация WB через телефон + SMS.
    Использует curl_cffi для имитации TLS-отпечатка Chrome —
    это решает проблему SSL EOF.
    """

    HEADERS = {
        "Content-Type": "application/json",
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Origin": "https://www.wildberries.ru",
        "Referer": "https://www.wildberries.ru/",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
    }

    @staticmethod
    def new_device_id() -> str:
        return str(uuid.uuid4())

    @classmethod
    def _get_proxies(cls):
        if PROXY_URL:
            return {"https": PROXY_URL, "http": PROXY_URL}
        return None

    # ────── Запрос SMS ──────────────────────
    @classmethod
    def request_sms(cls, phone: str, device_id: str) -> tuple:
        """Шаг 1: отправить номер → WB пришлёт SMS."""
        payload = {"login": phone, "device_id": device_id, "device_type": "iOS"}
        url = f"{WB_PASSPORT_API}/api/v2/auth/login"
        proxies = cls._get_proxies()

        # ── Способ 1: curl_cffi (имитация Chrome) ──
        if HAS_CURL_CFFI:
            try:
                r = cf_requests.post(
                    url,
                    json=payload,
                    headers=cls.HEADERS,
                    impersonate="chrome124",
                    timeout=30,
                    proxies=proxies,
                )
                logger.info("WBAuth.request_sms [curl_cffi] phone=%s status=%s body=%s",
                            phone, r.status_code, r.text[:200])

                if r.status_code == 200:
                    return True, "SMS отправлена"

                data = r.json() if r.text else {}
                err = data.get("errorText") or data.get("detail") or f"HTTP {r.status_code}"
                return False, err

            except Exception as e:
                logger.error("WBAuth.request_sms curl_cffi error: %s", e)
                return False, f"Ошибка curl_cffi: {e}"

        # ── Способ 2: fallback requests (может не работать) ──
        else:
            try:
                session = requests.Session()
                retry = Retry(total=2, backoff_factor=0.5,
                              status_forcelist=[429, 500, 502, 503, 504])
                adapter = HTTPAdapter(max_retries=retry)
                session.mount("https://", adapter)
                session.verify = certifi.where()

                r = session.post(url, json=payload, headers=cls.HEADERS,
                                 timeout=20, proxies=proxies)
                logger.info("WBAuth.request_sms [requests] phone=%s status=%s body=%s",
                            phone, r.status_code, r.text[:200])

                if r.status_code == 200:
                    return True, "SMS отправлена"
                data = r.json() if r.text else {}
                err = data.get("errorText") or data.get("detail") or f"HTTP {r.status_code}"
                return False, err

            except SSLError as e:
                logger.error("WBAuth.request_sms SSL fallback error: %s", e)
                return False, (
                    "SSL-ошибка: curl_cffi НЕ установлен!\n"
                    "Установите: pip install curl_cffi\n"
                    "и перезапустите бота."
                )
            except Exception as e:
                logger.error("WBAuth.request_sms fallback error: %s", e)
                return False, str(e)

    # ────── Подтверждение SMS-кода ──────────
    @classmethod
    def confirm_code(cls, phone: str, code: str, device_id: str) -> tuple:
        """Шаг 2: подтвердить SMS-код. Возвращает (ok, session_data)."""
        url = f"{WB_PASSPORT_API}/api/v2/auth/login/confirm"
        payload = {"login": phone, "code": str(code), "device_id": device_id}
        proxies = cls._get_proxies()

        if HAS_CURL_CFFI:
            try:
                session = cf_requests.Session(impersonate="chrome124")
                r = session.post(
                    url, json=payload, headers=cls.HEADERS,
                    timeout=30, proxies=proxies,
                )
                logger.info("WBAuth.confirm_code [curl_cffi] phone=%s status=%s body=%s",
                            phone, r.status_code, r.text[:300])

                if r.status_code not in (200, 201):
                    data = r.json() if r.text else {}
                    err = data.get("errorText") or data.get("detail") or f"HTTP {r.status_code}"
                    return False, {"error": err}

                data = r.json() if r.text else {}
                # curl_cffi cookies
                cookies = dict(session.cookies) if hasattr(session, 'cookies') else {}
                # Также забираем cookies из ответа
                if hasattr(r, 'cookies'):
                    cookies.update(dict(r.cookies))

                access_token = (
                    data.get("token")
                    or data.get("access_token")
                    or cookies.get("x-auth-token")
                    or cookies.get("WBToken")
                    or ""
                )
                return True, {"access_token": access_token, "cookies": cookies, "raw": data}

            except Exception as e:
                logger.error("WBAuth.confirm_code curl_cffi error: %s", e)
                return False, {"error": str(e)}
        else:
            # Fallback
            try:
                session = requests.Session()
                session.verify = certifi.where()
                r = session.post(url, json=payload, headers=cls.HEADERS,
                                 timeout=20, proxies=proxies)
                if r.status_code not in (200, 201):
                    data = r.json() if r.text else {}
                    err = data.get("errorText") or data.get("detail") or f"HTTP {r.status_code}"
                    return False, {"error": err}
                data = r.json() if r.text else {}
                cookies = requests.utils.dict_from_cookiejar(session.cookies)
                access_token = (
                    data.get("token") or data.get("access_token")
                    or cookies.get("x-auth-token") or cookies.get("WBToken") or ""
                )
                return True, {"access_token": access_token, "cookies": cookies, "raw": data}
            except SSLError:
                return False, {"error": "SSL-ошибка. Установите curl_cffi: pip install curl_cffi"}
            except Exception as e:
                return False, {"error": str(e)}

    # ────── Проверка сессии ─────────────────
    @classmethod
    def check_session(cls, account: dict) -> tuple:
        if not account or not (account.get("access_token") or account.get("cookies")):
            return False, "нет данных сессии"

        headers = dict(cls.HEADERS)
        if account.get("access_token"):
            headers["Authorization"] = account["access_token"]

        url = "https://seller.wildberries.ru/ns/sm-supply/supply-manager/api/v1/user"
        cookies = account.get("cookies", {})
        proxies = cls._get_proxies()

        try:
            if HAS_CURL_CFFI:
                r = cf_requests.get(
                    url, headers=headers, cookies=cookies,
                    impersonate="chrome124", timeout=15, proxies=proxies,
                )
            else:
                r = requests.get(url, headers=headers, cookies=cookies,
                                 timeout=15, proxies=proxies)

            if r.status_code == 200:
                return True, "активна"
            if r.status_code == 401:
                return False, "сессия истекла — требуется повторный вход"
            return False, f"HTTP {r.status_code}"
        except Exception as e:
            logger.error("WBAuth.check_session: %s", e)
            return False, str(e)


# ─────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────
def main_menu_keyboard():
    return ReplyKeyboardMarkup([
        ["👤 Аккаунты WB", "📊 Коэффициенты Сарапул"],
        ["🏪 Все склады", "➕ Создать задачу"],
        ["📋 Мои задачи", "⚡ Агрессивный режим"],
        ["📜 История броней", "⚙️ Настройки"],
    ], resize_keyboard=True)


def cancel_keyboard():
    return ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)


def coef_emoji(coef) -> str:
    return {0: "🟢", 1: "🟡", -1: "🔴"}.get(coef, "🟠")


def format_coefficients(coefs, max_items=20):
    if not coefs:
        return "Нет данных"
    lines, seen = [], set()
    for c in coefs:
        wh = c.get("warehouseName", "?")
        date = c.get("date", "")[:10]
        key = f"{wh}_{date}"
        if key in seen:
            continue
        seen.add(key)
        coef = c.get("coefficient", -1)
        allow = c.get("allowUnload", False)
        avail = " ✅" if (coef in (0, 1) and allow) else ""
        lines.append(f"{coef_emoji(coef)} {wh} | {date} | коэф: {coef}{avail}")
        if len(lines) >= max_items:
            break
    return "\n".join(lines)


# ─────────────────────────────────────────
# MONITORING ENGINE
# ─────────────────────────────────────────
async def run_monitoring_cycle(app):
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("SELECT DISTINCT tg_id FROM watch_tasks WHERE active=1")
    user_ids = [row[0] for row in c.fetchall()]
    conn.close()

    for tg_id in user_ids:
        user = get_user(tg_id)
        if not user or not user["wb_api_key"]:
            continue
        tasks = get_tasks(tg_id, active_only=True)
        if not tasks:
            continue
        client = WBClient(user["wb_api_key"])
        target_ids = list({t["target_warehouse_id"] for t in tasks if t["target_warehouse_id"]})
        coefs = client.get_coefficients(target_ids or None)
        for task in tasks:
            try:
                await check_task(app, tg_id, task, coefs)
            except Exception as e:
                logger.error(f"check_task task={task['id']}: {e}")


async def check_task(app, tg_id, task, coefs):
    target_id = task["target_warehouse_id"]
    max_coef = task["max_coefficient"]
    matches = [
        c for c in coefs
        if c.get("warehouseID") == target_id
        and 0 <= c.get("coefficient", -1) <= max_coef
        and c.get("allowUnload", False) is True
    ]
    if not matches:
        return

    best = min(matches, key=lambda x: (x["coefficient"], x["date"]))
    coef_val = best["coefficient"]
    slot_date = best.get("date", "")[:10]
    wh_name = best.get("warehouseName", task["target_warehouse_name"])
    barcodes_str = (
        ", ".join(task["barcodes"][:3]) + ("..." if len(task["barcodes"]) > 3 else "")
        if task["barcodes"] else "⚠️ баркоды не добавлены"
    )

    auto_book = task.get("auto_book", 0)
    supply_id = task.get("supply_id")
    account_phone = task.get("wb_account_phone")

    booked_ok = False
    booking_status = ""

    if auto_book and supply_id and account_phone:
        account = get_wb_account_by_phone(tg_id, account_phone)
        if account and account["is_active"]:
            valid, sess_status = WBAuth.check_session(account)
            if valid:
                user = get_user(tg_id)
                client = WBClient(user["wb_api_key"] if user else WB_API_KEY)
                booked_ok, booking_status = client.book_supply_slot(
                    supply_id=supply_id, warehouse_id=target_id,
                    slot_date=slot_date, account=account
                )
                if booked_ok:
                    deactivate_task(task["id"])
            else:
                mark_account_invalid(tg_id, account_phone)
                booking_status = f"⚠️ Сессия {account_phone} истекла — войдите заново"
        else:
            booking_status = f"⚠️ Аккаунт {account_phone} недоступен"

    if booked_ok:
        msg = (
            f"🎉 <b>ПОСТАВКА ЗАБРОНИРОВАНА АВТОМАТИЧЕСКИ!</b>\n\n"
            f"🏪 Склад: <b>{wh_name}</b>\n"
            f"📅 Дата: <b>{slot_date}</b>\n"
            f"📊 Коэффициент: <b>{coef_val}</b>\n"
            f"📦 Поставка: <b>{supply_id}</b>\n\n"
            f"{booking_status}\n\n"
            f"✅ Задача #{task['id']} закрыта.\n"
            f"🔗 <a href='https://seller.wildberries.ru/supplies-management/"
            f"all-supplies'>Проверить</a>"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📋 Мои задачи", callback_data="my_tasks"),
        ]])
    else:
        warn = f"\n⚠️ {booking_status}" if booking_status else ""
        auto_hint = (
            "\n\n💡 Для автобронирования — добавьте аккаунт WB (/accounts)"
            if not auto_book else ""
        )
        msg = (
            f"🚨 <b>СЛОТ ДОСТУПЕН!</b>\n\n"
            f"🏪 Склад: <b>{wh_name}</b>\n"
            f"📅 Дата: <b>{slot_date}</b>\n"
            f"📊 Коэффициент: <b>{coef_val}</b>\n"
            f"✅ Разгрузка: разрешена\n\n"
            f"📦 Задача #{task['id']}\n"
            f"Баркоды: {barcodes_str}{warn}{auto_hint}\n\n"
            f"⚡ Действуйте немедленно!\n"
            f"🔗 <a href='https://seller.wildberries.ru/supplies-management/"
            f"all-supplies'>Открыть кабинет WB</a>"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Зафиксировано", callback_data=f"ack_{task['id']}"),
            InlineKeyboardButton("🛑 Остановить", callback_data=f"stop_{task['id']}"),
        ]])

    try:
        await app.bot.send_message(
            chat_id=tg_id, text=msg, parse_mode="HTML",
            reply_markup=keyboard, disable_web_page_preview=True
        )
        log_booking(tg_id, task["id"], wh_name, coef_val, slot_date,
                    "AUTO_BOOKED" if booked_ok else "NOTIFIED")
    except Exception as e:
        logger.error(f"send_message: {e}")


# ─────────────────────────────────────────
# КОМАНДЫ
# ─────────────────────────────────────────
BOT_COMMANDS = [
    BotCommand("start",      "🏠 Главное меню"),
    BotCommand("accounts",   "👤 Аккаунты WB"),
    BotCommand("status",     "📊 Статус"),
    BotCommand("checkall",   "🔍 Проверить сейчас"),
    BotCommand("tasks",      "📋 Задачи"),
    BotCommand("newtask",    "➕ Создать задачу"),
    BotCommand("addbarcode", "📦 Добавить баркод"),
    BotCommand("aggressive", "⚡ Агрессивный режим"),
    BotCommand("history",    "📜 История"),
    BotCommand("setkey",     "🔑 API ключ"),
    BotCommand("help",       "📖 Справка"),
]


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    if not get_user(tg_id):
        save_user(tg_id, WB_API_KEY)
    ctx.user_data.clear()

    curl_status = "✅ curl_cffi" if HAS_CURL_CFFI else "❌ curl_cffi НЕ установлен!"
    await update.message.reply_text(
        f"🤖 <b>WB Авто-бронирование</b>\n\n"
        f"TLS-движок: {curl_status}\n\n"
        "Мониторю коэффициенты и мгновенно уведомляю / бронирую.\n"
        "🎯 Приоритетный склад: <b>Сарапул</b>\n\n"
        "Выберите действие:",
        parse_mode="HTML", reply_markup=main_menu_keyboard()
    )


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    tasks = get_tasks(tg_id)
    mode = "⚡ 5 сек" if user and user["aggressive_mode"] else "💤 60 сек"
    curl = "✅" if HAS_CURL_CFFI else "❌"
    proxy = f"✅ {PROXY_URL[:30]}..." if PROXY_URL else "❌ нет"
    await update.message.reply_text(
        f"📊 <b>Статус</b>\n\n"
        f"🔄 Режим: {mode}\n"
        f"📋 Задач: {len(tasks)}\n"
        f"🔐 curl_cffi: {curl}\n"
        f"🌐 Прокси: {proxy}\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        parse_mode="HTML"
    )


async def cmd_checkall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    if not user:
        await update.message.reply_text("⚠️ /setkey")
        return
    msg = await update.message.reply_text("🔍 Проверяю...")
    client = WBClient(user["wb_api_key"])
    coefs = client.get_coefficients()
    tasks = get_tasks(tg_id)
    triggered = 0
    for task in tasks:
        await check_task(ctx.application, tg_id, task, coefs)
        if any(
            c.get("warehouseID") == task["target_warehouse_id"]
            and c.get("coefficient", -1) in (0, 1) and c.get("allowUnload")
            for c in coefs
        ):
            triggered += 1
    await msg.edit_text(
        f"✅ Задач: {len(tasks)} | Сработало: {triggered} | {datetime.now().strftime('%H:%M:%S')}"
    )


async def cmd_tasks(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_tasks(update, ctx)


async def cmd_newtask(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await create_task_start(update, ctx)


async def cmd_addbarcode(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("📦 /addbarcode <ID> <баркод>")
        return
    tg_id = update.effective_user.id
    try:
        task_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ ID — число")
        return
    barcode = args[1].strip()
    ok, total = add_barcode_to_task(task_id, tg_id, barcode)
    if not ok:
        await update.message.reply_text(f"❌ Задача #{task_id} не найдена")
        return
    await update.message.reply_text(
        f"✅ <code>{barcode}</code> → #{task_id} (всего: {total})",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"➕ Ещё", callback_data=f"addb_{task_id}"),
            InlineKeyboardButton("📋 Задачи", callback_data="my_tasks"),
        ]])
    )


async def cmd_aggressive(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await toggle_aggressive(update, ctx)


async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_booking_history(update, ctx)


async def cmd_setkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["waiting_for"] = STATE_API_KEY
    await update.message.reply_text("🔑 Введите WB API ключ:", reply_markup=cancel_keyboard())


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 <b>Справка</b>\n\n"
        "/start — меню\n"
        "/accounts — аккаунты WB (вход через SMS)\n"
        "/newtask — создать задачу\n"
        "/tasks — задачи\n"
        "/checkall — проверить сейчас\n"
        "/aggressive — 5 сек режим\n"
        "/addbarcode &lt;id&gt; &lt;баркод&gt;\n"
        "/history — история\n"
        "/setkey — API ключ\n\n"
        "🔐 <b>Важно:</b> для авторизации в WB нужен <code>curl_cffi</code>:\n"
        "<code>pip install curl_cffi</code>",
        parse_mode="HTML"
    )


async def cmd_accounts(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_accounts_menu(update, ctx)


# ─────────────────────────────────────────
# ТЕКСТОВОЕ МЕНЮ
# ─────────────────────────────────────────
async def handle_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    waiting = ctx.user_data.get("waiting_for")
    if waiting:
        await handle_input(update, ctx, waiting)
        return

    text = update.message.text
    handlers_map = {
        "📊 Коэффициенты Сарапул": show_sarapul_coefs,
        "🏪 Все склады": show_all_warehouses,
        "➕ Создать задачу": create_task_start,
        "📋 Мои задачи": show_tasks,
        "⚡ Агрессивный режим": toggle_aggressive,
        "📜 История броней": show_booking_history,
        "⚙️ Настройки": show_settings,
        "👤 Аккаунты WB": show_accounts_menu,
    }
    handler = handlers_map.get(text)
    if handler:
        await handler(update, ctx)
    else:
        await update.message.reply_text("Выберите из меню или /help",
                                        reply_markup=main_menu_keyboard())


# ─────────────────────────────────────────
# ДИАЛОГОВЫЙ ВВОД (без дубликатов!)
# ─────────────────────────────────────────
async def handle_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE, waiting: str):
    text = update.message.text
    tg_id = update.effective_user.id

    if text == "❌ Отмена":
        ctx.user_data.clear()
        await update.message.reply_text("Отменено.", reply_markup=main_menu_keyboard())
        return

    # ── API ключ ──
    if waiting == STATE_API_KEY:
        api_key = text.strip()
        if len(api_key) < 50:
            await update.message.reply_text("⚠️ Слишком короткий. Попробуйте ещё:")
            return
        save_user(tg_id, api_key)
        ctx.user_data.clear()
        await update.message.reply_text("✅ Сохранён!", reply_markup=main_menu_keyboard())

    # ── Баркоды ──
    elif waiting == STATE_BARCODES:
        if text.strip() == "0":
            barcodes = []
        else:
            raw = text.replace("\n", ",").replace(";", ",")
            barcodes = [b.strip() for b in raw.split(",") if b.strip()]
            if not barcodes:
                await update.message.reply_text("⚠️ Введите через запятую:")
                return

        if "creating_task" not in ctx.user_data:
            ctx.user_data["creating_task"] = {"type": "booking"}
        ctx.user_data["creating_task"]["barcodes"] = barcodes
        ctx.user_data["waiting_for"] = STATE_WAREHOUSE
        await update.message.reply_text(
            f"✅ Баркодов: {len(barcodes)}\n\n🏪 Введите название склада:",
            reply_markup=cancel_keyboard()
        )

    # ── Склад ──
    elif waiting == STATE_WAREHOUSE:
        wh_input = text.strip()
        user = get_user(tg_id)
        client = WBClient(user["wb_api_key"] if user else WB_API_KEY)
        msg = await update.message.reply_text("🔍 Ищу...")
        wh = client.find_warehouse_by_name(wh_input)
        if not wh:
            await msg.edit_text(f"❌ «{wh_input}» не найден. Попробуйте другое:")
            return

        task_data = ctx.user_data.get("creating_task", {})
        barcodes = task_data.get("barcodes", [])
        task_id = add_task(
            tg_id=tg_id, task_type=task_data.get("type", "booking"),
            barcodes=barcodes, target_id=wh["ID"], target_name=wh["name"], max_coef=1
        )
        ctx.user_data.clear()

        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Баркод", callback_data=f"addb_{task_id}"),
            InlineKeyboardButton("📋 Задачи", callback_data="my_tasks"),
        ]])
        await msg.edit_text(
            f"✅ <b>Задача #{task_id}</b>\n🏪 {wh['name']} | 📦 {len(barcodes)} шт.",
            parse_mode="HTML", reply_markup=kb
        )
        await update.message.reply_text("OK", reply_markup=main_menu_keyboard())

    # ── Номер телефона ──
    elif waiting == STATE_PHONE:
        phone = text.strip()
        if phone.startswith("8") and len(phone) == 11 and phone.isdigit():
            phone = "+7" + phone[1:]
        elif not phone.startswith("+"):
            phone = "+" + phone

        if len(phone) != 12 or not phone[1:].isdigit():
            await update.message.reply_text(
                "❌ Формат: +7XXXXXXXXXX", reply_markup=cancel_keyboard()
            )
            return

        device_id = WBAuth.new_device_id()
        ctx.user_data["auth_phone"] = phone
        ctx.user_data["auth_device_id"] = device_id

        msg = await update.message.reply_text(f"📱 Отправляю SMS на {phone}...")
        ok, err_text = WBAuth.request_sms(phone, device_id)
        if not ok:
            await msg.edit_text(
                f"❌ Ошибка: {err_text}\n\n"
                f"{'⚠️ curl_cffi не установлен! pip install curl_cffi' if not HAS_CURL_CFFI else ''}\n"
                "Проверьте номер / сеть и попробуйте снова:"
            )
            # Остаёмся в STATE_PHONE — пользователь может ввести номер снова
            return

        ctx.user_data["waiting_for"] = STATE_SMS_CODE
        await msg.edit_text(f"✅ SMS → {phone}\n\n📩 Введите код:")

    # ── SMS-код ──
    elif waiting == STATE_SMS_CODE:
        code = text.strip()
        phone = ctx.user_data.get("auth_phone", "")
        device_id = ctx.user_data.get("auth_device_id", "")

        if not phone or not device_id:
            ctx.user_data.clear()
            await update.message.reply_text("Сессия истекла. /accounts",
                                            reply_markup=main_menu_keyboard())
            return

        msg = await update.message.reply_text("🔐 Проверяю...")
        ok, session = WBAuth.confirm_code(phone, code, device_id)
        if not ok:
            err = session.get("error", "неверный код")
            await msg.edit_text(f"❌ {err}\n\nВведите код снова:",
                                reply_markup=cancel_keyboard())
            ctx.user_data["waiting_for"] = STATE_SMS_CODE
            return

        save_wb_account(
            tg_id=tg_id, phone=phone, device_id=device_id,
            access_token=session.get("access_token"),
            cookies=session.get("cookies", {}),
        )
        ctx.user_data.clear()
        await msg.edit_text(
            f"✅ <b>Аккаунт {phone} добавлен!</b>\n\n"
            "Бот может бронировать от вашего имени.\n"
            "При создании задачи укажите аккаунт и ID поставки.",
            parse_mode="HTML"
        )
        await update.message.reply_text("OK", reply_markup=main_menu_keyboard())

    # ── ID поставки ──
    elif waiting == STATE_SUPPLY_ID:
        supply_id = text.strip()
        task_id = ctx.user_data.get("supply_task_id")
        account_phone = ctx.user_data.get("supply_account_phone")
        if not task_id:
            ctx.user_data.clear()
            await update.message.reply_text("Истекло.", reply_markup=main_menu_keyboard())
            return
        conn = sqlite3.connect("wb_bot.db")
        c = conn.cursor()
        c.execute(
            "UPDATE watch_tasks SET supply_id=?, wb_account_phone=?, auto_book=1 "
            "WHERE id=? AND tg_id=?",
            (supply_id, account_phone, task_id, tg_id)
        )
        conn.commit()
        conn.close()
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ <b>Автобронирование:</b>\n📦 {supply_id}\n👤 {account_phone}",
            parse_mode="HTML", reply_markup=main_menu_keyboard()
        )

    # ── Добавление баркода ──
    elif waiting == STATE_ADD_BARCODE:
        task_id = ctx.user_data.get("add_barcode_task_id")
        if not task_id:
            ctx.user_data.clear()
            await update.message.reply_text("Истекло.", reply_markup=main_menu_keyboard())
            return
        barcode = text.strip()
        ok, total = add_barcode_to_task(task_id, tg_id, barcode)
        if not ok:
            await update.message.reply_text(f"❌ Задача #{task_id} не найдена")
            ctx.user_data.clear()
            return
        await update.message.reply_text(
            f"✅ <code>{barcode}</code> → #{task_id} ({total} шт.)\n\nЕщё или Готово:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("➕ Ещё", callback_data=f"addb_{task_id}"),
                InlineKeyboardButton("✅ Готово", callback_data="my_tasks"),
            ]])
        )


# ─────────────────────────────────────────
# ЭКРАНЫ
# ─────────────────────────────────────────
async def show_sarapul_coefs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    if not user:
        await update.message.reply_text("⚠️ /start")
        return
    msg = await update.message.reply_text("⏳ Загружаю...")
    client = WBClient(user["wb_api_key"])
    wh = client.find_warehouse_by_name("Сарапул")
    if not wh:
        await msg.edit_text("❌ Сарапул не найден")
        return
    coefs = client.get_coefficients([wh["ID"]])
    if not coefs:
        await msg.edit_text(f"❌ Нет данных (ID: {wh['ID']})")
        return
    available = [c for c in coefs if c.get("coefficient", -1) in (0, 1) and c.get("allowUnload")]
    text = f"🏪 <b>Сарапул (ID: {wh['ID']})</b>\n🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
    if available:
        text += "✅ <b>ДОСТУПНО:</b>\n"
        for c in available[:5]:
            text += f"  {coef_emoji(c['coefficient'])} {c.get('date','')[:10]} | {c['coefficient']}\n"
        text += "\n"
    text += "<b>Все даты:</b>\n"
    seen = set()
    for c in coefs:
        key = c.get("date", "")[:10]
        if key in seen:
            continue
        seen.add(key)
        coef = c.get("coefficient", -1)
        allow = c.get("allowUnload", False)
        s = "✅" if (coef in (0, 1) and allow) else ("🔴" if coef == -1 else "🟠")
        text += f"  {s} {key} | {coef} | box:{c.get('boxTypeID','')}\n"
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh_sarapul"),
        InlineKeyboardButton("🔔 Следить", callback_data=f"watch_{wh['ID']}_{wh['name']}"),
    ]])
    await msg.edit_text(text, parse_mode="HTML", reply_markup=kb)


async def show_all_warehouses(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    if not user:
        return
    msg = await update.message.reply_text("⏳...")
    client = WBClient(user["wb_api_key"])
    coefs = client.get_coefficients()
    best = {}
    for c in coefs:
        name = c.get("warehouseName", "?")
        coef = c.get("coefficient", -1)
        allow = c.get("allowUnload", False)
        if name not in best or (coef >= 0 and coef < best[name]["coef"]):
            best[name] = {"coef": coef, "allow": allow, "date": c.get("date", "")[:10]}
    avail = sorted(
        [(n, i) for n, i in best.items() if i["coef"] in (0, 1) and i["allow"]],
        key=lambda x: x[1]["coef"]
    )
    unavail = sorted(
        [(n, i) for n, i in best.items() if not (i["coef"] in (0, 1) and i["allow"])],
        key=lambda x: x[0]
    )
    text = f"🏪 <b>Склады</b> | {datetime.now().strftime('%H:%M')}\n\n"
    if avail:
        text += f"✅ <b>ДОСТУПНЫ ({len(avail)}):</b>\n"
        for n, i in avail[:15]:
            text += f"  🟢 {n} | {i['coef']} | {i['date']}\n"
        text += "\n"
    text += f"❌ <b>Нет ({len(unavail)}):</b>\n"
    for n, i in unavail[:20]:
        text += f"  {'🟠' if i['coef'] > 1 else '🔴'} {n} | {i['coef']}\n"
    if len(unavail) > 20:
        text += f"  ...+{len(unavail)-20}\n"
    await msg.edit_text(text, parse_mode="HTML")


async def create_task_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Бронирование", callback_data="task_booking")],
        [InlineKeyboardButton("🔄 Перераспределение", callback_data="task_redist")],
        [InlineKeyboardButton("⚡ Быстро: Сарапул", callback_data="task_sarapul_quick")],
    ])
    await update.message.reply_text("📝 <b>Тип задачи:</b>", parse_mode="HTML", reply_markup=kb)


async def show_tasks(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    tasks = get_tasks(tg_id)
    if not tasks:
        await update.message.reply_text("📋 Нет задач.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("➕ Создать", callback_data="new_task")
            ]]))
        return
    text = f"📋 <b>Задачи ({len(tasks)}):</b>\n\n"
    buttons = []
    for t in tasks:
        bc = f"{len(t['barcodes'])}" if t["barcodes"] else "⚠️0"
        auto = "🤖" if t.get("auto_book") else ""
        text += (
            f"<b>#{t['id']}</b> {t['target_warehouse_name']} | "
            f"📦{bc} | коэф≤{t['max_coefficient']} {auto}\n"
        )
        buttons.append([
            InlineKeyboardButton("➕📦", callback_data=f"addb_{t['id']}"),
            InlineKeyboardButton("🤖Авто", callback_data=f"setup_autobook_{t['id']}"),
            InlineKeyboardButton("🛑", callback_data=f"stop_{t['id']}"),
        ])
    await update.message.reply_text(text, parse_mode="HTML",
                                    reply_markup=InlineKeyboardMarkup(buttons))


async def toggle_aggressive(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    if not user:
        return
    new_mode = 1 - user["aggressive_mode"]
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute("UPDATE users SET aggressive_mode=? WHERE tg_id=?", (new_mode, tg_id))
    conn.commit()
    conn.close()

    scheduler = ctx.application.bot_data.get("scheduler")
    if scheduler:
        interval = POLLING_INTERVAL_AGGRESSIVE if new_mode else POLLING_INTERVAL_NORMAL
        job = scheduler.get_job("monitoring")
        if job:
            job.reschedule("interval", seconds=interval)

    text = "⚡ <b>Агрессивный (5 сек) ВКЛ</b>" if new_mode else "💤 <b>Стандартный (60 сек)</b>"
    await update.message.reply_text(text, parse_mode="HTML")


async def show_booking_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    conn = sqlite3.connect("wb_bot.db")
    c = conn.cursor()
    c.execute(
        "SELECT warehouse_name, coefficient, date, status, created_at "
        "FROM booking_log WHERE tg_id=? ORDER BY id DESC LIMIT 20",
        (tg_id,)
    )
    rows = c.fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("📜 Пусто.")
        return
    text = "📜 <b>История:</b>\n\n"
    for r in rows:
        text += f"  🏪 {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4][:16]}\n"
    await update.message.reply_text(text, parse_mode="HTML")


async def show_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user = get_user(tg_id)
    mode = "⚡ 5с" if user and user["aggressive_mode"] else "💤 60с"
    curl = "✅" if HAS_CURL_CFFI else "❌"
    proxy = "✅" if PROXY_URL else "❌"
    await update.message.reply_text(
        f"⚙️ <b>Настройки</b>\n\n"
        f"🔑 API: {'✅' if user else '❌'}\n"
        f"⏱ Режим: {mode}\n"
        f"🔐 curl_cffi: {curl}\n"
        f"🌐 Прокси: {proxy}\n\n"
        "/setkey — сменить ключ",
        parse_mode="HTML"
    )


async def show_accounts_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    accounts = get_wb_accounts(tg_id)

    if not accounts:
        text = (
            "👤 <b>Аккаунты WB</b>\n\n"
            "Нет привязанных аккаунтов.\n\n"
            f"{'✅ curl_cffi установлен' if HAS_CURL_CFFI else '❌ curl_cffi НЕ установлен — pip install curl_cffi'}\n\n"
            "Войдите через телефон для автобронирования:"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Войти в WB", callback_data="acc_add"),
        ]])
    else:
        text = f"👤 <b>Аккаунты ({len(accounts)})</b>\n\n"
        buttons = []
        for acc in accounts:
            s = "🟢" if acc["is_active"] else "🔴"
            text += f"{s} <code>{acc['phone']}</code>\n"
            buttons.append([
                InlineKeyboardButton(f"🔍 {acc['phone']}", callback_data=f"acc_check_{acc['phone']}"),
                InlineKeyboardButton("🗑", callback_data=f"acc_del_{acc['phone']}"),
            ])
        buttons.append([InlineKeyboardButton("➕ Добавить", callback_data="acc_add")])
        kb = InlineKeyboardMarkup(buttons)

    if update.message:
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await update.callback_query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)


# ─────────────────────────────────────────
# CALLBACK HANDLER
# ─────────────────────────────────────────
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    tg_id = query.from_user.id
    user = get_user(tg_id)
    client = WBClient(user["wb_api_key"] if user else WB_API_KEY)

    if data == "refresh_sarapul":
        wh = client.find_warehouse_by_name("Сарапул")
        if not wh:
            await query.edit_message_text("❌ Не найден")
            return
        coefs = client.get_coefficients([wh["ID"]])
        avail = [c for c in coefs if c.get("coefficient", -1) in (0, 1) and c.get("allowUnload")]
        s = f"✅ {len(avail)} слотов" if avail else "❌ Нет"
        text = (
            f"🏪 <b>Сарапул</b> | {datetime.now().strftime('%H:%M:%S')}\n{s}\n\n"
            f"{format_coefficients(coefs, 14)}"
        )
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=query.message.reply_markup)

    elif data.startswith("watch_"):
        parts = data.split("_", 2)
        wh_id, wh_name = int(parts[1]), parts[2] if len(parts) > 2 else "Склад"
        task_id = add_task(tg_id=tg_id, task_type="booking", barcodes=[],
                          target_id=wh_id, target_name=wh_name)
        await query.edit_message_text(
            f"✅ #{task_id} → {wh_name}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("➕📦", callback_data=f"addb_{task_id}")
            ]])
        )

    elif data.startswith("addb_"):
        task_id = int(data.split("_")[1])
        ctx.user_data["waiting_for"] = STATE_ADD_BARCODE
        ctx.user_data["add_barcode_task_id"] = task_id
        await query.message.reply_text(f"📦 Баркод для #{task_id}:", reply_markup=cancel_keyboard())

    elif data == "task_sarapul_quick":
        wh = client.find_warehouse_by_name("Сарапул")
        if not wh:
            await query.edit_message_text("❌ Не найден")
            return
        task_id = add_task(tg_id=tg_id, task_type="booking", barcodes=[],
                          target_id=wh["ID"], target_name=wh["name"])
        ctx.user_data["waiting_for"] = STATE_ADD_BARCODE
        ctx.user_data["add_barcode_task_id"] = task_id
        await query.edit_message_text(f"✅ #{task_id} Сарапул\n📦 Введите баркод:", parse_mode="HTML")
        await query.message.reply_text("Баркод:", reply_markup=cancel_keyboard())

    elif data in ("task_booking", "task_redist"):
        task_type = "booking" if data == "task_booking" else "redistribution"
        ctx.user_data["creating_task"] = {"type": task_type}
        ctx.user_data["waiting_for"] = STATE_BARCODES
        await query.edit_message_text(
            "Введите баркоды через запятую (или 0 чтобы пропустить):",
            parse_mode="HTML"
        )

    elif data in ("my_tasks", "new_task"):
        ctx.user_data.clear()
        await query.message.reply_text("Используйте меню:", reply_markup=main_menu_keyboard())

    elif data.startswith("stop_"):
        task_id = int(data.split("_")[1])
        deactivate_task(task_id)
        await query.edit_message_text(
            (query.message.text or "") + f"\n\n🛑 #{task_id} остановлена", parse_mode="HTML"
        )

    elif data.startswith("ack_"):
        await query.edit_message_text(
            (query.message.text or "") + "\n\n✅ OK", parse_mode="HTML"
        )

    elif data == "acc_add":
        if not HAS_CURL_CFFI:
            await query.message.reply_text(
                "❌ <b>curl_cffi не установлен!</b>\n\n"
                "Без него авторизация WB невозможна (SSL-блокировка).\n\n"
                "Установите:\n<code>pip install curl_cffi</code>\n\n"
                "И перезапустите бота.",
                parse_mode="HTML"
            )
            return
        ctx.user_data["waiting_for"] = STATE_PHONE
        await query.message.reply_text(
            "📱 Номер WB аккаунта:\nФормат: +7XXXXXXXXXX",
            reply_markup=cancel_keyboard()
        )

    elif data.startswith("acc_check_"):
        phone = data[len("acc_check_"):]
        account = get_wb_account_by_phone(tg_id, phone)
        if not account:
            await query.edit_message_text("❌ Не найден")
            return
        msg = await query.message.reply_text(f"🔍 Проверяю {phone}...")
        valid, status = WBAuth.check_session(account)
        if valid:
            update_account_tokens(tg_id, phone,
                                  access_token=account["access_token"],
                                  cookies=account["cookies"])
            await msg.edit_text(f"✅ {phone}: активна")
        else:
            mark_account_invalid(tg_id, phone)
            await msg.edit_text(
                f"🔴 {phone}: {status}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Войти заново", callback_data="acc_add")
                ]])
            )

    elif data.startswith("acc_del_"):
        phone = data[len("acc_del_"):]
        delete_wb_account(tg_id, phone)
        await query.answer(f"{phone} удалён")
        # Обновляем список
        await show_accounts_menu(update, ctx)

    elif data.startswith("setup_autobook_"):
        task_id = int(data.split("_")[2])
        accounts = get_wb_accounts(tg_id)
        if not accounts:
            await query.edit_message_text("⚠️ Нет аккаунтов. /accounts")
            return
        buttons = [[
            InlineKeyboardButton(
                f"{'🟢' if a['is_active'] else '🔴'} {a['phone']}",
                callback_data=f"autobook_acc_{task_id}_{a['phone']}"
            )
        ] for a in accounts]
        await query.edit_message_text(
            f"🤖 Аккаунт для #{task_id}:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif data.startswith("autobook_acc_"):
        # autobook_acc_<task_id>_<phone>
        rest = data[len("autobook_acc_"):]
        idx = rest.index("_")
        task_id = int(rest[:idx])
        account_phone = rest[idx+1:]

        account = get_wb_account_by_phone(tg_id, account_phone)
        if not account:
            await query.edit_message_text("❌ Аккаунт не найден")
            return

        ctx.user_data["waiting_for"] = STATE_SUPPLY_ID
        ctx.user_data["supply_task_id"] = task_id
        ctx.user_data["supply_account_phone"] = account_phone

        await query.edit_message_text(
            f"✅ Аккаунт: {account_phone}\n\n"
            "📦 Введите ID поставки (из кабинета WB):\n"
            "Поставка должна быть создана БЕЗ даты."
        )
        await query.message.reply_text("ID поставки:", reply_markup=cancel_keyboard())


# ─────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────
async def post_init(app):
    await app.bot.set_my_commands(BOT_COMMANDS)
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_monitoring_cycle, "interval",
        seconds=POLLING_INTERVAL_NORMAL, id="monitoring",
        args=[app], misfire_grace_time=30,
    )
    scheduler.start()
    app.bot_data["scheduler"] = scheduler
    logger.info("Bot started, interval=%s, curl_cffi=%s, proxy=%s",
                POLLING_INTERVAL_NORMAL, HAS_CURL_CFFI, bool(PROXY_URL))


def main():
    init_db()
    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("accounts",   cmd_accounts))
    app.add_handler(CommandHandler("status",     cmd_status))
    app.add_handler(CommandHandler("checkall",   cmd_checkall))
    app.add_handler(CommandHandler("tasks",      cmd_tasks))
    app.add_handler(CommandHandler("newtask",    cmd_newtask))
    app.add_handler(CommandHandler("addbarcode", cmd_addbarcode))
    app.add_handler(CommandHandler("aggressive", cmd_aggressive))
    app.add_handler(CommandHandler("history",    cmd_history))
    app.add_handler(CommandHandler("setkey",     cmd_setkey))
    app.add_handler(CommandHandler("help",       cmd_help))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu))

    print(f"🤖 WB Bot запущен! curl_cffi={'✅' if HAS_CURL_CFFI else '❌'}")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()