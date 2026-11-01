"""Plush REDO Club — Telegram-бот предзаказов (по ТЗ).

aiogram 3 + SQLite (WAL). FSM хранится в БД, перезапуск не сбрасывает анкеты.
Бот же отдаёт публичный API прогресса для сайта: GET /api/progress.

Счётчик партии = только экземпляры с подтверждённым авансом (см. PAID_STATUSES).
Оплата: Telegram Payments (ЮKassa и др. через PAYMENT_PROVIDER_TOKEN).
Идемпотентность: payments.UNIQUE(provider, provider_payment_id).
Защита от переполнения: итоговая проверка вместимости внутри транзакции mark_paid.
"""

import asyncio
import csv
import html
import io
import json
import logging
import os
import re
import tempfile
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.types import (
    BotCommand,
    BufferedInputFile,
    CallbackQuery,
    ErrorEvent,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


# ============================================================
# CONFIG
# ============================================================

def env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        raise RuntimeError(f"Переменная {name} должна быть целым числом, сейчас: {raw!r}")


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {int(v) for v in re.split(r"[,\s]+", os.getenv("ADMIN_IDS", "")) if v.isdigit()}

DB_PATH = os.getenv("DB_PATH", "plush_redo.db")
BATCH_SIZE = env_int("BATCH_SIZE", 200, 1)
PRICE = env_int("PRICE_RUB", 2190, 1)
ADVANCE = env_int("ADVANCE_RUB", 500, 1)
REMAINDER = PRICE - ADVANCE
MAX_QUANTITY = env_int("MAX_QUANTITY_PER_ORDER", 10, 1)
MAX_UNPAID_PER_USER = env_int("MAX_UNPAID_PER_USER", 3, 1)
MAX_PENDING_DESIGNS = env_int("MAX_PENDING_DESIGNS", 5, 1)
MAX_IMAGE_SIZE_MB = env_int("MAX_IMAGE_SIZE_MB", 10, 1)
MAX_IMAGE_SIZE = MAX_IMAGE_SIZE_MB * 1024 * 1024
PAYMENT_TTL_MIN = env_int("PAYMENT_TTL_MIN", 30, 5)  # на сколько минут резервируется экземпляр под оплату
ENABLE_DESIGNS = os.getenv("ENABLE_DESIGNS", "1").strip() != "0"

PAYMENT_PROVIDER_TOKEN = os.getenv("PAYMENT_PROVIDER_TOKEN", "").strip()

# переменные из ТЗ
LAUNCH_DEADLINE = os.getenv("LAUNCH_DEADLINE", "").strip()          # {launch_deadline}
ESTIMATED_SHIPPING = os.getenv("ESTIMATED_SHIPPING", "").strip()    # {estimated_shipping}
SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "").strip()          # {support_contact}
SITE_URL = os.getenv("SITE_URL", "https://plush-redo-club.vercel.app/").strip()
CHANNEL_URL = os.getenv("CHANNEL_URL", "").strip()                  # {channel_url}
OFFER_URL = os.getenv("OFFER_URL", "").strip()                      # {offer_url}
PRIVACY_URL = os.getenv("PRIVACY_URL", "").strip()                  # {privacy_url}
CONSENT_URL = os.getenv("CONSENT_URL", "").strip()                  # {consent_url}
RETURNS_URL = os.getenv("RETURNS_URL", "").strip()                  # {returns_url}

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = env_int("API_PORT", 8080)  # 0 — выключить API
API_ALLOW_ORIGIN = os.getenv("API_ALLOW_ORIGIN", "*")

ASSETS_DIR = Path(os.getenv("ASSETS_DIR", "assets"))

try:
    TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/Moscow"))
except Exception:
    TZ = timezone.utc

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("plush_redo")

router = Router()


def config_missing() -> List[str]:
    """Без этих значений оплату не открываем (требование ТЗ)."""
    required = {
        "LAUNCH_DEADLINE": LAUNCH_DEADLINE,
        "SUPPORT_CONTACT": SUPPORT_CONTACT,
        "OFFER_URL": OFFER_URL,
        "PRIVACY_URL": PRIVACY_URL,
        "CONSENT_URL": CONSENT_URL,
        "RETURNS_URL": RETURNS_URL,
    }
    return [k for k, v in required.items() if not v]


# ============================================================
# UI-константы
# ============================================================

BTN_PREORDER = "🧸 Оформить предзаказ"
BTN_MY = "📋 Мои заказы"
BTN_PROGRESS = "📊 Прогресс партии"
BTN_ABOUT = "ℹ️ О проекте"
BTN_SUPPORT = "💬 Поддержка"
BTN_DESIGNER = "🎨 Предложить дизайн"

BTN_BACK_TEXT = "⬅️ Назад"
BTN_CANCEL_TEXT = "✖️ Отменить оформление"


def build_main_menu() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=BTN_PREORDER)],
        [KeyboardButton(text=BTN_MY), KeyboardButton(text=BTN_PROGRESS)],
        [KeyboardButton(text=BTN_ABOUT), KeyboardButton(text=BTN_SUPPORT)],
    ]
    if ENABLE_DESIGNS:
        rows.append([KeyboardButton(text=BTN_DESIGNER)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


MAIN_MENU = build_main_menu()

CANCEL_BUTTON = InlineKeyboardButton(text=BTN_CANCEL_TEXT, callback_data="cancel_form")
BACK_BUTTON = InlineKeyboardButton(text=BTN_BACK_TEXT, callback_data="form:back")
HOME_BUTTON = InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")

DELIVERY = {
    "cdek": "СДЭК",
    "post": "Почта России",
    "later": "Выберу позже",
}

CONSENTS = [
    ("c_offer", "Принимаю условия оферты", True),
    ("c_pd", "Даю согласие на обработку ПДн", True),
    ("c_proto", "Понимаю, что физического прототипа пока нет", True),
    ("c_ship", "Понимаю, что доставка оплачивается отдельно", True),
    ("c_news", "Хочу получать новости и предложения проекта", False),
]


def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def rub(n: int) -> str:
    return f"{n} ₽"


def fmt_dt(iso: Optional[str]) -> str:
    try:
        return datetime.fromisoformat(iso).astimezone(TZ).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso or "—"


def compose(*parts: Optional[str]) -> str:
    """Склейка строк; None пропускается (так не выводим незаполненные переменные)."""
    return "\n".join(p for p in parts if p is not None)


def opt(label: str, value: str) -> Optional[str]:
    return f"{label} {value}" if value else None


def valid_url(url: str) -> bool:
    return url.startswith(("http://", "https://"))


def url_button(text: str, url: str) -> Optional[InlineKeyboardButton]:
    return InlineKeyboardButton(text=text, url=url) if url and valid_url(url) else None


# ---------- картинки ----------

_file_ids: Dict[str, str] = {}
ASSET_EXTS = (".png", ".jpg", ".jpeg", ".webp")


def asset_path(key: str) -> Optional[Path]:
    for ext in ASSET_EXTS:
        path = ASSETS_DIR / f"{key}{ext}"
        if path.exists():
            return path
    return None


async def send_scene(target: Message, key: str, text: str, reply_markup=None) -> None:
    """Картинка из assets/<key>.(png|jpg|webp) + подпись. Нет файла — просто текст."""
    path = asset_path(key)
    if path is None:
        await target.answer(text, reply_markup=reply_markup)
        return

    photo = _file_ids.get(key) or FSInputFile(path)
    try:
        if len(text) <= 1000:
            sent = await target.answer_photo(photo, caption=text, reply_markup=reply_markup)
        else:
            sent = await target.answer_photo(photo)
            await target.answer(text, reply_markup=reply_markup)
        if sent.photo:
            _file_ids[key] = sent.photo[-1].file_id
    except TelegramAPIError:
        logger.warning("Scene %s failed, fallback to text", key, exc_info=True)
        _file_ids.pop(key, None)
        await target.answer(text, reply_markup=reply_markup)


# ============================================================
# FSM STATES
# ============================================================

class Preorder(StatesGroup):
    quantity = State()
    city = State()
    delivery = State()
    phone = State()
    consents = State()
    confirm = State()


class Designer(StatesGroup):
    wait = State()


class RefundFlow(StatesGroup):
    comment = State()


class AdminStates(StatesGroup):
    target = State()
    broadcast = State()
    notify_var = State()


# ============================================================
# DATABASE
# ============================================================

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    phone TEXT,
    consent_offer INTEGER NOT NULL DEFAULT 0,
    consent_pd INTEGER NOT NULL DEFAULT 0,
    consent_news INTEGER NOT NULL DEFAULT 0,
    consents_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS preorders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    public_id TEXT UNIQUE,
    telegram_id INTEGER NOT NULL,
    username TEXT,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
    city TEXT NOT NULL,
    delivery TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity >= 1),
    amount_total INTEGER NOT NULL,
    amount_advance INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    archived INTEGER NOT NULL DEFAULT 0,
    oversold INTEGER NOT NULL DEFAULT 0,
    payment_expires_at TEXT,
    paid_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    provider TEXT NOT NULL,
    provider_payment_id TEXT NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'succeeded',
    created_at TEXT NOT NULL,
    UNIQUE(provider, provider_payment_id)
);

CREATE TABLE IF NOT EXISTS refund_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    comment TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    requested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS refunds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    payment_id TEXT,
    status TEXT NOT NULL DEFAULT 'processing',
    admin_id INTEGER,
    created_at TEXT NOT NULL,
    confirmed_at TEXT
);

CREATE TABLE IF NOT EXISTS designs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    public_id TEXT UNIQUE,
    telegram_id INTEGER NOT NULL,
    username TEXT,
    name TEXT NOT NULL,
    file_id TEXT NOT NULL DEFAULT '',
    file_type TEXT NOT NULL DEFAULT 'photo',
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'new',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id INTEGER,
    details TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fsm (
    key TEXT PRIMARY KEY,
    state TEXT,
    data TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_preorders_status ON preorders(status);
CREATE INDEX IF NOT EXISTS idx_preorders_telegram ON preorders(telegram_id);
CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id);
CREATE INDEX IF NOT EXISTS idx_refunds_order ON refunds(order_id);
CREATE INDEX IF NOT EXISTS idx_designs_status ON designs(status);
CREATE INDEX IF NOT EXISTS idx_designs_telegram ON designs(telegram_id);
"""

# --- статусы заказа (внутренние, п. 21 ТЗ) ---
# Экземпляры в счётчике партии: аванс получен и ещё не возвращён.
# refund_requested / refund_processing — деньги ещё у нас, слот занят до статуса refunded.
PAID_STATUSES = (
    "advance_paid", "accepted", "prototype_stage", "in_production",
    "awaiting_balance", "ready_to_ship", "shipped", "completed",
    "refund_requested", "refund_processing",
)
COUNTED_SQL = "status IN (" + ", ".join(f"'{s}'" for s in PAID_STATUSES) + ") AND oversold = 0"
UNPAID_STATUSES = ("draft", "awaiting_payment")
STAGE_TARGETS = (
    "accepted", "prototype_stage", "in_production", "awaiting_balance",
    "ready_to_ship", "shipped", "completed",
)
REFUNDABLE = (
    "advance_paid", "accepted", "prototype_stage", "in_production",
    "awaiting_balance", "ready_to_ship",
)

STATUS_LABELS = {
    "draft": "Оформление не завершено",
    "awaiting_payment": "Ожидает оплаты",
    "advance_paid": "Аванс получен",
    "accepted": "Заказ принят в работу",
    "prototype_stage": "Разработка прототипа",
    "in_production": "В производстве",
    "awaiting_balance": "Ожидает доплаты",
    "ready_to_ship": "Готовится к отправке",
    "shipped": "Отправлен",
    "completed": "Завершён",
    "cancelled_unpaid": "Отменён без оплаты",
    "refund_requested": "Запрошен возврат",
    "refund_processing": "Возврат оформляется",
    "refunded": "Средства возвращены",
}
STATUS_EMOJI = {
    "draft": "📝", "awaiting_payment": "🕓", "advance_paid": "💰", "accepted": "✅",
    "prototype_stage": "🧵", "in_production": "🏭", "awaiting_balance": "💳",
    "ready_to_ship": "📦", "shipped": "🚚", "completed": "🎉", "cancelled_unpaid": "🚫",
    "refund_requested": "↩️", "refund_processing": "⏳", "refunded": "✔️",
}

DESIGN_STATUSES = {"new", "reviewing", "accepted", "rejected", "archived"}
DESIGN_STATUS_LABELS = {
    "new": "🆕 Новая", "reviewing": "🔎 На рассмотрении", "accepted": "✅ Принята",
    "rejected": "❌ Отклонена", "archived": "📦 Архив",
}


def status_label(status: str) -> str:
    return STATUS_LABELS.get(status, status)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@asynccontextmanager
async def db_connect():
    db = await aiosqlite.connect(DB_PATH, timeout=15)
    try:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA foreign_keys = ON")
        yield db
    finally:
        await db.close()


async def fetch_all(sql: str, params=()):
    async with db_connect() as db:
        cursor = await db.execute(sql, params)
        return await cursor.fetchall()


async def fetch_one(sql: str, params=()):
    async with db_connect() as db:
        cursor = await db.execute(sql, params)
        return await cursor.fetchone()


async def scalar(sql: str, params=()):
    row = await fetch_one(sql, params)
    return (row[0] or 0) if row else 0


async def execute(sql: str, params=()) -> int:
    async with db_connect() as db:
        cursor = await db.execute(sql, params)
        await db.commit()
        return cursor.rowcount


async def init_db():
    parent = Path(DB_PATH).parent
    if str(parent) not in ("", "."):
        parent.mkdir(parents=True, exist_ok=True)

    async with db_connect() as db:
        await db.execute("PRAGMA journal_mode = WAL")
        await db.executescript(DB_SCHEMA)

        # миграции старой схемы
        cursor = await db.execute("PRAGMA table_info(preorders)")
        cols = {r["name"] for r in await cursor.fetchall()}
        for col, ddl in (
            ("archived", "INTEGER NOT NULL DEFAULT 0"),
            ("oversold", "INTEGER NOT NULL DEFAULT 0"),
            ("payment_expires_at", "TEXT"),
        ):
            if col not in cols:
                await db.execute(f"ALTER TABLE preorders ADD COLUMN {col} {ddl}")

        cursor = await db.execute("PRAGMA table_info(designs)")
        dcols = {r["name"] for r in await cursor.fetchall()}
        if "file_type" not in dcols:
            await db.execute("ALTER TABLE designs ADD COLUMN file_type TEXT NOT NULL DEFAULT 'photo'")

        cursor = await db.execute("SELECT value FROM settings WHERE key = 'migrated_v2'")
        if not await cursor.fetchone():
            await db.execute("UPDATE preorders SET archived = 1 WHERE status = 'archived'")
            await db.execute(
                "UPDATE preorders SET status = CASE WHEN paid_at IS NOT NULL THEN 'advance_paid' "
                "ELSE 'cancelled_unpaid' END WHERE status IN ('archived', 'rejected')"
            )
            await db.execute("UPDATE preorders SET status = 'awaiting_payment' WHERE status = 'new'")
            await db.execute("UPDATE preorders SET status = 'advance_paid' WHERE status = 'paid'")
            await db.execute("UPDATE preorders SET status = 'refund_requested' WHERE status = 'refund'")
            await db.execute("UPDATE preorders SET status = 'cancelled_unpaid' WHERE status = 'cancelled'")
            await db.execute("INSERT INTO settings(key, value) VALUES('migrated_v2', '1')")

        for key, value in (("batch_size", str(BATCH_SIZE)), ("campaign_status", "open")):
            await db.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (key, value))

        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(timespec="seconds")
        await db.execute("DELETE FROM fsm WHERE updated_at < ?", (cutoff,))
        await db.commit()

    logger.info("Database initialized: %s", DB_PATH)


class SQLiteStorage(BaseStorage):
    """FSM-хранилище в SQLite: анкеты переживают перезапуск бота."""

    @staticmethod
    def _key(key: StorageKey) -> str:
        return (
            f"{key.bot_id}:{key.chat_id}:{key.user_id}:"
            f"{key.thread_id or 0}:{getattr(key, 'business_connection_id', None) or ''}:{key.destiny}"
        )

    async def set_state(self, key: StorageKey, state: Optional[Union[str, State]] = None) -> None:
        value = state.state if isinstance(state, State) else state
        async with db_connect() as db:
            await db.execute(
                """
                INSERT INTO fsm(key, state, data, updated_at) VALUES (?, ?, '{}', ?)
                ON CONFLICT(key) DO UPDATE SET
                    state = excluded.state, updated_at = excluded.updated_at
                """,
                (self._key(key), value, now_iso()),
            )
            await db.execute(
                "DELETE FROM fsm WHERE key = ? AND state IS NULL AND data = '{}'", (self._key(key),)
            )
            await db.commit()

    async def get_state(self, key: StorageKey) -> Optional[str]:
        row = await fetch_one("SELECT state FROM fsm WHERE key = ?", (self._key(key),))
        return row["state"] if row else None

    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False)
        async with db_connect() as db:
            await db.execute(
                """
                INSERT INTO fsm(key, state, data, updated_at) VALUES (?, NULL, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    data = excluded.data, updated_at = excluded.updated_at
                """,
                (self._key(key), payload, now_iso()),
            )
            await db.execute(
                "DELETE FROM fsm WHERE key = ? AND state IS NULL AND data = '{}'", (self._key(key),)
            )
            await db.commit()

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        row = await fetch_one("SELECT data FROM fsm WHERE key = ?", (self._key(key),))
        if not row:
            return {}
        try:
            return json.loads(row["data"])
        except (TypeError, ValueError):
            return {}

    async def close(self) -> None:
        return None


# ---------- settings / progress ----------

async def get_setting(key: str, default=None):
    row = await fetch_one("SELECT value FROM settings WHERE key = ?", (key,))
    return row["value"] if row else default


async def set_setting(key: str, value) -> None:
    await execute(
        """
        INSERT INTO settings(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, str(value)),
    )


async def get_progress() -> Dict[str, Any]:
    """Единственный источник цифр — и для бота, и для публичного API."""
    reserved = await scalar(f"SELECT COALESCE(SUM(quantity), 0) FROM preorders WHERE {COUNTED_SQL}")
    batch = max(1, int(await get_setting("batch_size", BATCH_SIZE)))
    campaign = await get_setting("campaign_status", "open")
    remaining = max(0, batch - reserved)
    return {
        "batch": batch,
        "reserved": reserved,
        "remaining": remaining,
        "percent": min(100, int(reserved * 100 / batch)),
        "campaign": campaign,
        "open": campaign == "open" and remaining > 0,
    }


async def active_holds(exclude_order: int = 0) -> int:
    """Экземпляры, временно удерживаемые под оплату (неистёкшие счета)."""
    return await scalar(
        "SELECT COALESCE(SUM(quantity), 0) FROM preorders "
        "WHERE status = 'awaiting_payment' AND payment_expires_at > ? AND id != ?",
        (now_iso(), exclude_order),
    )


async def audit(actor_id: int, action: str, entity: str, entity_id: int, details: str = "") -> None:
    await execute(
        """
        INSERT INTO audit_log(admin_id, action, entity_type, entity_id, details, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (actor_id, action, entity, entity_id, details, now_iso()),
    )


# ---------- users / orders / designs ----------

async def register_user(user) -> None:
    await execute(
        """
        INSERT INTO users(telegram_id, username, first_name, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            username = excluded.username, first_name = excluded.first_name
        """,
        (user.id, user.username, user.first_name or "", now_iso()),
    )


async def save_consents(user_id: int, phone: str, news: bool) -> None:
    await execute(
        """
        UPDATE users SET phone = ?, consent_offer = 1, consent_pd = 1,
            consent_news = ?, consents_at = ?
        WHERE telegram_id = ?
        """,
        (phone, 1 if news else 0, now_iso(), user_id),
    )


def profile_name(user) -> str:
    return (user.full_name or user.first_name or "Без имени").strip()[:100]


async def ensure_draft(user, data: Dict[str, Any]) -> Tuple[int, str]:
    """Создаёт черновик заказа (чтобы в сводке был номер) или обновляет существующий."""
    ts = now_iso()
    qty = data["quantity"]
    order_id = data.get("order_id")
    async with db_connect() as db:
        if order_id:
            cursor = await db.execute(
                """
                UPDATE preorders SET phone = ?, city = ?, delivery = ?, quantity = ?,
                    amount_total = ?, amount_advance = ?, updated_at = ?
                WHERE id = ? AND telegram_id = ? AND status = 'draft'
                """,
                (data["phone"], data["city"], data["delivery"], qty, PRICE * qty,
                 ADVANCE * qty, ts, order_id, user.id),
            )
            if cursor.rowcount:
                await db.commit()
                cursor = await db.execute("SELECT public_id FROM preorders WHERE id = ?", (order_id,))
                return order_id, (await cursor.fetchone())["public_id"]

        cursor = await db.execute(
            """
            INSERT INTO preorders(
                telegram_id, username, name, phone, city, delivery, quantity,
                amount_total, amount_advance, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)
            """,
            (user.id, user.username, profile_name(user), data["phone"], data["city"],
             data["delivery"], qty, PRICE * qty, ADVANCE * qty, ts, ts),
        )
        order_id = cursor.lastrowid
        public_id = f"REDO-{order_id:05d}"
        await db.execute("UPDATE preorders SET public_id = ? WHERE id = ?", (public_id, order_id))
        await db.commit()
    return order_id, public_id


async def set_order_status(order_id: int, new_status: str, actor_id: int, details: str = "",
                           allowed_from: Optional[Tuple[str, ...]] = None) -> bool:
    """Единая точка смены статуса: обновление + запись в audit_log."""
    async with db_connect() as db:
        cursor = await db.execute("SELECT status FROM preorders WHERE id = ?", (order_id,))
        row = await cursor.fetchone()
        if not row:
            return False
        old = row["status"]
        if allowed_from is not None and old not in allowed_from:
            return False
        if old == new_status:
            return True
        await db.execute(
            "UPDATE preorders SET status = ?, updated_at = ? WHERE id = ?",
            (new_status, now_iso(), order_id),
        )
        await db.execute(
            """
            INSERT INTO audit_log(admin_id, action, entity_type, entity_id, details, created_at)
            VALUES (?, ?, 'order', ?, ?, ?)
            """,
            (actor_id, f"{old}->{new_status}", order_id, details, now_iso()),
        )
        await db.commit()
    return True


async def mark_paid(order_id: int, provider: str, payment_id: str, amount: int):
    """Идемпотентная фиксация оплаты с итоговой проверкой вместимости.

    Возвращает (строка заказа, исход):
      "paid"      — оплата зафиксирована впервые, экземпляры учтены;
      "oversold"  — деньги пришли, но партия уже заполнена: заказ не учитывается, нужен возврат;
      "extra"     — заказ уже был оплачен (второй платёж): только записан, нужен ручной разбор;
      "duplicate" — повторное уведомление, ничего не меняем;
      "unknown"   — заказ не найден.
    """
    async with db_connect() as db:
        await db.execute("BEGIN IMMEDIATE")
        try:
            cursor = await db.execute(
                """
                INSERT OR IGNORE INTO payments(order_id, provider, provider_payment_id, amount, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (order_id, provider, payment_id, amount, now_iso()),
            )
            if cursor.rowcount == 0:
                await db.rollback()
                return None, "duplicate"

            cursor = await db.execute("SELECT * FROM preorders WHERE id = ?", (order_id,))
            row = await cursor.fetchone()
            if row is None:
                await db.commit()
                return None, "unknown"

            outcome = "extra"
            if row["status"] in ("draft", "awaiting_payment", "cancelled_unpaid"):
                cursor = await db.execute(
                    f"SELECT COALESCE(SUM(quantity), 0) FROM preorders WHERE {COUNTED_SQL}"
                )
                reserved = (await cursor.fetchone())[0]
                cursor = await db.execute("SELECT value FROM settings WHERE key = 'batch_size'")
                brow = await cursor.fetchone()
                batch = max(1, int(brow["value"])) if brow else BATCH_SIZE

                ts = now_iso()
                if reserved + row["quantity"] <= batch:
                    outcome = "paid"
                    await db.execute(
                        """
                        UPDATE preorders SET status = 'advance_paid', paid_at = ?, updated_at = ?,
                            payment_expires_at = NULL WHERE id = ?
                        """,
                        (ts, ts, order_id),
                    )
                    new_status = "advance_paid"
                else:
                    outcome = "oversold"
                    await db.execute(
                        """
                        UPDATE preorders SET status = 'refund_requested', oversold = 1, paid_at = ?,
                            updated_at = ?, payment_expires_at = NULL WHERE id = ?
                        """,
                        (ts, ts, order_id),
                    )
                    new_status = "refund_requested"
                await db.execute(
                    """
                    INSERT INTO audit_log(admin_id, action, entity_type, entity_id, details, created_at)
                    VALUES (0, ?, 'order', ?, ?, ?)
                    """,
                    (f"{row['status']}->{new_status}", order_id,
                     f"payment {provider}:{payment_id} outcome={outcome}", ts),
                )
            await db.commit()
        except Exception:
            await db.rollback()
            raise

        cursor = await db.execute("SELECT * FROM preorders WHERE id = ?", (order_id,))
        return await cursor.fetchone(), outcome


async def create_design(user, data):
    ts = now_iso()
    async with db_connect() as db:
        cursor = await db.execute(
            """
            INSERT INTO designs(
                telegram_id, username, name, file_id, file_type,
                description, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'new', ?, ?)
            """,
            (user.id, user.username, profile_name(user), data.get("file_id", ""),
             data.get("file_type", "none"), data.get("description", ""), ts, ts),
        )
        design_id = cursor.lastrowid
        public_id = f"DSN-{design_id:05d}"
        await db.execute("UPDATE designs SET public_id = ? WHERE id = ?", (public_id, design_id))
        await db.commit()
    return design_id, public_id


async def get_order(order_id: int):
    return await fetch_one("SELECT * FROM preorders WHERE id = ?", (order_id,))


async def get_design(design_id: int):
    return await fetch_one("SELECT * FROM designs WHERE id = ?", (design_id,))


async def order_money(order_id: int) -> Tuple[int, int]:
    paid = await scalar("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE order_id = ?", (order_id,))
    refunded = await scalar(
        "SELECT COALESCE(SUM(amount), 0) FROM refunds WHERE order_id = ? AND status = 'confirmed'",
        (order_id,),
    )
    return paid, refunded


async def change_design_status(design_id: int, status: str, admin_id: int) -> bool:
    if status not in DESIGN_STATUSES:
        return False
    async with db_connect() as db:
        cursor = await db.execute("SELECT status FROM designs WHERE id = ?", (design_id,))
        row = await cursor.fetchone()
        if not row:
            return False
        old = row["status"]
        if old == status:
            return True
        await db.execute(
            "UPDATE designs SET status = ?, updated_at = ? WHERE id = ?", (status, now_iso(), design_id)
        )
        await db.execute(
            """
            INSERT INTO audit_log(admin_id, action, entity_type, entity_id, details, created_at)
            VALUES (?, ?, 'design', ?, ?, ?)
            """,
            (admin_id, f"{old}->{status}", design_id, "design status", now_iso()),
        )
        await db.commit()
    return True


# ============================================================
# HELPERS / ТЕКСТЫ
# ============================================================

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def user_link(telegram_id: int, username: Optional[str] = None) -> str:
    if username:
        return f"@{esc(username)}"
    return f'<a href="tg://user?id={telegram_id}">профиль</a>'


def contact_url(telegram_id: int, username: Optional[str]) -> str:
    return f"https://t.me/{username}" if username else f"tg://user?id={telegram_id}"


def delivery_label(key: str) -> str:
    return DELIVERY.get(key, key)


def preorder_state_text(p: Dict[str, Any]) -> str:
    if p["campaign"] == "closed":
        return "закрыт"
    if p["campaign"] == "paused":
        return "приостановлен"
    if p["remaining"] == 0:
        return "партия собрана"
    return "открыт"


async def notify_admins(bot: Bot, text: str, reply_markup=None) -> None:
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=reply_markup)
        except TelegramAPIError:
            logger.warning("Could not notify admin %s", admin_id)


async def clear_markup(callback: CallbackQuery) -> None:
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except (TelegramAPIError, AttributeError):
        pass


def kb(rows: List[List[Optional[InlineKeyboardButton]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[b for b in row if b is not None] for row in rows if any(row)]
    )


def deadline_lines() -> List[Optional[str]]:
    return [
        opt("Дата принятия решения о запуске:", esc(LAUNCH_DEADLINE)),
        opt("Ориентировочная отправка:", esc(ESTIMATED_SHIPPING)),
    ]


def welcome_text(p: Dict[str, Any]) -> str:
    return compose(
        "👋 <b>Добро пожаловать в Plush REDO Club!</b>",
        "Мы создаём фанатскую плюшевую версию REDO.",
        f"Первая партия — {p['batch']} экземпляров.",
        f"Цена — {rub(PRICE)}.",
        f"Сейчас оплачивается аванс {rub(ADVANCE)} за штуку, остаток — перед отправкой. Доставка отдельно.",
        "",
        "Физического прототипа пока нет: изображения показывают концепт будущей игрушки.",
        "",
        f"Аванс внесён за {p['reserved']} из {p['batch']} экземпляров.",
        f"Доступно для предзаказа: {p['remaining']}.",
        "",
        "Что хотите сделать?",
    )


def progress_text(p: Dict[str, Any]) -> str:
    if p["campaign"] == "paused":
        return compose(
            "⏸ Приём новых предзаказов временно приостановлен.",
            "Ранее оформленные заказы доступны в разделе «Мои заказы». О возобновлении приёма сообщим в канале проекта.",
        )
    if p["campaign"] == "closed" or p["remaining"] == 0:
        return compose(
            "🎉 <b>Первая партия собрана!</b>" if p["remaining"] == 0 else "Приём новых предзаказов закрыт.",
            f"Аванс внесён за {p['reserved']} из {p['batch']} экземпляров. Новые предзаказы пока не принимаем.",
            "Если у вас уже есть заказ, следить за ним можно в разделе «Мои заказы».",
            "Новости о прототипе и производстве публикуем в канале.",
        )
    return compose(
        "📊 <b>Первая партия REDO</b>",
        f"Аванс внесён за {p['reserved']} из {p['batch']} экземпляров.",
        f"Доступно: {p['remaining']}.",
        "",
        "В счётчике учитываются только экземпляры с подтверждённой оплатой аванса. "
        "Неоплаченные заявки не учитываются.",
    )


def about_text(p: Dict[str, Any]) -> str:
    return compose(
        "ℹ️ <b>Plush REDO Club</b>",
        "Фанатский проект по созданию коллекционной плюшевой версии REDO.",
        "",
        f"Планируемый первый тираж — {p['batch']} экземпляров. Сейчас существует концепт дизайна. "
        "Физического прототипа пока нет.",
        "",
        f"Цена — {rub(PRICE)}.",
        f"Аванс — {rub(ADVANCE)}, входит в цену.",
        f"Остаток — {rub(REMAINDER)} перед отправкой.",
        "Доставка отдельно.",
        "",
        *deadline_lines(),
        "Если проект не будет запущен до установленной даты, внесённый аванс возвращается полностью "
        "в течение 14 календарных дней.",
        "",
        "Проект не является официальной продукцией REDO или Telegram.",
    )


def support_text() -> str:
    return compose(
        "💬 <b>Поддержка Plush REDO Club</b>",
        "По вопросам заказа, оплаты и возврата:",
        esc(SUPPORT_CONTACT) if SUPPORT_CONTACT else "Напишите нам через канал проекта.",
        "Если обращение связано с заказом, укажите его номер.",
        "Не присылайте пароль, код из SMS или полные данные банковской карты.",
    )


async def send_welcome(message: Message) -> None:
    p = await get_progress()
    await send_scene(message, "welcome", welcome_text(p), MAIN_MENU)


# ---------- заказы: тексты ----------

def order_card(row, paid: int, refunded: int, admin: bool = False) -> str:
    if admin:
        pay_state = "не оплачен"
        if paid:
            pay_state = "оплачен"
        if refunded and refunded < paid:
            pay_state = "возврат частично"
        elif refunded and refunded >= paid:
            pay_state = "возвращён"
        elif row["status"] == "refund_processing":
            pay_state = "возврат оформляется"
        return compose(
            f"🧸 <b>Заказ {row['public_id']}</b>",
            f"Пользователь: {esc(row['name'])} ({user_link(row['telegram_id'], row['username'])})",
            f"Telegram ID: <code>{row['telegram_id']}</code>",
            f"Телефон: {esc(row['phone'])}",
            f"Количество: {row['quantity']}",
            f"Город: {esc(row['city'])}",
            f"Доставка: {esc(delivery_label(row['delivery']))}",
            f"Стоимость игрушек: {rub(row['amount_total'])}.",
            f"Получено: {rub(paid)}.",
            f"Возвращено: {rub(refunded)}.",
            f"Статус заказа: {status_label(row['status'])}"
            + (" (архив)" if row["archived"] else "")
            + (" ⚠️ превышение партии" if row["oversold"] else ""),
            f"Статус оплаты: {pay_state}",
            f"Создан: {fmt_dt(row['created_at'])}",
            f"Обновлён: {fmt_dt(row['updated_at'])}",
        )

    outstanding = max(0, row["amount_total"] - paid)
    show_outstanding = row["status"] not in ("cancelled_unpaid", "refunded", "refund_requested", "refund_processing")
    return compose(
        f"🧸 <b>Заказ {row['public_id']}</b>",
        f"Статус: {status_label(row['status'])}",
        f"Количество: {row['quantity']}",
        f"Стоимость игрушек: {rub(row['amount_total'])}.",
        f"Получено: {rub(paid)}.",
        f"К доплате за игрушки: {rub(outstanding)}." if show_outstanding else None,
        f"Возвращено: {rub(refunded)}." if refunded else None,
        f"Город: {esc(row['city'])}",
        f"Доставка: {esc(delivery_label(row['delivery']))}",
        "Доставка оплачивается отдельно.",
    )


async def user_order_keyboard(row) -> InlineKeyboardMarkup:
    oid = row["id"]
    rows: List[List[Optional[InlineKeyboardButton]]] = []
    if row["status"] in UNPAID_STATUSES:
        rows.append([InlineKeyboardButton(text="💳 Оплатить аванс", callback_data=f"pay:{oid}")])
        rows.append([InlineKeyboardButton(text="✖️ Отменить заказ", callback_data=f"ocancel:{oid}")])
    elif row["status"] in PAID_STATUSES:
        rows.append([InlineKeyboardButton(text="💬 Вопрос по заказу", callback_data=f"ask:{oid}")])
        if row["status"] in REFUNDABLE:
            rows.append([InlineKeyboardButton(text="↩️ Запросить возврат", callback_data=f"ref:{oid}")])
    rows.append([InlineKeyboardButton(text="⬅️ К списку заказов", callback_data="ords")])
    return kb(rows)


async def send_user_order(message: Message, order_id: int, user_id: int) -> None:
    row = await get_order(order_id)
    if not row or row["telegram_id"] != user_id or row["status"] == "draft":
        await message.answer(
            "Эта кнопка больше неактуальна. Откройте заказ заново, чтобы увидеть текущий статус.",
            reply_markup=MAIN_MENU,
        )
        return
    paid, refunded = await order_money(order_id)
    await message.answer(order_card(row, paid, refunded), reply_markup=await user_order_keyboard(row))


def advance_paid_text(row) -> str:
    return compose(
        "✅ <b>Аванс получен. Спасибо за предзаказ!</b>",
        f"Заказ: {row['public_id']}",
        f"Количество: {row['quantity']}",
        f"Оплачено: {rub(row['amount_advance'])}.",
        f"Остаток перед отправкой: {rub(row['amount_total'] - row['amount_advance'])}.",
        "Доставка отдельно.",
        f"Статус: {status_label('advance_paid').lower()}.",
        "",
        "Ваши экземпляры учтены в первой партии. Заказ доступен в разделе «Мои заказы».",
        "Физического прототипа пока нет. Новости о его создании и запуске производства публикуем в канале проекта.",
    )


def advance_paid_keyboard(row) -> InlineKeyboardMarkup:
    return kb([
        [InlineKeyboardButton(text="🧸 Мой заказ", callback_data=f"ord:{row['id']}")],
        [url_button("📣 Канал проекта", CHANNEL_URL)],
        [HOME_BUTTON],
    ])


def admin_keyboard() -> InlineKeyboardMarkup:
    return kb([
        [InlineKeyboardButton(text="🧸 Заказы", callback_data="adm:orders"),
         InlineKeyboardButton(text="💳 Платежи", callback_data="adm:payments")],
        [InlineKeyboardButton(text="↩️ Запросы возврата", callback_data="adm:refunds"),
         InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="adm:settings"),
         InlineKeyboardButton(text="🎨 Идеи дизайна", callback_data="adm:designs")],
        [InlineKeyboardButton(text="🏠 Пользовательское меню", callback_data="menu:home")],
    ])


def admin_order_keyboard(row) -> InlineKeyboardMarkup:
    oid, st = row["id"], row["status"]
    rows: List[List[Optional[InlineKeyboardButton]]] = []
    if st == "advance_paid":
        rows.append([InlineKeyboardButton(text="✅ Принять в работу", callback_data=f"adm:st:{oid}:accepted")])
    if st in UNPAID_STATUSES:
        rows.append([InlineKeyboardButton(text="💰 Отметить оплаченным (вручную)", callback_data=f"adm:manual:{oid}")])
    if st not in ("cancelled_unpaid", "refunded"):
        rows.append([InlineKeyboardButton(text="❌ Отклонить", callback_data=f"adm:rej:{oid}")])
    if st in PAID_STATUSES and st not in ("refund_processing", "refunded"):
        rows.append([InlineKeyboardButton(text="↩️ Оформить возврат", callback_data=f"adm:arf:{oid}")])
    if st == "refund_processing":
        rows.append([InlineKeyboardButton(text="✅ Подтвердить возврат", callback_data=f"adm:arok:{oid}")])
    if st in PAID_STATUSES and st not in ("refund_requested", "refund_processing"):
        rows.append([InlineKeyboardButton(text="🔄 Сменить статус", callback_data=f"adm:stm:{oid}")])
    rows.append([
        InlineKeyboardButton(
            text="📤 Из архива" if row["archived"] else "📦 В архив",
            callback_data=f"adm:arch:{oid}",
        ),
        InlineKeyboardButton(text="🕘 История изменений", callback_data=f"adm:hist:{oid}"),
    ])
    rows.append([InlineKeyboardButton(text="✉️ Связаться с покупателем",
                                      url=contact_url(row["telegram_id"], row["username"]))])
    rows.append([InlineKeyboardButton(text="⬅️ Админ-меню", callback_data="adm:home")])
    return kb(rows)


async def send_admin_order(message: Message, order_id: int) -> None:
    row = await get_order(order_id)
    if not row:
        await message.answer("Заказ не найден.")
        return
    paid, refunded = await order_money(order_id)
    await message.answer(order_card(row, paid, refunded, admin=True), reply_markup=admin_order_keyboard(row))


async def send_design_card(bot: Bot, chat_id: int, row, markup=None) -> None:
    caption = compose(
        f"🎨 <b>{esc(row['public_id'])}</b>",
        "",
        f"Автор: {esc(row['name'])}",
        f"Контакт: {user_link(row['telegram_id'], row['username'])}",
        f"Telegram ID: <code>{row['telegram_id']}</code>",
        f"Описание: {esc(row['description'])}" if row["description"] else None,
        f"Статус: {DESIGN_STATUS_LABELS.get(row['status'], row['status'])}",
    )
    if row["file_type"] == "document" and row["file_id"]:
        await bot.send_document(chat_id, document=row["file_id"], caption=caption, reply_markup=markup)
    elif row["file_type"] == "photo" and row["file_id"]:
        await bot.send_photo(chat_id, photo=row["file_id"], caption=caption, reply_markup=markup)
    else:
        await bot.send_message(chat_id, caption, reply_markup=markup)


def design_admin_keyboard(row) -> InlineKeyboardMarkup:
    rows = []
    for label, st in (("🔎 Рассматривается", "reviewing"), ("✅ Принять", "accepted"),
                      ("❌ Отклонить", "rejected"), ("📦 Архивировать", "archived")):
        if st != row["status"]:
            rows.append([InlineKeyboardButton(text=label, callback_data=f"adm:ds:{row['id']}:{st}")])
    rows.append([InlineKeyboardButton(text="⬅️ Админ-меню", callback_data="adm:home")])
    return kb(rows)


# ============================================================
# ОТМЕНА / ВОЗВРАТ ПО ШАГАМ (первыми — работают в любом состоянии)
# ============================================================

async def abandon_form(state: FSMContext, user_id: int) -> None:
    data = await state.get_data()
    order_id = data.get("order_id")
    await state.clear()
    if order_id:
        await set_order_status(
            order_id, "cancelled_unpaid", user_id, "form cancelled by user",
            allowed_from=("draft",),
        )


async def cancel_flow(message: Message, state: FSMContext, user_id: int) -> None:
    await abandon_form(state, user_id)
    await message.answer(
        "Оформление отменено. Вернуться к нему можно в любой момент через меню.",
        reply_markup=MAIN_MENU,
    )


@router.callback_query(F.data == "cancel_form")
async def cancel_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Отменено.")
    await clear_markup(callback)
    await cancel_flow(callback.message, state, callback.from_user.id)


@router.message(F.text.in_({"Отмена", BTN_CANCEL_TEXT, "✖️ Отмена"}))
async def cancel_text(message: Message, state: FSMContext):
    await cancel_flow(message, state, message.from_user.id)


@router.message(Command("cancel"))
async def cancel_command(message: Message, state: FSMContext):
    await cancel_flow(message, state, message.from_user.id)


# ============================================================
# START / MENU / ИНФОРМАЦИОННЫЕ РАЗДЕЛЫ
# ============================================================

@router.message(CommandStart())
async def start(message: Message, command: CommandObject, state: FSMContext):
    await register_user(message.from_user)

    # deeplink с сайта: t.me/<bot>?start=preorder → сразу воронка, без длинного приветствия
    if (command.args or "").strip().lower() == "preorder":
        await begin_preorder(message, state, message.from_user)
        return

    await state.set_state(None)
    await send_welcome(message)


@router.message(Command("menu"))
async def menu_command(message: Message, state: FSMContext):
    await state.set_state(None)
    await send_welcome(message)


@router.callback_query(F.data == "menu:home")
async def menu_home(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(None)
    await send_welcome(callback.message)


@router.message(Command("help"))
async def help_command(message: Message):
    await message.answer(
        "/start — начать\n/menu — главное меню\n/cancel — отменить оформление\n"
        "/unsubscribe — отключить новости\n/help — помощь",
        reply_markup=MAIN_MENU,
    )


@router.message(Command("unsubscribe"))
async def unsubscribe_command(message: Message):
    await execute("UPDATE users SET consent_news = 0 WHERE telegram_id = ?", (message.from_user.id,))
    await message.answer(
        "Вы отписались от новостей и предложений проекта.\n"
        "Сообщения по вашим действующим заказам будут приходить отдельно.",
        reply_markup=MAIN_MENU,
    )


@router.message(Command("admin"))
async def admin_command(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет доступа к этому разделу.")
        return
    await state.clear()
    await send_admin_home(message)


async def send_admin_home(message: Message) -> None:
    p = await get_progress()
    await message.answer(
        compose(
            "🛠 <b>Plush REDO Club · Админ-панель</b>",
            f"Подтверждённые авансы: {p['reserved']} из {p['batch']} экземпляров.",
            f"Доступно: {p['remaining']}.",
            f"Приём предзаказов: {preorder_state_text(p)}.",
        ),
        reply_markup=admin_keyboard(),
    )


@router.message(F.text.startswith("/"))
async def unknown_command(message: Message):
    await message.answer(
        "Не совсем понял сообщение. Выберите действие в меню ниже.\n"
        "Если нужен человек, нажмите «Поддержка».",
        reply_markup=MAIN_MENU,
    )


async def show_progress(message: Message) -> None:
    p = await get_progress()
    rows: List[List[Optional[InlineKeyboardButton]]] = []
    if p["open"]:
        rows.append([InlineKeyboardButton(text="🧸 Оформить предзаказ", callback_data="preorder:start")])
    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="progress:refresh")])
    rows.append([HOME_BUTTON])
    key = "closed" if not p["open"] else "progress"
    await send_scene(message, key, progress_text(p), kb(rows))


@router.message(F.text.in_({BTN_PROGRESS, "📊 Прогресс"}))
async def progress_handler(message: Message, state: FSMContext):
    await state.set_state(None)
    await show_progress(message)


@router.callback_query(F.data == "progress:refresh")
async def progress_refresh(callback: CallbackQuery):
    p = await get_progress()
    rows: List[List[Optional[InlineKeyboardButton]]] = []
    if p["open"]:
        rows.append([InlineKeyboardButton(text="🧸 Оформить предзаказ", callback_data="preorder:start")])
    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="progress:refresh")])
    rows.append([HOME_BUTTON])
    text = progress_text(p)
    try:
        if callback.message.photo:
            await callback.message.edit_caption(caption=text, reply_markup=kb(rows))
        else:
            await callback.message.edit_text(text, reply_markup=kb(rows))
        await callback.answer("Обновлено.")
    except TelegramAPIError:
        await callback.answer("Без изменений.")


@router.message(F.text == BTN_ABOUT)
async def about_handler(message: Message, state: FSMContext):
    await state.set_state(None)
    p = await get_progress()
    markup = kb([
        [url_button("🌐 Сайт", SITE_URL), url_button("📄 Оферта", OFFER_URL)],
        [url_button("🔒 Политика ПДн", PRIVACY_URL), url_button("↩️ Условия возврата", RETURNS_URL)],
        [url_button("📣 Канал проекта", CHANNEL_URL)],
        [InlineKeyboardButton(text="🧸 Оформить предзаказ", callback_data="preorder:start")],
        [HOME_BUTTON],
    ])
    await send_scene(message, "about", about_text(p), markup)


@router.message(F.text == BTN_SUPPORT)
async def support_handler(message: Message, state: FSMContext):
    await state.set_state(None)
    await message.answer(
        support_text(),
        reply_markup=kb([
            [InlineKeyboardButton(text="📋 Мои заказы", callback_data="ords")],
            [url_button("🌐 Сайт", SITE_URL)],
            [HOME_BUTTON],
        ]),
    )


# ---------- Мои заказы ----------

async def show_orders(message: Message, user_id: int) -> None:
    rows = await fetch_all(
        "SELECT * FROM preorders WHERE telegram_id = ? AND status != 'draft' ORDER BY id DESC LIMIT 10",
        (user_id,),
    )
    if not rows:
        await send_scene(
            message, "orders",
            compose(
                "📋 У вас пока нет заказов.",
                "Здесь будут отображаться ваши предзаказы, суммы и текущие статусы.",
            ),
            kb([[InlineKeyboardButton(text="🧸 Оформить предзаказ", callback_data="preorder:start")], [HOME_BUTTON]]),
        )
        return

    builder = InlineKeyboardBuilder()
    for row in rows:
        builder.button(
            text=f"{row['public_id']} · {row['quantity']} шт. · {status_label(row['status'])}",
            callback_data=f"ord:{row['id']}",
        )
    builder.adjust(1)
    builder.row(HOME_BUTTON)
    await send_scene(
        message, "orders",
        "📋 <b>Мои заказы</b>\nВыберите заказ, чтобы посмотреть подробности.",
        builder.as_markup(),
    )


@router.message(F.text.in_({BTN_MY, "📋 Мои заявки"}))
async def my_orders(message: Message, state: FSMContext):
    await state.set_state(None)
    await show_orders(message, message.from_user.id)


@router.callback_query(F.data == "ords")
async def my_orders_cb(callback: CallbackQuery):
    await callback.answer()
    await show_orders(callback.message, callback.from_user.id)


@router.callback_query(F.data.startswith("ord:"))
async def my_order_card(callback: CallbackQuery):
    await callback.answer()
    await send_user_order(callback.message, int(callback.data.split(":")[1]), callback.from_user.id)


@router.callback_query(F.data.startswith("ask:"))
async def my_order_question(callback: CallbackQuery):
    await callback.answer()
    row = await get_order(int(callback.data.split(":")[1]))
    tail = f"\nУкажите номер заказа: {row['public_id']}." if row and row["telegram_id"] == callback.from_user.id else ""
    await callback.message.answer(support_text() + tail, reply_markup=MAIN_MENU)


@router.callback_query(F.data.startswith("ocancel:"))
async def my_cancel_ask(callback: CallbackQuery):
    order_id = int(callback.data.split(":")[1])
    row = await get_order(order_id)
    if not row or row["telegram_id"] != callback.from_user.id or row["status"] not in UNPAID_STATUSES:
        await callback.answer()
        await send_user_order(callback.message, order_id, callback.from_user.id)
        return
    await callback.answer()
    await callback.message.answer(
        compose(
            f"Отменить неоплаченный заказ {row['public_id']}?",
            "Если захотите вернуться, сможете оформить новый — при наличии доступных экземпляров.",
        ),
        reply_markup=kb([
            [InlineKeyboardButton(text="Да, отменить", callback_data=f"ocancelok:{order_id}")],
            [InlineKeyboardButton(text="Вернуться к заказу", callback_data=f"ord:{order_id}")],
        ]),
    )


@router.callback_query(F.data.startswith("ocancelok:"))
async def my_cancel_do(callback: CallbackQuery):
    order_id = int(callback.data.split(":")[1])
    row = await get_order(order_id)
    if not row or row["telegram_id"] != callback.from_user.id:
        await callback.answer("Заказ недоступен.", show_alert=True)
        return

    changed = await set_order_status(
        order_id, "cancelled_unpaid", callback.from_user.id, "cancelled by user",
        allowed_from=UNPAID_STATUSES,
    )
    await callback.answer()
    await clear_markup(callback)
    if changed:
        await callback.message.answer(
            compose(f"Заказ {row['public_id']} отменён.", "Подтверждённой оплаты по нему нет."),
            reply_markup=MAIN_MENU,
        )
    else:
        # оплата пришла одновременно с отменой — показываем уже оплаченный заказ
        await send_user_order(callback.message, order_id, callback.from_user.id)


# ---------- Запрос возврата ----------

@router.callback_query(F.data.startswith("ref:"))
async def refund_start(callback: CallbackQuery):
    order_id = int(callback.data.split(":")[1])
    row = await get_order(order_id)
    if not row or row["telegram_id"] != callback.from_user.id or row["status"] not in REFUNDABLE:
        await callback.answer("Возврат по этому заказу сейчас недоступен.", show_alert=True)
        return
    await callback.answer()
    await callback.message.answer(
        compose(
            f"↩️ <b>Возврат по заказу {row['public_id']}</b>",
            "Вы можете направить запрос на отказ от заказа и возврат средств.",
            "Подтвердите отправку запроса — мы проверим оплату и свяжемся с вами для оформления возврата.",
        ),
        reply_markup=kb([
            [InlineKeyboardButton(text="Отправить запрос на возврат", callback_data=f"refgo:{order_id}")],
            [InlineKeyboardButton(text="Вернуться к заказу", callback_data=f"ord:{order_id}")],
        ]),
    )


@router.callback_query(F.data.startswith("refgo:"))
async def refund_comment(callback: CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split(":")[1])
    row = await get_order(order_id)
    if not row or row["telegram_id"] != callback.from_user.id or row["status"] not in REFUNDABLE:
        await callback.answer("Возврат по этому заказу сейчас недоступен.", show_alert=True)
        return
    await callback.answer()
    await clear_markup(callback)
    await state.set_state(RefundFlow.comment)
    await state.update_data(refund_order=order_id)
    await callback.message.answer(
        "Если хотите, напишите комментарий к обращению. Это необязательно.",
        reply_markup=kb([[InlineKeyboardButton(text="Пропустить", callback_data="refskip")],
                         [InlineKeyboardButton(text="Вернуться к заказу", callback_data=f"ord:{order_id}")]]),
    )


async def submit_refund_request(message: Message, state: FSMContext, user_id: int,
                                bot: Bot, comment: str) -> None:
    data = await state.get_data()
    await state.clear()
    order_id = data.get("refund_order")
    row = await get_order(order_id) if order_id else None
    if not row or row["telegram_id"] != user_id:
        await message.answer(
            "Эта кнопка больше неактуальна. Откройте заказ заново, чтобы увидеть текущий статус.",
            reply_markup=MAIN_MENU,
        )
        return

    changed = await set_order_status(
        order_id, "refund_requested", user_id, "refund requested by customer",
        allowed_from=REFUNDABLE,
    )
    if not changed:
        await send_user_order(message, order_id, user_id)
        return

    await execute(
        "INSERT INTO refund_requests(order_id, comment, requested_at) VALUES (?, ?, ?)",
        (order_id, comment or None, now_iso()),
    )
    paid, _ = await order_money(order_id)
    await message.answer(
        compose(
            f"Запрос на возврат по заказу {row['public_id']} получен.",
            f"Сумма полученной оплаты: {rub(paid)}.",
            "Мы свяжемся с вами, если потребуются дополнительные сведения. Условия и сроки возврата:",
            RETURNS_URL or None,
            f"Для связи: {esc(SUPPORT_CONTACT)}." if SUPPORT_CONTACT else None,
        ),
        reply_markup=MAIN_MENU,
    )
    await notify_admins(
        bot,
        compose(
            "↩️ <b>Новый запрос на возврат</b>",
            f"Заказ: {row['public_id']}",
            f"Покупатель: {esc(row['name'])}",
            f"Количество: {row['quantity']}",
            f"Получено: {rub(paid)}.",
            "Комментарий:",
            esc(comment) if comment else "—",
            f"Запрос поступил: {fmt_dt(now_iso())}.",
        ),
        kb([[InlineKeyboardButton(text="↩️ Оформить возврат", callback_data=f"adm:arf:{order_id}")],
            [InlineKeyboardButton(text="🧸 Карточка заказа", callback_data=f"adm:ord:{order_id}")]]),
    )


@router.callback_query(RefundFlow.comment, F.data == "refskip")
async def refund_skip(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    await clear_markup(callback)
    await submit_refund_request(callback.message, state, callback.from_user.id, bot, "")


@router.message(RefundFlow.comment)
async def refund_comment_text(message: Message, state: FSMContext, bot: Bot):
    await submit_refund_request(message, state, message.from_user.id, bot, (message.text or "").strip()[:500])


# ============================================================
# ОФОРМЛЕНИЕ ПРЕДЗАКАЗА
# ============================================================

STEP_ORDER = ["quantity", "city", "delivery", "phone", "consents", "confirm"]


def next_missing_step(data: Dict[str, Any]) -> str:
    if not data.get("quantity"):
        return "quantity"
    if not data.get("city"):
        return "city"
    if not data.get("delivery"):
        return "delivery"
    if not data.get("phone"):
        return "phone"
    if not all(data.get(k) for k, _, req in CONSENTS if req):
        return "consents"
    return "confirm"


async def render_step(message: Message, state: FSMContext, step: str, user) -> None:
    if step == "quantity":
        await ask_quantity(message, state)
    elif step == "city":
        await ask_city(message, state)
    elif step == "delivery":
        await ask_delivery(message, state)
    elif step == "phone":
        await ask_phone(message, state)
    elif step == "consents":
        await ask_consents(message, state)
    else:
        await show_confirmation(message, state, user)


def closed_text(p: Dict[str, Any]) -> str:
    return progress_text(p)


async def begin_preorder(message: Message, state: FSMContext, user) -> None:
    await register_user(user)

    missing = config_missing()
    if missing:
        logger.error("Оформление заблокировано, не настроено: %s", ", ".join(missing))
        hint = ("\n\n⚙️ (видно только админу) Не заполнено в .env: " + ", ".join(missing)) if is_admin(user.id) else ""
        await message.answer(
            "Оформление предзаказа временно недоступно. Попробуйте позже или напишите в поддержку." + hint,
            reply_markup=MAIN_MENU,
        )
        return

    p = await get_progress()
    if not p["open"]:
        await send_scene(message, "closed", closed_text(p), kb([[HOME_BUTTON]]))
        return

    unpaid = await scalar(
        "SELECT COUNT(*) FROM preorders WHERE telegram_id = ? AND status = 'awaiting_payment'",
        (user.id,),
    )
    if unpaid >= MAX_UNPAID_PER_USER:
        await message.answer(
            "У вас уже есть неоплаченные заказы. Оплатите или отмените их в разделе «Мои заказы».",
            reply_markup=kb([[InlineKeyboardButton(text="📋 Мои заказы", callback_data="ords")], [HOME_BUTTON]]),
        )
        return

    data = await state.get_data()
    if data.get("quantity"):
        await message.answer(
            "У вас осталось незавершённое оформление.\nПродолжить с того места, где остановились?",
            reply_markup=kb([
                [InlineKeyboardButton(text="Продолжить", callback_data="resume:go")],
                [InlineKeyboardButton(text="Начать заново", callback_data="resume:new")],
                [HOME_BUTTON],
            ]),
        )
        return

    await start_fresh(message, state)


async def start_fresh(message: Message, state: FSMContext) -> None:
    await state.clear()
    await send_scene(
        message, "preorder",
        compose(
            "🧸 <b>Предзаказ плюшевого REDO</b>",
            f"Цена одной игрушки — {rub(PRICE)}.",
            f"Аванс сейчас — {rub(ADVANCE)} за штуку.",
            f"Остаток — {rub(REMAINDER)} за штуку перед отправкой.",
            "Доставка оплачивается отдельно.",
            "",
            "Физического прототипа пока нет: сейчас существует концепт дизайна.",
        ),
    )
    await ask_quantity(message, state)


@router.callback_query(F.data == "resume:go")
async def resume_go(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await clear_markup(callback)
    data = await state.get_data()
    await render_step(callback.message, state, next_missing_step(data), callback.from_user)


@router.callback_query(F.data == "resume:new")
async def resume_new(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await clear_markup(callback)
    await abandon_form(state, callback.from_user.id)
    await start_fresh(callback.message, state)


@router.message(F.text.in_({BTN_PREORDER, "🧸 Оставить предзаказ"}))
async def preorder_start(message: Message, state: FSMContext):
    await state.set_state(None)
    await begin_preorder(message, state, message.from_user)


@router.callback_query(F.data == "preorder:start")
async def preorder_start_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(None)
    await begin_preorder(callback.message, state, callback.from_user)


# ---------- назад ----------

@router.callback_query(F.data == "form:back")
async def form_back_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await clear_markup(callback)
    await go_back(callback.message, state, callback.from_user)


@router.message(Preorder.phone, F.text == BTN_BACK_TEXT)
async def form_back_text(message: Message, state: FSMContext):
    await go_back(message, state, message.from_user)


async def go_back(message: Message, state: FSMContext, user) -> None:
    current = (await state.get_state() or "").split(":")[-1]
    if current in STEP_ORDER and STEP_ORDER.index(current) > 0:
        await render_step(message, state, STEP_ORDER[STEP_ORDER.index(current) - 1], user)
    else:
        await render_step(message, state, "quantity", user)


# ---------- 1. количество ----------

async def ask_quantity(message: Message, state: FSMContext) -> None:
    p = await get_progress()
    cap = min(MAX_QUANTITY, p["remaining"])
    await state.set_state(Preorder.quantity)

    builder = InlineKeyboardBuilder()
    for n in (1, 2, 3):
        if n <= cap:
            builder.button(text=str(n), callback_data=f"qty:{n}")
    builder.button(text="Другое количество", callback_data="qty:other")
    builder.adjust(3, 1)
    builder.row(CANCEL_BUTTON)
    await message.answer(
        compose(
            "🧸 <b>Сколько экземпляров хотите заказать?</b>",
            f"Цена одной игрушки — {rub(PRICE)}.",
            f"Аванс — {rub(ADVANCE)} за каждый экземпляр.",
            f"Доступно для предзаказа: {p['remaining']}.",
        ),
        reply_markup=builder.as_markup(),
    )


async def accept_quantity(message: Message, state: FSMContext, quantity: int, user) -> None:
    p = await get_progress()
    if not p["open"]:
        await state.clear()
        await message.answer(progress_text(p), reply_markup=MAIN_MENU)
        return
    cap = min(MAX_QUANTITY, p["remaining"])
    if quantity > p["remaining"] >= 1:
        await message.answer(
            f"Сейчас доступно только {p['remaining']} экземпляров.\nВыберите другое количество."
        )
        return
    if not 1 <= quantity <= cap:
        await message.answer(f"Нужно целое число от 1 до {cap}. Попробуйте ещё раз.")
        return
    await state.update_data(quantity=quantity)
    data = await state.get_data()
    # при возврате к редактированию идём к следующему незаполненному шагу, иначе к городу
    await render_step(message, state, "city", user)


@router.callback_query(Preorder.quantity, F.data.startswith("qty:"))
async def quantity_button(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":")[1]
    await callback.answer()
    if value == "other":
        await callback.message.answer("Введите количество экземпляров целым числом.")
        return
    await clear_markup(callback)
    await accept_quantity(callback.message, state, int(value), callback.from_user)


@router.message(Preorder.quantity)
async def quantity_text(message: Message, state: FSMContext):
    p = await get_progress()
    cap = min(MAX_QUANTITY, p["remaining"])
    try:
        quantity = int((message.text or "").strip())
    except ValueError:
        await message.answer(f"Нужно целое число от 1 до {cap}. Попробуйте ещё раз.")
        return
    await accept_quantity(message, state, quantity, message.from_user)


# ---------- 2. город ----------

async def ask_city(message: Message, state: FSMContext) -> None:
    await state.set_state(Preorder.city)
    await message.answer(
        compose(
            "📍 <b>В какой город планируется доставка?</b>",
            "Укажите город. Если название встречается в разных регионах, добавьте область или край.",
            "Полный адрес сейчас не нужен — запросим его перед отправкой.",
        ),
        reply_markup=kb([[BACK_BUTTON], [CANCEL_BUTTON]]),
    )


@router.message(Preorder.city)
async def city_input(message: Message, state: FSMContext, ):
    city = (message.text or "").strip()
    if not 2 <= len(city) <= 80 or not re.search(r"[A-Za-zА-Яа-яЁё]", city):
        await message.answer("Не удалось распознать город. Напишите название текстом, например: «Москва» или «Пушкино, Московская область».")
        return
    await state.update_data(city=city)
    await ask_delivery(message, state)


# ---------- 3. доставка ----------

async def ask_delivery(message: Message, state: FSMContext) -> None:
    await state.set_state(Preorder.delivery)
    builder = InlineKeyboardBuilder()
    for key, label in DELIVERY.items():
        builder.button(text=label, callback_data=f"deliv:{key}")
    builder.adjust(1)
    builder.row(BACK_BUTTON)
    builder.row(CANCEL_BUTTON)
    await message.answer(
        compose(
            "🚚 <b>Какой способ доставки вам удобнее?</b>",
            "Доставка оплачивается отдельно. Стоимость и точные условия согласуем перед отправкой.",
        ),
        reply_markup=builder.as_markup(),
    )


@router.callback_query(Preorder.delivery, F.data.startswith("deliv:"))
async def delivery_choice(callback: CallbackQuery, state: FSMContext):
    key = callback.data.split(":", 1)[1]
    if key not in DELIVERY:
        await callback.answer("Неизвестный способ.", show_alert=True)
        return
    await callback.answer()
    await clear_markup(callback)
    await state.update_data(delivery=key)
    await callback.message.answer(f"Доставка: <b>{esc(DELIVERY[key])}</b> ✅")
    await ask_phone(callback.message, state)


# ---------- 4. телефон ----------

async def ask_phone(message: Message, state: FSMContext) -> None:
    await state.set_state(Preorder.phone)
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Поделиться номером", request_contact=True)],
            [KeyboardButton(text=BTN_BACK_TEXT), KeyboardButton(text=BTN_CANCEL_TEXT)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        compose(
            "📞 <b>Укажите номер телефона для связи по заказу.</b>",
            "Можно нажать «Поделиться номером» или отправить его сообщением.",
        ),
        reply_markup=keyboard,
    )


def normalize_phone(raw: str) -> Optional[str]:
    digits = re.sub(r"\D", "", raw or "")
    if not re.fullmatch(r"[\d\s+\-()]{10,20}", raw or "") or not 10 <= len(digits) <= 15:
        return None
    if len(digits) == 11 and digits[0] == "8":
        digits = "7" + digits[1:]
    return "+" + digits


async def accept_phone(message: Message, state: FSMContext, raw: str) -> None:
    phone = normalize_phone(raw)
    if not phone:
        await message.answer("Проверьте номер телефона и отправьте его ещё раз в международном формате, например: +79991234567.")
        return
    await state.update_data(phone=phone)
    await message.answer(f"Телефон: <b>{esc(phone)}</b> ✅", reply_markup=ReplyKeyboardRemove())
    await ask_consents(message, state)


@router.message(Preorder.phone, F.contact)
async def phone_contact(message: Message, state: FSMContext):
    await accept_phone(message, state, message.contact.phone_number)


@router.message(Preorder.phone)
async def phone_text(message: Message, state: FSMContext):
    await accept_phone(message, state, (message.text or "").strip())


# ---------- 5. документы и подтверждения ----------

def consents_keyboard(data: Dict[str, Any]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    docs = [
        url_button("📄 Оферта", OFFER_URL),
        url_button("🔒 Политика ПДн", PRIVACY_URL),
        url_button("📝 Согласие на обработку ПДн", CONSENT_URL),
        url_button("↩️ Возврат", RETURNS_URL),
    ]
    for btn in docs:
        if btn:
            builder.row(btn)
    for key, label, _ in CONSENTS:
        mark = "☑" if data.get(key) else "☐"
        builder.row(InlineKeyboardButton(text=f"{mark} {label}", callback_data=f"cons:{key}"))
    builder.row(InlineKeyboardButton(text="➡️ Продолжить", callback_data="cons:next"))
    builder.row(BACK_BUTTON)
    builder.row(CANCEL_BUTTON)
    return builder.as_markup()


def consents_text() -> str:
    return compose(
        "📋 <b>Перед оплатой</b>",
        "Пожалуйста, ознакомьтесь с документами и подтвердите условия.",
        "",
        f"Цена — {rub(PRICE)} за экземпляр.",
        f"Аванс — {rub(ADVANCE)}, засчитывается в цену.",
        "Остаток оплачивается перед отправкой.",
        "Доставка отдельно.",
        "Физического прототипа пока нет. Финальные характеристики будут подтверждены после его создания.",
        *deadline_lines(),
    )


async def ask_consents(message: Message, state: FSMContext) -> None:
    await state.set_state(Preorder.consents)
    data = await state.get_data()
    await message.answer(consents_text(), reply_markup=consents_keyboard(data))


@router.callback_query(Preorder.consents, F.data.startswith("cons:"))
async def consents_toggle(callback: CallbackQuery, state: FSMContext):
    key = callback.data.split(":", 1)[1]
    data = await state.get_data()

    if key == "next":
        if not all(data.get(k) for k, _, req in CONSENTS if req):
            await callback.answer(
                "Чтобы перейти к оплате, подтвердите обязательные условия.\n"
                "Подписка на новости необязательна и не влияет на оформление заказа.",
                show_alert=True,
            )
            return
        await callback.answer()
        await clear_markup(callback)
        await show_confirmation(callback.message, state, callback.from_user)
        return

    if key not in {k for k, _, _ in CONSENTS}:
        await callback.answer()
        return

    await state.update_data(**{key: not data.get(key)})
    data = await state.get_data()
    await callback.answer()
    try:
        await callback.message.edit_reply_markup(reply_markup=consents_keyboard(data))
    except TelegramAPIError:
        pass


@router.message(Preorder.consents)
async def consents_text_input(message: Message):
    await message.answer(
        "Чтобы перейти к оплате, подтвердите обязательные условия.\n"
        "Подписка на новости необязательна и не влияет на оформление заказа."
    )


# ---------- 6. сводка ----------

async def show_confirmation(message: Message, state: FSMContext, user) -> None:
    data = await state.get_data()
    try:
        order_id, public_id = await ensure_draft(user, data)
    except Exception:
        logger.exception("Failed to save draft")
        await message.answer(
            "Не удалось выполнить действие. Попробуйте немного позже.\n"
            "Если ошибка возникла после оплаты, не платите повторно — сначала проверьте статус заказа.",
            reply_markup=MAIN_MENU,
        )
        return
    await state.update_data(order_id=order_id)
    await state.set_state(Preorder.confirm)
    qty = data["quantity"]

    await message.answer(
        compose(
            "🧾 <b>Проверьте предзаказ</b>",
            f"Заказ: {public_id}",
            "Товар: плюшевый REDO",
            f"Количество: {qty}",
            f"Город: {esc(data['city'])}",
            f"Доставка: {esc(delivery_label(data['delivery']))}",
            f"Телефон: {esc(data['phone'])}",
            "",
            f"Стоимость игрушек: {rub(PRICE * qty)}.",
            f"Аванс сейчас: {rub(ADVANCE * qty)}.",
            f"Остаток перед отправкой: {rub(REMAINDER * qty)}.",
            "Доставка оплачивается отдельно.",
            "Физического прототипа пока нет.",
            opt("Ориентировочная отправка:", esc(ESTIMATED_SHIPPING)),
            "",
            "Всё верно?",
        ),
        reply_markup=kb([
            [InlineKeyboardButton(text="💳 Перейти к оплате", callback_data="sum:pay")],
            [InlineKeyboardButton(text="✏️ Изменить данные", callback_data="sum:edit")],
            [InlineKeyboardButton(text="✖️ Отменить заказ", callback_data="cancel_form")],
        ]),
    )


@router.callback_query(Preorder.confirm, F.data == "sum:edit")
async def summary_edit(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await clear_markup(callback)
    await ask_quantity(callback.message, state)


@router.callback_query(Preorder.confirm, F.data == "sum:pay")
async def summary_pay(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    needed = ("quantity", "city", "delivery", "phone", "order_id")
    if not all(data.get(k) for k in needed) or not all(data.get(k) for k, _, req in CONSENTS if req):
        await callback.answer("Форма устарела, начните заново.", show_alert=True)
        await abandon_form(state, callback.from_user.id)
        await callback.message.answer("Форма устарела. Начните заново через меню.", reply_markup=MAIN_MENU)
        return

    if config_missing():
        logger.error("Оплата заблокирована, не настроено: %s", ", ".join(config_missing()))
        await callback.answer("Оплата временно недоступна.", show_alert=True)
        return

    p = await get_progress()
    if not p["open"] or p["remaining"] < data["quantity"]:
        await callback.answer()
        await callback.message.answer(
            f"Сейчас доступно только {p['remaining']} экземпляров.\nВыберите другое количество."
            if p["open"] else progress_text(p),
            reply_markup=MAIN_MENU,
        )
        if p["open"]:
            await ask_quantity(callback.message, state)
        return

    await state.clear()  # защита от двойного нажатия
    await clear_markup(callback)
    try:
        await save_consents(callback.from_user.id, data["phone"], bool(data.get("c_news")))
    except Exception:
        logger.exception("save_consents failed")

    order_id = data["order_id"]
    moved = await set_order_status(
        order_id, "awaiting_payment", callback.from_user.id, "moved to payment", allowed_from=("draft",)
    )
    await callback.answer()
    row = await get_order(order_id)
    if moved:
        paid, refunded = await order_money(order_id)
        await notify_admins(
            bot,
            compose(
                f"🆕 <b>Создан заказ {row['public_id']}</b>",
                f"Пользователь: {esc(row['name'])}",
                f"Telegram ID: <code>{row['telegram_id']}</code>",
                f"Количество: {row['quantity']}",
                f"Аванс к оплате: {rub(row['amount_advance'])}.",
                f"Город: {esc(row['city'])}",
                f"Доставка: {esc(delivery_label(row['delivery']))}",
                "Статус: ожидает оплаты.",
                "В счётчик партии не включён.",
            ),
            kb([[InlineKeyboardButton(text="🧸 Карточка заказа", callback_data=f"adm:ord:{order_id}")]]),
        )
    await show_payment_offer(callback.message, row)


# ============================================================
# ОПЛАТА
# ============================================================

async def show_payment_offer(message: Message, row) -> None:
    expires = row["payment_expires_at"]
    active = bool(expires and expires > now_iso())
    await send_scene(
        message, "payment",
        compose(
            "💳 <b>Оплата аванса</b>",
            f"Заказ: {row['public_id']}",
            f"Количество: {row['quantity']}",
            f"К оплате: {rub(row['amount_advance'])}.",
            "Аванс засчитывается в стоимость игрушек.",
            "Нажмите кнопку ниже, чтобы перейти к оплате.",
            f"Счёт действует до {fmt_dt(expires)}." if active else None,
        ),
        kb([
            [InlineKeyboardButton(text=f"💳 Оплатить {rub(row['amount_advance'])}", callback_data=f"pay:{row['id']}")],
            [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"chk:{row['id']}")],
            [InlineKeyboardButton(text="💬 Поддержка", callback_data="support:show")],
        ]),
    )


@router.callback_query(F.data == "support:show")
async def support_cb(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(support_text(), reply_markup=MAIN_MENU)


@router.callback_query(F.data.startswith("pay:"))
async def pay_pressed(callback: CallbackQuery, bot: Bot):
    order_id = int(callback.data.split(":")[1])
    row = await get_order(order_id)
    if not row or row["telegram_id"] != callback.from_user.id:
        await callback.answer("Эта кнопка больше неактуальна.", show_alert=True)
        return
    if row["status"] in PAID_STATUSES:
        await callback.answer()
        await send_user_order(callback.message, order_id, callback.from_user.id)
        return
    if row["status"] not in UNPAID_STATUSES:
        await callback.answer("Эта заявка недоступна для оплаты.", show_alert=True)
        return

    if config_missing():
        logger.error("Оплата заблокирована, не настроено: %s", ", ".join(config_missing()))
        await callback.answer("Оплата временно недоступна. Попробуйте позже.", show_alert=True)
        return
    if not PAYMENT_PROVIDER_TOKEN:
        await callback.answer()
        await callback.message.answer(
            "Приём оплаты ещё настраивается. Заказ сохранён — вернитесь к нему позже в разделе «Мои заказы».\n"
            + (f"Вопросы: {esc(SUPPORT_CONTACT)}" if SUPPORT_CONTACT else ""),
            reply_markup=MAIN_MENU,
        )
        return

    # доступность с учётом экземпляров, удерживаемых чужими неоплаченными счетами
    p = await get_progress()
    holds = await active_holds(exclude_order=order_id)
    available = p["remaining"] - holds
    if not p["open"] or available < row["quantity"]:
        await callback.answer()
        avail = max(0, available)
        await callback.message.answer(
            progress_text(p) if not p["open"] else
            f"Сейчас доступно только {avail} экземпляров.\nПопробуйте позже или оформите меньшее количество.",
            reply_markup=MAIN_MENU,
        )
        return

    expires = (datetime.now(timezone.utc) + timedelta(minutes=PAYMENT_TTL_MIN)).isoformat(timespec="seconds")
    await set_order_status(order_id, "awaiting_payment", callback.from_user.id, "payment requested")
    await execute("UPDATE preorders SET payment_expires_at = ? WHERE id = ?", (expires, order_id))

    await callback.answer()
    try:
        await bot.send_invoice(
            chat_id=callback.message.chat.id,
            title=f"Аванс {row['public_id']}"[:32],
            description=(
                f"Аванс за предзаказ плюшевого REDO (концепт), {row['quantity']} шт. "
                f"Аванс входит в стоимость {rub(row['amount_total'])}."
            )[:255],
            payload=f"order:{order_id}",
            provider_token=PAYMENT_PROVIDER_TOKEN,
            currency="RUB",
            prices=[LabeledPrice(label=f"Аванс × {row['quantity']}", amount=row["amount_advance"] * 100)],
        )
    except TelegramAPIError:
        logger.exception("send_invoice failed")
        await execute("UPDATE preorders SET payment_expires_at = NULL WHERE id = ?", (order_id,))
        await callback.message.answer(
            compose(
                f"Не удалось завершить оплату заказа {row['public_id']}.",
                "Если деньги не списались, попробуйте снова.",
                "Если деньги списались, не оплачивайте повторно — обратитесь в поддержку.",
            ),
            reply_markup=kb([
                [InlineKeyboardButton(text="🔁 Попробовать снова", callback_data=f"pay:{order_id}")],
                [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"chk:{order_id}")],
                [InlineKeyboardButton(text="💬 Поддержка", callback_data="support:show")],
            ]),
        )
        return
    await callback.message.answer(f"Счёт действует до {fmt_dt(expires)}.")


@router.callback_query(F.data.startswith("chk:"))
async def check_payment(callback: CallbackQuery):
    order_id = int(callback.data.split(":")[1])
    row = await get_order(order_id)
    if not row or row["telegram_id"] != callback.from_user.id:
        await callback.answer("Эта кнопка больше неактуальна.", show_alert=True)
        return
    await callback.answer()

    if row["status"] in PAID_STATUSES:
        await send_scene(callback.message, "paid", advance_paid_text(row), advance_paid_keyboard(row))
        return

    expired = row["status"] in UNPAID_STATUSES and (
        not row["payment_expires_at"] or row["payment_expires_at"] <= now_iso()
    )
    if expired and row["payment_expires_at"]:
        await callback.message.answer(
            compose(
                f"Срок действия счёта на оплату заказа {row['public_id']} истёк.",
                "Нажмите ниже, чтобы проверить доступность экземпляров и получить новый счёт.",
            ),
            reply_markup=kb([
                [InlineKeyboardButton(text="🔗 Получить новый счёт", callback_data=f"pay:{order_id}")],
                [HOME_BUTTON],
            ]),
        )
        return

    await callback.message.answer(
        compose(
            "⏳ <b>Оплата ещё не подтверждена</b>",
            "Подтверждение оплаты пока не поступило.",
            "Если вы уже оплатили, не переводите деньги повторно. Подождите немного и нажмите «Проверить оплату».",
            f"Если статус не обновляется, напишите в поддержку и укажите номер заказа: {row['public_id']}.",
        ),
        reply_markup=kb([
            [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"chk:{order_id}")],
            [InlineKeyboardButton(text="💬 Поддержка", callback_data="support:show")],
        ]),
    )


@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    error = None
    try:
        order_id = int(query.invoice_payload.split(":", 1)[1])
        row = await get_order(order_id)
        p = await get_progress()
        if not row or row["telegram_id"] != query.from_user.id or row["status"] not in UNPAID_STATUSES:
            error = "Заказ недоступен для оплаты."
        elif query.total_amount != row["amount_advance"] * 100:
            error = "Сумма не совпадает с заказом."
        elif not row["payment_expires_at"] or row["payment_expires_at"] <= now_iso():
            error = "Срок действия счёта истёк. Вернитесь в бот и получите новый счёт."
        elif not p["open"] and row["status"] != "awaiting_payment":
            error = "Приём предзаказов закрыт."
        elif p["remaining"] < row["quantity"]:
            error = "К сожалению, свободных экземпляров уже нет."
    except Exception:
        logger.exception("pre_checkout failed")
        error = "Не удалось проверить заказ. Попробуйте позже."

    await query.answer(ok=error is None, error_message=error)


async def broadcast_batch_full(bot: Bot) -> None:
    """Однократное уведомление всем оплатившим, когда партия собрана."""
    if await get_setting("batch_full_notified", "0") == "1":
        return
    await set_setting("batch_full_notified", "1")
    sent, failed = await send_project_notice(bot, "batch_full", {})
    await notify_admins(bot, f"🎉 Партия собрана — приём закрыт автоматически. Уведомлено покупателей: {sent}, не доставлено: {failed}.")


@router.message(F.successful_payment)
async def successful_payment(message: Message, bot: Bot):
    sp = message.successful_payment
    try:
        order_id = int(sp.invoice_payload.split(":", 1)[1])
    except (ValueError, IndexError):
        logger.error("Bad invoice payload: %r", sp.invoice_payload)
        await notify_admins(bot, f"⚠️ Оплата с неизвестным payload: {esc(sp.invoice_payload)}")
        return

    payment_id = sp.provider_payment_charge_id or sp.telegram_payment_charge_id
    row, outcome = await mark_paid(order_id, "telegram", payment_id, sp.total_amount // 100)

    if outcome == "duplicate":
        logger.info("Duplicate payment %s ignored", payment_id)
        return
    if outcome == "unknown" or row is None:
        await notify_admins(bot, f"⚠️ Платёж {esc(payment_id)} по несуществующему заказу {order_id}. Проверь вручную.")
        return

    p = await get_progress()

    if outcome == "oversold":
        await message.answer(
            compose(
                f"Платёж по заказу {row['public_id']} получен, но свободных экземпляров к этому моменту уже не осталось.",
                f"Мы вернём {rub(sp.total_amount // 100)} полностью. Оформление возврата начнём автоматически — сообщим об изменении статуса.",
                f"Вопросы: {esc(SUPPORT_CONTACT)}" if SUPPORT_CONTACT else None,
            ),
            reply_markup=MAIN_MENU,
        )
        await notify_admins(
            bot,
            "⚠️ <b>Оплата сверх партии</b>\nПлатёж пришёл, когда экземпляры уже закончились. Нужен возврат.\n\n"
            + order_card(row, sp.total_amount // 100, 0, admin=True),
            kb([[InlineKeyboardButton(text="↩️ Оформить возврат", callback_data=f"adm:arf:{order_id}")]]),
        )
        return

    if outcome == "extra":
        await message.answer(
            f"Получена дополнительная оплата по заказу {row['public_id']}. Мы проверим её и свяжемся с вами.",
            reply_markup=MAIN_MENU,
        )
        await notify_admins(
            bot,
            f"⚠️ Повторная оплата по заказу {row['public_id']} (ID платежа {esc(payment_id)}, "
            f"{rub(sp.total_amount // 100)}). Проверь и при необходимости оформи возврат.",
            kb([[InlineKeyboardButton(text="🧸 Карточка заказа", callback_data=f"adm:ord:{order_id}")]]),
        )
        return

    await send_scene(message, "paid", advance_paid_text(row), advance_paid_keyboard(row))
    await notify_admins(
        bot,
        compose(
            "💰 <b>Получен аванс</b>",
            f"Заказ: {row['public_id']}",
            f"Количество: {row['quantity']}",
            f"Сумма: {rub(row['amount_advance'])}.",
            f"ID платежа: <code>{esc(payment_id)}</code>",
            f"Прогресс партии: {p['reserved']} из {p['batch']} экземпляров.",
        ),
        kb([[InlineKeyboardButton(text="🧸 Карточка заказа", callback_data=f"adm:ord:{order_id}")]]),
    )
    if p["remaining"] == 0:
        task = asyncio.create_task(broadcast_batch_full(bot))
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)


# ============================================================
# ИДЕИ ДИЗАЙНА (необязательный раздел)
# ============================================================

@router.message(F.text.in_({BTN_DESIGNER, "🎨 Стать дизайнером"}))
async def designer_start(message: Message, state: FSMContext):
    await state.set_state(None)
    if not ENABLE_DESIGNS:
        await message.answer("Выберите действие в меню ниже.", reply_markup=MAIN_MENU)
        return
    await register_user(message.from_user)
    await send_scene(
        message, "designer",
        compose(
            "🎨 <b>Есть идея для REDO?</b>",
            "Можете прислать эскиз, изображение или описание.",
            "Это заявка с идеей, а не заказ игрушки. Отправка материала не гарантирует его использования.",
        ),
        kb([[InlineKeyboardButton(text="Отправить идею", callback_data="design:go")], [HOME_BUTTON]]),
    )


@router.callback_query(F.data == "design:go")
async def design_go(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await clear_markup(callback)
    pending = await scalar(
        "SELECT COUNT(*) FROM designs WHERE telegram_id = ? AND status IN ('new', 'reviewing')",
        (callback.from_user.id,),
    )
    if pending >= MAX_PENDING_DESIGNS:
        await callback.message.answer(
            "У вас уже несколько идей на рассмотрении. Дождитесь ответа — и присылайте новые.",
            reply_markup=MAIN_MENU,
        )
        return
    await state.set_state(Designer.wait)
    await callback.message.answer(
        f"Пришлите изображение или описание одним сообщением. Отправляйте только материалы, которыми вы вправе делиться. "
        f"Размер файла — до {MAX_IMAGE_SIZE_MB} МБ.",
        reply_markup=kb([[CANCEL_BUTTON]]),
    )


@router.message(Designer.wait)
async def design_receive(message: Message, state: FSMContext, bot: Bot):
    data: Dict[str, Any] = {"description": (message.caption or message.text or "").strip()[:1000]}

    if message.photo:
        photo = message.photo[-1]
        if photo.file_size and photo.file_size > MAX_IMAGE_SIZE:
            await message.answer(f"Файл слишком большой (до {MAX_IMAGE_SIZE_MB} МБ). Попробуйте другое изображение.")
            return
        data.update(file_id=photo.file_id, file_type="photo")
    elif message.document:
        doc = message.document
        if not doc.mime_type or not doc.mime_type.startswith("image/"):
            await message.answer("Нужен файл-изображение или текстовое описание.")
            return
        if doc.file_size and doc.file_size > MAX_IMAGE_SIZE:
            await message.answer(f"Файл слишком большой (до {MAX_IMAGE_SIZE_MB} МБ). Попробуйте другое изображение.")
            return
        data.update(file_id=doc.file_id, file_type="document")
    elif len(data["description"]) < 5:
        await message.answer("Пришлите изображение или описание идеи одним сообщением.")
        return

    await state.clear()
    try:
        design_id, public_id = await create_design(message.from_user, data)
    except Exception:
        logger.exception("Failed to save design")
        await message.answer("Не удалось выполнить действие. Попробуйте немного позже.", reply_markup=MAIN_MENU)
        return

    await message.answer(
        compose("Спасибо! Идея получена.", "Если захотим обсудить её подробнее, свяжемся с вами в Telegram."),
        reply_markup=MAIN_MENU,
    )
    row = await get_design(design_id)
    for admin_id in ADMIN_IDS:
        try:
            await send_design_card(bot, admin_id, row, design_admin_keyboard(row))
        except TelegramAPIError:
            logger.exception("Failed to send design to admin %s", admin_id)


# ============================================================
# УВЕДОМЛЕНИЯ ПО ПРОЕКТУ (п. 16 ТЗ)
# ============================================================

# Тексты по ТЗ. {…} — переменные; значения от админа экранируются.
PROJECT_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "batch_full": {
        "title": "Первая партия собрана",
        "vars": [],
        "text": (
            "🎉 <b>Первая партия собрана!</b>\n"
            "Получены авансы за {batch} экземпляров REDO.\n"
            "Следующий этап — создание физического прототипа. Покажем образец и расскажем о дальнейших шагах.\n"
            "Ваш заказ: {order_id}."
        ),
    },
    "prototype_ready": {
        "title": "Прототип готов",
        "vars": [("prototype_post_url", "Пришлите ссылку на пост с прототипом.")],
        "text": (
            "🧸 <b>Первый физический прототип готов!</b>\n"
            "Опубликовали фотографии, видео и характеристики образца:\n"
            "{prototype_post_url}\n"
            "Посмотрите, каким получился REDO. Если возникнут вопросы по вашему заказу, напишите нам: {support_contact}."
        ),
    },
    "prototype_changes": {
        "title": "Изменения характеристик",
        "vars": [
            ("changes_description", "Опишите, что изменилось после изготовления прототипа."),
            ("prototype_post_url", "Пришлите ссылку на пост с образцом."),
        ],
        "text": (
            "ℹ️ <b>Обновление по заказу {order_id}</b>\n"
            "После изготовления прототипа изменились следующие характеристики:\n"
            "{changes_description}\n"
            "Посмотреть образец:\n"
            "{prototype_post_url}\n"
            "Если изменения вам не подходят, вы можете запросить отказ от заказа и возврат аванса в разделе «Мои заказы»."
        ),
    },
    "in_production": {
        "title": "Производство запущено",
        "vars": [("estimated_shipping", "Укажите ориентировочный срок отправки.")],
        "text": (
            "🏭 <b>Первая партия REDO передана в производство</b>\n"
            "Ваш заказ: {order_id}.\n"
            "Количество: {quantity}.\n"
            "Ориентировочная отправка: {estimated_shipping}.\n"
            "О следующих этапах сообщим в канале."
        ),
    },
    "delay": {
        "title": "Изменение сроков",
        "vars": [
            ("delay_reason", "Укажите причину изменения сроков."),
            ("estimated_shipping", "Укажите новый ориентир отправки."),
        ],
        "text": (
            "ℹ️ <b>Изменились сроки по первой партии</b>\n"
            "Причина: {delay_reason}.\n"
            "Новый ориентир отправки: {estimated_shipping}.\n"
            "Ваш заказ {order_id} сохраняется. Если новые сроки вам не подходят, свяжитесь с нами: {support_contact}."
        ),
    },
    "balance_due": {
        "title": "Доплата перед отправкой",
        "vars": [],
        "text": (
            "💳 <b>Заказ {order_id} готовится к отправке</b>\n"
            "Количество: {quantity}.\n"
            "Аванс получен: {advance_amount} ₽.\n"
            "Остаток за игрушки: {balance_amount} ₽.\n"
            "Доставку согласуем отдельно.\n"
            "Перейдите к заказу, чтобы проверить данные и внести остаток."
        ),
    },
    "project_cancelled": {
        "title": "Отмена проекта",
        "vars": [("refund_deadline", "Укажите крайнюю дату возврата.")],
        "text": (
            "<b>Проект не будет запущен в установленный срок</b>\n"
            "К сожалению, мы отменяем запуск первой партии REDO.\n"
            "По заказу {order_id} возвращаем всю полученную оплату: {refund_amount} ₽.\n"
            "Крайняя дата возврата: {refund_deadline}.\n"
            "Возврат оформим через исходный способ оплаты. Если это окажется невозможно, свяжемся с вами для выбора другого способа.\n"
            "Спасибо за доверие. По вопросам: {support_contact}."
        ),
    },
}

NOTIFY_STATUSES = tuple(s for s in PAID_STATUSES if s not in ("refund_requested", "refund_processing"))


async def render_project_text(key: str, row, values: Dict[str, str], batch: int) -> str:
    tpl = PROJECT_TEMPLATES[key]
    paid, refunded = await order_money(row["id"])
    mapping: Dict[str, str] = {
        "order_id": row["public_id"],
        "quantity": str(row["quantity"]),
        "advance_amount": str(row["amount_advance"]),
        "balance_amount": str(row["amount_total"] - row["amount_advance"]),
        "refund_amount": str(max(0, paid - refunded)),
        "batch": str(batch),
        "support_contact": esc(SUPPORT_CONTACT),
        "estimated_shipping": esc(ESTIMATED_SHIPPING),
    }
    mapping.update({k: esc(v) for k, v in values.items()})
    return tpl["text"].format_map(mapping)


async def notify_targets(key: str):
    statuses = PAID_STATUSES if key == "project_cancelled" else NOTIFY_STATUSES
    marks = ", ".join(f"'{s}'" for s in statuses)
    return await fetch_all(
        f"SELECT * FROM preorders WHERE status IN ({marks}) AND oversold = 0 ORDER BY id"
    )


def project_markup(key: str, row) -> Optional[InlineKeyboardMarkup]:
    if key == "balance_due":
        return kb([[InlineKeyboardButton(text="🧸 Открыть заказ", callback_data=f"ord:{row['id']}")],
                   [InlineKeyboardButton(text="💬 Связаться с нами", callback_data="support:show")]])
    if key in ("batch_full", "in_production", "delay"):
        return kb([[InlineKeyboardButton(text="🧸 Мой заказ", callback_data=f"ord:{row['id']}")]])
    return None


async def send_project_notice(bot: Bot, key: str, values: Dict[str, str]) -> Tuple[int, int]:
    batch = (await get_progress())["batch"]
    sent = failed = 0
    for row in await notify_targets(key):
        text = await render_project_text(key, row, values, batch)
        for _ in range(2):
            try:
                await bot.send_message(row["telegram_id"], text, reply_markup=project_markup(key, row))
                sent += 1
                break
            except TelegramRetryAfter as exc:
                await asyncio.sleep(exc.retry_after + 1)
            except TelegramAPIError:
                failed += 1
                break
        else:
            failed += 1
        await asyncio.sleep(0.05)
    await audit(0, f"project_notice:{key}", "settings", 0, f"sent={sent} failed={failed}")
    return sent, failed


# ============================================================
# АДМИНКА
# ============================================================

_background_tasks: set = set()


def spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def admin_orders_list(message: Message, flt: str) -> None:
    conds = {
        "active": "archived = 0 AND status != 'draft'",
        "paid": f"archived = 0 AND {COUNTED_SQL}",
        "wait": "archived = 0 AND status = 'awaiting_payment'",
        "refund": "status IN ('refund_requested', 'refund_processing', 'refunded')",
        "archive": "archived = 1",
    }
    rows = await fetch_all(f"SELECT * FROM preorders WHERE {conds.get(flt, conds['active'])} ORDER BY id DESC LIMIT 20")

    builder = InlineKeyboardBuilder()
    for row in rows:
        builder.button(
            text=f"{STATUS_EMOJI.get(row['status'], '•')} {row['public_id']} · {row['name'][:16]} · {row['quantity']} шт.",
            callback_data=f"adm:ord:{row['id']}",
        )
    builder.adjust(1)
    builder.row(
        InlineKeyboardButton(text="Все", callback_data="adm:orders:active"),
        InlineKeyboardButton(text="💰 Оплаченные", callback_data="adm:orders:paid"),
    )
    builder.row(
        InlineKeyboardButton(text="🕓 Ждут оплаты", callback_data="adm:orders:wait"),
        InlineKeyboardButton(text="↩️ Возвраты", callback_data="adm:orders:refund"),
    )
    builder.row(
        InlineKeyboardButton(text="📦 Архив", callback_data="adm:orders:archive"),
        InlineKeyboardButton(text="⬅️ Админ-меню", callback_data="adm:home"),
    )
    await message.answer("🧸 <b>Заказы</b>" + ("" if rows else "\nВ этом разделе заказов нет."), reply_markup=builder.as_markup())


async def admin_stats(message: Message) -> None:
    p = await get_progress()
    paid_orders = await scalar(f"SELECT COUNT(*) FROM preorders WHERE {COUNTED_SQL}")
    pending = await scalar("SELECT COUNT(*) FROM preorders WHERE status = 'awaiting_payment'")
    refund_reqs = await scalar("SELECT COUNT(*) FROM refund_requests WHERE status = 'open'")
    advances = await scalar("SELECT COALESCE(SUM(amount), 0) FROM payments")
    refunds_total = await scalar("SELECT COALESCE(SUM(amount), 0) FROM refunds WHERE status = 'confirmed'")
    users = await scalar("SELECT COUNT(*) FROM users")
    news = await scalar("SELECT COUNT(*) FROM users WHERE consent_news = 1")
    designs = await scalar("SELECT COUNT(*) FROM designs")
    await message.answer(
        compose(
            "📊 <b>Статистика первой партии</b>",
            f"Тираж: {p['batch']} экземпляров.",
            f"С подтверждённым авансом: {p['reserved']}.",
            f"Доступно: {p['remaining']}.",
            f"Оплаченных заказов: {paid_orders}.",
            f"Ожидают оплаты: {pending}.",
            f"Запросов на возврат: {refund_reqs}.",
            f"Получено авансов: {rub(advances)}.",
            f"Подтверждено возвратов: {rub(refunds_total)}.",
            f"Приём предзаказов: {preorder_state_text(p)}.",
            "",
            f"Пользователей: {users} (согласны на новости: {news}). Идей дизайна: {designs}.",
        ),
        reply_markup=kb([[InlineKeyboardButton(text="⬅️ Админ-меню", callback_data="adm:home")]]),
    )


@router.callback_query(F.data.startswith("adm:"))
async def admin_callbacks(callback: CallbackQuery, bot: Bot, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return

    a = callback.data.split(":")
    section = a[1] if len(a) > 1 else ""
    msg = callback.message
    admin_id = callback.from_user.id
    await callback.answer()

    back = kb([[InlineKeyboardButton(text="⬅️ Админ-меню", callback_data="adm:home")]])

    if section == "home":
        await state.clear()
        await send_admin_home(msg)

    elif section == "stats":
        await admin_stats(msg)

    elif section == "orders":
        await admin_orders_list(msg, a[2] if len(a) > 2 else "active")

    elif section == "ord" and len(a) == 3:
        await send_admin_order(msg, int(a[2]))

    elif section == "payments":
        rows = await fetch_all(
            "SELECT p.*, o.public_id FROM payments p LEFT JOIN preorders o ON o.id = p.order_id "
            "ORDER BY p.id DESC LIMIT 15"
        )
        if not rows:
            await msg.answer("Платежей пока нет.", reply_markup=back)
            return
        lines = ["💳 <b>Последние платежи</b>"]
        for r in rows:
            lines.append(
                f"{fmt_dt(r['created_at'])} · {r['public_id']} · {rub(r['amount'])} · "
                f"{esc(r['provider'])} · <code>{esc(r['provider_payment_id'])}</code>"
            )
        await msg.answer("\n".join(lines), reply_markup=back)

    elif section == "refunds":
        rows = await fetch_all(
            "SELECT r.*, o.public_id, o.name FROM refund_requests r JOIN preorders o ON o.id = r.order_id "
            "WHERE r.status = 'open' ORDER BY r.id DESC LIMIT 20"
        )
        if not rows:
            await msg.answer("Открытых запросов на возврат нет.", reply_markup=back)
            return
        builder = InlineKeyboardBuilder()
        for r in rows:
            builder.button(text=f"↩️ {r['public_id']} · {r['name'][:20]}", callback_data=f"adm:ord:{r['order_id']}")
        builder.adjust(1)
        builder.row(InlineKeyboardButton(text="⬅️ Админ-меню", callback_data="adm:home"))
        await msg.answer("↩️ <b>Запросы возврата</b>", reply_markup=builder.as_markup())

    elif section == "designs":
        rows = await fetch_all("SELECT * FROM designs ORDER BY id DESC LIMIT 15")
        if not rows:
            await msg.answer("Идей дизайна пока нет.", reply_markup=back)
            return
        builder = InlineKeyboardBuilder()
        for r in rows:
            builder.button(
                text=f"{DESIGN_STATUS_LABELS.get(r['status'], r['status']).split(' ')[0]} {r['public_id']} · {r['name'][:22]}",
                callback_data=f"adm:design:{r['id']}",
            )
        builder.adjust(1)
        builder.row(InlineKeyboardButton(text="⬅️ Админ-меню", callback_data="adm:home"))
        await msg.answer("🎨 <b>Идеи дизайна</b>", reply_markup=builder.as_markup())

    elif section == "design" and len(a) == 3:
        row = await get_design(int(a[2]))
        if not row:
            await msg.answer("Идея не найдена.")
            return
        try:
            await send_design_card(bot, msg.chat.id, row, design_admin_keyboard(row))
        except TelegramAPIError:
            logger.exception("Failed to show design")
            await msg.answer("Не удалось показать материалы идеи.")

    elif section == "ds" and len(a) == 4:
        design_id, status = int(a[2]), a[3]
        if not await change_design_status(design_id, status, admin_id):
            await msg.answer("Не удалось изменить статус.")
            return
        row = await get_design(design_id)
        try:
            await msg.edit_reply_markup(reply_markup=design_admin_keyboard(row))
        except TelegramAPIError:
            pass
        await msg.answer(f"{row['public_id']}: {DESIGN_STATUS_LABELS.get(status, status)}")
        text = {"accepted": "💜 Ваша идея принята. Спасибо!",
                "rejected": "Спасибо за идею. К сожалению, сейчас она не прошла отбор."}.get(status)
        if text:
            try:
                await bot.send_message(row["telegram_id"], text)
            except TelegramAPIError:
                pass

    # ---------- смена статуса заказа ----------
    elif section == "stm" and len(a) == 3:
        oid = int(a[2])
        builder = InlineKeyboardBuilder()
        for st in STAGE_TARGETS:
            builder.button(text=f"{STATUS_EMOJI.get(st, '')} {status_label(st)}", callback_data=f"adm:st:{oid}:{st}")
        builder.adjust(1)
        builder.row(InlineKeyboardButton(text="⬅️ К заказу", callback_data=f"adm:ord:{oid}"))
        await msg.answer("Выберите новый статус:", reply_markup=builder.as_markup())

    elif section == "st" and len(a) == 4:
        row = await get_order(int(a[2]))
        new = a[3]
        if not row or new not in STAGE_TARGETS or row["status"] not in PAID_STATUSES \
                or row["status"] in ("refund_requested", "refund_processing"):
            await msg.answer("Для этого заказа такая смена статуса недоступна.")
            return
        await msg.answer(
            compose(
                f"Изменить статус заказа {row['public_id']}?",
                f"Текущий: {status_label(row['status'])}",
                f"Новый: {status_label(new)}",
                f"Покупатель: {esc(row['name'])}",
                f"Количество: {row['quantity']}",
            ),
            reply_markup=kb([
                [InlineKeyboardButton(text="Подтвердить", callback_data=f"adm:stc:{row['id']}:{new}")],
                [InlineKeyboardButton(text="Отмена", callback_data=f"adm:ord:{row['id']}")],
            ]),
        )

    elif section == "stc" and len(a) == 4:
        oid, new = int(a[2]), a[3]
        row = await get_order(oid)
        if not row or new not in STAGE_TARGETS:
            await msg.answer("Заказ не найден.")
            return
        old = row["status"]
        allowed = tuple(s for s in PAID_STATUSES if s not in ("refund_requested", "refund_processing"))
        if not await set_order_status(oid, new, admin_id, "admin status change", allowed_from=allowed):
            await msg.answer("Не удалось изменить статус: заказ уже в другом состоянии.")
            return
        await clear_markup(callback)
        await msg.answer(
            compose(f"Статус заказа {row['public_id']} изменён:", f"{status_label(old)} → {status_label(new)}.",
                    "Изменение записано в журнал."),
            reply_markup=kb([[InlineKeyboardButton(text="🧸 Карточка заказа", callback_data=f"adm:ord:{oid}")]]),
        )
        try:
            await bot.send_message(row["telegram_id"], f"ℹ️ Статус заказа {row['public_id']}: {status_label(new).lower()}.")
        except TelegramAPIError:
            logger.info("Could not notify order owner %s", row["telegram_id"])

    # ---------- отклонение ----------
    elif section == "rej" and len(a) == 3:
        row = await get_order(int(a[2]))
        if not row:
            await msg.answer("Заказ не найден.")
            return
        paid, _ = await order_money(row["id"])
        if row["status"] in PAID_STATUSES or paid > 0:
            await msg.answer(
                compose(
                    "⚠️ <b>Попытка отклонить оплаченный заказ</b>",
                    f"По заказу {row['public_id']} получено {rub(paid)}.",
                    "Отклонение заказа не возвращает деньги автоматически. Для отмены оплаченного заказа оформите возврат.",
                ),
                reply_markup=kb([
                    [InlineKeyboardButton(text="↩️ Перейти к возврату", callback_data=f"adm:arf:{row['id']}")],
                    [InlineKeyboardButton(text="Отмена", callback_data=f"adm:ord:{row['id']}")],
                ]),
            )
            return
        await msg.answer(
            compose(f"Отклонить неоплаченный заказ {row['public_id']}?", f"Покупатель: {esc(row['name'])}",
                    f"Количество: {row['quantity']}"),
            reply_markup=kb([
                [InlineKeyboardButton(text="Подтвердить", callback_data=f"adm:rejc:{row['id']}")],
                [InlineKeyboardButton(text="Отмена", callback_data=f"adm:ord:{row['id']}")],
            ]),
        )

    elif section == "rejc" and len(a) == 3:
        row = await get_order(int(a[2]))
        if not row:
            return
        ok = await set_order_status(row["id"], "cancelled_unpaid", admin_id, "rejected by admin",
                                    allowed_from=UNPAID_STATUSES)
        await clear_markup(callback)
        if not ok:
            await msg.answer("Не удалось отклонить: возможно, оплата уже поступила. Откройте карточку заказа.",
                             reply_markup=kb([[InlineKeyboardButton(text="🧸 Карточка заказа", callback_data=f"adm:ord:{row['id']}")]]))
            return
        await msg.answer(f"Заказ {row['public_id']} отклонён: {status_label('cancelled_unpaid')}.")
        try:
            await bot.send_message(row["telegram_id"], f"Заказ {row['public_id']} отменён. Подтверждённой оплаты по нему нет.")
        except TelegramAPIError:
            pass

    # ---------- ручная оплата ----------
    elif section == "manual" and len(a) == 3:
        oid = int(a[2])
        row = await get_order(oid)
        if not row:
            await msg.answer("Заказ не найден.")
            return
        await msg.answer(
            compose(f"Отметить заказ {row['public_id']} оплаченным вручную?", f"Сумма: {rub(row['amount_advance'])}.",
                    "Подтверждайте только после проверки поступления денег."),
            reply_markup=kb([
                [InlineKeyboardButton(text="Да, оплата получена", callback_data=f"adm:manualc:{oid}")],
                [InlineKeyboardButton(text="Отмена", callback_data=f"adm:ord:{oid}")],
            ]),
        )

    elif section == "manualc" and len(a) == 3:
        oid = int(a[2])
        row = await get_order(oid)
        if not row:
            return
        updated, outcome = await mark_paid(oid, "manual", f"manual-{oid}", row["amount_advance"])
        await clear_markup(callback)
        if outcome == "duplicate":
            await msg.answer("Оплата уже была зафиксирована вручную.")
            return
        await audit(admin_id, "manual_paid", "order", oid, f"outcome={outcome}")
        p = await get_progress()
        await msg.answer(f"{updated['public_id']}: {status_label(updated['status'])} ({outcome}).\nПартия: {p['reserved']}/{p['batch']}")
        if outcome == "paid":
            try:
                await bot.send_message(updated["telegram_id"], advance_paid_text(updated),
                                       reply_markup=advance_paid_keyboard(updated))
            except TelegramAPIError:
                pass
            if p["remaining"] == 0:
                spawn(broadcast_batch_full(bot))

    # ---------- возврат ----------
    elif section == "arf" and len(a) == 3:
        oid = int(a[2])
        row = await get_order(oid)
        if not row:
            await msg.answer("Заказ не найден.")
            return
        paid, refunded = await order_money(oid)
        amount = paid - refunded
        last = await fetch_one("SELECT provider_payment_id FROM payments WHERE order_id = ? ORDER BY id DESC LIMIT 1", (oid,))
        if amount <= 0 or row["status"] in ("refund_processing", "refunded"):
            await msg.answer("По этому заказу нечего возвращать или возврат уже оформлен.")
            return
        await msg.answer(
            compose(
                f"Оформить возврат по заказу {row['public_id']}?",
                f"Сумма: {rub(amount)}.",
                f"ID платежа: <code>{esc(last['provider_payment_id']) if last else '—'}</code>",
                "Подтверждайте только после проверки платежа.",
            ),
            reply_markup=kb([
                [InlineKeyboardButton(text="Да, оформить возврат", callback_data=f"adm:arfc:{oid}")],
                [InlineKeyboardButton(text="Отмена", callback_data=f"adm:ord:{oid}")],
            ]),
        )

    elif section == "arfc" and len(a) == 3:
        oid = int(a[2])
        row = await get_order(oid)
        if not row:
            return
        paid, refunded = await order_money(oid)
        amount = paid - refunded
        last = await fetch_one("SELECT provider_payment_id FROM payments WHERE order_id = ? ORDER BY id DESC LIMIT 1", (oid,))
        ok = amount > 0 and await set_order_status(
            oid, "refund_processing", admin_id, f"refund {amount} initiated",
            allowed_from=("refund_requested",) + REFUNDABLE + ("shipped", "completed"),
        )
        await clear_markup(callback)
        if not ok:
            await msg.answer("Не удалось оформить возврат: заказ уже в другом состоянии.")
            return
        await execute(
            "INSERT INTO refunds(order_id, amount, payment_id, status, admin_id, created_at) VALUES (?, ?, ?, 'processing', ?, ?)",
            (oid, amount, last["provider_payment_id"] if last else None, admin_id, now_iso()),
        )
        await execute("UPDATE refund_requests SET status = 'done' WHERE order_id = ? AND status = 'open'", (oid,))
        await audit(admin_id, "refund_initiated", "order", oid, f"amount={amount}")
        await msg.answer(
            compose(f"Возврат по заказу {row['public_id']} зафиксирован: {rub(amount)}.",
                    "Проведите возврат в личном кабинете платёжного провайдера, затем нажмите «Подтвердить возврат» в карточке заказа."),
            reply_markup=kb([[InlineKeyboardButton(text="🧸 Карточка заказа", callback_data=f"adm:ord:{oid}")]]),
        )
        try:
            await bot.send_message(
                row["telegram_id"],
                compose(
                    f"↩️ <b>Возврат по заказу {row['public_id']} оформлен</b>",
                    f"Сумма: {rub(amount)}.",
                    "Ожидаем подтверждение возврата от платёжного сервиса. Сообщим об изменении статуса.",
                ),
            )
        except TelegramAPIError:
            pass

    elif section == "arok" and len(a) == 3:
        oid = int(a[2])
        row = await get_order(oid)
        if not row:
            return
        pending = await fetch_one(
            "SELECT * FROM refunds WHERE order_id = ? AND status = 'processing' ORDER BY id DESC LIMIT 1", (oid,)
        )
        if not pending:
            await msg.answer("Нет возврата в обработке по этому заказу.")
            return
        await execute("UPDATE refunds SET status = 'confirmed', confirmed_at = ? WHERE id = ?", (now_iso(), pending["id"]))
        await set_order_status(oid, "refunded", admin_id, f"refund {pending['amount']} confirmed",
                               allowed_from=("refund_processing",))
        await audit(admin_id, "refund_confirmed", "order", oid, f"amount={pending['amount']}")
        await clear_markup(callback)
        await msg.answer(f"Возврат по заказу {row['public_id']} подтверждён. Экземпляры снова доступны в партии.")
        try:
            await bot.send_message(
                row["telegram_id"],
                compose(
                    f"✅ <b>Возврат по заказу {row['public_id']} подтверждён</b>",
                    f"Сумма: {rub(pending['amount'])}.",
                    "Если средства ещё не появились на счёте, срок зачисления можно уточнить в вашем банке.",
                    f"По вопросам: {esc(SUPPORT_CONTACT)}." if SUPPORT_CONTACT else None,
                ),
            )
        except TelegramAPIError:
            pass

    # ---------- архив / история ----------
    elif section == "arch" and len(a) == 3:
        oid = int(a[2])
        row = await get_order(oid)
        if not row:
            return
        new = 0 if row["archived"] else 1
        await execute("UPDATE preorders SET archived = ?, updated_at = ? WHERE id = ?", (new, now_iso(), oid))
        await audit(admin_id, "archive" if new else "unarchive", "order", oid, "статус и счётчик не меняются")
        await msg.answer(f"{row['public_id']}: {'перенесён в архив' if new else 'возвращён из архива'}. Статус оплаты и счётчик не изменены.")

    elif section == "hist" and len(a) == 3:
        oid = int(a[2])
        rows = await fetch_all(
            "SELECT * FROM audit_log WHERE entity_type = 'order' AND entity_id = ? ORDER BY id DESC LIMIT 20", (oid,)
        )
        if not rows:
            await msg.answer("История изменений пуста.")
            return
        lines = [f"🕘 <b>История заказа {oid}</b>"]
        for r in rows:
            who = "система" if r["admin_id"] == 0 else str(r["admin_id"])
            lines.append(f"{fmt_dt(r['created_at'])} · {esc(r['action'])} · {who}")
        await msg.answer("\n".join(lines))

    # ---------- настройки ----------
    elif section == "settings":
        p = await get_progress()
        miss = config_missing()
        await msg.answer(
            compose(
                "⚙️ <b>Настройки</b>",
                f"Размер партии: {p['batch']}.",
                f"Приём предзаказов: {preorder_state_text(p)}.",
                f"Оплата: {'настроена' if PAYMENT_PROVIDER_TOKEN else 'НЕ настроена (нет PAYMENT_PROVIDER_TOKEN)'}.",
                f"Не заполнено в окружении: {', '.join(miss)}. Оплата заблокирована." if miss else "Обязательные переменные заполнены.",
            ),
            reply_markup=kb([
                [InlineKeyboardButton(text="🟢 Открыть приём", callback_data="adm:campset:open"),
                 InlineKeyboardButton(text="⏸ Пауза", callback_data="adm:campset:paused")],
                [InlineKeyboardButton(text="🔴 Закрыть приём", callback_data="adm:camp:closed")],
                [InlineKeyboardButton(text="🎯 Размер партии", callback_data="adm:target")],
                [InlineKeyboardButton(text="🔔 Уведомления по проекту", callback_data="adm:notify")],
                [InlineKeyboardButton(text="📣 Рассылка новостей", callback_data="adm:broadcast")],
                [InlineKeyboardButton(text="📤 Экспорт CSV", callback_data="adm:export"),
                 InlineKeyboardButton(text="💾 Бэкап БД", callback_data="adm:backup")],
                [InlineKeyboardButton(text="⬅️ Админ-меню", callback_data="adm:home")],
            ]),
        )

    elif section == "camp" and len(a) == 3:
        await msg.answer(
            compose("Закрыть приём новых предзаказов?",
                    "Оплаченные заказы сохранятся. Обработка уже созданных платёжных ссылок будет выполняться по правилам проверки при оплате."),
            reply_markup=kb([
                [InlineKeyboardButton(text="Закрыть приём", callback_data="adm:campset:closed")],
                [InlineKeyboardButton(text="Отмена", callback_data="adm:settings")],
            ]),
        )

    elif section == "campset" and len(a) == 3 and a[2] in {"open", "paused", "closed"}:
        await set_setting("campaign_status", a[2])
        await audit(admin_id, f"campaign->{a[2]}", "settings", 0, "campaign status")
        await clear_markup(callback)
        await msg.answer({"open": "🟢 Приём новых предзаказов открыт.",
                          "paused": "⏸ Приём новых предзаказов приостановлен.",
                          "closed": "🔴 Приём новых предзаказов закрыт."}[a[2]],
                         reply_markup=back)

    elif section == "target":
        p = await get_progress()
        await state.set_state(AdminStates.target)
        await msg.answer(f"Сейчас размер партии: {p['batch']} (подтверждено {p['reserved']}). Введите новый (целое от 1 до 100000).")

    elif section == "broadcast":
        await state.set_state(AdminStates.broadcast)
        users = await scalar("SELECT COUNT(*) FROM users WHERE consent_news = 1")
        await msg.answer(f"📣 Пришлите текст рассылки — получат только давшие согласие на новости ({users}).\n/cancel — отмена.")

    elif section == "export":
        await export_orders(bot, msg.chat.id)

    elif section == "backup":
        await send_backup(bot, msg.chat.id)

    # ---------- уведомления по проекту ----------
    elif section == "notify":
        builder = InlineKeyboardBuilder()
        for key, tpl in PROJECT_TEMPLATES.items():
            builder.button(text=tpl["title"], callback_data=f"adm:nt:{key}")
        builder.adjust(1)
        builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:settings"))
        await msg.answer("🔔 <b>Уведомления покупателям с оплаченным авансом</b>\nВыберите шаблон:",
                         reply_markup=builder.as_markup())

    elif section == "nt" and len(a) == 3 and a[2] in PROJECT_TEMPLATES:
        key = a[2]
        await state.clear()
        await state.update_data(tpl=key, vals={}, idx=0)
        if PROJECT_TEMPLATES[key]["vars"]:
            await state.set_state(AdminStates.notify_var)
            await msg.answer(PROJECT_TEMPLATES[key]["vars"][0][1] + "\n/cancel — отмена.")
        else:
            await notify_preview(msg, state)

    elif section == "ns":
        data = await state.get_data()
        key = data.get("tpl")
        vals = data.get("vals", {})
        await state.clear()
        await clear_markup(callback)
        if key not in PROJECT_TEMPLATES:
            await msg.answer("Сессия устарела.", reply_markup=back)
            return
        await msg.answer("🚀 Отправка запущена, пришлю отчёт.")
        spawn(run_notice(bot, key, vals, msg.chat.id))

    elif section == "nc":
        await state.clear()
        await clear_markup(callback)
        await msg.answer("Отправка отменена.", reply_markup=back)


async def notify_preview(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    key, vals = data["tpl"], data.get("vals", {})
    targets = await notify_targets(key)
    await state.set_state(None)
    if not targets:
        await state.clear()
        await message.answer("Нет подходящих получателей.", reply_markup=kb([[InlineKeyboardButton(text="⬅️ Админ-меню", callback_data="adm:home")]]))
        return
    batch = (await get_progress())["batch"]
    sample = await render_project_text(key, targets[0], vals, batch)
    try:
        await message.answer("👀 <b>Предпросмотр (на примере первого заказа):</b>")
        await message.answer(sample)
    except TelegramAPIError:
        await message.answer("Не получилось показать предпросмотр: проверьте введённые значения.")
        await state.clear()
        return
    await message.answer(
        f"Отправить {len(targets)} получателям?",
        reply_markup=kb([[InlineKeyboardButton(text="🚀 Отправить", callback_data="adm:ns")],
                         [InlineKeyboardButton(text="✖️ Отмена", callback_data="adm:nc")]]),
    )


async def run_notice(bot: Bot, key: str, vals: Dict[str, str], chat_id: int) -> None:
    sent, failed = await send_project_notice(bot, key, vals)
    try:
        await bot.send_message(chat_id, f"🔔 Уведомление «{PROJECT_TEMPLATES[key]['title']}» отправлено.\nДоставлено: {sent}\nНе доставлено: {failed}")
    except TelegramAPIError:
        pass


@router.message(AdminStates.notify_var)
async def notify_var_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    text = (message.text or "").strip()
    if not text:
        await message.answer("Нужен текст.")
        return
    data = await state.get_data()
    key, idx, vals = data["tpl"], data["idx"], dict(data.get("vals", {}))
    spec = PROJECT_TEMPLATES[key]["vars"]
    vals[spec[idx][0]] = text
    idx += 1
    await state.update_data(vals=vals, idx=idx)
    if idx < len(spec):
        await message.answer(spec[idx][1] + "\n/cancel — отмена.")
    else:
        await notify_preview(message, state)


@router.message(AdminStates.target)
async def admin_target_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        target = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число.")
        return
    if not 1 <= target <= 100000:
        await message.answer("От 1 до 100000.")
        return
    await set_setting("batch_size", target)
    await set_setting("batch_full_notified", "0")
    await audit(message.from_user.id, f"batch->{target}", "settings", 0, "batch size")
    await state.clear()
    await message.answer(f"Размер партии: {target}.\n\n{progress_text(await get_progress())}", reply_markup=admin_keyboard())


@router.message(AdminStates.broadcast)
async def admin_broadcast_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    if not message.text:
        await message.answer("Пока можно рассылать только текст.")
        return
    text = message.html_text
    if len(text) > 4000:
        await message.answer("Слишком длинно. Максимум ~4000 символов.")
        return

    await state.update_data(broadcast_text=text)
    users = await scalar("SELECT COUNT(*) FROM users WHERE consent_news = 1")
    try:
        await message.answer("👀 <b>Предпросмотр:</b>")
        await message.answer(text)
    except TelegramAPIError:
        await message.answer("Не получилось отправить: проверьте разметку.")
        return
    await message.answer(
        f"Отправить {users} пользователям, давшим согласие на новости?",
        reply_markup=kb([[InlineKeyboardButton(text="🚀 Отправить", callback_data="broadcast:send")],
                         [InlineKeyboardButton(text="✖️ Отмена", callback_data="broadcast:cancel")]]),
    )


@router.callback_query(F.data.startswith("broadcast:"))
async def admin_broadcast_action(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    action = callback.data.split(":", 1)[1]
    data = await state.get_data()
    text = data.get("broadcast_text")
    await state.clear()
    await clear_markup(callback)
    if action != "send" or not text:
        await callback.answer("Отменено.")
        await callback.message.answer("Рассылка отменена.", reply_markup=admin_keyboard())
        return
    await callback.answer("Запускаю…")
    await callback.message.answer("🚀 Рассылка запущена, пришлю отчёт.")
    spawn(run_broadcast(bot, text, callback.message.chat.id))


async def run_broadcast(bot: Bot, text: str, report_chat_id: int) -> None:
    users = await fetch_all("SELECT telegram_id FROM users WHERE consent_news = 1")
    sent = failed = 0
    for row in users:
        for _ in range(2):
            try:
                await bot.send_message(row["telegram_id"], text)
                sent += 1
                break
            except TelegramRetryAfter as exc:
                await asyncio.sleep(exc.retry_after + 1)
            except TelegramAPIError:
                failed += 1
                break
        else:
            failed += 1
        await asyncio.sleep(0.05)
    try:
        await bot.send_message(report_chat_id, f"📣 Рассылка завершена.\nДоставлено: {sent}\nНе доставлено: {failed}")
    except TelegramAPIError:
        logger.warning("Could not send broadcast report")


# ============================================================
# EXPORT / BACKUP
# ============================================================

def csv_safe(value) -> str:
    value = "" if value is None else str(value)
    if value.startswith(("=", "+", "-", "@", "\t", "\r")):
        return "'" + value
    return value


async def export_orders(bot: Bot, chat_id: int) -> None:
    rows = await fetch_all("SELECT * FROM preorders ORDER BY id ASC")
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow([
        "order_id", "created_at", "paid_at", "telegram_id", "username", "name", "phone", "city",
        "delivery", "quantity", "amount_total", "amount_advance", "status", "archived", "oversold", "updated_at",
    ])
    for row in rows:
        writer.writerow([
            csv_safe(row["public_id"]), csv_safe(row["created_at"]), csv_safe(row["paid_at"]),
            row["telegram_id"], csv_safe(row["username"]), csv_safe(row["name"]), csv_safe(row["phone"]),
            csv_safe(row["city"]), csv_safe(row["delivery"]), row["quantity"], row["amount_total"],
            row["amount_advance"], csv_safe(row["status"]), row["archived"], row["oversold"],
            csv_safe(row["updated_at"]),
        ])
    content = ("\ufeff" + buffer.getvalue()).encode("utf-8")
    await bot.send_document(
        chat_id=chat_id,
        document=BufferedInputFile(content, filename="plush_redo_orders.csv"),
        caption=f"📤 Экспорт заказов. Всего: {len(rows)}",
    )


async def send_backup(bot: Bot, chat_id: int) -> None:
    tmp = Path(tempfile.gettempdir()) / f"redo_backup_{int(time.time())}.db"
    try:
        quoted = str(tmp).replace("'", "''")
        async with db_connect() as db:
            await db.execute(f"VACUUM INTO '{quoted}'")
        content = tmp.read_bytes()
    finally:
        tmp.unlink(missing_ok=True)
    stamp = datetime.now(TZ).strftime("%Y%m%d_%H%M")
    await bot.send_document(
        chat_id=chat_id,
        document=BufferedInputFile(content, filename=f"redo_backup_{stamp}.db"),
        caption="💾 Бэкап БД. Внутри данные клиентов — храните надёжно.",
    )


# ============================================================
# FALLBACKS (последними)
# ============================================================

@router.message()
async def fallback_message(message: Message):
    await message.answer(
        "Не совсем понял сообщение. Выберите действие в меню ниже.\n"
        "Если нужен человек, нажмите «Поддержка».",
        reply_markup=MAIN_MENU,
    )


@router.callback_query()
async def fallback_callback(callback: CallbackQuery):
    await callback.answer(
        "Эта кнопка больше неактуальна. Откройте заказ заново, чтобы увидеть текущий статус.",
        show_alert=True,
    )


@router.errors()
async def on_error(event: ErrorEvent):
    logger.error("Unhandled error", exc_info=event.exception)
    update = event.update
    text = ("Не удалось выполнить действие. Попробуйте немного позже.\n"
            "Если ошибка возникла после оплаты, не платите повторно — сначала проверьте статус заказа.")
    try:
        if update.message:
            await update.message.answer(text)
        elif update.callback_query:
            await update.callback_query.answer("Не удалось выполнить действие. Попробуйте позже.", show_alert=True)
    except Exception:
        pass
    return True


# ============================================================
# THROTTLING
# ============================================================

class SimpleThrottle:
    def __init__(self, interval: float = 0.7):
        self.interval = interval
        self.last_event: Dict[int, float] = {}

    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        # админов и сообщения об оплате не троттлим — платёж терять нельзя
        if not user or is_admin(user.id) or (isinstance(event, Message) and event.successful_payment):
            return await handler(event, data)

        now = time.monotonic()
        if now - self.last_event.get(user.id, 0.0) < self.interval:
            if isinstance(event, Message):
                await event.answer("Не так быстро 😊 Попробуйте через секунду.")
            elif isinstance(event, CallbackQuery):
                await event.answer("Не так быстро 😊")
            return None

        self.last_event[user.id] = now
        if len(self.last_event) > 10000:
            cutoff = now - 60
            self.last_event = {k: v for k, v in self.last_event.items() if v > cutoff}
        return await handler(event, data)


# ============================================================
# ПУБЛИЧНЫЙ API ДЛЯ САЙТА
# ============================================================

def _cors_headers() -> Dict[str, str]:
    return {
        "Access-Control-Allow-Origin": API_ALLOW_ORIGIN,
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Cache-Control": "public, max-age=30",
    }


async def api_progress(request: web.Request) -> web.Response:
    """GET /api/progress → {"batch":200,"reserved":12,"remaining":188,"percent":6,"open":true}"""
    p = await get_progress()
    return web.json_response(
        {k: p[k] for k in ("batch", "reserved", "remaining", "percent", "open")},
        headers=_cors_headers(),
    )


async def api_options(request: web.Request) -> web.Response:
    return web.Response(status=204, headers=_cors_headers())


async def api_health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def start_api() -> Optional[web.AppRunner]:
    if API_PORT <= 0:
        return None
    app = web.Application()
    app.router.add_get("/api/progress", api_progress)
    app.router.add_route("OPTIONS", "/api/progress", api_options)
    app.router.add_get("/health", api_health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, API_HOST, API_PORT).start()
    logger.info("Public API on http://%s:%s/api/progress", API_HOST, API_PORT)
    return runner


# ============================================================
# MAIN
# ============================================================

def build_dispatcher() -> Dispatcher:
    dp = Dispatcher(storage=SQLiteStorage())
    dp.message.middleware(SimpleThrottle(interval=0.7))
    dp.callback_query.middleware(SimpleThrottle(interval=0.4))
    dp.include_router(router)
    return dp


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN. Добавьте токен в .env или переменную окружения.")

    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS пуст. Админка будет недоступна до настройки.")
    if not PAYMENT_PROVIDER_TOKEN:
        logger.warning("PAYMENT_PROVIDER_TOKEN не задан: оплата через бота недоступна (только вручную через админа).")
    if config_missing():
        logger.warning("Не заполнено: %s. Оформление и оплата заблокированы до настройки.", ", ".join(config_missing()))
    if not ASSETS_DIR.exists():
        logger.warning("Папка с картинками %s не найдена — бот будет отвечать текстом.", ASSETS_DIR)

    await init_db()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = build_dispatcher()
    runner = await start_api()

    try:
        me = await bot.get_me()
        logger.info("Plush REDO bot starting as @%s", me.username)
        await bot.set_my_commands([
            BotCommand(command="start", description="Начать"),
            BotCommand(command="menu", description="Главное меню"),
            BotCommand(command="cancel", description="Отменить оформление"),
            BotCommand(command="unsubscribe", description="Отключить новости"),
            BotCommand(command="help", description="Помощь"),
        ])
        try:
            await bot.set_my_short_description(
                "Предзаказ плюшевого REDO. Первая партия — 200 экземпляров. Физического прототипа пока нет."
            )
            await bot.set_my_description(
                "Предзаказ плюшевого REDO\n"
                f"Первая партия — {BATCH_SIZE} экземпляров.\n"
                f"Цена — {PRICE} ₽, аванс — {ADVANCE} ₽.\n"
                "Доставка отдельно.\n"
                "Физического прототипа пока нет. Здесь можно оформить предзаказ и следить за его статусом."
            )
        except TelegramAPIError:
            logger.warning("Could not set bot description", exc_info=True)
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        if runner:
            await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())