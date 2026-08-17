# Premium Shop Bot - single-file edition
# Python 3.10+
# Dependencies:
#   pip install aiogram aiohttp python-dotenv reportlab
#
# Required .env values:
# BOT_TOKEN=123456:ABC...
# BOT_NAME=Premium Shop
# ADMIN_ID=123456789
# SUPPORT_USERNAME=@YourSupport
# PUBLIC_CHANNEL_ID=-1001234567890
# PUBLIC_CHANNEL_URL=https://t.me/your_public_channel
# ADMIN_ALERT_CHANNEL_ID=-1001234567890
# GATEWAY_API_KEY=cpg_live_xxx
# DEPOSIT_WALLET_ADDRESS=0x...
# PUBLIC_BASE_URL=https://bot.your-domain.com
# INVOICE_CURRENCY=USDT
# SMTP_HOST=smtp.gmail.com
# SMTP_PORT=587
# SMTP_USER=you@example.com
# SMTP_PASSWORD=app-password
# SMTP_FROM=Premium Shop <you@example.com>
# DB_PATH=data/premium_shop.db
# WEB_HOST=0.0.0.0
# WEB_PORT=8080

import asyncio
import hashlib
import hmac
import html
import io
import json
import logging
import os
import re
import smtplib
import sqlite3
from contextlib import suppress
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.message import EmailMessage
from math import ceil
from pathlib import Path
from uuid import uuid4

import aiohttp
import firebase_admin
from firebase_admin import credentials, db as rtdb
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import BotCommand, BotCommandScopeDefault, BotCommandScopeChat
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    CopyTextButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_NAME = os.getenv("BOT_NAME", "Premium Shop").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or 0)
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "@YourSupport").strip()
PUBLIC_CHANNEL_ID = os.getenv("PUBLIC_CHANNEL_ID", "").strip()
PUBLIC_CHANNEL_URL = os.getenv("PUBLIC_CHANNEL_URL", "").strip()
ADMIN_ALERT_CHANNEL_ID = os.getenv("ADMIN_ALERT_CHANNEL_ID", "").strip()
GATEWAY_API_KEY = os.getenv("GATEWAY_API_KEY", os.getenv("API_KEY", "")).strip()
DEPOSIT_WALLET_BSC = os.getenv("DEPOSIT_WALLET_BSC", os.getenv("DEPOSIT_WALLET_ADDRESS_BSC", os.getenv("DEPOSIT_WALLET_BEP20", ""))).strip()
DEPOSIT_WALLET_POLYGON = os.getenv("DEPOSIT_WALLET_POLYGON", os.getenv("DEPOSIT_WALLET_ADDRESS_POLYGON", os.getenv("DEPOSIT_WALLET_MATIC", ""))).strip()
DEPOSIT_WALLET_ETH = os.getenv("DEPOSIT_WALLET_ETH", os.getenv("DEPOSIT_WALLET_ADDRESS_ETH", os.getenv("DEPOSIT_WALLET_ERC20", ""))).strip()
DEPOSIT_WALLET_ADDRESS = os.getenv("DEPOSIT_WALLET_ADDRESS", "").strip()
GATEWAY_BASE_URL = "https://wallet-watch-api.lovable.app"
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
INVOICE_CURRENCY = os.getenv("INVOICE_CURRENCY", "USDT").strip().upper()
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587") or 587)
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER).strip()
DB_PATH = os.getenv("DB_PATH", "data/premium_shop.db").strip()
FIREBASE_DATABASE_URL = os.getenv("FIREBASE_DATABASE_URL", "https://premium-hub-4e23d-default-rtdb.firebaseio.com").strip()
FIREBASE_CREDENTIALS = os.getenv("FIREBASE_CREDENTIALS", "firebase_service_account.json").strip()
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0").strip()
WEB_PORT = int(os.getenv("PORT", os.getenv("WEB_PORT", "8080")) or 8080)
PRODUCTS_PER_PAGE = 8
LOW_STOCK_THRESHOLDS = (10, 5, 2, 1, 0)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing from .env")

Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

router = Router()
EMAIL_RE = re.compile(r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$")
TX_HASH_RE = re.compile(r"^0x[a-fA-F0-9]{64}$")


# =========================
# FIREBASE REALTIME DB
# =========================

def init_firebase() -> bool:
    try:
        if firebase_admin._apps:
            return True
        cred = None
        if FIREBASE_CREDENTIALS.startswith("{"):
            cred_dict = json.loads(FIREBASE_CREDENTIALS)
            cred = credentials.Certificate(cred_dict)
        elif os.path.exists(FIREBASE_CREDENTIALS):
            cred = credentials.Certificate(FIREBASE_CREDENTIALS)
        elif os.path.exists("firebase_service_account.json"):
            cred = credentials.Certificate("firebase_service_account.json")
        
        if cred:
            firebase_admin.initialize_app(cred, {
                "databaseURL": FIREBASE_DATABASE_URL
            })
            logging.info("Firebase Realtime Database connected (%s)", FIREBASE_DATABASE_URL)
            return True
        return False
    except Exception as e:
        logging.warning("Firebase init warning: %s", e)
        return False


def fb_set(path: str, data):
    try:
        if firebase_admin._apps:
            rtdb.reference(path).set(data)
    except Exception as e:
        logging.debug("Firebase write error for %s: %s", path, e)


def fb_update(path: str, data: dict):
    try:
        if firebase_admin._apps:
            rtdb.reference(path).update(data)
    except Exception as e:
        logging.debug("Firebase update error for %s: %s", path, e)


def fb_delete(path: str):
    try:
        if firebase_admin._apps:
            rtdb.reference(path).delete()
    except Exception as e:
        logging.debug("Firebase delete error for %s: %s", path, e)


def fb_get(path: str):
    try:
        if firebase_admin._apps:
            return rtdb.reference(path).get()
    except Exception:
        return None


# =========================
# FSM STATES
# =========================

class EmailState(StatesGroup):
    waiting = State()


class AdminProductState(StatesGroup):
    name = State()
    price = State()
    warranty = State()
    note = State()
    stock = State()


class AdminStockState(StatesGroup):
    product_id = State()
    items = State()


class AdminBalanceState(StatesGroup):
    user = State()
    amount = State()
    confirm = State()


class AdminPriceState(StatesGroup):
    product_id = State()
    new_price = State()


class AdminDeleteProductState(StatesGroup):
    product_id = State()
    confirm = State()


class AdminDeleteStockState(StatesGroup):
    product_id = State()
    amount = State()
    confirm = State()


class AdminEditProductState(StatesGroup):
    product_id = State()
    field = State()
    value = State()


class CustomQtyState(StatesGroup):
    choosing = State()


class TopupState(StatesGroup):
    amount = State()


class PaymentVerifyState(StatesGroup):
    waiting_tx = State()


# =========================
# DATABASE
# =========================

def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con


def init_db() -> None:
    with db() as con:
        con.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT NOT NULL,
                email TEXT,
                wallet TEXT NOT NULL DEFAULT '0',
                language TEXT NOT NULL DEFAULT 'en',
                blocked INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price TEXT NOT NULL,
                warranty TEXT NOT NULL DEFAULT 'No Warranty',
                note TEXT NOT NULL DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS stock_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'AVAILABLE',
                order_id TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                sold_at TEXT,
                FOREIGN KEY(product_id) REFERENCES products(id)
            );

            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT UNIQUE NOT NULL,
                telegram_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                product_name TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price TEXT NOT NULL,
                total_amount TEXT NOT NULL,
                payment_method TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                delivered_content TEXT,
                invoice_pdf_token TEXT UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                paid_at TEXT,
                FOREIGN KEY(telegram_id) REFERENCES users(telegram_id)
            );

            CREATE TABLE IF NOT EXISTS wallet_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_id TEXT UNIQUE NOT NULL,
                telegram_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                amount TEXT NOT NULL,
                balance_before TEXT NOT NULL,
                balance_after TEXT NOT NULL,
                reference_id TEXT,
                note TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS payment_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id TEXT UNIQUE NOT NULL,
                telegram_id INTEGER NOT NULL,
                payment_kind TEXT NOT NULL,
                reference_id TEXT NOT NULL,
                invoice_amount TEXT NOT NULL,
                invoice_currency TEXT NOT NULL,
                chain TEXT,
                deposit_address TEXT,
                pay_amount TEXT,
                pay_currency TEXT,
                wallet_id TEXT,
                tx_hash TEXT UNIQUE,
                status TEXT NOT NULL DEFAULT 'PENDING',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                paid_at TEXT
            );

            CREATE TABLE IF NOT EXISTS processed_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash TEXT UNIQUE NOT NULL,
                invoice_id TEXT,
                chain TEXT,
                amount TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS processed_webhooks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id TEXT NOT NULL,
                tx_hash TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(invoice_id, tx_hash)
            );

            CREATE TABLE IF NOT EXISTS stock_alerts (
                product_id INTEGER NOT NULL,
                threshold INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(product_id, threshold)
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        # Lightweight migration for existing databases
        user_columns = {row["name"] for row in con.execute("PRAGMA table_info(users)").fetchall()}
        if "region" not in user_columns:
            con.execute("ALTER TABLE users ADD COLUMN region TEXT")
        
        inv_columns = {row["name"] for row in con.execute("PRAGMA table_info(payment_invoices)").fetchall()}
        if "chain" not in inv_columns:
            con.execute("ALTER TABLE payment_invoices ADD COLUMN chain TEXT")
        if "deposit_address" not in inv_columns:
            con.execute("ALTER TABLE payment_invoices ADD COLUMN deposit_address TEXT")
        con.commit()


def sync_firebase_to_sqlite():
    if not firebase_admin._apps:
        return
    try:
        # Sync app settings
        fb_settings = fb_get("app_settings") or {}
        with db() as con:
            for k, v in fb_settings.items():
                con.execute("INSERT INTO app_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(k), str(v)))
            
            # Sync products
            fb_products = fb_get("products") or {}
            for pid_str, p in fb_products.items():
                if not isinstance(p, dict): continue
                con.execute(
                    """
                    INSERT INTO products(id, name, price, warranty, note, active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        name=excluded.name,
                        price=excluded.price,
                        warranty=excluded.warranty,
                        note=excluded.note,
                        active=excluded.active
                    """,
                    (int(p.get("id", pid_str)), p.get("name", ""), str(p.get("price", "0")), p.get("warranty", "No Warranty"), p.get("note", ""), int(p.get("active", 1)), p.get("created_at", datetime.now(timezone.utc).isoformat())),
                )

            # Sync stock items
            fb_stock = fb_get("stock_items") or {}
            for sid_str, s in fb_stock.items():
                if not isinstance(s, dict): continue
                con.execute(
                    """
                    INSERT INTO stock_items(id, product_id, content, status, order_id, created_at, sold_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        status=excluded.status,
                        order_id=excluded.order_id,
                        sold_at=excluded.sold_at
                    """,
                    (int(s.get("id", sid_str)), int(s.get("product_id")), s.get("content", ""), s.get("status", "AVAILABLE"), s.get("order_id"), s.get("created_at", datetime.now(timezone.utc).isoformat()), s.get("sold_at")),
                )

            # Sync users
            fb_users = fb_get("users") or {}
            for uid_str, u in fb_users.items():
                if not isinstance(u, dict): continue
                con.execute(
                    """
                    INSERT INTO users(telegram_id, username, full_name, email, wallet, language, blocked, region, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(telegram_id) DO UPDATE SET
                        username=excluded.username,
                        full_name=excluded.full_name,
                        email=excluded.email,
                        wallet=excluded.wallet,
                        language=excluded.language,
                        blocked=excluded.blocked,
                        region=excluded.region
                    """,
                    (int(u.get("telegram_id", uid_str)), u.get("username"), u.get("full_name", ""), u.get("email"), str(u.get("wallet", "0")), u.get("language", "en"), int(u.get("blocked", 0)), u.get("region"), u.get("created_at", datetime.now(timezone.utc).isoformat())),
                )

            # Sync orders
            fb_orders = fb_get("orders") or {}
            for oid, o in fb_orders.items():
                if not isinstance(o, dict): continue
                con.execute(
                    """
                    INSERT INTO orders(order_id, telegram_id, product_id, product_name, quantity, unit_price, total_amount, payment_method, status, delivered_content, invoice_pdf_token, created_at, paid_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id) DO UPDATE SET
                        payment_method=excluded.payment_method,
                        status=excluded.status,
                        delivered_content=excluded.delivered_content,
                        paid_at=excluded.paid_at
                    """,
                    (oid, int(o.get("telegram_id")), int(o.get("product_id")), o.get("product_name", ""), int(o.get("quantity", 1)), str(o.get("unit_price", "0")), str(o.get("total_amount", "0")), o.get("payment_method"), o.get("status", "PENDING"), o.get("delivered_content"), o.get("invoice_pdf_token"), o.get("created_at", datetime.now(timezone.utc).isoformat()), o.get("paid_at")),
                )

            # Sync processed transactions
            fb_tx = fb_get("processed_transactions") or {}
            for tx_hash, t in fb_tx.items():
                if not isinstance(t, dict): continue
                con.execute(
                    """
                    INSERT OR IGNORE INTO processed_transactions(tx_hash, invoice_id, chain, amount, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (tx_hash, t.get("invoice_id"), t.get("chain"), str(t.get("amount", "0")), t.get("created_at", datetime.now(timezone.utc).isoformat())),
                )

            con.commit()
            logging.info("Synced data from Firebase Realtime Database to local memory.")
    except Exception as e:
        logging.warning("Firebase startup sync error: %s", e)


def push_sqlite_to_firebase() -> dict:
    """Takes a full snapshot of local database and pushes all records to Firebase Realtime Database."""
    if not firebase_admin._apps:
        return {"ok": False, "error": "Firebase not connected"}
    counts = {"users": 0, "products": 0, "stock": 0, "orders": 0, "settings": 0}
    try:
        with db() as con:
            # Users
            users = con.execute("SELECT * FROM users").fetchall()
            u_dict = {str(u["telegram_id"]): dict(u) for u in users}
            if u_dict:
                fb_update("users", u_dict)
                counts["users"] = len(u_dict)

            # Products
            products = con.execute("SELECT * FROM products").fetchall()
            p_dict = {str(p["id"]): dict(p) for p in products}
            if p_dict:
                fb_update("products", p_dict)
                counts["products"] = len(p_dict)

            # Stock items
            stock = con.execute("SELECT * FROM stock_items").fetchall()
            s_dict = {str(s["id"]): dict(s) for s in stock}
            if s_dict:
                fb_update("stock_items", s_dict)
                counts["stock"] = len(s_dict)

            # Orders
            orders = con.execute("SELECT * FROM orders").fetchall()
            o_dict = {str(o["order_id"]): dict(o) for o in orders}
            if o_dict:
                fb_update("orders", o_dict)
                counts["orders"] = len(o_dict)

            # App settings
            settings = con.execute("SELECT * FROM app_settings").fetchall()
            set_dict = {str(s["key"]): str(s["value"]) for s in settings}
            if set_dict:
                fb_update("app_settings", set_dict)
                counts["settings"] = len(set_dict)

        logging.info("Full database snapshot pushed to Firebase: %s", counts)
        return {"ok": True, "counts": counts}
    except Exception as e:
        logging.error("Failed to push database to Firebase: %s", e)
        return {"ok": False, "error": str(e)}


async def auto_cloud_sync_loop():
    """Background worker that periodically backs up the database to Firebase Cloud."""
    while True:
        try:
            await asyncio.sleep(300)  # Every 5 minutes
            push_sqlite_to_firebase()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.debug("Auto cloud sync background error: %s", e)


def get_app_setting(key: str, default: str = "") -> str:
    with db() as con:
        row = con.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    return str(row["value"]) if row else default


def set_app_setting(key: str, value: str) -> None:
    with db() as con:
        con.execute(
            "INSERT INTO app_settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        con.commit()
    fb_set(f"app_settings/{key}", str(value))


def maintenance_enabled() -> bool:
    return get_app_setting("maintenance_mode", "0") == "1"


async def broadcast_maintenance(bot: Bot, enabled: bool):
    if enabled:
        text = (
            "🛠 <b>Premium Hubs Maintenance</b>\n\n"
            "We are currently performing maintenance. "
            "Shopping, purchases and wallet top-ups are temporarily unavailable.\n\n"
            "Please try again later. Thank you for your patience. 🙏"
        )
    else:
        text = (
            "✅ <b>Premium Hubs is Back Online</b>\n\n"
            "Maintenance has finished. Shopping and other services are available again. 🛒"
        )
    with db() as con:
        users = con.execute("SELECT telegram_id FROM users WHERE blocked=0").fetchall()
    for row in users:
        await safe_send(bot, row["telegram_id"], text)


async def maintenance_guard(callback: CallbackQuery) -> bool:
    if is_admin(callback.from_user.id) or not maintenance_enabled():
        return False
    await callback.answer("🛠 Bot is currently under maintenance.", show_alert=True)
    return True


def money(v) -> str:
    return format(Decimal(str(v)).normalize(), "f")


def register_user(user) -> None:
    if not user:
        return
    with db() as con:
        con.execute(
            """
            INSERT INTO users(telegram_id, username, full_name, blocked)
            VALUES (?, ?, ?, 0)
            ON CONFLICT(telegram_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                blocked=0
            """,
            (user.id, user.username, user.full_name),
        )
        con.commit()
    fb_update(f"users/{user.id}", {
        "telegram_id": user.id,
        "username": user.username or "",
        "full_name": user.full_name,
        "blocked": 0,
    })


def get_user(uid: int):
    with db() as con:
        return con.execute("SELECT * FROM users WHERE telegram_id=?", (uid,)).fetchone()


def set_email(uid: int, email: str | None):
    with db() as con:
        con.execute("UPDATE users SET email=? WHERE telegram_id=?", (email, uid))
        con.commit()
    fb_update(f"users/{uid}", {"email": email or ""})


def set_region(uid: int, region: str | None):
    with db() as con:
        con.execute("UPDATE users SET region=? WHERE telegram_id=?", (region, uid))
        con.commit()
    fb_update(f"users/{uid}", {"region": region or ""})


def set_language(uid: int, language: str):
    with db() as con:
        con.execute("UPDATE users SET language=? WHERE telegram_id=?", (language, uid))
        con.commit()
    fb_update(f"users/{uid}", {"language": language})


def user_orders(uid: int, limit: int = 15):
    with db() as con:
        return con.execute(
            "SELECT * FROM orders WHERE telegram_id=? ORDER BY id DESC LIMIT ?",
            (uid, limit),
        ).fetchall()


def list_products():
    with db() as con:
        return con.execute("SELECT * FROM products WHERE active=1 ORDER BY id DESC").fetchall()


def get_product(pid: int):
    with db() as con:
        return con.execute("SELECT * FROM products WHERE id=? AND active=1", (pid,)).fetchone()


def stock_count(pid: int) -> int:
    with db() as con:
        return int(con.execute(
            "SELECT COUNT(*) c FROM stock_items WHERE product_id=? AND status='AVAILABLE'",
            (pid,),
        ).fetchone()["c"])


def add_stock(pid: int, items: list[str]) -> int:
    clean = [x.strip() for x in items if x.strip()]
    with db() as con:
        cur = con.cursor()
        for x in clean:
            cur.execute("INSERT INTO stock_items(product_id, content) VALUES (?,?)", (pid, x))
            sid = cur.lastrowid
            fb_set(f"stock_items/{sid}", {
                "id": sid,
                "product_id": pid,
                "content": x,
                "status": "AVAILABLE",
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
        con.execute("DELETE FROM stock_alerts WHERE product_id=?", (pid,))
        con.commit()
    return len(clean)


def create_product(name: str, price: Decimal, warranty: str, note: str) -> int:
    with db() as con:
        cur = con.execute(
            "INSERT INTO products(name, price, warranty, note) VALUES (?,?,?,?)",
            (name, str(price), warranty, note),
        )
        con.commit()
        pid = int(cur.lastrowid)
    fb_set(f"products/{pid}", {
        "id": pid,
        "name": name,
        "price": str(price),
        "warranty": warranty,
        "note": note,
        "active": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    return pid


def update_product_price(pid: int, new_price: Decimal) -> None:
    with db() as con:
        con.execute("UPDATE products SET price=? WHERE id=? AND active=1", (str(new_price), pid))
        con.commit()
    fb_update(f"products/{pid}", {"price": str(new_price)})


def update_product_field(pid: int, field: str, value: str) -> None:
    allowed = {"name", "price", "warranty", "note"}
    if field not in allowed:
        raise ValueError("Unsupported product field")
    with db() as con:
        con.execute(f"UPDATE products SET {field}=? WHERE id=? AND active=1", (value, pid))
        con.commit()
    fb_update(f"products/{pid}", {field: value})


def soft_delete_product(pid: int) -> int:
    """Hide a product and remove only its unsold stock. Order history stays intact."""
    with db() as con:
        count = con.execute(
            "SELECT COUNT(*) c FROM stock_items WHERE product_id=? AND status='AVAILABLE'",
            (pid,),
        ).fetchone()["c"]
        con.execute("DELETE FROM stock_items WHERE product_id=? AND status='AVAILABLE'", (pid,))
        con.execute("DELETE FROM stock_alerts WHERE product_id=?", (pid,))
        con.execute("UPDATE products SET active=0 WHERE id=?", (pid,))
        con.commit()
    fb_update(f"products/{pid}", {"active": 0})
    return int(count)


def delete_available_stock(pid: int, amount: int | None = None) -> int:
    with db() as con:
        if amount is None:
            rows = con.execute(
                "SELECT id FROM stock_items WHERE product_id=? AND status='AVAILABLE' ORDER BY id ASC",
                (pid,),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT id FROM stock_items WHERE product_id=? AND status='AVAILABLE' ORDER BY id ASC LIMIT ?",
                (pid, amount),
            ).fetchall()
        ids = [r["id"] for r in rows]
        if not ids:
            return 0
        marks = ",".join("?" for _ in ids)
        con.execute(f"DELETE FROM stock_items WHERE id IN ({marks})", ids)
        con.execute("DELETE FROM stock_alerts WHERE product_id=?", (pid,))
        con.commit()
    for sid in ids:
        fb_delete(f"stock_items/{sid}")
    return len(ids)



def list_available_stock_items(pid: int):
    """Return the actual unsold stock rows so admin can manage items one by one."""
    with db() as con:
        return con.execute(
            """
            SELECT id, content, created_at
            FROM stock_items
            WHERE product_id=? AND status='AVAILABLE'
            ORDER BY id ASC
            """,
            (pid,),
        ).fetchall()


def delete_stock_item_by_id(pid: int, stock_item_id: int) -> bool:
    """Delete one exact AVAILABLE stock item. Sold stock/order history is never touched."""
    with db() as con:
        cur = con.execute(
            """
            DELETE FROM stock_items
            WHERE id=? AND product_id=? AND status='AVAILABLE'
            """,
            (stock_item_id, pid),
        )
        if cur.rowcount:
            con.execute("DELETE FROM stock_alerts WHERE product_id=?", (pid,))
        con.commit()
    if cur.rowcount:
        fb_delete(f"stock_items/{stock_item_id}")
    return bool(cur.rowcount)


def create_order(uid: int, product, qty: int) -> str:
    oid = f"ORD-{uuid4().hex[:10].upper()}"
    total = Decimal(product["price"]) * qty
    token = uuid4().hex + uuid4().hex
    with db() as con:
        con.execute(
            """
            INSERT INTO orders(
                order_id, telegram_id, product_id, product_name,
                quantity, unit_price, total_amount, status, invoice_pdf_token
            ) VALUES (?,?,?,?,?,?,?,'PENDING_PAYMENT',?)
            """,
            (oid, uid, product["id"], product["name"], qty, product["price"], str(total), token),
        )
        con.commit()
    fb_set(f"orders/{oid}", {
        "order_id": oid,
        "telegram_id": uid,
        "product_id": product["id"],
        "product_name": product["name"],
        "quantity": qty,
        "unit_price": str(product["price"]),
        "total_amount": str(total),
        "status": "PENDING_PAYMENT",
        "invoice_pdf_token": token,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    return oid


def get_order(oid: str):
    with db() as con:
        return con.execute("SELECT * FROM orders WHERE order_id=?", (oid,)).fetchone()


def update_order(oid: str, **fields):
    allowed = {"payment_method", "status", "delivered_content", "paid_at"}
    clean = {k: v for k, v in fields.items() if k in allowed}
    if not clean:
        return
    sql = ", ".join(f"{k}=?" for k in clean)
    with db() as con:
        con.execute(f"UPDATE orders SET {sql} WHERE order_id=?", (*clean.values(), oid))
        con.commit()
    fb_update(f"orders/{oid}", clean)


def change_wallet(uid: int, amount: Decimal, tx_type: str, ref: str, note: str = "") -> Decimal:
    with db() as con:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute("SELECT wallet FROM users WHERE telegram_id=?", (uid,)).fetchone()
        if not row:
            con.rollback()
            raise ValueError("User not found")
        before = Decimal(row["wallet"])
        after = before + amount
        if after < 0:
            con.rollback()
            raise ValueError("Insufficient balance")
        con.execute("UPDATE users SET wallet=? WHERE telegram_id=?", (str(after), uid))
        tx_id = f"TX-{uuid4().hex[:14].upper()}"
        con.execute(
            """
            INSERT INTO wallet_transactions(
                tx_id, telegram_id, type, amount, balance_before, balance_after, reference_id, note
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (tx_id, uid, tx_type, str(amount), str(before), str(after), ref, note),
        )
        con.commit()
    fb_update(f"users/{uid}", {"wallet": str(after)})
    fb_set(f"wallet_transactions/{tx_id}", {
        "tx_id": tx_id,
        "telegram_id": uid,
        "type": tx_type,
        "amount": str(amount),
        "balance_before": str(before),
        "balance_after": str(after),
        "reference_id": ref,
        "note": note,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    return after


def take_stock(pid: int, qty: int, oid: str) -> list[str]:
    with db() as con:
        con.execute("BEGIN IMMEDIATE")
        rows = con.execute(
            "SELECT id, content FROM stock_items WHERE product_id=? AND status='AVAILABLE' ORDER BY id LIMIT ?",
            (pid, qty),
        ).fetchall()
        if len(rows) < qty:
            con.rollback()
            return []
        ids = [r["id"] for r in rows]
        marks = ",".join("?" for _ in ids)
        con.execute(
            f"UPDATE stock_items SET status='SOLD', order_id=?, sold_at=CURRENT_TIMESTAMP WHERE id IN ({marks})",
            (oid, *ids),
        )
        con.commit()
    for sid in ids:
        fb_update(f"stock_items/{sid}", {
            "status": "SOLD",
            "order_id": oid,
            "sold_at": datetime.now(timezone.utc).isoformat(),
        })
    return [r["content"] for r in rows]


SUPPORTED_CHAINS = {
    "bsc": {
        "name": "BNB Smart Chain (BEP20)",
        "badge": "🟡 BEP20 (BSC)",
        "currency": "USDT",
        "explorer": "https://bscscan.com/tx/",
    },
    "polygon": {
        "name": "Polygon (MATIC Network)",
        "badge": "🟣 Polygon",
        "currency": "USDT",
        "explorer": "https://polygonscan.com/tx/",
    },
    "ethereum": {
        "name": "Ethereum (ERC20)",
        "badge": "🔷 ERC20 (Ethereum)",
        "currency": "USDT",
        "explorer": "https://etherscan.io/tx/",
    },
}

CACHED_GATEWAY_WALLET: str = ""


def save_crypto_invoice(invoice_id: str, uid: int, kind: str, ref: str, amount, currency: str, chain: str, deposit_address: str):
    with db() as con:
        con.execute(
            """
            INSERT INTO payment_invoices(
                invoice_id, telegram_id, payment_kind, reference_id,
                invoice_amount, invoice_currency, chain, deposit_address, status
            ) VALUES (?,?,?,?,?,?,?,?,'PENDING')
            ON CONFLICT(invoice_id) DO UPDATE SET
                invoice_amount=excluded.invoice_amount,
                invoice_currency=excluded.invoice_currency,
                chain=excluded.chain,
                deposit_address=excluded.deposit_address
            """,
            (invoice_id, uid, kind, ref, str(amount), currency, chain, deposit_address),
        )
        con.commit()
    fb_set(f"payment_invoices/{invoice_id}", {
        "invoice_id": invoice_id,
        "telegram_id": uid,
        "payment_kind": kind,
        "reference_id": ref,
        "invoice_amount": str(amount),
        "invoice_currency": currency,
        "chain": chain,
        "deposit_address": deposit_address,
        "status": "PENDING",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })


def get_saved_invoice(invoice_id: str):
    with db() as con:
        return con.execute("SELECT * FROM payment_invoices WHERE invoice_id=?", (invoice_id,)).fetchone()


def is_tx_processed(tx_hash: str) -> bool:
    clean_tx = tx_hash.strip().lower()
    with db() as con:
        row1 = con.execute("SELECT 1 FROM processed_transactions WHERE lower(tx_hash)=?", (clean_tx,)).fetchone()
        if row1:
            return True
        row2 = con.execute("SELECT 1 FROM payment_invoices WHERE lower(tx_hash)=? AND status='PAID'", (clean_tx,)).fetchone()
        return bool(row2)


def record_processed_tx(tx_hash: str, invoice_id: str, chain: str, amount: str):
    clean_tx = tx_hash.strip().lower()
    with db() as con:
        con.execute(
            """
            INSERT OR IGNORE INTO processed_transactions(tx_hash, invoice_id, chain, amount)
            VALUES (?, ?, ?, ?)
            """,
            (clean_tx, invoice_id, chain, str(amount)),
        )
        con.commit()
    fb_set(f"processed_transactions/{clean_tx}", {
        "tx_hash": clean_tx,
        "invoice_id": invoice_id,
        "chain": chain,
        "amount": str(amount),
        "created_at": datetime.now(timezone.utc).isoformat(),
    })


def mark_webhook_processed(invoice_id: str, tx_hash: str) -> bool:
    try:
        with db() as con:
            con.execute(
                "INSERT INTO processed_webhooks(invoice_id, tx_hash) VALUES (?,?)",
                (invoice_id, tx_hash),
            )
            con.commit()
        return True
    except sqlite3.IntegrityError:
        return False


# =========================
# CRYPTOPAY GATEWAY API
# =========================

async def crypto_gateway_api(method: str, path: str, **kwargs):
    if not GATEWAY_API_KEY:
        raise RuntimeError("GATEWAY_API_KEY is not configured in .env")
    headers = kwargs.pop("headers", {})
    headers.update({
        "x-api-key": GATEWAY_API_KEY,
        "Authorization": f"Bearer {GATEWAY_API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    timeout = aiohttp.ClientTimeout(total=25)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.request(method, f"{GATEWAY_BASE_URL}{path}", headers=headers, **kwargs) as r:
            text = await r.text()
            if r.status >= 400:
                try:
                    err_json = json.loads(text)
                    err_msg = err_json.get("error") or err_json.get("message") or text
                    if "details" in err_json:
                        err_msg += f": {json.dumps(err_json['details'])}"
                except Exception:
                    err_msg = text
                raise RuntimeError(f"CryptoPay Error ({r.status}): {err_msg}")
            return json.loads(text) if text else {}


async def crypto_get_status() -> dict:
    return await crypto_gateway_api("GET", "/api/public/payments/status")


async def get_gateway_deposit_address(chain: str = "") -> str:
    global CACHED_GATEWAY_WALLET
    chain_clean = str(chain).lower().strip()
    if chain_clean == "bsc" and DEPOSIT_WALLET_BSC:
        return DEPOSIT_WALLET_BSC
    elif chain_clean == "polygon" and DEPOSIT_WALLET_POLYGON:
        return DEPOSIT_WALLET_POLYGON
    elif (chain_clean == "ethereum" or chain_clean == "eth") and DEPOSIT_WALLET_ETH:
        return DEPOSIT_WALLET_ETH

    if DEPOSIT_WALLET_ADDRESS:
        return DEPOSIT_WALLET_ADDRESS
    if CACHED_GATEWAY_WALLET:
        return CACHED_GATEWAY_WALLET
    try:
        data = await crypto_get_status()
        addr = str(data.get("walletAddress") or "").strip()
        if addr and addr.lower() != "none" and addr.lower() != "null":
            CACHED_GATEWAY_WALLET = addr
            return addr
    except Exception as e:
        logging.warning("Could not fetch deposit address from status API: %s", e)
    return DEPOSIT_WALLET_ADDRESS or ""


async def crypto_verify_payment(tx_hash: str, chain: str, expected_amount: str, notify: bool = True) -> dict:
    body = {
        "txHash": tx_hash.strip(),
        "chain": chain,
        "expectedAmount": str(expected_amount),
        "minConfirmations": 1,
        "notify": notify,
    }
    addr = await get_gateway_deposit_address(chain)
    if addr:
        body["walletAddress"] = addr
    return await crypto_gateway_api("POST", "/api/public/payments/verify", json=body)


async def crypto_get_deposits(chain: str | None = None, limit: int = 25) -> dict:
    params = {"limit": str(limit)}
    if chain:
        params["chain"] = chain
    addr = await get_gateway_deposit_address(chain or "")
    if addr:
        params["address"] = addr
    return await crypto_gateway_api("GET", "/api/public/payments/deposits", params=params)


# =========================
# INVOICE EMAIL + PDF
# =========================

def invoice_pdf_bytes(order, user) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=18*mm, rightMargin=18*mm, topMargin=16*mm, bottomMargin=16*mm)
    styles = getSampleStyleSheet()
    title = styles["Title"].clone("InvoiceTitle"); title.fontSize=22; title.leading=26
    normal = styles["Normal"].clone("InvoiceNormal"); normal.fontSize=10; normal.leading=14
    small = styles["Normal"].clone("InvoiceSmall"); small.fontSize=9; small.leading=12
    status = str(order["status"] or "").upper()
    story = [Paragraph(f"<b>{html.escape(BOT_NAME)}</b>", title), Paragraph("PAYMENT INVOICE", small), Spacer(1,12)]
    meta = Table([[f"Invoice No.\n{order['order_id']}", f"Status\n{status}"],[f"Date\n{order['created_at']}", f"Payment Method\n{order['payment_method'] or '-'}"]], colWidths=[82*mm,82*mm])
    meta.setStyle(TableStyle([("BOX",(0,0),(-1,-1),0.7,"#D0D5DD"),("INNERGRID",(0,0),(-1,-1),0.5,"#EAECF0"),("BACKGROUND",(0,0),(-1,-1),"#F9FAFB"),("FONTNAME",(0,0),(-1,-1),"Helvetica-Bold"),("VALIGN",(0,0),(-1,-1),"TOP"),("PADDING",(0,0),(-1,-1),9)]))
    story += [meta, Spacer(1,14)]
    bill = Table([["BILL TO", ""],["Customer Email", str(user['email'] or '-')],["Telegram ID", str(order['telegram_id'])]], colWidths=[45*mm,119*mm])
    bill.setStyle(TableStyle([("SPAN",(0,0),(1,0)),("BACKGROUND",(0,0),(1,0),"#111827"),("TEXTCOLOR",(0,0),(1,0),"#FFFFFF"),("FONTNAME",(0,0),(1,0),"Helvetica-Bold"),("FONTNAME",(0,1),(0,-1),"Helvetica-Bold"),("GRID",(0,1),(-1,-1),0.5,"#EAECF0"),("PADDING",(0,0),(-1,-1),8)]))
    story += [bill, Spacer(1,14)]
    items = Table([["PRODUCT","QTY","UNIT PRICE","TOTAL"],[str(order['product_name']),str(order['quantity']),f"{money(order['unit_price'])} {INVOICE_CURRENCY}",f"{money(order['total_amount'])} {INVOICE_CURRENCY}"]], colWidths=[75*mm,20*mm,35*mm,34*mm])
    items.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),"#111827"),("TEXTCOLOR",(0,0),(-1,0),"#FFFFFF"),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("GRID",(0,0),(-1,-1),0.5,"#D0D5DD"),("ALIGN",(1,1),(-1,-1),"RIGHT"),("PADDING",(0,0),(-1,-1),8)]))
    story += [items, Spacer(1,12)]
    total = Table([["TOTAL PAID", f"{money(order['total_amount'])} {INVOICE_CURRENCY}"]], colWidths=[100*mm,64*mm])
    total.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),"#F2F4F7"),("FONTNAME",(0,0),(-1,-1),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),12),("ALIGN",(1,0),(1,0),"RIGHT"),("BOX",(0,0),(-1,-1),0.8,"#D0D5DD"),("PADDING",(0,0),(-1,-1),10)]))
    story += [total, Spacer(1,20), Paragraph(f"Thank you for shopping with <b>{html.escape(BOT_NAME)}</b>.", normal), Paragraph(f"Support: {html.escape(SUPPORT_USERNAME)}", small), Paragraph("This invoice was generated automatically.", small)]
    doc.build(story); return buf.getvalue()

def invoice_html(order, user, pdf_url: str) -> str:
    safe=lambda x: html.escape(str(x)); status=safe(str(order['status'] or '').upper()); payment=safe(order['payment_method'] or '-')
    return f"""<!doctype html><html><body style='margin:0;background:#eef2f6;font-family:Arial,Helvetica,sans-serif;color:#101828'><div style='padding:28px 12px'><div style='max-width:700px;margin:0 auto;background:#fff;border-radius:22px;overflow:hidden;box-shadow:0 14px 45px rgba(16,24,40,.10)'><div style='background:#111827;color:#fff;padding:30px 32px'><div style='font-size:13px;letter-spacing:1.6px;opacity:.75'>PAYMENT INVOICE</div><div style='font-size:28px;font-weight:800;margin-top:7px'>{safe(BOT_NAME)}</div><div style='margin-top:14px;display:inline-block;padding:7px 12px;border-radius:999px;background:#166534;color:#dcfce7;font-size:12px;font-weight:700'>✓ {status}</div></div><div style='padding:30px 32px'><table width='100%' style='border-collapse:collapse;margin-bottom:24px'><tr><td style='color:#667085;font-size:12px'>INVOICE NO.</td><td style='color:#667085;font-size:12px;text-align:right'>DATE</td></tr><tr><td style='font-weight:700'>{safe(order['order_id'])}</td><td style='font-weight:700;text-align:right'>{safe(order['created_at'])}</td></tr></table><div style='border:1px solid #e4e7ec;border-radius:14px;padding:18px;margin-bottom:22px;background:#f9fafb'><div style='font-size:12px;color:#667085;margin-bottom:8px'>BILL TO</div><div style='font-weight:700'>{safe(user['email'] or '-')}</div><div style='font-size:13px;color:#667085;margin-top:4px'>Telegram ID: {safe(order['telegram_id'])}</div></div><table width='100%' style='border-collapse:collapse;border:1px solid #e4e7ec'><tr style='background:#111827;color:#fff'><td style='padding:13px 14px;font-size:12px;font-weight:700'>PRODUCT</td><td style='padding:13px 14px;font-size:12px;font-weight:700;text-align:center'>QTY</td><td style='padding:13px 14px;font-size:12px;font-weight:700;text-align:right'>PRICE</td></tr><tr><td style='padding:16px 14px;font-weight:700'>{safe(order['product_name'])}</td><td style='padding:16px 14px;text-align:center'>{safe(order['quantity'])}</td><td style='padding:16px 14px;text-align:right'>{safe(money(order['unit_price']))} {safe(INVOICE_CURRENCY)}</td></tr></table><table width='100%' style='margin-top:18px;border-collapse:collapse'><tr><td style='padding:7px 0;color:#667085'>Payment Method</td><td style='padding:7px 0;text-align:right;font-weight:700'>{payment}</td></tr><tr><td style='padding:12px 0 4px;font-size:17px;font-weight:800'>Total Paid</td><td style='padding:12px 0 4px;text-align:right;font-size:19px;font-weight:800'>{safe(money(order['total_amount']))} {safe(INVOICE_CURRENCY)}</td></tr></table><div style='margin-top:26px;text-align:center'><a href='{safe(pdf_url)}' style='display:inline-block;background:#111827;color:#fff;text-decoration:none;padding:14px 24px;border-radius:11px;font-weight:800'>⬇ Download PDF Invoice</a></div><div style='margin-top:28px;padding-top:20px;border-top:1px solid #e4e7ec;color:#667085;font-size:12px;line-height:1.7;text-align:center'>Thank you for shopping with {safe(BOT_NAME)}.<br>Support: {safe(SUPPORT_USERNAME)}<br>Secure automatic invoice • Keep this email for your records.</div></div></div></div></body></html>"""


async def send_invoice_email(order) -> bool:
    user = get_user(order["telegram_id"])
    if not user or not user["email"] or not SMTP_HOST or not SMTP_USER or not SMTP_PASSWORD or not PUBLIC_BASE_URL:
        return False
    pdf_url = f"{PUBLIC_BASE_URL}/invoice/{order['invoice_pdf_token']}"
    body = invoice_html(order, user, pdf_url)

    def _send():
        msg = EmailMessage()
        msg["Subject"] = f"{BOT_NAME} Invoice - {order['order_id']}"
        msg["From"] = SMTP_FROM
        msg["To"] = user["email"]
        msg.set_content(f"Invoice {order['order_id']}\nTotal: {order['total_amount']} {INVOICE_CURRENCY}\nPDF: {pdf_url}")
        msg.add_alternative(body, subtype="html")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASSWORD)
            s.send_message(msg)

    try:
        await asyncio.to_thread(_send)
        return True
    except Exception:
        logging.exception("Invoice email failed")
        return False


# =========================
# TRANSLATIONS
# =========================

TRANSLATIONS = {
    "en": {
        "welcome":"✨ <b>Welcome to {bot}</b>", "balance":"👛 Balance: <b>{amount} {currency}</b>", "choose":"👇 Choose an option:",
        "shop":"🛒 Shop", "topup":"💰 Topup Wallet", "settings":"⚙️ Settings", "support":"🎧 Support", "channel":"📢 Channel",
        "back":"◀ Back", "back_products":"◀️ Back to Products", "refresh":"🔄 Refresh", "prev":"⬅ Prev", "next":"Next ➡",
        "buy":"🛒 Buy Now", "custom_qty":"🔢 Custom Quantity", "copy_link":"🔗 Copy Link", "view_note":"📝 View Note",
        "available_products":"🛍 <b>Available Products</b>", "select_product":"👇 Select a product:",
        "price":"💵 Price", "stock":"📦 Available Stock", "warranty":"🛡️ Warranty", "selected":"🔢 Selected Qty", "total":"🧾 Total", "wallet":"👛 Wallet Balance",
        "not_found":"Product not found.", "note":"Note", "no_note":"No note added yet.", "current":"Current", "clear":"Clear", "max":"Max", "confirm":"✅ Confirm",
        "email_required":"⚠️ <b>Email Required</b>\n\nPlease set your email before checkout.", "set_email":"📧 Set Email", "not_enough":"Not enough stock for this quantity.",
        "select_payment":"💳 <b>Select Payment Network</b>", "quantity":"🔢 Quantity", "direct":"⚡ Direct Pay", "wallet_pay":"👛 Wallet Balance",
        "order_unavailable":"Order unavailable.", "insufficient":"Insufficient wallet balance.", "wallet_success":"✅ <b>Wallet Payment Successful</b>", "paid":"Paid",
        "payment_unavailable":"Payment service unavailable.", "payment_inactive":"CryptoPay Gateway is not active. Please check GATEWAY_API_KEY.", "amount":"Amount", "auto_webhook":"Select a network to get the deposit address and pay.",
        "topup_title":"💰 <b>Topup Wallet</b>", "topup_prompt":"Send the amount in {currency} you want to add to your wallet:", "positive":"❌ Send a valid positive amount.",
        "methods_unavailable":"Payment methods unavailable.", "session_expired":"Payment session expired.", "invoice_failed":"Could not create invoice.", "open_qr":"📷 Open QR",
        "cancel_invoice":"❌ Cancel Invoice", "payment_invoice":"🧾 <b>Payment Invoice</b>", "invoice":"Invoice", "pay_exactly":"💵 Pay exactly", "network":"🌐 Network", "address":"📍 Address", "expires":"⏱ Expires",
        "submit_tx":"🔍 Submit Tx Hash (Verify)", "verify_again":"🔄 Verify Again",
        "auto_confirm":"💡 After sending crypto, submit your Transaction Hash (TxID) to verify automatically.", "invoice_cancelled":"❎ <b>Invoice Cancelled</b>",
        "profile":"⚙️ <b>User Profile</b>", "first_name":"🪪 First Name", "username":"👤 Username", "status":"🚀 Status", "started":"started bot", "email":"📧 Email",
        "currency":"🪙 Currency", "language":"🌐 Language", "region":"🗺️ Region", "joined":"📅 Joined", "not_set":"Not set", "region_missing":"<i>not set</i> — tap <b>Set Region</b>", "saved":" — Has been Saved! ✨",
        "my_orders":"📦 My Orders", "email_btn":"📧 Email", "language_btn":"🌐 Language", "set_region":"🗺️ Set Region", "choose_region":"🗺️ <b>Choose Your Region</b>\n\nSelect your region below:",
        "no_orders":"No orders yet.", "order_status":"Status", "select_language":"🌐 <b>Select Language</b>\n\nChoose your preferred language:", "unsupported":"Unsupported language.",
        "email_settings":"📧 <b>Email Settings</b>", "current_email":"Current Email", "change_email":"✏️ Change Email", "delete_email":"🗑️ Delete Email", "back_settings":"◀️ Back to Settings",
        "send_email":"📧 <b>Send your email address.</b>", "invalid_email":"❌ Invalid email. Send a valid email address.", "email_saved":"✅ <b>Email Saved</b>", "email_deleted":"✅ <b>Email Deleted</b>",
        "channel_missing":"Public channel is not configured yet.", "support_text":"🎧 <b>Support</b>\n\nContact: @{username}", "invoice_sent":"📧 HTML invoice sent to your email. The email contains a secure PDF download button."
    },
    "hi": {
        "welcome":"✨ <b>{bot} में आपका स्वागत है</b>", "balance":"👛 बैलेंस: <b>{amount} {currency}</b>", "choose":"👇 एक विकल्प चुनें:",
        "shop":"🛒 शॉप", "topup":"💰 वॉलेट टॉपअप", "settings":"⚙️ सेटिंग्स", "support":"🎧 सहायता", "channel":"📢 चैनल", "back":"◀ वापस", "back_products":"◀️ प्रोडक्ट्स पर वापस",
        "refresh":"🔄 रिफ्रेश", "prev":"⬅ पिछला", "next":"अगला ➡", "buy":"🛒 अभी खरीदें", "custom_qty":"🔢 कस्टम क्वांटिटी", "copy_link":"🔗 लिंक कॉपी", "view_note":"📝 नोट देखें",
        "available_products":"🛍 <b>उपलब्ध प्रोडक्ट्स</b>", "select_product":"👇 एक प्रोडक्ट चुनें:", "price":"💵 कीमत", "stock":"📦 उपलब्ध स्टॉक", "warranty":"🛡️ वारंटी", "selected":"🔢 चुनी मात्रा", "total":"🧾 कुल", "wallet":"👛 वॉलेट बैलेंस",
        "not_found":"प्रोडक्ट नहीं मिला।", "note":"नोट", "no_note":"अभी कोई नोट नहीं है।", "current":"वर्तमान", "clear":"साफ़", "max":"अधिकतम", "confirm":"✅ कन्फर्म",
        "email_required":"⚠️ <b>ईमेल आवश्यक है</b>\n\nचेकआउट से पहले ईमेल सेट करें।", "set_email":"📧 ईमेल सेट करें", "not_enough":"पर्याप्त स्टॉक नहीं है।", "select_payment":"💳 <b>पेमेंट नेटवर्क चुनें</b>", "quantity":"🔢 मात्रा", "direct":"⚡ डायरेक्ट पे", "wallet_pay":"👛 वॉलेट बैलेंस",
        "order_unavailable":"ऑर्डर उपलब्ध नहीं है।", "insufficient":"वॉलेट बैलेंस पर्याप्त नहीं है।", "wallet_success":"✅ <b>वॉलेट पेमेंट सफल</b>", "paid":"भुगतान", "payment_unavailable":"पेमेंट सेवा उपलब्ध नहीं है।", "payment_inactive":"CryptoPay Gateway सक्रिय नहीं है।", "amount":"राशि", "auto_webhook":"डिपॉजिट एड्रेस पाने के लिए नेटवर्क चुनें।",
        "topup_title":"💰 <b>वॉलेट टॉपअप</b>", "topup_prompt":"{currency} में राशि भेजें जो आप वॉलेट में जोड़ना चाहते हैं:", "positive":"❌ सही positive amount भेजें।", "methods_unavailable":"पेमेंट मेथड उपलब्ध नहीं हैं।", "session_expired":"पेमेंट सेशन समाप्त हो गया।", "invoice_failed":"इनवॉइस नहीं बन सका।", "open_qr":"📷 QR खोलें", "cancel_invoice":"❌ इनवॉइस कैंसल",
        "submit_tx":"🔍 Tx Hash भेजें (Verify)", "verify_again":"🔄 फिर से Verify करें",
        "payment_invoice":"🧾 <b>पेमेंट इनवॉइस</b>", "invoice":"इनवॉइस", "pay_exactly":"💵 ठीक इतना भुगतान करें", "network":"🌐 नेटवर्क", "address":"📍 एड्रेस", "expires":"⏱ समाप्ति", "auto_confirm":"💡 क्रिप्टो भेजने के बाद Transaction Hash भेजकर verify करें।", "invoice_cancelled":"❎ <b>इनवॉइस कैंसल</b>",
        "profile":"⚙️ <b>यूज़र प्रोफाइल</b>", "first_name":"🪪 नाम", "username":"👤 यूज़रनेम", "status":"🚀 स्टेटस", "started":"bot शुरू किया", "email":"📧 ईमेल", "currency":"🪙 करेंसी", "language":"🌐 भाषा", "region":"🗺️ क्षेत्र", "joined":"📅 जुड़ने की तारीख", "not_set":"सेट नहीं", "region_missing":"<i>सेट नहीं</i> — <b>Set Region</b> दबाएँ", "saved":" — सेव है ✨",
        "my_orders":"📦 मेरे ऑर्डर", "email_btn":"📧 ईमेल", "language_btn":"🌐 भाषा", "set_region":"🗺️ क्षेत्र सेट करें", "choose_region":"🗺️ <b>अपना क्षेत्र चुनें</b>\n\nनीचे से चुनें:", "no_orders":"अभी कोई ऑर्डर नहीं।", "order_status":"स्टेटस", "select_language":"🌐 <b>भाषा चुनें</b>\n\nअपनी पसंदीदा भाषा चुनें:", "unsupported":"यह भाषा समर्थित नहीं है।",
        "email_settings":"📧 <b>ईमेल सेटिंग्स</b>", "current_email":"वर्तमान ईमेल", "change_email":"✏️ ईमेल बदलें", "delete_email":"🗑️ ईमेल हटाएँ", "back_settings":"◀️ सेटिंग्स पर वापस", "send_email":"📧 <b>अपना ईमेल एड्रेस भेजें।</b>", "invalid_email":"❌ सही ईमेल भेजें।", "email_saved":"✅ <b>ईमेल सेव हुआ</b>", "email_deleted":"✅ <b>ईमेल हटाया गया</b>", "channel_missing":"Public channel configure नहीं है।", "support_text":"🎧 <b>सहायता</b>\n\nसंपर्क: @{username}", "invoice_sent":"📧 HTML invoice आपके ईमेल पर भेजा गया है, साथ में secure PDF download button है।"
    },
    "ur": {}, "ar": {}, "es": {}, "id": {}
}

# Fill missing languages from English first, then override core UI texts.
for _code in ("ur", "ar", "es", "id"):
    TRANSLATIONS[_code] = dict(TRANSLATIONS["en"])

TRANSLATIONS["ur"].update({"welcome":"✨ <b>{bot} میں خوش آمدید</b>","balance":"👛 بیلنس: <b>{amount} {currency}</b>","choose":"👇 ایک آپشن منتخب کریں:","shop":"🛒 شاپ","topup":"💰 والٹ ٹاپ اپ","settings":"⚙️ سیٹنگز","support":"🎧 سپورٹ","channel":"📢 چینل","back":"◀ واپس","back_products":"◀️ پروڈکٹس پر واپس","refresh":"🔄 ریفریش","buy":"🛒 ابھی خریدیں","custom_qty":"🔢 کسٹم مقدار","copy_link":"🔗 لنک کاپی","view_note":"📝 نوٹ دیکھیں","available_products":"🛍 <b>دستیاب پروڈکٹس</b>","select_product":"👇 ایک پروڈکٹ منتخب کریں:","price":"💵 قیمت","stock":"📦 دستیاب اسٹاک","warranty":"🛡️ وارنٹی","selected":"🔢 منتخب مقدار","total":"🧾 کل","wallet":"👛 والٹ بیلنس","profile":"⚙️ <b>یوزر پروفائل</b>","my_orders":"📦 میرے آرڈرز","language_btn":"🌐 زبان","set_region":"🗺️ علاقہ سیٹ کریں","select_language":"🌐 <b>زبان منتخب کریں</b>\n\nاپنی پسند کی زبان منتخب کریں:"})
TRANSLATIONS["ar"].update({"welcome":"✨ <b>مرحبًا بك في {bot}</b>","balance":"👛 الرصيد: <b>{amount} {currency}</b>","choose":"👇 اختر خيارًا:","shop":"🛒 المتجر","topup":"💰 شحن المحفظة","settings":"⚙️ الإعدادات","support":"🎧 الدعم","channel":"📢 القناة","back":"◀ رجوع","back_products":"◀️ العودة للمنتجات","refresh":"🔄 تحديث","buy":"🛒 اشترِ الآن","custom_qty":"🔢 كمية مخصصة","copy_link":"🔗 نسخ الرابط","view_note":"📝 عرض الملاحظة","available_products":"🛍 <b>المنتجات المتاحة</b>","select_product":"👇 اختر منتجًا:","price":"💵 السعر","stock":"📦 المخزون المتاح","warranty":"🛡️ الضمان","selected":"🔢 الكمية المختارة","total":"🧾 الإجمالي","wallet":"👛 رصيد المحفظة","profile":"⚙️ <b>ملف المستخدم</b>","my_orders":"📦 طلباتي","language_btn":"🌐 اللغة","set_region":"🗺️ تحديد المنطقة","select_language":"🌐 <b>اختر اللغة</b>\n\nاختر لغتك المفضلة:"})
TRANSLATIONS["es"].update({"welcome":"✨ <b>Bienvenido a {bot}</b>","balance":"👛 Saldo: <b>{amount} {currency}</b>","choose":"👇 Elige una opción:","shop":"🛒 Tienda","topup":"💰 Recargar saldo","settings":"⚙️ Ajustes","support":"🎧 Soporte","channel":"📢 Canal","back":"◀ Volver","back_products":"◀️ Volver a productos","refresh":"🔄 Actualizar","buy":"🛒 Comprar ahora","custom_qty":"🔢 Cantidad personalizada","copy_link":"🔗 Copiar enlace","view_note":"📝 Ver nota","available_products":"🛍 <b>Productos disponibles</b>","select_product":"👇 Elige un producto:","price":"💵 Precio","stock":"📦 Stock disponible","warranty":"🛡️ Garantía","selected":"🔢 Cantidad elegida","total":"🧾 Total","wallet":"👛 Saldo de cartera","profile":"⚙️ <b>Perfil de usuario</b>","my_orders":"📦 Mis pedidos","language_btn":"🌐 Idioma","set_region":"🗺️ Configurar región","select_language":"🌐 <b>Selecciona idioma</b>\n\nElige tu idioma preferido:"})
TRANSLATIONS["id"].update({"welcome":"✨ <b>Selamat datang di {bot}</b>","balance":"👛 Saldo: <b>{amount} {currency}</b>","choose":"👇 Pilih opsi:","shop":"🛒 Toko","topup":"💰 Isi saldo","settings":"⚙️ Pengaturan","support":"🎧 Dukungan","channel":"📢 Channel","back":"◀ Kembali","back_products":"◀️ Kembali ke produk","refresh":"🔄 Refresh","buy":"🛒 Beli sekarang","custom_qty":"🔢 Jumlah custom","copy_link":"🔗 Salin link","view_note":"📝 Lihat catatan","available_products":"🛍 <b>Produk tersedia</b>","select_product":"👇 Pilih produk:","price":"💵 Harga","stock":"📦 Stok tersedia","warranty":"🛡️ Garansi","selected":"🔢 Jumlah dipilih","total":"🧾 Total","wallet":"👛 Saldo wallet","profile":"⚙️ <b>Profil pengguna</b>","my_orders":"📦 Pesanan saya","language_btn":"🌐 Bahasa","set_region":"🗺️ Atur wilayah","select_language":"🌐 <b>Pilih bahasa</b>\n\nPilih bahasa yang diinginkan:"})

def user_language(uid):
    if not uid: return "en"
    try:
        u=get_user(uid); code=str((u["language"] if u else "en") or "en").lower()
        return code if code in TRANSLATIONS else "en"
    except Exception:
        return "en"

def tr(uid, key, **kwargs):
    code=user_language(uid); value=TRANSLATIONS.get(code, TRANSLATIONS["en"]).get(key, TRANSLATIONS["en"].get(key,key))
    try: return value.format(**kwargs)
    except Exception: return value

def tr_user(user, key, **kwargs):
    try: code=str((user["language"] or "en")).lower()
    except Exception: code="en"
    if code not in TRANSLATIONS: code="en"
    value=TRANSLATIONS[code].get(key, TRANSLATIONS["en"].get(key,key))
    try: return value.format(**kwargs)
    except Exception: return value

# =========================
# UI HELPERS
# =========================

def is_admin(uid: int) -> bool:
    return ADMIN_ID and uid == ADMIN_ID


def main_kb(uid=None):
    channel_button = InlineKeyboardButton(text=tr(uid,"channel"), url=PUBLIC_CHANNEL_URL, style="success") if PUBLIC_CHANNEL_URL else InlineKeyboardButton(text=tr(uid,"channel"), callback_data="menu:channel", style="success")
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=tr(uid,"shop"),callback_data="menu:products",style="success")],[InlineKeyboardButton(text=tr(uid,"topup"),callback_data="menu:topup",style="primary"),InlineKeyboardButton(text=tr(uid,"settings"),callback_data="menu:settings",style="success")],[InlineKeyboardButton(text=tr(uid,"support"),callback_data="menu:support",style="primary"),channel_button]])


def back_home(uid=None):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=tr(uid,"back"), callback_data="menu:home", style="danger")]])


async def edit(callback: CallbackQuery, text: str, kb: InlineKeyboardMarkup):
    """Update the existing bot message in-place.

    Important: pressing Refresh must never create a duplicate message.
    Telegram raises "message is not modified" when content is unchanged;
    we simply acknowledge the callback instead of sending a new message.
    """
    if callback.message:
        try:
            await callback.message.edit_text(text, reply_markup=kb)
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                logging.warning("Could not edit UI message: %s", exc)
        except Exception:
            logging.exception("Could not update UI message")
    with suppress(Exception):
        await callback.answer()


def welcome_text(user) -> str:
    return tr_user(user,"welcome",bot=html.escape(BOT_NAME))+"\n\n"+tr_user(user,"balance",amount=money(user["wallet"]),currency=INVOICE_CURRENCY)+"\n"+tr_user(user,"choose")


def product_icon(name: str) -> str:
    """Return a compact brand-like emoji for common digital products.

    Telegram inline buttons cannot embed a normal image logo, so we use a
    recognizable emoji/symbol based on the product name.
    """
    n = (name or "").lower()
    if "youtube" in n:
        return "▶️"
    if "netflix" in n:
        return "🎬"
    if "chatgpt" in n or "openai" in n:
        return "🤖"
    if "gemini" in n:
        return "✨"
    if "canva" in n:
        return "🎨"
    if "capcut" in n:
        return "✂️"
    if "nord" in n or "vpn" in n:
        return "🛡️"
    if "surfshark" in n:
        return "🌊"
    if "quillbot" in n:
        return "✍️"
    if "coursera" in n:
        return "🎓"
    if "spotify" in n:
        return "🎵"
    if "telegram" in n:
        return "✈️"
    return "🛍"


def compact_product_button(p, stock: int) -> str:
    """Product-first label: brand-like icon → name → USD price → stock."""
    name = str(p["name"]).strip()
    icon = product_icon(name)
    # Keep enough room for price and stock on mobile screens.
    max_name = 22
    shown = name if len(name) <= max_name else name[: max_name - 1].rstrip() + "…"
    return f"{icon} {shown} • ${money(p['price'])} • 📦{stock}"


def products_kb(page:int, uid=None):
    products=list_products(); pages=max(1,ceil(len(products)/PRODUCTS_PER_PAGE)); page=min(max(page,1),pages); start=(page-1)*PRODUCTS_PER_PAGE; chunk=products[start:start+PRODUCTS_PER_PAGE]; rows=[]
    for p in chunk:
        s=stock_count(p["id"]); rows.append([InlineKeyboardButton(text=compact_product_button(p,s),callback_data=f"product:{p['id']}:1",style="success" if s>0 else "danger")])
    if pages>1:
        nav=[]
        if page>1: nav.append(InlineKeyboardButton(text=tr(uid,"prev"),callback_data=f"products:{page-1}",style="primary"))
        if page<pages: nav.append(InlineKeyboardButton(text=tr(uid,"next"),callback_data=f"products:{page+1}",style="primary"))
        if nav: rows.append(nav)
    rows.append([InlineKeyboardButton(text=tr(uid,"refresh"),callback_data=f"products:{page}",style="success"),InlineKeyboardButton(text=f"📊 {page}/{pages}",callback_data="noop",style="primary"),InlineKeyboardButton(text=tr(uid,"back"),callback_data="menu:home",style="danger")])
    return InlineKeyboardMarkup(inline_keyboard=rows),page,pages


def product_kb(pid:int,qty:int,bot_username:str|None=None,uid=None):
    rows=[[InlineKeyboardButton(text="➖",callback_data=f"qty:-:{pid}:{qty}",style="danger"),InlineKeyboardButton(text=f"📦 {qty}",callback_data="noop",style="primary"),InlineKeyboardButton(text="➕",callback_data=f"qty:+:{pid}:{qty}",style="success")],[InlineKeyboardButton(text=tr(uid,"buy"),callback_data=f"buy:{pid}:{qty}",style="success")],[InlineKeyboardButton(text=tr(uid,"refresh"),callback_data=f"product:{pid}:{qty}",style="primary"),InlineKeyboardButton(text=tr(uid,"custom_qty"),callback_data=f"custom:{pid}:{qty}",style="primary")]]
    if bot_username:
        link=f"https://t.me/{bot_username}?start=product_{pid}"; rows.append([InlineKeyboardButton(text=tr(uid,"copy_link"),copy_text=CopyTextButton(text=link),style="primary"),InlineKeyboardButton(text=tr(uid,"view_note"),callback_data=f"note:{pid}:{qty}",style="primary")])
    else: rows.append([InlineKeyboardButton(text=tr(uid,"view_note"),callback_data=f"note:{pid}:{qty}",style="primary")])
    rows.append([InlineKeyboardButton(text=tr(uid,"back_products"),callback_data="menu:products",style="danger")]); return InlineKeyboardMarkup(inline_keyboard=rows)


def qty_calc_kb(pid:int,value:str,uid=None):
    def b(t,d,style="primary"): return InlineKeyboardButton(text=t,callback_data=d,style=style)
    return InlineKeyboardMarkup(inline_keyboard=[[b("1",f"qcalc:{pid}:1"),b("2",f"qcalc:{pid}:2"),b("3",f"qcalc:{pid}:3")],[b("4",f"qcalc:{pid}:4"),b("5",f"qcalc:{pid}:5"),b("6",f"qcalc:{pid}:6")],[b("7",f"qcalc:{pid}:7"),b("8",f"qcalc:{pid}:8"),b("9",f"qcalc:{pid}:9")],[b("⌫",f"qcalc:{pid}:back"),b("0",f"qcalc:{pid}:0"),b(tr(uid,"clear"),f"qcalc:{pid}:clear","danger")],[b("25",f"qcalc:{pid}:set25"),b("50",f"qcalc:{pid}:set50"),b("100",f"qcalc:{pid}:set100")],[b(tr(uid,"max"),f"qcalc:{pid}:max","success")],[b(tr(uid,"confirm"),f"qconfirm:{pid}","success"),b(tr(uid,"back"),f"product:{pid}:1","danger")]])


def settings_kb(uid=None):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=tr(uid,"my_orders"),callback_data="settings:orders",style="success"),InlineKeyboardButton(text=tr(uid,"email_btn"),callback_data="settings:email",style="primary")],[InlineKeyboardButton(text=tr(uid,"language_btn"),callback_data="settings:language",style="success"),InlineKeyboardButton(text=tr(uid,"set_region"),callback_data="settings:region",style="primary")],[InlineKeyboardButton(text=tr(uid,"back"),callback_data="menu:home",style="danger")]])


def language_kb(uid=None):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🇬🇧 English",callback_data="lang:en",style="primary"),InlineKeyboardButton(text="🇮🇳 हिन्दी",callback_data="lang:hi",style="primary")],[InlineKeyboardButton(text="🇵🇰 اردو",callback_data="lang:ur",style="primary"),InlineKeyboardButton(text="🇸🇦 العربية",callback_data="lang:ar",style="primary")],[InlineKeyboardButton(text="🇪🇸 Español",callback_data="lang:es",style="primary"),InlineKeyboardButton(text="🇮🇩 Indonesia",callback_data="lang:id",style="primary")],[InlineKeyboardButton(text=tr(uid,"back_settings"),callback_data="menu:settings",style="danger")]])


def region_kb(uid=None):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🇧🇩 Bangladesh",callback_data="region:Bangladesh",style="primary"),InlineKeyboardButton(text="🇮🇳 India",callback_data="region:India",style="primary")],[InlineKeyboardButton(text="🇵🇰 Pakistan",callback_data="region:Pakistan",style="primary"),InlineKeyboardButton(text="🇺🇸 USA",callback_data="region:USA",style="primary")],[InlineKeyboardButton(text="🇬🇧 UK",callback_data="region:UK",style="primary"),InlineKeyboardButton(text="🌍 Other",callback_data="region:Other",style="primary")],[InlineKeyboardButton(text=tr(uid,"back_settings"),callback_data="menu:settings",style="danger")]])


def email_kb(uid=None):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=tr(uid,"set_email"),callback_data="email:set",style="success"),InlineKeyboardButton(text=tr(uid,"change_email"),callback_data="email:change",style="primary")],[InlineKeyboardButton(text=tr(uid,"delete_email"),callback_data="email:delete",style="danger")],[InlineKeyboardButton(text=tr(uid,"back_settings"),callback_data="menu:settings",style="danger")]])


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Add Product", callback_data="admin:add_product", style="success"),
            InlineKeyboardButton(text="📥 Add Stock", callback_data="admin:add_stock", style="primary"),
        ],
        [
            InlineKeyboardButton(text="✏️ Edit Product", callback_data="admin:edit_product", style="primary"),
            InlineKeyboardButton(text="💲 Change Price", callback_data="admin:change_price", style="success"),
        ],
        [
            InlineKeyboardButton(text="🗑 Delete Product", callback_data="admin:delete_product", style="danger"),
            InlineKeyboardButton(text="🧹 Delete Stock", callback_data="admin:delete_stock", style="danger"),
        ],
        [
            InlineKeyboardButton(text="💵 Add Balance", callback_data="admin:add_balance", style="success"),
            InlineKeyboardButton(text="📦 Products", callback_data="admin:products", style="primary"),
        ],
        [
            InlineKeyboardButton(
                text=("🛠 Maintenance: ON" if maintenance_enabled() else "🛠 Maintenance: OFF"),
                callback_data="admin:maintenance",
                style=("danger" if maintenance_enabled() else "success"),
            ),
            InlineKeyboardButton(text="☁️ Cloud Backup", callback_data="admin:cloud_menu", style="primary"),
        ],
        [InlineKeyboardButton(text="◀ Back to Customer", callback_data="menu:home", style="danger")],
    ])



# =========================
# NOTIFICATIONS
# =========================

async def safe_send(bot: Bot, chat_id, text: str, reply_markup=None):
    if not chat_id:
        return False
    try:
        await bot.send_message(chat_id, text, reply_markup=reply_markup)
        return True
    except Exception:
        return False


async def check_stock_alert(bot: Bot, pid: int):
    if not ADMIN_ALERT_CHANNEL_ID:
        return
    p = get_product(pid)
    if not p:
        return
    current = stock_count(pid)
    threshold = next((x for x in sorted(LOW_STOCK_THRESHOLDS) if current <= x), None)
    if threshold is None:
        return
    with db() as con:
        exists = con.execute("SELECT 1 FROM stock_alerts WHERE product_id=? AND threshold=?", (pid, threshold)).fetchone()
        if exists:
            return
        con.execute("INSERT INTO stock_alerts(product_id, threshold) VALUES (?,?)", (pid, threshold))
        con.commit()
    if current == 0:
        text = f"🚨 <b>OUT OF STOCK</b>\n\n📦 {html.escape(p['name'])}\n📊 Remaining Stock: <b>0</b>\n\n📥 Please add new stock."
    else:
        text = f"⚠️ <b>LOW STOCK ALERT</b>\n\n📦 {html.escape(p['name'])}\n📊 Remaining Stock: <b>{current}</b>\n💰 Price: <b>{money(p['price'])} {INVOICE_CURRENCY}</b>"
    await safe_send(bot, ADMIN_ALERT_CHANNEL_ID, text)


async def broadcast_new_product(bot: Bot, pid: int):
    p = get_product(pid)
    if not p:
        return
    s = stock_count(pid)
    me = await bot.get_me()
    text = (
        "🆕 <b>NEW PRODUCT ADDED</b>\n\n"
        f"📦 Product: <b>{html.escape(p['name'])}</b>\n"
        f"💰 Price: <b>{money(p['price'])} {INVOICE_CURRENCY}</b>\n"
        f"📊 Stock: <b>{s}</b>\n"
        f"🛡 Warranty: <b>{html.escape(p['warranty'])}</b>"
    )
    user_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛒 Buy Now", callback_data=f"product:{pid}:1", style="danger")]])
    public_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛒 Buy Now", url=f"https://t.me/{me.username}?start=product_{pid}", style="success")]])

    if PUBLIC_CHANNEL_ID:
        await safe_send(bot, PUBLIC_CHANNEL_ID, text, public_kb)

    with db() as con:
        users = con.execute("SELECT telegram_id FROM users WHERE blocked=0").fetchall()
    for row in users:
        try:
            await bot.send_message(row["telegram_id"], text, reply_markup=user_kb)
            await asyncio.sleep(0.04)
        except Exception:
            with db() as con:
                con.execute("UPDATE users SET blocked=1 WHERE telegram_id=?", (row["telegram_id"],))
                con.commit()


async def broadcast_stock_added(bot: Bot, pid: int, added: int):
    """Notify registered customers and the public channel when stock is replenished."""
    if added <= 0:
        return
    p = get_product(pid)
    if not p:
        return
    total = stock_count(pid)
    me = await bot.get_me()
    text = (
        "📦 <b>STOCK UPDATED</b>\n\n"
        f"🛍 Product: <b>{html.escape(p['name'])}</b>\n"
        f"💰 Price: <b>{money(p['price'])} {INVOICE_CURRENCY}</b>\n"
        f"➕ New Stock: <b>{added}</b>\n"
        f"📊 Available Now: <b>{total}</b>\n\n"
        "⚡ Available now — order before stock runs out."
    )
    user_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(
            text="🛒 Buy Now", callback_data=f"product:{pid}:1", style="success"
        )]]
    )
    public_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(
            text="🛒 Buy Now", url=f"https://t.me/{me.username}?start=product_{pid}", style="success"
        )]]
    )

    if PUBLIC_CHANNEL_ID:
        await safe_send(bot, PUBLIC_CHANNEL_ID, text, public_kb)

    with db() as con:
        users = con.execute("SELECT telegram_id FROM users WHERE blocked=0").fetchall()
    for row in users:
        try:
            await bot.send_message(row["telegram_id"], text, reply_markup=user_kb)
            await asyncio.sleep(0.04)
        except Exception:
            with db() as con:
                con.execute("UPDATE users SET blocked=1 WHERE telegram_id=?", (row["telegram_id"],))
                con.commit()


async def public_purchase_notice(bot: Bot, order):
    if not PUBLIC_CHANNEL_ID:
        return
    text = (
        "✅ <b>NEW ORDER COMPLETED</b>\n\n"
        f"📦 Product: <b>{html.escape(order['product_name'])}</b>\n"
        f"🔢 Quantity: <b>{order['quantity']}</b>\n"
        f"💰 Total: <b>{money(order['total_amount'])} {INVOICE_CURRENCY}</b>\n"
        "⚡ Delivered automatically."
    )
    await safe_send(bot, PUBLIC_CHANNEL_ID, text)


# =========================
# ORDER DELIVERY
# =========================

async def complete_paid_order(bot: Bot, oid: str, method: str):
    order = get_order(oid)
    if not order or order["status"] in ("COMPLETED", "REFUNDED"):
        return
    items = take_stock(order["product_id"], int(order["quantity"]), oid)
    if not items:
        update_order(oid, payment_method=method, status="PAID_WAITING_STOCK", paid_at=datetime.now(timezone.utc).isoformat())
        await bot.send_message(order["telegram_id"], f"🕒 <b>Payment received</b>\n\nOrder <code>{oid}</code> is waiting for stock. Delivery will be automatic when stock is added.")
        return

    delivered = "\n".join(f"{i}. <code>{html.escape(x)}</code>" for i, x in enumerate(items, 1))
    update_order(
        oid,
        payment_method=method,
        status="COMPLETED",
        delivered_content=delivered,
        paid_at=datetime.now(timezone.utc).isoformat(),
    )
    await bot.send_message(
        order["telegram_id"],
        f"✅ <b>Order Delivered Successfully</b>\n\n🆔 <code>{oid}</code>\n📦 <b>{html.escape(order['product_name'])}</b>\n🔢 Qty: <b>{order['quantity']}</b>\n\n<b>Your Product(s):</b>\n{delivered}",
    )
    updated = get_order(oid)
    sent = await send_invoice_email(updated)
    if sent:
        await bot.send_message(order["telegram_id"], tr(order["telegram_id"],"invoice_sent"))
    await check_stock_alert(bot, order["product_id"])
    await public_purchase_notice(bot, updated)


async def process_waiting_orders(bot: Bot, pid: int):
    while True:
        with db() as con:
            row = con.execute(
                "SELECT * FROM orders WHERE product_id=? AND status='PAID_WAITING_STOCK' ORDER BY id LIMIT 1",
                (pid,),
            ).fetchone()
        if not row or stock_count(pid) < int(row["quantity"]):
            break
        await complete_paid_order(bot, row["order_id"], row["payment_method"] or "PAID")


# =========================
# CUSTOMER ROUTES
# =========================

@router.message(CommandStart())
async def start(message: Message):
    register_user(message.from_user)
    payload = ""
    if message.text and " " in message.text:
        payload = message.text.split(" ", 1)[1].strip()
    if payload.startswith("product_"):
        with suppress(Exception):
            pid = int(payload.split("_", 1)[1])
            p = get_product(pid)
            if p:
                me = await message.bot.get_me()
                s = stock_count(pid)
                total = Decimal(p["price"])
                u = get_user(message.from_user.id)
                await message.answer(
                    f"📦 <b>{html.escape(p['name'])}</b>\n\n💰 Price Base: <b>{money(p['price'])} {INVOICE_CURRENCY}</b>\n📊 Available Stock: <b>{s}</b>\n🛡 Warranty: <b>{html.escape(p['warranty'])}</b>\n🔢 Selected Qty: <b>1</b>\n🧮 Total: <b>{money(total)} {INVOICE_CURRENCY}</b>\n👛 Wallet: <b>{money(u['wallet'])} {INVOICE_CURRENCY}</b>",
                    reply_markup=product_kb(pid, 1, me.username, message.from_user.id),
                )
                return
    u = get_user(message.from_user.id)
    await message.answer(welcome_text(u), reply_markup=main_kb(message.from_user.id))



async def broadcast_price_update(bot: Bot, pid: int, old_price: Decimal, new_price: Decimal):
    p = get_product(pid)
    if not p:
        return
    s_count = stock_count(pid)
    if s_count <= 0:
        return

    me = await bot.get_me()
    if new_price < old_price:
        title = "📉 <b>PRICE DROPPED</b>"
        line = "🔥 Grab it before stock runs out!"
    elif new_price > old_price:
        title = "📈 <b>PRICE UPDATED</b>"
        line = "✨ New price is now active."
    else:
        title = "💲 <b>PRICE UPDATED</b>"
        line = "✨ Product price refreshed."

    text = (
        f"{title}\n\n"
        f"📦 Product: <b>{html.escape(p['name'])}</b>\n"
        f"💵 Old Price: <s>{money(old_price)} {INVOICE_CURRENCY}</s>\n"
        f"✨ New Price: <b>{money(new_price)} {INVOICE_CURRENCY}</b>\n"
        f"📦 Stock: <b>{s_count}</b>\n\n"
        f"{line}"
    )

    user_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🛒 Buy Now", callback_data=f"product:{pid}:1", style="success")
    ]])
    public_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🛒 Buy Now", url=f"https://t.me/{me.username}?start=product_{pid}", style="success")
    ]])

    if PUBLIC_CHANNEL_ID:
        await safe_send(bot, PUBLIC_CHANNEL_ID, text, public_kb)

    with db() as con:
        users = con.execute("SELECT telegram_id FROM users WHERE blocked=0").fetchall()
    for row in users:
        await safe_send(bot, row["telegram_id"], text, user_kb)

@router.callback_query(F.data == "menu:home")
async def home(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    register_user(callback.from_user)
    await edit(callback, welcome_text(get_user(callback.from_user.id)), main_kb(callback.from_user.id))


@router.callback_query(F.data.in_({"menu:products"}) | F.data.startswith("products:"))
async def products(callback: CallbackQuery):
    if await maintenance_guard(callback):
        return
    page = 1 if callback.data == "menu:products" else int(callback.data.split(":")[1])
    kb, page, pages = products_kb(page, callback.from_user.id)
    await edit(callback, f"{tr(callback.from_user.id,'available_products')}  •  <b>{page}/{pages}</b>\n{tr(callback.from_user.id,'select_product')}", kb)


@router.callback_query(F.data.startswith("product:"))
async def product_page(callback: CallbackQuery):
    if await maintenance_guard(callback):
        return
    _, p, q = callback.data.split(":")
    pid, qty = int(p), max(1, int(q))
    product = get_product(pid)
    if not product:
        return await callback.answer(tr(callback.from_user.id,"not_found"), show_alert=True)
    me = await callback.bot.get_me()
    s = stock_count(pid)
    total = Decimal(product["price"]) * qty
    u = get_user(callback.from_user.id)
    await edit(
        callback,
        f"💎 <b>{html.escape(product['name'])}</b>\n\n{tr(callback.from_user.id,'price')}: <b>{money(product['price'])} {INVOICE_CURRENCY}</b>\n{tr(callback.from_user.id,'stock')}: <b>{s}</b>\n{tr(callback.from_user.id,'warranty')}: <b>{html.escape(product['warranty'])}</b>\n{tr(callback.from_user.id,'selected')}: <b>{qty}</b>\n{tr(callback.from_user.id,'total')}: <b>{money(total)} {INVOICE_CURRENCY}</b>\n{tr(callback.from_user.id,'wallet')}: <b>{money(u['wallet'])} {INVOICE_CURRENCY}</b>",
        product_kb(pid, qty, me.username, callback.from_user.id),
    )


@router.callback_query(F.data.startswith("qty:"))
async def qty_change(callback: CallbackQuery):
    _, op, p, q = callback.data.split(":")
    pid = int(p)
    qty = int(q) + (1 if op == "+" else -1)
    qty = max(1, min(qty, max(1, stock_count(pid))))
    product = get_product(pid)
    if not product:
        return await callback.answer(tr(callback.from_user.id,"not_found"), show_alert=True)
    me = await callback.bot.get_me()
    total = Decimal(product["price"]) * qty
    u = get_user(callback.from_user.id)
    await edit(
        callback,
        f"💎 <b>{html.escape(product['name'])}</b>\n\n{tr(callback.from_user.id,'price')}: <b>{money(product['price'])} {INVOICE_CURRENCY}</b>\n{tr(callback.from_user.id,'stock')}: <b>{stock_count(pid)}</b>\n{tr(callback.from_user.id,'warranty')}: <b>{html.escape(product['warranty'])}</b>\n{tr(callback.from_user.id,'selected')}: <b>{qty}</b>\n{tr(callback.from_user.id,'total')}: <b>{money(total)} {INVOICE_CURRENCY}</b>\n{tr(callback.from_user.id,'wallet')}: <b>{money(u['wallet'])} {INVOICE_CURRENCY}</b>",
        product_kb(pid, qty, me.username, callback.from_user.id),
    )


@router.callback_query(F.data.startswith("note:"))
async def note(callback: CallbackQuery):
    _, p, q = callback.data.split(":")
    product = get_product(int(p))
    if not product:
        return await callback.answer(tr(callback.from_user.id,"not_found"), show_alert=True)
    await edit(
        callback,
        f"📜 <b>{html.escape(product['name'])} - {tr(callback.from_user.id,'note')}</b>\n\n{html.escape(product['note'] or tr(callback.from_user.id,'no_note'))}",
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=tr(callback.from_user.id,"back"), callback_data=f"product:{p}:{q}", style="danger")]]),
    )


@router.callback_query(F.data.startswith("custom:"))
async def custom_qty(callback: CallbackQuery, state: FSMContext):
    _, p, q = callback.data.split(":")
    await state.set_state(CustomQtyState.choosing)
    await state.update_data(pid=int(p), value=str(q))
    await edit(callback, f"{tr(callback.from_user.id,'custom_qty')}\n\n{tr(callback.from_user.id,'current')}: <b>{q}</b>", qty_calc_kb(int(p), q, callback.from_user.id))


@router.callback_query(F.data.startswith("qcalc:"))
async def qcalc(callback: CallbackQuery, state: FSMContext):
    _, p, key = callback.data.split(":")
    pid = int(p)
    data = await state.get_data()
    value = str(data.get("value", ""))
    if key.isdigit() and len(key) == 1:
        value = (value + key).lstrip("0")[:6] or "0"
    elif key == "back":
        value = value[:-1] or "0"
    elif key == "clear":
        value = "0"
    elif key.startswith("set"):
        value = key[3:]
    elif key == "max":
        value = str(max(1, stock_count(pid)))
    await state.update_data(value=value)
    await edit(callback, f"{tr(callback.from_user.id,'custom_qty')}\n\n{tr(callback.from_user.id,'current')}: <b>{value}</b>", qty_calc_kb(pid, value, callback.from_user.id))


@router.callback_query(F.data.startswith("qconfirm:"))
async def qconfirm(callback: CallbackQuery, state: FSMContext):
    pid = int(callback.data.split(":")[1])
    data = await state.get_data()
    try:
        qty = int(data.get("value", "1"))
    except Exception:
        qty = 1
    qty = min(max(qty, 1), 100000)
    await state.clear()
    product = get_product(pid)
    if not product:
        return await callback.answer(tr(callback.from_user.id,"not_found"), show_alert=True)
    qty = min(qty, max(1, stock_count(pid)))
    me = await callback.bot.get_me()
    total = Decimal(product["price"]) * qty
    u = get_user(callback.from_user.id)
    await edit(
        callback,
        f"💎 <b>{html.escape(product['name'])}</b>\n\n{tr(callback.from_user.id,'price')}: <b>{money(product['price'])} {INVOICE_CURRENCY}</b>\n{tr(callback.from_user.id,'stock')}: <b>{stock_count(pid)}</b>\n{tr(callback.from_user.id,'warranty')}: <b>{html.escape(product['warranty'])}</b>\n{tr(callback.from_user.id,'selected')}: <b>{qty}</b>\n{tr(callback.from_user.id,'total')}: <b>{money(total)} {INVOICE_CURRENCY}</b>\n{tr(callback.from_user.id,'wallet')}: <b>{money(u['wallet'])} {INVOICE_CURRENCY}</b>",
        product_kb(pid, qty, me.username, callback.from_user.id),
    )


@router.callback_query(F.data.startswith("buy:"))
async def buy(callback: CallbackQuery):
    if await maintenance_guard(callback):
        return
    _, p, q = callback.data.split(":")
    pid, qty = int(p), int(q)
    product = get_product(pid)
    user = get_user(callback.from_user.id)
    if not product or not user:
        return await callback.answer("Unable to continue", show_alert=True)
    if not user["email"]:
        return await edit(callback, tr(callback.from_user.id,"email_required"), InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=tr(callback.from_user.id,"set_email"), callback_data="email:set", style="danger")],
            [InlineKeyboardButton(text=tr(callback.from_user.id,"back"), callback_data=f"product:{pid}:{qty}", style="danger")],
        ]))
    if stock_count(pid) < qty:
        return await callback.answer(tr(callback.from_user.id,"not_enough"), show_alert=True)
    oid = create_order(callback.from_user.id, product, qty)
    total = Decimal(product["price"]) * qty
    await edit(callback,
        f"{tr(callback.from_user.id,'select_payment')}\n\n🆔 <code>{oid}</code>\n📦 {html.escape(product['name'])}\n{tr(callback.from_user.id,'quantity')}: <b>{qty}</b>\n{tr(callback.from_user.id,'total')}: <b>{money(total)} {INVOICE_CURRENCY}</b>\n{tr(callback.from_user.id,'wallet')}: <b>{money(user['wallet'])} {INVOICE_CURRENCY}</b>",
        InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=tr(callback.from_user.id,"direct"), callback_data=f"pay:direct:{oid}", style="success")],
            [InlineKeyboardButton(text=tr(callback.from_user.id,"wallet_pay"), callback_data=f"pay:wallet:{oid}", style="primary")],
            [InlineKeyboardButton(text=tr(callback.from_user.id,"back"), callback_data=f"product:{pid}:{qty}", style="danger")],
        ]),
    )


@router.callback_query(F.data.startswith("pay:wallet:"))
async def pay_wallet(callback: CallbackQuery):
    oid = callback.data.split(":", 2)[2]
    order = get_order(oid)
    if not order or order["telegram_id"] != callback.from_user.id or order["status"] != "PENDING_PAYMENT":
        return await callback.answer(tr(callback.from_user.id,"order_unavailable"), show_alert=True)
    total = Decimal(order["total_amount"])
    user = get_user(callback.from_user.id)
    if Decimal(user["wallet"]) < total:
        return await callback.answer(tr(callback.from_user.id,"insufficient"), show_alert=True)
    try:
        balance = change_wallet(callback.from_user.id, -total, "PURCHASE", oid)
    except ValueError:
        return await callback.answer(tr(callback.from_user.id,"insufficient"), show_alert=True)
    update_order(oid, payment_method="WALLET", status="PAID")
    await edit(callback, f"{tr(callback.from_user.id,'wallet_success')}\n\n{tr(callback.from_user.id,'paid')}: <b>{money(total)} {INVOICE_CURRENCY}</b>\n{tr(callback.from_user.id,'balance',amount=money(balance),currency=INVOICE_CURRENCY)}", back_home(callback.from_user.id))
    await complete_paid_order(callback.bot, oid, "WALLET")


def payment_methods_kb(amount: Decimal, kind: str, ref: str, uid: int | None = None) -> InlineKeyboardMarkup:
    rows = []
    for c_key, c_info in SUPPORTED_CHAINS.items():
        rows.append([
            InlineKeyboardButton(
                text=f"{c_info['badge']} • {money(amount)} {INVOICE_CURRENCY}",
                callback_data=f"pickchain:{c_key}:{kind}:{ref}",
                style="danger",
            )
        ])
    back_cb = f"pay:direct:{ref}" if kind == "order" else "menu:topup"
    rows.append([InlineKeyboardButton(text=tr(uid, "back"), callback_data=back_cb, style="danger")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def render_invoice_ui(inv, uid: int | None = None) -> tuple[str, InlineKeyboardMarkup]:
    chain_key = inv["chain"] or "bsc"
    c_info = SUPPORTED_CHAINS.get(chain_key, {"name": chain_key, "badge": chain_key})
    amount_str = money(inv["invoice_amount"])
    inv_id = str(inv["invoice_id"])
    addr = str(inv["deposit_address"] or "-")
    kind = str(inv["payment_kind"])
    ref = str(inv["reference_id"])

    text = (
        f"{tr(uid, 'payment_invoice')}\n\n"
        f"🆔 <b>Invoice ID:</b> <code>{html.escape(inv_id)}</code>\n"
        f"💵 <b>Amount to Pay:</b> <code>{amount_str} {INVOICE_CURRENCY}</code>\n"
        f"🌐 <b>Network:</b> <b>{html.escape(c_info['name'])}</b>\n"
        f"📍 <b>Deposit Address (Tap to copy):</b>\n"
        f"<code>{html.escape(addr)}</code>\n\n"
        f"📋 <b>Instructions:</b>\n"
        f"1. Send exactly <b>{amount_str} {INVOICE_CURRENCY}</b> on <b>{html.escape(c_info['name'])}</b> to the address above.\n"
        f"2. After payment is sent from Trust Wallet / Binance / MetaMask, tap <b>{tr(uid, 'submit_tx')}</b> below.\n"
        f"3. Paste your <b>Transaction Hash (TxID)</b> to verify & activate automatically."
    )

    rows = [
        [InlineKeyboardButton(text=tr(uid, "submit_tx"), callback_data=f"pay:submittx:{inv_id}", style="success")],
        [InlineKeyboardButton(text=tr(uid, "cancel_invoice"), callback_data=f"cancel:{inv_id}", style="danger")],
    ]
    back_cb = f"pay:direct:{ref}" if kind == "order" else "menu:topup"
    rows.append([InlineKeyboardButton(text=tr(uid, "back"), callback_data=back_cb, style="primary")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def payment_methods(callback: CallbackQuery, state: FSMContext, amount: Decimal, kind: str, ref: str):
    await state.update_data(payment_amount=str(amount), payment_kind=kind, payment_ref=ref)
    kb = payment_methods_kb(amount, kind, ref, callback.from_user.id)
    await edit(
        callback,
        f"{tr(callback.from_user.id, 'select_payment')}\n\n"
        f"💰 {tr(callback.from_user.id, 'amount')}: <b>{money(amount)} {INVOICE_CURRENCY}</b>\n\n"
        f"{tr(callback.from_user.id, 'auto_webhook')}",
        kb,
    )


@router.callback_query(F.data.startswith("pay:direct:"))
async def pay_direct(callback: CallbackQuery, state: FSMContext):
    oid = callback.data.split(":", 2)[2]
    order = get_order(oid)
    if not order or order["telegram_id"] != callback.from_user.id:
        return await callback.answer(tr(callback.from_user.id, "not_found"), show_alert=True)
    await payment_methods(callback, state, Decimal(order["total_amount"]), "order", oid)


@router.callback_query(F.data == "menu:topup")
async def topup(callback: CallbackQuery, state: FSMContext):
    if await maintenance_guard(callback):
        return
    await state.set_state(TopupState.amount)
    await edit(callback, tr(callback.from_user.id, "topup_title") + "\n\n" + tr(callback.from_user.id, "topup_prompt", currency=INVOICE_CURRENCY), back_home(callback.from_user.id))


@router.message(TopupState.amount)
async def topup_amount(message: Message, state: FSMContext, bot: Bot):
    try:
        amount = Decimal((message.text or "").strip())
        if amount <= 0:
            raise InvalidOperation
    except Exception:
        return await message.answer(tr(message.from_user.id, "positive"))
    await message.delete()
    ref = f"DEP-{uuid4().hex[:10].upper()}"
    await state.update_data(payment_amount=str(amount), payment_kind="deposit", payment_ref=ref)
    kb = payment_methods_kb(amount, "deposit", ref, message.from_user.id)
    await bot.send_message(
        message.chat.id,
        f"{tr(message.from_user.id, 'select_payment')}\n\n"
        f"💰 {tr(message.from_user.id, 'amount')}: <b>{money(amount)} {INVOICE_CURRENCY}</b>\n\n"
        f"{tr(message.from_user.id, 'auto_webhook')}",
        reply_markup=kb,
    )


@router.callback_query(F.data.startswith("pickchain:"))
async def pickchain(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    chain = parts[1]
    kind = parts[2]
    ref = parts[3]
    data = await state.get_data()
    amount_raw = data.get("payment_amount")
    if not amount_raw:
        if kind == "order":
            order = get_order(ref)
            amount_raw = order["total_amount"] if order else None
    if not amount_raw:
        return await callback.answer(tr(callback.from_user.id, "session_expired"), show_alert=True)
    amount = Decimal(str(amount_raw))

    try:
        deposit_addr = await get_gateway_deposit_address(chain)
    except Exception:
        logging.exception("Could not get deposit address")
        deposit_addr = ""

    if not deposit_addr:
        return await callback.answer(f"⚠️ Deposit address for {chain.upper()} is not set. Please add DEPOSIT_WALLET_{chain.upper()} in Render Environment Variables.", show_alert=True)

    invoice_id = f"INV-{uuid4().hex[:10].upper()}"
    save_crypto_invoice(invoice_id, callback.from_user.id, kind, ref, amount, INVOICE_CURRENCY, chain, deposit_addr)
    inv = get_saved_invoice(invoice_id)
    text, kb = render_invoice_ui(inv, callback.from_user.id)
    await state.clear()
    await edit(callback, text, kb)


@router.callback_query(F.data.startswith("show_invoice:"))
async def show_invoice_handler(callback: CallbackQuery, state: FSMContext):
    inv_id = callback.data.split(":", 1)[1]
    inv = get_saved_invoice(inv_id)
    if not inv or inv["telegram_id"] != callback.from_user.id:
        return await callback.answer(tr(callback.from_user.id, "not_found"), show_alert=True)
    await state.clear()
    text, kb = render_invoice_ui(inv, callback.from_user.id)
    await edit(callback, text, kb)


@router.callback_query(F.data.startswith("pay:submittx:"))
async def pay_submittx(callback: CallbackQuery, state: FSMContext):
    inv_id = callback.data.split(":", 2)[2]
    inv = get_saved_invoice(inv_id)
    if not inv or inv["telegram_id"] != callback.from_user.id:
        return await callback.answer(tr(callback.from_user.id, "not_found"), show_alert=True)
    if inv["status"] == "PAID":
        return await callback.answer("✅ This invoice is already paid.", show_alert=True)
    if inv["status"] == "CANCELLED":
        return await callback.answer("❌ This invoice was cancelled.", show_alert=True)

    await state.set_state(PaymentVerifyState.waiting_tx)
    await state.update_data(invoice_id=inv_id, menu_msg_id=(callback.message.message_id if callback.message else None))
    chain_key = inv["chain"] or "bsc"
    c_info = SUPPORTED_CHAINS.get(chain_key, {"name": chain_key, "badge": chain_key})

    prompt_text = (
        f"📝 <b>Submit Transaction Hash (TxID)</b>\n\n"
        f"🆔 <b>Invoice:</b> <code>{html.escape(inv_id)}</code>\n"
        f"💵 <b>Expected:</b> <code>{money(inv['invoice_amount'])} {INVOICE_CURRENCY}</code>\n"
        f"🌐 <b>Network:</b> <b>{html.escape(c_info['name'])}</b>\n\n"
        "Please copy the <b>Transaction Hash</b> (e.g. <code>0x...</code>) from your wallet and send it here:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀ Back to Invoice", callback_data=f"show_invoice:{inv_id}", style="danger")]
    ])
    await edit(callback, prompt_text, kb)


async def execute_tx_verification(bot: Bot, uid: int, inv_id: str, tx_hash: str, chat_id: int, reply_to_msg_id: int | None = None):
    clean_tx = tx_hash.strip().lower()
    inv = get_saved_invoice(inv_id)
    if not inv or inv["telegram_id"] != uid:
        await bot.send_message(chat_id, "❌ Invoice not found or unauthorized.", reply_to_message_id=reply_to_msg_id)
        return

    if inv["status"] == "PAID":
        await bot.send_message(chat_id, "✅ This invoice is already marked as PAID.", reply_to_message_id=reply_to_msg_id)
        return

    if not clean_tx.startswith("0x") or len(clean_tx) < 40:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Try Again", callback_data=f"pay:submittx:{inv_id}", style="primary")],
            [InlineKeyboardButton(text="◀ Back to Invoice", callback_data=f"show_invoice:{inv_id}", style="danger")],
        ])
        await bot.send_message(
            chat_id,
            "❌ <b>Invalid Transaction Hash format.</b>\n\nTransaction hash must start with <code>0x</code> and be a valid hex string.",
            reply_markup=kb,
            reply_to_message_id=reply_to_msg_id,
        )
        return

    if is_tx_processed(clean_tx):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀ Back to Invoice", callback_data=f"show_invoice:{inv_id}", style="danger")]
        ])
        await bot.send_message(
            chat_id,
            "❌ <b>Duplicate Transaction Hash</b>\n\nThis transaction hash has already been claimed / processed.",
            reply_markup=kb,
            reply_to_message_id=reply_to_msg_id,
        )
        return

    chain_key = inv["chain"] or "bsc"
    c_info = SUPPORTED_CHAINS.get(chain_key, {"name": chain_key, "badge": chain_key})
    wait_msg = await bot.send_message(
        chat_id,
        f"🔍 <b>Verifying transaction on {c_info['name']}...</b>\n\nPlease wait a few seconds. ⏳",
        reply_to_message_id=reply_to_msg_id,
    )

    try:
        res = await crypto_verify_payment(
            tx_hash=clean_tx,
            chain=chain_key,
            expected_amount=str(inv["invoice_amount"]),
            notify=True,
        )
    except Exception as exc:
        logging.exception("Verification request failed")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Retry Verification", callback_data=f"do_verify:{inv_id}:{clean_tx}", style="primary")],
            [InlineKeyboardButton(text="◀ Back to Invoice", callback_data=f"show_invoice:{inv_id}", style="danger")],
        ])
        err_str = html.escape(str(exc))
        with suppress(Exception):
            await bot.delete_message(chat_id, wait_msg.message_id)
        await bot.send_message(
            chat_id,
            f"❌ <b>Verification Failed</b>\n\n{err_str}\n\nPlease check that you sent the exact amount on <b>{c_info['name']}</b> and try again.",
            reply_markup=kb,
        )
        return

    with suppress(Exception):
        await bot.delete_message(chat_id, wait_msg.message_id)

    confirmed = res.get("confirmed") is True or str(res.get("status", "")).lower() == "success"
    confirmations = res.get("confirmations", 0)
    req_confirmations = res.get("requiredConfirmations", 1)

    if confirmed:
        record_processed_tx(clean_tx, inv_id, chain_key, inv["invoice_amount"])
        with db() as con:
            con.execute(
                "UPDATE payment_invoices SET status='PAID', tx_hash=?, paid_at=CURRENT_TIMESTAMP WHERE invoice_id=?",
                (clean_tx, inv_id),
            )
            con.commit()

        if inv["payment_kind"] == "deposit":
            try:
                balance = change_wallet(uid, Decimal(inv["invoice_amount"]), "DEPOSIT", inv_id, clean_tx)
            except Exception:
                logging.exception("Deposit balance update failed")
                return await bot.send_message(chat_id, "⚠️ Payment confirmed, but wallet balance credit failed. Please contact support.")
            success_text = (
                f"✅ <b>Deposit Successful!</b>\n\n"
                f"💰 Amount Credited: <b>{money(inv['invoice_amount'])} {INVOICE_CURRENCY}</b>\n"
                f"👛 New Balance: <b>{money(balance)} {INVOICE_CURRENCY}</b>\n"
                f"🌐 Network: <b>{html.escape(c_info['name'])}</b>\n"
                f"🔗 TxHash: <code>{clean_tx}</code>"
            )
            await bot.send_message(chat_id, success_text, reply_markup=back_home(uid))
        elif inv["payment_kind"] == "order":
            oid = inv["reference_id"]
            order = get_order(oid)
            if order and order["status"] in ("PENDING", "PENDING_PAYMENT"):
                update_order(oid, payment_method=f"CRYPTO_{chain_key.upper()}", status="PAID")
                await complete_paid_order(bot, oid, f"CRYPTO_{chain_key.upper()}")
            success_text = (
                f"✅ <b>Payment Confirmed!</b>\n\n"
                f"🆔 Order ID: <code>{html.escape(oid)}</code>\n"
                f"💰 Amount Paid: <b>{money(inv['invoice_amount'])} {INVOICE_CURRENCY}</b>\n"
                f"🌐 Network: <b>{html.escape(c_info['name'])}</b>\n"
                f"🔗 TxHash: <code>{clean_tx}</code>\n\n"
                "📦 Your order has been processed and delivered above! ✨"
            )
            await bot.send_message(chat_id, success_text, reply_markup=back_home(uid))
    else:
        # Pending confirmations or unconfirmed
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Verify Again", callback_data=f"do_verify:{inv_id}:{clean_tx}", style="success")],
            [InlineKeyboardButton(text="◀ Back to Invoice", callback_data=f"show_invoice:{inv_id}", style="danger")],
        ])
        await bot.send_message(
            chat_id,
            f"⏳ <b>Transaction Detected (Pending Confirmations)</b>\n\n"
            f"Confirmations: <b>{confirmations} / {req_confirmations}</b>\n"
            f"Network: <b>{html.escape(c_info['name'])}</b>\n"
            f"TxHash: <code>{clean_tx}</code>\n\n"
            "Please wait a moment for block confirmations to complete on-chain, then tap <b>🔄 Verify Again</b>.",
            reply_markup=kb,
        )


@router.message(PaymentVerifyState.waiting_tx)
async def tx_received(message: Message, state: FSMContext, bot: Bot):
    raw_tx = (message.text or "").strip()
    data = await state.get_data()
    inv_id = data.get("invoice_id")
    await state.clear()
    if not inv_id:
        return await message.answer("❌ Session expired. Please open the invoice again.")
    await execute_tx_verification(bot, message.from_user.id, inv_id, raw_tx, message.chat.id, message.message_id)


@router.callback_query(F.data.startswith("do_verify:"))
async def do_verify_callback(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split(":", 2)
    inv_id = parts[1]
    tx_hash = parts[2]
    await callback.answer("Checking on-chain confirmations...", show_alert=False)
    await execute_tx_verification(bot, callback.from_user.id, inv_id, tx_hash, callback.message.chat.id)


@router.callback_query(F.data.startswith("cancel:"))
async def cancel_invoice(callback: CallbackQuery):
    invoice_id = callback.data.split(":", 1)[1]
    with db() as con:
        con.execute("UPDATE payment_invoices SET status='CANCELLED' WHERE invoice_id=?", (invoice_id,))
        con.commit()
    await edit(callback, tr(callback.from_user.id, "invoice_cancelled"), back_home(callback.from_user.id))


def profile_text(user,tg_user)->str:
    uid=tg_user.id; username=f"@{html.escape(tg_user.username)}" if tg_user.username else tr(uid,"not_set"); email=html.escape(user["email"]) if user["email"] else tr(uid,"not_set"); region=html.escape(user["region"]) if user["region"] else tr(uid,"region_missing")
    names={"en":"English 🇬🇧","hi":"Hindi 🇮🇳","ur":"Urdu 🇵🇰","ar":"Arabic 🇸🇦","es":"Spanish 🇪🇸","id":"Indonesian 🇮🇩"}; language=html.escape(names.get((user["language"] or "en").lower(),"EN")); joined=user["created_at"] or "-"
    try: joined=datetime.fromisoformat(str(joined).replace("Z","+00:00")).strftime("%d %b %Y")
    except Exception: pass
    suffix=tr(uid,"saved") if user["email"] else ""
    return tr(uid,"profile")+"\n\n"+f"🆔 ID: <code>{uid}</code>\n{tr(uid,'first_name')}: <b>{html.escape(tg_user.first_name or user['full_name'])}</b>\n{tr(uid,'username')}: {username}\n{tr(uid,'status')}: <b>{tr(uid,'started')}</b>\n{tr(uid,'email')}: <b>{email}</b>{suffix}\n💰 Balance: <b>{Decimal(str(user['wallet'])):.3f} {INVOICE_CURRENCY}</b>\n{tr(uid,'currency')}: <b>{INVOICE_CURRENCY}</b>\n{tr(uid,'language')}: <b>{language}</b>\n{tr(uid,'region')}: {region}\n{tr(uid,'joined')}: <b>{html.escape(str(joined))}</b>"


@router.callback_query(F.data == "menu:settings")
async def settings(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    register_user(callback.from_user)
    user = get_user(callback.from_user.id)
    await edit(callback, profile_text(user, callback.from_user), settings_kb(callback.from_user.id))


@router.callback_query(F.data == "settings:region")
async def region_settings(callback: CallbackQuery):
    await edit(callback, tr(callback.from_user.id,"choose_region"), region_kb(callback.from_user.id))


@router.callback_query(F.data.startswith("region:"))
async def region_selected(callback: CallbackQuery):
    region = callback.data.split(":", 1)[1]
    set_region(callback.from_user.id, region)
    user = get_user(callback.from_user.id)
    await edit(callback, profile_text(user, callback.from_user), settings_kb(callback.from_user.id))


@router.callback_query(F.data == "settings:orders")
async def my_orders(callback: CallbackQuery):
    rows = user_orders(callback.from_user.id)
    if not rows:
        text = tr(callback.from_user.id,"my_orders")+"\n\n"+tr(callback.from_user.id,"no_orders")
    else:
        parts=[tr(callback.from_user.id,"my_orders")]
        for o in rows:
            parts.append(f"\n🆔 <code>{o['order_id']}</code>\n📦 {html.escape(o['product_name'])}\n🔢 {o['quantity']}\n💰 {money(o['total_amount'])} {INVOICE_CURRENCY}\n{tr(callback.from_user.id,'order_status')}: <b>{html.escape(o['status'])}</b>")
        text = "\n".join(parts)
    await edit(callback, text, settings_kb(callback.from_user.id))


@router.callback_query(F.data == "settings:language")
async def language(callback: CallbackQuery):
    await edit(
        callback,
        tr(callback.from_user.id,"select_language"),
        language_kb(callback.from_user.id),
    )


@router.callback_query(F.data.startswith("lang:"))
async def language_selected(callback: CallbackQuery):
    code = callback.data.split(":", 1)[1].lower()
    allowed = {"en", "hi", "ur", "ar", "es", "id"}
    if code not in allowed:
        await callback.answer(tr(callback.from_user.id,"unsupported"), show_alert=True)
        return
    set_language(callback.from_user.id, code)
    user = get_user(callback.from_user.id)
    await edit(callback, profile_text(user, callback.from_user), settings_kb(callback.from_user.id))


@router.callback_query(F.data == "settings:email")
async def email_settings(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    u = get_user(callback.from_user.id)
    current = html.escape(u["email"]) if u["email"] else tr(callback.from_user.id,"not_set")
    await edit(callback, f"{tr(callback.from_user.id,'email_settings')}\n\n{tr(callback.from_user.id,'current_email')}: <b>{current}</b>", email_kb(callback.from_user.id))


@router.callback_query(F.data.in_({"email:set", "email:change"}))
async def email_input(callback: CallbackQuery, state: FSMContext):
    await state.set_state(EmailState.waiting)
    await state.update_data(menu_message_id=callback.message.message_id)
    await edit(callback, tr(callback.from_user.id,"send_email"), back_home())


@router.message(EmailState.waiting)
async def email_received(message: Message, state: FSMContext, bot: Bot):
    email = (message.text or "").strip().lower()
    if not EMAIL_RE.fullmatch(email):
        return await message.answer(tr(message.from_user.id,"invalid_email"))
    data = await state.get_data()
    set_email(message.from_user.id, email)
    await state.clear()
    with suppress(Exception): await message.delete()
    await bot.edit_message_text(chat_id=message.chat.id, message_id=int(data["menu_message_id"]), text=f"{tr(message.from_user.id,'email_saved')}\n\n{html.escape(email)}", reply_markup=email_kb(message.from_user.id))


@router.callback_query(F.data == "email:delete")
async def email_delete(callback: CallbackQuery):
    set_email(callback.from_user.id, None)
    await edit(callback, tr(callback.from_user.id,"email_deleted"), email_kb(callback.from_user.id))


@router.callback_query(F.data == "menu:channel")
async def channel_not_set(callback: CallbackQuery):
    await callback.answer(tr(callback.from_user.id,"channel_missing"), show_alert=True)


@router.callback_query(F.data == "menu:support")
async def support(callback: CallbackQuery):
    username = SUPPORT_USERNAME.lstrip("@")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=tr(callback.from_user.id,"support"), url=f"https://t.me/{username}", style="primary")],
        [InlineKeyboardButton(text=tr(callback.from_user.id,"back"), callback_data="menu:home", style="danger")],
    ])
    await edit(callback, tr(callback.from_user.id,"support_text",username=html.escape(username)), kb)


# =========================
# ADMIN ROUTES
# =========================

@router.message(Command("admin"))
async def admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("🛠 <b>Admin Panel</b>", reply_markup=admin_kb())


@router.callback_query(F.data == "admin:home")
async def admin_home(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await edit(callback, "🛠 <b>Admin Panel</b>", admin_kb())


@router.callback_query(F.data == "admin:maintenance")
async def admin_maintenance(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    enabled = maintenance_enabled()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=("🔴 Disable Maintenance" if enabled else "🟢 Enable Maintenance"),
                callback_data=("admin:maintenance_off" if enabled else "admin:maintenance_on"),
                style=("success" if enabled else "danger"),
            )
        ],
        [InlineKeyboardButton(text="◀ Back", callback_data="admin:home", style="primary")],
    ])
    await edit(
        callback,
        "🛠 <b>Maintenance Mode</b>\n\n"
        f"Current Status: <b>{'ON 🔴' if enabled else 'OFF 🟢'}</b>\n\n"
        "When enabled:\n"
        "• Customers receive a maintenance notification\n"
        "• Shop/product purchase is temporarily blocked\n"
        "• Wallet top-up is temporarily blocked\n"
        "• Admin can continue using the bot\n"
        "• Status stays saved after bot restart",
        kb,
    )


@router.callback_query(F.data == "admin:maintenance_on")
async def admin_maintenance_on(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    if maintenance_enabled():
        return await callback.answer("Maintenance is already ON.", show_alert=True)
    set_app_setting("maintenance_mode", "1")
    await callback.answer("Maintenance mode enabled.", show_alert=True)
    await edit(
        callback,
        "🔴 <b>Maintenance Mode Enabled</b>\n\n"
        "Customers are being notified. Shop, purchases and top-ups are temporarily blocked.",
        admin_kb(),
    )
    await broadcast_maintenance(callback.bot, True)


@router.callback_query(F.data == "admin:maintenance_off")
async def admin_maintenance_off(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    if not maintenance_enabled():
        return await callback.answer("Maintenance is already OFF.", show_alert=True)
    set_app_setting("maintenance_mode", "0")
    await callback.answer("Maintenance mode disabled.", show_alert=True)
    await edit(
        callback,
        "🟢 <b>Maintenance Mode Disabled</b>\n\n"
        "Premium Hubs is available normally again.",
        admin_kb(),
    )
    await broadcast_maintenance(callback.bot, False)


@router.message(Command("backup"))
@router.callback_query(F.data == "admin:cloud_menu")
async def admin_cloud_menu(event: Message | CallbackQuery):
    uid = event.from_user.id
    if not is_admin(uid):
        return
    is_connected = bool(firebase_admin._apps)
    status_text = "🟢 Connected (Active)" if is_connected else "🔴 Disconnected (Check .env)"
    
    with db() as con:
        user_c = con.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        prod_c = con.execute("SELECT COUNT(*) c FROM products WHERE active=1").fetchone()["c"]
        order_c = con.execute("SELECT COUNT(*) c FROM orders").fetchone()["c"]
        stock_c = con.execute("SELECT COUNT(*) c FROM stock_items WHERE status='AVAILABLE'").fetchone()["c"]
    
    text = (
        "☁️ <b>Firebase Cloud Database Center</b>\n\n"
        f"🌐 <b>Cloud Status:</b> <b>{status_text}</b>\n"
        f"🔗 <b>Project:</b> <code>premium-hub-4e23d</code>\n\n"
        "📊 <b>Local Memory Stats:</b>\n"
        f"• 👥 Users: <b>{user_c}</b>\n"
        f"• 🛍 Active Products: <b>{prod_c}</b>\n"
        f"• 📦 Available Stock: <b>{stock_c}</b>\n"
        f"• 📑 Orders: <b>{order_c}</b>\n\n"
        "⚡ <b>Protection Features:</b>\n"
        "• Render রিস্টার্ট নিলেও সব ইউজার ও প্রোডাক্ট সুরক্ষিত থাকে।\n"
        "• প্রতি ৫ মিনিট অন্তর অটোমেটিক ব্যাকআপ ক্লাউডে জমা হয়।\n"
        "• যেকোনো সময় নিচে বাটন চেপে ম্যানুয়াল ব্যাকআপ বা সিঙ্ক করতে পারবেন।"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="☁️ Backup to Cloud Now", callback_data="admin:cloud_backup_now", style="success"),
            InlineKeyboardButton(text="📥 Sync from Cloud Now", callback_data="admin:cloud_sync_now", style="primary"),
        ],
        [InlineKeyboardButton(text="◀ Back", callback_data="admin:home", style="danger")],
    ])
    
    if isinstance(event, CallbackQuery):
        await edit(event, text, kb)
    else:
        await event.answer(text, reply_markup=kb)


@router.callback_query(F.data == "admin:cloud_backup_now")
async def admin_cloud_backup_now(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.answer("⏳ Backing up to Firebase Cloud...", show_alert=False)
    res = push_sqlite_to_firebase()
    if res.get("ok"):
        c = res.get("counts", {})
        msg = (
            "✅ <b>Cloud Backup Successful!</b>\n\n"
            f"👥 Users backed up: <b>{c.get('users', 0)}</b>\n"
            f"🛍 Products backed up: <b>{c.get('products', 0)}</b>\n"
            f"📦 Stock items backed up: <b>{c.get('stock', 0)}</b>\n"
            f"📑 Orders backed up: <b>{c.get('orders', 0)}</b>\n"
            f"⚙️ Settings backed up: <b>{c.get('settings', 0)}</b>\n\n"
            "🛡️ All data is safely stored in Firebase Realtime Database."
        )
    else:
        msg = f"❌ <b>Backup Failed:</b> {html.escape(str(res.get('error')))}"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀ Back to Cloud Menu", callback_data="admin:cloud_menu", style="primary")],
    ])
    await edit(callback, msg, kb)


@router.message(Command("sync"))
@router.callback_query(F.data == "admin:cloud_sync_now")
async def admin_cloud_sync_now(event: Message | CallbackQuery):
    uid = event.from_user.id
    if not is_admin(uid):
        return
    if isinstance(event, CallbackQuery):
        await event.answer("⏳ Restoring data from Firebase Cloud...", show_alert=False)
    sync_firebase_to_sqlite()
    
    with db() as con:
        user_c = con.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        prod_c = con.execute("SELECT COUNT(*) c FROM products WHERE active=1").fetchone()["c"]
        stock_c = con.execute("SELECT COUNT(*) c FROM stock_items WHERE status='AVAILABLE'").fetchone()["c"]
        order_c = con.execute("SELECT COUNT(*) c FROM orders").fetchone()["c"]
    
    msg = (
        "✅ <b>Cloud Sync & Restore Complete!</b>\n\n"
        f"👥 Restored Users: <b>{user_c}</b>\n"
        f"🛍 Restored Products: <b>{prod_c}</b>\n"
        f"📦 Restored Stock: <b>{stock_c}</b>\n"
        f"📑 Restored Orders: <b>{order_c}</b>\n\n"
        "✨ Local database is 100% updated from Firebase Cloud."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀ Back to Cloud Menu", callback_data="admin:cloud_menu", style="primary")],
    ])
    if isinstance(event, CallbackQuery):
        await edit(event, msg, kb)
    else:
        await event.answer(msg, reply_markup=kb)


@router.callback_query(F.data == "admin:add_product")
async def admin_add_product(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.set_state(AdminProductState.name)
    await callback.message.edit_text("Send product name:")
    await callback.answer()


@router.message(AdminProductState.name)
async def ap_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.update_data(name=(message.text or "").strip())
    await state.set_state(AdminProductState.price)
    await message.answer(f"Send price in {INVOICE_CURRENCY}:")


@router.message(AdminProductState.price)
async def ap_price(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    try:
        price = Decimal((message.text or "").strip())
        if price <= 0: raise InvalidOperation
    except Exception:
        return await message.answer("Invalid price. Send again.")
    await state.update_data(price=str(price))
    await state.set_state(AdminProductState.warranty)
    await message.answer("Send warranty text (example: 30 DAYS):")


@router.message(AdminProductState.warranty)
async def ap_warranty(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.update_data(warranty=(message.text or "").strip())
    await state.set_state(AdminProductState.note)
    await message.answer("Send View Note / product note:")


@router.message(AdminProductState.note)
async def ap_note(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.update_data(note=(message.text or "").strip())
    await state.set_state(AdminProductState.stock)
    await message.answer("Send initial stock items, one per line. Send SKIP for no stock:")


@router.message(AdminProductState.stock)
async def ap_stock(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    data = await state.get_data()
    pid = create_product(data["name"], Decimal(data["price"]), data["warranty"], data["note"])
    raw = (message.text or "").strip()
    added = 0 if raw.upper() == "SKIP" else add_stock(pid, raw.splitlines())
    await state.clear()
    await message.answer(f"✅ Product added.\nID: <code>{pid}</code>\nInitial stock: <b>{added}</b>", reply_markup=admin_kb())
    await broadcast_new_product(message.bot, pid)
    await safe_send(message.bot, ADMIN_ALERT_CHANNEL_ID, f"✅ <b>NEW PRODUCT CREATED</b>\n\n📦 {html.escape(data['name'])}\n📊 Stock: <b>{added}</b>")


@router.callback_query(F.data == "admin:add_stock")
async def admin_add_stock(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.set_state(AdminStockState.product_id)
    await callback.message.edit_text("Send Product ID:")
    await callback.answer()


@router.message(AdminStockState.product_id)
async def as_pid(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    try: pid = int((message.text or "").strip())
    except: return await message.answer("Invalid Product ID")
    if not get_product(pid): return await message.answer("Product not found")
    await state.update_data(pid=pid)
    await state.set_state(AdminStockState.items)
    await message.answer("Send stock items, one per line:")


@router.message(AdminStockState.items)
async def as_items(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    data = await state.get_data(); pid = int(data["pid"])
    added = add_stock(pid, (message.text or "").splitlines())
    total = stock_count(pid)
    p = get_product(pid)
    await state.clear()
    await message.answer(f"✅ Added <b>{added}</b> stock item(s).\nTotal stock: <b>{total}</b>", reply_markup=admin_kb())
    await safe_send(message.bot, ADMIN_ALERT_CHANNEL_ID, f"✅ <b>STOCK ADDED</b>\n\n📦 {html.escape(p['name'])}\n➕ Added: <b>{added}</b>\n📊 Total Stock: <b>{total}</b>")
    await broadcast_stock_added(message.bot, pid, added)
    await process_waiting_orders(message.bot, pid)


@router.callback_query(F.data == "admin:change_price")
async def admin_change_price(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    available_products = [p for p in list_products() if stock_count(p["id"]) > 0]
    if not available_products:
        await callback.answer("No in-stock products available.", show_alert=True)
        return
    lines = ["💲 <b>Change Product Price</b>", "", "In-stock products:"]
    for p in available_products[:30]:
        lines.append(f"ID <code>{p['id']}</code> • {html.escape(p['name'])} • {money(p['price'])} {INVOICE_CURRENCY} • 📦{stock_count(p['id'])}")
    lines.append("\nSend Product ID:")
    await state.set_state(AdminPriceState.product_id)
    await callback.message.edit_text("\n".join(lines))
    await callback.answer()


@router.message(AdminPriceState.product_id)
async def change_price_product_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    if not raw.isdigit():
        return await message.answer("❌ Send a valid Product ID.")
    pid = int(raw)
    p = get_product(pid)
    if not p:
        return await message.answer("❌ Product not found.")
    current_stock = stock_count(pid)
    if current_stock <= 0:
        return await message.answer("❌ This product has no stock. Price can only be changed for in-stock products.")
    await state.update_data(price_pid=pid, old_price=str(p["price"]))
    await state.set_state(AdminPriceState.new_price)
    await message.answer(
        f"📦 <b>{html.escape(p['name'])}</b>\n"
        f"💵 Current Price: <b>{money(p['price'])} {INVOICE_CURRENCY}</b>\n"
        f"📦 Stock: <b>{current_stock}</b>\n\n"
        f"Send the new price:"
    )


@router.message(AdminPriceState.new_price)
async def change_price_new_price(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        new_price = Decimal((message.text or "").strip())
        if new_price <= 0:
            raise InvalidOperation
    except Exception:
        return await message.answer("❌ Send a valid positive price.")

    data = await state.get_data()
    pid = int(data["price_pid"])
    p = get_product(pid)
    if not p:
        await state.clear()
        return await message.answer("❌ Product not found.", reply_markup=admin_kb())
    if stock_count(pid) <= 0:
        await state.clear()
        return await message.answer("❌ Stock is now 0, so price was not changed.", reply_markup=admin_kb())

    old_price = Decimal(str(p["price"]))
    update_product_price(pid, new_price)
    await state.clear()

    direction = "📉 Decreased" if new_price < old_price else "📈 Increased" if new_price > old_price else "🔄 Updated"
    await message.answer(
        f"✅ <b>Price Changed</b>\n\n"
        f"📦 {html.escape(p['name'])}\n"
        f"💵 Old: <b>{money(old_price)} {INVOICE_CURRENCY}</b>\n"
        f"✨ New: <b>{money(new_price)} {INVOICE_CURRENCY}</b>\n"
        f"{direction}\n"
        f"📦 Stock: <b>{stock_count(pid)}</b>",
        reply_markup=admin_kb(),
    )
    if new_price != old_price:
        await broadcast_price_update(message.bot, pid, old_price, new_price)


@router.callback_query(F.data == "admin:delete_product")
async def admin_delete_product(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    products = list_products()
    if not products:
        return await callback.answer("No products found.", show_alert=True)
    lines = ["🗑 <b>Delete Product</b>", "", "Send the Product ID you want to delete:"]
    for p in products[:30]:
        lines.append(f"<code>{p['id']}</code> • {html.escape(p['name'])} • 📦{stock_count(p['id'])}")
    await state.set_state(AdminDeleteProductState.product_id)
    await callback.message.edit_text("\n".join(lines))
    await callback.answer()


@router.message(AdminDeleteProductState.product_id)
async def admin_delete_product_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    if not raw.isdigit():
        return await message.answer("❌ Send a valid Product ID.")
    pid = int(raw)
    p = get_product(pid)
    if not p:
        return await message.answer("❌ Product not found.")
    await state.update_data(delete_pid=pid)
    await state.set_state(AdminDeleteProductState.confirm)
    await message.answer(
        "⚠️ <b>Confirm Product Delete</b>\n\n"
        f"📦 {html.escape(p['name'])}\n"
        f"💵 Price: {money(p['price'])} {INVOICE_CURRENCY}\n"
        f"📊 Available Stock: {stock_count(pid)}\n\n"
        "This will hide the product and remove its unsold stock. Existing order history will stay saved.\n"
        "Customers will NOT receive a notification.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Yes, Delete", callback_data="admin:delete_product_confirm", style="danger")],
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin:cancel", style="primary")],
        ]),
    )


@router.callback_query(F.data == "admin:delete_product_confirm")
async def admin_delete_product_confirm(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    data = await state.get_data()
    pid = int(data.get("delete_pid", 0))
    p = get_product(pid)
    if not p:
        await state.clear()
        return await edit(callback, "❌ Product not found or already deleted.", admin_kb())
    removed_stock = soft_delete_product(pid)
    await state.clear()
    await edit(
        callback,
        f"✅ <b>Product Deleted</b>\n\n📦 {html.escape(p['name'])}\n🧹 Unsold stock removed: <b>{removed_stock}</b>\n\n🔕 No customer notification was sent.",
        admin_kb(),
    )


@router.callback_query(F.data == "admin:delete_stock")
async def admin_delete_stock(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()

    products = [p for p in list_products() if stock_count(p["id"]) > 0]
    if not products:
        return await callback.answer("No available stock to delete.", show_alert=True)

    lines = [
        "🧹 <b>Delete Added Stock</b>",
        "",
        "Select a product to view the exact stock items you added:",
        "",
    ]
    rows = []
    for serial, p in enumerate(products[:40], start=1):
        available = stock_count(p["id"])
        lines.append(f"<b>{serial}.</b> {html.escape(p['name'])} — Stock: <b>{available}</b>")
        rows.append([
            InlineKeyboardButton(
                text=f"{serial}. 📦 {p['name']} • Stock: {available}",
                callback_data=f"admin:delete_stock_pick:{p['id']}",
                style="primary",
            )
        ])

    rows.append([
        InlineKeyboardButton(
            text="◀ Back",
            callback_data="admin:home",
            style="danger",
        )
    ])

    await edit(
        callback,
        "\n".join(lines),
        InlineKeyboardMarkup(inline_keyboard=rows),
    )


def _stock_preview(value: str, max_len: int = 44) -> str:
    value = " ".join(str(value).replace("\r", " ").replace("\n", " ").split())
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"


async def show_exact_stock_items(callback: CallbackQuery, pid: int, page: int = 1):
    p = get_product(pid)
    if not p:
        return await callback.answer("Product not found.", show_alert=True)

    items = list_available_stock_items(pid)
    if not items:
        return await callback.answer("This product has no available stock.", show_alert=True)

    per_page = 10
    pages = max(1, ceil(len(items) / per_page))
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    chunk = items[start:start + per_page]

    lines = [
        f"🧹 <b>Delete Stock — {html.escape(p['name'])}</b>",
        "",
        f"📊 Available Stock: <b>{len(items)}</b>",
        "",
        "Tap the exact stock item you want to delete:",
        "",
    ]

    rows = []
    for absolute_index, item in enumerate(chunk, start=start + 1):
        content = str(item["content"])
        lines.append(
            f"<b>{absolute_index}.</b> <code>{html.escape(content)}</code>"
        )
        rows.append([
            InlineKeyboardButton(
                text=f"🗑 {absolute_index}. {_stock_preview(content)}",
                callback_data=f"admin:delete_exact:{pid}:{item['id']}:{page}",
                style="danger",
            )
        ])

    if pages > 1:
        nav = []
        if page > 1:
            nav.append(
                InlineKeyboardButton(
                    text="⬅ Prev",
                    callback_data=f"admin:delete_stock_page:{pid}:{page-1}",
                    style="primary",
                )
            )
        nav.append(
            InlineKeyboardButton(
                text=f"📊 {page}/{pages}",
                callback_data="noop",
                style="primary",
            )
        )
        if page < pages:
            nav.append(
                InlineKeyboardButton(
                    text="Next ➡",
                    callback_data=f"admin:delete_stock_page:{pid}:{page+1}",
                    style="primary",
                )
            )
        rows.append(nav)

    rows.append([
        InlineKeyboardButton(
            text="🗑 Delete ALL Stock",
            callback_data=f"admin:delete_exact_all_confirm:{pid}",
            style="danger",
        )
    ])
    rows.append([
        InlineKeyboardButton(
            text="◀ Back to Products",
            callback_data="admin:delete_stock",
            style="primary",
        )
    ])

    await edit(
        callback,
        "\n".join(lines),
        InlineKeyboardMarkup(inline_keyboard=rows),
    )


@router.callback_query(F.data.startswith("admin:delete_stock_pick:"))
async def admin_delete_stock_pick(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    pid = int(callback.data.rsplit(":", 1)[1])
    await show_exact_stock_items(callback, pid, 1)


@router.callback_query(F.data.startswith("admin:delete_stock_page:"))
async def admin_delete_stock_page(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    _, _, _, pid, page = callback.data.split(":")
    await show_exact_stock_items(callback, int(pid), int(page))


@router.callback_query(F.data.startswith("admin:delete_exact:"))
async def admin_delete_exact(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    _, _, pid_raw, item_id_raw, page_raw = callback.data.split(":")
    pid = int(pid_raw)
    item_id = int(item_id_raw)
    page = int(page_raw)

    p = get_product(pid)
    if not p:
        return await callback.answer("Product not found.", show_alert=True)

    item = None
    for row in list_available_stock_items(pid):
        if int(row["id"]) == item_id:
            item = row
            break

    if not item:
        return await callback.answer("This stock item was already removed.", show_alert=True)

    content = str(item["content"])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="✅ Delete This Stock",
                callback_data=f"admin:delete_exact_confirm:{pid}:{item_id}:{page}",
                style="danger",
            )
        ],
        [
            InlineKeyboardButton(
                text="◀ Cancel",
                callback_data=f"admin:delete_stock_page:{pid}:{page}",
                style="primary",
            )
        ],
    ])

    await edit(
        callback,
        "⚠️ <b>Delete This Exact Stock?</b>\n\n"
        f"📦 Product: <b>{html.escape(p['name'])}</b>\n"
        f"🆔 Stock Item ID: <code>{item_id}</code>\n\n"
        f"<code>{html.escape(content)}</code>\n\n"
        "🔕 Customers will NOT receive a notification.",
        kb,
    )


@router.callback_query(F.data.startswith("admin:delete_exact_confirm:"))
async def admin_delete_exact_confirm(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    _, _, _, pid_raw, item_id_raw, page_raw = callback.data.split(":")
    pid = int(pid_raw)
    item_id = int(item_id_raw)
    page = int(page_raw)

    deleted = delete_stock_item_by_id(pid, item_id)
    if not deleted:
        return await callback.answer("This stock item was already removed.", show_alert=True)

    remaining = stock_count(pid)
    await callback.answer("✅ Stock item deleted.", show_alert=False)

    if remaining <= 0:
        return await edit(
            callback,
            "✅ <b>Stock Item Deleted</b>\n\n"
            "📦 Remaining Stock: <b>0</b>\n"
            "🔕 No customer notification was sent.",
            InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="◀ Back to Products",
                        callback_data="admin:delete_stock",
                        style="primary",
                    )
                ]
            ]),
        )

    items = list_available_stock_items(pid)
    pages = max(1, ceil(len(items) / 10))
    page = min(page, pages)
    await show_exact_stock_items(callback, pid, page)


@router.callback_query(F.data.startswith("admin:delete_exact_all_confirm:"))
async def admin_delete_exact_all_confirm(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    pid = int(callback.data.rsplit(":", 1)[1])
    p = get_product(pid)
    if not p:
        return await callback.answer("Product not found.", show_alert=True)

    items = list_available_stock_items(pid)
    if not items:
        return await callback.answer("No available stock to delete.", show_alert=True)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=f"✅ Delete ALL ({len(items)})",
                callback_data=f"admin:delete_exact_all_do:{pid}",
                style="danger",
            )
        ],
        [
            InlineKeyboardButton(
                text="◀ Cancel",
                callback_data=f"admin:delete_stock_pick:{pid}",
                style="primary",
            )
        ],
    ])

    await edit(
        callback,
        "⚠️ <b>Delete ALL Available Stock?</b>\n\n"
        f"📦 Product: <b>{html.escape(p['name'])}</b>\n"
        f"🧹 Stock items to delete: <b>{len(items)}</b>\n\n"
        "This removes only unsold AVAILABLE stock.\n"
        "Existing order history and sold items are not touched.\n\n"
        "🔕 Customers will NOT receive a notification.",
        kb,
    )


@router.callback_query(F.data.startswith("admin:delete_exact_all_do:"))
async def admin_delete_exact_all_do(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    pid = int(callback.data.rsplit(":", 1)[1])
    p = get_product(pid)
    if not p:
        return await callback.answer("Product not found.", show_alert=True)

    deleted = delete_available_stock(pid, None)

    await edit(
        callback,
        "✅ <b>All Available Stock Deleted</b>\n\n"
        f"📦 Product: <b>{html.escape(p['name'])}</b>\n"
        f"🧹 Deleted: <b>{deleted}</b>\n"
        "📊 Remaining: <b>0</b>\n\n"
        "🔕 No customer notification was sent.",
        InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="◀ Back to Products",
                    callback_data="admin:delete_stock",
                    style="primary",
                )
            ]
        ]),
    )


@router.callback_query(F.data == "admin:edit_product")
async def admin_edit_product(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    products = list_products()
    if not products:
        return await callback.answer("No products found.", show_alert=True)
    lines = ["✏️ <b>Edit Product Information</b>", "", "Send Product ID:"]
    for p in products[:30]:
        lines.append(f"<code>{p['id']}</code> • {html.escape(p['name'])}")
    await state.set_state(AdminEditProductState.product_id)
    await callback.message.edit_text("\n".join(lines))
    await callback.answer()


@router.message(AdminEditProductState.product_id)
async def admin_edit_product_pid(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    if not raw.isdigit():
        return await message.answer("❌ Send a valid Product ID.")
    pid = int(raw)
    p = get_product(pid)
    if not p:
        return await message.answer("❌ Product not found.")
    await state.update_data(edit_pid=pid)
    await state.set_state(AdminEditProductState.field)
    await message.answer(
        f"✏️ <b>Edit Product</b>\n\n"
        f"📦 Name: <b>{html.escape(p['name'])}</b>\n"
        f"💵 Price: <b>{money(p['price'])} {INVOICE_CURRENCY}</b>\n"
        f"🛡 Warranty: <b>{html.escape(p['warranty'])}</b>\n"
        f"📝 Note/Details: {html.escape(p['note'] or 'Not set')}\n\n"
        "Choose what you want to edit:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📦 Name", callback_data="admin:edit_field:name", style="primary"),
                InlineKeyboardButton(text="💵 Price", callback_data="admin:edit_field:price", style="success"),
            ],
            [
                InlineKeyboardButton(text="🛡 Warranty", callback_data="admin:edit_field:warranty", style="primary"),
                InlineKeyboardButton(text="📝 Details / Note", callback_data="admin:edit_field:note", style="success"),
            ],
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin:cancel", style="danger")],
        ]),
    )


@router.callback_query(F.data.startswith("admin:edit_field:"))
async def admin_edit_field(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    field = callback.data.rsplit(":", 1)[1]
    if field not in {"name", "price", "warranty", "note"}:
        return await callback.answer("Invalid field.", show_alert=True)
    data = await state.get_data()
    pid = int(data.get("edit_pid", 0))
    if not get_product(pid):
        await state.clear()
        return await edit(callback, "❌ Product not found.", admin_kb())
    await state.update_data(edit_field=field)
    await state.set_state(AdminEditProductState.value)
    labels = {
        "name": "Send the new product name:",
        "price": f"Send the new price in {INVOICE_CURRENCY}:",
        "warranty": "Send the new warranty text:",
        "note": "Send the new product details / View Note text:",
    }
    await callback.message.edit_text(labels[field])
    await callback.answer()


@router.message(AdminEditProductState.value)
async def admin_edit_product_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    pid = int(data.get("edit_pid", 0))
    field = data.get("edit_field")
    p = get_product(pid)
    if not p or field not in {"name", "price", "warranty", "note"}:
        await state.clear()
        return await message.answer("❌ Edit session expired.", reply_markup=admin_kb())

    raw = (message.text or "").strip()
    if not raw:
        return await message.answer("❌ Value cannot be empty.")

    if field == "price":
        try:
            new_price = Decimal(raw)
            if new_price <= 0:
                raise InvalidOperation
        except Exception:
            return await message.answer("❌ Send a valid positive price.")
        old_price = Decimal(str(p["price"]))
        update_product_price(pid, new_price)
        await state.clear()
        await message.answer(
            f"✅ <b>Product Price Updated</b>\n\n📦 {html.escape(p['name'])}\n💵 Old: {money(old_price)} {INVOICE_CURRENCY}\n✨ New: {money(new_price)} {INVOICE_CURRENCY}",
            reply_markup=admin_kb(),
        )
        # Keep the earlier requirement: price changes are announced to users + public channel.
        if new_price != old_price and stock_count(pid) > 0:
            await broadcast_price_update(message.bot, pid, old_price, new_price)
        return

    old_value = str(p[field] or "")
    update_product_field(pid, field, raw)
    await state.clear()
    label = {"name": "Name", "warranty": "Warranty", "note": "Details / Note"}[field]
    await message.answer(
        f"✅ <b>{label} Updated</b>\n\n"
        f"📦 Product ID: <code>{pid}</code>\n"
        f"Old: {html.escape(old_value or 'Not set')}\n"
        f"New: <b>{html.escape(raw)}</b>\n\n"
        "🔕 No customer notification was sent for this information edit.",
        reply_markup=admin_kb(),
    )


@router.callback_query(F.data == "admin:add_balance")
async def admin_add_balance(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.set_state(AdminBalanceState.user)
    await callback.message.edit_text("Send user's Telegram ID or @username:")
    await callback.answer()


@router.message(AdminBalanceState.user)
async def ab_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    raw = (message.text or "").strip()
    with db() as con:
        if raw.startswith("@"):
            u = con.execute("SELECT * FROM users WHERE lower(username)=lower(?)", (raw[1:],)).fetchone()
        elif raw.isdigit():
            u = con.execute("SELECT * FROM users WHERE telegram_id=?", (int(raw),)).fetchone()
        else:
            u = None
    if not u: return await message.answer("User not found. The user must start the bot first.")
    await state.update_data(target_uid=u["telegram_id"], target_name=u["full_name"])
    await state.set_state(AdminBalanceState.amount)
    await message.answer(f"User: <b>{html.escape(u['full_name'])}</b>\nCurrent wallet: <b>{money(u['wallet'])}</b>\n\nSend amount to add:")


@router.message(AdminBalanceState.amount)
async def ab_amount(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    try:
        amount = Decimal((message.text or "").strip())
        if amount <= 0: raise InvalidOperation
    except Exception:
        return await message.answer("Send a valid positive amount.")
    data = await state.get_data()
    await state.update_data(amount=str(amount))
    await state.set_state(AdminBalanceState.confirm)
    await message.answer(
        f"Confirm balance add?\n\n👤 {html.escape(data['target_name'])}\n💰 +{money(amount)} {INVOICE_CURRENCY}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Confirm", callback_data="admin:balance_confirm", style="danger")],
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin:cancel", style="danger")],
        ]),
    )


@router.callback_query(F.data == "admin:balance_confirm")
async def ab_confirm(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    data = await state.get_data()
    uid = int(data["target_uid"]); amount = Decimal(data["amount"])
    balance = change_wallet(uid, amount, "ADMIN_ADD", f"ADMIN:{callback.from_user.id}", "Manual balance add")
    await state.clear()
    await edit(callback, f"✅ Balance added.\n\nAmount: <b>{money(amount)} {INVOICE_CURRENCY}</b>\nNew Balance: <b>{money(balance)} {INVOICE_CURRENCY}</b>", admin_kb())
    await safe_send(callback.bot, uid, f"💰 <b>Wallet Balance Added</b>\n\nAdmin added <b>{money(amount)} {INVOICE_CURRENCY}</b> to your wallet.\nNew Balance: <b>{money(balance)} {INVOICE_CURRENCY}</b>")


@router.callback_query(F.data == "admin:cancel")
async def admin_cancel(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id): return
    await state.clear(); await edit(callback, "❎ Cancelled.", admin_kb())


@router.callback_query(F.data == "admin:products")
async def admin_products(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    lines = ["📦 <b>Products</b>"]
    for p in list_products():
        note_preview = (p["note"] or "").replace("\n", " ").strip()
        if len(note_preview) > 70:
            note_preview = note_preview[:67] + "..."
        lines.append(
            f"\nID: <code>{p['id']}</code>\n"
            f"📦 {html.escape(p['name'])}\n"
            f"💵 Price: {money(p['price'])} {INVOICE_CURRENCY}\n"
            f"📊 Stock: {stock_count(p['id'])}\n"
            f"🛡 Warranty: {html.escape(p['warranty'])}\n"
            f"📝 Details: {html.escape(note_preview or 'Not set')}"
        )
    await edit(callback, "\n".join(lines), admin_kb())


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


# =========================
# WEBHOOK + PDF HTTP SERVER
# =========================

BOT_INSTANCE: Bot | None = None

async def ipn_handler(request: web.Request):
    return web.json_response({"ok": True, "message": "CryptoPay Gateway operates via on-chain verification endpoint /api/public/payments/verify"})


async def pdf_handler(request: web.Request):
    token = request.match_info["token"]
    with db() as con:
        order = con.execute("SELECT * FROM orders WHERE invoice_pdf_token=? AND status='COMPLETED'", (token,)).fetchone()
    if not order:
        raise web.HTTPNotFound(text="Invoice not found")
    user = get_user(order["telegram_id"])
    if not user:
        raise web.HTTPNotFound(text="User not found")
    pdf = invoice_pdf_bytes(order, user)
    headers = {"Content-Disposition": f'attachment; filename="invoice-{order["order_id"]}.pdf"'}
    return web.Response(body=pdf, content_type="application/pdf", headers=headers)


async def health_handler(request: web.Request):
    return web.json_response({"ok": True, "bot": BOT_NAME, "gateway": "CryptoPay"})


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_post("/ipn", ipn_handler)
    app.router.add_get("/invoice/{token}", pdf_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_HOST, WEB_PORT)
    await site.start()
    logging.info("HTTP server listening on %s:%s", WEB_HOST, WEB_PORT)
    return runner


# =========================
# MAIN
# =========================


async def setup_shop_commands(bot: Bot):
    """Force-register Telegram commands on every startup."""
    customer_commands = [
        BotCommand(command="start", description="Open Premium Hubs"),
    ]
    admin_commands = [
        BotCommand(command="start", description="Open Premium Hubs"),
        BotCommand(command="admin", description="Open Admin Panel"),
    ]

    # Clear common old scopes first so stale commands do not remain.
    scopes_to_clear = [BotCommandScopeDefault()]
    if ADMIN_ID:
        scopes_to_clear.append(BotCommandScopeChat(chat_id=int(ADMIN_ID)))

    for scope in scopes_to_clear:
        try:
            await bot.delete_my_commands(scope=scope)
        except Exception as e:
            logging.warning("Could not clear Telegram commands for %s: %s", scope, e)

    # Register customer/default menu.
    await bot.set_my_commands(
        customer_commands,
        scope=BotCommandScopeDefault(),
    )

    # Register admin-only menu.
    if ADMIN_ID:
        await bot.set_my_commands(
            admin_commands,
            scope=BotCommandScopeChat(chat_id=int(ADMIN_ID)),
        )

    try:
        me = await bot.get_me()
        logging.info(
            "Telegram commands registered successfully for @%s (admin_id=%s)",
            me.username,
            ADMIN_ID,
        )
    except Exception as e:
        logging.warning("Command registration verification failed: %s", e)



async def main():
    global BOT_INSTANCE
    init_db()
    init_firebase()
    sync_firebase_to_sqlite()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await setup_shop_commands(bot)
    BOT_INSTANCE = bot
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    runner = await start_web_server()
    sync_task = asyncio.create_task(auto_cloud_sync_loop())
    logging.info("%s is running with Firebase Cloud Persistence active...", BOT_NAME)
    try:
        await dp.start_polling(bot)
    finally:
        sync_task.cancel()
        with suppress(Exception):
            await sync_task
        push_sqlite_to_firebase()
        await runner.cleanup()
        await bot.session.close()
        logging.info("Bot stopped gracefully. Cloud snapshot saved.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped.")

