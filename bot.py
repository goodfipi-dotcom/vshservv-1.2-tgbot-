import os
import json
import time
import re
import threading
import logging
from datetime import datetime, timedelta
from urllib.parse import quote_plus, unquote_plus

import requests
import telebot
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)

# ─────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────
TOKEN         = os.getenv("TG_BOT_TOKEN")
OWNER_CHAT_ID = int(os.getenv("OWNER_CHAT_ID", "0"))

if not TOKEN:
    raise ValueError("❌ TG_BOT_TOKEN не установлен! Добавьте в переменные окружения.")
if not OWNER_CHAT_ID:
    raise ValueError("❌ OWNER_CHAT_ID не установлен! Добавьте в переменные окружения.")
MINI_APP_URL  = os.getenv("MINI_APP_URL", "https://mini-appsvsh.vercel.app")
API_BASE      = os.getenv("API_BASE",     "https://mini-appsvsh.vercel.app")
CLIENT_URL    = os.getenv("CLIENT_URL",   "https://mini-appsvsh.vercel.app/client.html")
WORKER_PIN    = os.getenv("WORKER_PIN",   "2026")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

bot = telebot.TeleBot(TOKEN, threaded=True, skip_pending=True)

# ─────────────────────────────────────────
# ФАЙЛЫ ХРАНЕНИЯ
# ─────────────────────────────────────────
RATINGS_FILE = "ratings.json"
WORKERS_FILE = "workers.json"
USERS_FILE   = "users.json"

def load_json(path, default):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.exception("Ошибка чтения %s: %s", path, e)
    return default

def save_json(path, data):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        logger.exception("Ошибка записи %s: %s", path, e)

ratings  = load_json(RATINGS_FILE, {})
users    = load_json(USERS_FILE, {})
_wlist   = load_json(WORKERS_FILE, [])
workers_stream_active = set(int(x) for x in _wlist if str(x).isdigit())

# ─────────────────────────────────────────
# СТЕЙТЫ
# ─────────────────────────────────────────
pending_admin_replies    = {}
pending_admin_direct     = {}
pending_admin_broadcast  = set()
user_roles               = {}
banned_users             = set()

request_lock = threading.Lock()

# ─────────────────────────────────────────
# АНТИСПАМ
# Лимит: 5 сообщений за 10 секунд → мут
# Мут нарастает: 1 час → 2 часа → 3 часа...
# Админ (OWNER) не подвержен антиспаму
# ─────────────────────────────────────────
SPAM_WINDOW     = 10      # секунд — окно отслеживания
SPAM_LIMIT      = 5       # макс. сообщений в окне
BASE_MUTE_SECS  = 3600    # 1 час — базовый мут

spam_tracker = {}  # chat_id -> {"timestamps": [...], "mute_until": float, "mute_count": int}

def check_spam(chat_id):
    """Проверяет спам. Возвращает True если пользователь замучен."""
    if chat_id == OWNER_CHAT_ID:
        return False  # Админ не блокируется

    now = time.time()

    if chat_id not in spam_tracker:
        spam_tracker[chat_id] = {"timestamps": [], "mute_until": 0, "mute_count": 0}

    rec = spam_tracker[chat_id]

    # Если пользователь в муте — проверяем истёк ли
    if rec["mute_until"] > now:
        remaining = int((rec["mute_until"] - now) / 60)
        return True  # Всё ещё замучен

    # Фильтруем старые timestamps (оставляем только за последние SPAM_WINDOW секунд)
    rec["timestamps"] = [t for t in rec["timestamps"] if now - t < SPAM_WINDOW]
    rec["timestamps"].append(now)

    # Проверяем лимит
    if len(rec["timestamps"]) > SPAM_LIMIT:
        rec["mute_count"] += 1
        mute_secs = BASE_MUTE_SECS * rec["mute_count"]  # 1ч, 2ч, 3ч и т.д.
        rec["mute_until"] = now + mute_secs
        hours = mute_secs // 3600

        try:
            bot.send_message(
                chat_id,
                f"⛔ <b>Антиспам:</b> вы отправляете слишком много сообщений.\n"
                f"Бот заблокирован для вас на <b>{hours} ч.</b>\n\n"
                f"При повторном нарушении время блокировки увеличится.",
                parse_mode="HTML"
            )
        except:
            pass

        logger.warning("Антиспам: замучен %s на %d сек (нарушение #%d)", chat_id, mute_secs, rec["mute_count"])
        return True

    return False

# ─────────────────────────────────────────
# УТИЛИТЫ
# ─────────────────────────────────────────
def user_key(chat_id, username):
    return f"@{username}" if username else str(chat_id)

def is_banned(uid):
    try:
        return int(uid) in banned_users
    except:
        return str(uid) in banned_users

def save_all():
    save_json(RATINGS_FILE, ratings)
    save_json(WORKERS_FILE, list(workers_stream_active))
    save_json(USERS_FILE, users)

def register_user(user):
    if not user:
        return None
    uid = str(user.id)
    if is_banned(uid):
        return None
    k = user_key(user.id, user.username)
    users[uid] = k
    save_json(USERS_FILE, users)
    if k not in ratings:
        ratings[k] = {"score": 0}
        save_json(RATINGS_FILE, ratings)
    return k

def api_post(endpoint, data):
    try:
        r = requests.post(f"{API_BASE}{endpoint}", json=data, timeout=10)
        return r.json()
    except Exception as e:
        logger.exception("API error %s: %s", endpoint, e)
        return {"error": str(e)}

# ─────────────────────────────────────────
# КЛАВИАТУРЫ
# ─────────────────────────────────────────
def role_select_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(KeyboardButton("👷 Я исполнитель"), KeyboardButton("📦 Я заказчик"))
    return kb

def worker_main_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(KeyboardButton("🚀 Открыть приложение"), KeyboardButton("🏆 Мой рейтинг"))
    kb.add(KeyboardButton("ℹ️ О сервисе"), KeyboardButton("🆘 Техподдержка"))
    kb.add(KeyboardButton("↩️ Сменить роль"))
    return kb

def customer_main_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(KeyboardButton("🌐 Оставить заявку на сайте"))
    kb.add(KeyboardButton("ℹ️ О сервисе"), KeyboardButton("🆘 Техподдержка"))
    kb.add(KeyboardButton("↩️ Сменить роль"))
    return kb

def owner_menu_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(KeyboardButton("🚀 Открыть приложение"), KeyboardButton("📊 Рейтинг рабочих"))
    kb.add(KeyboardButton("👥 Участники"),           KeyboardButton("📣 Рассылка всем"))
    kb.add(KeyboardButton("📨 Написать рабочему"),   KeyboardButton("ℹ️ О сервисе"))
    return kb

def open_app_inline_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(
        "🚀 Открыть VSH Service",
        web_app=telebot.types.WebAppInfo(url=MINI_APP_URL)
    ))
    return kb

def accept_order_inline(order_id):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Принять заявку", callback_data=f"take:{order_id}"))
    return kb

def confirm_order_inline(order_id):
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("🟢 Еду на заявку",    callback_data=f"confirm:{order_id}"),
        InlineKeyboardButton("🔴 Отказываюсь",      callback_data=f"decline:{order_id}")
    )
    kb.add(InlineKeyboardButton("🆘 Связаться с админом", callback_data=f"support_inline:{order_id}"))
    return kb

def owner_order_inline(order_id):
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("✅ Одобрить",   callback_data=f"approve:{order_id}"),
        InlineKeyboardButton("❌ Отклонить",  callback_data=f"reject:{order_id}")
    )
    return kb

def stats_nav_kb(page, total_pages):
    kb = InlineKeyboardMarkup()
    row = []
    if page > 1:
        row.append(InlineKeyboardButton("◀️", callback_data=f"stats_page:{page-1}"))
    if page < total_pages:
        row.append(InlineKeyboardButton("▶️", callback_data=f"stats_page:{page+1}"))
    if row:
        kb.row(*row)
    kb.add(InlineKeyboardButton("Закрыть", callback_data="stats_close"))
    return kb

# ─────────────────────────────────────────
# ТЕКСТ О СЕРВИСЕ (два варианта)
# ─────────────────────────────────────────
ABOUT_WORKER = (
    "🔧 <b>VSH Service — Октябрьский · Туймазы</b>\n\n"
    "Платформа для рабочих: грузчики, мастера, разнорабочие.\n\n"
    "👷 <b>Как это работает:</b>\n"
    "• Получаете уведомление о новой заявке\n"
    "• Открываете приложение и принимаете заказ\n"
    "• Звоните заказчику и едете на объект\n"
    "• Выполняете работу и получаете оплату\n\n"
    "⭐ <b>Рейтинг и звания:</b>\n"
    "За каждый заказ вы получаете звёзды. Чем больше звёзд — "
    "тем выше звание и доступ к лучшим заявкам.\n\n"
    "💰 <b>Без посредников:</b>\n"
    "Вся оплата идёт напрямую вам от заказчика.\n\n"
    "🆘 По вопросам: кнопка «Техподдержка»"
)

ABOUT_CUSTOMER = (
    "🔧 <b>VSH Service — Октябрьский · Туймазы</b>\n\n"
    "Быстрый найм рабочих, грузчиков и мастеров.\n\n"
    "⚡ <b>Как заказать:</b>\n"
    "• Оставляете заявку на сайте\n"
    "• Мы подбираем исполнителя за 15 минут\n"
    "• Рабочий перезвонит и приедет\n\n"
    "✅ <b>Наши преимущества:</b>\n"
    "• Цены ниже чем на Авито\n"
    "• Проверенные исполнители с рейтингом\n"
    "• Бригада — не один человек\n\n"
    "🆘 По вопросам: кнопка «Техподдержка»"
)

# ─────────────────────────────────────────
# /start — выбор роли
# ─────────────────────────────────────────
@bot.message_handler(commands=["start"])
def cmd_start(message):
    register_user(message.from_user)
    chat_id = message.chat.id

    if chat_id == OWNER_CHAT_ID:
        bot.send_message(
            chat_id,
            "👑 <b>Панель администратора VSH Service</b>",
            parse_mode="HTML",
            reply_markup=owner_menu_kb()
        )
        return

    bot.send_message(
        chat_id,
        "👋 Добро пожаловать в <b>VSH Service</b>!\n\n"
        "Мы помогаем найти работу и нанять рабочих "
        "в Октябрьском и Туймазах.\n\nКто вы?",
        parse_mode="HTML",
        reply_markup=role_select_kb()
    )

# ─────────────────────────────────────────
# ГЛАВНЫЙ ОБРАБОТЧИК СООБЩЕНИЙ
# ─────────────────────────────────────────
@bot.message_handler(func=lambda m: True)
def handle_message(message):
    chat_id = message.chat.id
    text    = (message.text or "").strip()
    key     = register_user(message.from_user)

    if is_banned(chat_id):
        return

    # ── АНТИСПАМ (добавлено) ──
    if check_spam(chat_id):
        return

    # ── ВЛАДЕЛЕЦ ──
    if chat_id == OWNER_CHAT_ID:
        _handle_owner(message, text, key)
        return

    # ── ВЫБОР РОЛИ ──
    if text == "👷 Я исполнитель":
        user_roles[chat_id] = "worker"
        workers_stream_active.add(chat_id)
        save_all()

        # Регистрируем рабочего в БД через API (чтобы получал уведомления при публикации через Mini App)
        try:
            user_obj = message.from_user
            api_post("/api/worker-auth", {
                "password": WORKER_PIN,
                "telegram_id": chat_id,
                "first_name": user_obj.first_name or "Рабочий",
                "telegram_username": user_obj.username or ""
            })
        except:
            pass

        bot.send_message(
            chat_id,
            "✅ <b>Режим исполнителя активирован!</b>\n\n"
            "🔔 Уведомления о новых заявках включены автоматически.\n\n"
            "🔐 Для входа в приложение нужен <b>PIN-код</b>.\n"
            "Если у вас нет кода — напишите в 🆘 <b>Техподдержку</b> слово <code>код</code> и мы вышлем его.\n\n"
            "Нажмите «🚀 Открыть приложение» чтобы войти.",
            parse_mode="HTML",
            reply_markup=worker_main_kb()
        )
        return

    if text == "📦 Я заказчик":
        user_roles[chat_id] = "customer"
        bot.send_message(
            chat_id,
            "✅ <b>Вы — заказчик</b>\n\n"
            "Для заказа рабочих перейдите на наш сайт.\n"
            "Если сайт ещё не готов — напишите в техподдержку, мы примем заявку вручную.",
            parse_mode="HTML",
            reply_markup=customer_main_kb()
        )
        return

    # ── ОБЩИЕ КНОПКИ ──
    if text == "🚀 Открыть приложение":
        bot.send_message(chat_id, "Нажмите кнопку ниже 👇", reply_markup=open_app_inline_kb())
        return

    if text == "🌐 Оставить заявку на сайте":
        bot.send_message(
            chat_id,
            f"🌐 <b>Заказать рабочих:</b>\n\n"
            f"👉 {CLIENT_URL}\n\n"
            f"Заполните форму — мы подберём исполнителя и перезвоним в течение 15 минут!",
            parse_mode="HTML"
        )
        return

    if text == "ℹ️ О сервисе":
        role = user_roles.get(chat_id)
        if role == "worker":
            bot.send_message(chat_id, ABOUT_WORKER, parse_mode="HTML")
        elif role == "customer":
            bot.send_message(chat_id, ABOUT_CUSTOMER, parse_mode="HTML")
        else:
            bot.send_message(chat_id, ABOUT_WORKER, parse_mode="HTML")
        return

    if text == "🏆 Мой рейтинг":
        score = ratings.get(key, {}).get("score", 0)
        bot.send_message(
            chat_id,
            f"⭐ <b>Ваш рейтинг:</b> {score} звёзд\n\n"
            f"Выполняйте заявки чтобы расти в рейтинге!",
            parse_mode="HTML"
        )
        return

    if text == "🆘 Техподдержка":
        msg = bot.send_message(chat_id, "🆘 Опишите вашу проблему или вопрос одним сообщением:")
        bot.register_next_step_handler(msg, support_handler)
        return

    if text == "↩️ Сменить роль":
        user_roles.pop(chat_id, None)
        bot.send_message(
            chat_id,
            "🔄 Роль сброшена. Выберите заново:",
            reply_markup=role_select_kb()
        )
        return

    # Если роль не выбрана
    if chat_id not in user_roles:
        bot.send_message(chat_id, "Выберите вашу роль:", reply_markup=role_select_kb())

# ─────────────────────────────────────────
# ВЛАДЕЛЕЦ: логика
# ─────────────────────────────────────────
def _handle_owner(message, text, key):
    chat_id = message.chat.id

    # Режим direct сообщения рабочему
    if pending_admin_direct.get(chat_id):
        target = pending_admin_direct.pop(chat_id)
        try:
            bot.send_message(target, f"📣 <b>Сообщение от администрации:</b>\n\n{text}", parse_mode="HTML")
            bot.send_message(chat_id, f"✅ Отправлено: {users.get(str(target), str(target))}")
        except:
            bot.send_message(chat_id, "❌ Не удалось отправить (рабочий заблокировал бота)")
        return

    # Режим рассылки
    if chat_id in pending_admin_broadcast:
        pending_admin_broadcast.discard(chat_id)
        sent = 0
        for wid in list(workers_stream_active):
            try:
                bot.send_message(int(wid), f"📢 <b>Сообщение от VSH Service:</b>\n\n{text}", parse_mode="HTML")
                sent += 1
            except:
                workers_stream_active.discard(int(wid))
        save_all()
        bot.send_message(chat_id, f"✅ Рассылка выполнена. Отправлено: {sent}")
        return

    # Режим ответа на тикет
    if pending_admin_replies.get(chat_id):
        target = pending_admin_replies.pop(chat_id)
        try:
            bot.send_message(target, f"📣 <b>Ответ администрации:</b>\n\n{text}", parse_mode="HTML")
            bot.send_message(chat_id, "✅ Ответ отправлен")
        except:
            bot.send_message(chat_id, "❌ Не удалось отправить ответ")
        return

    # ── КНОПКИ АДМИНА ──

    if text == "🚀 Открыть приложение":
        bot.send_message(chat_id, "Нажмите кнопку 👇", reply_markup=open_app_inline_kb())
        return

    if text == "📊 Рейтинг рабочих":
        _send_stats_page(chat_id, page=1, for_owner=True)
        return

    if text == "👥 Участники":
        _send_participants_page(chat_id, page=1)
        return

    if text == "📣 Рассылка всем":
        count = len(workers_stream_active)
        pending_admin_broadcast.add(chat_id)
        bot.send_message(chat_id, f"📣 Введите текст рассылки.\nПолучателей: {count} рабочих.")
        return

    if text == "📨 Написать рабочему":
        all_ids = sorted([int(u) for u in users if u.isdigit() and int(u) != OWNER_CHAT_ID])
        if not all_ids:
            bot.send_message(chat_id, "Нет зарегистрированных рабочих.")
            return
        kb = InlineKeyboardMarkup()
        for wid in all_ids[:30]:
            label = users.get(str(wid), str(wid))[:25]
            active = "🟢" if wid in workers_stream_active else "⚪"
            kb.add(InlineKeyboardButton(f"{active} {label}", callback_data=f"owner_direct:{wid}"))
        bot.send_message(chat_id, "Выберите рабочего:", reply_markup=kb)
        return

    if text == "ℹ️ О сервисе":
        bot.send_message(chat_id, ABOUT_WORKER, parse_mode="HTML")
        return

# ─────────────────────────────────────────
# ПУБЛИКАЦИЯ ЗАЯВОК
# Вся публикация заявок происходит через Mini App (админ-панель).
# Бот — только входной ресепшн: уведомления, выбор роли, техподдержка.
# Бот сам проверяет новые заявки через API и рассылает уведомления.
# ─────────────────────────────────────────

# Множество ID заявок, о которых уже разослали уведомления
NOTIFIED_ORDERS_FILE = "notified_orders.json"
notified_orders = set(load_json(NOTIFIED_ORDERS_FILE, []))

def save_notified():
    save_json(NOTIFIED_ORDERS_FILE, list(notified_orders))

def check_new_orders():
    """Проверяет новые заявки со статусом published и рассылает уведомления."""
    try:
        r = requests.get(f"{API_BASE}/api/order?status=published", timeout=10)
        orders = r.json()
    except Exception as e:
        logger.debug("check_new_orders error: %s", e)
        return

    if not isinstance(orders, list):
        return

    for order in orders:
        oid = order.get("id")
        if not oid or oid in notified_orders:
            continue

        # Новая заявка — рассылаем всем исполнителям
        notified_orders.add(oid)

        city    = order.get("city", "Октябрьский")
        task    = order.get("service") or order.get("task") or "Задача"
        address = order.get("address", "")
        workers = order.get("workers_needed", 1)
        comment = order.get("comment", "")

        msg_text = (
            f"🔥 <b>НОВАЯ ЗАЯВКА №{oid}</b>\n\n"
            f"📍 {city}\n"
            f"🔧 {task}\n"
            f"🏠 {address}\n"
            f"👷 Рабочих: {workers}\n"
            + (f"💬 {comment}\n" if comment else "")
            + f"\nОткройте приложение чтобы принять заказ 👇"
        )

        sent = 0
        for wid in list(workers_stream_active):
            try:
                bot.send_message(
                    int(wid),
                    msg_text,
                    parse_mode="HTML",
                    reply_markup=open_app_inline_kb()
                )
                sent += 1
            except Exception as e:
                logger.debug("Уведомление не отправлено %s: %s", wid, e)

        logger.info("📢 Заявка #%s: уведомления отправлены %d рабочим", oid, sent)

    save_notified()

def order_watcher():
    """Фоновый поток: проверяет новые заявки каждые 10 секунд."""
    logger.info("🔔 Order watcher запущен")
    while True:
        try:
            check_new_orders()
        except Exception as e:
            logger.debug("order_watcher error: %s", e)
        time.sleep(10)

# ─────────────────────────────────────────
# ТЕХПОДДЕРЖКА
# ─────────────────────────────────────────
def support_handler(message):
    worker_id = message.chat.id
    worker_key = users.get(str(worker_id), str(worker_id))
    text = (message.text or "").strip()
    if not text:
        bot.send_message(worker_id, "❌ Пустое сообщение. Попробуйте снова через кнопку техподдержки.")
        return

    # Автоответ на запрос PIN-кода
    if text.lower().strip() in ["код", "pin", "пин", "код доступа", "пароль"]:
        bot.send_message(
            worker_id,
            f"🔐 <b>PIN-код для входа в приложение:</b>\n\n"
            f"<code>{WORKER_PIN}</code>\n\n"
            f"Введите его на экране входа в Mini App.\n"
            f"Никому не передавайте код!",
            parse_mode="HTML"
        )
        return

    try:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("✉️ Ответить", callback_data=f"support_reply:{worker_id}"))
        role = user_roles.get(worker_id, "неизвестно")
        bot.send_message(
            OWNER_CHAT_ID,
            f"📩 <b>Техподдержка</b> от {worker_key} ({role})\nid: <code>{worker_id}</code>\n\n{text}",
            parse_mode="HTML",
            reply_markup=kb
        )
        bot.send_message(worker_id, "✅ Сообщение отправлено администратору. Ожидайте ответа.")
    except Exception as e:
        logger.exception("support_handler error: %s", e)
        bot.send_message(worker_id, "❌ Ошибка. Попробуйте позже.")

# ─────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    data    = call.data or ""
    user_id = call.from_user.id
    key     = register_user(call.from_user)

    bot.answer_callback_query(call.id)

    # ── Одобрить/отклонить заявку клиента ──
    if data.startswith("approve:"):
        order_id = data.split(":")[1]
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            bot.edit_message_text(
                call.message.text + "\n\n✅ <b>ОДОБРЕНО</b>",
                call.message.chat.id,
                call.message.message_id,
                parse_mode="HTML"
            )
        except:
            pass
        bot.send_message(OWNER_CHAT_ID, f"✅ Заявка #{order_id} одобрена. Откройте Mini App → Админ → Управление заявками для публикации.")
        return

    if data.startswith("reject:"):
        order_id = data.split(":")[1]
        try:
            bot.edit_message_text(
                call.message.text + "\n\n❌ <b>ОТКЛОНЕНО</b>",
                call.message.chat.id,
                call.message.message_id,
                parse_mode="HTML"
            )
        except:
            pass
        return

    # ── Рабочий принял заказ ──
    if data.startswith("take:"):
        order_id = data.split(":")[1]
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except:
            pass
        bot.send_message(
            user_id,
            f"✅ <b>Вы взяли заказ #{order_id}</b>\n\n"
            f"Откройте приложение чтобы увидеть детали:",
            parse_mode="HTML",
            reply_markup=open_app_inline_kb()
        )
        bot.send_message(
            OWNER_CHAT_ID,
            f"👷 <b>{key}</b> взял заказ #{order_id}",
            parse_mode="HTML"
        )
        return

    # ── Подтверждение выезда ──
    if data.startswith("confirm:"):
        order_id = data.split(":")[1]
        if key not in ratings:
            ratings[key] = {"score": 0}
        ratings[key]["score"] = min(1000, int(ratings[key].get("score", 0)) + 1)
        save_json(RATINGS_FILE, ratings)
        try:
            bot.edit_message_text("✅ Подтверждено. +1⭐ к рейтингу!", call.message.chat.id, call.message.message_id)
        except:
            bot.send_message(user_id, "✅ Подтверждено. +1⭐ к рейтингу!")
        bot.send_message(OWNER_CHAT_ID, f"✅ <b>{key}</b> подтвердил выезд на заказ #{order_id}. Рейтинг: {ratings[key]['score']}", parse_mode="HTML")
        return

    if data.startswith("decline:"):
        order_id = data.split(":")[1]
        try:
            bot.edit_message_text("❌ Вы отказались от заявки.", call.message.chat.id, call.message.message_id)
        except:
            pass
        bot.send_message(OWNER_CHAT_ID, f"❌ <b>{key}</b> отказался от заказа #{order_id}", parse_mode="HTML")
        return

    # ── Техподдержка ──
    if data.startswith("support_inline:"):
        msg = bot.send_message(user_id, "🆘 Опишите вашу проблему:")
        bot.register_next_step_handler(msg, support_handler)
        return

    if data.startswith("support_reply:"):
        if user_id != OWNER_CHAT_ID:
            return
        worker_id = int(data.split(":")[1])
        pending_admin_replies[OWNER_CHAT_ID] = worker_id
        bot.send_message(OWNER_CHAT_ID, f"✉️ Введите ответ для {users.get(str(worker_id), str(worker_id))}:")
        return

    if data.startswith("owner_direct:"):
        if user_id != OWNER_CHAT_ID:
            return
        worker_id = int(data.split(":")[1])
        pending_admin_direct[OWNER_CHAT_ID] = worker_id
        bot.send_message(OWNER_CHAT_ID, f"✉️ Введите сообщение для {users.get(str(worker_id), str(worker_id))}:")
        return

    # ── Навигация статистики ──
    if data.startswith("stats_page:"):
        page = int(data.split(":")[1])
        _send_stats_page(call.message.chat.id, page, for_owner=(user_id == OWNER_CHAT_ID))
        return

    if data == "stats_close":
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        return

    # ── Участники ──
    if data.startswith("participants_page:"):
        page = int(data.split(":")[1])
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        _send_participants_page(call.from_user.id, page)
        return

    if data == "participants_close":
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        return

    if data.startswith("delete_user:"):
        if user_id != OWNER_CHAT_ID:
            return
        uid = data.split(":")[1]
        _delete_user(uid)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        bot.send_message(OWNER_CHAT_ID, f"✅ Пользователь {uid} удалён.")
        _send_participants_page(OWNER_CHAT_ID, 1)
        return

# ─────────────────────────────────────────
# СТАТИСТИКА
# ─────────────────────────────────────────
PAGE_SIZE = 10

def _send_stats_page(chat_id, page=1, for_owner=False):
    if not ratings:
        bot.send_message(chat_id, "Пока нет данных по рейтингу.")
        return
    items = sorted(ratings.items(), key=lambda x: int(x[1].get("score", 0)), reverse=True)
    total_pages = max(1, (len(items) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(page, total_pages))
    sl = items[(page-1)*PAGE_SIZE : page*PAGE_SIZE]
    medals = ["🥇", "🥈", "🥉"]
    out = f"🏆 <b>Рейтинг рабочих</b> — стр. {page}/{total_pages}\n\n"
    for i, (k, v) in enumerate(sl, start=(page-1)*PAGE_SIZE+1):
        medal = medals[i-1] if i <= 3 else f"{i}."
        out += f"{medal} {k}: {v.get('score', 0)}⭐\n"
    bot.send_message(chat_id, out, parse_mode="HTML", reply_markup=stats_nav_kb(page, total_pages))

# ─────────────────────────────────────────
# УЧАСТНИКИ
# ─────────────────────────────────────────
PP = 6

def _send_participants_page(chat_id, page=1):
    uids = sorted([u for u in users if u.isdigit()], key=int)
    total = len(uids)
    total_pages = max(1, (total + PP - 1) // PP)
    page = max(1, min(page, total_pages))
    sl = uids[(page-1)*PP : page*PP]
    out = f"👥 <b>Участники</b> — стр. {page}/{total_pages} (всего: {total})\n\n"
    kb = InlineKeyboardMarkup()
    for uid in sl:
        disp  = users.get(uid, uid)
        flow  = "🟢" if int(uid) in workers_stream_active else "⚪"
        score = ratings.get(disp, {}).get("score", 0)
        out  += f"{flow} {disp} | ⭐{score}\n"
        kb.add(InlineKeyboardButton(f"❌ Удалить {disp[:15]}", callback_data=f"delete_user:{uid}"))
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"participants_page:{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"participants_page:{page+1}"))
    if nav:
        kb.row(*nav)
    kb.add(InlineKeyboardButton("Закрыть", callback_data="participants_close"))
    bot.send_message(chat_id, out, parse_mode="HTML", reply_markup=kb)

def _delete_user(uid):
    try:
        bot.send_message(int(uid), "⚠️ Вы были удалены администратором.")
    except:
        pass
    users.pop(uid, None)
    workers_stream_active.discard(int(uid))
    banned_users.add(int(uid))
    save_all()

# ─────────────────────────────────────────
# ВЕРСИОНИРОВАНИЕ И УВЕДОМЛЕНИЯ О НОВОВВЕДЕНИЯХ
# При каждом обновлении кода: поднимаешь BOT_VERSION и
# добавляешь текст в CHANGELOG — бот сам разошлёт всем
# пользователям при перезапуске. Нажимать /start не нужно.
# ─────────────────────────────────────────
BOT_VERSION = "2.2.0"
VERSION_FILE = "bot_version.json"

CHANGELOG = {
    "2.1.0": (
        "🆕 <b>Обновление VSH Service v2.1</b>\n\n"
        "• 🌐 Сайт для заказчиков запущен!\n"
        "• ✏️ Админ может редактировать заявки\n"
        "• ⛔ Антиспам-защита\n"
        "• ↩️ Кнопка «Сменить роль»\n"
        "• 🔔 Уведомления о новых заявках улучшены\n\n"
        "Спасибо что вы с нами! 💪"
    ),
    "2.2.0": (
        "🆕 <b>Обновление VSH Service v2.2</b>\n\n"
        "• 📢 Уведомления о заявках теперь приходят мгновенно\n"
        "• 🚀 Публикация заявок полностью через приложение\n"
        "• 🔄 Автоматические обновления без перезапуска\n"
        "• 🧹 Интерфейс бота упрощён и ускорен\n\n"
        "Спасибо что вы с нами! 💪"
    ),
}

def check_version_and_notify():
    """
    При обновлении бота — автоматически рассылает changelog
    ВСЕМ пользователям (из users.json). Не нужно нажимать /start.
    Каждый пользователь получает уведомление ровно один раз.
    """
    old_data = load_json(VERSION_FILE, {"version": "0.0.0", "notified": []})
    old_version = old_data.get("version", "0.0.0")
    already_notified = set(old_data.get("notified", []))

    if old_version == BOT_VERSION:
        return  # Версия не изменилась

    logger.info("Обновление: %s → %s", old_version, BOT_VERSION)

    changelog_text = CHANGELOG.get(BOT_VERSION)
    if not changelog_text:
        save_json(VERSION_FILE, {"version": BOT_VERSION, "notified": []})
        return

    # Собираем ВСЕХ пользователей: и из users.json, и из workers_stream_active
    all_user_ids = set()
    for uid in users:
        if uid.isdigit():
            all_user_ids.add(int(uid))
    for wid in workers_stream_active:
        all_user_ids.add(int(wid))

    # Не шлём админу отдельно — он тоже в списке
    new_notified = list(already_notified)
    sent = 0

    for uid in all_user_ids:
        uid_str = str(uid)
        if uid_str in already_notified:
            continue  # Уже получил
        try:
            bot.send_message(int(uid), changelog_text, parse_mode="HTML")
            new_notified.append(uid_str)
            sent += 1
        except Exception as e:
            logger.debug("Changelog не отправлен %s: %s", uid, e)
            new_notified.append(uid_str)  # Помечаем чтобы не спамить при следующем запуске

    save_json(VERSION_FILE, {"version": BOT_VERSION, "notified": new_notified})
    logger.info("Changelog v%s разослан: %d из %d пользователей", BOT_VERSION, sent, len(all_user_ids))

# ─────────────────────────────────────────
# ЗАПУСК С ЗАЩИТОЙ ОТ ПАДЕНИЙ
# ─────────────────────────────────────────
if __name__ == "__main__":
    logger.info("🚀 VSH Service Bot v%s запущен", BOT_VERSION)
    save_all()

    # Уведомляем о нововведениях
    try:
        check_version_and_notify()
    except Exception as e:
        logger.exception("Ошибка уведомления о версии: %s", e)

    # Запускаем фоновый поток проверки новых заявок
    watcher_thread = threading.Thread(target=order_watcher, daemon=True)
    watcher_thread.start()
    logger.info("🔔 Фоновая проверка заявок запущена (каждые 10 сек)")

    # Бесконечный цикл с автоперезапуском при сбоях
    MAX_RETRIES = 0  # 0 = бесконечно
    retry_count = 0
    retry_delay = 5  # секунд

    while True:
        try:
            logger.info("Запуск polling...")
            bot.infinity_polling(timeout=60, long_polling_timeout=30, allowed_updates=None)
        except KeyboardInterrupt:
            logger.info("Остановка по Ctrl+C")
            break
        except Exception as e:
            retry_count += 1
            logger.exception("Ошибка polling (попытка #%d): %s", retry_count, e)

            # Уведомляем админа о падении
            try:
                bot.send_message(
                    OWNER_CHAT_ID,
                    f"⚠️ <b>Бот перезапускается</b>\n\n"
                    f"Ошибка: <code>{str(e)[:200]}</code>\n"
                    f"Попытка: #{retry_count}\n"
                    f"Перезапуск через {retry_delay} сек...",
                    parse_mode="HTML"
                )
            except:
                pass

            time.sleep(retry_delay)
            # Увеличиваем задержку, но не больше 60 сек
            retry_delay = min(60, retry_delay + 5)

            if MAX_RETRIES > 0 and retry_count >= MAX_RETRIES:
                logger.critical("Превышено кол-во попыток (%d). Остановка.", MAX_RETRIES)
                break
        else:
            # Если polling завершился нормально — сбрасываем счётчик
            retry_count = 0
            retry_delay = 5
