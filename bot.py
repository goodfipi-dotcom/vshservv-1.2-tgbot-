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
TOKEN         = os.getenv("TG_BOT_TOKEN", "8796585755:AAH3inuCnhQfKI7rT-AEh1zNfOSXDffIKyo")
OWNER_CHAT_ID = int(os.getenv("OWNER_CHAT_ID", "7693616720"))
MINI_APP_URL  = os.getenv("MINI_APP_URL", "https://mini-appsvsh.vercel.app")
API_BASE      = os.getenv("API_BASE",     "https://mini-appsvsh.vercel.app")

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
    kb.add(KeyboardButton("🚀 Открыть приложение"), KeyboardButton("ℹ️ О сервисе"))
    kb.add(KeyboardButton("🔔 Включить уведомления"), KeyboardButton("🏆 Мой рейтинг"))
    kb.add(KeyboardButton("🆘 Техподдержка"))
    return kb

def customer_main_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    kb.add(KeyboardButton("🌐 Оставить заявку на сайте"))
    kb.add(KeyboardButton("ℹ️ О сервисе"))
    kb.add(KeyboardButton("🆘 Техподдержка"))
    return kb

def owner_menu_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(KeyboardButton("📋 Опубликовать заказ"), KeyboardButton("🚀 Открыть приложение"))
    kb.add(KeyboardButton("📊 Рейтинг рабочих"),    KeyboardButton("👥 Участники"))
    kb.add(KeyboardButton("📣 Рассылка всем"),       KeyboardButton("📨 Написать рабочему"))
    kb.add(KeyboardButton("ℹ️ О сервисе"))
    return kb

def open_app_inline_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(
        "🚀 Открыть VSH Service",
        web_app=telebot.types.WebAppInfo(url=MINI_APP_URL)
    ))
    return kb

def city_select_kb():
    """Выбор города при публикации заказа."""
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("📍 Октябрьский", callback_data="pub_city:Октябрьский"),
        InlineKeyboardButton("📍 Туймазы",     callback_data="pub_city:Туймазы")
    )
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

    # ── ВЛАДЕЛЕЦ ──
    if chat_id == OWNER_CHAT_ID:
        _handle_owner(message, text, key)
        return

    # ── ВЫБОР РОЛИ ──
    if text == "👷 Я исполнитель":
        user_roles[chat_id] = "worker"
        workers_stream_active.add(chat_id)
        save_all()
        bot.send_message(
            chat_id,
            "✅ <b>Режим исполнителя активирован!</b>\n\n"
            "🔔 Уведомления о новых заявках включены автоматически.\n\n"
            "Нажмите «🚀 Открыть приложение» чтобы войти в систему.",
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
        # TODO: заменить на реальный URL сайта заказчиков когда будет готов
        bot.send_message(
            chat_id,
            "🌐 Сайт для заказчиков скоро будет готов!\n\n"
            "А пока вы можете оставить заявку через техподдержку — нажмите 🆘",
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

    if text == "🔔 Включить уведомления":
        if chat_id in workers_stream_active:
            bot.send_message(chat_id, "✅ Уведомления уже включены. Ждите новых заявок!")
        else:
            workers_stream_active.add(chat_id)
            save_all()
            bot.send_message(
                chat_id,
                "🔔 <b>Уведомления включены!</b>\n"
                "Вы будете получать сообщения о новых заказах.",
                parse_mode="HTML"
            )
        return

    if text == "🆘 Техподдержка":
        msg = bot.send_message(chat_id, "🆘 Опишите вашу проблему или вопрос одним сообщением:")
        bot.register_next_step_handler(msg, support_handler)
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

    if text == "📋 Опубликовать заказ":
        bot.send_message(chat_id, "📍 <b>Выберите город:</b>", parse_mode="HTML", reply_markup=city_select_kb())
        return

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
# ПУБЛИКАЦИЯ ЗАКАЗА (через бота, 4 шага)
# Город → Задача → Адрес+Телефон → Кол-во рабочих
# Без маржи — модель без посредников
# ─────────────────────────────────────────
publish_temp = {}  # chat_id -> dict с данными формы

def pub_step_task(message):
    """Шаг 2: Задача / описание."""
    chat_id = message.chat.id
    text = (message.text or "").strip()
    if not text:
        msg = bot.send_message(chat_id, "❌ Пустое сообщение. Введите описание задачи:")
        bot.register_next_step_handler(msg, pub_step_task)
        return
    publish_temp[chat_id]["task"] = text
    msg = bot.send_message(chat_id, "📍 Введите <b>адрес</b> (улица, дом):", parse_mode="HTML")
    bot.register_next_step_handler(msg, pub_step_address)

def pub_step_address(message):
    """Шаг 3: Адрес."""
    chat_id = message.chat.id
    text = (message.text or "").strip()
    if not text:
        msg = bot.send_message(chat_id, "❌ Введите адрес:")
        bot.register_next_step_handler(msg, pub_step_address)
        return
    publish_temp[chat_id]["address"] = text
    msg = bot.send_message(chat_id, "📞 Введите <b>телефон заказчика</b>:", parse_mode="HTML")
    bot.register_next_step_handler(msg, pub_step_phone)

def pub_step_phone(message):
    """Шаг 4: Телефон."""
    chat_id = message.chat.id
    text = (message.text or "").strip()
    if not text:
        msg = bot.send_message(chat_id, "❌ Введите телефон:")
        bot.register_next_step_handler(msg, pub_step_phone)
        return
    publish_temp[chat_id]["phone"] = text
    msg = bot.send_message(
        chat_id,
        "👷 Сколько рабочих нужно? (число, по умолчанию 1)\n\n"
        "Или напишите <b>комментарий</b> через запятую, например: <code>2, нужен инструмент</code>",
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, pub_step_workers)

def pub_step_workers(message):
    """Шаг 5: Количество рабочих + комментарий → публикация."""
    chat_id = message.chat.id
    text = (message.text or "").strip()

    workers_needed = 1
    comment = ""

    if "," in text:
        parts = text.split(",", 1)
        try:
            workers_needed = max(1, int(re.sub(r'\D', '', parts[0])))
        except:
            workers_needed = 1
        comment = parts[1].strip()
    else:
        try:
            workers_needed = max(1, int(re.sub(r'\D', '', text)))
        except:
            workers_needed = 1

    temp = publish_temp.pop(chat_id, {})
    if not temp:
        bot.send_message(chat_id, "❌ Данные формы потеряны. Начните заново.", reply_markup=owner_menu_kb())
        return

    temp["workers_needed"] = workers_needed
    temp["comment"] = comment

    # Показываем превью
    preview = (
        f"📋 <b>Превью заявки:</b>\n\n"
        f"📍 {temp['city']}\n"
        f"🔧 {temp['task']}\n"
        f"🏠 {temp['address']}\n"
        f"📞 {temp['phone']}\n"
        f"👷 Рабочих: {workers_needed}\n"
    )
    if comment:
        preview += f"💬 {comment}\n"

    preview += "\nОпубликовать?"

    # Сохраняем для подтверждения
    publish_temp[chat_id] = temp

    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("✅ Опубликовать", callback_data="pub_confirm"),
        InlineKeyboardButton("❌ Отменить",     callback_data="pub_cancel")
    )
    bot.send_message(chat_id, preview, parse_mode="HTML", reply_markup=kb)

def _do_publish(chat_id):
    """Публикация заказа в БД и рассылка рабочим."""
    temp = publish_temp.pop(chat_id, {})
    if not temp:
        bot.send_message(chat_id, "❌ Данные формы потеряны.", reply_markup=owner_menu_kb())
        return

    # Отправляем в Vercel API → сохраняется в Neon БД
    result = api_post("/api/order", {
        "source":         "admin",
        "service":        temp["task"],
        "address":        temp["address"],
        "phone":          temp["phone"],
        "city":           temp["city"],
        "comment":        temp.get("comment", ""),
        "workers_needed": temp["workers_needed"],
    })

    if result.get("success") or result.get("orderId"):
        order_id = result.get("orderId", "?")

        bot.send_message(
            chat_id,
            f"✅ <b>Заказ №{order_id} опубликован!</b>\n\n"
            f"📍 {temp['city']}\n"
            f"🔧 {temp['task']}\n"
            f"🏠 {temp['address']}\n"
            f"📞 {temp['phone']}\n"
            f"👷 Рабочих: {temp['workers_needed']}",
            parse_mode="HTML",
            reply_markup=owner_menu_kb()
        )

        # Рассылаем рабочим (только через бота, не через API)
        _broadcast_order_to_workers(order_id, temp)
    else:
        bot.send_message(
            chat_id,
            f"⚠️ Ошибка API: {result.get('error', 'неизвестно')}\n"
            f"Проверьте Vercel.",
            reply_markup=owner_menu_kb()
        )

def _broadcast_order_to_workers(order_id, temp):
    """Рассылка уведомлений рабочим через бота."""
    sent = 0
    failed = 0
    for wid in list(workers_stream_active):
        try:
            bot.send_message(
                int(wid),
                f"🔥 <b>НОВАЯ ЗАЯВКА №{order_id}</b>\n\n"
                f"📍 {temp['city']}\n"
                f"🔧 {temp['task']}\n"
                f"🏠 {temp['address']}\n"
                f"👷 Нужно рабочих: {temp['workers_needed']}\n"
                + (f"💬 {temp['comment']}\n" if temp.get('comment') else "")
                + f"\nОткройте приложение чтобы принять заказ:",
                parse_mode="HTML",
                reply_markup=open_app_inline_kb()
            )
            sent += 1
        except Exception as e:
            logger.debug("Не удалось отправить рабочему %s: %s", wid, e)
            workers_stream_active.discard(int(wid))
            failed += 1
    save_all()
    logger.info("Рассылка заказа #%s: отправлено %d, ошибок %d", order_id, sent, failed)

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

    # ── Публикация: выбор города ──
    if data.startswith("pub_city:"):
        if user_id != OWNER_CHAT_ID:
            return
        city = data.split(":", 1)[1]
        publish_temp[user_id] = {"city": city}
        try:
            bot.edit_message_text(
                f"📍 Город: <b>{city}</b>\n\n🔧 Введите <b>описание задачи</b>:",
                call.message.chat.id,
                call.message.message_id,
                parse_mode="HTML"
            )
        except:
            bot.send_message(user_id, f"📍 Город: {city}\n\n🔧 Введите описание задачи:")
        bot.register_next_step_handler(call.message, pub_step_task)
        return

    if data == "pub_confirm":
        if user_id != OWNER_CHAT_ID:
            return
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except:
            pass
        _do_publish(user_id)
        return

    if data == "pub_cancel":
        if user_id != OWNER_CHAT_ID:
            return
        publish_temp.pop(user_id, None)
        try:
            bot.edit_message_text("❌ Публикация отменена.", call.message.chat.id, call.message.message_id)
        except:
            pass
        bot.send_message(user_id, "Отменено.", reply_markup=owner_menu_kb())
        return

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
        bot.send_message(OWNER_CHAT_ID, f"✅ Заявка #{order_id} одобрена. Опубликуйте через «📋 Опубликовать заказ».")
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
# ЗАПУСК
# ─────────────────────────────────────────
if __name__ == "__main__":
    logger.info("🚀 VSH Service Bot запущен")
    save_all()
    try:
        bot.infinity_polling(timeout=60, long_polling_timeout=30)
    except Exception as e:
        logger.exception("Ошибка polling: %s", e)
