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
# ФАЙЛЫ ХРАНЕНИЯ (JSON — для совместимости с Puzzle Bot)
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
user_roles               = {}   # chat_id -> "worker" | "customer"
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
    """Отправить данные в Vercel API."""
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
    btn_app = KeyboardButton("🚀 Открыть приложение")
    btn_info = KeyboardButton("ℹ️ О сервисе")
    btn_stats = KeyboardButton("🏆 Мой рейтинг")
    btn_stream = KeyboardButton("🔔 Включить поток заявок")
    btn_support = KeyboardButton("🆘 Техподдержка")
    kb.add(btn_app, btn_info)
    kb.add(btn_stream, btn_stats)
    kb.add(btn_support)
    return kb

def customer_main_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    kb.add(KeyboardButton("🌐 Оставить заявку на сайте"))
    kb.add(KeyboardButton("ℹ️ О сервисе"))
    return kb

def owner_menu_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(KeyboardButton("📋 Опубликовать заказ"), KeyboardButton("🚀 Открыть приложение"))
    kb.add(KeyboardButton("📊 Рейтинг рабочих"),    KeyboardButton("👥 Участники"))
    kb.add(KeyboardButton("📣 Рассылка всем"),       KeyboardButton("📨 Написать рабочему"))
    kb.add(KeyboardButton("ℹ️ О сервисе"))
    return kb

def open_app_inline_kb(role="worker"):
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
    kb.add(InlineKeyboardButton("🆘 Техподдержка", callback_data=f"support_inline:{order_id}"))
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
# ТЕКСТ О СЕРВИСЕ
# ─────────────────────────────────────────
ABOUT_TEXT = (
    "🔧 <b>VSH Service — Октябрьский</b>\n\n"
    "Сервис быстрого найма рабочих, грузчиков и мастеров.\n\n"
    "⚡ <b>Для заказчиков:</b>\n"
    "• Оставляете заявку на сайте\n"
    "• Диспетчер перезванивает в течение 15 минут\n"
    "• Бригада прибывает за 60 минут\n\n"
    "👷 <b>Для исполнителей:</b>\n"
    "• Работаете через приложение\n"
    "• Берёте заявки онлайн\n"
    "• Получаете оплату день в день\n\n"
    "📞 По вопросам: @vsh_support"
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
        "👋 Добро пожаловать в <b>VSH Service</b>!\n\nКто вы?",
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

    # ── ВЛАДЕЛЕЦ ──────────────────────────────────────
    if chat_id == OWNER_CHAT_ID:
        _handle_owner(message, text, key)
        return

    # ── ВЫБОР РОЛИ ────────────────────────────────────
    if text == "👷 Я исполнитель":
        user_roles[chat_id] = "worker"
        bot.send_message(
            chat_id,
            "✅ <b>Режим исполнителя активирован</b>\n\n"
            "Нажмите «🚀 Открыть приложение» чтобы начать брать заявки.",
            parse_mode="HTML",
            reply_markup=worker_main_kb()
        )
        return

    if text == "📦 Я заказчик":
        user_roles[chat_id] = "customer"
        bot.send_message(
            chat_id,
            "✅ <b>Вы выбрали режим заказчика</b>\n\n"
            "Для заказа рабочих перейдите на наш сайт:",
            parse_mode="HTML",
            reply_markup=customer_main_kb()
        )
        bot.send_message(
            chat_id,
            f"🌐 <b>Сайт для заказа:</b>\n{MINI_APP_URL}",
            parse_mode="HTML"
        )
        return

    # ── КНОПКИ РАБОЧЕГО ───────────────────────────────
    if text == "🚀 Открыть приложение":
        bot.send_message(
            chat_id,
            "Нажмите кнопку ниже 👇",
            reply_markup=open_app_inline_kb()
        )
        return

    if text == "🌐 Оставить заявку на сайте":
        bot.send_message(
            chat_id,
            f"🌐 Перейдите на сайт для оформления заявки:\n{MINI_APP_URL}",
            parse_mode="HTML"
        )
        return

    if text == "ℹ️ О сервисе":
        bot.send_message(chat_id, ABOUT_TEXT, parse_mode="HTML")
        return

    if text == "🏆 Мой рейтинг":
        score = ratings.get(key, {}).get("score", 0)
        bot.send_message(
            chat_id,
            f"⭐ <b>Ваш рейтинг:</b> {score}/1000",
            parse_mode="HTML"
        )
        return

    if text == "🔔 Включить поток заявок":
        if chat_id in workers_stream_active:
            bot.send_message(chat_id, "✅ Поток заявок уже активирован. Ждите новых заданий.")
        else:
            workers_stream_active.add(chat_id)
            save_all()
            bot.send_message(
                chat_id,
                "🔔 <b>Поток заявок включён!</b>\nВы будете получать уведомления о новых заказах.",
                parse_mode="HTML"
            )
        return

    if text == "🆘 Техподдержка":
        msg = bot.send_message(chat_id, "🆘 Опишите проблему или вопрос:")
        bot.register_next_step_handler(msg, support_handler)
        return

    # Если роль не выбрана — предлагаем выбрать
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
                bot.send_message(int(wid), f"📢 <b>Сообщение от администрации:</b>\n\n{text}", parse_mode="HTML")
                sent += 1
            except:
                pass
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

    if text == "📋 Опубликовать заказ":
        msg = bot.send_message(chat_id, "📍 Введите адрес и описание задачи:")
        bot.register_next_step_handler(msg, owner_step_price)
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
        pending_admin_broadcast.add(chat_id)
        bot.send_message(chat_id, "📣 Введите текст рассылки для всех активных рабочих:")
        return

    if text == "📨 Написать рабочему":
        all_ids = sorted([int(u) for u in users if u.isdigit() and int(u) != OWNER_CHAT_ID])
        if not all_ids:
            bot.send_message(chat_id, "Нет зарегистрированных рабочих.")
            return
        kb = InlineKeyboardMarkup()
        for wid in all_ids[:30]:
            label = users.get(str(wid), str(wid))[:25]
            kb.add(InlineKeyboardButton(label, callback_data=f"owner_direct:{wid}"))
        bot.send_message(chat_id, "Выберите рабочего:", reply_markup=kb)
        return

    if text == "ℹ️ О сервисе":
        bot.send_message(chat_id, ABOUT_TEXT, parse_mode="HTML")
        return

# ─────────────────────────────────────────
# ВЛАДЕЛЕЦ: публикация заказа (3 шага)
# ─────────────────────────────────────────
def owner_step_price(message):
    temp = {"address": message.text.strip()}
    msg = bot.send_message(message.chat.id, "💰 Ставка для клиента (₽):")
    bot.register_next_step_handler(msg, owner_step_margin, temp)

def owner_step_margin(message, temp):
    try:
        temp["client_price"] = int(re.sub(r'\D', '', message.text))
    except:
        bot.send_message(message.chat.id, "❌ Введите число. Попробуйте снова.")
        msg = bot.send_message(message.chat.id, "💰 Ставка для клиента (₽):")
        bot.register_next_step_handler(msg, owner_step_margin, temp)
        return
    msg = bot.send_message(message.chat.id, "📊 Ваша маржа (₽), например 200:")
    bot.register_next_step_handler(msg, owner_step_workers, temp)

def owner_step_workers(message, temp):
    try:
        temp["margin"] = int(re.sub(r'\D', '', message.text))
    except:
        temp["margin"] = 200
    temp["worker_price"] = temp["client_price"] - temp["margin"]
    if temp["worker_price"] <= 0:
        bot.send_message(message.chat.id, "❌ Ставка меньше маржи. Начните заново.")
        return
    msg = bot.send_message(message.chat.id, "👷 Сколько рабочих нужно (число):")
    bot.register_next_step_handler(msg, owner_publish_order, temp)

def owner_publish_order(message, temp):
    chat_id = message.chat.id
    try:
        temp["workers_needed"] = int(re.sub(r'\D', '', message.text))
    except:
        temp["workers_needed"] = 1

    # Публикуем через Vercel API
    result = api_post("/api/order", {
        "source":         "admin",
        "service":        temp["address"],
        "address":        temp["address"],
        "client_price":   temp["client_price"],
        "worker_price":   temp["worker_price"],
        "margin":         temp["margin"],
        "workers_needed": temp["workers_needed"],
        "status":         "published"
    })

    if result.get("success") or result.get("orderId"):
        order_id = result.get("orderId", "?")
        bot.send_message(
            chat_id,
            f"✅ <b>Заказ №{order_id} опубликован!</b>\n\n"
            f"🔧 {temp['address']}\n"
            f"💰 Клиент: {temp['client_price']}₽ | Рабочим: {temp['worker_price']}₽ | Маржа: {temp['margin']}₽\n"
            f"👷 Нужно: {temp['workers_needed']} чел.\n\n"
            f"Рабочие получили уведомления в Telegram.",
            parse_mode="HTML",
            reply_markup=owner_menu_kb()
        )
        # Также рассылаем через бота рабочим с кнопкой
        _broadcast_order_to_workers(order_id, temp["address"], temp["worker_price"])
    else:
        bot.send_message(
            chat_id,
            f"⚠️ Заказ создан, но API вернул ошибку: {result.get('error', 'неизвестно')}\n"
            f"Проверьте Vercel и базу данных.",
            reply_markup=owner_menu_kb()
        )

def _broadcast_order_to_workers(order_id, address, worker_price):
    """Рассылаем уведомление рабочим через бота с inline-кнопкой."""
    for wid in list(workers_stream_active):
        try:
            bot.send_message(
                int(wid),
                f"🔥 <b>НОВЫЙ ЗАКАЗ #{order_id}</b>\n\n"
                f"🔧 {address}\n"
                f"💰 Ставка: {worker_price}₽\n\n"
                f"Нажмите кнопку чтобы открыть приложение и принять заказ:",
                parse_mode="HTML",
                reply_markup=open_app_inline_kb()
            )
        except Exception as e:
            logger.debug("Не удалось отправить рабочему %s: %s", wid, e)
            workers_stream_active.discard(int(wid))
    save_all()

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
        bot.send_message(
            OWNER_CHAT_ID,
            f"📩 <b>Техподдержка</b> от {worker_key} (id: {worker_id}):\n\n{text}",
            parse_mode="HTML",
            reply_markup=kb
        )
        bot.send_message(worker_id, "✅ Сообщение отправлено. Ожидайте ответа администратора.")
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

    # Одобрить заявку клиента (владелец)
    if data.startswith("approve:"):
        order_id = data.split(":")[1]
        # Обновляем статус через API — меняем на published
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
        bot.send_message(OWNER_CHAT_ID, f"✅ Заявка #{order_id} одобрена. Теперь опубликуйте её как заказ через «📋 Опубликовать заказ».")
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

    # Рабочий принял заказ
    if data.startswith("take:"):
        order_id = data.split(":")[1]
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except:
            pass
        bot.send_message(
            user_id,
            f"✅ <b>Вы взяли заказ #{order_id}</b>\n\nОткройте приложение чтобы увидеть детали и адрес:",
            parse_mode="HTML",
            reply_markup=open_app_inline_kb()
        )
        bot.send_message(
            OWNER_CHAT_ID,
            f"👷 <b>{key}</b> взял заказ #{order_id}",
            parse_mode="HTML"
        )
        return

    # Подтверждение выезда
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

    # Техподдержка inline
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

    # Статистика навигация
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

    # Участники: пагинация и удаление
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
    out = f"🏆 <b>Рейтинг рабочих</b> — стр. {page}/{total_pages}\n\n"
    for i, (k, v) in enumerate(sl, start=(page-1)*PAGE_SIZE+1):
        out += f"{i}. {k}: {v.get('score', 0)}⭐\n"
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
    out = f"👥 <b>Участники</b> — стр. {page}/{total_pages}\n\n"
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
