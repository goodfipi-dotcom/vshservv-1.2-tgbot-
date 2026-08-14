"""
Работа с базой для бота.

Раньше список рабочих жил в workers.json на диске: он терялся при перезапуске,
не совпадал с базой, и рассылка уходила трём людям вместо сорока шести.
Теперь источник истины один — таблица workers в Neon.
"""
import os
import logging
import threading
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
_lock = threading.Lock()
_conn = None


def _connect():
    global _conn
    if _conn is not None and _conn.closed == 0:
        return _conn
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задан — бот не может работать без базы")
    _conn = psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=10)
    _conn.autocommit = True
    return _conn


@contextmanager
def cursor(dict_rows=False):
    """Курсор с автоматическим переподключением при обрыве связи."""
    with _lock:
        for attempt in (1, 2):
            try:
                conn = _connect()
                factory = psycopg2.extras.RealDictCursor if dict_rows else None
                cur = conn.cursor(cursor_factory=factory)
                try:
                    yield cur
                finally:
                    cur.close()
                return
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                global _conn
                _conn = None
                if attempt == 2:
                    logger.error("БД недоступна: %s", e)
                    raise


class WorkerRegistry:
    """
    Ведёт себя как множество telegram_id (add / discard / in / len / итерация),
    но данные лежат в базе. Благодаря этому старый код бота менять почти не пришлось.
    """

    def add(self, worker_id):
        wid = str(worker_id)
        try:
            with cursor() as cur:
                cur.execute(
                    """INSERT INTO workers (id, telegram_id, name, active, notify_orders, status, role)
                       VALUES (%s, %s, 'Рабочий', TRUE, TRUE, 'available', 'worker')
                       ON CONFLICT (id) DO UPDATE SET notify_orders = TRUE, active = TRUE""",
                    (wid, wid),
                )
        except Exception as e:
            logger.error("не удалось включить уведомления для %s: %s", wid, e)

    def discard(self, worker_id):
        wid = str(worker_id)
        try:
            with cursor() as cur:
                cur.execute("UPDATE workers SET notify_orders = FALSE WHERE id = %s", (wid,))
        except Exception as e:
            logger.error("не удалось выключить уведомления для %s: %s", wid, e)

    def __contains__(self, worker_id):
        try:
            with cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM workers WHERE id = %s AND active AND notify_orders",
                    (str(worker_id),),
                )
                return cur.fetchone() is not None
        except Exception:
            return False

    def __iter__(self):
        return iter(self.ids())

    def __len__(self):
        return len(self.ids())

    def ids(self):
        """Кому рассылать заявки: активные, с включёнными уведомлениями."""
        try:
            with cursor() as cur:
                cur.execute("SELECT id FROM workers WHERE active AND notify_orders")
                return [row[0] for row in cur.fetchall()]

        except Exception as e:
            logger.error("не удалось получить список рабочих: %s", e)
            return []

    def recipients_for(self, city_id=None, service_id=None):
        """
        Кому именно уходит конкретная заявка: город и специализация.
        Рабочий без указанного города или без специализаций получает всё —
        чтобы никого не потерять, пока справочники не заполнены.
        """
        conditions = ["active", "notify_orders"]
        params = []

        if city_id:
            conditions.append("(city_id IS NULL OR city_id = %s)")
            params.append(city_id)

        if service_id:
            conditions.append(
                "(NOT EXISTS (SELECT 1 FROM worker_specializations ws WHERE ws.worker_id = workers.id)"
                " OR EXISTS (SELECT 1 FROM worker_specializations ws"
                "            WHERE ws.worker_id = workers.id AND ws.service_id = %s))"
            )
            params.append(service_id)

        try:
            with cursor() as cur:
                cur.execute(f"SELECT id FROM workers WHERE {' AND '.join(conditions)}", params)
                return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error("не удалось подобрать получателей: %s", e)
            return self.ids()


def register_user(telegram_id, name="", username=""):
    """Запоминаем человека в базе. Уведомления не включаем — это отдельное действие."""
    wid = str(telegram_id)
    try:
        with cursor() as cur:
            cur.execute(
                """INSERT INTO workers (id, telegram_id, name, telegram_username, active, notify_orders, status, role, last_seen)
                   VALUES (%s, %s, %s, %s, TRUE, FALSE, 'available', 'worker', NOW())
                   ON CONFLICT (id) DO UPDATE
                     SET name = COALESCE(NULLIF(EXCLUDED.name, ''), workers.name),
                         telegram_username = COALESCE(NULLIF(EXCLUDED.telegram_username, ''), workers.telegram_username),
                         last_seen = NOW()""",
                (wid, wid, name or "Рабочий", username or ""),
            )
    except Exception as e:
        logger.error("не удалось записать пользователя %s: %s", wid, e)


def is_active(telegram_id):
    try:
        with cursor() as cur:
            cur.execute("SELECT active FROM workers WHERE id = %s", (str(telegram_id),))
            row = cur.fetchone()
            return bool(row and row[0])
    except Exception:
        return False


def get_worker(telegram_id):
    try:
        with cursor(dict_rows=True) as cur:
            cur.execute("SELECT * FROM workers WHERE id = %s", (str(telegram_id),))
            return cur.fetchone()
    except Exception:
        return None


# ── Журнал уведомлений: чтобы после перезапуска бот не рассылал заявки повторно ──

def was_notified(order_id, kind="new_order"):
    try:
        with cursor() as cur:
            cur.execute(
                "SELECT 1 FROM notifications WHERE order_id = %s AND type = %s AND status = 'sent' LIMIT 1",
                (order_id, kind),
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def log_notification(order_id, recipient, kind, status, error="", role="worker", payload=""):
    try:
        with cursor() as cur:
            cur.execute(
                """INSERT INTO notifications (order_id, recipient, recipient_role, type, status, attempts, error, payload, sent_at)
                   VALUES (%s,%s,%s,%s,%s,1,%s,%s, CASE WHEN %s = 'sent' THEN NOW() ELSE NULL END)""",
                (order_id, str(recipient), role, kind, status, str(error)[:500], str(payload)[:500], status),
            )
    except Exception as e:
        logger.error("журнал уведомлений недоступен: %s", e)


def log_order_event(order_id, kind, actor="bot", actor_role="system", payload=""):
    try:
        with cursor() as cur:
            cur.execute(
                "INSERT INTO order_events (order_id, type, actor, actor_role, payload) VALUES (%s,%s,%s,%s,%s)",
                (order_id, kind, str(actor), actor_role, str(payload)[:1000]),
            )
    except Exception as e:
        logger.error("событие заказа не записано: %s", e)


def all_known_users():
    """Все, кто хоть раз заходил — для служебных рассылок."""
    try:
        with cursor() as cur:
            cur.execute("SELECT id FROM workers WHERE active")
            return [row[0] for row in cur.fetchall()]
    except Exception:
        return []
