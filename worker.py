# worker.py — полная версия с heartbeat и Postgres-сессиями
# Таймзона: Europe/Bucharest

import asyncio
import os
import yaml
import random
import json
import signal
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import time
import psutil
os.environ["TZ"] = "Europe/Bucharest"
try:
    time.tzset()
except Exception:
    pass

from dotenv import load_dotenv
from telethon import TelegramClient, errors
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import User as TLUser, Channel, Chat as TLChat
from sqlalchemy.dialects.postgresql import insert as pg_insert


from utils import setup_logger, sleep_range, jitter_ms

# --- Константы/пути ---
BUCHAREST_TZ = ZoneInfo("Europe/Bucharest")
RUNTIME_DIR = Path("runtime")
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
HEARTBEAT_PATH = RUNTIME_DIR / "worker_heartbeat.json"

SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
import sys, atexit

PID_FILE = RUNTIME_DIR / "worker.pid"

if PID_FILE.exists():
    try:
        existing_pid = int(PID_FILE.read_text().strip())
    except Exception:
        existing_pid = None
    if existing_pid and psutil.pid_exists(existing_pid):
        print("⚠️  Worker already running (pid file exists). Exit.")
        os._exit(1)

PID_FILE.write_text(str(os.getpid()))

def _cleanup():
    try:
        PID_FILE.unlink(missing_ok=True)
    except Exception:
        pass
    try:
        HEARTBEAT_PATH.unlink(missing_ok=True)
    except Exception:
        pass

def _cleanup_and_exit(*_):
    _cleanup()
    os._exit(0)

atexit.register(_cleanup)

STARTED_AT = datetime.now(BUCHAREST_TZ).isoformat()

# --- ENV/CFG ---
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "research_account")
SESSION_PATH = SESSIONS_DIR / SESSION_NAME

with open("config.yaml", "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

LOG_PATH = CFG["storage"]["log_path"]
logger = setup_logger(LOG_PATH)

try:
    from db import (
        get_session, Account, User, Chat, Message, Cursor, Window,
        AccountChat, ChatBot, DirectPeer, engine
    )
except Exception as e:
    logger.error(f"Не удалось создать engine: {e}")
    sys.exit(1)

BATCH_MIN, BATCH_MAX = CFG["limits"]["batch_size_range"]
PBATCH = CFG["limits"]["pause_between_batches_sec"]
PCHAT  = CFG["limits"]["pause_between_chats_sec"]
MICRO_N = CFG["limits"]["micro_pause_every_n_msgs"]
MICRO_MS = CFG["limits"]["micro_pause_ms"]
USE_TAKEOUT = CFG["behavior"].get("use_takeout_for_bulk_exports", False)
INCLUDE_DIALOGS = CFG["behavior"].get("include_dialogs", False)

# -----------------------------
# HEARTBEAT
# -----------------------------
def write_heartbeat(*, last_action="tick", mode=None, last_chat_id=None, saved_messages_total=0):
    payload = {
        "pid": os.getpid(),
        "session": SESSION_NAME,
        "started_at": STARTED_AT,
        "last_tick": datetime.now(BUCHAREST_TZ).isoformat(),
        "last_action": last_action,
        "last_chat_id": last_chat_id,
        "saved_messages_total": saved_messages_total,
        "mode": mode,  # incremental | backfill | scan_directs | init
    }
    try:
        HEARTBEAT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

signal.signal(signal.SIGTERM, _cleanup_and_exit)
signal.signal(signal.SIGINT, _cleanup_and_exit)

# -----------------------------
# ВСПОМОГАТЕЛЬНЫЕ
# -----------------------------
def get_or_create_account(sess):
    # гарантируем Account с session_name
    # SQLModel 0.0.21: используем select(...)
    from sqlmodel import select as sql_select
    res = sess.exec(sql_select(Account).where(Account.session_name == SESSION_NAME)).first()
    if not res:
        res = Account(
            session_name=SESSION_NAME,
            display_name=SESSION_NAME,
            created_at=datetime.now(BUCHAREST_TZ).isoformat()
        )
        sess.add(res)
        sess.commit()
    return res

async def ensure_chat_record(sess, entity, account_id: int):
    ch = sess.get(Chat, entity.id)
    if not ch:
        is_group = isinstance(entity, (TLChat,)) or (getattr(entity, 'megagroup', False))
        is_channel = isinstance(entity, Channel) and not (getattr(entity, 'megagroup', False))
        ch = Chat(
            chat_id=entity.id,
            title=getattr(entity, "title", None),
            type=str(type(entity)).split("'")[1],
            is_group=bool(is_group),
            is_channel=bool(is_channel)
        )
        sess.add(ch)
        sess.commit()
    sess.merge(AccountChat(account_id=account_id, chat_id=entity.id))
    sess.commit()

def insert_message_no_conflict(sess, **vals):
    stmt = pg_insert(Message.__table__).values(**vals).on_conflict_do_nothing(index_elements=['chat_id', 'message_id'])
    sess.exec(stmt)

async def save_messages(sess, entity, msgs, account_id: int):
    chat_id = entity.id
    rows = []
    saved = 0

    for idx, m in enumerate(msgs):
        sender = await m.get_sender()
        uid = None
        if isinstance(sender, TLUser):
            uid = sender.id
            u = sess.get(User, uid)
            if not u:
                sess.add(User(
                    user_id=uid,
                    username=sender.username,
                    first_name=sender.first_name,
                    last_name=sender.last_name,
                    is_bot=bool(sender.bot)
                ))
                # не коммитим тут — один общий коммит в конце
            else:
                if u.is_bot is None and sender.bot is not None:
                    u.is_bot = bool(sender.bot)
                    sess.add(u)
            if getattr(sender, "bot", False):
                sess.merge(ChatBot(chat_id=chat_id, bot_user_id=uid))

        msg_dt_local = m.date.astimezone(BUCHAREST_TZ).isoformat()

        # копим строки для upsert
        rows.append({
            "chat_id": chat_id,
            "message_id": m.id,
            "account_id": account_id,
            "user_id": uid,
            "date": msg_dt_local,
            "text": (m.message or "").strip()
        })

        # микроджиттер (как было)
        if isinstance(MICRO_N, list) and len(MICRO_N) == 2:
            lo, hi = MICRO_N
            if (idx + 1) % random.randint(max(1, lo), max(lo, hi)) == 0:
                try:
                    jitter_ms(*MICRO_MS)
                except Exception:
                    pass

    # один батчевый upsert
    if rows:
        stmt = pg_insert(Message.__table__).values(rows)
        # конфликт по уникальному (chat_id, message_id) -> игнорируем дубликаты
        stmt = stmt.on_conflict_do_nothing(index_elements=["chat_id", "message_id"])
        res = sess.exec(stmt)
        # rowcount в Postgres показывает количество реально вставленных
        try:
            saved = res.rowcount or 0
        except Exception:
            # на всякий случай
            saved = 0

    sess.commit()
    write_heartbeat(last_action="save_messages", last_chat_id=chat_id, saved_messages_total=saved, mode="incremental")
    return saved


# -----------------------------
# СБОР ИСТОРИИ
# -----------------------------
async def fetch_incremental(client, entity, sess, account_id: int):
    cur = sess.get(Cursor, entity.id)
    if not cur:
        cur = Cursor(chat_id=entity.id, oldest_fetched_id=0, newest_fetched_id=0)
        sess.add(cur)
        sess.commit()

    min_id = (cur.newest_fetched_id or 0)
    if min_id:
        min_id += 1
    got = 0
    while True:
        write_heartbeat(last_action="loop", mode="incremental", last_chat_id=entity.id)
        limit = random.randint(BATCH_MIN, BATCH_MAX)
        try:
            hist = await client(GetHistoryRequest(
                peer=entity,
                offset_id=min_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=min_id,
                hash=0
            ))
        except errors.FloodWaitError as e:
            logger.warning(f"FLOOD_WAIT {e.seconds}s on incremental; sleeping")
            await asyncio.sleep(e.seconds + 5)
            continue

        msgs = hist.messages
        if not msgs:
            break

        got += await save_messages(sess, entity, msgs, account_id)
        cur.newest_fetched_id = max(cur.newest_fetched_id, max(m.id for m in msgs))
        sess.add(cur)
        sess.commit()
        sleep_range(*PBATCH)

    if got:
        logger.info(f"[{entity.id}] incremental saved {got}")
    return got

async def fetch_backfill(client, entity, sess, account_id: int, window=None):
    chat_id = entity.id
    cur = sess.get(Cursor, chat_id)
    if not cur:
        cur = Cursor(chat_id=chat_id, oldest_fetched_id=0, newest_fetched_id=0)
        sess.add(cur)
        sess.commit()

    offset_id = cur.oldest_fetched_id or 0
    max_id = window.max_id if window and window.max_id else 0

    total = 0
    while True:
        write_heartbeat(last_action="loop", mode="backfill", last_chat_id=chat_id)
        limit = random.randint(BATCH_MIN, BATCH_MAX)
        try:
            hist = await client(GetHistoryRequest(
                peer=entity,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=max_id,
                min_id=0,
                hash=0
            ))
        except errors.FloodWaitError as e:
            logger.warning(f"FLOOD_WAIT {e.seconds}s on backfill; sleeping")
            await asyncio.sleep(e.seconds + 5)
            continue

        msgs = hist.messages
        if not msgs:
            break

        total += await save_messages(sess, entity, msgs, account_id)
        new_oldest = min(m.id for m in msgs)

        if cur.oldest_fetched_id == 0 or new_oldest < cur.oldest_fetched_id:
            cur.oldest_fetched_id = new_oldest
        if cur.newest_fetched_id == 0:
            cur.newest_fetched_id = max(m.id for m in msgs)

        sess.add(cur)
        sess.commit()

        offset_id = new_oldest
        sleep_range(*PBATCH)

    if total:
        logger.info(f"[{chat_id}] backfill saved {total}")
    return total

async def scan_directs(client, sess, account_id: int):
    """Сканируем личные диалоги и сохраняем DirectPeer.
    ВАЖНО: сначала гарантируем наличие User, затем пишем DirectPeer — иначе FK.
    """
    if not INCLUDE_DIALOGS:
        return 0

    count = 0
    async for dlg in client.iter_dialogs():
        ent = dlg.entity
        if isinstance(ent, TLUser):
            # Сервисный Telegram (уведомления) иногда ломает семантику — пропустим
            if getattr(ent, "id", None) == 777000:
                continue

            # 1) гарантируем User
            u = sess.get(User, ent.id)
            if not u:
                u = User(
                    user_id=ent.id,
                    username=getattr(ent, "username", None),
                    first_name=getattr(ent, "first_name", None),
                    last_name=getattr(ent, "last_name", None),
                    is_bot=bool(getattr(ent, "bot", False)),
                )
                sess.add(u)
                # flush, чтобы FK на DirectPeer прошёл
                sess.flush()
            else:
                # актуализируем флаги/поля, если что-то поменялось
                changed = False
                if u.is_bot is None and getattr(ent, "bot", None) is not None:
                    u.is_bot = bool(ent.bot)
                    changed = True
                # при желании можно обновлять username/first/last
                if changed:
                    sess.add(u)
                    sess.flush()

            # 2) теперь можно писать DirectPeer
            sess.merge(DirectPeer(account_id=account_id, user_id=ent.id))
            count += 1

    sess.commit()
    if count:
        logger.info(f"Direct peers discovered: {count}")
    write_heartbeat(last_action="scan_directs", mode="scan_directs")
    return count


async def process_chat(client, chat_ref, account_id: int):
    entity = await client.get_entity(chat_ref)
    # единая сессия на чат
    with get_session() as sess:
        await ensure_chat_record(sess, entity, account_id)
        await fetch_incremental(client, entity, sess, account_id)
        await fetch_backfill(client, entity, sess, account_id)

# -----------------------------
# MAIN
# -----------------------------
async def main():
    write_heartbeat(last_action="start", mode="init")
    chats = CFG.get("chats", []) or []

    async with TelegramClient(str(SESSION_PATH), API_ID, API_HASH) as client_ctx:
        # создаём/получаем аккаунт
        with get_session() as sess:
            acc = get_or_create_account(sess)
            acc_id = acc.id

        # Личные диалоги (по желанию)
        if INCLUDE_DIALOGS:
            with get_session() as sess:
                await scan_directs(client_ctx, sess, acc_id)

        # Основной сбор
        if USE_TAKEOUT:
            async with client_ctx.takeout() as client:
                for c in chats:
                    try:
                        await process_chat(client, c, acc_id)
                    except Exception:
                        logger.exception(f"failed to process {c}")
                    sleep_range(*PCHAT)
        else:
            for c in chats:
                try:
                    await process_chat(client_ctx, c, acc_id)
                except Exception:
                    logger.exception(f"failed to process {c}")
                sleep_range(*PCHAT)

    write_heartbeat(last_action="finish", mode="done")

if __name__ == "__main__":
    asyncio.run(main())
