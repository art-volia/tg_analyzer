# dashboard_app.py — Streamlit-панель TG Analyzer (Postgres, heartbeat, безопасные ключи)
# Обновлено: уникальные key= у всех виджетов, без experimental_rerun, стабильные метрики и управление воркером

import os
import sys
import math
import json
import time
import yaml
import asyncio
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st
import pandas as pd
from dotenv import load_dotenv

# SQLModel / SQLAlchemy
from sqlalchemy import func, delete
from sqlmodel import select

# Telethon (для вкладки «Диалоги»)
from telethon import TelegramClient
from telethon.tl.types import User as TLUser, Channel, Chat as TLChat

# Проектные импорты
from db import (
    get_session, Account, User, Chat, Message, Cursor, Window,
    AccountChat, ChatBot, DirectPeer, ChatMeta, ChatTopic, ChatLanguage
)

# ------------------------------------------------------------------------------
# Константы/пути
# ------------------------------------------------------------------------------
CFG_PATH = "config.yaml"
ENV_PATH = ".env"
RUNTIME_DIR = Path("runtime")
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
HEARTBEAT_PATH = RUNTIME_DIR / "worker_heartbeat.json"


# ------------------------------------------------------------------------------
# Утилиты
# ------------------------------------------------------------------------------
def is_worker_process_running() -> bool:
    """Проверить, запущен ли worker.py как отдельный процесс."""
    try:
        out = subprocess.run(
            ["pgrep", "-f", r"python .*worker.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return out.returncode == 0
    except Exception:
        return False


def read_heartbeat():
    try:
        if HEARTBEAT_PATH.exists():
            return json.loads(HEARTBEAT_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return None


def load_cfg() -> dict:
    if not os.path.exists(CFG_PATH):
        # минимальный конфиг по умолчанию
        return {
            "chats": [],
            "limits": {
                "batch_size_range": [90, 180],
                "pause_between_batches_sec": [1.5, 3.5],
                "pause_between_chats_sec": [6, 15],
                "micro_pause_every_n_msgs": [30, 50],
                "micro_pause_ms": [200, 500],
            },
            "behavior": {
                "incremental": True,
                "use_takeout_for_bulk_exports": False,
                "warm_up_mode": True,
                "include_dialogs": False,
            },
            "storage": {
                "log_path": "logs/app.log",
            },
        }
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_cfg(cfg: dict) -> None:
    with open(CFG_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)


def load_env() -> dict:
    load_dotenv(ENV_PATH)
    return {
        "API_ID": os.getenv("API_ID", ""),
        "API_HASH": os.getenv("API_HASH", ""),
        "SESSION_NAME": os.getenv("SESSION_NAME", "research_account"),
    }


def save_env(values: dict) -> None:
    lines = []
    for k in ("API_ID", "API_HASH", "SESSION_NAME"):
        v = str(values.get(k, "")).strip()
        lines.append(f"{k}={v}\n")
    with open(ENV_PATH, "w", encoding="utf-8") as f:
        f.writelines(lines)


# ------------------------------------------------------------------------------
# Диалоги (получение списка из Telegram)
# ------------------------------------------------------------------------------
async def _fetch_dialogs(session_name: str, api_id: int, api_hash: str, limit: int = 500):
    items = []
    async with TelegramClient(session_name, api_id, api_hash) as client:
        async for dlg in client.iter_dialogs(limit=limit):
            ent = dlg.entity
            title = getattr(ent, "title", None) or getattr(ent, "username", None) or str(ent.id)
            username = getattr(ent, "username", None)
            is_group = isinstance(ent, (TLChat,)) or getattr(ent, "megagroup", False)
            is_channel = isinstance(ent, Channel) and not getattr(ent, "megagroup", False)
            items.append({
                "title": title,
                "id": ent.id,
                "username": username,
                "вид": "канал" if is_channel else ("группа" if is_group else ("личка" if isinstance(ent, TLUser) else "другое")),
            })
    return items


def fetch_dialogs_sync(session_name: str, api_id: int, api_hash: str, limit: int = 500):
    return asyncio.run(_fetch_dialogs(session_name, api_id, api_hash, limit))


# ------------------------------------------------------------------------------
# Страница
# ------------------------------------------------------------------------------
st.set_page_config(page_title="TG Analyzer Dashboard", layout="wide")
st.title("TG Analyzer — Панель управления")

tabs = st.tabs([
    "Состояние",        # 0
    "Логи",             # 1
    "Настройки",        # 2
    "Диалоги",          # 3
    "Просмотр БД",      # 4
    "Справочник чатов", # 5
    "Окна/Курсоры",     # 6
    "ENV (.env)",       # 7
    "Экспорт",          # 8
])

# ------------------------------------------------------------------------------
# 0) СОСТОЯНИЕ
# ------------------------------------------------------------------------------
with tabs[0]:
    st.subheader("Состояние воркера")
    cfg = load_cfg()
    log_path = cfg.get("storage", {}).get("log_path", "logs/app.log")

    col_run, col_stop, col_refresh = st.columns([1, 1, 1])
    with col_run:
        run_btn = st.button("▶️ Запустить worker", use_container_width=True, key="state_run")
    with col_stop:
        stop_btn = st.button("⏹ Остановить worker", use_container_width=True, key="state_stop")
    with col_refresh:
        refresh_btn = st.button("🔄 Обновить статус", use_container_width=True, key="state_refresh")

    # Кнопки управления процессом
    if run_btn:
        # мягкий рестарт
        os.system("pkill -f 'python worker.py' || true")
        time.sleep(0.3)
        cmd = f"nohup {sys.executable} worker.py > worker.log 2>&1 &"
        os.system(cmd)
        time.sleep(0.8)
        st.rerun()

    if stop_btn:
        os.system("pkill -f 'python worker.py' || true")
        time.sleep(0.6)
        st.rerun()

    if refresh_btn:
        st.rerun()

    # Статус
    status_col1, status_col2, status_col3 = st.columns([1, 1, 2])
    hb = read_heartbeat()
    proc = is_worker_process_running()

    with status_col1:
        if proc:
            st.success("Worker: RUNNING", icon="✅")
        else:
            st.error("Worker: STOPPED", icon="⛔")

    with status_col2:
        if hb:
            try:
                last_tick = datetime.fromisoformat(hb["last_tick"])
                secs_ago = (datetime.now().astimezone(last_tick.tzinfo) - last_tick).total_seconds()
                st.metric("Последний тик (сек назад)", f"{int(secs_ago)}")
            except Exception:
                st.metric("Последний тик (сек назад)", "—")
        else:
            st.metric("Последний тик (сек назад)", "—")

    with status_col3:
        if hb:
            st.caption(
                f"PID: {hb.get('pid')} • mode: {hb.get('mode')} • last_action: {hb.get('last_action')} • "
                f"last_chat_id: {hb.get('last_chat_id')} • saved(last batch): {hb.get('saved_messages_total')}"
            )
        else:
            st.caption("Нет heartbeat — запустите worker или обновите страницу.")

    # Метрики БД
    st.divider()
    try:
        with get_session() as sess:
            total_msgs = sess.exec(select(func.count()).select_from(Message)).one()
            total_chats = sess.exec(select(func.count()).select_from(Chat)).one()
            total_users = sess.exec(select(func.count()).select_from(User)).one()
            last_dt = sess.exec(select(Message.date).order_by(Message.date.desc()).limit(1)).first()
            channels_cnt = sess.exec(select(func.count()).select_from(Chat).where(Chat.is_channel == True)).one()
            groups_cnt = sess.exec(select(func.count()).select_from(Chat).where(Chat.is_group == True)).one()

        c_m1, c_m2, c_m3, c_m4 = st.columns(4)
        c_m1.metric("Сообщений", f"{int(total_msgs):,}".replace(",", " "))
        c_m2.metric("Чатов (всего)", f"{int(total_chats):,}".replace(",", " "), help=f"Каналы: {channels_cnt} • Группы: {groups_cnt}")
        c_m3.metric("Пользователей", f"{int(total_users):,}".replace(",", " "))
        c_m4.metric("Хранилище", "Postgres")

        if last_dt:
            st.caption(f"Последнее сообщение в базе: {last_dt}")
    except Exception as e:
        st.warning(f"Не удалось получить метрики БД: {e}")

    # Хвост лога воркера
    st.divider()
    st.caption("Последние строки лога воркера:")
    try:
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()[-200:]
            st.code("".join(lines) or "(лог пуст)", language="log")
        else:
            st.info(f"Лог-файл пока не создан: {log_path}")
    except Exception as e:
        st.error(f"Не удалось прочитать лог: {e}")

# ------------------------------------------------------------------------------
# 1) ЛОГИ
# ------------------------------------------------------------------------------
with tabs[1]:
    st.subheader("Логи приложения")
    cfg = load_cfg()
    log_path = cfg.get("storage", {}).get("log_path", "logs/app.log")
    follow = st.toggle("Следить в реальном времени (1сек)", value=False, key="logs_follow")
    placeholder = st.empty()

    def render_log():
        try:
            if os.path.exists(log_path):
                with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()[-500:]
                placeholder.code("".join(lines) or "(лог пуст)", language="log")
            else:
                placeholder.info(f"Лог-файл пока не создан: {log_path}")
        except Exception as e:
            placeholder.error(f"Ошибка чтения лога: {e}")

    render_log()
    if follow:
        for _ in range(30):  # ~30 секунд
            render_log()
            time.sleep(1)

# ------------------------------------------------------------------------------
# 2) НАСТРОЙКИ (config.yaml)
# ------------------------------------------------------------------------------

with tabs[2]:
    st.subheader("Настройки (config.yaml)")

    
    # === Перезагрузка формы из config.yaml ===
    def _cfg_to_state():
        cfg = load_cfg()
        st.session_state["cfg_chats"] = "\n".join(map(str, cfg.get("chats", [])))
        st.session_state["cfg_batch_lo"] = int(cfg["limits"]["batch_size_range"][0])
        st.session_state["cfg_batch_hi"] = int(cfg["limits"]["batch_size_range"][1])
        st.session_state["cfg_pbb_lo"] = float(cfg["limits"]["pause_between_batches_sec"][0])
        st.session_state["cfg_pbb_hi"] = float(cfg["limits"]["pause_between_batches_sec"][1])
        st.session_state["cfg_pchat_lo"] = float(cfg["limits"]["pause_between_chats_sec"][0])
        st.session_state["cfg_pchat_hi"] = float(cfg["limits"]["pause_between_chats_sec"][1])
        st.session_state["cfg_incremental"] = bool(cfg["behavior"].get("incremental", True))
        st.session_state["cfg_include_dialogs"] = bool(cfg["behavior"].get("include_dialogs", False))
        st.session_state["cfg_takeout"] = bool(cfg["behavior"].get("use_takeout_for_bulk_exports", False))
        st.session_state["cfg_warmup"] = bool(cfg["behavior"].get("warm_up_mode", True))

    if "cfg_state_inited" not in st.session_state:
        _cfg_to_state()
        st.session_state["cfg_state_inited"] = True

    col_reload_1, col_reload_2 = st.columns([1,2])
    with col_reload_1:
        if st.button("⟳ Обновить из config.yaml", key="btn_reload_cfg",
                     help="Перечитать файл и перезаполнить поля формы из файла. Несохранённые изменения в форме будут потеряны."):
            _cfg_to_state()
            st.rerun()
    with col_reload_2:
        try:
            from pathlib import Path as _P
            from datetime import datetime
            ts = datetime.fromtimestamp(_P(CFG_PATH).stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            st.caption(f"Файл: {CFG_PATH} • обновлён: {ts}")
        except Exception:
            st.caption("Файл config.yaml не найден")
# читаем cfg и mtime
    cfg = load_cfg()
    try:
        cfg_mtime = Path(CFG_PATH).stat().st_mtime
    except Exception:
        cfg_mtime = 0
    nonce = str(int(cfg_mtime))  # используем как суффикс key

    # Кнопка «обновить из файла» — просто перерендер
    row_reload_1, row_reload_2 = st.columns([1,2])
    with row_reload_1:
        if st.button("⟳ Обновить из config.yaml", key=f"btn_reload_cfg_{nonce}",
                     help="Перечитать файл и перезаполнить поля формы из файла. Несохранённые правки на экране перезатрутся."):
            st.rerun()
    with row_reload_2:
        try:
            ts = datetime.fromtimestamp(Path(CFG_PATH).stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            st.caption(f"Файл: {CFG_PATH} • обновлён: {ts}")
        except Exception:
            st.caption("Файл config.yaml не найден")

    st.write("**Чаты** (список @username или числовых ID):")
    chats = cfg.get("chats", []) or []
    chats_text = st.text_area("chats (по одному в строке)",
        value="\n".join(map(str, chats)),
        height=140,
        key=f"chats_text_{nonce}",
        help=(
            "Список целевых чатов/каналов. Допустимы: @username "
            "(публичные) или числовой chat_id (для приватных, где аккаунт "
            "уже состоит). Один пункт — одна строка. После изменения "
            "сохраните config.yaml и перезапустите воркер."
        )
    )

    st.write("**Лимиты и паузы**:")
    col_a, col_b = st.columns(2)
    with col_a:
        batch_lo = st.number_input(
            "batch min", 10, 1000,
            value=int(cfg["limits"]["batch_size_range"][0]),
            key=f"batch_lo_{nonce}",
            help="Минимальный размер пачки сообщений на один запрос истории."
        )
        pbb_lo = st.number_input(
            "pause_between_batches min (сек)", 0.0, 60.0,
            value=float(cfg["limits"]["pause_between_batches_sec"][0]),
            key=f"pbb_lo_{nonce}",
            help="Минимальная пауза между запросами истории (в секундах)."
        )
        pchat_lo = st.number_input(
            "pause_between_chats min (сек)", 0.0, 120.0,
            value=float(cfg["limits"]["pause_between_chats_sec"][0]),
            key=f"pchat_lo_{nonce}",
            help="Минимальная пауза при переключении между чатами."
        )
    with col_b:
        batch_hi = st.number_input(
            "batch max", 10, 2000,
            value=int(cfg["limits"]["batch_size_range"][1]),
            key=f"batch_hi_{nonce}",
            help="Максимальный размер пачки сообщений."
        )
        pbb_hi = st.number_input(
            "pause_between_batches max (сек)", 0.0, 120.0,
            value=float(cfg["limits"]["pause_between_batches_sec"][1]),
            key=f"pbb_hi_{nonce}",
            help="Максимальная пауза между запросами истории (в секундах)."
        )
        pchat_hi = st.number_input(
            "pause_between_chats max (сек)", 0.0, 300.0,
            value=float(cfg["limits"]["pause_between_chats_sec"][1]),
            key=f"pchat_hi_{nonce}",
            help="Максимальная пауза при переключении между чатами."
        )

    st.write("**Поведение**:")
    col_c, col_d = st.columns(2)
    with col_c:
        incremental = st.toggle(
            "Инкрементальный сбор",
            value=bool(cfg["behavior"].get("incremental", True)),
            key=f"incremental_{nonce}",
            help="Обновление только новых сообщений."
        )
        include_dialogs = st.toggle(
            "Собирать личные диалоги",
            value=bool(cfg["behavior"].get("include_dialogs", False)),
            key=f"include_dialogs_{nonce}",
            help="Сохранять собеседников из личных диалогов."
        )
    with col_d:
        takeout = st.toggle(
            "Использовать takeout для бэкапа",
            value=bool(cfg["behavior"].get("use_takeout_for_bulk_exports", False)),
            key=f"takeout_{nonce}",
            help="Telegram Takeout для больших выгрузок."
        )
        warmup = st.toggle(
            "Режим прогрева",
            value=bool(cfg["behavior"].get("warm_up_mode", True)),
            key=f"warmup_{nonce}",
            help="Щадящий режим в первые дни."
        )

    if st.button("💾 Сохранить config.yaml", type="primary", key=f"save_cfg_{nonce}",
                 help="Записывает изменения в файл config.yaml."):
        new_cfg = dict(cfg)  # копия
        new_cfg.setdefault("limits", dict(cfg.get("limits", {})))
        new_cfg.setdefault("behavior", dict(cfg.get("behavior", {})))

        new_cfg["chats"] = [x.strip() for x in chats_text.splitlines() if x.strip()]
        new_cfg["limits"]["batch_size_range"] = [int(batch_lo), int(batch_hi)]
        new_cfg["limits"]["pause_between_batches_sec"] = [float(pbb_lo), float(pbb_hi)]
        new_cfg["limits"]["pause_between_chats_sec"] = [float(pchat_lo), float(pchat_hi)]
        new_cfg["behavior"]["incremental"] = bool(incremental)
        new_cfg["behavior"]["include_dialogs"] = bool(include_dialogs)
        new_cfg["behavior"]["use_takeout_for_bulk_exports"] = bool(takeout)
        new_cfg["behavior"]["warm_up_mode"] = bool(warmup)
        save_cfg(new_cfg)
        st.success("Сохранено. Нажмите «⟳ Обновить из config.yaml», чтобы перечитать файл.")
with tabs[3]:
    st.subheader("Диалоги (чаты и каналы из Telegram)")

    env_vals = load_env()
    try:
        api_id = int(env_vals.get("API_ID", "0") or "0")
    except Exception:
        api_id = 0
    api_hash = env_vals.get("API_HASH", "")
    session_name = env_vals.get("SESSION_NAME", "research_account")

    if not api_id or not api_hash:
        st.warning("Сначала заполните API_ID и API_HASH во вкладке ‘ENV (.env)’.")
    else:
        col1, col2 = st.columns([1, 1])
        with col1:
            limit = st.number_input(
                "Сколько диалогов подтянуть",
                min_value=50, max_value=2000, value=500, step=50,
                key="dlg_limit",
            )
        with col2:
            go = st.button("🔄 Обновить список чатов из Telegram", use_container_width=True, key="dlg_refresh")

        if go:
            with st.spinner("Подключаемся к Telegram и читаем диалоги..."):
                try:
                    dialogs = fetch_dialogs_sync(session_name, api_id, api_hash, limit=int(limit))
                    st.session_state["dialogs_cache"] = dialogs
                    st.success(f"Найдено диалогов: {len(dialogs)}")
                except Exception as e:
                    st.error(f"Ошибка при получении диалогов: {e}")

        dialogs = st.session_state.get("dialogs_cache", [])
        if dialogs:
            st.caption("Отметьте галочками, что добавить в config.yaml → кнопка ниже")
            df = pd.DataFrame(dialogs)
            options = [f"{row['вид']} | {row['title']} | id={row['id']} | @{row['username'] or '-'}" for _, row in df.iterrows()]
            selected = st.multiselect("Выберите чаты", options=options, default=[], key="dlg_selected")

            if st.button("➕ Добавить выбранные в config.yaml", type="primary", key="dlg_add_to_cfg"):
                to_add = []
                for item in selected:
                    parts = item.split("|")
                    id_part = [p for p in parts if "id=" in p][0].strip()
                    id_val = int(id_part.replace("id=", "").strip())
                    uname_part = [p for p in parts if "@" in p][-1].strip()
                    uname = uname_part[1:] if uname_part.startswith("@") else None
                    if uname and uname != "-":
                        to_add.append(f"@{uname}")
                    else:
                        to_add.append(id_val)
                cfg = load_cfg()
                existing = cfg.get("chats", []) or []
                for x in to_add:
                    if x and x not in existing:
                        existing.append(x)
                cfg["chats"] = existing
                save_cfg(cfg)
                st.success(f"Добавлено в config.yaml: {len(to_add)}. Перезапустите воркер во вкладке ‘Состояние’.")
        else:
            st.info("Список диалогов пуст. Нажмите ‘Обновить список чатов из Telegram’.")

# ------------------------------------------------------------------------------
# 4) ПРОСМОТР БД (сообщения)
# ------------------------------------------------------------------------------
with tabs[4]:
    st.subheader("Просмотр БД: сообщения по чатам")

    # Безопасная инициализация пагинации
    st.session_state.setdefault("browser_page", 1)

    # Фильтры
    col_top1, col_top2, col_top3, col_top4 = st.columns([1, 1, 1, 1])
    with col_top1:
        kind = st.selectbox("Тип", ["любой", "канал", "группа", "личка"], key="db_kind")
    with col_top2:
        date_from = st.date_input("С даты", value=None, key="db_date_from")
    with col_top3:
        date_to = st.date_input("По дату", value=None, key="db_date_to")
    with col_top4:
        per_page = st.number_input("На страницу", min_value=10, max_value=500, value=100, step=10, key="db_per_page")

    q = st.text_input("Поиск по тексту (LIKE, без регистра)", key="db_text_query")

    with get_session() as sess:
        chats_q = select(Chat).order_by(Chat.title)
        chats = sess.exec(chats_q).all()
    chat_map = {f"{c.title or c.chat_id} (id={c.chat_id})": c.chat_id for c in chats}
    chat_label = st.selectbox("Выберите чат", ["(не выбран)"] + list(chat_map.keys()), key="db_chat_select")
    chat_id = chat_map.get(chat_label) if chat_label != "(не выбран)" else None

    st.divider()
    page = st.number_input("Страница", min_value=1, value=int(st.session_state.get("browser_page", 1)), step=1, key="db_page")
    if st.button("Применить фильтры", key="db_apply_filters"):
        st.session_state["browser_page"] = int(page)

    # WHERE-условия
    where = []
    if chat_id:
        where.append(Message.chat_id == chat_id)

    if kind != "любой":
        if kind == "канал":
            where.append(Chat.is_channel == True)
        elif kind == "группа":
            where.append(Chat.is_group == True)
        elif kind == "личка":
            where.append((Chat.is_channel == False) & (Chat.is_group == False))

    def to_iso(d):
        return datetime(d.year, d.month, d.day)

    if date_from:
        where.append(Message.date >= to_iso(date_from).isoformat())
    if date_to:
        where.append(Message.date < (to_iso(date_to) + timedelta(days=1)).isoformat())

    if q:
        where.append(func.lower(Message.text).like(f"%{q.lower()}%"))

    offset = (int(st.session_state.get("browser_page", 1)) - 1) * int(per_page)

    with get_session() as sess:
        base = select(
            Message.message_id,
            Message.chat_id,
            Message.user_id,
            Message.date,
            Message.text
        ).join(Chat, Chat.chat_id == Message.chat_id)

        if where:
            from sqlmodel import and_  # локальный импорт
            base = base.where(and_(*where))

        total = sess.exec(select(func.count()).select_from(base.subquery())).one()
        page_stmt = base.order_by(Message.date.desc()).limit(int(per_page)).offset(int(offset))
        rows = sess.exec(page_stmt).all()

        # подтянуть имена пользователей
        uids = [r.user_id for r in rows if r.user_id is not None]
        users = {}
        if uids:
            urows = sess.exec(select(User.user_id, User.username, User.first_name, User.last_name).where(User.user_id.in_(uids))).all()
            for uid, un, fn, ln in urows:
                users[uid] = un or " ".join(filter(None, [fn, ln])) or str(uid)

    st.caption(f"Найдено: {total} сообщений. Страниц: {max(1, math.ceil(total/int(per_page)))}")

    if rows:
        df = pd.DataFrame([
            {
                "дата": r.date,
                "пользователь": users.get(r.user_id, r.user_id),
                "chat_id": r.chat_id,
                "msg_id": r.message_id,
                "текст": r.text,
            }
            for r in rows
        ])
        st.dataframe(df, use_container_width=True)

        c1, c2, c3 = st.columns([1, 1, 1])
        total_pages = max(1, math.ceil(total / int(per_page)))
        with c1:
            if st.button("« Назад", disabled=int(st.session_state["browser_page"]) <= 1, key="db_prev"):
                st.session_state["browser_page"] -= 1
                st.rerun()
        with c2:
            st.write(f"Стр. {st.session_state['browser_page']} из {total_pages}")
        with c3:
            if st.button("Вперёд »", disabled=int(st.session_state["browser_page"]) >= total_pages, key="db_next"):
                pass
            else:
                if int(st.session_state["browser_page"]) < total_pages:
                    st.session_state["browser_page"] += 1
                    st.rerun()
    else:
        st.info("Нет данных по выбранным фильтрам.")

# ------------------------------------------------------------------------------
# 5) СПРАВОЧНИК ЧАТОВ (БД-мета)
# ------------------------------------------------------------------------------
with tabs[5]:
    st.subheader("Справочник чатов")

    # Фильтры
    col1, col2, col3 = st.columns(3)
    with col1:
        f_type = st.selectbox("Тип", ["любой", "канал", "группа", "личка"], key="dict_type")
    with col2:
        f_country = st.text_input("Страна", key="dict_country")
    with col3:
        f_lang = st.text_input("Язык", key="dict_lang")

    with get_session() as sess:
        q = select(Chat)
        if f_type != "любой":
            if f_type == "канал":
                q = q.where(Chat.is_channel == True)
            elif f_type == "группа":
                q = q.where(Chat.is_group == True)
            elif f_type == "личка":
                q = q.where((Chat.is_channel == False) & (Chat.is_group == False))

        chats = sess.exec(q).all()

        rows = []
        for c in chats:
            meta = sess.exec(select(ChatMeta).where(ChatMeta.chat_id == c.chat_id)).first()
            topics = sess.exec(select(ChatTopic.topic).where(ChatTopic.chat_id == c.chat_id)).all()
            langs = sess.exec(select(ChatLanguage.language).where(ChatLanguage.chat_id == c.chat_id)).all()

            country = meta.country if meta else ""
            topics_str = ", ".join([t[0] for t in topics]) if topics else ""
            langs_str = ", ".join([l[0] for l in langs]) if langs else ""

            if f_country and f_country.lower() not in country.lower():
                continue
            if f_lang and (not langs_str or f_lang.lower() not in langs_str.lower()):
                continue

            rows.append({
                "chat_id": c.chat_id,
                "title": c.title,
                "тип": "канал" if c.is_channel else ("группа" if c.is_group else "личка"),
                "страна": country,
                "темы": topics_str,
                "языки": langs_str
            })

        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)

            st.divider()
            st.subheader("Редактирование свойств чата")

            # список label: "chat_id — title"
            ids = [r["chat_id"] for r in rows]
            id_to_title = {r["chat_id"]: r.get("title") for r in rows}
            options = [f'{cid} — {id_to_title.get(cid) or ""}'.strip() for cid in ids]

            sel_label = st.selectbox("Выберите chat_id", ["-"] + options, key="dict_chat_id")
            if sel_label != "-":
                # распарсим chat_id из "123456 — Title"
                try:
                    selected_id = int(sel_label.split("—", 1)[0].strip())
                except Exception:
                    selected_id = None

                if selected_id is not None:
                    chat = sess.get(Chat, selected_id)
                    if chat:
                        meta = sess.exec(select(ChatMeta).where(ChatMeta.chat_id == chat.chat_id)).first()
                        topics = sess.exec(select(ChatTopic.topic).where(ChatTopic.chat_id == chat.chat_id)).all()
                        langs  = sess.exec(select(ChatLanguage.language).where(ChatLanguage.chat_id == chat.chat_id)).all()

                        in_country = st.text_input("Страна", value=(meta.country if meta else ""), key="dict_edit_country")
                        in_topics  = st.text_input("Темы (через запятую)", value=", ".join([t[0] for t in topics]), key="dict_edit_topics")
                        in_langs   = st.text_input("Языки (через запятую)", value=", ".join([l[0] for l in langs]), key="dict_edit_langs")

                        if st.button("💾 Сохранить изменения", key="dict_save"):
                            # 1) country (upsert)
                            if not meta:
                                meta = ChatMeta(chat_id=chat.chat_id, country=in_country.strip() or None)
                            else:
                                meta.country = in_country.strip() or None
                            sess.add(meta)
                            sess.commit()

                            # 2) topics — сначала удаляем все, затем вставляем новые
                            sess.exec(delete(ChatTopic).where(ChatTopic.chat_id == chat.chat_id))
                            for t in [x.strip() for x in in_topics.split(",") if x.strip()]:
                                sess.add(ChatTopic(chat_id=chat.chat_id, topic=t))
                            sess.commit()

                            # 3) languages — аналогично
                            sess.exec(delete(ChatLanguage).where(ChatLanguage.chat_id == chat.chat_id))
                            for l in [x.strip() for x in in_langs.split(",") if x.strip()]:
                                sess.add(ChatLanguage(chat_id=chat.chat_id, language=l))
                            sess.commit()

                            st.success("Сохранено в БД.")
                            st.rerun()
        else:
            st.info("Нет чатов по заданным фильтрам или база пуста.")

# ------------------------------------------------------------------------------
# 6) ОКНА/КУРСОРЫ
# ------------------------------------------------------------------------------
with tabs[6]:
    st.subheader("Окна/Курсоры (техданные)")

    with get_session() as sess:
        cursors = sess.exec(select(Cursor)).all()
        windows = sess.exec(select(Window)).all()

    if cursors:
        st.write("Cursor:")
        df_c = pd.DataFrame([{
            "chat_id": c.chat_id,
            "oldest_id": c.oldest_fetched_id,
            "newest_id": c.newest_fetched_id,
        } for c in cursors])
        st.dataframe(df_c, use_container_width=True)
    else:
        st.info("Cursor пусто")

    st.divider()

    if windows:
        st.write("Window:")
        df_w = pd.DataFrame([{
            "id": w.id, "chat_id": w.chat_id, "min_id": w.min_id, "max_id": w.max_id
        } for w in windows])
        st.dataframe(df_w, use_container_width=True)
    else:
        st.info("Window пусто")

# ------------------------------------------------------------------------------
# 7) ENV (.env)
# ------------------------------------------------------------------------------
with tabs[7]:
    st.subheader("ENV (.env)")
    env_vals = load_env()

    api_id = st.text_input("API_ID", value=str(env_vals.get("API_ID", "")), key="env_api_id")
    api_hash = st.text_input("API_HASH", value=env_vals.get("API_HASH", ""), key="env_api_hash")
    session_name = st.text_input("SESSION_NAME", value=env_vals.get("SESSION_NAME", "research_account"), key="env_session_name")

    if st.button("💾 Сохранить .env", type="primary", key="env_save"):
        save_env({"API_ID": api_id, "API_HASH": api_hash, "SESSION_NAME": session_name})
        st.success(".env сохранён. Перезапустите панель или воркер, чтобы применить.")

# ------------------------------------------------------------------------------
# 8) ЭКСПОРТ (CSV)
# ------------------------------------------------------------------------------
with tabs[8]:
    st.subheader("Экспорт сообщений в CSV")

    limit = st.number_input("Сколько строк выгрузить (последние)", min_value=10, max_value=100000, value=1000, step=10, key="export_limit")
    if st.button("Скачать CSV", key="export_btn"):
        with get_session() as sess:
            q = select(Message).order_by(Message.date.desc()).limit(int(limit))
            rows = sess.exec(q).all()
        df = pd.DataFrame([
            {
                "chat_id": r.chat_id,
                "message_id": r.message_id,
                "user_id": r.user_id,
                "date": r.date,
                "text": r.text,
            } for r in rows
        ])
        st.download_button(
            label="Скачать messages.csv",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="messages.csv",
            mime="text/csv",
            key="export_download_btn",
            help="CSV в UTF-8. Открой в Excel/Google Sheets.",
        )
