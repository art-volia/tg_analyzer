import os, yaml, time, signal, subprocess
import streamlit as st
from sqlmodel import Session, create_engine, select
from db import Account, User, Chat, Message, Cursor, Window, AccountChat, ChatBot, DirectPeer, get_session

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
if ADMIN_TOKEN:
    st.session_state.setdefault("auth_ok", False)
    if not st.session_state["auth_ok"]:
        t = st.text_input("Введите пароль для панели", type="password")
        if st.button("Войти") and t == ADMIN_TOKEN:
            st.session_state["auth_ok"] = True
        if not st.session_state["auth_ok"]:
            st.stop()

CFG_PATH = "config.yaml"
def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

cfg = load_cfg()
DB_PATH = cfg["storage"]["db_path"]
DB_URL = os.getenv("DATABASE_URL", "").strip() or None
engine = create_engine(DB_URL if DB_URL else f"sqlite:///{DB_PATH}")

st.set_page_config(page_title="TG Analyzer — Расширенная панель", layout="wide")
st.title("TG Analyzer — Аккаунты и Справочник")

tabs = st.tabs(["Аккаунты", "Справочник каналов"])

with tabs[0]:
    st.subheader("Аккаунты анализаторов")
    with Session(engine) as s:
        accounts = s.exec(select(Account)).all()
    if not accounts:
        st.info("Аккаунтов пока нет. Запусти воркер (он создаст запись по SESSION_NAME).")
    else:
        mapping = {f"{a.id}: {a.display_name or a.session_name}": a.id for a in accounts}
        key = st.selectbox("Выбери аккаунт", list(mapping.keys()))
        sel_id = mapping[key]
        with Session(engine) as s:
            acc = s.get(Account, sel_id)
        st.markdown("### Профиль")
        dn = st.text_input("Отображаемое имя", value=acc.display_name or "")
        desc = st.text_area("Описание", value=acc.description or "", height=120)
        roles_all = ["analyzer","commenter","dm_writer"]
        chosen = st.multiselect("Роли", roles_all, default=(acc.roles.split(',') if acc.roles else []))
        if st.button("💾 Сохранить профиль"):
            with Session(engine) as s:
                acc = s.get(Account, sel_id)
                acc.display_name = dn.strip() or None
                acc.description = desc.strip() or None
                acc.roles = ",".join(chosen) if chosen else None
                s.add(acc); s.commit()
            st.success("Сохранено")
        st.markdown("---")
        st.markdown("### Чаты аккаунта")
        with Session(engine) as s:
            rows = s.exec(select(AccountChat, Chat).where(AccountChat.account_id==sel_id).join(Chat, Chat.chat_id==AccountChat.chat_id)).all()
        st.dataframe([{
            "chat_id": ch.chat_id,
            "title": ch.title,
            "type": ch.type,
            "вид": "канал" if ch.is_channel else ("группа" if ch.is_group else ch.type),
            "country": ch.country,
            "topics": ch.topics,
            "languages": ch.languages
        } for ac, ch in rows], use_container_width=True)
        st.markdown("### Личные переписки (Direct)")
        with Session(engine) as s:
            dms = s.exec(select(DirectPeer, User).where(DirectPeer.account_id==sel_id).join(User, User.user_id==DirectPeer.user_id)).all()
        st.dataframe([{"user_id": u.user_id, "username": u.username, "first_name": u.first_name, "last_name": u.last_name} for dp, u in dms], use_container_width=True)

with tabs[1]:
    st.subheader("Справочник каналов/групп")
    with Session(engine) as s:
        chats = s.exec(select(Chat)).all()
    for ch in chats[:500]:
        with st.expander(f"{ch.title or ch.chat_id} — {('канал' if ch.is_channel else ('группа' if ch.is_group else ch.type))}"):
            st.write(f"chat_id: `{ch.chat_id}`  | type: `{ch.type}`")
            cval = st.text_input(f"Страна для {ch.chat_id}", value=ch.country or "", key=f"country_{ch.chat_id}")
            tval = st.text_input(f"Темы (CSV) для {ch.chat_id}", value=ch.topics or "", key=f"topics_{ch.chat_id}")
            lval = st.text_input(f"Языки (CSV) для {ch.chat_id}", value=ch.languages or "", key=f"lang_{ch.chat_id}")
            with Session(engine) as s:
                bots = s.exec(select(ChatBot, User).where(ChatBot.chat_id==ch.chat_id).join(User, User.user_id==ChatBot.bot_user_id)).all()
            if bots:
                st.write("Боты в чате:", [{"user_id": u.user_id, "username": u.username} for cb, u in bots])
            if st.button(f"💾 Сохранить {ch.chat_id}"):
                with Session(engine) as s:
                    chx = s.get(Chat, ch.chat_id)
                    chx.country = cval.strip() or None
                    chx.topics = tval.strip() or None
                    chx.languages = lval.strip() or None
                    s.add(chx); s.commit()
                st.success("Сохранено")
