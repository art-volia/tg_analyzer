#!/usr/bin/env python
import sqlite3, sys
if len(sys.argv) < 2:
    print("Usage: migrate_v2.py data/db.sqlite")
    sys.exit(1)
db = sys.argv[1]
conn = sqlite3.connect(db)
cur = conn.cursor()
def has_table(name):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",(name,))
    return cur.fetchone() is not None
def has_col(table, col):
    cur.execute(f"PRAGMA table_info({table})"); cols = [r[1] for r in cur.fetchall()]; return col in cols
if not has_table("account"):
    cur.execute("CREATE TABLE account (id INTEGER PRIMARY KEY, session_name TEXT, display_name TEXT, description TEXT, roles TEXT, created_at TEXT)")
if not has_table("accountchat"):
    cur.execute("CREATE TABLE accountchat (account_id INTEGER, chat_id INTEGER, PRIMARY KEY(account_id, chat_id))")
if not has_table("chatbot"):
    cur.execute("CREATE TABLE chatbot (chat_id INTEGER, bot_user_id INTEGER, PRIMARY KEY(chat_id, bot_user_id))")
if not has_table("directpeer"):
    cur.execute("CREATE TABLE directpeer (account_id INTEGER, user_id INTEGER, PRIMARY KEY(account_id, user_id))")
if has_table("chat"):
    for c in ("is_group","is_channel","country","topics","languages"):
        if not has_col("chat", c): cur.execute(f"ALTER TABLE chat ADD COLUMN {c}")
if has_table("user"):
    if not has_col("user","is_bot"): cur.execute("ALTER TABLE user ADD COLUMN is_bot")
if has_table("message"):
    if not has_col("message","account_id"): cur.execute("ALTER TABLE message ADD COLUMN account_id")
conn.commit(); conn.close(); print("Migration v2 complete.")
