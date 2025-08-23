#!/usr/bin/env python
from sqlmodel import Session, select, create_engine, SQLModel
from db import User, Chat, Message, Cursor, Window, Account, AccountChat, ChatBot, DirectPeer
import sys

if len(sys.argv) < 3:
    print("Usage: migrate.py <sqlite_path> <database_url>")
    sys.exit(1)

sqlite_path = sys.argv[1]
target_url = sys.argv[2]

src = create_engine(f"sqlite:///{sqlite_path}", connect_args={"check_same_thread": False})
dst = create_engine(target_url)
SQLModel.metadata.create_all(dst)

def copy_table(session_src, session_dst, model, batch=5000):
    offset = 0
    while True:
        rows = session_src.exec(select(model).offset(offset).limit(batch)).all()
        if not rows: break
        for r in rows:
            session_dst.merge(r)
        session_dst.commit()
        offset += len(rows)
        print(f"Copied {offset} rows of {model.__name__}")

with Session(src) as ssrc, Session(dst) as sdst:
    for model in (Account, User, Chat, Message, Cursor, Window, AccountChat, ChatBot, DirectPeer):
        copy_table(ssrc, sdst, model)
    print("Done.")
