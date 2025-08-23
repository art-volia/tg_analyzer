# db.py — SQLModel models for Postgres (BIGINT ids), корректное объявление PK/FK

from typing import Optional
import os
from dotenv import load_dotenv

from sqlmodel import SQLModel, Field, Session, create_engine
from sqlalchemy import Column, BigInteger, String, Boolean, ForeignKey, Text, UniqueConstraint, Index

load_dotenv()

# --- Engine / DSN ---
DB_USER = os.getenv("DB_USER", "tgadmin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "tg_analyzer")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)

from sqlmodel import SQLModel
SQLModel.metadata.clear() 

# --- Models ---

class Account(SQLModel, table=True):
    # локальный PK
    id: Optional[int] = Field(default=None, primary_key=True, index=True)
    session_name: str = Field(sa_column=Column(String(255), unique=True, nullable=False, index=True))
    display_name: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    created_at: Optional[str] = Field(default=None, sa_column=Column(String(64)))


class User(SQLModel, table=True):
    # Telegram user_id может быть > 2^31 ⇒ BIGINT
    user_id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    username: Optional[str]   = Field(default=None, sa_column=Column(String(255), index=True))
    first_name: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    last_name: Optional[str]  = Field(default=None, sa_column=Column(String(255)))
    is_bot: Optional[bool]    = Field(default=None, sa_column=Column(Boolean))


class Chat(SQLModel, table=True):
    # Telegram chat_id ⇒ BIGINT
    chat_id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    title: Optional[str] = Field(default=None, sa_column=Column(String(512), index=True))
    type: Optional[str]  = Field(default=None, sa_column=Column(String(64)))
    is_group: Optional[bool]   = Field(default=False, sa_column=Column(Boolean, index=True))
    is_channel: Optional[bool] = Field(default=False, sa_column=Column(Boolean, index=True))


class Message(SQLModel, table=True):
    # удобнее иметь surrogate PK, а уникальность обеспечить на (chat_id, message_id)
    id: Optional[int] = Field(default=None, primary_key=True)

    chat_id: int = Field(sa_column=Column(BigInteger, ForeignKey("chat.chat_id", ondelete="CASCADE"), nullable=False, index=True))
    message_id: int = Field(sa_column=Column(BigInteger, nullable=False))
    account_id: Optional[int] = Field(default=None, sa_column=Column(ForeignKey("account.id", ondelete="SET NULL")))
    user_id: Optional[int] = Field(default=None, sa_column=Column(BigInteger, ForeignKey("user.user_id", ondelete="SET NULL"), index=True))
    date: Optional[str] = Field(default=None, sa_column=Column(String(40), index=True))
    text: Optional[str] = Field(default=None, sa_column=Column(Text))

    __table_args__ = (
        UniqueConstraint("chat_id", "message_id", name="uq_message_chat_msg"),
    )


class Cursor(SQLModel, table=True):
    chat_id: int = Field(sa_column=Column(BigInteger, ForeignKey("chat.chat_id", ondelete="CASCADE"), primary_key=True))
    oldest_fetched_id: int = Field(default=0, sa_column=Column(BigInteger))
    newest_fetched_id: int = Field(default=0, sa_column=Column(BigInteger))


class Window(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    chat_id: int = Field(sa_column=Column(BigInteger, ForeignKey("chat.chat_id", ondelete="CASCADE"), nullable=False, index=True))
    min_id: Optional[int] = Field(default=None, sa_column=Column(BigInteger))
    max_id: Optional[int] = Field(default=None, sa_column=Column(BigInteger))


class AccountChat(SQLModel, table=True):
    account_id: int = Field(sa_column=Column(ForeignKey("account.id", ondelete="CASCADE"), primary_key=True))
    chat_id: int = Field(sa_column=Column(BigInteger, ForeignKey("chat.chat_id", ondelete="CASCADE"), primary_key=True))


class ChatBot(SQLModel, table=True):
    chat_id: int = Field(sa_column=Column(BigInteger, ForeignKey("chat.chat_id", ondelete="CASCADE"), primary_key=True))
    bot_user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("user.user_id", ondelete="CASCADE"), primary_key=True))


class DirectPeer(SQLModel, table=True):
    account_id: int = Field(sa_column=Column(ForeignKey("account.id", ondelete="CASCADE"), primary_key=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("user.user_id", ondelete="CASCADE"), primary_key=True))


class ChatMeta(SQLModel, table=True):
    chat_id: int = Field(sa_column=Column(BigInteger, ForeignKey("chat.chat_id", ondelete="CASCADE"), primary_key=True))
    country: Optional[str] = Field(default=None, sa_column=Column(String(64)))


class ChatTopic(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    chat_id: int = Field(sa_column=Column(BigInteger, ForeignKey("chat.chat_id", ondelete="CASCADE"), nullable=False, index=True))
    topic: str = Field(sa_column=Column(String(128), index=True))


class ChatLanguage(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    chat_id: int = Field(sa_column=Column(BigInteger, ForeignKey("chat.chat_id", ondelete="CASCADE"), nullable=False, index=True))
    language: str = Field(sa_column=Column(String(64), index=True))




# --- Helpers ---
def get_session():
    return Session(engine)

def create_all():
    SQLModel.metadata.create_all(engine)

def drop_all():
    SQLModel.metadata.drop_all(engine)
