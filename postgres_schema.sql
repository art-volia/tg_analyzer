CREATE TABLE IF NOT EXISTS account (
  id BIGSERIAL PRIMARY KEY,
  session_name TEXT NOT NULL,
  display_name TEXT,
  description TEXT,
  roles TEXT,
  created_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS users (
  user_id      BIGINT PRIMARY KEY,
  username     TEXT,
  first_name   TEXT,
  last_name    TEXT,
  last_seen_at TIMESTAMPTZ,
  is_bot       BOOLEAN
);
CREATE TABLE IF NOT EXISTS chats (
  chat_id BIGINT PRIMARY KEY,
  title   TEXT,
  type    TEXT,
  is_group BOOLEAN,
  is_channel BOOLEAN,
  country TEXT,
  topics  TEXT,
  languages TEXT
);
CREATE TABLE IF NOT EXISTS accountchat (
  account_id BIGINT,
  chat_id BIGINT,
  PRIMARY KEY (account_id, chat_id)
);
CREATE TABLE IF NOT EXISTS chatbot (
  chat_id BIGINT,
  bot_user_id BIGINT,
  PRIMARY KEY (chat_id, bot_user_id)
);
CREATE TABLE IF NOT EXISTS directpeer (
  account_id BIGINT,
  user_id BIGINT,
  PRIMARY KEY (account_id, user_id)
);
CREATE TABLE IF NOT EXISTS messages (
  chat_id     BIGINT NOT NULL,
  message_id  BIGINT NOT NULL,
  account_id  BIGINT,
  user_id     BIGINT,
  date        TIMESTAMPTZ NOT NULL,
  text        TEXT,
  PRIMARY KEY (chat_id, message_id)
);
CREATE TABLE IF NOT EXISTS cursor (
  chat_id            BIGINT PRIMARY KEY,
  oldest_fetched_id  BIGINT NOT NULL,
  newest_fetched_id  BIGINT NOT NULL,
  updated_at         TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS window (
  id BIGSERIAL PRIMARY KEY,
  chat_id BIGINT NOT NULL,
  min_id BIGINT,
  max_id BIGINT,
  status TEXT DEFAULT 'queued',
  taken_by TEXT,
  note TEXT
);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date DESC);
CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id);
CREATE INDEX IF NOT EXISTS idx_messages_chat_date ON messages(chat_id, date DESC);
