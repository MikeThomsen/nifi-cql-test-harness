-- Chat metadata: one row per chat, supports any number of participants (3+).
-- participants holds member user_ids only; each member's SSO identity
-- (authenticator, principal, joined_at) lives on chats_by_user.
CREATE TABLE chats (
    chat_id      uuid,
    created_at   timestamp,
    chat_name    text,
    participants set<uuid>,          -- 3 or more member user_ids
    PRIMARY KEY (chat_id)
);

-- Reverse lookup: which chats a given user belongs to
CREATE TABLE chats_by_user (
    user_id       uuid,
    chat_id       uuid,
    joined_at     timestamp,
    authenticator text,             -- SSO scheme that authenticated this member
    principal     text,             -- external login identifier (not a UUID)
    PRIMARY KEY (user_id, chat_id)
);

-- Messages, partitioned by (chat, day) so partitions stay bounded
-- and spread evenly across the cluster. Clustering by hour then time
-- makes date + hour-of-day range queries efficient.
CREATE TABLE messages_by_chat (
    chat_id    uuid,
    msg_date   date,                 -- day bucket, part of the partition key
    msg_hour   tinyint,              -- 0-23, clustering column for hour queries
    sent_at    timestamp,            -- final clustering column: identifies the row
    sender_id  uuid,
    body       text,
    PRIMARY KEY ((chat_id), msg_date, msg_hour, sent_at)
) WITH CLUSTERING ORDER BY (msg_date ASC, msg_hour ASC, sent_at ASC)
  AND compaction = { 'class': 'TimeWindowCompactionStrategy',
                     'compaction_window_unit': 'DAYS',
                     'compaction_window_size': 1 };

CREATE TABLE IF NOT EXISTS chat_users_by_authenticator (
    authenticator text,
    principal text,
    user_id uuid,
    joined_at timestamp,
    primary key ((authenticator), principal)
);

CREATE TABLE IF NOT EXISTS chat_users_by_principal (
    authenticator text,
    principal text,
    user_id uuid,
    joined_at timestamp,
    primary key ((principal), authenticator)
)

-- Example queries this schema serves:
--
-- All messages in a chat for a given day:
--   SELECT * FROM messages_by_chat
--   WHERE chat_id = ? AND msg_date = '2026-08-06';
--
-- Messages in a specific hour of that day:
--   SELECT * FROM messages_by_chat
--   WHERE chat_id = ? AND msg_date = '2026-08-06' AND msg_hour = 14;
--
-- A range of hours:
--   SELECT * FROM messages_by_chat
--   WHERE chat_id = ? AND msg_date = '2026-08-06'
--     AND msg_hour >= 9 AND msg_hour < 17;
