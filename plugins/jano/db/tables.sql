-- ============================================================
-- Jano Plugin — Database tables
-- ============================================================

-- Instance configuration
CREATE TABLE IF NOT EXISTS jano_instances (
    name                        TEXT PRIMARY KEY,
    category_id                 BIGINT NOT NULL,
    role_id                     BIGINT,                 -- NULL = @everyone
    text_channel_id             BIGINT,
    voice_channel_id            BIGINT,
    mention_role_id             BIGINT,
    active_days                 INTEGER[],
    opening_time                TEXT    NOT NULL DEFAULT '19:00',
    closing_time                TEXT    NOT NULL DEFAULT '22:00',
    max_manual_hours            FLOAT   NOT NULL DEFAULT 0.0,
    command_role_ids_instance   BIGINT[],               -- NULL = inherit global only
    status_icon                 BOOLEAN NOT NULL DEFAULT true
);

-- Runtime state per instance
CREATE TABLE IF NOT EXISTS jano_state (
    name                    TEXT PRIMARY KEY REFERENCES jano_instances(name) ON DELETE CASCADE ON UPDATE CASCADE,
    current_state           BOOLEAN,
    category_name_cache     TEXT,
    last_message_id         BIGINT,
    manual_override         BOOLEAN,                    -- NULL = no override
    override_ts             TIMESTAMP WITH TIME ZONE,
    manual_hours_active     FLOAT   NOT NULL DEFAULT 0.0,
    max_hours_override      FLOAT,
    schedule_override       JSONB                       -- {days, opening, closing} or NULL
);

-- Global config (single-row table)
CREATE TABLE IF NOT EXISTS jano_global (
    id                      INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    command_role_ids_global BIGINT[]
);

INSERT INTO jano_global (id, command_role_ids_global)
VALUES (1, ARRAY[]::BIGINT[])
ON CONFLICT DO NOTHING;
