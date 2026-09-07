import json
import ssl
import asyncpg
from config import DATABASE_URL, TZ_NAME, LOG

POOL = None

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        telegram_user_id BIGINT
          UNIQUE NOT NULL,
        telegram_chat_id BIGINT,
        first_name TEXT, username TEXT,
        display_name TEXT,
        exam_goal TEXT, exam_date DATE,
        wake_time TIME DEFAULT '06:30',
        sleep_time TIME DEFAULT '23:00',
        school_start TIME, school_end TIME,
        school_days INT[],
        coaching_start TIME,
        coaching_end TIME,
        coaching_days INT[],
        meal_minutes INT DEFAULT 60,
        commute_minutes INT DEFAULT 0,
        energy TEXT DEFAULT 'normal',
        energy_set_at TIMESTAMPTZ,
        onboarded BOOLEAN DEFAULT FALSE,
        ob_state JSONB DEFAULT '{}'::jsonb,
        ob_step TEXT,
        last_tick DATE,
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS subjects (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        name TEXT NOT NULL,
        difficulty INT DEFAULT 3,
        created_at TIMESTAMPTZ
          DEFAULT now(),
        UNIQUE(user_id, name))""",
    """CREATE TABLE IF NOT EXISTS chapters (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID NOT NULL
          REFERENCES subjects(id)
          ON DELETE CASCADE,
        name TEXT NOT NULL,
        position INT DEFAULT 0,
        created_at TIMESTAMPTZ
          DEFAULT now(),
        UNIQUE(user_id, subject_id, name))""",
    """CREATE TABLE IF NOT EXISTS topics (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID NOT NULL
          REFERENCES subjects(id)
          ON DELETE CASCADE,
        chapter_id UUID REFERENCES chapters(id)
          ON DELETE SET NULL,
        name TEXT NOT NULL,
        position INT DEFAULT 0,
        created_at TIMESTAMPTZ
          DEFAULT now(),
        UNIQUE(user_id, subject_id, name))""",
    """CREATE TABLE IF NOT EXISTS study_sessions (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        topic_id UUID REFERENCES topics(id)
          ON DELETE SET NULL,
        source TEXT DEFAULT 'log',
        status TEXT DEFAULT 'finished',
        title TEXT,
        questions_attempted INT DEFAULT 0,
        questions_correct INT,
        duration_minutes INT DEFAULT 0,
        started_at TIMESTAMPTZ,
        ended_at TIMESTAMPTZ,
        paused_at TIMESTAMPTZ,
        paused_seconds INT DEFAULT 0,
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE INDEX IF NOT EXISTS ix_sess_user
        ON study_sessions(user_id, created_at)""",
    """CREATE TABLE IF NOT EXISTS homework (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        title TEXT NOT NULL,
        hw_type TEXT DEFAULT 'custom',
        total_q INT,
        completed_q INT DEFAULT 0,
        est_minutes INT,
        due_at TIMESTAMPTZ,
        status TEXT DEFAULT 'not_started',
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE INDEX IF NOT EXISTS ix_hw_user
        ON homework(user_id, due_at)""",
    """CREATE TABLE IF NOT EXISTS tests (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        name TEXT NOT NULL,
        test_at TIMESTAMPTZ,
        total_marks INT,
        obtained_marks INT,
        status TEXT DEFAULT 'scheduled',
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS mistakes (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        topic_id UUID REFERENCES topics(id)
          ON DELETE SET NULL,
        mtype TEXT DEFAULT 'unknown',
        description TEXT DEFAULT '',
        count INT DEFAULT 1,
        fingerprint TEXT NOT NULL,
        resolved BOOLEAN DEFAULT FALSE,
        resolved_at TIMESTAMPTZ,
        first_seen TIMESTAMPTZ
          DEFAULT now(),
        last_seen TIMESTAMPTZ
          DEFAULT now(),
        created_at TIMESTAMPTZ
          DEFAULT now(),
        UNIQUE(user_id, fingerprint))""",
    """CREATE TABLE IF NOT EXISTS revisions (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        topic_id UUID NOT NULL
          REFERENCES topics(id)
          ON DELETE CASCADE,
        rev_type TEXT
          DEFAULT 'active_recall',
        mode TEXT DEFAULT 'review',
        interval_days INT DEFAULT 1,
        ease NUMERIC DEFAULT 2.3,
        streak INT DEFAULT 0,
        lapses INT DEFAULT 0,
        last_outcome TEXT,
        last_reviewed TIMESTAMPTZ,
        due_at TIMESTAMPTZ DEFAULT now(),
        status TEXT DEFAULT 'due',
        created_at TIMESTAMPTZ
          DEFAULT now(),
        UNIQUE(user_id, topic_id))""",
    """CREATE INDEX IF NOT EXISTS ix_rev_due
        ON revisions(user_id, due_at)""",
    """CREATE TABLE IF NOT EXISTS mastery (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE CASCADE,
        topic_id UUID REFERENCES topics(id)
          ON DELETE CASCADE,
        mastery REAL,
        attempts INT DEFAULT 0,
        correct INT DEFAULT 0,
        events INT DEFAULT 0,
        last_evidence TIMESTAMPTZ,
        algo INT DEFAULT 1,
        computed_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE UNIQUE INDEX IF NOT EXISTS
        uq_mast_topic
        ON mastery(user_id, topic_id)
        WHERE topic_id IS NOT NULL""",
    """CREATE UNIQUE INDEX IF NOT EXISTS
        uq_mast_subj
        ON mastery(user_id, subject_id)
        WHERE topic_id IS NULL""",
    """CREATE TABLE IF NOT EXISTS mastery_log (
        id BIGSERIAL PRIMARY KEY,
        user_id UUID NOT NULL,
        topic_id UUID,
        mastery REAL NOT NULL,
        recorded_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE INDEX IF NOT EXISTS ix_mlog
        ON mastery_log(user_id, recorded_at)""",
    """CREATE TABLE IF NOT EXISTS quizzes (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        topic_id UUID REFERENCES topics(id)
          ON DELETE SET NULL,
        topic_label TEXT,
        questions JSONB,
        answers JSONB DEFAULT '[]'::jsonb,
        status TEXT DEFAULT 'active',
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS classes (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        title TEXT NOT NULL,
        weekdays INT[],
        start_time TIME NOT NULL,
        end_time TIME NOT NULL,
        active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS extra_events (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        title TEXT NOT NULL,
        on_date DATE NOT NULL,
        start_time TIME NOT NULL,
        end_time TIME NOT NULL,
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS pending_actions (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        msg_id BIGINT,
        kind TEXT,
        payload JSONB DEFAULT '{}'::jsonb,
        code TEXT,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS update_inbox (
        update_id BIGINT PRIMARY KEY,
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS resources (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        topic_id UUID REFERENCES topics(id)
          ON DELETE SET NULL,
        title TEXT,
        content TEXT,
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS daily_plans (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        plan_date DATE,
        available_minutes INT,
        tasks JSONB,
        created_at TIMESTAMPTZ
          DEFAULT now(),
        UNIQUE(user_id, plan_date))""",
    """CREATE TABLE IF NOT EXISTS schedule_changes (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        op TEXT NOT NULL,
        which TEXT NOT NULL,
        from_date DATE NOT NULL,
        to_date DATE,
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS ai_memory (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        kind TEXT DEFAULT 'fact',
        content TEXT NOT NULL,
        source TEXT DEFAULT 'chat',
        active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS chat_history (
        id BIGSERIAL PRIMARY KEY,
        user_id UUID NOT NULL
          REFERENCES users(id)
          ON DELETE CASCADE,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    """CREATE INDEX IF NOT EXISTS ix_chat_hist
        ON chat_history(user_id, id)""",
    """CREATE TABLE IF NOT EXISTS ai_log (
        id BIGSERIAL PRIMARY KEY,
        kind TEXT, status TEXT,
        latency_ms INT, error TEXT,
        created_at TIMESTAMPTZ
          DEFAULT now())""",
    # --- migrations for pre-existing DBs
    # (must run BEFORE the index below)
    """ALTER TABLE topics
       ADD COLUMN IF NOT EXISTS
       chapter_id UUID""",
    """ALTER TABLE resources
       ADD COLUMN IF NOT EXISTS
       subject_id UUID""",
    """ALTER TABLE resources
       ADD COLUMN IF NOT EXISTS
       topic_id UUID""",
    """ALTER TABLE pending_actions
       ADD COLUMN IF NOT EXISTS code TEXT""",
    """ALTER TABLE mistakes
       ADD COLUMN IF NOT EXISTS
       resolved_at TIMESTAMPTZ""",
    # --- index on migrated column: LAST
    """CREATE UNIQUE INDEX IF NOT EXISTS
       ix_pa_code
       ON pending_actions(user_id, code)""",
]


def jd(v, default):
    """Safe JSONB decode — Supabase may
    return jsonb as str OR dict."""
    if v is None:
        return default
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (str, bytes)):
        try:
            out = json.loads(v)
            if isinstance(out, (dict, list)):
                return out
        except Exception:
            pass
    return default


async def init_db():
    global POOL
    dsn = DATABASE_URL.split("?")[0].strip()
    if not dsn.startswith(
            ("postgresql://", "postgres://")):
        raise RuntimeError(
            "DATABASE_URL is wrong! It must start "
            "with postgresql:// — get it from "
            "Supabase Connect -> Session pooler "
            "and replace [YOUR-PASSWORD].")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    POOL = await asyncpg.create_pool(
        dsn, min_size=1, max_size=3,
        command_timeout=20,
        statement_cache_size=0, ssl=ctx,
        server_settings={"TimeZone": TZ_NAME})
    async with POOL.acquire() as c:
        for stmt in SCHEMA:
            await c.execute(stmt)
    LOG.info("database ready (schema ensured)")


async def q(sql, *a):
    async with POOL.acquire() as c:
        return await c.execute(sql, *a)


async def qrow(sql, *a):
    async with POOL.acquire() as c:
        return await c.fetchrow(sql, *a)


async def qrows(sql, *a):
    async with POOL.acquire() as c:
        return await c.fetch(sql, *a)


async def qval(sql, *a):
    async with POOL.acquire() as c:
        return await c.fetchval(sql, *a)
