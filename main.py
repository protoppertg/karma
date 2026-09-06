"""
StudyOS — Personal AI Study Operating System
Single-user. Telegram + Supabase + Gemini free
tier + Render. Tables auto-create on boot.
AI interprets language; Python computes every
number.
"""

import os
import re
import ssl
import io
import json
import time
import math
import uuid
import secrets
import hashlib
import logging
import asyncio
from datetime import datetime, date, timedelta
from datetime import time as dtime
from datetime import timezone
from zoneinfo import ZoneInfo

import asyncpg
import httpx
from fastapi import FastAPI, Request, Header
from fastapi import HTTPException
from contextlib import asynccontextmanager

from google import genai
from google.genai import types

# ============================================================
# CONFIG
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN", "")
TELEGRAM_WEBHOOK_SECRET = os.getenv(
    "TELEGRAM_WEBHOOK_SECRET", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv(
    "GEMINI_MODEL", "gemini-2.0-flash")

TZ_NAME = os.getenv(
    "DEFAULT_TIMEZONE", "Asia/Kolkata")
try:
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    TZ_NAME, TZ = "UTC", timezone.utc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s "
           "%(name)s: %(message)s")
LOG = logging.getLogger("studyos")

HTTP = httpx.AsyncClient(timeout=35.0)
POOL = None
AIC = None

# ============================================================
# DB SCHEMA (auto-applied on boot, idempotent)
# ============================================================
SCHEMA = [
    """CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        telegram_user_id BIGINT UNIQUE NOT NULL,
        telegram_chat_id BIGINT,
        first_name TEXT, username TEXT,
        display_name TEXT,
        exam_goal TEXT, exam_date DATE,
        wake_time TIME DEFAULT '06:30',
        sleep_time TIME DEFAULT '23:00',
        school_start TIME, school_end TIME,
        school_days INT[],
        coaching_start TIME, coaching_end TIME,
        coaching_days INT[],
        meal_minutes INT DEFAULT 60,
        commute_minutes INT DEFAULT 0,
        energy TEXT DEFAULT 'normal',
        energy_set_at TIMESTAMPTZ,
        onboarded BOOLEAN DEFAULT FALSE,
        ob_state JSONB DEFAULT '{}'::jsonb,
        ob_step TEXT,
        last_tick DATE,
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS subjects (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        difficulty INT DEFAULT 3,
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, name))""",
    """CREATE TABLE IF NOT EXISTS topics (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID NOT NULL
          REFERENCES subjects(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        position INT DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, subject_id, name))""",
    """CREATE TABLE IF NOT EXISTS study_sessions (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
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
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE INDEX IF NOT EXISTS ix_sess_user
        ON study_sessions(user_id, created_at)""",
    """CREATE TABLE IF NOT EXISTS homework (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        title TEXT NOT NULL,
        hw_type TEXT DEFAULT 'custom',
        total_q INT,
        completed_q INT DEFAULT 0,
        est_minutes INT,
        due_at TIMESTAMPTZ,
        status TEXT DEFAULT 'not_started',
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE INDEX IF NOT EXISTS ix_hw_user
        ON homework(user_id, due_at)""",
    """CREATE TABLE IF NOT EXISTS tests (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        name TEXT NOT NULL,
        test_at TIMESTAMPTZ,
        total_marks INT,
        obtained_marks INT,
        status TEXT DEFAULT 'scheduled',
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS mistakes (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
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
        first_seen TIMESTAMPTZ DEFAULT now(),
        last_seen TIMESTAMPTZ DEFAULT now(),
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, fingerprint))""",
    """CREATE TABLE IF NOT EXISTS revisions (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        topic_id UUID NOT NULL
          REFERENCES topics(id) ON DELETE CASCADE,
        rev_type TEXT DEFAULT 'active_recall',
        mode TEXT DEFAULT 'review',
        interval_days INT DEFAULT 1,
        ease NUMERIC DEFAULT 2.3,
        streak INT DEFAULT 0,
        lapses INT DEFAULT 0,
        last_outcome TEXT,
        last_reviewed TIMESTAMPTZ,
        due_at TIMESTAMPTZ DEFAULT now(),
        status TEXT DEFAULT 'due',
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, topic_id))""",
    """CREATE INDEX IF NOT EXISTS ix_rev_due
        ON revisions(user_id, due_at)""",
    """CREATE TABLE IF NOT EXISTS mastery (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
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
        computed_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE UNIQUE INDEX IF NOT EXISTS
        uq_mast_topic ON mastery(user_id, topic_id)
        WHERE topic_id IS NOT NULL""",
    """CREATE UNIQUE INDEX IF NOT EXISTS
        uq_mast_subj ON mastery(user_id, subject_id)
        WHERE topic_id IS NULL""",
    """CREATE TABLE IF NOT EXISTS quizzes (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        topic_id UUID REFERENCES topics(id)
          ON DELETE SET NULL,
        topic_label TEXT,
        questions JSONB,
        answers JSONB DEFAULT '[]'::jsonb,
        status TEXT DEFAULT 'active',
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS pending_actions (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        msg_id BIGINT,
        kind TEXT,
        payload JSONB DEFAULT '{}'::jsonb,
        code TEXT,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE UNIQUE INDEX IF NOT EXISTS ix_pa_code
        ON pending_actions(user_id, code)""",
    """CREATE TABLE IF NOT EXISTS update_inbox (
        update_id BIGINT PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS resources (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        title TEXT,
        content TEXT,
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS daily_plans (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        plan_date DATE,
        available_minutes INT,
        tasks JSONB,
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, plan_date))""",
    """CREATE TABLE IF NOT EXISTS schedule_changes (
        id UUID PRIMARY KEY
          DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        op TEXT NOT NULL,
        which TEXT NOT NULL,
        from_date DATE NOT NULL,
        to_date DATE,
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS ai_log (
        id BIGSERIAL PRIMARY KEY,
        kind TEXT,
        status TEXT,
        latency_ms INT,
        error TEXT,
        created_at TIMESTAMPTZ DEFAULT now())""",
    # migrations for pre-existing databases
    """ALTER TABLE pending_actions
       ADD COLUMN IF NOT EXISTS code TEXT""",
    """ALTER TABLE mistakes
       ADD COLUMN IF NOT EXISTS resolved_at
       TIMESTAMPTZ""",
]


async def init_db():
    global POOL
    dsn = DATABASE_URL.split("?")[0].strip()
    if not dsn.startswith(
            ("postgresql://", "postgres://")):
        raise RuntimeError(
            "DATABASE_URL is wrong! It must "
            "start with postgresql:// — you "
            "likely pasted the https:// "
            "Supabase project URL. Correct "
            "place: Supabase → Connect → "
            "Connection pooling → Session "
            "pooler, then replace "
            "[YOUR-PASSWORD].")
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

# ============================================================
# TELEGRAM HELPERS
# ============================================================
TG_BASE = ("https://api.telegram.org/bot"
           + TELEGRAM_BOT_TOKEN)


def IK(*rows):
    kb = []
    for r in rows:
        kb.append([{"text": t,
                    "callback_data": d}
                   for t, d in r])
    return {"inline_keyboard": kb}


def esc(s) -> str:
    s = str(s if s is not None else "")
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;"))


def trunc(s, n=3500) -> str:
    return s if len(s) <= n else s[:n] + "\n…"


async def tg(method, **payload):
    if payload.get("reply_markup") is None:
        payload.pop("reply_markup", None)
    for attempt in range(3):
        try:
            r = await HTTP.post(
                TG_BASE + "/" + method,
                json=payload)
            data = r.json()
            if data.get("ok"):
                return data.get("result")
            code = data.get("error_code")
            desc = data.get("description", "")
            if "message is not modified" in desc:
                return True
            if code == 429:
                p = data.get("parameters", {})
                ra = p.get("retry_after", 3)
                await asyncio.sleep(min(ra, 10))
                continue
            if code and code >= 500:
                await asyncio.sleep(
                    1.5 * (attempt + 1))
                continue
            LOG.warning("tg %s: %s", method, desc)
            return None
        except (httpx.TimeoutException,
                httpx.TransportError) as e:
            LOG.warning("tg %s net: %s", method, e)
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


async def send(chat_id, text, kb=None):
    text = trunc(text)
    res = await tg(
        "sendMessage", chat_id=chat_id,
        text=text, parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=kb)
    if res is None and ("<" in text
                        or ">" in text):
        plain = re.sub(r"<[^>]+>", "", text)
        await tg("sendMessage",
                 chat_id=chat_id, text=plain,
                 disable_web_page_preview=True,
                 reply_markup=kb)
    return res


async def edit(chat_id, msg_id, text,
               kb=None):
    text = trunc(text)
    res = await tg(
        "editMessageText", chat_id=chat_id,
        message_id=msg_id, text=text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=kb)
    if res is None:
        await send(chat_id, text, kb)
    return res


async def answer_cb(cb_id, text=""):
    try:
        await tg("answerCallbackQuery",
                 callback_query_id=cb_id,
                 text=text)
    except Exception:
        pass


async def get_file_bytes(file_id):
    res = await tg("getFile", file_id=file_id)
    if not res:
        raise RuntimeError("getFile failed")
    url = (TG_BASE + "/file/bot"
           + TELEGRAM_BOT_TOKEN + "/"
           + res["file_path"])
    r = await HTTP.get(url)
    r.raise_for_status()
    return r.content

# ============================================================
# GEMINI — free-tier friendly
# ============================================================
class AIError(Exception):
    pass


_last_ai = 0.0
AI_CACHE = {}


async def _ai_space():
    global _last_ai
    wait = 4.6 - (time.monotonic() - _last_ai)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_ai = time.monotonic()


def _cache_get(key):
    hit = AI_CACHE.get(key)
    if hit and time.time() - hit[0] < 21600:
        return hit[1]
    return None


def _cache_set(key, val):
    if len(AI_CACHE) > 300:
        AI_CACHE.clear()
    AI_CACHE[key] = (time.time(), val)


async def ai_call(contents, json_mode=True,
                  max_tokens=1200):
    for attempt in range(3):
        await _ai_space()
        t0 = time.monotonic()
        try:
            mime = ("application/json"
                    if json_mode
                    else "text/plain")
            cfg = types.GenerateContentConfig(
                response_mime_type=mime,
                max_output_tokens=max_tokens,
                temperature=0.2)

            def _do():
                return (AIC.models
                        .generate_content(
                            model=GEMINI_MODEL,
                            contents=contents,
                            config=cfg))

            resp = await asyncio.to_thread(_do)
            text = resp.text
            if not text:
                raise AIError("empty AI response")
            return text
        except Exception as e:
            msg = str(e)
            low = msg.lower()
            quota = ("429" in msg
                     or "resource" in low
                     or "quota" in low)
            try:
                await q(
                    """INSERT INTO ai_log
                       (kind,status,
                        latency_ms,error)
                       VALUES('call',
                        $1,$2,$3)""",
                    "retry" if attempt < 2
                    else "error",
                    int((time.monotonic() - t0)
                        * 1000),
                    msg[:400])
            except Exception:
                pass
            if quota:
                await asyncio.sleep(
                    9 * (attempt + 1))
                continue
            if attempt >= 1:
                raise AIError(msg[:300])
            await asyncio.sleep(1.2)
    raise AIError(
        "AI quota exhausted — try again soon")


def _load_json(text):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?",
                      "", text)
        text = text.rstrip("`").strip()
    i = text.find("{")
    j = text.rfind("}")
    if i == -1 or j == -1:
        raise AIError("no JSON in AI response")
    return json.loads(text[i:j + 1])

# ============================================================
# DETERMINISTIC PARSERS (zero AI cost)
# ============================================================
WEEKDAYS = {"monday": 0, "mon": 0,
            "tuesday": 1, "tue": 1, "tues": 1,
            "wednesday": 2, "wed": 2,
            "thursday": 3, "thu": 3,
            "thurs": 3, "friday": 4, "fri": 4,
            "saturday": 5, "sat": 5,
            "sunday": 6, "sun": 6}
MONTHS = {m.lower(): i + 1
          for i, m in enumerate(
              ["January", "February", "March",
               "April", "May", "June", "July",
               "August", "September",
               "October", "November",
               "December"])}
SKIP_WORDS = {"skip", "/skip", "later", "no",
              "nothing", "none", "n", "na"}


def parse_date(text, today):
    t = (text or "").strip().lower()
    t = t.rstrip(".,!?")
    if not t:
        return None
    if t in ("today", "tonight"):
        return today
    if t in ("tomorrow", "tmrw", "tmr"):
        return today + timedelta(days=1)
    if t == "day after tomorrow":
        return today + timedelta(days=2)
    m = re.match(r"^in\s+(\d+)\s+days?$", t)
    if m:
        n = int(m.group(1))
        return today + timedelta(days=n)
    for wd in WEEKDAYS:
        if t == wd or t == "next " + wd \
                or t == "this " + wd:
            d = (WEEKDAYS[wd]
                 - today.weekday()) % 7
            return today + timedelta(
                days=d if d else 7)
    m = re.match(r"^(\d{1,2})[\s\-/](\d{1,2})"
                 r"(?:[\s\-/](\d{2,4}))?$", t)
    if m:
        dd = int(m.group(1))
        mo = int(m.group(2))
        yy = today.year
        if m.group(3):
            yy = int(m.group(3))
            yy += 2000 if yy < 100 else 0
        try:
            return date(yy, mo, dd)
        except ValueError:
            return None
    m = re.match(r"^(\d{1,2})"
                 r"(?:st|nd|rd|th)?"
                 r"\s+([a-z]+)$", t)
    if m and m.group(2) in MONTHS:
        try:
            return date(today.year,
                        MONTHS[m.group(2)],
                        int(m.group(1)))
        except ValueError:
            return None
    for name, mo in MONTHS.items():
        if t.startswith(name):
            m = re.match(
                "^" + name
                + r"\s+(\d{1,2})"
                  r"(?:st|nd|rd|th)?$", t)
            if m:
                try:
                    return date(
                        today.year, mo,
                        int(m.group(1)))
                except ValueError:
                    return None
    return None


def parse_time(t):
    t = (t or "").strip().lower()
    t = t.replace(" ", "")
    m = re.match(
        r"^(\d{1,2})(?::(\d{2}))?(am|pm)?$",
        t)
    if not m:
        return None
    h = int(m.group(1))
    mi = int(m.group(2) or 0)
    ap = m.group(3)
    if ap == "pm" and h != 12:
        h += 12
    if ap == "am" and h == 12:
        h = 0
    if not (0 <= h < 24 and 0 <= mi < 60):
        return None
    return dtime(h, mi)


def parse_time_range(t):
    t = (t or "").strip().lower()
    t = t.replace(" ", "")
    t = t.replace("–", "-")
    t = t.replace("—", "-")
    m = re.match(
        r"^(\d{1,2})(?::(\d{2}))?(am|pm)?"
        r"(?:-|to)"
        r"(\d{1,2})(?::(\d{2}))?(am|pm)?$",
        t)
    if not m:
        return None
    h1 = int(m.group(1))
    m1 = int(m.group(2) or 0)
    ap1 = m.group(3)
    h2 = int(m.group(4))
    m2 = int(m.group(5) or 0)
    ap2 = m.group(6)
    if ap1:
        if ap1 == "pm" and h1 != 12:
            h1 += 12
        if ap1 == "am" and h1 == 12:
            h1 = 0
    if ap2:
        if ap2 == "pm" and h2 != 12:
            h2 += 12
        if ap2 == "am" and h2 == 12:
            h2 = 0
    elif ap1:
        if h2 < 12 and h2 < h1:
            h2 += 12
    elif h2 <= h1 and h2 < 12:
        h2 += 12
    elif h1 <= 7 and h2 <= 7:
        h1 += 12
        h2 += 12
    if h2 * 60 + m2 <= h1 * 60 + m1:
        return None
    return dtime(h1, m1), dtime(h2, m2)


def parse_duration(text):
    t = (text or "").lower()
    m = re.search(
        r"(\d+(?:\.\d+)?)\s*"
        r"(?:h|hr|hrs|hour|hours)\s*"
        r"(?:(\d+)\s*"
        r"(?:m|min|mins|minutes))?", t)
    if m:
        return int(float(m.group(1)) * 60
                   + int(m.group(2) or 0))
    m = re.search(
        r"(\d+)\s*"
        r"(?:m|min|mins|minute|minutes)\b",
        t)
    if m:
        return int(m.group(1))
    return None


def parse_questions(text):
    t = (text or "").lower()
    m = re.search(
        r"(\d+)\s*"
        r"(?:questions?|qs|mcqs?|"
        r"problems?|pyqs?)\b", t)
    if not m:
        return None
    att = int(m.group(1))
    cor = wre = None
    mc = re.search(
        r"(\d+)\s*"
        r"(?:correct|right|correctly)", t)
    mw = re.search(
        r"(\d+)\s*"
        r"(?:wrong|incorrect|mistakes?)",
        t)
    if mc:
        cor = int(mc.group(1))
    if mw:
        wre = int(mw.group(1))
    if (cor is None and wre is not None
            and wre <= att):
        cor = att - wre
    return att, cor, wre


def detect_energy(text):
    t = (text or "").lower()
    bad = ("exhausted", "dead tired",
           "burnt out", "burned out",
           "no energy", "so tired",
           "can't do this", "cant do this")
    mid = ("tired", "sleepy", "cant focus",
           "can't focus", "low energy",
           "drained")
    good = ("energetic", "feeling fresh",
            "fired up")
    if any(p in t for p in bad):
        return "exhausted"
    if any(p in t for p in mid):
        return "tired"
    if any(p in t for p in good):
        return "energetic"
    return None


def parse_days(s):
    s = (s or "").lower().strip()
    if not s:
        return list(range(7))
    if s in ("daily", "everyday",
             "every day", "all days"):
        return list(range(7))
    if s == "weekdays":
        return [0, 1, 2, 3, 4]
    if s in ("weekend", "weekends"):
        return [5, 6]
    out = set()
    for part in re.split(r"[,\s&+]+", s):
        part = part.strip().rstrip("s")
        if part in WEEKDAYS:
            out.add(WEEKDAYS[part])
            continue
        bits = re.split(r"-|\s+to\s+", part)
        if (len(bits) == 2
                and bits[0] in WEEKDAYS
                and bits[1] in WEEKDAYS):
            a = WEEKDAYS[bits[0]]
            b = WEEKDAYS[bits[1]]
            if b >= a:
                out.update(range(a, b + 1))
            else:
                out.update(range(a, 7))
                out.update(range(0, b + 1))
    return sorted(out) if out else None


def parse_class(text):
    m = re.search(
        r"(\d{1,2}(?::\d{2})?\s*"
        r"(?:am|pm)?\s*(?:-|–|to)\s*"
        r"\d{1,2}(?::\d{2})?\s*"
        r"(?:am|pm)?)", text.lower())
    if not m:
        return None
    tr = parse_time_range(m.group(1))
    if not tr:
        return None
    rest = text.lower()
    rest = rest.replace(m.group(1), " ")
    rest = rest.strip()
    days = parse_days(rest) if rest \
        else list(range(7))
    return tr[0], tr[1], days

# ============================================================
# ENGINES — deterministic
# ============================================================
SOURCE_W = {"log": 1.0, "live": 1.0,
            "quiz": 0.9, "test": 1.6}


def compute_mastery(events):
    """Mastery v1: bounded EMA fold.
    prior=25; alpha=min(0.15, 0.08*vol*w);
    volume=log10(1+att)/log10(31).
    One event moves mastery <= 15 pts."""
    if not events:
        return None
    m = 25.0
    att = cor = n = 0
    last = None
    evs = sorted(events, key=lambda x: x["at"])
    for e in evs:
        a = e["a"]
        c = e["c"]
        if not a or a <= 0 or c is None:
            continue
        att += a
        cor += c
        n += 1
        last = e["at"]
        score = 100.0 * c / a
        volume = min(
            1.0, math.log10(1 + a)
            / math.log10(31.0))
        w = SOURCE_W.get(e["src"], 1.0)
        alpha = min(0.15, 0.08 * volume * w)
        m += alpha * (score - m)
        m = max(0.0, min(100.0, m))
    return {"mastery": round(m, 1),
            "attempts": att, "correct": cor,
            "events": n, "last": last}


def grade_of(acc):
    if acc is None:
        return "good"
    if acc >= 0.9:
        return "easy"
    if acc >= 0.75:
        return "good"
    if acc >= 0.6:
        return "hard"
    return "again"


def revision_step(r, acc, reps):
    interval = int(r["interval_days"])
    ease = float(r["ease"])
    streak = int(r["streak"])
    lapses = int(r["lapses"])
    mode = r["mode"]
    rev_type = r["rev_type"]
    g = grade_of(acc)
    if g == "again":
        interval = 1
        streak = 0
        lapses += 1
        ease -= 0.20
    elif g == "hard":
        interval = max(1, round(interval * 1.2))
        ease -= 0.15
    elif g == "good":
        if streak == 0:
            interval = 1
        elif streak == 1:
            interval = 3
        else:
            interval = round(interval * ease)
        streak += 1
    else:
        interval = max(
            1, round(interval * ease * 1.3))
        streak += 1
        ease += 0.15
    ease = max(1.3, min(2.8, ease))
    if lapses >= 3:
        mode = "practice"
        rev_type = "mcq"
        interval = min(interval, 3)
    if acc is not None and acc < 0.5:
        interval = min(interval, 2)
    if (reps or 0) >= 2:
        rev_type = "mistake_review"
        interval = min(interval, 2)
    return {"interval_days": interval,
            "ease": round(ease, 2),
            "streak": streak,
            "lapses": lapses, "mode": mode,
            "rev_type": rev_type, "grade": g}


BASE_SCORE = {"test_prep": 40, "homework": 30,
              "mistake_review": 28,
              "revision": 25, "quiz": 22,
              "study": 20}
LIGHT = {"revision", "mistake_review", "quiz"}


def score_task(c, now, energy):
    s = float(BASE_SCORE.get(c["kind"], 15))
    rs = []
    k = c["kind"]
    m = c.get("meta", {})
    if k == "homework":
        due = m.get("due_at")
        if due:
            hrs = (due - now)
            hrs = hrs.total_seconds() / 3600
            if hrs < 0:
                s += 25
                od = int(-hrs // 24) + 1
                rs.append("overdue ~" + str(od)
                          + "d")
            elif hrs <= 24:
                s += 15
                rs.append("due <24h")
            elif hrs <= 48:
                s += 8
                rs.append("due tomorrow")
        rem = m.get("remaining")
        if rem:
            s += min(6, rem / 10)
            rs.append(str(rem)
                      + " questions left")
    t_in = m.get("test_in_days")
    heavy = ("revision", "study",
             "mistake_review", "homework")
    if t_in is not None and t_in <= 14 \
            and k in heavy:
        if t_in <= 3:
            s += 25
        elif t_in <= 7:
            s += 15
        else:
            s += 8
        rs.append("test in " + str(t_in) + "d")
    if k == "revision":
        od = m.get("overdue_days") or 0
        if od > 0:
            s += min(20, 2 * od)
            rs.append("revision overdue "
                      + str(od) + "d")
        mast = m.get("mastery")
        if mast is not None and mast < 40:
            s += 10
            rs.append("weak ("
                      + format(mast, ".0f")
                      + "%)")
        if m.get("mode") == "practice":
            rs.append("needs active practice")
    if k == "mistake_review":
        r = m.get("reps") or 0
        if r:
            s += min(12, 2 * r)
            rs.append(str(r)
                      + " repeated mistake(s)")
    if k == "study":
        mast = m.get("mastery")
        if mast is not None and mast < 40:
            s += 8
            rs.append("weak topic ("
                      + format(mast, ".0f")
                      + "%)")
        st = m.get("stale_days")
        if st and st > 14:
            s += 5
            rs.append("untouched " + str(st)
                      + "d")
    if energy in ("tired", "exhausted"):
        if k in LIGHT:
            s += 10
            rs.append("light task fits energy")
        else:
            s -= 10
            rs.append("heavy — low energy today")
    if c.get("subject"):
        pre = c["subject"]
        if c.get("chapter"):
            pre = pre + " — " + c["chapter"]
        rs.insert(0, pre)
    return round(max(0.0, min(100.0, s)), 1), rs


def choose_next(scored, minutes,
                exclude=None):
    if minutes <= 0:
        return None
    limit = max(minutes, 15)
    pool = scored
    if exclude:
        pool = [x for x in scored
                if x[0]["key"] not in exclude]
    fitting = [x for x in pool
               if x[0]["est"] <= limit]
    if not fitting:
        light = [x for x in pool
                 if x[0]["kind"] in LIGHT]
        fitting = light or pool
    if not fitting:
        return None
    fitting.sort(key=lambda x: (-x[1],
                                x[0]["est"]))
    return fitting[0]


def day_minutes(u, day, mods=None):
    """available = awake − school −
    coaching − commute − meals − buffer.
    mods: one-off cancels/moves."""
    start = datetime.combine(
        day, u["wake_time"], tzinfo=TZ)
    end = datetime.combine(
        day, u["sleep_time"], tzinfo=TZ)
    if end <= start:
        end += timedelta(days=1)
    awake = (end - start).total_seconds() / 60
    busy = 0.0
    commute = 0
    wd = day.weekday()
    pairs = (("school_start", "school_end",
              "school_days", "school"),
             ("coaching_start",
              "coaching_end",
              "coaching_days", "coach"))
    for sk, ek, dk, base in pairs:
        days = u.get(dk)
        scheduled = bool(days and wd in days
                         and u.get(sk)
                         and u.get(ek))
        run = scheduled
        if mods and mods.get(base + "_off"):
            run = False
        if (mods and mods.get(base + "_on")
                and not scheduled):
            run = True
        if not run:
            continue
        bs = datetime.combine(
            day, u[sk], tzinfo=TZ)
        be = datetime.combine(
            day, u[ek], tzinfo=TZ)
        if be <= bs:
            be += timedelta(days=1)
        cs = max(bs, start)
        ce = min(be, end)
        if ce > cs:
            busy += (ce - cs)
            busy = busy
            busy = busy
        if ce > cs:
            mins = (ce - cs)
            busy += mins.total_seconds() / 60 \
                - mins.total_seconds() / 60
        if ce > cs:
            delta = (ce - cs)
            busy += delta.total_seconds() / 60
        if ce > cs:
            cm = u["commute_minutes"] or 0
            commute += min(cm, 60)
    commute = min(commute,
                  int(max(0, awake - busy)), 120)
    meals = u["meal_minutes"] or 60
    avail = awake - busy - commute - meals
    if avail > 0:
        avail *= 0.9
    return max(0, int(avail)), int(busy)


def urgent(c, now):
    k = c["kind"]
    if k in ("test_prep", "mistake_review"):
        return True
    if k == "homework":
        due = c.get("meta", {}).get("due_at")
        if due is None:
            return True
        return (due - now).total_seconds() \
            <= 48 * 3600
    if k == "revision":
        od = c.get("meta", {}).get(
            "overdue_days") or 0
        return od > 7
    return False


def build_plan(cands, avail, energy,
               missed_days, now):
    target = int(avail * 0.9)
    if energy == "exhausted":
        target = min(target, 60)
    elif energy == "tired":
        target = min(target, int(target * 0.6))
    scored = []
    for c in cands:
        sc, rs = score_task(c, now, energy)
        scored.append((c, sc, rs))
    if missed_days > 0:
        kept = []
        non_urgent_seen = 0
        for item in scored:
            if urgent(item[0], now):
                kept.append(item)
            elif non_urgent_seen < 2:
                c = item[0]
                sc = round(item[1] * 0.5, 1)
                kept.append((c, sc, item[2]))
                non_urgent_seen += 1
        scored = kept
    scored.sort(key=lambda x: -x[1])
    alloc = 0
    rev_n = 0
    tasks = []
    deferred = []
    for c, sc, rs in scored:
        if c["kind"] == "revision" \
                and rev_n >= 6:
            deferred.append(
                (c["title"],
                 "revision backlog cap"))
            continue
        chunk = min(c["est"], 50)
        if alloc + chunk > target:
            note = ("no time today ("
                    + str(alloc) + "/"
                    + str(target) + "m)")
            deferred.append((c["title"], note))
            continue
        tasks.append({"kind": c["kind"],
                      "title": c["title"],
                      "est": chunk,
                      "score": sc,
                      "reasons": rs})
        alloc += chunk
        if c["kind"] == "revision":
            rev_n += 1
        if c["est"] > chunk:
            rest = c["est"] - chunk
            note = "split — " + str(rest)
            note += "m later"
            deferred.append((c["title"], note))
    return {"available": avail,
            "allocated": alloc,
            "tasks": tasks,
            "deferred": deferred,
            "missed": missed_days,
            "target": target}

# ============================================================
# FORMAT HELPERS
# ============================================================
def now_tz():
    return datetime.now(TZ)


def today_d():
    return datetime.now(TZ).date()


def sod(d):
    return datetime.combine(
        d, dtime(0, 0), tzinfo=TZ)


def fm(mins):
    if mins is None:
        return "—"
    mins = int(mins)
    h, m = divmod(abs(mins), 60)
    body = str(h) + "h "
    body += format(m, "02d") + "m" if h \
        else str(m) + "m"
    return ("-" if mins < 0 else "") + body


def bar(pct, w=10):
    pct = max(0.0, min(100.0, pct or 0.0))
    f = int(round(w * pct / 100))
    return "█" * f + "░" * (w - f)

# ============================================================
# DB SERVICES
# ============================================================
async def ensure_user(tg_from, chat_id):
    row = await qrow(
        """INSERT INTO users
           (telegram_user_id,
            telegram_chat_id,
            first_name, username)
           VALUES($1,$2,$3,$4)
           ON CONFLICT(telegram_user_id)
           DO UPDATE SET
           telegram_chat_id = COALESCE(
             EXCLUDED.telegram_chat_id,
             users.telegram_chat_id),
           first_name = COALESCE(
             EXCLUDED.first_name,
             users.first_name),
           username = COALESCE(
             EXCLUDED.username, users.username)
           RETURNING *""",
        tg_from["id"], chat_id,
        tg_from.get("first_name"),
        tg_from.get("username"))
    return dict(row)


async def get_subjects(user_id):
    rows = await qrows(
        """SELECT id, name FROM subjects
           WHERE user_id=$1::uuid
           ORDER BY name""", user_id)
    return [dict(r) for r in rows]


def resolve_subject(name, subjects):
    if not name or not subjects:
        return None, None
    n = name.strip().casefold()
    for s in subjects:
        if s["name"].casefold() == n:
            return s["id"], s["name"]
    for s in subjects:
        sn = s["name"].casefold()
        if n in sn or sn in n:
            return s["id"], s["name"]
    return None, None


async def upsert_subject(user_id, name):
    name = (name or "").strip().title()
    name = name or "General"
    r = await qrow(
        """INSERT INTO subjects
           (user_id, name)
           VALUES($1::uuid,$2)
           ON CONFLICT(user_id, name)
           DO UPDATE SET name=EXCLUDED.name
           RETURNING id, name""",
        user_id, name)
    return dict(r)


async def upsert_topic(user_id, subject_id,
                       name):
    name = (name or "").strip().title()
    name = name or "General"
    r = await qrow(
        """INSERT INTO topics
           (user_id, subject_id, name)
           VALUES($1::uuid,$2::uuid,$3)
           ON CONFLICT(user_id, subject_id,
                       name)
           DO UPDATE SET name=EXCLUDED.name
           RETURNING id, name""",
        user_id, subject_id, name)
    return dict(r)


async def find_topic(user_id, subject_id,
                     name):
    if not name:
        return None
    r = await qrow(
        """SELECT id, name FROM topics
           WHERE user_id=$1::uuid
           AND subject_id=$2::uuid
           AND name ILIKE $3 LIMIT 1""",
        user_id, subject_id,
        "%" + name.strip() + "%")
    return dict(r) if r else None


async def load_day_mods(user_id, day):
    rows = await qrows(
        """SELECT op, which, from_date,
                  to_date
           FROM schedule_changes
           WHERE user_id=$1::uuid
           AND (from_date=$2
                OR to_date=$2)""",
        user_id, day)
    mods = {"school_off": False,
            "coach_off": False,
            "school_on": False,
            "coach_on": False}
    for r in rows:
        which = r["which"]
        if which == "school":
            base = "school"
        elif which == "coaching":
            base = "coach"
        else:
            continue
        if r["from_date"] == day:
            mods[base + "_off"] = True
        if (r["op"] == "move"
                and r["to_date"] == day):
            mods[base + "_on"] = True
    return mods


async def topic_mastery_events(user_id,
                               topic_id):
    rows = await qrows(
        """SELECT questions_attempted a,
                  questions_correct c,
                  created_at, source
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND topic_id=$2::uuid
           AND status='finished'
           AND questions_attempted > 0
           AND questions_correct
             IS NOT NULL""",
        user_id, topic_id)
    out = []
    for r in rows:
        out.append({"a": r["a"], "c": r["c"],
                    "at": r["created_at"],
                    "src": r["source"]})
    return out


async def subject_mastery_events(
        user_id, subject_id):
    rows = await qrows(
        """SELECT questions_attempted a,
                  questions_correct c,
                  created_at, source
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND subject_id=$2::uuid
           AND status='finished'
           AND questions_attempted > 0
           AND questions_correct
             IS NOT NULL""",
        user_id, subject_id)
    out = []
    for r in rows:
        out.append({"a": r["a"], "c": r["c"],
                    "at": r["created_at"],
                    "src": r["source"]})
    return out


async def upsert_mastery(user_id,
                         subject_id,
                         topic_id, snap):
    if snap is None or snap["events"] == 0:
        return
    if topic_id:
        sql = (
            """INSERT INTO mastery
               (user_id, subject_id,
                topic_id, mastery,
                attempts, correct, events,
                last_evidence, algo)
               VALUES($1::uuid,$2::uuid,
                      $3::uuid,$4,$5,$6,
                      $7,$8,1)
               ON CONFLICT (user_id, topic_id)
               WHERE topic_id IS NOT NULL
               DO UPDATE SET
                 mastery=EXCLUDED.mastery,
                 attempts=EXCLUDED.attempts,
                 correct=EXCLUDED.correct,
                 events=EXCLUDED.events,
                 last_evidence=
                   EXCLUDED.last_evidence,
                 computed_at=now()""")
        await q(sql, user_id, subject_id,
                topic_id, snap["mastery"],
                snap["attempts"],
                snap["correct"],
                snap["events"], snap["last"])
    else:
        sql = (
            """INSERT INTO mastery
               (user_id, subject_id,
                topic_id, mastery,
                attempts, correct, events,
                last_evidence, algo)
               VALUES($1::uuid,$2::uuid,
                      NULL,$3,$4,$5,$6,$7,1)
               ON CONFLICT (user_id,
                            subject_id)
               WHERE topic_id IS NULL
               DO UPDATE SET
                 mastery=EXCLUDED.mastery,
                 attempts=EXCLUDED.attempts,
                 correct=EXCLUDED.correct,
                 events=EXCLUDED.events,
                 last_evidence=
                   EXCLUDED.last_evidence,
                 computed_at=now()""")
        await q(sql, user_id, subject_id,
                snap["mastery"],
                snap["attempts"],
                snap["correct"],
                snap["events"], snap["last"])


async def recompute(user_id, subject_id,
                    topic_id=None):
    if topic_id:
        ev = await topic_mastery_events(
            user_id, topic_id)
        snap = compute_mastery(ev)
        await upsert_mastery(
            user_id, subject_id,
            topic_id, snap)
    if subject_id:
        ev = await subject_mastery_events(
            user_id, subject_id)
        snap = compute_mastery(ev)
        await upsert_mastery(
            user_id, subject_id, None, snap)


async def mistake_reps(user_id, topic_id):
    if not topic_id:
        return 0
    v = await qval(
        """SELECT COALESCE(SUM(count),0)
           FROM mistakes
           WHERE user_id=$1::uuid
           AND topic_id=$2::uuid
           AND resolved=false""",
        user_id, topic_id)
    return int(v or 0)


async def apply_revision(user_id,
                         subject_id,
                         topic_id, acc, now):
    reps = await mistake_reps(
        user_id, topic_id)
    r = await qrow(
        """SELECT * FROM revisions
           WHERE user_id=$1::uuid
           AND topic_id=$2::uuid""",
        user_id, topic_id)
    if not r:
        interval = 2
        if acc is not None and acc < 0.5:
            interval = 1
        due = now + timedelta(days=interval)
        await q(
            """INSERT INTO revisions
               (user_id, subject_id,
                topic_id, rev_type,
                interval_days, due_at,
                last_outcome,
                last_reviewed, status)
               VALUES($1::uuid,$2::uuid,
                      $3::uuid,
                      'active_recall',
                      $4,$5,$6,$7,'due')
               ON CONFLICT(user_id, topic_id)
               DO NOTHING""",
            user_id, subject_id, topic_id,
            interval, due, grade_of(acc), now)
        return due
    st = revision_step(dict(r), acc, reps)
    due = now + timedelta(
        days=st["interval_days"])
    await q(
        """UPDATE revisions SET
           interval_days=$2, ease=$3,
           streak=$4, lapses=$5, mode=$6,
           rev_type=$7, last_outcome=$8,
           last_reviewed=$9, due_at=$10,
           status='due'
           WHERE id=$1::uuid""",
        str(r["id"]), st["interval_days"],
        st["ease"], st["streak"],
        st["lapses"], st["mode"],
        st["rev_type"], st["grade"],
        now, due)
    return due


async def save_evidence(u, subject_id,
                        topic_id, att, cor,
                        minutes, source="log",
                        when=None, title=None):
    now = when or now_tz()
    await q(
        """INSERT INTO study_sessions
           (user_id, subject_id,
            topic_id, source, status,
            title, questions_attempted,
            questions_correct,
            duration_minutes,
            started_at, ended_at)
           VALUES($1::uuid,$2::uuid,
                  $3::uuid,$4,'finished',
                  $5,$6,$7,$8,$9,$9)""",
        u["id"], subject_id, topic_id,
        source, title, att or 0, cor,
        minutes or 0, now)
    out = {}
    if att and cor is not None:
        out["acc"] = 100.0 * cor / att
        if topic_id and subject_id:
            await recompute(
                u["id"], subject_id, topic_id)
            out["next_rev"] = \
                await apply_revision(
                    u["id"], subject_id,
                    topic_id, cor / att, now)
            m = await qrow(
                """SELECT mastery, events
                   FROM mastery
                   WHERE user_id=$1::uuid
                   AND topic_id=$2::uuid""",
                u["id"], topic_id)
            if m:
                out["mastery"] = m["mastery"]
                out["events"] = m["events"]
        elif subject_id:
            await recompute(
                u["id"], subject_id)
            m = await qrow(
                """SELECT mastery
                   FROM mastery
                   WHERE user_id=$1::uuid
                   AND subject_id=$2::uuid
                   AND topic_id IS NULL""",
                u["id"], subject_id)
            if m:
                out["mastery"] = m["mastery"]
    return out


async def live_session(user_id):
    r = await qrow(
        """SELECT * FROM study_sessions
           WHERE user_id=$1::uuid
           AND status IN
             ('active','paused')
           LIMIT 1""", user_id)
    return dict(r) if r else None


async def live_minutes(u, now):
    s = await live_session(u["id"])
    if not s:
        return 0
    paused = s["paused_seconds"] or 0
    if s["status"] == "paused" \
            and s["paused_at"]:
        elapsed = (s["paused_at"]
                   - s["started_at"])
    else:
        elapsed = now - s["started_at"]
    secs = elapsed.total_seconds()
    return max(0, int((secs - paused) / 60))


async def spent_today(u, day):
    v = await qval(
        """SELECT COALESCE(
                    SUM(duration_minutes),0)
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >= $2
           AND created_at < $3""",
        u["id"], sod(day),
        sod(day) + timedelta(days=1))
    return int(v or 0)


async def cleanup_stale(u, now):
    s = await live_session(u["id"])
    if s:
        age = (now - s["started_at"])
        if age.total_seconds() > 3 * 3600:
            await q(
                """UPDATE study_sessions
                   SET status='abandoned'
                   WHERE id=$1::uuid""",
                str(s["id"]))


async def lazy_tick(u, now):
    today = now.date()
    if u.get("last_tick") == today:
        await cleanup_stale(u, now)
        return u
    es = u.get("energy_set_at")
    if (u["energy"] != "normal" and es
            and (now - es).total_seconds()
            > 12 * 3600):
        await q(
            """UPDATE users
               SET energy='normal'
               WHERE id=$1::uuid""",
            u["id"])
        u["energy"] = "normal"
    await q(
        """UPDATE revisions
           SET status='due'
           WHERE user_id=$1::uuid
           AND status='scheduled'
           AND due_at < now()""", u["id"])
    await q(
        """UPDATE users SET last_tick=$2
           WHERE id=$1::uuid""",
        u["id"], today)
    u["last_tick"] = today
    await cleanup_stale(u, now)
    return u

# ============================================================
# AI ROUTER
# ============================================================
ROUTER_SYS = """You convert a student's
Telegram message into ONE JSON action for a
study app. Output ONLY JSON:
{"intent":"...","fields":{...},
 "confidence":"high|medium|low",
 "reply": short reply or null}

Intents and fields (null for unstated):
- "log_session": {"subject","topic",
  "questions","correct","wrong","minutes",
  "day"} — day is "today" or "yesterday".
- "hw_add": {"title","subject","questions",
  "minutes","due"} — due as natural words.
- "hw_progress": {"title","done"}
- "test_add": {"name","subject","date",
  "total_marks"}
- "test_result": {"name","subject",
  "obtained","total"}
- "mistake_add": {"subject","topic","count",
  "mtype"} — mtype: conceptual, calculation,
  careless, memory, misread, guessing,
  time_pressure.
- "mistake_resolve": {"subject","topic"} —
  they say they FIXED previous mistakes.
- "class_cancel": {"which","date"} — which
  is "school" or "coaching"; date natural
  words like "tomorrow".
- "class_move": {"which","from","to"} —
  e.g. moving tomorrow's class to friday.
- "energy": {"level"} — energetic|normal|
  tired|exhausted.
- "quiz": {"topic","subject","count"}
- "tutor": {"question"}
- "query": {"view"} — plan, next, homework,
  tests, revision, analytics, mistakes,
  dashboard, syllabus.
- "syllabus": {"blocks":[
  {"subject":"...","topics":["..."]}]}
- "chat": {"reply":"short warm reply"}
- "none": {}

Rules: extract only stated facts; never
invent numbers; numbers are integers; prefer
the student's subject names from context;
if unsure about a field, set confidence
"low"."""


async def ai_context(u):
    parts = []
    subs = await get_subjects(u["id"])
    if subs:
        names = ", ".join(
            s["name"] for s in subs)
        parts.append("subjects: " + names)
    if u.get("exam_goal"):
        parts.append("goal: " + u["exam_goal"])
    weak = await qrows(
        """SELECT s.name subj, t.name top,
                  m.mastery
           FROM mastery m
           JOIN topics t
             ON t.id=m.topic_id
           JOIN subjects s
             ON s.id=m.subject_id
           WHERE m.user_id=$1::uuid
           AND m.events>0
           ORDER BY m.mastery ASC
           LIMIT 3""", u["id"])
    if weak:
        bits = []
        for r in weak:
            m = format(r["mastery"], ".0f")
            bits.append(r["subj"] + "/"
                        + r["top"] + " "
                        + m + "%")
        parts.append("weakest topics: "
                     + "; ".join(bits))
    due_rev = await qval(
        """SELECT COUNT(*) FROM revisions
           WHERE user_id=$1::uuid
           AND due_at<now()""", u["id"])
    if due_rev:
        parts.append(
            str(due_rev)
            + " revisions due")
    over = await qval(
        """SELECT COUNT(*) FROM homework
           WHERE user_id=$1::uuid
           AND status!='completed'
           AND due_at<now()""", u["id"])
    if over:
        parts.append(
            str(over) + " homework overdue")
    nt = await qrow(
        """SELECT name FROM tests
           WHERE user_id=$1::uuid
           AND status='scheduled'
           AND test_at>now()
           ORDER BY test_at LIMIT 1""",
        u["id"])
    if nt:
        parts.append("next test: "
                     + nt["name"])
    parts.append("energy today: "
                 + u["energy"])
    return "\n".join(parts)


async def ai_route(text, u):
    prompt = (ROUTER_SYS
              + "\n\nCONTEXT:\n"
              + await ai_context(u)
              + "\n\nMESSAGE:\n" + text)
    key = "rt:" + hashlib.md5(
        prompt.encode()).hexdigest()
    cached = _cache_get(key)
    if cached is not None:
        return cached
    raw = _load_json(
        await ai_call(prompt, max_tokens=700))
    _cache_set(key, raw)
    return raw

# ============================================================
# PENDING ACTIONS (short codes — callback
# data must stay under 64 bytes)
# ============================================================
async def new_pending(u, kind, fields,
                      origin="ai"):
    pid = str(uuid.uuid4())
    code = secrets.token_hex(5)
    await q(
        """INSERT INTO pending_actions
           (id, user_id, kind, payload,
            code)
           VALUES($1::uuid,$2::uuid,$3,
                  $4::jsonb,$5)""",
        pid, u["id"], kind,
        json.dumps({"fields": fields,
                    "origin": origin}),
        code)
    return code


async def get_pending(u, code):
    r = await qrow(
        """SELECT * FROM pending_actions
           WHERE user_id=$1::uuid
           AND code=$2""",
        u["id"], code)
    return dict(r) if r else None


async def set_pending_payload(code,
                              payload):
    await q(
        """UPDATE pending_actions
           SET payload=$2::jsonb
           WHERE code=$1""",
        code, json.dumps(payload))


def confirm_kb(code):
    yes = "cfm:" + code + ":yes"
    no = "cfm:" + code + ":no"
    ed = "cfm:" + code + ":edit"
    return IK([("✅ Confirm", yes),
               ("❌ Cancel", no)],
              [("✏️ Correct it", ed)])


async def subject_pick_card(u, chat, code,
                            header):
    subs = await get_subjects(u["id"])
    rows = []
    for s in subs[:6]:
        data = "subj:" + code + ":"
        data += str(s["id"])
        rows.append([(s["name"], data)])
    no = "cfm:" + code + ":no"
    rows.append([("❌ Cancel", no)])
    await send(chat,
               header
               + "\n\nWhich subject?",
               IK(*rows))


def card_lines(fields, order):
    out = []
    for k, label in order:
        v = fields.get(k)
        if v is not None and v != "":
            out.append(label + ": <b>"
                       + esc(v) + "</b>")
    return out

# ============================================================
# APPLY HANDLERS
# ============================================================
async def do_log_session(u, f):
    sid = f.get("subject_id")
    if not sid:
        return "Subject missing — cancelled."
    att = f.get("questions")
    cor = f.get("correct")
    mins = f.get("minutes") or 0
    when = now_tz()
    if f.get("day") == "yesterday":
        when = when - timedelta(days=1)
    topic_id = None
    topic_name = None
    if f.get("topic"):
        t = await find_topic(
            u["id"], sid, f["topic"])
        if not t:
            t = await upsert_topic(
                u["id"], sid, f["topic"])
        topic_id = t["id"]
        topic_name = t["name"]
    res = await save_evidence(
        u, sid, topic_id, att, cor, mins,
        "log", when=when,
        title="logged session")
    parts = ["✅ <b>Logged</b>"]
    if att and cor is not None:
        acc = res.get("acc", 0)
        parts.append(
            "❓ " + str(att)
            + " questions • ✅ "
            + str(cor) + " correct ("
            + format(acc, ".0f") + "%)")
    if mins:
        parts.append("⏱ " + fm(mins))
    if topic_name:
        parts.append("📚 Topic: "
                     + esc(topic_name))
    mast = res.get("mastery")
    if mast is not None:
        parts.append(
            "🧠 Mastery now: <b>"
            + format(mast, ".0f")
            + "%</b>")
    nr = res.get("next_rev")
    if nr:
        parts.append(
            "🔁 Next revision: "
            + nr.strftime("%d %b"))
    return "\n".join(parts)


async def do_hw_add(u, f):
    sid = f.get("subject_id")
    due = None
    if f.get("due"):
        d = parse_date(str(f["due"]),
                       today_d())
        if d:
            due = datetime.combine(
                d, dtime(23, 59), tzinfo=TZ)
    tq = f.get("questions")
    if tq:
        est = f.get("minutes") or max(
            15, min(60, 2 * tq))
    else:
        est = f.get("minutes") or 30
    await q(
        """INSERT INTO homework
           (user_id, subject_id, title,
            hw_type, total_q, est_minutes,
            due_at)
           VALUES($1::uuid,$2::uuid,$3,
                  $4,$5,$6,$7)""",
        u["id"], sid,
        (f.get("title")
         or "Homework").strip()[:120],
        f.get("hw_type") or "custom",
        tq, est, due)
    out = "📝 Saved: <b>" \
          + esc(f.get("title")) + "</b>"
    if due:
        out += " — due " + esc(f["due"])
    return out


async def do_hw_progress(u, f):
    title = (f.get("title") or "").strip()
    rows = []
    if title:
        rows = await qrows(
            """SELECT * FROM homework
               WHERE user_id=$1::uuid
               AND status NOT IN
                 ('completed')
               AND title ILIKE $2
               ORDER BY due_at""",
            u["id"], "%" + title + "%")
    if not rows:
        msg = ("Couldn't find that homework "
               "— check the title with "
               "/homework.")
        return msg, False
    hw = rows[0]
    done = int(f.get("done") or 0)
    total = hw["total_q"]
    newc = (hw["completed_q"] or 0) + done
    if total:
        newc = min(newc, total)
    completed = bool(total
                     and newc >= total)
    if completed:
        status = "completed"
    else:
        status = "in_progress"
    await q(
        """UPDATE homework
           SET completed_q=$2, status=$3
           WHERE id=$1::uuid""",
        str(hw["id"]), newc, status)
    prog = ""
    if total:
        prog = (" [" + str(newc) + "/"
                + str(total) + "]")
    if completed:
        tail = "done! 🎉"
    else:
        tail = "keep going."
    msg = ("📝 Progress saved: <b>"
           + esc(hw["title"]) + "</b>"
           + prog + " — " + tail)
    return msg, completed


async def do_test_add(u, f):
    sid = f.get("subject_id")
    when = None
    if f.get("date"):
        d = parse_date(str(f["date"]),
                       today_d())
        if d:
            when = datetime.combine(
                d, dtime(9, 0), tzinfo=TZ)
    await q(
        """INSERT INTO tests
           (user_id, subject_id, name,
            test_at, total_marks)
           VALUES($1::uuid,$2::uuid,$3,
                  $4,$5)""",
        u["id"], sid,
        (f.get("name")
         or "Test").strip()[:120],
        when, f.get("total_marks"))
    out = "🧪 Saved: <b>" \
          + esc(f.get("name")) + "</b>"
    if when:
        out += " — " \
               + when.strftime("%a %d %b")
    return out


async def do_test_result(u, f):
    name = (f.get("name") or "").strip()
    rows = await qrows(
        """SELECT * FROM tests
           WHERE user_id=$1::uuid
           ORDER BY test_at DESC
           NULLS LAST""", u["id"])
    test = None
    if name:
        for r in rows:
            if name.lower() \
                    in r["name"].lower():
                test = r
                break
    if not test and f.get("subject_id"):
        for r in rows:
            if str(r["subject_id"]) \
                    == str(f.get(
                        "subject_id")):
                test = r
                break
    if not test and rows:
        test = rows[0]
    if not test:
        return ("Couldn't find that test. "
                "Add it first: "
                "'test on friday'.")
    obt = f.get("obtained")
    tot = f.get("total") \
        or test["total_marks"]
    if obt is None or tot is None:
        return ("I need both obtained "
                "and total marks.")
    pct = 100.0 * obt / tot
    await q(
        """UPDATE tests
           SET obtained_marks=$2,
               total_marks=$3,
               status='completed'
           WHERE id=$1::uuid""",
        str(test["id"]), obt, tot)
    ev_att = 20
    ev_cor = round(ev_att * pct / 100)
    await save_evidence(
        u, test["subject_id"], None,
        ev_att, ev_cor, 0, "test",
        title="test: " + test["name"])
    if pct >= 80:
        verdict = "strong 💪"
    elif pct >= 60:
        verdict = "solid 👍"
    else:
        verdict = "needs work 🔧"
    base = ("🧪 <b>"
            + esc(test["name"])
            + "</b>: " + str(obt) + "/"
            + str(tot) + " ("
            + format(pct, ".0f")
            + "%) — " + verdict
            + "\nRecorded as evidence "
            + "(test weight).")
    try:
        prompt = ("A student scored "
                  + str(obt) + "/" + str(tot)
                  + " (" + format(pct, ".0f")
                  + "%) in '"
                  + test["name"]
                  + "'. Write ONE "
                  + "encouraging, specific "
                  + "sentence (max 25 words) "
                  + "about what to do next.")
        txt = await ai_call(
            prompt, json_mode=False,
            max_tokens=80)
        if txt and "<" not in txt:
            return (base + "\n💬 "
                    + esc(txt.strip()[:200]))
    except AIError:
        pass
    return base


async def do_mistake_add(u, f):
    sid = f.get("subject_id")
    if not sid:
        return "Which subject? Try again."
    topic_id = None
    if f.get("topic"):
        t = await find_topic(
            u["id"], sid, f["topic"])
        if not t:
            t = await upsert_topic(
                u["id"], sid, f["topic"])
        topic_id = t["id"]
    valid = ("conceptual", "calculation",
             "careless", "memory", "misread",
             "guessing", "time_pressure",
             "unknown")
    mtype = f.get("mtype") or "unknown"
    if mtype not in valid:
        mtype = "unknown"
    cnt = max(1, min(50,
                     int(f.get("count")
                         or 1)))
    fp = hashlib.md5(
        (str(u["id"]) + "|" + str(sid)
         + "|" + str(topic_id) + "|"
         + mtype).encode()).hexdigest()
    await q(
        """INSERT INTO mistakes
           (user_id, subject_id,
            topic_id, mtype, description,
            count, fingerprint)
           VALUES($1::uuid,$2::uuid,
                  $3::uuid,$4,$5,$6,$7)
           ON CONFLICT(user_id,
                       fingerprint)
           DO UPDATE SET
             count=mistakes.count
               +EXCLUDED.count,
             last_seen=now(),
             resolved=false""",
        u["id"], sid, topic_id, mtype,
        (f.get("description")
         or mtype)[:200], cnt, fp)
    reps = 0
    if topic_id:
        reps = await mistake_reps(
            u["id"], topic_id)
    warn = ""
    if reps >= 2:
        warn = ("\n⚠️ Repeated mistake — "
                "it now drives your "
                "revision priority.")
    return ("🧠 " + str(cnt)
            + " mistake(s) banked ("
            + mtype + ")." + warn)


async def do_mistake_resolve(u, f):
    sid = f.get("subject_id")
    topic = f.get("topic") or ""
    sql = """UPDATE mistakes
             SET resolved=true,
                 resolved_at=now()
             WHERE user_id=$1::uuid
             AND resolved=false"""
    args = [u["id"]]
    if sid:
        sql += " AND subject_id=$2::uuid"
        args.append(sid)
    if topic:
        n = len(args) + 1
        sql += (" AND topic_id IN ("
                "SELECT id FROM topics"
                " WHERE user_id=$1::uuid"
                " AND name ILIKE $" + str(n)
                + ")")
        args.append("%" + topic + "%")
    res = await q(sql, *args)
    n = 0
    m = re.search(r"UPDATE (\d+)", res or "")
    if m:
        n = int(m.group(1))
    if n == 0:
        return ("No open mistakes matched "
                "that — check /mistakes.")
    return ("✅ Marked " + str(n)
            + " mistake group(s) as "
            "resolved. They'll stop "
            "driving your priorities.")


async def do_class_op(u, f):
    op = f.get("op")
    which = f.get("which") or "coaching"
    if which not in ("school", "coaching"):
        which = "coaching"
    d = parse_date(str(f.get("date")
                       or "tomorrow"),
                   today_d())
    if not d:
        return "Which date? Try again."
    if op == "cancel":
        await q(
            """INSERT INTO schedule_changes
               (user_id, op, which,
                from_date)
               VALUES($1::uuid,'cancel',
                      $2,$3)""",
            u["id"], which, d)
        label = d.strftime("%a %d %b")
        return ("🗓 " + which
                + " cancelled on " + label
                + ". Your plan adapts.")
    to_d = None
    if f.get("to"):
        to_d = parse_date(
            str(f["to"]), today_d())
    if not to_d:
        return "Move to which date?"
    await q(
        """INSERT INTO schedule_changes
           (user_id, op, which,
            from_date, to_date)
           VALUES($1::uuid,'move',$2,
                  $3,$4)""",
        u["id"], which, d, to_d)
    f1 = d.strftime("%a %d %b")
    f2 = to_d.strftime("%a %d %b")
    return ("🗓 " + which + " moved: "
            + f1 + " → " + f2
            + ". Your plan adapts.")


async def do_syllabus(u, blocks):
    added_s = 0
    added_t = 0
    for b in blocks[:12]:
        sname = (b.get("subject")
                 or "").strip().title()
        if not sname:
            continue
        s = await upsert_subject(
            u["id"], sname)
        added_s += 1
        topics = (b.get("topics")
                  or [])[:60]
        for tname in topics:
            t = (tname or "").strip()
            if not t:
                continue
            await upsert_topic(
                u["id"], s["id"], t)
            added_t += 1
    return ("📚 Syllabus saved: "
            + str(added_s) + " subjects, "
            + str(added_t) + " topics.")


async def do_save_note(u, f):
    await q(
        """INSERT INTO resources
           (title, content, user_id)
           VALUES($1,$2,$3::uuid)""",
        (f.get("title") or "Note")[:80],
        (f.get("content") or "")[:4000],
        u["id"])
    return ("📎 Saved to your notes. "
            "/notes to browse.")

# ============================================================
# PLAN REFRESH (dynamic replanning)
# ============================================================
async def refresh_plan(u, chat, reason):
    today = today_d()
    exists = await qval(
        """SELECT 1 FROM daily_plans
           WHERE user_id=$1::uuid
           AND plan_date=$2""",
        u["id"], today)
    if not exists:
        return
    now = now_tz()
    mods = await load_day_mods(
        u["id"], today)
    avail, _ = day_minutes(u, today, mods)
    cands = await build_candidates(u, now)
    plan = build_plan(cands, avail,
                      u["energy"], 0, now)
    tasks = json.dumps(plan["tasks"])
    await q(
        """INSERT INTO daily_plans
           (user_id, plan_date,
            available_minutes, tasks)
           VALUES($1::uuid,$2,$3,$4::jsonb)
           ON CONFLICT(user_id, plan_date)
           DO UPDATE SET
             available_minutes=
               EXCLUDED.available_minutes,
             tasks=EXCLUDED.tasks,
             created_at=now()""",
        u["id"], today, plan["available"],
        tasks)
    lines = ["🔄 <b>Plan updated</b> ("
             + esc(reason) + ")", ""]
    for i, t in enumerate(
            plan["tasks"], 1):
        lines.append(str(i) + ". "
                     + esc(t["title"])
                     + " — " + fm(t["est"]))
    if not plan["tasks"]:
        lines.append("Nothing left today.")
    await send(chat, "\n".join(lines),
               IK([("▶️ Start Next",
                    "nav:next")]))

# ============================================================
# COMMANDS / VIEWS
# ============================================================
MENU_KB = IK(
    [("▶️ Start Next", "nav:next"),
     ("📅 Plan", "nav:plan")],
    [("🏠 Dashboard", "nav:dash"),
     ("📊 Analytics", "nav:an")],
    [("📝 Homework", "nav:hw"),
     ("🧪 Tests", "nav:tests"),
     ("🔁 Revision", "nav:rev")],
    [("🧠 Quiz me", "nav:quiz"),
     ("📚 Syllabus", "nav:syl"),
     ("🧨 Mistakes", "nav:mist")])


async def cmd_start(u, chat):
    if not u["onboarded"]:
        await ob_start(u, chat)
    else:
        await cmd_dashboard(u, chat)


async def cmd_help(u, chat):
    txt = (
        "🧭 <b>StudyOS</b>\n\n"
        "Talk to me normally:\n"
        "• \"I finished 50 physics "
        "questions, 39 correct\"\n"
        "• \"studied organic chemistry "
        "1h 30m\"\n"
        "• \"add homework: DPP 3, 40 "
        "questions, due friday\"\n"
        "• \"did 25 of 50 DPP\"\n"
        "• \"physics test on sunday\"\n"
        "• \"got 68 out of 75 in "
        "physics test\"\n"
        "• \"made 3 conceptual mistakes "
        "in rotation\"\n"
        "• \"fixed my rotation mistakes\"\n"
        "• \"school is cancelled "
        "tomorrow\"\n"
        "• \"move coaching to friday\"\n"
        "• \"I'm exhausted\"\n"
        "• \"quiz me on thermodynamics\"\n"
        "• \"explain rotational motion\"\n"
        "• \"what should I study?\"\n\n"
        "Commands: /plan /next /homework "
        "/tests /revision /mistakes "
        "/analytics /quiz /notes "
        "/syllabus /settings\n\n"
        "📷 Send a syllabus photo to "
        "import it.\n"
        "📄 Send a PDF syllabus.\n"
        "🎙 Send a voice note.")
    await send(chat, txt, MENU_KB)


async def cmd_dashboard(u, chat):
    now = now_tz()
    today = today_d()
    live = await live_session(u["id"])
    if live:
        mins = await live_minutes(u, now)
        title = live["title"] or "Study"
        await send(
            chat,
            "⏱ <b>SESSION ACTIVE</b>\n"
            + esc(title)
            + "\nRunning: " + fm(mins),
            IK([("⏸ Pause", "ses:pause"),
                ("✅ Finish", "ses:finish"),
                ("❌ Abandon",
                 "ses:abandon")]))
        return
    mods = await load_day_mods(
        u["id"], today)
    avail, busy = day_minutes(
        u, today, mods)
    done = await spent_today(u, today)
    remaining = max(0, avail - done)
    lines = ["🏠 <b>STUDYOS</b>", "",
             "Today's capacity: <b>"
             + fm(avail) + "</b>",
             "Studied: <b>"
             + fm(done) + "</b>",
             "Left: <b>"
             + fm(remaining) + "</b>"]
    if mods.get("school_off") \
            or mods.get("coach_off"):
        lines.append("🗓 Schedule changed "
                     "today — plan adapted")
    if u["energy"] in ("tired",
                       "exhausted"):
        lines.append("⚡ Energy: <b>"
                     + u["energy"]
                     + "</b> — light today")
    tests = await qrows(
        """SELECT name, test_at
           FROM tests
           WHERE user_id=$1::uuid
           AND status='scheduled'
           AND test_at > now()
             - interval '1 day'
           ORDER BY test_at LIMIT 2""",
        u["id"])
    if tests:
        lines += ["", "🧪 <b>TESTS</b>"]
        for t in tests:
            d = None
            if t["test_at"]:
                d = (t["test_at"]
                     - now).days
            if d == 0:
                tag = "TODAY"
            elif d is not None:
                tag = "in " + str(d) + "d"
            else:
                tag = "unscheduled"
            lines.append("• "
                         + esc(t["name"])
                         + " — " + tag)
    hw = await qrow(
        """SELECT COUNT(*) FILTER (
                     WHERE due_at
                       < now()) over,
                  COUNT(*) FILTER (
                     WHERE due_at >= now()
                     OR due_at
                       IS NULL) up
           FROM homework
           WHERE user_id=$1::uuid
           AND status NOT IN
             ('completed',
              'abandoned')""", u["id"])
    if hw and (hw["over"] or hw["up"]):
        lines += ["", "📝 <b>HOMEWORK</b>"]
        if hw["over"]:
            lines.append(
                "⚠️ " + str(hw["over"])
                + " overdue")
        if hw["up"]:
            lines.append(
                "🟡 " + str(hw["up"])
                + " open")
    rev = await qval(
        """SELECT COUNT(*)
           FROM revisions
           WHERE user_id=$1::uuid
           AND due_at < now()""",
        u["id"])
    if rev:
        lines += ["",
                  "🔁 <b>" + str(rev)
                  + "</b> revision(s) due"]
    kb = []
    if tests and tests[0]["test_at"]:
        d = (tests[0]["test_at"]
             - now).days
        if d <= 3:
            kb.append(
                [("🧪 TEST SOON — prep",
                  "nav:next")])
    if hw and hw["over"]:
        kb.append(
            [("⚠️ OVERDUE homework",
              "nav:hw")])
    kb += [[("▶️ START NEXT",
             "nav:next"),
            ("📅 Plan", "nav:plan")],
           [("📊 Analytics", "nav:an"),
            ("📚 More", "nav:menu")]]
    await send(chat, "\n".join(lines),
               IK(*kb))


async def build_candidates(u, now):
    cands = []
    tests = await qrows(
        """SELECT id, name, test_at
           FROM tests
           WHERE user_id=$1::uuid
           AND status='scheduled'
           AND test_at BETWEEN now()
             AND now()
               + interval '14 days'
           ORDER BY test_at""", u["id"])
    test_days = [t for t in tests
                 if t["test_at"]
                 and (t["test_at"]
                      - now).days >= 0]
    if test_days:
        min_test = min(
            (t["test_at"] - now).days
            for t in test_days)
    else:
        min_test = None
    for t in tests[:3]:
        d = 99
        if t["test_at"]:
            d = (t["test_at"] - now).days
        if d < 0:
            d = 0
        cands.append({
            "kind": "test_prep",
            "key": "test:" + str(t["id"]),
            "title": "Prepare — "
                     + t["name"],
            "est": 45, "subject": "",
            "subject_id": None,
            "topic_id": None,
            "meta": {"test_in_days": d}})
    rows = await qrows(
        """SELECT rv.id, rv.due_at,
                  rv.mode, rv.rev_type,
                  rv.topic_id,
                  rv.subject_id,
                  s.name subj,
                  t.name top,
                  m.mastery
           FROM revisions rv
           LEFT JOIN subjects s
             ON s.id=rv.subject_id
           LEFT JOIN topics t
             ON t.id=rv.topic_id
           LEFT JOIN mastery m
             ON m.user_id=rv.user_id
             AND m.topic_id=rv.topic_id
           WHERE rv.user_id=$1::uuid
           AND rv.due_at < now()
             + interval '1 day'
           ORDER BY rv.due_at
           LIMIT 25""", u["id"])
    for r in rows:
        od = max(0,
                 (now - r["due_at"]).days)
        if r["mode"] == "practice":
            kind = "quiz"
            est = 15
        else:
            kind = "revision"
            est = 20
        rt = r["rev_type"].replace(
            "_", " ")
        title = (r["subj"] or "")
        title += " — "
        title += (r["top"] or "")
        title += " — " + rt
        subj_id = r["subject_id"]
        cands.append({
            "kind": kind,
            "key": "rev:" + str(r["id"]),
            "title": title.strip(" —"),
            "est": est,
            "subject": r["subj"] or "",
            "chapter": r["top"] or "",
            "subject_id":
                str(subj_id) if subj_id
                else None,
            "topic_id":
                str(r["topic_id"]),
            "meta": {
                "overdue_days": od,
                "mastery": r["mastery"],
                "mode": r["mode"],
                "rev_type": r["rev_type"],
                "revision_id":
                    str(r["id"]),
                "topic": r["top"],
                "test_in_days": min_test}})
    rows = await qrows(
        """SELECT SUM(m.count) reps,
                  s.name subj,
                  t.name top,
                  m.subject_id sid,
                  m.topic_id tid
           FROM mistakes m
           LEFT JOIN subjects s
             ON s.id=m.subject_id
           LEFT JOIN topics t
             ON t.id=m.topic_id
           WHERE m.user_id=$1::uuid
           AND m.resolved=false
           GROUP BY s.name, t.name,
                    m.subject_id,
                    m.topic_id
           HAVING SUM(m.count) >= 2
           ORDER BY reps DESC
           LIMIT 5""", u["id"])
    for r in rows:
        subj_id = r["sid"]
        topic_id = r["tid"]
        cands.append({
            "kind": "mistake_review",
            "key": "mist:"
                   + str(topic_id),
            "title": "Mistake review — "
                     + (r["subj"] or "")
                     + " "
                     + (r["top"] or ""),
            "est": 25,
            "subject": r["subj"] or "",
            "chapter": r["top"] or "",
            "subject_id":
                str(subj_id) if subj_id
                else None,
            "topic_id":
                str(topic_id) if topic_id
                else None,
            "meta": {
                "reps": int(r["reps"]),
                "test_in_days": min_test}})
    rows = await qrows(
        """SELECT h.id, h.title,
                  h.due_at, h.total_q,
                  h.completed_q,
                  h.est_minutes,
                  s.name subj,
                  h.subject_id sid
           FROM homework h
           LEFT JOIN subjects s
             ON s.id=h.subject_id
           WHERE h.user_id=$1::uuid
           AND h.status NOT IN
             ('completed','abandoned')
           ORDER BY h.due_at
             NULLS LAST
           LIMIT 20""", u["id"])
    for r in rows:
        rem = None
        if r["total_q"]:
            rem = (r["total_q"]
                   - r["completed_q"])
        if r["est_minutes"]:
            est = r["est_minutes"]
        elif rem:
            est = max(15,
                      min(60, 2 * rem))
        else:
            est = 30
        subj_id = r["sid"]
        cands.append({
            "kind": "homework",
            "key": "hw:" + str(r["id"]),
            "title": r["title"],
            "est": est,
            "subject": r["subj"] or "",
            "subject_id":
                str(subj_id) if subj_id
                else None,
            "topic_id": None,
            "meta": {
                "due_at": r["due_at"],
                "remaining": rem,
                "homework_id":
                    str(r["id"]),
                "test_in_days": min_test}})
    rows = await qrows(
        """SELECT t.id tid,
                  t.name top,
                  t.subject_id sid,
                  s.name subj,
                  m.mastery,
                  m.last_evidence
           FROM topics t
           JOIN subjects s
             ON s.id=t.subject_id
           LEFT JOIN mastery m
             ON m.user_id=t.user_id
             AND m.topic_id=t.id
           WHERE t.user_id=$1::uuid
           ORDER BY m.mastery ASC
             NULLS FIRST
           LIMIT 8""", u["id"])
    for r in rows:
        stale = 999
        if r["last_evidence"]:
            le = r["last_evidence"].date()
            stale = (now.date()
                     - le).days
        cands.append({
            "kind": "study",
            "key": "study:"
                   + str(r["tid"]),
            "title": "Study — "
                     + r["subj"]
                     + " — "
                     + r["top"],
            "est": 45,
            "subject": r["subj"],
            "chapter": r["top"],
            "subject_id":
                str(r["sid"]),
            "topic_id":
                str(r["tid"]),
            "meta": {
                "mastery": r["mastery"],
                "stale_days": stale,
                "test_in_days": min_test}})
    return cands


async def cmd_next(u, chat,
                   max_minutes=None,
                   exclude=None):
    now = now_tz()
    today = today_d()
    live = await live_session(u["id"])
    if live:
        await cmd_dashboard(u, chat)
        return
    mods = await load_day_mods(
        u["id"], today)
    avail, _ = day_minutes(
        u, today, mods)
    spent = await spent_today(u, today)
    lmins = await live_minutes(u, now)
    remaining = max(
        0, avail - spent - lmins)
    if max_minutes:
        remaining = min(remaining,
                        max_minutes)
    cands = await build_candidates(
        u, now)
    scored = []
    for c in cands:
        sc, rs = score_task(
            c, now, u["energy"])
        scored.append((c, sc, rs))
    pick = choose_next(scored, remaining,
                       exclude)
    if not pick:
        await send(
            chat,
            "Nothing left that fits right "
            "now — rest is also strategy. "
            "🌙 Log something with '50 "
            "questions 39 correct'.",
            MENU_KB)
        return
    c = pick[0]
    sc = pick[1]
    rs = pick[2]
    code = await new_pending(
        u, "task_start",
        {"cand": c, "score": sc,
         "reasons": rs}, "engine")
    why = "\n".join(
        "• " + esc(r) for r in rs)
    icons = {"revision": "🔁",
             "quiz": "🧠",
             "homework": "📝",
             "test_prep": "🧪",
             "mistake_review": "🧨",
             "study": "📖"}
    icon = icons.get(c["kind"], "▶️")
    txt = ("▶️ <b>START NEXT</b>\n\n"
           + icon + " <b>"
           + esc(c["title"]) + "</b>\n"
           "⏱ " + fm(c["est"])
           + " • fits your "
           + fm(remaining)
           + " left\n\n"
           "<b>Why:</b>\n" + why)
    go = "go:" + code
    alt = "go:" + code + ":alt"
    await send(chat, txt, IK(
        [("▶️ Start", go),
         ("🔄 Another", alt)],
        [("🏠 Menu", "nav:menu")]))


async def start_live_session(
        u, chat, cand):
    existing = await live_session(
        u["id"])
    if existing:
        await cmd_dashboard(u, chat)
        return
    now = now_tz()
    await qrow(
        """INSERT INTO study_sessions
           (user_id, subject_id,
            topic_id, source, status,
            title, started_at)
           VALUES($1::uuid,$2::uuid,
                  $3::uuid,'live',
                  'active',$4,$5)
           RETURNING id""",
        u["id"], cand.get("subject_id"),
        cand.get("topic_id"),
        cand["title"], now)
    await send(
        chat,
        "⏱ <b>Session started</b> — "
        + esc(cand["title"])
        + "\nTap ✅ when done. I'll ask "
        "how many questions you solved.",
        IK([("⏸ Pause", "ses:pause"),
            ("✅ Finish", "ses:finish")],
           [("❌ Abandon",
             "ses:abandon")]))


async def cmd_plan(u, chat):
    now = now_tz()
    today = today_d()
    mods = await load_day_mods(
        u["id"], today)
    avail, busy = day_minutes(
        u, today, mods)
    cands = await build_candidates(
        u, now)
    last = await qval(
        """SELECT MAX(created_at::date)
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'""",
        u["id"])
    missed = 0
    if last and (today - last).days > 1:
        missed = (today - last).days - 1
    plan = build_plan(cands, avail,
                      u["energy"],
                      missed, now)
    tasks = json.dumps(plan["tasks"])
    await q(
        """INSERT INTO daily_plans
           (user_id, plan_date,
            available_minutes, tasks)
           VALUES($1::uuid,$2,$3,
                  $4::jsonb)
           ON CONFLICT(user_id,
                        plan_date)
           DO UPDATE SET
             available_minutes=
               EXCLUDED.available_minutes,
             tasks=EXCLUDED.tasks,
             created_at=now()""",
        u["id"], today,
        plan["available"], tasks)
    lines = ["📅 <b>PLAN — "
             + today.strftime(
                 "%a %d %b")
             + "</b>",
             "Capacity: <b>"
             + fm(plan["available"])
             + "</b> (after school/"
             "coaching/meals)",
             "Planned: <b>"
             + fm(plan["allocated"])
             + "</b>", ""]
    for i, t in enumerate(
            plan["tasks"], 1):
        lines.append(str(i) + ". "
                     + esc(t["title"])
                     + " — "
                     + fm(t["est"]))
    if plan["missed"]:
        lines.append(
            "\n<i>Missed "
            + str(plan["missed"])
            + " day(s) — kept urgent "
            "items, spread the rest.</i>")
    if plan["deferred"]:
        lines += ["",
                  "<i>Kept for later:</i>"]
        for t, r in plan["deferred"][:5]:
            lines.append("• " + esc(t)
                         + " — "
                         + esc(r))
    await send(chat, "\n".join(lines),
               IK([("▶️ Start Next",
                    "nav:next"),
                   ("🔄 Replan",
                    "nav:plan")]))


async def cmd_homework(u, chat):
    rows = await qrows(
        """SELECT h.*, s.name subj
           FROM homework h
           LEFT JOIN subjects s
             ON s.id=h.subject_id
           WHERE h.user_id=$1::uuid
           AND h.status NOT IN
             ('completed','abandoned')
           ORDER BY h.due_at
             NULLS LAST
           LIMIT 15""", u["id"])
    now = now_tz()
    if not rows:
        await send(chat,
                   "📝 No open homework. 🎉\n"
                   "Add: 'homework DPP 4, "
                   "30 questions, due "
                   "monday'.", MENU_KB)
        return
    lines = ["📝 <b>HOMEWORK</b>", ""]
    for r in rows:
        mark = "•"
        if r["due_at"] \
                and r["due_at"] < now:
            mark = "⚠️"
        line = mark + " " \
               + esc(r["title"])
        if r["subj"]:
            line += " (" \
                    + esc(r["subj"]) + ")"
        if r["total_q"]:
            done = r["completed_q"] or 0
            pct = 100 * done / r["total_q"]
            line += (" ["
                     + str(done) + "/"
                     + str(r["total_q"])
                     + "] " + bar(pct, 6))
        if r["due_at"]:
            d = (r["due_at"].date()
                 - now.date()).days
            if d == 0:
                line += " — due today!"
            elif d > 0:
                line += " — in " \
                        + str(d) + "d"
            else:
                line += (" — "
                         + str(-d)
                         + "d OVERDUE")
        lines.append(line)
    lines += ["",
              "<i>Report: 'did 25 of 50 "
              "DPP'. Finish: 'did rest of "
              "DPP'.</i>"]
    await send(chat, "\n".join(lines),
               MENU_KB)


async def cmd_tests(u, chat):
    rows = await qrows(
        """SELECT t.*, s.name subj
           FROM tests t
           LEFT JOIN subjects s
             ON s.id=t.subject_id
           WHERE t.user_id=$1::uuid
           ORDER BY t.test_at DESC
           NULLS LAST LIMIT 12""",
        u["id"])
    if not rows:
        await send(chat,
                   "🧪 No tests yet. Add: "
                   "'physics test on "
                   "sunday'.", MENU_KB)
        return
    now = now_tz()
    up = [r for r in rows
          if r["test_at"]
          and r["test_at"] >= now
          and r["status"] == "scheduled"]
    done = [r for r in rows
            if r["status"] == "completed"]
    lines = ["🧪 <b>TESTS</b>", ""]
    if up:
        lines.append("<b>Upcoming</b>")
        for r in sorted(
                up,
                key=lambda x: x["test_at"]):
            d = (r["test_at"].date()
                 - now.date()).days
            if d == 0:
                tag = "TODAY"
            else:
                tag = "in " + str(d) + "d"
            lines.append(
                "• " + esc(r["name"])
                + " ("
                + esc(r["subj"] or "")
                + ") — " + tag)
        lines.append("")
    if done:
        lines.append("<b>Results</b>")
        for r in done[:6]:
            if r["total_marks"]:
                om = r["obtained_marks"] \
                    or 0
                pct = (100 * om
                       / r["total_marks"])
                lines.append(
                    "• " + esc(r["name"])
                    + " — " + str(om) + "/"
                    + str(r["total_marks"])
                    + " ("
                    + format(pct, ".0f")
                    + "%) " + bar(pct, 6))
    await send(chat, "\n".join(lines),
               MENU_KB)


async def cmd_revision(u, chat):
    rows = await qrows(
        """SELECT rv.*, s.name subj,
                  t.name top
           FROM revisions rv
           LEFT JOIN subjects s
             ON s.id=rv.subject_id
           LEFT JOIN topics t
             ON t.id=rv.topic_id
           WHERE rv.user_id=$1::uuid
           AND rv.due_at < now()
             + interval '2 days'
           ORDER BY rv.due_at
           LIMIT 12""", u["id"])
    if not rows:
        await send(chat,
                   "🔁 No revision due. It "
                   "appears automatically once "
                   "you log questions — "
                   "accuracy decides when it "
                   "returns.", MENU_KB)
        return
    now = now_tz()
    lines = ["🔁 <b>REVISION QUEUE</b>",
             ""]
    for r in rows:
        od = max(0,
                 (now - r["due_at"]).days)
        mode = ""
        if r["mode"] == "practice":
            mode = " 🧠practice"
        over = ""
        if od:
            over = (" (overdue "
                    + str(od) + "d) 🔴")
        rt = r["rev_type"].replace(
            "_", " ")
        lines.append(
            "• " + esc(r["subj"] or "")
            + " / "
            + esc(r["top"] or "")
            + " — " + rt + over + mode)
    await send(chat, "\n".join(lines),
               IK([("▶️ Start Next",
                    "nav:next")]))


async def cmd_mistakes(u, chat):
    rows = await qrows(
        """SELECT SUM(m.count) c,
                  m.mtype,
                  s.name subj,
                  t.name top
           FROM mistakes m
           LEFT JOIN subjects s
             ON s.id=m.subject_id
           LEFT JOIN topics t
             ON t.id=m.topic_id
           WHERE m.user_id=$1::uuid
           AND m.resolved=false
           GROUP BY m.mtype, s.name,
                    t.name
           ORDER BY c DESC LIMIT 12""",
        u["id"])
    if not rows:
        await send(chat,
                   "🧨 No mistakes banked. "
                   "Report: '3 conceptual "
                   "mistakes in rotation'.",
                   MENU_KB)
        return
    lines = ["🧨 <b>MISTAKE BANK</b>",
             ""]
    for r in rows:
        lines.append(
            "• " + esc(r["subj"] or "")
            + " / "
            + esc(r["top"] or "")
            + " — " + r["mtype"]
            + " × " + str(r["c"]))
    lines += ["",
              "<i>2+ repeats = it takes "
              "over your revision. Say "
              "'fixed my X mistakes' to "
              "clear.</i>"]
    await send(chat, "\n".join(lines),
               MENU_KB)


async def cmd_syllabus(u, chat):
    rows = await qrows(
        """SELECT s.name subj,
                  COUNT(t.id) n,
                  (SELECT COUNT(*)
                   FROM mastery m
                   WHERE m.user_id=
                       s.user_id
                   AND m.subject_id=s.id
                   AND m.events>0) tracked
           FROM subjects s
           LEFT JOIN topics t
             ON t.subject_id=s.id
           WHERE s.user_id=$1::uuid
           GROUP BY s.name, s.user_id,
                    s.id
           ORDER BY s.name""", u["id"])
    if not rows:
        await send(chat,
                   "📚 No subjects yet. Send "
                   "a syllabus photo or PDF, "
                   "or type:\n"
                   "<code>Physics: Rotation, "
                   "SHM</code>", MENU_KB)
        return
    lines = ["📚 <b>SYLLABUS</b>", ""]
    for r in rows:
        lines.append(
            "• <b>"
            + esc(r["subj"])
            + "</b> — "
            + str(r["n"]) + " topics, "
            + str(r["tracked"])
            + " with data")
    lines += ["",
              "<i>Send a syllabus photo "
              "or PDF to import "
              "more.</i>"]
    await send(chat, "\n".join(lines),
               MENU_KB)


async def cmd_notes(u, chat):
    rows = await qrows(
        """SELECT id, title
           FROM resources
           WHERE user_id=$1::uuid
           ORDER BY created_at DESC
           LIMIT 10""", u["id"])
    if not rows:
        await send(chat,
                   "📎 No notes yet. Send "
                   "any photo — I'll read "
                   "it and can save the "
                   "text.", MENU_KB)
        return
    kb_rows = []
    for r in rows[:8]:
        t = r["title"] or "note"
        label = "📄 " + t[:40]
        data = "note:" + str(r["id"])
        kb_rows.append([(label, data)])
    await send(chat,
               "📎 <b>YOUR NOTES</b>",
               IK(*kb_rows))


async def cmd_analytics(u, chat,
                        days=7):
    today = today_d()
    wk_start = sod(
        today - timedelta(days=days))
    wk_end = sod(
        today + timedelta(days=1))
    prev_start = sod(
        today - timedelta(
            days=2 * days))
    r = await qrow(
        """SELECT COALESCE(
                    SUM(duration_minutes),
                    0) mins,
                  COALESCE(
                    SUM(questions_attempted),
                    0) q,
                  COALESCE(
                    SUM(questions_correct),
                    0) c
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >= $2
           AND created_at < $3""",
        u["id"], wk_start, wk_end)
    prev = await qval(
        """SELECT COALESCE(
                    SUM(duration_minutes),
                    0)
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >= $2
           AND created_at < $3""",
        u["id"], prev_start, wk_start)
    if not r or (r["mins"] == 0
                 and r["q"] == 0):
        await send(chat,
                   "📊 Not enough data yet "
                   "— log a few sessions "
                   "first.", MENU_KB)
        return
    mins = int(r["mins"])
    qn = int(r["q"])
    cn = int(r["c"])
    acc = 100.0 * cn / qn if qn else 0
    prev_i = int(prev or 0)
    ref = max(days * 150,
              int(prev_i * 120 // 100))
    if mins >= prev_i:
        trend = "↑"
    else:
        trend = "↓"
    qref = days * 600 // 7
    lines = ["📊 <b>LAST "
             + str(days) + " DAYS</b>",
             "",
             "Study      "
             + bar(min(100,
                       100 * mins / ref))
             + " " + fm(mins) + " "
             + trend,
             "Questions  "
             + bar(min(100,
                       100 * qn / qref))
             + " " + str(qn),
             "Accuracy   " + bar(acc)
             + " " + format(acc, ".0f")
             + "%"]
    subs = await qrows(
        """SELECT s.name,
                  SUM(ss.questions_attempted)
                    q,
                  SUM(ss.questions_correct)
                    c
           FROM study_sessions ss
           JOIN subjects s
             ON s.id=ss.subject_id
           WHERE ss.user_id=$1::uuid
           AND ss.status='finished'
           AND ss.created_at >= $2
           AND ss.created_at < $3
           AND ss.questions_attempted
             > 0
           GROUP BY s.name
           ORDER BY SUM(
             ss.questions_correct)::float
             / NULLIF(SUM(
               ss.questions_attempted),0)
             ASC""",
        u["id"], wk_start, wk_end)
    if subs:
        lines += ["", "<b>By subject</b>"]
        for srow in subs:
            sa = 0
            if srow["q"]:
                sa = (100
                      * (srow["c"] or 0)
                      / srow["q"])
            nm = srow["name"]
            lines.append(
                nm.ljust(11) + " "
                + bar(sa, 8) + " "
                + format(sa, ".0f")
                + "% (" + str(srow["q"])
                + "q)")
    mast = await qrows(
        """SELECT s.name subj,
                  t.name top,
                  m.mastery
           FROM mastery m
           JOIN topics t
             ON t.id=m.topic_id
           JOIN subjects s
             ON s.id=m.subject_id
           WHERE m.user_id=$1::uuid
           AND m.events>0
           ORDER BY m.mastery ASC
           LIMIT 5""", u["id"])
    if mast:
        lines += ["",
                  "<b>Weakest topics</b>"]
        for m in mast:
            mv = m["mastery"]
            lines.append(
                "• " + esc(m["subj"])
                + "/" + esc(m["top"])
                + " — "
                + format(mv, ".0f")
                + "% " + bar(mv, 8))
    dates = await qrows(
        """SELECT DISTINCT
                  created_at::date d
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND (duration_minutes > 0
                OR questions_attempted
                  > 0)
           ORDER BY d DESC LIMIT 90""",
        u["id"])
    streak = 0
    if dates:
        ds = {r["d"] for r in dates}
        cur = today
        if today not in ds:
            cur = today - timedelta(
                days=1)
        while cur in ds:
            streak += 1
            cur -= timedelta(days=1)
    if streak:
        lines += ["",
                  "🔥 <b>"
                  + str(streak)
                  + "-day streak</b>"]
    rep = await qrows(
        """SELECT t.name top,
                  SUM(m.count) c
           FROM mistakes m
           JOIN topics t
             ON t.id=m.topic_id
           WHERE m.user_id=$1::uuid
           AND m.resolved=false
           GROUP BY t.name
           ORDER BY c DESC LIMIT 3""",
        u["id"])
    if rep:
        lines += ["",
                  "⚠️ <b>Repeated "
                  "errors</b>"]
        for i, r in enumerate(rep, 1):
            lines.append(
                str(i) + ". "
                + esc(r["top"])
                + " — " + str(r["c"]))
    kb = IK([("7 days", "anp:7"),
             ("30 days", "anp:30")],
            [("🏠 Menu", "nav:menu")])
    await send(chat, "\n".join(lines), kb)


async def cmd_settings(u, chat):
    lines = ["⚙️ <b>SETTINGS</b>", ""]
    nm = (u["display_name"]
          or u["first_name"])
    lines.append("👤 "
                 + esc(nm or "Student"))
    goal = esc(u["exam_goal"] or "—")
    if u["exam_date"]:
        days = (u["exam_date"]
                - today_d()).days
        goal += " (" + str(days)
        goal += "d away)"
    lines.append("🎯 " + goal)
    lines.append(
        "😴 "
        + u["wake_time"].strftime(
            "%H:%M")
        + "–"
        + u["sleep_time"].strftime(
            "%H:%M"))
    if u["school_days"]:
        lines.append(
            "🏫 "
            + u["school_start"].strftime(
                "%H:%M")
            + "–"
            + u["school_end"].strftime(
                "%H:%M"))
    if u["coaching_days"]:
        lines.append(
            "📖 "
            + u["coaching_start"]
            .strftime("%H:%M")
            + "–"
            + u["coaching_end"]
            .strftime("%H:%M"))
    lines.append("⚡ Energy: "
                 + u["energy"])
    lines += ["",
              "<i>Editable by telling me, "
              "e.g. 'wake at 6' or "
              "'school 8-2 "
              "mon-sat'.</i>"]
    await send(chat, "\n".join(lines),
               IK([("🔄 Restart "
                    "onboarding",
                    "ob:restart")],
                  [("🧨 WIPE ALL DATA",
                    "reset:ask")]))

# ============================================================
# QUIZ
# ============================================================
async def quiz_label(u):
    r = await qrow(
        """SELECT s.name subj,
                  t.name top
           FROM mastery m
           JOIN topics t
             ON t.id=m.topic_id
           JOIN subjects s
             ON s.id=m.subject_id
           WHERE m.user_id=$1::uuid
           AND m.events>0
           ORDER BY m.mastery ASC
           LIMIT 1""", u["id"])
    if r:
        label = (r["subj"] + " — "
                 + r["top"])
        return label, r["subj"], r["top"]
    r = await qrow(
        """SELECT t.name top,
                  s.name subj
           FROM topics t
           JOIN subjects s
             ON s.id=t.subject_id
           WHERE t.user_id=$1::uuid
           LIMIT 1""", u["id"])
    if r:
        label = (r["subj"] + " — "
                 + r["top"])
        return label, r["subj"], r["top"]
    return "mixed revision", None, None


async def start_quiz(u, chat,
                     topic_text,
                     count=5,
                     subject_id=None,
                     topic_id=None,
                     topic_label=None,
                     flavor=None):
    label = topic_label or topic_text
    if not label:
        label, sname, tname = \
            await quiz_label(u)
    count = max(1, min(8, count))
    goal = (u.get("exam_goal")
            or "competitive exam")
    style = flavor or "exam-level"
    prompt = (
        "Generate " + str(count)
        + " multiple-choice questions on \""
        + label + "\" for a " + goal
        + " student. Return ONLY JSON: "
        + '{"questions":[{"question":'
        + '"...","options":["a","b",'
        + '"c","d"],"answer":0,'
        + '"explanation":"short why"}]} '
        + "Rules: exactly 4 options; "
        + '"answer" is the 0-based '
        + "index of the correct option; "
        + "exactly one correct; no "
        + "trick options; "
        + style + " difficulty.")
    key = "qz:" + hashlib.md5(
        prompt.encode()).hexdigest()
    try:
        raw = _cache_get(key)
        if raw is None:
            raw = _load_json(
                await ai_call(
                    prompt,
                    max_tokens=200 * count))
            _cache_set(key, raw)
    except AIError:
        await send(chat,
                   "🧠 My quiz brain is "
                   "rate-limited — try "
                   "again in a minute.",
                   MENU_KB)
        return
    qs = []
    for x in (raw.get("questions")
              or [])[:count]:
        try:
            opts = [str(o)[:150]
                    for o in
                    x["options"][:4]]
            ans = int(x["answer"])
            ok = (len(opts) == 4
                  and 0 <= ans <= 3
                  and x.get("question"))
            if ok:
                qs.append({
                    "question": str(
                        x["question"])[:400],
                    "options": opts,
                    "answer": ans,
                    "explanation": str(
                        x.get(
                            "explanation",
                            ""))[:400]})
        except Exception:
            continue
    if len(qs) < 3:
        await send(chat,
                   "🧠 Couldn't build a "
                   "clean quiz for that — "
                   "try a more specific "
                   "topic.", MENU_KB)
        return
    if not topic_id and topic_text:
        subs = await get_subjects(
            u["id"])
        sid = subject_id
        if not sid and len(subs) == 1:
            sid = subs[0]["id"]
        if sid:
            t = await find_topic(
                u["id"], sid, topic_text)
            if not t:
                t = await upsert_topic(
                    u["id"], sid,
                    topic_text)
            topic_id = t["id"]
            subject_id = sid
    qid = await qval(
        """INSERT INTO quizzes
           (user_id, subject_id,
            topic_id, topic_label,
            questions)
           VALUES($1::uuid,$2::uuid,
                  $3::uuid,$4,
                  $5::jsonb)
           RETURNING id::text""",
        u["id"], subject_id, topic_id,
        label, json.dumps(qs))
    await send_quiz_q(chat, qid, qs, 0,
                      label)


async def send_quiz_q(chat, qid, qs,
                     idx, label):
    q = qs[idx]
    letters = "ABCD"
    body = ("🧠 <b>QUIZ — "
            + esc(label) + "</b> ("
            + str(idx + 1) + "/"
            + str(len(qs)) + ")\n\n"
            + esc(q["question"]) + "\n\n")
    for i, o in enumerate(q["options"]):
        body += letters[i] + ". " \
                + esc(o) + "\n"
    row = []
    for i in range(4):
        data = ("quiz:" + qid + ":"
                + str(idx) + ":"
                + str(i))
        row.append((letters[i], data))
    stop = "quiz:" + qid + ":stop"
    kb = IK(row, [("🛑 Stop", stop)])
    await send(chat, body, kb)


async def quiz_answer(u, chat, qid,
                      qi, opt):
    row = await qrow(
        """SELECT * FROM quizzes
           WHERE id=$1::uuid
           AND user_id=$2::uuid""",
        qid, u["id"])
    if not row or row["status"] != \
            "active":
        return
    qs = json.loads(row["questions"])
    answers = json.loads(
        row["answers"])
    if len(answers) != qi:
        return
    answers.append(opt)
    await q(
        """UPDATE quizzes
           SET answers=$2::jsonb
           WHERE id=$1::uuid""",
        qid, json.dumps(answers))
    if len(answers) >= len(qs):
        await quiz_finish(
            u, chat, qid, qs,
            answers, row)
    else:
        await send_quiz_q(
            chat, qid, qs,
            len(answers),
            row["topic_label"])


async def quiz_finish(u, chat, qid,
                      qs, answers, row):
    correct = 0
    for a, qq in zip(answers, qs):
        if a == qq["answer"]:
            correct += 1
    att = len(answers)
    await q(
        """UPDATE quizzes
           SET status='done'
           WHERE id=$1::uuid""", qid)
    if att:
        await save_evidence(
            u, row["subject_id"],
            row["topic_id"],
            att, correct, 0, "quiz",
            title="quiz: "
                  + str(row[
                      "topic_label"]))
    letters = "ABCD"
    recap = []
    for a, qq in zip(answers, qs):
        if a == qq["answer"]:
            mark = "✅"
        else:
            mark = "❌"
        idx = qq["answer"]
        letter = letters[idx]
        option = qq["options"][idx]
        line = (mark + " " + letter
                + ". "
                + esc(option))
        if qq["explanation"]:
            line += " — " \
                    + esc(qq[
                        "explanation"])
        recap.append(line)
    acc = 100 * correct // att \
        if att else 0
    lines = ["🧠 <b>Quiz done: "
             + str(correct) + "/"
             + str(att) + "</b> ("
             + str(acc) + "%)", ""]
    lines += recap[:8]
    lines += ["",
              "Logged as evidence — "
              "mastery & revision "
              "updated."]
    await send(chat, "\n".join(lines),
               IK([("▶️ Start Next",
                    "nav:next")]))

# ============================================================
# ONBOARDING
# ============================================================
OB_STEPS = [
    ("name",
     "👤 What should I call you?",
     True, "text"),
    ("exam",
     "🎯 What are you preparing "
     "for?\n(e.g. 'JEE 2027', "
     "'NEET', 'Boards')",
     True, "text"),
    ("exam_date",
     "📅 Main exam date?\n"
     "(e.g. '24 may 2027')\n"
     "/skip if not fixed",
     False, "date"),
    ("subjects",
     "📚 Your subjects, "
     "comma-separated\n"
     "(e.g. 'Physics, Chemistry, "
     "Maths')",
     True, "subjects"),
    ("wake",
     "🌅 Wake time? (e.g. '6:30')",
     True, "time"),
    ("sleep",
     "🌙 Sleep time? (e.g. "
     "'23:00')",
     True, "time"),
    ("school",
     "🏫 School hours?\n"
     "(e.g. '8-2 mon-sat')\n"
     "/skip if none",
     False, "class"),
    ("coaching",
     "📖 Coaching hours?\n"
     "(e.g. '5-8 mon,wed,fri')\n"
     "/skip if none",
     False, "class"),
]


async def ob_start(u, chat):
    first = OB_STEPS[0]
    await q(
        """UPDATE users
           SET ob_state='{}'::jsonb,
               ob_step=$2
           WHERE id=$1::uuid""",
        u["id"], first[0])
    await send(
        chat,
        "🧠 <b>Welcome to StudyOS"
        "</b>\n\n"
        "I learn from what you "
        "actually do — not from plans "
        "you forget. A few quick "
        "questions, one at a time. "
        "Optional ones you can "
        "/skip.\n\n" + first[1])


async def ob_summary(u, chat):
    st = u["ob_state"] or {}
    lines = ["<b>Here's what I've "
             "understood.</b>", ""]
    if st.get("name"):
        lines.append("👤 "
                     + esc(st["name"]))
    if st.get("exam"):
        line = "🎯 " + esc(st["exam"])
        if st.get("exam_date"):
            line += " — " \
                    + esc(st[
                        "exam_date"])
        lines.append(line)
    if st.get("subjects"):
        lines.append(
            "📚 "
            + esc(", ".join(
                st["subjects"])))
    if st.get("school"):
        lines.append("🏫 "
                     + esc(st[
                         "school"]))
    if st.get("coaching"):
        lines.append("📖 "
                     + esc(st[
                         "coaching"]))
    lines.append("😴 "
                 + str(st.get("wake"))
                 + " – "
                 + str(st.get("sleep")))
    lines += ["", "All correct?"]
    await send(chat, "\n".join(lines),
               IK([("✅ Confirm",
                    "ob:confirm")],
                  [("🔄 Start over",
                    "ob:restart")]))


def ob_next_step(key):
    keys = [s[0] for s in OB_STEPS]
    if key not in keys:
        return "__done"
    i = keys.index(key)
    if i + 1 < len(keys):
        return keys[i + 1]
    return "__done"


async def ob_prompt(u, chat, step):
    if step == "__done":
        await ob_summary(u, chat)
        return
    for s in OB_STEPS:
        if s[0] == step:
            await send(chat, s[1])
            return
    await ob_summary(u, chat)


async def ob_handle(u, chat, text):
    st = dict(u["ob_state"] or {})
    step = u["ob_step"]
    if not step:
        first = OB_STEPS[0]
        await q(
            """UPDATE users
               SET ob_step=$2
               WHERE id=$1::uuid""",
            u["id"], first[0])
        await send(chat, first[1])
        return
    if step == "__done":
        await ob_summary(u, chat)
        return
    step_def = None
    for s in OB_STEPS:
        if s[0] == step:
            step_def = s
            break
    if not step_def:
        step_def = OB_STEPS[0]
    key = step_def[0]
    prompt = step_def[1]
    required = step_def[2]
    kind = step_def[3]
    t = (text or "").strip()
    if t.lower() in SKIP_WORDS \
            and not required:
        nxt = ob_next_step(key)
        await q(
            """UPDATE users
               SET ob_step=$2,
                   ob_state=$3::jsonb
               WHERE id=$1::uuid""",
            u["id"], nxt,
            json.dumps(st))
        await ob_prompt(u, chat, nxt)
        return
    if kind == "text":
        if not t:
            await send(chat,
                       "A short answer "
                       "works. "
                       + prompt)
            return
        st[key] = t[:60]
    elif kind == "date":
        d = parse_date(t, today_d())
        if not d:
            await send(chat,
                       "Couldn't read "
                       "that date — try "
                       "'24 may 2027'.")
            return
        st[key] = d.isoformat()
    elif kind == "subjects":
        names = [x.strip().title()
                 for x in re.split(
                     r"[,;]+", t)
                 if x.strip()]
        if not names:
            await send(chat,
                       "List them like: "
                       "Physics, "
                       "Chemistry, Maths")
            return
        st[key] = names[:8]
    elif kind == "time":
        tm = parse_time(t)
        if not tm:
            await send(chat,
                       "Try a time like "
                       "'6:30'.")
            return
        st[key] = tm.strftime("%H:%M")
    elif kind == "class":
        pc = parse_class(t)
        if not pc:
            await send(chat,
                       "Try: '8-2 "
                       "mon-sat' (time + "
                       "days). Or /skip.")
            return
        st[key] = t[:60]
        st[key + "_data"] = {
            "s": pc[0].strftime(
                "%H:%M"),
            "e": pc[1].strftime(
                "%H:%M"),
            "d": pc[2]}
    nxt = ob_next_step(key)
    await q(
        """UPDATE users
           SET ob_step=$2,
               ob_state=$3::jsonb
           WHERE id=$1::uuid""",
        u["id"], nxt, json.dumps(st))
    await ob_prompt(u, chat, nxt)


async def ob_commit(u, chat):
    st = u["ob_state"] or {}
    for name in (st.get("subjects")
                 or [])[:8]:
        await upsert_subject(
            u["id"], name)
    sch = st.get("school_data")
    coa = st.get("coaching_data")
    exam_d = None
    if st.get("exam_date"):
        try:
            exam_d = date.fromisoformat(
                st["exam_date"])
        except ValueError:
            pass
    wake = parse_time(
        st.get("wake") or "06:30")
    if not wake:
        wake = dtime(6, 30)
    sleep = parse_time(
        st.get("sleep") or "23:00")
    if not sleep:
        sleep = dtime(23, 0)
    sch_s = None
    sch_e = None
    coa_s = None
    coa_e = None
    sch_d = None
    coa_d = None
    if sch:
        sch_s = dtime.fromisoformat(
            sch["s"])
        sch_e = dtime.fromisoformat(
            sch["e"])
        sch_d = sch["d"]
    if coa:
        coa_s = dtime.fromisoformat(
            coa["s"])
        coa_e = dtime.fromisoformat(
            coa["e"])
        coa_d = coa["d"]
    await q(
        """UPDATE users
           SET display_name=$2,
               exam_goal=$3,
               exam_date=$4,
               wake_time=$5,
               sleep_time=$6,
               school_start=$7,
               school_end=$8,
               school_days=$9,
               coaching_start=$10,
               coaching_end=$11,
               coaching_days=$12,
               onboarded=true,
               ob_step=NULL
           WHERE id=$1::uuid""",
        u["id"], st.get("name"),
        st.get("exam"), exam_d,
        wake, sleep, sch_s, sch_e,
        sch_d, coa_s, coa_e, coa_d)
    await send(
        chat,
        "✅ <b>You're set up.</b>\n\n"
        "Now just talk to me:\n"
        "• \"I finished 50 physics "
        "questions, 39 correct\"\n"
        "• 📷 send a syllabus photo\n"
        "• 📄 send a syllabus PDF\n"
        "• \"what should I study?\"",
        IK([("▶️ Start Next",
             "nav:next")],
           [("🏠 Dashboard",
             "nav:dash")]))

# ============================================================
# PHOTO / VOICE / PDF
# ============================================================
PHOTO_SYS = """You analyze a photo for
a study app. Read ONLY what is clearly
visible. Never invent content. Return
ONLY JSON:
{"type":"syllabus|test|homework|notes|
 unknown",
 "text":"all readable text (or empty)",
 "syllabus":[{"subject":"...",
   "topics":["..."]}],
 "test":{"name":"...","subject":"...",
   "total":null,"obtained":null},
 "homework":{"title":"...",
   "subject":"...","questions":null},
 "confidence":"high|medium|low"}
Use "syllabus" when the image lists
chapters per subject. Use "test" for
papers with visible marks. Use "notes"
otherwise."""

PDF_SYS = """Extract subjects and
topics/chapters from this syllabus text.
Return ONLY JSON:
{"blocks":[{"subject":"...",
  "topics":["..."]}]}
Only include content actually present
in the text. Never invent entries."""


async def handle_photo(u, chat, msg):
    photo = msg["photo"][-1]
    try:
        img = await get_file_bytes(
            photo["file_id"])
    except Exception as e:
        LOG.warning("photo dl: %s", e)
        await send(chat,
                   "Couldn't download "
                   "that photo — try "
                   "again.")
        return
    await send(chat,
               "🔍 Reading image…")
    try:
        part = types.Part.from_bytes(
            data=img,
            mime_type="image/jpeg")
        contents = [PHOTO_SYS, part]
        raw = _load_json(
            await ai_call(
                contents,
                max_tokens=2000))
    except AIError:
        await send(chat,
                   "My vision service is "
                   "rate-limited — try "
                   "again in a minute.",
                   MENU_KB)
        return
    ptype = (raw.get("type")
             or "unknown").lower()
    conf = (raw.get("confidence")
            or "low").lower()
    text = (raw.get("text")
            or "")[:2500]
    if ptype == "unknown" \
            or conf == "low":
        await send(chat,
                   "I can't read this "
                   "confidently enough "
                   "to save anything. "
                   "Could you type the "
                   "key parts?", MENU_KB)
        return
    if ptype == "syllabus" \
            and raw.get("syllabus"):
        blocks = raw["syllabus"][:12]
        preview = ""
        for b in blocks:
            topics = ", ".join(
                esc(t) for t in
                (b.get("topics")
                 or [])[:10])
            preview += ("• <b>"
                        + esc(b.get(
                            "subject"))
                        + "</b>: "
                        + topics + "\n")
        code = await new_pending(
            u, "syllabus",
            {"blocks": blocks}, "ai")
        await send(
            chat,
            "📷 <b>Syllabus detected"
            "</b>\n\n" + preview
            + "\nConfidence: "
            + conf.upper(),
            confirm_kb(code))
        return
    if ptype == "test" \
            and raw.get("test"):
        t = raw["test"]
        f = {k: v
             for k, v in t.items()
             if v is not None}
        code = await new_pending(
            u, "test_result", f, "ai")
        order = [("name", "Test"),
                 ("subject",
                  "Subject"),
                 ("total", "Total"),
                 ("obtained",
                  "Obtained")]
        lines = card_lines(f, order)
        await send(
            chat,
            "📷 <b>Test paper "
            "detected</b>\n\n"
            + "\n".join(lines)
            + "\n\nConfidence: "
            + conf.upper(),
            confirm_kb(code))
        return
    if ptype == "homework" \
            and raw.get("homework"):
        h = raw["homework"]
        f = {k: v
             for k, v in h.items()
             if v is not None}
        subs = await get_subjects(
            u["id"])
        sid, sname = resolve_subject(
            f.get("subject"), subs)
        f["subject_id"] = sid
        if sname:
            f["subject"] = sname
        code = await new_pending(
            u, "hw_add", f, "ai")
        if not sid and subs:
            header = ("📷 <b>Homework "
                      "detected</b>\n"
                      + esc(f.get(
                          "title")))
            await subject_pick_card(
                u, chat, code, header)
            return
        order = [("title",
                  "Homework"),
                 ("subject",
                  "Subject"),
                 ("questions",
                  "Questions")]
        lines = card_lines(f, order)
        await send(
            chat,
            "📷 <b>Homework "
            "detected</b>\n\n"
            + "\n".join(lines)
            + "\n\nConfidence: "
            + conf.upper(),
            confirm_kb(code))
        return
    code = await new_pending(
        u, "save_note",
        {"title": "Photo note",
         "content": text}, "ai")
    await send(chat,
               "📷 I read this as "
               "notes:\n\n<i>"
               + esc(text[:800])
               + "</i>\n\nConfidence: "
               + conf.upper(),
               confirm_kb(code))


async def handle_document(u, chat,
                          msg):
    doc = msg.get("document") or {}
    name = (doc.get("file_name")
            or "").lower()
    if not name.endswith(".pdf"):
        await send(chat,
                   "📄 I can read PDFs — "
                   "send the file as a "
                   "PDF document.",
                   MENU_KB)
        return
    try:
        data = await get_file_bytes(
            doc["file_id"])
    except Exception as e:
        LOG.warning("pdf dl: %s", e)
        await send(chat,
                   "Couldn't download "
                   "that PDF.")
        return
    await send(chat,
               "📄 Reading PDF…")
    try:
        from pypdf import PdfReader
        reader = PdfReader(
            io.BytesIO(data))
        pages = reader.pages[:30]
        chunks = []
        for p in pages:
            chunks.append(
                p.extract_text() or "")
        text = "\n".join(chunks)
    except Exception as e:
        LOG.warning("pdf parse: %s", e)
        await send(chat,
                   "Couldn't read that "
                   "PDF (scanned?) — "
                   "send photos of the "
                   "pages instead.",
                   MENU_KB)
        return
    text = text.strip()[:15000]
    if len(text) < 80:
        await send(chat,
                   "This PDF has no "
                   "extractable text "
                   "(scanned?) — send "
                   "page photos instead.",
                   MENU_KB)
        return
    try:
        prompt = (PDF_SYS
                  + "\n\nTEXT:\n"
                  + text[:8000])
        raw = _load_json(
            await ai_call(
                prompt,
                max_tokens=1500))
    except AIError:
        await send(chat,
                   "My AI brain is "
                   "rate-limited — try "
                   "again in a minute.",
                   MENU_KB)
        return
    blocks = raw.get("blocks") or []
    if blocks:
        blocks = blocks[:12]
        preview = ""
        for b in blocks:
            topics = ", ".join(
                esc(x) for x in
                (b.get("topics")
                 or [])[:10])
            preview += ("• <b>"
                        + esc(b.get(
                            "subject"))
                        + "</b>: "
                        + topics + "\n")
        code = await new_pending(
            u, "syllabus",
            {"blocks": blocks},
            "pdf")
        await send(
            chat,
            "📄 <b>Syllabus found in "
            "PDF</b>\n\n" + preview,
            confirm_kb(code))
        return
    code = await new_pending(
        u, "save_note",
        {"title": "PDF note",
         "content": text[:4000]},
        "pdf")
    await send(chat,
               "📄 No subject/topic "
               "structure found — "
               "save the text as a "
               "note instead?",
               confirm_kb(code))


async def handle_voice(u, chat, msg):
    try:
        audio = await get_file_bytes(
            msg["voice"]["file_id"])
    except Exception as e:
        LOG.warning("voice dl: %s", e)
        await send(chat,
                   "Couldn't download "
                   "that voice note.")
        return
    await send(chat,
               "🎙 Transcribing…")
    try:
        part = types.Part.from_bytes(
            data=audio,
            mime_type="audio/ogg")
        contents = [
            "Transcribe this audio "
            "to plain text. Output "
            "only the transcription.",
            part]
        text = await ai_call(
            contents, json_mode=False,
            max_tokens=300)
        text = text.strip()
    except AIError:
        await send(chat,
                   "Voice transcription "
                   "is rate-limited right "
                   "now — type it "
                   "instead?", MENU_KB)
        return
    if not text:
        await send(chat,
                   "I couldn't hear that "
                   "clearly — try typing "
                   "it.")
        return
    await send(chat, "🎙 <i>"
               + esc(text[:300])
               + "</i>")
    await handle_text_msg(
        u, chat, text)

# ============================================================
# TEXT ROUTING
# ============================================================
async def handle_text_msg(u, chat,
                          text):
    if not u["onboarded"]:
        await ob_handle(u, chat, text)
        return
    t = text.strip()
    low = t.lower()
    pend = await qrow(
        """SELECT *
           FROM pending_actions
           WHERE user_id=$1::uuid
           AND kind='session_log'
           AND status='pending'
           ORDER BY created_at DESC
           LIMIT 1""", u["id"])
    if pend:
        await wizard_session_log(
            u, chat, dict(pend), t)
        return
    parts = low.split()
    cmd = ""
    if parts:
        cmd = parts[0].lstrip("/")
        cmd = cmd.split("@")[0]
    if cmd == "start":
        await cmd_start(u, chat)
        return
    if cmd in ("menu", "home"):
        await send(chat,
                   "🏠 <b>StudyOS</b>",
                   MENU_KB)
        return
    if cmd == "help":
        await cmd_help(u, chat)
        return
    if cmd == "plan":
        await cmd_plan(u, chat)
        return
    if cmd in ("next", "startnext"):
        await cmd_next(u, chat)
        return
    if cmd == "homework":
        await cmd_homework(u, chat)
        return
    if cmd == "tests":
        await cmd_tests(u, chat)
        return
    if cmd in ("revision", "rev"):
        await cmd_revision(u, chat)
        return
    if cmd in ("mistakes",
               "errorbank"):
        await cmd_mistakes(u, chat)
        return
    if cmd in ("analytics", "stats"):
        await cmd_analytics(
            u, chat, days=7)
        return
    if cmd == "quiz":
        rest = " ".join(
            low.split()[1:])
        await start_quiz(
            u, chat, rest or None)
        return
    if cmd == "syllabus":
        await cmd_syllabus(u, chat)
        return
    if cmd in ("notes", "resources"):
        await cmd_notes(u, chat)
        return
    if cmd in ("settings", "profile"):
        await cmd_settings(u, chat)
        return
    # ---- deterministic fast paths ----
    energy = detect_energy(t)
    if energy and len(low.split()) \
            <= 6:
        await set_energy(
            u, chat, energy)
        return
    pat = (r"(what should i study|"
           r"what do i study|"
           r"whats next|what's next|"
           r"next task|start next)")
    if re.search(pat, low):
        await cmd_next(u, chat)
        return
    if low in ("plan",
               "make me a plan",
               "today's plan",
               "todays plan",
               "make today's plan"):
        await cmd_plan(u, chat)
        return
    m = re.search(
        r"(?:i (?:have|got|"
        r"only have)|only)\s+(\d+)"
        r"\s*(?:minutes|min|mins)\b",
        low)
    if m:
        await cmd_next(
            u, chat,
            max_minutes=int(
                m.group(1)))
        return
    pq = parse_questions(t)
    if pq and (pq[1] is not None
               or pq[2] is not None):
        await candidate_log_session(
            u, chat,
            questions=pq[0],
            correct=pq[1],
            minutes=parse_duration(t),
            topic_maybe=t,
            origin="fast")
        return
    stud = re.search(
        r"\b(studied|revised|"
        r"solved|did)\b", low)
    if stud and parse_duration(t) \
            and not pq:
        await candidate_log_session(
            u, chat,
            questions=None,
            correct=None,
            minutes=parse_duration(t),
            topic_maybe=t,
            origin="fast")
        return
    # ---- AI router ----
    try:
        routed = await ai_route(t, u)
    except AIError:
        await send(chat,
                   "My AI brain is "
                   "rate-limited — wait "
                   "a minute and resend. "
                   "(Buttons still work: "
                   "🏠)", MENU_KB)
        return
    intent = (routed.get("intent")
              or "none").lower()
    f = routed.get("fields") or {}
    conf = (routed.get("confidence")
            or "medium").lower()
    if intent in ("chat", "none"):
        reply = routed.get("reply")
        if reply:
            await send(chat,
                       esc(reply),
                       MENU_KB)
        else:
            await cmd_dashboard(
                u, chat)
        return
    if intent == "query":
        view = (f.get("view")
                or "dashboard").lower()
        views = {"plan": cmd_plan,
                 "next": cmd_next,
                 "homework":
                     cmd_homework,
                 "tests": cmd_tests,
                 "revision":
                     cmd_revision,
                 "analytics":
                     cmd_analytics,
                 "mistakes":
                     cmd_mistakes,
                 "syllabus":
                     cmd_syllabus,
                 "dashboard":
                     cmd_dashboard}
        fn = views.get(view,
                       cmd_dashboard)
        await fn(u, chat)
        return
    if intent == "energy":
        lvl = (f.get("level")
               or "").lower()
        if lvl in ("energetic",
                   "normal", "tired",
                   "exhausted"):
            await set_energy(
                u, chat, lvl)
        else:
            await cmd_dashboard(
                u, chat)
        return
    if intent == "tutor":
        await tutor_reply(
            u, chat,
            f.get("question")
            or t)
        return
    if intent == "quiz":
        await start_quiz(
            u, chat,
            f.get("topic")
            or f.get("subject"),
            count=int(
                f.get("count")
                or 5))
        return
    if intent == "log_session":
        await candidate_log_session(
            u, chat,
            questions=clampi(
                f.get("questions"),
                1, 999),
            correct=clampi(
                f.get("correct"),
                0, 999),
            minutes=clampi(
                f.get("minutes"),
                1, 960),
            subject=f.get("subject"),
            topic=f.get("topic"),
            day=f.get("day"),
            origin="ai",
            confidence=conf)
        return
    if intent == "hw_add":
        await candidate_hw(
            u, chat, f, conf)
        return
    if intent == "hw_progress":
        if f.get("done") is None:
            await send(chat,
                       "How many did you "
                       "do? e.g. 'did 25 "
                       "of 50 DPP'")
            return
        code = await new_pending(
            u, "hw_progress", f, "ai")
        title = esc(f.get("title")
                    or "homework")
        await send(
            chat,
            "📝 <b>" + title
            + "</b>\nDone now: <b>"
            + str(f.get("done"))
            + "</b> more",
            confirm_kb(code))
        return
    if intent == "test_add":
        await candidate_test_add(
            u, chat, f, conf)
        return
    if intent == "test_result":
        clean = {k: v
                 for k, v in
                 f.items()
                 if v is not None}
        code = await new_pending(
            u, "test_result",
            clean, "ai")
        order = [("name", "Test"),
                 ("subject",
                  "Subject"),
                 ("obtained",
                  "Obtained"),
                 ("total", "Total")]
        lines = card_lines(f, order)
        flag = ""
        if conf != "high":
            flag = " ⚠️"
        await send(
            chat,
            "I found:\n\n"
            + "\n".join(lines)
            + "\n\nConfidence: <b>"
            + conf.upper() + flag
            + "</b>",
            confirm_kb(code))
        return
    if intent == "mistake_add":
        await candidate_mistake(
            u, chat, f, conf)
        return
    if intent == "mistake_resolve":
        clean = {k: v
                 for k, v in
                 f.items()
                 if v is not None}
        subs = await get_subjects(
            u["id"])
        sid, sname = resolve_subject(
            f.get("subject"), subs)
        clean["subject_id"] = sid
        if sname:
            clean["subject"] = sname
        code = await new_pending(
            u, "mistake_resolve",
            clean, "ai")
        order = [("subject",
                  "Subject"),
                 ("topic", "Topic")]
        lines = card_lines(clean,
                           order)
        await send(
            chat,
            "Mark these as "
            "resolved?\n\n"
            + "\n".join(lines),
            confirm_kb(code))
        return
    if intent in ("class_cancel",
                  "class_move"):
        await candidate_class(
            u, chat, f,
            intent, conf)
        return
    if intent == "syllabus":
        blocks = f.get("blocks") or []
        if not blocks:
            await cmd_syllabus(
                u, chat)
            return
        code = await new_pending(
            u, "syllabus",
            {"blocks": blocks}, "ai")
        preview = ""
        for b in blocks[:10]:
            topics = ", ".join(
                esc(x) for x in
                (b.get("topics")
                 or [])[:10])
            preview += ("• <b>"
                        + esc(b.get(
                            "subject"))
                        + "</b>: "
                        + topics + "\n")
        await send(
            chat,
            "📚 <b>Syllabus</b>\n\n"
            + preview,
            confirm_kb(code))
        return
    await cmd_dashboard(u, chat)


def clampi(v, lo, hi):
    if v is None:
        return None
    try:
        v = int(v)
    except (TypeError, ValueError):
        return None
    return max(lo, min(hi, v))


async def set_energy(u, chat, level):
    await q(
        """UPDATE users
           SET energy=$2,
               energy_set_at=now()
           WHERE id=$1::uuid""",
        u["id"], level)
    u["energy"] = level
    msgs = {
        "exhausted":
            "Understood. Today: light "
            "recall and mistake review "
            "only — full effort again "
            "tomorrow. 🌙",
        "tired":
            "Got it — I'll keep today "
            "short and light.",
        "energetic":
            "Nice. I'll use that "
            "energy. 🔥",
        "normal":
            "Noted. Back to normal "
            "load."}
    await send(chat, msgs[level],
               IK([("▶️ Start Next",
                    "nav:next")]))


async def note_excerpts(u, question):
    words = re.findall(
        r"[a-zA-Z]{4,}",
        question.lower())
    words = list(set(words))[:12]
    if not words:
        return []
    rows = await qrows(
        """SELECT title, content
           FROM resources
           WHERE user_id=$1::uuid
           AND content
             ILIKE ANY($2::text[])
           ORDER BY created_at DESC
           LIMIT 2""",
        u["id"], words)
    out = []
    for r in rows:
        out.append(
            (r["title"] or "note")
            + ": "
            + (r["content"]
               or "")[:700])
    return out


async def tutor_reply(u, chat,
                      question):
    ctx = await ai_context(u)
    excerpts = []
    try:
        excerpts = await note_excerpts(
            u, question)
    except Exception:
        pass
    prompt = ("STUDENT CONTEXT:\n"
              + ctx + "\n\n")
    if excerpts:
        prompt += ("YOUR SAVED NOTES "
                   "(ground the answer "
                   "in these when "
                   "relevant):\n"
                   + "\n---\n".join(
                       excerpts)
                   + "\n\n")
    prompt += ("Answer this study "
               "question. Be clear and "
               "concise (max 200 "
               "words), one concrete "
               "example, one common "
               "mistake to avoid. Never "
               "invent statistics about "
               "the student.\n\n"
               "QUESTION: "
               + question)
    key = "tu:" + hashlib.md5(
        prompt.encode()).hexdigest()
    try:
        txt = _cache_get(key)
        if txt is None:
            txt = await ai_call(
                prompt,
                json_mode=False,
                max_tokens=800)
            _cache_set(key, txt)
        title = esc(question[:80])
        await send(
            chat,
            "📖 <b>" + title
            + "</b>\n\n"
            + esc(txt),
            IK([("▶️ Back to "
                 "studying",
                 "nav:next")]))
    except AIError:
        rows = await qrows(
            """SELECT s.name subj,
                      t.name top,
                      m.mastery
               FROM mastery m
               JOIN topics t
                 ON t.id=m.topic_id
               JOIN subjects s
                 ON s.id=m.subject_id
               WHERE m.user_id=$1::uuid
               AND m.events>0
               ORDER BY m.mastery ASC
               LIMIT 3""", u["id"])
        lines = ["My tutor brain is "
                 "rate-limited right "
                 "now. Your weakest "
                 "topics:"]
        for r in rows:
            mv = r["mastery"]
            lines.append(
                "• " + r["subj"] + "/"
                + r["top"] + " — "
                + format(mv, ".0f")
                + "%")
        if not rows:
            lines.append(
                "• log more sessions "
                "to build mastery "
                "data")
        await send(chat,
                   "\n".join(lines),
                   MENU_KB)


async def candidate_log_session(
        u, chat, questions, correct,
        minutes, subject=None,
        topic=None, day=None,
        topic_maybe=None,
        origin="fast",
        confidence="high"):
    subs = await get_subjects(u["id"])
    if not subs:
        await send(chat,
                   "You have no subjects "
                   "yet — send 'Physics: "
                   "Rotation, SHM' or a "
                   "syllabus photo first.",
                   MENU_KB)
        return
    if (questions is not None
            and correct is not None
            and correct > questions):
        correct = None
    if not subject and topic_maybe:
        for s in subs:
            if s["name"].lower() \
                    in topic_maybe \
                    .lower():
                subject = s["name"]
                break
    sid, sname = resolve_subject(
        subject, subs)
    day_v = day if day \
        == "yesterday" else None
    fields = {"questions": questions,
              "correct": correct,
              "minutes": minutes,
              "subject":
                  sname or subject,
              "subject_id": sid,
              "topic": topic,
              "day": day_v}
    code = await new_pending(
        u, "log_session", fields,
        origin)
    lines = ["I found:", ""]
    if questions:
        lines.append("Questions: <b>"
                     + str(questions)
                     + "</b>")
        if correct is not None:
            pct = (100 * correct
                   // questions)
            lines.append(
                "Correct: <b>"
                + str(correct)
                + "</b> ("
                + str(pct) + "%)")
    else:
        lines.append(
            "Questions: <b>not "
            "given</b>")
    if minutes:
        lines.append("Duration: <b>"
                     + fm(minutes)
                     + "</b>")
    subj_label = (sname or subject
                  or "?")
    lines.append("Subject: <b>"
                 + esc(subj_label)
                 + "</b>")
    if topic:
        lines.append("Topic: <b>"
                     + esc(topic)
                     + "</b>")
    if (origin == "fast" and sid
            and confidence == "high"):
        res = await do_log_session(
            u, fields)
        await q(
            """UPDATE pending_actions
               SET status='confirmed'
               WHERE code=$1""",
            code)
        ucode = await new_pending(
            u, "undo_log",
            {"fields": fields},
            "fast")
        undo = "cfm:" + ucode + ":undo"
        await send(
            chat,
            res + "\n\n<i>(Tap ↩️ if "
            "wrong)</i>",
            IK([("↩️ Undo", undo)]))
        return
    if not sid:
        await subject_pick_card(
            u, chat, code,
            "\n".join(lines))
        return
    flag = ""
    if confidence != "high":
        flag = " ⚠️"
    await send(
        chat, "\n".join(lines)
        + "\n\nConfidence: <b>"
        + confidence.upper() + flag
        + "</b>", confirm_kb(code))


async def candidate_hw(u, chat, f,
                       conf):
    subs = await get_subjects(
        u["id"])
    sid, sname = resolve_subject(
        f.get("subject"), subs)
    title = (f.get("title")
             or "").strip()[:120]
    if not title:
        await send(chat,
                   "What's it called? "
                   "e.g. 'add DPP 4, 40 "
                   "questions, due "
                   "friday'")
        return
    fields = {
        "title": title,
        "subject":
            sname or f.get("subject"),
        "subject_id": sid,
        "questions": clampi(
            f.get("questions"),
            1, 999),
        "minutes": clampi(
            f.get("minutes"),
            5, 480),
        "due": f.get("due"),
        "hw_type": f.get("hw_type")
        or "custom"}
    code = await new_pending(
        u, "hw_add", fields, "ai")
    if not sid and subs:
        await subject_pick_card(
            u, chat, code,
            "📝 <b>"
            + esc(title) + "</b>")
        return
    order = [("title", "Homework"),
             ("subject", "Subject"),
             ("questions",
              "Questions"),
             ("due", "Due"),
             ("minutes",
              "Est. minutes")]
    lines = card_lines(fields, order)
    flag = ""
    if conf != "high":
        flag = " ⚠️"
    await send(chat,
               "I found:\n\n"
               + "\n".join(lines)
               + "\n\nConfidence: <b>"
               + conf.upper() + flag
               + "</b>",
               confirm_kb(code))


async def candidate_test_add(
        u, chat, f, conf):
    subs = await get_subjects(
        u["id"])
    sid, sname = resolve_subject(
        f.get("subject"), subs)
    name = (f.get("name")
            or "Test").strip()[:120]
    fields = {
        "name": name,
        "subject":
            sname or f.get("subject"),
        "subject_id": sid,
        "date": f.get("date"),
        "total_marks": clampi(
            f.get("total_marks"),
            1, 1000)}
    code = await new_pending(
        u, "test_add", fields, "ai")
    if not sid and subs:
        await subject_pick_card(
            u, chat, code,
            "🧪 <b>" + esc(name)
            + "</b>")
        return
    order = [("name", "Test"),
             ("subject", "Subject"),
             ("date", "When"),
             ("total_marks", "Marks")]
    lines = card_lines(fields, order)
    flag = ""
    if conf != "high":
        flag = " ⚠️"
    await send(chat,
               "I found:\n\n"
               + "\n".join(lines)
               + "\n\nConfidence: <b>"
               + conf.upper() + flag
               + "</b>",
               confirm_kb(code))


async def candidate_mistake(
        u, chat, f, conf):
    subs = await get_subjects(
        u["id"])
    sid, sname = resolve_subject(
        f.get("subject"), subs)
    mtype = (f.get("mtype")
             or "unknown").lower()
    fields = {
        "subject":
            sname or f.get("subject"),
        "subject_id": sid,
        "topic": f.get("topic"),
        "count": clampi(
            f.get("count"), 1, 50)
        or 1,
        "mtype": mtype,
        "description":
            (f.get("description")
             or "")[:200]}
    code = await new_pending(
        u, "mistake_add", fields,
        "ai")
    if not sid and subs:
        cnt = fields["count"]
        header = ("🧨 <b>"
                  + str(cnt) + " × "
                  + esc(mtype)
                  + " mistake(s)</b>")
        await subject_pick_card(
            u, chat, code, header)
        return
    order = [("count", "Count"),
             ("mtype", "Type"),
             ("subject", "Subject"),
             ("topic", "Topic")]
    lines = card_lines(fields, order)
    flag = ""
    if conf != "high":
        flag = " ⚠️"
    await send(chat,
               "I found:\n\n"
               + "\n".join(lines)
               + "\n\nConfidence: <b>"
               + conf.upper() + flag
               + "</b>",
               confirm_kb(code))


async def candidate_class(
        u, chat, f, intent, conf):
    which = (f.get("which")
             or "coaching").lower()
    if which not in ("school",
                     "coaching"):
        which = "coaching"
    today = today_d()
    if intent == "class_move":
        frm = parse_date(
            str(f.get("from")
                or "tomorrow"),
            today)
        to = parse_date(
            str(f.get("to") or ""),
            today)
        if not to:
            await send(chat,
                       "Move it to which "
                       "day? e.g. 'move "
                       "coaching to "
                       "friday'")
            return
        if not frm:
            frm = today \
                + timedelta(days=1)
        fields = {"op": "move",
                  "which": which,
                  "date": frm
                  .isoformat(),
                  "to": to.isoformat()}
        f1 = frm.strftime("%a %d %b")
        f2 = to.strftime("%a %d %b")
        lines = ["Move <b>"
                 + which + "</b>",
                 "From: <b>" + f1
                 + "</b>",
                 "To: <b>" + f2
                 + "</b>"]
    else:
        d = parse_date(
            str(f.get("date")
                or "tomorrow"),
            today)
        if not d:
            await send(chat,
                       "Which day? e.g. "
                       "'school cancelled "
                       "tomorrow'")
            return
        fields = {"op": "cancel",
                  "which": which,
                  "date": d.isoformat()}
        dl = d.strftime("%a %d %b")
        lines = ["Cancel <b>"
                 + which + "</b>",
                 "Date: <b>" + dl
                 + "</b>"]
    code = await new_pending(
        u, "class_op", fields, "ai")
    flag = ""
    if conf != "high":
        flag = " ⚠️"
    await send(chat,
               "\n".join(lines)
               + "\n\nConfidence: <b>"
               + conf.upper() + flag
               + "</b>",
               confirm_kb(code))

# ============================================================
# SESSION CONTROLS
# ============================================================
async def wizard_session_log(
        u, chat, pend, text):
    code = pend["code"]
    if text.lower().startswith(
            "/skip"):
        await q(
            """UPDATE pending_actions
               SET status='confirmed'
               WHERE code=$1""",
            code)
        await send(chat,
                   "Logged as time-only. "
                   "🏠", MENU_KB)
        return
    # fast formats: "25 18" or
    # "25 questions 18 correct"
    att = None
    cor = None
    m = re.match(
        r"^(\d+)\s+(\d+)$",
        text.strip())
    if m:
        att = int(m.group(1))
        cor = int(m.group(2))
    else:
        pq = parse_questions(text)
        if pq:
            att = pq[0]
            cor = pq[1]
    if att and cor is not None \
            and cor <= att:
        payload = json.loads(
            pend["payload"])
        sess_id = payload.get(
            "session_id")
        await q(
            """UPDATE study_sessions
               SET questions_attempted
                   =$2,
                   questions_correct=$3
               WHERE id=$1::uuid""",
            sess_id, att, cor)
        s = await qrow(
            """SELECT *
               FROM study_sessions
               WHERE id=$1::uuid""",
            sess_id)
        res = {}
        if (s and s["topic_id"]
                and s["subject_id"]):
            await recompute(
                u["id"],
                s["subject_id"],
                s["topic_id"])
            acc = cor / att
            nr = await apply_revision(
                u["id"],
                s["subject_id"],
                s["topic_id"],
                acc, now_tz())
            mrow = await qrow(
                """SELECT mastery,
                          events
                   FROM mastery
                   WHERE user_id=
                     $1::uuid
                   AND topic_id=
                     $2::uuid""",
                u["id"],
                s["topic_id"])
            if mrow:
                res = {
                    "mastery":
                        mrow["mastery"],
                    "events":
                        mrow["events"],
                    "next_rev": nr}
        elif s and s["subject_id"]:
            await recompute(
                u["id"],
                s["subject_id"])
            mrow = await qrow(
                """SELECT mastery
                   FROM mastery
                   WHERE user_id=
                     $1::uuid
                   AND subject_id=
                     $2::uuid
                   AND topic_id
                     IS NULL""",
                u["id"],
                s["subject_id"])
            if mrow:
                res = {
                    "mastery":
                        mrow["mastery"]}
        await q(
            """UPDATE pending_actions
               SET status='confirmed'
               WHERE code=$1""",
            code)
        accp = 100 * cor // att
        lines = ["✅ Logged: "
                 + str(att)
                 + " questions, "
                 + str(cor)
                 + " correct ("
                 + str(accp) + "%)."]
        mast = res.get("mastery")
        if mast is not None:
            ev = res.get("events")
            line = ("🧠 Mastery: "
                    + format(mast,
                             ".0f")
                    + "%")
            if ev:
                line += " (" + str(ev)
                line += " sessions)"
            lines.append(line)
        nr = res.get("next_rev")
        if nr:
            lines.append(
                "🔁 Next revision: "
                + nr.strftime(
                    "%d %b"))
        await send(chat,
                   "\n".join(lines),
                   MENU_KB)
        return
    await send(chat,
               "Try: '25 18' or "
               "'25 questions 18 "
               "correct' — or /skip.")


async def session_ctl(u, chat, op):
    now = now_tz()
    s = await live_session(u["id"])
    if not s:
        await cmd_dashboard(
            u, chat)
        return
    if op == "pause":
        await q(
            """UPDATE study_sessions
               SET status='paused',
                   paused_at=$2
               WHERE id=$1::uuid""",
            str(s["id"]), now)
        await send(chat,
                   "⏸ Paused.",
                   IK([("▶️ Resume",
                        "ses:resume")],
                      [("❌ Abandon",
                        "ses:abandon")]))
    elif op == "resume":
        paused = \
            s["paused_seconds"] or 0
        if s["paused_at"]:
            add = (now
                   - s["paused_at"])
            paused += int(
                add.total_seconds())
        await q(
            """UPDATE study_sessions
               SET status='active',
                   paused_at=NULL,
                   paused_seconds=$2
               WHERE id=$1::uuid""",
            str(s["id"]), paused)
        await send(chat,
                   "▶️ Resumed.",
                   IK([("⏸ Pause",
                        "ses:pause"),
                       ("✅ Finish",
                        "ses:finish")],
                      [("❌ Abandon",
                        "ses:abandon")]))
    elif op == "finish":
        paused = \
            s["paused_seconds"] or 0
        if (s["status"] == "paused"
                and s["paused_at"]):
            add = (now
                   - s["paused_at"])
            paused += int(
                add.total_seconds())
        dur = (now
               - s["started_at"])
        dur = max(
            0, int(
                dur.total_seconds()
                - paused) // 60)
        await q(
            """UPDATE study_sessions
               SET status='finished',
                   ended_at=$2,
                   duration_minutes=$3,
                   paused_seconds=$4,
                   paused_at=NULL
               WHERE id=$1::uuid""",
            str(s["id"]), now,
            dur, paused)
        code = await new_pending(
            u, "session_log",
            {"session_id":
             str(s["id"])},
            "wizard")
        skip = "cfm:" + code \
               + ":skipq"
        await send(
            chat,
            "✅ Finished — "
            + fm(dur)
            + ".\n\nHow many "
            "questions did you "
            "attempt?\ne.g. '25 18' "
            "or '25 questions 18 "
            "correct' (or /skip)",
            IK([("🤷 Skip", skip)]))
    elif op == "abandon":
        await q(
            """UPDATE study_sessions
               SET status='abandoned',
                   ended_at=$2
               WHERE id=$1::uuid""",
            str(s["id"]), now)
        await send(chat,
                   "Session discarded — "
                   "no guilt; the next "
                   "one counts.",
                   MENU_KB)

# ============================================================
# CALLBACKS
# ============================================================
async def handle_callback(cb):
    data = cb.get("data", "")
    chat = cb["message"]["chat"]["id"]
    if not cb.get("from"):
        return
    u = await ensure_user(
        cb["from"], chat)
    u = await lazy_tick(u, now_tz())
    await answer_cb(cb["id"])
    scope, _, rest = data.partition(
        ":")
    try:
        if scope == "nav":
            await nav_cb(u, chat, rest)
        elif scope == "anp":
            days = int(rest or "7")
            await cmd_analytics(
                u, chat, days=days)
        elif scope == "cfm":
            msg_id = cb["message"][
                "message_id"]
            await confirm_cb(
                u, chat, msg_id, rest)
        elif scope == "subj":
            await subj_cb(
                u, chat, rest)
        elif scope == "ses":
            await session_ctl(
                u, chat, rest)
        elif scope == "quiz":
            await quiz_cb(
                u, chat, rest)
        elif scope == "go":
            await go_cb(
                u, chat, rest)
        elif scope == "ob":
            await ob_cb(
                u, chat, rest)
        elif scope == "note":
            await note_cb(
                u, chat, rest)
        elif scope == "reset":
            await reset_cb(
                u, chat, rest)
    except Exception:
        LOG.exception(
            "callback error: %s", data)
        await send(chat,
                   "Something broke — "
                   "the action wasn't "
                   "completed. Try "
                   "again.", MENU_KB)


async def nav_cb(u, chat, what):
    views = {
        "dash": cmd_dashboard,
        "next": cmd_next,
        "plan": cmd_plan,
        "hw": cmd_homework,
        "tests": cmd_tests,
        "rev": cmd_revision,
        "an": cmd_analytics,
        "mist": cmd_mistakes,
        "syl": cmd_syllabus}
    if what in views:
        fn = views[what]
        if fn is cmd_analytics:
            await fn(u, chat, days=7)
        else:
            await fn(u, chat)
    elif what == "menu":
        await send(chat,
                   "🏠 <b>StudyOS</b>",
                   MENU_KB)
    elif what == "quiz":
        await start_quiz(
            u, chat, None)
    else:
        await cmd_dashboard(
            u, chat)


async def quiz_cb(u, chat, rest):
    qid, _, spec = rest.partition(
        ":")
    if spec.startswith("stop"):
        await q(
            """UPDATE quizzes
               SET status='stopped'
               WHERE id=$1::uuid
               AND user_id=$2::uuid""",
            qid, u["id"])
        await send(chat,
                   "Quiz stopped.",
                   MENU_KB)
        return
    try:
        qi_s, opt_s = spec.split(
            ":")
        await quiz_answer(
            u, chat, qid,
            int(qi_s), int(opt_s))
    except ValueError:
        pass


async def go_cb(u, chat, rest):
    code, _, mode = rest.partition(
        ":")
    pend = await get_pending(
        u, code)
    if not pend \
            or pend["status"] \
            != "pending":
        return
    payload = json.loads(
        pend["payload"])
    cand = payload["cand"]
    if mode == "alt":
        await q(
            """UPDATE pending_actions
               SET status='cancelled'
               WHERE code=$1""",
            code)
        await cmd_next(
            u, chat,
            exclude={cand["key"]})
        return
    await q(
        """UPDATE pending_actions
           SET status='confirmed'
           WHERE code=$1""", code)
    if cand["kind"] == "quiz":
        meta = cand.get(
            "meta", {})
        label = cand.get(
            "chapter") \
            or cand["title"]
        flavor = None
        rt = meta.get("rev_type")
        if rt == "formula_review":
            flavor = "formula "
            flavor += "recall focus"
        elif rt == "pyq":
            flavor = "previous-year "
            flavor += "exam style"
        await start_quiz(
            u, chat,
            meta.get("topic"),
            count=5,
            subject_id=cand.get(
                "subject_id"),
            topic_id=cand.get(
                "topic_id"),
            topic_label=label,
            flavor=flavor)
    else:
        await start_live_session(
            u, chat, cand)


async def ob_cb(u, chat, what):
    if what == "confirm":
        if not u["onboarded"]:
            await ob_commit(
                u, chat)
        else:
            await cmd_dashboard(
                u, chat)
    elif what == "restart":
        await q(
            """UPDATE users SET
               ob_state='{}'::jsonb,
               ob_step=NULL,
               onboarded=false
               WHERE id=$1::uuid""",
            u["id"])
        await ob_start(u, chat)


async def note_cb(u, chat, nid):
    r = await qrow(
        """SELECT title, content
           FROM resources
           WHERE id=$1::uuid
           AND user_id=$2::uuid""",
        nid, u["id"])
    if r:
        body = esc(
            (r["content"]
             or "")[:3000])
        await send(
            chat,
            "📎 <b>"
            + esc(r["title"])
            + "</b>\n\n" + body,
            MENU_KB)


async def reset_cb(u, chat, what):
    if what == "ask":
        await send(
            chat,
            "🧨 <b>Wipe ALL your "
            "StudyOS data?</b>\n"
            "Sessions, mastery, "
            "homework, tests — "
            "everything.",
            IK([("✅ Yes, wipe it",
                 "reset:yes")],
               [("❌ No, keep it",
                 "nav:menu")]))
    elif what == "yes":
        await q(
            "DELETE FROM users "
            "WHERE id=$1::uuid",
            u["id"])
        await send(chat,
                   "🧨 Wiped clean. Send "
                   "/start to set up "
                   "again.")


async def subj_cb(u, chat, rest):
    code, _, sid = rest.partition(
        ":")
    pend = await get_pending(
        u, code)
    if not pend \
            or pend["status"] \
            != "pending":
        return
    payload = json.loads(
        pend["payload"])
    sname = await qval(
        """SELECT name
           FROM subjects
           WHERE id=$1::uuid""",
        sid)
    if not sname:
        return
    payload["fields"][
        "subject_id"] = sid
    payload["fields"][
        "subject"] = sname
    await set_pending_payload(
        code, payload)
    kind = pend["kind"]
    f = payload["fields"]
    if kind == "log_session":
        lines = ["I found:", ""]
        if f.get("questions"):
            lines.append(
                "Questions: <b>"
                + str(
                    f["questions"])
                + "</b>")
            if f.get("correct") \
                    is not None:
                lines.append(
                    "Correct: <b>"
                    + str(
                        f["correct"])
                    + "</b>")
        if f.get("minutes"):
            lines.append(
                "Duration: <b>"
                + fm(
                    f["minutes"])
                + "</b>")
        lines.append(
            "Subject: <b>"
            + esc(sname)
            + "</b>")
        if f.get("topic"):
            lines.append(
                "Topic: <b>"
                + esc(
                    f["topic"])
                + "</b>")
        await send(chat,
                   "\n".join(lines),
                   confirm_kb(code))
        return
    orders = {
        "hw_add": [
            ("title", "Homework"),
            ("subject", "Subject"),
            ("questions",
             "Questions"),
            ("due", "Due")],
        "test_add": [
            ("name", "Test"),
            ("subject", "Subject"),
            ("date", "When")],
        "mistake_add": [
            ("count", "Count"),
            ("mtype", "Type"),
            ("subject", "Subject"),
            ("topic", "Topic")]}
    lines = card_lines(
        f, orders.get(kind, []))
    await send(chat,
               "I found:\n\n"
               + "\n".join(lines),
               confirm_kb(code))


async def confirm_cb(u, chat,
                     msg_id, rest):
    code, _, answer = \
        rest.rpartition(":")
    pend = await get_pending(
        u, code)
    if not pend \
            or pend["status"] \
            != "pending":
        return
    payload = json.loads(
        pend["payload"])
    kind = pend["kind"]
    if answer in ("no", "undo"):
        if answer == "undo" \
                and kind \
                == "undo_log":
            f = payload["fields"]
            await q(
                """DELETE
                   FROM study_sessions
                   WHERE user_id=
                     $1::uuid
                   AND created_at
                     > now()
                     - interval '10'
                     - interval 'minutes'
                   AND source='log'
                   AND questions_attempted
                     =$2
                   AND questions_correct
                     =$3""",
                u["id"],
                f.get("questions")
                or 0,
                f.get("correct"))
            sid = f.get(
                "subject_id")
            if sid:
                await recompute(
                    u["id"], sid,
                    None)
            await edit(chat, msg_id,
                       "↩️ Undone.")
        else:
            await edit(chat, msg_id,
                       "Cancelled. ✖️")
        await q(
            """UPDATE pending_actions
               SET status='cancelled'
               WHERE code=$1""",
            code)
        return
    if answer == "edit":
        await q(
            """UPDATE pending_actions
               SET status='cancelled'
               WHERE code=$1""",
            code)
        await edit(chat, msg_id,
                   "Send the corrected "
                   "message and I'll "
                   "re-read it. 👍")
        return
    if answer == "skipq":
        await q(
            """UPDATE pending_actions
               SET status='confirmed'
               WHERE code=$1""",
            code)
        await edit(chat, msg_id,
                   "Logged as time-only. "
                   "🏠")
        return
    # answer == yes
    if kind == "log_session":
        res = await do_log_session(
            u, payload["fields"])
        ucode = await new_pending(
            u, "undo_log",
            {"fields":
             payload["fields"]},
            "confirm")
        undo = "cfm:" + ucode \
               + ":undo"
        await edit(chat, msg_id,
                   res,
                   IK([("↩️ Undo",
                        undo)]))
    elif kind == "hw_add":
        await edit(chat, msg_id,
                   await do_hw_add(
                       u,
                       payload[
                           "fields"]))
        await refresh_plan(
            u, chat,
            "homework added")
    elif kind == "hw_progress":
        msg, completed = \
            await do_hw_progress(
                u,
                payload["fields"])
        await edit(chat, msg_id,
                   msg)
        if completed:
            await refresh_plan(
                u, chat,
                "homework finished")
    elif kind == "test_add":
        await edit(chat, msg_id,
                   await do_test_add(
                       u,
                       payload[
                           "fields"]))
        await refresh_plan(
            u, chat, "test added")
    elif kind == "test_result":
        await edit(chat, msg_id,
                   await do_test_result(
                       u,
                       payload[
                           "fields"]))
    elif kind == "mistake_add":
        await edit(chat, msg_id,
                   await do_mistake_add(
                       u,
                       payload[
                           "fields"]))
    elif kind \
            == "mistake_resolve":
        await edit(chat, msg_id,
                   await do_mistake_resolve(
                       u,
                       payload[
                           "fields"]))
    elif kind == "class_op":
        await edit(chat, msg_id,
                   await do_class_op(
                       u,
                       payload[
                           "fields"]))
        await refresh_plan(
            u, chat,
            "schedule changed")
    elif kind == "syllabus":
        await edit(chat, msg_id,
                   await do_syllabus(
                       u,
                       payload.get(
                           "blocks",
                           [])))
    elif kind == "save_note":
        await edit(chat, msg_id,
                   await do_save_note(
                       u,
                       payload[
                           "fields"]))
    else:
        await edit(chat, msg_id,
                   "✅ Done.")
    await q(
        """UPDATE pending_actions
           SET status='confirmed'
           WHERE code=$1""",
        code)

# ============================================================
# MESSAGE ENTRY
# ============================================================
async def process_message(msg):
    chat = msg["chat"]["id"]
    try:
        if not msg.get("from"):
            return
        u = await ensure_user(
            msg["from"], chat)
        u = await lazy_tick(
            u, now_tz())
        if msg.get("voice"):
            await handle_voice(
                u, chat, msg)
            return
        if msg.get("photo"):
            await handle_photo(
                u, chat, msg)
            return
        if msg.get("document"):
            await handle_document(
                u, chat, msg)
            return
        text = (msg.get("text")
                or msg.get("caption")
                or "").strip()
        if not text:
            return
        if not u["onboarded"] \
                and not text \
                .startswith("/"):
            await ob_handle(
                u, chat, text)
            return
        if not u["onboarded"]:
            if text.lower() \
                    .startswith(
                        "/start"):
                await ob_start(
                    u, chat)
            else:
                await send(chat,
                           "Let's finish "
                           "setup first 🙂")
            return
        await handle_text_msg(
            u, chat, text)
    except Exception:
        LOG.exception(
            "message failed")
        await send(chat,
                   "Something broke on "
                   "my side — try "
                   "again.", MENU_KB)


async def process_update(upd):
    if "callback_query" in upd:
        await handle_callback(
            upd["callback_query"])
    elif "message" in upd:
        await process_message(
            upd["message"])

# ============================================================
# FASTAPI APP
# ============================================================
@asynccontextmanager
async def lifespan(_app):
    global AIC
    pairs = (
        ("TELEGRAM_BOT_TOKEN",
         TELEGRAM_BOT_TOKEN),
        ("DATABASE_URL",
         DATABASE_URL),
        ("GEMINI_API_KEY",
         GEMINI_API_KEY),
        ("TELEGRAM_WEBHOOK_SECRET",
         TELEGRAM_WEBHOOK_SECRET))
    missing = [k for k, v in pairs
               if not v]
    if missing:
        LOG.error(
            "Missing env vars: %s",
            ", ".join(missing))
        raise RuntimeError(
            "missing env: "
            + str(missing))
    await init_db()
    AIC = genai.Client(
        api_key=GEMINI_API_KEY)
    LOG.info("gemini ready (model=%s)",
             GEMINI_MODEL)
    ext = os.getenv(
        "RENDER_EXTERNAL_URL")
    if ext:
        url = ext + "/webhook"
        try:
            res = await tg(
                "setWebhook",
                url=url,
                secret_token=
                TELEGRAM_WEBHOOK_SECRET,
                allowed_updates=[
                    "message",
                    "callback_query"])
            LOG.info(
                "webhook registered: "
                "%s -> %s",
                url, bool(res))
        except Exception as e:
            LOG.error(
                "webhook registration "
                "failed: %s", e)
    else:
        LOG.warning(
            "RENDER_EXTERNAL_URL not "
            "set — skipping webhook")
    yield
    await HTTP.aclose()
    if POOL:
        await POOL.close()
    LOG.info(
        "studyos shutdown done")


app = FastAPI(title="StudyOS",
              lifespan=lifespan)


@app.get("/health")
async def health():
    try:
        ok = await qval(
            "SELECT 1") == 1
    except Exception:
        ok = False
    return {"ok": ok,
            "service": "studyos",
            "db": ok,
            "ai": AIC is not None}


@app.post("/webhook")
async def webhook(
        request: Request,
        x_telegram_bot_api_secret_token:
        str = Header(default="")):
    if (x_telegram_bot_api_secret_token
            != TELEGRAM_WEBHOOK_SECRET):
        raise HTTPException(
            status_code=403)
    upd = await request.json()
    uid = upd.get("update_id")
    if uid is None:
        return {"ok": True}
    inserted = await qval(
        """INSERT INTO update_inbox
           (update_id)
           VALUES($1)
           ON CONFLICT DO NOTHING
           RETURNING update_id""",
        uid)
    if inserted is None:
        return {"ok": True}
    try:
        await process_update(upd)
    except Exception:
        LOG.exception(
            "update %s failed", uid)
    return {"ok": True}


@app.get("/")
async def root():
    return {"service": "StudyOS",
            "health": "/health"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, host="0.0.0.0",
        port=int(
            os.getenv("PORT", 8000)))
