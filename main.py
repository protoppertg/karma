"""
StudyOS — Personal AI Study Operating System
Single-user. Telegram + Supabase + Gemini free tier + Render.
Tables auto-create on boot. AI interprets language;
Python computes every number.
"""

import os
import re
import ssl
import json
import time
import math
import uuid
import logging
import asyncio
from datetime import datetime, date, timedelta, timezone
from datetime import time as dtime
from zoneinfo import ZoneInfo

import asyncpg
import httpx
from fastapi import FastAPI, Request, Header, HTTPException
from contextlib import asynccontextmanager

from google import genai
from google.genai import types

# ============================================================
# CONFIG
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

TZ_NAME = os.getenv("DEFAULT_TIMEZONE", "Asia/Kolkata")
try:
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    TZ_NAME, TZ = "UTC", timezone.utc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
LOG = logging.getLogger("studyos")

HTTP = httpx.AsyncClient(timeout=35.0)
POOL = None
AIC = None

# ============================================================
# DB SCHEMA (auto-applied on boot)
# ============================================================
SCHEMA = [
    """CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        telegram_user_id BIGINT UNIQUE NOT NULL,
        telegram_chat_id BIGINT,
        first_name TEXT, username TEXT, display_name TEXT,
        exam_goal TEXT, exam_date DATE,
        wake_time TIME DEFAULT '06:30',
        sleep_time TIME DEFAULT '23:00',
        school_start TIME, school_end TIME, school_days INT[],
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
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
          ON DELETE CASCADE,
        name TEXT NOT NULL,
        difficulty INT DEFAULT 3,
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, name))""",
    """CREATE TABLE IF NOT EXISTS topics (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID NOT NULL REFERENCES subjects(id)
          ON DELETE CASCADE,
        name TEXT NOT NULL,
        position INT DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, subject_id, name))""",
    """CREATE TABLE IF NOT EXISTS study_sessions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
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
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE INDEX IF NOT EXISTS ix_sess_user
        ON study_sessions(user_id, created_at)""",
    """CREATE TABLE IF NOT EXISTS homework (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
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
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE INDEX IF NOT EXISTS ix_hw_user
        ON homework(user_id, due_at)""",
    """CREATE TABLE IF NOT EXISTS tests (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        name TEXT NOT NULL,
        test_at TIMESTAMPTZ,
        total_marks INT,
        obtained_marks INT,
        status TEXT DEFAULT 'scheduled',
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS mistakes (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
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
        first_seen TIMESTAMPTZ DEFAULT now(),
        last_seen TIMESTAMPTZ DEFAULT now(),
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, fingerprint))""",
    """CREATE TABLE IF NOT EXISTS revisions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
          ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id)
          ON DELETE SET NULL,
        topic_id UUID NOT NULL REFERENCES topics(id)
          ON DELETE CASCADE,
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
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
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
        computed_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE UNIQUE INDEX IF NOT EXISTS uq_mast_topic
        ON mastery(user_id, topic_id)
        WHERE topic_id IS NOT NULL""",
    """CREATE UNIQUE INDEX IF NOT EXISTS uq_mast_subj
        ON mastery(user_id, subject_id)
        WHERE topic_id IS NULL""",
    """CREATE TABLE IF NOT EXISTS quizzes (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
          ON DELETE CASCADE,
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
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
          ON DELETE CASCADE,
        msg_id BIGINT,
        kind TEXT,
        payload JSONB DEFAULT '{}'::jsonb,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS update_inbox (
        update_id BIGINT PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS resources (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
          ON DELETE CASCADE,
        title TEXT,
        content TEXT,
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS daily_plans (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id)
          ON DELETE CASCADE,
        plan_date DATE,
        available_minutes INT,
        tasks JSONB,
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, plan_date))""",
    """CREATE TABLE IF NOT EXISTS ai_log (
        id BIGSERIAL PRIMARY KEY,
        kind TEXT,
        status TEXT,
        latency_ms INT,
        error TEXT,
        created_at TIMESTAMPTZ DEFAULT now())""",
]


async def init_db():
    global POOL
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    dsn = DATABASE_URL.split("?")[0].strip()
    POOL = await asyncpg.create_pool(
        dsn, min_size=1, max_size=3, command_timeout=20,
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
TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


def IK(*rows):
    return {"inline_keyboard": [
        [{"text": t, "callback_data": d} for t, d in r] for r in rows]}


def esc(s) -> str:
    return str(s if s is not None else "") \
        .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def trunc(s, n=3500) -> str:
    return s if len(s) <= n else s[:n] + "\n…"


async def tg(method, **payload):
    if payload.get("reply_markup") is None:
        payload.pop("reply_markup", None)
    for attempt in range(3):
            r = await HTTP.post(f"{TG_BASE}/{method}", json=payload)
            data = r.json()
            if data.get("ok"):
                return data.get("result")
            code = data.get("error_code")
            desc = data.get("description", "")
            if code == 429:
                ra = data.get("parameters", {}).get("retry_after", 3)
                await asyncio.sleep(min(ra, 10))
                continue
            if code and code >= 500:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            LOG.warning("tg %s: %s", method, desc)
            return None
        except (httpx.TimeoutException, httpx.TransportError) as e:
            LOG.warning("tg %s net: %s", method, e)
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


async def send(chat_id, text, kb=None):
    text = trunc(text)
    res = await tg("sendMessage", chat_id=chat_id, text=text,
                   parse_mode="HTML",
                   disable_web_page_preview=True,
                   reply_markup=kb)
    if res is None and ("<" in text or ">" in text):
        await tg("sendMessage", chat_id=chat_id,
                 text=re.sub(r"<[^>]+>", "", text),
                 disable_web_page_preview=True,
                 reply_markup=kb)
    return res


async def edit(chat_id, msg_id, text, kb=None):
    text = trunc(text)
    res = await tg("editMessageText", chat_id=chat_id,
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
                 callback_query_id=cb_id, text=text)
    except Exception:
        pass


async def get_file_bytes(file_id) -> bytes:
    res = await tg("getFile", file_id=file_id)
    if not res:
        raise RuntimeError("getFile failed")
    url = f"{TG_BASE}/file/bot{TELEGRAM_BOT_TOKEN}/"
    url += res["file_path"]
    r = await HTTP.get(url)
    r.raise_for_status()
    return r.content

# ============================================================
# GEMINI — free-tier friendly (spaced, cached, retried)
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


async def ai_call(contents, json_mode=True, max_tokens=1200):
    """contents: str or list of parts. Returns text."""
    for attempt in range(3):
        await _ai_space()
        t0 = time.monotonic()
        try:
            cfg = types.GenerateContentConfig(
                response_mime_type=(
                    "application/json" if json_mode else "text/plain"),
                max_output_tokens=max_tokens,
                temperature=0.2)

            def _do():
                return AIC.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=contents,
                    config=cfg)

            resp = await asyncio.to_thread(_do)
            text = resp.text
            if not text:
                raise AIError("empty AI response")
            return text
        except Exception as e:
            msg = str(e)
            low = msg.lower()
            quota = ("429" in msg or "resource" in low
                     or "quota" in low)
            try:
                await q(
                    "INSERT INTO ai_log"
                    "(kind,status,latency_ms,error) "
                    "VALUES('call',$1,$2,$3)",
                    "retry" if attempt < 2 else "error",
                    int((time.monotonic() - t0) * 1000),
                    msg[:400])
            except Exception:
                pass
            if quota:
                await asyncio.sleep(9 * (attempt + 1))
                continue
            if attempt >= 1:
                raise AIError(msg[:300])
            await asyncio.sleep(1.2)
    raise AIError("AI quota exhausted — try again in a minute")


def _load_json(text):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
        text = text.rstrip("`").strip()
    i, j = text.find("{"), text.rfind("}")
    if i == -1 or j == -1:
        raise AIError("no JSON in AI response")
    return json.loads(text[i:j + 1])

# ============================================================
# DETERMINISTIC PARSERS (zero AI cost)
# ============================================================
WEEKDAYS = {"monday": 0, "mon": 0, "tuesday": 1, "tue": 1,
            "tues": 1, "wednesday": 2, "wed": 2,
            "thursday": 3, "thu": 3, "thurs": 3,
            "friday": 4, "fri": 4, "saturday": 5, "sat": 5,
            "sunday": 6, "sun": 6}
MONTHS = {m.lower(): i + 1 for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June",
     "July", "August", "September", "October", "November",
     "December"])}
SKIP_WORDS = {"skip", "/skip", "later", "no", "nothing",
              "none", "n", "na"}


def parse_date(text, today: date):
    t = (text or "").strip().lower().rstrip(".,!?")
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
        return today + timedelta(days=int(m.group(1)))
    for wd in WEEKDAYS:
        if t == wd or t == f"next {wd}" or t == f"this {wd}":
            d = (WEEKDAYS[wd] - today.weekday()) % 7
            return today + timedelta(days=d if d else 7)
    m = re.match(r"^(\d{1,2})[\s\-/](\d{1,2})"
                 r"(?:[\s\-/](\d{2,4}))?$", t)
    if m:
        dd, mo = int(m.group(1)), int(m.group(2))
        yy = today.year
        if m.group(3):
            yy = int(m.group(3))
            yy += 2000 if yy < 100 else 0
        try:
            return date(yy, mo, dd)
        except ValueError:
            return None
    m = re.match(r"^(\d{1,2})(?:st|nd|rd|th)?\s+([a-z]+)$", t)
    if m and m.group(2) in MONTHS:
        try:
            return date(today.year, MONTHS[m.group(2)],
                        int(m.group(1)))
        except ValueError:
            return None
    for name, mo in MONTHS.items():
        if t.startswith(name):
            m = re.match(rf"^{name}\s+(\d{{1,2}})"
                         r"(?:st|nd|rd|th)?$", t)
            if m:
                try:
                    return date(today.year, mo,
                                int(m.group(1)))
                except ValueError:
                    return None
    return None


def parse_time(t):
    t = (t or "").strip().lower().replace(" ", "")
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?(am|pm)?$", t)
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
    t = (t or "").strip().lower().replace(" ", "")
    t = t.replace("–", "-").replace("—", "-")
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?(am|pm)?"
                 r"(?:-|to)"
                 r"(\d{1,2})(?::(\d{2}))?(am|pm)?$", t)
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
    m = re.search(r"(\d+(?:\.\d+)?)\s*"
                  r"(?:h|hr|hrs|hour|hours)\s*"
                  r"(?:(\d+)\s*(?:m|min|mins|minutes))?", t)
    if m:
        return int(float(m.group(1)) * 60
                   + int(m.group(2) or 0))
    m = re.search(r"(\d+)\s*(?:m|min|mins|minute|minutes)\b", t)
    if m:
        return int(m.group(1))
    return None


def parse_questions(text):
    """Returns (attempted, correct, wrong) or None."""
    t = (text or "").lower()
    m = re.search(r"(\d+)\s*(?:questions?|qs|mcqs?|problems?|"
                  r"pyqs?)\b", t)
    if not m:
        return None
    att = int(m.group(1))
    cor = wre = None
    mc = re.search(r"(\d+)\s*"
                   r"(?:correct|right|correctly)", t)
    mw = re.search(r"(\d+)\s*"
                   r"(?:wrong|incorrect|mistakes?)", t)
    if mc:
        cor = int(mc.group(1))
    if mw:
        wre = int(mw.group(1))
    if cor is None and wre is not None and wre <= att:
        cor = att - wre
    return att, cor, wre


def detect_energy(text):
    t = (text or "").lower()
    bad = ("exhausted", "dead tired", "burnt out",
           "burned out", "no energy", "so tired",
           "can't do this", "cant do this")
    mid = ("tired", "sleepy", "cant focus", "can't focus",
           "low energy", "drained")
    good = ("energetic", "feeling fresh", "fired up")
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
    if s in ("daily", "everyday", "every day", "all days"):
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
        if (len(bits) == 2 and bits[0] in WEEKDAYS
                and bits[1] in WEEKDAYS):
            a, b = WEEKDAYS[bits[0]], WEEKDAYS[bits[1]]
            if b >= a:
                out.update(range(a, b + 1))
            else:
                out.update(range(a, 7))
                out.update(range(0, b + 1))
    return sorted(out) if out else None


def parse_class(text):
    """'8-2 mon-sat' -> (start, end, days)"""
    m = re.search(r"(\d{1,2}(?::\d{2})?\s*(?:am|pm)?\s*"
                  r"(?:-|–|to)\s*"
                  r"\d{1,2}(?::\d{2})?\s*(?:am|pm)?)",
                  text.lower())
    if not m:
        return None
    tr = parse_time_range(m.group(1))
    if not tr:
        return None
    rest = text.lower().replace(m.group(1), " ").strip()
    days = parse_days(rest) if rest else list(range(7))
    return tr[0], tr[1], days

# ============================================================
# ENGINES — deterministic
# ============================================================
SOURCE_W = {"log": 1.0, "live": 1.0, "quiz": 0.9, "test": 1.6}


def compute_mastery(events):
    """Mastery v1: bounded EMA fold over raw evidence.
    prior=25; alpha=min(0.15, 0.08*volume*source_weight);
    volume=log10(1+attempted)/log10(31).
    One event moves mastery by at most 15 points."""
    if not events:
        return None
    m = 25.0
    att = cor = n = 0
    last = None
    for e in sorted(events, key=lambda x: x["at"]):
        a, c = e["a"], e["c"]
        if not a or a <= 0 or c is None:
            continue
        att += a
        cor += c
        n += 1
        last = e["at"]
        score = 100.0 * c / a
        volume = min(1.0,
                     math.log10(1 + a) / math.log10(31.0))
        w = SOURCE_W.get(e["src"], 1.0)
        alpha = min(0.15, 0.08 * volume * w)
        m += alpha * (score - m)
        m = max(0.0, min(100.0, m))
    return {"mastery": round(m, 1), "attempts": att,
            "correct": cor, "events": n, "last": last}


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
    """SM-2 style, driven by measured accuracy."""
    interval = int(r["interval_days"])
    ease = float(r["ease"])
    streak = int(r["streak"])
    lapses = int(r["lapses"])
    mode = r["mode"]
    rev_type = r["rev_type"]
    g = grade_of(acc)
    if g == "again":
        interval, streak = 1, 0
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
        interval = max(1, round(interval * ease * 1.3))
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
    return {"interval_days": interval, "ease": round(ease, 2),
            "streak": streak, "lapses": lapses,
            "mode": mode, "rev_type": rev_type, "grade": g}


BASE_SCORE = {"test_prep": 40, "homework": 30,
              "mistake_review": 28, "revision": 25,
              "quiz": 22, "study": 20}
LIGHT = {"revision", "mistake_review", "quiz"}


def score_task(c, now, energy):
    s = float(BASE_SCORE.get(c["kind"], 15))
    rs = []
    k = c["kind"]
    m = c.get("meta", {})
    if k == "homework":
        due = m.get("due_at")
        if due:
            hrs = (due - now).total_seconds() / 3600
            if hrs < 0:
                s += 25
                od = int(-hrs // 24) + 1
                rs.append(f"overdue ~{od}d")
            elif hrs <= 24:
                s += 15
                rs.append("due <24h")
            elif hrs <= 48:
                s += 8
                rs.append("due tomorrow")
        rem = m.get("remaining")
        if rem:
            s += min(6, rem / 10)
            rs.append(f"{rem} questions left")
    t_in = m.get("test_in_days")
    heavy = ("revision", "study", "mistake_review", "homework")
    if t_in is not None and t_in <= 14 and k in heavy:
        if t_in <= 3:
            s += 25
        elif t_in <= 7:
            s += 15
        else:
            s += 8
        rs.append(f"test in {t_in}d")
    if k == "revision":
        od = m.get("overdue_days") or 0
        if od > 0:
            s += min(20, 2 * od)
            rs.append(f"revision overdue {od}d")
        mast = m.get("mastery")
        if mast is not None and mast < 40:
            s += 10
            rs.append(f"weak ({mast:.0f}%)")
        if m.get("mode") == "practice":
            rs.append("needs active practice")
    if k == "mistake_review":
        r = m.get("reps") or 0
        if r:
            s += min(12, 2 * r)
            rs.append(f"{r} repeated mistake(s)")
    if k == "study":
        mast = m.get("mastery")
        if mast is not None and mast < 40:
            s += 8
            rs.append(f"weak topic ({mast:.0f}%)")
        st = m.get("stale_days")
        if st and st > 14:
            s += 5
            rs.append(f"untouched {st}d")
    if energy in ("tired", "exhausted"):
        if k in LIGHT:
            s += 10
            rs.append("light task fits your energy")
        else:
            s -= 10
            rs.append("heavy task — low energy today")
    if c.get("subject"):
        pre = c["subject"]
        if c.get("chapter"):
            pre += f" — {c['chapter']}"
        rs.insert(0, pre)
    return round(max(0.0, min(100.0, s)), 1), rs


def choose_next(scored, minutes, exclude=None):
    if minutes <= 0:
        return None
    limit = max(minutes, 15)
    pool = scored
    if exclude:
        pool = [x for x in scored
                if x[0]["key"] not in exclude]
    fitting = [x for x in pool if x[0]["est"] <= limit]
    if not fitting:
        light = [x for x in pool if x[0]["kind"] in LIGHT]
        fitting = light or pool
    if not fitting:
        return None
    fitting.sort(key=lambda x: (-x[1], x[0]["est"]))
    return fitting[0]


def day_minutes(u, day: date):
    """available = awake − school − coaching − commute
    − meals − 10% buffer"""
    start = datetime.combine(day, u["wake_time"], tzinfo=TZ)
    end = datetime.combine(day, u["sleep_time"], tzinfo=TZ)
    if end <= start:
        end += timedelta(days=1)
    awake = (end - start).total_seconds() / 60
    busy = 0.0
    commute = 0
    wd = day.weekday()
    pairs = (("school_start", "school_end", "school_days"),
             ("coaching_start", "coaching_end",
              "coaching_days"))
    for sk, ek, dk in pairs:
        days = u.get(dk)
        if days and wd in days and u.get(sk) and u.get(ek):
            bs = datetime.combine(day, u[sk], tzinfo=TZ)
            be = datetime.combine(day, u[ek], tzinfo=TZ)
            if be <= bs:
                be += timedelta(days=1)
            cs, ce = max(bs, start), min(be, end)
            if ce > cs:
                busy += (ce - cs).total_seconds() / 60
                cm = u["commute_minutes"] or 0
                commute += min(cm, 60)
    commute = min(commute, int(max(0, awake - busy)), 120)
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
        return (due - now).total_seconds() <= 48 * 3600
    if k == "revision":
        od = c.get("meta", {}).get("overdue_days") or 0
        return od > 7
    return False


def build_plan(cands, avail, energy, missed_days, now):
    """Never exceeds available time."""
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
                c, sc, rs = item
                kept.append((c, round(sc * 0.5, 1), rs))
                non_urgent_seen += 1
        scored = kept
    scored.sort(key=lambda x: -x[1])
    alloc = 0
    rev_n = 0
    tasks = []
    deferred = []
    for c, sc, rs in scored:
        if c["kind"] == "revision" and rev_n >= 6:
            deferred.append((c["title"],
                             "revision backlog cap"))
            continue
        chunk = min(c["est"], 50)
        if alloc + chunk > target:
            deferred.append(
                (c["title"],
                 f"no time today ({alloc}/{target}m)"))
            continue
        tasks.append({"kind": c["kind"], "title": c["title"],
                      "est": chunk, "score": sc,
                      "reasons": rs})
        alloc += chunk
        if c["kind"] == "revision":
            rev_n += 1
        if c["est"] > chunk:
            rest = c["est"] - chunk
            deferred.append((c["title"],
                             f"split — {rest}m later"))
    return {"available": avail, "allocated": alloc,
            "tasks": tasks, "deferred": deferred,
            "missed": missed_days, "target": target}

# ============================================================
# TIME / FORMAT HELPERS
# ============================================================
def now_tz():
    return datetime.now(TZ)


def today_d():
    return datetime.now(TZ).date()


def sod(d: date):
    return datetime.combine(d, dtime(0, 0), tzinfo=TZ)


def fm(mins):
    if mins is None:
        return "—"
    mins = int(mins)
    h, m = divmod(abs(mins), 60)
    body = f"{h}h {m:02d}m" if h else f"{m}m"
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
           (telegram_user_id, telegram_chat_id,
            first_name, username)
           VALUES($1,$2,$3,$4)
           ON CONFLICT(telegram_user_id) DO UPDATE SET
           telegram_chat_id = COALESCE(
             EXCLUDED.telegram_chat_id,
             users.telegram_chat_id),
           first_name = COALESCE(
             EXCLUDED.first_name, users.first_name),
           username = COALESCE(
             EXCLUDED.username, users.username)
           RETURNING *""",
        tg_from["id"], chat_id,
        tg_from.get("first_name"),
        tg_from.get("username"))
    return dict(row)


async def get_subjects(user_id):
    rows = await qrows(
        "SELECT id, name FROM subjects "
        "WHERE user_id=$1::uuid ORDER BY name", user_id)
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
    name = (name or "").strip().title() or "General"
    r = await qrow(
        """INSERT INTO subjects(user_id, name)
           VALUES($1::uuid,$2)
           ON CONFLICT(user_id, name) DO UPDATE
             SET name=EXCLUDED.name
           RETURNING id, name""", user_id, name)
    return dict(r)


async def upsert_topic(user_id, subject_id, name):
    name = (name or "").strip().title() or "General"
    r = await qrow(
        """INSERT INTO topics(user_id, subject_id, name)
           VALUES($1::uuid,$2::uuid,$3)
           ON CONFLICT(user_id, subject_id, name)
             DO UPDATE SET name=EXCLUDED.name
           RETURNING id, name""",
        user_id, subject_id, name)
    return dict(r)


async def find_topic(user_id, subject_id, name):
    if not name:
        return None
    r = await qrow(
        """SELECT id, name FROM topics
           WHERE user_id=$1::uuid
           AND subject_id=$2::uuid
           AND name ILIKE $3 LIMIT 1""",
        user_id, subject_id, f"%{name.strip()}%")
    return dict(r) if r else None


async def topic_mastery_events(user_id, topic_id):
    rows = await qrows(
        """SELECT questions_attempted a,
                  questions_correct c, created_at, source
           FROM study_sessions
           WHERE user_id=$1::uuid AND topic_id=$2::uuid
           AND status='finished'
           AND questions_attempted > 0
           AND questions_correct IS NOT NULL""",
        user_id, topic_id)
    return [{"a": r["a"], "c": r["c"],
             "at": r["created_at"], "src": r["source"]}
            for r in rows]


async def subject_mastery_events(user_id, subject_id):
    rows = await qrows(
        """SELECT questions_attempted a,
                  questions_correct c, created_at, source
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND subject_id=$2::uuid
           AND status='finished'
           AND questions_attempted > 0
           AND questions_correct IS NOT NULL""",
        user_id, subject_id)
    return [{"a": r["a"], "c": r["c"],
             "at": r["created_at"], "src": r["source"]}
            for r in rows]


async def upsert_mastery(user_id, subject_id,
                         topic_id, snap):
    if snap is None or snap["events"] == 0:
        return
    cols = ("INSERT INTO mastery"
            "(user_id, subject_id, topic_id, mastery,"
            " attempts, correct, events, last_evidence,"
            " algo) VALUES(")
    upd = (" ON CONFLICT (user_id, {tgt}) {where}"
           " DO UPDATE SET"
           " mastery=EXCLUDED.mastery,"
           " attempts=EXCLUDED.attempts,"
           " correct=EXCLUDED.correct,"
           " events=EXCLUDED.events,"
           " last_evidence=EXCLUDED.last_evidence,"
           " computed_at=now()")
    if topic_id:
        sql = (cols + "$1::uuid,$2::uuid,$3::uuid,"
               "$4,$5,$6,$7,$8,1)"
               + upd.format(tgt="topic_id",
                            where="WHERE topic_id IS NOT NULL"))
        await q(sql, user_id, subject_id, topic_id,
                snap["mastery"], snap["attempts"],
                snap["correct"], snap["events"],
                snap["last"])
    else:
        sql = (cols + "$1::uuid,$2::uuid,NULL,"
               "$3,$4,$5,$6,$7,1)"
               + upd.format(tgt="subject_id",
                            where="WHERE topic_id IS NULL"))
        await q(sql, user_id, subject_id,
                snap["mastery"], snap["attempts"],
                snap["correct"], snap["events"],
                snap["last"])


async def recompute(user_id, subject_id, topic_id=None):
    if topic_id:
        ev = await topic_mastery_events(user_id, topic_id)
        snap = compute_mastery(ev)
        await upsert_mastery(user_id, subject_id,
                             topic_id, snap)
    if subject_id:
        ev = await subject_mastery_events(user_id,
                                          subject_id)
        snap = compute_mastery(ev)
        await upsert_mastery(user_id, subject_id,
                             None, snap)


async def mistake_reps(user_id, topic_id):
    if not topic_id:
        return 0
    v = await qval(
        """SELECT COALESCE(SUM(count),0) FROM mistakes
           WHERE user_id=$1::uuid AND topic_id=$2::uuid
           AND resolved=false""", user_id, topic_id)
    return int(v or 0)


async def apply_revision(user_id, subject_id, topic_id,
                         acc, now):
    reps = await mistake_reps(user_id, topic_id)
    r = await qrow(
        """SELECT * FROM revisions
           WHERE user_id=$1::uuid AND topic_id=$2::uuid""",
        user_id, topic_id)
    if not r:
        interval = 2
        if acc is not None and acc < 0.5:
            interval = 1
        await q(
            """INSERT INTO revisions
               (user_id, subject_id, topic_id, rev_type,
                interval_days, due_at, last_outcome,
                last_reviewed, status)
               VALUES($1::uuid,$2::uuid,$3::uuid,
                'active_recall',$4,$5,$6,$7,'due')
               ON CONFLICT(user_id, topic_id)
                 DO NOTHING""",
            user_id, subject_id, topic_id, interval,
            now + timedelta(days=interval),
            grade_of(acc), now)
        return now + timedelta(days=interval)
    st = revision_step(dict(r), acc, reps)
    await q(
        """UPDATE revisions SET interval_days=$2,
           ease=$3, streak=$4, lapses=$5, mode=$6,
           rev_type=$7, last_outcome=$8,
           last_reviewed=$9, due_at=$10, status='due'
           WHERE id=$1::uuid""",
        str(r["id"]), st["interval_days"], st["ease"],
        st["streak"], st["lapses"], st["mode"],
        st["rev_type"], st["grade"], now,
        now + timedelta(days=st["interval_days"]))
    return now + timedelta(days=st["interval_days"])


async def save_evidence(u, subject_id, topic_id, att,
                        cor, minutes, source="log",
                        when=None, title=None):
    now = when or now_tz()
    await q(
        """INSERT INTO study_sessions
           (user_id, subject_id, topic_id, source,
            status, title, questions_attempted,
            questions_correct, duration_minutes,
            started_at, ended_at)
           VALUES($1::uuid,$2::uuid,$3::uuid,$4,
            'finished',$5,$6,$7,$8,$9,$9)""",
        u["id"], subject_id, topic_id, source, title,
        att or 0, cor, minutes or 0, now)
    out = {}
    if att and cor is not None:
        out["acc"] = 100.0 * cor / att
        if topic_id and subject_id:
            await recompute(u["id"], subject_id, topic_id)
            out["next_rev"] = await apply_revision(
                u["id"], subject_id, topic_id,
                cor / att, now)
            m = await qrow(
                """SELECT mastery, events FROM mastery
                   WHERE user_id=$1::uuid
                   AND topic_id=$2::uuid""",
                u["id"], topic_id)
            if m:
                out["mastery"] = m["mastery"]
                out["events"] = m["events"]
        elif subject_id:
            await recompute(u["id"], subject_id)
            m = await qrow(
                """SELECT mastery FROM mastery
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
           AND status IN ('active','paused') LIMIT 1""",
        user_id)
    return dict(r) if r else None


async def live_minutes(u, now):
    s = await live_session(u["id"])
    if not s:
        return 0
    paused = s["paused_seconds"] or 0
    if s["status"] == "paused" and s["paused_at"]:
        elapsed = (s["paused_at"]
                   - s["started_at"]).total_seconds()
    else:
        elapsed = (now
                   - s["started_at"]).total_seconds()
    return max(0, int((elapsed - paused) / 60))


async def spent_today(u, day):
    v = await qval(
        """SELECT COALESCE(SUM(duration_minutes),0)
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >= $2 AND created_at < $3""",
        u["id"], sod(day), sod(day) + timedelta(days=1))
    return int(v or 0)


async def cleanup_stale(u, now):
    s = await live_session(u["id"])
    if s:
        age = (now - s["started_at"]).total_seconds()
        if age > 3 * 3600:
            await q("UPDATE study_sessions SET "
                    "status='abandoned' "
                    "WHERE id=$1::uuid", str(s["id"]))


async def lazy_tick(u, now):
    today = now.date()
    if u.get("last_tick") == today:
        await cleanup_stale(u, now)
        return u
    es = u.get("energy_set_at")
    if (u["energy"] != "normal" and es
            and (now - es).total_seconds() > 12 * 3600):
        await q("UPDATE users SET energy='normal' "
                "WHERE id=$1::uuid", u["id"])
        u["energy"] = "normal"
    await q("""UPDATE revisions SET status='due'
               WHERE user_id=$1::uuid
               AND status='scheduled'
               AND due_at < now()""", u["id"])
    await q("UPDATE users SET last_tick=$2 "
            "WHERE id=$1::uuid", u["id"], today)
    u["last_tick"] = today
    await cleanup_stale(u, now)
    return u

# ============================================================
# AI ROUTER
# ============================================================
ROUTER_SYS = """You convert a student's Telegram message
into ONE JSON action for a study-tracking app.
Output ONLY JSON:
{"intent":"...","fields":{...},
 "confidence":"high|medium|low",
 "reply": short reply or null}

Intents and fields (use null for anything not stated):
- "log_session": {"subject","topic","questions",
  "correct","wrong","minutes","day"} — day is "today"
  or "yesterday". Used when they REPORT study done.
- "hw_add": {"title","subject","questions","minutes",
  "due"} — due as natural words like "friday".
- "hw_progress": {"title","done"} — partial progress.
- "test_add": {"name","subject","date","total_marks"}
- "test_result": {"name","subject","obtained","total"}
- "mistake_add": {"subject","topic","count","mtype"}
  mtype: conceptual, calculation, careless, memory,
  misread, guessing, time_pressure.
- "energy": {"level"} — energetic|normal|tired|
  exhausted.
- "quiz": {"topic","subject","count"}
- "tutor": {"question"}
- "query": {"view"} — plan, next, homework, tests,
  revision, analytics, mistakes, dashboard, syllabus.
- "syllabus": {"blocks":[{"subject":"...",
  "topics":["..."]}]}
- "chat": {"reply":"your own short warm reply"}
- "none": {}

Rules: extract only stated facts; never invent numbers;
all numbers are integers; prefer the student's subject
names from context; if unsure about a field, set
confidence "low"."""


async def ai_context(u):
    parts = []
    subs = await get_subjects(u["id"])
    if subs:
        parts.append("subjects: "
                     + ", ".join(s["name"] for s in subs))
    if u.get("exam_goal"):
        parts.append("goal: " + u["exam_goal"])
    weak = await qrows(
        """SELECT s.name subj, t.name top, m.mastery
           FROM mastery m
           JOIN topics t ON t.id=m.topic_id
           JOIN subjects s ON s.id=t.subject_id
           WHERE m.user_id=$1::uuid AND m.events>0
           ORDER BY m.mastery ASC LIMIT 3""", u["id"])
    if weak:
        bits = [f"{r['subj']}/{r['top']} "
                f"{r['mastery']:.0f}%" for r in weak]
        parts.append("weakest topics: " + "; ".join(bits))
    due_rev = await qval(
        """SELECT COUNT(*) FROM revisions
           WHERE user_id=$1::uuid AND due_at<now()""",
        u["id"])
    if due_rev:
        parts.append(f"{due_rev} revisions due")
    over = await qval(
        """SELECT COUNT(*) FROM homework
           WHERE user_id=$1::uuid
           AND status!='completed' AND due_at<now()""",
        u["id"])
    if over:
        parts.append(f"{over} homework overdue")
    nt = await qrow(
        """SELECT name FROM tests
           WHERE user_id=$1::uuid
           AND status='scheduled' AND test_at>now()
           ORDER BY test_at LIMIT 1""", u["id"])
    if nt:
        parts.append(f"next test: {nt['name']}")
    parts.append(f"energy today: {u['energy']}")
    return "\n".join(parts)


async def ai_route(text, u):
    prompt = (ROUTER_SYS + "\n\nCONTEXT:\n"
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
# PENDING ACTIONS / CONFIRM CARDS
# ============================================================
async def new_pending(u, kind, fields, origin="ai"):
    pid = str(uuid.uuid4())
    await q(
        """INSERT INTO pending_actions
           (id, user_id, kind, payload)
           VALUES($1::uuid,$2::uuid,$3,$4::jsonb)""",
        pid, u["id"], kind,
        json.dumps({"fields": fields, "origin": origin}))
    return pid


async def get_pending(u, pid):
    r = await qrow(
        """SELECT * FROM pending_actions
           WHERE id=$1::uuid AND user_id=$2::uuid""",
        pid, u["id"])
    return dict(r) if r else None


async def set_pending_payload(pid, payload):
    await q(
        """UPDATE pending_actions
           SET payload=$2::jsonb WHERE id=$1::uuid""",
        pid, json.dumps(payload))


def confirm_kb(pid):
    return IK(
        [("✅ Confirm", f"cfm:{pid}:yes"),
         ("❌ Cancel", f"cfm:{pid}:no")],
        [("✏️ Correct it", f"cfm:{pid}:edit")])


async def subject_pick_card(u, chat, pid, header):
    subs = await get_subjects(u["id"])
    rows = [[(s["name"], f"subj:{pid}:{s['id']}")]
            for s in subs[:6]]
    rows.append([("❌ Cancel", f"cfm:{pid}:no")])
    await send(chat, header + "\n\nWhich subject?",
               IK(*rows))


def card_lines(fields, order):
    out = []
    for k, label in order:
        v = fields.get(k)
        if v is not None and v != "":
            out.append(f"{label}: <b>{esc(v)}</b>")
    return out

# ============================================================
# APPLY HANDLERS — the only place evidence is written
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
        t = await find_topic(u["id"], sid, f["topic"])
        if not t:
            t = await upsert_topic(u["id"], sid,
                                   f["topic"])
        topic_id, topic_name = t["id"], t["name"]
    res = await save_evidence(
        u, sid, topic_id, att, cor, mins, "log",
        when=when, title="logged session")
    parts = ["✅ <b>Logged</b>"]
    if att and cor is not None:
        parts.append(f"❓ {att} questions • "
                     f"✅ {cor} correct "
                     f"({res['acc']:.0f}%)")
    if mins:
        parts.append(f"⏱ {fm(mins)}")
    if topic_name:
        parts.append(f"📚 Topic: {esc(topic_name)}")
    if res.get("mastery") is not None:
        parts.append(
            f"🧠 Mastery now: "
            f"<b>{res['mastery']:.0f}%</b>")
    if res.get("next_rev"):
        parts.append("🔁 Next revision: "
                     + res["next_rev"].strftime("%d %b"))
    return "\n".join(parts)


async def do_hw_add(u, f):
    sid = f.get("subject_id")
    due = None
    if f.get("due"):
        d = parse_date(str(f["due"]), today_d())
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
           (user_id, subject_id, title, hw_type,
            total_q, est_minutes, due_at)
           VALUES($1::uuid,$2::uuid,$3,$4,$5,$6,$7)""",
        u["id"], sid,
        (f.get("title") or "Homework").strip()[:120],
        f.get("hw_type") or "custom", tq, est, due)
    out = f"📝 Saved: <b>{esc(f.get('title'))}</b>"
    if due:
        out += f" — due {esc(f['due'])}"
    return out


async def do_hw_progress(u, f):
    title = (f.get("title") or "").strip()
    rows = []
    if title:
        rows = await qrows(
            """SELECT * FROM homework
               WHERE user_id=$1::uuid
               AND status NOT IN ('completed')
               AND title ILIKE $2
               ORDER BY due_at""",
            u["id"], f"%{title}%")
    if not rows:
        return ("Couldn't find that homework — "
                "check the title with /homework.")
    hw = rows[0]
    done = int(f.get("done") or 0)
    total = hw["total_q"]
    newc = (hw["completed_q"] or 0) + done
    if total:
        newc = min(newc, total)
    if total and newc >= total:
        status = "completed"
    else:
        status = "in_progress"
    await q(
        """UPDATE homework SET completed_q=$2,
           status=$3 WHERE id=$1::uuid""",
        str(hw["id"]), newc, status)
    prog = f" [{newc}/{total}]" if total else ""
    if status == "completed":
        tail = "done! 🎉"
    else:
        tail = "keep going."
    return (f"📝 Progress saved: "
            f"<b>{esc(hw['title'])}</b>{prog} — {tail}")


async def do_test_add(u, f):
    sid = f.get("subject_id")
    when = None
    if f.get("date"):
        d = parse_date(str(f["date"]), today_d())
        if d:
            when = datetime.combine(
                d, dtime(9, 0), tzinfo=TZ)
    await q(
        """INSERT INTO tests
           (user_id, subject_id, name, test_at,
            total_marks)
           VALUES($1::uuid,$2::uuid,$3,$4,$5)""",
        u["id"], sid,
        (f.get("name") or "Test").strip()[:120],
        when, f.get("total_marks"))
    out = f"🧪 Saved: <b>{esc(f.get('name'))}</b>"
    if when:
        out += " — " + when.strftime("%a %d %b")
    return out


async def do_test_result(u, f):
    name = (f.get("name") or "").strip()
    rows = await qrows(
        """SELECT * FROM tests WHERE user_id=$1::uuid
           ORDER BY test_at DESC NULLS LAST""",
        u["id"])
    test = None
    if name:
        for r in rows:
            if name.lower() in r["name"].lower():
                test = r
                break
    if not test and f.get("subject_id"):
        for r in rows:
            if str(r["subject_id"]) == str(
                    f.get("subject_id")):
                test = r
                break
    if not test and rows:
        test = rows[0]
    if not test:
        return ("Couldn't find that test. "
                "Add it first: 'test on friday'.")
    obt = f.get("obtained")
    tot = f.get("total") or test["total_marks"]
    if obt is None or tot is None:
        return "I need both obtained and total marks."
    pct = 100.0 * obt / tot
    await q(
        """UPDATE tests SET obtained_marks=$2,
           total_marks=$3, status='completed'
           WHERE id=$1::uuid""",
        str(test["id"]), obt, tot)
    ev_att = 20
    ev_cor = round(ev_att * pct / 100)
    await save_evidence(
        u, test["subject_id"], None, ev_att, ev_cor, 0,
        "test", title=f"test: {test['name']}")
    if pct >= 80:
        verdict = "strong 💪"
    elif pct >= 60:
        verdict = "solid 👍"
    else:
        verdict = "needs work 🔧"
    base = (f"🧪 <b>{esc(test['name'])}</b>: "
            f"{obt}/{tot} ({pct:.0f}%) — {verdict}\n"
            f"Recorded as evidence (test weight).")
    try:
        prompt = (f"A student scored {obt}/{tot} "
                  f"({pct:.0f}%) in "
                  f"'{test['name']}'. Write ONE "
                  f"encouraging, specific sentence "
                  f"(max 25 words) about what to do next.")
        txt = await ai_call(prompt, json_mode=False,
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
        return "Which subject? Try again with it."
    topic_id = None
    if f.get("topic"):
        t = await find_topic(u["id"], sid, f["topic"])
        if not t:
            t = await upsert_topic(u["id"], sid,
                                   f["topic"])
        topic_id = t["id"]
    valid = ("conceptual", "calculation", "careless",
             "memory", "misread", "guessing",
             "time_pressure", "unknown")
    mtype = f.get("mtype") or "unknown"
    if mtype not in valid:
        mtype = "unknown"
    cnt = max(1, min(50, int(f.get("count") or 1)))
    fp = hashlib.md5(
        f"{u['id']}|{sid}|{topic_id}|{mtype}"
        .encode()).hexdigest()
    await q(
        """INSERT INTO mistakes
           (user_id, subject_id, topic_id, mtype,
            description, count, fingerprint)
           VALUES($1::uuid,$2::uuid,$3::uuid,$4,$5,$6,$7)
           ON CONFLICT(user_id, fingerprint)
           DO UPDATE SET
             count=mistakes.count+EXCLUDED.count,
             last_seen=now(), resolved=false""",
        u["id"], sid, topic_id, mtype,
        (f.get("description") or mtype)[:200], cnt, fp)
    reps = 0
    if topic_id:
        reps = await mistake_reps(u["id"], topic_id)
    warn = ""
    if reps >= 2:
        warn = ("\n⚠️ Repeated mistake — it now drives "
                "your revision priority.")
    return f"🧠 {cnt} mistake(s) banked ({mtype}).{warn}"


async def do_syllabus(u, blocks):
    added_s = 0
    added_t = 0
    for b in blocks[:12]:
        sname = (b.get("subject") or "").strip().title()
        if not sname:
            continue
        s = await upsert_subject(u["id"], sname)
        added_s += 1
        for tname in (b.get("topics") or [])[:60]:
            t = (tname or "").strip()
            if not t:
                continue
            await upsert_topic(u["id"], s["id"], t)
            added_t += 1
    return (f"📚 Syllabus saved: {added_s} subjects, "
            f"{added_t} topics.")


async def do_save_note(u, f):
    await q(
        """INSERT INTO resources(title, content,
           user_id) VALUES($1,$2,$3::uuid)""",
        (f.get("title") or "Note")[:80],
        (f.get("content") or "")[:4000], u["id"])
    return "📎 Saved to your notes. /notes to browse."

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
        "• \"I finished 50 physics questions, "
        "39 correct\"\n"
        "• \"studied organic chemistry 1h 30m\"\n"
        "• \"add homework: DPP 3, 40 questions, "
        "due friday\"\n"
        "• \"did 25 of 50 DPP\"\n"
        "• \"physics test on sunday\"\n"
        "• \"got 68 out of 75 in physics test\"\n"
        "• \"made 3 conceptual mistakes in "
        "rotation\"\n"
        "• \"I'm exhausted\"\n"
        "• \"quiz me on thermodynamics\"\n"
        "• \"explain rotational motion\"\n"
        "• \"what should I study?\"\n\n"
        "Commands: /plan /next /homework /tests "
        "/revision /mistakes /analytics /quiz /notes "
        "/syllabus /settings\n\n"
        "📷 Send a syllabus photo to import it.\n"
        "🎙 Send a voice note instead of typing.")
    await send(chat, txt, MENU_KB)


async def cmd_dashboard(u, chat):
    now, today = now_tz(), today_d()
    live = await live_session(u["id"])
    if live:
        mins = await live_minutes(u, now)
        title = live['title'] or 'Study'
        await send(
            chat,
            f"⏱ <b>SESSION ACTIVE</b>\n"
            f"{esc(title)}\nRunning: {fm(mins)}",
            IK([("⏸ Pause", "ses:pause"),
                ("✅ Finish", "ses:finish"),
                ("❌ Abandon", "ses:abandon")]))
        return
    avail, busy = day_minutes(u, today)
    done = await spent_today(u, today)
    remaining = max(0, avail - done)
    lines = ["🏠 <b>STUDYOS</b>", "",
             f"Today's capacity: <b>{fm(avail)}</b>",
             f"Studied: <b>{fm(done)}</b>",
             f"Left: <b>{fm(remaining)}</b>"]
    if u["energy"] in ("tired", "exhausted"):
        lines.append(f"⚡ Energy: <b>{u['energy']}</b> "
                     "— keeping it light today")
    tests = await qrows(
        """SELECT name, test_at FROM tests
           WHERE user_id=$1::uuid
           AND status='scheduled'
           AND test_at > now() - interval '1 day'
           ORDER BY test_at LIMIT 2""", u["id"])
    if tests:
        lines += ["", "🧪 <b>TESTS</b>"]
        for t in tests:
            d = None
            if t["test_at"]:
                d = (t["test_at"] - now).days
            if d == 0:
                tag = "TODAY"
            elif d is not None:
                tag = f"in {d}d"
            else:
                tag = "unscheduled"
            lines.append(f"• {esc(t['name'])} — {tag}")
    hw = await qrow(
        """SELECT COUNT(*) FILTER (
                     WHERE due_at < now()) over,
                  COUNT(*) FILTER (
                     WHERE due_at >= now()
                     OR due_at IS NULL) up
           FROM homework
           WHERE user_id=$1::uuid
           AND status NOT IN ('completed',
                              'abandoned')""", u["id"])
    if hw and (hw["over"] or hw["up"]):
        lines += ["", "📝 <b>HOMEWORK</b>"]
        if hw["over"]:
            lines.append(f"⚠️ {hw['over']} overdue")
        if hw["up"]:
            lines.append(f"🟡 {hw['up']} open")
    rev = await qval(
        """SELECT COUNT(*) FROM revisions
           WHERE user_id=$1::uuid
           AND due_at < now()""", u["id"])
    if rev:
        lines += ["", f"🔁 <b>{rev}</b> revision(s) due"]
    kb = []
    if tests and tests[0]["test_at"]:
        d = (tests[0]["test_at"] - now).days
        if d <= 3:
            kb.append([("🧪 TEST SOON — prepare",
                        "nav:next")])
    if hw and hw["over"]:
        kb.append([("⚠️ OVERDUE homework", "nav:hw")])
    kb += [[("▶️ START NEXT", "nav:next"),
            ("📅 Plan", "nav:plan")],
           [("📊 Analytics", "nav:an"),
            ("📚 More", "nav:menu")]]
    await send(chat, "\n".join(lines), IK(*kb))


async def build_candidates(u, now):
    cands = []
    tests = await qrows(
        """SELECT id, name, test_at FROM tests
           WHERE user_id=$1::uuid
           AND status='scheduled'
           AND test_at BETWEEN now()
             AND now() + interval '14 days'
           ORDER BY test_at""", u["id"])
    test_days = [t for t in tests if t["test_at"]
                 and (t["test_at"] - now).days >= 0]
    if test_days:
        min_test = min((t["test_at"] - now).days
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
            "key": f"test:{t['id']}",
            "title": f"Prepare — {t['name']}",
            "est": 45, "subject": "",
            "subject_id": None, "topic_id": None,
            "meta": {"test_in_days": d}})
    rows = await qrows(
        """SELECT rv.id, rv.due_at, rv.mode,
                  rv.rev_type, rv.topic_id,
                  rv.subject_id,
                  s.name subj, t.name top,
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
           AND rv.due_at < now() + interval '1 day'
           ORDER BY rv.due_at LIMIT 25""", u["id"])
    for r in rows:
        od = max(0, (now - r["due_at"]).days)
        if r["mode"] == "practice":
            kind = "quiz"
            est = 15
        else:
            kind = "revision"
            est = 20
        title = (f"{r['subj'] or ''} — "
                 f"{r['top'] or ''} — "
                 + r["rev_type"].replace("_", " "))
        cands.append({
            "kind": kind, "key": f"rev:{r['id']}",
            "title": title.strip(" —"), "est": est,
            "subject": r["subj"] or "",
            "chapter": r["top"] or "",
            "subject_id": r["subject_id"]
            and str(r["subject_id"]),
            "topic_id": str(r["topic_id"]),
            "meta": {"overdue_days": od,
                     "mastery": r["mastery"],
                     "mode": r["mode"],
                     "rev_type": r["rev_type"],
                     "revision_id": str(r["id"]),
                     "topic": r["top"],
                     "test_in_days": min_test}})
    rows = await qrows(
        """SELECT SUM(m.count) reps, s.name subj,
                  t.name top, m.subject_id sid,
                  m.topic_id tid
           FROM mistakes m
           LEFT JOIN subjects s ON s.id=m.subject_id
           LEFT JOIN topics t ON t.id=m.topic_id
           WHERE m.user_id=$1::uuid
           AND m.resolved=false
           GROUP BY s.name, t.name,
                    m.subject_id, m.topic_id
           HAVING SUM(m.count) >= 2
           ORDER BY reps DESC LIMIT 5""", u["id"])
    for r in rows:
        cands.append({
            "kind": "mistake_review",
            "key": f"mist:{r['tid']}",
            "title": (f"Mistake review — "
                      f"{r['subj']} {r['top']}"),
            "est": 25, "subject": r["subj"] or "",
            "chapter": r["top"] or "",
            "subject_id": r["sid"]
            and str(r["sid"]),
            "topic_id": r["tid"] and str(r["tid"]),
            "meta": {"reps": int(r["reps"]),
                     "test_in_days": min_test}})
    rows = await qrows(
        """SELECT h.id, h.title, h.due_at, h.total_q,
                  h.completed_q, h.est_minutes,
                  s.name subj, h.subject_id sid
           FROM homework h
           LEFT JOIN subjects s ON s.id=h.subject_id
           WHERE h.user_id=$1::uuid
           AND h.status NOT IN ('completed',
                                'abandoned')
           ORDER BY h.due_at NULLS LAST
           LIMIT 20""", u["id"])
    for r in rows:
        rem = None
        if r["total_q"]:
            rem = r["total_q"] - r["completed_q"]
        if r["est_minutes"]:
            est = r["est_minutes"]
        elif rem:
            est = max(15, min(60, 2 * rem))
        else:
            est = 30
        cands.append({
            "kind": "homework",
            "key": f"hw:{r['id']}",
            "title": r["title"], "est": est,
            "subject": r["subj"] or "",
            "subject_id": r["sid"] and str(r["sid"]),
            "topic_id": None,
            "meta": {"due_at": r["due_at"],
                     "remaining": rem,
                     "homework_id": str(r["id"]),
                     "test_in_days": min_test}})
    rows = await qrows(
        """SELECT t.id tid, t.name top,
                  t.subject_id sid, s.name subj,
                  m.mastery, m.last_evidence
           FROM topics t
           JOIN subjects s ON s.id=t.subject_id
           LEFT JOIN mastery m
             ON m.user_id=t.user_id
             AND m.topic_id=t.id
           WHERE t.user_id=$1::uuid
           ORDER BY m.mastery ASC NULLS FIRST
           LIMIT 8""", u["id"])
    for r in rows:
        stale = 999
        if r["last_evidence"]:
            stale = (now.date()
                     - r["last_evidence"].date()).days
        cands.append({
            "kind": "study",
            "key": f"study:{r['tid']}",
            "title": (f"Study — {r['subj']} — "
                      f"{r['top']}"),
            "est": 45, "subject": r["subj"],
            "chapter": r["top"],
            "subject_id": str(r["sid"]),
            "topic_id": str(r["tid"]),
            "meta": {"mastery": r["mastery"],
                     "stale_days": stale,
                     "test_in_days": min_test}})
    return cands


async def cmd_next(u, chat, max_minutes=None,
                   exclude=None):
    now, today = now_tz(), today_d()
    live = await live_session(u["id"])
    if live:
        await cmd_dashboard(u, chat)
        return
    avail, _ = day_minutes(u, today)
    spent = await spent_today(u, today)
    lmins = await live_minutes(u, now)
    remaining = max(0, avail - spent - lmins)
    if max_minutes:
        remaining = min(remaining, max_minutes)
    cands = await build_candidates(u, now)
    scored = []
    for c in cands:
        sc, rs = score_task(c, now, u["energy"])
        scored.append((c, sc, rs))
    pick = choose_next(scored, remaining, exclude)
    if not pick:
        await send(
            chat,
            "Nothing left that fits right now — rest "
            "is also strategy. 🌙 Log something with "
            "'50 questions 39 correct'.", MENU_KB)
        return
    c, sc, rs = pick
    pid = await new_pending(
        u, "task_start",
        {"cand": c, "score": sc, "reasons": rs},
        "engine")
    why = "\n".join(f"• {esc(r)}" for r in rs)
    icons = {"revision": "🔁", "quiz": "🧠",
             "homework": "📝", "test_prep": "🧪",
             "mistake_review": "🧨", "study": "📖"}
    icon = icons.get(c["kind"], "▶️")
    txt = (f"▶️ <b>START NEXT</b>\n\n"
           f"{icon} <b>{esc(c['title'])}</b>\n"
           f"⏱ {fm(c['est'])} • fits your "
           f"{fm(remaining)} left\n\n"
           f"<b>Why:</b>\n{why}")
    await send(chat, txt, IK(
        [("▶️ Start", f"go:{pid}"),
         ("🔄 Another", f"go:{pid}:alt")],
        [("🏠 Menu", "nav:menu")]))


async def start_live_session(u, chat, cand):
    existing = await live_session(u["id"])
    if existing:
        await cmd_dashboard(u, chat)
        return
    now = now_tz()
    await qrow(
        """INSERT INTO study_sessions
           (user_id, subject_id, topic_id, source,
            status, title, started_at)
           VALUES($1::uuid,$2::uuid,$3::uuid,'live',
            'active',$4,$5) RETURNING id""",
        u["id"], cand.get("subject_id"),
        cand.get("topic_id"), cand["title"], now)
    await send(
        chat,
        f"⏱ <b>Session started</b> — "
        f"{esc(cand['title'])}\n"
        f"Tap ✅ when done. I'll ask how many "
        f"questions you solved.",
        IK([("⏸ Pause", "ses:pause"),
            ("✅ Finish", "ses:finish")],
           [("❌ Abandon", "ses:abandon")]))


async def cmd_plan(u, chat):
    now, today = now_tz(), today_d()
    avail, busy = day_minutes(u, today)
    cands = await build_candidates(u, now)
    last = await qval(
        """SELECT MAX(created_at::date)
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'""", u["id"])
    missed = 0
    if last and (today - last).days > 1:
        missed = (today - last).days - 1
    plan = build_plan(cands, avail, u["energy"],
                      missed, now)
    tasks = json.dumps(plan["tasks"])
    await q(
        """INSERT INTO daily_plans
           (user_id, plan_date, available_minutes,
            tasks)
           VALUES($1::uuid,$2,$3,$4::jsonb)
           ON CONFLICT(user_id, plan_date)
           DO UPDATE SET
             available_minutes=
               EXCLUDED.available_minutes,
             tasks=EXCLUDED.tasks,
             created_at=now()""",
        u["id"], today, plan["available"], tasks)
    lines = [f"📅 <b>PLAN — "
             f"{today.strftime('%a %d %b')}</b>",
             f"Capacity: <b>{fm(plan['available'])}</b> "
             "(after school/coaching/meals)",
             f"Planned: <b>{fm(plan['allocated'])}</b>",
             ""]
    for i, t in enumerate(plan["tasks"], 1):
        lines.append(f"{i}. {esc(t['title'])} — "
                     f"{fm(t['est'])}")
    if plan["missed"]:
        lines.append(
            f"\n<i>Missed {plan['missed']} day(s) — "
            f"kept urgent items, spread the rest.</i>")
    if plan["deferred"]:
        lines += ["", "<i>Kept for later:</i>"]
        for t, r in plan["deferred"][:5]:
            lines.append(f"• {esc(t)} — {esc(r)}")
    await send(chat, "\n".join(lines), IK(
        [("▶️ Start Next", "nav:next"),
         ("🔄 Replan", "nav:plan")]))


async def cmd_homework(u, chat):
    rows = await qrows(
        """SELECT h.*, s.name subj FROM homework h
           LEFT JOIN subjects s ON s.id=h.subject_id
           WHERE h.user_id=$1::uuid
           AND h.status NOT IN ('completed',
                                'abandoned')
           ORDER BY h.due_at NULLS LAST
           LIMIT 15""", u["id"])
    now = now_tz()
    if not rows:
        await send(chat, "📝 No open homework. 🎉\n"
                   "Add: 'homework DPP 4, "
                   "30 questions, due monday'.", MENU_KB)
        return
    lines = ["📝 <b>HOMEWORK</b>", ""]
    for r in rows:
        mark = "•"
        if r["due_at"] and r["due_at"] < now:
            mark = "⚠️"
        line = f"{mark} {esc(r['title'])}"
        if r["subj"]:
            line += f" ({esc(r['subj'])})"
        if r["total_q"]:
            done = r["completed_q"] or 0
            pct = 100 * done / r["total_q"]
            line += (f" [{done}/{r['total_q']}] "
                     f"{bar(pct, 6)}")
        if r["due_at"]:
            d = (r["due_at"].date() - now.date()).days
            if d == 0:
                line += " — due today!"
            elif d > 0:
                line += f" — in {d}d"
            else:
                line += f" — {-d}d OVERDUE"
        lines.append(line)
    lines += ["", "<i>Report: 'did 25 of 50 DPP'.</i>"]
    await send(chat, "\n".join(lines), MENU_KB)


async def cmd_tests(u, chat):
    rows = await qrows(
        """SELECT t.*, s.name subj FROM tests t
           LEFT JOIN subjects s ON s.id=t.subject_id
           WHERE t.user_id=$1::uuid
           ORDER BY t.test_at DESC NULLS LAST
           LIMIT 12""", u["id"])
    if not rows:
        await send(chat, "🧪 No tests yet. Add: "
                   "'physics test on sunday'.", MENU_KB)
        return
    now = now_tz()
    up = [r for r in rows
          if r["test_at"] and r["test_at"] >= now
          and r["status"] == "scheduled"]
    done = [r for r in rows
            if r["status"] == "completed"]
    lines = ["🧪 <b>TESTS</b>", ""]
    if up:
        lines.append("<b>Upcoming</b>")
        for r in sorted(up, key=lambda x: x["test_at"]):
            d = (r["test_at"].date() - now.date()).days
            if d == 0:
                tag = "TODAY"
            else:
                tag = f"in {d}d"
            lines.append(f"• {esc(r['name'])} "
                         f"({esc(r['subj'] or '')}) — "
                         f"{tag}")
        lines.append("")
    if done:
        lines.append("<b>Results</b>")
        for r in done[:6]:
            if r["total_marks"]:
                pct = (100 * (r["obtained_marks"] or 0)
                       / r["total_marks"])
                lines.append(
                    f"• {esc(r['name'])} — "
                    f"{r['obtained_marks']}/"
                    f"{r['total_marks']} "
                    f"({pct:.0f}%) {bar(pct, 6)}")
    await send(chat, "\n".join(lines), MENU_KB)


async def cmd_revision(u, chat):
    rows = await qrows(
        """SELECT rv.*, s.name subj, t.name top
           FROM revisions rv
           LEFT JOIN subjects s ON s.id=rv.subject_id
           LEFT JOIN topics t ON t.id=rv.topic_id
           WHERE rv.user_id=$1::uuid
           AND rv.due_at < now() + interval '2 days'
           ORDER BY rv.due_at LIMIT 12""", u["id"])
    if not rows:
        await send(chat, "🔁 No revision due. It "
                   "appears automatically once you log "
                   "questions — accuracy decides when "
                   "it returns.", MENU_KB)
        return
    now = now_tz()
    lines = ["🔁 <b>REVISION QUEUE</b>", ""]
    for r in rows:
        od = max(0, (now - r["due_at"]).days)
        mode = ""
        if r["mode"] == "practice":
            mode = " 🧠practice"
        over = ""
        if od:
            over = f" (overdue {od}d) 🔴"
        lines.append(f"• {esc(r['subj'] or '')} / "
                     f"{esc(r['top'] or '')} — "
                     + r["rev_type"].replace("_", " ")
                     + over + mode)
    await send(chat, "\n".join(lines),
               IK([("▶️ Start Next", "nav:next")]))


async def cmd_mistakes(u, chat):
    rows = await qrows(
        """SELECT SUM(m.count) c, m.mtype,
                  s.name subj, t.name top
           FROM mistakes m
           LEFT JOIN subjects s ON s.id=m.subject_id
           LEFT JOIN topics t ON t.id=m.topic_id
           WHERE m.user_id=$1::uuid
           AND m.resolved=false
           GROUP BY m.mtype, s.name, t.name
           ORDER BY c DESC LIMIT 12""", u["id"])
    if not rows:
        await send(chat, "🧨 No mistakes banked. "
                   "Report: '3 conceptual mistakes "
                   "in rotation'.", MENU_KB)
        return
    lines = ["🧨 <b>MISTAKE BANK</b>", ""]
    for r in rows:
        lines.append(f"• {esc(r['subj'] or '')} / "
                     f"{esc(r['top'] or '')} — "
                     f"{r['mtype']} × {r['c']}")
    lines += ["", "<i>2+ repeats = it takes over "
                  "your revision.</i>"]
    await send(chat, "\n".join(lines), MENU_KB)


async def cmd_syllabus(u, chat):
    rows = await qrows(
        """SELECT s.name subj, COUNT(t.id) n,
                  (SELECT COUNT(*) FROM mastery m
                   WHERE m.user_id=s.user_id
                   AND m.subject_id=s.id
                   AND m.events>0) tracked
           FROM subjects s
           LEFT JOIN topics t ON t.subject_id=s.id
           WHERE s.user_id=$1::uuid
           GROUP BY s.name, s.user_id, s.id
           ORDER BY s.name""", u["id"])
    if not rows:
        await send(chat, "📚 No subjects yet. Send a "
                   "syllabus photo, or type:\n"
                   "<code>Physics: Rotation, SHM</code>",
                   MENU_KB)
        return
    lines = ["📚 <b>SYLLABUS</b>", ""]
    for r in rows:
        lines.append(f"• <b>{esc(r['subj'])}</b> — "
                     f"{r['n']} topics, "
                     f"{r['tracked']} with data")
    lines += ["", "<i>Send a syllabus photo to "
                  "import more.</i>"]
    await send(chat, "\n".join(lines), MENU_KB)


async def cmd_notes(u, chat):
    rows = await qrows(
        """SELECT id, title FROM resources
           WHERE user_id=$1::uuid
           ORDER BY created_at DESC LIMIT 10""",
        u["id"])
    if not rows:
        await send(chat, "📎 No notes yet. Send any "
                   "photo — I'll read it and can save "
                   "the text.", MENU_KB)
        return
    kb_rows = []
    for r in rows[:8]:
        label = f"📄 {(r['title'] or 'note')[:40]}"
        kb_rows.append([(label, f"note:{r['id']}")])
    await send(chat, "📎 <b>YOUR NOTES</b>",
               IK(*kb_rows))


async def cmd_analytics(u, chat):
    today = today_d()
    wk_start = sod(today - timedelta(days=7))
    wk_end = sod(today + timedelta(days=1))
    prev_start = sod(today - timedelta(days=14))
    r = await qrow(
        """SELECT COALESCE(SUM(duration_minutes),0)
                 mins,
                  COALESCE(SUM(questions_attempted),0)
                 q,
                  COALESCE(SUM(questions_correct),0) c
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >= $2
           AND created_at < $3""",
        u["id"], wk_start, wk_end)
    prev = await qval(
        """SELECT COALESCE(SUM(duration_minutes),0)
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >= $2
           AND created_at < $3""",
        u["id"], prev_start, wk_start)
    if not r or (r["mins"] == 0 and r["q"] == 0):
        await send(chat, "📊 Not enough data yet — "
                   "log a few sessions first.", MENU_KB)
        return
    mins = int(r["mins"])
    q = int(r["q"])
    c = int(r["c"])
    acc = 100.0 * c / q if q else 0
    ref = max(1200, int(prev or 0) * 120 // 100)
    if prev is None or mins >= prev:
        trend = "↑"
    else:
        trend = "↓"
    lines = ["📊 <b>LAST 7 DAYS</b>", "",
             f"Study      "
             f"{bar(min(100, 100 * mins / ref))} "
             f"{fm(mins)} {trend}",
             f"Questions  {bar(min(100, 100 * q / 600))} "
             f"{q}",
             f"Accuracy   {bar(acc)} {acc:.0f}%"]
    subs = await qrows(
        """SELECT s.name, SUM(ss.questions_attempted) q,
                  SUM(ss.questions_correct) c
           FROM study_sessions ss
           JOIN subjects s ON s.id=ss.subject_id
           WHERE ss.user_id=$1::uuid
           AND ss.status='finished'
           AND ss.created_at >= $2
           AND ss.created_at < $3
           AND ss.questions_attempted > 0
           GROUP BY s.name
           ORDER BY SUM(ss.questions_correct)::float
             / NULLIF(SUM(ss.questions_attempted),0)
             ASC""",
        u["id"], wk_start, wk_end)
    if subs:
        lines += ["", "<b>By subject</b>"]
        for srow in subs:
            sa = 0
            if srow["q"]:
                sa = 100 * (srow["c"] or 0) / srow["q"]
            lines.append(f"{srow['name']:<11} "
                         f"{bar(sa, 8)} {sa:.0f}% "
                         f"({srow['q']}q)")
    mast = await qrows(
        """SELECT s.name subj, t.name top, m.mastery
           FROM mastery m
           JOIN topics t ON t.id=m.topic_id
           JOIN subjects s ON s.id=m.subject_id
           WHERE m.user_id=$1::uuid AND m.events>0
           ORDER BY m.mastery ASC LIMIT 5""", u["id"])
    if mast:
        lines += ["", "<b>Weakest topics</b>"]
        for m in mast:
            lines.append(f"• {esc(m['subj'])}/"
                         f"{esc(m['top'])} — "
                         f"{m['mastery']:.0f}% "
                         f"{bar(m['mastery'], 8)}")
    dates = await qrows(
        """SELECT DISTINCT created_at::date d
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND (duration_minutes > 0
                OR questions_attempted > 0)
           ORDER BY d DESC LIMIT 60""", u["id"])
    streak = 0
    if dates:
        ds = {r["d"] for r in dates}
        cur = today
        if today not in ds:
            cur = today - timedelta(days=1)
        while cur in ds:
            streak += 1
            cur -= timedelta(days=1)
    if streak:
        lines += ["", f"🔥 <b>{streak}-day streak</b>"]
    rep = await qrows(
        """SELECT t.name top, SUM(m.count) c
           FROM mistakes m
           JOIN topics t ON t.id=m.topic_id
           WHERE m.user_id=$1::uuid
           AND m.resolved=false
           GROUP BY t.name
           ORDER BY c DESC LIMIT 3""", u["id"])
    if rep:
        lines += ["", "⚠️ <b>Repeated errors</b>"]
        for i, r in enumerate(rep, 1):
            lines.append(f"{i}. {esc(r['top'])} — "
                         f"{r['c']}")
    await send(chat, "\n".join(lines), MENU_KB)


async def cmd_settings(u, chat):
    lines = ["⚙️ <b>SETTINGS</b>", ""]
    nm = u['display_name'] or u['first_name']
    lines.append(f"👤 {esc(nm or 'Student')}")
    goal = esc(u["exam_goal"] or "—")
    if u["exam_date"]:
        days = (u["exam_date"] - today_d()).days
        goal += f" ({days}d away)"
    lines.append(f"🎯 {goal}")
    lines.append("😴 " + u["wake_time"].strftime("%H:%M")
                 + "–"
                 + u["sleep_time"].strftime("%H:%M"))
    if u["school_days"]:
        lines.append("🏫 "
                     + u["school_start"].strftime(
                         "%H:%M")
                     + "–"
                     + u["school_end"].strftime("%H:%M"))
    if u["coaching_days"]:
        lines.append("📖 "
                     + u["coaching_start"].strftime(
                         "%H:%M")
                     + "–"
                     + u["coaching_end"].strftime(
                         "%H:%M"))
    lines.append(f"⚡ Energy: {u['energy']}")
    lines += ["", "<i>Editable by telling me, e.g. "
                  "'wake at 6' or 'school 8-2 "
                  "mon-sat'.</i>"]
    await send(chat, "\n".join(lines), IK(
        [("🔄 Restart onboarding", "ob:restart")],
        [("🧨 WIPE ALL DATA", "reset:ask")]))

# ============================================================
# QUIZ
# ============================================================
async def start_quiz(u, chat, topic_text, count=5,
                     subject_id=None, topic_id=None,
                     topic_label=None):
    label = topic_label or topic_text or "mixed"
    count = max(1, min(8, count))
    goal = u.get("exam_goal") or "competitive exam"
    prompt = (
        f'Generate {count} multiple-choice questions '
        f'on "{label}" for a {goal} student. '
        f'Return ONLY JSON: '
        f'{{"questions":[{{"question":"...",'
        f'"options":["a","b","c","d"],"answer":0,'
        f'"explanation":"short why"}}]}} '
        f'Rules: exactly 4 options; "answer" is the '
        f'0-based index of the correct option; '
        f'exactly one correct; no trick options; '
        f'exam-level difficulty.')
    key = "qz:" + hashlib.md5(
        prompt.encode()).hexdigest()
    try:
        raw = _cache_get(key)
        if raw is None:
            raw = _load_json(
                await ai_call(prompt,
                              max_tokens=200 * count))
            _cache_set(key, raw)
    except AIError:
        await send(chat, "🧠 My quiz brain is "
                   "rate-limited — try again in a "
                   "minute.", MENU_KB)
        return
    qs = []
    for x in (raw.get("questions") or [])[:count]:
        try:
            opts = [str(o)[:150]
                    for o in x["options"][:4]]
            ans = int(x["answer"])
            if (len(opts) == 4 and 0 <= ans <= 3
                    and x.get("question")):
                qs.append({
                    "question": str(x["question"])[:400],
                    "options": opts, "answer": ans,
                    "explanation": str(
                        x.get("explanation", ""))[:400]})
        except Exception:
            continue
    if len(qs) < 3:
        await send(chat, "🧠 Couldn't build a clean "
                   "quiz for that — try a more "
                   "specific topic.", MENU_KB)
        return
    if not topic_id and topic_text:
        subs = await get_subjects(u["id"])
        sid = subject_id
        if not sid and len(subs) == 1:
            sid = subs[0]["id"]
        if sid:
            t = await find_topic(u["id"], sid,
                                 topic_text)
            if not t:
                t = await upsert_topic(u["id"], sid,
                                       topic_text)
            topic_id = t["id"]
            subject_id = sid
    qid = await qval(
        """INSERT INTO quizzes
           (user_id, subject_id, topic_id,
            topic_label, questions)
           VALUES($1::uuid,$2::uuid,$3::uuid,$4,
                  $5::jsonb)
           RETURNING id::text""",
        u["id"], subject_id, topic_id, label,
        json.dumps(qs))
    await send_quiz_q(chat, qid, qs, 0, label)


async def send_quiz_q(chat, qid, qs, idx, label):
    q = qs[idx]
    letters = "ABCD"
    body = (f"🧠 <b>QUIZ — {esc(label)}</b> "
            f"({idx + 1}/{len(qs)})\n\n"
            f"{esc(q['question'])}\n\n")
    for i, o in enumerate(q["options"]):
        body += f"{letters[i]}. {esc(o)}\n"
    row = [(f"{letters[i]}",
            f"quiz:{qid}:{idx}:{i}")
           for i in range(4)]
    kb = IK(row, [("🛑 Stop", f"quiz:{qid}:stop")])
    await send(chat, body, kb)


async def quiz_answer(u, chat, qid, qi, opt):
    row = await qrow(
        """SELECT * FROM quizzes
           WHERE id=$1::uuid AND user_id=$2::uuid""",
        qid, u["id"])
    if not row or row["status"] != "active":
        return
    qs = json.loads(row["questions"])
    answers = json.loads(row["answers"])
    if len(answers) != qi:
        return
    answers.append(opt)
    await q(
        """UPDATE quizzes SET answers=$2::jsonb
           WHERE id=$1::uuid""",
        qid, json.dumps(answers))
    if len(answers) >= len(qs):
        await quiz_finish(u, chat, qid, qs, answers,
                          row)
    else:
        await send_quiz_q(chat, qid, qs,
                          len(answers),
                          row["topic_label"])


async def quiz_finish(u, chat, qid, qs, answers, row):
    correct = 0
    for a, qq in zip(answers, qs):
        if a == qq["answer"]:
            correct += 1
    att = len(answers)
    await q("UPDATE quizzes SET status='done' "
            "WHERE id=$1::uuid", qid)
    if att:
        await save_evidence(
            u, row["subject_id"], row["topic_id"],
            att, correct, 0, "quiz",
            title=f"quiz: {row['topic_label']}")
    letters = "ABCD"
    recap = []
    for a, qq in zip(answers, qs):
        mark = "✅" if a == qq["answer"] else "❌"
        line = (f"{mark} {letters[qq['answer']]}. "
                f"{esc(qq['options'][qq['answer']])}")
        if qq["explanation"]:
            line += f" — {esc(qq['explanation'])}"
        recap.append(line)
    acc = 100 * correct // att if att else 0
    lines = [f"🧠 <b>Quiz done: {correct}/{att}</b> "
             f"({acc}%)", ""]
    lines += recap[:8]
    lines += ["", "Logged as evidence — mastery & "
              "revision updated."]
    await send(chat, "\n".join(lines),
               IK([("▶️ Start Next", "nav:next")]))

# ============================================================
# ONBOARDING
# ============================================================
OB_STEPS = [
    ("name", "👤 What should I call you?",
     True, "text"),
    ("exam", "🎯 What are you preparing for?\n"
             "(e.g. 'JEE 2027', 'NEET', 'Boards')",
     True, "text"),
    ("exam_date", "📅 Main exam date?\n"
             "(e.g. '24 may 2027')\n/skip if not fixed",
     False, "date"),
    ("subjects", "📚 Your subjects, "
             "comma-separated\n(e.g. 'Physics, "
             "Chemistry, Maths')", True, "subjects"),
    ("wake", "🌅 Wake time? (e.g. '6:30')",
     True, "time"),
    ("sleep", "🌙 Sleep time? (e.g. '23:00')",
     True, "time"),
    ("school", "🏫 School hours?\n"
             "(e.g. '8-2 mon-sat')\n/skip if none",
     False, "class"),
    ("coaching", "📖 Coaching hours?\n"
             "(e.g. '5-8 mon,wed,fri')\n"
             "/skip if none", False, "class"),
]


async def ob_start(u, chat):
    first = OB_STEPS[0]
    await q(
        """UPDATE users SET ob_state='{}'::jsonb,
           ob_step=$2 WHERE id=$1::uuid""",
        u["id"], first[0])
    await send(
        chat,
        "🧠 <b>Welcome to StudyOS</b>\n\n"
        "I learn from what you actually do — not "
        "from plans you forget. A few quick "
        "questions, one at a time. Optional ones "
        "you can /skip.\n\n" + first[1])


async def ob_summary(u, chat):
    st = u["ob_state"] or {}
    lines = ["<b>Here's what I've understood.</b>",
             ""]
    if st.get("name"):
        lines.append(f"👤 {esc(st['name'])}")
    if st.get("exam"):
        line = f"🎯 {esc(st['exam'])}"
        if st.get("exam_date"):
            line += f" — {esc(st['exam_date'])}"
        lines.append(line)
    if st.get("subjects"):
        lines.append("📚 "
                     + esc(", ".join(st["subjects"])))
    if st.get("school"):
        lines.append(f"🏫 {esc(st['school'])}")
    if st.get("coaching"):
        lines.append(f"📖 {esc(st['coaching'])}")
    lines.append(f"😴 {st.get('wake')} – "
                 f"{st.get('sleep')}")
    lines += ["", "All correct?"]
    await send(chat, "\n".join(lines), IK(
        [("✅ Confirm", "ob:confirm")],
        [("🔄 Start over", "ob:restart")]))


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
        await q("UPDATE users SET ob_step=$2 "
                "WHERE id=$1::uuid",
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
    key, prompt, required, kind = step_def
    t = (text or "").strip()
    if t.lower() in SKIP_WORDS and not required:
        nxt = ob_next_step(key)
        await q(
            """UPDATE users SET ob_step=$2,
               ob_state=$3::jsonb
               WHERE id=$1::uuid""",
            u["id"], nxt, json.dumps(st))
        await ob_prompt(u, chat, nxt)
        return
    if kind == "text":
        if not t:
            await send(chat,
                       "A short answer works. " + prompt)
            return
        st[key] = t[:60]
    elif kind == "date":
        d = parse_date(t, today_d())
        if not d:
            await send(chat, "Couldn't read that date "
                       "— try '24 may 2027'.")
            return
        st[key] = d.isoformat()
    elif kind == "subjects":
        names = [x.strip().title()
                 for x in re.split(r"[,;]+", t)
                 if x.strip()]
        if not names:
            await send(chat, "List them like: "
                       "Physics, Chemistry, Maths")
            return
        st[key] = names[:8]
    elif kind == "time":
        tm = parse_time(t)
        if not tm:
            await send(chat,
                       "Try a time like '6:30'.")
            return
        st[key] = tm.strftime("%H:%M")
    elif kind == "class":
        pc = parse_class(t)
        if not pc:
            await send(chat, "Try: '8-2 mon-sat' "
                       "(time + days). Or /skip.")
            return
        st[key] = t[:60]
        st[key + "_data"] = {
            "s": pc[0].strftime("%H:%M"),
            "e": pc[1].strftime("%H:%M"),
            "d": pc[2]}
    nxt = ob_next_step(key)
    await q(
        """UPDATE users SET ob_step=$2,
           ob_state=$3::jsonb WHERE id=$1::uuid""",
        u["id"], nxt, json.dumps(st))
    await ob_prompt(u, chat, nxt)


async def ob_commit(u, chat):
    st = u["ob_state"] or {}
    for name in (st.get("subjects") or [])[:8]:
        await upsert_subject(u["id"], name)
    sch = st.get("school_data")
    coa = st.get("coaching_data")
    exam_d = None
    if st.get("exam_date"):
        try:
            exam_d = date.fromisoformat(st["exam_date"])
        except ValueError:
            pass
    wake = parse_time(st.get("wake") or "06:30")
    if not wake:
        wake = dtime(6, 30)
    sleep = parse_time(st.get("sleep") or "23:00")
    if not sleep:
        sleep = dtime(23, 0)
    sch_s = sch_e = coa_s = coa_e = None
    sch_d = coa_d = None
    if sch:
        sch_s = dtime.fromisoformat(sch["s"])
        sch_e = dtime.fromisoformat(sch["e"])
        sch_d = sch["d"]
    if coa:
        coa_s = dtime.fromisoformat(coa["s"])
        coa_e = dtime.fromisoformat(coa["e"])
        coa_d = coa["d"]
    await q(
        """UPDATE users SET display_name=$2,
           exam_goal=$3, exam_date=$4, wake_time=$5,
           sleep_time=$6, school_start=$7,
           school_end=$8, school_days=$9,
           coaching_start=$10, coaching_end=$11,
           coaching_days=$12, onboarded=true,
           ob_step=NULL WHERE id=$1::uuid""",
        u["id"], st.get("name"), st.get("exam"),
        exam_d, wake, sleep, sch_s, sch_e, sch_d,
        coa_s, coa_e, coa_d)
    await send(
        chat,
        "✅ <b>You're set up.</b>\n\nNow just talk "
        "to me:\n• \"I finished 50 physics "
        "questions, 39 correct\"\n• 📷 send a "
        "syllabus photo to import chapters\n"
        "• \"what should I study?\"",
        IK([("▶️ Start Next", "nav:next")],
           [("🏠 Dashboard", "nav:dash")]))

# ============================================================
# PHOTO / VOICE
# ============================================================
PHOTO_SYS = """You analyze a photo for a study app.
Read ONLY what is clearly visible. Never invent
content. Return ONLY JSON:
{"type":"syllabus|test|homework|notes|unknown",
 "text":"all readable text (or empty string)",
 "syllabus":[{"subject":"...","topics":["..."]}],
 "test":{"name":"...","subject":"...",
         "total":null,"obtained":null},
 "homework":{"title":"...","subject":"...",
             "questions":null},
 "confidence":"high|medium|low"}
Use "syllabus" when the image lists chapters per
subject. Use "test" for papers with visible marks.
Use "notes" otherwise."""


async def handle_photo(u, chat, msg):
    photo = msg["photo"][-1]
    try:
        img = await get_file_bytes(photo["file_id"])
    except Exception as e:
        LOG.warning("photo download failed: %s", e)
        await send(chat, "Couldn't download that "
                   "photo — try again.")
        return
    await send(chat, "🔍 Reading the image…")
    try:
        part = types.Part.from_bytes(
            data=img, mime_type="image/jpeg")
        contents = [PHOTO_SYS, part]
        raw = _load_json(
            await ai_call(contents, max_tokens=2000))
    except AIError:
        await send(chat, "My vision service is "
                   "rate-limited — try again in a "
                   "minute.", MENU_KB)
        return
    ptype = (raw.get("type") or "unknown").lower()
    conf = (raw.get("confidence") or "low").lower()
    text = (raw.get("text") or "")[:2500]
    if ptype == "unknown" or conf == "low":
        await send(chat, "I can't read this "
                   "confidently enough to save anything. "
                   "Could you type the key parts?",
                   MENU_KB)
        return
    if ptype == "syllabus" and raw.get("syllabus"):
        blocks = raw["syllabus"][:12]
        preview = ""
        for b in blocks:
            topics = ", ".join(
                esc(t)
                for t in (b.get("topics") or [])[:10])
            preview += (f"• <b>{esc(b.get('subject'))}"
                        f"</b>: {topics}\n")
        pid = await new_pending(u, "syllabus",
                                {"blocks": blocks},
                                "ai")
        await send(chat, "📷 <b>Syllabus detected"
                   "</b>\n\n" + preview
                   + "\nConfidence: " + conf.upper(),
                   confirm_kb(pid))
        return
    if ptype == "test" and raw.get("test"):
        t = raw["test"]
        f = {k: v for k, v in t.items()
             if v is not None}
        pid = await new_pending(u, "test_result", f,
                                "ai")
        order = [("name", "Test"),
                 ("subject", "Subject"),
                 ("total", "Total"),
                 ("obtained", "Obtained")]
        lines = card_lines(f, order)
        await send(chat, "📷 <b>Test paper detected"
                   "</b>\n\n" + "\n".join(lines)
                   + f"\n\nConfidence: {conf.upper()}",
                   confirm_kb(pid))
        return
    if ptype == "homework" and raw.get("homework"):
        h = raw["homework"]
        f = {k: v for k, v in h.items()
             if v is not None}
        subs = await get_subjects(u["id"])
        sid, sname = resolve_subject(f.get("subject"),
                                     subs)
        f["subject_id"] = sid
        if sname:
            f["subject"] = sname
        pid = await new_pending(u, "hw_add", f, "ai")
        if not sid and subs:
            header = (f"📷 <b>Homework detected</b>\n"
                      f"{esc(f.get('title'))}")
            await subject_pick_card(u, chat, pid,
                                    header)
            return
        order = [("title", "Homework"),
                 ("subject", "Subject"),
                 ("questions", "Questions")]
        lines = card_lines(f, order)
        await send(chat, "📷 <b>Homework detected"
                   "</b>\n\n" + "\n".join(lines)
                   + f"\n\nConfidence: {conf.upper()}",
                   confirm_kb(pid))
        return
    pid = await new_pending(
        u, "save_note",
        {"title": "Photo note", "content": text},
        "ai")
    await send(chat, "📷 I read this as notes:\n\n<i>"
               + esc(text[:800]) + "</i>\n\n"
               "Confidence: " + conf.upper(),
               confirm_kb(pid))


async def handle_voice(u, chat, msg):
    try:
        audio = await get_file_bytes(
            msg["voice"]["file_id"])
    except Exception as e:
        LOG.warning("voice download failed: %s", e)
        await send(chat, "Couldn't download that "
                   "voice note.")
        return
    await send(chat, "🎙 Transcribing…")
    try:
        part = types.Part.from_bytes(
            data=audio, mime_type="audio/ogg")
        contents = ["Transcribe this audio to plain "
                    "text. Output only the "
                    "transcription.", part]
        text = await ai_call(contents, json_mode=False,
                             max_tokens=300)
        text = text.strip()
    except AIError:
        await send(chat, "Voice transcription is "
                   "rate-limited right now — type it "
                   "instead?", MENU_KB)
        return
    if not text:
        await send(chat, "I couldn't hear that "
                   "clearly — try typing it.")
        return
    await send(chat, f"🎙 <i>{esc(text[:300])}</i>")
    await handle_text_msg(u, chat, text)

# ============================================================
# TEXT ROUTING — fast path first, AI second
# ============================================================
async def handle_text_msg(u, chat, text):
    if not u["onboarded"]:
        await ob_handle(u, chat, text)
        return
    t = text.strip()
    low = t.lower()
    pend = await qrow(
        """SELECT * FROM pending_actions
           WHERE user_id=$1::uuid
           AND kind='session_log'
           AND status='pending'
           ORDER BY created_at DESC LIMIT 1""",
        u["id"])
    if pend:
        await wizard_session_log(u, chat, dict(pend),
                                 t)
        return
    parts = low.split()
    cmd = ""
    if parts:
        cmd = parts[0].lstrip("/").split("@")[0]
    if cmd == "start":
        await cmd_start(u, chat)
        return
    if cmd in ("menu", "home"):
        await send(chat, "🏠 <b>StudyOS</b>", MENU_KB)
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
    if cmd in ("mistakes", "errorbank"):
        await cmd_mistakes(u, chat)
        return
    if cmd in ("analytics", "stats"):
        await cmd_analytics(u, chat)
        return
    if cmd == "quiz":
        rest = " ".join(low.split()[1:])
        await start_quiz(u, chat, rest or None)
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
    # ---- deterministic fast paths (no AI cost) ----
    energy = detect_energy(t)
    if energy and len(low.split()) <= 6:
        await set_energy(u, chat, energy)
        return
    pat = (r"(what should i study|what do i study|"
           r"whats next|what's next|next task|"
           r"start next)")
    if re.search(pat, low):
        await cmd_next(u, chat)
        return
    if low in ("plan", "make me a plan",
               "today's plan", "todays plan",
               "make today's plan"):
        await cmd_plan(u, chat)
        return
    m = re.search(r"(?:i (?:have|got|only have)|"
                  r"only)\s+(\d+)\s*"
                  r"(?:minutes|min|mins)\b", low)
    if m:
        await cmd_next(u, chat,
                       max_minutes=int(m.group(1)))
        return
    pq = parse_questions(t)
    if pq and (pq[1] is not None or pq[2] is not None):
        await candidate_log_session(
            u, chat, questions=pq[0], correct=pq[1],
            minutes=parse_duration(t),
            topic_maybe=t, origin="fast")
        return
    stud = re.search(r"\b(studied|revised|solved|"
                     r"did)\b", low)
    if stud and parse_duration(t) and not pq:
        await candidate_log_session(
            u, chat, questions=None, correct=None,
            minutes=parse_duration(t),
            topic_maybe=t, origin="fast")
        return
    # ---- AI router ----
    try:
        routed = await ai_route(t, u)
    except AIError:
        await send(chat, "My AI brain is "
                   "rate-limited — wait a minute and "
                   "resend. (Buttons still work: 🏠)",
                   MENU_KB)
        return
    intent = (routed.get("intent")
              or "none").lower()
    f = routed.get("fields") or {}
    conf = (routed.get("confidence")
            or "medium").lower()
    if intent in ("chat", "none"):
        reply = routed.get("reply")
        if reply:
            await send(chat, esc(reply), MENU_KB)
        else:
            await cmd_dashboard(u, chat)
        return
    if intent == "query":
        view = (f.get("view") or "dashboard").lower()
        views = {"plan": cmd_plan, "next": cmd_next,
                 "homework": cmd_homework,
                 "tests": cmd_tests,
                 "revision": cmd_revision,
                 "analytics": cmd_analytics,
                 "mistakes": cmd_mistakes,
                 "syllabus": cmd_syllabus,
                 "dashboard": cmd_dashboard}
        fn = views.get(view, cmd_dashboard)
        await fn(u, chat)
        return
    if intent == "energy":
        lvl = (f.get("level") or "").lower()
        if lvl in ("energetic", "normal", "tired",
                   "exhausted"):
            await set_energy(u, chat, lvl)
        else:
            await cmd_dashboard(u, chat)
        return
    if intent == "tutor":
        await tutor_reply(u, chat,
                          f.get("question") or t)
        return
    if intent == "quiz":
        await start_quiz(
            u, chat,
            f.get("topic") or f.get("subject"),
            count=int(f.get("count") or 5))
        return
    if intent == "log_session":
        await candidate_log_session(
            u, chat,
            questions=clampi(f.get("questions"), 1,
                             999),
            correct=clampi(f.get("correct"), 0, 999),
            minutes=clampi(f.get("minutes"), 1, 960),
            subject=f.get("subject"),
            topic=f.get("topic"),
            day=f.get("day"),
            origin="ai", confidence=conf)
        return
    if intent == "hw_add":
        await candidate_hw(u, chat, f, conf)
        return
    if intent == "hw_progress":
        if f.get("done") is None:
            await send(chat, "How many did you do? "
                       "e.g. 'did 25 of 50 DPP'")
            return
        pid = await new_pending(u, "hw_progress", f,
                                "ai")
        title = esc(f.get("title") or "homework")
        await send(chat, f"📝 <b>{title}</b>\n"
                   f"Done now: <b>{f.get('done')}"
                   f"</b> more",
                   confirm_kb(pid))
        return
    if intent == "test_add":
        await candidate_test_add(u, chat, f, conf)
        return
    if intent == "test_result":
        clean = {k: v for k, v in f.items()
                 if v is not None}
        pid = await new_pending(u, "test_result",
                                clean, "ai")
        order = [("name", "Test"),
                 ("subject", "Subject"),
                 ("obtained", "Obtained"),
                 ("total", "Total")]
        lines = card_lines(f, order)
        flag = ""
        if conf != "high":
            flag = " ⚠️"
        await send(chat, "I found:\n\n"
                   + "\n".join(lines)
                   + f"\n\nConfidence: <b>"
                   f"{conf.upper()}{flag}</b>",
                   confirm_kb(pid))
        return
    if intent == "mistake_add":
        await candidate_mistake(u, chat, f, conf)
        return
    if intent == "syllabus":
        blocks = f.get("blocks") or []
        if not blocks:
            await cmd_syllabus(u, chat)
            return
        pid = await new_pending(u, "syllabus",
                                {"blocks": blocks},
                                "ai")
        preview = ""
        for b in blocks[:10]:
            topics = ", ".join(
                esc(x)
                for x in (b.get("topics") or [])[:10])
            preview += (f"• <b>"
                        f"{esc(b.get('subject'))}"
                        f"</b>: {topics}\n")
        await send(chat, "📚 <b>Syllabus</b>\n\n"
                   + preview, confirm_kb(pid))
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
        """UPDATE users SET energy=$2,
           energy_set_at=now() WHERE id=$1::uuid""",
        u["id"], level)
    u["energy"] = level
    msgs = {
        "exhausted": "Understood. Today: light recall "
                     "and mistake review only — "
                     "full effort again tomorrow. 🌙",
        "tired": "Got it — I'll keep today short "
                 "and light.",
        "energetic": "Nice. I'll use that energy. 🔥",
        "normal": "Noted. Back to normal load."}
    await send(chat, msgs[level],
               IK([("▶️ Start Next", "nav:next")]))


async def tutor_reply(u, chat, question):
    ctx = await ai_context(u)
    prompt = (f"STUDENT CONTEXT:\n{ctx}\n\n"
              f"Answer this study question. Be clear "
              f"and concise (max 200 words), one "
              f"concrete example, one common mistake "
              f"to avoid. Never invent statistics "
              f"about the student.\n\n"
              f"QUESTION: {question}")
    key = "tu:" + hashlib.md5(
        prompt.encode()).hexdigest()
    try:
        txt = _cache_get(key)
        if txt is None:
            txt = await ai_call(prompt,
                                json_mode=False,
                                max_tokens=800)
            _cache_set(key, txt)
        title = esc(question[:80])
        await send(chat, f"📖 <b>{title}</b>\n\n"
                  + esc(txt),
                  IK([("▶️ Back to studying",
                       "nav:next")]))
    except AIError:
        rows = await qrows(
            """SELECT s.name subj, t.name top,
                      m.mastery FROM mastery m
               JOIN topics t ON t.id=m.topic_id
               JOIN subjects s ON s.id=m.subject_id
               WHERE m.user_id=$1::uuid
               AND m.events>0
               ORDER BY m.mastery ASC LIMIT 3""",
            u["id"])
        lines = ["My tutor brain is rate-limited "
                 "right now. Your weakest topics:"]
        for r in rows:
            lines.append(f"• {r['subj']}/{r['top']}"
                         f" — {r['mastery']:.0f}%")
        if not rows:
            lines.append("• log more sessions to "
                         "build mastery data")
        await send(chat, "\n".join(lines), MENU_KB)


async def candidate_log_session(
        u, chat, questions, correct, minutes,
        subject=None, topic=None, day=None,
        topic_maybe=None, origin="fast",
        confidence="high"):
    subs = await get_subjects(u["id"])
    if not subs:
        await send(chat, "You have no subjects yet — "
                   "send 'Physics: Rotation, SHM' or "
                   "a syllabus photo first.", MENU_KB)
        return
    if (questions is not None and correct
            is not None and correct > questions):
        correct = None
    if not subject and topic_maybe:
        for s in subs:
            if s["name"].lower() in topic_maybe.lower():
                subject = s["name"]
                break
    sid, sname = resolve_subject(subject, subs)
    fields = {"questions": questions,
              "correct": correct, "minutes": minutes,
              "subject": sname or subject,
              "subject_id": sid, "topic": topic,
              "day": day if day == "yesterday"
              else None}
    pid = await new_pending(u, "log_session",
                            fields, origin)
    lines = ["I found:", ""]
    if questions:
        lines.append(f"Questions: <b>{questions}</b>")
        if correct is not None:
            pct = 100 * correct // questions
            lines.append(f"Correct: <b>{correct}</b> "
                         f"({pct}%)")
    else:
        lines.append("Questions: <b>not given</b>")
    if minutes:
        lines.append(f"Duration: <b>{fm(minutes)}</b>")
    lines.append(f"Subject: <b>"
                 f"{esc(sname or subject or '?')}"
                 f"</b>")
    if topic:
        lines.append(f"Topic: <b>{esc(topic)}</b>")
    if origin == "fast" and sid and confidence == "high":
        res = await do_log_session(u, fields)
        await q(
            """UPDATE pending_actions
               SET status='confirmed'
               WHERE id=$1::uuid""", pid)
        undo_pid = await new_pending(
            u, "undo_log", {"fields": fields}, "fast")
        await send(chat, res
                   + "\n\n<i>(Tap ↩️ if wrong)</i>",
                   IK([("↩️ Undo",
                        f"cfm:{undo_pid}:undo")]))
        return
    if not sid:
        await subject_pick_card(u, chat, pid,
                                "\n".join(lines))
        return
    flag = ""
    if confidence != "high":
        flag = " ⚠️"
    await send(chat, "\n".join(lines)
               + f"\n\nConfidence: <b>"
               f"{confidence.upper()}{flag}</b>",
               confirm_kb(pid))


async def candidate_hw(u, chat, f, conf):
    subs = await get_subjects(u["id"])
    sid, sname = resolve_subject(f.get("subject"),
                                 subs)
    title = (f.get("title") or "").strip()[:120]
    if not title:
        await send(chat, "What's it called? e.g. "
                   "'add DPP 4, 40 questions, "
                   "due friday'")
        return
    fields = {"title": title,
              "subject": sname or f.get("subject"),
              "subject_id": sid,
              "questions": clampi(f.get("questions"),
                                  1, 999),
              "minutes": clampi(f.get("minutes"),
                                5, 480),
              "due": f.get("due"),
              "hw_type": f.get("hw_type")
              or "custom"}
    pid = await new_pending(u, "hw_add", fields, "ai")
    if not sid and subs:
        await subject_pick_card(
            u, chat, pid,
            f"📝 <b>{esc(title)}</b>")
        return
    order = [("title", "Homework"),
             ("subject", "Subject"),
             ("questions", "Questions"),
             ("due", "Due"),
             ("minutes", "Est. minutes")]
    lines = card_lines(fields, order)
    flag = ""
    if conf != "high":
        flag = " ⚠️"
    await send(chat, "I found:\n\n"
               + "\n".join(lines)
               + f"\n\nConfidence: <b>{conf.upper()}"
               f"{flag}</b>", confirm_kb(pid))


async def candidate_test_add(u, chat, f, conf):
    subs = await get_subjects(u["id"])
    sid, sname = resolve_subject(f.get("subject"),
                                 subs)
    name = (f.get("name") or "Test").strip()[:120]
    fields = {"name": name,
              "subject": sname or f.get("subject"),
              "subject_id": sid,
              "date": f.get("date"),
              "total_marks": clampi(
                  f.get("total_marks"), 1, 1000)}
    pid = await new_pending(u, "test_add", fields,
                            "ai")
    if not sid and subs:
        await subject_pick_card(
            u, chat, pid, f"🧪 <b>{esc(name)}</b>")
        return
    order = [("name", "Test"),
             ("subject", "Subject"),
             ("date", "When"),
             ("total_marks", "Marks")]
    lines = card_lines(fields, order)
    flag = ""
    if conf != "high":
        flag = " ⚠️"
    await send(chat, "I found:\n\n"
               + "\n".join(lines)
               + f"\n\nConfidence: <b>{conf.upper()}"
               f"{flag}</b>", confirm_kb(pid))


async def candidate_mistake(u, chat, f, conf):
    subs = await get_subjects(u["id"])
    sid, sname = resolve_subject(f.get("subject"),
                                 subs)
    mtype = (f.get("mtype") or "unknown").lower()
    fields = {"subject": sname or f.get("subject"),
              "subject_id": sid,
              "topic": f.get("topic"),
              "count": clampi(f.get("count"), 1, 50)
              or 1,
              "mtype": mtype,
              "description": (f.get("description")
                              or "")[:200]}
    pid = await new_pending(u, "mistake_add",
                            fields, "ai")
    if not sid and subs:
        header = (f"🧨 <b>{fields['count']} × "
                  f"{esc(mtype)} mistake(s)</b>")
        await subject_pick_card(u, chat, pid, header)
        return
    order = [("count", "Count"),
             ("mtype", "Type"),
             ("subject", "Subject"),
             ("topic", "Topic")]
    lines = card_lines(fields, order)
    flag = ""
    if conf != "high":
        flag = " ⚠️"
    await send(chat, "I found:\n\n"
               + "\n".join(lines)
               + f"\n\nConfidence: <b>{conf.upper()}"
               f"{flag}</b>", confirm_kb(pid))

# ============================================================
# SESSION CONTROLS
# ============================================================
async def wizard_session_log(u, chat, pend, text):
    pid = str(pend["id"])
    if text.lower().startswith("/skip"):
        await q(
            """UPDATE pending_actions
               SET status='confirmed'
               WHERE id=$1::uuid""", pid)
        await send(chat, "Logged as time-only. 🏠",
                   MENU_KB)
        return
    pq = parse_questions(text)
    if pq and pq[1] is None:
        m = re.match(r"^(\d+)\s*$", text.strip())
        if m:
            pq = (int(m.group(1)), None, None)
    if pq and pq[1] is not None:
        payload = json.loads(pend["payload"])
        sess_id = payload.get("session_id")
        await q(
            """UPDATE study_sessions
               SET questions_attempted=$2,
               questions_correct=$3
               WHERE id=$1::uuid""",
            sess_id, pq[0], pq[1])
        s = await qrow(
            """SELECT * FROM study_sessions
               WHERE id=$1::uuid""", sess_id)
        res = {}
        if s and s["topic_id"] and s["subject_id"]:
            await recompute(u["id"], s["subject_id"],
                            s["topic_id"])
            acc = pq[1] / pq[0]
            nr = await apply_revision(
                u["id"], s["subject_id"],
                s["topic_id"], acc, now_tz())
            m = await qrow(
                """SELECT mastery, events
                   FROM mastery
                   WHERE user_id=$1::uuid
                   AND topic_id=$2::uuid""",
                u["id"], s["topic_id"])
            if m:
                res = {"mastery": m["mastery"],
                       "events": m["events"],
                       "next_rev": nr}
        elif s and s["subject_id"]:
            await recompute(u["id"], s["subject_id"])
            m = await qrow(
                """SELECT mastery FROM mastery
                   WHERE user_id=$1::uuid
                   AND subject_id=$2::uuid
                   AND topic_id IS NULL""",
                u["id"], s["subject_id"])
            if m:
                res = {"mastery": m["mastery"]}
        await q(
            """UPDATE pending_actions
               SET status='confirmed'
               WHERE id=$1::uuid""", pid)
        acc = 100 * pq[1] // pq[0]
        lines = [f"✅ Logged: {pq[0]} questions, "
                 f"{pq[1]} correct ({acc}%)."]
        if res.get("mastery") is not None:
            ev = res.get("events")
            line = f"🧠 Mastery: {res['mastery']:.0f}%"
            if ev:
                line += f" ({ev} sessions)"
            lines.append(line)
        if res.get("next_rev"):
            lines.append("🔁 Next revision: "
                         + res["next_rev"].strftime(
                             "%d %b"))
        await send(chat, "\n".join(lines), MENU_KB)
        return
    await send(chat, "Try: '25 questions 18 "
               "correct' — or /skip.")


async def session_ctl(u, chat, op):
    now = now_tz()
    s = await live_session(u["id"])
    if not s:
        await cmd_dashboard(u, chat)
        return
    if op == "pause":
        await q(
            """UPDATE study_sessions
               SET status='paused', paused_at=$2
               WHERE id=$1::uuid""",
            str(s["id"]), now)
        await send(chat, "⏸ Paused.",
                   IK([("▶️ Resume", "ses:resume")],
                      [("❌ Abandon", "ses:abandon")]))
    elif op == "resume":
        paused = s["paused_seconds"] or 0
        if s["paused_at"]:
            paused += int(
                (now - s["paused_at"]).total_seconds())
        await q(
            """UPDATE study_sessions
               SET status='active', paused_at=NULL,
               paused_seconds=$2
               WHERE id=$1::uuid""",
            str(s["id"]), paused)
        await send(chat, "▶️ Resumed.",
                   IK([("⏸ Pause", "ses:pause"),
                       ("✅ Finish", "ses:finish")],
                      [("❌ Abandon",
                        "ses:abandon")]))
    elif op == "finish":
        paused = s["paused_seconds"] or 0
        if s["status"] == "paused" and s["paused_at"]:
            paused += int(
                (now - s["paused_at"]).total_seconds())
        dur = (now - s["started_at"]).total_seconds()
        dur = max(0, int(dur - paused) // 60)
        await q(
            """UPDATE study_sessions
               SET status='finished', ended_at=$2,
               duration_minutes=$3,
               paused_seconds=$4, paused_at=NULL
               WHERE id=$1::uuid""",
            str(s["id"]), now, dur, paused)
        pid = await new_pending(
            u, "session_log",
            {"session_id": str(s["id"])}, "wizard")
        await send(
            chat,
            f"✅ Finished — {fm(dur)}.\n\n"
            f"How many questions did you attempt?\n"
            f"e.g. '25 questions 18 correct' "
            f"(or /skip)",
            IK([("🤷 Skip", f"cfm:{pid}:skipq")]))
    elif op == "abandon":
        await q(
            """UPDATE study_sessions
               SET status='abandoned', ended_at=$2
               WHERE id=$1::uuid""",
            str(s["id"]), now)
        await send(chat, "Session discarded — no "
                   "guilt; the next one counts.",
                   MENU_KB)

# ============================================================
# CALLBACKS
# ============================================================
async def handle_callback(cb):
    data = cb.get("data", "")
    chat = cb["message"]["chat"]["id"]
    u = await ensure_user(cb["from"], chat)
    u = await lazy_tick(u, now_tz())
    await answer_cb(cb["id"])
    scope, _, rest = data.partition(":")
    try:
        if scope == "nav":
            await nav_cb(u, chat, rest)
        elif scope == "cfm":
            msg_id = cb["message"]["message_id"]
            await confirm_cb(u, chat, msg_id, rest)
        elif scope == "subj":
            await subj_cb(u, chat, rest)
        elif scope == "ses":
            await session_ctl(u, chat, rest)
        elif scope == "quiz":
            await quiz_cb(u, chat, rest)
        elif scope == "go":
            await go_cb(u, chat, rest)
        elif scope == "ob":
            await ob_cb(u, chat, rest)
        elif scope == "note":
            await note_cb(u, chat, rest)
        elif scope == "reset":
            await reset_cb(u, chat, rest)
    except Exception:
        LOG.exception("callback error: %s", data)
        await send(chat, "Something broke — the "
                   "action wasn't completed. Try "
                   "again.", MENU_KB)


async def nav_cb(u, chat, what):
    views = {
        "dash": cmd_dashboard, "next": cmd_next,
        "plan": cmd_plan, "hw": cmd_homework,
        "tests": cmd_tests, "rev": cmd_revision,
        "an": cmd_analytics, "mist": cmd_mistakes,
        "syl": cmd_syllabus}
    if what in views:
        await views[what](u, chat)
    elif what == "menu":
        await send(chat, "🏠 <b>StudyOS</b>", MENU_KB)
    elif what == "quiz":
        await start_quiz(u, chat, None)
    else:
        await cmd_dashboard(u, chat)


async def quiz_cb(u, chat, rest):
    qid, _, spec = rest.partition(":")
    if spec.startswith("stop"):
        await q(
            """UPDATE quizzes SET status='stopped'
               WHERE id=$1::uuid
               AND user_id=$2::uuid""",
            qid, u["id"])
        await send(chat, "Quiz stopped.", MENU_KB)
        return
    try:
        qi_s, opt_s = spec.split(":")
        await quiz_answer(u, chat, qid,
                          int(qi_s), int(opt_s))
    except ValueError:
        pass


async def go_cb(u, chat, rest):
    pid, _, mode = rest.partition(":")
    pend = await get_pending(u, pid)
    if not pend or pend["status"] != "pending":
        return
    payload = json.loads(pend["payload"])
    cand = payload["cand"]
    if mode == "alt":
        await q(
            """UPDATE pending_actions
               SET status='cancelled'
               WHERE id=$1::uuid""", pid)
        await cmd_next(u, chat,
                       exclude={cand["key"]})
        return
    await q(
        """UPDATE pending_actions
           SET status='confirmed'
           WHERE id=$1::uuid""", pid)
    if cand["kind"] == "quiz":
        meta = cand.get("meta", {})
        label = cand.get("chapter") or cand["title"]
        await start_quiz(
            u, chat, meta.get("topic"), count=5,
            subject_id=cand.get("subject_id"),
            topic_id=cand.get("topic_id"),
            topic_label=label)
    else:
        await start_live_session(u, chat, cand)


async def ob_cb(u, chat, what):
    if what == "confirm":
        if not u["onboarded"]:
            await ob_commit(u, chat)
        else:
            await cmd_dashboard(u, chat)
    elif what == "restart":
        await q(
            """UPDATE users SET
               ob_state='{}'::jsonb,
               ob_step=NULL, onboarded=false
               WHERE id=$1::uuid""", u["id"])
        await ob_start(u, chat)


async def note_cb(u, chat, nid):
    r = await qrow(
        """SELECT title, content FROM resources
           WHERE id=$1::uuid AND user_id=$2::uuid""",
        nid, u["id"])
    if r:
        body = esc((r["content"] or "")[:3000])
        await send(chat, f"📎 <b>{esc(r['title'])}"
                  f"</b>\n\n{body}", MENU_KB)


async def reset_cb(u, chat, what):
    if what == "ask":
        await send(chat, "🧨 <b>Wipe ALL your "
                   "StudyOS data?</b>\nSessions, "
                   "mastery, homework, tests — "
                   "everything.",
                   IK([("✅ Yes, wipe it",
                        "reset:yes")],
                      [("❌ No, keep it",
                        "nav:menu")]))
    elif what == "yes":
        await q("DELETE FROM users "
                "WHERE id=$1::uuid", u["id"])
        await send(chat, "🧨 Wiped clean. "
                   "Send /start to set up again.")


async def subj_cb(u, chat, rest):
    pid, _, sid = rest.partition(":")
    pend = await get_pending(u, pid)
    if not pend or pend["status"] != "pending":
        return
    payload = json.loads(pend["payload"])
    sname = await qval(
        "SELECT name FROM subjects "
        "WHERE id=$1::uuid", sid)
    if not sname:
        return
    payload["fields"]["subject_id"] = sid
    payload["fields"]["subject"] = sname
    await set_pending_payload(pid, payload)
    kind = pend["kind"]
    f = payload["fields"]
    if kind == "log_session":
        lines = ["I found:", ""]
        if f.get("questions"):
            lines.append(f"Questions: "
                         f"<b>{f['questions']}</b>")
            if f.get("correct") is not None:
                lines.append(f"Correct: "
                             f"<b>{f['correct']}</b>")
        if f.get("minutes"):
            lines.append(f"Duration: "
                         f"<b>{fm(f['minutes'])}</b>")
        lines.append(f"Subject: <b>{esc(sname)}</b>")
        if f.get("topic"):
            lines.append(f"Topic: "
                         f"<b>{esc(f['topic'])}</b>")
        await send(chat, "\n".join(lines),
                   confirm_kb(pid))
        return
    orders = {
        "hw_add": [("title", "Homework"),
                   ("subject", "Subject"),
                   ("questions", "Questions"),
                   ("due", "Due")],
        "test_add": [("name", "Test"),
                     ("subject", "Subject"),
                     ("date", "When")],
        "mistake_add": [("count", "Count"),
                        ("mtype", "Type"),
                        ("subject", "Subject"),
                        ("topic", "Topic")]}
    lines = card_lines(f, orders.get(kind, []))
    await send(chat, "I found:\n\n"
               + "\n".join(lines), confirm_kb(pid))


async def confirm_cb(u, chat, msg_id, rest):
    pid, _, answer = rest.rpartition(":")
    pend = await get_pending(u, pid)
    if not pend or pend["status"] != "pending":
        return
    payload = json.loads(pend["payload"])
    kind = pend["kind"]
    if answer in ("no", "undo"):
        if answer == "undo" and kind == "undo_log":
            f = payload["fields"]
            await q(
                """DELETE FROM study_sessions
                   WHERE user_id=$1::uuid
                   AND created_at > now()
                     - interval '10 minutes'
                   AND source='log'
                   AND questions_attempted=$2
                   AND questions_correct=$3""",
                u["id"], f.get("questions") or 0,
                f.get("correct"))
            if f.get("subject_id"):
                await recompute(
                    u["id"], f["subject_id"], None)
            await edit(chat, msg_id, "↩️ Undone.")
        else:
            await edit(chat, msg_id, "Cancelled. ✖️")
        await q(
            """UPDATE pending_actions
               SET status='cancelled'
               WHERE id=$1::uuid""", pid)
        return
    if answer == "edit":
        await q(
            """UPDATE pending_actions
               SET status='cancelled'
               WHERE id=$1::uuid""", pid)
        await edit(chat, msg_id,
                   "Send the corrected message and "
                   "I'll re-read it. 👍")
        return
    if answer == "skipq":
        await q(
            """UPDATE pending_actions
               SET status='confirmed'
               WHERE id=$1::uuid""", pid)
        await edit(chat, msg_id,
                   "Logged as time-only. 🏠")
        return
    # answer == yes
    if kind == "log_session":
        res = await do_log_session(
            u, payload["fields"])
        undo_pid = await new_pending(
            u, "undo_log",
            {"fields": payload["fields"]}, "confirm")
        await edit(chat, msg_id, res,
                   IK([("↩️ Undo",
                        f"cfm:{undo_pid}:undo")]))
    elif kind == "hw_add":
        await edit(chat, msg_id,
                   await do_hw_add(u,
                                   payload["fields"]))
    elif kind == "hw_progress":
        await edit(chat, msg_id,
                   await do_hw_progress(
                       u, payload["fields"]))
    elif kind == "test_add":
        await edit(chat, msg_id,
                   await do_test_add(
                       u, payload["fields"]))
    elif kind == "test_result":
        await edit(chat, msg_id,
                   await do_test_result(
                       u, payload["fields"]))
    elif kind == "mistake_add":
        await edit(chat, msg_id,
                   await do_mistake_add(
                       u, payload["fields"]))
    elif kind == "syllabus":
        await edit(chat, msg_id,
                   await do_syllabus(
                       u, payload.get("blocks", [])))
    elif kind == "save_note":
        await edit(chat, msg_id,
                   await do_save_note(
                       u, payload["fields"]))
    else:
        await edit(chat, msg_id, "✅ Done.")
    await q(
        """UPDATE pending_actions
           SET status='confirmed'
           WHERE id=$1::uuid""", pid)

# ============================================================
# MESSAGE ENTRY
# ============================================================
async def process_message(msg):
    chat = msg["chat"]["id"]
    try:
        u = await ensure_user(msg["from"], chat)
        u = await lazy_tick(u, now_tz())
        if msg.get("voice"):
            await handle_voice(u, chat, msg)
            return
        if msg.get("photo"):
            await handle_photo(u, chat, msg)
            return
        if msg.get("document"):
            await send(chat, "📄 PDF import is on "
                       "the roadmap — send photos "
                       "of the pages instead.",
                       MENU_KB)
            return
        text = (msg.get("text")
                or msg.get("caption") or "").strip()
        if not text:
            return
        if not u["onboarded"] \
                and not text.startswith("/"):
            await ob_handle(u, chat, text)
            return
        if not u["onboarded"]:
            if text.lower().startswith("/start"):
                await ob_start(u, chat)
            else:
                await send(chat,
                           "Let's finish setup "
                           "first 🙂")
            return
        await handle_text_msg(u, chat, text)
    except Exception:
        LOG.exception("message failed")
        await send(chat, "Something broke on my "
                   "side — try again.", MENU_KB)


async def process_update(upd):
    if "callback_query" in upd:
        await handle_callback(upd["callback_query"])
    elif "message" in upd:
        await process_message(upd["message"])

# ============================================================
# FASTAPI APP
# ============================================================
@asynccontextmanager
async def lifespan(_app):
    global AIC
    pairs = (("TELEGRAM_BOT_TOKEN",
              TELEGRAM_BOT_TOKEN),
             ("DATABASE_URL", DATABASE_URL),
             ("GEMINI_API_KEY", GEMINI_API_KEY),
             ("TELEGRAM_WEBHOOK_SECRET",
              TELEGRAM_WEBHOOK_SECRET))
    missing = [k for k, v in pairs if not v]
    if missing:
        LOG.error("Missing env vars: %s",
                  ", ".join(missing))
        raise RuntimeError(f"missing env: {missing}")
    await init_db()
    AIC = genai.Client(api_key=GEMINI_API_KEY)
    LOG.info("gemini ready (model=%s)", GEMINI_MODEL)
    ext = os.getenv("RENDER_EXTERNAL_URL")
    if ext:
        url = f"{ext}/webhook"
        try:
            res = await tg(
                "setWebhook", url=url,
                secret_token=TELEGRAM_WEBHOOK_SECRET,
                allowed_updates=["message",
                                 "callback_query"])
            LOG.info("webhook registered: %s -> %s",
                     url, bool(res))
        except Exception as e:
            LOG.error("webhook registration "
                      "failed: %s", e)
    else:
        LOG.warning("RENDER_EXTERNAL_URL not set — "
                    "skipping webhook setup")
    yield
    await HTTP.aclose()
    if POOL:
        await POOL.close()
    LOG.info("studyos shutdown complete")


app = FastAPI(title="StudyOS", lifespan=lifespan)


@app.get("/health")
async def health():
    try:
        ok = await qval("SELECT 1") == 1
    except Exception:
        ok = False
    return {"ok": ok, "service": "studyos",
            "db": ok, "ai": AIC is not None}


@app.post("/webhook")
async def webhook(request: Request,
                  x_telegram_bot_api_secret_token:
                  str = Header(default="")):
    if x_telegram_bot_api_secret_token \
            != TELEGRAM_WEBHOOK_SECRET:
        raise HTTPException(status_code=403)
    upd = await request.json()
    uid = upd.get("update_id")
    if uid is None:
        return {"ok": True}
    inserted = await qval(
        """INSERT INTO update_inbox(update_id)
           VALUES($1)
           ON CONFLICT DO NOTHING
           RETURNING update_id""", uid)
    if inserted is None:
        return {"ok": True}
    try:
        await process_update(upd)
    except Exception:
        LOG.exception("update %s failed", uid)
    return {"ok": True}


@app.get("/")
async def root():
    return {"service": "StudyOS", "health": "/health"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0",
                port=int(os.getenv("PORT", 8000)))
