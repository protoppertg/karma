"""
StudyOS — Personal AI Study Operating System
Single-user. Telegram + Supabase + Gemini (free tier) + Render.
Tables are created automatically on first boot. No manual SQL needed.

Doctrine: AI interprets language. Python computes every number.
"""

import os
import re
import io
import ssl
import json
import time
import math
import uuid
import asyncpg
import hashlib
import logging
import asyncio
from datetime import datetime, date, timedelta, timezone
from datetime import time as dtime
from zoneinfo import ZoneInfo
from collections import defaultdict

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
    ZoneInfo(TZ_NAME)
except Exception:
    TZ_NAME, TZ = "UTC", timezone.utc

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
LOG = logging.getLogger("studyos")

HTTP = httpx.AsyncClient(timeout=35.0)
POOL: asyncpg.Pool = None
AIC = None  # Gemini client

# ============================================================
# DB
# ============================================================
SCHEMA = [
    """CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        telegram_user_id BIGINT UNIQUE NOT NULL,
        telegram_chat_id BIGINT,
        first_name TEXT, username TEXT, display_name TEXT,
        exam_goal TEXT, exam_date DATE,
        wake_time TIME DEFAULT '06:30', sleep_time TIME DEFAULT '23:00',
        school_start TIME, school_end TIME, school_days INT[],
        coaching_start TIME, coaching_end TIME, coaching_days INT[],
        meal_minutes INT DEFAULT 60, commute_minutes INT DEFAULT 0,
        energy TEXT DEFAULT 'normal', energy_set_at TIMESTAMPTZ,
        onboarded BOOLEAN DEFAULT FALSE,
        ob_state JSONB DEFAULT '{}'::jsonb, ob_step TEXT,
        last_tick DATE,
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS subjects (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        name TEXT NOT NULL, difficulty INT DEFAULT 3,
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, name))""",
    """CREATE TABLE IF NOT EXISTS topics (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
        name TEXT NOT NULL, position INT DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, subject_id, name))""",
    """CREATE TABLE IF NOT EXISTS study_sessions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id) ON DELETE SET NULL,
        topic_id UUID REFERENCES topics(id) ON DELETE SET NULL,
        source TEXT DEFAULT 'log', status TEXT DEFAULT 'finished',
        title TEXT,
        questions_attempted INT DEFAULT 0,
        questions_correct INT,
        duration_minutes INT DEFAULT 0,
        started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ,
        paused_at TIMESTAMPTZ, paused_seconds INT DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE INDEX IF NOT EXISTS ix_sess_user ON study_sessions(user_id, created_at)""",
    """CREATE TABLE IF NOT EXISTS homework (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id) ON DELETE SET NULL,
        title TEXT NOT NULL, hw_type TEXT DEFAULT 'custom',
        total_q INT, completed_q INT DEFAULT 0,
        est_minutes INT, due_at TIMESTAMPTZ,
        status TEXT DEFAULT 'not_started',
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE INDEX IF NOT EXISTS ix_hw_user ON homework(user_id, due_at)""",
    """CREATE TABLE IF NOT EXISTS tests (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id) ON DELETE SET NULL,
        name TEXT NOT NULL, test_at TIMESTAMPTZ,
        total_marks INT, obtained_marks INT,
        status TEXT DEFAULT 'scheduled',
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS mistakes (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id) ON DELETE SET NULL,
        topic_id UUID REFERENCES topics(id) ON DELETE SET NULL,
        mtype TEXT DEFAULT 'unknown', description TEXT DEFAULT '',
        count INT DEFAULT 1, fingerprint TEXT NOT NULL,
        resolved BOOLEAN DEFAULT FALSE,
        first_seen TIMESTAMPTZ DEFAULT now(), last_seen TIMESTAMPTZ DEFAULT now(),
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, fingerprint))""",
    """CREATE TABLE IF NOT EXISTS revisions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id) ON DELETE SET NULL,
        topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
        rev_type TEXT DEFAULT 'active_recall', mode TEXT DEFAULT 'review',
        interval_days INT DEFAULT 1, ease NUMERIC DEFAULT 2.3,
        streak INT DEFAULT 0, lapses INT DEFAULT 0, last_outcome TEXT,
        last_reviewed TIMESTAMPTZ, due_at TIMESTAMPTZ DEFAULT now(),
        status TEXT DEFAULT 'due',
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, topic_id))""",
    """CREATE INDEX IF NOT EXISTS ix_rev_due ON revisions(user_id, due_at)""",
    """CREATE TABLE IF NOT EXISTS mastery (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id) ON DELETE CASCADE,
        topic_id UUID REFERENCES topics(id) ON DELETE CASCADE,
        mastery REAL, attempts INT DEFAULT 0, correct INT DEFAULT 0,
        events INT DEFAULT 0, last_evidence TIMESTAMPTZ,
        algo INT DEFAULT 1, computed_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE UNIQUE INDEX IF NOT EXISTS uq_mast_topic
        ON mastery(user_id, topic_id) WHERE topic_id IS NOT NULL""",
    """CREATE UNIQUE INDEX IF NOT EXISTS uq_mast_subj
        ON mastery(user_id, subject_id) WHERE topic_id IS NULL""",
    """CREATE TABLE IF NOT EXISTS quizzes (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        subject_id UUID REFERENCES subjects(id) ON DELETE SET NULL,
        topic_id UUID REFERENCES topics(id) ON DELETE SET NULL,
        topic_label TEXT, questions JSONB, answers JSONB DEFAULT '[]'::jsonb,
        status TEXT DEFAULT 'active',
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS pending_actions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        msg_id BIGINT, kind TEXT, payload JSONB DEFAULT '{}'::jsonb,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS update_inbox (
        update_id BIGINT PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS resources (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        title TEXT, content TEXT,
        created_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS daily_plans (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        plan_date DATE, available_minutes INT, tasks JSONB,
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(user_id, plan_date))""",
    """CREATE TABLE IF NOT EXISTS ai_log (
        id BIGSERIAL PRIMARY KEY,
        kind TEXT, status TEXT, latency_ms INT,
        error TEXT, created_at TIMESTAMPTZ DEFAULT now())""",
]


async def init_db():
    global POOL
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    POOL = await asyncpg.create_pool(
        DATABASE_URL, min_size=1, max_size=3, command_timeout=20,
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
# TELEGRAM
# ============================================================
TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


def IK(*rows):
    return {"inline_keyboard": [[{"text": t, "callback_data": d} for t, d in r]
                                for r in rows]}


def esc(s) -> str:
    return str(s if s is not None else "").replace("&", "&amp;") \
        .replace("<", "&lt;").replace(">", "&gt;")


def trunc(s, n=3500) -> str:
    return s if len(s) <= n else s[:n] + "\n…"


async def tg(method, **payload):
    for attempt in range(3):
        try:
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
                   parse_mode="HTML", disable_web_page_preview=True,
                   reply_markup=kb)
    if res is None and ("<" in text or ">" in text):
        await tg("sendMessage", chat_id=chat_id,
                 text=re.sub(r"<[^>]+>", "", text),
                 disable_web_page_preview=True, reply_markup=kb)
    return res


async def edit(chat_id, msg_id, text, kb=None):
    text = trunc(text)
    res = await tg("editMessageText", chat_id=chat_id, message_id=msg_id,
                   text=text, parse_mode="HTML",
                   disable_web_page_preview=True, reply_markup=kb)
    if res is None:
        send_task = asyncio.create_task(send(chat_id, text, kb))
        await send_task
    return res


async def answer_cb(cb_id, text=""):
    try:
        await tg("answerCallbackQuery", callback_query_id=cb_id, text=text)
    except Exception:
        pass


async def get_file_bytes(file_id) -> bytes:
    res = await tg("getFile", file_id=file_id)
    if not res:
        raise RuntimeError("getFile failed")
    r = await HTTP.get(f"{TG_BASE}/file/bot{TELEGRAM_BOT_TOKEN}/{res['file_path']}")
    r.raise_for_status()
    return r.content

# ============================================================
# GEMINI (free-tier friendly: rate limited, cached, retried)
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
                response_mime_type="application/json" if json_mode else "text/plain",
                max_output_tokens=max_tokens, temperature=0.2)
            def _do():
                return AIC.models.generate_content(
                    model=GEMINI_MODEL, contents=contents, config=cfg)
            resp = await asyncio.to_thread(_do)
            text = resp.text
            if not text:
                raise AIError("empty AI response")
            return text
        except Exception as e:
            msg = str(e)
            quota = "429" in msg or "RESOURCE_EXHAUSTED" in msg or "quota" in msg.lower()
            await q("INSERT INTO ai_log(kind,status,latency_ms,error) "
                    "VALUES('call',$1,$2,$3)",
                    "retry" if attempt < 2 else "error",
                    int((time.monotonic() - t0) * 1000), msg[:400])
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
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text).rstrip("`").strip()
    i, j = text.find("{"), text.rfind("}")
    if i == -1 or j == -1:
        raise AIError("no JSON in AI response")
    return json.loads(text[i:j + 1])

# ============================================================
# DETERMINISTIC PARSERS (no AI cost)
# ============================================================
WEEKDAYS = {"monday": 0, "mon": 0, "tuesday": 1, "tue": 1, "tues": 1,
            "wednesday": 2, "wed": 2, "thursday": 3, "thu": 3, "thurs": 3,
            "friday": 4, "fri": 4, "saturday": 5, "sat": 5,
            "sunday": 6, "sun": 6}
MONTHS = {m.lower(): i + 1 for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"])}
SKIP_WORDS = {"skip", "/skip", "later", "no", "nothing", "none", "n", "na"}


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
    m = re.match(r"^(\d{1,2})[\s\-/](\d{1,2})(?:[\s\-/](\d{2,4}))?$", t)
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
            return date(today.year, MONTHS[m.group(2)], int(m.group(1)))
        except ValueError:
            return None
    for name, mo in MONTHS.items():
        if t.startswith(name):
            m = re.match(rf"^{name}\s+(\d{{1,2}})(?:st|nd|rd|th)?$", t)
            if m:
                try:
                    return date(today.year, mo, int(m.group(1)))
                except ValueError:
                    return None
    return None


def parse_time(t):
    t = (t or "").strip().lower().replace(" ", "")
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?(am|pm)?$", t)
    if not m:
        return None
    h, mi, ap = int(m.group(1)), int(m.group(2) or 0), m.group(3)
    if ap == "pm" and h != 12:
        h += 12
    if ap == "am" and h == 12:
        h = 0
    if not (0 <= h < 24 and 0 <= mi < 60):
        return None
    return dtime(h, mi)


def parse_time_range(t):
    t = (t or "").strip().lower().replace("–", "-").replace("—", "-")
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(?:-|to)\s*"
                 r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$", t.replace(" ", ""))
    if not m:
        return None
    h1, m1, ap1 = int(m.group(1)), int(m.group(2) or 0), m.group(3)
    h2, m2, ap2 = int(m.group(4)), int(m.group(5) or 0), m.group(6)
    if ap1:
        if ap1 == "pm" and h1 != 12: h1 += 12
        if ap1 == "am" and h1 == 12: h1 = 0
    if ap2:
        if ap2 == "pm" and h2 != 12: h2 += 12
        if ap2 == "am" and h2 == 12: h2 = 0
    elif ap1:
        if h2 < 12 and h2 < h1: h2 += 12
    elif h2 <= h1 and h2 < 12:      # 8-2 => 08:00-14:00
        h2 += 12
    elif h1 <= 7 and h2 <= 7:       # 5-7 => evening coaching
        h1 += 12; h2 += 12
    if h2 * 60 + m2 <= h1 * 60 + m1:
        return None
    return dtime(h1, m1), dtime(h2, m2)


def parse_duration(text):
    t = (text or "").lower()
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:h|hr|hrs|hour|hours)\s*"
                  r"(?:(\d+)\s*(?:m|min|mins|minutes))?", t)
    if m:
        return int(float(m.group(1)) * 60 + int(m.group(2) or 0))
    m = re.search(r"(\d+)\s*(?:m|min|mins|minute|minutes)\b", t)
    if m:
        return int(m.group(1))
    return None


def parse_questions(text):
    """Returns (attempted, correct, wrong) or None. correct/wrong may be None."""
    t = (text or "").lower()
    m = re.search(r"(\d+)\s*(?:questions?|qs?|mcqs?|problems?|pyqs?)", t)
    if not m:
        return None
    att = int(m.group(1))
    cor = wre = None
    mc = re.search(r"(\d+)\s*(?:correct|right|correctly|correct answers?|right answers?)", t)
    mw = re.search(r"(\d+)\s*(?:wrong|incorrect|incorrectly|mistakes?)", t)
    if mc:
        cor = int(mc.group(1))
    if mw:
        wre = int(mw.group(1))
    if cor is None and wre is not None and wre <= att:
        cor = att - wre
    return att, cor, wre


def detect_energy(text):
    t = (text or "").lower()
    if any(p in t for p in ("exhausted", "dead tired", "burnt out", "burned out",
                            "no energy", "so tired", "cant do this", "can't do this")):
        return "exhausted"
    if any(p in t for p in ("tired", "sleepy", "cant focus", "can't focus",
                            "low energy", "drained")):
        return "tired"
    if any(p in t for p in ("energetic", "feeling fresh", "fired up")):
        return "energetic"
    return None


def parse_days(s):
    s = (s or "").lower().strip()
    if not s:
        return list(range(7))
    if s in ("daily", "everyday", "every day", "all days"):
        return list(range(7))
    if s in ("weekdays",):
        return [0, 1, 2, 3, 4]
    if s in ("weekend", "weekends"):
        return [5, 6]
    out = set()
    for part in re.split(r"[,\s&+]+", s):
        part = part.strip().rstrip("s")
        if "-" in part or "to" in part:
            bits = re.split(r"-|\s+to\s+", part)
            if len(bits) == 2 and bits[0] in WEEKDAYS and bits[1] in WEEKDAYS:
                a, b = WEEKDAYS[bits[0]], WEEKDAYS[bits[1]]
                span = list(range(a, b + 1)) if b >= a else \
                    list(range(a, 7)) + list(range(0, b + 1))
                out.update(span)
        elif part in WEEKDAYS:
            out.add(WEEKDAYS[part])
    return sorted(out) if out else None


def parse_class(text):
    """'8-2 mon-sat' -> (start_time, end_time, days)"""
    m = re.search(r"(\d{1,2}(?::\d{2})?\s*(?:am|pm)?\s*(?:-|–|to)\s*"
                  r"\d{1,2}(?::\d{2})?\s*(?:am|pm)?)", text.lower())
    if not m:
        return None
    tr = parse_time_range(m.group(1).replace(" ", ""))
    if not tr:
        return None
    rest = text.lower().replace(m.group(1), " ").strip()
    days = parse_days(rest) if rest else list(range(7))
    return tr[0], tr[1], days

# ============================================================
# ENGINES — all deterministic, documented
# ============================================================
SOURCE_W = {"log": 1.0, "live": 1.0, "quiz": 0.9, "test": 1.6}


def compute_mastery(events):
    """Mastery v1: EMA fold over chronological evidence.
    prior=25; alpha = min(0.15, 0.08 * volume * source_weight);
    volume = log10(1+attempted)/log10(31). One event moves mastery
    by at most 15 points. Recomputable from raw evidence."""
    if not events:
        return None
    m = 25.0
    att = cor = n = 0
    last = None
    for e in sorted(events, key=lambda x: x["at"]):
        a, c = e["a"], e["c"]
        if not a or a <= 0 or c is None:
            continue
        att += a; cor += c; n += 1; last = e["at"]
        score = 100.0 * c / a
        volume = min(1.0, math.log10(1 + a) / math.log10(31.0))
        alpha = min(0.15, 0.08 * volume * SOURCE_W.get(e["src"], 1.0))
        m += alpha * (score - m)
        m = max(0.0, min(100.0, m))
    return {"mastery": round(m, 1), "attempts": att, "correct": cor,
            "events": n, "last": last}


def grade_of(acc):
    if acc is None:
        return "good"
    if acc >= 0.9: return "easy"
    if acc >= 0.75: return "good"
    if acc >= 0.6: return "hard"
    return "again"


def revision_step(r, acc, reps):
    """SM-2 style, driven by measured accuracy. Lapses>=3 => practice mode."""
    interval, ease = int(r["interval_days"]), float(r["ease"])
    streak, lapses = int(r["streak"]), int(r["lapses"])
    mode, rev_type = r["mode"], r["rev_type"]
    g = grade_of(acc)
    if g == "again":
        interval, streak, lapses = 1, 0, lapses + 1
        ease -= 0.20
    elif g == "hard":
        interval = max(1, round(interval * 1.2))
        ease -= 0.15
    elif g == "good":
        interval = 1 if streak == 0 else (3 if streak == 1 else round(interval * ease))
        streak += 1
    else:
        interval = max(1, round(interval * ease * 1.3))
        streak += 1
        ease += 0.15
    ease = max(1.3, min(2.8, ease))
    if lapses >= 3:
        mode, rev_type, interval = "practice", "mcq", min(interval, 3)
    if acc is not None and acc < 0.5:
        interval = min(interval, 2)
    if (reps or 0) >= 2:
        rev_type, interval = "mistake_review", min(interval, 2)
    return {"interval_days": interval, "ease": round(ease, 2), "streak": streak,
            "lapses": lapses, "mode": mode, "rev_type": rev_type, "grade": g}


BASE_SCORE = {"test_prep": 40, "homework": 30, "mistake_review": 28,
              "revision": 25, "quiz": 22, "study": 20}
LIGHT = {"revision", "mistake_review", "quiz"}


def score_task(c, now, energy):
    s = float(BASE_SCORE.get(c["kind"], 15))
    rs = []
    k, m = c["kind"], c.get("meta", {})
    if k == "homework":
        due = m.get("due_at")
        if due:
            hrs = (due - now).total_seconds() / 3600
            if hrs < 0:
                s += 25; rs.append(f"overdue ~{int(-hrs // 24) + 1}d")
            elif hrs <= 24:
                s += 15; rs.append("due <24h")
            elif hrs <= 48:
                s += 8; rs.append("due tomorrow")
        rem = m.get("remaining")
        if rem:
            s += min(6, rem / 10)
            rs.append(f"{rem} questions left")
    t_in = m.get("test_in_days")
    if t_in is not None and t_in <= 14 and k in ("revision", "study",
                                                 "mistake_review", "homework"):
        s += 25 if t_in <= 3 else 15 if t_in <= 7 else 8
        rs.append(f"test in {t_in}d")
    if k == "revision":
        od = m.get("overdue_days") or 0
        if od > 0:
            s += min(20, 2 * od); rs.append(f"revision overdue {od}d")
        mast = m.get("mastery")
        if mast is not None and mast < 40:
            s += 10; rs.append(f"weak ({mast:.0f}%)")
        if m.get("mode") == "practice":
            rs.append("needs active practice, not passive review")
    if k == "mistake_review":
        r = m.get("reps") or 0
        if r:
            s += min(12, 2 * r); rs.append(f"{r} repeated mistake(s)")
    if k == "study":
        mast = m.get("mastery")
        if mast is not None and mast < 40:
            s += 8; rs.append(f"weak topic ({mast:.0f}%)")
        st = m.get("stale_days")
        if st and st > 14:
            s += 5; rs.append(f"untouched {st}d")
    if energy in ("tired", "exhausted"):
        if k in LIGHT:
            s += 10; rs.append("light task fits your energy")
        else:
            s -= 10; rs.append("heavy task — deferred at low energy")
    if c.get("subject"):
        pre = c["subject"] + (f" — {c['chapter']}" if c.get("chapter") else "")
        rs.insert(0, pre)
    return round(max(0.0, min(100.0, s)), 1), rs


def choose_next(scored, minutes, exclude=None):
    if minutes <= 0:
        return None
    limit = max(minutes, 15)
    pool = [x for x in scored
            if not (exclude and x[0]["key"] in exclude)]
    fitting = [x for x in pool if x[0]["est"] <= limit]
    if not fitting:
        light = [x for x in pool if x[0]["kind"] in LIGHT]
        fitting = light or pool
    if not fitting:
        return None
    fitting.sort(key=lambda x: (-x[1], x[0]["est"]))
    return fitting[0]


def day_minutes(u, day: date):
    """available = awake − school − coaching − commute − meals − 10% buffer."""
    wake, sleep = u["wake_time"], u["sleep_time"]
    start = datetime.combine(day, wake, tzinfo=TZ)
    end = datetime.combine(day, sleep, tzinfo=TZ)
    if end <= start:
        end += timedelta(days=1)
    awake = (end - start).total_seconds() / 60
    busy = 0.0
    commute = 0
    wd = day.weekday()
    for s_key, e_key, d_key in (("school_start", "school_end", "school_days"),
                                ("coaching_start", "coaching_end", "coaching_days")):
        if u.get(d_key) and wd in u[d_key] and u.get(s_key) and u.get(e_key):
            bs = datetime.combine(day, u[s_key], tzinfo=TZ)
            be = datetime.combine(day, u[e_key], tzinfo=TZ)
            if be <= bs:
                be += timedelta(days=1)
            cs, ce = max(bs, start), min(be, end)
            if ce > cs:
                busy += (ce - cs).total_seconds() / 60
                commute += min(u["commute_minutes"] or 0, 60)
    commute = min(commute, int(max(0, awake - busy)), 120)
    avail = awake - busy - commute - (u["meal_minutes"] or 60)
    if avail > 0:
        avail *= 0.9
    return max(0, int(avail)), int(busy)


def urgent(c, s, now):
    k = c["kind"]
    if k in ("test_prep", "mistake_review"):
        return True
    if k == "homework":
        due = c.get("meta", {}).get("due_at")
        return due is None or (due - now).total_seconds() <= 48 * 3600
    if k == "revision":
        return (c.get("meta", {}).get("overdue_days") or 0) > 7
    return False


def build_plan(u, cands, avail, energy, missed_days, now):
    """Never exceeds available time. Caps revisions, chunks long tasks."""
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
            if urgent(item[0], item[1], now):
                kept.append(item)
            elif non_urgent_seen < 2:
                kept.append((item[0], round(item[1] * 0.5, 1), item[2]))
                non_urgent_seen += 1
        scored = kept
    scored.sort(key=lambda x: -x[1])
    alloc, rev_n, tasks, deferred = 0, 0, [], []
    for c, sc, rs in scored:
        if c["kind"] == "revision" and rev_n >= 6:
            deferred.append((c["title"], "revision backlog cap"))
            continue
        chunk = min(c["est"], 50)
        if alloc + chunk > target:
            deferred.append((c["title"], f"no time today ({alloc}/{target}m)"))
            continue
        tasks.append({"kind": c["kind"], "title": c["title"], "est": chunk,
                      "score": sc, "reasons": rs})
        alloc += chunk
        if c["kind"] == "revision":
            rev_n += 1
        if c["est"] > chunk:
            deferred.append((c["title"], f"split — {c['est'] - chunk}m later"))
    return {"available": avail, "allocated": alloc, "tasks": tasks,
            "deferred": deferred, "missed": missed_days, "target": target}

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


def arrow(v):
    return "→" if v is None else ("↑" if v >= 0 else "↓")

# ============================================================
# DB SERVICES
# ============================================================
async def ensure_user(tg_from, chat_id):
    row = await qrow(
        """INSERT INTO users(telegram_user_id, telegram_chat_id, first_name, username)
           VALUES($1,$2,$3,$4)
           ON CONFLICT(telegram_user_id) DO UPDATE SET
             telegram_chat_id = COALESCE(EXCLUDED.telegram_chat_id, users.telegram_chat_id),
             first_name = COALESCE(EXCLUDED.first_name, users.first_name),
             username = COALESCE(EXCLUDED.username, users.username)
           RETURNING *""",
        tg_from["id"], chat_id, tg_from.get("first_name"), tg_from.get("username"))
    return dict(row)


async def get_subjects(user_id):
    return [dict(r) for r in await qrows(
        "SELECT id, name FROM subjects WHERE user_id=$1::uuid ORDER BY name", user_id)]


def resolve_subject(name, subjects):
    if not name or not subjects:
        return None, None
    n = name.strip().casefold()
    for s in subjects:
        if s["name"].casefold() == n:
            return s["id"], s["name"]
    for s in subjects:
        if n in s["name"].casefold() or s["name"].casefold() in n:
            return s["id"], s["name"]
    return None, None


async def upsert_subject(user_id, name):
    name = (name or "").strip().title() or "General"
    r = await qrow(
        """INSERT INTO subjects(user_id, name) VALUES($1::uuid, $2)
           ON CONFLICT(user_id, name) DO UPDATE SET name=EXCLUDED.name
           RETURNING id, name""", user_id, name)
    return dict(r)


async def upsert_topic(user_id, subject_id, name):
    name = (name or "").strip().title() or "General"
    r = await qrow(
        """INSERT INTO topics(user_id, subject_id, name)
           VALUES($1::uuid,$2::uuid,$3)
           ON CONFLICT(user_id, subject_id, name) DO UPDATE SET name=EXCLUDED.name
           RETURNING id, name""", user_id, subject_id, name)
    return dict(r)


async def find_topic(user_id, subject_id, name):
    if not name:
        return None
    r = await qrow(
        """SELECT id, name FROM topics WHERE user_id=$1::uuid
           AND subject_id=$2::uuid AND name ILIKE $3 LIMIT 1""",
        user_id, subject_id, f"%{name.strip()}%")
    return dict(r) if r else None


async def topic_mastery_events(user_id, topic_id):
    rows = await qrows(
        """SELECT questions_attempted a, questions_correct c, created_at, source
           FROM study_sessions WHERE user_id=$1::uuid AND topic_id=$2::uuid
           AND status='finished' AND questions_attempted > 0
           AND questions_correct IS NOT NULL""",
        user_id, topic_id)
    return [{"a": r["a"], "c": r["c"], "at": r["created_at"], "src": r["source"]}
            for r in rows]


async def subject_mastery_events(user_id, subject_id):
    rows = await qrows(
        """SELECT questions_attempted a, questions_correct c, created_at, source
           FROM study_sessions WHERE user_id=$1::uuid AND subject_id=$2::uuid
           AND status='finished' AND questions_attempted > 0
           AND questions_correct IS NOT NULL""",
        user_id, subject_id)
    return [{"a": r["a"], "c": r["c"], "at": r["created_at"], "src": r["source"]}
            for r in rows]


async def upsert_mastery(user_id, subject_id, topic_id, snap):
    if snap is None or snap["events"] == 0:
        return
    if topic_id:
        await q(
            """INSERT INTO mastery(user_id, subject_id, topic_id, mastery,
               attempts, correct, events, last_evidence, algo)
               VALUES($1::uuid,$2::uuid,$3::uuid,$4,$5,$6,$7,$8,1)
               ON CONFLICT(user_id, topic_id) WHERE topic_id IS NOT NULL DO UPDATE SET
                 mastery=EXCLUDED.mastery, attempts=EXCLUDED.attempts,
                 correct=EXCLUDED.correct, events=EXCLUDED.events,
                 last_evidence=EXCLUDED.last_evidence, computed_at=now()""",
            user_id, subject_id, topic_id, snap["mastery"], snap["attempts"],
            snap["correct"], snap["events"], snap["last"])
    else:
        await q(
            """INSERT INTO mastery(user_id, subject_id, topic_id, mastery,
               attempts, correct, events, last_evidence, algo)
               VALUES($1::uuid,$2::uuid,NULL,$4,$5,$6,$7,$8,1)
               ON CONFLICT(user_id, subject_id) WHERE topic_id IS NULL DO UPDATE SET
                 mastery=EXCLUDED.mastery, attempts=EXCLUDED.attempts,
                 correct=EXCLUDED.correct, events=EXCLUDED.events,
                 last_evidence=EXCLUDED.last_evidence, computed_at=now()""",
            user_id, subject_id, snap["mastery"], snap["attempts"],
            snap["correct"], snap["events"], snap["last"])


async def recompute(user_id, subject_id, topic_id=None):
    if topic_id:
        snap = compute_mastery(await topic_mastery_events(user_id, topic_id))
        await upsert_mastery(user_id, subject_id, topic_id, snap)
    snap = compute_mastery(await subject_mastery_events(user_id, subject_id))
    await upsert_mastery(user_id, subject_id, None, snap)


async def mistake_reps(user_id, topic_id):
    if not topic_id:
        return 0
    return int(await qval(
        "SELECT COALESCE(SUM(count),0) FROM mistakes WHERE user_id=$1::uuid "
        "AND topic_id=$2::uuid AND resolved=false", user_id, topic_id) or 0)


async def apply_revision(user_id, subject_id, topic_id, acc, now):
    """Create/advance the revision item using measured accuracy."""
    reps = await mistake_reps(user_id, topic_id)
    r = await qrow(
        "SELECT * FROM revisions WHERE user_id=$1::uuid AND topic_id=$2::uuid",
        user_id, topic_id)
    if not r:
        interval = 2
        if acc is not None and acc < 0.5:
            interval = 1
        await q(
            """INSERT INTO revisions(user_id, subject_id, topic_id, rev_type,
               interval_days, due_at, last_outcome, last_reviewed, status)
               VALUES($1::uuid,$2::uuid,$3::uuid,'active_recall',$4,
                      $5,$6,$7,'due')
               ON CONFLICT(user_id, topic_id) DO NOTHING""",
            user_id, subject_id, topic_id, interval,
            now + timedelta(days=interval), grade_of(acc), now)
        return now + timedelta(days=interval)
    st = revision_step(dict(r), acc, reps)
    await q(
        """UPDATE revisions SET interval_days=$2, ease=$3, streak=$4, lapses=$5,
           mode=$6, rev_type=$7, last_outcome=$8, last_reviewed=$9, due_at=$10,
           status='due' WHERE id=$1::uuid""",
        str(r["id"]), st["interval_days"], st["ease"], st["streak"],
        st["lapses"], st["mode"], st["rev_type"], st["grade"], now,
        now + timedelta(days=st["interval_days"]))
    return now + timedelta(days=st["interval_days"])


async def save_evidence(u, subject_id, topic_id, att, cor, minutes,
                        source="log", when=None, title=None):
    """Insert a finished evidence session; update mastery + revision.
    Returns dict with computed feedback numbers."""
    now = when or now_tz()
    await q(
        """INSERT INTO study_sessions(user_id, subject_id, topic_id, source,
           status, title, questions_attempted, questions_correct,
           duration_minutes, started_at, ended_at)
           VALUES($1::uuid,$2::uuid,$3::uuid,$4,'finished',$5,$6,$7,$8,$9,$9)""",
        u["id"], subject_id, topic_id, source, title,
        att or 0, cor, minutes or 0, now)
    out = {}
    if att and cor is not None:
        out["acc"] = 100.0 * cor / att
        if topic_id:
            await recompute(u["id"], subject_id, topic_id)
            out["next_rev"] = await apply_revision(
                u["id"], subject_id, topic_id, cor / att, now)
            m = await qrow(
                "SELECT mastery, events FROM mastery WHERE user_id=$1::uuid "
                "AND topic_id=$2::uuid", u["id"], topic_id)
            if m:
                out["mastery"] = m["mastery"]; out["events"] = m["events"]
        else:
            await recompute(u["id"], subject_id)
            m = await qrow(
                "SELECT mastery, events FROM mastery WHERE user_id=$1::uuid "
                "AND subject_id=$2::uuid AND topic_id IS NULL", u["id"], subject_id)
            if m:
                out["mastery"] = m["mastery"]
    return out


async def live_session(user_id):
    r = await qrow(
        "SELECT * FROM study_sessions WHERE user_id=$1::uuid "
        "AND status IN ('active','paused') LIMIT 1", user_id)
    return dict(r) if r else None


async def live_minutes(u, now):
    s = await live_session(u["id"])
    if not s:
        return 0
    if s["status"] == "paused" and s["paused_at"]:
        elapsed = (s["paused_at"] - s["started_at"]).total_seconds() - \
            (s["paused_seconds"] or 0)
    else:
        elapsed = (now - s["started_at"]).total_seconds() - \
            (s["paused_seconds"] or 0)
    return max(0, int(elapsed / 60))


async def spent_today(u, day):
    v = await qval(
        """SELECT COALESCE(SUM(duration_minutes),0) FROM study_sessions
           WHERE user_id=$1::uuid AND status='finished'
           AND created_at >= $2 AND created_at < $3""",
        u["id"], sod(day), sod(day) + timedelta(days=1))
    return int(v or 0)


async def cleanup_stale(u, now):
    s = await live_session(u["id"])
    if s and (now - s["started_at"]).total_seconds() > 3 * 3600:
        await q("UPDATE study_sessions SET status='abandoned' "
                "WHERE id=$1::uuid", str(s["id"]))


async def lazy_tick(u, now):
    today = now.date()
    if u.get("last_tick") == today:
        await cleanup_stale(u, now)
        return u
    if u["energy"] != "normal" and u.get("energy_set_at") and \
            (now - u["energy_set_at"]).total_seconds() > 12 * 3600:
        await q("UPDATE users SET energy='normal' WHERE id=$1::uuid", u["id"])
        u["energy"] = "normal"
    await q("UPDATE revisions SET status='due' WHERE user_id=$1::uuid "
            "AND status='scheduled' AND due_at < now()", u["id"])
    await q("UPDATE users SET last_tick=$2 WHERE id=$1::uuid", u["id"], today)
    u["last_tick"] = today
    await cleanup_stale(u, now)
    return u

# ============================================================
# AI PROMPTS
# ============================================================
ROUTER_SYS = """You convert a student's Telegram message into ONE JSON action for a study-tracking app. Output ONLY JSON:
{"intent":"...","fields":{...},"confidence":"high|medium|low","reply": short reply or null}

Intents and fields (use null for anything not stated):
- "log_session": {"subject","topic","questions","correct","wrong","minutes","day"} — day is "today" or "yesterday". Used when they REPORT study already done.
- "hw_add": {"title","subject","questions","minutes","due"} — due as natural words like "friday" or "tomorrow".
- "hw_progress": {"title","done"} — partial homework progress ("did 25 of 50 DPP").
- "test_add": {"name","subject","date","total_marks"}
- "test_result": {"name","subject","obtained","total"} — they report marks obtained in a test.
- "mistake_add": {"subject","topic","count","mtype"} — mtype is one of: conceptual, calculation, careless, memory, misread, guessing, time_pressure.
- "energy": {"level"} — one of energetic, normal, tired, exhausted.
- "quiz": {"topic","subject","count"} — they want to be quizzed.
- "tutor": {"question"} — they ask to explain/teach a concept.
- "query": {"view"} — one of plan, next, homework, tests, revision, analytics, mistakes, dashboard, syllabus.
- "syllabus": {"blocks":[{"subject":"...","topics":["..."]}]} — they list chapters per subject.
- "chat": {"reply":"your own short warm reply"} — casual conversation.
- "none": {} — nothing matches.

Rules: extract only stated facts; never invent numbers; all numbers are integers; prefer the student's subject names from context; if unsure about a field, set confidence "low"."""


async def ai_context(u):
    parts = []
    subs = await get_subjects(u["id"])
    if subs:
        parts.append("subjects: " + ", ".join(s["name"] for s in subs))
    if u.get("exam_goal"):
        parts.append("goal: " + u["exam_goal"])
    weak = await qrows(
        """SELECT s.name subj, t.name top, m.mastery FROM mastery m
           JOIN topics t ON t.id=m.topic_id JOIN subjects s ON s.id=t.subject_id
           WHERE m.user_id=$1::uuid AND m.events>0
           ORDER BY m.mastery ASC LIMIT 3""", u["id"])
    if weak:
        parts.append("weakest topics: " + "; ".join(
            f"{r['subj']}/{r['top']} {r['mastery']:.0f}%" for r in weak))
    due_rev = await qval(
        "SELECT COUNT(*) FROM revisions WHERE user_id=$1::uuid AND due_at<now()",
        u["id"])
    if due_rev:
        parts.append(f"{due_rev} revisions due")
    over = await qval(
        "SELECT COUNT(*) FROM homework WHERE user_id=$1::uuid "
        "AND status!='completed' AND due_at<now()", u["id"])
    if over:
        parts.append(f"{over} homework overdue")
    nt = await qrow(
        "SELECT name, test_at FROM tests WHERE user_id=$1::uuid "
        "AND status='scheduled' AND test_at>now() ORDER BY test_at LIMIT 1", u["id"])
    if nt:
        parts.append(f"next test: {nt['name']}")
    parts.append(f"energy today: {u['energy']}")
    return "\n".join(parts)


async def ai_route(text, u):
    prompt = (ROUTER_SYS + "\n\nCONTEXT:\n" + await ai_context(u) +
              "\n\nMESSAGE:\n" + text)
    key = "rt:" + hashlib.md5(prompt.encode()).hexdigest()
    cached = _cache_get(key)
    if cached is not None:
        return cached
    raw = _load_json(await ai_call(prompt, json_mode=True, max_tokens=700))
    _cache_set(key, raw)
    return raw

# ============================================================
# PENDING ACTIONS / CONFIRMATION CARDS
# ============================================================
async def new_pending(u, kind, fields, origin="ai"):
    pid = str(uuid.uuid4())
    await q("INSERT INTO pending_actions(id, user_id, kind, payload) "
            "VALUES($1::uuid,$2::uuid,$3,$4::jsonb)",
            pid, u["id"], kind, json.dumps({"fields": fields, "origin": origin}))
    return pid


async def get_pending(u, pid):
    r = await qrow("SELECT * FROM pending_actions WHERE id=$1::uuid "
                   "AND user_id=$2::uuid", pid, u["id"])
    return dict(r) if r else None


async def set_pending_payload(pid, payload):
    await q("UPDATE pending_actions SET payload=$2::jsonb WHERE id=$1::uuid",
            pid, json.dumps(payload))


CONFIRM_KB = lambda pid: IK(
    [("✅ Confirm", f"cfm:{pid}:yes"), ("❌ Cancel", f"cfm:{pid}:no")],
    [("✏️ Correct it", f"cfm:{pid}:edit")])


async def subject_pick_card(u, chat, pid, kind, fields, header):
    subs = await get_subjects(u["id"])
    rows = [[(s["name"], f"subj:{pid}:{s['id']}")] for s in subs[:6]]
    rows.append([("❌ Cancel", f"cfm:{pid}:no")])
    await send(chat, header + "\n\nWhich subject?", IK(*rows))


def card_lines(fields, order):
    out = []
    for k, label in order:
        v = fields.get(k)
        if v is not None and v != "":
            out.append(f"{label}: <b>{esc(v)}</b>")
    return out

# ============================================================
# APPLY (confirm) HANDLERS — the only place evidence is written
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
    topic_id, topic_name = None, None
    if f.get("topic"):
        t = await find_topic(u["id"], sid, f["topic"])
        if t:
            topic_id, topic_name = t["id"], t["name"]
        else:
            t = await upsert_topic(u["id"], sid, f["topic"])
            topic_id, topic_name = t["id"], t["name"]
    res = await save_evidence(u, sid, topic_id, att, cor, mins, "log",
                              when=when, title="logged session")
    parts = ["✅ <b>Logged</b>"]
    if att and cor is not None:
        parts.append(f"❓ {att} questions • ✅ {cor} correct "
                     f"({res['acc']:.0f}% accuracy)")
    if mins:
        parts.append(f"⏱ {fm(mins)}")
    if topic_name:
        parts.append(f"📚 Topic: {esc(topic_name)}")
    if res.get("mastery") is not None:
        parts.append(f"🧠 Mastery now: <b>{res['mastery']:.0f}%</b>")
    if res.get("next_rev"):
        parts.append(f"🔁 Next revision: "
                     f"{res['next_rev'].strftime('%d %b')}")
    return "\n".join(parts)


async def do_hw_add(u, f):
    sid = f.get("subject_id")
    due = None
    if f.get("due"):
        d = parse_date(str(f["due"]), today_d())
        if d:
            due = datetime.combine(d, dtime(23, 59), tzinfo=TZ)
    tq = f.get("questions")
    est = f.get("minutes") or (max(15, min(60, 2 * tq)) if tq else 30)
    await q(
        """INSERT INTO homework(user_id, subject_id, title, hw_type, total_q,
           est_minutes, due_at) VALUES($1::uuid,$2::uuid,$3,$4,$5,$6,$7)""",
        u["id"], sid, (f.get("title") or "Homework").strip()[:120],
        f.get("hw_type") or "custom", tq, est, due)
    return (f"📝 Saved homework: <b>{esc(f.get('title'))}</b>"
            + (f" — due {esc(f['due'])}" if due else ""))


async def do_hw_progress(u, f):
    title = (f.get("title") or "").strip()
    rows = await qrows(
        "SELECT * FROM homework WHERE user_id=$1::uuid "
        "AND status NOT IN ('completed') AND title ILIKE $2 ORDER BY due_at",
        u["id"], f"%{title}%") if title else []
    if not rows:
        return "I couldn't find that homework — check the exact title with /homework."
    hw = rows[0]
    done = int(f.get("done") or 0)
    total = hw["total_q"]
    newc = (hw["completed_q"] or 0) + done
    if total:
        newc = min(newc, total)
    status = "completed" if total and newc >= total else "in_progress"
    await q("UPDATE homework SET completed_q=$2, status=$3 WHERE id=$1::uuid",
            str(hw["id"]), newc, status)
    prog = f" [{newc}/{total}]" if total else ""
    return (f"📝 Progress saved: <b>{esc(hw['title'])}</b>{prog} — "
            f"{'done! 🎉' if status == 'completed' else 'keep going.'}")


async def do_test_add(u, f):
    sid = f.get("subject_id")
    when = None
    if f.get("date"):
        d = parse_date(str(f["date"]), today_d())
        if d:
            when = datetime.combine(d, dtime(9, 0), tzinfo=TZ)
    await q(
        """INSERT INTO tests(user_id, subject_id, name, test_at, total_marks)
           VALUES($1::uuid,$2::uuid,$3,$4,$5)""",
        u["id"], sid, (f.get("name") or "Test").strip()[:120], when,
        f.get("total_marks"))
    return f"🧪 Test saved: <b>{esc(f.get('name'))}</b>" + \
        (f" — {when.strftime('%a %d %b')}" if when else "")


async def do_test_result(u, f):
    name = (f.get("name") or "").strip()
    rows = await qrows(
        "SELECT * FROM tests WHERE user_id=$1::uuid ORDER BY test_at DESC NULLS LAST",
        u["id"])
    test = None
    if name:
        for r in rows:
            if name.lower() in r["name"].lower():
                test = r; break
    if not test and f.get("subject_id"):
        for r in rows:
            if str(r["subject_id"]) == str(f.get("subject_id")):
                test = r; break
    if not test and rows:
        test = rows[0]
    if not test:
        return "I couldn't find that test. Add it first: 'test on friday'."
    obt, tot = f.get("obtained"), f.get("total") or test["total_marks"]
    if obt is None or tot is None:
        return "I need both obtained and total marks."
    pct = 100.0 * obt / tot
    await q("UPDATE tests SET obtained_marks=$2, total_marks=$3, "
            "status='completed' WHERE id=$1::uuid",
            str(test["id"]), obt, tot)
    # deterministic evidence: 20-question-equivalent, test weight 1.6
    ev_att = 20
    ev_cor = round(ev_att * pct / 100)
    await save_evidence(u, test["subject_id"], None, ev_att, ev_cor, 0,
                        "test", title=f"test: {test['name']}")
    verdict = "strong 💪" if pct >= 80 else \
        "solid 👍" if pct >= 60 else "needs work 🔧"
    base = (f"🧪 <b>{esc(test['name'])}</b>: {obt}/{tot} ({pct:.0f}%) — {verdict}\n"
            f"Recorded as evidence (test weight).")
    try:
        txt = await ai_call(
            f"A student scored {obt}/{tot} ({pct:.0f}%) in '{test['name']}'. "
            f"Write ONE encouraging, specific sentence (max 25 words) about what "
            f"to do next. No numbers except the percentage.", max_tokens=80)
        if txt and "<" not in txt:
            return base + "\n💬 " + esc(txt.strip()[:200])
    except AIError:
        pass
    return base


async def do_mistake_add(u, f):
    sid = f.get("subject_id")
    if not sid:
        return "Which subject? Try again with the subject name."
    topic_id = None
    if f.get("topic"):
        t = await find_topic(u["id"], sid, f["topic"]) or \
            await upsert_topic(u["id"], sid, f["topic"])
        topic_id = t["id"]
    mtype = (f.get("mtype") or "unknown")
    valid = ("conceptual", "calculation", "careless", "memory", "misread",
             "guessing", "time_pressure", "unknown")
    if mtype not in valid:
        mtype = "unknown"
    cnt = max(1, min(50, int(f.get("count") or 1)))
    fp = hashlib.md5(f"{u['id']}|{sid}|{topic_id}|{mtype}".encode()).hexdigest()
    await q(
        """INSERT INTO mistakes(user_id, subject_id, topic_id, mtype,
           description, count, fingerprint)
           VALUES($1::uuid,$2::uuid,$3::uuid,$4,$5,$6,$7)
           ON CONFLICT(user_id, fingerprint) DO UPDATE SET
             count=mistakes.count+EXCLUDED.count,
             last_seen=now(), resolved=false""",
        u["id"], sid, topic_id, mtype,
        (f.get("description") or mtype)[:200], cnt, fp)
    reps = await mistake_reps(u["id"], topic_id) if topic_id else 0
    warn = ("\n⚠️ That's a repeated mistake — it will now drive your "
            "revision priority." if reps >= 2 else "")
    return f"🧠 {cnt} mistake(s) banked ({mtype}).{warn}"


async def do_syllabus(u, blocks):
    added_s, added_t = 0, 0
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
    return (f"📚 Syllabus saved: {added_s} subjects, {added_t} topics.\n"
            f"Log study against them and I'll start tracking mastery.")


async def do_save_note(u, f):
    await q("INSERT INTO resources(user_id, title, content) "
            "VALUES($1::uuid,$2,$3)", u["id"],
            (f.get("title") or "Note")[:80], (f.get("content") or "")[:4000])
    return "📎 Saved to your notes. /notes to browse."

# ============================================================
# COMMANDS / VIEWS
# ============================================================
MENU_KB = IK(
    [("▶️ Start Next", "nav:next"), ("📅 Plan", "nav:plan")],
    [("🏠 Dashboard", "nav:dash"), ("📊 Analytics", "nav:an")],
    [("📝 Homework", "nav:hw"), ("🧪 Tests", "nav:tests"),
     ("🔁 Revision", "nav:rev")],
    [("🧠 Quiz me", "nav:quiz"), ("📚 Syllabus", "nav:syl"),
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
        "• \"I finished 50 physics questions, 39 correct\"\n"
        "• \"studied organic chemistry 1h 30m\"\n"
        "• \"add homework: DPP 3, 40 questions, due friday\"\n"
        "• \"did 25 of 50 DPP\"\n"
        "• \"physics test on sunday\"\n"
        "• \"got 68 out of 75 in physics test\"\n"
        "• \"made 3 conceptual mistakes in rotation\"\n"
        "• \"I'm exhausted\" (I'll go easy today)\n"
        "• \"quiz me on thermodynamics\"\n"
        "• \"explain rotational motion\"\n"
        "• \"what should I study?\"\n\n"
        "Commands: /plan /next /homework /tests /revision /mistakes "
        "/analytics /quiz /notes /syllabus /settings\n\n"
        "📷 Send a syllabus photo and I'll import it.\n"
        "🎙 Send a voice note instead of typing.")
    await send(chat, txt, MENU_KB)


async def cmd_dashboard(u, chat):
    now, today = now_tz(), today_d()
    live = await live_session(u["id"])
    if live:
        mins = await live_minutes(u, now)
        await send(chat, f"⏱ <b>SESSION ACTIVE</b>\n{esc(live['title'] or 'Study')}\n"
                  f"Running: {fm(mins)}",
                   IK([("⏸ Pause", "ses:pause"), ("✅ Finish", "ses:finish"),
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
        lines.append(f"⚡ Energy: <b>{u['energy']}</b> — keeping it light today")
    tests = await qrows(
        "SELECT name, test_at FROM tests WHERE user_id=$1::uuid "
        "AND status='scheduled' AND test_at > now() - interval '1 day' "
        "ORDER BY test_at LIMIT 2", u["id"])
    if tests:
        lines += ["", "🧪 <b>TESTS</b>"]
        for t in tests:
            d = (t["test_at"] - now).days if t["test_at"] else None
            tag = "TODAY" if d == 0 else f"in {d}d" if d is not None else "unscheduled"
            lines.append(f"• {esc(t['name'])} — {tag}")
    hw = await qrow(
        """SELECT COUNT(*) FILTER (WHERE due_at < now()) over,
                  COUNT(*) FILTER (WHERE due_at >= now() OR due_at IS NULL) up
           FROM homework WHERE user_id=$1::uuid
           AND status NOT IN ('completed','abandoned')""", u["id"])
    if hw and (hw["over"] or hw["up"]):
        lines += ["", "📝 <b>HOMEWORK</b>"]
        if hw["over"]:
            lines.append(f"⚠️ {hw['over']} overdue")
        if hw["up"]:
            lines.append(f"🟡 {hw['up']} open")
    rev = await qval(
        "SELECT COUNT(*) FROM revisions WHERE user_id=$1::uuid AND due_at < now()",
        u["id"])
    if rev:
        lines += ["", f"🔁 <b>{rev}</b> revision(s) due"]
    kb = []
    if tests and tests[0]["test_at"] and (tests[0]["test_at"] - now).days <= 3:
        kb.append([("🧪 TEST SOON — prepare", "nav:next")])
    if hw and hw["over"]:
        kb.append([("⚠️ OVERDUE homework", "nav:hw")])
    kb += [[("▶️ START NEXT", "nav:next"), ("📅 Plan", "nav:plan")],
           [("📊 Analytics", "nav:an"), ("📚 More", "nav:menu")]]
    await send(chat, "\n".join(lines), IK(*kb))


async def build_candidates(u, now):
    cands = []
    tests = await qrows(
        "SELECT id, name, test_at FROM tests WHERE user_id=$1::uuid "
        "AND status='scheduled' AND test_at BETWEEN now() AND "
        "now() + interval '14 days' ORDER BY test_at", u["id"])
    test_days = [t for t in tests if t["test_at"] and
                 (t["test_at"] - now).days >= 0]
    min_test = min((t["test_at"] - now).days for t in test_days) if test_days else None
    for t in tests[:3]:
        d = (t["test_at"] - now).days if t["test_at"] else 99
        cands.append({"kind": "test_prep", "key": f"test:{t['id']}",
                      "title": f"Prepare — {t['name']}", "est": 45,
                      "subject": "", "subject_id": None, "topic_id": None,
                      "meta": {"test_in_days": d if d >= 0 else 0}})
    for r in await qrows(
            """SELECT rv.id, rv.due_at, rv.mode, rv.rev_type, rv.topic_id,
                      s.name subj, t.name top, m.mastery
               FROM revisions rv
               LEFT JOIN subjects s ON s.id=rv.subject_id
               LEFT JOIN topics t ON t.id=rv.topic_id
               LEFT JOIN mastery m ON m.user_id=rv.user_id AND m.topic_id=rv.topic_id
               WHERE rv.user_id=$1::uuid AND rv.due_at < now() + interval '1 day'
               ORDER BY rv.due_at LIMIT 25""", u["id"]):
        od = max(0, (now - r["due_at"]).days)
        kind = "revision" if r["mode"] != "practice" else "quiz"
        cands.append({
            "kind": kind, "key": f"rev:{r['id']}",
            "title": f"{r['subj'] or ''} — {r['top'] or ''} — "
                     f"{r['rev_type'].replace('_', ' ')}".strip(" —"),
            "est": 20 if kind == "revision" else 15,
            "subject": r["subj"] or "", "chapter": r["top"] or "",
            "subject_id": r["subject_id"] and str(r["subject_id"]),
            "topic_id": str(r["topic_id"]),
            "meta": {"overdue_days": od, "mastery": r["mastery"],
                     "mode": r["mode"], "rev_type": r["rev_type"],
                     "revision_id": str(r["id"]), "topic": r["top"],
                     "test_in_days": min_test}})
    for r in await qrows(
            """SELECT SUM(m.count) reps, s.name subj, t.name top,
                      m.subject_id sid, m.topic_id tid
               FROM mistakes m LEFT JOIN subjects s ON s.id=m.subject_id
               LEFT JOIN topics t ON t.id=m.topic_id
               WHERE m.user_id=$1::uuid AND m.resolved=false
               GROUP BY s.name, t.name, m.subject_id, m.topic_id
               HAVING SUM(m.count) >= 2 ORDER BY reps DESC LIMIT 5""", u["id"]):
        cands.append({"kind": "mistake_review",
                      "key": f"mist:{r['tid']}",
                      "title": f"Mistake review — {r['subj']} {r['top']}",
                      "est": 25, "subject": r["subj"] or "",
                      "chapter": r["top"] or "",
                      "subject_id": r["sid"] and str(r["sid"]),
                      "topic_id": r["tid"] and str(r["tid"]),
                      "meta": {"reps": int(r["reps"]),
                               "test_in_days": min_test}})
    for r in await qrows(
            """SELECT h.id, h.title, h.due_at, h.total_q, h.completed_q,
                      h.est_minutes, s.name subj, h.subject_id sid
               FROM homework h LEFT JOIN subjects s ON s.id=h.subject_id
               WHERE h.user_id=$1::uuid
               AND h.status NOT IN ('completed','abandoned')
               ORDER BY h.due_at NULLS LAST LIMIT 20""", u["id"]):
        rem = (r["total_q"] - r["completed_q"]) if r["total_q"] else None
        est = r["est_minutes"] or (max(15, min(60, 2 * rem)) if rem else 30)
        cands.append({"kind": "homework", "key": f"hw:{r['id']}",
                      "title": r["title"], "est": est,
                      "subject": r["subj"] or "",
                      "subject_id": r["sid"] and str(r["sid"]),
                      "topic_id": None,
                      "meta": {"due_at": r["due_at"], "remaining": rem,
                               "homework_id": str(r["id"]),
                               "test_in_days": min_test}})
    for r in await qrows(
            """SELECT t.id tid, t.name top, t.subject_id sid, s.name subj,
                      m.mastery, m.last_evidence
               FROM topics t JOIN subjects s ON s.id=t.subject_id
               LEFT JOIN mastery m ON m.user_id=t.user_id AND m.topic_id=t.id
               WHERE t.user_id=$1::uuid
               ORDER BY m.mastery ASC NULLS FIRST LIMIT 8""", u["id"]):
        stale = 999
        if r["last_evidence"]:
            stale = (now.date() - r["last_evidence"].date()).days
        cands.append({"kind": "study", "key": f"study:{r['tid']}",
                      "title": f"Study — {r['subj']} — {r['top']}",
                      "est": 45, "subject": r["subj"], "chapter": r["top"],
                      "subject_id": str(r["sid"]), "topic_id": str(r["tid"]),
                      "meta": {"mastery": r["mastery"], "stale_days": stale,
                               "test_in_days": min_test}})
    return cands


async def cmd_next(u, chat, max_minutes=None, exclude=None):
    now, today = now_tz(), today_d()
    live = await live_session(u["id"])
    if live:
        await cmd_dashboard(u, chat)
        return
    avail, _ = day_minutes(u, today)
    remaining = max(0, avail - await spent_today(u, today)
                    - await live_minutes(u, now))
    if max_minutes:
        remaining = min(remaining, max_minutes)
    cands = await build_candidates(u, now)
    scored = []
    for c in cands:
        sc, rs = score_task(c, now, u["energy"])
        scored.append((c, sc, rs))
    pick = choose_next(scored, remaining, exclude)
    if not pick:
        await send(chat, "Nothing left that fits right now — rest is also "
                   "strategy. 🌙 Log something with a message like "
                   "'50 questions 39 correct'.", MENU_KB)
        return
    c, sc, rs = pick
    pid = await new_pending(u, "task_start",
                            {"cand": c, "score": sc, "reasons": rs}, "engine")
    why = "\n".join(f"• {esc(r)}" for r in rs)
    est = fm(c["est"])
    icon = {"revision": "🔁", "quiz": "🧠", "homework": "📝",
            "test_prep": "🧪", "mistake_review": "🧨",
            "study": "📖"}.get(c["kind"], "▶️")
    txt = (f"▶️ <b>START NEXT</b>\n\n{icon} <b>{esc(c['title'])}</b>\n"
           f"⏱ {est} • fits in your {fm(remaining)} left\n\n"
           f"<b>Why:</b>\n{why}")
    await send(chat, txt, IK(
        [("▶️ Start", f"go:{pid}"), ("🔄 Another", f"go:{pid}:alt")],
        [("🏠 Menu", "nav:menu")]))


async def start_live_session(u, chat, cand):
    existing = await live_session(u["id"])
    if existing:
        await cmd_dashboard(u, chat)
        return
    kind = cand["kind"]
    src_type = {"revision": "live", "quiz": "live", "homework": "live",
                "test_prep": "live", "mistake_review": "live",
                "study": "live"}[kind]
    now = now_tz()
    row = await qrow(
        """INSERT INTO study_sessions(user_id, subject_id, topic_id, source,
           status, title, started_at) VALUES($1::uuid,$2::uuid,$3::uuid,$4,
           'active',$5,$6) RETURNING id""",
        u["id"], cand.get("subject_id"), cand.get("topic_id"), src_type,
        cand["title"], now)
    await send(chat, f"⏱ <b>Session started</b> — {esc(cand['title'])}\n"
              f"Tap ✅ when done. I'll ask how many questions you solved.",
              IK([("⏸ Pause", "ses:pause"), ("✅ Finish", "ses:finish")],
                 [("❌ Abandon", "ses:abandon")]))


async def cmd_plan(u, chat):
    now, today = now_tz(), today_d()
    avail, busy = day_minutes(u, today)
    cands = await build_candidates(u, now)
    last = await qval(
        "SELECT MAX(created_at::date) FROM study_sessions "
        "WHERE user_id=$1::uuid AND status='finished'", u["id"])
    missed = 0
    if last and (today - last).days > 1:
        missed = (today - last).days - 1
    plan = build_plan(u, cands, avail, u["energy"], missed, now)
    await q("""INSERT INTO daily_plans(user_id, plan_date, available_minutes, tasks)
               VALUES($1::uuid,$2,$3,$4::jsonb)
               ON CONFLICT(user_id, plan_date) DO UPDATE SET
                 available_minutes=EXCLUDED.available_minutes,
                 tasks=EXCLUDED.tasks, created_at=now()""",
            u["id"], today, plan["available"], json.dumps(plan["tasks"]))
    lines = [f"📅 <b>PLAN — {today.strftime('%a %d %b')}</b>",
             f"Capacity: <b>{fm(plan['available'])}</b> "
             f"(after school/coaching/meals)",
             f"Planned: <b>{fm(plan['allocated'])}</b>", ""]
    for i, t in enumerate(plan["tasks"], 1):
        lines.append(f"{i}. {esc(t['title'])} — {fm(t['est'])}")
    if plan["missed"]:
        lines.append(f"\n<i>Missed {plan['missed']} day(s) — kept urgent items, "
                     f"spread the rest.</i>")
    if plan["deferred"]:
        lines += ["", "<i>Kept for later (not lost):</i>"]
        lines += [f"• {esc(t)} — {esc(r)}" for t, r in plan["deferred"][:5]]
    await send(chat, "\n".join(lines), IK(
        [("▶️ Start Next", "nav:next"), ("🔄 Replan", "nav:plan")]))


async def cmd_homework(u, chat):
    rows = await qrows(
        """SELECT h.*, s.name subj FROM homework h
           LEFT JOIN subjects s ON s.id=h.subject_id
           WHERE h.user_id=$1::uuid
           AND h.status NOT IN ('completed','abandoned')
           ORDER BY h.due_at NULLS LAST LIMIT 15""", u["id"])
    now = now_tz()
    if not rows:
        await send(chat, "📝 No open homework. 🎉\nAdd some: "
                   "'homework DPP 4, 30 questions, due monday'.", MENU_KB)
        return
    lines = ["📝 <b>HOMEWORK</b>", ""]
    for r in rows:
        mark = "⚠️" if r["due_at"] and r["due_at"] < now else "•"
        line = f"{mark} {esc(r['title'])}"
        if r["subj"]:
            line += f" ({esc(r['subj'])})"
        if r["total_q"]:
            pct = 100 * (r["completed_q"] or 0) / r["total_q"]
            line += f" [{r['completed_q']}/{r['total_q']}] {bar(pct, 6)}"
        if r["due_at"]:
            d = (r["due_at"].date() - now.date()).days
            line += " — due today!" if d == 0 else \
                f" — in {d}d" if d > 0 else f" — {-d}d OVERDUE"
        lines.append(line)
    lines += ["", "<i>Report progress: 'did 25 of 50 DPP'.</i>"]
    await send(chat, "\n".join(lines), MENU_KB)


async def cmd_tests(u, chat):
    rows = await qrows(
        """SELECT t.*, s.name subj FROM tests t
           LEFT JOIN subjects s ON s.id=t.subject_id
           WHERE t.user_id=$1::uuid
           ORDER BY t.test_at DESC NULLS LAST LIMIT 12""", u["id"])
    if not rows:
        await send(chat, "🧪 No tests yet. Add: 'physics test on sunday'.", MENU_KB)
        return
    now = now_tz()
    up = [r for r in rows if r["test_at"] and r["test_at"] >= now
          and r["status"] == "scheduled"]
    done = [r for r in rows if r["status"] == "completed"]
    lines = ["🧪 <b>TESTS</b>", ""]
    if up:
        lines.append("<b>Upcoming</b>")
        for r in sorted(up, key=lambda x: x["test_at"]):
            d = (r["test_at"].date() - now.date()).days
            lines.append(f"• {esc(r['name'])} ({esc(r['subj'] or '')}) — "
                         f"{'TODAY' if d == 0 else f'in {d}d'}")
        lines.append("")
    if done:
        lines.append("<b>Results</b>")
        for r in done[:6]:
            if r["total_marks"]:
                pct = 100 * (r["obtained_marks"] or 0) / r["total_marks"]
                lines.append(f"• {esc(r['name'])} — {r['obtained_marks']}/"
                             f"{r['total_marks']} ({pct:.0f}%) {bar(pct, 6)}")
    await send(chat, "\n".join(lines), MENU_KB)


async def cmd_revision(u, chat):
    rows = await qrows(
        """SELECT rv.*, s.name subj, t.name top FROM revisions rv
           LEFT JOIN subjects s ON s.id=rv.subject_id
           LEFT JOIN topics t ON t.id=rv.topic_id
           WHERE rv.user_id=$1::uuid AND rv.due_at < now() + interval '2 days'
           ORDER BY rv.due_at LIMIT 12""", u["id"])
    if not rows:
        await send(chat, "🔁 No revision due. It appears automatically once you "
                   "log questions — accuracy decides when it returns.", MENU_KB)
        return
    now = now_tz()
    lines = ["🔁 <b>REVISION QUEUE</b>", ""]
    for r in rows:
        od = max(0, (now - r["due_at"]).days)
        tag = " 🔴" if od > 0 else ""
        mode = " 🧠practice" if r["mode"] == "practice" else ""
        lines.append(f"• {esc(r['subj'] or '')} / {esc(r['top'] or '')} — "
                     f"{r['rev_type'].replace('_', ' ')}"
                     f"{' (overdue ' + str(od) + 'd)' if od else ''}{tag}{mode}")
    await send(chat, "\n".join(lines), IK([("▶️ Start Next", "nav:next")]))


async def cmd_mistakes(u, chat):
    rows = await qrows(
        """SELECT SUM(m.count) c, m.mtype, s.name subj, t.name top
           FROM mistakes m LEFT JOIN subjects s ON s.id=m.subject_id
           LEFT JOIN topics t ON t.id=m.topic_id
           WHERE m.user_id=$1::uuid AND m.resolved=false
           GROUP BY m.mtype, s.name, t.name
           ORDER BY c DESC LIMIT 12""", u["id"])
    if not rows:
        await send(chat, "🧨 No mistakes banked. Report them: "
                   "'3 conceptual mistakes in rotation'.", MENU_KB)
        return
    lines = ["🧨 <b>MISTAKE BANK</b>", ""]
    for r in rows:
        lines.append(f"• {esc(r['subj'] or '')} / {esc(r['top'] or '')} — "
                     f"{r['mtype']} × {r['c']}")
    lines += ["", "<i>2+ repeats on a topic = it takes over your revision.</i>"]
    await send(chat, "\n".join(lines), MENU_KB)


async def cmd_syllabus(u, chat):
    rows = await qrows(
        """SELECT s.name subj, COUNT(t.id) n,
                  (SELECT COUNT(*) FROM mastery m
                    WHERE m.user_id=s.user_id AND m.subject_id=s.id
                    AND m.events>0) tracked
           FROM subjects s LEFT JOIN topics t ON t.subject_id=s.id
           WHERE s.user_id=$1::uuid GROUP BY s.name, s.user_id, s.id
           ORDER BY s.name""", u["id"])
    if not rows:
        await send(chat, "📚 No subjects yet. Send a syllabus photo, or type:\n"
                   "<code>Physics: Rotation, SHM, Thermodynamics</code>", MENU_KB)
        return
    lines = ["📚 <b>SYLLABUS</b>", ""]
    for r in rows:
        lines.append(f"• <b>{esc(r['subj'])}</b> — {r['n']} topics, "
                     f"{r['tracked']} with mastery data")
    lines += ["", "<i>Send a photo of your syllabus page to import more.</i>"]
    await send(chat, "\n".join(lines), MENU_KB)


async def cmd_notes(u, chat):
    rows = await qrows(
        "SELECT id, title FROM resources WHERE user_id=$1::uuid "
        "ORDER BY created_at DESC LIMIT 10", u["id"])
    if not rows:
        await send(chat, "📎 No notes yet. Send any photo — I'll read it and "
                   "can save the text.", MENU_KB)
        return
    rows_kb = [[(f"📄 {r['title'][:40]}", f"note:{r['id']}")] for r in rows[:8]]
    await send(chat, "📎 <b>YOUR NOTES</b>", IK(*rows_kb))


async def cmd_analytics(u, chat):
    today = today_d()
    wk_start = sod(today - timedelta(days=7))
    wk_end = sod(today + timedelta(days=1))
    prev_start = sod(today - timedelta(days=14))
    r = await qrow(
        """SELECT COALESCE(SUM(duration_minutes),0) mins,
                  COALESCE(SUM(questions_attempted),0) q,
                  COALESCE(SUM(questions_correct),0) c
           FROM study_sessions WHERE user_id=$1::uuid
           AND status='finished' AND created_at >= $2 AND created_at < $3""",
        u["id"], wk_start, wk_end)
    prev = await qval(
        """SELECT COALESCE(SUM(duration_minutes),0) FROM study_sessions
           WHERE user_id=$1::uuid AND status='finished'
           AND created_at >= $2 AND created_at < $3""",
        u["id"], prev_start, wk_start)
    if not r or (r["mins"] == 0 and r["q"] == 0):
        await send(chat, "📊 Not enough data yet — log a few sessions first "
                   "('50 questions 39 correct').", MENU_KB)
        return
    mins, q, c = int(r["mins"]), int(r["q"]), int(r["c"])
    acc = 100.0 * c / q if q else 0
    ref = max(1200, int(prev or 0) * 120 // 100)
    trend = "→" if prev is None else ("↑" if mins >= prev else "↓")
    lines = ["📊 <b>LAST 7 DAYS</b>", "",
             f"Study      {bar(min(100, 100*mins/ref))} {fm(mins)} {trend}",
             f"Questions  {bar(min(100, 100*q/600))} {q}",
             f"Accuracy   {bar(acc)} {acc:.0f}%"]
    subs = await qrows(
        """SELECT s.name, SUM(ss.questions_attempted) q,
                  SUM(ss.questions_correct) c
           FROM study_sessions ss JOIN subjects s ON s.id=ss.subject_id
           WHERE ss.user_id=$1::uuid AND ss.status='finished'
           AND ss.created_at >= $2 AND ss.created_at < $3
           AND ss.questions_attempted > 0
           GROUP BY s.name ORDER BY c::float/NULLIF(SUM(ss.questions_attempted),0) ASC""",
        u["id"], wk_start, wk_end)
    if subs:
        lines += ["", "<b>By subject</b>"]
        for srow in subs:
            sa = 100 * (srow["c"] or 0) / srow["q"] if srow["q"] else 0
            lines.append(f"{srow['name']:<11} {bar(sa, 8)} {sa:.0f}% ({srow['q']}q)")
    mast = await qrows(
        """SELECT s.name subj, t.name top, m.mastery FROM mastery m
           JOIN topics t ON t.id=m.topic_id JOIN subjects s ON s.id=m.subject_id
           WHERE m.user_id=$1::uuid AND m.events>0
           ORDER BY m.mastery ASC LIMIT 5""", u["id"])
    if mast:
        lines += ["", "<b>Weakest topics</b>"]
        for m in mast:
            lines.append(f"• {esc(m['subj'])}/{esc(m['top'])} — "
                         f"{m['mastery']:.0f}% {bar(m['mastery'], 8)}")
    dates = await qrows(
        """SELECT DISTINCT created_at::date d FROM study_sessions
           WHERE user_id=$1::uuid AND status='finished'
           AND (duration_minutes > 0 OR questions_attempted > 0)
           ORDER BY d DESC LIMIT 60""", u["id"])
    streak = 0
    if dates:
        ds = {r["d"] for r in dates}
        cur = today if today in ds else today - timedelta(days=1)
        while cur in ds:
            streak += 1
            cur -= timedelta(days=1)
    if streak:
        lines += ["", f"🔥 <b>{streak}-day streak</b>"]
    rep = await qrows(
        """SELECT t.name top, SUM(m.count) c FROM mistakes m
           JOIN topics t ON t.id=m.topic_id
           WHERE m.user_id=$1::uuid AND m.resolved=false
           GROUP BY t.name ORDER BY c DESC LIMIT 3""", u["id"])
    if rep:
        lines += ["", "⚠️ <b>Repeated errors</b>"]
        lines += [f"{i}. {esc(r['top'])} — {r['c']}"
                  for i, r in enumerate(rep, 1)]
    await send(chat, "\n".join(lines), MENU_KB)


async def cmd_settings(u, chat):
    lines = ["⚙️ <b>SETTINGS</b>", "",
             f"👤 {esc(u['display_name'] or u['first_name'] or 'Student')}",
             f"🎯 {esc(u['exam_goal'] or '—')}"
             + (f" ({(u['exam_date'] - today_d()).days}d away)"
                if u["exam_date"] else ""),
             f"😴 {u['wake_time'].strftime('%H:%M')}–"
             f"{u['sleep_time'].strftime('%H:%M')}"]
    if u["school_days"]:
        lines.append(f"🏫 {u['school_start'].strftime('%H:%M')}–"
                     f"{u['school_end'].strftime('%H:%M')}")
    if u["coaching_days"]:
        lines.append(f"📖 {u['coaching_start'].strftime('%H:%M')}–"
                     f"{u['coaching_end'].strftime('%H:%M')}")
    lines.append(f"⚡ Energy: {u['energy']}")
    lines += ["", "<i>Everything is editable by telling me, e.g. "
                  "'wake at 6' or 'school 8-2 mon-sat'.</i>"]
    await send(chat, "\n".join(lines), IK(
        [("🔄 Restart onboarding", "ob:restart")],
        [("🧨 WIPE ALL DATA", "reset:ask")]))

# ============================================================
# QUIZ
# ============================================================
async def start_quiz(u, chat, topic_text, count=5, subject_id=None,
                     topic_id=None, topic_label=None):
    label = topic_label or topic_text or "mixed revision"
    prompt = (f'Generate {max(1, min(8, count))} multiple-choice questions '
              f'on "{label}" for a {u.get("exam_goal") or "competitive exam"} '
              f'student. Return ONLY JSON: '
              f'{{"questions":[{{"question":"...","options":["a","b","c","d"],'
              f'"answer":0,"explanation":"short why"}}]}} '
              f'Rules: exactly 4 options; "answer" is the 0-based index of '
              f'the correct option; exactly one correct; no trick options; '
              f'exam-level difficulty.')
    key = "qz:" + hashlib.md5(prompt.encode()).hexdigest()
    try:
        raw = _cache_get(key)
        if raw is None:
            raw = _load_json(await ai_call(prompt, max_tokens=200 * count))
            _cache_set(key, raw)
    except AIError:
        await send(chat, "🧠 My quiz brain is rate-limited — try again in a "
                   "minute (free Gemini limits).", MENU_KB)
        return
    qs = []
    for x in (raw.get("questions") or [])[:count]:
        try:
            opts = [str(o)[:150] for o in x["options"][:4]]
            ans = int(x["answer"])
            if len(opts) == 4 and 0 <= ans <= 3 and x.get("question"):
                qs.append({"question": str(x["question"])[:400],
                           "options": opts, "answer": ans,
                           "explanation": str(x.get("explanation", ""))[:400]})
        except Exception:
            continue
    if len(qs) < 3:
        await send(chat, "🧠 Couldn't build a clean quiz for that — try a "
                   "more specific topic.", MENU_KB)
        return
    if not topic_id and topic_text:
        subs = await get_subjects(u["id"])
        sid = subject_id
        if not sid and len(subs) == 1:
            sid = subs[0]["id"]
        if sid:
            t = await find_topic(u["id"], sid, topic_text) or \
                await upsert_topic(u["id"], sid, topic_text)
            topic_id = t["id"]
            subject_id = sid
    qid = await qval(
        """INSERT INTO quizzes(user_id, subject_id, topic_id, topic_label,
           questions) VALUES($1::uuid,$2::uuid,$3::uuid,$4,$5::jsonb)
           RETURNING id::text""",
        u["id"], subject_id, topic_id, label, json.dumps(qs))
    await send_quiz_q(chat, qid, qs, 0, label)


async def send_quiz_q(chat, qid, qs, idx, label):
    q = qs[idx]
    letters = "ABCD"
    body = (f"🧠 <b>QUIZ — {esc(label)}</b> ({idx + 1}/{len(qs)})\n\n"
            f"{esc(q['question'])}\n\n"
            + "\n".join(f"{letters[i]}. {esc(o)}"
                        for i, o in enumerate(q["options"])))
    kb = IK([(f"{letters[i]}", f"quiz:{qid}:{idx}:{i}") for i in range(4)],
            [("🛑 Stop", f"quiz:{qid}:stop:0")])
    await send(chat, body, kb)


async def quiz_answer(u, chat, qid, qi, opt):
    row = await qrow("SELECT * FROM quizzes WHERE id=$1::uuid "
                     "AND user_id=$2::uuid", qid, u["id"])
    if not row or row["status"] != "active":
        return
    qs = json.loads(row["questions"])
    answers = json.loads(row["answers"])
    if len(answers) != qi:
        return  # stale button
    answers.append(opt)
    await q("UPDATE quizzes SET answers=$2::jsonb WHERE id=$1::uuid",
            qid, json.dumps(answers))
    if len(answers) >= len(qs):
        await quiz_finish(u, chat, qid, qs, answers, row)
    else:
        await send_quiz_q(chat, qid, qs, len(answers), row["topic_label"])


async def quiz_finish(u, chat, qid, qs, answers, row):
    correct = sum(1 for a, qq in zip(answers, qs) if a == qq["answer"])
    att = len(answers)
    await q("UPDATE quizzes SET status='done' WHERE id=$1::uuid", qid)
    if att:
        await save_evidence(u, row["subject_id"], row["topic_id"], att,
                            correct, 0, "quiz",
                            title=f"quiz: {row['topic_label']}")
    letters = "ABCD"
    recap = []
    for a, qq in zip(answers, qs):
        mark = "✅" if a == qq["answer"] else "❌"
        recap.append(f"{mark} {letters[qq['answer']]}. "
                     f"{esc(qq['options'][qq['answer']])}"
                     + (f" — {esc(qq['explanation'])}" if qq["explanation"] else ""))
    acc = 100 * correct / att if att else 0
    lines = [f"🧠 <b>Quiz done: {correct}/{att}</b> ({acc:.0f}%)", ""]
    lines += recap[:8]
    lines += ["", "Logged as evidence — mastery & revision updated."]
    await send(chat, "\n".join(lines), IK([("▶️ Start Next", "nav:next")]))

# ============================================================
# ONBOARDING
# ============================================================
OB_STEPS = [
    ("name", "👤 What should I call you?", True, "text"),
    ("exam", "🎯 What are you preparing for?\n"
             "(e.g. 'JEE 2027', 'NEET', 'Class 12 boards')", True, "text"),
    ("exam_date", "📅 Main exam date? (e.g. '24 may 2027')\n/skip if not fixed",
     False, "date"),
    ("subjects", "📚 Your subjects, comma-separated\n"
                 "(e.g. 'Physics, Chemistry, Maths')", True, "subjects"),
    ("wake", "🌅 Wake time? (e.g. '6:30')", True, "time"),
    ("sleep", "🌙 Sleep time? (e.g. '23:00')", True, "time"),
    ("school", "🏫 School hours? (e.g. '8-2 mon-sat')\n/skip if none",
     False, "class"),
    ("coaching", "📖 Coaching hours? (e.g. '5-8 mon,wed,fri')\n/skip if none",
     False, "class"),
]


async def ob_start(u, chat):
    await q("UPDATE users SET ob_state='{}'::jsonb, ob_step=$2 "
            "WHERE id=$1::uuid", u["id"], OB_STEPS[0][0])
    await send(chat,
               "🧠 <b>Welcome to StudyOS</b>\n\nI learn from what you actually "
               "do — not from plans you forget. A few quick questions, one at "
               "a time. Optional ones you can /skip.\n\n" +
               OB_STEPS[0][1])


async def ob_summary(u, chat):
    st = u["ob_state"]
    lines = ["<b>Here's what I've understood.</b>", ""]
    if st.get("name"):
        lines.append(f"👤 {esc(st['name'])}")
    if st.get("exam"):
        lines.append(f"🎯 {esc(st['exam'])}" +
                     (f" — {esc(st['exam_date'])}" if st.get("exam_date") else ""))
    if st.get("subjects"):
        lines.append("📚 " + esc(", ".join(st["subjects"])))
    if st.get("school"):
        lines.append(f"🏫 {st['school']}")
    if st.get("coaching"):
        lines.append(f"📖 {st['coaching']}")
    lines.append(f"😴 {st.get('wake')} – {st.get('sleep')}")
    lines += ["", "All correct?"]
    await send(chat, "\n".join(lines), IK(
        [("✅ Confirm", "ob:confirm")],
        [("🔄 Start over", "ob:restart")]))


async def ob_handle(u, chat, text):
    st = dict(u["ob_state"] or {})
    step = u["ob_step"]
    if step is None or step == "":
        step = OB_STEPS[0][0]
    if step == "__done":
        await ob_summary(u, chat)
        return
    key, prompt, required, kind = next(
        (s for s in OB_STEPS if s[0] == step), OB_STEPS[0])
    t = (text or "").strip()
    if t.lower() in SKIP_WORDS and not required:
        await q("UPDATE users SET ob_step=$2, ob_state=$3::jsonb "
                "WHERE id=$1::uuid", u["id"], next_step(key), json.dumps(st))
        await ob_prompt_next(u, chat, next_step(key))
        return
    if kind == "text":
        if not t:
            await send(chat, "A short answer works. " + prompt)
            return
        st[key] = t[:60]
    elif kind == "date":
        d = parse_date(t, today_d())
        if not d:
            await send(chat, "Couldn't read that date — try '24 may 2027'.")
            return
        st[key] = d.isoformat()
    elif kind == "subjects":
        names = [x.strip().title() for x in re.split(r"[,;]+", t) if x.strip()]
        if not names:
            await send(chat, "List them like: Physics, Chemistry, Maths")
            return
        st[key] = names[:8]
    elif kind == "time":
        tm = parse_time(t)
        if not tm:
            await send(chat, "Try a time like '6:30' or '11pm'.")
            return
        st[key] = tm.strftime("%H:%M")
    elif kind == "class":
        pc = parse_class(t)
        if not pc:
            await send(chat, "Try: '8-2 mon-sat' (time range + days). Or /skip.")
            return
        st[key] = t[:60]
        st[key + "_data"] = {"s": pc[0].strftime("%H:%M"),
                             "e": pc[1].strftime("%H:%M"), "d": pc[2]}
    nxt = next_step(key)
    await q("UPDATE users SET ob_step=$2, ob_state=$3::jsonb WHERE id=$1::uuid",
            u["id"], nxt, json.dumps(st))
    await ob_prompt_next(u, chat, nxt)


def next_step(key):
    keys = [s[0] for s in OB_STEPS]
    i = keys.index(key) if key in keys else len(keys)
    return keys[i + 1] if i + 1 < len(keys) else "__done"


async def ob_prompt_next(u, chat, step):
    if step == "__done":
        await ob_summary(u, chat)
        return
    _, prompt, required, _ = next(s for s in OB_STEPS if s[0] == step)
    await send(chat, prompt)


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
    wake = parse_time(st.get("wake") or "06:30") or dtime(6, 30)
    sleep = parse_time(st.get("sleep") or "23:00") or dtime(23, 0)
    await q(
        """UPDATE users SET display_name=$2, exam_goal=$3, exam_date=$4,
           wake_time=$5, sleep_time=$6,
           school_start=$7, school_end=$8, school_days=$9,
           coaching_start=$10, coaching_end=$11, coaching_days=$12,
           onboarded=true, ob_step=NULL
           WHERE id=$1::uuid""",
        u["id"], st.get("name"), st.get("exam"), exam_d, wake, sleep,
        (dtime.fromisoformat(sch["s"]) if sch else None),
        (dtime.fromisoformat(sch["e"]) if sch else None),
        (sch["d"] if sch else None),
        (dtime.fromisoformat(coa["s"]) if coa else None),
        (dtime.fromisoformat(coa["e"]) if coa else None),
        (coa["d"] if coa else None))
    await send(chat,
               "✅ <b>You're set up.</b>\n\nNow just talk to me:\n"
               "• \"I finished 50 physics questions, 39 correct\"\n"
               "• 📷 send a syllabus photo to import chapters\n"
               "• \"what should I study?\"", IK(
                   [("▶️ Start Next", "nav:next")],
                   [("🏠 Dashboard", "nav:dash")]))

# ============================================================
# PHOTO / VOICE
# ============================================================
PHOTO_SYS = """You analyze a photo for a study app. Read ONLY what is clearly visible. Never invent content. Return ONLY JSON:
{"type":"syllabus|test|homework|notes|unknown",
 "text":"all readable text (or empty string)",
 "syllabus":[{"subject":"...","topics":["..."]}],
 "test":{"name":"...","subject":"...","total":null,"obtained":null},
 "homework":{"title":"...","subject":"...","questions":null},
 "confidence":"high|medium|low"}
Use "syllabus" when the image lists chapters/units per subject. Use "test" for exam papers with visible marks. Use "notes" otherwise."""


async def handle_photo(u, chat, msg):
    photo = msg["photo"][-1]
    try:
        img = await get_file_bytes(photo["file_id"])
    except Exception as e:
        LOG.warning("photo download failed: %s", e)
        await send(chat, "Couldn't download that photo — try again.")
        return
    await send(chat, "🔍 Reading the image…")
    try:
        contents = [PHOTO_SYS, types.Part.from_bytes(data=img,
                                                     mime_type="image/jpeg")]
        raw = _load_json(await ai_call(contents, max_tokens=2000))
    except AIError:
        await send(chat, "My vision service is rate-limited — try again in a "
                   "minute.", MENU_KB)
        return
    ptype = (raw.get("type") or "unknown").lower()
    conf = (raw.get("confidence") or "low").lower()
    text = (raw.get("text") or "")[:2500]
    if ptype == "unknown" or conf == "low":
        await send(chat, "I can't read this confidently enough to save "
                   "anything. Could you type the key parts?", MENU_KB)
        return
    if ptype == "syllabus" and raw.get("syllabus"):
        blocks = raw["syllabus"][:12]
        preview = "\n".join(
            f"• <b>{esc(b.get('subject'))}</b>: "
            + ", ".join(esc(t) for t in (b.get("topics") or [])[:10])
            for b in blocks)
        pid = await new_pending(u, "syllabus", {"blocks": blocks}, "ai")
        await send(chat, "📷 <b>Syllabus detected</b>\n\n" + preview +
                   "\n\nConfidence: " + conf.upper(), CONFIRM_KB(pid))
        return
    if ptype == "test" and raw.get("test"):
        f = {k: v for k, v in raw["test"].items() if v is not None}
        pid = await new_pending(u, "test_result", f, "ai")
        lines = card_lines(f, [("name", "Test"), ("subject", "Subject"),
                               ("total", "Total"), ("obtained", "Obtained")])
        await send(chat, "📷 <b>Test paper detected</b>\n\n" +
                   "\n".join(lines) + f"\n\nConfidence: {conf.upper()}",
                   CONFIRM_KB(pid))
        return
    if ptype == "homework" and raw.get("homework"):
        f = {k: v for k, v in raw["homework"].items() if v is not None}
        subs = await get_subjects(u["id"])
        sid, sname = resolve_subject(f.get("subject"), subs)
        f["subject_id"] = sid
        f["subject"] = sname or f.get("subject")
        pid = await new_pending(u, "hw_add", f, "ai")
        if not sid and subs:
            await subject_pick_card(u, chat, pid, "hw_add", f,
                                    f"📷 <b>Homework detected</b>\n"
                                    f"{esc(f.get('title'))}")
            return
        lines = card_lines(f, [("title", "Homework"), ("subject", "Subject"),
                               ("questions", "Questions")])
        await send(chat, "📷 <b>Homework detected</b>\n\n" +
                   "\n".join(lines) + f"\n\nConfidence: {conf.upper()}",
                   CONFIRM_KB(pid))
        return
    pid = await new_pending(u, "save_note",
                            {"title": "Photo note", "content": text}, "ai")
    await send(chat, "📷 I read this as notes:\n\n<i>" + esc(text[:800]) +
               "</i>\n\nConfidence: " + conf.upper(), CONFIRM_KB(pid))


async def handle_voice(u, chat, msg):
    try:
        audio = await get_file_bytes(msg["voice"]["file_id"])
    except Exception as e:
        LOG.warning("voice download failed: %s", e)
        await send(chat, "Couldn't download that voice note.")
        return
    await send(chat, "🎙 Transcribing…")
    try:
        contents = ["Transcribe this audio to plain text. Output only the "
                    "transcription.",
                    types.Part.from_bytes(data=audio, mime_type="audio/ogg")]
        text = (await ai_call(contents, json_mode=False, max_tokens=300)).strip()
    except AIError:
        await send(chat, "Voice transcription is rate-limited right now — "
                   "type it instead?", MENU_KB)
        return
    if not text:
        await send(chat, "I couldn't hear that clearly — try typing it.")
        return
    await send(chat, f"🎙 <i>{esc(text[:300])}</i>")
    await handle_text_msg(u, chat, text)

# ============================================================
# TEXT ROUTING — fast path first (zero cost), AI second
# ============================================================
async def handle_text_msg(u, chat, text):
    t = text.strip()
    low = t.lower()
    # live session question logging
    pend = await qrow(
        "SELECT * FROM pending_actions WHERE user_id=$1::uuid "
        "AND kind='session_log' AND status='pending' "
        "ORDER BY created_at DESC LIMIT 1", u["id"])
    if pend:
        await wizard_session_log(u, chat, dict(pend), t)
        return
    # commands
    cmd = low.split()[0].lstrip("/").split("@")[0] if low else ""
    if cmd in ("start",):
        await cmd_start(u, chat); return
    if cmd in ("menu", "home", "help"):
        if cmd == "help":
            await cmd_help(u, chat)
        else:
            await send(chat, "🏠 <b>StudyOS</b>", MENU_KB)
        return
    if cmd == "plan":
        await cmd_plan(u, chat); return
    if cmd in ("next", "startnext"):
        await cmd_next(u, chat); return
    if cmd == "homework":
        await cmd_homework(u, chat); return
    if cmd == "tests":
        await cmd_tests(u, chat); return
    if cmd in ("revision", "rev"):
        await cmd_revision(u, chat); return
    if cmd in ("mistakes", "errorbank"):
        await cmd_mistakes(u, chat); return
    if cmd in ("analytics", "stats"):
        await cmd_analytics(u, chat); return
    if cmd == "quiz":
        await start_quiz(u, chat, " ".join(low.split()[1:]) or None); return
    if cmd in ("syllabus",):
        await cmd_syllabus(u, chat); return
    if cmd in ("notes", "resources"):
        await cmd_notes(u, chat); return
    if cmd in ("settings", "profile"):
        await cmd_settings(u, chat); return
    if cmd == "notes":
        await cmd_notes(u, chat); return
    if not u["onboarded"]:
        await ob_handle(u, chat, t); return
    # ---- deterministic fast paths (no AI cost) ----
    energy = detect_energy(t)
    if energy and len(low.split()) <= 6:
        await set_energy(u, chat, energy)
        return
    if re.search(r"(what should i study|what do i study|whats next|"
                 r"what's next|next task|start next)", low):
        await cmd_next(u, chat); return
    if low in ("plan", "make me a plan", "today's plan", "todays plan",
               "make today's plan"):
        await cmd_plan(u, chat); return
    m = re.search(r"(?:i (?:have|got|only have)|only)\s+(\d+)\s*"
                  r"(?:minutes|min|mins)\b", low)
    if m:
        await cmd_next(u, chat, max_minutes=int(m.group(1))); return
    pq = parse_questions(t)
    if pq and (pq[1] is not None or pq[2] is not None):
        await candidate_log_session(u, chat, questions=pq[0],
                                    correct=pq[1], minutes=parse_duration(t),
                                    origin="fast")
        return
    if re.search(r"\b(studied|revised|solved|did)\b", low) and \
            parse_duration(t) and not pq:
        await candidate_log_session(u, chat, questions=None, correct=None,
                                    minutes=parse_duration(t),
                                    topic_maybe=t, origin="fast")
        return
    # ---- AI router ----
    try:
        routed = await ai_route(t, u)
    except AIError:
        await send(chat, "My AI brain is rate-limited — wait a minute and "
                   "resend. (Numbers, plans and buttons all still work: 🏠)",
                   MENU_KB)
        return
    intent = (routed.get("intent") or "none").lower()
    f = routed.get("fields") or {}
    conf = (routed.get("confidence") or "medium").lower()
    uncertain = routed.get("uncertain") or []
    if intent in ("chat", "none"):
        reply = routed.get("reply")
        if reply:
            await send(chat, esc(reply), MENU_KB)
        else:
            await cmd_dashboard(u, chat)
        return
    if intent == "query":
        view = (f.get("view") or "dashboard").lower()
        await {"plan": cmd_plan, "next": cmd_next, "homework": cmd_homework,
               "tests": cmd_tests, "revision": cmd_revision,
               "analytics": cmd_analytics, "mistakes": cmd_mistakes,
               "syllabus": cmd_syllabus, "dashboard": cmd_dashboard} \
            .get(view, cmd_dashboard)(u, chat)
        return
    if intent == "energy":
        lvl = (f.get("level") or "").lower()
        if lvl in ("energetic", "normal", "tired", "exhausted"):
            await set_energy(u, chat, lvl)
        else:
            await cmd_dashboard(u, chat)
        return
    if intent == "tutor":
        await tutor_reply(u, chat, f.get("question") or t)
        return
    if intent == "quiz":
        await start_quiz(u, chat, f.get("topic") or f.get("subject"),
                         count=int(f.get("count") or 5))
        return
    if intent == "log_session":
        await candidate_log_session(
            u, chat, questions=clampi(f.get("questions"), 1, 999),
            correct=clampi(f.get("correct"), 0, 999),
            minutes=clampi(f.get("minutes"), 1, 960),
            subject=f.get("subject"), topic=f.get("topic"),
            day=f.get("day"), origin="ai", confidence=conf)
        return
    if intent == "hw_add":
        await candidate_hw(u, chat, f, conf)
        return
    if intent == "hw_progress":
        if f.get("done") is None:
            await send(chat, "How many did you do? e.g. 'did 25 of 50 DPP'")
            return
        pid = await new_pending(u, "hw_progress", f, "ai")
        await send(chat, card_text_hwprog(f), CONFIRM_KB(pid))
        return
    if intent == "test_add":
        await candidate_test_add(u, chat, f, conf)
        return
    if intent == "test_result":
        pid = await new_pending(u, "test_result",
                                {k: v for k, v in f.items() if v is not None},
                                "ai")
        lines = card_lines(f, [("name", "Test"), ("subject", "Subject"),
                               ("obtained", "Obtained"), ("total", "Total")])
        await send(chat, "I found:\n\n" + "\n".join(lines) +
                   f"\n\nConfidence: {conf.upper()}", CONFIRM_KB(pid))
        return
    if intent == "mistake_add":
        await candidate_mistake(u, chat, f, conf)
        return
    if intent == "syllabus":
        blocks = f.get("blocks") or []
        if not blocks:
            await cmd_syllabus(u, chat)
            return
        pid = await new_pending(u, "syllabus", {"blocks": blocks}, "ai")
        preview = "\n".join(
            f"• <b>{esc(b.get('subject'))}</b>: "
            + ", ".join(esc(x) for x in (b.get("topics") or [])[:10])
            for b in blocks[:10])
        await send(chat, "📚 <b>Syllabus</b>\n\n" + preview, CONFIRM_KB(pid))
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
    await q("UPDATE users SET energy=$2, energy_set_at=now() WHERE id=$1::uuid",
            u["id"], level)
    u["energy"] = level
    msg = {"exhausted": "Understood. Today: light recall and mistake review "
                        "only — I'll expect full effort again tomorrow. 🌙",
           "tired": "Got it — I'll keep today short and light.",
           "energetic": "Nice. I'll use that energy. 🔥",
           "normal": "Noted. Back to normal load."}[level]
    await send(chat, msg, IK([("▶️ Start Next", "nav:next")]))


async def tutor_reply(u, chat, question):
    ctx = await ai_context(u)
    prompt = (f"STUDENT CONTEXT:\n{ctx}\n\nAnswer this study question. "
              f"Be clear and concise (max 200 words), give one concrete "
              f"example, and one common mistake to avoid. Never invent "
              f"statistics about the student.\n\nQUESTION: {question}")
    key = "tu:" + hashlib.md5(prompt.encode()).hexdigest()
    try:
        cached = _cache_get(key)
        if cached is None:
            txt = await ai_call(prompt, json_mode=False, max_tokens=800)
            _cache_set(key, txt)
        else:
            txt = cached
        await send(chat, "📖 <b>" + esc(question[:80]) + "</b>\n\n" +
                   esc(txt), IK([("▶️ Back to studying", "nav:next")]))
    except AIError:
        weak = await qrows(
            """SELECT s.name subj, t.name top, m.mastery FROM mastery m
               JOIN topics t ON t.id=m.topic_id
               JOIN subjects s ON s.id=m.subject_id
               WHERE m.user_id=$1::uuid AND m.events>0
               ORDER BY m.mastery ASC LIMIT 3""", u["id"])
        lines = ["My tutor brain is rate-limited right now. Meanwhile, your "
                 "weakest topics:"]
        lines += [f"• {r['subj']}/{r['top']} — {r['mastery']:.0f}%"
                  for r in weak] or ["• log more sessions to build mastery data"]
        await send(chat, "\n".join(lines), MENU_KB)


async def candidate_log_session(u, chat, questions, correct, minutes,
                                subject=None, topic=None, day=None,
                                topic_maybe=None, origin="fast",
                                confidence="high"):
    subs = await get_subjects(u["id"])
    if not subs:
        await send(chat, "You have no subjects yet — send 'Physics: Rotation, "
                   "SHM' or a syllabus photo first.", MENU_KB)
        return
    if questions is not None and correct is not None and correct > questions:
        correct = None
    if not subject and topic_maybe and origin == "fast":
        for s in subs:
            if s["name"].lower() in topic_maybe.lower():
                subject = s["name"]
                break
    if not topic and topic_maybe:
        for word in re.split(r"\s+", topic_maybe):
            if len(word) > 3 and word.lower() not in (
                    "studied", "revised", "solved", "did", "questions",
                    "correct", "minutes", "hours", "for", "and", "with"):
                topic = topic_maybe  # let AI-free path keep the whole phrase
                break
    sid, sname = resolve_subject(subject, subs)
    fields = {"questions": questions, "correct": correct, "minutes": minutes,
              "subject": sname or subject, "subject_id": sid,
              "topic": topic, "day": day if day == "yesterday" else None}
    pid = await new_pending(u, "log_session", fields, origin)
    lines = ["I found:", ""]
    if questions:
        lines.append(f"Questions: <b>{questions}</b>")
        if correct is not None:
            lines.append(f"Correct: <b>{correct}</b> "
                         f"({100 * correct // questions}%)")
    else:
        lines.append("Questions: <b>not given</b>")
    if minutes:
        lines.append(f"Duration: <b>{fm(minutes)}</b>")
    lines.append(f"Subject: <b>{esc(sname or subject or '?')}</b>")
    if topic:
        lines.append(f"Topic: <b>{esc(topic)}</b>")
    if origin == "fast" and sid and confidence == "high":
        res = await do_log_session(u, fields)
        await q("UPDATE pending_actions SET status='confirmed' "
                "WHERE id=$1::uuid", pid)
        undo_pid = await new_pending(u, "undo_log",
                                     {"fields": fields}, "fast")
        await send(chat, res + "\n\n<i>(Tap ↩️ if this is wrong)</i>", IK(
            [("↩️ Undo", f"cfm:{undo_pid}:undo")]))
        return
    if not sid:
        await subject_pick_card(u, chat, pid, "log_session", fields,
                                "\n".join(lines))
        return
    conf_flag = " ⚠️" if confidence != "high" else ""
    await send(chat, "\n".join(lines) +
               f"\n\nConfidence: <b>{confidence.upper()}{conf_flag}</b>",
               CONFIRM_KB(pid))


async def candidate_hw(u, chat, f, conf):
    subs = await get_subjects(u["id"])
    sid, sname = resolve_subject(f.get("subject"), subs)
    fields = {"title": (f.get("title") or "Homework").strip()[:120],
              "subject": sname or f.get("subject"), "subject_id": sid,
              "questions": clampi(f.get("questions"), 1, 999),
              "minutes": clampi(f.get("minutes"), 5, 480),
              "due": f.get("due"), "hw_type": f.get("hw_type") or "custom"}
    if not f.get("title"):
        await send(chat, "What's it called? e.g. 'add DPP 4, 40 questions, "
                   "due friday'")
        return
    pid = await new_pending(u, "hw_add", fields, "ai")
    if not sid and subs:
        await subject_pick_card(u, chat, pid, "hw_add", fields,
                                f"📝 <b>{esc(fields['title'])}</b>")
        return
    lines = card_lines(fields, [("title", "Homework"), ("subject", "Subject"),
                                ("questions", "Questions"),
                                ("due", "Due"), ("minutes", "Est. minutes")])
    flag = " ⚠️" if conf != "high" else ""
    await send(chat, "I found:\n\n" + "\n".join(lines) +
               f"\n\nConfidence: <b>{conf.upper()}{flag}</b>", CONFIRM_KB(pid))


def card_text_hwprog(f):
    return (f"📝 <b>{esc(f.get('title'))}</b>\n"
            f"Done now: <b>{f.get('done')}</b> more questions")


async def candidate_test_add(u, chat, f, conf):
    subs = await get_subjects(u["id"])
    sid, sname = resolve_subject(f.get("subject"), subs)
    fields = {"name": (f.get("name") or "Test").strip()[:120],
              "subject": sname or f.get("subject"), "subject_id": sid,
              "date": f.get("date"),
              "total_marks": clampi(f.get("total_marks"), 1, 1000)}
    pid = await new_pending(u, "test_add", fields, "ai")
    if not sid and subs:
        await subject_pick_card(u, chat, pid, "test_add", fields,
                                f"🧪 <b>{esc(fields['name'])}</b>")
        return
    lines = card_lines(fields, [("name", "Test"), ("subject", "Subject"),
                                ("date", "When"), ("total_marks", "Marks")])
    flag = " ⚠️" if conf != "high" else ""
    await send(chat, "I found:\n\n" + "\n".join(lines) +
               f"\n\nConfidence: <b>{conf.upper()}{flag}</b>", CONFIRM_KB(pid))


async def candidate_mistake(u, chat, f, conf):
    subs = await get_subjects(u["id"])
    sid, sname = resolve_subject(f.get("subject"), subs)
    fields = {"subject": sname or f.get("subject"), "subject_id": sid,
              "topic": f.get("topic"),
              "count": clampi(f.get("count"), 1, 50) or 1,
              "mtype": (f.get("mtype") or "unknown").lower(),
              "description": (f.get("description") or "")[:200]}
    pid = await new_pending(u, "mistake_add", fields, "ai")
    if not sid and subs:
        await subject_pick_card(u, chat, pid, "mistake_add", fields,
                                f"🧨 <b>{fields['count']} × "
                                f"{esc(fields['mtype'])} mistake(s)</b>")
        return
    lines = card_lines(fields, [("count", "Count"), ("mtype", "Type"),
                                ("subject", "Subject"), ("topic", "Topic")])
    flag = " ⚠️" if conf != "high" else ""
    await send(chat, "I found:\n\n" + "\n".join(lines) +
               f"\n\nConfidence: <b>{conf.upper()}{flag}</b>", CONFIRM_KB(pid))

# ============================================================
# SESSION CONTROLS
# ============================================================
async def wizard_session_log(u, chat, pend, text):
    pid = str(pend["id"])
    if text.lower().startswith("/skip"):
        await q("UPDATE pending_actions SET status='confirmed' "
                "WHERE id=$1::uuid", pid)
        await send(chat, "Logged as time-only. 🏠", MENU_KB)
        return
    pq = parse_questions(text)
    if not pq or pq[1] is None:
        m = re.match(r"^(\d+)\s*$", text.strip())
        if m:
            pq = (int(m.group(1)), None, None)
    if pq and pq[1] is not None:
        payload = json.loads(pend["payload"])
        sess_id = payload.get("session_id")
        await q("UPDATE study_sessions SET questions_attempted=$2, "
                "questions_correct=$3 WHERE id=$1::uuid",
                sess_id, pq[0], pq[1])
        s = await qrow("SELECT * FROM study_sessions WHERE id=$1::uuid", sess_id)
        res = {}
        if s and s["topic_id"]:
            await recompute(u["id"], s["subject_id"], s["topic_id"])
            nr = await apply_revision(u["id"], s["subject_id"], s["topic_id"],
                                      pq[1] / pq[0], now_tz())
            m = await qrow("SELECT mastery, events FROM mastery "
                           "WHERE user_id=$1::uuid AND topic_id=$2::uuid",
                           u["id"], s["topic_id"])
            if m:
                res = {"mastery": m["mastery"], "events": m["events"],
                       "next_rev": nr}
        elif s and s["subject_id"]:
            await recompute(u["id"], s["subject_id"])
            m = await qrow("SELECT mastery FROM mastery WHERE user_id=$1::uuid "
                           "AND subject_id=$2::uuid AND topic_id IS NULL",
                           u["id"], s["subject_id"])
            if m:
                res = {"mastery": m["mastery"]}
        await q("UPDATE pending_actions SET status='confirmed' "
                "WHERE id=$1::uuid", pid)
        acc = 100 * pq[1] // pq[0]
        lines = [f"✅ Logged: {pq[0]} questions, {pq[1]} correct ({acc}%)."]
        if res.get("mastery") is not None:
            lines.append(f"🧠 Mastery: {res['mastery']:.0f}% "
                         f"({res.get('events', '')} sessions)".replace(" ()", ""))
        if res.get("next_rev"):
            lines.append(f"🔁 Next revision: "
                         f"{res['next_rev'].strftime('%d %b')}")
        await send(chat, "\n".join(lines), MENU_KB)
        return
    await send(chat, "Try: '25 questions 18 correct' — or /skip.")


async def session_ctl(u, chat, op):
    now = now_tz()
    s = await live_session(u["id"])
    if not s:
        await cmd_dashboard(u, chat)
        return
    if op == "pause":
        await q("UPDATE study_sessions SET status='paused', paused_at=$2 "
                "WHERE id=$1::uuid", str(s["id"]), now)
        await send(chat, "⏸ Paused.", IK([("▶️ Resume", "ses:resume")],
                                          [("❌ Abandon", "ses:abandon")]))
    elif op == "resume":
        paused = (s["paused_seconds"] or 0)
        if s["paused_at"]:
            paused += int((now - s["paused_at"]).total_seconds())
        await q("UPDATE study_sessions SET status='active', paused_at
