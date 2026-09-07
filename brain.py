"""
brain.py — Gemini client, AI router,
persistent cache, long-term memory
with rolling summaries, smart context
selection, chunked extraction for
large documents. Built to handle high
data volumes within free-tier limits.
"""

import re
import json
import time
import hashlib
import asyncio
from google import genai
from google.genai import types
from config import (GEMINI_API_KEY,
                    GEMINI_MODEL,
                    LOG)
from database import (q, qrow,
                      qrows, qval,
                      jd)
import services as S

AIC = None

# ---- tunables for high volume ----
AI_MIN_SPACING = 4.6
AI_CACHE_MEM_MAX = 2000
AI_CACHE_TTL = 6 * 3600
HISTORY_MAX = 300
HISTORY_RECENT = 6
HISTORY_RELEVANT = 6
MEMORY_MAX = 40
MEMORY_CONTEXT = 25
SUMMARY_TRIGGER = 150
SUMMARY_EVERY = 80
CHUNK_CHARS = 7000

_last_ai = 0.0
_cooldown_until = 0.0
_call_count = 0
AI_CACHE = {}
_cache_inited = False
_summary_inited = False
_summarizing = {}


class AIError(Exception):
    pass


def init_ai():
    global AIC
    AIC = genai.Client(
        api_key=GEMINI_API_KEY)


# ============================================================
# CACHE — memory hot layer + DB
# persistent layer (survives
# Render restarts)
# ============================================================
async def _ensure_cache_table():
    global _cache_inited
    if _cache_inited:
        return
    try:
        await q(
            """CREATE TABLE IF NOT EXISTS
               ai_cache (
                cache_key TEXT
                  PRIMARY KEY,
                payload JSONB
                  NOT NULL,
                created_at TIMESTAMPTZ
                  DEFAULT now())""")
        await q(
            """CREATE INDEX IF NOT EXISTS
               ix_aicache
               ON ai_cache
                 (created_at)""")
        await q(
            """DELETE FROM ai_cache
               WHERE created_at <
                 now() - interval
                   '2 days'""")
    except Exception:
        pass
    _cache_inited = True


def _cache_get(key):
    """Sync memory-only lookup
    (signature kept for views)."""
    hit = AI_CACHE.get(key)
    if hit and time.time() - hit[0] \
            < AI_CACHE_TTL:
        return hit[1]
    return None


def _cache_set(key, val):
    """Sync memory-only store
    (signature kept for views)."""
    if len(AI_CACHE) \
            >= AI_CACHE_MEM_MAX:
        items = sorted(
            AI_CACHE.items(),
            key=lambda kv:
                kv[1][0])
        drop = AI_CACHE_MEM_MAX // 4
        for k, _ in items[:drop]:
            AI_CACHE.pop(k, None)
    AI_CACHE[key] = (time.time(),
                     val)


async def _db_cache_get(key):
    hit = AI_CACHE.get(key)
    if hit and time.time() - hit[0] \
            < AI_CACHE_TTL:
        return hit[1]
    try:
        await _ensure_cache_table()
        row = await qrow(
            """SELECT payload
               FROM ai_cache
               WHERE cache_key=$1
               AND created_at >
                 now() - interval
                   '2 days'""",
            key)
        if row:
            val = jd(row["payload"],
                     None)
            if val is not None:
                AI_CACHE[key] = \
                    (time.time(), val)
                return val
    except Exception:
        pass
    return None


async def _db_cache_set(key, val):
    _cache_set(key, val)
    try:
        await _ensure_cache_table()
        await q(
            """INSERT INTO ai_cache
               (cache_key, payload)
               VALUES($1,$2::jsonb)
               ON CONFLICT(cache_key)
               DO UPDATE SET
                 payload=EXCLUDED
                   .payload,
                 created_at=now()""",
            key, json.dumps(val))
    except Exception:
        pass


# ============================================================
# RATE LIMITS + CALL
# ============================================================
async def _ai_space():
    global _last_ai
    cd = _cooldown_until \
        - time.time()
    if cd > 0:
        await asyncio.sleep(
            min(cd, 30))
    wait = (AI_MIN_SPACING
            - (time.monotonic()
               - _last_ai))
    if wait > 0:
        await asyncio.sleep(wait)
    _last_ai = time.monotonic()


async def ai_call(contents,
                  json_mode=True,
                  max_tokens=1200):
    global _cooldown_until
    global _call_count
    if AIC is None:
        raise AIError(
            "AI not initialised")
    for attempt in range(4):
        await _ai_space()
        t0 = time.monotonic()
        try:
            mime = ("application/json"
                    if json_mode
                    else "text/plain")
            cfg = types \
                .GenerateContentConfig(
                    response_mime_type=
                        mime,
                    max_output_tokens=
                        max_tokens,
                    temperature=0.2)

            def _do():
                return (AIC.models
                        .generate_content(
                            model=
                                GEMINI_MODEL,
                            contents=
                                contents,
                            config=cfg))

            resp = await asyncio \
                .to_thread(_do)
            text = resp.text
            if not text:
                raise AIError(
                    "empty AI response")
            _call_count += 1
            if _call_count % 50 == 0:
                try:
                    await q(
                        """DELETE FROM
                           ai_log
                           WHERE created_at
                             < now()
                             - interval
                               '7 days'""")
                except Exception:
                    pass
            return text
        except Exception as e:
            msg = str(e)
            low = msg.lower()
            quota = ("429" in msg
                     or "resource"
                     in low
                     or "quota" in low)
            auth = ("api key"
                    in low
                    or "401" in msg
                    or "permission"
                    in low
                    or "unauthenticated"
                    in low)
            try:
                await q(
                    """INSERT INTO ai_log
                       (kind, status,
                        latency_ms,
                        error)
                       VALUES('call',
                              $1,$2,$3)""",
                    "retry"
                    if attempt < 3
                    else "error",
                    int((time.monotonic()
                         - t0) * 1000),
                    msg[:400])
            except Exception:
                pass
            if auth:
                raise AIError(
                    "AI key invalid: "
                    + msg[:150])
            if quota:
                _cooldown_until = \
                    time.time() \
                    + 20 * (attempt + 1)
                continue
            if attempt >= 2:
                raise AIError(
                    msg[:300])
            await asyncio.sleep(1.2)
    raise AIError(
        "AI quota exhausted — "
        "try again soon")


def _load_json(text):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(
            r"^```[a-zA-Z]*\n?",
            "", text)
        text = text.rstrip("`") \
            .strip()
    i = text.find("{")
    j = text.rfind("}")
    if i == -1 or j == -1:
        raise AIError(
            "no JSON in AI response")
    return json.loads(
        text[i:j + 1])


async def ai_vision(img_bytes,
                    system,
                    mime="image/jpeg"):
    part = types.Part.from_bytes(
        data=img_bytes,
        mime_type=mime)
    return _load_json(
        await ai_call(
            [system, part],
            max_tokens=2000))


async def ai_transcribe(audio):
    part = types.Part.from_bytes(
        data=audio,
        mime_type="audio/ogg")
    text = await ai_call(
        ["Transcribe this audio "
         "to plain text. Output "
         "only the transcription.",
         part],
        json_mode=False,
        max_tokens=400)
    return text.strip()


# ============================================================
# CHUNKED EXTRACTION for large
# documents (WhatsApp exports,
# long PDFs) — splits, extracts
# per chunk, merges lists
# ============================================================
async def ai_json_chunked(
        system, text,
        merge_keys=("items",
                    "blocks"),
        max_chunks=6):
    text = text or ""
    if len(text) <= CHUNK_CHARS:
        return _load_json(
            await ai_call(
                system
                + "\n\nTEXT:\n"
                + text,
                max_tokens=1500))
    chunks = []
    step = CHUNK_CHARS - 500
    i = 0
    while i < len(text) \
            and len(chunks) \
            < max_chunks:
        chunks.append(
            text[i:i + CHUNK_CHARS])
        i += step
    merged = {}
    for idx, ch in \
            enumerate(chunks):
        sys2 = (system
                + "\nThis is PART "
                + str(idx + 1)
                + " of "
                + str(len(chunks))
                + " of a longer "
                  "document. "
                  "Extract only "
                  "from this part.")
        try:
            raw = _load_json(
                await ai_call(
                    sys2
                    + "\n\nTEXT:\n"
                    + ch,
                    max_tokens=1500))
        except AIError:
            continue
        raw = jd(raw, {})
        for k, v in raw.items():
            if isinstance(v, list) \
                    and k in merge_keys:
                merged.setdefault(
                    k, [])
                merged[k].extend(v)
            elif k not in merged \
                    or merged[k] \
                    in (None, "", {}):
                merged[k] = v
    seen = set()
    for k in merge_keys:
        lst = merged.get(k) or []
        deduped = []
        for item in lst:
            fp = json.dumps(
                item, sort_keys=True)
            if fp not in seen:
                seen.add(fp)
                deduped.append(item)
        merged[k] = deduped
    return merged


# ============================================================
# MEMORY SYSTEM
# ============================================================
async def get_memories(u):
    rows = await qrows(
        """SELECT id, content
           FROM ai_memory
           WHERE user_id=$1::uuid
           AND active=true
           ORDER BY created_at DESC
           LIMIT """ + str(
               MEMORY_CONTEXT),
        u["id"])
    return [dict(r)
            for r in rows]


async def memory_block(u):
    rows = await get_memories(u)
    if not rows:
        return ""
    out = []
    total = 0
    for r in rows:
        line = ("- "
                + (r["content"]
                   or "")[:140])
        total += len(line)
        if total > 2600:
            break
        out.append(line)
    if not out:
        return ""
    return ("THINGS TO REMEMBER "
            "ABOUT THIS STUDENT:\n"
            + "\n".join(out))


async def save_memory(u, content):
    content = (content
               or "").strip()[:300]
    if not content:
        return None
    row = await qrow(
        """INSERT INTO ai_memory
           (user_id, kind,
            content, source)
           VALUES($1::uuid,
                  'fact',$2,
                  'chat')
           RETURNING id""",
        u["id"], content)
    try:
        await q(
            """DELETE FROM ai_memory
               WHERE user_id=$1::uuid
               AND active=true
               AND id NOT IN (
                 SELECT id
                 FROM ai_memory
                 WHERE user_id=
                   $1::uuid
                 AND active=true
                 ORDER BY created_at
                   DESC
                 LIMIT """
            + str(MEMORY_MAX)
            + ")",
            u["id"])
    except Exception:
        pass
    return row["id"]


# ============================================================
# CHAT HISTORY + ROLLING
# SUMMARY (unbounded conversation
# memory in fixed tokens)
# ============================================================
async def log_chat(u, role,
                   content):
    try:
        await q(
            """INSERT INTO
               chat_history
               (user_id, role,
                content)
               VALUES($1::uuid,
                      $2,$3)""",
            u["id"], role,
            (content or "")[:1200])
        await q(
            """DELETE FROM
               chat_history
               WHERE user_id=
                 $1::uuid
               AND id NOT IN (
                 SELECT id
                 FROM chat_history
                 WHERE user_id=
                   $1::uuid
                 ORDER BY id DESC
                 LIMIT """
            + str(HISTORY_MAX)
            + ")",
            u["id"])
    except Exception:
        pass
    try:
        await maybe_summarize(u)
    except Exception:
        pass


async def _ensure_summary_table():
    global _summary_inited
    if _summary_inited:
        return
    try:
        await q(
            """CREATE TABLE IF NOT
               EXISTS conv_summary (
                user_id UUID
                  PRIMARY KEY
                  REFERENCES users(id)
                  ON DELETE CASCADE,
                summary TEXT
                  NOT NULL
                  DEFAULT '',
                msg_count INT
                  NOT NULL
                  DEFAULT 0,
                updated_at
                  TIMESTAMPTZ
                  DEFAULT now())""")
    except Exception:
        pass
    _summary_inited = True


async def maybe_summarize(u):
    """When history grows past
    SUMMARY_TRIGGER, compress the
    older portion into a durable
    summary, then trim it."""
    uid = str(u["id"])
    if _summarizing.get(uid):
        return
    _summarizing[uid] = True
    try:
        row = await qrow(
            """SELECT COUNT(*) n
               FROM chat_history
               WHERE user_id=
                 $1::uuid""",
            u["id"])
        n = int(row["n"] or 0) \
            if row else 0
        if n < SUMMARY_TRIGGER:
            return
        await _ensure_summary_table()
        srow = await qrow(
            """SELECT msg_count
               FROM conv_summary
               WHERE user_id=
                 $1::uuid""",
            u["id"])
        last_count = 0
        if srow:
            last_count = int(
                srow["msg_count"]
                or 0)
        if srow and n - last_count \
                < SUMMARY_EVERY:
            return
        rows = await qrows(
            """SELECT role, content
               FROM chat_history
               WHERE user_id=
                 $1::uuid
               ORDER BY id ASC""",
            u["id"])
        keep = 60
        if len(rows) <= keep:
            return
        older = rows[:-keep]
        prev = await qval(
            """SELECT summary
               FROM conv_summary
               WHERE user_id=
                 $1::uuid""",
            u["id"])
        prev = (prev
                or "")[:2000]
        convo = []
        for r in older:
            if r["role"] == "user":
                who = "Student"
            else:
                who = "You"
            convo.append(
                who + ": "
                + (r["content"]
                   or "")[:280])
        text = "\n".join(
            convo)[:7000]
        prompt = (
            "Summarize this study-"
            "assistant conversation "
            "into max 100 words of "
            "durable facts: what the "
            "student is working on, "
            "habits, moods, deadlines "
            "mentioned, decisions "
            "made. Plain text.\n"
            "PREVIOUS SUMMARY "
            "(extend it):\n"
            + prev
            + "\n\nCONVERSATION:\n"
            + text)
        txt = await ai_call(
            prompt,
            json_mode=False,
            max_tokens=300)
        txt = txt.strip()[:1200]
        await q(
            """INSERT INTO
               conv_summary
               (user_id, summary,
                msg_count)
               VALUES($1::uuid,
                      $2,$3)
               ON CONFLICT(user_id)
               DO UPDATE SET
                 summary=EXCLUDED
                   .summary,
                 msg_count=EXCLUDED
                   .msg_count,
                 updated_at=now()""",
            u["id"], txt, n)
        await q(
            """DELETE FROM
               chat_history
               WHERE user_id=
                 $1::uuid
               AND id NOT IN (
                 SELECT id
                 FROM chat_history
                 WHERE user_id=
                   $1::uuid
                 ORDER BY id DESC
                 LIMIT 60)""",
            u["id"])
    except AIError:
        pass
    except Exception:
        LOG.exception("summarize")
    finally:
        _summarizing[uid] = False


async def chat_context_block(
        u, focus=None):
    """Summary + last messages +
    keyword-relevant older ones."""
    summ = None
    try:
        await _ensure_summary_table()
        summ = await qval(
            """SELECT summary
               FROM conv_summary
               WHERE user_id=
                 $1::uuid""",
            u["id"])
    except Exception:
        summ = None
    rows = await qrows(
        """SELECT role, content
           FROM chat_history
           WHERE user_id=$1::uuid
           ORDER BY id DESC
           LIMIT 40""",
        u["id"])
    if not rows and not summ:
        return ""
    recent = list(reversed(
        rows[:HISTORY_RECENT]))
    older = rows[HISTORY_RECENT:]
    picked = []
    if focus and older:
        words = set(
            re.findall(
                r"[a-z]{4,}",
                focus.lower()))
        stop = {"this", "that",
                "with", "have",
                "what", "when",
                "today", "study",
                "should", "there",
                "about", "would"}
        words -= stop
        scored = []
        for r in older:
            c = (r["content"]
                 or "").lower()
            hits = sum(
                1 for w in words
                if w in c)
            if hits:
                scored.append(
                    (hits, r))
        scored.sort(
            key=lambda x: -x[0])
        picked = [r for _, r
                  in scored[
                      :HISTORY_RELEVANT]]
    out = []
    if summ:
        out.append(
            "SUMMARY OF EARLIER "
            "CONVERSATION:\n"
            + str(summ)[:900])
    for r in picked + recent:
        if r["role"] == "user":
            who = "Student"
        else:
            who = "You"
        out.append(
            who + ": "
            + (r["content"]
               or "")[:220])
    return ("RECENT CONVERSATION "
            "(oldest first):\n"
            + "\n".join(out))


# ============================================================
# ROUTER
# ============================================================
ROUTER_SYS = """You convert a
student's Telegram message into ONE JSON
action for a study app. Output ONLY JSON:
{"intent":"...","fields":{...},
 "confidence":"high|medium|low",
 "reply": short reply or null}

"reply" is REQUIRED for every intent
(except "none"): one warm, human sentence
(max 22 words) acknowledging what they
said. For "none" or pure chat, the reply
IS the full answer (max 80 words, warm,
uses their name, memories and context).

Intents and fields (null for unstated):
- "log_session": {"subject","topic",
  "questions","correct","wrong",
  "minutes","day"}
- "hw_add": {"title","subject",
  "questions","minutes","due"}
- "hw_progress": {"title","done"} —
  done is a number or "rest".
- "test_add": {"name","subject","date",
  "total_marks"}
- "test_result": {"name","subject",
  "obtained","total","topics":[
  {"topic":"...","correct":n,
   "total":m}],"weak_topics":["..."]}
- "mistake_add": {"subject","topic",
  "count","mtype"}
- "mistake_resolve": {"subject","topic"}
- "timetable": {"which":"school|"
  "coaching","days":[{"day":"
  "monday","start":"8:00",
  "end":"14:00"}]} — when the
  student describes their full
  timetable/period schedule.
- "class_add": {"title","subject",
  "days":"monday and wednesday",
  "time_range":"5-7"}
- "extra_class": {"title","date",
  "time_range":"5-7"}
- "class_cancel": {"which","date"} —
  which is "school","coaching" or
  "classes" (all recurring classes).
- "class_move": {"which","from","to"}
- "energy": {"level"}
- "quiz": {"topic","subject","count"}
- "tutor": {"question"}
- "remember": {"content":"..."} — the
  user asks you to remember a fact,
  habit or preference about them.
  Paraphrase tightly, max 12 words.
- "forget": {"content":"..."} — the
  user asks you to forget something.
- "query": {"view"} — plan, next,
  homework, tests, revision, analytics,
  mistakes, dashboard, syllabus,
  classes, dayreview, review, insights.
- "syllabus": {"blocks":[
  {"subject":"...",
   "chapters":[{"name":"...",
     "topics":["..."]}],
   "topics":["..."]}]}
- "chat": {"reply":"full reply"}
- "none": {}

Rules: extract only stated facts; never
invent numbers; numbers are integers;
prefer the student's subject names from
context; use the student's memories and
recent conversation so you sound like
you truly know them; if the message is
very long, classify its main request;
if unsure about a field, set confidence
"low"."""


async def ai_context(u):
    parts = []
    subs = await S.get_subjects(
        u["id"])
    if subs:
        names = ", ".join(
            s["name"]
            for s in subs[:10])
        parts.append("subjects: "
                     + names)
    if u.get("exam_goal"):
        parts.append(
            "goal: " + u["exam_goal"])
    if u.get("exam_date"):
        days = (u["exam_date"]
                - S.now().date()) \
            if hasattr(S, "now") \
            else None
    weak = await qrows(
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
    if weak:
        bits = []
        for r in weak:
            m = format(
                r["mastery"],
                ".0f")
            bits.append(
                r["subj"] + "/"
                + r["top"] + " "
                + m + "%")
        parts.append(
            "weakest topics: "
            + "; ".join(bits))
    due_rev = await qval(
        """SELECT COUNT(*)
           FROM revisions
           WHERE user_id=$1::uuid
           AND due_at<now()""",
        u["id"])
    if due_rev:
        parts.append(
            str(due_rev)
            + " revisions due")
    over = await qval(
        """SELECT COUNT(*)
           FROM homework
           WHERE user_id=$1::uuid
           AND status!='completed'
           AND due_at<now()""",
        u["id"])
    if over:
        parts.append(
            str(over)
            + " homework overdue")
    nt = await qrow(
        """SELECT name
           FROM tests
           WHERE user_id=$1::uuid
           AND status='scheduled'
           AND test_at>now()
           ORDER BY test_at
           LIMIT 1""", u["id"])
    if nt:
        parts.append(
            "next test: "
            + nt["name"])
    parts.append(
        "student name: "
        + str(u.get(
            "display_name")
            or u.get(
                "first_name")
            or "student"))
    parts.append(
        "energy today: "
        + u["energy"])
    return "\n".join(parts)


async def ai_route(text, u):
    text = text or ""
    base = await ai_context(u)
    mem = await memory_block(u)
    hist = await \
        chat_context_block(
            u, focus=text)
    shown = text[:2500]
    prompt = (ROUTER_SYS
              + "\n\nCONTEXT:\n"
              + base + "\n"
              + mem + "\n"
              + hist
              + "\n\nMESSAGE:\n"
              + shown)
    key = ("rt:"
           + hashlib.md5(
               prompt.encode()
           ).hexdigest())
    cached = await \
        _db_cache_get(key)
    if cached is not None:
        return cached
    raw = _load_json(
        await ai_call(
            prompt,
            max_tokens=900))
    raw = jd(raw, {})
    await _db_cache_set(key, raw)
    return raw
