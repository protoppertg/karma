import json
import time
import hashlib
import asyncio
from google import genai
from google.genai import types
from config import (GEMINI_API_KEY,
                    GEMINI_MODEL, LOG)
from database import q, qrow, qrows, qval, jd
from tg import esc, send, IK
import services as S

AIC = None

_last_ai = 0.0
AI_CACHE = {}


class AIError(Exception):
    pass


def init_ai():
    global AIC
    AIC = genai.Client(
        api_key=GEMINI_API_KEY)


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


async def ai_call(contents,
                  json_mode=True,
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
                raise AIError(
                    "empty AI response")
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
                       (kind, status,
                        latency_ms, error)
                       VALUES('call',
                              $1, $2, $3)""",
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
        raise AIError(
            "no JSON in AI response")
    return json.loads(text[i:j + 1])


async def ai_vision(img_bytes, system,
                    mime="image/jpeg"):
    part = types.Part.from_bytes(
        data=img_bytes, mime_type=mime)
    return _load_json(
        await ai_call([system, part],
                      max_tokens=2000))


async def ai_transcribe(audio):
    part = types.Part.from_bytes(
        data=audio,
        mime_type="audio/ogg")
    text = await ai_call(
        ["Transcribe this audio to "
         "plain text. Output only "
         "the transcription.", part],
        json_mode=False,
        max_tokens=300)
    return text.strip()


import re  # noqa: E402 (used above)

# ---- memory system ----
async def get_memories(u):
    rows = await qrows(
        """SELECT id, content
           FROM ai_memory
           WHERE user_id=$1::uuid
           AND active=true
           ORDER BY created_at DESC
           LIMIT 20""",
        u["id"])
    return [dict(r) for r in rows]


async def memory_block(u):
    rows = await get_memories(u)
    if not rows:
        return ""
    out = []
    for r in rows:
        out.append("- "
                   + (r["content"]
                      or "")[:120])
    return ("THINGS TO REMEMBER "
            "ABOUT THIS STUDENT:\n"
            + "\n".join(out))


async def log_chat(u, role, content):
    try:
        await q(
            """INSERT INTO chat_history
               (user_id, role,
                content)
               VALUES($1::uuid,
                      $2,$3)""",
            u["id"], role,
            (content or "")[:800])
        await q(
            """DELETE FROM chat_history
               WHERE user_id=$1::uuid
               AND id NOT IN (
                 SELECT id
                 FROM chat_history
                 WHERE user_id=
                   $1::uuid
                 ORDER BY id DESC
                 LIMIT 60)""",
            u["id"])
    except Exception:
        pass


async def chat_context_block(u):
    rows = await qrows(
        """SELECT role, content
           FROM chat_history
           WHERE user_id=$1::uuid
           ORDER BY id DESC
           LIMIT 8""",
        u["id"])
    if not rows:
        return ""
    out = []
    for r in reversed(rows):
        if r["role"] == "user":
            who = "Student"
        else:
            who = "You"
        out.append(who + ": "
                   + (r["content"]
                      or "")[:200])
    return ("RECENT CONVERSATION "
            "(oldest first):\n"
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
                 LIMIT 30)""",
            u["id"])
    except Exception:
        pass
    return row["id"]


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
uses their name and context).

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
- "chat": {"reply":"short warm reply"}
- "none": {}

Rules: extract only stated facts; never
invent numbers; numbers are integers;
prefer the student's subject names from
context; use the student's memories and
recent conversation so you sound like
you truly know them; if unsure about a
field, set confidence "low"."""


async def ai_context(u):
    parts = []
    subs = await S.get_subjects(u["id"])
    if subs:
        names = ", ".join(
            s["name"] for s in subs)
        parts.append("subjects: " + names)
    if u.get("exam_goal"):
        parts.append(
            "goal: " + u["exam_goal"])
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
           LIMIT 3""", u["id"])
    if weak:
        bits = []
        for r in weak:
            m = format(
                r["mastery"], ".0f")
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
        """SELECT name FROM tests
           WHERE user_id=$1::uuid
           AND status='scheduled'
           AND test_at>now()
           ORDER BY test_at
           LIMIT 1""", u["id"])
    if nt:
        parts.append(
            "next test: " + nt["name"])
    parts.append(
        "student name: "
        + str(u.get("display_name")
              or u.get("first_name")
              or "student"))
    parts.append(
        "energy today: " + u["energy"])
    return "\n".join(parts)


async def ai_route(text, u):
    base = await ai_context(u)
    mem = await memory_block(u)
    hist = await chat_context_block(u)
    prompt = (ROUTER_SYS
              + "\n\nCONTEXT:\n"
              + base + "\n"
              + mem + "\n"
              + hist
              + "\n\nMESSAGE:\n"
              + text)
    key = "rt:" + hashlib.md5(
        prompt.encode()).hexdigest()
    cached = _cache_get(key)
    if cached is not None:
        return cached
    raw = _load_json(
        await ai_call(prompt,
                      max_tokens=800))
    _cache_set(key, raw)
    return raw
async def chat_reply(u, chat, text):
    """Real conversation when the router
    produced no reply. Uses memories,
    chat history and study context."""
    try:
        base = await ai_context(u)
        mem = await memory_block(u)
        hist = await chat_context_block(u)
        prompt = (
            "You are StudyOS — a warm, "
            "witty study companion who "
            "truly knows this student. "
            "Reply to their message like "
            "a smart friend (max 60 words). "
            "Use their name and the "
            "memories/context below. If "
            "they ask a study question, "
            "answer it helpfully. Never "
            "invent their statistics. "
            "Plain text only, no JSON.\n\n"
            "CONTEXT:\n" + base + "\n"
            + mem + "\n"
            + hist
            + "\n\nMESSAGE:\n" + text)
        txt = await ai_call(
            prompt, json_mode=False,
            max_tokens=300)
        txt = txt.strip()[:900]
        await log_chat(u, "ai", txt)
        await send(chat, esc(txt))
    except AIError:
        await send(chat,
                   "I'm here — my AI "
                   "brain is just rate-"
                   "limited for a minute. "
                   "🧠 Try again soon.")
