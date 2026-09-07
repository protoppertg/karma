import io
import json
from config import LOG
from database import q, qrow, qval, jd
from tg import (send, esc, IK,
                get_file_bytes)
import services as S
from services import (
    get_subjects,
    resolve_subject,
    upsert_subject,
    get_pending,
    set_pending_payload,
    confirm_kb)
from actions import refresh_plan
from views import MENU_KB
from parsers import (parse_any_date,
                     parse_days,
                     parse_time,
                     clean_wa_text,
                     clampi)
from brain import (AIError,
                   ai_call,
                   ai_vision,
                   _load_json)
from tg import today_d
from datetime import datetime
from datetime import time as dtime
from config import TZ

PHOTO_SYS = """You analyze a
photo for a study app. Read ONLY what
is clearly visible. Never invent
content. Return ONLY JSON:
{"type":"syllabus|test|homework|
 timetable|notes|unknown",
 "text":"all readable text (or empty)",
 "syllabus":[{"subject":"...",
   "chapters":[{"name":"...",
     "topics":["..."]}],
   "topics":["..."]}],
 "test":{"name":"...","subject":"...",
   "total":null,"obtained":null},
 "homework":{"title":"...",
   "subject":"...","questions":null},
 "timetable_data":null,
 "confidence":"high|medium|low"}
Use "syllabus" when the image lists
chapters per subject. Use "test" for
papers with visible marks. Use
"timetable" for class/period schedules
and fill "timetable_data":
{"school_or_coaching":"school",
 "days":[{"day":"monday",
   "start":"8:00","end":"14:00"}],
 "detail":"periods if visible"}.
Use "notes" otherwise."""

PDF_SYS = """Extract subjects,
chapters and topics from this
syllabus text. Return ONLY JSON:
{"blocks":[{"subject":"...",
  "chapters":[{"name":"...",
    "topics":["..."]}],
  "topics":["..."]}]}
Only include content actually present
in the text. Never invent entries."""

WA_SYS = """This is a WhatsApp chat
export from a student's study/coaching
group. Find homework/assignments given
by teachers. Return ONLY JSON:
{"items":[{"title":"...",
  "subject":"...",
  "chapter":null,
  "questions":null,
  "due":"natural words or null",
  "source":"teacher/coach name"}],
 "confidence":"high|medium|low"}
Rules: only real homework present in
the text; never invent; questions is
an integer if a count/range is stated;
due as natural words like "tomorrow"
or "friday" or null."""


async def handle_photo(u, chat, msg):
    photo = msg["photo"][-1]
    try:
        img = await get_file_bytes(
            photo["file_id"])
    except Exception as e:
        LOG.warning("photo dl: %s", e)
        await send(
            chat,
            "Couldn't download that "
            "photo — try again.")
        return
    await send(chat,
               "🔍 Reading image…")
    try:
        raw = await ai_vision(
            img, PHOTO_SYS)
    except AIError:
        await send(
            chat,
            "My vision service is "
            "rate-limited — try "
            "again in a minute.",
            MENU_KB)
        return
    raw = jd(raw, {})
    ptype = (raw.get("type")
             or "unknown").lower()
    conf = (raw.get("confidence")
            or "low").lower()
    text = (raw.get("text")
            or "")[:2500]
    if ptype == "unknown" \
            or conf == "low":
        await send(
            chat,
            "I can't read this "
            "confidently enough "
            "to save anything. "
            "Could you type the "
            "key parts?", MENU_KB)
        return
    if ptype == "timetable" \
            and raw.get(
                "timetable_data"):
        td = jd(raw.get(
            "timetable_data"), {})
        code = await S.new_pending(
            u, "timetable_import",
            {"td": td}, "ai")
        lines = ["📷 <b>TIMETABLE "
                 "detected</b>",
                 ""]
        which = td.get(
            "school_or_coaching"
        ) or "school"
        lines.append("For: <b>"
                     + esc(which)
                     + "</b>")
        for d in (td.get("days")
                  or [])[:8]:
            d = jd(d, {})
            day = d.get("day") or ""
            st = d.get("start") or "?"
            en = d.get("end") or "?"
            lines.append(
                "• " + esc(day)
                + ": "
                + esc(st) + "–"
                + esc(en))
        det = td.get("detail")
        if det:
            lines.append("")
            lines.append("<i>"
                         + esc(
                             str(det)
                             [:300])
                         + "</i>")
        lines += ["",
                  "Save this "
                  "schedule?"]
        await send(
            chat,
            "\n".join(lines),
            confirm_kb(code))
        return
    if ptype == "syllabus" \
            and raw.get("syllabus"):
        blocks = raw["syllabus"][:12]
        code = await S.new_pending(
            u, "syllabus",
            {"blocks": blocks},
            "ai")
        await send(
            chat,
            "📷 <b>Syllabus "
            "detected</b> — review "
            "then confirm:",
            confirm_kb(code))
        return
    if ptype == "test" \
            and raw.get("test"):
        t = jd(raw["test"], {})
        f = {k: v
             for k, v in
             t.items()
             if v is not None}
        code = await S.new_pending(
            u, "test_result", f,
            "ai")
        order = [("name", "Test"),
                 ("subject",
                  "Subject"),
                 ("total", "Total"),
                 ("obtained",
                  "Obtained")]
        from services import \
            card_lines
        lines = card_lines(
            f, order)
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
        h = jd(raw["homework"],
               {})
        f = {k: v
             for k, v in
             h.items()
             if v is not None}
        subs = await get_subjects(
            u["id"])
        sid, sname = \
            resolve_subject(
                f.get("subject"),
                subs)
        f["subject_id"] = sid
        if sname:
            f["subject"] = sname
        code = await S.new_pending(
            u, "hw_add", f, "ai")
        if not sid and subs:
            header = (
                "📷 <b>Homework "
                "detected</b>\n"
                + esc(f.get(
                    "title")))
            await S.subject_pick_card(
                u, chat, code,
                header)
            return
        order = [("title",
                  "Homework"),
                 ("subject",
                  "Subject"),
                 ("questions",
                  "Questions")]
        from services import \
            card_lines
        lines = card_lines(
            f, order)
        await send(
            chat,
            "📷 <b>Homework "
            "detected</b>\n\n"
            + "\n".join(lines)
            + "\n\nConfidence: "
            + conf.upper(),
            confirm_kb(code))
        return
    code = await S.new_pending(
        u, "save_note",
        {"title": "Photo note",
         "content": text},
        "ai")
    await send(
        chat,
        "📷 I read this as "
        "notes:\n\n<i>"
        + esc(text[:800])
        + "</i>\n\nConfidence: "
        + conf.upper(),
        confirm_kb(code))


async def handle_document(
        u, chat, msg):
    doc = msg.get("document") or {}
    name = (doc.get("file_name")
            or "").lower()
    try:
        data = await get_file_bytes(
            doc["file_id"])
    except Exception as e:
        LOG.warning("doc dl: %s", e)
        await send(chat,
                   "Couldn't download "
                   "that file.")
        return
    if name.endswith(".txt") \
            or "whatsapp" in name:
        try:
            text = data.decode(
                "utf-8",
                errors="ignore")
        except Exception:
            text = ""
        await handle_wa_import(
            u, chat, text)
        return
    if name.endswith(".zip"):
        await send(
            chat,
            "📥 Please export the "
            "chat <b>without "
            "media</b> — that "
            "gives a .txt file I "
            "can read.",
            MENU_KB)
        return
    if not name.endswith(".pdf"):
        await send(
            chat,
            "📄 I can read PDFs and "
            "WhatsApp .txt exports. "
            "For photos, send them "
            "as images.",
            MENU_KB)
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
                p.extract_text()
                or "")
        text = "\n".join(chunks)
    except Exception as e:
        LOG.warning("pdf parse: %s",
                    e)
        await send(
            chat,
            "Couldn't read that "
            "PDF (scanned?) — "
            "send photos of the "
            "pages instead.",
            MENU_KB)
        return
    text = text.strip()[:15000]
    if len(text) < 80:
        await send(
            chat,
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
        await send(
            chat,
            "My AI brain is "
            "rate-limited — try "
            "again in a minute.",
            MENU_KB)
        return
    raw = jd(raw, {})
    blocks = raw.get("blocks") or []
    if blocks:
        blocks = blocks[:12]
        code = await S.new_pending(
            u, "syllabus",
            {"blocks": blocks},
            "pdf")
        await send(
            chat,
            "📄 <b>Syllabus found "
            "in PDF</b> — review "
            "then confirm:",
            confirm_kb(code))
        return
    code = await S.new_pending(
        u, "save_note",
        {"title": "PDF note",
         "content":
             text[:4000]},
        "pdf")
    await send(
        chat,
        "📄 No subject/topic "
        "structure found — "
        "save the text as a "
        "note instead?",
        confirm_kb(code))


# ---- WhatsApp import ----
async def handle_wa_import(
        u, chat, text):
    cleaned = clean_wa_text(text)
    if len(cleaned) < 60:
        await send(
            chat,
            "This export looks "
            "empty — export the "
            "chat <b>without "
            "media</b> (.txt).",
            MENU_KB)
        return
    await send(chat,
               "📥 Scanning chat for "
               "homework…")
    try:
        prompt = (WA_SYS
                  + "\n\nCHAT:\n"
                  + cleaned[:9000])
        raw = _load_json(
            await ai_call(
                prompt,
                max_tokens=1200))
    except AIError:
        await send(
            chat,
            "My AI brain is "
            "rate-limited — try "
            "again in a minute.",
            MENU_KB)
        return
    raw = jd(raw, {})
    items = jd(raw.get("items"),
               [])
    conf = (raw.get("confidence")
            or "medium").lower()
    if not items:
        await send(
            chat,
            "No homework found in "
            "that chat. 🎉",
            MENU_KB)
        return
    items = items[:10]
    lines = ["📥 <b>HOMEWORK FOUND"
             "</b>", ""]
    for i, it in enumerate(
            items, 1):
        it = jd(it, {})
        lines.append(
            str(i) + ". "
            + esc(it.get("title")
                  or "Homework"))
        if it.get("questions"):
            lines[-1] += (" — "
                          + str(it[
                              "questions"
                          ])
                          + " questions")
        if it.get("due"):
            lines[-1] += (" — due "
                          + esc(it[
                              "due"]))
    lines.append("")
    lines.append("Confidence: "
                 + conf.upper())
    code = await S.new_pending(
        u, "wa_import",
        {"items": items}, "wa")
    await send(
        chat, "\n".join(lines),
        IK([("✅ ADD ALL",
             "waa:" + code)],
           [("✏️ Review one by one",
             "war:" + code)],
           [("❌ Cancel",
             "cfm:" + code + ":no")]))


async def wa_add_item(u, it):
    subs = await get_subjects(
        u["id"])
    sid, sname = resolve_subject(
        it.get("subject"), subs)
    if not sid:
        sname = (it.get("subject")
                 or "").strip().title()
        if sname:
            s = await upsert_subject(
                u["id"], sname)
            sid = s["id"]
            sname = s["name"]
    due = None
    if it.get("due"):
        d = parse_any_date(
            str(it["due"]),
            today_d())
        if d:
            due = datetime.combine(
                d, dtime(23, 59),
                tzinfo=TZ)
    tq = it.get("questions")
    if tq:
        tq = clampi(tq, 1, 999)
        est = max(15,
                  min(60, 2 * tq))
    else:
        est = 30
    await q(
        """INSERT INTO homework
           (user_id, subject_id,
            title, hw_type,
            total_q, est_minutes,
            due_at)
           VALUES($1::uuid,
                  $2::uuid,
                  $3,'imported',
                  $4,$5,$6)""",
        u["id"], sid,
        (it.get("title")
         or "Homework"
         ).strip()[:120],
        tq, est, due)


async def wa_add_all(u, chat, code):
    pend = await get_pending(u, code)
    if not pend \
            or pend["status"] \
            != "pending":
        return
    payload = jd(pend["payload"],
                 {})
    items = payload.get("items") \
        or []
    added = 0
    for it in items[:10]:
        it = jd(it, {})
        await wa_add_item(u, it)
        added += 1
    await q(
        """UPDATE pending_actions
           SET status='confirmed'
           WHERE code=$1""",
        code)
    await send(
        chat,
        "✅ Added " + str(added)
        + " homework item(s) "
        "from WhatsApp.",
        IK([("▶️ Start Next",
             "nav:next")]))
    await refresh_plan(
        u, chat, "WhatsApp import")


async def wa_review(u, chat, code):
    pend = await get_pending(u, code)
    if not pend \
            or pend["status"] \
            != "pending":
        return
    payload = jd(pend["payload"],
                 {})
    items = payload.get("items") \
        or []
    if not items:
        await send(chat,
                   "All items "
                   "processed.",
                   MENU_KB)
        return
    it = jd(items[0], {})
    lines = ["📥 <b>Item 1/"
             + str(len(items))
             + "</b>", "",
             "Title: <b>"
             + esc(it.get("title")
                   or "Homework")
             + "</b>"]
    if it.get("subject"):
        lines.append("Subject: "
                     + esc(it[
                         "subject"]))
    if it.get("questions"):
        lines.append("Questions: "
                     + str(it[
                         "questions"]))
    if it.get("due"):
        lines.append("Due: "
                     + esc(it["due"]))
    yes = "wa:" + code + ":0:yes"
    no = "wa:" + code + ":0:no"
    stop = "wa:" + code + ":0:stop"
    await send(chat,
               "\n".join(lines),
               IK([("✅ Add", yes),
                   ("⏭️ Skip", no)],
                  [("🛑 Stop",
                    stop)]))


async def wa_item_cb(
        u, chat, code, idx, ans):
    pend = await get_pending(u, code)
    if not pend \
            or pend["status"] \
            != "pending":
        return
    payload = jd(pend["payload"],
                 {})
    items = payload.get("items") \
        or []
    if idx >= len(items):
        return
    it = jd(items[idx], {})
    if ans == "yes":
        await wa_add_item(u, it)
    if ans == "stop":
        await q(
            """UPDATE pending_actions
               SET status='confirmed'
               WHERE code=$1""",
            code)
        await send(chat,
                   "Stopped. Added "
                   "items so far are "
                   "saved.",
                   MENU_KB)
        await refresh_plan(
            u, chat, "WhatsApp import")
        return
    rest = items[idx + 1:]
    payload["items"] = rest
    await set_pending_payload(
        code, payload)
    if not rest:
        await q(
            """UPDATE pending_actions
               SET status='confirmed'
               WHERE code=$1""",
            code)
        await send(chat,
                   "✅ All items "
                   "processed.",
                   MENU_KB)
        await refresh_plan(
            u, chat, "WhatsApp import")
        return
    await wa_review(u, chat, code)
