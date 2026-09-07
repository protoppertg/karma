import re
from datetime import timedelta
from datetime import time as dtime
from config import LOG
from database import (q, qrow, qval,
                      jd)
from tg import (send, edit, IK, esc,
                answer_cb, fm, now_tz,
                today_d,
                get_file_bytes)
import services as S
from services import (
    get_subjects,
    resolve_subject,
    live_session,
    live_minutes,
    spent_today,
    cleanup_stale,
    load_day_mods,
    get_classes,
    get_extras,
    new_pending,
    get_pending,
    set_pending_payload,
    confirm_kb,
    card_lines,
    build_candidates,
    recompute,
    apply_revision)
from engines import (
    day_minutes,
    choose_next,
    score_task)
from parsers import (
    parse_time,
    parse_class,
    parse_days,
    parse_any_date,
    parse_duration,
    parse_questions,
    detect_energy,
    clampi)
from brain import (ai_route, AIError,
                   log_chat, ai_call,
                   ai_context,
                   memory_block,
                   chat_context_block,
                   ai_transcribe)
import actions as A
import views as V
import media as M
from views import (
    MENU_KB, cmd_start,
    cmd_help, cmd_dashboard,
    cmd_plan, cmd_homework,
    cmd_tests, cmd_revision,
    cmd_mistakes, cmd_classes,
    cmd_syllabus,
    cmd_analytics, cmd_notes,
    cmd_settings, cmd_review,
    cmd_dayreview,
    cmd_doctor, cmd_export,
    cmd_insights,
    cmd_memories,
    do_remember, do_forget,
    start_quiz, quiz_answer,
    ob_start, ob_handle,
    morning_card)


async def lazy_tick(u, now,
                    chat=None):
    today = now.date()
    if u.get("last_tick") == today:
        await cleanup_stale(u, now)
        return u
    await q(
        """UPDATE pending_actions
           SET status='cancelled'
           WHERE user_id=$1::uuid
           AND status='pending'
           AND created_at <
             now() - interval
               '24 hours'""",
        u["id"])
    await q(
        """UPDATE pending_actions
           SET status='cancelled'
           WHERE user_id=$1::uuid
           AND status='pending'
           AND kind='session_log'
           AND created_at <
             now() - interval
               '2 hours'""",
        u["id"])
    await q(
        """DELETE FROM update_inbox
           WHERE created_at <
             now() - interval
               '2 days'""")
    await q(
        """DELETE FROM mastery_log
           WHERE recorded_at <
             now() - interval
               '90 days'""")
    es = u.get("energy_set_at")
    if (u["energy"] != "normal" and es
            and (now - es)
            .total_seconds()
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
           AND due_at < now()""",
        u["id"])
    await q(
        """UPDATE users
           SET last_tick=$2
           WHERE id=$1::uuid""",
        u["id"], today)
    u["last_tick"] = today
    await cleanup_stale(u, now)
    if chat and u["onboarded"]:
        try:
            await morning_card(
                u, chat)
        except Exception:
            LOG.exception("morning card")
    return u


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
            "Understood. Today: "
            "light recall and "
            "mistake review only "
            "— full effort again "
            "tomorrow. 🌙",
        "tired":
            "Got it — I'll keep "
            "today short and "
            "light.",
        "energetic":
            "Nice. I'll use that "
            "energy. 🔥",
        "normal":
            "Noted. Back to "
            "normal load."}
    await send(
        chat, msgs[level],
        IK([("▶️ Start Next",
             "nav:next")]))


async def chat_reply(u, chat, text):
    """Real conversation when the
    router produced no reply. Uses
    memories, history and context."""
    try:
        base = await ai_context(u)
        mem = await memory_block(u)
        hist = await chat_context_block(u)
        prompt = (
            "You are StudyOS — a warm, "
            "witty study companion who "
            "truly knows this student. "
            "Reply like a smart friend "
            "(max 60 words). Use their "
            "name and the memories and "
            "context below. If they ask "
            "a study question, answer "
            "it helpfully. Never invent "
            "their statistics. Plain "
            "text only, no JSON.\n\n"
            "CONTEXT:\n" + base + "\n"
            + mem + "\n" + hist
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


BANNED = ("cancel", "move",
          "moved", "holiday",
          "off", "skip",
          "tomorrow", "test",
          "homework", "hw",
          "quiz", "class")


async def try_settings(
        u, chat, t, low):
    if not u["onboarded"]:
        return False
    m = re.search(
        r"call me ([a-zA-Z ]{2,30})",
        low)
    if m:
        nm = m.group(1).strip() \
            .title()[:40]
        await q(
            """UPDATE users
               SET display_name=$2
               WHERE id=$1::uuid""",
            u["id"], nm)
        await send(
            chat,
            "👤 Got it, "
            + esc(nm) + ".",
            MENU_KB)
        return True
    m = re.search(
        r"\bwake(?:\s*up)?"
        r"\s*(?:at|by)?\s*"
        r"(\d{1,2})"
        r"(?::(\d{2}))?"
        r"\s*(am|pm)?\b", low)
    if m:
        raw = m.group(1) + ":"
        raw += (m.group(2)
                or "00")
        raw += (m.group(3) or "")
        tm = parse_time(raw)
        if tm:
            await q(
                """UPDATE users
                   SET wake_time=$2
                   WHERE id=$1::uuid""",
                u["id"], tm)
            await send(
                chat,
                "🌅 Wake time set to "
                + tm.strftime("%H:%M")
                + ".",
                MENU_KB)
            return True
    m = re.search(
        r"\bsleep(?:ing)?"
        r"\s*(?:at|by)?\s*"
        r"(\d{1,2})"
        r"(?::(\d{2}))?"
        r"\s*(am|pm)?\b", low)
    if m:
        h = int(m.group(1))
        ap = m.group(3)
        mi = m.group(2) or "00"
        if not ap and 1 <= h <= 11:
            tm = dtime(h + 12,
                       int(mi))
        else:
            raw = m.group(1) + ":"
            raw += mi
            raw += (ap or "")
            tm = parse_time(raw)
        if tm:
            await q(
                """UPDATE users
                   SET sleep_time=$2
                   WHERE id=$1::uuid""",
                u["id"], tm)
            await send(
                chat,
                "🌙 Sleep time set to "
                + tm.strftime("%H:%M")
                + " (assumed pm).",
                MENU_KB)
            return True
    m = re.search(
        r"exam(?:\s+date)?"
        r"\s+(?:is|on)\s+(.+)",
        low)
    if m:
        d = parse_any_date(
            m.group(1).strip(),
            today_d())
        if d:
            await q(
                """UPDATE users
                   SET exam_date=$2
                   WHERE id=$1::uuid""",
                u["id"], d)
            await send(
                chat,
                "🎯 Exam date set: "
                + d.strftime(
                    "%a %d %b %Y")
                + " ("
                + str((d
                       - today_d())
                      .days)
                + " days away).",
                MENU_KB)
            return True
    if ("school" in low
            or "coaching" in low) \
            and not any(
                b in low
                for b in BANNED):
        pc = parse_class(t)
        if pc:
            if "coaching" in low:
                c0 = "coaching_start"
                c1 = "coaching_end"
                c2 = "coaching_days"
                word = "Coaching"
            else:
                c0 = "school_start"
                c1 = "school_end"
                c2 = "school_days"
                word = "School"
            await q(
                """UPDATE users SET
                   """ + c0
                + "=$2, "
                + c1
                + "=$3, "
                + c2
                + """=$4
                   WHERE id=$1::uuid""",
                u["id"], pc[0],
                pc[1], pc[2])
            names = ["Mon", "Tue",
                     "Wed", "Thu",
                     "Fri", "Sat",
                     "Sun"]
            days = ", ".join(
                names[d]
                for d in pc[2])
            await send(
                chat,
                "🗓 " + word
                + " set: "
                + pc[0].strftime(
                    "%H:%M")
                + "–"
                + pc[1].strftime(
                    "%H:%M")
                + " (" + days
                + "). Capacity "
                "recalculates.",
                MENU_KB)
            return True
    m = re.search(
        r"\bmeals?\b[^0-9]{0,20}"
        r"(\d{2,3})\b", low)
    if m:
        v = clampi(m.group(1),
                   0, 300)
        await q(
            """UPDATE users
               SET meal_minutes=$2
               WHERE id=$1::uuid""",
            u["id"], v)
        await send(chat,
                   "🍱 Meals: "
                   + str(v)
                   + " minutes.",
                   MENU_KB)
        return True
    m = re.search(
        r"\bcommute\b[^0-9]{0,20}"
        r"(\d{2,3})\b", low)
    if m:
        v = clampi(m.group(1),
                   0, 300)
        await q(
            """UPDATE users
               SET commute_minutes
                   =$2
               WHERE id=$1::uuid""",
            u["id"], v)
        await send(chat,
                   "🚌 Commute: "
                   + str(v)
                   + " minutes.",
                   MENU_KB)
        return True
    return False


async def candidate_log_session(
        u, chat, questions,
        correct, minutes,
        subject=None, topic=None,
        day=None,
        topic_maybe=None,
        origin="fast",
        confidence="high"):
    subs = await get_subjects(
        u["id"])
    if not subs:
        await send(
            chat,
            "You have no subjects "
            "yet — send 'Physics: "
            "Rotation, SHM' or a "
            "syllabus photo first.",
            MENU_KB)
        return
    if (questions is not None
            and correct
            is not None
            and correct
            > questions):
        correct = None
    if not subject \
            and topic_maybe:
        for s in subs:
            if s["name"].lower() \
                    in topic_maybe \
                    .lower():
                subject = s["name"]
                break
    sid, sname = resolve_subject(
        subject, subs)
    day_v = (day
             if day == "yesterday"
             else None)
    fields = {
        "questions": questions,
        "correct": correct,
        "minutes": minutes,
        "subject":
            sname or subject,
        "subject_id": sid,
        "topic": topic,
        "day": day_v}
    code = await new_pending(
        u, "log_session",
        fields, origin)
    lines = ["I found:", ""]
    if questions:
        lines.append(
            "Questions: <b>"
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
        lines.append(
            "Duration: <b>"
            + fm(minutes)
            + "</b>")
    subj_label = (sname
                  or subject
                  or "?")
    lines.append(
        "Subject: <b>"
        + esc(subj_label)
        + "</b>")
    if topic:
        lines.append(
            "Topic: <b>"
            + esc(topic) + "</b>")
    if (origin == "fast"
            and sid
            and confidence
            == "high"):
        res = await A.do_log_session(
            u, fields)
        await q(
            """UPDATE
               pending_actions
               SET status=
                 'confirmed'
               WHERE code=$1""",
            code)
        ucode = await new_pending(
            u, "undo_log",
            {"fields": fields},
            "fast")
        undo = ("cfm:" + ucode
                + ":undo")
        await send(
            chat,
            res + "\n\n<i>(Tap ↩️ "
            "if wrong)</i>",
            IK([("↩️ Undo",
                 undo)]))
        return
    if not sid:
        await S.subject_pick_card(
            u, chat, code,
            "\n".join(lines))
        return
    flag = ""
    if confidence != "high":
        flag = " ⚠️"
    await send(
        chat,
        "\n".join(lines)
        + "\n\nConfidence: <b>"
        + confidence.upper()
        + flag + "</b>",
        confirm_kb(code))


async def candidate_hw(
        u, chat, f, conf):
    subs = await get_subjects(
        u["id"])
    sid, sname = resolve_subject(
        f.get("subject"), subs)
    title = (f.get("title")
             or "").strip()[:120]
    if not title:
        await send(
            chat,
            "What's it called? "
            "e.g. 'add DPP 4, 40 "
            "questions, due "
            "friday'")
        return
    fields = {
        "title": title,
        "subject":
            sname
            or f.get("subject"),
        "subject_id": sid,
        "questions": clampi(
            f.get("questions"),
            1, 999),
        "minutes": clampi(
            f.get("minutes"),
            5, 480),
        "due": f.get("due"),
        "hw_type":
            f.get("hw_type")
            or "custom"}
    code = await new_pending(
        u, "hw_add", fields, "ai")
    if not sid and subs:
        await S.subject_pick_card(
            u, chat, code,
            "📝 <b>"
            + esc(title)
            + "</b>")
        return
    order = [("title",
              "Homework"),
             ("subject",
              "Subject"),
             ("questions",
              "Questions"),
             ("due", "Due")]
    lines = card_lines(
        fields, order)
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


async def candidate_test_add(
        u, chat, f, conf):
    subs = await get_subjects(
        u["id"])
    sid, sname = resolve_subject(
        f.get("subject"), subs)
    name = (f.get("name")
            or "Test"
            ).strip()[:120]
    fields = {
        "name": name,
        "subject":
            sname
            or f.get("subject"),
        "subject_id": sid,
        "date": f.get("date"),
        "total_marks": clampi(
            f.get("total_marks"),
            1, 1000)}
    code = await new_pending(
        u, "test_add", fields, "ai")
    if not sid and subs:
        await S.subject_pick_card(
            u, chat, code,
            "🧪 <b>"
            + esc(name)
            + "</b>")
        return
    order = [("name", "Test"),
             ("subject",
              "Subject"),
             ("date", "When"),
             ("total_marks",
              "Marks")]
    lines = card_lines(
        fields, order)
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


async def candidate_mistake(
        u, chat, f, conf):
    subs = await get_subjects(
        u["id"])
    sid, sname = resolve_subject(
        f.get("subject"), subs)
    mtype = (f.get("mtype")
             or "unknown"
             ).lower()
    fields = {
        "subject":
            sname
            or f.get("subject"),
        "subject_id": sid,
        "topic":
            f.get("topic"),
        "count": clampi(
            f.get("count"),
            1, 50) or 1,
        "mtype": mtype,
        "description":
            (f.get("description")
             or "")[:200]}
    code = await new_pending(
        u, "mistake_add",
        fields, "ai")
    if not sid and subs:
        cnt = fields["count"]
        header = (
            "🧨 <b>"
            + str(cnt) + " × "
            + esc(mtype)
            + " mistake(s)</b>")
        await S.subject_pick_card(
            u, chat, code,
            header)
        return
    order = [("count", "Count"),
             ("mtype", "Type"),
             ("subject",
              "Subject"),
             ("topic", "Topic")]
    lines = card_lines(
        fields, order)
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


async def candidate_class(
        u, chat, f, intent,
        conf):
    which = (f.get("which")
             or "coaching"
             ).lower()
    if which not in ("school",
                     "coaching",
                     "classes"):
        which = "coaching"
    today = today_d()
    if intent == "class_move":
        frm = parse_any_date(
            str(f.get("from")
                or "tomorrow"),
            today)
        to = parse_any_date(
            str(f.get("to")
                or ""), today)
        if not to:
            await send(
                chat,
                "Move it to which day? "
                "e.g. 'move coaching "
                "to friday'")
            return
        if not frm:
            frm = today + timedelta(
                days=1)
        fields = {
            "op": "move",
            "which": which,
            "date": frm.isoformat(),
            "to": to.isoformat()}
        f1 = frm.strftime(
            "%a %d %b")
        f2 = to.strftime(
            "%a %d %b")
        lines = ["Move <b>"
                 + which
                 + "</b>",
                 "From: <b>"
                 + f1 + "</b>",
                 "To: <b>"
                 + f2 + "</b>"]
    else:
        d = parse_any_date(
            str(f.get("date")
                or "tomorrow"),
            today)
        if not d:
            d = today + timedelta(
                days=1)
        fields = {
            "op": "cancel",
            "which": which,
            "date": d.isoformat()}
        dl = d.strftime(
            "%a %d %b")
        lines = ["Cancel <b>"
                 + which
                 + "</b>",
                 "Date: <b>"
                 + dl + "</b>"]
    code = await new_pending(
        u, "class_op",
        fields, "ai")
    flag = ""
    if conf != "high":
        flag = " ⚠️"
    await send(
        chat,
        "\n".join(lines)
        + "\n\nConfidence: <b>"
        + conf.upper() + flag
        + "</b>",
        confirm_kb(code))


async def wizard_session_log(
        u, chat, pend, text):
    pid = str(pend["id"])
    if text.lower().startswith(
            "/skip"):
        await q(
            """UPDATE
               pending_actions
               SET status=
                 'confirmed'
               WHERE id=$1::uuid""",
            pid)
        await send(
            chat,
            "Logged as time-only. "
            "🏠", MENU_KB)
        return
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
        payload = jd(
            pend["payload"], {})
        sess_id = payload.get(
            "session_id")
        await q(
            """UPDATE
               study_sessions
               SET questions_attempted
                   =$2,
                   questions_correct
                   =$3
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
            nr = await \
                apply_revision(
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
                        mrow[
                            "mastery"
                        ],
                    "events":
                        mrow[
                            "events"
                        ],
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
                        mrow[
                            "mastery"
                        ]}
        await q(
            """UPDATE
               pending_actions
               SET status=
                 'confirmed'
               WHERE id=$1::uuid""",
            pid)
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
                    + format(
                        mast, ".0f")
                    + "%")
            if ev:
                line += (" ("
                         + str(ev)
                         + " sessions)")
            lines.append(line)
        nr = res.get("next_rev")
        if nr:
            lines.append(
                "🔁 Next revision: "
                + nr.strftime(
                    "%d %b"))
        await send(
            chat,
            "\n".join(lines),
            MENU_KB)
        return
    # not session data — if the
    # message has NO digits at
    # all, it's conversation:
    # cancel the wizard instead
    # of trapping the user
    if not re.search(r"\d", text):
        await q(
            """UPDATE
               pending_actions
               SET status=
                 'cancelled'
               WHERE id=$1::uuid""",
            pid)
        await send(
            chat,
            "Okay — logged that "
            "session as time-only. "
            "🏠")
        await handle_text_msg(
            u, chat, text)
        return
    await send(
        chat,
        "Try: '25 18' or '25 "
        "questions 18 correct' "
        "— or /skip.")


async def session_ctl(
        u, chat, op):
    now = now_tz()
    s = await live_session(
        u["id"])
    if not s:
        await cmd_dashboard(
            u, chat)
        return
    if op == "pause":
        await q(
            """UPDATE
               study_sessions
               SET status='paused',
                   paused_at=$2
               WHERE id=$1::uuid""",
            str(s["id"]), now)
        await send(
            chat, "⏸ Paused.",
            IK([("▶️ Resume",
                 "ses:resume")],
               [("❌ Abandon",
                 "ses:abandon")]))
    elif op == "resume":
        paused = (s["paused_seconds"]
                  or 0)
        if s["paused_at"]:
            add = (now
                   - s["paused_at"])
            paused += int(
                add.total_seconds())
        await q(
            """UPDATE
               study_sessions
               SET status='active',
                   paused_at=NULL,
                   paused_seconds=$2
               WHERE id=$1::uuid""",
            str(s["id"]), paused)
        await send(
            chat, "▶️ Resumed.",
            IK([("⏸ Pause",
                 "ses:pause"),
                ("✅ Finish",
                 "ses:finish")],
               [("❌ Abandon",
                 "ses:abandon")]))
    elif op == "finish":
        paused = (s["paused_seconds"]
                  or 0)
        if (s["status"] == "paused"
                and s["paused_at"]):
            add = (now
                   - s["paused_at"])
            paused += int(
                add.total_seconds())
        dur = (now
               - s["started_at"])
        dur = max(
            0,
            int(dur.total_seconds()
                - paused) // 60)
        await q(
            """UPDATE
               study_sessions
               SET status=
                 'finished',
                   ended_at=$2,
                   duration_minutes
                     =$3,
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
        skip = ("cfm:" + code
                + ":skipq")
        await send(
            chat,
            "✅ Finished — "
            + fm(dur)
            + ".\n\nHow many "
            "questions did you "
            "attempt?\ne.g. '25 18' "
            "or '25 questions 18 "
            "correct' (or /skip)",
            IK([("🤷 Skip",
                 skip)]))
    elif op == "abandon":
        await q(
            """UPDATE
               study_sessions
               SET status=
                 'abandoned',
                   ended_at=$2
               WHERE id=$1::uuid""",
            str(s["id"]), now)
        await send(
            chat,
            "Session discarded — "
            "no guilt; the next "
            "one counts.",
            MENU_KB)


async def handle_voice(u, chat,
                       msg):
    try:
        audio = await get_file_bytes(
            msg["voice"]["file_id"])
    except Exception as e:
        LOG.warning("voice dl: %s", e)
        await send(
            chat,
            "Couldn't download "
            "that voice note.")
        return
    await send(chat,
               "🎙 Transcribing…")
    try:
        text = await ai_transcribe(
            audio)
    except AIError:
        await send(
            chat,
            "Voice transcription "
            "is rate-limited right "
            "now — type it "
            "instead?", MENU_KB)
        return
    if not text:
        await send(
            chat,
            "I couldn't hear that "
            "clearly — try typing "
            "it.")
        return
    await send(
        chat,
        "🎙 <i>"
        + esc(text[:300])
        + "</i>")
    await handle_text_msg(
        u, chat, text)


async def handle_text_msg(
        u, chat, text):
    if not u["onboarded"]:
        await ob_handle(
            u, chat, text)
        return
    t = text.strip()
    low = t.lower()
    pend = await qrow(
        """SELECT *
           FROM pending_actions
           WHERE user_id=$1::uuid
           AND kind='session_log'
           AND status='pending'
           ORDER BY created_at
             DESC
           LIMIT 1""", u["id"])
    if pend:
        await wizard_session_log(
            u, chat, dict(pend),
            t)
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
        await send(
            chat,
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
    if cmd == "classes":
        await cmd_classes(u, chat)
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
        await cmd_syllabus(
            u, chat)
        return
    if cmd in ("notes", "resources"):
        await cmd_notes(u, chat)
        return
    if cmd in ("settings", "profile"):
        await cmd_settings(u, chat)
        return
    if cmd == "insights":
        await cmd_insights(u, chat)
        return
    if cmd == "memories":
        await cmd_memories(u, chat)
        return
    if cmd == "review":
        await cmd_review(u, chat)
        return
    if cmd in ("dayreview", "day"):
        await cmd_dayreview(
            u, chat)
        return
    if cmd == "export":
        await cmd_export(u, chat)
        return
    if cmd == "doctor":
        await cmd_doctor(u, chat)
        return
    # ---- settings by chat ----
    if await try_settings(
            u, chat, t, low):
        return
    # ---- deterministic fast paths ----
    energy = detect_energy(t)
    if energy \
            and len(low.split()) <= 6:
        await set_energy(
            u, chat, energy)
        return
    pat = (r"(what should i "
           r"study|what do i "
           r"study|whats next|"
           r"what's next|"
           r"next task|"
           r"start next|"
           r"what.s due|"
           r"overdue work)")
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
    if re.search(
            r"(day review|"
            r"review my day)",
            low):
        await cmd_dayreview(
            u, chat)
        return
    m = re.search(
        r"(?:i (?:have|got|"
        r"only have)|only)"
        r"\s+(\d+)\s*"
        r"(?:minutes|min|mins)"
        r"\b", low)
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
        r"solved|did|study)\b",
        low)
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
        await log_chat(u, "user", t)
        routed = await ai_route(
            t, u)
    except AIError:
        await send(
            chat,
            "My AI brain is "
            "rate-limited — wait "
            "a minute and resend. "
            "(Buttons still work: "
            "🏠)", MENU_KB)
        return
    routed = jd(routed, {})
    intent = (routed.get("intent")
              or "none").lower()
    f = jd(routed.get("fields"),
           {})
    conf = (routed.get("confidence")
            or "medium").lower()
    # reply may live at top level
    # OR inside fields (Gemini
    # puts it in both places)
    reply = routed.get("reply")
    if not reply:
        reply = f.get("reply")
    if intent in ("chat", "none"):
        if reply:
            await log_chat(
                u, "ai",
                str(reply))
            await send(chat,
                       esc(str(reply)
                           [:900]),
                       MENU_KB)
        else:
            await chat_reply(
                u, chat, t)
        return
    if reply:
        await log_chat(
            u, "ai", str(reply))
        await send(chat,
                   esc(str(reply)
                       [:400]))
    if intent == "query":
        view = (f.get("view")
                or "dashboard"
                ).lower()
        views = {
            "plan": cmd_plan,
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
            "classes":
                cmd_classes,
            "syllabus":
                cmd_syllabus,
            "dashboard":
                cmd_dashboard,
            "dayreview":
                cmd_dayreview,
            "insights":
                cmd_insights,
            "review": cmd_review}
        fn = views.get(
            view, cmd_dashboard)
        if fn is cmd_analytics:
            await fn(u, chat, days=7)
        else:
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
    if intent == "remember":
        await do_remember(
            u, chat, f)
        return
    if intent == "forget":
        await do_forget(
            u, chat, f)
        return
    if intent == "tutor":
        await V.tutor_reply(
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
            await send(
                chat,
                "How many did you "
                "do? e.g. 'did 25 "
                "of 50 DPP'")
            return
        code = await new_pending(
            u, "hw_progress",
            f, "ai")
        title = esc(
            f.get("title")
            or "homework")
        done = f.get("done")
        await send(
            chat,
            "📝 <b>" + title
            + "</b>\nDone now: <b>"
            + str(done)
            + "</b>",
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
        lines = card_lines(
            clean, order)
        topics = clean.get(
            "topics") or []
        if topics:
            lines.append(
                "Topic breakdown: <b>"
                + str(len(topics))
                + " topics</b>")
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
        sid, sname = \
            resolve_subject(
                f.get("subject"),
                subs)
        clean["subject_id"] = sid
        if sname:
            clean["subject"] = sname
        code = await new_pending(
            u, "mistake_resolve",
            clean, "ai")
        order = [("subject",
                  "Subject"),
                 ("topic", "Topic")]
        lines = card_lines(
            clean, order)
        await send(
            chat,
            "Mark these as "
            "resolved?\n\n"
            + "\n".join(lines),
            confirm_kb(code))
        return
    if intent == "timetable":
        days = f.get("days") or []
        days = jd(days, [])
        if not days:
            pc = parse_class(t)
            if pc:
                days = [{
                    "day": "daily",
                    "start": pc[0]
                    .strftime("%H:%M"),
                    "end": pc[1]
                    .strftime("%H:%M")}]
        if not days:
            await send(chat,
                       "What times? "
                       "e.g. 'school "
                       "8-2 mon-sat'")
            return
        code = await new_pending(
            u, "timetable_import",
            {"td": {
                "school_or_"
                "coaching":
                    f.get("which")
                    or "school",
                "days": days}},
            "ai")
        lines = ["🗓 "
                 "Timetable read:",
                 ""]
        for d in days[:8]:
            d = jd(d, {})
            lines.append(
                "• "
                + esc(str(
                    d.get("day")
                    or ""))
                + ": "
                + esc(str(
                    d.get("start")
                    or "?"))
                + "–"
                + esc(str(
                    d.get("end")
                    or "?")))
        lines += ["", "Save?"]
        await send(
            chat,
            "\n".join(lines),
            confirm_kb(code))
        return
    if intent == "class_add":
        code = await new_pending(
            u, "class_add", f, "ai")
        await send(
            chat,
            "Add this class as "
            "read?",
            confirm_kb(code))
        return
    if intent == "extra_class":
        code = await new_pending(
            u, "extra_class", f,
            "ai")
        await send(
            chat,
            "Add one-off extra "
            "class as read?",
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
            {"blocks": blocks},
            "ai")
        await send(
            chat,
            "📚 <b>Syllabus</b> — "
            "review then confirm:",
            confirm_kb(code))
        return
    await cmd_dashboard(
        u, chat)


async def cmd_next(u, chat,
                   max_minutes=None,
                   exclude=None):
    now = now_tz()
    today = today_d()
    live = await live_session(
        u["id"])
    if live:
        await cmd_dashboard(
            u, chat)
        return
    mods = await load_day_mods(
        u["id"], today)
    cls = await get_classes(
        u["id"])
    extras = await get_extras(
        u["id"], today)
    avail, _ = day_minutes(
        u, today, mods, cls, extras)
    spent = await spent_today(
        u, today)
    lmins = await live_minutes(
        u, now)
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
    pick = choose_next(
        scored, remaining, exclude)
    if not pick:
        await send(
            chat,
            "Nothing left that fits "
            "right now — rest is "
            "also strategy. 🌙 Log "
            "something with '50 "
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
        "• " + esc(r)
        for r in rs)
    icons = {"revision": "🔁",
             "quiz": "🧠",
             "homework": "📝",
             "test_prep": "🧪",
             "mistake_review": "🧨",
             "study": "📖"}
    icon = icons.get(
        c["kind"], "▶️")
    txt = ("▶️ <b>START NEXT"
           "</b>\n\n"
           + icon + " <b>"
           + esc(c["title"])
           + "</b>\n"
           "⏱ " + fm(c["est"])
           + " • fits your "
           + fm(remaining)
           + " left\n\n"
           "<b>Why this now?</b>\n"
           + why)
    go = "go:" + code
    alt = "go:" + code + ":alt"
    whyb = "go:" + code + ":why"
    await send(chat, txt, IK(
        [("▶️ Start", go),
         ("🔄 Another", alt)],
        [("📖 Why?", whyb),
         ("🏠 Menu", "nav:menu")]))


async def start_live_session(
        u, chat, cand):
    existing = await live_session(
        u["id"])
    if existing:
        await cmd_dashboard(
            u, chat)
        return
    now = now_tz()
    await qrow(
        """INSERT INTO
           study_sessions
           (user_id, subject_id,
            topic_id, source,
            status, title,
            started_at)
           VALUES($1::uuid,
                  $2::uuid,
                  $3::uuid,'live',
                  'active',$4,$5)
           RETURNING id""",
        u["id"],
        cand.get("subject_id"),
        cand.get("topic_id"),
        cand["title"], now)
    await send(
        chat,
        "⏱ <b>Session started"
        "</b> — "
        + esc(cand["title"])
        + "\nTap ✅ when done. "
        "I'll ask how many "
        "questions you solved.",
        IK([("⏸ Pause",
             "ses:pause"),
            ("✅ Finish",
             "ses:finish")],
           [("❌ Abandon",
             "ses:abandon")]))


async def handle_callback(cb):
    data = cb.get("data", "")
    chat = (cb["message"]
            ["chat"]["id"])
    if not cb.get("from"):
        return
    u = await S.ensure_user(
        cb["from"], chat)
    u = await lazy_tick(
        u, now_tz(), chat)
    await answer_cb(cb["id"])
    scope, _, rest = \
        data.partition(":")
    try:
        await dispatch_cb(
            u, chat, scope, rest,
            cb)
    except Exception:
        LOG.exception(
            "callback error: %s",
            data)
        await send(
            chat,
            "Something broke — "
            "the action wasn't "
            "completed. Try again.",
            MENU_KB)


async def dispatch_cb(
        u, chat, scope, rest, cb):
    if scope == "nav":
        await nav_cb(u, chat, rest)
    elif scope == "anp":
        days = int(rest or "7")
        await cmd_analytics(
            u, chat, days=days)
    elif scope == "an":
        kind, _, ident = \
            rest.partition(":")
        if kind == "s":
            await V.cmd_subject_an(
                u, chat, ident)
        elif kind == "c":
            await V.cmd_chapter_an(
                u, chat, ident)
        elif kind == "t":
            await V.cmd_topic_an(
                u, chat, ident)
        else:
            await cmd_analytics(
                u, chat, days=7)
    elif scope == "env":
        if rest in ("energetic",
                    "normal",
                    "tired"):
            await set_energy(
                u, chat, rest)
            await send(
                chat,
                "Noted — tomorrow's "
                "planning will use "
                "this. 🌙", MENU_KB)
    elif scope == "syls":
        await cmd_syllabus(
            u, chat,
            subject_id=rest)
    elif scope == "syl":
        await V.cmd_chapter_topics(
            u, chat, rest)
    elif scope == "quizt":
        r = await qrow(
            """SELECT t.name top,
                      s.name subj,
                      t.subject_id sid
               FROM topics t
               JOIN subjects s
                 ON s.id=t.subject_id
               WHERE t.id=$1::uuid""",
            rest)
        if r:
            label = (r["subj"]
                     + " — "
                     + r["top"])
            await start_quiz(
                u, chat, r["top"],
                subject_id=str(
                    r["sid"]),
                topic_id=rest,
                topic_label=label)
    elif scope == "cfm":
        msg_id = (cb["message"]
                  ["message_id"])
        await confirm_cb(
            u, chat, msg_id, rest)
    elif scope == "subj":
        await subj_cb(
            u, chat, rest)
    elif scope == "ses":
        await session_ctl(
            u, chat, rest)
    elif scope == "quiz":
        qid, _, spec = \
            rest.partition(":")
        if spec.startswith("stop"):
            await q(
                """UPDATE quizzes
                   SET status=
                     'stopped'
                   WHERE id=$1::uuid
                   AND user_id=
                     $2::uuid""",
                qid, u["id"])
            await send(
                chat, "Quiz stopped.",
                MENU_KB)
            return
        try:
            qi_s, opt_s = \
                spec.split(":")
            await quiz_answer(
                u, chat, qid,
                int(qi_s),
                int(opt_s))
        except ValueError:
            pass
    elif scope == "go":
        await go_cb(u, chat, rest)
    elif scope == "ob":
        await ob_cb(u, chat, rest)
    elif scope == "note":
        r = await qrow(
            """SELECT title, content
               FROM resources
               WHERE id=$1::uuid
               AND user_id=$2::uuid""",
            rest, u["id"])
        if r:
            body = esc(
                (r["content"]
                 or "")[:3000])
            await send(
                chat,
                "📎 <b>"
                + esc(r["title"])
                + "</b>\n\n"
                + body, MENU_KB)
    elif scope == "reset":
        await reset_cb(u, chat, rest)
    elif scope == "mem":
        await q(
            """DELETE FROM ai_memory
               WHERE id=$1::uuid
               AND user_id=$2::uuid""",
            rest, u["id"])
        await send(chat,
                   "🗑 Forgotten.",
                   MENU_KB)
    elif scope == "waa":
        await M.wa_add_all(
            u, chat, rest)
    elif scope == "war":
        await M.wa_review(
            u, chat, rest)
    elif scope == "wa":
        code, _, spec = \
            rest.partition(":")
        parts = spec.split(":")
        if len(parts) == 2:
            try:
                idx = int(parts[0])
                await M.wa_item_cb(
                    u, chat, code,
                    idx, parts[1])
            except ValueError:
                pass


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
        "syl": cmd_syllabus,
        "classes": cmd_classes,
        "insights": cmd_insights,
        "memories": cmd_memories,
        "export": cmd_export}
    if what in views:
        fn = views[what]
        if fn is cmd_analytics:
            await fn(u, chat, days=7)
        elif fn is cmd_syllabus:
            await fn(u, chat)
        else:
            await fn(u, chat)
    elif what == "menu":
        await send(
            chat,
            "🏠 <b>StudyOS</b>",
            MENU_KB)
    elif what == "quiz":
        await start_quiz(
            u, chat, None)
    else:
        await cmd_dashboard(
            u, chat)


async def go_cb(u, chat, rest):
    code, _, mode = \
        rest.partition(":")
    pend = await get_pending(
        u, code)
    if not pend \
            or pend["status"] \
            != "pending":
        await send(
            chat,
            "⏳ That suggestion "
            "expired — ask me "
            "again ('what should "
            "I study?').",
            MENU_KB)
        return
    payload = jd(
        pend["payload"], {})
    cand = payload.get("cand")
    if not cand:
        return
    if mode == "why":
        rs = payload.get(
            "reasons") or []
        lines = ["📖 <b>WHY THIS?"
                 "</b>", ""]
        for r in rs:
            lines.append("• "
                         + esc(r))
        meta = cand.get(
            "meta", {})
        mast = meta.get("mastery")
        if mast is not None:
            lines.append("")
            lines.append(
                "🧠 mastery: "
                + format(mast,
                         ".0f") + "%")
        await send(chat,
                   "\n".join(lines),
                   MENU_KB)
        return
    if mode == "alt":
        await q(
            """UPDATE
               pending_actions
               SET status=
                 'cancelled'
               WHERE code=$1""",
            code)
        await cmd_next(
            u, chat,
            exclude={cand["key"]})
        return
    await q(
        """UPDATE pending_actions
           SET status='confirmed'
           WHERE code=$1""",
        code)
    if cand["kind"] == "quiz":
        meta = cand.get(
            "meta", {})
        label = (cand.get(
            "chapter")
            or cand["title"])
        flavor = None
        rt = meta.get("rev_type")
        if rt == "mistake_review":
            flavor = ("focus on "
                      "the kinds of "
                      "errors "
                      "students make "
                      "here")
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


async def ob_cb(u, chat, rest):
    what, _, arg = \
        rest.partition(":")
    if what == "confirm":
        if not u["onboarded"]:
            await V.ob_commit(
                u, chat)
        else:
            await cmd_dashboard(
                u, chat)
    elif what == "g":
        keys = [s[0]
                for s in V.OB_STEPS]
        if arg in keys:
            await q(
                """UPDATE users
                   SET ob_step=$2
                   WHERE id=$1::uuid""",
                u["id"], arg)
            await V.ob_prompt(
                u, chat, arg)
    elif what == "restart":
        await q(
            """UPDATE users SET
               ob_state=
                 '{}'::jsonb,
               ob_step=NULL,
               onboarded=false
               WHERE id=$1::uuid""",
            u["id"])
        await ob_start(u, chat)


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
        await send(
            chat,
            "🧨 Wiped clean. Send "
            "/start to set up "
            "again.")


async def subj_cb(u, chat, rest):
    code, _, sid = \
        rest.partition(":")
    pend = await get_pending(
        u, code)
    if not pend \
            or pend["status"] \
            != "pending":
        return
    payload = jd(
        pend["payload"], {})
    sname = await qval(
        """SELECT name
           FROM subjects
           WHERE id=$1::uuid""",
        sid)
    if not sname:
        return
    fields = payload.get(
        "fields") or {}
    fields["subject_id"] = sid
    fields["subject"] = sname
    payload["fields"] = fields
    await set_pending_payload(
        code, payload)
    kind = pend["kind"]
    if kind == "log_session":
        lines = ["I found:", ""]
        if fields.get("questions"):
            lines.append(
                "Questions: <b>"
                + str(
                    fields[
                        "questions"])
                + "</b>")
            if fields.get(
                    "correct") \
                    is not None:
                lines.append(
                    "Correct: <b>"
                    + str(
                        fields[
                            "correct"
                        ])
                    + "</b>")
        if fields.get("minutes"):
            lines.append(
                "Duration: <b>"
                + fm(
                    fields[
                        "minutes"])
                + "</b>")
        lines.append(
            "Subject: <b>"
            + esc(sname)
            + "</b>")
        if fields.get("topic"):
            lines.append(
                "Topic: <b>"
                + esc(
                    fields[
                        "topic"])
                + "</b>")
        await send(chat,
                   "\n".join(lines),
                   confirm_kb(code))
        return
    orders = {
        "hw_add": [
            ("title", "Homework"),
            ("subject",
             "Subject"),
            ("questions",
             "Questions"),
            ("due", "Due")],
        "test_add": [
            ("name", "Test"),
            ("subject",
             "Subject"),
            ("date", "When")],
        "mistake_add": [
            ("count", "Count"),
            ("mtype", "Type"),
            ("subject",
             "Subject"),
            ("topic", "Topic")]}
    lines = card_lines(
        fields,
        orders.get(kind, []))
    await send(chat,
               "I found:\n\n"
               + "\n".join(lines),
               confirm_kb(code))


async def confirm_cb(
        u, chat, msg_id, rest):
    code, _, answer = \
        rest.rpartition(":")
    pend = await get_pending(
        u, code)
    if not pend \
            or pend["status"] \
            != "pending":
        await edit(
            chat, msg_id,
            "⏳ This expired — "
            "send the message "
            "again.")
        return
    payload = jd(
        pend["payload"], {})
    fields = payload.get(
        "fields") or {}
    kind = pend["kind"]
    if answer in ("no", "undo"):
        if answer == "undo" \
                and kind \
                == "undo_log":
            await q(
                """DELETE
                   FROM study_sessions
                   WHERE user_id=
                     $1::uuid
                   AND created_at >
                     now() - interval
                       '10 minutes'
                   AND source='log'
                   AND questions_attempted=$2
                   AND questions_correct
                     IS NOT DISTINCT
                     FROM $3""",
                u["id"],
                fields.get(
                    "questions")
                or 0,
                fields.get(
                    "correct"))
            sid = fields.get(
                "subject_id")
            if sid:
                await recompute(
                    u["id"], sid,
                    None)
            await edit(
                chat, msg_id,
                "↩️ Undone.")
        else:
            await edit(
                chat, msg_id,
                "Cancelled. ✖️")
        await q(
            """UPDATE
               pending_actions
               SET status=
                 'cancelled'
               WHERE code=$1""",
            code)
        return
    if answer == "edit":
        await q(
            """UPDATE
               pending_actions
               SET status=
                 'cancelled'
               WHERE code=$1""",
            code)
        await edit(
            chat, msg_id,
            "Send the corrected "
            "message and I'll "
            "re-read it. 👍")
        return
    if answer == "skipq":
        await q(
            """UPDATE
               pending_actions
               SET status=
                 'confirmed'
               WHERE code=$1""",
            code)
        await edit(
            chat, msg_id,
            "Logged as time-only. "
            "🏠")
        return
    # answer == yes
    if kind == "log_session":
        res = await \
            A.do_log_session(
                u, fields)
        ucode = await new_pending(
            u, "undo_log",
            {"fields": fields},
            "confirm")
        undo = ("cfm:" + ucode
                + ":undo")
        await edit(
            chat, msg_id, res,
            IK([("↩️ Undo",
                 undo)]))
    elif kind == "hw_add":
        await edit(
            chat, msg_id,
            await A.do_hw_add(
                u, fields))
        await A.refresh_plan(
            u, chat,
            "homework added")
    elif kind == "hw_progress":
        msg, completed = \
            await A.do_hw_progress(
                u, fields)
        await edit(
            chat, msg_id, msg)
        if completed:
            await A.refresh_plan(
                u, chat,
                "homework finished")
    elif kind == "test_add":
        await edit(
            chat, msg_id,
            await A.do_test_add(
                u, fields))
        await A.refresh_plan(
            u, chat,
            "test added")
    elif kind == "test_result":
        await edit(
            chat, msg_id,
            await A.do_test_result(
                u, fields))
    elif kind == "mistake_add":
        await edit(
            chat, msg_id,
            await A.do_mistake_add(
                u, fields))
    elif kind == \
            "mistake_resolve":
        await edit(
            chat, msg_id,
            await
            A.do_mistake_resolve(
                u, fields))
    elif kind == "class_add":
        await edit(
            chat, msg_id,
            await A.do_class_add(
                u, fields))
        await A.refresh_plan(
            u, chat,
            "class added")
    elif kind == "extra_class":
        await edit(
            chat, msg_id,
            await A.do_extra_class(
                u, fields))
        await A.refresh_plan(
            u, chat,
            "extra class added")
    elif kind == "class_op":
        await edit(
            chat, msg_id,
            await A.do_class_op(
                u, fields))
        await A.refresh_plan(
            u, chat,
            "schedule changed")
    elif kind == "timetable_import":
        td = payload.get("td") \
            or {}
        td = jd(td, {})
        which = (td.get(
            "school_or_coaching")
            or "school").lower()
        if "coach" in which:
            c0 = "coaching_start"
            c1 = "coaching_end"
            c2 = "coaching_days"
            word = "Coaching"
        else:
            c0 = "school_start"
            c1 = "school_end"
            c2 = "school_days"
            word = "School"
        days = (td.get("days")
                or [])
        if not days:
            await edit(chat, msg_id,
                       "No days "
                       "found — "
                       "cancelled.")
            await q(
                """UPDATE
                   pending_actions
                   SET status=
                     'cancelled'
                   WHERE code=$1""",
                code)
            return
        starts = []
        ends = []
        wds = []
        for d in days[:8]:
            d = jd(d, {})
            st = parse_time(
                str(d.get("start")
                    or ""))
            en = parse_time(
                str(d.get("end")
                    or ""))
            dd = parse_days(
                str(d.get("day")
                    or ""))
            if st and en and dd:
                starts.append(st)
                ends.append(en)
                wds.extend(dd)
        if not starts:
            await edit(chat, msg_id,
                       "Couldn't "
                       "read times — "
                       "type them "
                       "instead?")
            await q(
                """UPDATE
                   pending_actions
                   SET status=
                     'cancelled'
                   WHERE code=$1""",
                code)
            return
        st = min(starts)
        en = max(ends)
        wds = sorted(set(wds))
        await q(
            """UPDATE users SET
               """ + c0
            + "=$2, "
            + c1
            + "=$3, "
            + c2
            + """=$4
               WHERE id=$1::uuid""",
            u["id"], st, en, wds)
        await edit(
            chat, msg_id,
            "✅ " + word
            + " schedule saved: "
            + st.strftime("%H:%M")
            + "–"
            + en.strftime("%H:%M")
            + " ("
            + str(len(wds))
            + " days/week). "
            "Capacity adapts.")
        await A.refresh_plan(
            u, chat,
            "timetable imported")
    elif kind == "syllabus":
        await edit(
            chat, msg_id,
            await A.do_syllabus(
                u,
                payload.get(
                    "blocks",
                    [])))
    elif kind == "save_note":
        await edit(
            chat, msg_id,
            await A.do_save_note(
                u, fields))
    else:
        await edit(
            chat, msg_id,
            "✅ Done.")
    await q(
        """UPDATE pending_actions
           SET status='confirmed'
           WHERE code=$1""",
        code)


async def process_message(msg):
    chat = msg["chat"]["id"]
    try:
        if not msg.get("from"):
            return
        u = await S.ensure_user(
            msg["from"], chat)
        u = await lazy_tick(
            u, now_tz(), chat)
        if msg.get("voice"):
            await handle_voice(
                u, chat, msg)
            return
        if msg.get("photo"):
            await M.handle_photo(
                u, chat, msg)
            return
        if msg.get("document"):
            await M.handle_document(
                u, chat, msg)
            return
        text = (msg.get("text")
                or msg.get(
                    "caption")
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
                await send(
                    chat,
                    "Let's finish "
                    "setup first 🙂")
            return
        await handle_text_msg(
            u, chat, text)
    except Exception:
        LOG.exception(
            "message failed")
        await send(
            chat,
            "Something broke on "
            "my side — try again.",
            MENU_KB)


async def process_update(upd):
    if "callback_query" in upd:
        await handle_callback(
            upd["callback_query"])
    elif "message" in upd:
        await process_message(
            upd["message"])
