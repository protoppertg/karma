import re
import json
import hashlib
from datetime import timedelta
from datetime import time as dtime
from config import TZ, TZ_NAME, GEMINI_MODEL, LOG
from database import (q, qrow, qrows,
                      qval, jd)
from tg import (send, IK, esc, fm,
                bar, arrow, now_tz,
                today_d, sod,
                send_document)
import services as S
from services import (get_subjects,
                      get_classes,
                      get_extras,
                      load_day_mods,
                      live_session,
                      live_minutes,
                      spent_today,
                      mistake_reps,
                      save_evidence,
                      build_candidates,
                      new_pending,
                      get_pending,
                      set_pending_payload,
                      find_topic,
                      upsert_topic,
                      upsert_subject)
from engines import (day_minutes,
                     build_plan)
from parsers import (parse_time,
                     parse_class,
                     parse_date,
                     parse_any_date,
                     SKIP_WORDS)
from brain import (ai_call, AIError,
                   _cache_get,
                   _cache_set,
                   memory_block,
                   log_chat,
                   save_memory,
                   get_memories,
                   chat_context_block)

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
        "• \"add homework: DPP 3, "
        "40 questions, due "
        "friday\"\n"
        "• \"did 25 of 50 DPP\" / "
        "\"did the rest of DPP\"\n"
        "• \"physics test sunday\" / "
        "\"got 68/75, weak in "
        "rotation\"\n"
        "• \"3 conceptual mistakes "
        "in rotation\" / \"fixed my "
        "rotation mistakes\"\n"
        "• \"my school is 8 to 2 "
        "monday to saturday\"\n"
        "• \"physics class every "
        "mon and wed 5-7\"\n"
        "• \"extra class tomorrow "
        "5-7\" / \"no class "
        "tomorrow\"\n"
        "• \"I'm exhausted\" / "
        "\"only 20 minutes\"\n"
        "• \"quiz me on "
        "thermodynamics\"\n"
        "• \"remember that I "
        "study best at 6am\" / "
        "\"forget everything\"\n\n"
        "Commands: /plan /next "
        "/homework /tests /revision "
        "/mistakes /classes "
        "/analytics /insights /quiz "
        "/notes /memories /syllabus "
        "/review /dayreview "
        "/export /settings /doctor\n\n"
        "📷 syllabus or timetable "
        "photo · 📄 PDF · 📥 "
        "WhatsApp export (.txt) · "
        "🎙 voice note.")
    await send(chat, txt, MENU_KB)


async def morning_card(u, chat):
    today = today_d()
    mods = await load_day_mods(
        u["id"], today)
    cls = await get_classes(u["id"])
    extras = await get_extras(
        u["id"], today)
    avail, _ = day_minutes(
        u, today, mods, cls, extras)
    name = (u["display_name"]
            or u["first_name"]
            or "there")
    ymins = await spent_today(
        u, today - timedelta(days=1))
    s = None
    try:
        s = await gather_stats(u, 7)
    except Exception:
        pass
    facts = {
        "name": name,
        "date": today.strftime(
            "%A %d %b"),
        "capacity_minutes": avail,
        "yesterday_minutes": ymins,
        "energy": u["energy"]}
    if s:
        facts["week_minutes"] = \
            s["mins"]
        facts["week_questions"] = \
            s["q"]
        facts["streak"] = s["streak"]
    lines = [
        "🌅 <b>MORNING, "
        + esc(name) + "</b>",
        today.strftime("%A, %d %b"),
        ""]
    body = ""
    try:
        mem = await memory_block(u)
        prompt = (
            "Write a morning study "
            "briefing for this "
            "student. Use ONLY "
            "these facts (never "
            "invent numbers): "
            + json.dumps(facts)
            + "\n"
            + mem
            + "\nStyle: 3-5 short "
            "lines, warm, direct, "
            "address them by name, "
            "end with ONE clear "
            "focus for today. "
            "Plain text.")
        txt = await ai_call(
            prompt, json_mode=False,
            max_tokens=250)
        body = esc(txt.strip()[:600])
    except AIError:
        body = ""
    if body:
        lines.append(body)
        lines.append("")
    lines.append("Capacity today: <b>"
                 + fm(avail) + "</b>")
    if ymins:
        lines.append("Yesterday: "
                     + fm(ymins))
    if s and s["streak"]:
        lines.append("🔥 "
                     + str(s["streak"])
                     + "-day streak")
    await send(chat,
               "\n".join(lines),
               IK([("▶️ START NEXT",
                    "nav:next"),
                   ("📊 Insights",
                    "nav:insights")],
                  [("📅 Plan",
                    "nav:plan")]))
    # auto-push today's full plan
    try:
        now = now_tz()
        cands = await build_candidates(
            u, now)
        plan = build_plan(
            cands, avail, u["energy"],
            0, now)
        tasks = json.dumps(
            plan["tasks"])
        await q(
            """INSERT INTO daily_plans
               (user_id, plan_date,
                available_minutes,
                tasks)
               VALUES($1::uuid,$2,$3,
                      $4::jsonb)
               ON CONFLICT
                 (user_id, plan_date)
               DO UPDATE SET
                 available_minutes=
                   EXCLUDED
                   .available_minutes,
                 tasks=EXCLUDED.tasks,
                 created_at=now()""",
            u["id"], today,
            plan["available"], tasks)
        plines = ["",
                  "📅 <b>TODAY'S "
                  "PLAN</b>",
                  "Planned: "
                  + fm(plan["allocated"])
                  + " of "
                  + fm(avail), ""]
        for i, tk in enumerate(
                plan["tasks"], 1):
            plines.append(
                str(i) + ". "
                + esc(tk["title"])
                + " — "
                + fm(tk["est"]))
        if not plan["tasks"]:
            plines.append(
                "Nothing due — "
                "free day. 🌙")
        if plan["deferred"]:
            plines.append("")
            plines.append(
                "<i>Also waiting: "
                + str(len(
                    plan["deferred"]))
                + " items</i>")
        await send(
            chat,
            "\n".join(plines),
            IK([("▶️ START NEXT",
                 "nav:next"),
               ("🔄 Replan",
                "nav:plan")]))
    except Exception:
        LOG.exception("auto plan push")


async def cmd_dashboard(u, chat):
    now = now_tz()
    today = today_d()
    live = await live_session(u["id"])
    if live:
        mins = await live_minutes(
            u, now)
        title = live["title"] \
            or "Study"
        await send(
            chat,
            "⏱ <b>SESSION ACTIVE"
            "</b>\n"
            + esc(title)
            + "\nRunning: "
            + fm(mins),
            IK([("⏸ Pause",
                 "ses:pause"),
                ("✅ Finish",
                 "ses:finish"),
                ("❌ Abandon",
                 "ses:abandon")]))
        return
    mods = await load_day_mods(
        u["id"], today)
    cls = await get_classes(u["id"])
    extras = await get_extras(
        u["id"], today)
    avail, busy = day_minutes(
        u, today, mods, cls, extras)
    done = await spent_today(
        u, today)
    remaining = max(0,
                    avail - done)
    plan = await qrow(
        """SELECT available_minutes
           FROM daily_plans
           WHERE user_id=$1::uuid
           AND plan_date=$2""",
        u["id"], today)
    planned = 0
    if plan:
        planned = int(
            (plan["available_minutes"]
             or 0))
    name = (u["display_name"]
            or u["first_name"]
            or "there")
    hour = now.hour
    if hour < 12:
        greet = "Good morning"
    elif hour < 17:
        greet = "Good afternoon"
    else:
        greet = "Good evening"
    lines = [
        "🏠 <b>STUDYOS</b>",
        "",
        greet + ", "
        + esc(name) + " 👋",
        "",
        "TODAY",
        "━━━━━━━━━━━━",
        "Available    "
        + fm(avail),
        "Planned      "
        + fm(planned),
        "Completed    "
        + fm(done),
        "Remaining    "
        + fm(remaining)]
    tests = await qrows(
        """SELECT name, test_at
           FROM tests
           WHERE user_id=$1::uuid
           AND status='scheduled'
           AND test_at > now()
             - interval '1 day'
           ORDER BY test_at
           LIMIT 2""", u["id"])
    attn = []
    hw = await qrow(
        """SELECT COUNT(*)
                    FILTER (
                     WHERE due_at
                       < now()) over,
                  COUNT(*)
                    FILTER (
                     WHERE due_at
                       >= now()
                     OR due_at
                       IS NULL) up
           FROM homework
           WHERE user_id=$1::uuid
           AND status NOT IN
             ('completed',
              'abandoned')""",
        u["id"])
    rv = await qval(
        """SELECT COUNT(*)
           FROM revisions
           WHERE user_id=$1::uuid
           AND due_at < now()""",
        u["id"])
    for t in tests:
        d = None
        if t["test_at"]:
            d = (t["test_at"]
                 - now).days
        if d is not None and d <= 2:
            attn.append("🧪 "
                        + esc(t["name"])
                        + " in " + str(d)
                        + "d")
    if rv:
        attn.append("🔁 "
                    + str(rv)
                    + " revisions overdue")
    if hw and hw["over"]:
        attn.append("📝 "
                    + str(hw["over"])
                    + " homework overdue")
    if u["energy"] in ("tired",
                       "exhausted"):
        attn.append("⚡ energy: "
                    + u["energy"])
    if attn:
        lines += ["",
                  "⚠️ ATTENTION"]
        lines += ["• " + a
                  for a in attn[:5]]
    lines += ["",
              "📈 THIS WEEK"]
    s = None
    try:
        s = await gather_stats(u, 7)
    except Exception:
        pass
    if s:
        lines.append("Study time   "
                     + fm(s["mins"]))
        lines.append("Questions    "
                     + str(s["q"]))
        lines.append("Accuracy     "
                     + format(s["acc"],
                              ".0f")
                     + "%")
    kb = []
    if tests and tests[0]["test_at"]:
        d = (tests[0]["test_at"]
             - now).days
        if d <= 3:
            kb.append(
                [("🧪 TEST SOON — "
                  "prep",
                  "nav:next")])
    if hw and hw["over"]:
        kb.append(
            [("⚠️ OVERDUE homework",
              "nav:hw")])
    kb += [[("▶️ START NEXT",
             "nav:next"),
            ("📅 Plan", "nav:plan")],
           [("📊 Analytics",
             "nav:an"),
            ("📚 More", "nav:menu")]]
    await send(chat,
               "\n".join(lines),
               IK(*kb))


async def cmd_plan(u, chat):
    now = now_tz()
    today = today_d()
    mods = await load_day_mods(
        u["id"], today)
    cls = await get_classes(u["id"])
    extras = await get_extras(
        u["id"], today)
    avail, busy = day_minutes(
        u, today, mods, cls, extras)
    cands = await build_candidates(
        u, now)
    last = await qval(
        """SELECT MAX(
                    created_at::date)
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'""",
        u["id"])
    missed = 0
    if last and (today - last).days \
            > 1:
        missed = (today
                  - last).days - 1
    plan = build_plan(
        cands, avail, u["energy"],
        missed, now)
    tasks = json.dumps(
        plan["tasks"])
    await q(
        """INSERT INTO daily_plans
           (user_id, plan_date,
            available_minutes,
            tasks)
           VALUES($1::uuid,$2,$3,
                  $4::jsonb)
           ON CONFLICT
             (user_id, plan_date)
           DO UPDATE SET
             available_minutes=
               EXCLUDED
               .available_minutes,
             tasks=EXCLUDED.tasks,
             created_at=now()""",
        u["id"], today,
        plan["available"], tasks)
    lines = [
        "📅 <b>PLAN — "
        + today.strftime(
            "%a %d %b") + "</b>",
        "Capacity: <b>"
        + fm(plan["available"])
        + "</b> (after school/"
        "coaching/classes/meals)",
        "Planned: <b>"
        + fm(plan["allocated"])
        + "</b>", ""]
    for i, t in enumerate(
            plan["tasks"], 1):
        lines.append(
            str(i) + ". "
            + esc(t["title"])
            + " — "
            + fm(t["est"]))
    if plan["missed"]:
        lines.append(
            "\n<i>Missed "
            + str(plan["missed"])
            + " day(s) — kept "
            "urgent items, spread "
            "the rest.</i>")
    if plan["deferred"]:
        lines += ["",
                  "<i>Kept for "
                  "later:</i>"]
        for t, r in plan[
                "deferred"][:5]:
            lines.append(
                "• " + esc(t)
                + " — " + esc(r))
    await send(chat,
               "\n".join(lines),
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
             ('completed',
              'abandoned')
           ORDER BY h.due_at
             NULLS LAST
           LIMIT 15""", u["id"])
    now = now_tz()
    if not rows:
        await send(
            chat,
            "📝 No open homework. 🎉"
            "\nAdd: 'homework DPP 4,"
            " 30 questions, due "
            "monday'.", MENU_KB)
        return
    lines = ["📝 <b>HOMEWORK"
             "</b>", ""]
    for r in rows:
        mark = "•"
        if r["due_at"] \
                and r["due_at"] < now:
            mark = "⚠️"
        line = (mark + " "
                + esc(r["title"]))
        if r["subj"]:
            line += (" ("
                     + esc(r["subj"])
                     + ")")
        if r["total_q"]:
            done = r["completed_q"] \
                or 0
            pct = (100 * done
                   / r["total_q"])
            line += (" ["
                     + str(done)
                     + "/"
                     + str(r["total_q"])
                     + "] "
                     + bar(pct, 6))
        if r["due_at"]:
            d = (r["due_at"].date()
                 - now.date()).days
            if d == 0:
                line += (" — due "
                         "today!")
            elif d > 0:
                line += (" — in "
                         + str(d)
                         + "d")
            else:
                line += (" — "
                         + str(-d)
                         + "d OVERDUE")
        lines.append(line)
    lines += ["",
              "<i>Report: 'did 25 of "
              "50 DPP' or 'did the "
              "rest of DPP'.</i>"]
    await send(chat,
               "\n".join(lines),
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
        await send(
            chat,
            "🧪 No tests yet. Add: "
            "'physics test on "
            "sunday'.", MENU_KB)
        return
    now = now_tz()
    up = [r for r in rows
          if r["test_at"]
          and r["test_at"] >= now
          and r["status"]
          == "scheduled"]
    done = [r for r in rows
            if r["status"]
            == "completed"]
    lines = ["🧪 <b>TESTS</b>", ""]
    if up:
        lines.append(
            "<b>Upcoming</b>")
        for r in sorted(
                up,
                key=lambda x:
                    x["test_at"]):
            d = (r["test_at"].date()
                 - now.date()).days
            if d == 0:
                tag = "TODAY"
            else:
                tag = ("in "
                       + str(d) + "d")
            lines.append(
                "• "
                + esc(r["name"])
                + " ("
                + esc(r["subj"]
                      or "")
                + ") — " + tag)
        lines.append("")
    if done:
        lines.append(
            "<b>Results</b>")
        for r in done[:6]:
            if r["total_marks"]:
                om = (r[
                    "obtained_marks"]
                    or 0)
                pct = (100 * om
                       / r[
                           "total_marks"
                       ])
                lines.append(
                    "• "
                    + esc(r["name"])
                    + " — "
                    + str(om) + "/"
                    + str(r[
                        "total_marks"
                    ])
                    + " ("
                    + format(pct,
                             ".0f")
                    + "%) "
                    + bar(pct, 6))
    await send(chat,
               "\n".join(lines),
               MENU_KB)


async def cmd_revision(u, chat):
    rows = await qrows(
        """SELECT rv.*,
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
             + interval '2 days'
           ORDER BY rv.due_at
           LIMIT 12""", u["id"])
    if not rows:
        await send(
            chat,
            "🔁 No revision due. It "
            "appears automatically "
            "once you log questions "
            "— accuracy decides when "
            "it returns.", MENU_KB)
        return
    now = now_tz()
    lines = ["🔁 <b>REVISION QUEUE"
             "</b>", ""]
    for r in rows:
        od = max(0,
                 (now
                  - r["due_at"]).days)
        mode = ""
        if r["mode"] == "practice":
            mode = " 🧠practice"
        over = ""
        if od:
            over = (" (overdue "
                    + str(od)
                    + "d) 🔴")
        rt = r["rev_type"].replace(
            "_", " ")
        mast = ""
        if r["mastery"] is not None:
            mast = (" — "
                    + format(
                        r["mastery"],
                        ".0f")
                    + "%")
        lines.append(
            "• "
            + esc(r["subj"] or "")
            + " / "
            + esc(r["top"] or "")
            + mast
            + " — " + rt + over
            + mode)
    await send(chat,
               "\n".join(lines),
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
           GROUP BY m.mtype,
                    s.name, t.name
           ORDER BY c DESC
           LIMIT 12""", u["id"])
    if not rows:
        await send(
            chat,
            "🧨 No mistakes banked. "
            "Report: '3 conceptual "
            "mistakes in rotation'.",
            MENU_KB)
        return
    lines = ["🧨 <b>MISTAKE BANK"
             "</b>", ""]
    for r in rows:
        lines.append(
            "• "
            + esc(r["subj"] or "")
            + " / "
            + esc(r["top"] or "")
            + " — "
            + r["mtype"]
            + " × "
            + str(r["c"]))
    lines += ["",
              "<i>2+ repeats = it "
              "takes over your "
              "revision. Say 'fixed "
              "my X mistakes' to "
              "clear.</i>"]
    await send(chat,
               "\n".join(lines),
               MENU_KB)


async def cmd_classes(u, chat):
    cls = await get_classes(u["id"])
    extras = await qrows(
        """SELECT * FROM extra_events
           WHERE user_id=$1::uuid
           AND on_date >=
             current_date - 1
           ORDER BY on_date
           LIMIT 8""", u["id"])
    if not cls and not extras:
        await send(
            chat,
            "🗓 No classes yet. Add: "
            "'physics class every "
            "mon and wed 5-7'.",
            MENU_KB)
        return
    lines = ["🗓 <b>TIMETABLE</b>",
             ""]
    names = ["Mon", "Tue", "Wed",
             "Thu", "Fri", "Sat",
             "Sun"]
    for c in cls:
        dstr = ", ".join(
            names[d]
            for d in (c["weekdays"]
                      or []))
        lines.append(
            "• <b>"
            + esc(c["title"])
            + "</b> "
            + c["start_time"]
            .strftime("%H:%M")
            + "–"
            + c["end_time"]
            .strftime("%H:%M")
            + " (" + dstr + ")")
    for e in extras:
        e = dict(e)
        lines.append(
            "• <b>"
            + esc(e["title"])
            + "</b> "
            + e["on_date"].strftime(
                "%a %d %b")
            + " "
            + e["start_time"]
            .strftime("%H:%M")
            + "–"
            + e["end_time"]
            .strftime("%H:%M")
            + " (extra)")
    lines += ["",
              "<i>'no class "
              "tomorrow' cancels "
              "all classes that "
              "day.</i>"]
    await send(chat,
               "\n".join(lines),
               MENU_KB)


async def cmd_syllabus(u, chat,
                       subject_id=None):
    if subject_id:
        rows = await qrows(
            """SELECT c.id, c.name,
                      AVG(m.mastery)
                        mast
               FROM chapters c
               LEFT JOIN topics t
                 ON t.chapter_id=c.id
               LEFT JOIN mastery m
                 ON m.topic_id=t.id
                 AND m.user_id=c.user_id
               WHERE c.user_id=$1::uuid
               AND c.subject_id=$2::uuid
               GROUP BY c.id, c.name
               ORDER BY c.position""",
            u["id"], subject_id)
        sname = await qval(
            """SELECT name
               FROM subjects
               WHERE id=$1::uuid""",
            subject_id)
        if not rows:
            await send(
                chat,
                "📚 No chapters in "
                + esc(sname or "")
                + " yet. Send a "
                "syllabus photo/PDF.",
                MENU_KB)
            return
        lines = ["📚 <b>"
                 + esc(sname or "")
                 + "</b>", ""]
        for r in rows:
            mast = r["mast"]
            if mast is None:
                ic = "⚪"
            elif mast < 40:
                ic = "🔴"
            elif mast < 60:
                ic = "🟠"
            elif mast < 80:
                ic = "🟡"
            else:
                ic = "✅"
            lines.append(ic + " "
                         + esc(r["name"]))
        kb = []
        for r in rows[:6]:
            data = ("syl:"
                    + str(r["id"]))
            kb.append(
                [(r["name"][:24],
                  data)])
        kb.append([("⬅️ Back",
                    "nav:syl")])
        await send(chat,
                   "\n".join(lines),
                   IK(*kb))
        return
    rows = await qrows(
        """SELECT s.id, s.name,
                  COUNT(DISTINCT c.id)
                    nch,
                  COUNT(DISTINCT t.id)
                    ntop,
                  (SELECT COUNT(*)
                   FROM mastery m
                   WHERE m.user_id=
                       s.user_id
                   AND m.subject_id=s.id
                   AND m.events>0)
                     tracked
           FROM subjects s
           LEFT JOIN chapters c
             ON c.subject_id=s.id
           LEFT JOIN topics t
             ON t.subject_id=s.id
           WHERE s.user_id=$1::uuid
           GROUP BY s.id, s.name
           ORDER BY s.name""",
        u["id"])
    if not rows:
        await send(
            chat,
            "📚 No subjects yet. "
            "Send a syllabus photo "
            "or PDF, or type:\n"
            "<code>Physics: Rotation, "
            "SHM</code>", MENU_KB)
        return
    lines = ["📚 <b>SYLLABUS</b>",
             ""]
    kb = []
    for r in rows:
        lines.append(
            "• <b>"
            + esc(r["name"])
            + "</b> — "
            + str(r["nch"])
            + " chapters, "
            + str(r["ntop"])
            + " topics, "
            + str(r["tracked"])
            + " with data")
        data = ("syls:"
                + str(r["id"]))
        kb.append([(r["name"],
                    data)])
    kb.append([("🏠 Menu", "nav:menu")])
    await send(chat,
               "\n".join(lines),
               IK(*kb))


async def cmd_chapter_topics(
        u, chat, chapter_id):
    rows = await qrows(
        """SELECT t.id, t.name,
                  m.mastery,
                  m.attempts,
                  m.events,
                  m.last_evidence,
                  rv.due_at
           FROM topics t
           LEFT JOIN mastery m
             ON m.user_id=t.user_id
             AND m.topic_id=t.id
           LEFT JOIN revisions rv
             ON rv.user_id=t.user_id
             AND rv.topic_id=t.id
           WHERE t.user_id=$1::uuid
           AND t.chapter_id=$2::uuid
           ORDER BY t.position""",
        u["id"], chapter_id)
    chname = await qval(
        """SELECT name
           FROM chapters
           WHERE id=$1::uuid""",
        chapter_id)
    if not rows:
        await send(chat,
                   "No topics in this "
                   "chapter yet.",
                   MENU_KB)
        return
    lines = ["📘 <b>"
             + esc(chname or "")
             + "</b>", ""]
    for r in rows:
        mast = r["mastery"]
        if mast is None:
            ic = "⚪"
        elif mast < 40:
            ic = "🔴"
        elif mast < 60:
            ic = "🟠"
        elif mast < 80:
            ic = "🟡"
        else:
            ic = "✅"
        line = (ic + " "
                + esc(r["name"]))
        if mast is not None:
            line += (" — "
                     + format(mast,
                              ".0f")
                     + "%")
        lines.append(line)
    lines += ["",
              "<i>Tap a topic for "
              "detail.</i>"]
    kb = []
    for r in rows[:8]:
        data = ("an:t:"
                + str(r["id"]))
        kb.append(
            [(r["name"][:24], data)])
    kb.append([("⬅️ Back", "nav:syl")])
    await send(chat,
               "\n".join(lines),
               IK(*kb))


async def cmd_notes(u, chat):
    rows = await qrows(
        """SELECT id, title
           FROM resources
           WHERE user_id=$1::uuid
           ORDER BY created_at
             DESC
           LIMIT 10""", u["id"])
    if not rows:
        await send(
            chat,
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
        kb_rows.append(
            [(label, data)])
    await send(chat,
               "📎 <b>YOUR NOTES</b>",
               IK(*kb_rows))


async def gather_stats(u, days=7):
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
                    SUM(
                     duration_minutes),
                    0) mins,
                  COALESCE(
                    SUM(
                     questions_attempted),
                    0) q,
                  COALESCE(
                    SUM(
                     questions_correct),
                    0) c
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >= $2
           AND created_at < $3""",
        u["id"], wk_start, wk_end)
    prev = await qval(
        """SELECT COALESCE(
                    SUM(
                     duration_minutes),
                    0)
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >= $2
           AND created_at < $3""",
        u["id"], prev_start,
        wk_start)
    if not r:
        return None
    mins = int(r["mins"])
    qn = int(r["q"])
    cn = int(r["c"])
    if mins == 0 and qn == 0:
        return None
    acc = (100.0 * cn / qn
           if qn else 0)
    subs = await qrows(
        """SELECT s.name,
                  SUM(ss
                    .questions_attempted)
                    q,
                  SUM(ss
                    .questions_correct)
                    c
           FROM study_sessions ss
           JOIN subjects s
             ON s.id=ss.subject_id
           WHERE ss.user_id=$1::uuid
           AND ss.status='finished'
           AND ss.created_at >= $2
           AND ss.created_at < $3
           AND ss
             .questions_attempted
             > 0
           GROUP BY s.name""",
        u["id"], wk_start, wk_end)
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
    dates = await qrows(
        """SELECT DISTINCT
                  created_at::date d
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND (duration_minutes
                > 0
                OR
                questions_attempted
                > 0)
           ORDER BY d DESC
           LIMIT 90""", u["id"])
    streak = 0
    if dates:
        ds = {r2["d"]
              for r2 in dates}
        cur = today
        if today not in ds:
            cur = today - timedelta(
                days=1)
        while cur in ds:
            streak += 1
            cur -= timedelta(days=1)
    rep = await qrows(
        """SELECT t.name top,
                  SUM(m.count) c
           FROM mistakes m
           JOIN topics t
             ON t.id=m.topic_id
           WHERE m.user_id=$1::uuid
           AND m.resolved=false
           GROUP BY t.name
           ORDER BY c DESC
           LIMIT 3""", u["id"])
    hw_total = await qval(
        """SELECT COUNT(*)
           FROM homework
           WHERE user_id=$1::uuid""",
        u["id"])
    hw_done = await qval(
        """SELECT COUNT(*)
           FROM homework
           WHERE user_id=$1::uuid
           AND status='completed'""",
        u["id"])
    rev_done = await qval(
        """SELECT COUNT(*)
           FROM revisions
           WHERE user_id=$1::uuid
           AND last_reviewed > $2""",
        u["id"], wk_start)
    rev_due = await qval(
        """SELECT COUNT(*)
           FROM revisions
           WHERE user_id=$1::uuid
           AND due_at < now()""",
        u["id"])
    return {
        "days": days, "mins": mins,
        "q": qn, "c": cn, "acc": acc,
        "prev_mins": int(prev or 0),
        "subs": [dict(s) for s in subs],
        "mast": [dict(m) for m in mast],
        "streak": streak,
        "rep": [dict(x) for x in rep],
        "hw_total": int(hw_total or 0),
        "hw_done": int(hw_done or 0),
        "rev_done": int(rev_done or 0),
        "rev_due": int(rev_due or 0)}


async def gather_high(u):
    out = {}
    row = await qrow(
        """SELECT COUNT(*) n,
                  COALESCE(
                    AVG(
                      duration_minutes),
                    0) avgm
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >
             now() - interval '7 days'
           AND duration_minutes
             > 0""",
        u["id"])
    if row:
        out["sessions"] = int(
            row["n"] or 0)
        out["avg_session"] = int(
            row["avgm"] or 0)
    v = await qval(
        """SELECT COUNT(
                    DISTINCT
                    created_at::date)
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >
             now() - interval '7 days'""",
        u["id"])
    out["study_days"] = int(v or 0)
    rows = await qrows(
        """SELECT s.name subj,
                  AVG(m.mastery)
                    now_m
           FROM mastery m
           JOIN topics t
             ON t.id=m.topic_id
           JOIN subjects s
             ON s.id=t.subject_id
           WHERE m.user_id=$1::uuid
           AND m.events>0
           GROUP BY s.name""",
        u["id"])
    now_map = {}
    for r in rows:
        now_map[r["subj"]] = \
            float(r["now_m"])
    rows = await qrows(
        """SELECT s.name subj,
                  AVG(ml.mastery)
                    old_m
           FROM mastery_log ml
           JOIN topics t
             ON t.id=ml.topic_id
           JOIN subjects s
             ON s.id=t.subject_id
           WHERE ml.user_id=$1::uuid
           AND ml.recorded_at <
             now() - interval '6 days'
           GROUP BY s.name""",
        u["id"])
    deltas = []
    for r in rows:
        subj = r["subj"]
        if subj in now_map:
            old = float(r["old_m"])
            d = round(
                now_map[subj] - old, 1)
            deltas.append((subj, d))
    out["mastery_delta"] = deltas
    rows = await qrows(
        """SELECT name,
                  obtained_marks om,
                  total_marks tm
           FROM tests
           WHERE user_id=$1::uuid
           AND status='completed'
           AND total_marks > 0
           ORDER BY test_at DESC
           LIMIT 7""",
        u["id"])
    trend = []
    for r in reversed(rows):
        pct = (100.0
               * (r["om"] or 0)
               / r["tm"])
        label = (r["name"]
                 or "")[:12]
        trend.append(
            (round(pct, 0), label))
    out["test_trend"] = trend
    return out


async def cmd_analytics(u, chat,
                        days=7):
    s = await gather_stats(u, days)
    if s is None:
        await send(
            chat,
            "📊 Not enough data yet "
            "— log a few sessions "
            "first.", MENU_KB)
        return
    ref = max(
        s["days"] * 150,
        int(s["prev_mins"]
            * 120 // 100))
    if s["mins"] >= s["prev_mins"]:
        trend = "↑"
    else:
        trend = "↓"
    qref = s["days"] * 600 // 7
    lines = [
        "📊 <b>LAST "
        + str(s["days"]) + " DAYS"
        "</b>",
        "",
        "Study      "
        + bar(min(100,
                  100 * s["mins"]
                  / ref))
        + " " + fm(s["mins"]) + " "
        + trend,
        "Questions  "
        + bar(min(100,
                  100 * s["q"]
                  / qref))
        + " " + str(s["q"]),
        "Accuracy   "
        + bar(s["acc"]) + " "
        + format(s["acc"], ".0f")
        + "%",
        "Homework   "
        + str(s["hw_done"]) + "/"
        + str(s["hw_total"])
        + " done",
        "Revision   "
        + str(s["rev_done"])
        + " done · "
        + str(s["rev_due"])
        + " due"]
    if s["streak"]:
        lines += ["",
                  "🔥 <b>"
                  + str(s["streak"])
                  + "-day streak</b>"]
    hi = await gather_high(u)
    if hi:
        if hi.get("avg_session"):
            lines.append(
                "Avg session: "
                + fm(hi["avg_session"]))
        if hi.get("study_days"):
            lines.append(
                "Active days: "
                + str(hi["study_days"])
                + "/7")
        md = hi.get("mastery_delta") or []
        if md:
            lines += ["",
                      "🧠 <b>MASTERY "
                      "7-DAY MOVE</b>"]
            for subj, d in md[:5]:
                sign = ""
                if d >= 0:
                    sign = "+"
                lines.append(
                    "• "
                    + esc(subj)
                    + " " + arrow(d)
                    + sign
                    + format(d, ".1f"))
        tt = hi.get("test_trend") or []
        if tt:
            lines += ["",
                      "📈 <b>TEST "
                      "TREND</b>"]
            for pct, label in tt[:7]:
                lines.append(
                    format(pct, "2.0f")
                    + "% "
                    + bar(pct, 8)
                    + " "
                    + esc(label))
    rows = await qrows(
        """SELECT s.id sid,
                  s.name,
                  AVG(m.mastery) mast
           FROM topics t
           JOIN subjects s
             ON s.id=t.subject_id
           JOIN mastery m
             ON m.topic_id=t.id
             AND m.user_id=t.user_id
           WHERE t.user_id=$1::uuid
           AND m.events > 0
           GROUP BY s.id, s.name
           ORDER BY AVG(m.mastery)
             ASC""",
        u["id"])
    kb = []
    if rows:
        lines += ["",
                  "🧠 <b>MASTERY</b>"]
        for r in rows[:6]:
            nm = r["name"]
            lines.append(
                nm.ljust(11) + " "
                + bar(r["mast"], 10)
                + " "
                + format(r["mast"],
                         ".0f") + "%")
            data = ("an:s:"
                    + str(r["sid"]))
            kb.append([(nm, data)])
    lines += ["",
              "<i>Tap a subject to "
              "drill down · "
              "/insights for AI "
              "deep dive.</i>"]
    kb.append(
        [("7 days", "anp:7"),
         ("30 days", "anp:30")])
    kb.append([("🏠 Menu", "nav:menu")])
    await send(chat,
               "\n".join(lines),
               IK(*kb))


async def cmd_subject_an(
        u, chat, sid):
    sname = await qval(
        """SELECT name
           FROM subjects
           WHERE id=$1::uuid""",
        sid)
    rows = await qrows(
        """SELECT c.id, c.name,
                  AVG(m.mastery) mast
           FROM chapters c
           LEFT JOIN topics t
             ON t.chapter_id=c.id
           LEFT JOIN mastery m
             ON m.topic_id=t.id
             AND m.user_id=c.user_id
           WHERE c.user_id=$1::uuid
           AND c.subject_id=$2::uuid
           GROUP BY c.id, c.name
           ORDER BY AVG(m.mastery)
             ASC NULLS LAST""",
        u["id"], sid)
    if not rows:
        await send(chat,
                   "No chapters with "
                   "data yet.",
                   MENU_KB)
        return
    lines = ["📊 <b>"
             + esc(sname or "")
             + "</b>", ""]
    for r in rows[:12]:
        mast = r["mast"]
        if mast is None:
            txt = "⚪ no data"
        else:
            txt = (bar(mast, 10)
                   + " "
                   + format(mast,
                            ".0f")
                   + "%")
        lines.append(
            r["name"].ljust(22)
            + txt)
    kb = []
    for r in rows[:6]:
        data = ("an:c:"
                + str(r["id"]))
        kb.append(
            [(r["name"][:24], data)])
    kb.append([("⬅️ Analytics",
                "nav:an")])
    await send(chat,
               "\n".join(lines),
               IK(*kb))


async def cmd_chapter_an(
        u, chat, cid):
    rows = await qrows(
        """SELECT t.id, t.name,
                  m.mastery,
                  m.attempts,
                  m.correct,
                  m.last_evidence,
                  rv.due_at
           FROM topics t
           LEFT JOIN mastery m
             ON m.user_id=t.user_id
             AND m.topic_id=t.id
           LEFT JOIN revisions rv
             ON rv.user_id=t.user_id
             AND rv.topic_id=t.id
           WHERE t.user_id=$1::uuid
           AND t.chapter_id=$2::uuid
           ORDER BY m.mastery ASC
             NULLS LAST
           LIMIT 15""",
        u["id"], cid)
    ch = await qrow(
        """SELECT name
           FROM chapters
           WHERE id=$1::uuid""",
        cid)
    if not rows:
        await send(chat,
                   "No topics here.",
                   MENU_KB)
        return
    chname = (ch["name"]
              if ch else "Chapter")
    lines = ["📊 <b>"
             + esc(chname)
             + "</b>", ""]
    kb = []
    for r in rows[:10]:
        mast = r["mastery"]
        if mast is None:
            txt = "⚪"
        else:
            txt = (format(mast,
                          ".0f")
                   + "%")
        line = (esc(r["name"])
                + " — " + txt)
        if r["due_at"]:
            d = (r["due_at"]
                 - now_tz()).days
            line += (" · rev "
                     + str(d) + "d")
        lines.append("• " + line)
        data = ("an:t:"
                + str(r["id"]))
        kb.append(
            [(r["name"][:24], data)])
    kb.append([("⬅️ Back", "nav:an")])
    await send(chat,
               "\n".join(lines),
               IK(*kb))


async def cmd_topic_an(
        u, chat, tid):
    r = await qrow(
        """SELECT t.name top,
                  s.name subj,
                  m.mastery,
                  m.attempts,
                  m.correct,
                  m.events,
                  m.last_evidence,
                  rv.due_at,
                  rv.interval_days
           FROM topics t
           JOIN subjects s
             ON s.id=t.subject_id
           LEFT JOIN mastery m
             ON m.user_id=t.user_id
             AND m.topic_id=t.id
           LEFT JOIN revisions rv
             ON rv.user_id=t.user_id
             AND rv.topic_id=t.id
           WHERE t.id=$1::uuid""",
        tid)
    if not r:
        return
    reps = await mistake_reps(
        u["id"], tid)
    lines = ["📊 <b>"
             + esc(r["subj"])
             + " — "
             + esc(r["top"])
             + "</b>", ""]
    if r["mastery"] is None:
        lines.append("Mastery: no "
                     "data yet")
    else:
        lines.append(
            "Mastery: "
            + bar(r["mastery"], 10)
            + " "
            + format(r["mastery"],
                     ".0f") + "%")
        att = r["attempts"] or 0
        cor = r["correct"] or 0
        acc = 0
        if att:
            acc = 100 * cor / att
        lines.append(
            "Accuracy: "
            + format(acc, ".0f")
            + "% (" + str(att)
            + " questions, "
            + str(r["events"])
            + " sessions)")
    if r["last_evidence"]:
        d = (now_tz().date()
             - r["last_evidence"]
             .date()).days
        lines.append(
            "Last evidence: "
            + str(d) + "d ago")
    if r["due_at"]:
        dd = (r["due_at"]
              - now_tz()).days
        lines.append(
            "Next revision: "
            + ("today"
               if dd <= 0
               else "in "
               + str(dd) + "d")
            + " (interval "
            + str(r["interval_days"])
            + "d)")
    if reps:
        lines.append(
            "🧨 repeated mistakes: "
            + str(reps))
    await send(chat,
               "\n".join(lines),
               IK([("🧠 Quiz me",
                    "quizt:" + tid),
                   ("⬅️ Back",
                    "nav:an")]))


async def cmd_insights(u, chat):
    s = await gather_stats(u, 7)
    if s is None:
        await send(chat,
                   "📊 Not enough data "
                   "yet — log a few "
                   "sessions first.",
                   MENU_KB)
        return
    hi = await gather_high(u)
    data = {
        "week_minutes": s["mins"],
        "prev_week_minutes":
            s["prev_mins"],
        "questions": s["q"],
        "accuracy_pct":
            round(s["acc"], 1),
        "streak_days": s["streak"],
        "study_days_7d":
            hi.get("study_days", 0),
        "sessions_7d":
            hi.get("sessions", 0),
        "avg_session_minutes":
            hi.get("avg_session", 0),
        "homework_done":
            s["hw_done"],
        "homework_total":
            s["hw_total"],
        "revision_done_7d":
            s["rev_done"],
        "revision_due_now":
            s["rev_due"],
        "mastery_change":
            hi.get("mastery_delta", []),
        "weakest_topics": [
            m["subj"] + " / "
            + m["top"]
            for m in s["mast"][:5]],
        "test_trend_pct": [
            t[0] for t in
            hi.get("test_trend", [])]}
    mem = await memory_block(u)
    key = ("ins:"
           + hashlib.md5(
               json.dumps(
                   data,
                   sort_keys=True)
               .encode()
           ).hexdigest())
    lines = ["📊 <b>DEEP "
             "INSIGHTS</b>", ""]
    try:
        txt = _cache_get(key)
        if txt is None:
            prompt = (
                "You are the "
                "analytics engine "
                "of a study app. "
                "Write a deep but "
                "concise insight "
                "report (max 110 "
                "words) from ONLY "
                "these exact stats: "
                + json.dumps(data)
                + "\n"
                + mem
                + "\nCover: workload "
                "trend, accuracy, "
                "mastery movement, "
                "and ONE concrete "
                "focus for next "
                "week. Never invent "
                "numbers. Plain "
                "text, address the "
                "student directly.")
            txt = await ai_call(
                prompt,
                json_mode=False,
                max_tokens=350)
            _cache_set(key, txt)
        lines.append(
            esc(txt.strip()))
        lines.append("")
    except AIError:
        pass
    if hi.get("avg_session"):
        lines.append("Avg session: "
                     + fm(hi[
                         "avg_session"
                     ]))
    if hi.get("study_days"):
        lines.append("Active days: "
                     + str(hi[
                         "study_days"
                     ]) + "/7")
    md = hi.get("mastery_delta") or []
    if md:
        lines += ["",
                  "🧠 <b>MASTERY "
                  "7-DAY MOVE</b>"]
        for subj, d in md[:5]:
            sign = ""
            if d >= 0:
                sign = "+"
            lines.append(
                "• " + esc(subj)
                + " " + arrow(d)
                + sign
                + format(d, ".1f"))
    tt = hi.get("test_trend") or []
    if tt:
        lines += ["",
                  "📈 <b>TEST "
                  "TREND</b>"]
        for pct, label in tt[:7]:
            lines.append(
                format(pct, "2.0f")
                + "% "
                + bar(pct, 8)
                + " "
                + esc(label))
    await send(chat,
               "\n".join(lines),
               IK([("📊 Analytics",
                    "nav:an")],
                  [("🏠 Menu",
                    "nav:menu")]))


async def cmd_review(u, chat):
    s = await gather_stats(u, 7)
    if s is None:
        await send(chat,
                   "📊 Not enough data "
                   "for a review yet — "
                   "log a few sessions "
                   "first.", MENU_KB)
        return
    data = {
        "study_minutes": s["mins"],
        "prev_week_minutes":
            s["prev_mins"],
        "questions": s["q"],
        "accuracy_pct":
            round(s["acc"], 1),
        "streak_days": s["streak"],
        "weakest_topics": [
            {"subject": m["subj"],
             "topic": m["top"],
             "mastery": m["mastery"]}
            for m in s["mast"][:5]],
        "repeated_errors": [
            {"topic": r["top"],
             "count": r["c"]}
            for r in s["rep"]],
        "homework_done":
            s["hw_done"],
        "homework_total":
            s["hw_total"],
        "revision_done":
            s["rev_done"]}
    key = ("rv:"
           + hashlib.md5(
               json.dumps(
                   data,
                   sort_keys=True
               ).encode()
           ).hexdigest())
    try:
        txt = _cache_get(key)
        if txt is None:
            prompt = (
                "Write a weekly study "
                "review for this "
                "student from these "
                "EXACT stats (never "
                "invent numbers): "
                + json.dumps(data)
                + "\nStructure: one "
                "strength, one "
                "concern, one focus "
                "for next week. Max "
                "90 words. Plain "
                "text, warm but "
                "honest.")
            txt = await ai_call(
                prompt,
                json_mode=False,
                max_tokens=300)
            _cache_set(key, txt)
        lines = ["📋 <b>WEEKLY "
                 "REVIEW</b>", "",
                 esc(txt.strip()),
                 "",
                 "Study " + fm(s["mins"])
                 + " • "
                 + str(s["q"])
                 + " questions • "
                 + format(s["acc"],
                          ".0f")
                 + "% accuracy"
                 + (" • 🔥 "
                    + str(s["streak"])
                    + "d streak"
                    if s["streak"]
                    else "")]
        await send(chat,
                   "\n".join(lines),
                   MENU_KB)
    except AIError:
        await cmd_analytics(
            u, chat, days=7)


async def cmd_dayreview(u, chat):
    today = today_d()
    plan = await qrow(
        """SELECT available_minutes,
                  tasks
           FROM daily_plans
           WHERE user_id=$1::uuid
           AND plan_date=$2""",
        u["id"], today)
    actual = await spent_today(
        u, today)
    planned = 0
    if plan:
        tasks = jd(plan["tasks"], [])
        planned = sum(
            t.get("est", 0)
            for t in tasks)
    r = await qrow(
        """SELECT COALESCE(
                    SUM(
                     questions_attempted),
                    0) q,
                  COALESCE(
                    SUM(
                     questions_correct),
                    0) c
           FROM study_sessions
           WHERE user_id=$1::uuid
           AND status='finished'
           AND created_at >= $2
           AND created_at < $3""",
        u["id"], sod(today),
        sod(today)
        + timedelta(days=1))
    qn = int(r["q"] or 0)
    cn = int(r["c"] or 0)
    acc = (100 * cn / qn
           if qn else 0)
    lines = ["🌙 <b>DAY REVIEW"
             "</b>",
             today.strftime(
                 "%A, %d %b"),
             "",
             "Planned:  "
             + fm(planned),
             "Actual:   "
             + fm(actual),
             "Questions: "
             + str(qn),
             "Accuracy: "
             + format(acc, ".0f")
             + "%",
             "",
             "How was your energy "
             "today?"]
    await send(chat,
               "\n".join(lines),
               IK([("🔥 Great",
                    "env:energetic"),
                   ("🙂 Okay",
                    "env:normal"),
                   ("😴 Tired",
                    "env:tired")]))


async def cmd_doctor(u, chat):
    lines = ["🩺 <b>StudyOS Doctor"
             "</b>", ""]
    ok_all = True
    try:
        await qval("SELECT 1")
        lines.append("✅ Database: "
                     "connected")
        n = await qval(
            """SELECT COUNT(*)
               FROM study_sessions
               WHERE user_id=
                 $1::uuid""",
            u["id"])
        hw = await qval(
            """SELECT COUNT(*)
               FROM homework
               WHERE user_id=$1::uuid
               AND status NOT IN
                 ('completed',
                  'abandoned')""",
            u["id"])
        rv = await qval(
            """SELECT COUNT(*)
               FROM revisions
               WHERE user_id=$1::uuid
               AND due_at < now()""",
            u["id"])
        lines.append(
            "   sessions: "
            + str(n)
            + " • open hw: "
            + str(hw)
            + " • rev due: "
            + str(rv))
    except Exception as e:
        ok_all = False
        lines.append("❌ Database: "
                     + esc(str(e)[:120]))
    lines.append("🕒 Time: "
                 + now_tz().strftime(
                     "%d %b %Y %H:%M")
                 + " (" + TZ_NAME + ")")
    lines.append("👤 Onboarded: "
                 + ("yes"
                    if u["onboarded"]
                    else "NO — /start"))
    lines.append("🤖 Model: "
                 + GEMINI_MODEL)
    try:
        import time as _t
        t0 = _t.monotonic()
        await ai_call(
            "Reply with exactly: OK",
            json_mode=False,
            max_tokens=10)
        lines.append("✅ Gemini: "
                     + str(int(
                         (_t.monotonic()
                          - t0) * 1000))
                     + "ms")
    except Exception as e:
        ok_all = False
        lines.append("❌ Gemini: "
                     + esc(str(e)[:120]))
    lines += ["",
              ("ALL SYSTEMS GO ✅"
               if ok_all
               else "Needs "
                    "attention ⚠️")]
    await send(chat,
               "\n".join(lines),
               MENU_KB)


async def cmd_export(u, chat):
    data = {}
    tables = ("users", "subjects",
              "chapters", "topics",
              "study_sessions",
              "homework", "tests",
              "mistakes", "revisions",
              "mastery", "classes",
              "extra_events",
              "resources",
              "daily_plans",
              "ai_memory")
    for tname in tables:
        try:
            rows = await qrows(
                "SELECT * FROM "
                + tname
                + " WHERE user_id="
                  "$1::uuid", u["id"])
            data[tname] = [
                {k: str(v)
                 for k, v in
                 dict(r).items()}
                for r in rows]
        except Exception:
            data[tname] = []
    blob = json.dumps(
        data, indent=1).encode()
    ok = await send_document(
        chat, "studyos_export.json",
        blob)
    if not ok:
        await send(chat,
                   "Export failed — "
                   "try again.",
                   MENU_KB)


async def cmd_settings(u, chat):
    lines = ["⚙️ <b>SETTINGS</b>",
             ""]
    nm = (u["display_name"]
          or u["first_name"])
    lines.append(
        "👤 "
        + esc(nm or "Student"))
    goal = esc(u["exam_goal"]
               or "—")
    if u["exam_date"]:
        days = (u["exam_date"]
                - today_d()).days
        goal += (" ("
                 + str(days)
                 + "d away)")
    lines.append("🎯 " + goal)
    lines.append("🕒 " + TZ_NAME)
    lines.append("🤖 " + GEMINI_MODEL)
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
            + u["school_start"]
            .strftime("%H:%M")
            + "–"
            + u["school_end"]
            .strftime("%H:%M"))
    if u["coaching_days"]:
        lines.append(
            "📖 "
            + u["coaching_start"]
            .strftime("%H:%M")
            + "–"
            + u["coaching_end"]
            .strftime("%H:%M"))
    lines.append(
        "🍱 Meals: "
        + str(u["meal_minutes"])
        + "m • 🚌 Commute: "
        + str(u["commute_minutes"])
        + "m")
    lines.append(
        "⚡ Energy: "
        + u["energy"])
    lines += ["",
              "<i>Change by chat: "
              "'wake at 6', 'school "
              "8-2 mon-sat', 'exam "
              "on 24 may 2027'.</i>"]
    await send(chat,
               "\n".join(lines),
               IK([("💾 Export data",
                    "nav:export")],
                  [("🔄 Restart "
                    "onboarding",
                    "ob:restart")],
                  [("🧨 WIPE ALL "
                    "DATA",
                    "reset:ask")]))


# ---- memory views ----
async def cmd_memories(u, chat):
    rows = await get_memories(u)
    if not rows:
        await send(chat,
                   "🧠 No memories yet. "
                   "Say 'remember that "
                   "...' to teach me "
                   "something about "
                   "you.",
                   MENU_KB)
        return
    lines = ["🧠 <b>WHAT I "
             "REMEMBER</b>", ""]
    for r in rows:
        lines.append("• "
                     + esc(
                         r["content"]))
    kb = []
    for r in rows[:8]:
        label = "🗑 " \
                + (r["content"]
                   or "")[:18]
        data = ("mem:del:"
                + str(r["id"]))
        kb.append([(label, data)])
    await send(chat,
               "\n".join(lines),
               IK(*kb))


async def do_remember(u, chat, f):
    content = (f.get("content")
               or f.get("fact")
               or f.get("memory"))
    if not content:
        await send(chat,
                   "Remember what? "
                   "e.g. 'remember "
                   "that I study best "
                   "in the morning'.")
        return
    content = str(content).strip()
    mid = await save_memory(
        u, content)
    if not mid:
        await send(chat,
                   "Couldn't save "
                   "that — try "
                   "rephrasing.")
        return
    btn = "mem:del:" + str(mid)
    await send(
        chat,
        "🧠 I'll remember:\n<i>"
        + esc(content[:300])
        + "</i>\n\nThis shapes "
        "my advice, plans and "
        "answers.",
        IK([("🗑 Forget", btn)]))


async def do_forget(u, chat, f):
    content = (f.get("content")
               or "").strip()
    if not content:
        res = await q(
            """UPDATE ai_memory
               SET active=false
               WHERE user_id=
                 $1::uuid
               AND active=true""",
            u["id"])
        cnt = 0
        m = re.search(
            r"UPDATE (\d+)",
            res or "")
        if m:
            cnt = int(m.group(1))
        await send(chat,
                   "🗑 Cleared "
                   + str(cnt)
                   + " memories.",
                   MENU_KB)
        return
    res = await q(
        """UPDATE ai_memory
           SET active=false
           WHERE user_id=$1::uuid
           AND active=true
           AND content ILIKE $2""",
        u["id"],
        "%" + content + "%")
    cnt = 0
    m = re.search(r"UPDATE (\d+)",
                  res or "")
    if m:
        cnt = int(m.group(1))
    await send(chat,
               "🗑 Forgot "
               + str(cnt)
               + " matching "
               "memor(ies).",
               MENU_KB)


# ---- quiz ----
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
        label = (r["subj"]
                 + " — "
                 + r["top"])
        return (label,
                r["subj"],
                r["top"])
    r = await qrow(
        """SELECT t.name top,
                  s.name subj
           FROM topics t
           JOIN subjects s
             ON s.id=t.subject_id
           WHERE t.user_id=$1::uuid
           LIMIT 1""", u["id"])
    if r:
        label = (r["subj"]
                 + " — "
                 + r["top"])
        return (label,
                r["subj"],
                r["top"])
    return ("mixed revision",
            None, None)


async def start_quiz(
        u, chat, topic_text,
        count=5, subject_id=None,
        topic_id=None,
        topic_label=None,
        flavor=None):
    label = (topic_label
             or topic_text)
    if not label:
        label, sname, tname = \
            await quiz_label(u)
        if sname and tname:
            subs = await get_subjects(
                u["id"])
            sid, _ = S.resolve_subject(
                sname, subs)
            if sid:
                if not subject_id:
                    subject_id = sid
                t = await find_topic(
                    u["id"], sid, tname)
                if t and not topic_id:
                    topic_id = t["id"]
    count = max(1, min(8, count))
    goal = (u.get("exam_goal")
            or "competitive exam")
    style = flavor or "exam-level"
    prompt = (
        "Generate " + str(count)
        + " multiple-choice "
        "questions on \""
        + label + "\" for a "
        + goal + " student. "
        "Return ONLY JSON: "
        + '{"questions":'
        + '[{"question":"...",'
        + '"options":'
        + '["a","b","c","d"],'
        + '"answer":0,'
        + '"explanation":'
        + '"short why"}]} '
        + "Rules: exactly 4 "
        "options; \"answer\" is "
        "the 0-based index of "
        "the correct option; "
        "exactly one correct; "
        "no trick options; "
        + style + " difficulty.")
    key = ("qz:"
           + hashlib.md5(
               prompt.encode()
           ).hexdigest())
    try:
        raw = _cache_get(key)
        if raw is None:
            from brain import _load_json
            raw = _load_json(
                await ai_call(
                    prompt,
                    max_tokens=(
                        200 * count)))
            _cache_set(key, raw)
    except AIError:
        await send(
            chat,
            "🧠 My quiz brain is "
            "rate-limited — try "
            "again in a minute.",
            MENU_KB)
        return
    raw = jd(raw, {})
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
                  and x.get(
                      "question"))
            if ok:
                qs.append({
                    "question":
                        str(x[
                            "question"
                        ])[:400],
                    "options": opts,
                    "answer": ans,
                    "explanation":
                        str(x.get(
                            "explanation",
                            ""))[:400]})
        except Exception:
            continue
    if len(qs) < 3:
        await send(
            chat,
            "🧠 Couldn't build a "
            "clean quiz for that "
            "— try a more "
            "specific topic.",
            MENU_KB)
        return
    if not topic_id and topic_text:
        subs = await get_subjects(
            u["id"])
        sid = subject_id
        if not sid \
                and len(subs) == 1:
            sid = subs[0]["id"]
        if sid:
            t = await find_topic(
                u["id"], sid,
                topic_text)
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
           VALUES($1::uuid,
                  $2::uuid,
                  $3::uuid,$4,
                  $5::jsonb)
           RETURNING id::text""",
        u["id"], subject_id,
        topic_id, label,
        json.dumps(qs))
    await send_quiz_q(
        chat, qid, qs, 0, label)


async def send_quiz_q(
        chat, qid, qs, idx, label):
    q = qs[idx]
    letters = "ABCD"
    body = ("🧠 <b>QUIZ — "
            + esc(label) + "</b> ("
            + str(idx + 1) + "/"
            + str(len(qs))
            + ")\n\n"
            + esc(q["question"])
            + "\n\n")
    for i, o in enumerate(
            q["options"]):
        body += (letters[i]
                 + ". "
                 + esc(o) + "\n")
    row = []
    for i in range(4):
        data = ("quiz:" + qid
                + ":" + str(idx)
                + ":" + str(i))
        row.append((letters[i],
                    data))
    stop = "quiz:" + qid + ":stop"
    kb = IK(row,
            [("🛑 Stop", stop)])
    await send(chat, body, kb)


async def quiz_answer(
        u, chat, qid, qi, opt):
    row = await qrow(
        """SELECT * FROM quizzes
           WHERE id=$1::uuid
           AND user_id=$2::uuid""",
        qid, u["id"])
    if not row \
            or row["status"] \
            != "active":
        return
    qs = jd(row["questions"], [])
    answers = jd(row["answers"],
                 [])
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


async def quiz_finish(
        u, chat, qid, qs,
        answers, row):
    correct = 0
    for a, qq in zip(answers,
                     qs):
        if a == qq["answer"]:
            correct += 1
    att = len(answers)
    await q(
        """UPDATE quizzes
           SET status='done'
           WHERE id=$1::uuid""",
        qid)
    if att:
        await save_evidence(
            u, row["subject_id"],
            row["topic_id"],
            att, correct, 0,
            "quiz",
            title=("quiz: "
                   + str(row[
                       "topic_label"
                   ])))
    letters = "ABCD"
    recap = []
    for a, qq in zip(answers,
                     qs):
        if a == qq["answer"]:
            mark = "✅"
        else:
            mark = "❌"
        idx = qq["answer"]
        letter = letters[idx]
        option = (qq["options"]
                  [idx])
        line = (mark + " "
                + letter + ". "
                + esc(option))
        if qq["explanation"]:
            line += (" — "
                     + esc(
                         qq[
                             "explanation"
                         ]))
        recap.append(line)
    acc = (100 * correct // att
           if att else 0)
    lines = ["🧠 <b>Quiz done: "
             + str(correct) + "/"
             + str(att)
             + "</b> ("
             + str(acc) + "%)",
             ""]
    lines += recap[:8]
    lines += ["",
              "Logged as evidence "
              "— mastery & "
              "revision updated."]
    await send(chat,
               "\n".join(lines),
               IK([("▶️ Start Next",
                    "nav:next")]))


# ---- tutor ----
async def note_excerpts(
        u, question):
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
           ORDER BY created_at
             DESC
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


async def tutor_reply(
        u, chat, question):
    await log_chat(u, "user",
                   question)
    from brain import ai_context
    ctx = await ai_context(u)
    mem = await memory_block(u)
    hist = await chat_context_block(u)
    excerpts = []
    try:
        excerpts = await \
            note_excerpts(
                u, question)
    except Exception:
        pass
    prompt = ("STUDENT CONTEXT:"
              "\n" + ctx + "\n"
              + mem + "\n"
              + hist + "\n\n")
    if excerpts:
        prompt += (
            "YOUR SAVED NOTES "
            "(ground the answer "
            "in these when "
            "relevant):\n"
            + "\n---\n".join(
                excerpts)
            + "\n\n")
    prompt += ("Answer this "
               "study question. "
               "Be clear and "
               "concise (max 200 "
               "words), one "
               "concrete example, "
               "one common mistake "
               "to avoid. Never "
               "invent statistics "
               "about the student."
               "\n\nQUESTION: "
               + question)
    key = ("tu:"
           + hashlib.md5(
               prompt.encode()
           ).hexdigest())
    try:
        txt = _cache_get(key)
        if txt is None:
            txt = await ai_call(
                prompt,
                json_mode=False,
                max_tokens=800)
            _cache_set(key, txt)
        title = esc(
            question[:80])
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
                 ON s.id=
                   m.subject_id
               WHERE m.user_id=
                 $1::uuid
               AND m.events>0
               ORDER BY
                 m.mastery ASC
               LIMIT 3""",
            u["id"])
        lines = ["My tutor brain "
                 "is rate-limited "
                 "right now. Your "
                 "weakest topics:"]
        for r in rows:
            mv = r["mastery"]
            lines.append(
                "• "
                + r["subj"] + "/"
                + r["top"] + " — "
                + format(mv, ".0f")
                + "%")
        if not rows:
            lines.append(
                "• log more sessions "
                "to build mastery "
                "data")
        await send(
            chat,
            "\n".join(lines),
            MENU_KB)


# ---- onboarding ----
OB_STEPS = [
    ("name",
     "👤 What should I call "
     "you?",
     True, "text"),
    ("exam",
     "🎯 What are you "
     "preparing for?\n"
     "(e.g. 'JEE 2027', "
     "'NEET', 'Boards')",
     True, "text"),
    ("exam_date",
     "📅 Main exam date?\n"
     "(e.g. '24 may 2027', "
     "'24/5/2027')\n"
     "/skip if not fixed",
     False, "date"),
    ("subjects",
     "📚 Your subjects, "
     "comma-separated\n"
     "(e.g. 'Physics, "
     "Chemistry, Maths')",
     True, "subjects"),
    ("wake",
     "🌅 Wake time? "
     "(e.g. '6:30' or '6am')",
     True, "time"),
    ("sleep",
     "🌙 Sleep time? "
     "(e.g. '23:00' or "
     "'11pm')",
     True, "time"),
    ("school",
     "🏫 School hours?\n"
     "(e.g. '8-2 mon-sat')\n"
     "/skip if none",
     False, "class"),
    ("coaching",
     "📖 Coaching hours?\n"
     "(e.g. '5-8 mon,wed,"
     "fri')\n"
     "/skip if none",
     False, "class"),
]

OB_LABELS = {
    "name": "👤 Name",
    "exam": "🎯 Goal",
    "exam_date": "📅 Exam date",
    "subjects": "📚 Subjects",
    "wake": "🌅 Wake",
    "sleep": "🌙 Sleep",
    "school": "🏫 School",
    "coaching": "📖 Coaching"}


def ob_state(u):
    return jd(u.get("ob_state"), {})


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
        "🧠 <b>Welcome to "
        "StudyOS</b>\n\n"
        "I learn from what you "
        "actually do — not from "
        "plans you forget. A few "
        "quick questions, one at "
        "a time. Optional ones "
        "you can /skip.\n\n"
        + first[1])


async def ob_summary(u, chat):
    st = ob_state(u)
    lines = ["<b>Here's what "
             "I've understood."
             "</b>", ""]
    if st.get("name"):
        lines.append(
            "👤 "
            + esc(st["name"]))
    if st.get("exam"):
        line = ("🎯 "
                + esc(st["exam"]))
        if st.get("exam_date"):
            line += (" — "
                     + esc(st[
                         "exam_date"
                     ]))
        lines.append(line)
    if st.get("subjects"):
        lines.append(
            "📚 "
            + esc(", ".join(
                st["subjects"])))
    if st.get("school"):
        lines.append(
            "🏫 "
            + esc(st["school"]))
    if st.get("coaching"):
        lines.append(
            "📖 "
            + esc(st[
                "coaching"]))
    lines.append(
        "😴 "
        + str(st.get("wake"))
        + " – "
        + str(st.get("sleep")))
    lines += ["", "All correct?"]
    kb_rows = [[("✅ Confirm",
                 "ob:confirm")]]
    edit_row = []
    for key in OB_STEPS:
        label = OB_LABELS[
            key[0]]
        edit_row.append(
            (label, "ob:g:"
             + key[0]))
    kb_rows.append(edit_row)
    kb_rows.append(
        [("🔄 Start over",
          "ob:restart")])
    await send(chat,
               "\n".join(lines),
               IK(*kb_rows))


def ob_next_step(key):
    keys = [s[0]
            for s in OB_STEPS]
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
    st = dict(ob_state(u))
    step = u.get("ob_step")
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
                   ob_state=
                     $3::jsonb
               WHERE id=$1::uuid""",
            u["id"], nxt,
            json.dumps(st))
        await ob_prompt(
            u, chat, nxt)
        return
    if kind == "text":
        if not t:
            await send(
                chat,
                "A short answer "
                "works. "
                + prompt)
            return
        st[key] = t[:60]
    elif kind == "date":
        d = parse_date(t, today_d())
        if not d:
            await send(
                chat,
                "Couldn't read that "
                "date. Try '24 may "
                "2027' or '24/5/2027'.")
            return
        st[key] = d.isoformat()
    elif kind == "subjects":
        names = [x.strip().title()
                 for x in re.split(
                     r"[,;]+", t)
                 if x.strip()]
        if not names:
            await send(
                chat,
                "List them like: "
                "Physics, "
                "Chemistry, Maths")
            return
        st[key] = names[:8]
    elif kind == "time":
        tm = parse_time(t)
        if not tm:
            await send(
                chat,
                "Try a time like "
                "'6:30' or '11pm'.")
            return
        st[key] = tm.strftime(
            "%H:%M")
    elif kind == "class":
        pc = parse_class(t)
        if not pc:
            await send(
                chat,
                "Try: '8-2 mon-sat' "
                "(time + days). "
                "Or /skip.")
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
        u["id"], nxt,
        json.dumps(st))
    await ob_prompt(u, chat, nxt)


async def ob_commit(u, chat):
    st = ob_state(u)
    for name in (st.get("subjects")
                 or [])[:8]:
        await upsert_subject(
            u["id"], name)
    sch = st.get("school_data")
    coa = st.get("coaching_data")
    exam_d = None
    if st.get("exam_date"):
        try:
            from datetime import \
                date as _date
            exam_d = \
                _date.fromisoformat(
                    st["exam_date"])
        except ValueError:
            exam_d = parse_any_date(
                str(st["exam_date"]),
                today_d())
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
        sch_s = dtime \
            .fromisoformat(
                sch["s"])
        sch_e = dtime \
            .fromisoformat(
                sch["e"])
        sch_d = sch["d"]
    if coa:
        coa_s = dtime \
            .fromisoformat(
                coa["s"])
        coa_e = dtime \
            .fromisoformat(
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
        wake, sleep, sch_s,
        sch_e, sch_d, coa_s,
        coa_e, coa_d)
    await send(
        chat,
        "✅ <b>You're set up."
        "</b>\n\n"
        "Now just talk to me:\n"
        "• \"I finished 50 "
        "physics questions, "
        "39 correct\"\n"
        "• 📷 send a syllabus "
        "photo or PDF\n"
        "• 📥 send a WhatsApp "
        "export (.txt)\n"
        "• \"remember that I "
        "study best at 6am\"\n"
        "• \"what should I "
        "study?\"",
        IK([("▶️ Start Next",
             "nav:next")],
           [("🏠 Dashboard",
             "nav:dash")]))
