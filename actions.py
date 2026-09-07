import re
import hashlib
from datetime import datetime, timedelta
from datetime import time as dtime
from config import TZ
from database import q, qrow, qrows, jd
from tg import send, IK, esc, fm
import services as S
from services import (get_subjects,
                      resolve_subject,
                      find_topic,
                      upsert_topic,
                      upsert_subject,
                      upsert_chapter,
                      build_candidates,
                      save_evidence)
from engines import build_plan
from parsers import (parse_any_date,
                     parse_days,
                     parse_time_range,
                     clampi)
from brain import AIError, ai_call
from tg import now_tz, today_d


async def do_log_session(u, f):
    sid = f.get("subject_id")
    if not sid:
        return ("Subject missing — "
                "cancelled.")
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
        u, sid, topic_id, att, cor,
        mins, "log", when=when,
        title="logged session")
    parts = ["✅ <b>Logged</b>"]
    if att and cor is not None:
        acc = res.get("acc", 0)
        parts.append(
            "❓ " + str(att)
            + " questions • ✅ "
            + str(cor) + " correct ("
            + format(acc, ".0f")
            + "%)")
    if mins:
        parts.append("⏱ " + fm(mins))
    if topic_name:
        parts.append(
            "📚 Topic: "
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
        d = parse_any_date(
            str(f["due"]), today_d())
        if d:
            due = datetime.combine(
                d, dtime(23, 59),
                tzinfo=TZ)
    tq = f.get("questions")
    if tq:
        est = f.get("minutes") or max(
            15, min(60, 2 * tq))
    else:
        est = f.get("minutes") or 30
    await q(
        """INSERT INTO homework
           (user_id, subject_id,
            title, hw_type, total_q,
            est_minutes, due_at)
           VALUES($1::uuid,$2::uuid,
                  $3,$4,$5,$6,$7)""",
        u["id"], sid,
        (f.get("title")
         or "Homework").strip()[:120],
        f.get("hw_type") or "custom",
        tq, est, due)
    out = ("📝 Saved: <b>"
           + esc(f.get("title"))
           + "</b>")
    if due:
        out += (" — due "
                + esc(f["due"]))
    return out


async def do_hw_progress(u, f):
    title = (f.get("title")
             or "").strip()
    rows = []
    if title:
        rows = await qrows(
            """SELECT * FROM homework
               WHERE user_id=$1::uuid
               AND status NOT IN
                 ('completed')
               AND title ILIKE $2
               ORDER BY due_at""",
            u["id"],
            "%" + title + "%")
    if not rows:
        msg = ("Couldn't find that "
               "homework — check the "
               "title with /homework.")
        return msg, False
    hw = rows[0]
    total = hw["total_q"]
    done_raw = f.get("done")
    rest = False
    if isinstance(done_raw, str):
        dl = done_raw.strip().lower()
        if dl in ("rest", "all",
                  "remaining",
                  "rest of it",
                  "the rest"):
            rest = True
            if total:
                done = total \
                    - (hw["completed_q"]
                       or 0)
            else:
                return ("How many "
                        "questions does "
                        "it have in "
                        "total?", False)
        else:
            try:
                done = int(dl)
            except ValueError:
                done = 0
    else:
        done = int(done_raw or 0)
    newc = (hw["completed_q"]
            or 0) + max(0, done)
    if total:
        newc = min(newc, total)
    completed = bool(
        total and newc >= total)
    if completed:
        status = "completed"
    else:
        status = "in_progress"
    await q(
        """UPDATE homework
           SET completed_q=$2,
               status=$3
           WHERE id=$1::uuid""",
        str(hw["id"]), newc, status)
    prog = ""
    if total:
        prog = (" [" + str(newc)
                + "/" + str(total)
                + "]")
    if completed:
        tail = "done! 🎉"
    elif rest:
        tail = "rest logged."
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
        d = parse_any_date(
            str(f["date"]), today_d())
        if d:
            when = datetime.combine(
                d, dtime(9, 0),
                tzinfo=TZ)
    await q(
        """INSERT INTO tests
           (user_id, subject_id,
            name, test_at,
            total_marks)
           VALUES($1::uuid,$2::uuid,
                  $3,$4,$5)""",
        u["id"], sid,
        (f.get("name")
         or "Test").strip()[:120],
        when, f.get("total_marks"))
    out = ("🧪 Saved: <b>"
           + esc(f.get("name"))
           + "</b>")
    if when:
        out += (" — "
                + when.strftime(
                    "%a %d %b"))
    return out


async def do_test_result(u, f):
    name = (f.get("name")
            or "").strip()
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
    if not test \
            and f.get("subject_id"):
        for r in rows:
            if str(r["subject_id"]) \
                    == str(f.get(
                        "subject_id")):
                test = r
                break
    if not test and rows:
        test = rows[0]
    if not test:
        return ("Couldn't find that "
                "test. Add it first: "
                "'test on friday'.")
    obt = f.get("obtained")
    tot = f.get("total") \
        or test["total_marks"]
    if obt is None or tot is None:
        return ("I need both "
                "obtained and total "
                "marks.")
    pct = 100.0 * obt / tot
    await q(
        """UPDATE tests
           SET obtained_marks=$2,
               total_marks=$3,
               status='completed'
           WHERE id=$1::uuid""",
        str(test["id"]), obt, tot)
    sid = test["subject_id"]
    topics = jd(f.get("topics"), [])
    per_topic = 0
    if sid and topics:
        for tp in topics[:12]:
            tp = jd(tp, {})
            tname = tp.get("topic")
            tc = tp.get("correct")
            tt = tp.get("total")
            if not tname:
                continue
            if tc is None or tt is None:
                continue
            tc = int(tc)
            tt = int(tt)
            if tt <= 0 or tc > tt:
                continue
            t = await find_topic(
                u["id"], sid, tname)
            if not t:
                t = await upsert_topic(
                    u["id"], sid, tname)
            await save_evidence(
                u, sid, t["id"],
                tt, tc, 0, "test",
                title="test topic: "
                      + tname)
            per_topic += 1
    if per_topic == 0:
        ev_att = 20
        ev_cor = round(ev_att * pct / 100)
        await save_evidence(
            u, sid, None,
            ev_att, ev_cor, 0, "test",
            title="test: "
                  + test["name"])
    weak = jd(f.get("weak_topics"), [])
    banked = 0
    if sid and isinstance(weak, list):
        for wname in weak[:8]:
            wname = str(wname).strip()
            if not wname:
                continue
            t = await find_topic(
                u["id"], sid, wname)
            if not t:
                t = await upsert_topic(
                    u["id"], sid, wname)
            fp = hashlib.md5(
                (str(u["id"]) + "|"
                 + str(sid) + "|"
                 + str(t["id"])
                 + "|test_weakness"
                 ).encode()).hexdigest()
            await q(
                """INSERT INTO mistakes
                   (user_id, subject_id,
                    topic_id, mtype,
                    description, count,
                    fingerprint)
                   VALUES($1::uuid,
                          $2::uuid,
                          $3::uuid,
                          'test_weakness',
                          $4, 1, $5)
                   ON CONFLICT
                     (user_id,
                      fingerprint)
                   DO UPDATE SET
                     count=mistakes.count
                       +1,
                     last_seen=now(),
                     resolved=false""",
                u["id"], sid,
                t["id"],
                ("weak in test: "
                 + wname)[:200], fp)
            banked += 1
    if pct >= 80:
        verdict = "strong 💪"
    elif pct >= 60:
        verdict = "solid 👍"
    else:
        verdict = "needs work 🔧"
    base = ("🧪 <b>"
            + esc(test["name"])
            + "</b>: " + str(obt)
            + "/" + str(tot) + " ("
            + format(pct, ".0f")
            + "%) — " + verdict)
    if per_topic:
        base += ("\n📊 " + str(per_topic)
                 + " topics logged as "
                   "evidence.")
    if banked:
        base += ("\n🧨 " + str(banked)
                 + " weak topic(s) "
                   "banked for revision.")
    return base


async def do_mistake_add(u, f):
    sid = f.get("subject_id")
    if not sid:
        return ("Which subject? "
                "Try again.")
    topic_id = None
    if f.get("topic"):
        t = await find_topic(
            u["id"], sid, f["topic"])
        if not t:
            t = await upsert_topic(
                u["id"], sid,
                f["topic"])
        topic_id = t["id"]
    valid = ("conceptual",
             "calculation", "careless",
             "memory", "misread",
             "guessing",
             "time_pressure",
             "test_weakness",
             "unknown")
    mtype = f.get("mtype") or "unknown"
    if mtype not in valid:
        mtype = "unknown"
    cnt = max(1, min(50,
                     int(f.get("count")
                         or 1)))
    fp = hashlib.md5(
        (str(u["id"]) + "|"
         + str(sid) + "|"
         + str(topic_id) + "|"
         + mtype).encode()).hexdigest()
    await q(
        """INSERT INTO mistakes
           (user_id, subject_id,
            topic_id, mtype,
            description, count,
            fingerprint)
           VALUES($1::uuid,
                  $2::uuid,
                  $3::uuid,$4,
                  $5,$6,$7)
           ON CONFLICT
             (user_id, fingerprint)
           DO UPDATE SET
             count=mistakes.count
               +EXCLUDED.count,
             last_seen=now(),
             resolved=false""",
        u["id"], sid, topic_id,
        mtype,
        (f.get("description")
         or mtype)[:200], cnt, fp)
    reps = 0
    if topic_id:
        reps = await S.mistake_reps(
            u["id"], topic_id)
    warn = ""
    if reps >= 4:
        warn = ("\n⚠️ <b>PATTERN DETECTED"
                "</b> — " + str(reps)
                + " of these now. "
                "Priority raised.")
    elif reps >= 2:
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
        sql += (" AND subject_id="
                "$2::uuid")
        args.append(sid)
    if topic:
        n = len(args) + 1
        sql += (" AND topic_id IN "
                "(SELECT id "
                "FROM topics "
                "WHERE user_id="
                "$1::uuid "
                "AND name ILIKE $"
                + str(n) + ")")
        args.append("%" + topic + "%")
    res = await q(sql, *args)
    n = 0
    m = re.search(r"UPDATE (\d+)",
                  res or "")
    if m:
        n = int(m.group(1))
    if n == 0:
        return ("No open mistakes "
                "matched that — "
                "check /mistakes.")
    return ("✅ Marked " + str(n)
            + " mistake group(s) "
            "as resolved. They'll "
            "stop driving your "
            "priorities.")


async def do_class_add(u, f):
    subs = get_subjects_result = \
        await get_subjects(u["id"])
    sid, sname = resolve_subject(
        f.get("subject"),
        get_subjects_result)
    title = (f.get("title")
             or f.get("subject")
             or "Class").strip()[:80]
    days = parse_days(
        str(f.get("days") or ""))
    tr = parse_time_range(
        str(f.get("time_range") or ""))
    if not days:
        days = list(range(7))
    if not tr:
        return ("Which times? e.g. "
                "'physics class every "
                "mon and wed 5-7'")
    await q(
        """INSERT INTO classes
           (user_id, subject_id,
            title, weekdays,
            start_time, end_time)
           VALUES($1::uuid,$2::uuid,
                  $3,$4,$5,$6)""",
        u["id"], sid, title, days,
        tr[0], tr[1])
    names = ["Mon", "Tue", "Wed",
             "Thu", "Fri", "Sat",
             "Sun"]
    dstr = ", ".join(names[d]
                     for d in days)
    return ("🗓 Class saved: <b>"
            + esc(title) + "</b>\n"
            + tr[0].strftime("%H:%M")
            + "–"
            + tr[1].strftime("%H:%M")
            + " (" + dstr
            + "). Capacity adapts.")


async def do_extra_class(u, f):
    d = parse_any_date(
        str(f.get("date")
            or "tomorrow"),
        today_d())
    if not d:
        d = today_d() + timedelta(
            days=1)
    tr = parse_time_range(
        str(f.get("time_range") or ""))
    if not tr:
        return ("Which times? e.g. "
                "'extra class tomorrow "
                "5-7'")
    title = (f.get("title")
             or "Extra class"
             ).strip()[:80]
    await q(
        """INSERT INTO extra_events
           (user_id, title, on_date,
            start_time, end_time)
           VALUES($1::uuid,$2,$3,
                  $4,$5)""",
        u["id"], title, d,
        tr[0], tr[1])
    return ("🗓 Extra class: <b>"
            + esc(title) + "</b> on "
            + d.strftime("%a %d %b")
            + " "
            + tr[0].strftime("%H:%M")
            + "–"
            + tr[1].strftime("%H:%M")
            + ". Plan adapts.")


async def do_class_op(u, f):
    op = f.get("op")
    which = f.get("which") \
        or "coaching"
    if which not in ("school",
                     "coaching",
                     "classes"):
        which = "coaching"
    d = parse_any_date(
        str(f.get("date")
            or "tomorrow"),
        today_d())
    if not d:
        d = today_d() + timedelta(
            days=1)
    if op == "cancel":
        await q(
            """INSERT INTO
               schedule_changes
               (user_id, op, which,
                from_date)
               VALUES($1::uuid,
                      'cancel',
                      $2,$3)""",
            u["id"], which, d)
        label = d.strftime(
            "%a %d %b")
        if which == "classes":
            what = "All classes"
        else:
            what = which
        return ("🗓 " + what
                + " cancelled on "
                + label
                + ". Your plan adapts.")
    to_d = None
    if f.get("to"):
        to_d = parse_any_date(
            str(f["to"]), today_d())
    if not to_d:
        return "Move to which date?"
    await q(
        """INSERT INTO
           schedule_changes
           (user_id, op, which,
            from_date, to_date)
           VALUES($1::uuid,
                  'move',$2,
                  $3,$4)""",
        u["id"], which, d, to_d)
    f1 = d.strftime("%a %d %b")
    f2 = to_d.strftime("%a %d %b")
    return ("🗓 " + which
            + " moved: " + f1
            + " → " + f2
            + ". Your plan adapts.")


async def do_syllabus(u, blocks):
    added_s = 0
    added_c = 0
    added_t = 0
    for b in blocks[:12]:
        b = jd(b, {})
        sname = (b.get("subject")
                 or "").strip().title()
        if not sname:
            continue
        s = await upsert_subject(
            u["id"], sname)
        added_s += 1
        chapters = jd(
            b.get("chapters"), [])
        for ch in chapters[:40]:
            ch = jd(ch, {})
            cname = (ch.get("name")
                     or "").strip().title()
            if not cname:
                continue
            chrow = await upsert_chapter(
                u["id"], s["id"], cname)
            added_c += 1
            topics = (ch.get("topics")
                      or [])[:60]
            for tname in topics:
                t = (tname or "").strip()
                if not t:
                    continue
                await upsert_topic(
                    u["id"], s["id"], t,
                    chrow["id"])
                added_t += 1
        for tname in (b.get("topics")
                      or [])[:60]:
            t = (tname or "").strip()
            if not t:
                continue
            await upsert_topic(
                u["id"], s["id"], t)
            added_t += 1
    return ("📚 Syllabus saved: "
            + str(added_s)
            + " subjects, "
            + str(added_c)
            + " chapters, "
            + str(added_t)
            + " topics.")


async def do_save_note(u, f):
    f = jd(f, {})
    await q(
        """INSERT INTO resources
           (title, content, user_id,
            subject_id, topic_id)
           VALUES($1,$2,$3::uuid,
                  $4::uuid,$5::uuid)""",
        (f.get("title")
         or "Note")[:80],
        (f.get("content")
         or "")[:4000],
        u["id"], f.get("subject_id"),
        f.get("topic_id"))
    return ("📎 Saved to your notes. "
            "/notes to browse.")


async def refresh_plan(u, chat, reason):
    today = today_d()
    exists = await q  # placeholder
    exists = await S.qval_plan(
        u["id"], today) \
        if False else None
    from database import qval
    exists = await qval(
        """SELECT 1
           FROM daily_plans
           WHERE user_id=$1::uuid
           AND plan_date=$2""",
        u["id"], today)
    if not exists:
        return
    now = now_tz()
    mods = await S.load_day_mods(
        u["id"], today)
    cls = await S.get_classes(u["id"])
    extras = await S.get_extras(
        u["id"], today)
    from engines import day_minutes
    avail, _ = day_minutes(
        u, today, mods, cls, extras)
    cands = await build_candidates(
        u, now)
    plan = build_plan(
        cands, avail, u["energy"],
        0, now)
    tasks = json.dumps(plan["tasks"]) \
        if False else None
    import json as _json
    tasks = _json.dumps(
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
    lines = ["🔄 <b>Plan updated"
             "</b> ("
             + esc(reason) + ")", ""]
    for i, t in enumerate(
            plan["tasks"], 1):
        lines.append(
            str(i) + ". "
            + esc(t["title"])
            + " — " + fm(t["est"]))
    if not plan["tasks"]:
        lines.append(
            "Nothing left today.")
    await send(chat,
               "\n".join(lines),
               IK([("▶️ Start Next",
                    "nav:next")]))
