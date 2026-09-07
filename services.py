import json
import uuid
import secrets
import hashlib
from datetime import datetime, timedelta
from datetime import time as dtime
from config import TZ
from database import q, qrow, qrows, qval, jd
from engines import (compute_mastery,
                     revision_step,
                     grade_of)
from tg import (send, IK, esc, fm,
                now_tz, today_d, sod)


async def ensure_user(tg_from, chat_id):
    row = await qrow(
        """INSERT INTO users
           (telegram_user_id,
            telegram_chat_id,
            first_name, username)
           VALUES($1,$2,$3,$4)
           ON CONFLICT
             (telegram_user_id)
           DO UPDATE SET
           telegram_chat_id =
             COALESCE(
               EXCLUDED.telegram_chat_id,
               users.telegram_chat_id),
           first_name =
             COALESCE(
               EXCLUDED.first_name,
               users.first_name),
           username =
             COALESCE(
               EXCLUDED.username,
               users.username)
           RETURNING *""",
        tg_from["id"], chat_id,
        tg_from.get("first_name"),
        tg_from.get("username"))
    return dict(row)


async def get_subjects(user_id):
    rows = await qrows(
        """SELECT id, name
           FROM subjects
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
           DO UPDATE SET
             name=EXCLUDED.name
           RETURNING id, name""",
        user_id, name)
    return dict(r)


async def upsert_chapter(user_id,
                         subject_id, name):
    name = (name or "").strip().title()
    name = name or "General"
    r = await qrow(
        """INSERT INTO chapters
           (user_id, subject_id, name)
           VALUES($1::uuid,$2::uuid,$3)
           ON CONFLICT
             (user_id, subject_id, name)
           DO UPDATE SET
             name=EXCLUDED.name
           RETURNING id, name""",
        user_id, subject_id, name)
    return dict(r)


async def upsert_topic(user_id,
                       subject_id, name,
                       chapter_id=None):
    name = (name or "").strip().title()
    name = name or "General"
    if chapter_id:
        r = await qrow(
            """INSERT INTO topics
               (user_id, subject_id,
                chapter_id, name)
               VALUES($1::uuid,
                      $2::uuid,
                      $3::uuid,$4)
               ON CONFLICT
                 (user_id, subject_id,
                  name)
               DO UPDATE SET
                 chapter_id=
                   EXCLUDED.chapter_id,
                 name=EXCLUDED.name
               RETURNING id, name""",
            user_id, subject_id,
            chapter_id, name)
        return dict(r)
    r = await qrow(
        """INSERT INTO topics
           (user_id, subject_id, name)
           VALUES($1::uuid,$2::uuid,$3)
           ON CONFLICT
             (user_id, subject_id, name)
           DO UPDATE SET
             name=EXCLUDED.name
           RETURNING id, name""",
        user_id, subject_id, name)
    return dict(r)


async def find_topic(user_id,
                     subject_id, name):
    if not name:
        return None
    r = await qrow(
        """SELECT id, name
           FROM topics
           WHERE user_id=$1::uuid
           AND subject_id=$2::uuid
           AND name ILIKE $3
           LIMIT 1""",
        user_id, subject_id,
        "%" + name.strip() + "%")
    return dict(r) if r else None


async def load_day_mods(user_id, day):
    rows = await qrows(
        """SELECT op, which,
                  from_date, to_date
           FROM schedule_changes
           WHERE user_id=$1::uuid
           AND (from_date=$2
                OR to_date=$2)""",
        user_id, day)
    mods = {"school_off": False,
            "coach_off": False,
            "school_on": False,
            "coach_on": False,
            "classes_off": False}
    for r in rows:
        which = r["which"]
        if which == "school":
            base = "school"
        elif which == "coaching":
            base = "coach"
        elif which == "classes":
            if r["from_date"] == day:
                mods["classes_off"] = True
            continue
        else:
            continue
        if r["from_date"] == day:
            mods[base + "_off"] = True
        if (r["op"] == "move"
                and r["to_date"] == day):
            mods[base + "_on"] = True
    return mods


async def get_classes(user_id):
    rows = await qrows(
        """SELECT * FROM classes
           WHERE user_id=$1::uuid
           AND active=true""",
        user_id)
    return [dict(r) for r in rows]


async def get_extras(user_id, day):
    rows = await qrows(
        """SELECT * FROM extra_events
           WHERE user_id=$1::uuid
           AND on_date BETWEEN $2
             AND $2 + interval '1 day'""",
        user_id, day)
    return [dict(r) for r in rows]


async def topic_mastery_events(
        user_id, topic_id):
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
        out.append(
            {"a": r["a"], "c": r["c"],
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
        out.append(
            {"a": r["a"], "c": r["c"],
             "at": r["created_at"],
             "src": r["source"]})
    return out


async def upsert_mastery(
        user_id, subject_id,
        topic_id, snap):
    if snap is None \
            or snap["events"] == 0:
        return
    if topic_id:
        sql = (
            """INSERT INTO mastery
               (user_id, subject_id,
                topic_id, mastery,
                attempts, correct,
                events, last_evidence,
                algo)
               VALUES($1::uuid,
                      $2::uuid,
                      $3::uuid,
                      $4,$5,$6,$7,$8,1)
               ON CONFLICT
                 (user_id, topic_id)
                 WHERE topic_id
                   IS NOT NULL
               DO UPDATE SET
                 mastery=
                   EXCLUDED.mastery,
                 attempts=
                   EXCLUDED.attempts,
                 correct=
                   EXCLUDED.correct,
                 events=
                   EXCLUDED.events,
                 last_evidence=
                   EXCLUDED.last_evidence,
                 computed_at=now()""")
        await q(sql, user_id,
                subject_id, topic_id,
                snap["mastery"],
                snap["attempts"],
                snap["correct"],
                snap["events"],
                snap["last"])
        try:
            await q(
                """INSERT INTO mastery_log
                   (user_id, topic_id,
                    mastery)
                   VALUES($1::uuid,
                          $2::uuid,$3)""",
                user_id, topic_id,
                snap["mastery"])
        except Exception:
            pass
    else:
        sql = (
            """INSERT INTO mastery
               (user_id, subject_id,
                topic_id, mastery,
                attempts, correct,
                events, last_evidence,
                algo)
               VALUES($1::uuid,
                      $2::uuid,
                      NULL,
                      $3,$4,$5,$6,$7,1)
               ON CONFLICT
                 (user_id, subject_id)
                 WHERE topic_id IS NULL
               DO UPDATE SET
                 mastery=
                   EXCLUDED.mastery,
                 attempts=
                   EXCLUDED.attempts,
                 correct=
                   EXCLUDED.correct,
                 events=
                   EXCLUDED.events,
                 last_evidence=
                   EXCLUDED.last_evidence,
                 computed_at=now()""")
        await q(sql, user_id,
                subject_id,
                snap["mastery"],
                snap["attempts"],
                snap["correct"],
                snap["events"],
                snap["last"])


async def recompute(user_id,
                    subject_id,
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
            user_id, subject_id,
            None, snap)


async def mistake_reps(user_id,
                       topic_id):
    if not topic_id:
        return 0
    v = await qval(
        """SELECT COALESCE(
                    SUM(count),0)
           FROM mistakes
           WHERE user_id=$1::uuid
           AND topic_id=$2::uuid
           AND resolved=false""",
        user_id, topic_id)
    return int(v or 0)


async def apply_revision(
        user_id, subject_id,
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
        due = now + timedelta(
            days=interval)
        await q(
            """INSERT INTO revisions
               (user_id, subject_id,
                topic_id, rev_type,
                interval_days, due_at,
                last_outcome,
                last_reviewed, status)
               VALUES($1::uuid,
                      $2::uuid,
                      $3::uuid,
                      'active_recall',
                      $4,$5,$6,$7,'due')
               ON CONFLICT
                 (user_id, topic_id)
               DO NOTHING""",
            user_id, subject_id,
            topic_id, interval, due,
            grade_of(acc), now)
        return due
    st = revision_step(
        dict(r), acc, reps)
    due = now + timedelta(
        days=st["interval_days"])
    await q(
        """UPDATE revisions SET
           interval_days=$2,
           ease=$3, streak=$4,
           lapses=$5, mode=$6,
           rev_type=$7,
           last_outcome=$8,
           last_reviewed=$9,
           due_at=$10,
           status='due'
           WHERE id=$1::uuid""",
        str(r["id"]),
        st["interval_days"],
        st["ease"], st["streak"],
        st["lapses"], st["mode"],
        st["rev_type"], st["grade"],
        now, due)
    return due


async def save_evidence(
        u, subject_id, topic_id,
        att, cor, minutes,
        source="log", when=None,
        title=None):
    now = when or now_tz()
    await q(
        """INSERT INTO study_sessions
           (user_id, subject_id,
            topic_id, source,
            status, title,
            questions_attempted,
            questions_correct,
            duration_minutes,
            started_at, ended_at)
           VALUES($1::uuid,
                  $2::uuid,
                  $3::uuid,$4,
                  'finished',$5,
                  $6,$7,$8,$9,$9)""",
        u["id"], subject_id,
        topic_id, source, title,
        att or 0, cor,
        minutes or 0, now)
    out = {}
    if att and cor is not None:
        out["acc"] = 100.0 * cor / att
        if topic_id and subject_id:
            await recompute(
                u["id"], subject_id,
                topic_id)
            out["next_rev"] = \
                await apply_revision(
                    u["id"], subject_id,
                    topic_id,
                    cor / att, now)
            m = await qrow(
                """SELECT mastery,
                          events
                   FROM mastery
                   WHERE user_id=$1::uuid
                   AND topic_id=$2::uuid""",
                u["id"], topic_id)
            if m:
                out["mastery"] = \
                    m["mastery"]
                out["events"] = \
                    m["events"]
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
                out["mastery"] = \
                    m["mastery"]
    return out


async def live_session(user_id):
    r = await qrow(
        """SELECT *
           FROM study_sessions
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
    return max(0,
               int((secs - paused) / 60))


async def spent_today(u, day):
    v = await qval(
        """SELECT COALESCE(
                    SUM(duration_minutes),
                    0)
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
        age = now - s["started_at"]
        if age.total_seconds() > 3 * 3600:
            await q(
                """UPDATE study_sessions
                   SET status='abandoned'
                   WHERE id=$1::uuid""",
                str(s["id"]))


# ---- pending actions ----
async def new_pending(u, kind, fields,
                      origin="ai"):
    pid = str(uuid.uuid4())
    code = secrets.token_hex(5)
    await q(
        """INSERT INTO pending_actions
           (id, user_id, kind,
            payload, code)
           VALUES($1::uuid,$2::uuid,
                  $3,$4::jsonb,$5)""",
        pid, u["id"], kind,
        json.dumps({"fields": fields,
                    "origin": origin}),
        code)
    return code


async def get_pending(u, code):
    r = await qrow(
        """SELECT *
           FROM pending_actions
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
    return IK(
        [("✅ Confirm", yes),
         ("❌ Cancel", no)],
        [("✏️ Correct it", ed)])


async def subject_pick_card(
        u, chat, code, header):
    subs = await get_subjects(u["id"])
    rows = []
    for s in subs[:6]:
        data = ("subj:" + code + ":"
                + str(s["id"]))
        rows.append(
            [(s["name"], data)])
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
            out.append(
                label + ": <b>"
                + esc(v) + "</b>")
    return out


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
           ORDER BY test_at""",
        u["id"])
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
            d = (t["test_at"]
                 - now).days
        if d < 0:
            d = 0
        cands.append({
            "kind": "test_prep",
            "key": "test:"
                   + str(t["id"]),
            "title": "Prepare — "
                     + t["name"],
            "est": 45,
            "subject": "",
            "subject_id": None,
            "topic_id": None,
            "meta": {
                "test_in_days": d}})
    rows = await qrows(
        """SELECT rv.id, rv.due_at,
                  rv.mode,
                  rv.rev_type,
                  rv.topic_id,
                  rv.subject_id,
                  s.name subj,
                  t.name top,
                  c.name chap,
                  m.mastery
           FROM revisions rv
           LEFT JOIN subjects s
             ON s.id=rv.subject_id
           LEFT JOIN topics t
             ON t.id=rv.topic_id
           LEFT JOIN chapters c
             ON c.id=t.chapter_id
           LEFT JOIN mastery m
             ON m.user_id=rv.user_id
             AND m.topic_id=
               rv.topic_id
           WHERE rv.user_id=$1::uuid
           AND rv.due_at < now()
             + interval '1 day'
           ORDER BY rv.due_at
           LIMIT 25""", u["id"])
    for r in rows:
        od = max(0,
                 (now
                  - r["due_at"]).days)
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
            "key": "rev:"
                   + str(r["id"]),
            "title":
                title.strip(" —"),
            "est": est,
            "subject":
                r["subj"] or "",
            "chapter":
                r["top"] or "",
            "subject_id":
                str(subj_id)
                if subj_id else None,
            "topic_id":
                str(r["topic_id"]),
            "meta": {
                "overdue_days": od,
                "mastery":
                    r["mastery"],
                "mode": r["mode"],
                "rev_type":
                    r["rev_type"],
                "revision_id":
                    str(r["id"]),
                "topic": r["top"],
                "test_in_days":
                    min_test}})
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
            "kind":
                "mistake_review",
            "key": "mist:"
                   + str(topic_id),
            "title":
                "Mistake review — "
                + (r["subj"] or "")
                + " "
                + (r["top"] or ""),
            "est": 25,
            "subject":
                r["subj"] or "",
            "chapter":
                r["top"] or "",
            "subject_id":
                str(subj_id)
                if subj_id else None,
            "topic_id":
                str(topic_id)
                if topic_id
                else None,
            "meta": {
                "reps":
                    int(r["reps"]),
                "test_in_days":
                    min_test}})
    rows = await qrows(
        """SELECT h.id, h.title,
                  h.due_at,
                  h.total_q,
                  h.completed_q,
                  h.est_minutes,
                  s.name subj,
                  h.subject_id sid
           FROM homework h
           LEFT JOIN subjects s
             ON s.id=h.subject_id
           WHERE h.user_id=$1::uuid
           AND h.status NOT IN
             ('completed',
              'abandoned')
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
                      min(60,
                          2 * rem))
        else:
            est = 30
        subj_id = r["sid"]
        cands.append({
            "kind": "homework",
            "key": "hw:"
                   + str(r["id"]),
            "title": r["title"],
            "est": est,
            "subject":
                r["subj"] or "",
            "subject_id":
                str(subj_id)
                if subj_id
                else None,
            "topic_id": None,
            "meta": {
                "due_at":
                    r["due_at"],
                "remaining": rem,
                "homework_id":
                    str(r["id"]),
                "test_in_days":
                    min_test}})
    rows = await qrows(
        """SELECT t.id tid,
                  t.name top,
                  t.subject_id sid,
                  s.name subj,
                  c.name chap,
                  m.mastery,
                  m.last_evidence
           FROM topics t
           JOIN subjects s
             ON s.id=t.subject_id
           LEFT JOIN chapters c
             ON c.id=t.chapter_id
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
            le = (r["last_evidence"]
                  .date())
            stale = (now.date()
                     - le).days
        title = "Study — "
        title += r["subj"]
        title += " — "
        title += (r["top"] or "")
        cands.append({
            "kind": "study",
            "key": "study:"
                   + str(r["tid"]),
            "title": title,
            "est": 45,
            "subject": r["subj"],
            "chapter":
                (r["chap"] or "")
                or r["top"],
            "subject_id":
                str(r["sid"]),
            "topic_id":
                str(r["tid"]),
            "meta": {
                "mastery":
                    r["mastery"],
                "stale_days": stale,
                "test_in_days":
                    min_test}})
    return cands
