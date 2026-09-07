import math
from datetime import datetime, timedelta
from config import TZ

SOURCE_W = {"log": 1.0, "live": 1.0,
            "quiz": 0.9, "test": 1.6}


def compute_mastery(events):
    """Mastery v1: bounded EMA fold.
    prior=25; alpha=min(0.15,
    0.08*volume*source_weight);
    volume=log10(1+att)/log10(31).
    One event moves mastery <= 15 pts.
    Recomputable from raw evidence."""
    if not events:
        return None
    m = 25.0
    att = cor = n = 0
    last = None
    evs = sorted(events,
                 key=lambda x: x["at"])
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
            1.0,
            math.log10(1 + a)
            / math.log10(31.0))
        w = SOURCE_W.get(e["src"], 1.0)
        alpha = min(0.15,
                    0.08 * volume * w)
        m += alpha * (score - m)
        m = max(0.0, min(100.0, m))
    return {"mastery": round(m, 1),
            "attempts": att,
            "correct": cor,
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
        interval = max(
            1, round(interval * 1.2))
        ease -= 0.15
    elif g == "good":
        if streak == 0:
            interval = 1
        elif streak == 1:
            interval = 3
        else:
            interval = round(
                interval * ease)
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
            "lapses": lapses,
            "mode": mode,
            "rev_type": rev_type,
            "grade": g}


BASE_SCORE = {"test_prep": 40,
              "homework": 30,
              "mistake_review": 28,
              "revision": 25,
              "quiz": 22, "study": 20}
LIGHT = {"revision", "mistake_review",
         "quiz"}


def score_task(c, now, energy):
    s = float(BASE_SCORE.get(
        c["kind"], 15))
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
                rs.append("overdue ~"
                          + str(od) + "d")
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
        rs.append("test in "
                  + str(t_in) + "d")
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
            rs.append("untouched "
                      + str(st) + "d")
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
    return round(
        max(0.0, min(100.0, s)), 1), rs


def choose_next(scored, minutes,
                exclude=None):
    if minutes <= 0:
        return None
    limit = max(minutes, 15)
    pool = scored
    if exclude:
        pool = [x for x in scored
                if x[0]["key"]
                not in exclude]
    fitting = [x for x in pool
               if x[0]["est"] <= limit]
    if not fitting:
        light = [x for x in pool
                 if x[0]["kind"] in LIGHT]
        fitting = light or pool
    if not fitting:
        return None
    fitting.sort(
        key=lambda x: (-x[1], x[0]["est"]))
    return fitting[0]


def day_minutes(u, day, mods=None,
                classes=None,
                extras=None):
    start = datetime.combine(
        day, u["wake_time"], tzinfo=TZ)
    end = datetime.combine(
        day, u["sleep_time"], tzinfo=TZ)
    if end <= start:
        end += timedelta(days=1)
    awake = (end - start)
    awake = awake.total_seconds() / 60
    busy = 0.0
    commute = 0
    wd = day.weekday()
    pairs = (("school_start",
              "school_end",
              "school_days", "school"),
             ("coaching_start",
              "coaching_end",
              "coaching_days", "coach"))
    for sk, ek, dk, base in pairs:
        days = u.get(dk)
        scheduled = bool(
            days and wd in days
            and u.get(sk) and u.get(ek))
        run = scheduled
        if mods and mods.get(
                base + "_off"):
            run = False
        if (mods and mods.get(
                base + "_on")
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
            span = ce - cs
            busy += (span
                     .total_seconds() / 60)
            cm = u["commute_minutes"] or 0
            commute += min(cm, 60)
    classes_off = bool(
        mods and mods.get("classes_off"))
    if classes and not classes_off:
        for cl in classes:
            if wd in (cl["weekdays"] or []):
                bs = datetime.combine(
                    day, cl["start_time"],
                    tzinfo=TZ)
                be = datetime.combine(
                    day, cl["end_time"],
                    tzinfo=TZ)
                cs = max(bs, start)
                ce = min(be, end)
                if ce > cs:
                    span = ce - cs
                    busy += (span
                             .total_seconds()
                             / 60)
    if extras:
        for ev in extras:
            if ev["on_date"] == day:
                bs = datetime.combine(
                    day, ev["start_time"],
                    tzinfo=TZ)
                be = datetime.combine(
                    day, ev["end_time"],
                    tzinfo=TZ)
                cs = max(bs, start)
                ce = min(be, end)
                if ce > cs:
                    span = ce - cs
                    busy += (span
                             .total_seconds()
                             / 60)
    commute = min(
        commute,
        int(max(0, awake - busy)), 120)
    meals = u["meal_minutes"] or 60
    avail = awake - busy - commute - meals
    if avail > 0:
        avail *= 0.9
    return max(0, int(avail)), int(busy)


def urgent(c, now):
    k = c["kind"]
    if k in ("test_prep",
             "mistake_review"):
        return True
    if k == "homework":
        due = c.get("meta", {}).get(
            "due_at")
        if due is None:
            return True
        return (due - now) \
            .total_seconds() <= 48 * 3600
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
        target = min(target,
                     int(target * 0.6))
    scored = []
    for c in cands:
        sc, rs = score_task(
            c, now, energy)
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
                kept.append(
                    (c, sc, item[2]))
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
            deferred.append(
                (c["title"], note))
            continue
        tasks.append(
            {"kind": c["kind"],
             "title": c["title"],
             "est": chunk,
             "score": sc,
             "reasons": rs})
        alloc += chunk
        if c["kind"] == "revision":
            rev_n += 1
        if c["est"] > chunk:
            rest = c["est"] - chunk
            note = ("split — "
                    + str(rest) + "m later")
            deferred.append(
                (c["title"], note))
    return {"available": avail,
            "allocated": alloc,
            "tasks": tasks,
            "deferred": deferred,
            "missed": missed_days,
            "target": target}
