import re
from datetime import date, timedelta
from datetime import time as dtime

WEEKDAYS = {"monday": 0, "mon": 0,
            "tuesday": 1, "tue": 1,
            "tues": 1,
            "wednesday": 2, "wed": 2,
            "thursday": 3, "thu": 3,
            "thurs": 3,
            "friday": 4, "fri": 4,
            "saturday": 5, "sat": 5,
            "sunday": 6, "sun": 6}
MONTHS = {m.lower(): i + 1
          for i, m in enumerate(
              ["January", "February",
               "March", "April", "May",
               "June", "July", "August",
               "September", "October",
               "November", "December"])}
SKIP_WORDS = {"skip", "/skip", "later",
              "no", "nothing", "none",
              "n", "na"}


def _try_date(y, mo, dd):
    try:
        return date(y, mo, dd)
    except ValueError:
        return None


def parse_date(text, today):
    t = (text or "").strip().lower()
    t = t.rstrip(".,!?")
    if not t:
        return None
    if t in ("today", "tonight"):
        return today
    if t in ("tomorrow", "tmrw", "tmr"):
        return today + timedelta(days=1)
    if t in ("day after tomorrow",
             "day after"):
        return today + timedelta(days=2)
    if t == "next week":
        return today + timedelta(days=7)
    m = re.match(
        r"^in\s+(\d+)\s+days?$", t)
    if m:
        n = int(m.group(1))
        return today + timedelta(days=n)
    for wd in WEEKDAYS:
        if t == wd or t == "next " + wd \
                or t == "this " + wd:
            d = (WEEKDAYS[wd]
                 - today.weekday()) % 7
            return today + timedelta(
                days=d if d else 7)
    m = re.match(
        r"^(\d{4})-(\d{1,2})-(\d{1,2})$",
        t)
    if m:
        return _try_date(int(m.group(1)),
                         int(m.group(2)),
                         int(m.group(3)))
    m = re.match(
        r"^(\d{1,2})[/\-.](\d{1,2})"
        r"[/\-.](\d{2,4})$", t)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        y += 2000 if y < 100 else 0
        if mo > 12 and d <= 12:
            d, mo = mo, d
        return _try_date(y, mo, d)
    m = re.match(
        r"^(\d{1,2})[/\-.](\d{1,2})$", t)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        if mo > 12 and d <= 12:
            d, mo = mo, d
        if not (1 <= mo <= 12
                and 1 <= d <= 31):
            return None
        out = _try_date(today.year, mo, d)
        if out is None:
            return None
        if out < today:
            out = _try_date(today.year + 1,
                            mo, d)
        return out
    words = re.findall(r"[a-z]+|\d+", t)
    month = None
    nums = []
    year = None
    for w in words:
        if w in MONTHS and month is None:
            month = MONTHS[w]
        elif w.isdigit():
            n = int(w)
            if len(w) == 4 \
                    and 1990 <= n <= 2100:
                year = n
            else:
                nums.append(n)
    if month and nums:
        day = None
        for n in nums:
            if 1 <= n <= 31:
                day = n
                break
        if day:
            y = year or today.year
            out = _try_date(y, month, day)
            if out is None and year is None:
                out = _try_date(y + 1,
                                month, day)
            if out is None:
                return None
            if year is None and out < today:
                out = _try_date(
                    today.year + 1, month, day)
            return out
    return None


def parse_any_date(text, today):
    t = (text or "").strip()
    if not t:
        return None
    try:
        return date.fromisoformat(t)
    except ValueError:
        return parse_date(t, today)


def parse_time(t):
    t = (t or "").strip().lower()
    t = t.replace(" ", "")
    m = re.match(
        r"^(\d{1,2})(?::(\d{2}))?"
        r"(am|pm)?$", t)
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
    t = (t or "").strip().lower()
    t = t.replace(" ", "")
    t = t.replace("–", "-")
    t = t.replace("—", "-")
    m = re.match(
        r"^(\d{1,2})(?::(\d{2}))?(am|pm)?"
        r"(?:-|to)"
        r"(\d{1,2})(?::(\d{2}))?(am|pm)?$",
        t)
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
    m = re.search(
        r"(\d+(?:\.\d+)?)\s*"
        r"(?:h|hr|hrs|hour|hours)\s*"
        r"(?:(\d+)\s*"
        r"(?:m|min|mins|minutes))?", t)
    if m:
        return int(float(m.group(1)) * 60
                   + int(m.group(2) or 0))
    m = re.search(
        r"(\d+)\s*"
        r"(?:m|min|mins|minute|minutes)\b",
        t)
    if m:
        return int(m.group(1))
    return None


def parse_questions(text):
    t = (text or "").lower()
    m = re.search(
        r"(\d+)\s*"
        r"(?:questions?|qs|mcqs?|"
        r"problems?|pyqs?)\b", t)
    if not m:
        return None
    att = int(m.group(1))
    cor = wre = None
    mc = re.search(
        r"(\d+)\s*"
        r"(?:correct|right|correctly)", t)
    mw = re.search(
        r"(\d+)\s*"
        r"(?:wrong|incorrect|mistakes?)",
        t)
    if mc:
        cor = int(mc.group(1))
    if mw:
        wre = int(mw.group(1))
    if (cor is None and wre is not None
            and wre <= att):
        cor = att - wre
    return att, cor, wre


def detect_energy(text):
    t = (text or "").lower()
    bad = ("exhausted", "dead tired",
           "burnt out", "burned out",
           "no energy", "so tired",
           "can't do this",
           "cant do this")
    mid = ("tired", "sleepy",
           "cant focus", "can't focus",
           "low energy", "drained")
    good = ("energetic",
            "feeling fresh", "fired up")
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
    if s in ("daily", "everyday",
             "every day", "all days"):
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
        bits = re.split(r"-|\s+to\s+",
                        part)
        if (len(bits) == 2
                and bits[0] in WEEKDAYS
                and bits[1] in WEEKDAYS):
            a = WEEKDAYS[bits[0]]
            b = WEEKDAYS[bits[1]]
            if b >= a:
                out.update(range(a, b + 1))
            else:
                out.update(range(a, 7))
                out.update(range(0, b + 1))
    return sorted(out) if out else None


def parse_class(text):
    m = re.search(
        r"(\d{1,2}(?::\d{2})?\s*"
        r"(?:am|pm)?\s*(?:-|–|to)\s*"
        r"\d{1,2}(?::\d{2})?\s*"
        r"(?:am|pm)?)", text.lower())
    if not m:
        return None
    tr = parse_time_range(m.group(1))
    if not tr:
        return None
    rest = text.lower()
    rest = rest.replace(m.group(1), " ")
    rest = rest.strip()
    days = parse_days(rest) if rest \
        else list(range(7))
    return tr[0], tr[1], days


def clampi(v, lo, hi):
    if v is None:
        return None
    try:
        v = int(v)
    except (TypeError, ValueError):
        return None
    return max(lo, min(hi, v))


def clean_wa_text(raw):
    lines = (raw or "").splitlines()
    out = []
    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        s = re.sub(
            r"^\d{1,2}/\d{1,2}/\d{2,4},?"
            r"\s*\d{1,2}:\d{2}\s*"
            r"[ap]m?\s*-\s*", "", s)
        s = re.sub(
            r"^\[?\d{1,2}:\d{2}\]?\s*-+\s*",
            "", s)
        s = re.sub(
            r"^[^:\n]{1,25}:\s", "", s)
        if s:
            out.append(s)
    return "\n".join(out)[:15000]
