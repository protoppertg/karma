import re
import asyncio
import httpx
from datetime import datetime, timedelta
from datetime import time as dtime
from config import TELEGRAM_BOT_TOKEN, TZ, LOG

HTTP = httpx.AsyncClient(timeout=35.0)
TG_BASE = ("https://api.telegram.org/bot"
           + TELEGRAM_BOT_TOKEN)


def IK(*rows):
    kb = []
    for r in rows:
        line = []
        for t, d in r:
            line.append(
                {"text": t,
                 "callback_data": d})
        kb.append(line)
    return {"inline_keyboard": kb}


def esc(s):
    s = str(s if s is not None else "")
    s = s.replace("&", "&amp;")
    s = s.replace("<", "&lt;")
    s = s.replace(">", "&gt;")
    return s


def trunc(s, n=3500):
    return s if len(s) <= n \
        else s[:n] + "\n…"


async def tg(method, **payload):
    if payload.get("reply_markup") is None:
        payload.pop("reply_markup", None)
    for attempt in range(3):
        try:
            r = await HTTP.post(
                TG_BASE + "/" + method,
                json=payload)
            data = r.json()
            if data.get("ok"):
                return data.get("result")
            code = data.get("error_code")
            desc = data.get("description", "")
            if "message is not modified" in desc:
                return True
            if code == 429:
                p = data.get("parameters", {})
                ra = p.get("retry_after", 3)
                await asyncio.sleep(min(ra, 10))
                continue
            if code and code >= 500:
                await asyncio.sleep(
                    1.5 * (attempt + 1))
                continue
            LOG.warning("tg %s: %s",
                        method, desc)
            return None
        except (httpx.TimeoutException,
                httpx.TransportError) as e:
            LOG.warning("tg %s net: %s",
                        method, e)
            await asyncio.sleep(
                1.5 * (attempt + 1))
    return None


async def send(chat_id, text, kb=None):
    text = trunc(text)
    res = await tg(
        "sendMessage", chat_id=chat_id,
        text=text, parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=kb)
    if res is None and ("<" in text
                        or ">" in text):
        plain = re.sub(r"<[^>]+>", "", text)
        await tg("sendMessage",
                 chat_id=chat_id,
                 text=plain,
                 disable_web_page_preview=True,
                 reply_markup=kb)
    return res


async def edit(chat_id, msg_id, text,
               kb=None):
    text = trunc(text)
    res = await tg(
        "editMessageText",
        chat_id=chat_id,
        message_id=msg_id,
        text=text, parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=kb)
    if res is None:
        await send(chat_id, text, kb)
    return res


async def answer_cb(cb_id, text=""):
    try:
        await tg("answerCallbackQuery",
                 callback_query_id=cb_id,
                 text=text)
    except Exception:
        pass


async def send_document(chat_id,
                        filename, data):
    try:
        r = await HTTP.post(
            TG_BASE + "/sendDocument",
            data={"chat_id": str(chat_id)},
            files={"document":
                   (filename, data,
                    "application/json")})
        return r.json().get("ok")
    except Exception as e:
        LOG.warning("sendDocument: %s", e)
        return None


async def get_file_bytes(file_id):
    res = await tg("getFile",
                   file_id=file_id)
    if not res:
        raise RuntimeError("getFile failed")
    url = (TG_BASE + "/file/bot"
           + TELEGRAM_BOT_TOKEN + "/"
           + res["file_path"])
    r = await HTTP.get(url)
    r.raise_for_status()
    return r.content


# ---- format helpers ----
def now_tz():
    return datetime.now(TZ)


def today_d():
    return datetime.now(TZ).date()


def sod(d):
    return datetime.combine(
        d, dtime(0, 0), tzinfo=TZ)


def fm(mins):
    if mins is None:
        return "—"
    mins = int(mins)
    h, m = divmod(abs(mins), 60)
    if h:
        body = (str(h) + "h "
                + format(m, "02d") + "m")
    else:
        body = str(m) + "m"
    return ("-" if mins < 0
            else "") + body


def bar(pct, w=10):
    pct = max(0.0,
              min(100.0, pct or 0.0))
    f = int(round(w * pct / 100))
    return "█" * f + "░" * (w - f)


def arrow(v):
    if v is None:
        return "→"
    if v >= 1.5:
        return "↑"
    if v <= -1.5:
        return "↓"
    return "→"
