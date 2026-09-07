import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi import Request, Header
from fastapi import HTTPException

from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_WEBHOOK_SECRET,
    DATABASE_URL,
    GEMINI_API_KEY,
    GEMINI_MODEL, LOG)
import database
from database import qval
import tg
from tg import tg
import brain
import handlers


@asynccontextmanager
async def lifespan(_app):
    pairs = (
        ("TELEGRAM_BOT_TOKEN",
         TELEGRAM_BOT_TOKEN),
        ("DATABASE_URL",
         DATABASE_URL),
        ("GEMINI_API_KEY",
         GEMINI_API_KEY),
        ("TELEGRAM_WEBHOOK_SECRET",
         TELEGRAM_WEBHOOK_SECRET))
    missing = [k for k, v in pairs
               if not v]
    if missing:
        LOG.error(
            "Missing env vars: %s",
            ", ".join(missing))
        raise RuntimeError(
            "missing env: "
            + str(missing))
    await database.init_db()
    brain.init_ai()
    LOG.info(
        "gemini ready (model=%s)",
        GEMINI_MODEL)
    ext = os.getenv(
        "RENDER_EXTERNAL_URL")
    if ext:
        url = ext + "/webhook"
        try:
            res = await tg(
                "setWebhook",
                url=url,
                secret_token=
                TELEGRAM_WEBHOOK_SECRET,
                allowed_updates=[
                    "message",
                    "callback_query"])
            LOG.info(
                "webhook registered: "
                "%s -> %s",
                url, bool(res))
        except Exception as e:
            LOG.error(
                "webhook registration "
                "failed: %s", e)
    else:
        LOG.warning(
            "RENDER_EXTERNAL_URL "
            "not set — skipping "
            "webhook setup")
    yield
    await tg.HTTP.aclose()
    if database.POOL:
        await database.POOL.close()
    LOG.info("studyos shutdown done")


app = FastAPI(title="StudyOS",
              lifespan=lifespan)


@app.get("/health")
async def health():
    try:
        ok = await qval(
            "SELECT 1") == 1
    except Exception:
        ok = False
    return {"ok": ok,
            "service": "studyos",
            "db": ok,
            "ai": brain.AIC is not None}


@app.post("/webhook")
async def webhook(
        request: Request,
        x_telegram_bot_api_secret_token:
        str = Header(default="")):
    if (x_telegram_bot_api_secret_token
            != TELEGRAM_WEBHOOK_SECRET):
        raise HTTPException(
            status_code=403)
    upd = await request.json()
    uid = upd.get("update_id")
    if uid is None:
        return {"ok": True}
    inserted = await qval(
        """INSERT INTO update_inbox
           (update_id)
           VALUES($1)
           ON CONFLICT DO NOTHING
           RETURNING update_id""",
        uid)
    if inserted is None:
        return {"ok": True}
    try:
        await handlers.process_update(
            upd)
    except Exception:
        LOG.exception(
            "update %s failed", uid)
    return {"ok": True}


@app.get("/")
async def root():
    return {"service": "StudyOS",
            "health": "/health"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, host="0.0.0.0",
        port=int(
            os.getenv("PORT", 8000)))
