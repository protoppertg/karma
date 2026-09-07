import os
import logging
from datetime import timezone
from zoneinfo import ZoneInfo

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN", "")
TELEGRAM_WEBHOOK_SECRET = os.getenv(
    "TELEGRAM_WEBHOOK_SECRET", "")
DATABASE_URL = os.getenv(
    "DATABASE_URL", "")
GEMINI_API_KEY = os.getenv(
    "GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv(
    "GEMINI_MODEL", "gemini-2.0-flash")

TZ_NAME = os.getenv(
    "DEFAULT_TIMEZONE", "Asia/Kolkata")
try:
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    TZ_NAME, TZ = "UTC", timezone.utc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s "
           "%(name)s: %(message)s")
LOG = logging.getLogger("studyos")
