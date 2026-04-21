"""
state.py — Variables globales compartidas de Knot.

Todos los módulos importan desde acá. Este archivo no importa nada
de los handlers ni de main.py, evitando imports circulares.
"""

import os
import asyncio
from datetime import datetime, timedelta, timezone
from anthropic import Anthropic
from notion_datastore import NotionDataStore, QueryFilter, DateRange  # noqa: F401

# ── Credenciales y constantes de entorno ──────────────────────────────────────

NOTION_TOKEN         = os.environ["NOTION_TOKEN"]
NOTION_DB_ID         = os.environ["NOTION_DATABASE_ID"]
PLANTS_DB_ID         = os.environ.get("NOTION_PLANTS_DB_ID",         "1ba00dbf2b074e358b296d1d944b914f")
SHOPPING_DB_ID       = os.environ.get("NOTION_SHOPPING_DB_ID",       "cb85fdf75d684f61bafea20b5eeb653f")
RECIPES_DB_ID        = os.environ.get("NOTION_RECIPES_DB_ID",        "8fa008a7-0720-475a-9868-7c3ba077bc50")
MEETINGS_DB_ID       = os.environ.get("NOTION_MEETINGS_DB_ID",       "4ad838f5-3c0e-4605-8859-18fe7b47ac09")
TASKS_DB_ID          = os.environ.get("NOTION_TASKS_DB_ID",          "90b44158-7916-4837-94de-129dde448fc4")
GEO_REMINDERS_DB_ID  = os.environ.get("NOTION_GEO_REMINDERS_DB_ID",  "5fe7a531722843a5af93de1c54a14e02")
CONFIG_DB_ID         = os.environ.get("NOTION_CONFIG_DB_ID",         "2f81017d-a20c-426a-aada-88fcf0743338")
PROJECTS_DB_ID       = os.environ.get("NOTION_PROJECTS_DB_ID",       "0924aff739194c5b8438d03ed82e9e21")
HEALTH_RECORDS_DB_ID = os.environ.get("NOTION_HEALTH_RECORDS_DB_ID", "5f9cde7223f346e48a22f54dbc0836f6")
MEDICATIONS_DB_ID    = os.environ.get("NOTION_MEDICATIONS_DB_ID",    "d16f6826e18d4e4c9e6768a9ebd07507")
FITNESS_DB_ID        = os.environ.get("NOTION_FITNESS_DB_ID",        "c6eb4ddbfe0245bdb5bfcb2b5e33a6e5")

WA_TOKEN    = os.environ["WHATSAPP_TOKEN"]
WA_PHONE_ID = os.environ["WHATSAPP_PHONE_ID"]
WA_API      = f"https://graph.facebook.com/v22.0/{WA_PHONE_ID}/messages"
MY_NUMBER   = os.environ.get("MY_WA_NUMBER", "54298154894334")

DAILY_SUMMARY_HOUR = int(os.environ.get("DAILY_SUMMARY_HOUR", "8"))

USER_LAT = os.environ.get("USER_LAT")
USER_LON = os.environ.get("USER_LON")

GCAL_TOKEN_URL    = "https://oauth2.googleapis.com/token"
GCAL_CLIENT_ID    = os.environ.get("GCAL_CLIENT_ID", "")
GCAL_CLIENT_SECRET = os.environ.get("GCAL_CLIENT_SECRET", "")
GCAL_REFRESH_TOKEN = os.environ.get("GCAL_REFRESH_TOKEN", "")
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY", "")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")
GMAPS_GEOCODING_KEY = os.environ.get("GMAPS_GEOCODING_KEY", "")

# ── DataStore ─────────────────────────────────────────────────────────────────

_ds = NotionDataStore(
    token=NOTION_TOKEN,
    db_ids={
        "finances":       NOTION_DB_ID,
        "shopping":       SHOPPING_DB_ID,
        "recipes":        RECIPES_DB_ID,
        "plants":         PLANTS_DB_ID,
        "meetings":       MEETINGS_DB_ID,
        "tasks":          TASKS_DB_ID,
        "config":         CONFIG_DB_ID,
        "geo_reminders":  GEO_REMINDERS_DB_ID,
        "projects":       PROJECTS_DB_ID,
        "health_records": HEALTH_RECORDS_DB_ID,
        "medications":    MEDICATIONS_DB_ID,
        "fitness":        FITNESS_DB_ID,
    },
)

# ── Constantes de dominio ─────────────────────────────────────────────────────

DIAS_SEMANA = ["lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo"]

INGRESO_EXACT = "\u2192INGRESO\u2190"
EGRESO_EXACT  = "\u2190 EGRESO \u2192"

MAX_HISTORY = 10

# ── Variables de estado en memoria (mutables, compartidas entre módulos) ──────

user_prefs: dict = {
    "daily_summary_hour": None,
    "daily_summary_minute": None,
    "greeting_name": None,
    "activities": {},
    "resumen_extras": [],
    "resumen_nocturno_hour": 22,
    "resumen_nocturno_enabled": True,
    "resumen_semanal_enabled": True,
    "resumen_semanal_hour": 21,
    "news_topics": [],
    "service_providers": {},
    "known_places": [],
    "_config_page_id": None,
    "domain_profiles": {
        "actividad_fisica": "",
        "dieta": "",
        "supermercado": "",
        "gastos": "",
        "salud": "",
        "social": "",
        "hogar": "",
        "productividad": "",
    },
    "purchase_counts": {},
}

current_location: dict = {
    "lat": float(USER_LAT) if USER_LAT else None,
    "lon": float(USER_LON) if USER_LON else None,
    "updated_at": None,
    "location_name": None,
    "source": "env" if USER_LAT else "default",
}

geo_reminders_cache: list[dict] = []

last_event_touched: dict[str, dict] = {}

pending_state: dict[str, dict] = {}

message_buffer: dict[str, list] = {}

chat_history: dict[str, list] = {}

_last_summary_sent: dict[str, datetime | None] = {"daily": None, "nocturno": None}

# ── Helpers de tiempo ─────────────────────────────────────────────────────────

def now_argentina() -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=3)

# ── Claude client ──────────────────────────────────────────────────────────────

anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

async def claude_create(**kwargs):
    """Wrapper con reintentos automaticos para errores 529 (API sobrecargada)."""
    last_err = None
    for attempt in range(3):
        try:
            return anthropic.messages.create(**kwargs)
        except Exception as e:
            last_err = e
            if "529" in str(e) or "overloaded" in str(e).lower():
                await asyncio.sleep(2 ** attempt)
                continue
            raise
    raise last_err

# ── Helpers de tiempo y conversacion ──────────────────────────────────────────

def hoy_str(now: datetime = None) -> str:
    """Retorna 'martes 07/04/2026 08:33' en espanol."""
    if not now:
        now = now_argentina()
    dia = DIAS_SEMANA[now.weekday()]
    return f"{dia} {now.strftime('%d/%m/%Y')} {now.strftime('%H:%M')}"

def semana_str(now: datetime = None) -> str:
    """Retorna tabla de los proximos 7 dias para que Claude no calcule."""
    if not now:
        now = now_argentina()
    lines = []
    for i in range(8):
        d = now + timedelta(days=i)
        label = "HOY" if i == 0 else "MANANA" if i == 1 else ""
        dia = DIAS_SEMANA[d.weekday()]
        entry = f"{dia} {d.strftime('%d/%m/%Y')}"
        if label:
            entry += f" ({label})"
        lines.append(entry)
    return " | ".join(lines)

def get_history(phone: str) -> list:
    return chat_history.get(phone, [])

def add_to_history(phone: str, role: str, content: str):
    if phone not in chat_history:
        chat_history[phone] = []
    chat_history[phone].append({"role": role, "content": content})
    if len(chat_history[phone]) > MAX_HISTORY:
        chat_history[phone] = chat_history[phone][-MAX_HISTORY:]
