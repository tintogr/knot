import os
import json
import base64
import time
import httpx
from datetime import date, datetime, timedelta, timezone
from calendar import monthrange
from math import radians, sin, cos, sqrt, atan2
from fastapi import FastAPI, Request, BackgroundTasks
from anthropic import Anthropic

app = FastAPI()

anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

def claude_create(**kwargs):
    """Wrapper con reintentos automáticos para errores 529 (API sobrecargada)."""
    last_err = None
    for attempt in range(3):
        try:
            return anthropic.messages.create(**kwargs)
        except Exception as e:
            last_err = e
            if "529" in str(e) or "overloaded" in str(e).lower():
                time.sleep(2 ** attempt)
                continue
            raise
    raise last_err

NOTION_TOKEN   = os.environ["NOTION_TOKEN"]
NOTION_DB_ID   = os.environ["NOTION_DATABASE_ID"]
PLANTS_DB_ID   = os.environ.get("NOTION_PLANTS_DB_ID", "39d22615-0106-43f8-9f01-2632734c38da")
SHOPPING_DB_ID = os.environ.get("NOTION_SHOPPING_DB_ID", "cb85fdf75d684f61bafea20b5eeb653f")
RECIPES_DB_ID  = os.environ.get("NOTION_RECIPES_DB_ID", "8fa008a7-0720-475a-9868-7c3ba077bc50")
MEETINGS_DB_ID = os.environ.get("NOTION_MEETINGS_DB_ID", "ed5b5023-c17c-46e5-be7d-56655f0257ee")
TASKS_DB_ID    = os.environ.get("NOTION_TASKS_DB_ID", "90b44158-7916-4837-94de-129dde448fc4")
CONFIG_DB_ID   = os.environ.get("NOTION_CONFIG_DB_ID", "2f81017d-a20c-426a-aada-88fcf0743338")
WA_TOKEN       = os.environ["WHATSAPP_TOKEN"]
WA_PHONE_ID    = os.environ["WHATSAPP_PHONE_ID"]
WA_API         = f"https://graph.facebook.com/v22.0/{WA_PHONE_ID}/messages"
MY_NUMBER      = os.environ.get("MY_WA_NUMBER", "54298154894334")
DAILY_SUMMARY_HOUR = int(os.environ.get("DAILY_SUMMARY_HOUR", "8"))

USER_LAT = float(os.environ.get("USER_LAT", "-38.95"))
USER_LON = float(os.environ.get("USER_LON", "-68.06"))

def now_argentina() -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=3)

# ── Normalizacion de In-Out ───────────────────────────────────────────────────
INGRESO_EXACT = "\u2192INGRESO\u2190"
EGRESO_EXACT  = "\u2190 EGRESO \u2192"

def normalize_in_out(raw: str) -> str:
    """Fuerza el valor exacto de In-Out para que Notion no rompa formulas."""
    if not raw:
        return EGRESO_EXACT
    upper = raw.upper().strip()
    if "INGRESO" in upper:
        return INGRESO_EXACT
    return EGRESO_EXACT

# ── Helpers de ubicacion ──────────────────────────────────────────────────────
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia en km entre dos puntos GPS."""
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

def get_current_location() -> tuple[float, float]:
    """Devuelve lat, lon actual (dinamica si hay OwnTracks, sino default)."""
    return current_location["lat"], current_location["lon"]

def is_at_known_place() -> dict | None:
    """Retorna el lugar conocido si el usuario esta en uno, sino None."""
    lat, lon = get_current_location()
    for place in user_prefs.get("known_places", []):
        dist_m = haversine_km(lat, lon, place["lat"], place["lon"]) * 1000
        radius = place.get("radius", 200)
        if dist_m <= radius:
            return place
    return None

def is_in_transit() -> bool:
    """True si el usuario se esta moviendo (velocidad > 5 km/h)."""
    return current_location.get("velocity", 0) > 5

async def search_nearby_shops(lat: float, lon: float, shop_type: str) -> list[dict]:
    """Busca comercios cercanos via Google Places API. Retorna lista vacia si no hay API key."""
    api_key = os.environ.get("GOOGLE_PLACES_KEY")
    if not api_key:
        return []
    type_map = {
        "Super": "supermarket",
        "Panaderia": "bakery",
        "Verduleria": "grocery_or_supermarket",
        "Farmacia": "pharmacy",
        "Ferreteria": "hardware_store",
        "Dietetica": "health_food_store",
        "Drogueria": "drugstore",
    }
    place_type = type_map.get(shop_type, "store")
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            r = await http.get(
                "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
                params={
                    "location": f"{lat},{lon}",
                    "radius": 500,
                    "type": place_type,
                    "key": api_key
                }
            )
            if r.status_code == 200:
                results = r.json().get("results", [])
                return [{"name": p["name"], "address": p.get("vicinity", ""),
                         "distance_m": int(haversine_km(lat, lon, p["geometry"]["location"]["lat"], p["geometry"]["location"]["lng"]) * 1000)}
                        for p in results[:3]]
    except Exception:
        pass
    return []

async def check_shopping_proximity():
    """Chequea si hay comercios cerca que vendan cosas de la lista de compras."""
    if is_at_known_place() or is_in_transit():
        return None
    if not current_location.get("updated_at"):
        return None
    # Solo chequear si la ubicacion es reciente (ultimos 10 min)
    age = (now_argentina() - current_location["updated_at"]).total_seconds()
    if age > 600:
        return None
    # Buscar items pendientes agrupados por store
    try:
        async with httpx.AsyncClient() as http:
            r = await http.post(
                f"https://api.notion.com/v1/databases/{SHOPPING_DB_ID}/query",
                headers=notion_headers(),
                json={"filter": {"property": "Stock", "checkbox": {"equals": False}}, "page_size": 50}
            )
            if r.status_code != 200:
                return None
            items = r.json().get("results", [])
            if not items:
                return None
            by_store = {}
            for item in items:
                stores = item["properties"].get("Store", {}).get("multi_select", [])
                name = item["properties"]["Name"]["title"][0]["plain_text"] if item["properties"]["Name"]["title"] else ""
                for s in stores:
                    store_name = s["name"]
                    if store_name not in by_store:
                        by_store[store_name] = []
                    by_store[store_name].append(name)
            if not by_store:
                return None
            lat, lon = get_current_location()
            for store_type, item_names in by_store.items():
                shops = await search_nearby_shops(lat, lon, store_type)
                if shops:
                    return {
                        "store_type": store_type,
                        "items": item_names,
                        "shops": shops
                    }
    except Exception:
        pass
    return None

# ── Memoria de categorías ──────────────────────────────────────────────────────
category_overrides: dict[str, list[str]] = {}

# ── Preferencias del usuario ──────────────────────────────────────────────────
user_prefs: dict = {
    "daily_summary_hour": None,
    "daily_summary_minute": None,
    "greeting_name": None,
    "resumen_extras": [],
    "resumen_nocturno_hour": 22,
    "resumen_nocturno_enabled": True,
    "news_topics": [],
    "service_providers": {},   # {"electricidad": "CALF", "gas": "Camuzzi", ...}
    "known_places": [],        # [{"name": "Casa", "lat": -38.95, "lon": -68.06, "radius": 200}, ...]
    "_config_page_id": None,
}

# ── Ubicacion en tiempo real ──────────────────────────────────────────────────
current_location: dict = {
    "lat": float(os.environ.get("USER_LAT", "-38.95")),
    "lon": float(os.environ.get("USER_LON", "-68.06")),
    "updated_at": None,
    "velocity": 0,
    "source": "default",   # "default" | "owntracks" | "whatsapp"
}

# ── Última entrada tocada (gastos) ────────────────────────────────────────────
last_touched: dict[str, dict] = {}

# ── Último evento tocado (para ediciones contextuales) ────────────────────────
last_event_touched: dict[str, dict] = {}

# ── Estado pendiente (follow-ups) ────────────────────────────────────────────
pending_state: dict[str, dict] = {}

# ── Deduplicación de mensajes ─────────────────────────────────────────────────
processed_message_ids: set[str] = set()
MAX_PROCESSED_IDS = 500

# ── WhatsApp helpers ───────────────────────────────────────────────────────────
async def send_message(to: str, text: str):
    async with httpx.AsyncClient() as http:
        await http.post(WA_API, headers={
            "Authorization": f"Bearer {WA_TOKEN}",
            "Content-Type": "application/json"
        }, json={
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": text}
        })

async def send_interactive_buttons(to: str, body: str, buttons: list[dict], header: str = None):
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": b["id"], "title": b["title"]}}
                    for b in buttons[:3]
                ]
            }
        }
    }
    if header:
        payload["interactive"]["header"] = {"type": "text", "text": header}
    async with httpx.AsyncClient() as http:
        await http.post(WA_API, headers={
            "Authorization": f"Bearer {WA_TOKEN}",
            "Content-Type": "application/json"
        }, json=payload)

async def get_media_base64(media_id: str) -> tuple[str, str]:
    async with httpx.AsyncClient() as http:
        r = await http.get(
            f"https://graph.facebook.com/v22.0/{media_id}",
            headers={"Authorization": f"Bearer {WA_TOKEN}"}
        )
        media_url = r.json()["url"]
        mime_type = r.json().get("mime_type", "image/jpeg")
        img_r = await http.get(media_url, headers={"Authorization": f"Bearer {WA_TOKEN}"})
        return base64.b64encode(img_r.content).decode(), mime_type

# ── Transcripción de audio con Groq Whisper ───────────────────────────────────
async def transcribe_audio(media_id: str) -> str | None:
    groq_key = os.environ.get("GROQ_API_KEY")
    if not groq_key:
        return None
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.get(
            f"https://graph.facebook.com/v22.0/{media_id}",
            headers={"Authorization": f"Bearer {WA_TOKEN}"}
        )
        if r.status_code != 200:
            return None
        media_url = r.json()["url"]
        audio_r = await http.get(media_url, headers={"Authorization": f"Bearer {WA_TOKEN}"})
        if audio_r.status_code != 200:
            return None
        resp = await http.post(
            "https://api.groq.com/openai/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {groq_key}"},
            files={"file": ("audio.ogg", audio_r.content, "audio/ogg")},
            data={"model": "whisper-large-v3", "language": "es"},
        )
        if resp.status_code == 200:
            return resp.json().get("text", "").strip()
    return None

# ── Clima (Open-Meteo) ────────────────────────────────────────────────────────
WMO_CODES = {
    0:  ("Despejado", "☀️"),   1:  ("Mayormente despejado", "🌤️"),
    2:  ("Parcialmente nublado", "⛅"), 3:  ("Nublado", "☁️"),
    45: ("Neblina", "🌫️"),    48: ("Neblina helada", "🌫️"),
    51: ("Llovizna", "🌦️"),   53: ("Llovizna", "🌦️"),   55: ("Llovizna intensa", "🌧️"),
    61: ("Lluvia leve", "🌧️"), 63: ("Lluvia", "🌧️"),     65: ("Lluvia intensa", "🌧️"),
    71: ("Nieve leve", "🌨️"), 73: ("Nieve", "🌨️"),      75: ("Nieve intensa", "🌨️"),
    80: ("Chubascos", "🌦️"),  81: ("Chubascos", "🌦️"),  82: ("Chubascos fuertes", "⛈️"),
    95: ("Tormenta", "⛈️"),   96: ("Tormenta con granizo", "⛈️"), 99: ("Tormenta con granizo", "⛈️"),
}

def wind_description(kmh: float) -> str:
    if kmh < 6:   return "Calma"
    if kmh < 20:  return "Brisa suave"
    if kmh < 39:  return "Brisa moderada"
    if kmh < 62:  return "Viento fuerte"
    if kmh < 89:  return "Viento muy fuerte"
    return "Temporal"

async def get_weather() -> dict | None:
    try:
        lat, lon = get_current_location()
        async with httpx.AsyncClient(timeout=5) as http:
            r = await http.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat, "longitude": lon,
                    "current": "temperature_2m,apparent_temperature,precipitation,windspeed_10m,weathercode",
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max,weathercode",
                    "timezone": "America/Argentina/Buenos_Aires",
                    "forecast_days": 2
                }
            )
            if r.status_code != 200:
                return None
            data = r.json()
            c = data["current"]
            d = data["daily"]
            desc, emoji = WMO_CODES.get(c["weathercode"], ("Variable", "🌡️"))
            viento = round(c["windspeed_10m"])
            desc_manana, emoji_manana = WMO_CODES.get(d["weathercode"][1], ("Variable", "🌡️"))
            viento_manana = round(d["windspeed_10m_max"][1])
            return {
                "temp":           round(c["temperature_2m"]),
                "sensacion":      round(c["apparent_temperature"]),
                "lluvia":         c["precipitation"],
                "viento":         viento,
                "desc":           desc,
                "emoji":          emoji,
                "wind_desc":      wind_description(viento),
                "hoy_max":        round(d["temperature_2m_max"][0]),
                "hoy_min":        round(d["temperature_2m_min"][0]),
                "hoy_lluvia":     d["precipitation_sum"][0],
                "hoy_desc":       desc,
                "hoy_emoji":      emoji,
                "manana_max":     round(d["temperature_2m_max"][1]),
                "manana_min":     round(d["temperature_2m_min"][1]),
                "manana_lluvia":  d["precipitation_sum"][1],
                "manana_viento":  viento_manana,
                "manana_desc":    desc_manana,
                "manana_emoji":   emoji_manana,
                "manana_wind_desc": wind_description(viento_manana),
            }
    except Exception:
        return None

def format_weather_lines(w: dict) -> list[str]:
    lines = [
        f"🌡️ {w['temp']}°C (sensacion {w['sensacion']}°C)",
        f"{w['emoji']} {w['desc']}",
    ]
    if w["lluvia"] > 0:
        lines.append(f"🌧️ Lluvia: {w['lluvia']}mm")
    lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
    return lines

def format_weather_chat(w: dict, include_tomorrow: bool = False) -> str:
    lines = [
        "*Hoy:*",
        f"🌡️ {w['temp']}°C (sensacion {w['sensacion']}°C)",
        f"{w['emoji']} {w['desc']}",
    ]
    if w["lluvia"] > 0:
        lines.append(f"🌧️ Lluvia: {w['lluvia']}mm")
    lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
    if include_tomorrow:
        lines += [
            "", "*Manana:*",
            f"🌡️ {w['manana_min']}°C — {w['manana_max']}°C",
            f"{w['manana_emoji']} {w['manana_desc']}",
        ]
        if w["manana_lluvia"] > 0:
            lines.append(f"🌧️ Lluvia: {w['manana_lluvia']}mm")
        lines.append(f"💨 {w['manana_wind_desc']} ({w['manana_viento']} km/h)")
    return "\n".join(lines)

# ── Tasa de cambio ────────────────────────────────────────────────────────────
async def get_exchange_rate() -> float:
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            r = await http.get("https://dolarapi.com/v1/dolares/blue")
            return float(r.json()["venta"])
    except Exception:
        try:
            async with httpx.AsyncClient(timeout=5) as http:
                r = await http.get("https://dolarapi.com/v1/dolares/oficial")
                return float(r.json()["venta"])
        except Exception:
            return 1000.0

# ── MODULO GASTOS ──────────────────────────────────────────────────────────────

async def handle_gasto_agent(phone: str, text: str, image_b64=None, image_type=None, exchange_rate=1000.0) -> str:
    now = now_argentina()
    tools = [{
        "name": "registrar_gasto",
        "description": "Registra un gasto o ingreso en Notion. Usa solo cuando tenes descripcion Y monto claros.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name":      {"type": "string", "description": "Descripcion corta del gasto"},
                "in_out":    {"type": "string", "enum": ["\u2192INGRESO\u2190", "\u2190 EGRESO \u2192"]},
                "value_ars": {"type": "number"},
                "categoria": {"type": "array", "items": {"type": "string"}},
                "metodo":    {"type": "string", "enum": ["Payment", "Suscription"]},
                "date":      {"type": "string", "description": "YYYY-MM-DD"},
                "time":      {"type": ["string", "null"], "description": "HH:MM o null"},
                "litros":    {"type": ["number", "null"]},
                "notas":     {"type": ["string", "null"]},
                "client":    {"type": "array", "items": {"type": "string"}},
                "emoji":     {"type": "string"}
            },
            "required": ["name", "in_out", "value_ars", "categoria", "metodo", "date", "emoji"]
        }
    }]

    content = []
    if image_b64:
        content.append({"type": "image", "source": {"type": "base64", "media_type": image_type, "data": image_b64}})
    content.append({"type": "text", "text": text or "(ver imagen adjunta)"})

    history = get_history(phone)

    system = f"""Sos Matrics, asistente personal por WhatsApp. Hablas en espanol rioplatense, natural y conciso.
Hoy: {now.strftime("%Y-%m-%d")} {now.strftime("%H:%M")}. Tasa dolar blue: ${exchange_rate:,.0f} ARS/USD.

Tu tarea: registrar gastos e ingresos del usuario.
- Si el mensaje tiene descripcion Y monto -> usa la tool registrar_gasto directamente.
- Si hay una imagen (ticket, screenshot de pedido, factura) -> lee TODOS los items, suma los montos vos mismo, y registra el total. No le pidas al usuario que sume.
- Si falta el monto Y no hay imagen de donde sacarlo -> pregunta de forma natural y breve.
- Si hay ambiguedad (ej: "compre algo" sin monto ni imagen) -> pregunta que fue y cuanto.

Categorias disponibles: Supermercado, Sueldo, Servicios, Transporte, Vianda, Salud, Salud Mental, Salida, Birra, Ocio, Compras, Depto, Plantas, Viajes, Venta.
Servicios = pagos recurrentes (alquiler, luz, gas, internet, streaming, gimnasio). Depto = compras fisicas para el depto (muebles, materiales, herramientas).
Metodo Suscription: gastos recurrentes mensuales. Payment: todo lo demas.
Si in_out es INGRESO -> categoria solo puede ser Sueldo o Venta.
Clientes posibles: LBL, OPERA, ALPATACO, Juan Martin, Depto, Work, Santi Vales, Jorge, Barbara, Vanguardia, Alejo, Dinamo, Paula Diaz, Labti, PlanA, JGA, ATE.
Emoji: elegi el mas especifico segun el contexto real."""

    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=1000,
        system=system,
        messages=history + [{"role": "user", "content": content}],
        tools=tools
    )

    if response.stop_reason == "end_turn":
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "Error procesando").strip()
        add_to_history(phone, "user", text)
        add_to_history(phone, "assistant", reply)
        return reply

    tool_block = next((b for b in response.content if b.type == "tool_use"), None)
    if not tool_block:
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "Error procesando").strip()
        add_to_history(phone, "user", text)
        add_to_history(phone, "assistant", reply)
        return reply

    data = dict(tool_block.input)
    final_cats, cat_note = await check_and_apply_category(data.get("name", ""), data.get("categoria", []))
    data["categoria"] = final_cats

    success, result = await create_notion_entry(data, exchange_rate)

    if success:
        page_id = result
        usd = data["value_ars"] / exchange_rate
        tool_result = (
            f"Registrado exitosamente en Notion. "
            f"Nombre: {data['name']}, "
            f"Monto: ${data['value_ars']:,.0f} ARS (USD {usd:.2f}), "
            f"Categoria: {', '.join(data['categoria'])}, "
            f"Fecha: {data['date']}, "
            f"Cambio usado: ${exchange_rate:,.0f}/USD."
        )
        if cat_note:
            tool_result += f" {cat_note}"
    else:
        page_id = None
        tool_result = f"Error al guardar en Notion: {result[:200]}"

    messages = [
        {"role": "user", "content": content},
        {"role": "assistant", "content": response.content},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tool_block.id, "content": tool_result}]}
    ]
    final_response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=400,
        system=system,
        messages=messages,
        tools=tools
    )
    reply = next((b.text for b in final_response.content if hasattr(b, "text") and b.text), "").strip()

    if success and page_id:
        name_lower = data.get("name", "").lower()
        is_fuel = data.get("emoji") == "⛽" or any(k in name_lower for k in FUEL_KEYWORDS)
        if is_fuel and not data.get("litros"):
            pending_state[phone] = {"type": "litros_followup", "page_id": page_id, "name": data["name"]}
            reply += "\n\n⛽ Cuantos litros cargaste?"

    add_to_history(phone, "user", text)
    add_to_history(phone, "assistant", reply)
    return reply


async def create_notion_entry(data: dict, exchange_rate: float) -> tuple[bool, str]:
    if not data.get("value_ars") or not data.get("in_out"):
        return False, "No se pudo interpretar"
    normalized_in_out = normalize_in_out(data["in_out"])
    props = {
        "Name":          {"title": [{"text": {"content": data["name"]}}]},
        "In - Out":      {"select": {"name": normalized_in_out}},
        "Value (ars)":   {"number": float(data["value_ars"])},
        "Exchange Rate": {"number": exchange_rate},
        "Method":        {"select": {"name": data.get("metodo", "Payment")}},
    }
    if data.get("categoria"):
        props["Category"] = {"multi_select": [{"name": c} for c in data["categoria"]]}
    if data.get("date"):
        if data.get("time"):
            props["Date"] = {"date": {"start": f"{data['date']}T{data['time']}:00", "time_zone": "America/Argentina/Buenos_Aires"}}
        else:
            props["Date"] = {"date": {"start": data["date"]}}
    if data.get("client"):
        props["Client"] = {"multi_select": [{"name": c} for c in data["client"]]}
    if data.get("litros") is not None:
        props["Liters"] = {"number": float(data["litros"])}
    if data.get("consumo_kwh") is not None:
        props["Consumption (kWh)"] = {"number": float(data["consumo_kwh"])}
    if data.get("notas"):
        props["Notes"] = {"rich_text": [{"text": {"content": data["notas"]}}]}
    emoji = data.get("emoji") or "\U0001f4b8"
    db_id = NOTION_DB_ID.replace("-", "")
    async with httpx.AsyncClient() as http:
        r = await http.post("https://api.notion.com/v1/pages",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={"parent": {"database_id": db_id}, "icon": {"type": "emoji", "emoji": emoji}, "properties": props}
        )
        if r.status_code == 200:
            page_id = r.json().get("id", "")
            last_touched[MY_NUMBER] = {"page_id": page_id, "name": data["name"]}
            return True, page_id
        return False, r.text

async def check_and_apply_category(name: str, predicted_cats: list[str]) -> tuple[list[str], str | None]:
    name_lower = name.lower()
    for keyword, saved_cats in category_overrides.items():
        if keyword in name_lower:
            if saved_cats != predicted_cats:
                return saved_cats, f"Categoria: {', '.join(saved_cats)} (segun tu correccion anterior)"
            return saved_cats, None
    try:
        search_key = " ".join(name.split()[:3])
        async with httpx.AsyncClient() as http:
            r = await http.post(
                f"https://api.notion.com/v1/databases/{NOTION_DB_ID.replace('-','')}/query",
                headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
                json={
                    "filter": {"property": "Name", "title": {"contains": search_key}},
                    "sorts": [{"property": "Date", "direction": "descending"}],
                    "page_size": 3
                }
            )
            if r.status_code == 200:
                results = r.json().get("results", [])
                if results:
                    notion_cats = [c["name"] for c in results[0]["properties"].get("Category", {}).get("multi_select", [])]
                    if notion_cats and notion_cats != predicted_cats:
                        category_overrides[search_key.lower()] = notion_cats
                        return notion_cats, f"Categoria: {', '.join(notion_cats)} (como en cargas anteriores)"
    except Exception:
        pass
    return predicted_cats, None

async def corregir_gasto(text: str, phone: str = None) -> tuple[bool, str]:
    now = now_argentina()
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=200,
        system="Extrae que gasto corregir y que cambiar. Si el mensaje no menciona un nombre concreto, usa null en search_term. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Hoy: {now.strftime("%Y-%m-%d")}
Mensaje: {text}
Responde:
{{"search_term": "nombre del gasto o null si no se menciona uno concreto",
  "new_value_ars": nuevo monto en ARS o null,
  "new_categoria": ["categoria"] o null,
  "new_name": "nuevo nombre" o null}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    intent = json.loads(raw)

    search_term = intent.get("search_term")
    page_id_direct = None

    if not search_term and phone and phone in last_touched:
        entry = last_touched[phone]
        page_id_direct = entry["page_id"]
        search_term = entry["name"]
    elif not search_term:
        return False, "No entendi que gasto queres corregir"

    async with httpx.AsyncClient() as http:
        if page_id_direct:
            page_id = page_id_direct
            old_name = search_term
            old_value = 0
        else:
            r = await http.post(
                f"https://api.notion.com/v1/databases/{NOTION_DB_ID.replace('-','')}/query",
                headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
                json={
                    "filter": {"property": "Name", "title": {"contains": search_term[:30]}},
                    "sorts": [{"property": "Date", "direction": "descending"}],
                    "page_size": 1
                }
            )
            if r.status_code != 200 or not r.json().get("results"):
                return False, f"No encontre ningun gasto llamado _{search_term}_"
            page = r.json()["results"][0]
            page_id = page["id"]
            old_name = page["properties"]["Name"]["title"][0]["plain_text"] if page["properties"]["Name"]["title"] else "?"
            old_value = page["properties"].get("Value (ars)", {}).get("number", 0)

        props = {}
        if intent.get("new_value_ars"):
            props["Value (ars)"] = {"number": float(intent["new_value_ars"])}
        if intent.get("new_categoria"):
            props["Category"] = {"multi_select": [{"name": c} for c in intent["new_categoria"]]}
        if intent.get("new_name"):
            props["Name"] = {"title": [{"text": {"content": intent["new_name"]}}]}
        if not props:
            return False, "No entendi que campo queres cambiar"

        upd = await http.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={"properties": props}
        )
        if upd.status_code != 200:
            return False, f"Error actualizando en Notion: {upd.text[:100]}"

        if phone:
            new_name = intent.get("new_name") or old_name
            last_touched[phone] = {"page_id": page_id, "name": new_name}

        changes = []
        if intent.get("new_value_ars"):
            changes.append(f"${old_value:,.0f} -> *${float(intent['new_value_ars']):,.0f} ARS*")
        if intent.get("new_categoria"):
            changes.append(f"Categoria -> _{', '.join(intent['new_categoria'])}_")
        if intent.get("new_name"):
            changes.append(f"Nombre -> _{intent['new_name']}_")
        return True, f"*{old_name}* corregido\n" + "\n".join(changes) + "\n\nActualizado en Notion"

async def eliminar_gasto(text: str) -> tuple[bool, str]:
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=100,
        system="Extrae el nombre de la entrada de Notion a eliminar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f'Mensaje: {text}\nResponde: {{"search_term": "nombre de la entrada a eliminar"}}'}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    search_term = json.loads(raw).get("search_term", "")
    if not search_term:
        return False, "No entendi que entrada queres eliminar"

    async with httpx.AsyncClient() as http:
        r = await http.post(
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID.replace('-','')}/query",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={
                "filter": {"property": "Name", "title": {"contains": search_term[:30]}},
                "sorts": [{"property": "Date", "direction": "descending"}],
                "page_size": 1
            }
        )
        if r.status_code != 200 or not r.json().get("results"):
            return False, f"No encontre ninguna entrada llamada _{search_term}_"

        page = r.json()["results"][0]
        page_id = page["id"]
        old_name = page["properties"]["Name"]["title"][0]["plain_text"] if page["properties"]["Name"]["title"] else "?"

        del_r = await http.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={"archived": True}
        )
        if del_r.status_code == 200:
            return True, f"*{old_name}* eliminado de Notion"

async def eliminar_shopping(text: str) -> tuple[bool, str]:
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=100,
        system="Extrae el nombre del item de la lista de compras a eliminar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"Mensaje: {text}\nResponde: {{\"search_term\": \"nombre del item\"}}"}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    search_term = json.loads(raw).get("search_term", "")
    if not search_term:
        return False, "No entendi que item queres eliminar"
    existing = await search_shopping_item(search_term)
    if not existing:
        return False, f"No encontre ningun item llamado _{search_term}_ en la lista"
    page_id = existing[0]["id"]
    item_name = existing[0]["properties"]["Name"]["title"][0]["plain_text"] if existing[0]["properties"]["Name"]["title"] else search_term
    async with httpx.AsyncClient() as http:
        r = await http.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={"archived": True}
        )
        if r.status_code == 200:
            return True, f"*{item_name}* eliminado de la lista de compras"
        return False, f"Error eliminando el item: {r.text[:100]}"

# ── MODULO PLANTAS ─────────────────────────────────────────────────────────────
PLANTA_SYSTEM = """Extrae info de una planta y genera recomendaciones de cuidado.
Responde UNICAMENTE con JSON valido, sin markdown.
Valores para "luz": Sombra, Indirecta, Directa parcial, Pleno sol
Valores para "riego": Cada 2-3 dias, Semanal, Quincenal, Mensual
Valores para "ubicacion": Interior, Exterior, Balcon, Terraza
Valores para "estado": Excelente, Bien, Regular, Necesita atencion"""

async def parse_planta(text: str, exchange_rate: float) -> dict:
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=600,
        system=PLANTA_SYSTEM,
        messages=[{"role": "user", "content": f"""Hoy: {now_argentina().strftime("%Y-%m-%d")}. Dolar: ${exchange_rate:,.0f}
Mensaje: {text}
Responde:
{{"name":"nombre comun","especie":"nombre cientifico o null","fecha_compra":"YYYY-MM-DD","precio":numero o null,"luz":"Indirecta","riego":"Semanal","ubicacion":"Interior","estado":"Bien","emoji":"emoji planta","notas":"2-3 consejos concisos de cuidado"}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

async def create_planta(data: dict) -> tuple[bool, str]:
    props = {"Name": {"title": [{"text": {"content": data.get("name", "Planta")}}]}}
    if data.get("especie"):
        props["Species"] = {"rich_text": [{"text": {"content": data["especie"]}}]}
    if data.get("fecha_compra"):
        props["Purchase Date"] = {"date": {"start": data["fecha_compra"]}}
    if data.get("precio"):
        props["Price"] = {"number": float(data["precio"])}
    if data.get("luz"):
        props["Light"] = {"select": {"name": data["luz"]}}
    if data.get("riego"):
        props["Watering"] = {"select": {"name": data["riego"]}}
    if data.get("ubicacion"):
        props["Location"] = {"select": {"name": data["ubicacion"]}}
    if data.get("estado"):
        props["Status"] = {"select": {"name": data["estado"]}}
    if data.get("notas"):
        props["Notes"] = {"rich_text": [{"text": {"content": data["notas"]}}]}
    emoji = data.get("emoji", "\U0001f33f")
    async with httpx.AsyncClient() as http:
        r = await http.post("https://api.notion.com/v1/pages",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={"parent": {"database_id": PLANTS_DB_ID}, "icon": {"type": "emoji", "emoji": emoji}, "properties": props}
        )
        return (True, "") if r.status_code == 200 else (False, r.text)

def format_planta(data: dict) -> str:
    emoji = data.get("emoji", "\U0001f33f")
    lines = [
        f"{emoji} *{data['name']}*",
        f"Especie: _{data.get('especie') or 'desconocida'}_",
        f"Luz: {data.get('luz', '-')}",
        f"Riego: {data.get('riego', '-')}",
        f"Ubicacion: {data.get('ubicacion', '-')}",
    ]
    if data.get("notas"):
        lines.append(f"\n{data['notas']}")
    lines.append("\nGuardada en Notion")
    return "\n".join(lines)

# ── MODULO EVENTOS ─────────────────────────────────────────────────────────────
def format_evento(data: dict, guardado: bool) -> str:
    emoji = data.get("emoji", "📅")
    hora = f" a las {data['time']}" if data.get("time") else ""
    fecha_raw = data.get("date", "")
    try:
        fecha = datetime.strptime(fecha_raw, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        fecha = fecha_raw
    summary = data.get("summary", "Evento")
    caption = data.get("caption", "")
    if caption and caption.lower() not in summary.lower():
        summary = f"{summary} -- {caption.strip().capitalize()}"
    lines = [f"{emoji} *{summary}*", f"Fecha: {fecha}{hora}"]
    if data.get("location"):
        lines.append(f"📍 {data['location']}")
    if data.get("description"):
        lines.append(f"Nota: {data['description']}")
    lines.append("\nAgregado a Google Calendar" if guardado else "\nAnota esto manualmente -- Calendar no configurado")
    return "\n".join(lines)

async def parse_evento(text: str, image_b64: str = None, image_type: str = None) -> dict:
    now = now_argentina()
    user_content = []
    if image_b64:
        user_content.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    user_content.append({"type": "text", "text": f"""Hoy es {now.strftime("%Y-%m-%d")}, hora actual: {now.strftime("%H:%M")}
Mensaje: {text or "(ver imagen adjunta)"}
Extrae la info del evento de la imagen si la hay, o del texto.
Responde:
{{"summary":"titulo","date":"YYYY-MM-DD","time":"HH:MM o null","duration_minutes":60,"location":"lugar o null","description":"desc o null","emoji":"emoji"}}"""})
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extrae info de un evento. Responde SOLO JSON valido sin markdown. Usa zona horaria Argentina (UTC-3).",
        messages=[{"role": "user", "content": user_content}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

async def get_gcal_access_token() -> str | None:
    refresh_token = os.environ.get("GCAL_REFRESH_TOKEN")
    client_id     = os.environ.get("GCAL_CLIENT_ID")
    client_secret = os.environ.get("GCAL_CLIENT_SECRET")
    if not all([refresh_token, client_id, client_secret]):
        return None
    async with httpx.AsyncClient() as http:
        r = await http.post("https://oauth2.googleapis.com/token", data={
            "grant_type": "refresh_token", "refresh_token": refresh_token,
            "client_id": client_id, "client_secret": client_secret,
        })
        if r.status_code == 200:
            return r.json().get("access_token")
    return None

def get_event_color(summary: str, is_temp: bool = False) -> str:
    if is_temp:
        return "4"
    medical_kw = {"dr", "dra", "doctor", "medico", "turno", "cita", "hospital",
                  "clinica", "odontologo", "psicologo", "dentista", "cardiologo", "ortopedista", "kinesiologo"}
    if any(kw in summary.lower() for kw in medical_kw):
        return "2"
    return "1"

async def create_evento_gcal(data: dict) -> tuple[bool, str]:
    access_token = await get_gcal_access_token()
    if not access_token:
        return False, ""
    if data.get("time"):
        start = {"dateTime": f"{data['date']}T{data['time']}:00", "timeZone": "America/Argentina/Buenos_Aires"}
        end_dt = datetime.strptime(f"{data['date']}T{data['time']}", "%Y-%m-%dT%H:%M") + timedelta(minutes=data.get("duration_minutes", 60))
        end = {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}
    else:
        start = {"date": data["date"]}
        end = {"date": data["date"]}
    event = {
        "summary": data.get("summary", "Evento"),
        "start": start,
        "end": end,
        "source": {"title": "Matrics", "url": "https://web-production-6874a.up.railway.app"},
        "colorId": get_event_color(data.get("summary", "")),
        "extendedProperties": {"private": {"created_by": "matrics", "type": "evento"}},
    }
    if data.get("description"):
        event["description"] = data["description"]
    if data.get("location"):
        event["location"] = data["location"]
    async with httpx.AsyncClient() as http:
        r = await http.post(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json=event
        )
        if r.status_code in [200, 201]:
            event_id = r.json().get("id", "")
            return True, event_id
        return False, ""

def fuzzy_match_event(search_term: str, events: list) -> dict | None:
    if not events:
        return None
    if not search_term:
        return events[0]
    search_lower = search_term.lower()
    for e in events:
        if search_lower == e.get("summary", "").lower():
            return e
    for e in events:
        if search_lower in e.get("summary", "").lower():
            return e
    for e in events:
        if e.get("summary", "").lower() in search_lower:
            return e
    search_words = set(search_lower.split())
    best_score = 0
    best_event = None
    for e in events:
        event_words = set(e.get("summary", "").lower().split())
        score = len(search_words & event_words)
        if score > best_score:
            best_score = score
            best_event = e
    if best_score > 0:
        return best_event
    return events[0]

async def _find_calendar_event(search_term: str = None, phone: str = None) -> tuple[dict | None, str]:
    """Busca un evento en Calendar con multiples estrategias."""
    access_token = await get_gcal_access_token()
    if not access_token:
        return None, "Calendar no configurado"
    now = now_argentina()
    time_min = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00-03:00")
    time_max = (now + timedelta(days=60)).strftime("%Y-%m-%dT23:59:59-03:00")
    async with httpx.AsyncClient() as http:
        headers = {"Authorization": f"Bearer {access_token}"}
        if not search_term:
            if phone and phone in last_event_touched:
                entry = last_event_touched[phone]
                r = await http.get(
                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{entry['event_id']}",
                    headers=headers
                )
                if r.status_code == 200:
                    return r.json(), ""
            return None, "No encontre contexto de evento reciente."
        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers=headers,
            params={"q": search_term, "timeMin": time_min, "timeMax": time_max,
                    "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"}
        )
        if r.status_code == 200:
            candidates = [e for e in r.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
            if candidates:
                return fuzzy_match_event(search_term, candidates), ""
        if len(search_term.split()) > 1:
            first_word = search_term.split()[0]
            r2 = await http.get(
                "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                headers=headers,
                params={"q": first_word, "timeMin": time_min, "timeMax": time_max,
                        "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"}
            )
            if r2.status_code == 200:
                candidates2 = [e for e in r2.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
                if candidates2:
                    return fuzzy_match_event(search_term, candidates2), ""
        if phone and phone in last_event_touched:
            entry = last_event_touched[phone]
            r3 = await http.get(
                f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{entry['event_id']}",
                headers=headers
            )
            if r3.status_code == 200:
                candidate = r3.json()
                search_words = set(search_term.lower().split())
                event_words = set(candidate.get("summary", "").lower().split())
                if search_words & event_words:
                    return candidate, ""
        return None, f"No encontre ningun evento relacionado con '{search_term}'."

async def find_similar_calendar_events(data: dict) -> list:
    access_token = await get_gcal_access_token()
    if not access_token:
        return []
    summary = data.get("summary", "")
    if not summary or len(summary) < 4:
        return []
    stopwords = {"con", "en", "de", "la", "el", "los", "las", "del", "al", "por", "para",
                 "turno", "cita", "reunion", "evento", "con", "una", "uno"}
    keywords = [w for w in summary.lower().split() if len(w) > 3 and w not in stopwords]
    if not keywords:
        return []
    now = now_argentina()
    time_min = now.strftime("%Y-%m-%dT00:00:00-03:00")
    time_max = (now + timedelta(days=180)).strftime("%Y-%m-%dT23:59:59-03:00")
    found = {}
    async with httpx.AsyncClient() as http:
        headers = {"Authorization": f"Bearer {access_token}"}
        for kw in keywords[:2]:
            try:
                r = await http.get(
                    "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                    headers=headers,
                    params={"q": kw, "timeMin": time_min, "timeMax": time_max,
                            "singleEvents": "true", "orderBy": "startTime", "maxResults": "5"}
                )
                if r.status_code == 200:
                    for e in r.json().get("items", []):
                        if "[TEMP]" not in (e.get("description") or ""):
                            found[e["id"]] = e
            except Exception:
                pass
    return list(found.values())[:3]

# ── HISTORIAL DE CONVERSACION ──────────────────────────────────────────────────
chat_history: dict[str, list] = {}
MAX_HISTORY = 10

def get_history(phone: str) -> list:
    return chat_history.get(phone, [])

def add_to_history(phone: str, role: str, content: str):
    if phone not in chat_history:
        chat_history[phone] = []
    chat_history[phone].append({"role": role, "content": content})
    if len(chat_history[phone]) > MAX_HISTORY:
        chat_history[phone] = chat_history[phone][-MAX_HISTORY:]

# ── Inteligencia conversacional ────────────────────────────────────────────────
async def needs_clarification(phone: str, text: str, context: str) -> str | None:
    try:
        resp = claude_create(
            model="claude-sonnet-4-20250514", max_tokens=100,
            system=f"""Sos Matrics. Evalua si el mensaje del usuario es suficientemente claro para ejecutar la accion indicada.
Contexto: {context}
Si el mensaje es claro -> responde solo: CLEAR
Si hay ambiguedad -> responde solo la pregunta de aclaracion mas concisa y natural posible (max 1 pregunta, tono rioplatense).""",
            messages=[{"role": "user", "content": text}]
        )
        result = resp.content[0].text.strip()
        if result == "CLEAR" or result.startswith("CLEAR"):
            return None
        return result
    except Exception:
        return None

# ── CLASIFICADOR ───────────────────────────────────────────────────────────────
async def classify(text: str, has_image: bool, image_b64: str = None, image_type: str = None, history: list = None) -> str:
    if has_image and not text.strip() and not image_b64:
        return "GASTO"
    content = []
    if image_b64:
        content.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    prompt_text = text if text.strip() else "(ver imagen adjunta)"
    history_ctx = ""
    if history and len(text.strip()) < 80:
        recent = history[-10:] if len(history) >= 10 else history
        history_ctx = "\nContexto reciente de la conversacion:\n" + "\n".join(
            f"{'Usuario' if m['role']=='user' else 'Matrics'}: {str(m['content'])[:120]}"
            for m in recent
        ) + "\n\nTeniendo en cuenta ese contexto, clasifica el siguiente mensaje:"
    content.append({"type": "text", "text": history_ctx + "\n" + prompt_text if history_ctx else prompt_text})
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=10,
        system="""Responde SOLO una palabra: GASTO, CORREGIR_GASTO, PLANTA, EVENTO, EDITAR_EVENTO, ELIMINAR_EVENTO, RECORDATORIO, SHOPPING, REUNION, CONFIGURAR o CHAT.

GASTO: registrar un pago, compra o ingreso concreto con monto. Tambien cuando el mensaje menciona una compra o gasto SIN monto (ej: "compre en la verduleria", "fui al super") -- Matrics pedira el monto.
CORREGIR_GASTO: corregir un gasto ya registrado.
ELIMINAR_GASTO: eliminar o borrar una entrada de Notion.
ELIMINAR_SHOPPING: eliminar o borrar un item de la lista de compras.
PLANTA: adquirir o mencionar una planta.
EDITAR_EVENTO: modificar un evento existente en el calendario.
ELIMINAR_EVENTO: eliminar o borrar un evento del calendario.
RECORDATORIO: "recordame en X tiempo", "avisame en X". NUNCA para cambios de horario del resumen.
EVENTO: crear un evento nuevo -- turno, reunion, cumple, cita, viaje.
SHOPPING: gestionar lista de compras o recetas.
REUNION: notas o fotos de una reunion/llamada.
CONFIGURAR: cambiar configuracion de Matrics. Solo cuando el usuario quiere CAMBIAR algo. Ej: "el resumen mandamelo a las 7", "cambia el horario", "agrega una frase motivadora al resumen". Nunca cuando pregunta o se queja.
CHAT: cualquier pregunta, consulta o conversacion. Si tiene "?" o pide informacion -> CHAT. Incluye preguntas sobre por que no llego el resumen, quejas, consultas sobre el estado del bot, etc.

REGLA: si el mensaje PREGUNTA algo -> siempre CHAT, nunca GASTO.

IMAGENES SIN TEXTO:
- Factura, ticket, recibo -> GASTO
- Invitacion, flyer, screenshot de turno/evento -> EVENTO
- Foto de receta, lista de ingredientes -> SHOPPING
- Pizarron, apuntes de reunion -> REUNION
- Documento de texto generico -> CHAT""",
        messages=[{"role": "user", "content": content}]
    )
    r = response.content[0].text.strip().upper()
    if "ELIMINAR_EVENTO" in r:    return "ELIMINAR_EVENTO"
    if "EDITAR_EVENTO" in r:      return "EDITAR_EVENTO"
    if "ELIMINAR_SHOPPING" in r:  return "ELIMINAR_SHOPPING"
    if "ELIMINAR_GASTO" in r:     return "ELIMINAR_GASTO"
    if "CORREGIR_GASTO" in r:     return "CORREGIR_GASTO"
    if "SHOPPING" in r:           return "SHOPPING"
    if "REUNION" in r:            return "REUNION"
    if "CONFIGURAR" in r:         return "CONFIGURAR"
    if "RECORDATORIO" in r:       return "RECORDATORIO"
    if "PLANTA" in r:             return "PLANTA"
    if "EVENTO" in r:             return "EVENTO"
    if "CHAT" in r:               return "CHAT"
    return "GASTO"

async def query_finances(month: str = None) -> str:
    now = now_argentina()
    if not month:
        month = now.strftime("%Y-%m")
    year, mon = map(int, month.split("-"))
    last_day = monthrange(year, mon)[1]
    async with httpx.AsyncClient() as http:
        r = await http.post(
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID.replace('-','')}/query",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={
                "filter": {"and": [
                    {"property": "Date", "date": {"on_or_after": f"{month}-01"}},
                    {"property": "Date", "date": {"on_or_before": f"{month}-{last_day:02d}"}}
                ]},
                "page_size": 100
            }
        )
        if r.status_code != 200:
            return None
        results = r.json().get("results", [])
        if not results:
            return f"No hay registros para {month}."
        ingresos = egresos = 0
        por_categoria = {}
        for page in results:
            props = page.get("properties", {})
            in_out_name = (props.get("In - Out", {}).get("select", {}) or {}).get("name", "")
            value = props.get("Value (ars)", {}).get("number", 0) or 0
            cats = [c["name"] for c in props.get("Category", {}).get("multi_select", [])]
            if "INGRESO" in in_out_name:
                ingresos += value
            else:
                egresos += value
                for cat in cats:
                    por_categoria[cat] = por_categoria.get(cat, 0) + value
        balance = ingresos - egresos
        top_cats = sorted(por_categoria.items(), key=lambda x: x[1], reverse=True)[:5]
        summary = f"*Finanzas {month}*\n\nIngresos: ${ingresos:,.0f}\nEgresos: ${egresos:,.0f}\nBalance: ${balance:,.0f}\n"
        if top_cats:
            summary += "\n*Top categorias:*\n" + "".join(f"- {c}: ${v:,.0f}\n" for c, v in top_cats)
        return summary

async def query_calendar(days_ahead: int = 2, days_back: int = 0) -> str | None:
    access_token = await get_gcal_access_token()
    if not access_token:
        return None
    now = now_argentina()
    time_min = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00-03:00")
    time_max = (now + timedelta(days=days_ahead)).strftime("%Y-%m-%dT23:59:59-03:00")
    async with httpx.AsyncClient() as http:
        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"timeMin": time_min, "timeMax": time_max, "singleEvents": "true",
                    "orderBy": "startTime", "maxResults": "20"}
        )
        if r.status_code != 200:
            return None
        events = [e for e in r.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
        if not events:
            return "No hay eventos en ese periodo."
        lines = []
        for e in events:
            start = e.get("start", {})
            loc_str = f" -- 📍{e.get('location', '')}" if e.get("location") else ""
            if "dateTime" in start:
                dt = datetime.strptime(start["dateTime"][:16], "%Y-%m-%dT%H:%M")
                lines.append(f"- {dt.strftime('%d/%m')} {dt.strftime('%H:%M')} -- {e.get('summary', 'Evento')}{loc_str}")
            else:
                lines.append(f"- {start.get('date', '')} -- {e.get('summary', 'Evento')} (todo el dia){loc_str}")
        return "\n".join(lines)

async def infer_service_providers() -> dict:
    access_token = await get_gcal_access_token()
    if not access_token:
        return {}
    try:
        async with httpx.AsyncClient(timeout=15) as http:
            headers = {"Authorization": f"Bearer {access_token}"}
            r = await http.get(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages",
                headers=headers,
                params={"q": "newer_than:60d (factura OR comprobante OR boleta OR vencimiento OR suministro OR servicio)", "maxResults": 30}
            )
            if r.status_code != 200:
                return {}
            messages = r.json().get("messages", [])
            if not messages:
                return {}
            mail_summaries = []
            for msg in messages[:15]:
                msg_r = await http.get(
                    f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg['id']}",
                    headers=headers,
                    params={"format": "metadata", "metadataHeaders": ["Subject", "From"]}
                )
                if msg_r.status_code == 200:
                    hdrs = {h["name"]: h["value"] for h in msg_r.json().get("payload", {}).get("headers", [])}
                    snippet = msg_r.json().get("snippet", "")[:150]
                    mail_summaries.append(f"De: {hdrs.get('From','')}\nAsunto: {hdrs.get('Subject','')}\nPreview: {snippet}")
            if not mail_summaries:
                return {}
            resp = claude_create(
                model="claude-sonnet-4-20250514", max_tokens=300,
                system="""Analiza estos mails de facturas/servicios e identifica que empresa provee que servicio.
Responde SOLO JSON con este formato:
{"electricidad": "Nombre empresa", "gas": "Nombre empresa", "internet": "Nombre empresa", "agua": "Nombre empresa", "telefono": "Nombre empresa"}
Solo inclui los servicios que puedas identificar con certeza. Si no hay info suficiente para un servicio, no lo incluyas.""",
                messages=[{"role": "user", "content": "\n---\n".join(mail_summaries)}]
            )
            raw = resp.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.strip("`").lstrip("json").strip()
            return json.loads(raw)
    except Exception:
        return {}

async def get_gmail_summary(query_hint: str = None) -> str | None:
    providers = user_prefs.get("service_providers", {})
    provider_names = list(providers.values())
    if query_hint:
        base_query = query_hint
    elif provider_names:
        providers_query = " OR ".join(provider_names[:5])
        base_query = f"newer_than:30d ({providers_query} OR factura OR comprobante)"
    else:
        base_query = "newer_than:30d (factura OR comprobante OR boleta)"
    access_token = await get_gcal_access_token()
    if not access_token:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as http:
            headers = {"Authorization": f"Bearer {access_token}"}
            r = await http.get(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages",
                headers=headers,
                params={"q": base_query, "maxResults": 20}
            )
            if r.status_code != 200:
                return None
            messages = r.json().get("messages", [])
            if not messages:
                return None
            mail_data = []
            for msg in messages[:15]:
                msg_r = await http.get(
                    f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg['id']}",
                    headers=headers,
                    params={"format": "metadata", "metadataHeaders": ["Subject", "From", "Date"]}
                )
                if msg_r.status_code != 200:
                    continue
                msg_meta = msg_r.json()
                hdrs = {h["name"]: h["value"] for h in msg_meta.get("payload", {}).get("headers", [])}
                snippet = msg_meta.get("snippet", "")[:300]
                invoice_keywords = ["factura", "comprobante", "invoice", "vencimiento", "pago", "importe", "total"]
                subject_lower = hdrs.get("Subject", "").lower()
                is_invoice = any(k in subject_lower or k in snippet.lower() for k in invoice_keywords)
                pdf_texts = []
                if is_invoice:
                    full_r = await http.get(
                        f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg['id']}",
                        headers=headers,
                        params={"format": "full"}
                    )
                    if full_r.status_code == 200:
                        parts = full_r.json().get("payload", {}).get("parts", [])
                        for part in parts[:5]:
                            mime = part.get("mimeType", "")
                            filename = part.get("filename", "")
                            is_pdf = mime == "application/pdf" or (mime == "application/octet-stream" and filename.lower().endswith(".pdf"))
                            if is_pdf:
                                attachment_id = part.get("body", {}).get("attachmentId")
                                if attachment_id:
                                    try:
                                        att_r = await http.get(
                                            f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg['id']}/attachments/{attachment_id}",
                                            headers=headers
                                        )
                                        if att_r.status_code == 200:
                                            pdf_b64 = att_r.json().get("data", "").replace("-", "+").replace("_", "/")
                                            if pdf_b64:
                                                pdf_texts.append(pdf_b64)
                                                break
                                    except Exception:
                                        pass
                mail_data.append({
                    "from": hdrs.get("From", ""),
                    "subject": hdrs.get("Subject", ""),
                    "snippet": snippet,
                    "pdf_attachments": pdf_texts
                })
            if not mail_data:
                return None
            content = []
            mail_summary_text = ""
            for m in mail_data:
                mail_summary_text += f"\nDe: {m['from']}\nAsunto: {m['subject']}\nPreview: {m['snippet']}\n"
            content.append({"type": "text", "text": f"""Analiza estos mails importantes del ultimo mes e identifica los verdaderamente relevantes.
Importante: facturas/vencimientos con montos, mails de personas conocidas que requieren respuesta, algo urgente.
Ignora: newsletters, notificaciones automaticas, publicidad, confirmaciones rutinarias, notificaciones de GitHub/Railway/Notion.
Si hay PDFs adjuntos, leelos y extrae la info relevante (monto, vencimiento, servicio).
Resumi en espanol rioplatense, max 5 lineas. Si no hay nada importante responde solo: NONE

Mails:
{mail_summary_text}"""})
            for m in mail_data:
                for pdf_b64 in m["pdf_attachments"][:1]:
                    try:
                        content.append({
                            "type": "document",
                            "source": {
                                "type": "base64",
                                "media_type": "application/pdf",
                                "data": pdf_b64
                            }
                        })
                    except Exception:
                        pass
            resp = claude_create(
                model="claude-sonnet-4-20250514", max_tokens=400,
                messages=[{"role": "user", "content": content}]
            )
            result = resp.content[0].text.strip()
            return None if result == "NONE" else result
    except Exception:
        return None

async def buscar_gastos(query: str, mes: str = None) -> str:
    now = now_argentina()
    if not mes:
        mes = now.strftime("%Y-%m")
    year, mon = map(int, mes.split("-"))
    from calendar import monthrange
    last_day = monthrange(year, mon)[1]
    try:
        async with httpx.AsyncClient() as http:
            r = await http.post(
                f"https://api.notion.com/v1/databases/{NOTION_DB_ID.replace('-','')}/query",
                headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
                json={
                    "filter": {"and": [
                        {"property": "Date", "date": {"on_or_after": f"{mes}-01"}},
                        {"property": "Date", "date": {"on_or_before": f"{mes}-{last_day:02d}"}},
                        {"property": "Name", "title": {"contains": query[:30]}}
                    ]},
                    "sorts": [{"property": "Date", "direction": "descending"}],
                    "page_size": 10
                }
            )
            if r.status_code != 200:
                return "Error consultando Notion."
            results = r.json().get("results", [])
            if not results:
                return f"No encontre gastos que contengan '{query}' en {mes}."
            lines = []
            for page in results:
                props = page.get("properties", {})
                name = props.get("Name", {}).get("title", [{}])[0].get("plain_text", "?") if props.get("Name", {}).get("title") else "?"
                value = props.get("Value (ars)", {}).get("number", 0) or 0
                date = (props.get("Date", {}).get("date") or {}).get("start", "")[:10]
                in_out = (props.get("In - Out", {}).get("select") or {}).get("name", "")
                direction = "INGRESO" if "INGRESO" in in_out else "EGRESO"
                lines.append(f"- {date} -- {name}: ${value:,.0f} ({direction})")
            return "\n".join(lines)
    except Exception as e:
        return f"Error: {str(e)[:100]}"


async def handle_chat(phone: str, text: str) -> str:
    history = get_history(phone)
    add_to_history(phone, "user", text)
    now = now_argentina()

    # Armar contexto del usuario desde su config en Notion
    user_context_parts = []
    providers = user_prefs.get("service_providers", {})
    if providers:
        prov_str = ", ".join(f"{k}: {v}" for k, v in providers.items())
        user_context_parts.append(f"Proveedores de servicios: {prov_str}.")
    if user_prefs.get("greeting_name"):
        user_context_parts.append(f"Nombre del usuario: {user_prefs['greeting_name']}.")
    resumen_h = user_prefs.get("daily_summary_hour")
    resumen_m = user_prefs.get("daily_summary_minute", 0)
    if resumen_h is not None:
        user_context_parts.append(f"Resumen diario configurado a las {resumen_h:02d}:{resumen_m:02d}.")
    extras = user_prefs.get("resumen_extras", [])
    if extras:
        user_context_parts.append(f"Extras del resumen: {', '.join(extras)}.")
    noc_h = user_prefs.get("resumen_nocturno_hour", 22)
    noc_en = user_prefs.get("resumen_nocturno_enabled", True)
    user_context_parts.append(f"Resumen nocturno: {'activado' if noc_en else 'desactivado'} a las {noc_h:02d}:00.")
    if current_location.get("source") == "owntracks":
        place = is_at_known_place()
        if place:
            user_context_parts.append(f"Ubicacion actual: {place['name']}.")
        else:
            user_context_parts.append(f"Ubicacion actual: {current_location['lat']:.4f}, {current_location['lon']:.4f} (zona desconocida).")
    user_context = "\n".join(user_context_parts)

    tools = [
        {
            "name": "consultar_calendario",
            "description": "Consulta eventos del calendario de Google Calendar. Usa cuando el usuario pregunta sobre su agenda, eventos, turnos, que tiene programado, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "dias_adelante": {"type": "integer", "description": "Cuantos dias hacia adelante consultar. Default 2, usar 7 para 'esta semana', 30 para 'este mes'."},
                    "dias_atras": {"type": "integer", "description": "Cuantos dias hacia atras consultar. Default 0."}
                },
                "required": []
            }
        },
        {
            "name": "consultar_finanzas",
            "description": "Consulta gastos e ingresos registrados en Notion. Usa cuando el usuario pregunta sobre plata, gastos, balance, cuanto gasto, finanzas del mes, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "mes": {"type": "string", "description": "Mes a consultar en formato YYYY-MM. Si no se especifica, usar el mes actual."}
                },
                "required": []
            }
        },
        {
            "name": "consultar_clima",
            "description": "Consulta el clima actual y pronostico. Usa cuando el usuario pregunta sobre el tiempo, temperatura, lluvia, si necesita abrigo, paraguas, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "incluir_manana": {"type": "boolean", "description": "True si pregunta por manana o el pronostico."}
                },
                "required": []
            }
        },
        {
            "name": "consultar_gmail",
            "description": "Consulta los mails importantes no leidos de los ultimos 2 dias. Usa cuando el usuario pregunta sobre emails, correos, facturas recibidas, si le escribieron, notificaciones importantes, etc.",
            "input_schema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        },
        {
            "name": "corregir_gasto",
            "description": "Corrige el monto u otros campos de un gasto ya registrado en Notion. Usa cuando el usuario confirma que queres corregir algo, o cuando encontras una diferencia entre una factura y lo registrado y el usuario pide corregirlo.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_term": {"type": "string", "description": "Nombre o parte del nombre del gasto a corregir. Ej: 'luz', 'CALF', 'Movistar'"},
                    "new_value_ars": {"type": "number", "description": "Nuevo monto en ARS"},
                    "mes": {"type": "string", "description": "Mes en formato YYYY-MM. Si no se especifica usa el mes actual."}
                },
                "required": ["search_term", "new_value_ars"]
            }
        },
        {
            "name": "buscar_gastos",
            "description": "Busca entradas individuales de gastos/ingresos en Notion por nombre. Usa cuando el usuario pregunta si pago algo especifico, si hay un gasto de una empresa concreta, si registro tal o cual pago, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Nombre o parte del nombre a buscar. Ej: 'CALF', 'Movistar', 'alquiler'"},
                    "mes": {"type": "string", "description": "Mes a consultar en formato YYYY-MM. Si no se especifica, usa el mes actual."}
                },
                "required": ["query"]
            }
        },
        {
            "type": "web_search_20250305",
            "name": "web_search"
        },
        {
            "name": "configurar_matrics",
            "description": "Cambia configuracion de Matrics: horario del resumen diario, extras del resumen, saludo, resumen nocturno. Usa SOLO cuando el usuario quiere CAMBIAR algo de la config, no cuando pregunta.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "hour": {"type": ["integer", "null"], "description": "Nueva hora del resumen diario (0-23)"},
                    "minute": {"type": ["integer", "null"], "description": "Nuevos minutos del resumen (0-59), default 0"},
                    "greeting_name": {"type": ["string", "null"], "description": "Nuevo nombre para el saludo matutino"},
                    "add_extra": {"type": ["string", "null"], "description": "Instruccion extra a agregar al resumen"},
                    "remove_extra": {"type": ["string", "null"], "description": "Extra a remover del resumen"},
                    "nocturno_enabled": {"type": ["boolean", "null"], "description": "Activar/desactivar resumen nocturno"},
                    "nocturno_hour": {"type": ["integer", "null"], "description": "Hora del resumen nocturno (0-23)"}
                },
                "required": []
            }
        }
    ]

    system = f"""Sos Matrics, asistente personal en WhatsApp. Respondes conciso y natural en espanol rioplatense.
Hoy: {now.strftime("%d/%m/%Y")} {now.strftime("%H:%M")}.
{user_context}
Si el usuario pregunta algo que ya sabes por su configuracion, responde directamente sin usar herramientas.

Tenes acceso a informacion real del usuario a traves de herramientas:
- Su calendario de Google (eventos, turnos, agenda)
- Sus finanzas en Notion (gastos e ingresos registrados, por categoria o por nombre)
- Su Gmail (mails recibidos, facturas, comprobantes, comunicaciones)
- El clima actual y pronostico
- Busqueda web para informacion externa
- Configuracion de Matrics (cambiar horario del resumen, extras, saludo, nocturno)

Antes de responder cualquier pregunta, pensa que fuentes son relevantes y consulta todas las que hagan falta.

RAZONAMIENTO IMPORTANTE para preguntas sobre pagos de servicios:
1. Busca la factura en Gmail para saber el monto exacto que deberia haberse pagado
2. Busca en Notion usando MULTIPLES terminos: el nombre de la empresa (ej: "CALF") Y el tipo de servicio (ej: "luz", "electricidad") Y variantes posibles
3. Si encontras un pago en Notion con monto parecido al de la factura, asumi que corresponde al mismo gasto aunque el nombre sea diferente
4. Si el monto registrado difiere del de la factura, mencionalo y ofrece corregirlo
5. Si no encontras ningun pago relacionado, deci que no aparece registrado
6. Si mencionaste facturas pendientes y el usuario dice que ya las pago, busca en Notion para verificar antes de pedir montos

Podes usar varias herramientas en el mismo turno. No respondas hasta tener la informacion necesaria.
IMPORTANTE: No inventes datos. Si no encontras info en ninguna fuente, decilo claramente."""

    messages = history + [{"role": "user", "content": text}]

    try:
        response = claude_create(
            model="claude-sonnet-4-20250514", max_tokens=1000,
            system=system,
            messages=messages,
            tools=tools
        )
    except Exception:
        return "Error procesando tu mensaje. Intenta de nuevo."

    if response.stop_reason == "end_turn":
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()
        add_to_history(phone, "assistant", reply)
        return reply

    tool_results = []
    for block in response.content:
        if block.type != "tool_use":
            continue
        tool_name = block.name
        tool_input = block.input
        result = ""
        try:
            if tool_name == "consultar_calendario":
                dias_adelante = tool_input.get("dias_adelante", 2)
                dias_atras = tool_input.get("dias_atras", 0)
                result = await query_calendar(days_ahead=dias_adelante, days_back=dias_atras) or "No hay eventos en ese periodo."
            elif tool_name == "consultar_finanzas":
                mes = tool_input.get("mes") or now.strftime("%Y-%m")
                result = await query_finances(mes) or f"No hay registros para {mes}."
            elif tool_name == "corregir_gasto":
                search_term = tool_input.get("search_term", "")
                new_value = tool_input.get("new_value_ars")
                mes = tool_input.get("mes") or now.strftime("%Y-%m")
                year, mon = map(int, mes.split("-"))
                from calendar import monthrange as mr
                last_day = mr(year, mon)[1]
                try:
                    async with httpx.AsyncClient() as http:
                        r = await http.post(
                            f"https://api.notion.com/v1/databases/{NOTION_DB_ID.replace('-','')}/query",
                            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
                            json={
                                "filter": {"and": [
                                    {"property": "Date", "date": {"on_or_after": f"{mes}-01"}},
                                    {"property": "Date", "date": {"on_or_before": f"{mes}-{last_day:02d}"}},
                                    {"property": "Name", "title": {"contains": search_term[:30]}}
                                ]},
                                "sorts": [{"property": "Date", "direction": "descending"}],
                                "page_size": 1
                            }
                        )
                        if r.status_code == 200 and r.json().get("results"):
                            page = r.json()["results"][0]
                            page_id = page["id"]
                            old_name = page["properties"]["Name"]["title"][0]["plain_text"] if page["properties"]["Name"]["title"] else search_term
                            old_value = page["properties"].get("Value (ars)", {}).get("number", 0) or 0
                            upd = await http.patch(
                                f"https://api.notion.com/v1/pages/{page_id}",
                                headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
                                json={"properties": {"Value (ars)": {"number": float(new_value)}}}
                            )
                            if upd.status_code == 200:
                                result = f"Correccion exitosa: '{old_name}' actualizado de ${old_value:,.0f} a ${new_value:,.0f} ARS."
                            else:
                                result = f"Error actualizando en Notion: {upd.text[:100]}"
                        else:
                            result = f"No encontre ningun gasto llamado '{search_term}' en {mes}."
                except Exception as e:
                    result = f"Error: {str(e)[:100]}"
            elif tool_name == "buscar_gastos":
                query = tool_input.get("query", "")
                mes = tool_input.get("mes") or now.strftime("%Y-%m")
                result = await buscar_gastos(query, mes)
            elif tool_name == "consultar_clima":
                w = await get_weather()
                if w:
                    incluir_manana = tool_input.get("incluir_manana", False)
                    result = format_weather_chat(w, include_tomorrow=incluir_manana)
                else:
                    result = "No pude obtener el clima en este momento."
            elif tool_name == "consultar_gmail":
                if not user_prefs.get("service_providers"):
                    inferred = await infer_service_providers()
                    if inferred:
                        resumen = "\n".join(f"- {k.capitalize()}: *{v}*" for k, v in inferred.items())
                        pending_state[phone] = {
                            "type": "confirm_service_providers",
                            "proposed": inferred
                        }
                        await send_message(phone, f"Encontre tus proveedores de servicios en tus mails:\n\n{resumen}\n\nEs correcto?")
                        await send_interactive_buttons(
                            phone,
                            "Confirmo estos proveedores?",
                            [
                                {"id": "providers_ok", "title": "Si, correcto"},
                                {"id": "providers_no", "title": "Quiero corregir"},
                            ]
                        )
                        result = "Inferi los proveedores y le pregunte al usuario para confirmar. No hay resultado de mail todavia."
                    else:
                        result = "No encontre mails suficientes para identificar proveedores de servicios."
                else:
                    gmail_data = await get_gmail_summary()
                    result = gmail_data or "No encontre mails relevantes."
            elif tool_name == "web_search":
                result = "Busqueda web ejecutada."
            elif tool_name == "configurar_matrics":
                changed = []
                if tool_input.get("greeting_name"):
                    user_prefs["greeting_name"] = tool_input["greeting_name"]
                    changed.append(f"Saludo -> {tool_input['greeting_name']}")
                if tool_input.get("add_extra"):
                    ex = user_prefs.get("resumen_extras", [])
                    ex.append(tool_input["add_extra"])
                    user_prefs["resumen_extras"] = ex
                    changed.append(f"Extra agregado: {tool_input['add_extra']}")
                if tool_input.get("remove_extra"):
                    ex = user_prefs.get("resumen_extras", [])
                    user_prefs["resumen_extras"] = [e for e in ex if tool_input["remove_extra"].lower() not in e.lower()]
                    changed.append(f"Extra removido: {tool_input['remove_extra']}")
                if tool_input.get("hour") is not None:
                    h = int(tool_input["hour"])
                    m = int(tool_input.get("minute", 0) or 0)
                    if 0 <= h <= 23:
                        user_prefs["daily_summary_hour"] = h
                        user_prefs["daily_summary_minute"] = m
                        changed.append(f"Horario resumen -> {h:02d}:{m:02d}")
                if tool_input.get("nocturno_enabled") is not None:
                    user_prefs["resumen_nocturno_enabled"] = tool_input["nocturno_enabled"]
                    changed.append(f"Resumen nocturno -> {'activado' if tool_input['nocturno_enabled'] else 'desactivado'}")
                if tool_input.get("nocturno_hour") is not None:
                    user_prefs["resumen_nocturno_hour"] = int(tool_input["nocturno_hour"])
                    changed.append(f"Hora nocturno -> {int(tool_input['nocturno_hour']):02d}:00")
                if changed:
                    await save_user_config(MY_NUMBER)
                    result = "Configuracion actualizada: " + ", ".join(changed)
                else:
                    result = "No se especifico que cambiar."
        except Exception as e:
            result = f"Error ejecutando {tool_name}: {str(e)[:100]}"

        tool_results.append({
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": result
        })

    if not tool_results:
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()
        add_to_history(phone, "assistant", reply)
        return reply

    messages = messages + [
        {"role": "assistant", "content": response.content},
        {"role": "user", "content": tool_results}
    ]

    try:
        final_response = claude_create(
            model="claude-sonnet-4-20250514", max_tokens=800,
            system=system,
            messages=messages,
            tools=tools
        )
        reply = next((b.text for b in final_response.content if hasattr(b, "text") and b.text), "").strip()
    except Exception:
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "Error procesando").strip()

    add_to_history(phone, "assistant", reply)
    return reply


# ── HANDLER EVENTOS (tool calling) ────────────────────────────────────────────
async def handle_evento_agent(phone: str, text: str, image_b64=None, image_type=None) -> str | None:
    now = now_argentina()
    last_ev = last_event_touched.get(phone, {})
    last_ev_ctx = f"\nUltimo evento creado/editado: \"{last_ev['summary']}\"." if last_ev.get("summary") else ""

    tools = [
        {
            "name": "crear_evento",
            "description": "Crea un nuevo evento en Google Calendar. Usa cuando el usuario quiere agendar algo nuevo.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string", "description": "Titulo del evento"},
                    "date": {"type": "string", "description": "YYYY-MM-DD"},
                    "time": {"type": ["string", "null"], "description": "HH:MM o null para todo el dia"},
                    "duration_minutes": {"type": "integer", "description": "Duracion en minutos, default 60"},
                    "location": {"type": ["string", "null"]},
                    "description": {"type": ["string", "null"]},
                    "emoji": {"type": "string", "description": "Emoji representativo"}
                },
                "required": ["summary", "date", "emoji"]
            }
        },
        {
            "name": "editar_evento",
            "description": "Edita un evento existente. Si no se especifica search_term y hay un ultimo evento, se edita ese.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_term": {"type": ["string", "null"], "description": "Keyword para buscar el evento, o null para el ultimo tocado"},
                    "new_title": {"type": ["string", "null"]},
                    "new_date": {"type": ["string", "null"], "description": "YYYY-MM-DD"},
                    "new_time": {"type": ["string", "null"], "description": "HH:MM"},
                    "new_location": {"type": ["string", "null"]},
                    "new_description": {"type": ["string", "null"]}
                },
                "required": []
            }
        },
        {
            "name": "eliminar_evento",
            "description": "Elimina uno o varios eventos del calendario.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_term": {"type": "string", "description": "Nombre o keyword del evento"},
                    "target_date": {"type": ["string", "null"], "description": "YYYY-MM-DD si menciona fecha"},
                    "delete_all": {"type": "boolean", "description": "True para borrar todos los de esa fecha"}
                },
                "required": ["search_term"]
            }
        },
        {
            "name": "consultar_calendario",
            "description": "Consulta eventos del calendario para verificar antes de actuar.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "dias_adelante": {"type": "integer", "description": "Default 7"},
                    "dias_atras": {"type": "integer", "description": "Default 0"}
                },
                "required": []
            }
        }
    ]

    system = f"""Sos Matrics, asistente personal en WhatsApp. Hablas en espanol rioplatense, natural y conciso.
Hoy: {now.strftime("%d/%m/%Y")} {now.strftime("%H:%M")}.{last_ev_ctx}

Tu tarea: gestionar eventos del calendario del usuario.
- Si el mensaje tiene titulo Y fecha claros -> usa crear_evento.
- Si quiere modificar un evento -> usa editar_evento.
- Si quiere borrar -> usa eliminar_evento.
- Si falta info esencial -> pregunta de forma natural y breve.
- Podes consultar el calendario primero si necesitas verificar algo.
- Si el usuario manda una imagen (flyer, screenshot de turno, invitacion), extrae la info y crea el evento.
IMPORTANTE: No inventes datos. Usa zona horaria Argentina (UTC-3)."""

    content = []
    if image_b64:
        content.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    content.append({"type": "text", "text": text or "(ver imagen adjunta)"})

    messages = get_history(phone) + [{"role": "user", "content": content}]

    try:
        response = claude_create(
            model="claude-sonnet-4-20250514", max_tokens=1000,
            system=system, messages=messages, tools=tools
        )
    except Exception:
        return "Error procesando tu mensaje. Intenta de nuevo."

    if response.stop_reason == "end_turn":
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()
        add_to_history(phone, "user", text)
        add_to_history(phone, "assistant", reply)
        return reply

    tool_results = []
    evento_creado = None

    for block in response.content:
        if block.type != "tool_use":
            continue
        tool_name = block.name
        tool_input = block.input
        result = ""
        try:
            if tool_name == "crear_evento":
                data = dict(tool_input)
                if not data.get("duration_minutes"):
                    data["duration_minutes"] = 60
                guardado, event_id = await create_evento_gcal(data)
                if guardado and event_id:
                    last_event_touched[phone] = {"event_id": event_id, "summary": data.get("summary", "Evento")}
                    evento_creado = {"data": data, "event_id": event_id}
                    hora = f" a las {data['time']}" if data.get("time") else ""
                    try:
                        fecha = datetime.strptime(data["date"], "%Y-%m-%d").strftime("%d/%m/%Y")
                    except Exception:
                        fecha = data["date"]
                    result = f"Evento creado: {data.get('emoji','')} {data['summary']} el {fecha}{hora}."
                    if data.get("location"):
                        result += f" Ubicacion: {data['location']}."
                else:
                    result = "Error creando el evento en Google Calendar."

            elif tool_name == "editar_evento":
                search_term = tool_input.get("search_term")
                target_event, err = await _find_calendar_event(search_term, phone)
                if not target_event:
                    result = err
                else:
                    event = dict(target_event)
                    event_id = event["id"]
                    event_name = event.get("summary", "Evento")
                    if tool_input.get("new_title"):
                        event["summary"] = tool_input["new_title"]
                    if tool_input.get("new_location"):
                        event["location"] = tool_input["new_location"]
                    if tool_input.get("new_description"):
                        event["description"] = tool_input["new_description"]
                    if tool_input.get("new_date") or tool_input.get("new_time"):
                        if "dateTime" in event.get("start", {}):
                            old_dt = event["start"]["dateTime"][:16]
                            new_date = tool_input.get("new_date") or old_dt[:10]
                            new_time = tool_input.get("new_time") or old_dt[11:16]
                            event["start"] = {"dateTime": f"{new_date}T{new_time}:00", "timeZone": "America/Argentina/Buenos_Aires"}
                            if "dateTime" in event.get("end", {}):
                                dur = datetime.strptime(event["end"]["dateTime"][:16], "%Y-%m-%dT%H:%M") - datetime.strptime(old_dt, "%Y-%m-%dT%H:%M")
                                new_end = datetime.strptime(f"{new_date}T{new_time}", "%Y-%m-%dT%H:%M") + dur
                                event["end"] = {"dateTime": new_end.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}
                        elif tool_input.get("new_date"):
                            event["start"] = {"date": tool_input["new_date"]}
                            event["end"] = {"date": tool_input["new_date"]}
                    access_token = await get_gcal_access_token()
                    async with httpx.AsyncClient() as http:
                        update_r = await http.put(
                            f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                            json=event
                        )
                    if update_r.status_code in [200, 201]:
                        last_event_touched[phone] = {"event_id": event_id, "summary": event.get("summary", event_name)}
                        result = f"Evento '{event_name}' actualizado correctamente."
                    else:
                        result = "Error actualizando el evento."

            elif tool_name == "eliminar_evento":
                search_term = tool_input.get("search_term", "")
                target_date = tool_input.get("target_date")
                delete_all = tool_input.get("delete_all", False)
                access_token = await get_gcal_access_token()
                if not access_token:
                    result = "Calendar no configurado"
                else:
                    now_dt = now_argentina()
                    if target_date:
                        t_min = f"{target_date}T00:00:00-03:00"
                        t_max = f"{target_date}T23:59:59-03:00"
                    else:
                        t_min = (now_dt - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00-03:00")
                        t_max = (now_dt + timedelta(days=60)).strftime("%Y-%m-%dT23:59:59-03:00")
                    async with httpx.AsyncClient() as http:
                        headers = {"Authorization": f"Bearer {access_token}"}
                        r = await http.get(
                            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                            headers=headers,
                            params={"q": search_term, "timeMin": t_min, "timeMax": t_max,
                                    "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"}
                        )
                        if r.status_code != 200 or not r.json().get("items"):
                            result = f"No encontre eventos con '{search_term}'."
                        else:
                            events = [e for e in r.json()["items"] if "[TEMP]" not in (e.get("description") or "")]
                            to_delete = events if delete_all else events[:1]
                            deleted = []
                            for ev in to_delete:
                                del_r = await http.delete(
                                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{ev['id']}",
                                    headers=headers
                                )
                                if del_r.status_code == 204:
                                    deleted.append(ev.get("summary", "Evento"))
                            if deleted:
                                result = f"Eliminados: {', '.join(deleted)}."
                            else:
                                result = "No pude eliminar los eventos."

            elif tool_name == "consultar_calendario":
                dias = tool_input.get("dias_adelante", 7)
                dias_atras = tool_input.get("dias_atras", 0)
                result = await query_calendar(days_ahead=dias, days_back=dias_atras) or "No hay eventos."

        except Exception as e:
            result = f"Error: {str(e)[:100]}"

        tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result})

    if not tool_results:
        return next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()

    messages = messages + [
        {"role": "assistant", "content": response.content},
        {"role": "user", "content": tool_results}
    ]
    try:
        final_response = claude_create(
            model="claude-sonnet-4-20250514", max_tokens=600,
            system=system, messages=messages, tools=tools
        )
        reply = next((b.text for b in final_response.content if hasattr(b, "text") and b.text), "").strip()
    except Exception:
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "Error procesando").strip()

    if evento_creado and evento_creado["data"].get("time"):
        data = evento_creado["data"]
        event_dt = f"{data['date']}T{data['time']}"
        pending_state[phone] = {
            "type": "event_reminder",
            "event_id": evento_creado["event_id"],
            "summary": data.get("summary", "Evento"),
            "event_datetime": event_dt
        }
        await send_message(phone, reply)
        await send_interactive_buttons(
            phone,
            "Queres que te avise antes?",
            [
                {"id": "rem_15", "title": "15 min antes"},
                {"id": "rem_60", "title": "1 hora antes"},
                {"id": "rem_no", "title": "No gracias"},
            ]
        )
        add_to_history(phone, "user", text)
        add_to_history(phone, "assistant", reply)
        return None

    add_to_history(phone, "user", text)
    add_to_history(phone, "assistant", reply)
    return reply


# ── Config persistente en Notion ───────────────────────────────────────────────
async def load_user_config(wa_number: str):
    try:
        async with httpx.AsyncClient() as http:
            r = await http.post(
                f"https://api.notion.com/v1/databases/{CONFIG_DB_ID.replace('-','')}/query",
                headers=notion_headers(),
                json={"filter": {"property": "WA Number", "rich_text": {"equals": wa_number}}, "page_size": 1}
            )
            if r.status_code != 200 or not r.json().get("results"):
                return
            page = r.json()["results"][0]
            props = page["properties"]
            def get_num(p): return (props.get(p, {}).get("number") or None)
            def get_txt(p):
                rt = props.get(p, {}).get("rich_text", [])
                return rt[0]["plain_text"] if rt else None
            def get_chk(p): return props.get(p, {}).get("checkbox", False)

            if get_num("Resumen Hour") is not None:
                user_prefs["daily_summary_hour"]   = int(get_num("Resumen Hour"))
            if get_num("Resumen Minute") is not None:
                user_prefs["daily_summary_minute"] = int(get_num("Resumen Minute"))
            if get_num("Resumen Nocturno Hour") is not None:
                user_prefs["resumen_nocturno_hour"] = int(get_num("Resumen Nocturno Hour"))
            user_prefs["resumen_nocturno_enabled"] = get_chk("Resumen Nocturno Enabled")
            extras = get_txt("Resumen Extras")
            if extras:
                user_prefs["resumen_extras"] = [e.strip() for e in extras.split("|") if e.strip()]
            greeting = get_txt("Greeting Name")
            if greeting:
                user_prefs["greeting_name"] = greeting
            topics = get_txt("News Topics")
            if topics:
                user_prefs["news_topics"] = [t.strip() for t in topics.split(",") if t.strip()]
            providers = get_txt("Service Providers")
            if providers:
                try:
                    user_prefs["service_providers"] = json.loads(providers)
                except Exception:
                    user_prefs["service_providers"] = {}
            known = get_txt("Known Places")
            if known:
                try:
                    user_prefs["known_places"] = json.loads(known)
                except Exception:
                    user_prefs["known_places"] = []
            user_prefs["_config_page_id"] = page["id"]
    except Exception:
        pass

async def save_user_config(wa_number: str):
    try:
        if not user_prefs.get("_config_page_id"):
            await load_user_config(wa_number)
        page_id = user_prefs.get("_config_page_id")
        if not page_id:
            return
        extras_str = " | ".join(user_prefs.get("resumen_extras", []))
        topics_str = ", ".join(user_prefs.get("news_topics", []))
        props = {
            "Greeting Name":     {"rich_text": [{"text": {"content": user_prefs.get("greeting_name") or "Buenos dias"}}]},
            "Resumen Extras":    {"rich_text": [{"text": {"content": extras_str}}]},
            "News Topics":       {"rich_text": [{"text": {"content": topics_str}}]},
            "Service Providers": {"rich_text": [{"text": {"content": json.dumps(user_prefs.get("service_providers", {}), ensure_ascii=False)}}]},
            "Known Places":      {"rich_text": [{"text": {"content": json.dumps(user_prefs.get("known_places", []), ensure_ascii=False)}}]},
        }
        if user_prefs.get("daily_summary_hour") is not None:
            props["Resumen Hour"]   = {"number": user_prefs["daily_summary_hour"]}
            props["Resumen Minute"] = {"number": user_prefs.get("daily_summary_minute", 0)}
        if user_prefs.get("resumen_nocturno_hour") is not None:
            props["Resumen Nocturno Hour"] = {"number": user_prefs["resumen_nocturno_hour"]}
        props["Resumen Nocturno Enabled"] = {"checkbox": user_prefs.get("resumen_nocturno_enabled", True)}
        async with httpx.AsyncClient() as http:
            await http.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers=notion_headers(),
                json={"properties": props}
            )
    except Exception:
        pass

# ── MODULO CONFIGURACION ──────────────────────────────────────────────────────
async def handle_configurar(text: str) -> str:
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=200,
        system="Extrae que configuracion cambiar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Mensaje: {text}
Responde:
{{"setting": "daily_summary_hour",
  "hour": hora en formato 24h como entero. null si no hay horario,
  "minute": minutos como entero. si no se mencionan usa 0,
  "greeting_name": nuevo nombre del saludo matutino o null,
  "add_extra": instruccion nueva para agregar al Resumen Diario, o null,
  "remove_extra": texto de instruccion a quitar del Resumen Diario, o null}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        data = json.loads(raw)
    except Exception:
        return "No entendi que configuracion queres cambiar"

    setting = data.get("setting")
    hour    = data.get("hour")
    minute  = data.get("minute", 0) or 0
    greeting_name = data.get("greeting_name")
    add_extra  = data.get("add_extra")
    remove_extra = data.get("remove_extra")

    changed = []

    if greeting_name:
        user_prefs["greeting_name"] = greeting_name
        changed.append(f"Saludo del Resumen Diario -> *{greeting_name}*")

    if add_extra:
        extras = user_prefs.get("resumen_extras", [])
        if add_extra not in extras:
            extras.append(add_extra)
            user_prefs["resumen_extras"] = extras
        changed.append(f"Extra agregado: _{add_extra}_")

    if remove_extra:
        extras = user_prefs.get("resumen_extras", [])
        user_prefs["resumen_extras"] = [e for e in extras if remove_extra.lower() not in e.lower()]
        changed.append(f"Extra removido: _{remove_extra}_")

    if setting == "daily_summary_hour" and hour is not None:
        try:
            hora = int(hour)
            mins = int(minute)
            if not 0 <= hora <= 23:
                return "El horario tiene que estar entre 0 y 23"
            if not 0 <= mins <= 59:
                mins = 0
            user_prefs["daily_summary_hour"]   = hora
            user_prefs["daily_summary_minute"] = mins
            hora_fmt = f"{hora:02d}:{mins:02d}"
            changed.append(f"Horario del resumen -> *{hora_fmt}*")
        except Exception:
            return "No pude interpretar el horario"

    if changed:
        await save_user_config(MY_NUMBER)
        return "Listo:\n" + "\n".join(changed)

    extras_actuales = user_prefs.get("resumen_extras", [])
    hora_actual = user_prefs.get("daily_summary_hour") or DAILY_SUMMARY_HOUR
    mins_actual = user_prefs.get("daily_summary_minute") or 0
    estado = f"Actualmente el Resumen Diario llega a las *{hora_actual:02d}:{mins_actual:02d}*"
    if extras_actuales:
        estado += f" e incluye: {', '.join(extras_actuales)}"
    else:
        estado += " sin extras configurados"
    return f"Dale! Que queres modificar del Resumen Diario?\n\n{estado}\n\nPodes pedirme cosas como cambiar el horario, agregar que te cuente el clima de manana, una frase del dia, o lo que se te ocurra."

# ── MODULO REUNIONES ──────────────────────────────────────────────────────────
async def handle_reunion(text: str, image_b64: str = None, image_type: str = None) -> str:
    now = now_argentina()
    content_parts = []
    if image_b64:
        content_parts.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    prompt_reunion = (
        f"Hoy: {now.strftime('%Y-%m-%d %H:%M')}\n"
        f"Mensaje: {text or '(ver imagen adjunta)'}\n\n"
        "Extrae info de la reunion. Responde SOLO JSON:\n"
        '{"nombre": "titulo/asunto de la reunion",'
        '"con_quien": "nombre(s) de los participantes o null",'
        '"fecha": "YYYY-MM-DD o null si no se menciona",'
        '"notas": "transcripcion o resumen de las notas"}'
    )
    content_parts.append({"type": "text", "text": prompt_reunion})

    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=600,
        system="Extrae info de notas de reunion. Responde SOLO JSON valido sin markdown.",
        messages=[{"role": "user", "content": content_parts}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()

    try:
        data = json.loads(raw)
    except Exception:
        return "No pude interpretar las notas de la reunion"

    nombre    = data.get("nombre") or "Reunion"
    con_quien = data.get("con_quien") or ""
    fecha     = data.get("fecha") or now.strftime("%Y-%m-%d")
    notas     = data.get("notas") or ""

    cal_link = ""
    access_token = await get_gcal_access_token()
    if access_token and con_quien:
        try:
            async with httpx.AsyncClient() as http:
                r = await http.get(
                    "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                    headers={"Authorization": f"Bearer {access_token}"},
                    params={"q": con_quien, "timeMin": f"{fecha}T00:00:00-03:00",
                            "timeMax": f"{fecha}T23:59:59-03:00",
                            "singleEvents": "true", "maxResults": "3"}
                )
                if r.status_code == 200:
                    events = r.json().get("items", [])
                    if events:
                        cal_link = events[0].get("htmlLink", "")
        except Exception:
            pass

    props = {
        "Name": {"title": [{"text": {"content": nombre}}]},
        "Source": {"select": {"name": "Matrics"}},
    }
    if con_quien:
        props["With"] = {"rich_text": [{"text": {"content": con_quien}}]}
    if notas:
        props["Notes"] = {"rich_text": [{"text": {"content": notas[:2000]}}]}
    if fecha:
        props["Date"] = {"date": {"start": fecha}}
    if cal_link:
        props["Calendar Link"] = {"url": cal_link}

    async with httpx.AsyncClient() as http:
        r = await http.post(
            "https://api.notion.com/v1/pages",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={"parent": {"database_id": MEETINGS_DB_ID}, "properties": props}
        )
        if r.status_code != 200:
            return f"Error guardando la reunion: {r.text[:100]}"

    try:
        fecha_fmt = datetime.strptime(fecha, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        fecha_fmt = fecha
    con_str = f" with {con_quien}" if con_quien else ""
    cal_str = f"\nVinculada al evento de Calendar" if cal_link else ""
    return f"*{nombre}* guardada en Meetings{cal_str}\n{fecha_fmt}{con_str}\n\nNotas guardadas en Notion"

# ── PENDING STATE HANDLER ──────────────────────────────────────────────────────
async def handle_pending_state(phone: str, text: str, state: dict) -> bool:
    state_type = state.get("type")

    if state_type == "litros_followup":
        page_id = state["page_id"]
        name    = state["name"]
        try:
            clean = text.strip().replace(",", ".").split()[0]
            litros = float(clean)
            if litros <= 0 or litros > 9999:
                raise ValueError("Numero fuera de rango")
        except (ValueError, IndexError):
            del pending_state[phone]
            return False

        async with httpx.AsyncClient() as http:
            r = await http.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
                json={"properties": {"Liters": {"number": litros}}}
            )
        del pending_state[phone]
        if r.status_code == 200:
            await send_message(phone, f"*{name}* -- {litros}L registrados")
        else:
            await send_message(phone, f"No pude actualizar los litros: {r.text[:80]}")
        return True

    if state_type == "snooze":
        summary = state.get("summary", "Recordatorio")
        del pending_state[phone]

        if text.strip() == "snooze_no":
            await send_message(phone, "Recordatorio descartado")
            return True

        snooze_map = {"snooze_5": 5, "snooze_15": 15, "snooze_30": 30}
        minutes = snooze_map.get(text.strip())
        if minutes:
            fire_at = now_argentina() + timedelta(minutes=minutes)
            event_data = {
                "summary": summary,
                "fire_at": fire_at.strftime("%Y-%m-%dT%H:%M")
            }
            success, _ = await create_recordatorio(event_data)
            if success:
                await send_message(phone, f"Te recuerdo en {minutes} minutos")
            else:
                await send_message(phone, "No pude posponer el recordatorio")
        return True

    if state_type == "event_reminder":
        reminder_map = {
            "rem_15": 15, "rem_30": 30, "rem_60": 60,
            "rem_1d": 1440, "rem_no": None
        }
        minutes = reminder_map.get(text.strip(), "unknown")
        del pending_state[phone]
        if minutes == "unknown":
            return False
        if minutes is None:
            await send_message(phone, "Sin recordatorio adicional")
            return True
        event_summary = state.get("summary", "Evento")
        event_datetime = state.get("event_datetime")
        if event_datetime:
            try:
                fire_dt = datetime.strptime(event_datetime, "%Y-%m-%dT%H:%M") - timedelta(minutes=minutes)
                if fire_dt > now_argentina().replace(tzinfo=None):
                    event_data = {
                        "summary": f"🔔 {event_summary}",
                        "fire_at": fire_dt.strftime("%Y-%m-%dT%H:%M")
                    }
                    success, _ = await create_recordatorio(event_data)
                    label = "1 dia" if minutes == 1440 else f"{minutes} minutos"
                    await send_message(phone, f"Te aviso {label} antes de _{event_summary}_" if success else "No pude crear el recordatorio")
                else:
                    await send_message(phone, "Ese momento ya paso, no puedo crear el recordatorio")
            except Exception:
                await send_message(phone, "Error creando el recordatorio")
        return True

    if state_type == "recipe_ingredients":
        recipe_name = state.get("recipe_name", "Receta")
        ingredients = state.get("ingredients", [])
        del pending_state[phone]
        if text.strip() == "recipe_add_yes":
            results_text = []
            for item in ingredients:
                item_name = item.get("name", "")
                existing = await search_shopping_item(item_name)
                if existing:
                    async with httpx.AsyncClient() as http:
                        await http.patch(f"https://api.notion.com/v1/pages/{existing[0]['id']}",
                                         headers=notion_headers(),
                                         json={"properties": {"Stock": {"checkbox": False}}})
                    results_text.append(f"_{item_name}_ ya estaba, aparece como faltante")
                else:
                    ok, err = await add_shopping_item(item)
                    results_text.append(f"_{item_name}_ agregado" if ok else f"Error: {err[:50]}")
            await send_message(phone, "\n".join(results_text) + "\n\nLista actualizada en Notion")
        else:
            await send_message(phone, f"_{recipe_name.capitalize()}_ guardada. Ingredientes no agregados a la lista de compras.")
        return True

    if state_type == "recipe_review":
        recipe_name = state.get("recipe_name", "Receta")
        ingredients = state.get("ingredients", [])
        recipe_text = state.get("recipe_text", "")

        if text.strip() == "recipe_ok":
            del pending_state[phone]
            pending_state[phone] = {
                "type": "recipe_save_confirm",
                "recipe_name": recipe_name,
                "recipe_text": recipe_text,
                "ingredients": ingredients,
            }
            await send_interactive_buttons(
                phone,
                f"Guardamos *{recipe_name.capitalize()}* en tus Recetas de Notion?",
                [
                    {"id": "recipe_save_yes", "title": "Si, guardar"},
                    {"id": "recipe_save_no",  "title": "No gracias"},
                ]
            )
        elif text.strip() == "recipe_correct":
            del pending_state[phone]
            pending_state[phone] = {
                "type": "recipe_correction_pending",
                "recipe_name": recipe_name,
                "recipe_text": recipe_text,
                "ingredients": ingredients,
            }
            await send_message(phone, "Decime que esta mal -- que falta, que sobra o que cambiar.")
        return True

    if state_type == "recipe_correction_pending":
        recipe_name = state.get("recipe_name", "Receta")
        recipe_text = state.get("recipe_text", "")
        ingredients = state.get("ingredients", [])
        del pending_state[phone]
        try:
            ing_names = [i.get("name", "") for i in ingredients]
            corr_resp = claude_create(
                model="claude-sonnet-4-20250514", max_tokens=600,
                system="Responde SOLO JSON valido sin markdown.",
                messages=[{"role": "user", "content": f"""Receta: "{recipe_name}"
Lista actual de ingredientes: {json.dumps(ing_names, ensure_ascii=False)}
Correccion del usuario: {text}
Aplica la correccion y devolve la lista corregida como array JSON simple:
["ingrediente1", "ingrediente2", ...]"""}]
            )
            raw_corr = corr_resp.content[0].text.strip()
            if raw_corr.startswith("```"):
                raw_corr = raw_corr.strip("`").lstrip("json").strip()
            corrected_names = json.loads(raw_corr)
            enriched_corrected = await enrich_items_with_claude(corrected_names)
        except Exception:
            enriched_corrected = ingredients
        ing_list = "\n".join(f"- {i.get('emoji','🛒')} {i.get('name','')}" for i in enriched_corrected)
        pending_state[phone] = {
            "type": "recipe_review",
            "recipe_name": recipe_name,
            "recipe_text": recipe_text,
            "ingredients": enriched_corrected,
        }
        await send_message(
            phone,
            f"*{recipe_name.capitalize()}* -- version corregida:\n\n*Ingredientes:*\n{ing_list}"
        )
        await send_interactive_buttons(
            phone,
            "Esta todo bien o seguis corrigiendo?",
            [
                {"id": "recipe_ok",      "title": "Esta bien"},
                {"id": "recipe_correct", "title": "Seguir corrigiendo"},
            ]
        )
        return True

    if state_type == "recipe_save_confirm":
        recipe_name = state.get("recipe_name", "Receta")
        recipe_text = state.get("recipe_text", "")
        ingredients = state.get("ingredients", [])
        del pending_state[phone]
        if text.strip() == "recipe_save_yes":
            await send_message(phone, "Guardando receta en Notion...")
            ok, err = await save_recipe_to_notion(recipe_name, source="Matrics", ingredient_names=ingredients, recipe_text=recipe_text)
            if not ok:
                await send_message(phone, f"Error guardando la receta: {err}")
                return True
            ing_list = "\n".join(f"- {i.get('emoji','🛒')} {i.get('name','')}" for i in ingredients)
            pending_state[phone] = {
                "type": "recipe_ingredients",
                "recipe_name": recipe_name,
                "ingredients": ingredients,
            }
            await send_message(
                phone,
                f"*{recipe_name.capitalize()}* guardada en Recipes\n\n*Ingredientes:*\n{ing_list}"
            )
            await send_interactive_buttons(
                phone,
                "Los agregas a la lista de compras?",
                [
                    {"id": "recipe_add_yes", "title": "Si, agregar"},
                    {"id": "recipe_add_no",  "title": "No por ahora"},
                ]
            )
        else:
            await send_message(phone, "Receta no guardada.")
        return True

    if state_type == "chat_correction":
        page_id   = state.get("page_id")
        old_value = state.get("old_value")
        new_value = state.get("new_value")
        name      = state.get("name", "gasto")
        del pending_state[phone]
        if text.strip().lower() in ["si", "dale", "ok", "yes", "corregilo", "corrigelo"]:
            if page_id and new_value:
                async with httpx.AsyncClient() as http:
                    r = await http.patch(
                        f"https://api.notion.com/v1/pages/{page_id}",
                        headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
                        json={"properties": {"Value (ars)": {"number": float(new_value)}}}
                    )
                if r.status_code == 200:
                    await send_message(phone, f"*{name}* corregido: ${old_value:,.0f} -> *${new_value:,.0f} ARS*")
                else:
                    await send_message(phone, f"No pude corregir: {r.text[:100]}")
            else:
                await send_message(phone, "No tengo suficiente info para hacer la correccion.")
        else:
            await send_message(phone, "Quedo como estaba.")
        return True

    if state_type == "confirm_service_providers":
        proposed = state.get("proposed", {})
        del pending_state[phone]
        if text.strip() == "providers_ok":
            user_prefs["service_providers"] = proposed
            await save_user_config(phone)
            await send_message(phone, "Listo, ya se quienes son tus proveedores de servicios. La proxima vez que me preguntes sobre facturas voy a buscar directamente.")
        else:
            pending_state[phone] = {"type": "correct_service_providers", "proposed": proposed}
            await send_message(phone, "Dale, decime las correcciones. Por ejemplo: \"gas es Camuzzi, internet es Personal\"")
        return True

    if state_type == "correct_service_providers":
        proposed = state.get("proposed", {})
        del pending_state[phone]
        try:
            resp = claude_create(
                model="claude-sonnet-4-20250514", max_tokens=200,
                system="Aplica las correcciones del usuario al JSON de proveedores. Responde SOLO JSON.",
                messages=[{"role": "user", "content": f"Proveedores actuales: {json.dumps(proposed, ensure_ascii=False)}\nCorrecciones: {text}\nResponde el JSON corregido."}]
            )
            raw = resp.content[0].text.strip().strip("`").lstrip("json").strip()
            corrected = json.loads(raw)
            user_prefs["service_providers"] = corrected
            await save_user_config(phone)
            resumen = ", ".join(f"{k}: {v}" for k, v in corrected.items())
            await send_message(phone, f"Guardado: {resumen}")
        except Exception:
            await send_message(phone, "No pude aplicar las correcciones. Intenta de nuevo.")
        return True

    return False

# ── Webhook ────────────────────────────────────────────────────────────────────
@app.get("/webhook")
async def verify_webhook(request: Request):
    params = dict(request.query_params)
    if params.get("hub.verify_token") == os.environ.get("WHATSAPP_VERIFY_TOKEN", "finanzas_bot_token"):
        return int(params.get("hub.challenge", 0))
    return {"error": "Verification failed"}

@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    try:
        messages = body["entry"][0]["changes"][0]["value"].get("messages")
        if messages:
            background_tasks.add_task(process_message, messages[0])
    except Exception:
        pass
    return {"ok": True}

# ── Keywords para detectar carga de combustible ──────────────────────────────
FUEL_KEYWORDS = {"nafta", "combustible", "gnc", "gasoil", "premium", "super nafta",
                 "carga nafta", "cargue nafta", "puse nafta"}

BOT_PREFIXES = (
    "Procesando", "Receta", "Recordatorio", "Recorda",
    "Buenos dias", "Tu lista", "Finanzas", "Te recuerdo",
    "Guardado", "guardada",
)

def is_bot_message(text: str) -> bool:
    clean = text.lstrip(" *_~")
    return clean.startswith(BOT_PREFIXES)

async def process_message(message: dict):
    from_number = "54298154894334"
    try:
        msg_id = message.get("id", "")
        if msg_id and msg_id in processed_message_ids:
            return
        if msg_id:
            processed_message_ids.add(msg_id)
            if len(processed_message_ids) > MAX_PROCESSED_IDS:
                processed_message_ids.clear()

        msg_type = message["type"]
        text = ""
        image_b64 = image_type = None

        if msg_type == "text":
            text = message["text"]["body"]
        elif msg_type == "interactive":
            btn = message.get("interactive", {}).get("button_reply", {})
            text = btn.get("id", "")
            if not text:
                return
        elif msg_type == "image":
            media_id = message["image"]["id"]
            text = message["image"].get("caption", "")
            image_b64, image_type = await get_media_base64(media_id)
        elif msg_type == "document":
            media_id = message["document"]["id"]
            text = message["document"].get("caption", "")
            image_b64, image_type = await get_media_base64(media_id)
        elif msg_type == "audio":
            media_id = message["audio"]["id"]
            await send_message(from_number, "🎙️ Transcribiendo audio...")
            transcripcion = await transcribe_audio(media_id)
            if transcripcion:
                text = transcripcion
                await send_message(from_number, f"_{transcripcion}_")
            else:
                await send_message(from_number, "No pude transcribir el audio. Mandalo como texto.")
                return
        elif msg_type == "location":
            loc = message.get("location", {})
            lat = loc.get("latitude")
            lon = loc.get("longitude")
            if lat and lon:
                current_location["lat"] = float(lat)
                current_location["lon"] = float(lon)
                current_location["updated_at"] = now_argentina()
                current_location["source"] = "whatsapp"
                current_location["velocity"] = 0
                place = is_at_known_place()
                if place:
                    await send_message(from_number, f"📍 Ubicacion actualizada: *{place['name']}*")
                else:
                    await send_message(from_number, "📍 Ubicacion actualizada. No reconozco este lugar, queres que lo guarde?")
            return
        else:
            return

        if msg_type == "text" and is_bot_message(text):
            return

        if text.strip().lower() in ["/start", "hola", "help", "ayuda"]:
            await send_message(from_number,
                "*Hola! Soy Matrics*\n\n"
                "*Gastos:* _\"Verduleria 3500\"_\n"
                "*Plantas:* _\"Me compre un potus\"_\n"
                "*Eventos:* _\"Manana a las 10 turno medico\"_\n"
                "*Fotos:* manda cualquier factura\n"
                "*Audios:* habla directo, te entiendo\n\n"
                "Todo se guarda automaticamente"
            )
            return

        await send_message(from_number, "Procesando...")

        if from_number in pending_state:
            handled = await handle_pending_state(from_number, text, pending_state.get(from_number, {}))
            if handled:
                return

        if user_prefs.get("_config_page_id") is None:
            await load_user_config(from_number)

        tipo = await classify(text, image_b64 is not None, image_b64, image_type, history=get_history(from_number))
        exchange_rate = await get_exchange_rate()

        if tipo == "GASTO":
            reply = await handle_gasto_agent(from_number, text, image_b64, image_type, exchange_rate)
            await send_message(from_number, reply)

        elif tipo == "ELIMINAR_SHOPPING":
            success, msg = await eliminar_shopping(text)
            await send_message(from_number, msg if success else msg)

        elif tipo == "ELIMINAR_GASTO":
            success, msg = await eliminar_gasto(text)
            await send_message(from_number, msg if success else msg)

        elif tipo == "CORREGIR_GASTO":
            success, msg = await corregir_gasto(text, phone=from_number)
            await send_message(from_number, msg if success else msg)

        elif tipo == "PLANTA":
            parsed = await parse_planta(text, exchange_rate)
            success, error = await create_planta(parsed)
            if success:
                await send_message(from_number, format_planta(parsed))
            else:
                await send_message(from_number, f"Error guardando planta: {error[:200]}")

        elif tipo in ("EVENTO", "EDITAR_EVENTO", "ELIMINAR_EVENTO"):
            reply = await handle_evento_agent(from_number, text, image_b64, image_type)
            if reply:
                await send_message(from_number, reply)

        elif tipo == "RECORDATORIO":
            parsed = await parse_recordatorio(text)
            success, error = await create_recordatorio(parsed)
            if success:
                await send_message(from_number, format_recordatorio(parsed))
            else:
                await send_message(from_number, f"No pude crear el recordatorio: {error[:100]}")

        elif tipo == "SHOPPING":
            shopping_text = text
            if not shopping_text.strip() and image_b64:
                try:
                    extr = claude_create(
                        model="claude-sonnet-4-20250514", max_tokens=1200,
                        system="Transcribi TODO el contenido de la imagen exactamente como esta escrito. Si es una receta: copia el titulo, luego todas las secciones tal como aparecen. No omitas nada.",
                        messages=[{"role": "user", "content": [
                            {"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}},
                            {"type": "text", "text": "Transcribi todo el contenido de esta imagen fielmente."}
                        ]}]
                    )
                    shopping_text = extr.content[0].text.strip()
                except Exception:
                    shopping_text = ""
            respuesta = await handle_shopping(shopping_text, phone=from_number)
            if respuesta is not None:
                await send_message(from_number, respuesta)

        elif tipo == "CONFIGURAR":
            respuesta = await handle_chat(from_number, text)
            await send_message(from_number, respuesta)

        elif tipo == "REUNION":
            respuesta = await handle_reunion(text, image_b64, image_type)
            await send_message(from_number, respuesta)

        elif tipo == "CHAT":
            respuesta = await handle_chat(from_number, text)
            await send_message(from_number, respuesta)
            if "Ingredientes:" in respuesta and "Preparacion:" in respuesta:
                try:
                    ext_response = claude_create(
                        model="claude-sonnet-4-20250514", max_tokens=400,
                        system="Responde SOLO JSON valido sin markdown.",
                        messages=[{"role": "user", "content": f"""Del siguiente texto de receta, extrae el nombre y TODOS los ingredientes.
Texto: {respuesta[:2000]}
Responde:
{{"name": "nombre de la receta",
  "ingredients": ["ingrediente1", "ingrediente2", ...]}}"""}]
                    )
                    raw_ext = ext_response.content[0].text.strip()
                    if raw_ext.startswith("```"):
                        raw_ext = raw_ext.strip("`").lstrip("json").strip()
                    extracted = json.loads(raw_ext)
                    recipe_name_chat = extracted.get("name", "Receta")
                    ingredient_list_chat = extracted.get("ingredients", [])
                    enriched_chat = await enrich_items_with_claude(ingredient_list_chat) if ingredient_list_chat else []
                except Exception:
                    recipe_name_chat = "Receta"
                    enriched_chat = []
                pending_state[from_number] = {
                    "type": "recipe_save_confirm",
                    "recipe_name": recipe_name_chat,
                    "recipe_text": respuesta,
                    "ingredients": enriched_chat,
                }
                await send_interactive_buttons(
                    from_number,
                    f"Guardamos *{recipe_name_chat.capitalize()}* en tus Recetas de Notion?",
                    [
                        {"id": "recipe_save_yes", "title": "Si, guardar"},
                        {"id": "recipe_save_no",  "title": "No gracias"},
                    ]
                )

    except json.JSONDecodeError:
        pass
    except Exception as e:
        try:
            err_msg = f"{type(e).__name__}: {str(e)}"
            await send_message(from_number, f"Error: {err_msg[:200]}")
        except Exception:
            pass

@app.get("/")
async def health():
    return {"status": "ok", "bot": "matrics"}

# ── MODULO RECORDATORIOS ───────────────────────────────────────────────────────
async def parse_recordatorio(text: str) -> dict:
    now = now_argentina()
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extrae info del recordatorio. Responde SOLO JSON valido sin markdown.",
        messages=[{"role": "user", "content": f"""Ahora son las {now.strftime("%Y-%m-%d %H:%M")} en Argentina.
Mensaje: {text}
Responde:
{{"summary": "descripcion","fire_at": "YYYY-MM-DDTHH:MM","emoji": "emoji"}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

async def create_recordatorio(data: dict) -> tuple[bool, str]:
    access_token = await get_gcal_access_token()
    if not access_token:
        return False, "Calendar no configurado"
    fire_at = data["fire_at"]
    start_dt = datetime.strptime(fire_at, "%Y-%m-%dT%H:%M")
    end_dt = start_dt + timedelta(minutes=1)
    summary_raw = data.get("summary", "Recordatorio")
    if summary_raw.startswith("🔔"):
        summary_final = summary_raw
    else:
        summary_final = f"🔔 {summary_raw}"
    event = {
        "summary": summary_final,
        "description": "[TEMP]",
        "start": {"dateTime": f"{fire_at}:00", "timeZone": "America/Argentina/Buenos_Aires"},
        "end":   {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"},
        "source":   {"title": "Matrics", "url": "https://web-production-6874a.up.railway.app"},
        "colorId":  "4",
        "extendedProperties": {"private": {"created_by": "matrics", "type": "recordatorio"}},
    }
    async with httpx.AsyncClient() as http:
        r = await http.post(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json=event
        )
        return (True, "") if r.status_code in [200, 201] else (False, r.text)

def format_recordatorio(data: dict) -> str:
    emoji = data.get("emoji", "🔔")
    fire_at = data.get("fire_at", "")
    try:
        dt = datetime.strptime(fire_at, "%Y-%m-%dT%H:%M")
        fecha = dt.strftime("%d/%m") if dt.date() != now_argentina().date() else "hoy"
        tiempo_str = f"{fecha} a las {dt.strftime('%H:%M')}"
    except Exception:
        tiempo_str = fire_at
    return f"{emoji} *{data['summary']}*\nTe aviso {tiempo_str}\n\nRecordatorio configurado"

# ── CRON JOB ───────────────────────────────────────────────────────────────────
@app.get("/cron")
async def cron_job():
    await load_user_config(MY_NUMBER)
    now = now_argentina()
    fired = []

    effective_hour   = user_prefs.get("daily_summary_hour")
    effective_minute = user_prefs.get("daily_summary_minute")
    if effective_hour is None:   effective_hour   = DAILY_SUMMARY_HOUR
    if effective_minute is None: effective_minute = 0
    if now.hour == effective_hour and now.minute == effective_minute:
        try:
            access_token_summary = await get_gcal_access_token()
            async with httpx.AsyncClient() as http_summary:
                await send_daily_summary(http_summary, access_token_summary, now)
            fired.append("DAILY_SUMMARY")
        except Exception:
            pass

    nocturno_enabled = user_prefs.get("resumen_nocturno_enabled", True)
    nocturno_hour    = user_prefs.get("resumen_nocturno_hour", 22)
    if nocturno_enabled and now.hour == nocturno_hour and now.minute == 0:
        try:
            access_token_noc = await get_gcal_access_token()
            async with httpx.AsyncClient() as http_noc:
                await send_resumen_nocturno(http_noc, access_token_noc, now)
            fired.append("RESUMEN_NOCTURNO")
        except Exception:
            pass

    access_token = await get_gcal_access_token()
    if not access_token:
        return {"ok": True, "fired": fired, "time": now.strftime("%H:%M"), "warning": "no gcal token"}

    async with httpx.AsyncClient() as http:
        headers = {"Authorization": f"Bearer {access_token}"}
        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers=headers,
            params={
                "timeMin": now.strftime("%Y-%m-%dT%H:%M:00-03:00"),
                "timeMax": (now + timedelta(minutes=61)).strftime("%Y-%m-%dT%H:%M:00-03:00"),
                "singleEvents": "true", "orderBy": "startTime", "maxResults": "20"
            }
        )
        if r.status_code != 200:
            return {"ok": True, "fired": fired, "time": now.strftime("%H:%M")}
        for event in r.json().get("items", []):
            event_id  = event.get("id")
            summary   = event.get("summary", "Evento")
            desc      = event.get("description", "") or ""
            start     = event.get("start", {})
            if "dateTime" not in start:
                continue
            try:
                diff_seconds = int(
                    (datetime.strptime(start["dateTime"][:16], "%Y-%m-%dT%H:%M") - now.replace(tzinfo=None))
                    .total_seconds()
                )
            except Exception:
                continue

            if "[TEMP]" in desc and -30 <= diff_seconds <= 90:
                clean_summary = summary.replace("🔔 ", "").strip()
                await send_message(MY_NUMBER, f"🔔 *Recordatorio*\n{clean_summary}")
                await http.delete(
                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                    headers=headers
                )
                pending_state[MY_NUMBER] = {"type": "snooze", "summary": clean_summary}
                await send_interactive_buttons(
                    MY_NUMBER,
                    f"Queres posponer este recordatorio?\n_{clean_summary}_",
                    [
                        {"id": "snooze_5",  "title": "5 min"},
                        {"id": "snooze_15", "title": "15 min"},
                        {"id": "snooze_no", "title": "No posponer"},
                    ]
                )
                fired.append(f"TEMP: {summary}")
            elif "[REM:60]" in desc and 59 <= diff_seconds // 60 <= 61:
                loc_str = f"\n📍 {event.get('location')}" if event.get("location") else ""
                await send_message(MY_NUMBER, f"*En 1 hora:* {summary}{loc_str}")
                fired.append(f"REM60: {summary}")
            elif "[REM:15]" in desc and 14 <= diff_seconds // 60 <= 16:
                loc_str = f"\n📍 {event.get('location')}" if event.get("location") else ""
                await send_message(MY_NUMBER, f"*En 15 minutos:* {summary}{loc_str}")
                fired.append(f"REM15: {summary}")

    return {"ok": True, "fired": fired, "time": now.strftime("%H:%M")}


async def send_daily_summary(http, access_token: str, now: datetime):
    r = await http.get(
        "https://www.googleapis.com/calendar/v3/calendars/primary/events",
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "timeMin": now.replace(hour=0, minute=0, second=0).strftime("%Y-%m-%dT00:00:00-03:00"),
            "timeMax": now.replace(hour=23, minute=59, second=59).strftime("%Y-%m-%dT23:59:59-03:00"),
            "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"
        }
    )
    if r.status_code != 200:
        return
    events = [e for e in r.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
    w = await get_weather()
    await load_user_config(MY_NUMBER)
    greeting = user_prefs.get("greeting_name") or "Buenos dias"
    lines = [f"*{greeting}!*", ""]
    if w:
        lines.append(f"🌡️ {w['temp']}C (sensacion {w['sensacion']}C) -- {w['emoji']} {w['desc']}")
        if w["lluvia"] > 0:
            lines.append(f"🌧️ Lluvia ahora: {w['lluvia']}mm")
        lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
        pronostico = f"Hoy: max {w['hoy_max']}C, min {w['hoy_min']}C"
        if w["hoy_lluvia"] > 0:
            pronostico += f", 🌧️ {w['hoy_lluvia']}mm esperados"
        lines.append(pronostico)
        lines.append("")
    if now.weekday() == 0:
        try:
            async with httpx.AsyncClient() as http_week:
                r_week = await http_week.get(
                    "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                    headers={"Authorization": f"Bearer {access_token}"},
                    params={"timeMin": now.strftime("%Y-%m-%dT00:00:00-03:00"),
                            "timeMax": (now + timedelta(days=7)).strftime("%Y-%m-%dT23:59:59-03:00"),
                            "singleEvents": "true", "orderBy": "startTime", "maxResults": "20"}
                )
                if r_week.status_code == 200:
                    week_events = [e for e in r_week.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
                    if week_events:
                        lines.append("*Tu semana:*")
                        for e in week_events:
                            s = e.get("start", {})
                            if "dateTime" in s:
                                dt = datetime.strptime(s["dateTime"][:16], "%Y-%m-%dT%H:%M")
                                lines.append(f"- {dt.strftime('%a %d/%m')} {dt.strftime('%H:%M')} -- {e.get('summary', '')}")
                            else:
                                lines.append(f"- {s.get('date', '')[:10]} -- {e.get('summary', '')} (todo el dia)")
                        lines.append("")
        except Exception:
            pass
    else:
        if not events:
            lines.append("Hoy no tenes eventos agendados.")
        else:
            lines.append(f"*{'Tus eventos de hoy' if len(events) > 1 else 'Tu evento de hoy'}:*")
            for e in events:
                start = e.get("start", {})
                loc_str = f" -- 📍{e.get('location', '')}" if e.get("location") else ""
                if "dateTime" in start:
                    lines.append(f"- {start['dateTime'][11:16]} -- {e.get('summary', 'Evento')}{loc_str}")
                else:
                    lines.append(f"- {e.get('summary', 'Evento')} (todo el dia){loc_str}")
    gmail_summary = await get_gmail_summary()
    if gmail_summary:
        lines.append("")
        lines.append(f"*Mails importantes:*\n{gmail_summary}")

    extras = user_prefs.get("resumen_extras", [])
    if extras:
        try:
            extras_prompt = "\n".join(f"- {e}" for e in extras)
            extra_resp = claude_create(
                model="claude-sonnet-4-20250514", max_tokens=300,
                system=f"Sos Matrics. Hoy es {now.strftime('%A %d/%m/%Y')}. Genera contenido breve (max 3 lineas por item) para los siguientes extras del Resumen Diario. Usas espanol rioplatense, tono natural y calido.",
                messages=[{"role": "user", "content": f"Genera estos extras para el resumen matutino:\n{extras_prompt}"}]
            )
            extra_text = extra_resp.content[0].text.strip()
            if extra_text:
                lines.append("")
                lines.append(extra_text)
        except Exception:
            pass
    await send_message(MY_NUMBER, "\n".join(lines))

async def send_resumen_nocturno(http, access_token: str, now: datetime):
    manana = now + timedelta(days=1)
    r = await http.get(
        "https://www.googleapis.com/calendar/v3/calendars/primary/events",
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "timeMin": manana.strftime("%Y-%m-%dT00:00:00-03:00"),
            "timeMax": manana.strftime("%Y-%m-%dT23:59:59-03:00"),
            "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"
        }
    )
    events_manana = []
    if r.status_code == 200:
        events_manana = [e for e in r.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]

    eventos_str = ""
    if events_manana:
        lineas = []
        for e in events_manana:
            s = e.get("start", {})
            if "dateTime" in s:
                lineas.append(f"- {s['dateTime'][11:16]} -- {e.get('summary','')}")
            else:
                lineas.append(f"- {e.get('summary','')} (todo el dia)")
        eventos_str = "\n".join(lineas)

    context = f"Hoy es {now.strftime('%A %d/%m/%Y')}. Hora: {now.strftime('%H:%M')}."
    if eventos_str:
        context += f"\nEventos de manana:\n{eventos_str}"
    else:
        context += "\nManana no hay eventos agendados."

    try:
        resp = claude_create(
            model="claude-sonnet-4-20250514", max_tokens=300,
            system=f"""Sos Matrics. {context}
Genera un resumen nocturno breve y natural en espanol rioplatense. Inclui:
1. Un saludo de buenas noches y que hay para manana.
2. Una sugerencia espontanea: agendar algo, agregar a la lista de compras, registrar un gasto del dia, o un pensamiento de cierre.
Se conciso, calido, natural. Maximo 5 lineas en total.""",
            messages=[{"role": "user", "content": "Genera el resumen nocturno."}]
        )
        msg = resp.content[0].text.strip()
    except Exception:
        if eventos_str:
            msg = f"Buenas noches! Manana tenes:\n{eventos_str}\n\nQue descanses"
        else:
            msg = "Buenas noches! Manana el dia esta libre. Que descanses"

    await send_message(MY_NUMBER, msg)

# ── ENDPOINT UBICACION (OwnTracks) ────────────────────────────────────────────
_last_proximity_check: dict[str, datetime] = {}
_last_proximity_store: dict[str, str] = {}

@app.post("/location")
async def receive_location(request: Request):
    """Recibe updates de OwnTracks (o cualquier fuente de ubicacion)."""
    try:
        body = await request.json()
        # OwnTracks manda _type: "location"
        msg_type = body.get("_type", "location")
        if msg_type != "location":
            return {"ok": True, "ignored": msg_type}

        lat = body.get("lat")
        lon = body.get("lon")
        vel = body.get("vel", 0)  # velocidad en km/h
        if lat is None or lon is None:
            return {"ok": False, "error": "missing lat/lon"}

        now = now_argentina()
        current_location["lat"] = float(lat)
        current_location["lon"] = float(lon)
        current_location["velocity"] = float(vel) if vel else 0
        current_location["updated_at"] = now
        current_location["source"] = "owntracks"

        # Chequear si hay oportunidad de compra cercana
        # Solo si: no esta en lugar conocido, no esta en transito, no se chequeo hace poco
        phone = MY_NUMBER
        last_check = _last_proximity_check.get(phone)
        should_check = (
            not is_at_known_place()
            and not is_in_transit()
            and (not last_check or (now - last_check).total_seconds() > 1800)  # max cada 30 min
            and 9 <= now.hour <= 21  # solo en horario razonable
        )

        if should_check:
            _last_proximity_check[phone] = now
            proximity = await check_shopping_proximity()
            if proximity:
                store_type = proximity["store_type"]
                # No repetir mismo tipo de tienda en el mismo dia
                last_store = _last_proximity_store.get(phone)
                today = now.strftime("%Y-%m-%d")
                store_key = f"{today}:{store_type}"
                if last_store != store_key:
                    _last_proximity_store[phone] = store_key
                    items_str = ", ".join(proximity["items"][:5])
                    shop = proximity["shops"][0]
                    try:
                        msg_resp = claude_create(
                            model="claude-sonnet-4-20250514", max_tokens=150,
                            system="Sos Matrics. Genera un mensaje breve y natural en espanol rioplatense avisando que el usuario esta cerca de una tienda donde puede comprar cosas que necesita. No seas pesado, se casual y util. Max 3 lineas.",
                            messages=[{"role": "user", "content": f"El usuario esta cerca de {shop['name']} ({shop['address']}, a {shop['distance_m']}m). Necesita comprar: {items_str}. Tipo de tienda: {store_type}."}]
                        )
                        msg = msg_resp.content[0].text.strip()
                    except Exception:
                        msg = f"Estas cerca de {shop['name']} y te faltan: {items_str}"
                    await send_message(phone, msg)

        return {
            "ok": True,
            "lat": lat, "lon": lon,
            "known_place": (is_at_known_place() or {}).get("name"),
            "in_transit": is_in_transit()
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:100]}

@app.post("/location/places")
async def manage_known_places(request: Request):
    """Agregar/listar lugares conocidos. POST con {action: add/list/remove, name, lat, lon, radius}."""
    try:
        body = await request.json()
        action = body.get("action", "list")
        places = user_prefs.get("known_places", [])

        if action == "list":
            return {"ok": True, "places": places}

        elif action == "add":
            name = body.get("name")
            lat = body.get("lat")
            lon = body.get("lon")
            radius = body.get("radius", 200)
            if not name or lat is None or lon is None:
                return {"ok": False, "error": "missing name/lat/lon"}
            places.append({"name": name, "lat": float(lat), "lon": float(lon), "radius": int(radius)})
            user_prefs["known_places"] = places
            await save_user_config(MY_NUMBER)
            return {"ok": True, "added": name, "total": len(places)}

        elif action == "remove":
            name = body.get("name", "").lower()
            user_prefs["known_places"] = [p for p in places if p["name"].lower() != name]
            await save_user_config(MY_NUMBER)
            return {"ok": True, "removed": name}

        return {"ok": False, "error": "unknown action"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:100]}

@app.get("/health")
async def health_check():
    return {"status": "ok", "time": now_argentina().strftime("%H:%M"), "bot": "matrics",
            "location": {"lat": current_location["lat"], "lon": current_location["lon"],
                         "source": current_location["source"],
                         "known_place": (is_at_known_place() or {}).get("name")}}

# ── MODULO SHOPPING ────────────────────────────────────────────────────────────
def notion_headers():
    return {"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"}

SHOPPING_CATEGORIES = ["Frutas y verduras", "Enlatado", "Infusion", "Lacteo", "Especias",
                       "Limpieza", "Panificado", "Herramienta", "Construccion", "Higiene",
                       "Electronica", "Carne", "Galletitas", "Alcohol", "Bebida", "Fiambre",
                       "Grano", "Comida", "Cosmetica"]
SHOPPING_STORES    = ["Super", "Panaderia", "Verduleria", "Dietetica", "Farmacia", "Drogueria", "Ferreteria"]
SHOPPING_FREQUENCY = ["Often", "Monthly", "Annual", "One time"]

async def get_ingredients_and_enrich(recipe_name: str, recipe_text: str = None) -> tuple[list[dict], bool]:
    if recipe_text:
        context = f'Receta: "{recipe_name}"\nTexto completo de la receta:\n{recipe_text[:2000]}\n\nExtrae TODOS los ingredientes que aparecen en el texto de la receta.'
    else:
        context = f'Receta: "{recipe_name}"\n\nInferi los ingredientes tipicos/estandar completos de esta receta.'
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=800,
        system="Responde SOLO JSON valido sin markdown ni texto extra.",
        messages=[{"role": "user", "content": f"""{context}

Responde SOLO este array JSON:
[{{
  "name": "nombre del ingrediente capitalizado SIN cantidad",
  "display": "cantidad + nombre como aparece en la receta",
  "emoji": "emoji especifico del producto",
  "category": una de {SHOPPING_CATEGORIES},
  "store": tienda mas logica,
  "frequency": uno de {SHOPPING_FREQUENCY}
}}]"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        items = json.loads(raw)
        return items, True
    except Exception:
        return [], False

async def enrich_items_with_claude(items: list[str]) -> list[dict]:
    if not items:
        return []
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=1500,
        system="Enriquece una lista de items. Responde SOLO JSON valido sin markdown.",
        messages=[{"role": "user", "content": f"""Items: {json.dumps(items, ensure_ascii=False)}

Para cada item responde un array con:
- "name": nombre capitalizado
- "emoji": emoji especifico (nunca 🛒)
- "category": una de {SHOPPING_CATEGORIES}
- "store": tienda mas logica
- "frequency": uno de {SHOPPING_FREQUENCY}

Responde SOLO el array JSON."""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        return json.loads(raw)
    except Exception:
        return [{"name": i.capitalize(), "emoji": "🛒", "category": "", "store": "", "frequency": "One time"} for i in items]

async def search_recipe_in_notion(recipe_name: str) -> list[str] | None:
    try:
        async with httpx.AsyncClient() as http:
            r = await http.post(
                f"https://api.notion.com/v1/databases/{RECIPES_DB_ID.replace('-','')}/query",
                headers=notion_headers(),
                json={"filter": {"property": "Name", "title": {"contains": recipe_name[:30]}}, "page_size": 1}
            )
            if r.status_code != 200 or not r.json().get("results"):
                return None
            page = r.json()["results"][0]
            ingredientes = [i["name"] for i in page["properties"].get("Ingredientes", {}).get("multi_select", [])]
            return ingredientes if ingredientes else None
    except Exception:
        return None

def _parse_bold(text: str) -> list:
    parts = []
    remaining = text
    while "**" in remaining:
        idx = remaining.find("**")
        if idx > 0:
            parts.append({"type": "text", "text": {"content": remaining[:idx]}})
        remaining = remaining[idx+2:]
        end = remaining.find("**")
        if end == -1:
            parts.append({"type": "text", "text": {"content": "**" + remaining}})
            remaining = ""
            break
        parts.append({"type": "text", "text": {"content": remaining[:end]}, "annotations": {"bold": True}})
        remaining = remaining[end+2:]
    if remaining:
        parts.append({"type": "text", "text": {"content": remaining}})
    return parts if parts else [{"type": "text", "text": {"content": text}}]

async def save_recipe_to_notion(recipe_name: str, source: str = "Matrics", ingredient_names: list[str] = None, recipe_text: str = None) -> tuple[bool, str]:
    try:
        try:
            props_response = claude_create(
                model="claude-sonnet-4-20250514", max_tokens=200,
                system="Responde SOLO JSON valido sin markdown.",
                messages=[{"role": "user", "content": f'''Receta: "{recipe_name}"
Texto: {(recipe_text or "")[:500]}
Responde SOLO este JSON:
{{"difficult": "Easy"|"Moderate"|"Hard"|null,
  "type": ["Postre"|"Cena"|"Almuerzo"|"Desayuno"|"Snack"|"Cosmetica"],
  "coccion": "Horno"|"Sarten"|"Pochar"|"Frizzer "|"Varias prep."|null,
  "healthy": "Healthy"|"Fatty"|"ni healthy ni fatty"|null}}'''}]
            )
            raw_meta = props_response.content[0].text.strip().strip("`").lstrip("json").strip()
            meta = json.loads(raw_meta)
        except Exception:
            meta = {}

        relation_ids = []
        if ingredient_names:
            items_list = ingredient_names if (isinstance(ingredient_names, list) and ingredient_names and isinstance(ingredient_names[0], dict)) else [{"name": n} for n in ingredient_names]
            for ing_item in items_list:
                ing_name = ing_item.get("name", "").strip()
                if not ing_name:
                    continue
                results = await search_shopping_item(ing_name)
                if results:
                    relation_ids.append({"id": results[0]["id"]})
                else:
                    emoji = ing_item.get("emoji", "🛒")
                    ing_props = {
                        "Name":  {"title": [{"text": {"content": ing_name}}]},
                        "Stock": {"checkbox": True},
                    }
                    if ing_item.get("category") in SHOPPING_CATEGORIES:
                        ing_props["Category"] = {"select": {"name": ing_item["category"]}}
                    if ing_item.get("store"):
                        ing_props["Store"] = {"multi_select": [{"name": ing_item["store"]}]}
                    if ing_item.get("frequency") in SHOPPING_FREQUENCY:
                        ing_props["Frequency"] = {"status": {"name": ing_item["frequency"]}}
                    async with httpx.AsyncClient() as http:
                        r = await http.post(
                            "https://api.notion.com/v1/pages",
                            headers=notion_headers(),
                            json={"parent": {"database_id": SHOPPING_DB_ID}, "icon": {"type": "emoji", "emoji": emoji}, "properties": ing_props}
                        )
                        if r.status_code == 200:
                            relation_ids.append({"id": r.json()["id"]})
                        else:
                            return False, f"Error creando ingrediente '{ing_name}': {r.text[:100]}"

        props = {
            "Name": {"title": [{"text": {"content": recipe_name.capitalize()}}]},
            "Source": {"select": {"name": source}},
        }
        if meta.get("difficult") in ["Easy", "Moderate", "Hard"]:
            props["Difficult "] = {"select": {"name": meta["difficult"]}}
        if meta.get("type") and isinstance(meta["type"], list):
            valid_types = [t for t in meta["type"] if t in ["Postre", "Cena", "Almuerzo", "Desayuno", "Snack", "Cosmetica"]]
            if valid_types:
                props["Type"] = {"multi_select": [{"name": t} for t in valid_types]}
        if meta.get("coccion") in ["Horno", "Sarten", "Pochar", "Frizzer ", "Varias prep."]:
            props["Coccion "] = {"select": {"name": meta["coccion"]}}
        if meta.get("healthy") in ["Healthy", "Fatty", "ni healthy ni fatty"]:
            props["😈 / 😇"] = {"select": {"name": meta["healthy"]}}
        if relation_ids:
            props["Ingredients"] = {"relation": relation_ids}

        async with httpx.AsyncClient() as http:
            r = await http.post(
                "https://api.notion.com/v1/pages",
                headers=notion_headers(),
                json={
                    "parent": {"database_id": RECIPES_DB_ID.replace("-", "")},
                    "icon": {"type": "emoji", "emoji": "🍽️"},
                    "properties": props
                }
            )
            if r.status_code not in [200, 201]:
                return False, f"Error creando receta en Notion (status {r.status_code}): {r.text[:200]}"

            page_id = r.json().get("id", "")

            if recipe_text and page_id:
                try:
                    fmt_resp = claude_create(
                        model="claude-sonnet-4-20250514", max_tokens=1500,
                        system="Formatea la siguiente receta para guardarla en Notion. Usa este formato:\n- Titulo de seccion como ## (Ingredientes, Procedimiento, Notas)\n- Listas con - para ingredientes y pasos numerados con 1. 2. 3.\n- **negrita** para cantidades importantes\n- Responde SOLO el texto formateado, sin comentarios adicionales.",
                        messages=[{"role": "user", "content": f"Receta: {recipe_name}\n\nTexto original:\n{recipe_text[:3000]}"}]
                    )
                    formatted = fmt_resp.content[0].text.strip()
                except Exception:
                    formatted = recipe_text

                blocks = []
                for line in formatted.split("\n"):
                    line_stripped = line.strip()
                    if not line_stripped:
                        continue
                    if line_stripped.startswith("## "):
                        blocks.append({"object": "block", "type": "heading_2",
                            "heading_2": {"rich_text": [{"type": "text", "text": {"content": line_stripped[3:]}}]}})
                    elif line_stripped.startswith("# "):
                        blocks.append({"object": "block", "type": "heading_1",
                            "heading_1": {"rich_text": [{"type": "text", "text": {"content": line_stripped[2:]}}]}})
                    elif line_stripped.startswith("- "):
                        content = line_stripped[2:]
                        rich = _parse_bold(content)
                        blocks.append({"object": "block", "type": "bulleted_list_item",
                            "bulleted_list_item": {"rich_text": rich}})
                    elif line_stripped[:2] in [f"{i}." for i in range(1, 30)] or (len(line_stripped) > 2 and line_stripped[0].isdigit() and line_stripped[1] == "."):
                        content = line_stripped.split(".", 1)[-1].strip()
                        rich = _parse_bold(content)
                        blocks.append({"object": "block", "type": "numbered_list_item",
                            "numbered_list_item": {"rich_text": rich}})
                    else:
                        rich = _parse_bold(line_stripped)
                        blocks.append({"object": "block", "type": "paragraph",
                            "paragraph": {"rich_text": rich}})

                if blocks:
                    await http.patch(
                        f"https://api.notion.com/v1/blocks/{page_id}/children",
                        headers=notion_headers(),
                        json={"children": blocks[:100]}
                    )

            return True, ""
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        return False, f"Excepcion: {str(e) or repr(e)} | {tb[-200:]}"

async def parse_shopping_intent(text: str) -> dict:
    safe_text = text.replace('"', "'").replace('\r', ' ').replace('\n', ' ')[:2000]
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=800,
        system="Analiza mensajes sobre lista de compras. Responde SOLO JSON valido sin markdown.",
        messages=[{"role": "user", "content": f"""Mensaje: {safe_text}

Responde:
{{"action": "out_of_stock"|"in_stock"|"add"|"list",
  "items": ["item1", "item2"],
  "recipe_name": "nombre de la receta o null",
  "is_recipe_request": true/false,
  "recipe_ingredients": ["ingrediente1", "ingrediente2", ...]}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

GENERIC_WORDS = {"salsa", "crema", "pasta", "sopa", "caldo", "jugo",
                  "queso", "pan", "leche", "aceite", "harina", "arroz"}

async def search_shopping_item(name: str) -> list:
    name = name.strip()
    candidates = [name]
    if name.endswith("s") and len(name) > 3:
        candidates.append(name[:-1])
    first_word = name.split()[0].lower()
    if len(name.split()) > 1 and len(first_word) > 5 and first_word not in GENERIC_WORDS:
        candidates.append(name.split()[0])
    async with httpx.AsyncClient() as http:
        for candidate in candidates:
            r = await http.post(
                f"https://api.notion.com/v1/databases/{SHOPPING_DB_ID}/query",
                headers=notion_headers(),
                json={"filter": {"property": "Name", "title": {"contains": candidate[:25]}}}
            )
            results = r.json().get("results", []) if r.status_code == 200 else []
            if results:
                return results
    return []

async def add_shopping_item(item: dict) -> tuple[bool, str]:
    name  = item.get("name", "").strip()
    emoji = item.get("emoji", "🛒")
    freq  = item.get("frequency", "One time")
    store = item.get("store", "")
    props = {
        "Name":  {"title": [{"text": {"content": name}}]},
        "Stock": {"checkbox": False},
    }
    if item.get("category") in SHOPPING_CATEGORIES:
        props["Category"] = {"select": {"name": item["category"]}}
    if store:
        props["Store"] = {"multi_select": [{"name": store}]}
    if freq in SHOPPING_FREQUENCY:
        props["Frequency"] = {"status": {"name": freq}}
    async with httpx.AsyncClient() as http:
        r = await http.post(
            "https://api.notion.com/v1/pages",
            headers=notion_headers(),
            json={"parent": {"database_id": SHOPPING_DB_ID}, "icon": {"type": "emoji", "emoji": emoji}, "properties": props}
        )
        return r.status_code == 200, r.text[:150] if r.status_code != 200 else ""

async def handle_shopping(text: str, phone: str = None) -> str:
    try:
        intent = await parse_shopping_intent(text)
    except Exception as e:
        return f"No pude interpretar el mensaje: {str(e)[:100]}"

    action      = intent.get("action")
    items       = intent.get("items", [])
    is_recipe   = intent.get("is_recipe_request", False)
    recipe_name = intent.get("recipe_name")
    recipe_ingredients_raw = intent.get("recipe_ingredients", [])
    recipe_note = ""

    if action == "add" and is_recipe and recipe_name:
        notion_ingredients = await search_recipe_in_notion(recipe_name)
        if notion_ingredients:
            if phone:
                enriched = await enrich_items_with_claude(notion_ingredients)
                ing_list = "\n".join(f"- {i.get('emoji','🛒')} {i.get('name','')}" for i in enriched)
                pending_state[phone] = {
                    "type": "recipe_ingredients",
                    "recipe_name": recipe_name,
                    "ingredients": enriched
                }
                await send_interactive_buttons(
                    phone,
                    f"Receta encontrada en tus recetas.\n\nIngredientes:\n{ing_list}\n\nLos agregas a la lista de compras?",
                    [
                        {"id": "recipe_add_yes", "title": "Si, agregar"},
                        {"id": "recipe_add_no",  "title": "No por ahora"},
                    ]
                )
                return f"*{recipe_name.capitalize()}* encontrada en tus recetas"
            else:
                items = notion_ingredients
                recipe_note = f"*{recipe_name.capitalize()}* (de tus recetas)\n"
        else:
            try:
                if recipe_ingredients_raw:
                    enriched_direct = await enrich_items_with_claude(recipe_ingredients_raw)
                    ok = True
                else:
                    enriched_direct, ok = await get_ingredients_and_enrich(recipe_name, recipe_text=text)
            except Exception:
                enriched_direct, ok = [], False
            if ok and enriched_direct:
                if phone:
                    pending_state[phone] = {
                        "type": "recipe_review",
                        "recipe_name": recipe_name,
                        "recipe_text": text,
                        "ingredients": enriched_direct,
                    }
                    ing_list_display = "\n".join(
                        f"- {i.get('emoji','🛒')} {i.get('display') or i.get('name','')}"
                        for i in enriched_direct
                    )
                    await send_message(
                        phone,
                        f"*{recipe_name.capitalize()}*\n\n*Ingredientes:*\n{ing_list_display}"
                    )
                    if text and len(text) > 100:
                        try:
                            proc_resp = claude_create(
                                model="claude-sonnet-4-20250514", max_tokens=600,
                                system="Extrae SOLO la seccion de preparacion/procedimiento de la receta. Sin titulo, sin lista de ingredientes. Solo los pasos de preparacion en texto limpio.",
                                messages=[{"role": "user", "content": text[:2000]}]
                            )
                            proc_text = proc_resp.content[0].text.strip()
                        except Exception:
                            proc_text = text[:600]
                        await send_message(phone, f"*Preparacion:*\n{proc_text}")
                    await send_interactive_buttons(
                        phone,
                        "Esta todo bien o queres corregir algo?",
                        [
                            {"id": "recipe_ok",      "title": "Esta bien"},
                            {"id": "recipe_correct", "title": "Quiero corregir"},
                        ]
                    )
                    return None
                else:
                    return f"*{recipe_name.capitalize()}* -- {len(enriched_direct)} ingredientes detectados."
            else:
                items = []
                recipe_note = f"No pude inferir los ingredientes para esa receta\n"

    if action == "list":
        async with httpx.AsyncClient() as http:
            r = await http.post(
                f"https://api.notion.com/v1/databases/{SHOPPING_DB_ID}/query",
                headers=notion_headers(),
                json={"filter": {"property": "Stock", "checkbox": {"equals": False}},
                      "sorts": [{"property": "Category", "direction": "ascending"}]}
            )
            if r.status_code != 200:
                return f"No pude leer la lista: {r.text[:100]}"
            results = r.json().get("results", [])
            if not results:
                return "No te falta nada! La lista esta vacia."
            lines = ["*Tu lista de compras:*\n"]
            for item in results:
                name = item["properties"]["Name"]["title"][0]["plain_text"] if item["properties"]["Name"]["title"] else "?"
                cat  = (item["properties"].get("Category", {}).get("select") or {}).get("name", "")
                lines.append(f"- {name}{f' _{cat}_' if cat else ''}")
            return "\n".join(lines)

    if not items:
        return "No entendi que producto queres actualizar."

    if action == "add":
        try:
            enriched = await enrich_items_with_claude(items)
        except Exception:
            enriched = [{"name": i.capitalize(), "emoji": "🛒", "category": "", "store": "", "frequency": "One time"} for i in items]
        results_text = []
        for item in enriched:
            item_name = item.get("name", "")
            existing = await search_shopping_item(item_name)
            if existing:
                async with httpx.AsyncClient() as http:
                    await http.patch(f"https://api.notion.com/v1/pages/{existing[0]['id']}",
                                     headers=notion_headers(),
                                     json={"properties": {"Stock": {"checkbox": False}}})
                results_text.append(f"{item.get('emoji','🛒')} _{item_name}_ ya estaba, aparece como faltante")
            else:
                ok, err = await add_shopping_item(item)
                results_text.append(f"{item.get('emoji','🛒')} _{item_name}_ agregado" if ok else f"Error agregando _{item_name}_: {err}")
        return recipe_note + "\n".join(results_text) + "\n\nLista actualizada en Notion"

    results_text = []
    for item_name in items:
        display  = item_name.capitalize()
        in_stock = action == "in_stock"
        existing = await search_shopping_item(item_name)
        if existing:
            async with httpx.AsyncClient() as http:
                await http.patch(f"https://api.notion.com/v1/pages/{existing[0]['id']}",
                                 headers=notion_headers(),
                                 json={"properties": {"Stock": {"checkbox": in_stock}}})
            results_text.append(f"_{display}_ marcado como en stock" if in_stock else f"_{display}_ agregado a la lista")
        else:
            if not in_stock:
                try:
                    enriched = await enrich_items_with_claude([item_name])
                    item_data = enriched[0] if enriched else {"name": display, "emoji": "🛒", "category": "", "store": "", "frequency": "One time"}
                except Exception:
                    item_data = {"name": display, "emoji": "🛒", "category": "", "store": "", "frequency": "One time"}
                ok, _ = await add_shopping_item(item_data)
                results_text.append(f"{item_data.get('emoji','🛒')} _{display}_ agregado como faltante" if ok else f"Error agregando _{display}_")
            else:
                results_text.append(f"_{display}_ no esta en la lista")

    return "\n".join(results_text) + "\n\nLista actualizada en Notion"
