import os
import json
import base64
import time
import httpx
from datetime import date, datetime, timedelta, timezone
from calendar import monthrange
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
    "_config_page_id": None,
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
        async with httpx.AsyncClient(timeout=5) as http:
            r = await http.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": USER_LAT, "longitude": USER_LON,
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
        f"🌡️ {w['temp']}°C (sensación {w['sensacion']}°C)",
        f"{w['emoji']} {w['desc']}",
    ]
    if w["lluvia"] > 0:
        lines.append(f"🌧️ Lluvia: {w['lluvia']}mm")
    lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
    return lines

def format_weather_chat(w: dict, include_tomorrow: bool = False) -> str:
    lines = [
        "*Hoy:*",
        f"🌡️ {w['temp']}°C (sensación {w['sensacion']}°C)",
        f"{w['emoji']} {w['desc']}",
    ]
    if w["lluvia"] > 0:
        lines.append(f"🌧️ Lluvia: {w['lluvia']}mm")
    lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
    if include_tomorrow:
        lines += [
            "", "*Mañana:*",
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

# ── MÓDULO GASTOS ──────────────────────────────────────────────────────────────

async def handle_gasto_agent(phone: str, text: str, image_b64=None, image_type=None, exchange_rate=1000.0) -> str:
    now = now_argentina()
    tools = [{
        "name": "registrar_gasto",
        "description": "Registra un gasto o ingreso en Notion. Usá solo cuando tenés descripción Y monto claros.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name":      {"type": "string", "description": "Descripción corta del gasto"},
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

    system = f"""Sos Matrics, asistente personal por WhatsApp. Hablás en español rioplatense, natural y conciso.
Hoy: {now.strftime("%Y-%m-%d")} {now.strftime("%H:%M")}. Tasa dólar blue: ${exchange_rate:,.0f} ARS/USD.

Tu tarea: registrar gastos e ingresos del usuario.
- Si el mensaje tiene descripción Y monto → usá la tool registrar_gasto directamente.
- Si falta el monto u otra info esencial → preguntá de forma natural y breve, sin registrar nada.
- Si hay ambigüedad (ej: "compré algo") → preguntá qué fue y cuánto.

Categorías disponibles: Supermercado, Sueldo, Servicios, Transporte, Vianda, Salud, Salud Mental, Salida, Birra, Ocio, Compras, Depto, Plantas, Viajes, Venta.
Servicios = pagos recurrentes (alquiler, luz, gas, internet, streaming, gimnasio). Depto = compras físicas para el depto (muebles, materiales, herramientas).
Metodo Suscription: gastos recurrentes mensuales. Payment: todo lo demás.
Si in_out es INGRESO → categoría solo puede ser Sueldo o Venta.
Clientes posibles: LBL, OPERA, ALPATACO, Juan Martin, Depto, Work, Santi Vales, Jorge, Barbara, Vanguardia, Alejo, Dinamo, Paula Diaz, Labti, PlanA, JGA, ATE.
Emoji: elegí el más específico según el contexto real."""

    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=1000,
        system=system,
        messages=[{"role": "user", "content": content}],
        tools=tools
    )

    # Si Claude pide info adicional (no usa tool)
    if response.stop_reason == "end_turn":
        return next((b.text for b in response.content if hasattr(b, "text")), "❌ Error procesando").strip()

    # Claude usó la tool
    tool_block = next((b for b in response.content if b.type == "tool_use"), None)
    if not tool_block:
        return next((b.text for b in response.content if hasattr(b, "text")), "❌ Error procesando").strip()

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
            f"Monto: ${data['value_ars']:,.0f} ARS (≈ USD {usd:.2f}), "
            f"Categoría: {', '.join(data['categoria'])}, "
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
    reply = next((b.text for b in final_response.content if hasattr(b, "text")), "").strip()

    # Follow-up nafta (mantiene comportamiento actual)
    if success and page_id:
        name_lower = data.get("name", "").lower()
        is_fuel = data.get("emoji") == "⛽" or any(k in name_lower for k in FUEL_KEYWORDS)
        if is_fuel and not data.get("litros"):
            pending_state[phone] = {"type": "litros_followup", "page_id": page_id, "name": data["name"]}
            reply += "\n\n⛽ ¿Cuántos litros cargaste?"

    return reply


async def create_notion_entry(data: dict, exchange_rate: float) -> tuple[bool, str]:
    """Crea entrada en Finances. Retorna (True, page_id) o (False, error_msg)."""
    if not data.get("value_ars") or not data.get("in_out"):
        return False, "No se pudo interpretar"
    props = {
        "Name":          {"title": [{"text": {"content": data["name"]}}]},
        "In - Out":      {"select": {"name": data["in_out"]}},
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
                return saved_cats, f"📚 Categoría: _{', '.join(saved_cats)}_ (según tu corrección anterior)"
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
                        return notion_cats, f"📚 Categoría: _{', '.join(notion_cats)}_ (como en cargas anteriores)"
    except Exception:
        pass
    return predicted_cats, None

async def corregir_gasto(text: str, phone: str = None) -> tuple[bool, str]:
    now = now_argentina()
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=200,
        system="Extraé qué gasto corregir y qué cambiar. Si el mensaje no menciona un nombre concreto, usá null en search_term. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Hoy: {now.strftime("%Y-%m-%d")}
Mensaje: {text}
Respondé:
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
        return False, "No entendí qué gasto querés corregir"

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
                return False, f"No encontré ningún gasto llamado _{search_term}_"
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
            return False, "No entendí qué campo querés cambiar"

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
            changes.append(f"${old_value:,.0f} → *${float(intent['new_value_ars']):,.0f} ARS*")
        if intent.get("new_categoria"):
            changes.append(f"Categoría → _{', '.join(intent['new_categoria'])}_")
        if intent.get("new_name"):
            changes.append(f"Nombre → _{intent['new_name']}_")
        return True, f"✏️ *{old_name}* corregido\n" + "\n".join(changes) + "\n\n✅ Actualizado en Notion"

async def eliminar_gasto(text: str) -> tuple[bool, str]:
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=100,
        system="Extraé el nombre de la entrada de Notion a eliminar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f'Mensaje: {text}\nRespondé: {{"search_term": "nombre de la entrada a eliminar"}}'}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    search_term = json.loads(raw).get("search_term", "")
    if not search_term:
        return False, "No entendí qué entrada querés eliminar"

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
            return False, f"No encontré ninguna entrada llamada _{search_term}_"

        page = r.json()["results"][0]
        page_id = page["id"]
        old_name = page["properties"]["Name"]["title"][0]["plain_text"] if page["properties"]["Name"]["title"] else "?"

        del_r = await http.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={"archived": True}
        )
        if del_r.status_code == 200:
            return True, f"🗑️ *{old_name}* eliminado de Notion"

async def eliminar_shopping(text: str) -> tuple[bool, str]:
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=100,
        system="Extraé el nombre del ítem de la lista de compras a eliminar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"Mensaje: {text}\nRespondé: {{\"search_term\": \"nombre del item\"}}"}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    search_term = json.loads(raw).get("search_term", "")
    if not search_term:
        return False, "No entendí qué ítem querés eliminar"
    existing = await search_shopping_item(search_term)
    if not existing:
        return False, f"No encontré ningún ítem llamado _{search_term}_ en la lista"
    page_id = existing[0]["id"]
    item_name = existing[0]["properties"]["Name"]["title"][0]["plain_text"] if existing[0]["properties"]["Name"]["title"] else search_term
    async with httpx.AsyncClient() as http:
        r = await http.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={"archived": True}
        )
        if r.status_code == 200:
            return True, f"🗑️ *{item_name}* eliminado de la lista de compras"
        return False, f"Error eliminando el ítem: {r.text[:100]}"

# ── MÓDULO PLANTAS ─────────────────────────────────────────────────────────────
PLANTA_SYSTEM = """Extraé info de una planta y generá recomendaciones de cuidado.
Responde ÚNICAMENTE con JSON válido, sin markdown.
Valores para "luz": Sombra, Indirecta, Directa parcial, Pleno sol
Valores para "riego": Cada 2-3 días, Semanal, Quincenal, Mensual
Valores para "ubicacion": Interior, Exterior, Balcón, Terraza
Valores para "estado": Excelente, Bien, Regular, Necesita atención"""

async def parse_planta(text: str, exchange_rate: float) -> dict:
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=600,
        system=PLANTA_SYSTEM,
        messages=[{"role": "user", "content": f"""Hoy: {now_argentina().strftime("%Y-%m-%d")}. Dolar: ${exchange_rate:,.0f}
Mensaje: {text}
Respondé:
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
        f"☀️ Luz: {data.get('luz', '-')}",
        f"💧 Riego: {data.get('riego', '-')}",
        f"🏠 Ubicación: {data.get('ubicacion', '-')}",
    ]
    if data.get("notas"):
        lines.append(f"\n📝 {data['notas']}")
    lines.append("\n✅ Guardada en Notion")
    return "\n".join(lines)

# ── MÓDULO EVENTOS ─────────────────────────────────────────────────────────────
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
        summary = f"{summary} — {caption.strip().capitalize()}"
    lines = [f"{emoji} *{summary}*", f"Fecha: {fecha}{hora}"]
    if data.get("location"):
        lines.append(f"📍 {data['location']}")
    if data.get("description"):
        lines.append(f"Nota: {data['description']}")
    lines.append("\n✅ Agregado a Google Calendar" if guardado else "\n⚠️ Anota esto manualmente — Calendar no configurado")
    return "\n".join(lines)

async def parse_evento(text: str, image_b64: str = None, image_type: str = None) -> dict:
    now = now_argentina()
    user_content = []
    if image_b64:
        user_content.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    user_content.append({"type": "text", "text": f"""Hoy es {now.strftime("%Y-%m-%d")}, hora actual: {now.strftime("%H:%M")}
Mensaje: {text or "(ver imagen adjunta)"}
Extraé la info del evento de la imagen si la hay, o del texto.
Respondé:
{{"summary":"titulo","date":"YYYY-MM-DD","time":"HH:MM o null","duration_minutes":60,"location":"lugar o null","description":"desc o null","emoji":"emoji"}}"""})
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extraé info de un evento. Responde SOLO JSON válido sin markdown. Usa zona horaria Argentina (UTC-3).",
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
    medical_kw = {"dr", "dra", "doctor", "médico", "medico", "turno", "cita", "hospital",
                  "clínica", "clinica", "odontólogo", "odontologo", "psicólogo", "psicologo",
                  "dentista", "cardiólogo", "cardiologo", "ortopedista", "kinésiologo"}
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

async def search_and_edit_evento(text: str, phone: str = None) -> tuple[bool, str]:
    access_token = await get_gcal_access_token()
    if not access_token:
        return False, "Calendar no configurado"
    now = now_argentina()
    last_summary = last_event_touched.get(phone, {}).get("summary", "") if phone else ""
    last_event_ctx = f"\nÚltimo evento creado/editado: \"{last_summary}\"." if last_summary else ""
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system=f"""Extraé qué evento editar y qué cambiar. Hoy es {now.strftime("%Y-%m-%d")}.{last_event_ctx}
Reglas:
- Si el mensaje menciona una actividad, nombre o keyword relacionado con un evento → poné esa keyword en search_term.
- Si el mensaje es claramente una corrección → si hay último evento usá search_term=null, si no intentá inferir.
- Si el mensaje menciona una fecha "vieja" junto a una "nueva", usá la vieja para encontrar y la nueva como new_date.
- Extraé TODOS los cambios mencionados.
- Responde SOLO JSON.""",
        messages=[{"role": "user", "content": f"""Mensaje: {text}
Respondé:
{{"search_term":"keyword o nombre del evento para buscar en Calendar, o null si hay último evento","location":"nueva ubicacion o null","new_title":"nuevo titulo o null","new_time":"HH:MM o null","new_date":"YYYY-MM-DD o null","description":"nueva descripcion o null"}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    edit_data = json.loads(raw)

    search_term = edit_data.get("search_term")
    target_event = None

    async with httpx.AsyncClient() as http:
        headers = {"Authorization": f"Bearer {access_token}"}
        time_min = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00-03:00")
        time_max = (now + timedelta(days=60)).strftime("%Y-%m-%dT23:59:59-03:00")

        if not search_term:
            if phone and phone in last_event_touched:
                entry = last_event_touched[phone]
                event_id = entry["event_id"]
                r = await http.get(
                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                    headers=headers
                )
                if r.status_code == 200:
                    target_event = r.json()
                else:
                    return False, f"No encontré el evento _{entry['summary']}_ en tu calendario"
            else:
                return False, "¿De qué evento hablás? No encontré contexto reciente."

        if not target_event and search_term:
            r = await http.get(
                "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                headers=headers,
                params={"q": search_term, "timeMin": time_min, "timeMax": time_max,
                        "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"}
            )
            if r.status_code == 200:
                candidates = r.json().get("items", [])
                target_event = fuzzy_match_event(search_term, candidates)

            if not target_event and search_term and len(search_term.split()) > 1:
                first_word = search_term.split()[0]
                r2 = await http.get(
                    "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                    headers=headers,
                    params={"q": first_word, "timeMin": time_min, "timeMax": time_max,
                            "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"}
                )
                if r2.status_code == 200:
                    candidates2 = r2.json().get("items", [])
                    target_event = fuzzy_match_event(search_term, candidates2)

            if not target_event and phone and phone in last_event_touched:
                entry = last_event_touched[phone]
                r3 = await http.get(
                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{entry['event_id']}",
                    headers=headers
                )
                if r3.status_code == 200:
                    candidate = r3.json()
                    event_name = candidate.get("summary", "")
                    search_words = set(search_term.lower().split())
                    event_words = set(event_name.lower().split())
                    if search_words & event_words:
                        target_event = candidate

            if not target_event:
                r_all = await http.get(
                    "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                    headers=headers,
                    params={"timeMin": time_min, "timeMax": time_max,
                            "singleEvents": "true", "orderBy": "startTime", "maxResults": "20"}
                )
                if r_all.status_code == 200:
                    all_events = [e for e in r_all.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
                    if all_events and search_term:
                        target_event = fuzzy_match_event(search_term, all_events)
                if not target_event:
                    return False, f"No encontré ningún evento relacionado con _{search_term}_."

        event = dict(target_event)
        event_id = event["id"]
        event_name = event.get("summary", "Evento")

        if edit_data.get("new_title"):    event["summary"] = edit_data["new_title"]
        if edit_data.get("location"):     event["location"] = edit_data["location"]
        if edit_data.get("description"):  event["description"] = edit_data["description"]
        if edit_data.get("new_date") or edit_data.get("new_time"):
            if "dateTime" in event.get("start", {}):
                old_dt   = event["start"]["dateTime"][:16]
                new_date = edit_data.get("new_date") or old_dt[:10]
                new_time = edit_data.get("new_time") or old_dt[11:16]
                event["start"] = {"dateTime": f"{new_date}T{new_time}:00", "timeZone": "America/Argentina/Buenos_Aires"}
                if "dateTime" in event.get("end", {}):
                    dur = datetime.strptime(event["end"]["dateTime"][:16], "%Y-%m-%dT%H:%M") - datetime.strptime(old_dt, "%Y-%m-%dT%H:%M")
                    new_end = datetime.strptime(f"{new_date}T{new_time}", "%Y-%m-%dT%H:%M") + dur
                    event["end"] = {"dateTime": new_end.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}

        update_r = await http.put(
            f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
            headers={**headers, "Content-Type": "application/json"},
            json=event
        )
        if update_r.status_code in [200, 201]:
            if phone:
                last_event_touched[phone] = {"event_id": event_id, "summary": event.get("summary", event_name)}
            loc_str = f"\n📍 {edit_data['location']}" if edit_data.get("location") else ""
            time_str = f"\n🕐 {edit_data['new_time']}" if edit_data.get("new_time") else ""
            return True, f"✅ *{event_name}* actualizado{loc_str}{time_str}"
        return False, "Error actualizando el evento"

async def delete_evento(text: str) -> tuple[bool, str]:
    access_token = await get_gcal_access_token()
    if not access_token:
        return False, "Calendar no configurado"
    now = now_argentina()
    tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=200,
        system="Extraé info sobre qué evento(s) eliminar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Hoy: {now.strftime("%Y-%m-%d")}, mañana: {tomorrow}
Mensaje: {text}
Respondé:
{{"search_terms": ["nombre evento 1", "nombre evento 2"],
  "target_date": "YYYY-MM-DD si se menciona fecha, sino null",
  "delete_all": true si quiere borrar todos los de esa fecha}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    intent = json.loads(raw)
    search_terms = intent.get("search_terms") or []
    if not search_terms and intent.get("search_term"):
        search_terms = [intent["search_term"]]

    async with httpx.AsyncClient() as http:
        headers = {"Authorization": f"Bearer {access_token}"}
        if intent.get("target_date"):
            date_str = intent["target_date"]
            time_min = f"{date_str}T00:00:00-03:00"
            time_max = f"{date_str}T23:59:59-03:00"
        else:
            time_min = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00-03:00")
            time_max = (now + timedelta(days=60)).strftime("%Y-%m-%dT23:59:59-03:00")

        deleted = []
        if not search_terms and not intent.get("delete_all"):
            return False, "No entendí qué evento querés eliminar"
        if intent.get("delete_all") and not search_terms:
            search_terms = [None]

        for term in search_terms:
            params = {"timeMin": time_min, "timeMax": time_max,
                      "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"}
            if term:
                params["q"] = term
            r = await http.get(
                "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                headers=headers, params=params
            )
            if r.status_code != 200:
                continue
            events = [e for e in r.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
            if not events:
                continue
            to_delete = events if intent.get("delete_all") else [events[0]]
            for event in to_delete:
                if event.get("summary") in deleted:
                    continue
                del_r = await http.delete(
                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event['id']}",
                    headers=headers
                )
                if del_r.status_code == 204:
                    deleted.append(event.get("summary", "Evento"))

        if not deleted:
            return False, "No encontré ningún evento para eliminar"
        if len(deleted) == 1:
            return True, f"🗑️ *{deleted[0]}* eliminado del calendario"
        lista = "\n".join(f"• {e}" for e in deleted)
        return True, f"🗑️ *{len(deleted)} eventos eliminados:*\n{lista}"

async def find_similar_calendar_events(data: dict) -> list:
    access_token = await get_gcal_access_token()
    if not access_token:
        return []
    summary = data.get("summary", "")
    if not summary or len(summary) < 4:
        return []
    stopwords = {"con", "en", "de", "la", "el", "los", "las", "del", "al", "por", "para",
                 "turno", "cita", "reunión", "reunion", "evento", "con", "una", "uno"}
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

# ── HISTORIAL DE CONVERSACIÓN ──────────────────────────────────────────────────
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
            system=f"""Sos Matrics. Evaluá si el mensaje del usuario es suficientemente claro para ejecutar la acción indicada.
Contexto: {context}
Si el mensaje es claro → respondé solo: CLEAR
Si hay ambigüedad → respondé solo la pregunta de aclaración más concisa y natural posible (máx 1 pregunta, tono rioplatense).""",
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
        history_ctx = "\nContexto reciente de la conversación:\n" + "\n".join(
            f"{'Usuario' if m['role']=='user' else 'Matrics'}: {str(m['content'])[:120]}"
            for m in recent
        ) + "\n\nTeniendo en cuenta ese contexto, clasificá el siguiente mensaje:"
    content.append({"type": "text", "text": history_ctx + "\n" + prompt_text if history_ctx else prompt_text})
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=10,
        system="""Responde SOLO una palabra: GASTO, CORREGIR_GASTO, PLANTA, EVENTO, EDITAR_EVENTO, ELIMINAR_EVENTO, RECORDATORIO, SHOPPING, REUNION, CONFIGURAR o CHAT.

GASTO: registrar un pago, compra o ingreso concreto con monto. También cuando el mensaje menciona una compra o gasto SIN monto (ej: "compré en la verdulería", "fui al super") — Matrics pedirá el monto.
CORREGIR_GASTO: corregir un gasto ya registrado.
ELIMINAR_GASTO: eliminar o borrar una entrada de Notion.
ELIMINAR_SHOPPING: eliminar o borrar un ítem de la lista de compras.
PLANTA: adquirir o mencionar una planta.
EDITAR_EVENTO: modificar un evento existente en el calendario.
ELIMINAR_EVENTO: eliminar o borrar un evento del calendario.
RECORDATORIO: "recordame en X tiempo", "avisame en X". NUNCA para cambios de horario del resumen.
EVENTO: crear un evento nuevo — turno, reunión, cumple, cita, viaje.
SHOPPING: gestionar lista de compras o recetas.
REUNION: notas o fotos de una reunión/llamada.
CONFIGURAR: cambiar configuración de Matrics. Solo cuando el usuario quiere CAMBIAR algo. Ej: "el resumen mandámelo a las 7", "cambiá el horario", "agregá una frase motivadora al resumen". Nunca cuando pregunta o se queja.
CHAT: cualquier pregunta, consulta o conversación. Si tiene "?" o pide información → CHAT. Incluye preguntas sobre por qué no llegó el resumen, quejas, consultas sobre el estado del bot, etc.

REGLA: si el mensaje PREGUNTA algo → siempre CHAT, nunca GASTO.

IMÁGENES SIN TEXTO:
- Factura, ticket, recibo → GASTO
- Invitación, flyer, screenshot de turno/evento → EVENTO
- Foto de receta, lista de ingredientes → SHOPPING
- Pizarrón, apuntes de reunión → REUNION
- Documento de texto genérico → CHAT""",
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
        summary = f"📊 *Finanzas {month}*\n\n💚 Ingresos: ${ingresos:,.0f}\n🔴 Egresos: ${egresos:,.0f}\n{'✅' if balance >= 0 else '⚠️'} Balance: ${balance:,.0f}\n"
        if top_cats:
            summary += "\n📂 *Top categorías:*\n" + "".join(f"• {c}: ${v:,.0f}\n" for c, v in top_cats)
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
            return "No hay eventos en ese período."
        lines = []
        for e in events:
            start = e.get("start", {})
            loc_str = f" — 📍{e.get('location', '')}" if e.get("location") else ""
            if "dateTime" in start:
                dt = datetime.strptime(start["dateTime"][:16], "%Y-%m-%dT%H:%M")
                lines.append(f"• {dt.strftime('%d/%m')} {dt.strftime('%H:%M')} — {e.get('summary', 'Evento')}{loc_str}")
            else:
                lines.append(f"• {start.get('date', '')} — {e.get('summary', 'Evento')} (todo el día){loc_str}")
        return "\n".join(lines)

async def infer_service_providers() -> dict:
    """Escanea mails del último mes e infiere proveedores de servicios."""
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
                system="""Analizá estos mails de facturas/servicios e identificá qué empresa provee qué servicio.
Respondé SOLO JSON con este formato:
{"electricidad": "Nombre empresa", "gas": "Nombre empresa", "internet": "Nombre empresa", "agua": "Nombre empresa", "telefono": "Nombre empresa"}
Solo incluí los servicios que puedas identificar con certeza. Si no hay info suficiente para un servicio, no lo incluyas.""",
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
            for msg in messages[:10]:
                msg_r = await http.get(
                    f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg['id']}",
                    headers=headers,
                    params={"format": "full"}
                )
                if msg_r.status_code != 200:
                    continue
                msg_data = msg_r.json()
                hdrs = {h["name"]: h["value"] for h in msg_data.get("payload", {}).get("headers", [])}
                snippet = msg_data.get("snippet", "")

                # Buscar PDFs adjuntos
                pdf_texts = []
                parts = msg_data.get("payload", {}).get("parts", [])
                for part in parts:
                    if part.get("mimeType") == "application/pdf":
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
                            except Exception:
                                pass

                mail_data.append({
                    "from": hdrs.get("From", ""),
                    "subject": hdrs.get("Subject", ""),
                    "snippet": snippet[:300],
                    "pdf_attachments": pdf_texts
                })

            if not mail_data:
                return None

            # Armar contenido para Claude — texto + PDFs
            content = []
            mail_summary_text = ""
            for m in mail_data:
                mail_summary_text += f"\nDe: {m['from']}\nAsunto: {m['subject']}\nPreview: {m['snippet']}\n"

            content.append({"type": "text", "text": f"""Analizá estos mails importantes del último mes e identificá los verdaderamente relevantes.
Importante: facturas/vencimientos con montos, mails de personas conocidas que requieren respuesta, algo urgente.
Ignorá: newsletters, notificaciones automáticas, publicidad, confirmaciones rutinarias, notificaciones de GitHub/Railway/Notion.
Si hay PDFs adjuntos, leelos y extraé la info relevante (monto, vencimiento, servicio).
Resumí en español rioplatense, máx 5 líneas. Si no hay nada importante respondé solo: NONE

Mails:
{mail_summary_text}"""})

            # Agregar PDFs como imágenes/documentos
            for m in mail_data:
                for pdf_b64 in m["pdf_attachments"][:2]:  # máx 2 PDFs por mail
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

async def handle_chat(phone: str, text: str) -> str:
    history = get_history(phone)
    add_to_history(phone, "user", text)
    now = now_argentina()

    tools = [
        {
            "name": "consultar_calendario",
            "description": "Consulta eventos del calendario de Google Calendar. Usá cuando el usuario pregunta sobre su agenda, eventos, turnos, qué tiene programado, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "dias_adelante": {"type": "integer", "description": "Cuántos días hacia adelante consultar. Default 2, usar 7 para 'esta semana', 30 para 'este mes'."},
                    "dias_atras": {"type": "integer", "description": "Cuántos días hacia atrás consultar. Default 0."}
                },
                "required": []
            }
        },
        {
            "name": "consultar_finanzas",
            "description": "Consulta gastos e ingresos registrados en Notion. Usá cuando el usuario pregunta sobre plata, gastos, balance, cuánto gastó, finanzas del mes, etc.",
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
            "description": "Consulta el clima actual y pronóstico. Usá cuando el usuario pregunta sobre el tiempo, temperatura, lluvia, si necesita abrigo, paraguas, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "incluir_manana": {"type": "boolean", "description": "True si pregunta por mañana o el pronóstico."}
                },
                "required": []
            }
        },
        {
            "name": "consultar_gmail",
            "description": "Consulta los mails importantes no leídos de los últimos 2 días. Usá cuando el usuario pregunta sobre emails, correos, facturas recibidas, si le escribieron, notificaciones importantes, etc.",
            "input_schema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        },
        {
            "type": "web_search_20250305",
            "name": "web_search"
        }
    ]

    system = f"""Sos Matrics, asistente personal en WhatsApp. Respondés conciso y natural en español rioplatense.
Hoy: {now.strftime("%d/%m/%Y")} {now.strftime("%H:%M")}.
Tenés herramientas disponibles — usalas cuando el usuario necesite información real (agenda, finanzas, clima, mails, info actual de internet).
Podés usar varias herramientas en el mismo turno si el mensaje lo requiere.
Si no necesitás herramientas, respondé directamente.
IMPORTANTE: No inventes datos que no tenés."""

    messages = history + [{"role": "user", "content": text}]

    # Primera llamada — Claude decide qué tools usar
    try:
        response = claude_create(
            model="claude-sonnet-4-20250514", max_tokens=1000,
            system=system,
            messages=messages,
            tools=tools
        )
    except Exception:
        return "❌ Error procesando tu mensaje. Intentá de nuevo."

    # Si no usó tools, devolver respuesta directa
    if response.stop_reason == "end_turn":
        reply = next((b.text for b in response.content if hasattr(b, "text")), "").strip()
        add_to_history(phone, "assistant", reply)
        return reply

    # Ejecutar todas las tools que pidió Claude
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
                result = await query_calendar(days_ahead=dias_adelante, days_back=dias_atras) or "No hay eventos en ese período."

            elif tool_name == "consultar_finanzas":
                mes = tool_input.get("mes") or now.strftime("%Y-%m")
                result = await query_finances(mes) or f"No hay registros para {mes}."

            elif tool_name == "consultar_clima":
                w = await get_weather()
                if w:
                    incluir_manana = tool_input.get("incluir_manana", False)
                    result = format_weather_chat(w, include_tomorrow=incluir_manana)
                else:
                    result = "No pude obtener el clima en este momento."

            elif tool_name == "consultar_gmail":
                # Si no hay proveedores guardados, inferirlos primero
                if not user_prefs.get("service_providers"):
                    inferred = await infer_service_providers()
                    if inferred:
                        resumen = "\n".join(f"• {k.capitalize()}: *{v}*" for k, v in inferred.items())
                        pending_state[phone] = {
                            "type": "confirm_service_providers",
                            "proposed": inferred
                        }
                        await send_message(phone, f"Encontré tus proveedores de servicios en tus mails:\n\n{resumen}\n\n¿Es correcto?")
                        await send_interactive_buttons(
                            phone,
                            "¿Confirmo estos proveedores?",
                            [
                                {"id": "providers_ok", "title": "Sí, correcto"},
                                {"id": "providers_no", "title": "Quiero corregir"},
                            ]
                        )
                        result = "Inferí los proveedores y le pregunté al usuario para confirmar. No hay resultado de mail todavía."
                    else:
                        result = "No encontré mails suficientes para identificar proveedores de servicios."
                else:
                    gmail_data = await get_gmail_summary()
                    result = gmail_data or "No encontré mails relevantes."

            elif tool_name == "web_search":
                result = "Búsqueda web ejecutada."

        except Exception as e:
            result = f"Error ejecutando {tool_name}: {str(e)[:100]}"

        tool_results.append({
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": result
        })

    if not tool_results:
        reply = next((b.text for b in response.content if hasattr(b, "text")), "").strip()
        add_to_history(phone, "assistant", reply)
        return reply

    # Segunda llamada — Claude redacta la respuesta con los resultados
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
        reply = next((b.text for b in final_response.content if hasattr(b, "text")), "").strip()
    except Exception:
        reply = next((b.text for b in response.content if hasattr(b, "text")), "❌ Error procesando").strip()

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
            "Greeting Name":     {"rich_text": [{"text": {"content": user_prefs.get("greeting_name") or "Buenos días"}}]},
            "Resumen Extras":    {"rich_text": [{"text": {"content": extras_str}}]},
            "News Topics":       {"rich_text": [{"text": {"content": topics_str}}]},
            "Service Providers": {"rich_text": [{"text": {"content": json.dumps(user_prefs.get("service_providers", {}), ensure_ascii=False)}}]},
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

# ── MÓDULO CONFIGURACIÓN ──────────────────────────────────────────────────────
async def handle_configurar(text: str) -> str:
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=200,
        system="Extraé qué configuración cambiar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Mensaje: {text}
Respondé:
{{"setting": "daily_summary_hour",
  "hour": hora en formato 24h como entero — convertí AM/PM. null si no hay horario,
  "minute": minutos como entero — si no se mencionan usá 0,
  "greeting_name": nuevo nombre del saludo matutino o null,
  "add_extra": instrucción nueva para agregar al Resumen Diario, o null,
  "remove_extra": texto de instrucción a quitar del Resumen Diario, o null}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        data = json.loads(raw)
    except Exception:
        return "❌ No entendí qué configuración querés cambiar"

    setting = data.get("setting")
    hour    = data.get("hour")
    minute  = data.get("minute", 0) or 0
    greeting_name = data.get("greeting_name")
    add_extra  = data.get("add_extra")
    remove_extra = data.get("remove_extra")

    changed = []

    if greeting_name:
        user_prefs["greeting_name"] = greeting_name
        changed.append(f"📛 Saludo del Resumen Diario → *{greeting_name}*")

    if add_extra:
        extras = user_prefs.get("resumen_extras", [])
        if add_extra not in extras:
            extras.append(add_extra)
            user_prefs["resumen_extras"] = extras
        changed.append(f"➕ Extra agregado: _{add_extra}_")

    if remove_extra:
        extras = user_prefs.get("resumen_extras", [])
        user_prefs["resumen_extras"] = [e for e in extras if remove_extra.lower() not in e.lower()]
        changed.append(f"➖ Extra removido: _{remove_extra}_")

    if setting == "daily_summary_hour" and hour is not None:
        try:
            hora = int(hour)
            mins = int(minute)
            if not 0 <= hora <= 23:
                return "❌ El horario tiene que estar entre 0 y 23"
            if not 0 <= mins <= 59:
                mins = 0
            user_prefs["daily_summary_hour"]   = hora
            user_prefs["daily_summary_minute"] = mins
            hora_fmt = f"{hora:02d}:{mins:02d}"
            changed.append(f"🕐 Horario del resumen → *{hora_fmt}*")
        except Exception:
            return "❌ No pude interpretar el horario"

    if changed:
        await save_user_config(MY_NUMBER)
        return "✅ Listo:\n" + "\n".join(changed)

    extras_actuales = user_prefs.get("resumen_extras", [])
    hora_actual = user_prefs.get("daily_summary_hour") or DAILY_SUMMARY_HOUR
    mins_actual = user_prefs.get("daily_summary_minute") or 0
    estado = f"Actualmente el Resumen Diario llega a las *{hora_actual:02d}:{mins_actual:02d}*"
    if extras_actuales:
        estado += f" e incluye: {', '.join(extras_actuales)}"
    else:
        estado += " sin extras configurados"
    return f"¡Dale! ¿Qué querés modificar del Resumen Diario?\n\n{estado}\n\nPodés pedirme cosas como cambiar el horario, agregar que te cuente el clima de mañana, una frase del día, o lo que se te ocurra."

# ── MÓDULO REUNIONES ──────────────────────────────────────────────────────────
async def handle_reunion(text: str, image_b64: str = None, image_type: str = None) -> str:
    now = now_argentina()
    content_parts = []
    if image_b64:
        content_parts.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    prompt_reunion = (
        f"Hoy: {now.strftime('%Y-%m-%d %H:%M')}\n"
        f"Mensaje: {text or '(ver imagen adjunta)'}\n\n"
        "Extraé info de la reunión. Respondé SOLO JSON:\n"
        '{"nombre": "título/asunto de la reunión",'
        '"con_quien": "nombre(s) de los participantes o null",'
        '"fecha": "YYYY-MM-DD o null si no se menciona",'
        '"notas": "transcripción o resumen de las notas"}'
    )
    content_parts.append({"type": "text", "text": prompt_reunion})

    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=600,
        system="Extraé info de notas de reunión. Responde SOLO JSON válido sin markdown.",
        messages=[{"role": "user", "content": content_parts}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()

    try:
        data = json.loads(raw)
    except Exception:
        return "❌ No pude interpretar las notas de la reunión"

    nombre    = data.get("nombre") or "Reunión"
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
            return f"❌ Error guardando la reunión: {r.text[:100]}"

    try:
        fecha_fmt = datetime.strptime(fecha, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        fecha_fmt = fecha
    con_str = f" with {con_quien}" if con_quien else ""
    cal_str = f"\n🔗 Vinculada al evento de Calendar" if cal_link else ""
    return f"🤝 *{nombre}* guardada en Meetings{cal_str}\n📅 {fecha_fmt}{con_str}\n\n✅ Notas guardadas en Notion"

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
                raise ValueError("Número fuera de rango")
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
            await send_message(phone, f"⛽ *{name}* — {litros}L registrados ✅")
        else:
            await send_message(phone, f"❌ No pude actualizar los litros: {r.text[:80]}")
        return True

    if state_type == "event_clarification":
        candidates = state["candidates"]
        edit_data  = state["edit_data"]
        chosen = None
        text_strip = text.strip()
        if text_strip.isdigit():
            idx = int(text_strip) - 1
            if 0 <= idx < len(candidates):
                chosen = candidates[idx]
        else:
            chosen = fuzzy_match_event(text_strip, candidates)

        if not chosen:
            del pending_state[phone]
            await send_message(phone, "No entendí cuál. Cancelando edición — volvé a decirme qué evento querés cambiar.")
            return True

        del pending_state[phone]
        access_token = await get_gcal_access_token()
        if not access_token:
            await send_message(phone, "⚠️ Calendar no configurado")
            return True

        event = dict(chosen)
        event_id   = event["id"]
        event_name = event.get("summary", "Evento")

        if edit_data.get("new_title"):    event["summary"] = edit_data["new_title"]
        if edit_data.get("location"):     event["location"] = edit_data["location"]
        if edit_data.get("description"):  event["description"] = edit_data["description"]
        if edit_data.get("new_date") or edit_data.get("new_time"):
            if "dateTime" in event.get("start", {}):
                old_dt   = event["start"]["dateTime"][:16]
                new_date = edit_data.get("new_date") or old_dt[:10]
                new_time = edit_data.get("new_time") or old_dt[11:16]
                event["start"] = {"dateTime": f"{new_date}T{new_time}:00", "timeZone": "America/Argentina/Buenos_Aires"}
                if "dateTime" in event.get("end", {}):
                    dur = datetime.strptime(event["end"]["dateTime"][:16], "%Y-%m-%dT%H:%M") - datetime.strptime(old_dt, "%Y-%m-%dT%H:%M")
                    new_end = datetime.strptime(f"{new_date}T{new_time}", "%Y-%m-%dT%H:%M") + dur
                    event["end"] = {"dateTime": new_end.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}

        async with httpx.AsyncClient() as http:
            update_r = await http.put(
                f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                json=event
            )
        if update_r.status_code in [200, 201]:
            last_event_touched[phone] = {"event_id": event_id, "summary": event.get("summary", event_name)}
            await send_message(phone, f"✅ *{event_name}* actualizado")
        else:
            await send_message(phone, "❌ Error actualizando el evento")
        return True

    if state_type == "snooze":
        summary = state.get("summary", "Recordatorio")
        del pending_state[phone]

        if text.strip() == "snooze_no":
            await send_message(phone, "👍 Recordatorio descartado")
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
                await send_message(phone, f"⏰ Te recuerdo en {minutes} minutos")
            else:
                await send_message(phone, "❌ No pude posponer el recordatorio")
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
            await send_message(phone, "👍 Sin recordatorio adicional")
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
                    label = "1 día" if minutes == 1440 else f"{minutes} minutos"
                    await send_message(phone, f"⏰ Te aviso {label} antes de _{event_summary}_" if success else "❌ No pude crear el recordatorio")
                else:
                    await send_message(phone, "⚠️ Ese momento ya pasó, no puedo crear el recordatorio")
            except Exception:
                await send_message(phone, "❌ Error creando el recordatorio")
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
                    results_text.append(f"📋 {item.get('emoji','🛒')} _{item_name}_ ya estaba, aparece como faltante")
                else:
                    ok, err = await add_shopping_item(item)
                    results_text.append(f"✅ {item.get('emoji','🛒')} _{item_name}_ agregado" if ok else f"❌ Error: {err[:50]}")
            await send_message(phone, "\n".join(results_text) + "\n\n📋 Lista actualizada en Notion")
        else:
            await send_message(phone, f"👍 _{recipe_name.capitalize()}_ guardada. Ingredientes no agregados a la lista de compras.")
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
                f"¿Guardamos *{recipe_name.capitalize()}* en tus Recetas de Notion?",
                [
                    {"id": "recipe_save_yes", "title": "Sí, guardar"},
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
            await send_message(phone, "✏️ Decime qué está mal — qué falta, qué sobra o qué cambiar.")
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
                system="Respondé SOLO JSON válido sin markdown.",
                messages=[{"role": "user", "content": f"""Receta: "{recipe_name}"
Lista actual de ingredientes: {json.dumps(ing_names, ensure_ascii=False)}
Corrección del usuario: {text}
Aplicá la corrección y devolvé la lista corregida como array JSON simple:
["ingrediente1", "ingrediente2", ...]"""}]
            )
            raw_corr = corr_resp.content[0].text.strip()
            if raw_corr.startswith("```"):
                raw_corr = raw_corr.strip("`").lstrip("json").strip()
            corrected_names = json.loads(raw_corr)
            enriched_corrected = await enrich_items_with_claude(corrected_names)
        except Exception:
            enriched_corrected = ingredients
        ing_list = "\n".join(f"• {i.get('emoji','🛒')} {i.get('name','')}" for i in enriched_corrected)
        pending_state[phone] = {
            "type": "recipe_review",
            "recipe_name": recipe_name,
            "recipe_text": recipe_text,
            "ingredients": enriched_corrected,
        }
        await send_message(
            phone,
            f"🍽️ *{recipe_name.capitalize()}* — versión corregida:\n\n*Ingredientes:*\n{ing_list}"
        )
        await send_interactive_buttons(
            phone,
            "¿Está todo bien o seguís corrigiendo?",
            [
                {"id": "recipe_ok",      "title": "Está bien"},
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
            await send_message(phone, "⏳ Guardando receta en Notion...")
            ok, err = await save_recipe_to_notion(recipe_name, source="Matrics", ingredient_names=ingredients, recipe_text=recipe_text)
            if not ok:
                await send_message(phone, f"❌ Error guardando la receta: {err}")
                return True
            ing_list = "\n".join(f"• {i.get('emoji','🛒')} {i.get('name','')}" for i in ingredients)
            pending_state[phone] = {
                "type": "recipe_ingredients",
                "recipe_name": recipe_name,
                "ingredients": ingredients,
            }
            await send_message(
                phone,
                f"🍽️ *{recipe_name.capitalize()}* guardada en Recipes ✅\n\n*Ingredientes:*\n{ing_list}"
            )
            await send_interactive_buttons(
                phone,
                "¿Los agregás a la lista de compras?",
                [
                    {"id": "recipe_add_yes", "title": "Sí, agregar"},
                    {"id": "recipe_add_no",  "title": "No por ahora"},
                ]
            )
        else:
            await send_message(phone, "👍 Receta no guardada.")
        return True

    if state_type == "confirm_service_providers":
        proposed = state.get("proposed", {})
        del pending_state[phone]
        if text.strip() == "providers_ok":
            user_prefs["service_providers"] = proposed
            await save_user_config(phone)
            await send_message(phone, "✅ Listo, ya sé quiénes son tus proveedores de servicios. La próxima vez que me preguntes sobre facturas voy a buscar directamente.")
        else:
            # El usuario quiere corregir — pedir corrección en texto libre
            pending_state[phone] = {"type": "correct_service_providers", "proposed": proposed}
            await send_message(phone, "Dale, decime las correcciones. Por ejemplo: \"gas es Camuzzi, internet es Personal\"")
        return True

    if state_type == "correct_service_providers":
        proposed = state.get("proposed", {})
        del pending_state[phone]
        try:
            resp = claude_create(
                model="claude-sonnet-4-20250514", max_tokens=200,
                system="Aplicá las correcciones del usuario al JSON de proveedores. Respondé SOLO JSON.",
                messages=[{"role": "user", "content": f"Proveedores actuales: {json.dumps(proposed, ensure_ascii=False)}\nCorrecciones: {text}\nRespondé el JSON corregido."}]
            )
            raw = resp.content[0].text.strip().strip("`").lstrip("json").strip()
            corrected = json.loads(raw)
            user_prefs["service_providers"] = corrected
            await save_user_config(phone)
            resumen = ", ".join(f"{k}: {v}" for k, v in corrected.items())
            await send_message(phone, f"✅ Guardado: {resumen}")
        except Exception:
            await send_message(phone, "❌ No pude aplicar las correcciones. Intentá de nuevo.")
        return True


        new_data = state.get("new_event_data", {})
        similar  = state.get("similar_events", [])
        del pending_state[phone]

        if text.strip() == "evt_update" and similar:
            target   = similar[0]
            event_id = target["id"]
            event    = dict(target)
            access_token = await get_gcal_access_token()
            if not access_token:
                await send_message(phone, "⚠️ Calendar no configurado")
                return True
            if new_data.get("time"):
                start = {"dateTime": f"{new_data['date']}T{new_data['time']}:00", "timeZone": "America/Argentina/Buenos_Aires"}
                end_dt = datetime.strptime(f"{new_data['date']}T{new_data['time']}", "%Y-%m-%dT%H:%M") + timedelta(minutes=new_data.get("duration_minutes", 60))
                end = {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}
            else:
                start = {"date": new_data["date"]}
                end   = {"date": new_data["date"]}
            event["start"] = start
            event["end"]   = end
            if new_data.get("location"):
                event["location"] = new_data["location"]
            async with httpx.AsyncClient() as http:
                r = await http.put(
                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                    headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                    json=event
                )
            if r.status_code in [200, 201]:
                old_name = target.get("summary", "Evento")
                time_str = f" {new_data['time']}" if new_data.get("time") else ""
                try:
                    fmt_date = datetime.strptime(new_data['date'], "%Y-%m-%d").strftime("%d/%m/%Y")
                except Exception:
                    fmt_date = new_data['date']
                await send_message(phone, f"✅ *{old_name}* actualizado al {fmt_date}{time_str}")
                last_event_touched[phone] = {"event_id": event_id, "summary": old_name}
            else:
                await send_message(phone, "❌ Error actualizando el evento")
        else:
            guardado, event_id = await create_evento_gcal(new_data)
            if guardado and event_id:
                last_event_touched[phone] = {"event_id": event_id, "summary": new_data.get("summary", "Evento")}
                await send_message(phone, format_evento(new_data, guardado))
                if new_data.get("time"):
                    event_dt = f"{new_data['date']}T{new_data['time']}"
                    pending_state[phone] = {
                        "type": "event_reminder",
                        "event_id": event_id,
                        "summary": new_data.get("summary", "Evento"),
                        "event_datetime": event_dt
                    }
                    await send_interactive_buttons(
                        phone,
                        "¿Querés que te avise antes?",
                        [
                            {"id": "rem_15", "title": "15 min antes"},
                            {"id": "rem_60", "title": "1 hora antes"},
                            {"id": "rem_no", "title": "No gracias"},
                        ]
                    )
            else:
                await send_message(phone, format_evento(new_data, guardado))
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
    "⏳ Procesando", "🍽️ Receta", "✅ Recordatorio", "🔔 Recorda",
    "☀️ Buenos días", "🛒 Tu lista", "📊 Finanzas", "⏰ Te recuerdo",
    "🍽️", "✅ Guardado", "🗑️", "✏️", "🤝",
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
                await send_message(from_number, f"🗣️ _{transcripcion}_")
            else:
                await send_message(from_number, "❌ No pude transcribir el audio. Mandalo como texto.")
                return
        else:
            return

        if msg_type == "text" and is_bot_message(text):
            return

        if text.strip().lower() in ["/start", "hola", "help", "ayuda"]:
            await send_message(from_number,
                "👋 *Hola! Soy Matrics*\n\n"
                "💸 *Gastos:* _\"Verduleria 3500\"_\n"
                "🌿 *Plantas:* _\"Me compre un potus\"_\n"
                "📅 *Eventos:* _\"Manana a las 10 turno medico\"_\n"
                "📸 *Fotos:* manda cualquier factura\n"
                "🎤 *Audios:* hablá directo, te entiendo\n\n"
                "Todo se guarda automaticamente 💪"
            )
            return

        await send_message(from_number, "⏳ Procesando...")

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
            await send_message(from_number, msg if success else f"⚠️ {msg}")

        elif tipo == "ELIMINAR_GASTO":
            success, msg = await eliminar_gasto(text)
            await send_message(from_number, msg if success else f"⚠️ {msg}")

        elif tipo == "CORREGIR_GASTO":
            success, msg = await corregir_gasto(text, phone=from_number)
            await send_message(from_number, msg if success else f"⚠️ {msg}")

        elif tipo == "PLANTA":
            parsed = await parse_planta(text, exchange_rate)
            success, error = await create_planta(parsed)
            if success:
                await send_message(from_number, format_planta(parsed))
            else:
                await send_message(from_number, f"❌ Error guardando planta: {error[:200]}")

        elif tipo == "EVENTO":
            if not image_b64:
                clarif = await needs_clarification(from_number, text,
                    "el usuario quiere crear un evento en Google Calendar. "
                    "Si el mensaje tiene un título claro Y una fecha → CLEAR. "
                    "Solo preguntar si falta tanto el título como la fecha, o si es completamente ambiguo.")
                if clarif:
                    await send_message(from_number, clarif)
                    return
            parsed = await parse_evento(text, image_b64, image_type)
            if text.strip():
                parsed["caption"] = text.strip()

            if image_b64:
                similar = await find_similar_calendar_events(parsed)
                if similar:
                    sim_lines = []
                    for e in similar:
                        e_start = e.get("start", {})
                        e_date_raw = e_start.get("dateTime", e_start.get("date", ""))[:10]
                        try:
                            e_date = datetime.strptime(e_date_raw, "%Y-%m-%d").strftime("%d/%m/%Y")
                        except Exception:
                            e_date = e_date_raw
                        sim_lines.append(f"• {e.get('summary','')} ({e_date})")
                    sim_text = "\n".join(sim_lines)
                    pending_state[from_number] = {
                        "type": "confirm_event_or_update",
                        "new_event_data": parsed,
                        "similar_events": similar
                    }
                    await send_interactive_buttons(
                        from_number,
                        f"Encontré eventos similares en tu calendario:\n{sim_text}\n\n¿Qué hacemos?",
                        [
                            {"id": "evt_update", "title": "Actualizar existente"},
                            {"id": "evt_new",    "title": "Crear nuevo"},
                        ]
                    )
                    return

            guardado, event_id = await create_evento_gcal(parsed)
            if guardado and event_id:
                last_event_touched[from_number] = {
                    "event_id": event_id,
                    "summary": parsed.get("summary", "Evento")
                }
                await send_message(from_number, format_evento(parsed, guardado))
                if parsed.get("time"):
                    event_dt = f"{parsed['date']}T{parsed['time']}"
                    pending_state[from_number] = {
                        "type": "event_reminder",
                        "event_id": event_id,
                        "summary": parsed.get("summary", "Evento"),
                        "event_datetime": event_dt
                    }
                    await send_interactive_buttons(
                        from_number,
                        "¿Querés que te avise antes?",
                        [
                            {"id": "rem_15", "title": "15 min antes"},
                            {"id": "rem_60", "title": "1 hora antes"},
                            {"id": "rem_no", "title": "No gracias"},
                        ]
                    )
            else:
                await send_message(from_number, format_evento(parsed, guardado))

        elif tipo == "EDITAR_EVENTO":
            last_ev = last_event_touched.get(from_number, {}).get("summary", "")
            if last_ev:
                success, msg = await search_and_edit_evento(text, phone=from_number)
                await send_message(from_number, msg if success else f"⚠️ {msg}")
            else:
                clarif = await needs_clarification(from_number, text,
                    "el usuario quiere editar un evento del calendario pero no hay contexto reciente. "
                    "Si no queda claro qué evento editar, preguntar cuál.")
                if clarif:
                    await send_message(from_number, clarif)
                else:
                    success, msg = await search_and_edit_evento(text, phone=from_number)
                    await send_message(from_number, msg if success else f"⚠️ {msg}")

        elif tipo == "ELIMINAR_EVENTO":
            success, msg = await delete_evento(text)
            await send_message(from_number, msg if success else f"⚠️ {msg}")

        elif tipo == "RECORDATORIO":
            parsed = await parse_recordatorio(text)
            success, error = await create_recordatorio(parsed)
            if success:
                await send_message(from_number, format_recordatorio(parsed))
            else:
                await send_message(from_number, f"⚠️ No pude crear el recordatorio: {error[:100]}")

        elif tipo == "SHOPPING":
            shopping_text = text
            if not shopping_text.strip() and image_b64:
                try:
                    extr = claude_create(
                        model="claude-sonnet-4-20250514", max_tokens=1200,
                        system="Transcribí TODO el contenido de la imagen exactamente como está escrito. Si es una receta: copiá el título, luego todas las secciones tal como aparecen. No omitas nada.",
                        messages=[{"role": "user", "content": [
                            {"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}},
                            {"type": "text", "text": "Transcribí todo el contenido de esta imagen fielmente."}
                        ]}]
                    )
                    shopping_text = extr.content[0].text.strip()
                except Exception:
                    shopping_text = ""
            respuesta = await handle_shopping(shopping_text, phone=from_number)
            if respuesta is not None:
                await send_message(from_number, respuesta)

        elif tipo == "CONFIGURAR":
            respuesta = await handle_configurar(text)
            await send_message(from_number, respuesta)

        elif tipo == "REUNION":
            respuesta = await handle_reunion(text, image_b64, image_type)
            await send_message(from_number, respuesta)

        elif tipo == "CHAT":
            respuesta = await handle_chat(from_number, text)
            await send_message(from_number, respuesta)
            if "Ingredientes:" in respuesta and "Preparación:" in respuesta:
                try:
                    ext_response = claude_create(
                        model="claude-sonnet-4-20250514", max_tokens=400,
                        system="Respondé SOLO JSON válido sin markdown.",
                        messages=[{"role": "user", "content": f"""Del siguiente texto de receta, extraé el nombre y TODOS los ingredientes.
Texto: {respuesta[:2000]}
Respondé:
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
                    f"¿Guardamos *{recipe_name_chat.capitalize()}* en tus Recetas de Notion?",
                    [
                        {"id": "recipe_save_yes", "title": "Sí, guardar"},
                        {"id": "recipe_save_no",  "title": "No gracias"},
                    ]
                )

    except json.JSONDecodeError:
        pass
    except Exception as e:
        try:
            err_msg = f"{type(e).__name__}: {str(e)}"
            await send_message(from_number, f"❌ Error: {err_msg[:200]}")
        except Exception:
            pass

@app.get("/")
async def health():
    return {"status": "ok", "bot": "matrics"}

# ── MÓDULO RECORDATORIOS ───────────────────────────────────────────────────────
async def parse_recordatorio(text: str) -> dict:
    now = now_argentina()
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extraé info del recordatorio. Responde SOLO JSON válido sin markdown.",
        messages=[{"role": "user", "content": f"""Ahora son las {now.strftime("%Y-%m-%d %H:%M")} en Argentina.
Mensaje: {text}
Respondé:
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
    return f"{emoji} *{data['summary']}*\nTe aviso {tiempo_str}\n\n✅ Recordatorio configurado"

# ── CRON JOB ───────────────────────────────────────────────────────────────────
@app.get("/cron")
async def cron_job():
    await load_user_config(MY_NUMBER)
    now = now_argentina()
    fired = []

    # ── Resumen diario — no depende del token de Calendar ─────────────────────
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

    # ── Resumen nocturno — no depende del token de Calendar ───────────────────
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

    # ── Recordatorios — requieren Calendar ────────────────────────────────────
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
                    f"⏰ *¿Querés posponer este recordatorio?*\n_{clean_summary}_",
                    [
                        {"id": "snooze_5",  "title": "5 min"},
                        {"id": "snooze_15", "title": "15 min"},
                        {"id": "snooze_no", "title": "No posponer"},
                    ]
                )
                fired.append(f"TEMP: {summary}")
            elif "[REM:60]" in desc and 59 <= diff_seconds // 60 <= 61:
                loc_str = f"\n📍 {event.get('location')}" if event.get("location") else ""
                await send_message(MY_NUMBER, f"⏰ *En 1 hora:* {summary}{loc_str}")
                fired.append(f"REM60: {summary}")
            elif "[REM:15]" in desc and 14 <= diff_seconds // 60 <= 16:
                loc_str = f"\n📍 {event.get('location')}" if event.get("location") else ""
                await send_message(MY_NUMBER, f"⏰ *En 15 minutos:* {summary}{loc_str}")
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
    greeting = user_prefs.get("greeting_name") or "Buenos días"
    lines = [f"☀️ *{greeting}, Martín!*", ""]
    if w:
        lines.append(f"🌡️ {w['temp']}°C (sensación {w['sensacion']}°C) — {w['emoji']} {w['desc']}")
        if w["lluvia"] > 0:
            lines.append(f"🌧️ Lluvia ahora: {w['lluvia']}mm")
        lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
        pronostico = f"📊 Hoy: máx {w['hoy_max']}°C, mín {w['hoy_min']}°C"
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
                        lines.append("📆 *Tu semana:*")
                        for e in week_events:
                            s = e.get("start", {})
                            if "dateTime" in s:
                                dt = datetime.strptime(s["dateTime"][:16], "%Y-%m-%dT%H:%M")
                                lines.append(f"• {dt.strftime('%a %d/%m')} {dt.strftime('%H:%M')} — {e.get('summary', '')}")
                            else:
                                lines.append(f"• {s.get('date', '')[:10]} — {e.get('summary', '')} (todo el día)")
                        lines.append("")
        except Exception:
            pass
    else:
        if not events:
            lines.append("📅 Hoy no tenés eventos agendados.")
        else:
            lines.append(f"📅 *{'Tus eventos de hoy' if len(events) > 1 else 'Tu evento de hoy'}:*")
            for e in events:
                start = e.get("start", {})
                loc_str = f" — 📍{e.get('location', '')}" if e.get("location") else ""
                if "dateTime" in start:
                    lines.append(f"• {start['dateTime'][11:16]} — {e.get('summary', 'Evento')}{loc_str}")
                else:
                    lines.append(f"• {e.get('summary', 'Evento')} (todo el día){loc_str}")
    gmail_summary = await get_gmail_summary()
    if gmail_summary:
        lines.append("")
        lines.append(f"📬 *Mails importantes:*\n{gmail_summary}")

    extras = user_prefs.get("resumen_extras", [])
    if extras:
        try:
            extras_prompt = "\n".join(f"- {e}" for e in extras)
            extra_resp = claude_create(
                model="claude-sonnet-4-20250514", max_tokens=300,
                system=f"Sos Matrics. Hoy es {now.strftime('%A %d/%m/%Y')}. Generá contenido breve (máx 3 líneas por item) para los siguientes extras del Resumen Diario. Usás español rioplatense, tono natural y cálido.",
                messages=[{"role": "user", "content": f"Generá estos extras para el resumen matutino:\n{extras_prompt}"}]
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
                lineas.append(f"• {s['dateTime'][11:16]} — {e.get('summary','')}")
            else:
                lineas.append(f"• {e.get('summary','')} (todo el día)")
        eventos_str = "\n".join(lineas)

    context = f"Hoy es {now.strftime('%A %d/%m/%Y')}. Hora: {now.strftime('%H:%M')}."
    if eventos_str:
        context += f"\nEventos de mañana:\n{eventos_str}"
    else:
        context += "\nMañana no hay eventos agendados."

    try:
        resp = claude_create(
            model="claude-sonnet-4-20250514", max_tokens=300,
            system=f"""Sos Matrics. {context}
Generá un resumen nocturno breve y natural en español rioplatense. Incluí:
1. Un saludo de buenas noches y qué hay para mañana.
2. Una sugerencia espontánea: agendar algo, agregar a la lista de compras, registrar un gasto del día, o un pensamiento de cierre.
Sé conciso, cálido, natural. Máximo 5 líneas en total.""",
            messages=[{"role": "user", "content": "Generá el resumen nocturno."}]
        )
        msg = resp.content[0].text.strip()
    except Exception:
        if eventos_str:
            msg = f"🌙 Buenas noches! Mañana tenés:\n{eventos_str}\n\nQue descanses 😴"
        else:
            msg = "🌙 Buenas noches! Mañana el día está libre. Que descanses 😴"

    await send_message(MY_NUMBER, msg)

@app.get("/health")
async def health_check():
    return {"status": "ok", "time": now_argentina().strftime("%H:%M"), "bot": "matrics"}

# ── MÓDULO SHOPPING ────────────────────────────────────────────────────────────
def notion_headers():
    return {"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"}

SHOPPING_CATEGORIES = ["Frutas y verduras", "Enlatado", "Infusion", "Lacteo", "Especias",
                       "Limpieza", "Panificado", "Herramienta", "Construccion", "Higiene",
                       "Electrónica", "Carne", "Galletitas", "Alcohol", "Bebida", "Fiambre",
                       "Grano", "Comida", "Cosmética"]
SHOPPING_STORES    = ["Super", "Panaderia", "Verduleria", "Dietetica", "Farmacia", "Drogueria", "Ferreteria"]
SHOPPING_FREQUENCY = ["Often", "Monthly", "Annual", "One time"]

async def get_ingredients_and_enrich(recipe_name: str, recipe_text: str = None) -> tuple[list[dict], bool]:
    if recipe_text:
        context = f'Receta: "{recipe_name}"\nTexto completo de la receta:\n{recipe_text[:2000]}\n\nExtrae TODOS los ingredientes que aparecen en el texto de la receta.'
    else:
        context = f'Receta: "{recipe_name}"\n\nInferí los ingredientes típicos/estándar completos de esta receta.'

    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=800,
        system="Respondé SOLO JSON válido sin markdown ni texto extra.",
        messages=[{"role": "user", "content": f"""{context}

Respondé SOLO este array JSON:
[{{
  "name": "nombre del ingrediente capitalizado SIN cantidad",
  "display": "cantidad + nombre como aparece en la receta",
  "emoji": "emoji específico del producto",
  "category": una de {SHOPPING_CATEGORIES},
  "store": tienda más lógica,
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
        system="Enriquecé una lista de ítems. Responde SOLO JSON válido sin markdown.",
        messages=[{"role": "user", "content": f"""Items: {json.dumps(items, ensure_ascii=False)}

Para cada item respondé un array con:
- "name": nombre capitalizado
- "emoji": emoji específico (nunca 🛒)
- "category": una de {SHOPPING_CATEGORIES}
- "store": tienda más lógica
- "frequency": uno de {SHOPPING_FREQUENCY}

Respondé SOLO el array JSON."""}]
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
                system="Respondé SOLO JSON válido sin markdown.",
                messages=[{"role": "user", "content": f'''Receta: "{recipe_name}"
Texto: {(recipe_text or "")[:500]}
Respondé SOLO este JSON:
{{"difficult": "Easy"|"Moderate"|"Hard"|null,
  "type": ["Postre"|"Cena"|"Almuerzo"|"Desayuno"|"Snack"|"Cosmética"],
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
            valid_types = [t for t in meta["type"] if t in ["Postre", "Cena", "Almuerzo", "Desayuno", "Snack", "Cosmética"]]
            if valid_types:
                props["Type"] = {"multi_select": [{"name": t} for t in valid_types]}
        if meta.get("coccion") in ["Horno", "Sarten", "Pochar", "Frizzer ", "Varias prep."]:
            props["Cocción "] = {"select": {"name": meta["coccion"]}}
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
                        system="Formateá la siguiente receta para guardarla en Notion. Usá este formato:\n- Título de sección como ## (Ingredientes, Procedimiento, Notas)\n- Listas con - para ingredientes y pasos numerados con 1. 2. 3.\n- **negrita** para cantidades importantes\n- Responde SOLO el texto formateado, sin comentarios adicionales.",
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
        return False, f"Excepción: {str(e) or repr(e)} | {tb[-200:]}"

async def parse_shopping_intent(text: str) -> dict:
    safe_text = text.replace('"', "'").replace('\r', ' ').replace('\n', ' ')[:2000]
    response = claude_create(
        model="claude-sonnet-4-20250514", max_tokens=800,
        system="Analizá mensajes sobre lista de compras. Responde SOLO JSON válido sin markdown.",
        messages=[{"role": "user", "content": f"""Mensaje: {safe_text}

Respondé:
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
        return f"❌ No pude interpretar el mensaje: {str(e)[:100]}"

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
                ing_list = "\n".join(f"• {i.get('emoji','🛒')} {i.get('name','')}" for i in enriched)
                pending_state[phone] = {
                    "type": "recipe_ingredients",
                    "recipe_name": recipe_name,
                    "ingredients": enriched
                }
                await send_interactive_buttons(
                    phone,
                    f"📖 Receta encontrada en tus recetas.\n\nIngredientes:\n{ing_list}\n\n¿Los agregás a la lista de compras?",
                    [
                        {"id": "recipe_add_yes", "title": "Sí, agregar"},
                        {"id": "recipe_add_no",  "title": "No por ahora"},
                    ]
                )
                return f"📖 *{recipe_name.capitalize()}* encontrada en tus recetas ✅"
            else:
                items = notion_ingredients
                recipe_note = f"📖 *{recipe_name.capitalize()}* (de tus recetas)\n"
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
                        f"• {i.get('emoji','🛒')} {i.get('display') or i.get('name','')}"
                        for i in enriched_direct
                    )
                    await send_message(
                        phone,
                        f"🍽️ *{recipe_name.capitalize()}*\n\n*Ingredientes:*\n{ing_list_display}"
                    )
                    if text and len(text) > 100:
                        try:
                            proc_resp = claude_create(
                                model="claude-sonnet-4-20250514", max_tokens=600,
                                system="Extraé SOLO la sección de preparación/procedimiento de la receta. Sin título, sin lista de ingredientes. Solo los pasos de preparación en texto limpio.",
                                messages=[{"role": "user", "content": text[:2000]}]
                            )
                            proc_text = proc_resp.content[0].text.strip()
                        except Exception:
                            proc_text = text[:600]
                        await send_message(phone, f"📝 *Preparación:*\n{proc_text}")
                    await send_interactive_buttons(
                        phone,
                        "¿Está todo bien o querés corregir algo?",
                        [
                            {"id": "recipe_ok",      "title": "Está bien"},
                            {"id": "recipe_correct", "title": "Quiero corregir"},
                        ]
                    )
                    return None
                else:
                    return f"🍽️ *{recipe_name.capitalize()}* — {len(enriched_direct)} ingredientes detectados."
            else:
                items = []
                recipe_note = f"⚠️ No pude inferir los ingredientes para esa receta\n"

    if action == "list":
        async with httpx.AsyncClient() as http:
            r = await http.post(
                f"https://api.notion.com/v1/databases/{SHOPPING_DB_ID}/query",
                headers=notion_headers(),
                json={"filter": {"property": "Stock", "checkbox": {"equals": False}},
                      "sorts": [{"property": "Category", "direction": "ascending"}]}
            )
            if r.status_code != 200:
                return f"❌ No pude leer la lista: {r.text[:100]}"
            results = r.json().get("results", [])
            if not results:
                return "✅ ¡No te falta nada! La lista está vacía."
            lines = ["🛒 *Tu lista de compras:*\n"]
            for item in results:
                name = item["properties"]["Name"]["title"][0]["plain_text"] if item["properties"]["Name"]["title"] else "?"
                cat  = (item["properties"].get("Category", {}).get("select") or {}).get("name", "")
                lines.append(f"• {name}{f' _{cat}_' if cat else ''}")
            return "\n".join(lines)

    if not items:
        return "❓ No entendí qué producto querés actualizar."

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
                results_text.append(f"📋 {item.get('emoji','🛒')} _{item_name}_ ya estaba, aparece como faltante")
            else:
                ok, err = await add_shopping_item(item)
                results_text.append(f"✅ {item.get('emoji','🛒')} _{item_name}_ agregado" if ok else f"❌ Error agregando _{item_name}_: {err}")
        return recipe_note + "\n".join(results_text) + "\n\n📋 Lista actualizada en Notion"

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
            results_text.append(f"✅ _{display}_ marcado como en stock" if in_stock else f"🛒 _{display}_ agregado a la lista")
        else:
            if not in_stock:
                try:
                    enriched = await enrich_items_with_claude([item_name])
                    item_data = enriched[0] if enriched else {"name": display, "emoji": "🛒", "category": "", "store": "", "frequency": "One time"}
                except Exception:
                    item_data = {"name": display, "emoji": "🛒", "category": "", "store": "", "frequency": "One time"}
                ok, _ = await add_shopping_item(item_data)
                results_text.append(f"🛒 {item_data.get('emoji','🛒')} _{display}_ agregado como faltante" if ok else f"❌ Error agregando _{display}_")
            else:
                results_text.append(f"❓ _{display}_ no está en la lista")

    return "\n".join(results_text) + "\n\n📋 Lista actualizada en Notion"
