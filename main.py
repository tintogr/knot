import os
import json
import base64
import httpx
from datetime import date, datetime, timedelta, timezone
from calendar import monthrange
from fastapi import FastAPI, Request, BackgroundTasks
from anthropic import Anthropic

app = FastAPI()

anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
NOTION_TOKEN   = os.environ["NOTION_TOKEN"]
NOTION_DB_ID   = os.environ["NOTION_DATABASE_ID"]
PLANTS_DB_ID   = os.environ.get("NOTION_PLANTS_DB_ID", "39d22615-0106-43f8-9f01-2632734c38da")
SHOPPING_DB_ID = os.environ.get("NOTION_SHOPPING_DB_ID", "cb85fdf75d684f61bafea20b5eeb653f")
RECIPES_DB_ID  = os.environ.get("NOTION_RECIPES_DB_ID", "5eda6aa7-3c8c-4fd6-8433-c6714bcfdd20")
WA_TOKEN       = os.environ["WHATSAPP_TOKEN"]
WA_PHONE_ID    = os.environ["WHATSAPP_PHONE_ID"]
WA_API         = f"https://graph.facebook.com/v22.0/{WA_PHONE_ID}/messages"
MY_NUMBER      = os.environ.get("MY_WA_NUMBER", "54298154894334")
DAILY_SUMMARY_HOUR = int(os.environ.get("DAILY_SUMMARY_HOUR", "8"))

# Coordenadas del usuario (default: Neuquén)
USER_LAT = float(os.environ.get("USER_LAT", "-38.95"))
USER_LON = float(os.environ.get("USER_LON", "-68.06"))

def now_argentina() -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=3)

# ── Memoria de categorías ──────────────────────────────────────────────────────
category_overrides: dict[str, list[str]] = {}

# ── Última entrada tocada (para correcciones contextuales) ────────────────────
# Guarda {page_id, name} del último gasto creado/corregido/eliminado
last_touched: dict[str, dict] = {}  # phone → {page_id, name}

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

# ── Clima (Open-Meteo, sin API key) ───────────────────────────────────────────
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
    """Obtiene clima actual + pronóstico de mañana desde Open-Meteo."""
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

            # Pronóstico mañana (índice 1 del daily)
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
    """Formato para el resumen matutino: hoy + mañana."""
    lines = [
        "*Hoy:*",
        f"🌡️ {w['temp']}°C (sensación {w['sensacion']}°C)",
        f"{w['emoji']} {w['desc']}",
    ]
    if w["lluvia"] > 0:
        lines.append(f"🌧️ Lluvia: {w['lluvia']}mm")
    lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
    lines.append("")
    lines.append("*Mañana:*")
    lines.append(f"🌡️ {w['manana_min']}°C — {w['manana_max']}°C")
    lines.append(f"{w['manana_emoji']} {w['manana_desc']}")
    if w["manana_lluvia"] > 0:
        lines.append(f"🌧️ Lluvia: {w['manana_lluvia']}mm")
    lines.append(f"💨 {w['manana_wind_desc']} ({w['manana_viento']} km/h)")
    return lines

def format_weather_chat(w: dict, include_tomorrow: bool = False) -> str:
    """Formato para respuestas de chat."""
    lines = [
        "*Hoy:*",
        f"🌡️ {w['temp']}°C (sensación {w['sensacion']}°C)",
        f"{w['emoji']} {w['desc']}",
    ]
    if w["lluvia"] > 0:
        lines.append(f"🌧️ Lluvia: {w['lluvia']}mm")
    lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
    if include_tomorrow:
        lines.append("")
        lines.append("*Mañana:*")
        lines.append(f"🌡️ {w['manana_min']}°C — {w['manana_max']}°C")
        lines.append(f"{w['manana_emoji']} {w['manana_desc']}")
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
SYSTEM_PROMPT = """Sos un asistente que extrae datos financieros de mensajes o imagenes para cargar en Notion.

Responde SIEMPRE y UNICAMENTE con un JSON valido, sin markdown, sin texto adicional.
Si algun campo no aplica, usa null.

Categorias disponibles (podes poner mas de una si tiene sentido, ej: Salida + Birra):
Supermercado, Sueldo, Servicios, Transporte, Vianda, Salud, Salud Mental,
Salida, Birra, Ocio, Compras, Depto, Plantas, Viajes

Distincion importante entre Servicios y Depto:
- Servicios: alquiler, expensas, luz, gas, agua, internet, telefono — pagos recurrentes de servicios
- Depto: maderas, pintura, muebles, herramientas, cortinas — compras fisicas para el departamento

Metodo: usa "Suscription" para gastos recurrentes mensuales (alquiler, expensas, luz, gas, agua, internet, telefono, streaming, gimnasio, psicologo, monotributo, seguros). Para todo lo demas usa "Payment".

in_out: exactamente "\u2192INGRESO\u2190" o "\u2190 EGRESO \u2192"

Clientes: LBL, OPERA, ALPATACO, Juan Martin, Depto, Work, Santi Vales,
Jorge, Barbara, Vanguardia, Alejo, Dinamo, Paula Diaz, Labti, PlanA, JGA, ATE

Para el campo "emoji": elegir el emoji MAS especifico segun el contexto real del gasto.
- Verdura/fruta/feria/verduleria -> \U0001f96c
- Supermercado general/almacen -> \U0001f6d2
- Nafta/combustible -> \u26fd
- Repuesto/mecanico/auto -> \U0001f527
- Birra/cerveza -> \U0001f37a
- Salir a comer/restaurant -> \U0001f37d
- Farmacia/medicamento -> \U0001f48a
- Psicologo/salud mental -> \U0001f9e0
- Colectivo/uber/taxi -> \U0001f697
- Ropa/zapatillas -> \U0001f6cd
- Planta/maceta -> \U0001f33f
- Viaje/avion/hotel -> \u2708
- Luz/gas/agua/internet -> \U0001f4c4
- Alquiler/expensas -> \U0001f3e0
- Sueldo/ingreso -> \U0001f4b0
- Salida nocturna -> \U0001f389
- Streaming/ocio -> \U0001f3ae
- Vianda/tupper -> \U0001f961
- Si no es claro -> \U0001f4b8"""

def build_user_prompt(text: str, exchange_rate: float) -> str:
    now = now_argentina()
    ingreso = "\u2192INGRESO\u2190"
    egreso = "\u2190 EGRESO \u2192"
    return f"""Tasa dolar blue: ${exchange_rate:,.0f} ARS/USD.
Fecha y hora actual: {now.strftime("%Y-%m-%d")} {now.strftime("%H:%M")}

Extrae la informacion y responde con este JSON:
{{
  "name": "descripcion corta",
  "in_out": "{ingreso}" o "{egreso}",
  "value_ars": numero,
  "categoria": ["categoria1"] o ["cat1", "cat2"],
  "metodo": "Payment",
  "date": "YYYY-MM-DD",
  "time": "HH:MM",
  "litros": numero o null,
  "consumo_kwh": numero o null,
  "notas": "info extra" o null,
  "client": ["nombre"] o [],
  "emoji": "emoji especifico"
}}

Mensaje: {text or "(ver imagen adjunta)"}"""

async def parse_with_claude(text="", image_b64=None, image_type=None, exchange_rate=1000.0) -> dict:
    content = []
    if image_b64:
        content.append({"type": "image", "source": {"type": "base64", "media_type": image_type, "data": image_b64}})
    content.append({"type": "text", "text": build_user_prompt(text, exchange_rate)})
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=1000,
        system=SYSTEM_PROMPT, messages=[{"role": "user", "content": content}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

async def create_notion_entry(data: dict, exchange_rate: float) -> tuple[bool, str]:
    if not data.get("value_ars") or not data.get("in_out"):
        return False, "No se pudo interpretar"
    props = {
        "Name":        {"title": [{"text": {"content": data["name"]}}]},
        "In - Out":    {"select": {"name": data["in_out"]}},
        "Value (ars)": {"number": float(data["value_ars"])},
        "Cambio":      {"number": exchange_rate},
        "Metodo":      {"select": {"name": data.get("metodo", "Payment")}},
    }
    if data.get("categoria"):
        props["Categor\u00eda"] = {"multi_select": [{"name": c} for c in data["categoria"]]}
    if data.get("date"):
        if data.get("time"):
            props["Date"] = {"date": {"start": f"{data['date']}T{data['time']}:00", "time_zone": "America/Argentina/Buenos_Aires"}}
        else:
            props["Date"] = {"date": {"start": data["date"]}}
    if data.get("client"):
        props["Client"] = {"multi_select": [{"name": c} for c in data["client"]]}
    if data.get("litros") is not None:
        props["Litros"] = {"number": float(data["litros"])}
    if data.get("consumo_kwh") is not None:
        props["Consumo (kWh)"] = {"number": float(data["consumo_kwh"])}
    if data.get("notas"):
        props["Notas adicionales"] = {"rich_text": [{"text": {"content": data["notas"]}}]}
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
            return True, ""
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
                    notion_cats = [c["name"] for c in results[0]["properties"].get("Categor\u00eda", {}).get("multi_select", [])]
                    if notion_cats and notion_cats != predicted_cats:
                        category_overrides[search_key.lower()] = notion_cats
                        return notion_cats, f"📚 Categoría: _{', '.join(notion_cats)}_ (como en cargas anteriores)"
    except Exception:
        pass
    return predicted_cats, None

async def corregir_gasto(text: str, phone: str = None) -> tuple[bool, str]:
    now = now_argentina()
    response = anthropic.messages.create(
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

    # Si no hay search_term claro, usar la última entrada tocada
    search_term = intent.get("search_term")
    page_id_direct = None

    if not search_term and phone and phone in last_touched:
        entry = last_touched[phone]
        page_id_direct = entry["page_id"]
        search_term = entry["name"]
    elif not search_term:
        return False, "No entendí qué gasto querés corregir"

    async with httpx.AsyncClient() as http:
        # Si tenemos el page_id directo, no necesitamos buscar
        if page_id_direct:
            page_id = page_id_direct
            old_name = search_term
            old_value = 0  # No crítico para el mensaje
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
            props["Categor\u00eda"] = {"multi_select": [{"name": c} for c in intent["new_categoria"]]}
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

        # Actualizar last_touched con el nombre nuevo si cambió
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
    """Busca la entrada más reciente en Notion que coincida y la archiva."""
    now = now_argentina()
    response = anthropic.messages.create(
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

        # Archivar la página (equivalente a mover a papelera en Notion)
        del_r = await http.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={"archived": True}
        )
        if del_r.status_code == 200:
            return True, f"🗑️ *{old_name}* eliminado de Notion"
        else:
            return False, f"Error eliminando la entrada: {del_r.text[:100]}"

def format_reply(data: dict, exchange_rate: float) -> str:
    is_expense = "EGRESO" in data["in_out"]
    entry_emoji = data.get("emoji", "\U0001f4b8")
    direction = "Egreso" if is_expense else "Ingreso"
    usd = data["value_ars"] / exchange_rate
    categorias = data.get("categoria") or []
    lines = [
        f"{entry_emoji} *{data['name']}*",
        f"{direction}: *${data['value_ars']:,.0f} ARS* (\u2248 USD {usd:.2f})",
        f"Categor\u00eda: {', '.join(categorias) if categorias else '-'}",
        f"M\u00e9todo: {data.get('metodo', 'Payment')}",
        f"Cambio: ${exchange_rate:,.0f}/USD",
    ]
    extras = []
    if data.get("litros"):
        extras.append(f"\u26fd {data['litros']}L")
    if data.get("consumo_kwh"):
        extras.append(f"\u26a1 {data['consumo_kwh']} kWh")
    if extras:
        lines.append(" · ".join(extras))
    lines.append("\n\u2705 Guardado en Notion")
    return "\n".join(lines)

# ── MÓDULO PLANTAS ─────────────────────────────────────────────────────────────
PLANTA_SYSTEM = """Extraé info de una planta y generá recomendaciones de cuidado.
Responde ÚNICAMENTE con JSON válido, sin markdown.
Valores para "luz": Sombra, Indirecta, Directa parcial, Pleno sol
Valores para "riego": Cada 2-3 días, Semanal, Quincenal, Mensual
Valores para "ubicacion": Interior, Exterior, Balcón, Terraza
Valores para "estado": Excelente, Bien, Regular, Necesita atención"""

async def parse_planta(text: str, exchange_rate: float) -> dict:
    response = anthropic.messages.create(
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
        props["Especie"] = {"rich_text": [{"text": {"content": data["especie"]}}]}
    if data.get("fecha_compra"):
        props["Fecha de compra"] = {"date": {"start": data["fecha_compra"]}}
    if data.get("precio"):
        props["Precio"] = {"number": float(data["precio"])}
    if data.get("luz"):
        props["Luz"] = {"select": {"name": data["luz"]}}
    if data.get("riego"):
        props["Riego"] = {"select": {"name": data["riego"]}}
    if data.get("ubicacion"):
        props["Ubicaci\u00f3n"] = {"select": {"name": data["ubicacion"]}}
    if data.get("estado"):
        props["Estado"] = {"select": {"name": data["estado"]}}
    if data.get("notas"):
        props["Notas"] = {"rich_text": [{"text": {"content": data["notas"]}}]}
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
    lines = [f"{emoji} *{data['summary']}*", f"Fecha: {data['date']}{hora}"]
    if data.get("location"):
        lines.append(f"📍 {data['location']}")
    if data.get("description"):
        lines.append(f"Nota: {data['description']}")
    lines.append("\n✅ Agregado a Google Calendar" if guardado else "\n⚠️ Anota esto manualmente — Calendar no configurado")
    return "\n".join(lines)

async def parse_evento(text: str) -> dict:
    now = now_argentina()
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extraé info de un evento. Responde SOLO JSON válido sin markdown. Usa zona horaria Argentina (UTC-3).",
        messages=[{"role": "user", "content": f"""Hoy es {now.strftime("%Y-%m-%d")}, hora actual: {now.strftime("%H:%M")}
Mensaje: {text}
Respondé:
{{"summary":"titulo","date":"YYYY-MM-DD","time":"HH:MM o null","duration_minutes":60,"location":"lugar o null","description":"desc o null","emoji":"emoji"}}"""}]
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

async def create_evento_gcal(data: dict) -> bool:
    access_token = await get_gcal_access_token()
    if not access_token:
        return False
    if data.get("time"):
        start = {"dateTime": f"{data['date']}T{data['time']}:00", "timeZone": "America/Argentina/Buenos_Aires"}
        end_dt = datetime.strptime(f"{data['date']}T{data['time']}", "%Y-%m-%dT%H:%M") + timedelta(minutes=data.get("duration_minutes", 60))
        end = {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}
    else:
        start = {"date": data["date"]}
        end = {"date": data["date"]}
    event = {"summary": data.get("summary", "Evento"), "start": start, "end": end}
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
        return r.status_code in [200, 201]

async def search_and_edit_evento(text: str) -> tuple[bool, str]:
    access_token = await get_gcal_access_token()
    if not access_token:
        return False, "Calendar no configurado"
    now = now_argentina()
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extraé qué evento editar y qué cambiar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Hoy: {now.strftime("%Y-%m-%d")}
Mensaje: {text}
Respondé:
{{"search_term":"nombre del evento","location":"nueva ubicacion o null","new_title":"nuevo titulo o null","new_time":"HH:MM o null","new_date":"YYYY-MM-DD o null","description":"nueva descripcion o null"}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    edit_data = json.loads(raw)
    async with httpx.AsyncClient() as http:
        time_min = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00-03:00")
        time_max = (now + timedelta(days=60)).strftime("%Y-%m-%dT23:59:59-03:00")
        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"q": edit_data.get("search_term", ""), "timeMin": time_min, "timeMax": time_max,
                    "singleEvents": "true", "orderBy": "startTime", "maxResults": "5"}
        )
        if r.status_code != 200:
            return False, "Error buscando eventos"
        events = r.json().get("items", [])
        if not events:
            return False, "No encontré ningún evento con ese nombre"
        event = events[0]
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
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json=event
        )
        if update_r.status_code in [200, 201]:
            loc_str = f"\n📍 {edit_data['location']}" if edit_data.get("location") else ""
            return True, f"✅ *{event_name}* actualizado{loc_str}"
        return False, "Error actualizando el evento"

async def delete_evento(text: str) -> tuple[bool, str]:
    access_token = await get_gcal_access_token()
    if not access_token:
        return False, "Calendar no configurado"
    now = now_argentina()
    tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=150,
        system="Extraé info sobre qué evento(s) eliminar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Hoy: {now.strftime("%Y-%m-%d")}, mañana: {tomorrow}
Mensaje: {text}
Respondé:
{{"search_term": "nombre del evento o null",
  "target_date": "YYYY-MM-DD si se menciona fecha, sino null",
  "delete_all": true si quiere borrar todos los eventos de esa fecha}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    intent = json.loads(raw)
    async with httpx.AsyncClient() as http:
        headers = {"Authorization": f"Bearer {access_token}"}
        if intent.get("target_date"):
            date_str = intent["target_date"]
            time_min = f"{date_str}T00:00:00-03:00"
            time_max = f"{date_str}T23:59:59-03:00"
        else:
            time_min = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00-03:00")
            time_max = (now + timedelta(days=60)).strftime("%Y-%m-%dT23:59:59-03:00")
        params = {"timeMin": time_min, "timeMax": time_max, "singleEvents": "true",
                  "orderBy": "startTime", "maxResults": "10"}
        if intent.get("search_term"):
            params["q"] = intent["search_term"]
        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers=headers, params=params
        )
        if r.status_code != 200:
            return False, "Error buscando eventos"
        events = r.json().get("items", [])
        events = [e for e in events if "[TEMP]" not in (e.get("description") or "")]
        if not events:
            return False, "No encontré ningún evento para eliminar"
        deleted = []
        for event in events:
            del_r = await http.delete(
                f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event['id']}",
                headers=headers
            )
            if del_r.status_code == 204:
                deleted.append(event.get("summary", "Evento"))
            if not intent.get("delete_all"):
                break
        if not deleted:
            return False, "Error eliminando el evento"
        if len(deleted) == 1:
            return True, f"🗑️ *{deleted[0]}* eliminado del calendario"
        lista = "\n".join(f"• {e}" for e in deleted)
        return True, f"🗑️ *{len(deleted)} eventos eliminados:*\n{lista}"

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

# ── CLASIFICADOR ───────────────────────────────────────────────────────────────
async def classify(text: str, has_image: bool) -> str:
    if has_image and not text.strip():
        return "GASTO"
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=10,
        system="""Responde SOLO una palabra: GASTO, CORREGIR_GASTO, PLANTA, EVENTO, EDITAR_EVENTO, ELIMINAR_EVENTO, RECORDATORIO, SHOPPING o CHAT.

GASTO: registrar un pago, compra o ingreso concreto con monto.
CORREGIR_GASTO: corregir un gasto ya registrado. Ej: "me equivoqué, era 7000 no 7500", "cambiá el monto de la verdulería".
ELIMINAR_GASTO: eliminar o borrar una entrada de Notion. Ej: "borrá ese gasto", "eliminá la entrada que se llama X", "sacá ese registro de Notion".
PLANTA: adquirir o mencionar una planta.
EDITAR_EVENTO: modificar un evento existente en el calendario.
ELIMINAR_EVENTO: eliminar o borrar un evento del calendario.
RECORDATORIO: "recordame en X tiempo", "avisame en X", "haceme acordar".
EVENTO: crear un evento nuevo — turno, reunión, cumple, cita, viaje.
SHOPPING: gestionar lista de compras — "me quedé sin X", "compré X", "agregá X", "qué me falta".
CHAT: cualquier pregunta, consulta o conversación. Si tiene "?" o pide información → CHAT.

REGLA: si el mensaje PREGUNTA algo → siempre CHAT, nunca GASTO.""",
        messages=[{"role": "user", "content": text}]
    )
    r = response.content[0].text.strip().upper()
    if "ELIMINAR_EVENTO" in r:  return "ELIMINAR_EVENTO"
    if "EDITAR_EVENTO" in r:    return "EDITAR_EVENTO"
    if "ELIMINAR_GASTO" in r:   return "ELIMINAR_GASTO"
    if "CORREGIR_GASTO" in r:   return "CORREGIR_GASTO"
    if "SHOPPING" in r:         return "SHOPPING"
    if "RECORDATORIO" in r:     return "RECORDATORIO"
    if "PLANTA" in r:           return "PLANTA"
    if "EVENTO" in r:           return "EVENTO"
    if "CHAT" in r:             return "CHAT"
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
            cats = [c["name"] for c in props.get("Categor\u00eda", {}).get("multi_select", [])]
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

async def handle_chat(phone: str, text: str) -> str:
    history = get_history(phone)
    add_to_history(phone, "user", text)
    now = now_argentina()
    text_lower = text.lower()

    finance_context = ""
    if any(k in text_lower for k in ["gasté","gaste","gastado","ingres","gané","gane","balance","cuánto","cuanto","finanzas","plata","mes"]):
        mes = now.strftime("%Y-%m")
        if "febrero" in text_lower: mes = f"{now.year}-02"
        elif "enero" in text_lower:  mes = f"{now.year}-01"
        elif "marzo" in text_lower:  mes = f"{now.year}-03"
        elif "abril" in text_lower:  mes = f"{now.year}-04"
        data = await query_finances(mes)
        if data:
            finance_context = f"\n\nDATO REAL DE NOTION:\n{data}"

    calendar_context = ""
    if any(k in text_lower for k in ["evento","turno","reunión","reunion","agenda","calendario","tengo algo","qué tengo","que tengo","esta semana","próximos","proximos","mañana","hoy"]):
        days = 7 if ("esta semana" in text_lower or "próximos" in text_lower) else (30 if "este mes" in text_lower else 2)
        cal_data = await query_calendar(days_ahead=days)
        if cal_data:
            calendar_context = f"\n\nDATO REAL DE GOOGLE CALENDAR:\n{cal_data}"

    weather_context = ""
    if any(k in text_lower for k in ["clima","lluvia","frío","frio","calor","temperatura","viento","tiempo","paraguas","abrigo","nublado","sol","llueve"]):
        w = await get_weather()
        if w:
            include_tomorrow = any(k in text_lower for k in ["mañana","manana","semana","pronóstico","pronostico"])
            weather_context = f"\n\nCLIMA:\n{format_weather_chat(w, include_tomorrow=include_tomorrow)}"

    # Solo mencionar la fuente si la piden explícitamente
    source_note = ""
    if any(k in text_lower for k in ["de dónde","de donde","fuente","qué app","que app","cómo sabés","como sabes","qué modelo","que modelo"]):
        source_note = "\n\nSi te preguntan: los datos vienen de Open-Meteo, usando modelos meteorológicos ECMWF y GFS."

    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=800,
        system=f"""Sos Matrics, asistente personal en WhatsApp. Respondés conciso y natural.
Usás español rioplatense. Hoy: {now.strftime("%d/%m/%Y")} {now.strftime("%H:%M")}.{finance_context}{calendar_context}{weather_context}{source_note}""",
        messages=history + [{"role": "user", "content": text}]
    )
    reply = response.content[0].text.strip()
    add_to_history(phone, "assistant", reply)
    return reply

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

async def process_message(message: dict):
    from_number = "54298154894334"
    try:
        msg_type = message["type"]
        text = ""
        image_b64 = image_type = None

        if msg_type == "text":
            text = message["text"]["body"]
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

        tipo = await classify(text, image_b64 is not None)
        exchange_rate = await get_exchange_rate()

        if tipo == "GASTO":
            parsed = await parse_with_claude(text, image_b64, image_type, exchange_rate)
            final_cats, cat_note = await check_and_apply_category(parsed["name"], parsed.get("categoria", []))
            parsed["categoria"] = final_cats
            success, error = await create_notion_entry(parsed, exchange_rate)
            if success:
                reply = format_reply(parsed, exchange_rate)
                if cat_note:
                    reply += f"\n{cat_note}"
                await send_message(from_number, reply)
            elif "No se pudo interpretar" in error:
                await send_message(from_number, "❌ No entendi el monto. Ejemplo: _\"Verduleria 3500\"_")
            else:
                await send_message(from_number, f"❌ Error Notion:\n{error[:200]}")

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
            parsed = await parse_evento(text)
            guardado = await create_evento_gcal(parsed)
            await send_message(from_number, format_evento(parsed, guardado))

        elif tipo == "EDITAR_EVENTO":
            success, msg = await search_and_edit_evento(text)
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
            respuesta = await handle_shopping(text)
            await send_message(from_number, respuesta)

        elif tipo == "CHAT":
            respuesta = await handle_chat(from_number, text)
            await send_message(from_number, respuesta)

    except json.JSONDecodeError:
        pass
    except Exception as e:
        try:
            await send_message(from_number, f"❌ Error: {str(e)[:200]}")
        except Exception:
            pass

@app.get("/")
async def health():
    return {"status": "ok", "bot": "matrics"}

# ── MÓDULO RECORDATORIOS ───────────────────────────────────────────────────────
async def parse_recordatorio(text: str) -> dict:
    now = now_argentina()
    response = anthropic.messages.create(
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
    event = {
        "summary": f"🔔 {data['summary']}",
        "description": "[TEMP]",
        "start": {"dateTime": f"{fire_at}:00", "timeZone": "America/Argentina/Buenos_Aires"},
        "end":   {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"},
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
    access_token = await get_gcal_access_token()
    if not access_token:
        return {"ok": False, "reason": "no gcal token"}
    now = now_argentina()
    fired = []
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
            return {"ok": False}
        for event in r.json().get("items", []):
            event_id  = event.get("id")
            summary   = event.get("summary", "Evento")
            desc      = event.get("description", "") or ""
            start     = event.get("start", {})
            if "dateTime" not in start:
                continue
            try:
                diff_minutes = int((datetime.strptime(start["dateTime"][:16], "%Y-%m-%dT%H:%M") - now.replace(tzinfo=None)).total_seconds() / 60)
            except Exception:
                continue
            if "[TEMP]" in desc and 0 <= diff_minutes <= 1:
                await send_message(MY_NUMBER, f"🔔 *Recordatorio*\n{summary.replace('🔔 ', '')}")
                await http.delete(f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}", headers=headers)
                fired.append(f"TEMP: {summary}")
            elif "[REM:60]" in desc and 59 <= diff_minutes <= 61:
                loc_str = f"\n📍 {event.get('location')}" if event.get("location") else ""
                await send_message(MY_NUMBER, f"⏰ *En 1 hora:* {summary}{loc_str}")
                fired.append(f"REM60: {summary}")
            elif "[REM:15]" in desc and 14 <= diff_minutes <= 16:
                loc_str = f"\n📍 {event.get('location')}" if event.get("location") else ""
                await send_message(MY_NUMBER, f"⏰ *En 15 minutos:* {summary}{loc_str}")
                fired.append(f"REM15: {summary}")
        if now.hour == DAILY_SUMMARY_HOUR and now.minute == 0:
            await send_daily_summary(http, access_token, now)
            fired.append("DAILY_SUMMARY")
    return {"ok": True, "fired": fired, "time": now.strftime("%H:%M")}

async def send_daily_summary(http, access_token: str, now: datetime):
    """Resumen matutino: clima + eventos del día."""
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

    lines = ["☀️ *Buenos días, Martín!*", ""]
    if w:
        lines.extend(format_weather_lines(w))
        lines.append("")

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

    await send_message(MY_NUMBER, "\n".join(lines))

@app.get("/health")
async def health_check():
    return {"status": "ok", "time": now_argentina().strftime("%H:%M"), "bot": "matrics"}

# ── MÓDULO SHOPPING ────────────────────────────────────────────────────────────
def notion_headers():
    return {"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"}

# ── Categorías y tiendas válidas de la DB Shopping ────────────────────────────
SHOPPING_CATEGORIES = ["Frutas y verduras", "Enlatado", "Infusion", "Lacteo", "Especias",
                       "Limpieza", "Panificado", "Herramienta", "Construccion", "Higiene",
                       "Electrónica", "Carne", "Galletitas", "Alcohol", "Bebida", "Fiambre",
                       "Grano", "Comida"]
SHOPPING_STORES     = ["Super", "Panaderia", "Verduleria", "Dietetica"]
SHOPPING_FREQUENCY  = ["Often", "Monthly", "Annual", "One time"]

async def enrich_items_with_claude(items: list[str]) -> list[dict]:
    """Pide a Claude que enriquezca cada ítem con emoji, categoría, tienda y frecuencia."""
    if not items:
        return []
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=600,
        system="Enriquecé una lista de ítems de supermercado. Responde SOLO JSON válido sin markdown.",
        messages=[{"role": "user", "content": f"""Items: {json.dumps(items, ensure_ascii=False)}

Para cada item respondé un array con:
- "name": nombre capitalizado correctamente
- "emoji": emoji MÁS específico para ese producto (nunca usar 🛒)
- "category": una de {SHOPPING_CATEGORIES}
- "store": la tienda más lógica de {SHOPPING_STORES}
- "frequency": uno de {SHOPPING_FREQUENCY}
  * "Often" = compra frecuente (verduras, lácteos, pan, yerba)
  * "Monthly" = mensual (aceite, harina, arroz, pasta, limpieza)
  * "Annual" = esporádico (especias raras, herramientas)
  * "One time" = compra puntual única

Respondé SOLO el array JSON:
[{{"name": "Aceite de oliva", "emoji": "🫒", "category": "Comida", "store": "Super", "frequency": "Monthly"}}]"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

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

async def save_recipe_to_notion(recipe_name: str, source: str = "Matrics"):
    try:
        async with httpx.AsyncClient() as http:
            await http.post(
                "https://api.notion.com/v1/pages",
                headers=notion_headers(),
                json={
                    "parent": {"database_id": RECIPES_DB_ID.replace("-", "")},
                    "icon": {"type": "emoji", "emoji": "🍽️"},
                    "properties": {
                        "Name": {"title": [{"text": {"content": recipe_name.capitalize()}}]},
                        "Fuente": {"select": {"name": source}},
                    }
                }
            )
    except Exception:
        pass

async def parse_shopping_intent(text: str) -> dict:
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Analizá mensajes sobre lista de compras. Responde SOLO JSON válido sin markdown.",
        messages=[{"role": "user", "content": f"""Mensaje: {text}

Respondé:
{{"action": "out_of_stock"|"in_stock"|"add"|"list",
  "items": ["item1", "item2"],
  "recipe_name": "nombre de la receta o null",
  "is_recipe_request": true/false}}

- out_of_stock: "me quedé sin X", "no tengo X"
- in_stock: "compré X", "ya tengo X"
- add: "agregá X", "necesito X", ingredientes para algo
- list: "qué me falta", "mostrame la lista"
- is_recipe_request=true si pide ingredientes de una receta específica"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

async def search_shopping_item(name: str) -> list:
    async with httpx.AsyncClient() as http:
        r = await http.post(
            f"https://api.notion.com/v1/databases/{SHOPPING_DB_ID}/query",
            headers=notion_headers(),
            json={"filter": {"property": "Name", "title": {"contains": name[:30]}}}
        )
        return r.json().get("results", []) if r.status_code == 200 else []

async def add_shopping_item(item: dict) -> tuple[bool, str]:
    name  = item.get("name", "").strip()
    emoji = item.get("emoji", "🛒")
    freq  = item.get("frequency", "One time")
    props = {
        "Name":  {"title": [{"text": {"content": name}}]},
        "Stock": {"checkbox": False},
    }
    if item.get("category") in SHOPPING_CATEGORIES:
        props["Category"] = {"select": {"name": item["category"]}}
    if item.get("store") in SHOPPING_STORES:
        props["Store"] = {"multi_select": [{"name": item["store"]}]}
    if freq in SHOPPING_FREQUENCY:
        props["Frequency"] = {"status": {"name": freq}}
    async with httpx.AsyncClient() as http:
        r = await http.post(
            "https://api.notion.com/v1/pages",
            headers=notion_headers(),
            json={"parent": {"database_id": SHOPPING_DB_ID}, "icon": {"type": "emoji", "emoji": emoji}, "properties": props}
        )
        return r.status_code == 200, r.text[:150] if r.status_code != 200 else ""

async def handle_shopping(text: str) -> str:
    try:
        intent = await parse_shopping_intent(text)
    except Exception as e:
        return f"❌ No pude interpretar el mensaje: {str(e)[:100]}"

    action      = intent.get("action")
    items       = intent.get("items", [])
    is_recipe   = intent.get("is_recipe_request", False)
    recipe_name = intent.get("recipe_name")
    recipe_note = ""

    if action == "add" and is_recipe and recipe_name:
        notion_ingredients = await search_recipe_in_notion(recipe_name)
        if notion_ingredients:
            items = notion_ingredients
            recipe_note = f"📖 *{recipe_name.capitalize()}* (de tus recetas)\n"
        else:
            try:
                resp = anthropic.messages.create(
                    model="claude-sonnet-4-20250514", max_tokens=200,
                    system="Respondé SOLO JSON sin markdown.",
                    messages=[{"role": "user", "content": f'Ingredientes básicos para "{recipe_name}". Respondé: {{"items": ["ingrediente1", ...]}}'}]
                )
                raw = resp.content[0].text.strip()
                if raw.startswith("```"):
                    raw = raw.strip("`").lstrip("json").strip()
                items = json.loads(raw).get("items", [])
            except Exception:
                items = []
            await save_recipe_to_notion(recipe_name, source="Matrics")
            recipe_note = f"🍽️ No tenía esa receta — la agregué a Recetas\n"

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
