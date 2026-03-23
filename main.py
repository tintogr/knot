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
WA_TOKEN       = os.environ["WHATSAPP_TOKEN"]
WA_PHONE_ID    = os.environ["WHATSAPP_PHONE_ID"]
WA_API         = f"https://graph.facebook.com/v22.0/{WA_PHONE_ID}/messages"
MY_NUMBER      = os.environ.get("MY_WA_NUMBER", "54298154894334")
DAILY_SUMMARY_HOUR = int(os.environ.get("DAILY_SUMMARY_HOUR", "8"))

def now_argentina() -> datetime:
    """Hora actual en Argentina (UTC-3)."""
    return datetime.now(timezone.utc) - timedelta(hours=3)

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

# ── NUEVO: Transcripción de audio con Groq Whisper ────────────────────────────
async def transcribe_audio(media_id: str) -> str | None:
    """Descarga audio de WhatsApp y transcribe con Groq Whisper (gratis)."""
    groq_key = os.environ.get("GROQ_API_KEY")
    if not groq_key:
        return None
    async with httpx.AsyncClient(timeout=30) as http:
        # 1. Obtener URL del audio desde Meta
        r = await http.get(
            f"https://graph.facebook.com/v22.0/{media_id}",
            headers={"Authorization": f"Bearer {WA_TOKEN}"}
        )
        if r.status_code != 200:
            return None
        media_url = r.json()["url"]
        # 2. Descargar el archivo de audio
        audio_r = await http.get(media_url, headers={"Authorization": f"Bearer {WA_TOKEN}"})
        if audio_r.status_code != 200:
            return None
        audio_bytes = audio_r.content
        # 3. Mandar a Groq Whisper para transcribir
        resp = await http.post(
            "https://api.groq.com/openai/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {groq_key}"},
            files={"file": ("audio.ogg", audio_bytes, "audio/ogg")},
            data={"model": "whisper-large-v3", "language": "es"},
        )
        if resp.status_code == 200:
            return resp.json().get("text", "").strip()
    return None

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

Metodo: usa "Suscription" para gastos recurrentes mensuales (alquiler, expensas, luz, gas, agua, internet, telefono, streaming, gimnasio, psicologo, monotributo, seguros, cualquier servicio que se paga todos los meses). Para todo lo demas usa "Payment".

in_out: exactamente "\u2192INGRESO\u2190" o "\u2190 EGRESO \u2192"

Clientes: LBL, OPERA, ALPATACO, Juan Martin, Depto, Work, Santi Vales,
Jorge, Barbara, Vanguardia, Alejo, Dinamo, Paula Diaz, Labti, PlanA, JGA, ATE

Para el campo "emoji": elegir el emoji MAS especifico segun el contexto real del gasto.
Ejemplos de criterio:
- Verdura/fruta/feria/verduleria -> \U0001f96c
- Supermercado general/almacen -> \U0001f6d2
- Nafta/combustible/YPF/Shell -> \u26fd
- Repuesto/mecanico/taller/auto -> \U0001f527
- Birra/cerveza -> \U0001f37a
- Salir a comer/restaurant/pizza/sushi -> \U0001f37d
- Farmacia/medicamento/salud -> \U0001f48a
- Psicologo/salud mental -> \U0001f9e0
- Colectivo/uber/taxi -> \U0001f697
- Ropa/zapatillas/compras -> \U0001f6cd
- Planta/maceta/tierra -> \U0001f33f
- Viaje/avion/hotel -> \u2708
- Luz/gas/agua/internet/servicio -> \U0001f4c4
- Alquiler/expensas -> \U0001f3e0
- Sueldo/ingreso laboral -> \U0001f4b0
- Salida nocturna/boliche -> \U0001f389
- Streaming/juego/ocio -> \U0001f3ae
- Vianda/tupper/comida llevada -> \U0001f961
- Si no es claro -> \U0001f4b8"""

def build_user_prompt(text: str, exchange_rate: float) -> str:
    now = now_argentina()
    today = now.strftime("%Y-%m-%d")
    hora_actual = now.strftime("%H:%M")
    ingreso = "\u2192INGRESO\u2190"
    egreso = "\u2190 EGRESO \u2192"
    return f"""Tasa de cambio dolar blue hoy: ${exchange_rate:,.0f} ARS por USD.
Fecha y hora actual en Argentina: {today} {hora_actual}

Reglas para el campo "datetime":
- Si el mensaje incluye una hora especifica, usala.
- Si dice "anoche", "esta noche" -> hora nocturna (22:00-23:00)
- Si dice "esta manana", "hoy a la manana" -> hora matutina (09:00-10:00)
- Si dice "al mediodia" -> 12:00
- Si no hay referencia de hora -> usa la hora actual ({hora_actual})

Extrae la informacion y responde con este JSON:
{{
  "name": "descripcion corta del movimiento",
  "in_out": "{ingreso}" o "{egreso}",
  "value_ars": numero,
  "categoria": ["categoria1"] o ["categoria1", "categoria2"],
  "metodo": "Payment",
  "date": "YYYY-MM-DD",
  "time": "HH:MM",
  "litros": numero o null,
  "consumo_kwh": numero o null,
  "notas": "info extra" o null,
  "client": ["nombre"] o [],
  "emoji": "un solo emoji que represente especificamente este gasto"
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
        return (True, "") if r.status_code == 200 else (False, r.text)

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
        lines.append(" \u00b7 ".join(extras))
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
        f"\u2600\ufe0f Luz: {data.get('luz', '-')}",
        f"\U0001f4a7 Riego: {data.get('riego', '-')}",
        f"\U0001f3e0 Ubicaci\u00f3n: {data.get('ubicacion', '-')}",
    ]
    if data.get("notas"):
        lines.append(f"\n\U0001f4dd {data['notas']}")
    lines.append("\n\u2705 Guardada en Notion")
    return "\n".join(lines)

# ── MÓDULO EVENTOS ─────────────────────────────────────────────────────────────
def format_evento(data: dict, guardado: bool) -> str:
    emoji = data.get("emoji", "\U0001f4c5")
    hora = f" a las {data['time']}" if data.get("time") else ""
    lines = [f"{emoji} *{data['summary']}*", f"Fecha: {data['date']}{hora}"]
    if data.get("location"):
        lines.append(f"\U0001f4cd {data['location']}")
    if data.get("description"):
        lines.append(f"Nota: {data['description']}")
    lines.append("\n\u2705 Agregado a Google Calendar" if guardado else "\n\u26a0\ufe0f Anota esto manualmente \u2014 Calendar no configurado aun")
    return "\n".join(lines)

async def parse_evento(text: str) -> dict:
    now = now_argentina()
    today = now.strftime("%Y-%m-%d")
    hora_actual = now.strftime("%H:%M")
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extraé info de un evento. Responde SOLO JSON válido sin markdown. Usa zona horaria Argentina (UTC-3).",
        messages=[{"role": "user", "content": f"""Hoy es {today}, hora actual en Argentina: {hora_actual}
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
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "client_id":     client_id,
            "client_secret": client_secret,
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
        system="Extraé qué evento se quiere editar y qué se quiere cambiar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Hoy: {now.strftime("%Y-%m-%d")}
Mensaje: {text}
Respondé:
{{"search_term": "nombre del evento a buscar","location": "nueva ubicacion o null","new_title": "nuevo titulo o null","new_time": "HH:MM o null","new_date": "YYYY-MM-DD o null","description": "nueva descripcion o null"}}"""}]
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
            params={
                "q": edit_data.get("search_term", ""),
                "timeMin": time_min,
                "timeMax": time_max,
                "singleEvents": "true",
                "orderBy": "startTime",
                "maxResults": "5"
            }
        )

        if r.status_code != 200:
            return False, "Error buscando eventos"

        events = r.json().get("items", [])
        if not events:
            return False, f"No encontré ningún evento con ese nombre"

        event = events[0]
        event_id = event["id"]
        event_name = event.get("summary", "Evento")

        if edit_data.get("new_title"):
            event["summary"] = edit_data["new_title"]
        if edit_data.get("location"):
            event["location"] = edit_data["location"]
        if edit_data.get("description"):
            event["description"] = edit_data["description"]
        if edit_data.get("new_date") or edit_data.get("new_time"):
            if "dateTime" in event.get("start", {}):
                old_dt = event["start"]["dateTime"][:16]
                old_date = old_dt[:10]
                old_time = old_dt[11:16]
                new_date = edit_data.get("new_date") or old_date
                new_time = edit_data.get("new_time") or old_time
                event["start"] = {"dateTime": f"{new_date}T{new_time}:00", "timeZone": "America/Argentina/Buenos_Aires"}
                if "dateTime" in event.get("end", {}):
                    end_dt = datetime.strptime(event["end"]["dateTime"][:16], "%Y-%m-%dT%H:%M")
                    start_dt = datetime.strptime(f"{new_date}T{new_time}", "%Y-%m-%dT%H:%M")
                    dur = end_dt - datetime.strptime(old_dt, "%Y-%m-%dT%H:%M")
                    new_end = start_dt + dur
                    event["end"] = {"dateTime": new_end.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}

        update_r = await http.put(
            f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json=event
        )

        if update_r.status_code in [200, 201]:
            location_str = f"\n📍 {edit_data['location']}" if edit_data.get("location") else ""
            return True, f"✅ *{event_name}* actualizado{location_str}"
        else:
            return False, "Error actualizando el evento"

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
        system="""Responde SOLO una palabra: GASTO, PLANTA, EVENTO, EDITAR_EVENTO, RECORDATORIO, SHOPPING o CHAT.

GASTO: registrar un pago, compra o ingreso concreto con monto. Ej: "gasté 3500 en verdura", "cargué nafta", "me pagaron 50000", foto de factura.
PLANTA: adquirir o mencionar una planta sin contexto de precio como gasto.
EDITAR_EVENTO: modificar un evento existente en el calendario.
RECORDATORIO: "recordame en X tiempo", "avisame en X", "haceme acordar".
EVENTO: crear un evento nuevo en el calendario — turno, reunión, cumple, cita, viaje.
SHOPPING: gestionar lista de compras — "me quedé sin X", "compré X", "agregá X a la lista", "qué me falta".
CHAT: CUALQUIER pregunta, consulta o conversación — "cuánto gasté?", "cuánto llevo gastado?", "cuánto gané este mes?", preguntas sobre finanzas, consejos, charla, cálculos, redactar algo. Si el mensaje tiene signo de pregunta o pide información → CHAT.

REGLA CLAVE: si el mensaje PREGUNTA algo (tiene "?", "cuánto", "qué", "cómo", "cuál") → siempre es CHAT, nunca GASTO.""",
        messages=[{"role": "user", "content": text}]
    )
    r = response.content[0].text.strip().upper()
    if "EDITAR_EVENTO" in r: return "EDITAR_EVENTO"
    if "SHOPPING" in r: return "SHOPPING"
    if "RECORDATORIO" in r: return "RECORDATORIO"
    if "PLANTA" in r: return "PLANTA"
    if "EVENTO" in r: return "EVENTO"
    if "CHAT" in r: return "CHAT"
    return "GASTO"

async def query_finances(month: str = None) -> str:
    now = now_argentina()
    if not month:
        month = now.strftime("%Y-%m")

    year, mon = map(int, month.split("-"))
    last_day = monthrange(year, mon)[1]
    date_start = f"{month}-01"
    date_end = f"{month}-{last_day:02d}"

    async with httpx.AsyncClient() as http:
        r = await http.post(
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID.replace('-','')}/query",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
            json={
                "filter": {
                    "and": [
                        {"property": "Date", "date": {"on_or_after": date_start}},
                        {"property": "Date", "date": {"on_or_before": date_end}}
                    ]
                },
                "page_size": 100
            }
        )
        if r.status_code != 200:
            return None

        results = r.json().get("results", [])
        if not results:
            return f"No hay registros para {month}."

        ingresos = 0
        egresos = 0
        por_categoria = {}

        for page in results:
            props = page.get("properties", {})
            in_out = props.get("In - Out", {}).get("select", {})
            in_out_name = in_out.get("name", "") if in_out else ""
            value = props.get("Value (ars)", {}).get("number", 0) or 0
            cats = [c["name"] for c in props.get("Categoría", {}).get("multi_select", [])]

            if "INGRESO" in in_out_name:
                ingresos += value
            else:
                egresos += value
                for cat in cats:
                    por_categoria[cat] = por_categoria.get(cat, 0) + value

        balance = ingresos - egresos
        top_cats = sorted(por_categoria.items(), key=lambda x: x[1], reverse=True)[:5]

        summary = f"📊 *Finanzas {month}*\n\n"
        summary += f"💚 Ingresos: ${ingresos:,.0f}\n"
        summary += f"🔴 Egresos: ${egresos:,.0f}\n"
        summary += f"{'✅' if balance >= 0 else '⚠️'} Balance: ${balance:,.0f}\n"
        if top_cats:
            summary += f"\n📂 *Top categorías de gasto:*\n"
            for cat, val in top_cats:
                summary += f"• {cat}: ${val:,.0f}\n"
        return summary

async def handle_chat(phone: str, text: str) -> str:
    history = get_history(phone)
    add_to_history(phone, "user", text)
    now = now_argentina()

    finance_context = ""
    text_lower = text.lower()
    finance_keywords = ["gasté", "gaste", "gastado", "ingres", "gané", "gane", "balance", "cuánto", "cuanto", "finanzas", "plata", "mes"]
    if any(k in text_lower for k in finance_keywords):
        mes = now.strftime("%Y-%m")
        if "febrero" in text_lower: mes = f"{now.year}-02"
        elif "enero" in text_lower: mes = f"{now.year}-01"
        elif "marzo" in text_lower: mes = f"{now.year}-03"
        elif "abril" in text_lower: mes = f"{now.year}-04"
        finance_data = await query_finances(mes)
        if finance_data:
            finance_context = f"\n\nDATO REAL DE NOTION (usá esto para responder):\n{finance_data}"

    messages = history + [{"role": "user", "content": text}]

    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=800,
        system=f"""Sos Matrics, asistente personal en WhatsApp. Respondés conciso y natural.
Usás español rioplatense. Hoy: {now.strftime("%d/%m/%Y")} {now.strftime("%H:%M")}.
Podés ayudar con cualquier cosa: preguntas, cálculos, redacción, consejos, etc.
Si el mensaje tiene un monto para registrar, avisale que lo mande sin preámbulos.{finance_context}""",
        messages=messages
    )

    reply = response.content[0].text.strip()
    add_to_history(phone, "assistant", reply)
    return reply

# ── Webhook ────────────────────────────────────────────────────────────────────
@app.get("/webhook")
async def verify_webhook(request: Request):
    params = dict(request.query_params)
    verify_token = os.environ.get("WHATSAPP_VERIFY_TOKEN", "finanzas_bot_token")
    if params.get("hub.verify_token") == verify_token:
        return int(params.get("hub.challenge", 0))
    return {"error": "Verification failed"}

@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    try:
        entry = body["entry"][0]
        changes = entry["changes"][0]["value"]
        messages = changes.get("messages")
        if not messages:
            return {"ok": True}
        message = messages[0]
        background_tasks.add_task(process_message, message)
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
            caption = message["image"].get("caption", "")
            text = caption
            image_b64, image_type = await get_media_base64(media_id)

        elif msg_type == "document":
            media_id = message["document"]["id"]
            caption = message["document"].get("caption", "")
            text = caption
            image_b64, image_type = await get_media_base64(media_id)

        # ── NUEVO: soporte de audios ──────────────────────────────────────────
        elif msg_type == "audio":
            media_id = message["audio"]["id"]
            await send_message(from_number, "🎙️ Transcribiendo audio...")
            transcripcion = await transcribe_audio(media_id)
            if transcripcion:
                text = transcripcion
                await send_message(from_number, f"🎙️ _{transcripcion}_")
            else:
                await send_message(from_number, "❌ No pude transcribir el audio. Mandalo como texto por favor.")
                return
        # ─────────────────────────────────────────────────────────────────────

        else:
            return

        if text.strip().lower() in ["/start", "hola", "help", "ayuda"]:
            await send_message(from_number,
                "\U0001f44b *Hola! Soy Matrics*\n\n"
                "\U0001f4b8 *Gastos:* _\"Verduleria 3500\"_\n"
                "\U0001f33f *Plantas:* _\"Me compre un potus\"_\n"
                "\U0001f4c5 *Eventos:* _\"Manana a las 10 turno medico\"_\n"
                "\U0001f4f8 *Fotos:* manda cualquier factura\n"
                "\U0001f3a4 *Audios:* hablá directo, te entiendo\n\n"
                "Todo se guarda automaticamente \U0001f4aa"
            )
            return

        await send_message(from_number, "\u23f3 Procesando...")

        tipo = await classify(text, image_b64 is not None)
        exchange_rate = await get_exchange_rate()

        if tipo == "GASTO":
            parsed = await parse_with_claude(text, image_b64, image_type, exchange_rate)
            success, error = await create_notion_entry(parsed, exchange_rate)
            if success:
                await send_message(from_number, format_reply(parsed, exchange_rate))
            elif "No se pudo interpretar" in error:
                await send_message(from_number, "\u274c No entendi el monto. Ejemplo: _\"Verduleria 3500\"_")
            else:
                await send_message(from_number, f"\u274c Error Notion:\n{error[:200]}")

        elif tipo == "PLANTA":
            parsed = await parse_planta(text, exchange_rate)
            success, error = await create_planta(parsed)
            if success:
                await send_message(from_number, format_planta(parsed))
            else:
                await send_message(from_number, f"\u274c Error guardando planta: {error[:200]}")

        elif tipo == "EVENTO":
            parsed = await parse_evento(text)
            guardado = await create_evento_gcal(parsed)
            await send_message(from_number, format_evento(parsed, guardado))

        elif tipo == "EDITAR_EVENTO":
            success, msg = await search_and_edit_evento(text)
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
            await send_message(from_number, f"\u274c Error: {str(e)[:200]}")
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
{{"summary": "descripcion del recordatorio","fire_at": "YYYY-MM-DDTHH:MM (hora exacta en que disparar)","emoji": "emoji"}}
Ejemplos:
- "en 1 hora abri el lavarropa" → fire_at = ahora + 1 hora
- "en 30 minutos llamar al medico" → fire_at = ahora + 30 min
- "mañana a las 9 tomar medicamento" → fire_at = mañana 09:00"""}]
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
        "end": {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"},
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
        hora = dt.strftime("%H:%M")
        fecha = dt.strftime("%d/%m") if dt.date() != now_argentina().date() else "hoy"
        tiempo_str = f"{fecha} a las {hora}"
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

        time_min = now.strftime("%Y-%m-%dT%H:%M:00-03:00")
        time_max = (now + timedelta(minutes=61)).strftime("%Y-%m-%dT%H:%M:00-03:00")

        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers=headers,
            params={
                "timeMin": time_min,
                "timeMax": time_max,
                "singleEvents": "true",
                "orderBy": "startTime",
                "maxResults": "20"
            }
        )

        if r.status_code != 200:
            return {"ok": False}

        events = r.json().get("items", [])

        for event in events:
            event_id = event.get("id")
            summary = event.get("summary", "Evento")
            description = event.get("description", "") or ""
            start = event.get("start", {})

            if "dateTime" in start:
                event_dt_str = start["dateTime"][:16]
                try:
                    event_dt = datetime.strptime(event_dt_str, "%Y-%m-%dT%H:%M")
                    diff_minutes = int((event_dt - now.replace(tzinfo=None)).total_seconds() / 60)
                except Exception:
                    continue
            else:
                continue

            if "[TEMP]" in description and 0 <= diff_minutes <= 1:
                clean_name = summary.replace("🔔 ", "")
                await send_message(MY_NUMBER, f"🔔 *Recordatorio*\n{clean_name}")
                await http.delete(
                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                    headers=headers
                )
                fired.append(f"TEMP: {summary}")

            elif "[REM:60]" in description and 59 <= diff_minutes <= 61:
                loc = event.get("location", "")
                loc_str = f"\n📍 {loc}" if loc else ""
                await send_message(MY_NUMBER, f"⏰ *En 1 hora:* {summary}{loc_str}")
                fired.append(f"REM60: {summary}")

            elif "[REM:15]" in description and 14 <= diff_minutes <= 16:
                loc = event.get("location", "")
                loc_str = f"\n📍 {loc}" if loc else ""
                await send_message(MY_NUMBER, f"⏰ *En 15 minutos:* {summary}{loc_str}")
                fired.append(f"REM15: {summary}")

        if now.hour == DAILY_SUMMARY_HOUR and now.minute == 0:
            await send_daily_summary(http, access_token, now)
            fired.append("DAILY_SUMMARY")

    return {"ok": True, "fired": fired, "time": now.strftime("%H:%M")}

async def send_daily_summary(http, access_token: str, now: datetime):
    today_start = now.replace(hour=0, minute=0, second=0).strftime("%Y-%m-%dT00:00:00-03:00")
    today_end = now.replace(hour=23, minute=59, second=59).strftime("%Y-%m-%dT23:59:59-03:00")

    r = await http.get(
        "https://www.googleapis.com/calendar/v3/calendars/primary/events",
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "timeMin": today_start,
            "timeMax": today_end,
            "singleEvents": "true",
            "orderBy": "startTime",
            "maxResults": "10"
        }
    )

    if r.status_code != 200:
        return

    events = r.json().get("items", [])
    events = [e for e in events if "[TEMP]" not in (e.get("description") or "")]

    if not events:
        await send_message(MY_NUMBER, f"☀️ *Buenos días!*\nNo tenés eventos para hoy.")
        return

    lines = [f"☀️ *Buenos días! Tus eventos de hoy:*\n"]
    for e in events:
        summary = e.get("summary", "Evento")
        start = e.get("start", {})
        loc = e.get("location", "")
        if "dateTime" in start:
            hora = start["dateTime"][11:16]
            loc_str = f" — {loc}" if loc else ""
            lines.append(f"• {hora} — {summary}{loc_str}")
        else:
            lines.append(f"• {summary} (todo el día)")

    await send_message(MY_NUMBER, "\n".join(lines))

@app.get("/health")
async def health_check():
    return {"status": "ok", "time": now_argentina().strftime("%H:%M"), "bot": "matrics"}

# ── MÓDULO SHOPPING ────────────────────────────────────────────────────────────
def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }

async def parse_shopping_intent(text: str) -> dict:
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Analizá mensajes sobre lista de compras. Responde SOLO JSON válido sin markdown.",
        messages=[{"role": "user", "content": f"""Mensaje: {text}

Respondé con este JSON:
{{
  "action": "out_of_stock" | "in_stock" | "add" | "list",
  "items": ["item1", "item2"],
  "frequency": "Habitual" | "Monthly" | "One-time" | null,
  "store": "Supermercado" | "Verdulería" | "Farmacia" | null
}}

Reglas:
- "out_of_stock": "me quedé sin X", "no tengo X", "se acabó X" → destildar en stock
- "in_stock": "compré X", "ya tengo X", "conseguí X" → tildar en stock
- "add": "agregá X", "necesito comprar X", "añadí X" → crear ítem nuevo si no existe
- "list": "qué me falta", "qué tengo que comprar", "mostrame la lista" → listar items sin stock"""}]
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
            json={
                "filter": {
                    "property": "Name",
                    "title": {"contains": name[:30]}
                }
            }
        )
        if r.status_code == 200:
            return r.json().get("results", [])
        return []

async def handle_shopping(text: str) -> str:
    intent = await parse_shopping_intent(text)
    action = intent.get("action")
    items = intent.get("items", [])

    if action == "list":
        async with httpx.AsyncClient() as http:
            r = await http.post(
                f"https://api.notion.com/v1/databases/{SHOPPING_DB_ID}/query",
                headers=notion_headers(),
                json={
                    "filter": {"property": "en stock", "checkbox": {"equals": False}},
                    "sorts": [{"property": "tipo", "direction": "ascending"}]
                }
            )
            if r.status_code != 200:
                return "❌ No pude leer la lista de compras"
            results = r.json().get("results", [])
            if not results:
                return "✅ ¡No te falta nada! La lista está vacía."
            lines = ["🛒 *Tu lista de compras:*\n"]
            for item in results:
                name = item["properties"]["Name"]["title"][0]["plain_text"] if item["properties"]["Name"]["title"] else "?"
                tipo = item["properties"].get("tipo", {}).get("select", {})
                tipo_str = f" _{tipo.get('name', '')}_" if tipo else ""
                lines.append(f"• {name}{tipo_str}")
            return "\n".join(lines)

    if not items:
        return "❓ No entendí qué producto querés actualizar."

    results_text = []

    for item_name in items:
        if action == "add":
            existing = await search_shopping_item(item_name)
            if existing:
                page_id = existing[0]["id"]
                async with httpx.AsyncClient() as http:
                    await http.patch(
                        f"https://api.notion.com/v1/pages/{page_id}",
                        headers=notion_headers(),
                        json={"properties": {"en stock": {"checkbox": False}}}
                    )
                results_text.append(f"📋 _{item_name}_ ya estaba en la lista, aparece ahora como faltante")
            else:
                props = {
                    "Name": {"title": [{"text": {"content": item_name}}]},
                    "en stock": {"checkbox": False}
                }
                async with httpx.AsyncClient() as http:
                    r = await http.post(
                        "https://api.notion.com/v1/pages",
                        headers=notion_headers(),
                        json={"parent": {"database_id": SHOPPING_DB_ID}, "properties": props}
                    )
                if r.status_code == 200:
                    results_text.append(f"✅ _{item_name}_ agregado a la lista")
                else:
                    results_text.append(f"❌ Error agregando _{item_name}_: {r.status_code} — {r.text[:150]}")

        elif action in ["out_of_stock", "in_stock"]:
            in_stock = action == "in_stock"
            existing = await search_shopping_item(item_name)
            if existing:
                page_id = existing[0]["id"]
                async with httpx.AsyncClient() as http:
                    await http.patch(
                        f"https://api.notion.com/v1/pages/{page_id}",
                        headers=notion_headers(),
                        json={"properties": {"en stock": {"checkbox": in_stock}}}
                    )
                if in_stock:
                    results_text.append(f"✅ _{item_name}_ marcado como en stock")
                else:
                    results_text.append(f"🛒 _{item_name}_ agregado a la lista de compras")
            else:
                if not in_stock:
                    props = {
                        "Name": {"title": [{"text": {"content": item_name}}]},
                        "en stock": {"checkbox": False}
                    }
                    async with httpx.AsyncClient() as http:
                        r = await http.post(
                            "https://api.notion.com/v1/pages",
                            headers=notion_headers(),
                            json={"parent": {"database_id": SHOPPING_DB_ID}, "properties": props}
                        )
                    if r.status_code == 200:
                        results_text.append(f"🛒 _{item_name}_ no estaba en la lista, lo agregué como faltante")
                    else:
                        results_text.append(f"❌ Error: {r.status_code} — {r.text[:150]}")
                else:
                    results_text.append(f"❓ _{item_name}_ no está en la lista")

    return "\n".join(results_text) + "\n\n📋 Lista actualizada en Notion"
