import os
import json
import base64
import httpx
from datetime import date, datetime, timedelta, timezone
from fastapi import FastAPI, Request
from anthropic import Anthropic

app = FastAPI()

anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
NOTION_TOKEN   = os.environ["NOTION_TOKEN"]
NOTION_DB_ID   = os.environ["NOTION_DATABASE_ID"]
PLANTS_DB_ID   = os.environ.get("NOTION_PLANTS_DB_ID", "39d22615-0106-43f8-9f01-2632734c38da")
WA_TOKEN       = os.environ["WHATSAPP_TOKEN"]
WA_PHONE_ID    = os.environ["WHATSAPP_PHONE_ID"]
WA_API         = f"https://graph.facebook.com/v22.0/{WA_PHONE_ID}/messages"

def now_argentina() -> datetime:
    """Hora actual en Argentina (UTC-3)."""
    return datetime.now(timezone.utc) - timedelta(hours=3)

# ── WhatsApp helpers ── EXACTAMENTE IGUAL AL CODIGO QUE FUNCIONA ──────────────
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

# ── MÓDULO GASTOS ── EXACTAMENTE IGUAL AL CODIGO QUE FUNCIONA ─────────────────
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

# ── MÓDULO PLANTAS (NUEVO) ────────────────────────────────────────────────────
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

# ── MÓDULO EVENTOS (NUEVO) ────────────────────────────────────────────────────
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
    """Obtiene un access token fresco usando el refresh token."""
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
    """Busca un evento en Google Calendar y lo edita."""
    access_token = await get_gcal_access_token()
    if not access_token:
        return False, "Calendar no configurado"

    # Claude extrae qué evento editar y qué cambiar
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

    # Buscar el evento en Google Calendar
    async with httpx.AsyncClient() as http:
        # Buscar en los próximos 30 días y últimos 7 días
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

        # Tomar el primer resultado
        event = events[0]
        event_id = event["id"]
        event_name = event.get("summary", "Evento")

        # Aplicar cambios
        if edit_data.get("new_title"):
            event["summary"] = edit_data["new_title"]
        if edit_data.get("location"):
            event["location"] = edit_data["location"]
        if edit_data.get("description"):
            event["description"] = edit_data["description"]
        if edit_data.get("new_date") or edit_data.get("new_time"):
            # Actualizar fecha/hora si se especifica
            if "dateTime" in event.get("start", {}):
                old_dt = event["start"]["dateTime"][:16]  # YYYY-MM-DDTHH:MM
                old_date = old_dt[:10]
                old_time = old_dt[11:16]
                new_date = edit_data.get("new_date") or old_date
                new_time = edit_data.get("new_time") or old_time
                event["start"] = {"dateTime": f"{new_date}T{new_time}:00", "timeZone": "America/Argentina/Buenos_Aires"}
                # Mantener duración original
                if "dateTime" in event.get("end", {}):
                    end_dt = datetime.strptime(event["end"]["dateTime"][:16], "%Y-%m-%dT%H:%M")
                    start_dt = datetime.strptime(f"{new_date}T{new_time}", "%Y-%m-%dT%H:%M")
                    dur = end_dt - datetime.strptime(old_dt, "%Y-%m-%dT%H:%M")
                    new_end = start_dt + dur
                    event["end"] = {"dateTime": new_end.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}

        # Guardar cambios
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
    emoji = data.get("emoji", "\U0001f4c5")
    hora = f" a las {data['time']}" if data.get("time") else ""
    lines = [f"{emoji} *{data['summary']}*", f"Fecha: {data['date']}{hora}"]
    if data.get("location"):
        lines.append(f"\U0001f4cd {data['location']}")
    if data.get("description"):
        lines.append(f"Nota: {data['description']}")
    lines.append("\n\u2705 Agregado a Google Calendar" if guardado else "\n\u26a0\ufe0f Anota esto manualmente \u2014 Calendar no configurado aun")
    return "\n".join(lines)

# ── CLASIFICADOR (NUEVO) ──────────────────────────────────────────────────────
async def classify(text: str, has_image: bool) -> str:
    if has_image and not text.strip():
        return "GASTO"
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=10,
        system="""Responde SOLO una palabra: GASTO, PLANTA, EVENTO o EDITAR_EVENTO.
GASTO: pago/compra/ingreso/monto/precio/factura.
PLANTA: planta sin mencionar precio.
EDITAR_EVENTO: editar/modificar/agregar ubicacion o info a un evento existente que ya existe en el calendario.
EVENTO: crear un evento nuevo — turno/reunion/cumple/cita/fecha.""",
        messages=[{"role": "user", "content": text}]
    )
    r = response.content[0].text.strip().upper()
    if "EDITAR_EVENTO" in r: return "EDITAR_EVENTO"
    if "PLANTA" in r: return "PLANTA"
    if "EVENTO" in r: return "EVENTO"
    return "GASTO"

# ── Webhook ── ESTRUCTURA EXACTAMENTE IGUAL AL CODIGO QUE FUNCIONA ────────────
@app.get("/webhook")
async def verify_webhook(request: Request):
    params = dict(request.query_params)
    verify_token = os.environ.get("WHATSAPP_VERIFY_TOKEN", "finanzas_bot_token")
    if params.get("hub.verify_token") == verify_token:
        return int(params.get("hub.challenge", 0))
    return {"error": "Verification failed"}

@app.post("/webhook")
async def webhook(request: Request):
    body = await request.json()

    try:
        entry = body["entry"][0]
        changes = entry["changes"][0]["value"]
        messages = changes.get("messages")
        if not messages:
            return {"ok": True}

        message = messages[0]
        from_number = "54298154894334"  # fallback que funciona
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
        else:
            return {"ok": True}

        if text.strip().lower() in ["/start", "hola", "help", "ayuda"]:
            await send_message(from_number,
                "\U0001f44b *Tu asistente personal*\n\n"
                "\U0001f4b8 *Gastos:* _\"Verduleria 3500\"_\n"
                "\U0001f33f *Plantas:* _\"Me compre un potus\"_\n"
                "\U0001f4c5 *Eventos:* _\"Manana a las 10 turno medico\"_\n"
                "\U0001f4f8 *Fotos:* manda cualquier factura\n\n"
                "Todo se guarda automaticamente \U0001f4aa"
            )
            return {"ok": True}

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

    except json.JSONDecodeError:
        pass
    except Exception as e:
        try:
            await send_message(from_number, f"\u274c Error: {str(e)[:200]}")
        except Exception:
            pass

    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "ok", "bot": "asistente-personal"}
