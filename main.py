import os
import json
import base64
import httpx
from datetime import date, datetime, timedelta
from fastapi import FastAPI, Request
from anthropic import Anthropic

app = FastAPI()

anthropic      = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
NOTION_TOKEN   = os.environ["NOTION_TOKEN"]
NOTION_DB_ID   = os.environ["NOTION_DATABASE_ID"]
PLANTS_DB_ID   = os.environ.get("NOTION_PLANTS_DB_ID", "39d22615-0106-43f8-9f01-2632734c38da")
WA_TOKEN       = os.environ["WHATSAPP_TOKEN"]
WA_PHONE_ID    = os.environ["WHATSAPP_PHONE_ID"]
WA_API         = f"https://graph.facebook.com/v22.0/{WA_PHONE_ID}/messages"

# ── WhatsApp helpers ──────────────────────────────────────────────────────────
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

# ── CLASIFICADOR ──────────────────────────────────────────────────────────────
CLASSIFIER_PROMPT = """Analiza el mensaje y determina de qué tipo es.
Responde UNICAMENTE con una de estas palabras:
- GASTO  → pago, compra, gasto, ingreso, factura, cobro, monto, precio
- PLANTA → planta, flor, arbol, maceta, semilla, cactus, suculenta, helecho, potus, monstera, etc. SIN mencionar precio
- EVENTO → evento, turno, reunion, cita, cumpleanos, recordatorio, algo que ocurre en fecha/hora
- IMAGEN → solo imagen sin texto que aclare el tipo

Si menciona una planta Y cuanto pago → GASTO"""

async def classify_message(text: str, has_image: bool) -> str:
    if has_image and not text.strip():
        return "IMAGEN"
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=10,
        system=CLASSIFIER_PROMPT,
        messages=[{"role": "user", "content": f"Mensaje: {text}"}]
    )
    result = response.content[0].text.strip().upper()
    for tipo in ["GASTO", "PLANTA", "EVENTO", "IMAGEN"]:
        if tipo in result:
            return tipo
    return "GASTO"

# ── MÓDULO 1: GASTOS ──────────────────────────────────────────────────────────
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
Ejemplos:
- Verdura/feria/verduleria -> \U0001f96c
- Supermercado general/almacen -> \U0001f6d2
- Nafta/combustible -> \u26fd
- Repuesto/mecanico/taller -> \U0001f527
- Birra/cerveza -> \U0001f37a
- Restaurant/pizza/sushi -> \U0001f37d
- Farmacia/medicamento -> \U0001f48a
- Psicologo/salud mental -> \U0001f9e0
- Transporte/uber/taxi -> \U0001f697
- Ropa/zapatillas -> \U0001f6cd
- Planta/maceta -> \U0001f33f
- Viaje/avion -> \u2708
- Luz/gas/agua/servicio -> \U0001f4c4
- Alquiler/expensas -> \U0001f3e0
- Sueldo/ingreso -> \U0001f4b0
- Salida nocturna -> \U0001f389
- Streaming/ocio -> \U0001f3ae
- Vianda -> \U0001f961
- Default -> \U0001f4b8"""

def build_user_prompt(text: str, exchange_rate: float) -> str:
    today = date.today().isoformat()
    ingreso = "\u2192INGRESO\u2190"
    egreso = "\u2190 EGRESO \u2192"
    return f"""Tasa de cambio dolar blue hoy: ${exchange_rate:,.0f} ARS por USD.
Fecha de hoy: {today}

Extrae la informacion y responde con este JSON:
{{
  "name": "descripcion corta del movimiento",
  "in_out": "{ingreso}" o "{egreso}",
  "value_ars": numero,
  "categoria": ["categoria1"] o ["categoria1", "categoria2"],
  "metodo": "Payment",
  "date": "YYYY-MM-DD",
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
        return False, "No se pudo interpretar el monto o el tipo de movimiento."
    props = {
        "Name":        {"title": [{"text": {"content": data.get("name") or "Sin nombre"}}]},
        "In - Out":    {"select": {"name": data["in_out"]}},
        "Value (ars)": {"number": float(data["value_ars"])},
        "Cambio":      {"number": exchange_rate},
        "Metodo":      {"select": {"name": data.get("metodo", "Payment")}},
    }
    if data.get("categoria"):
        props["Categor\u00eda"] = {"multi_select": [{"name": c} for c in data["categoria"]]}
    if data.get("date"):
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

# ── MÓDULO 2: PLANTAS ─────────────────────────────────────────────────────────
PLANTA_SYSTEM = """Extraé info de una planta y generá recomendaciones de cuidado.
Responde ÚNICAMENTE con JSON válido, sin markdown.
Valores para "luz": Sombra, Indirecta, Directa parcial, Pleno sol
Valores para "riego": Cada 2-3 días, Semanal, Quincenal, Mensual
Valores para "ubicacion": Interior, Exterior, Balcón, Terraza
Valores para "estado": Excelente, Bien, Regular, Necesita atención"""

async def parse_planta(text: str, exchange_rate: float) -> dict:
    today = date.today().isoformat()
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=800,
        system=PLANTA_SYSTEM,
        messages=[{"role": "user", "content": f"""Hoy: {today}. Precio dolar: ${exchange_rate:,.0f}

Mensaje: {text}

Respondé con este JSON:
{{
  "name": "nombre comun de la planta",
  "especie": "nombre cientifico si lo sabes o null",
  "fecha_compra": "YYYY-MM-DD",
  "precio": numero en ARS o null,
  "luz": "Indirecta",
  "riego": "Semanal",
  "ubicacion": "Interior",
  "estado": "Bien",
  "emoji": "emoji que representa esta planta",
  "notas": "2-3 consejos de cuidado especificos y concisos para esta planta"
}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

async def create_planta(data: dict) -> tuple[bool, str]:
    props = {"Name": {"title": [{"text": {"content": data.get("name", "Planta nueva")}}]}}
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

# ── MÓDULO 3: EVENTOS ─────────────────────────────────────────────────────────
EVENTO_SYSTEM = """Extraé info de un evento para Google Calendar.
Responde ÚNICAMENTE con JSON válido, sin markdown."""

async def parse_evento(text: str) -> dict:
    today = date.today().isoformat()
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=400,
        system=EVENTO_SYSTEM,
        messages=[{"role": "user", "content": f"""Hoy es {today}, hora actual: {now}.

Mensaje: {text}

Respondé con este JSON:
{{
  "summary": "titulo del evento",
  "date": "YYYY-MM-DD",
  "time": "HH:MM" o null,
  "duration_minutes": 60,
  "description": "descripcion" o null,
  "emoji": "emoji del tipo de evento"
}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

async def create_evento_gcal(data: dict) -> tuple[bool, str]:
    gcal_token = os.environ.get("GCAL_TOKEN")
    if not gcal_token:
        return False, "sin_token"

    if data.get("time"):
        start = {"dateTime": f"{data['date']}T{data['time']}:00", "timeZone": "America/Argentina/Neuquen"}
        end_dt = datetime.strptime(f"{data['date']}T{data['time']}", "%Y-%m-%dT%H:%M")
        end_dt += timedelta(minutes=data.get("duration_minutes", 60))
        end = {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Neuquen"}
    else:
        start = {"date": data["date"]}
        end = {"date": data["date"]}

    event = {"summary": data.get("summary", "Evento"), "start": start, "end": end}
    if data.get("description"):
        event["description"] = data["description"]

    async with httpx.AsyncClient() as http:
        r = await http.post(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {gcal_token}", "Content-Type": "application/json"},
            json=event
        )
        return (True, "") if r.status_code in [200, 201] else (False, r.text)

def format_evento(data: dict, guardado: bool) -> str:
    emoji = data.get("emoji", "\U0001f4c5")
    hora = f" a las {data['time']}" if data.get("time") else ""
    lines = [
        f"{emoji} *{data['summary']}*",
        f"Fecha: {data['date']}{hora}",
    ]
    if data.get("description"):
        lines.append(f"Nota: {data['description']}")
    if guardado:
        lines.append("\n\u2705 Agregado a Google Calendar")
    else:
        lines.append("\n\u26a0\ufe0f _(Calendar no configurado aun \u2014 guarda esto manualmente)_")
    return "\n".join(lines)

# ── Webhook principal ──────────────────────────────────────────────────────────
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
    from_number = "54298154894334"  # fallback

    try:
        entry = body["entry"][0]
        changes = entry["changes"][0]["value"]
        messages = changes.get("messages")
        if not messages:
            return {"ok": True}

        message = messages[0]
        # Normalizar número argentino
        raw_from = message["from"]
        if raw_from.startswith("549"):
            from_number = "541" + raw_from[3:]
        else:
            from_number = raw_from

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
        else:
            return {"ok": True}

        # Ayuda
        if text.strip().lower() in ["/start", "hola", "help", "ayuda"]:
            await send_message(from_number,
                "\U0001f44b *Tu asistente personal*\n\n"
                "\U0001f4b8 *Gastos e ingresos*\n"
                "_\"Verduleria 3500\"_\n"
                "_\"Cargue nafta 40L\"_\n"
                "_[foto de factura]_\n\n"
                "\U0001f33f *Plantas*\n"
                "_\"Me compre un potus\"_\n\n"
                "\U0001f4c5 *Calendario*\n"
                "_\"Manana a las 10 tengo turno\"_\n"
                "_\"El viernes cumple Tincho\"_\n\n"
                "Todo se guarda automaticamente \U0001f4aa"
            )
            return {"ok": True}

        await send_message(from_number, "\u23f3 Procesando...")

        # Clasificar y rutear
        tipo = await classify_message(text, image_b64 is not None)

        if tipo in ["GASTO", "IMAGEN"]:
            exchange_rate = await get_exchange_rate()
            parsed = await parse_with_claude(text, image_b64, image_type, exchange_rate)
            success, error = await create_notion_entry(parsed, exchange_rate)
            if success:
                await send_message(from_number, format_reply(parsed, exchange_rate))
            elif "No se pudo interpretar" in error:
                await send_message(from_number,
                    "\u274c No entendi el monto.\n\nEjemplos:\n"
                    "\u2022 _\"Verduleria 3500\"_\n"
                    "\u2022 _\"Pague la luz 62000\"_"
                )
            else:
                await send_message(from_number, f"\u274c Error Notion:\n{error[:200]}")

        elif tipo == "PLANTA":
            exchange_rate = await get_exchange_rate()
            parsed = await parse_planta(text, exchange_rate)
            success, error = await create_planta(parsed)
            if success:
                await send_message(from_number, format_planta(parsed))
            else:
                await send_message(from_number, f"\u274c Error guardando planta: {error[:200]}")

        elif tipo == "EVENTO":
            parsed = await parse_evento(text)
            success, error = await create_evento_gcal(parsed)
            await send_message(from_number, format_evento(parsed, success))

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
