import os, json, base64, time, httpx
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request, BackgroundTasks
from anthropic import Anthropic

app = FastAPI()
anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ── CREDENCIALES ──
NOTION_TOKEN   = os.environ["NOTION_TOKEN"]
NOTION_DB_ID   = os.environ["NOTION_DATABASE_ID"]
PLANTS_DB_ID   = os.environ.get("NOTION_PLANTS_DB_ID", "39d22615-0106-43f8-9f01-2632734c38da")
SHOPPING_DB_ID = os.environ.get("NOTION_SHOPPING_DB_ID", "cb85fdf75d684f61bafea20b5eeb653f")
CONFIG_DB_ID   = os.environ.get("NOTION_CONFIG_DB_ID", "2f81017d-a20c-426a-aada-88fcf0743338")
WA_TOKEN       = os.environ["WHATSAPP_TOKEN"]
WA_PHONE_ID    = os.environ["WHATSAPP_PHONE_ID"]
WA_API         = f"https://graph.facebook.com/v22.0/{WA_PHONE_ID}/messages"

# ── CONFIG DINAMICA ──
user_configs = {}

async def get_user_config(wa_number: str) -> dict:
    if wa_number in user_configs: return user_configs[wa_number]
    config = {"name": "Martín", "city": "Neuquén", "lat": -38.95, "lon": -68.06, "tz": "America/Argentina/Buenos_Aires"}
    try:
        async with httpx.AsyncClient() as http:
            r = await http.post(f"https://api.notion.com/v1/databases/{CONFIG_DB_ID.replace('-','')}/query",
                headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"},
                json={"filter": {"property": "WA Number", "rich_text": {"equals": wa_number}}, "page_size": 1})
            if r.status_code == 200 and r.json().get("results"):
                props = r.json()["results"][0]["properties"]
                if "City" in props and props["City"]["rich_text"]: config["city"] = props["City"]["rich_text"][0]["plain_text"]
                if "Latitude" in props and props["Latitude"]["number"] is not None: config["lat"] = props["Latitude"]["number"]
                if "Longitude" in props and props["Longitude"]["number"] is not None: config["lon"] = props["Longitude"]["number"]
    except Exception: pass
    user_configs[wa_number] = config
    return config

def now_local(tz_str: str) -> datetime: return datetime.now(timezone.utc) - timedelta(hours=3)

# ── CLAUDE WRAPPER ANTI-CRASH ──
def claude_create(**kwargs):
    for attempt in range(4):
        try: return anthropic.messages.create(**kwargs)
        except Exception as e:
            if "529" in str(e) or "overloaded" in str(e).lower():
                time.sleep(2 ** (attempt + 1))
                continue
            raise 
    try:
        kwargs["model"] = "claude-3-haiku-20240307"
        return anthropic.messages.create(**kwargs)
    except Exception: raise Exception("Estoy con los servidores colapsados, bancame un ratito 🙏")

# ── HELPERS ──
async def send_whatsapp(to: str, text: str):
    async with httpx.AsyncClient() as http:
        await http.post(WA_API, headers={"Authorization": f"Bearer {WA_TOKEN}", "Content-Type": "application/json"},
            json={"messaging_product": "whatsapp", "to": to, "type": "text", "text": {"body": text}})

async def transcribe_audio(media_id: str) -> str:
    groq_key = os.environ.get("GROQ_API_KEY")
    if not groq_key: return ""
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.get(f"https://graph.facebook.com/v22.0/{media_id}", headers={"Authorization": f"Bearer {WA_TOKEN}"})
        if r.status_code != 200: return ""
        audio_r = await http.get(r.json()["url"], headers={"Authorization": f"Bearer {WA_TOKEN}"})
        resp = await http.post("https://api.groq.com/openai/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {groq_key}"}, files={"file": ("audio.ogg", audio_r.content, "audio/ogg")},
            data={"model": "whisper-large-v3", "language": "es"})
        return resp.json().get("text", "").strip() if resp.status_code == 200 else ""

async def get_gcal_access_token() -> str:
    r_token, c_id, c_secret = os.environ.get("GCAL_REFRESH_TOKEN"), os.environ.get("GCAL_CLIENT_ID"), os.environ.get("GCAL_CLIENT_SECRET")
    if not all([r_token, c_id, c_secret]): return ""
    async with httpx.AsyncClient() as http:
        r = await http.post("https://oauth2.googleapis.com/token", data={"grant_type": "refresh_token", "refresh_token": r_token, "client_id": c_id, "client_secret": c_secret})
        return r.json().get("access_token", "") if r.status_code == 200 else ""

# ── HERRAMIENTAS (TOOLS) ──
MATRICS_TOOLS = [
    {"name": "registrar_gasto", "description": "Registra un gasto o ingreso.", "input_schema": {"type": "object", "properties": {"concepto": {"type": "string"}, "monto": {"type": "number"}, "tipo": {"type": "string", "enum": ["\u2190 EGRESO \u2192", "\u2192INGRESO\u2190"]}, "categorias": {"type": "array", "items": {"type": "string"}}}, "required": ["concepto", "monto", "tipo"]}},
    {"name": "gestionar_shopping", "description": "Gestiona lista de compras.", "input_schema": {"type": "object", "properties": {"accion": {"type": "string", "enum": ["agregar", "eliminar", "leer"]}, "items": {"type": "array", "items": {"type": "string"}}}, "required": ["accion"]}},
    {"name": "consultar_clima", "description": "Clima y pronóstico.", "input_schema": {"type": "object", "properties": {"dias_adelante": {"type": "integer"}}}},
    {"name": "gestionar_evento", "description": "Crea/elimina eventos en Google Calendar.", "input_schema": {"type": "object", "properties": {"accion": {"type": "string", "enum": ["crear", "eliminar"]}, "titulo": {"type": "string"}, "fecha": {"type": "string", "description": "YYYY-MM-DD"}, "hora": {"type": "string", "description": "HH:MM (vacío=todo el día)"}}, "required": ["accion", "titulo", "fecha"]}},
    {"name": "registrar_planta", "description": "Guarda planta.", "input_schema": {"type": "object", "properties": {"nombre": {"type": "string"}, "luz": {"type": "string"}, "riego": {"type": "string"}}, "required": ["nombre"]}}
]

async def execute_tool(tool_name: str, args: dict, user_config: dict) -> str:
    headers_notion = {"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"}
    
    if tool_name == "registrar_gasto":
        props = {"Name": {"title": [{"text": {"content": args["concepto"]}}]}, "In - Out": {"select": {"name": args["tipo"]}}, "Value (ars)": {"number": float(args["monto"])}, "Exchange Rate": {"number": 1000.0}, "Date": {"date": {"start": now_local(user_config["tz"]).strftime("%Y-%m-%dT%H:%M:00-03:00")}}}
        if args.get("categorias"): props["Category"] = {"multi_select": [{"name": c} for c in args["categorias"]]}
        async with httpx.AsyncClient() as http:
            r = await http.post("https://api.notion.com/v1/pages", headers=headers_notion, json={"parent": {"database_id": NOTION_DB_ID.replace("-", "")}, "icon": {"type": "emoji", "emoji": "💸"}, "properties": props})
            return f"ÉXITO: Registrado en Finanzas." if r.status_code == 200 else f"ERROR: {r.text}"

    elif tool_name == "gestionar_shopping":
        accion = args["accion"]
        async with httpx.AsyncClient() as http:
            if accion == "leer":
                r = await http.post(f"https://api.notion.com/v1/databases/{SHOPPING_DB_ID.replace('-','')}/query", headers=headers_notion, json={"filter": {"property": "Stock", "checkbox": {"equals": False}}})
                faltantes = [i["properties"]["Name"]["title"][0]["plain_text"] for i in r.json().get("results", []) if i["properties"]["Name"]["title"]]
                return f"ÉXITO: Faltan {', '.join(faltantes)}" if faltantes else "ÉXITO: Lista vacía."
            elif accion == "agregar":
                for item in args.get("items", []):
                    await http.post("https://api.notion.com/v1/pages", headers=headers_notion, json={"parent": {"database_id": SHOPPING_DB_ID.replace("-", "")}, "properties": {"Name": {"title": [{"text": {"content": item.capitalize()}}]}, "Stock": {"checkbox": False}}})
                return "ÉXITO: Agregados a la lista."

    elif tool_name == "consultar_clima":
        dias = args.get("dias_adelante", 0)
        async with httpx.AsyncClient() as http:
            r = await http.get(f"https://api.open-meteo.com/v1/forecast?latitude={user_config['lat']}&longitude={user_config['lon']}&current=temperature_2m,apparent_temperature,precipitation,windspeed_10m&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=auto")
            d = r.json()
            if dias == 0: return f"HOY: {d['current']['temperature_2m']}°C, Lluvia: {d['current']['precipitation']}mm."
            return f"PRONOSTICO DIA {dias}: Max {d['daily']['temperature_2m_max'][dias]}°C, Lluvia {d['daily']['precipitation_sum'][dias]}mm."

    elif tool_name == "gestionar_evento":
        token = await get_gcal_access_token()
        if not token: return "ERROR: Sin token de Calendar."
        if args["accion"] == "crear":
            start = {"dateTime": f"{args['fecha']}T{args['hora']}:00", "timeZone": user_config["tz"]} if args.get("hora") else {"date": args["fecha"]}
            end = {"dateTime": (datetime.strptime(f"{args['fecha']}T{args['hora']}", "%Y-%m-%dT%H:%M") + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:00"), "timeZone": user_config["tz"]} if args.get("hora") else {"date": args["fecha"]}
            async with httpx.AsyncClient() as http:
                r = await http.post("https://www.googleapis.com/calendar/v3/calendars/primary/events", headers={"Authorization": f"Bearer {token}"}, json={"summary": args["titulo"], "start": start, "end": end})
                return "ÉXITO: Agendado en Calendar." if r.status_code in [200, 201] else f"ERROR: {r.text}"

    elif tool_name == "registrar_planta":
        props = {"Name": {"title": [{"text": {"content": args["nombre"]}}]}, "Light": {"select": {"name": args.get("luz", "Indirecta")}}, "Watering": {"select": {"name": args.get("riego", "Semanal")}}}
        async with httpx.AsyncClient() as http:
            r = await http.post("https://api.notion.com/v1/pages", headers=headers_notion, json={"parent": {"database_id": PLANTS_DB_ID.replace("-", "")}, "icon": {"type": "emoji", "emoji": "🌿"}, "properties": props})
            return "ÉXITO: Planta guardada." if r.status_code == 200 else "ERROR."

    return "Herramienta desconocida."

# ── AGENTE MATRICS ──
chat_history = {}
MAX_HISTORY = 8

async def handle_agent_chat(wa_number: str, text: str) -> str:
    user_config = await get_user_config(wa_number)
    now = now_local(user_config["tz"])
    if wa_number not in chat_history: chat_history[wa_number] = []
    
    chat_history[wa_number].append({"role": "user", "content": text})
    if len(chat_history[wa_number]) > MAX_HISTORY * 2: chat_history[wa_number] = chat_history[wa_number][-(MAX_HISTORY * 2):]
    
    sys_prompt = f"""Sos Matrics, asistente personal. Hablás en español rioplatense, sos directo y natural. Usuario: {user_config['name']}. Ciudad: {user_config['city']}. Fecha: {now.strftime("%A %d/%m/%Y %H:%M")}. USÁ TUS HERRAMIENTAS SI TE PIDEN ACCIONES."""

    try:
        resp = claude_create(model="claude-3-5-sonnet-20241022", max_tokens=800, system=sys_prompt, messages=chat_history[wa_number], tools=MATRICS_TOOLS)
        chat_history[wa_number].append({"role": "assistant", "content": resp.content})

        if resp.stop_reason == "tool_use":
            tool_res = []
            for block in resp.content:
                if block.type == "tool_use":
                    r_str = await execute_tool(block.name, block.input, user_config)
                    tool_res.append({"type": "tool_result", "tool_use_id": block.id, "content": r_str})
            chat_history[wa_number].append({"role": "user", "content": tool_res})
            
            final_resp = claude_create(model="claude-3-5-sonnet-20241022", max_tokens=800, system=sys_prompt, messages=chat_history[wa_number], tools=MATRICS_TOOLS)
            reply = final_resp.content[0].text
            chat_history[wa_number].append({"role": "assistant", "content": reply})
            return reply
        else:
            return resp.content[0].text
    except Exception as e: return str(e)

# ── FASTAPI ──
@app.get("/webhook")
async def verify(request: Request):
    p = dict(request.query_params)
    if p.get("hub.verify_token") == os.environ.get("WHATSAPP_VERIFY_TOKEN", "finanzas_bot_token"): return int(p.get("hub.challenge", 0))
    return {"error": "Fail"}

async def process_msg(msg: dict):
    from_num = msg.get("from")
    m_type = msg.get("type")
    if not m_type: return
    text = ""
    await send_whatsapp(from_num, "⏳ Procesando...")
    
    if m_type == "text": text = msg["text"]["body"]
    elif m_type == "audio":
        text = await transcribe_audio(msg["audio"]["id"])
        if not text: await send_whatsapp(from_num, "❌ Error de audio.")
    elif m_type == "image":
        text = msg["image"].get("caption", "(Imagen adjunta)")
        
    if text: await send_whatsapp(from_num, await handle_agent_chat(from_num, text))

@app.post("/webhook")
async def webhook(request: Request, bg: BackgroundTasks):
    body = await request.json()
    try:
        msgs = body["entry"][0]["changes"][0]["value"].get("messages")
        if msgs: bg.add_task(process_msg, msgs[0])
    except Exception: pass
    return {"ok": True}

@app.get("/")
async def health(): return {"status": "ok", "bot": "matrics_v2"}
