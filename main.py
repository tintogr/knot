import os
import json
import base64
import httpx
from datetime import date
from fastapi import FastAPI, Request
from anthropic import Anthropic

app = FastAPI()

anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
NOTION_TOKEN   = os.environ["NOTION_TOKEN"]
NOTION_DB_ID   = os.environ["NOTION_DATABASE_ID"]
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
    """Descarga una imagen de WhatsApp y la devuelve en base64."""
    async with httpx.AsyncClient() as http:
        # Obtener URL del media
        r = await http.get(
            f"https://graph.facebook.com/v22.0/{media_id}",
            headers={"Authorization": f"Bearer {WA_TOKEN}"}
        )
        media_url = r.json()["url"]
        mime_type = r.json().get("mime_type", "image/jpeg")

        # Descargar el archivo
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

# ── Claude: parseo inteligente ─────────────────────────────────────────────────
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

async def parse_with_claude(
    text: str = "",
    image_b64: str = None,
    image_type: str = None,
    exchange_rate: float = 1000.0
) -> dict:
    content = []
    if image_b64:
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": image_type, "data": image_b64}
        })
    content.append({"type": "text", "text": build_user_prompt(text, exchange_rate)})

    response = anthropic.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": content}]
    )

    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

# ── Notion: crear entrada ──────────────────────────────────────────────────────
async def create_notion_entry(data: dict, exchange_rate: float) -> tuple[bool, str]:
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
        r = await http.post(
            "https://api.notion.com/v1/pages",
            headers={
                "Authorization": f"Bearer {NOTION_TOKEN}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json"
            },
            json={
                "parent": {"database_id": db_id},
                "icon": {"type": "emoji", "emoji": emoji},
                "properties": props
            }
        )
        if r.status_code == 200:
            return True, ""
        return False, r.text

# ── Formatear respuesta ──────────────────────────────────────────────────────
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

# ── Webhook WhatsApp ──────────────────────────────────────────────────────────
@app.get("/webhook")
async def verify_webhook(request: Request):
    """Verificación del webhook de Meta."""
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
        from_number = message["from"]
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

        # Comando /start
        if text.strip().lower() in ["/start", "hola", "help", "ayuda"]:
            await send_message(from_number,
                "\U0001f44b *Bot de finanzas activo*\n\n"
                "Mand\u00e1me:\n"
                "\u2022 _\"Verduleria 3500\"_\n"
                "\u2022 _\"Cargu\u00e9 nafta 40L\"_\n"
                "\u2022 _\"Sali a comer con Manu, 15000\"_\n"
                "\u2022 Una foto de factura o ticket\n\n"
                "Se guarda en Notion con el d\u00f3lar blue del d\u00eda \U0001f4aa"
            )
            return {"ok": True}

        await send_message(from_number, "\u23f3 Procesando...")

        exchange_rate = await get_exchange_rate()
        parsed = await parse_with_claude(text, image_b64, image_type, exchange_rate)
        success, error_detail = await create_notion_entry(parsed, exchange_rate)

        if success:
            await send_message(from_number, format_reply(parsed, exchange_rate))
        else:
            await send_message(from_number, f"\u274c Error Notion:\n{error_detail[:300]}")

    except json.JSONDecodeError:
        pass
    except Exception as e:
        try:
            if from_number:
                await send_message(from_number, f"\u274c Error: {str(e)[:200]}")
        except Exception:
            pass

    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "ok", "bot": "finanzas-whatsapp"}
