import os
import json
import base64
import httpx
from datetime import date
from fastapi import FastAPI, Request
from anthropic import Anthropic

app = FastAPI()

# ── Clientes y config ──────────────────────────────────────────────────────────
anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
NOTION_TOKEN     = os.environ["NOTION_TOKEN"]
NOTION_DB_ID     = os.environ["NOTION_DATABASE_ID"]
TELEGRAM_API     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# ── Tasa de cambio (dólar blue Argentina) ─────────────────────────────────────
async def get_exchange_rate() -> float:
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            r = await http.get("https://dolarapi.com/v1/dolares/blue")
            return float(r.json()["venta"])
    except Exception:
        # Fallback: dólar oficial si falla el blue
        try:
            async with httpx.AsyncClient(timeout=5) as http:
                r = await http.get("https://dolarapi.com/v1/dolares/oficial")
                return float(r.json()["venta"])
        except Exception:
            return 1000.0  # valor de emergencia

# ── Telegram helpers ──────────────────────────────────────────────────────────
async def send_message(chat_id: int, text: str):
    async with httpx.AsyncClient() as http:
        await http.post(f"{TELEGRAM_API}/sendMessage", json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown"
        })

async def get_photo_base64(file_id: str) -> tuple[str, str]:
    """Descarga una foto de Telegram y la devuelve en base64."""
    async with httpx.AsyncClient() as http:
        r = await http.get(f"{TELEGRAM_API}/getFile", params={"file_id": file_id})
        file_path = r.json()["result"]["file_path"]
        img_r = await http.get(f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}")
        return base64.b64encode(img_r.content).decode(), "image/jpeg"

# ── Claude: parseo inteligente ─────────────────────────────────────────────────
SYSTEM_PROMPT = """Sos un asistente que extrae datos financieros de mensajes de texto o imágenes 
(fotos de facturas, tickets, recibos, planillas) para cargarlos en una base de datos Notion.

Respondé SIEMPRE y ÚNICAMENTE con un JSON válido, sin markdown, sin texto adicional.
Si algún campo no aplica, usá null.

Categorías disponibles para "tipo":
ALPATACO, OPERA, LBL, Sueldo, Fijo, Comida, Salud, Salida, Servicios, 
Supermercado, Quartieri, Birra, Ocio, Salud Mental, Compras, Viajes, 
Depto, Transporte, Gula, Ocio malo, Vianda, Plantas

Valores posibles para "type": Payment, Suscription, Purchasement
Valores posibles para "income_outcome": "→INGRESO←" o "← EGRESO →"

Clientes conocidos: Juan Martin, Depto, Work, LBL, Opera, Tinto, Santi Vales, 
Jorge, Barbara, Vanguardia, Alejo, Dinamo, Paula Diaz, Labti, PlanA, JGA"""

def build_user_prompt(text: str, exchange_rate: float) -> str:
    today = date.today().isoformat()
    return f"""Tasa de cambio dólar blue hoy: ${exchange_rate:,.0f} ARS por USD.
Fecha de hoy: {today}

Extraé la información del siguiente mensaje/imagen y respondé con este JSON exacto:
{{
  "name": "descripción corta del movimiento",
  "income_outcome": "→INGRESO←" o "← EGRESO →",
  "value_ars": número (monto en pesos, sin símbolos),
  "tipo": ["categoria"],
  "type": "Payment",
  "date": "YYYY-MM-DD",
  "litros_nafta": número o null,
  "precio_litro_ars": número o null,
  "km": número o null,
  "consumo_kwh": número o null,
  "notas": "info extra relevante de la factura" o null,
  "client": ["nombre"] o []
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
    # Limpiar por si Claude pone markdown igual
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

# ── Notion: crear entrada ──────────────────────────────────────────────────────
async def create_notion_entry(data: dict, exchange_rate: float) -> tuple[bool, str]:
    props = {
        "Name": {"title": [{"text": {"content": data["name"]}}]},
        "Income / Outcome": {"select": {"name": data["income_outcome"]}},
        "Value (ars)": {"number": float(data["value_ars"])},
        "EXCHANGE": {"number": exchange_rate},
        "Type": {"select": {"name": data.get("type", "Payment")}},
        "Status": {"status": {"name": "Done"}},
    }

    if data.get("tipo"):
        props["Tipo"] = {"multi_select": [{"name": t} for t in data["tipo"]]}

    if data.get("date"):
        props["Date"] = {"date": {"start": data["date"]}}

    if data.get("client"):
        props["Client"] = {"multi_select": [{"name": c} for c in data["client"]]}

    if data.get("litros_nafta") is not None:
        props["Litros de nafta"] = {"number": float(data["litros_nafta"])}

    if data.get("precio_litro_ars") is not None:
        props["Precio por litro (ARS)"] = {"number": float(data["precio_litro_ars"])}

    if data.get("km") is not None:
        props["km a la hora de cargar"] = {"number": float(data["km"])}

    if data.get("consumo_kwh") is not None:
        props["Consumo (kWh)"] = {"number": float(data["consumo_kwh"])}

    if data.get("notas"):
        props["Notas adicionales"] = {"rich_text": [{"text": {"content": data["notas"]}}]}

    async with httpx.AsyncClient() as http:
        r = await http.post(
            "https://api.notion.com/v1/pages",
            headers={
                "Authorization": f"Bearer {NOTION_TOKEN}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json"
            },
            json={"parent": {"database_id": NOTION_DB_ID}, "properties": props}
        )
        if r.status_code == 200:
            return True, ""
        return False, r.text

# ── Formatear respuesta para Telegram ─────────────────────────────────────────
def format_reply(data: dict, exchange_rate: float) -> str:
    is_expense = "EGRESO" in data["income_outcome"]
    emoji = "💸" if is_expense else "💰"
    direction = "Egreso" if is_expense else "Ingreso"
    usd = data["value_ars"] / exchange_rate

    lines = [
        f"{emoji} *{data['name']}*",
        f"{direction}: *${data['value_ars']:,.0f} ARS* (≈ USD {usd:.2f})",
        f"Categoría: {', '.join(data.get('tipo', []) or ['—'])}",
        f"Tipo: {data.get('type', 'Payment')}",
        f"Cambio: ${exchange_rate:,.0f}/USD",
    ]

    extras = []
    if data.get("litros_nafta"):
        extras.append(f"⛽ {data['litros_nafta']}L")
    if data.get("precio_litro_ars"):
        extras.append(f"${data['precio_litro_ars']:,.0f}/L")
    if data.get("km"):
        extras.append(f"km {data['km']:,.0f}")
    if data.get("consumo_kwh"):
        extras.append(f"⚡ {data['consumo_kwh']} kWh")
    if extras:
        lines.append(" · ".join(extras))

    lines.append("\n✅ _Guardado en Notion_")
    return "\n".join(lines)

# ── Webhook principal ──────────────────────────────────────────────────────────
@app.post("/webhook")
async def webhook(request: Request):
    body = await request.json()
    message = body.get("message", {})
    chat_id = message.get("chat", {}).get("id")

    if not chat_id:
        return {"ok": True}

    text   = message.get("text", "") or message.get("caption", "") or ""
    photos = message.get("photo")

    # Ignorar comandos de sistema (ej: /start)
    if text.startswith("/"):
        await send_message(chat_id,
            "👋 *Bot de finanzas activo*\n\n"
            "Mandame:\n"
            "• Un mensaje de texto: _\"Verdulería 4000\"_\n"
            "• Una foto de factura o ticket\n"
            "• Carga de nafta: _\"Cargué 35L a $1400, km 48500\"_\n\n"
            "Todo se guarda automáticamente en tu Notion con el dólar del día 💪"
        )
        return {"ok": True}

    if not text and not photos:
        return {"ok": True}

    # Indicador de procesamiento
    await send_message(chat_id, "⏳ _Procesando..._")

    try:
        exchange_rate = await get_exchange_rate()
        image_b64 = image_type = None

        if photos:
            # La última foto es la de mayor resolución
            file_id = photos[-1]["file_id"]
            image_b64, image_type = await get_photo_base64(file_id)

        parsed   = await parse_with_claude(text, image_b64, image_type, exchange_rate)
        success, error_detail  = await create_notion_entry(parsed, exchange_rate)

        if success:
            await send_message(chat_id, format_reply(parsed, exchange_rate))
        else:
            await send_message(chat_id, f"❌ Error Notion:\n`{error_detail[:500]}`")

    except json.JSONDecodeError:
        await send_message(chat_id, "❌ No pude interpretar el mensaje. ¿Podés ser más específico?")
    except Exception as e:
        await send_message(chat_id, f"❌ Error inesperado: {str(e)}")

    return {"ok": True}

@app.get("/")
async def health():
    return {"status": "ok", "bot": "finanzas-bot"}
