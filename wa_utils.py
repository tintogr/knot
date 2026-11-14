import asyncio
import httpx
from state import WA_API, WA_TOKEN, record_message_text, registrar_enviado, add_to_history

# Un solo cliente HTTP para todo lo que se manda a WhatsApp: antes se abría una
# conexión nueva por cada mensaje (más lento, y sin límite de tiempo claro).
_http: httpx.AsyncClient | None = None


def _cliente() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(timeout=20)
    return _http


async def cerrar_cliente():
    global _http
    if _http is not None and not _http.is_closed:
        await _http.aclose()
    _http = None


async def _post(payload: dict) -> httpx.Response | None:
    """Manda a la API de WhatsApp. Si la conexión falla, reintenta una vez; si vuelve a
    fallar devuelve None (no tira una excepción que corte al que estaba respondiendo)."""
    for intento in range(2):
        try:
            return await _cliente().post(WA_API, headers={
                "Authorization": f"Bearer {WA_TOKEN}",
                "Content-Type": "application/json",
            }, json=payload)
        except httpx.HTTPError as e:
            print(f"[whatsapp] fallo de conexión al enviar (intento {intento + 1}): {type(e).__name__}: {e}")
            if intento == 0:
                await asyncio.sleep(1)
    return None


async def send_message(to: str, text: str, descartable: bool = False) -> bool:
    """descartable=True: si WhatsApp no lo entrega (ventana de 24 h), no se reenvía
    después. Para los resúmenes, que solo sirven en su momento."""
    r = await _post({
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text},
    })
    if r is None:
        return False
    if r.status_code != 200:
        # Antes se ignoraba: si WhatsApp rechazaba el envio, nadie se enteraba.
        print(f"[whatsapp] envío rechazado ({r.status_code}): {r.text[:300]}")
        return False
    # Registrar el wamid saliente para poder resolver cuando el usuario
    # responda/cite este mensaje mas adelante, y para reenviarlo si no se entrega.
    try:
        mid = (r.json().get("messages") or [{}])[0].get("id")
        if mid:
            record_message_text(mid, text)
            registrar_enviado(mid, text, descartable)
    except Exception:
        pass
    # Todo lo que Knot dice queda en la conversación, también lo que manda solo
    # (recordatorios, avisos, respuestas a preguntas pendientes). Si no, el agente
    # contestaba "no tengo registro" sobre un recordatorio que él mismo mandó.
    if not text.startswith("⏳"):
        add_to_history(to, "assistant", text)
    return True


async def send_interactive_buttons(to: str, body: str, buttons: list[dict], header: str = None) -> bool:
    """Devuelve True si WhatsApp aceptó el mensaje. Si lo rechaza (por ejemplo, texto de
    más de 1024 caracteres) se manda la pregunta como texto común con las opciones
    escritas: antes la pregunta se perdía en silencio y Knot quedaba esperando una
    respuesta a algo que el usuario nunca vio."""
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
    opciones = " | ".join(b["title"] for b in buttons[:3])
    r = await _post(payload)
    if r is not None and r.status_code == 200:
        try:
            mid = (r.json().get("messages") or [{}])[0].get("id")
            if mid:
                record_message_text(mid, body)
        except Exception:
            pass
        add_to_history(to, "assistant", f"{body}\n[opciones: {opciones}]")
        return True
    if r is not None:
        print(f"[whatsapp] botones rechazados ({r.status_code}): {r.text[:300]}")
    return await send_message(to, f"{body}\n\n_Respondeme: {opciones}_")


async def send_reaction(to: str, message_id: str, emoji: str):
    try:
        await _cliente().post(WA_API, headers={
            "Authorization": f"Bearer {WA_TOKEN}",
            "Content-Type": "application/json"
        }, json={
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "reaction",
            "reaction": {"message_id": message_id, "emoji": emoji}
        })
    except Exception:
        pass


def error_servicio(servicio: str) -> str:
    msgs = {
        "notion":   "No pude conectarme a Notion para guardar/consultar. Intentá en unos minutos.",
        "calendar": "No pude acceder a tu calendario de Google. Intentá en unos minutos.",
        "gmail":    "No pude consultar tu Gmail. Intentá en unos minutos.",
    }
    return msgs.get(servicio.lower(), "Tuve un problema técnico al procesar tu mensaje. Intentá en unos minutos.")
