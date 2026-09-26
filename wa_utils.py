import httpx
from state import WA_API, WA_TOKEN, record_message_text, registrar_enviado, add_to_history


async def send_message(to: str, text: str) -> bool:
    async with httpx.AsyncClient() as http:
        r = await http.post(WA_API, headers={
            "Authorization": f"Bearer {WA_TOKEN}",
            "Content-Type": "application/json"
        }, json={
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": text}
        })
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
                registrar_enviado(mid, text)
        except Exception:
            pass
        # Todo lo que Knot dice queda en la conversación, también lo que manda solo
        # (recordatorios, avisos, respuestas a preguntas pendientes). Si no, el agente
        # contestaba "no tengo registro" sobre un recordatorio que él mismo mandó.
        if not text.startswith("⏳"):
            add_to_history(to, "assistant", text)
        return True


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
        r = await http.post(WA_API, headers={
            "Authorization": f"Bearer {WA_TOKEN}",
            "Content-Type": "application/json"
        }, json=payload)
        try:
            mid = (r.json().get("messages") or [{}])[0].get("id")
            if mid:
                record_message_text(mid, body)
        except Exception:
            pass
        if r.status_code == 200:
            opciones = " | ".join(b["title"] for b in buttons[:3])
            add_to_history(to, "assistant", f"{body}\n[opciones: {opciones}]")


async def send_reaction(to: str, message_id: str, emoji: str):
    try:
        async with httpx.AsyncClient() as http:
            await http.post(WA_API, headers={
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
