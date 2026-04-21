import httpx
from state import WA_API, WA_TOKEN


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
