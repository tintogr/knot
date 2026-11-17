import asyncio
import os
import httpx
from state import WA_API, WA_TOKEN, WA_PHONE_ID, record_message_text, registrar_enviado, add_to_history

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


# ── Formato del número de destino ─────────────────────────────────────────────
# Un celular argentino se puede escribir de tres maneras (54 298 4894334, 54 9 298 4894334,
# 54 298 15 4894334) y Meta solo acepta la que tiene anotada. Con un número de prueba, si
# no coincide con la lista de destinatarios contesta "(#131030) Recipient phone number not
# in allowed list". Pasó en noviembre 2026 con el número nuevo: la lista lo guardó sin 9 ni
# 15 y Knot escribía con 15. Ante ese error se prueban las otras maneras y se recuerda la
# que funcionó.
_destino_que_anda: dict = {}


def _otras_formas(numero: str) -> list:
    """Las otras maneras de escribir un celular argentino. Vacío si no es argentino."""
    d = "".join(c for c in str(numero or "") if c.isdigit())
    if not d.startswith("54"):
        return []
    resto = d[2:]
    bases = set()
    if resto.startswith("9") and len(resto) == 11:
        bases.add(resto[1:])
    elif len(resto) == 10:
        bases.add(resto)
    elif len(resto) == 12:
        for i in (2, 3, 4):
            if resto[i:i + 2] == "15":
                bases.add(resto[:i] + resto[i + 2:])
    formas = []
    for b in sorted(bases):
        for f in ("54" + b, "549" + b):
            if f != d and f not in formas:
                formas.append(f)
    return formas


def _fuera_de_la_lista(r) -> bool:
    return r is not None and r.status_code == 400 and "131030" in (r.text or "")


async def _post_una_vez(payload: dict) -> httpx.Response | None:
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


async def _post(payload: dict) -> httpx.Response | None:
    """Manda a la API de WhatsApp. Si la conexión falla, reintenta una vez; si vuelve a
    fallar devuelve None (no tira una excepción que corte al que estaba respondiendo).
    Si Meta dice que el destinatario no está en la lista, prueba las otras maneras de
    escribir el número."""
    original = str(payload.get("to") or "")
    if original in _destino_que_anda:
        payload = {**payload, "to": _destino_que_anda[original]}
    r = await _post_una_vez(payload)
    if not _fuera_de_la_lista(r):
        return r
    for forma in _otras_formas(original):
        if forma == payload.get("to"):
            continue
        r2 = await _post_una_vez({**payload, "to": forma})
        if r2 is not None and r2.status_code == 200:
            _destino_que_anda[original] = forma
            print(f"[whatsapp] el destinatario se escribe ...{forma[-4:]} con {len(forma)} dígitos "
                  f"(no {len(original)}): queda anotado.")
            return r2
    return r


# ── Registro del número ───────────────────────────────────────────────────────
# Un número de WhatsApp recién creado queda "Pendiente": Meta acepta los pedidos pero
# contesta "(#133010) Account not registered" y no entrega nada, ni de ida ni de vuelta.
# Pasó en noviembre 2026, cuando Meta borró el número de prueba y hubo que pedir otro.
# Se arregla con un pedido de registro que lleva un PIN de 6 dígitos (queda como el PIN
# de verificación en dos pasos del número). Knot lo hace solo si en Render está
# WHATSAPP_PIN. Con WHATSAPP_WABA_ID además se asegura de que la cuenta le avise a la app
# cuando entra un mensaje.
_GRAPH = "https://graph.facebook.com/v22.0"
_registro: dict = {"intentado": False}


def _cabeceras() -> dict:
    return {"Authorization": f"Bearer {WA_TOKEN}", "Content-Type": "application/json"}


async def estado_del_numero() -> str | None:
    """Estado del número según Meta ("CONNECTED", "PENDING"...), o None si no se pudo leer."""
    try:
        r = await _cliente().get(f"{_GRAPH}/{WA_PHONE_ID}", headers=_cabeceras(),
                                 params={"fields": "display_phone_number,status,code_verification_status"})
    except httpx.HTTPError as e:
        print(f"[whatsapp] no pude consultar el estado del número: {type(e).__name__}: {e}")
        return None
    if r.status_code != 200:
        print(f"[whatsapp] no pude consultar el estado del número ({r.status_code}): {r.text[:200]}")
        return None
    datos = r.json()
    print(f"[whatsapp] número {datos.get('display_phone_number')}: estado {datos.get('status')}")
    return datos.get("status")


async def registrar_numero() -> bool:
    """Registra el número en la API de WhatsApp y conecta la cuenta con la app.
    Devuelve True si Meta aceptó el registro. Se intenta una sola vez por arranque:
    Meta permite 10 registros cada 72 horas."""
    pin = (os.environ.get("WHATSAPP_PIN") or "").strip()
    if not (pin.isdigit() and len(pin) == 6):
        print("[whatsapp] el número no está registrado y falta WHATSAPP_PIN (6 dígitos) en Render "
              "para registrarlo.")
        return False
    if _registro["intentado"]:
        return False
    _registro["intentado"] = True
    ok = False
    try:
        r = await _cliente().post(f"{_GRAPH}/{WA_PHONE_ID}/register", headers=_cabeceras(),
                                  json={"messaging_product": "whatsapp", "pin": pin})
        ok = r.status_code == 200 and bool(r.json().get("success"))
        print(f"[whatsapp] registro del número: {'ACEPTADO' if ok else 'RECHAZADO'} "
              f"({r.status_code}) {'' if ok else r.text[:300]}")
    except httpx.HTTPError as e:
        print(f"[whatsapp] no pude registrar el número: {type(e).__name__}: {e}")
    waba = (os.environ.get("WHATSAPP_WABA_ID") or "").strip()
    if waba.isdigit():
        try:
            r2 = await _cliente().post(f"{_GRAPH}/{waba}/subscribed_apps", headers=_cabeceras())
            print(f"[whatsapp] conexión de la cuenta con la app: "
                  f"{'OK' if r2.status_code == 200 else 'FALLÓ'} ({r2.status_code}) "
                  f"{'' if r2.status_code == 200 else r2.text[:300]}")
        except httpx.HTTPError as e:
            print(f"[whatsapp] no pude conectar la cuenta con la app: {type(e).__name__}: {e}")
    return ok


async def revisar_numero_al_arrancar():
    """Al arrancar: si hay PIN cargado y Meta dice que el número no está conectado, lo
    registra. Si no se puede leer el estado no hace nada (queda el aviso del primer envío)."""
    if not (os.environ.get("WHATSAPP_PIN") or "").strip():
        return
    estado = await estado_del_numero()
    if estado and estado != "CONNECTED":
        await registrar_numero()


def _sin_registrar(r) -> bool:
    return r is not None and r.status_code == 400 and "133010" in (r.text or "")


async def send_message(to: str, text: str, descartable: bool = False) -> bool:
    """descartable=True: si WhatsApp no lo entrega (ventana de 24 h), no se reenvía
    después. Para los resúmenes, que solo sirven en su momento."""
    r = await _post({
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text},
    })
    if _sin_registrar(r) and await registrar_numero():
        # El número acaba de quedar registrado: se reintenta este mismo mensaje.
        r = await _post({"messaging_product": "whatsapp", "to": to, "type": "text", "text": {"body": text}})
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
    except Exception as _err_ignorado:
        print(f"[wa_utils.send_message] ignorado: {type(_err_ignorado).__name__}: {_err_ignorado}")
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
        except Exception as _err_ignorado:
            print(f"[wa_utils.send_interactive_buttons] ignorado: {type(_err_ignorado).__name__}: {_err_ignorado}")
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
    except Exception as _err_ignorado:
        print(f"[wa_utils.send_reaction] ignorado: {type(_err_ignorado).__name__}: {_err_ignorado}")


def error_servicio(servicio: str) -> str:
    msgs = {
        "notion":   "No pude conectarme a Notion para guardar/consultar. Intentá en unos minutos.",
        "calendar": "No pude acceder a tu calendario de Google. Intentá en unos minutos.",
        "gmail":    "No pude consultar tu Gmail. Intentá en unos minutos.",
    }
    return msgs.get(servicio.lower(), "Tuve un problema técnico al procesar tu mensaje. Intentá en unos minutos.")
