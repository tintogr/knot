"""Aviso por otro canal cuando WhatsApp no anda.

Si Meta corta el número, vence el token o rechaza los envíos, Knot no tiene cómo
decírselo a Martin por WhatsApp. Antes nadie se enteraba hasta que Martin notaba que
Knot no contestaba. Ahora se avisa por dos caminos que no dependen de WhatsApp:

1. Un evento en Google Calendar, ahora mismo, con alarma: llega como notificación
   al celular. Usa el mismo permiso de Calendar que Knot ya tiene. No lleva [TEMP]:
   si no, el cron lo tomaría como recordatorio y lo mandaría por WhatsApp.
2. Un mail a la misma cuenta de Google, si el permiso de Gmail incluye enviar.
   Si no lo incluye, queda en el log y alcanza con el evento.

Nunca usa WhatsApp (sería un círculo) y nunca tira una excepción: el que llama está
justo manejando un error. Como mucho un aviso cada AVISO_CADA_HORAS horas.
"""
import base64
from datetime import timedelta
from email.mime.text import MIMEText

import httpx

from gcal import get_gcal_access_token
from state import now_argentina

AVISO_CADA_HORAS = 6
_ultimo_aviso: dict = {"cuando": None}

# Errores de WhatsApp que NO son una falla del canal: fuera de la ventana de 24 h
# (131047) se reenvía solo cuando Martin escribe.
CODIGOS_NORMALES = {131047}


def _toca_avisar() -> bool:
    ultimo = _ultimo_aviso["cuando"]
    return not ultimo or now_argentina() - ultimo >= timedelta(hours=AVISO_CADA_HORAS)


async def _evento_en_calendario(http: httpx.AsyncClient, token: str, motivo: str) -> bool:
    ahora = now_argentina()
    evento = {
        "summary": "⚠️ Knot no puede mandarte WhatsApp",
        "description": (f"{motivo}\n\nMientras tanto Knot no te puede escribir. Revisá Render y Meta, "
                        f"o abrí una sesión de Claude Code en la carpeta de Knot.\n[ALERTA KNOT]"),
        "start": {"dateTime": (ahora + timedelta(minutes=1)).isoformat()},
        "end": {"dateTime": (ahora + timedelta(minutes=16)).isoformat()},
        "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 0}]},
    }
    r = await http.post("https://www.googleapis.com/calendar/v3/calendars/primary/events",
                        headers={"Authorization": f"Bearer {token}"}, json=evento)
    if r.status_code != 200:
        print(f"[alerta] no pude crear el evento ({r.status_code}): {r.text[:200]}")
        return False
    return True


async def _mail_a_si_mismo(http: httpx.AsyncClient, token: str, motivo: str) -> bool:
    cab = {"Authorization": f"Bearer {token}"}
    perfil = await http.get("https://gmail.googleapis.com/gmail/v1/users/me/profile", headers=cab)
    if perfil.status_code != 200:
        print(f"[alerta] sin acceso al perfil de Gmail ({perfil.status_code})")
        return False
    direccion = perfil.json().get("emailAddress")
    if not direccion:
        return False
    msg = MIMEText(f"{motivo}\n\nMientras tanto Knot no te puede escribir por WhatsApp.", "plain", "utf-8")
    msg["To"] = direccion
    msg["From"] = direccion
    msg["Subject"] = "⚠️ Knot no puede mandarte WhatsApp"
    crudo = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    r = await http.post("https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
                        headers=cab, json={"raw": crudo})
    if r.status_code != 200:
        print(f"[alerta] no pude mandar el mail ({r.status_code}): el permiso de Gmail no incluye enviar")
        return False
    return True


async def avisar_whatsapp_caido(motivo: str) -> bool:
    """Avisa que WhatsApp falla. Devuelve True si al menos un canal funcionó."""
    try:
        if not _toca_avisar():
            return False
        token = await get_gcal_access_token()
        if not token:
            print(f"[alerta] WhatsApp falla y no hay token de Google para avisar: {motivo}")
            return False
        async with httpx.AsyncClient(timeout=15) as http:
            en_calendario = await _evento_en_calendario(http, token, motivo)
            por_mail = await _mail_a_si_mismo(http, token, motivo)
        if en_calendario or por_mail:
            _ultimo_aviso["cuando"] = now_argentina()
            print(f"[alerta] avisé por otro canal (calendario={en_calendario}, mail={por_mail}): {motivo}")
            return True
        return False
    except Exception as e:
        print(f"[alerta] no pude avisar por otro canal: {type(e).__name__}: {e}")
        return False
