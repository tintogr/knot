import json
import httpx
from datetime import datetime, timedelta

from state import (
    _ds, QueryFilter, DateRange,
    MY_NUMBER, user_prefs, current_location, geo_reminders_cache,
    now_argentina, claude_create, add_to_history, DIAS_SEMANA,
)
from wa_utils import send_message
from gcal import get_gcal_access_token
from config import load_user_config


# ── WMO codes y viento ────────────────────────────────────────────────────────

WMO_CODES = {
    0:  ("Despejado", "☀️"),   1:  ("Mayormente despejado", "🌤️"),
    2:  ("Parcialmente nublado", "⛅"), 3:  ("Nublado", "☁️"),
    45: ("Neblina", "🌫️"),    48: ("Neblina helada", "🌫️"),
    51: ("Llovizna", "🌦️"),   53: ("Llovizna", "🌦️"),   55: ("Llovizna intensa", "🌧️"),
    61: ("Lluvia leve", "🌧️"), 63: ("Lluvia", "🌧️"),     65: ("Lluvia intensa", "🌧️"),
    71: ("Nieve leve", "🌨️"), 73: ("Nieve", "🌨️"),      75: ("Nieve intensa", "🌨️"),
    80: ("Chubascos", "🌦️"),  81: ("Chubascos", "🌦️"),  82: ("Chubascos fuertes", "⛈️"),
    95: ("Tormenta", "⛈️"),   96: ("Tormenta con granizo", "⛈️"), 99: ("Tormenta con granizo", "⛈️"),
}


def wind_description(kmh: float) -> str:
    if kmh < 6:   return "Calma"
    if kmh < 20:  return "Brisa suave"
    if kmh < 39:  return "Brisa moderada"
    if kmh < 62:  return "Viento fuerte"
    if kmh < 89:  return "Viento muy fuerte"
    return "Temporal"


# ── Clima ─────────────────────────────────────────────────────────────────────

async def get_weather(days: int = 2) -> dict | None:
    try:
        lat = current_location.get("lat")
        lon = current_location.get("lon")
        if lat is None or lon is None:
            return None
        async with httpx.AsyncClient(timeout=10) as http:
            r = await http.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat, "longitude": lon,
                    "current": "temperature_2m,apparent_temperature,precipitation,windspeed_10m,weathercode",
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max,weathercode",
                    "timezone": "America/Argentina/Buenos_Aires",
                    "forecast_days": max(days, 7)
                }
            )
            if r.status_code != 200:
                return None
            data = r.json()
            c = data["current"]
            d = data["daily"]
            desc, emoji = WMO_CODES.get(c["weathercode"], ("Variable", "🌡️"))
            viento = round(c["windspeed_10m"])
            desc_manana, emoji_manana = WMO_CODES.get(d["weathercode"][1], ("Variable", "🌡️"))
            viento_manana = round(d["windspeed_10m_max"][1])
            forecast_days = []
            for i in range(min(7, len(d["weathercode"]))):
                fd, fe = WMO_CODES.get(d["weathercode"][i], ("Variable", "🌡️"))
                forecast_days.append({
                    "date": d.get("time", [""] * 7)[i] if "time" in d else "",
                    "max": round(d["temperature_2m_max"][i]),
                    "min": round(d["temperature_2m_min"][i]),
                    "lluvia": d["precipitation_sum"][i],
                    "desc": fd,
                    "emoji": fe,
                })
            return {
                "temp":           round(c["temperature_2m"]),
                "sensacion":      round(c["apparent_temperature"]),
                "lluvia":         c["precipitation"],
                "viento":         viento,
                "desc":           desc,
                "emoji":          emoji,
                "wind_desc":      wind_description(viento),
                "hoy_max":        round(d["temperature_2m_max"][0]),
                "hoy_min":        round(d["temperature_2m_min"][0]),
                "hoy_lluvia":     d["precipitation_sum"][0],
                "hoy_desc":       desc,
                "hoy_emoji":      emoji,
                "manana_max":     round(d["temperature_2m_max"][1]),
                "manana_min":     round(d["temperature_2m_min"][1]),
                "manana_lluvia":  d["precipitation_sum"][1],
                "manana_viento":  viento_manana,
                "manana_desc":    desc_manana,
                "manana_emoji":   emoji_manana,
                "manana_wind_desc": wind_description(viento_manana),
                "forecast_days":  forecast_days,
            }
    except Exception:
        return None


def format_weather_lines(w: dict) -> list[str]:
    lines = [
        f"🌡️ {w['temp']}°C (sensacion {w['sensacion']}°C)",
        f"{w['emoji']} {w['desc']}",
    ]
    if w["lluvia"] > 0:
        lines.append(f"🌧️ Lluvia: {w['lluvia']}mm")
    lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
    return lines


def format_weather_chat(w: dict, include_tomorrow: bool = False) -> str:
    lines = [
        "*Hoy:*",
        f"🌡️ {w['temp']}°C (sensacion {w['sensacion']}°C)",
        f"{w['emoji']} {w['desc']}",
    ]
    if w["lluvia"] > 0:
        lines.append(f"🌧️ Lluvia: {w['lluvia']}mm")
    lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
    if include_tomorrow:
        lines += [
            "", "*Manana:*",
            f"🌡️ {w['manana_min']}°C — {w['manana_max']}°C",
            f"{w['manana_emoji']} {w['manana_desc']}",
        ]
        if w["manana_lluvia"] > 0:
            lines.append(f"🌧️ Lluvia: {w['manana_lluvia']}mm")
        lines.append(f"💨 {w['manana_wind_desc']} ({w['manana_viento']} km/h)")
    return "\n".join(lines)


# ── Gmail ─────────────────────────────────────────────────────────────────────

async def get_gmail_summary(query_hint: str = None) -> str | None:
    providers = user_prefs.get("service_providers", {})
    provider_names = list(providers.values())
    if query_hint:
        base_query = query_hint
    elif provider_names:
        providers_query = " OR ".join(provider_names[:5])
        base_query = f"newer_than:30d ({providers_query} OR factura OR comprobante)"
    else:
        base_query = "newer_than:30d (factura OR comprobante OR boleta)"
    access_token = await get_gcal_access_token()
    if not access_token:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as http:
            headers = {"Authorization": f"Bearer {access_token}"}
            r = await http.get(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages",
                headers=headers,
                params={"q": base_query, "maxResults": 20}
            )
            if r.status_code != 200:
                return None
            messages = r.json().get("messages", [])
            if not messages:
                return None
            mail_data = []
            for msg in messages[:15]:
                msg_r = await http.get(
                    f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg['id']}",
                    headers=headers,
                    params={"format": "metadata", "metadataHeaders": ["Subject", "From", "Date"]}
                )
                if msg_r.status_code != 200:
                    continue
                msg_meta = msg_r.json()
                hdrs = {h["name"]: h["value"] for h in msg_meta.get("payload", {}).get("headers", [])}
                snippet = msg_meta.get("snippet", "")[:300]
                invoice_keywords = ["factura", "comprobante", "invoice", "vencimiento", "pago", "importe", "total"]
                subject_lower = hdrs.get("Subject", "").lower()
                is_invoice = any(k in subject_lower or k in snippet.lower() for k in invoice_keywords)
                pdf_texts = []
                if is_invoice:
                    full_r = await http.get(
                        f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg['id']}",
                        headers=headers,
                        params={"format": "full"}
                    )
                    if full_r.status_code == 200:
                        parts = full_r.json().get("payload", {}).get("parts", [])
                        for part in parts[:5]:
                            mime = part.get("mimeType", "")
                            filename = part.get("filename", "")
                            is_pdf = mime == "application/pdf" or (mime == "application/octet-stream" and filename.lower().endswith(".pdf"))
                            if is_pdf:
                                attachment_id = part.get("body", {}).get("attachmentId")
                                if attachment_id:
                                    try:
                                        att_r = await http.get(
                                            f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg['id']}/attachments/{attachment_id}",
                                            headers=headers
                                        )
                                        if att_r.status_code == 200:
                                            pdf_b64 = att_r.json().get("data", "").replace("-", "+").replace("_", "/")
                                            if pdf_b64:
                                                pdf_texts.append(pdf_b64)
                                                break
                                    except Exception:
                                        pass
                mail_data.append({
                    "from": hdrs.get("From", ""),
                    "subject": hdrs.get("Subject", ""),
                    "snippet": snippet,
                    "pdf_attachments": pdf_texts
                })
            if not mail_data:
                return None
            content = []
            mail_summary_text = ""
            for m in mail_data:
                mail_summary_text += f"\nDe: {m['from']}\nAsunto: {m['subject']}\nPreview: {m['snippet']}\n"
            content.append({"type": "text", "text": f"""Analiza estos mails importantes del ultimo mes e identifica los verdaderamente relevantes.
Importante: facturas/vencimientos con montos, mails de personas conocidas que requieren respuesta, algo urgente.
Ignora: newsletters, notificaciones automaticas, publicidad, confirmaciones rutinarias, notificaciones de GitHub/Railway/Notion.
Si hay PDFs adjuntos, leelos y extrae la info relevante (monto, vencimiento, servicio).
Resumi en espanol rioplatense, max 5 lineas. Si no hay nada importante responde solo: NONE

Mails:
{mail_summary_text}"""})
            for m in mail_data:
                for pdf_b64 in m["pdf_attachments"][:1]:
                    try:
                        content.append({
                            "type": "document",
                            "source": {
                                "type": "base64",
                                "media_type": "application/pdf",
                                "data": pdf_b64
                            }
                        })
                    except Exception:
                        pass
            resp = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=400,
                messages=[{"role": "user", "content": content}]
            )
            result = resp.content[0].text.strip()
            return None if result == "NONE" else result
    except Exception:
        return None


# ── Contexto geografico ───────────────────────────────────────────────────────

async def build_geo_context(lat: float, lon: float) -> str:
    """Usa Claude para sugerir items de shopping/geo-reminders que se puedan resolver de camino."""
    try:
        shopping = await _ds.get_shopping_list(only_missing=True)
        shopping_names = [item.name for item in (shopping or [])[:10]]
        geo_items = [r["name"] for r in geo_reminders_cache if r.get("name")][:10]
        if not shopping_names and not geo_items:
            return ""
        context_parts = []
        if shopping_names:
            context_parts.append(f"Lista de compras pendiente: {', '.join(shopping_names)}")
        if geo_items:
            context_parts.append(f"Geo-reminders activos: {', '.join(geo_items)}")
        resp = await claude_create(
            model="claude-haiku-4-5-20251001", max_tokens=80,
            system="""Sos Knot. El usuario va a un evento cercano a estas coordenadas. Tenés su lista de compras y geo-reminders.
Decide si hay algo de la lista que pueda resolverse de camino (dietéticas, farmacias, kioscos, supermercados en esa zona general).
Si hay algo concreto, respondé en 1 linea max, español rioplatense, natural, sin markdown.
Si no hay nada relevante, respondé exactamente la palabra: NADA""",
            messages=[{"role": "user", "content": f"Coordenadas destino: {lat:.4f}, {lon:.4f}\n" + "\n".join(context_parts)}]
        )
        result = resp.content[0].text.strip()
        return "" if result == "NADA" or not result else result
    except Exception:
        return ""


# ── Resumen diario ────────────────────────────────────────────────────────────

async def send_daily_summary(http, access_token: str, now: datetime):
    _hora = now.hour
    if _hora < 12:
        _saludo_tiempo = "Buenos días"
    elif _hora < 19:
        _saludo_tiempo = "Buenas tardes"
    else:
        _saludo_tiempo = "Buenas noches"
    events = []
    try:
        async with httpx.AsyncClient(timeout=10) as _cal_http:
            r = await _cal_http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}"},
            params={
                "timeMin": now.replace(hour=0, minute=0, second=0).strftime("%Y-%m-%dT00:00:00-03:00"),
                "timeMax": now.replace(hour=23, minute=59, second=59).strftime("%Y-%m-%dT23:59:59-03:00"),
                "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"
            }
        )
        if r.status_code == 200:
            events = [e for e in r.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
    except Exception:
        pass
    await load_user_config(MY_NUMBER)
    w = await get_weather()
    greeting = user_prefs.get("greeting_name") or _saludo_tiempo
    lines = [f"*{greeting}!*", ""]
    if w:
        _loc_src = current_location.get("source", "unknown")
        _loc_name = current_location.get("location_name")
        _loc_header = ""
        if _loc_src == "restored" and _loc_name:
            _loc_header = f" _(ultima ubicacion guardada: {_loc_name})_"
        elif _loc_src not in ("owntracks", "whatsapp") and _loc_name:
            _loc_header = f" _({_loc_name})_"
        lines.append(f"🌡️ {w['temp']}C (sensacion {w['sensacion']}C) -- {w['emoji']} {w['desc']}{_loc_header}")
        if w["lluvia"] > 0:
            lines.append(f"🌧️ Lluvia ahora: {w['lluvia']}mm")
        lines.append(f"💨 {w['wind_desc']} ({w['viento']} km/h)")
        pronostico = f"Hoy: max {w['hoy_max']}C, min {w['hoy_min']}C"
        if w["hoy_lluvia"] > 0:
            pronostico += f", 🌧️ {w['hoy_lluvia']}mm esperados"
        lines.append(pronostico)
        try:
            clima_ctx = f"Temp actual: {w['temp']}C (sensacion {w['sensacion']}C). Max: {w['hoy_max']}C, min: {w['hoy_min']}C. Condicion: {w['desc']}. Viento: {w['viento']}km/h. Lluvia esperada: {w['hoy_lluvia']}mm."
            narrativa_resp = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=60,
                system="Genera UNA sola linea (max 15 palabras) describiendo como va a estar el dia para alguien en Neuquen. Tono casual rioplatense. Sin emoji. Sin repetir datos numericos. Ejemplos: 'Arrancas fresco pero al mediodia pega fuerte. Sin lluvia.' o 'Dia gris y ventoso, lleva campera.' o 'Lindo dia para estar afuera, fresco pero agradable.'",
                messages=[{"role": "user", "content": clima_ctx}]
            )
            narrativa = narrativa_resp.content[0].text.strip()
            if narrativa:
                lines.append(f"_{narrativa}_")
        except Exception:
            pass
        lines.append("")
    if now.weekday() == 0:
        try:
            async with httpx.AsyncClient() as http_week:
                r_week = await http_week.get(
                    "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                    headers={"Authorization": f"Bearer {access_token}"},
                    params={"timeMin": now.strftime("%Y-%m-%dT00:00:00-03:00"),
                            "timeMax": (now + timedelta(days=7)).strftime("%Y-%m-%dT23:59:59-03:00"),
                            "singleEvents": "true", "orderBy": "startTime", "maxResults": "20"}
                )
                if r_week.status_code == 200:
                    week_events = [e for e in r_week.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
                    if week_events:
                        lines.append("*Tu semana:*")
                        for e in week_events:
                            s = e.get("start", {})
                            if "dateTime" in s:
                                dt = datetime.strptime(s["dateTime"][:16], "%Y-%m-%dT%H:%M")
                                lines.append(f"- {dt.strftime('%a %d/%m')} {dt.strftime('%H:%M')} -- {e.get('summary', '')}")
                            else:
                                lines.append(f"- {s.get('date', '')[:10]} -- {e.get('summary', '')} (todo el dia)")
                        lines.append("")
        except Exception:
            pass
    else:
        if not events:
            lines.append("Hoy no tenes eventos agendados.")
        else:
            lines.append(f"*{'Tus eventos de hoy' if len(events) > 1 else 'Tu evento de hoy'}:*")
            for e in events:
                start = e.get("start", {})
                loc_str = f" -- 📍{e.get('location', '')}" if e.get("location") else ""
                if "dateTime" in start:
                    lines.append(f"- {start['dateTime'][11:16]} -- {e.get('summary', 'Evento')}{loc_str}")
                else:
                    lines.append(f"- {e.get('summary', 'Evento')} (todo el dia){loc_str}")

    try:
        gmail_summary = await get_gmail_summary()
        if gmail_summary:
            period_str = now.strftime("%B %Y")
            try:
                extract_resp = await claude_create(
                    model="claude-haiku-4-5-20251001", max_tokens=400,
                    system="Extrae facturas/servicios del resumen de Gmail. Responde SOLO JSON array. Si no hay facturas, responde []. Formato: [{\"provider\": \"nombre\", \"amount\": numero_o_null, \"due_date\": \"YYYY-MM-DD_o_null\", \"period\": \"Mes YYYY\", \"category\": \"Servicios\"}]",
                    messages=[{"role": "user", "content": gmail_summary}]
                )
                raw = extract_resp.content[0].text.strip().strip("`").lstrip("json").strip()
                invoices = json.loads(raw) if raw.startswith("[") else []
            except Exception:
                invoices = []

            for inv in invoices:
                provider = inv.get("provider", "")
                amount = float(inv.get("amount") or 0)
                period = inv.get("period") or period_str
                due_date = inv.get("due_date") or ""
                if not provider:
                    continue
                historial = await _ds.get_finance_history_by_provider(provider, limit=2)
                ya_pagada = False
                pago_dudoso = None
                for h in historial:
                    if amount and h.value_ars:
                        diff = abs(h.value_ars - amount) / max(amount, 1)
                        if diff <= 0.10:
                            ya_pagada = True
                            break
                        elif diff > 0.10:
                            pago_dudoso = h
                if ya_pagada:
                    continue
                ok, page_id = await _ds.create_finance_invoice(provider, amount, period, due_date, inv.get("category", "Servicios"))
                if ok:
                    await _ds.create_factura_task(provider, amount, due_date, period, finance_page_id=page_id)
                if pago_dudoso and amount:
                    lines.append(f"_⚠️ {provider}: factura ${amount:,.0f} pero último pago registrado ${pago_dudoso.value_ars:,.0f} — revisá si coincide._")

        impagas = await _ds.get_impaga_facturas()
        if impagas:
            lines.append("")
            lines.append("*Facturas pendientes:*")
            for imp in impagas:
                monto = f"${imp.value_ars:,.0f}" if imp.value_ars else ""
                if imp.date:
                    dias = (now.date() - imp.date).days
                    if dias > 30:
                        dias_str = f" ⚠️ _({dias} días pendiente)_"
                    else:
                        dias_str = f" _({dias} días pendiente)_"
                else:
                    dias_str = ""
                lines.append(f"- {imp.name} {monto}{dias_str}".strip())
        else:
            lines.append("")
            lines.append("✅ Facturas al día")
    except Exception:
        pass

    extras = user_prefs.get("resumen_extras", [])
    if extras:
        try:
            extras_prompt = "\n".join(f"- {e}" for e in extras)
            extra_resp = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=300,
                system=f"Sos Knot. Hoy es {now.strftime('%A %d/%m/%Y')}. Genera contenido breve (max 3 lineas por item) para los siguientes extras del Resumen Diario. Usas espanol rioplatense, tono natural y calido.",
                messages=[{"role": "user", "content": f"Genera estos extras para el resumen matutino:\n{extras_prompt}"}]
            )
            extra_text = extra_resp.content[0].text.strip()
            if extra_text:
                lines.append("")
                lines.append(extra_text)
        except Exception:
            pass

    msg_text = "\n".join(lines)
    await send_message(MY_NUMBER, msg_text)
    add_to_history(MY_NUMBER, "assistant", msg_text)


# ── Resumen nocturno ──────────────────────────────────────────────────────────

async def send_resumen_nocturno(http, access_token: str, now: datetime):
    is_sunday = now.weekday() == 6
    if is_sunday:
        await send_resumen_nocturno_dominical(http, access_token, now)
    else:
        await send_resumen_nocturno_regular(http, access_token, now)


async def send_resumen_nocturno_regular(http, access_token: str, now: datetime):
    """Resumen nocturno de lunes a sabado."""
    manana = now + timedelta(days=1)
    r = await http.get(
        "https://www.googleapis.com/calendar/v3/calendars/primary/events",
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "timeMin": manana.strftime("%Y-%m-%dT00:00:00-03:00"),
            "timeMax": manana.strftime("%Y-%m-%dT23:59:59-03:00"),
            "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"
        }
    )
    events_manana = []
    if r.status_code == 200:
        events_manana = [e for e in r.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]

    eventos_str = ""
    if events_manana:
        lineas = []
        for e in events_manana:
            s = e.get("start", {})
            if "dateTime" in s:
                lineas.append(f"- {s['dateTime'][11:16]} -- {e.get('summary','')}")
            else:
                lineas.append(f"- {e.get('summary','')} (todo el dia)")
        eventos_str = "\n".join(lineas)

    w = await get_weather()
    context = f"Hoy es {now.strftime('%A %d/%m/%Y')}. Hora: {now.strftime('%H:%M')}."
    if eventos_str:
        context += f"\nEventos de manana:\n{eventos_str}"
    else:
        context += "\nManana no hay eventos agendados."
    if w:
        context += f"\nClima esta noche: {w['temp']}°C, {w['desc']}."
        context += f"\nManana: {w['manana_min']}-{w['manana_max']}°C, {w['manana_desc']}."

    try:
        resp = await claude_create(
            model="claude-sonnet-4-20250514", max_tokens=300,
            system=f"""Sos Knot. {context}
Genera un resumen nocturno breve y natural en espanol rioplatense. Inclui:
1. Saludo de buenas noches con clima de esta noche y de manana.
2. Que hay para manana (o que el dia esta libre).
3. Una sugerencia espontanea: agendar algo, agregar a la lista, registrar un gasto, o pensamiento de cierre.
Conciso, calido, natural. Maximo 5 lineas.""",
            messages=[{"role": "user", "content": "Genera el resumen nocturno."}]
        )
        msg = resp.content[0].text.strip()
    except Exception:
        if eventos_str:
            msg = f"Buenas noches! Manana tenes:\n{eventos_str}\n\nQue descanses"
        else:
            msg = "Buenas noches! Manana el dia esta libre. Que descanses"

    await send_message(MY_NUMBER, msg)
    add_to_history(MY_NUMBER, "assistant", msg)


async def send_resumen_nocturno_dominical(http, access_token: str, now: datetime):
    """Resumen nocturno especial del domingo."""
    lines = ["🌙 *Buenas noches! Resumen del domingo*", ""]

    w = await get_weather(days=7)
    if w:
        lines.append(f"🌡️ *Esta noche:* {w['temp']}°C, {w['desc']}")
        lines.append(f"☀️ *Manana lunes:* {w['manana_min']}-{w['manana_max']}°C, {w['manana_desc']}")
        lines.append("")

    try:
        r_week = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}"},
            params={
                "timeMin": (now + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00-03:00"),
                "timeMax": (now + timedelta(days=7)).strftime("%Y-%m-%dT23:59:59-03:00"),
                "singleEvents": "true", "orderBy": "startTime", "maxResults": "20"
            }
        )
        week_events = []
        if r_week.status_code == 200:
            week_events = [e for e in r_week.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
        if week_events:
            lines.append("🗓️ *Tu semana:*")
            for e in week_events:
                s = e.get("start", {})
                if "dateTime" in s:
                    dt = datetime.strptime(s["dateTime"][:16], "%Y-%m-%dT%H:%M")
                    lines.append(f"- {dt.strftime('%a %d/%m')} {dt.strftime('%H:%M')} — {e.get('summary','')}")
                else:
                    lines.append(f"- {s.get('date','')[:10]} — {e.get('summary','')} (todo el dia)")
            lines.append("")
            lunes_early = [e for e in week_events if e.get("start",{}).get("dateTime","")[:10] == (now + timedelta(days=1)).strftime("%Y-%m-%d")]
            if lunes_early:
                primero = lunes_early[0]
                hora = primero.get("start",{}).get("dateTime","")[11:16]
                if hora and hora < "10:00":
                    lines.append(f"⚠️ Mañana arranças temprano: *{primero.get('summary','')}* a las {hora}")
                    lines.append("")
        else:
            lines.append("🗓️ La semana que viene está libre de eventos.")
            lines.append("")
    except Exception:
        pass

    try:
        lunes_date = (now - timedelta(days=now.weekday())).date()
        hoy_date = now.date()
        week_entries = await _ds.query_expenses(QueryFilter(
            date_range=DateRange(start=lunes_date, end=hoy_date),
            limit=50,
        ))
        egresos = 0
        por_cat: dict = {}
        for e in week_entries:
            if e.in_out != "INGRESO":
                egresos += e.value_ars
                for cat in (e.categories or []):
                    por_cat[cat] = por_cat.get(cat, 0) + e.value_ars
        if egresos > 0:
            top = sorted(por_cat.items(), key=lambda x: x[1], reverse=True)[:3]
            top_str = " · ".join(f"{c} ${v:,.0f}" for c, v in top)
            lines.append(f"💰 *Esta semana gastaste:* ${egresos:,.0f}")
            if top_str:
                lines.append(f"_{top_str}_")
            lines.append("")
    except Exception:
        pass

    if w and w.get("forecast_days"):
        try:
            forecast_txt = "\n".join(
                f"- {fd['date']}: {fd['min']}-{fd['max']}°C, {fd['desc']}, lluvia {fd['lluvia']}mm"
                for fd in w["forecast_days"][1:7]
            )
            clima_resp = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=80,
                system="Resume el pronostico semanal en 2 lineas maximas, lenguaje natural rioplatense, destacando lo mas relevante (frio, lluvia, calor).",
                messages=[{"role": "user", "content": forecast_txt}]
            )
            clima_semana = clima_resp.content[0].text.strip()
            lines.append(f"🌦️ *Clima de la semana:* {clima_semana}")
            lines.append("")
        except Exception:
            pass

    try:
        pending_tasks = await _ds.get_pending_factura_tasks()
        semana_fin = (now + timedelta(days=7)).date()
        facturas_urgentes = []
        for t in pending_tasks:
            if t.get("due"):
                try:
                    due_date = datetime.strptime(t["due"][:10], "%Y-%m-%d").date()
                    if due_date <= semana_fin:
                        days_left = (due_date - now.date()).days
                        facturas_urgentes.append((t["name"], t["due"][:10], days_left))
                except Exception:
                    pass
        if facturas_urgentes:
            lines.append("⚠️ *Facturas con vencimiento esta semana:*")
            for nombre, fecha, dias in facturas_urgentes:
                alerta = "mañana" if dias == 1 else f"en {dias} días" if dias > 0 else "hoy"
                lines.append(f"- {nombre} — vence {alerta} ({fecha})")
            lines.append("")
    except Exception:
        pass

    try:
        impagas_sem = await _ds.get_impaga_facturas()
        if impagas_sem:
            lines.append("💳 *Facturas Impaga:*")
            for imp in impagas_sem:
                monto = f"${imp.value_ars:,.0f}" if imp.value_ars else ""
                if imp.date:
                    dias = (now.date() - imp.date).days
                    if dias > 30:
                        dias_str = f" ⚠️ _({dias} días pendiente)_"
                    else:
                        dias_str = f" _({dias} días pendiente)_"
                else:
                    dias_str = ""
                lines.append(f"- {imp.name} {monto}{dias_str}".strip())
            lines.append("")
    except Exception:
        pass

    lines.append("_Es un buen momento para anotar algo pendiente — un evento, una tarea, lo que se te venga a la cabeza para la semana._")
    lines.append("")
    lines.append("_Si querés, también puedo mostrarte:_")
    lines.append("_• Tu lista de compras_")
    lines.append("_• Tus recordatorios geolocalizados activos_")
    lines.append("_• Tus tasks pendientes_")

    msg = "\n".join(lines)
    await send_message(MY_NUMBER, msg)
    add_to_history(MY_NUMBER, "assistant", msg)
