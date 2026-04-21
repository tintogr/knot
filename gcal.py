import os
import re
import calendar as _calendar
import httpx
from datetime import datetime, timedelta

from state import now_argentina, DIAS_SEMANA, last_event_touched, claude_create


# ── Auth ──────────────────────────────────────────────────────────────────────

async def get_gcal_access_token() -> str | None:
    refresh_token = os.environ.get("GCAL_REFRESH_TOKEN")
    client_id     = os.environ.get("GCAL_CLIENT_ID")
    client_secret = os.environ.get("GCAL_CLIENT_SECRET")
    if not all([refresh_token, client_id, client_secret]):
        return None
    async with httpx.AsyncClient() as http:
        r = await http.post("https://oauth2.googleapis.com/token", data={
            "grant_type": "refresh_token", "refresh_token": refresh_token,
            "client_id": client_id, "client_secret": client_secret,
        })
        if r.status_code == 200:
            return r.json().get("access_token")
    return None


# ── Helpers de color y creacion ────────────────────────────────────────────────

def get_event_color(summary: str, is_temp: bool = False) -> str:
    if is_temp:
        return "4"
    medical_kw = {"dr", "dra", "doctor", "medico", "turno", "cita", "hospital",
                  "clinica", "odontologo", "psicologo", "dentista", "cardiologo",
                  "ortopedista", "kinesiologo"}
    if any(kw in summary.lower() for kw in medical_kw):
        return "2"
    return "1"


async def create_evento_gcal(data: dict) -> tuple[bool, str]:
    access_token = await get_gcal_access_token()
    if not access_token:
        return False, ""
    if data.get("time"):
        start = {"dateTime": f"{data['date']}T{data['time']}:00", "timeZone": "America/Argentina/Buenos_Aires"}
        end_dt = datetime.strptime(f"{data['date']}T{data['time']}", "%Y-%m-%dT%H:%M") + timedelta(minutes=data.get("duration_minutes", 60))
        end = {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}
    else:
        start = {"date": data["date"]}
        end = {"date": data["date"]}
    event = {
        "summary": data.get("summary", "Evento"),
        "start": start,
        "end": end,
        "source": {"title": "Knot", "url": os.environ.get("APP_URL", "https://knot.onrender.com")},
        "colorId": get_event_color(data.get("summary", "")),
        "extendedProperties": {"private": {"created_by": "matrics", "type": "evento"}},
    }
    if data.get("description"):
        event["description"] = data["description"]
    if data.get("location"):
        event["location"] = data["location"]
    if data.get("recurrence"):
        event["recurrence"] = [data["recurrence"]]
    async with httpx.AsyncClient() as http:
        r = await http.post(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json=event
        )
        if r.status_code in [200, 201]:
            event_id = r.json().get("id", "")
            return True, event_id
        return False, ""


# ── Fuzzy matching ────────────────────────────────────────────────────────────

def fuzzy_match_event(search_term: str, events: list) -> dict | None:
    if not events:
        return None
    if not search_term:
        return events[0]
    search_lower = search_term.lower()
    for e in events:
        if search_lower == e.get("summary", "").lower():
            return e
    for e in events:
        if search_lower in e.get("summary", "").lower():
            return e
    for e in events:
        if e.get("summary", "").lower() in search_lower:
            return e
    search_words = set(search_lower.split())
    best_score = 0
    best_event = None
    for e in events:
        event_words = set(e.get("summary", "").lower().split())
        score = len(search_words & event_words)
        if score > best_score:
            best_score = score
            best_event = e
    if best_score > 0:
        return best_event
    return events[0]


async def _find_calendar_event(search_term: str = None, phone: str = None, target_date: str = None) -> tuple[dict | None, str]:
    """Busca un evento en Calendar con multiples estrategias."""
    access_token = await get_gcal_access_token()
    if not access_token:
        return None, "Calendar no configurado"
    now = now_argentina()
    if target_date:
        time_min = f"{target_date}T00:00:00-03:00"
        time_max = f"{target_date}T23:59:59-03:00"
    else:
        time_min = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00-03:00")
        time_max = (now + timedelta(days=60)).strftime("%Y-%m-%dT23:59:59-03:00")
    async with httpx.AsyncClient() as http:
        headers = {"Authorization": f"Bearer {access_token}"}
        if not search_term:
            if phone and phone in last_event_touched:
                entry = last_event_touched[phone]
                r = await http.get(
                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{entry['event_id']}",
                    headers=headers
                )
                if r.status_code == 200:
                    return r.json(), ""
            return None, "No encontre contexto de evento reciente."
        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers=headers,
            params={"q": search_term, "timeMin": time_min, "timeMax": time_max,
                    "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"}
        )
        if r.status_code == 200:
            candidates = [e for e in r.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
            if candidates:
                return fuzzy_match_event(search_term, candidates), ""
        if len(search_term.split()) > 1:
            first_word = search_term.split()[0]
            r2 = await http.get(
                "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                headers=headers,
                params={"q": first_word, "timeMin": time_min, "timeMax": time_max,
                        "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"}
            )
            if r2.status_code == 200:
                candidates2 = [e for e in r2.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
                if candidates2:
                    return fuzzy_match_event(search_term, candidates2), ""
        if phone and phone in last_event_touched:
            entry = last_event_touched[phone]
            r3 = await http.get(
                f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{entry['event_id']}",
                headers=headers
            )
            if r3.status_code == 200:
                candidate = r3.json()
                search_words = set(search_term.lower().split())
                event_words = set(candidate.get("summary", "").lower().split())
                if search_words & event_words:
                    return candidate, ""
        return None, f"No encontre ningun evento relacionado con '{search_term}'."


async def find_similar_calendar_events(data: dict) -> list:
    access_token = await get_gcal_access_token()
    if not access_token:
        return []
    summary = data.get("summary", "")
    if not summary or len(summary) < 4:
        return []
    stopwords = {"con", "en", "de", "la", "el", "los", "las", "del", "al", "por", "para",
                 "turno", "cita", "reunion", "evento", "con", "una", "uno"}
    keywords = [w for w in summary.lower().split() if len(w) > 3 and w not in stopwords]
    if not keywords:
        return []
    now = now_argentina()
    time_min = now.strftime("%Y-%m-%dT00:00:00-03:00")
    time_max = (now + timedelta(days=180)).strftime("%Y-%m-%dT23:59:59-03:00")
    found = {}
    async with httpx.AsyncClient() as http:
        headers = {"Authorization": f"Bearer {access_token}"}
        for kw in keywords[:2]:
            try:
                r = await http.get(
                    "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                    headers=headers,
                    params={"q": kw, "timeMin": time_min, "timeMax": time_max,
                            "singleEvents": "true", "orderBy": "startTime", "maxResults": "5"}
                )
                if r.status_code == 200:
                    for e in r.json().get("items", []):
                        if "[TEMP]" not in (e.get("description") or ""):
                            found[e["id"]] = e
            except Exception:
                pass
    return list(found.values())[:3]


# ── RRULE helpers ─────────────────────────────────────────────────────────────

RRULE_DAY_MAP = {"MO": 0, "TU": 1, "WE": 2, "TH": 3, "FR": 4, "SA": 5, "SU": 6}
WEEKDAY_TO_RRULE = {0: "MO", 1: "TU", 2: "WE", 3: "TH", 4: "FR", 5: "SA", 6: "SU"}


def next_weekday_date(from_date, target_weekday: int):
    """Retorna la proxima fecha (inclusive hoy) que caiga en target_weekday (0=lunes)."""
    days_ahead = target_weekday - from_date.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return from_date + timedelta(days=days_ahead)


def fix_recurring_event_date(event_date_str: str, rrule: str) -> str:
    """Si el RRULE tiene BYDAY, verifica que la fecha coincida. Si no, la corrige."""
    if not rrule or "BYDAY=" not in rrule:
        return event_date_str
    try:
        event_date = datetime.strptime(event_date_str, "%Y-%m-%d").date()
        for part in rrule.split(";"):
            if "BYDAY=" in part:
                day_code = part.split("BYDAY=")[1].strip().split(",")[0].strip()
                target = RRULE_DAY_MAP.get(day_code)
                if target is not None and event_date.weekday() != target:
                    fixed = next_weekday_date(event_date, target)
                    return fixed.strftime("%Y-%m-%d")
    except Exception:
        pass
    return event_date_str


# ── Consultas de calendario ────────────────────────────────────────────────────

async def query_calendar(days_ahead: int = 2, days_back: int = 0) -> str | None:
    access_token = await get_gcal_access_token()
    if not access_token:
        return None
    now = now_argentina()
    time_min = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00-03:00")
    time_max = (now + timedelta(days=days_ahead)).strftime("%Y-%m-%dT23:59:59-03:00")
    async with httpx.AsyncClient() as http:
        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"timeMin": time_min, "timeMax": time_max, "singleEvents": "true",
                    "orderBy": "startTime", "maxResults": "20"}
        )
        if r.status_code != 200:
            return None
        events = [e for e in r.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
        if not events:
            return "No hay eventos en ese periodo."
        lines = []
        for e in events:
            start = e.get("start", {})
            loc_str = f" -- 📍{e.get('location', '')}" if e.get("location") else ""
            if "dateTime" in start:
                dt_str = start["dateTime"]
                if dt_str.endswith("Z"):
                    dt = datetime.strptime(dt_str[:16], "%Y-%m-%dT%H:%M") - timedelta(hours=3)
                else:
                    dt = datetime.strptime(dt_str[:16], "%Y-%m-%dT%H:%M")
                dia = DIAS_SEMANA[dt.weekday()]
                lines.append(f"- {dia} {dt.strftime('%d/%m')} {dt.strftime('%H:%M')} -- {e.get('summary', 'Evento')}{loc_str}")
            else:
                date_str = start.get("date", "")
                if date_str:
                    d = datetime.strptime(date_str, "%Y-%m-%d")
                    dia = DIAS_SEMANA[d.weekday()]
                    lines.append(f"- {dia} {d.strftime('%d/%m')} -- {e.get('summary', 'Evento')} (todo el dia){loc_str}")
                else:
                    lines.append(f"- {date_str} -- {e.get('summary', 'Evento')} (todo el dia){loc_str}")
        return "\n".join(lines)


async def query_calendar_date(fecha: str) -> str | None:
    """Consulta eventos de un dia especifico (YYYY-MM-DD)."""
    access_token = await get_gcal_access_token()
    if not access_token:
        return None
    time_min = f"{fecha}T00:00:00-03:00"
    time_max = f"{fecha}T23:59:59-03:00"
    async with httpx.AsyncClient() as http:
        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"timeMin": time_min, "timeMax": time_max, "singleEvents": "true",
                    "orderBy": "startTime", "maxResults": "20"}
        )
        if r.status_code != 200:
            return None
        events = [e for e in r.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
        if not events:
            return "No hay eventos ese dia."
        lines = []
        for e in events:
            start = e.get("start", {})
            loc_str = f" -- 📍{e.get('location', '')}" if e.get("location") else ""
            if "dateTime" in start:
                dt_str = start["dateTime"]
                if dt_str.endswith("Z"):
                    dt = datetime.strptime(dt_str[:16], "%Y-%m-%dT%H:%M") - timedelta(hours=3)
                else:
                    dt = datetime.strptime(dt_str[:16], "%Y-%m-%dT%H:%M")
                dia = DIAS_SEMANA[dt.weekday()]
                lines.append(f"- {dia} {dt.strftime('%d/%m')} {dt.strftime('%H:%M')} -- {e.get('summary', 'Evento')}{loc_str}")
            else:
                d = datetime.strptime(start.get("date", fecha), "%Y-%m-%d")
                dia = DIAS_SEMANA[d.weekday()]
                lines.append(f"- {dia} {d.strftime('%d/%m')} -- {e.get('summary', 'Evento')} (todo el dia){loc_str}")
        return "\n".join(lines)


# ── Calculo de fechas ─────────────────────────────────────────────────────────

def calcular_fecha_exacta(descripcion: str) -> str:
    """Calcula fechas exactas a partir de descripciones en lenguaje natural."""
    now = now_argentina()
    desc = descripcion.lower().strip()
    DIAS = {"lunes": 0, "martes": 1, "miercoles": 2, "miércoles": 2, "jueves": 3,
            "viernes": 4, "sabado": 5, "sábado": 5, "domingo": 6}
    MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
             "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}
    ORDINAL = {"primer": 1, "primero": 1, "segundo": 2, "tercer": 3, "tercero": 3,
               "cuarto": 4, "quinto": 5, "ultimo": -1, "último": -1}

    year = now.year
    year_match = re.search(r'\b(202\d)\b', desc)
    if year_match:
        year = int(year_match.group(1))
    elif "año que viene" in desc or "proximo año" in desc or "próximo año" in desc:
        year = now.year + 1

    target_month = None
    for nombre, num in MESES.items():
        if nombre in desc:
            target_month = num
            break

    for ord_name, ord_num in ORDINAL.items():
        for dia_name, dia_num in DIAS.items():
            if ord_name in desc and dia_name in desc and target_month:
                _, days_in_month = _calendar.monthrange(year, target_month)
                ocurrencias = []
                for day in range(1, days_in_month + 1):
                    if datetime(year, target_month, day).weekday() == dia_num:
                        ocurrencias.append(day)
                if ord_num == -1:
                    chosen = ocurrencias[-1]
                elif ord_num <= len(ocurrencias):
                    chosen = ocurrencias[ord_num - 1]
                else:
                    return f"No existe el {ord_name} {dia_name} de ese mes."
                result_date = datetime(year, target_month, chosen)
                dia_semana = DIAS_SEMANA[result_date.weekday()]
                return f"{dia_semana} {result_date.strftime('%d/%m/%Y')}"

    match = re.search(r'dentro de (\d+) d[ií]as?', desc)
    if match:
        target = now + timedelta(days=int(match.group(1)))
        return f"{DIAS_SEMANA[target.weekday()]} {target.strftime('%d/%m/%Y')}"

    return f"No pude interpretar la fecha: '{descripcion}'"


async def calcular_fecha_con_verificacion(descripcion: str) -> str:
    """Calcula la fecha con Python y la verifica."""
    resultado_python = calcular_fecha_exacta(descripcion)
    return f"Calculado: {resultado_python} (verificado con Python)"
