import os
import json
import asyncio
import base64
import time
import httpx
from datetime import date, datetime, timedelta, timezone
from calendar import monthrange
from math import radians, sin, cos, sqrt, atan2
from fastapi import FastAPI, Request, BackgroundTasks

from state import (
    _ds, QueryFilter, DateRange,
    WA_TOKEN, WA_PHONE_ID, WA_API, MY_NUMBER, DAILY_SUMMARY_HOUR,
    USER_LAT, USER_LON,
    DIAS_SEMANA, INGRESO_EXACT, EGRESO_EXACT, MAX_HISTORY,
    user_prefs, current_location, geo_reminders_cache, payment_methods_cache,
    last_event_touched, pending_state, message_buffer,
    chat_history, _last_summary_sent,
    now_argentina,
    claude_create, hoy_str, semana_str, get_history, add_to_history,
)
from wa_utils import send_message, send_interactive_buttons, send_reaction, error_servicio
import suggestion_gate
from gcal import (
    get_gcal_access_token, get_event_color, create_evento_gcal,
    fuzzy_match_event, _find_calendar_event, find_similar_calendar_events,
    RRULE_DAY_MAP, WEEKDAY_TO_RRULE, next_weekday_date, fix_recurring_event_date,
    query_calendar, query_calendar_date, calcular_fecha_exacta, calcular_fecha_con_verificacion,
)
from config import load_user_config, save_user_config, handle_configurar
from summaries import (
    get_weather, format_weather_lines, format_weather_chat,
    get_gmail_summary, build_geo_context,
    send_daily_summary, send_resumen_nocturno,
)

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    """Carga config del usuario al arrancar para no esperar al primer mensaje."""
    await load_user_config(MY_NUMBER)
    await load_geo_reminders()
    await load_payment_methods()
    await _ds.ensure_db_select_field("finances", "Estado", ["Impaga", "Pagada"])
    await _ds.ensure_db_text_field("config", "Last Summary Date")
    asyncio.create_task(_run_once_migrations())
    asyncio.create_task(_cron_loop())

@app.on_event("shutdown")
async def shutdown_event():
    await _ds.aclose()


# ── Normalizacion de In-Out ───────────────────────────────────────────────────
def normalize_in_out(raw: str) -> str:
    """Fuerza el valor exacto de In-Out para que Notion no rompa formulas."""
    if not raw:
        return EGRESO_EXACT
    upper = raw.upper().strip()
    if "INGRESO" in upper:
        return INGRESO_EXACT
    return EGRESO_EXACT

# ── Helpers de ubicacion ──────────────────────────────────────────────────────
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia en km entre dos puntos GPS."""
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

def get_current_location() -> tuple[float, float]:
    """Devuelve lat, lon actual (dinamica si hay OwnTracks, sino default)."""
    return current_location["lat"], current_location["lon"]

def is_at_known_place() -> dict | None:
    """Retorna el lugar conocido si el usuario esta en uno, sino None."""
    lat, lon = get_current_location()
    for place in user_prefs.get("known_places", []):
        dist_m = haversine_km(lat, lon, place["lat"], place["lon"]) * 1000
        radius = place.get("radius", 200)
        if dist_m <= radius:
            return place
    return None

def is_in_transit() -> bool:
    """True si el usuario se esta moviendo (velocidad > 15 km/h)."""
    return current_location.get("velocity", 0) > 15

async def reverse_geocode(lat: float, lon: float) -> str | None:
    """Devuelve direccion completa usando Google Geocoding API, con fallback a Nominatim."""
    api_key = os.environ.get("GOOGLE_PLACES_KEY", "")
    if api_key:
        try:
            async with httpx.AsyncClient(timeout=5) as http:
                r = await http.get(
                    "https://maps.googleapis.com/maps/api/geocode/json",
                    params={"latlng": f"{lat},{lon}", "key": api_key, "language": "es"}
                )
                if r.status_code == 200:
                    results = r.json().get("results", [])
                    if results:
                        # El primer resultado es el más específico (calle + número)
                        return results[0].get("formatted_address", "")
        except Exception:
            pass
    # Fallback a Nominatim si no hay key o falla Google
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            for zoom in [14, 12, 10, 8, 6]:
                r = await http.get(
                    "https://nominatim.openstreetmap.org/reverse",
                    params={"lat": lat, "lon": lon, "format": "json", "zoom": zoom, "addressdetails": 1},
                    headers={"User-Agent": "Knot/1.0"}
                )
                if r.status_code != 200:
                    continue
                data = r.json()
                addr = data.get("address", {})
                for key in ["village", "town", "suburb", "city_district", "municipality", "city", "county", "state_district", "state"]:
                    val = addr.get(key)
                    if val:
                        state = addr.get("state", "")
                        if state and state.lower() not in val.lower():
                            return f"{val}, {state}"
                        return val
                display = data.get("display_name", "")
                if display:
                    parts = [p.strip() for p in display.split(",")[:2]]
                    return ", ".join(parts)
    except Exception:
        pass
    return None


def _format_place_name(result: dict, fallback: str = "") -> str:
    """Construye un nombre limpio desde un resultado de Nominatim con addressdetails=1.
    Evita el truncamiento crudo de display_name que produce artefactos como "...Neuquén, Municipio"."""
    addr = result.get("address") or {}
    if not addr:
        raw = result.get("display_name", fallback)
        return raw[:60] if raw else fallback[:60]

    parts: list[str] = []
    road = addr.get("road") or addr.get("pedestrian") or addr.get("footway")
    house = addr.get("house_number")
    if road:
        parts.append(f"{road} {house}".strip() if house else road)

    area = addr.get("suburb") or addr.get("neighbourhood") or addr.get("city_district")
    if area:
        parts.append(area)

    city = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality")
    if city:
        clean = city.replace("Municipio de ", "").replace("Municipio ", "")
        if not parts or clean.lower() not in ", ".join(parts).lower():
            parts.append(clean)

    state = addr.get("state")
    if state and not any(state.lower() in p.lower() for p in parts):
        parts.append(state)

    if not parts:
        raw = result.get("display_name", fallback)
        return raw[:60] if raw else fallback[:60]
    return ", ".join(parts)[:80]


async def _save_known_place(name: str | None, lat: float, lon: float, radius: int = 150) -> None:
    """Upsert de un lugar conocido en user_prefs y persiste en Notion."""
    if not name:
        return
    nombre = name.strip().capitalize()
    if not nombre:
        return
    places = user_prefs.get("known_places", [])
    places = [p for p in places if p["name"].lower() != nombre.lower()]
    places.append({"name": nombre, "lat": lat, "lon": lon, "radius": radius})
    user_prefs["known_places"] = places
    try:
        await save_user_config(MY_NUMBER)
    except Exception:
        pass


async def extract_coords_from_maps_url(url: str) -> tuple[float, float] | None:
    """Extrae coordenadas de un link de Google Maps (maps.app.goo.gl, etc)."""
    import re
    m = re.search(r'@(-?\d{1,3}\.\d{4,}),(-?\d{1,3}\.\d{4,})', url)
    if m:
        return float(m.group(1)), float(m.group(2))
    try:
        async with httpx.AsyncClient(timeout=8, follow_redirects=True) as http:
            r = await http.get(url, headers={"User-Agent": "Mozilla/5.0"})
            final_url = str(r.url)
            for pattern in [
                r'@(-?\d{1,3}\.\d{4,}),(-?\d{1,3}\.\d{4,})',
                r'[?&]q=(-?\d{1,3}\.\d{4,}),(-?\d{1,3}\.\d{4,})',
                r'/place/[^@]+@(-?\d{1,3}\.\d{4,}),(-?\d{1,3}\.\d{4,})',
            ]:
                m = re.search(pattern, final_url)
                if m:
                    return float(m.group(1)), float(m.group(2))
    except Exception:
        pass
    return None

async def search_nearby_shops(lat: float, lon: float, radius: int = 500, shop_types: list = None, name_filter: str = None) -> list[dict]:
    """Busca comercios cercanos usando Google Places API."""
    api_key = os.environ.get("GOOGLE_PLACES_KEY", "")
    if not api_key:
        print("[Places] No hay GOOGLE_PLACES_API_KEY configurada")
        return []

    try:
        async with httpx.AsyncClient(timeout=10) as http:
            if name_filter:
                # Busqueda por nombre especifico
                r = await http.get(
                    "https://maps.googleapis.com/maps/api/place/textsearch/json",
                    params={
                        "query": name_filter,
                        "location": f"{lat},{lon}",
                        "radius": radius,
                        "key": api_key,
                        "language": "es"
                    }
                )
            else:
                # Busqueda por tipo
                query_type = "supermarket"
                if shop_types:
                    query_type = shop_types[0]
                r = await http.get(
                    "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
                    params={
                        "location": f"{lat},{lon}",
                        "radius": radius,
                        "type": query_type,
                        "key": api_key,
                        "language": "es"
                    }
                )

            if r.status_code != 200:
                print(f"[Places] Error {r.status_code}: {r.text[:100]}")
                return []

            results = r.json().get("results", [])
            shops = []
            for place in results[:10]:
                plat = place["geometry"]["location"]["lat"]
                plon = place["geometry"]["location"]["lng"]
                dist_m = round(haversine_km(lat, lon, plat, plon) * 1000)
                address = place.get("vicinity", "")
                maps_link = f"https://www.google.com/maps/place/?q=place_id:{place['place_id']}"
                opening = ""
                if place.get("opening_hours", {}).get("open_now") is True:
                    opening = "Abierto ahora"
                elif place.get("opening_hours", {}).get("open_now") is False:
                    opening = "Cerrado ahora"
                shops.append({
                    "name": place.get("name", ""),
                    "type": place.get("types", [""])[0],
                    "distance_m": dist_m,
                    "lat": plat,
                    "lon": plon,
                    "address": address,
                    "opening_hours": opening,
                    "maps_link": maps_link,
                })

            shops.sort(key=lambda x: x["distance_m"])
            # Excluir resultados claramente fuera del area (mas de 50km)
            shops = [s for s in shops if s["distance_m"] <= 50000]
            return shops

    except Exception as e:
        print(f"[Places] Error buscando comercios: {e}")
        return []

async def check_shopping_proximity():
    """Chequea si hay comercios cerca que vendan cosas de la lista de compras."""
    if is_at_known_place() or is_in_transit():
        return None
    if not current_location.get("updated_at"):
        return None
    # Solo chequear si la ubicacion es reciente (ultimos 10 min)
    age = (now_argentina() - current_location["updated_at"]).total_seconds()
    if age > 600:
        return None
    # Mapeo de tipos de tienda Matrics -> tipos OSM
    store_type_map = {
        "Super":      ["supermarket", "convenience"],
        "Panaderia":  ["bakery"],
        "Verduleria": ["greengrocer", "farm"],
        "Farmacia":   ["supermarket"],  # pharmacy se busca via amenity siempre
        "Ferreteria": ["hardware"],
        "Dietetica":  ["health_food", "organic"],
        "Drogueria":  ["chemist"],
    }
    # Buscar items pendientes agrupados por store
    try:
        shopping_items = await _ds.get_shopping_list(only_missing=True)
        if not shopping_items:
            return None
        by_store = {}
        for item in shopping_items:
            for store_name in (item.stores or []):
                if store_name not in by_store:
                    by_store[store_name] = []
                by_store[store_name].append(item.name)
        if not by_store:
            return None
            lat, lon = get_current_location()
            for store_type, item_names in by_store.items():
                osm_types = store_type_map.get(store_type, ["supermarket"])
                shops = await search_nearby_shops(lat, lon, shop_types=osm_types)
                if shops:
                    return {
                        "store_type": store_type,
                        "items": item_names,
                        "shops": shops
                    }
    except Exception:
        pass
    return None

# ── Memoria de categorias ──────────────────────────────────────────────────────
category_overrides: dict[str, list[str]] = {}

# ── Ultima entrada tocada (gastos) ────────────────────────────────────────────
last_touched: dict[str, dict] = {}

# ── Deduplicacion de mensajes ─────────────────────────────────────────────────
processed_message_ids: set[str] = set()
MAX_PROCESSED_IDS = 500

# ── Buffer de mensajes (agrupa mensajes relacionados en ventana de tiempo) ────
buffer_timers: dict[str, asyncio.Task] = {}
BUFFER_WINDOW_SECS = 4.0
PROCESSING_INDICATOR_DELAY = 4.0


async def get_media_base64(media_id: str) -> tuple[str, str]:
    async with httpx.AsyncClient() as http:
        r = await http.get(
            f"https://graph.facebook.com/v22.0/{media_id}",
            headers={"Authorization": f"Bearer {WA_TOKEN}"}
        )
        media_url = r.json()["url"]
        mime_type = r.json().get("mime_type", "image/jpeg")
        img_r = await http.get(media_url, headers={"Authorization": f"Bearer {WA_TOKEN}"})
        return base64.b64encode(img_r.content).decode(), mime_type

# ── Transcripcion de audio con Groq Whisper ───────────────────────────────────
async def transcribe_audio(media_id: str) -> str | None:
    groq_key = os.environ.get("GROQ_API_KEY")
    if not groq_key:
        return None
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.get(
            f"https://graph.facebook.com/v22.0/{media_id}",
            headers={"Authorization": f"Bearer {WA_TOKEN}"}
        )
        if r.status_code != 200:
            return None
        media_url = r.json()["url"]
        audio_r = await http.get(media_url, headers={"Authorization": f"Bearer {WA_TOKEN}"})
        if audio_r.status_code != 200:
            return None
        resp = await http.post(
            "https://api.groq.com/openai/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {groq_key}"},
            files={"file": ("audio.ogg", audio_r.content, "audio/ogg")},
            data={"model": "whisper-large-v3", "language": "es"},
        )
        if resp.status_code == 200:
            return resp.json().get("text", "").strip()
    return None

# ── Clima (Open-Meteo) ────────────────────────────────────────────────────────

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

_BUSINESS_TYPE_TO_CATEGORY = {
    "supermercado": "Supermercado",
    "farmacia": "Salud",
    "droguería": "Salud",
    "drogueria": "Salud",
    "restaurante": "Salida",
    "restaurant": "Salida",
    "bar": "Birra",
    "café": "Salida",
    "cafe": "Salida",
    "panadería": "Vianda",
    "panaderia": "Vianda",
    "ferretería": "Depto",
    "ferreteria": "Depto",
    "librería": "Compras",
    "libreria": "Compras",
    "estación de servicio": "Recurrente",
    "estacion de servicio": "Recurrente",
}

def _business_type_to_category(btype: str) -> str | None:
    return _BUSINESS_TYPE_TO_CATEGORY.get(btype.lower().strip())


async def _check_and_confirm_shop(phone: str, page_id: str, shop_name: str, current_cats: list):
    """Background task: busca tipo de negocio y pregunta al usuario si no es conocido."""
    business_type = await lookup_business_type(shop_name)
    if not business_type:
        return
    shop_key = shop_name.lower().strip()
    inferred_cat = _business_type_to_category(business_type)
    # Si la categoría inferida ya coincide con la actual, guardar silenciosamente sin preguntar
    if inferred_cat and inferred_cat in current_cats:
        shops = user_prefs.get("known_shops") or {}
        shops[shop_key] = business_type
        user_prefs["known_shops"] = shops
        await save_user_config(MY_NUMBER)
        return
    cur_state = pending_state.get(phone, {})
    if cur_state.get("type") == "undo_window" and cur_state.get("page_id") == page_id:
        pending_state[phone] = {
            "type": "confirm_known_shop",
            "shop_name": shop_name,
            "shop_key": shop_key,
            "inferred_type": business_type,
            "inferred_category": inferred_cat,
            "current_category": current_cats,
            "page_id": page_id,
        }
        await send_message(phone, f"¿_{shop_name}_ es {business_type}?")


async def _check_daily_hints(now) -> None:
    """Chequeos diarios para triggers #9 (provider recurrente) y #10 (planta sin riego).
    Encola hints en pending_hints_queue para que se emitan en la próxima interacción."""
    queue = user_prefs.setdefault("pending_hints_queue", [])

    # Trigger #9: proveedor Recurrente pagado 3+ meses seguidos → heads-up al cuarto
    try:
        from datetime import date as _date
        today = now.date()
        # Mirar últimos 4 meses
        start = today.replace(day=1) - timedelta(days=120)
        qfilter = QueryFilter(limit=200)
        qfilter.date_range = DateRange(start=start, end=today)
        recent = await _ds.query_expenses(qfilter)
        # Agrupar por nombre normalizado, contando meses distintos
        by_name = {}
        for e in recent:
            cats = getattr(e, "categories", None) or getattr(e, "categoria", None) or []
            if "Recurrente" not in (cats if isinstance(cats, list) else []):
                continue
            name = (e.name or "").strip()
            if not name:
                continue
            key = name.lower()
            d = getattr(e, "date", None)
            if d:
                try:
                    if isinstance(d, str):
                        d = _date.fromisoformat(d[:10])
                    months = by_name.setdefault(key, {"name": name, "months": set(), "last_day": 1})
                    months["months"].add((d.year, d.month))
                    months["last_day"] = max(months["last_day"], d.day)
                except Exception:
                    pass
        for key, info in by_name.items():
            if len(info["months"]) >= 3:
                # Heads-up: si hoy estamos cerca del día del mes habitual, encolar
                trigger_id = f"recurring_provider_{key[:20]}"
                hints_state = user_prefs.get("feature_hints") or {}
                if hints_state.get(trigger_id, {}).get("accepted") or hints_state.get(trigger_id, {}).get("dismissed_count", 0) >= 2:
                    continue
                # Solo encolar 3 dias antes del dia habitual
                if abs(info["last_day"] - today.day) <= 3 and not any(h.get("trigger_id") == trigger_id for h in queue):
                    queue.append({
                        "trigger_id": trigger_id,
                        "message": f"📅 *{info['name']}* suele venir por estos días. ¿Te aviso cuando llegue el mail? Decime *si* o *no*.",
                        "action_intent": "info_only",
                        "payload": {},
                    })
    except Exception:
        pass

    # Trigger #10: planta sin riego ≥14 días (solo si user activo: 2+ riegos registrados)
    try:
        if user_prefs.get("plant_water_count", 0) >= 2:
            plants = await _ds.query_plants() if hasattr(_ds, "query_plants") else []
            from datetime import date as _date
            today = now.date()
            for plant in plants:
                lw = getattr(plant, "last_watering", None)
                if not lw:
                    continue
                try:
                    if isinstance(lw, str):
                        lw = _date.fromisoformat(lw[:10])
                    days_since = (today - lw).days
                    if days_since >= 14:
                        trigger_id = f"plant_thirsty_{(plant.name or '').lower()[:20]}"
                        hints_state = user_prefs.get("feature_hints") or {}
                        if hints_state.get(trigger_id, {}).get("accepted") or hints_state.get(trigger_id, {}).get("dismissed_count", 0) >= 2:
                            continue
                        if not any(h.get("trigger_id") == trigger_id for h in queue):
                            queue.append({
                                "trigger_id": trigger_id,
                                "message": f"🌿 *{plant.name}* lleva {days_since} días sin riego registrado. ¿Te recuerdo cada lunes para que me digas si la regaste? Decime *si* o *no*.",
                                "action_intent": "enable_plant_reminder",
                                "payload": {"plant_name": plant.name},
                            })
                except Exception:
                    pass
    except Exception:
        pass


_ROUTINE_GEO_KEYWORDS = {"regar", "tomar", "darle de comer", "dar de comer", "alimentar",
                         "medicacion", "medicación", "medicamento", "pastilla", "remedio",
                         "estudiar", "leer", "meditar", "ejercicio", "hacer ejercicio"}


def _check_routine_geo_hint(phone: str, description: str, place_name: str) -> None:
    """Trigger #5: si el geo-reminder parece rutinario, ofrecer también recordatorio horario."""
    desc_l = (description or "").lower()
    if any(k in desc_l for k in _ROUTINE_GEO_KEYWORDS):
        emit_hint(phone, suggestion_gate.Hint(
            trigger_id="routine_geo_hint",
            message=f"⏰ Veo que es algo rutinario. Si querés, te puedo recordar todos los días a un horario fijo en vez de (o además de) cuando llegues a {place_name}. Decime *si* o *no*.",
            action_intent="enable_time_reminder",
            payload={"description": description, "place_name": place_name},
        ))


# ═══ Suggestion Gate — buffer de hints + helpers ════════════════════════════
hint_buffer: dict[str, "suggestion_gate.Hint"] = {}  # phone -> hint pendiente del turn
finance_query_log: dict[str, list] = {}  # phone -> [datetime, ...] consultas finanzas


def emit_hint(phone: str, hint: "suggestion_gate.Hint") -> None:
    """Un handler llama esto cuando detecta una oportunidad. El gate decide después si emitirla."""
    # Si ya hay uno bufferado, mantener el primero (no sobreescribir)
    if phone not in hint_buffer:
        hint_buffer[phone] = hint


async def maybe_fire_hint(phone: str) -> bool:
    """Llamar al final de process_single_item. Si hay hint válido, lo manda."""
    # Drenar la queue persistente (hints de jobs background como summaries)
    queue = user_prefs.get("pending_hints_queue") or []
    if queue and phone not in hint_buffer:
        item = queue.pop(0)
        user_prefs["pending_hints_queue"] = queue
        hint_buffer[phone] = suggestion_gate.Hint(
            trigger_id=item.get("trigger_id", ""),
            message=item.get("message", ""),
            action_intent=item.get("action_intent", ""),
            payload=item.get("payload", {}),
        )
    hint = hint_buffer.pop(phone, None)
    if not hint:
        return False
    hints_state = user_prefs.get("feature_hints") or {}
    today_count = suggestion_gate.count_today(hints_state)
    if not suggestion_gate.should_fire(hint, hints_state, today_count):
        return False
    suggestion_gate.record_suggested(hints_state, hint.trigger_id)
    user_prefs["feature_hints"] = hints_state
    pending_state[phone] = {
        "type": "hint_response",
        "trigger_id": hint.trigger_id,
        "action_intent": hint.action_intent,
        "payload": hint.payload,
    }
    await send_message(phone, hint.message)
    asyncio.create_task(save_user_config(MY_NUMBER))
    return True


async def lookup_business_type(name: str) -> str | None:
    """Busca en DuckDuckGo + Claude qué tipo de negocio es 'name'. Retorna ej: 'Supermercado'."""
    abstract = ""
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            r = await http.get(
                "https://api.duckduckgo.com/",
                params={"q": f"{name} Argentina", "format": "json", "no_html": "1", "skip_disambig": "1"},
            )
            data = r.json()
            abstract = data.get("AbstractText") or data.get("Answer") or ""
    except Exception:
        pass

    context = f'Resultado de búsqueda web: "{abstract}"' if abstract else "No hay resultado de búsqueda disponible."
    try:
        resp = await claude_create(
            model="claude-haiku-4-5-20251001", max_tokens=20,
            system="Respondé SOLO con el tipo de negocio más específico (ej: Supermercado, Farmacia, Restaurante, Librería, Ferretería, Estación de servicio, etc.) o 'desconocido'. Sin explicaciones.",
            messages=[{"role": "user", "content": f"¿Qué tipo de negocio es '{name}' en Argentina?\n{context}"}]
        )
        result = resp.content[0].text.strip()
        if result.lower() == "desconocido":
            return None
        return result
    except Exception:
        return None


# ── HELPERS RECONCILIACION DE FACTURAS ────────────────────────────────────────

import uuid as _uuid

def _pending_confirmations() -> list:
    return user_prefs.get("pending_invoice_confirmations") or []

async def _add_invoice_confirmation(situation: str, provider: str,
                                    finance_page_ids: list[str], gasto_page_id: str | None,
                                    paid_amount: float, invoice_amount: float | None) -> str:
    """Agrega una confirmación pendiente y persiste en Notion. Retorna el conf_id."""
    conf_id = str(_uuid.uuid4())[:8]
    conf = {
        "id":               conf_id,
        "situation":        situation,
        "provider":         provider,
        "paid_amount":      paid_amount,
        "invoice_amount":   invoice_amount,
        "finance_page_ids": finance_page_ids,
        "gasto_page_id":    gasto_page_id,
        "asked_count":      1,
        "last_asked_at":    now_argentina().isoformat(),
        "created_at":       now_argentina().isoformat(),
    }
    confs = user_prefs.setdefault("pending_invoice_confirmations", [])
    confs.append(conf)
    await save_user_config(MY_NUMBER)
    return conf_id

async def _remove_invoice_confirmation(conf_id: str) -> None:
    confs = user_prefs.get("pending_invoice_confirmations") or []
    user_prefs["pending_invoice_confirmations"] = [c for c in confs if c.get("id") != conf_id]
    await save_user_config(MY_NUMBER)

async def _auto_mark_invoice_paid(impaga, paid_amount: float, payment_method: str | None = None) -> None:
    """Marca la factura como pagada y su task asociada."""
    await _ds.mark_finance_paid(impaga.id, paid_amount, payment_method)
    tasks = await get_pending_factura_tasks()
    for t in tasks:
        if t.get("finance_page_id") == impaga.id:
            await mark_factura_task_paid(t["page_id"])
            break

async def _find_invoice_candidates(name_lower: str) -> list:
    """Busca facturas impagas que matcheen el proveedor por palabras clave."""
    provider_words = [w for w in name_lower.split() if len(w) > 3]
    # También buscar por service_providers (ej: "electricidad" → "CALF")
    service_providers = user_prefs.get("service_providers") or {}
    for alias, canonical in service_providers.items():
        if alias.lower() in name_lower:
            provider_words.append(canonical.lower())
    seen_ids = set()
    candidatos = []
    for pw in provider_words:
        for c in await _ds.get_impaga_facturas(provider=pw):
            if c.id not in seen_ids:
                seen_ids.add(c.id)
                candidatos.append(c)
    return candidatos


# ── MODULO GASTOS ──────────────────────────────────────────────────────────────

async def handle_gasto_agent(phone: str, text: str, image_b64=None, image_type=None, exchange_rate=1000.0, extra_images=None) -> str:
    now = now_argentina()
    tools = [{
        "name": "registrar_gasto",
        "description": "Registra un gasto o ingreso en Notion. Usa solo cuando tenes descripcion Y monto claros.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name":      {"type": "string", "description": "Descripcion corta del gasto"},
                "in_out":    {"type": "string", "enum": ["\u2192INGRESO\u2190", "\u2190 EGRESO \u2192"]},
                "value_ars": {"type": "number"},
                "categoria": {"type": "array", "items": {"type": "string"}},
                "date":      {"type": "string", "description": "YYYY-MM-DD"},
                "time":      {"type": ["string", "null"], "description": "HH:MM o null"},
                "litros":    {"type": ["number", "null"]},
                "notas":          {"type": ["string", "null"]},
                "client":         {"type": "array", "items": {"type": "string"}},
                "emoji":          {"type": "string"},
                "payment_method": {"type": ["string", "null"], "description": "Medio de pago: banco o tarjeta. Deducilo del ticket si menciona digitos de tarjeta. Null si no se puede determinar."}
            },
            "required": ["name", "in_out", "value_ars", "categoria", "date", "emoji"]
        }
    }]

    content = []
    if image_b64:
        content.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    for b64, itype in (extra_images or []):
        content.append({"type": "image", "source": {"type": "base64", "media_type": itype or "image/jpeg", "data": b64}})
    n_imgs = 1 + len(extra_images or []) if image_b64 else len(extra_images or [])
    content.append({"type": "text", "text": text or (f"(ver {n_imgs} imágenes adjuntas)" if n_imgs > 1 else "(ver imagen adjunta)")})

    profile_gastos = get_domain_profile("gastos")
    profile_gastos_ctx = f"\nPerfil de gastos del usuario: {profile_gastos}\n" if profile_gastos else ""
    providers = user_prefs.get("service_providers") or {}
    providers_ctx = ""
    if providers:
        prov_lines = [f"  - {k}: {v}" for k, v in providers.items()]
        providers_ctx = "\nProveedores de servicios configurados (usar para resolver nombres):\n" + "\n".join(prov_lines) + "\nSi el gasto menciona un proveedor conocido, usa el nombre canonico (ej: 'electricidad' → 'Electricidad - {nombre proveedor}').\n"
    pm_lines = []
    for pm in payment_methods_cache:
        parts = [f"  - {pm.name} ({pm.modality}"]
        if pm.bank:
            parts.append(f", {pm.bank}")
        if pm.last4:
            parts.append(f" ****{pm.last4}")
        if pm.owner:
            parts.append(f", de {pm.owner}")
        if pm.is_default:
            parts.append(", DEFAULT")
        parts.append(")")
        pm_lines.append("".join(parts))
    cards_ctx = ("\nMedios de pago registrados:\n" + "\n".join(pm_lines) + "\n") if pm_lines else ""
    known_shops = user_prefs.get("known_shops") or {}
    known_shops_ctx = ""
    if known_shops:
        shops_lines = "\n".join(f"  - {name}: {btype}" for name, btype in known_shops.items())
        known_shops_ctx = f"\nComercios conocidos del usuario (usar para categorizar correctamente):\n{shops_lines}\n"
    system = f"""Sos Knot, asistente personal por WhatsApp. Hablas en espanol rioplatense, natural y conciso.
Hoy: {hoy_str(now)}. Calendario: {semana_str(now)}.
Tasa dolar blue: ${exchange_rate:,.0f}/USD
{profile_gastos_ctx}{providers_ctx}{cards_ctx}{known_shops_ctx}
Tu tarea: registrar gastos e ingresos NUEVOS del usuario.
IMPORTANTE: Si el mensaje habla de corregir, editar, cambiar o actualizar algo ya registrado -> NO uses la tool, respondé que no podés hacer correcciones desde acá.
- Si el mensaje tiene descripcion Y monto -> usa la tool registrar_gasto directamente.
- Si hay una imagen (ticket, screenshot de pedido, factura) -> lee TODOS los items, suma los montos vos mismo, y registra el total. No le pidas al usuario que sume.
- Si el ticket o recibo muestra digitos de tarjeta, deducí el payment_method comparando con los medios de pago (ultimos 4 digitos → campo Last4). Si no hay coincidencia exacta pero se ve el banco, usá el método DEFAULT de ese banco.
- Si el usuario menciona un banco sin especificar modalidad, usá el método marcado DEFAULT para ese banco. Si no hay DEFAULT y hay múltiples opciones, igual poné el nombre del banco en payment_method (ej: "BBVA") y el sistema preguntará al usuario cuál usar.
- Si el usuario NO menciona ningún método de pago (ni banco, ni efectivo, ni transferencia, ni dígitos de tarjeta) -> dejá payment_method en null. El sistema le va a preguntar despues. NUNCA inventes "Cash" o "Efectivo" si el usuario no lo dijo explícitamente.
- Si falta el monto Y no hay imagen de donde sacarlo -> pregunta de forma natural y breve.
- Si hay ambiguedad (ej: "compre algo" sin monto ni imagen) -> pregunta que fue y cuanto.

Categorias disponibles: Supermercado, Sueldo, Recurrente, Servicio, Transporte, Vianda, Salud, Salud Mental, Salida, Birra, Ocio, Compras, Depto, Plantas, Viajes, Venta.
Recurrente = pagos que se repiten todos los meses (alquiler, luz, gas, internet, streaming, gimnasio, suscripciones fijas). Servicio = pago puntual de un servicio no recurrente (factura extra, credito adicional, uso puntual — ej: Anthropic extra usage, multa, servicio tecnico). Depto = compras fisicas para el depto (muebles, materiales, herramientas).
Si in_out es INGRESO -> categoria solo puede ser Sueldo o Venta.
Clientes posibles: LBL, OPERA, ALPATACO, Juan Martin, Depto, Work, Santi Vales, Jorge, Barbara, Vanguardia, Alejo, Dinamo, Paula Diaz, Labti, PlanA, JGA, ATE.
Emoji: elegi el mas especifico segun el contexto real."""

    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=1000,
        system=system,
        messages=[{"role": "user", "content": content}],
        tools=tools
    )

    _hist_text = text or "(imagen)"
    if response.stop_reason == "end_turn":
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "Error procesando").strip()
        add_to_history(phone, "user", _hist_text)
        add_to_history(phone, "assistant", reply)
        return reply

    tool_blocks = [b for b in response.content if b.type == "tool_use"]
    if not tool_blocks:
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "Error procesando").strip()
        add_to_history(phone, "user", _hist_text)
        add_to_history(phone, "assistant", reply)
        return reply

    all_tool_results = []
    created_entries = []
    for tool_block in tool_blocks:
        data = dict(tool_block.input)
        final_cats, cat_note = await check_and_apply_category(data.get("name", ""), data.get("categoria", []))
        data["categoria"] = final_cats

        success_i, result_i = await create_notion_entry(data, exchange_rate)

        if success_i:
            usd = data["value_ars"] / exchange_rate
            tr = (
                f"Registrado exitosamente en Notion. "
                f"Nombre: {data['name']}, "
                f"Monto: ${data['value_ars']:,.0f} ARS (USD {usd:.2f}), "
                f"Categoria: {', '.join(data['categoria'])}, "
                f"Fecha: {data['date']}, "
                f"Cambio usado: ${exchange_rate:,.0f}/USD."
            )
            if cat_note:
                tr += f" {cat_note}"
            created_entries.append((result_i, data, True))
            cats = data.get("categoria", [])
            event_desc = f"Registró: {data['name']}, ${data['value_ars']:,.0f} ARS, categoría: {', '.join(cats)}, fecha: {data['date']}"
            asyncio.create_task(update_domain_profile_bg("gastos", event_desc))
            if any(c in cats for c in ("Salud", "Salud Mental")):
                asyncio.create_task(update_domain_profile_bg("salud", event_desc))
            if any(c in cats for c in ("Salida", "Birra", "Ocio", "Viajes")):
                asyncio.create_task(update_domain_profile_bg("social", event_desc))
            if any(c in cats for c in ("Depto", "Plantas")):
                asyncio.create_task(update_domain_profile_bg("hogar", event_desc))
        else:
            tr = f"Error al guardar en Notion: {result_i[:200]}"
            created_entries.append((None, data, False))

        all_tool_results.append({
            "type": "tool_result",
            "tool_use_id": tool_block.id,
            "content": tr
        })

    # Texto libre que Claude puso ANTES de llamar la tool (ej: aclaración, pregunta)
    pre_text = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()

    if not created_entries:
        reply = pre_text or "No encontré nada para registrar."
    elif all(not ok for _, _, ok in created_entries):
        reply = "No pude registrar el gasto en Notion."
    else:
        lines = []
        for _, data, ok in created_entries:
            if not ok:
                lines.append(f"❌ No pude registrar *{data.get('name', '?')}*.")
                continue
            usd = data["value_ars"] / exchange_rate
            cats = data.get("categoria") or []
            pm = (data.get("payment_method") or "").strip()
            # Solo mostrar pm si efectivamente matcheó con un metodo conocido
            pm_matched = False
            if pm:
                pm_lower = pm.lower()
                pm_matched = any(
                    (p.last4 and p.last4 in pm_lower)
                    or (p.name and p.name.lower() in pm_lower)
                    or (p.bank and p.bank.lower() in pm_lower)
                    or (p.modality and p.modality.lower() in pm_lower)
                    for p in payment_methods_cache
                )
            line = f"✅ *{data['name']}* — ${data['value_ars']:,.0f} ARS"
            if abs(usd - data["value_ars"]) > 1:
                line += f" (USD {usd:.2f})"
            if cats:
                line += f" · _{', '.join(cats)}_"
            if pm and pm_matched:
                line += f" · {pm}"
            lines.append(line)
        if pre_text:
            lines.insert(0, pre_text)
        reply = "\n".join(lines)

    # Pending state solo cuando hay un unico gasto en el mensaje
    if len(created_entries) == 1:
        page_id, data, success = created_entries[0]
        if success and page_id:
            name_lower = data.get("name", "").lower()
            cats_lower = [c.lower() for c in (data.get("categoria") or [])]
            # Combustible solo si: emoji ⛽ Y categoría Transporte, O keywords explícitas Y Transporte
            is_fuel = "transporte" in cats_lower and (
                data.get("emoji") == "⛽" or any(k in name_lower for k in FUEL_KEYWORDS)
            )
            if is_fuel and not data.get("litros"):
                pending_state[phone] = {"type": "litros_followup", "page_id": page_id, "name": data["name"]}
                reply += "\n\n⛽ Cuantos litros cargaste?"
            elif data.get("value_ars", 0) > 1000 and "EGRESO" in data.get("in_out", "").upper():
                paid_amount = data.get("value_ars", 0)
                payment_method = data.get("payment_method")
                candidatos = await _find_invoice_candidates(name_lower)

                if len(candidatos) == 1:
                    impaga = candidatos[0]
                    inv_amount = impaga.value_ars or 0
                    diff_pct = abs(paid_amount - inv_amount) / max(inv_amount, 1) if inv_amount else 1

                    if diff_pct <= 0.10:
                        # Auto-marca sin preguntar
                        await _auto_mark_invoice_paid(impaga, paid_amount, payment_method)
                        reply += f"\n\n✅ Marqué *{impaga.name}* como pagada."
                        # Trigger #6: pago con apuro (≤2 días al vencimiento)
                        try:
                            due = getattr(impaga, "due_date", None)
                            from datetime import date as _date
                            if isinstance(due, str):
                                due = _date.fromisoformat(due[:10])
                            if isinstance(due, _date) and -1 <= (due - now.date()).days <= 2:
                                hints_state = user_prefs.setdefault("feature_hints", {})
                                s = hints_state.get("apuro_factura", {})
                                s["apuro_count"] = s.get("apuro_count", 0) + 1
                                hints_state["apuro_factura"] = s
                                if s["apuro_count"] >= 2:
                                    emit_hint(phone, suggestion_gate.Hint(
                                        trigger_id="apuro_factura",
                                        message="⏰ Veo que las últimas facturas las pagás sobre la fecha. ¿Querés que te avise 5 días antes la próxima? Decime *si* o *no*.",
                                        action_intent="enable_factura_early_warning",
                                        payload={},
                                    ))
                        except Exception:
                            pass

                    elif diff_pct <= 0.30:
                        # Pide confirmación — podría ser la misma factura
                        conf_id = await _add_invoice_confirmation(
                            "diff_moderate", impaga.name, [impaga.id], page_id, paid_amount, inv_amount
                        )
                        pending_state[phone] = {
                            "type": "factura_confirm", "situation": "diff_moderate",
                            "conf_id": conf_id, "finance_page_id": impaga.id,
                            "paid_amount": paid_amount, "payment_method": payment_method,
                            "provider_name": impaga.name,
                        }
                        reply += f"\n\n💡 Tenés una factura de *{impaga.name}* por ${inv_amount:,.0f}. ¿Este pago de ${paid_amount:,.0f} corresponde a esa? (sí/no)"

                    else:
                        # Diff grande — registra el gasto pero no toca la factura
                        conf_id = await _add_invoice_confirmation(
                            "diff_large", impaga.name, [impaga.id], page_id, paid_amount, inv_amount
                        )
                        pending_state[phone] = {
                            "type": "factura_confirm", "situation": "diff_large",
                            "conf_id": conf_id, "finance_page_id": impaga.id,
                            "paid_amount": paid_amount, "payment_method": payment_method,
                            "provider_name": impaga.name,
                        }
                        reply += f"\n\n⚠️ La factura de *{impaga.name}* era ${inv_amount:,.0f} pero pagaste ${paid_amount:,.0f}. ¿Fue un pago parcial? (sí/no)"

                elif len(candidatos) > 1:
                    # Múltiples facturas — pregunta cuál
                    options = "\n".join(f"• {c.name} (${c.value_ars:,.0f})" for c in candidatos[:3])
                    conf_id = await _add_invoice_confirmation(
                        "multiple_invoices", candidatos[0].name,
                        [c.id for c in candidatos[:3]], page_id, paid_amount, None
                    )
                    pending_state[phone] = {
                        "type": "factura_confirm", "situation": "multiple_invoices",
                        "conf_id": conf_id,
                        "candidates": [{"id": c.id, "name": c.name, "amount": c.value_ars} for c in candidatos[:3]],
                        "paid_amount": paid_amount, "payment_method": payment_method,
                    }
                    reply += f"\n\n💡 Tenés varias facturas pendientes:\n{options}\n¿A cuál corresponde este pago?"

                else:
                    expires_at = (now_argentina() + timedelta(seconds=60)).replace(tzinfo=None).isoformat()
                    pending_state[phone] = {
                        "type": "undo_window", "action": "expense",
                        "page_id": page_id, "name": data.get("name", "gasto"),
                        "expires_at": expires_at,
                    }
                    reply += "\n\n_Si algo no quedó bien, avisame._"
            else:
                expires_at = (now_argentina() + timedelta(seconds=60)).replace(tzinfo=None).isoformat()
                pending_state[phone] = {
                    "type": "undo_window", "action": "expense",
                    "page_id": page_id, "name": data.get("name", "gasto"),
                    "expires_at": expires_at,
                }
                reply += "\n\n_Si algo no quedó bien, avisame._"

    # Trigger #4: primer gasto categoría Transporte → soft-introduce $/L tracking
    if len(created_entries) == 1:
        _p, _d, _s = created_entries[0]
        if _s and "Transporte" in (_d.get("categoria") or []):
            emit_hint(phone, suggestion_gate.Hint(
                trigger_id="first_combustible",
                message="💡 Si la próxima cargas combustible y me decís los litros, te puedo trackear el precio por litro mes a mes en Notion.",
                action_intent="info_only",
                payload={},
            ))

    # Trigger #1: 3+ gastos del mismo nombre en misma franja horaria semanal
    if len(created_entries) == 1:
        _p, _d, _s = created_entries[0]
        if _s and _d.get("name") and "EGRESO" in (_d.get("in_out") or "").upper():
            try:
                from datetime import date as _date
                today = now.date()
                start = today - timedelta(days=28)
                qfilter = QueryFilter(name_contains=_d["name"][:30], limit=20)
                qfilter.date_range = DateRange(start=start, end=today)
                similar = await _ds.query_expenses(qfilter)
                # Filtrar por mismo weekday y hora ±1h
                target_wd = now.weekday()
                target_hr = now.hour
                matches = []
                for e in similar:
                    e_dt = getattr(e, "date", None)
                    if not e_dt:
                        continue
                    try:
                        if hasattr(e_dt, "weekday"):
                            ewd = e_dt.weekday()
                            ehr = getattr(e_dt, "hour", target_hr)
                        else:
                            continue
                    except Exception:
                        continue
                    if ewd == target_wd and abs(ehr - target_hr) <= 1:
                        matches.append(e)
                if len(matches) >= 3:
                    weekday_es = ["lunes","martes","miércoles","jueves","viernes","sábado","domingo"][target_wd]
                    emit_hint(phone, suggestion_gate.Hint(
                        trigger_id=f"recurring_pattern_{_d['name'][:20].lower().replace(' ','_')}",
                        message=f"📅 Veo que registrás *{_d['name']}* los {weekday_es} a esta hora seguido. ¿Lo agendo en el calendario para avisarte? Decime *si* o *no*.",
                        action_intent="create_recurring_event",
                        payload={"name": _d["name"], "weekday": target_wd, "hour": target_hr},
                    ))
            except Exception:
                pass

    # Trigger #6: facturas pagadas con apuro (≤2 días al vencimiento)
    # Se chequea cuando el código encuentra una impaga match (revisar bloque mark_finance_paid abajo)

    # Ask payment method if missing OR if agent invented one not in cache
    if len(created_entries) == 1:
        _page_id, _data, _success = created_entries[0]
        _pm = (_data.get("payment_method") or "").strip()
        _pm_matched = False
        if _pm:
            _pm_l = _pm.lower()
            _pm_matched = any(
                (p.last4 and p.last4 in _pm_l)
                or (p.name and p.name.lower() in _pm_l)
                or (p.bank and p.bank.lower() in _pm_l)
                or (p.modality and p.modality.lower() in _pm_l)
                for p in payment_methods_cache
            )
        if (
            _success and _page_id
            and "EGRESO" in (_data.get("in_out") or "").upper()
            and not _pm_matched
            and pending_state.get(phone, {}).get("type") == "undo_window"
        ):
            opts = []
            seen = set()
            for pm in payment_methods_cache:
                key = (pm.name or pm.bank or "").lower()
                if key and key not in seen:
                    seen.add(key)
                    opts.append(pm.name or pm.bank)
            opts_str = ", ".join(opts[:8]) if opts else "Efectivo, BBVA, Mercado Pago, Transferencia"
            pending_state[phone] = {
                "type": "ask_payment_method",
                "page_id": _page_id,
                "name": _data.get("name", "gasto"),
            }
            reply += f"\n\n💳 ¿Con qué pagaste? ({opts_str})"

    # Known shop detection: si el EGRESO es de un comercio no reconocido, buscar y confirmar
    if len(created_entries) == 1:
        _page_id, _data, _success = created_entries[0]
        if _success and _page_id and "EGRESO" in (_data.get("in_out") or "").upper():
            _shop_name = _data.get("name", "")
            _shop_key = _shop_name.lower().strip()
            _known_shops = user_prefs.get("known_shops") or {}
            _cats = _data.get("categoria") or []
            _is_fuel = _data.get("emoji") == "⛽" or any(k in _shop_key for k in FUEL_KEYWORDS)
            if (
                _shop_key not in _known_shops
                and not _is_fuel
                and "Recurrente" not in _cats
                and "Sueldo" not in _cats
                and pending_state.get(phone, {}).get("type") == "undo_window"
            ):
                asyncio.create_task(_check_and_confirm_shop(phone, _page_id, _shop_name, _cats))

    # Unknown card detection: si el payment_method tiene dígitos no registrados, preguntar
    # undo_window no bloquea esta detección (es el estado más común post-registro)
    if len(created_entries) == 1:
        _, data_entry, success_entry = created_entries[0]
        _cur_state_type = pending_state.get(phone, {}).get("type")
        if success_entry and data_entry.get("payment_method") and _cur_state_type in (None, "undo_window"):
            import re as _re
            pm = data_entry["payment_method"]
            digits = _re.findall(r'\d{4}', pm)
            if digits:
                last4 = digits[-1]
                is_known = any(
                    pm_entry.last4 == last4 or (pm_entry.name and pm_entry.name.lower() in pm.lower())
                    for pm_entry in payment_methods_cache
                )
                if not is_known:
                    pending_state[phone] = {"type": "unknown_card_register", "last4": last4}
                    reply += f"\n\n💳 Vi una tarjeta terminada en *{last4}* que no tengo registrada. ¿De qué banco es y es débito o crédito?"

    # Banco sin modalidad: si payment_method es solo nombre de banco (sin dígitos) y hay múltiples opciones
    if len(created_entries) == 1 and pending_state.get(phone, {}).get("type") in (None, "undo_window"):
        _, data_entry, success_entry = created_entries[0]
        pm = (data_entry.get("payment_method") or "").strip()
        if success_entry and pm and not any(c.isdigit() for c in pm):
            matching = [e for e in payment_methods_cache if e.bank and e.bank.lower() == pm.lower()]
            if len(matching) > 1:
                default = next((e for e in matching if e.is_default), None)
                if not default:
                    page_id_entry = created_entries[0][0]
                    opts = ", ".join(f"*{e.modality}*" + (f" ****{e.last4}" if e.last4 else "") for e in matching)
                    pending_state[phone] = {
                        "type": "select_payment_method",
                        "page_id": page_id_entry,
                        "bank": pm,
                        "options": [{"modality": e.modality, "last4": e.last4, "name": e.name} for e in matching],
                    }
                    reply += f"\n\n💳 ¿Con qué método de *{pm}* pagaste? {opts}"

    add_to_history(phone, "user", _hist_text)
    add_to_history(phone, "assistant", reply)
    return reply


async def create_notion_entry(data: dict, exchange_rate: float) -> tuple[bool, str]:
    if not data.get("value_ars") or not data.get("in_out"):
        return False, "No se pudo interpretar"
    try:
        notes = data.get("notas") or None
        pm_str = (data.get("payment_method") or "").lower()
        matched_pm = None
        if pm_str and payment_methods_cache:
            matched_pm = next((
                pm for pm in payment_methods_cache
                if (pm.last4 and pm.last4 in pm_str)
                or (pm.name and pm.name.lower() in pm_str)
                or (pm.bank and pm.bank.lower() in pm_str)
                or (pm.modality and pm.modality.lower() in pm_str)
            ), None)
        entry = await _ds.create_expense({
            "name":             data["name"],
            "in_out":           data["in_out"],
            "value_ars":        data["value_ars"],
            "exchange_rate":    exchange_rate,
            "categories":       data.get("categoria"),
            "date":             data.get("date"),
            "time":             data.get("time"),
            "client":           data.get("client"),
            "liters":           data.get("litros"),
            "consumo_kwh":      data.get("consumo_kwh"),
            "notes":            notes or None,
            "emoji":            data.get("emoji"),
            "payment_method_id": matched_pm.id if matched_pm else None,
        })
        last_touched[MY_NUMBER] = {"page_id": entry.id, "name": data["name"]}
        if matched_pm:
            asyncio.create_task(_ds.increment_payment_method_uses(matched_pm.id, matched_pm.uses))
            matched_pm.uses += 1
        return True, entry.id
    except Exception as e:
        return False, str(e)
async def check_and_apply_category(name: str, predicted_cats: list[str]) -> tuple[list[str], str | None]:
    name_lower = name.lower()
    for keyword, saved_cats in category_overrides.items():
        if keyword in name_lower:
            if saved_cats != predicted_cats:
                return saved_cats, f"Categoria: {', '.join(saved_cats)} (segun tu correccion anterior)"
            return saved_cats, None
    try:
        cats, changed = await _ds.find_category_from_history(name, predicted_cats)
        if changed:
            search_key = " ".join(name.split()[:3]).lower()
            category_overrides[search_key] = cats
            return cats, f"Categoria: {', '.join(cats)} (como en cargas anteriores)"
    except Exception:
        pass
    return predicted_cats, None

async def corregir_gasto(text: str, phone: str = None) -> tuple[bool, str]:
    now = now_argentina()
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extrae que gasto corregir y que cambiar. Si el mensaje no menciona un nombre concreto, usa null en search_term. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Hoy: {now.strftime("%Y-%m-%d")}
Mensaje: {text}
Responde:
{{"search_term": "nombre del gasto o null si no se menciona uno concreto",
  "all_matching": true si hay que corregir TODOS los que coinciden (ej: 'los 3 de anthropic', 'todos los de hoy'), false si es uno solo,
  "date_filter": "YYYY-MM-DD" si el usuario menciona 'hoy', 'ayer', una fecha concreta, null si no especifica fecha,
  "new_value_ars": nuevo monto en ARS o null,
  "new_categoria": ["categoria"] o null,
  "new_name": "nuevo nombre" o null,
  "new_notes": "nueva nota" o null,
  "new_emoji": emoji nuevo (un solo caracter) o null. Ej: si dice 'ponele uno de supermercado' -> '🛒', 'cerveza' -> '🍺', 'comida' -> '🍽️'}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    intent = json.loads(raw)

    search_term = intent.get("search_term")
    all_matching = intent.get("all_matching", False)
    date_filter = intent.get("date_filter")
    page_id_direct = None

    if not search_term and phone and phone in last_touched:
        entry = last_touched[phone]
        page_id_direct = entry["page_id"]
        search_term = entry["name"]
    elif not search_term:
        return False, "No entendi que gasto queres corregir"

    updates = {}
    if intent.get("new_value_ars"):
        updates["value_ars"] = float(intent["new_value_ars"])
    if intent.get("new_categoria"):
        updates["categories"] = intent["new_categoria"]
    if intent.get("new_name"):
        updates["name"] = intent["new_name"]
    if intent.get("new_notes") is not None:
        updates["notes"] = intent["new_notes"]
    if intent.get("new_emoji"):
        updates["emoji"] = intent["new_emoji"]
    if not updates:
        return False, "No entendi que campo queres cambiar"

    if page_id_direct and not all_matching:
        entries_to_update = [type("E", (), {"id": page_id_direct, "name": search_term, "value_ars": 0})()]
    else:
        limit = 20 if all_matching else 1
        qfilter = QueryFilter(name_contains=search_term, limit=limit)
        from datetime import date as _date
        if date_filter:
            try:
                d = _date.fromisoformat(date_filter)
                qfilter.date_range = DateRange(start=d, end=d)
            except Exception:
                pass
        elif all_matching:
            # Sin fecha explícita en batch → acotar a hoy para no tocar historial
            today = now.date()
            qfilter.date_range = DateRange(start=today, end=today)
        results = await _ds.query_expenses(qfilter)
        if not results:
            return False, f"No encontre ningun gasto llamado _{search_term}_"
        entries_to_update = results

    # Confirmación obligatoria para cambios masivos (más de 1 entrada)
    if len(entries_to_update) > 1 and phone:
        changes_preview = []
        if intent.get("new_value_ars"):
            changes_preview.append(f"monto → *${float(intent['new_value_ars']):,.0f} ARS*")
        if intent.get("new_categoria"):
            changes_preview.append(f"categoría → _{', '.join(intent['new_categoria'])}_")
        if intent.get("new_name"):
            changes_preview.append(f"nombre → _{intent['new_name']}_")
        if intent.get("new_notes") is not None:
            changes_preview.append(f"nota → _{intent['new_notes']}_")
        names_preview = "\n".join(f"  • {e.name} (${e.value_ars:,.0f})" for e in entries_to_update)
        pending_state[phone] = {
            "type": "bulk_correction_confirm",
            "entries": [{"id": e.id, "name": e.name} for e in entries_to_update],
            "updates": updates,
            "search_term": search_term,
        }
        return True, (
            f"Voy a modificar *{len(entries_to_update)} entradas* ({', '.join(changes_preview)}):\n"
            f"{names_preview}\n\n¿Confirmás?"
        )

    errors = []
    updated = 0
    for entry in entries_to_update:
        try:
            await _ds.update_expense(entry.id, updates)
            updated += 1
            if phone:
                last_touched[phone] = {"page_id": entry.id, "name": intent.get("new_name") or entry.name}
        except Exception as e:
            errors.append(str(e)[:80])

    if errors and not updated:
        return False, f"Error actualizando en Notion: {errors[0]}"

    changes = []
    if intent.get("new_value_ars"):
        changes.append(f"Monto -> *${float(intent['new_value_ars']):,.0f} ARS*")
    if intent.get("new_categoria"):
        changes.append(f"Categoria -> _{', '.join(intent['new_categoria'])}_")
    if intent.get("new_name"):
        changes.append(f"Nombre -> _{intent['new_name']}_")
    if intent.get("new_notes") is not None:
        changes.append(f"Nota -> _{intent['new_notes']}_")

    names = list({e.name for e in entries_to_update[:updated]})
    if updated > 1:
        label = f"*{updated}* entradas de _{names[0] if len(names)==1 else search_term}_"
    else:
        label = f"*{entries_to_update[0].name}*"
    return True, f"✅ {label} corregida{'s' if updated > 1 else ''}\n" + "\n".join(changes)

async def eliminar_gasto(text: str, phone: str = None) -> tuple[bool, str]:
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=100,
        system="Extrae el nombre de la entrada de Notion a eliminar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f'Mensaje: {text}\nResponde: {{"search_term": "nombre de la entrada a eliminar"}}'}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    search_term = json.loads(raw).get("search_term", "")
    if not search_term:
        return False, "No entendi que entrada queres eliminar"

    results = await _ds.query_expenses(QueryFilter(name_contains=search_term, limit=1))
    if not results:
        return False, f"No encontre ninguna entrada llamada _{search_term}_"
    entry = results[0]
    if phone:
        expires_at = (now_argentina() + timedelta(minutes=5)).replace(tzinfo=None).isoformat()
        pending_state[phone] = {
            "type": "confirm_delete", "action": "expense",
            "page_id": entry.id, "name": entry.name, "expires_at": expires_at,
        }
        await send_interactive_buttons(phone, f"¿Eliminás *{entry.name}*?", [
            {"id": "confirm_delete_yes", "title": "Sí, eliminalo"},
            {"id": "confirm_delete_no", "title": "No, cancelar"},
        ])
        return True, ""
    ok = await _ds.archive_expense(entry.id)
    return (True, f"*{entry.name}* eliminado de Notion") if ok else (False, "Error al eliminar")

async def eliminar_shopping(text: str) -> tuple[bool, str]:
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=100,
        system="Extrae el nombre del item de la lista de compras a eliminar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"Mensaje: {text}\nResponde: {{\"search_term\": \"nombre del item\"}}"}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    search_term = json.loads(raw).get("search_term", "")
    if not search_term:
        return False, "No entendi que item queres eliminar"
    results = await _ds.search_shopping_item(search_term)
    if not results:
        return False, f"No encontre ningun item llamado _{search_term}_ en la lista"
    item = results[0]
    ok = await _ds.archive_shopping_item(item.id)
    if ok:
        return True, f"*{item.name}* eliminado de la lista de compras"
    return False, "Error eliminando el item"

async def corregir_shopping(text: str) -> tuple[bool, str]:
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=200,
        system="Extrae el item de la lista de compras a corregir y los campos a actualizar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f'Mensaje: {text}\nResponde: {{"search_term": "nombre del item", "updates": {{"notes": "nueva cantidad/nota o null", "category": "nueva categoria o null"}}}}'}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        parsed = json.loads(raw)
    except Exception:
        return False, "No entendi qué item querés corregir"
    search_term = parsed.get("search_term", "")
    updates_raw = parsed.get("updates", {})
    updates = {k: v for k, v in updates_raw.items() if v is not None}
    if not search_term:
        return False, "No entendi qué item querés corregir"
    results = await _ds.search_shopping_item(search_term)
    if not results:
        return False, f"No encontré ningún item llamado _{search_term}_ en la lista"
    item = results[0]
    if not updates:
        return False, f"No entendi qué querés cambiar de _{item.name}_"
    ok = await _ds.update_shopping_item(item.id, updates)
    if ok:
        changes = ", ".join(f"{k}: {v}" for k, v in updates.items())
        return True, f"*{item.name}* actualizado: {changes}"
    return False, "Error actualizando el item"


async def cancelar_recordatorio(text: str) -> tuple[bool, str]:
    access_token = await get_gcal_access_token()
    if not access_token:
        return False, "Calendar no configurado"
    now = now_argentina()
    time_min = now.strftime("%Y-%m-%dT%H:%M:00-03:00")
    time_max = (now + timedelta(days=7)).strftime("%Y-%m-%dT23:59:59-03:00")
    async with httpx.AsyncClient() as http:
        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"timeMin": time_min, "timeMax": time_max, "singleEvents": "true", "maxResults": "20"},
        )
    if r.status_code != 200:
        return False, "No pude acceder al calendario"
    temp_events = [e for e in r.json().get("items", []) if "[TEMP]" in (e.get("description") or "")]
    if not temp_events:
        return False, "No tenés recordatorios pendientes"
    if len(temp_events) == 1:
        ev = temp_events[0]
        event_id = ev["id"]
        summary = ev.get("summary", "Recordatorio")
        start = ev.get("start", {}).get("dateTime", "")
        try:
            dt = datetime.fromisoformat(start).astimezone()
            hora = dt.strftime("%H:%M")
        except Exception:
            hora = ""
        async with httpx.AsyncClient() as http:
            dr = await http.delete(
                f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                headers={"Authorization": f"Bearer {access_token}"},
            )
        if dr.status_code in (200, 204):
            return True, f"Recordatorio *{summary}*{f' ({hora})' if hora else ''} cancelado"
        return False, "Error cancelando el recordatorio"
    # múltiples: identificar cuál con Claude
    summaries = [(e["id"], e.get("summary", ""), e.get("start", {}).get("dateTime", "")) for e in temp_events]
    options_str = "\n".join(f'{i+1}. {s} — {t}' for i, (_, s, t) in enumerate(summaries))
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=10,
        system="Respondé SOLO con el número de la opción más probable. Sin texto extra.",
        messages=[{"role": "user", "content": f"Mensaje del usuario: {text}\n\nRecordatorios pendientes:\n{options_str}\n\n¿Cuál quiere cancelar? Respondé solo el número."}]
    )
    try:
        idx = int(response.content[0].text.strip()) - 1
        ev_id, ev_summary, ev_start = summaries[idx]
    except Exception:
        list_str = "\n".join(f"- {s}" for _, s, _ in summaries)
        return False, f"Tenés {len(summaries)} recordatorios pendientes:\n{list_str}\n\nEspecificá cuál cancelar."
    try:
        dt = datetime.fromisoformat(ev_start).astimezone()
        hora = dt.strftime("%H:%M")
    except Exception:
        hora = ""
    async with httpx.AsyncClient() as http:
        dr = await http.delete(
            f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{ev_id}",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    if dr.status_code in (200, 204):
        return True, f"Recordatorio *{ev_summary}*{f' ({hora})' if hora else ''} cancelado"
    return False, "Error cancelando el recordatorio"


# ── MODULO PLANTAS ─────────────────────────────────────────────────────────────
PLANTA_SYSTEM = """Extrae info de una planta y genera recomendaciones de cuidado.
Responde UNICAMENTE con JSON valido, sin markdown.
Valores para "luz": Sombra, Indirecta, Directa parcial, Pleno sol
Valores para "riego": Cada 2-3 dias, Semanal, Quincenal, Mensual
Valores para "ubicacion": Interior, Exterior, Balcon, Terraza
Valores para "estado": Excelente, Bien, Regular, Necesita atencion"""

async def parse_planta(text: str, exchange_rate: float) -> dict:
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=600,
        system=PLANTA_SYSTEM,
        messages=[{"role": "user", "content": f"""Hoy: {now_argentina().strftime("%Y-%m-%d")}. Dolar: ${exchange_rate:,.0f}
Mensaje: {text}
Responde:
{{"name":"nombre comun","especie":"nombre cientifico o null","fecha_compra":"YYYY-MM-DD","precio":numero o null,"luz":"Indirecta","riego":"Semanal","ubicacion":"Interior","estado":"Bien","emoji":"emoji planta","notas":"2-3 consejos concisos de cuidado"}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

async def create_planta(data: dict) -> tuple[bool, str]:
    try:
        plant = await _ds.create_plant({
            "name":          data.get("name"),
            "species":       data.get("especie"),
            "purchase_date": data.get("fecha_compra"),
            "price":         data.get("precio"),
            "light":         data.get("luz"),
            "watering":      data.get("riego"),
            "location":      data.get("ubicacion"),
            "status":        data.get("estado"),
            "notes":         data.get("notas"),
            "emoji":         data.get("emoji"),
        })
        asyncio.create_task(update_domain_profile_bg(
            "hogar",
            f"Nueva planta: {data.get('name')}, especie: {data.get('especie') or 'desconocida'}, ubicación: {data.get('ubicacion') or '-'}"
        ))
        return True, plant.id
    except Exception as e:
        return False, error_servicio("notion")

def format_planta(data: dict) -> str:
    emoji = data.get("emoji", "\U0001f33f")
    lines = [
        f"{emoji} *{data['name']}*",
        f"Especie: _{data.get('especie') or 'desconocida'}_",
        f"Luz: {data.get('luz', '-')}",
        f"Riego: {data.get('riego', '-')}",
        f"Ubicacion: {data.get('ubicacion', '-')}",
    ]
    if data.get("notas"):
        lines.append(f"\n{data['notas']}")
    lines.append("\nGuardada en Notion")
    return "\n".join(lines)

async def editar_planta(text: str) -> tuple[bool, str]:
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=200,
        system="Extrae el nombre de la planta a editar y los campos a actualizar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": (
            f"Mensaje: {text}\n"
            'Responde: {"search_term": "nombre de la planta", "updates": {"status": "Excelente|Bien|Regular|Necesita atencion o null", '
            '"watering": "Cada 2-3 dias|Semanal|Quincenal|Mensual o null", '
            '"location": "Interior|Exterior|Balcon|Terraza o null", '
            '"light": "Sombra|Indirecta|Directa parcial|Pleno sol o null", '
            '"notes": "nuevas notas o null", '
            '"watered_today": true si el mensaje indica que la regó (regué, le di agua, regado, ya está regada) hoy, sino false}}'
        )}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        parsed = json.loads(raw)
    except Exception:
        return False, "No entendi qué planta querés editar"
    search_term = parsed.get("search_term", "")
    raw_updates = parsed.get("updates", {})
    watered_today = bool(raw_updates.pop("watered_today", False)) if isinstance(raw_updates, dict) else False
    updates = {k: v for k, v in raw_updates.items() if v is not None}
    if watered_today:
        updates["last_watering"] = now_argentina().date().isoformat()
        # Trackear cantidad de riegos para activar trigger #10
        cnt = user_prefs.get("plant_water_count", 0) + 1
        user_prefs["plant_water_count"] = cnt
    if not search_term:
        return False, "No entendi qué planta querés editar"
    results = await _ds.search_plants(search_term)
    if not results:
        return False, f"No encontré ninguna planta llamada _{search_term}_"
    plant = results[0]
    if not updates:
        return False, f"No entendi qué querés cambiar de _{plant.name}_"
    await _ds.update_plant(plant.id, updates)
    if watered_today:
        return True, f"💧 *{plant.name}* regada hoy."
    changes = ", ".join(f"{k}: {v}" for k, v in updates.items() if k != "last_watering")
    return True, f"*{plant.name}* actualizada: {changes}"


async def eliminar_planta(text: str, phone: str = None) -> tuple[bool, str]:
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=100,
        system="Extrae el nombre de la planta a eliminar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f'Mensaje: {text}\nResponde: {{"search_term": "nombre de la planta"}}'}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    search_term = json.loads(raw).get("search_term", "")
    if not search_term:
        return False, "No entendi qué planta querés eliminar"
    results = await _ds.search_plants(search_term)
    if not results:
        return False, f"No encontré ninguna planta llamada _{search_term}_"
    plant = results[0]
    if phone:
        expires_at = (now_argentina() + timedelta(minutes=5)).replace(tzinfo=None).isoformat()
        pending_state[phone] = {
            "type": "confirm_delete", "action": "plant",
            "page_id": plant.id, "name": plant.name, "expires_at": expires_at,
        }
        await send_interactive_buttons(phone, f"¿Eliminás *{plant.name}*?", [
            {"id": "confirm_delete_yes", "title": "Sí, eliminala"},
            {"id": "confirm_delete_no", "title": "No, cancelar"},
        ])
        return True, ""
    ok = await _ds.archive_plant(plant.id)
    return (True, f"*{plant.name}* eliminada de Notion") if ok else (False, "Error eliminando la planta")


# ── MODULO EVENTOS ─────────────────────────────────────────────────────────────
def format_evento(data: dict, guardado: bool) -> str:
    emoji = data.get("emoji", "📅")
    hora = f" a las {data['time']}" if data.get("time") else ""
    fecha_raw = data.get("date", "")
    try:
        fecha = datetime.strptime(fecha_raw, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        fecha = fecha_raw
    summary = data.get("summary", "Evento")
    caption = data.get("caption", "")
    if caption and caption.lower() not in summary.lower():
        summary = f"{summary} -- {caption.strip().capitalize()}"
    lines = [f"{emoji} *{summary}*", f"Fecha: {fecha}{hora}"]
    if data.get("location"):
        lines.append(f"📍 {data['location']}")
    if data.get("description"):
        lines.append(f"Nota: {data['description']}")
    lines.append("\nAgregado a Google Calendar" if guardado else "\nAnota esto manualmente -- Calendar no configurado")
    return "\n".join(lines)

async def parse_evento(text: str, image_b64: str = None, image_type: str = None) -> dict:
    now = now_argentina()
    user_content = []
    if image_b64:
        user_content.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    user_content.append({"type": "text", "text": f"""Hoy es {now.strftime("%Y-%m-%d")}, hora actual: {now.strftime("%H:%M")}
Mensaje: {text or "(ver imagen adjunta)"}
Extrae la info del evento de la imagen si la hay, o del texto.
Responde:
{{"summary":"titulo","date":"YYYY-MM-DD","time":"HH:MM o null","duration_minutes":60,"location":"lugar o null","description":"desc o null","emoji":"emoji"}}"""})
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extrae info de un evento. Responde SOLO JSON valido sin markdown. Usa zona horaria Argentina (UTC-3).",
        messages=[{"role": "user", "content": user_content}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)


# ── Inteligencia conversacional ────────────────────────────────────────────────
async def needs_clarification(phone: str, text: str, context: str) -> str | None:
    try:
        resp = await claude_create(
            model="claude-sonnet-4-20250514", max_tokens=100,
            system=f"""Sos Knot. Evalua si el mensaje del usuario es suficientemente claro para ejecutar la accion indicada.
Contexto: {context}
Si el mensaje es claro -> responde solo: CLEAR
Si hay ambiguedad -> responde solo la pregunta de aclaracion mas concisa y natural posible (max 1 pregunta, tono rioplatense).""",
            messages=[{"role": "user", "content": text}]
        )
        result = resp.content[0].text.strip()
        if result == "CLEAR" or result.startswith("CLEAR"):
            return None
        return result
    except Exception:
        return None

# ── CLASIFICADOR ───────────────────────────────────────────────────────────────
async def classify(text: str, has_image: bool, image_b64: str = None, image_type: str = None, history: list = None, extra_images: list = None) -> str:
    if has_image and not text.strip() and not image_b64:
        return "GASTO"
    content = []
    if image_b64:
        content.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    for b64, itype in (extra_images or []):
        content.append({"type": "image", "source": {"type": "base64", "media_type": itype or "image/jpeg", "data": b64}})
    prompt_text = text if text.strip() else "(ver imagen adjunta)"
    history_ctx = ""
    if history and len(text.strip()) < 80:
        recent = history[-10:] if len(history) >= 10 else history
        history_ctx = "\nContexto reciente de la conversacion:\n" + "\n".join(
            f"{'Usuario' if m['role']=='user' else 'Matrics'}: {str(m['content'])[:120]}"
            for m in recent
        ) + "\n\nTeniendo en cuenta ese contexto, clasifica el siguiente mensaje:"
    content.append({"type": "text", "text": history_ctx + "\n" + prompt_text if history_ctx else prompt_text})
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=10,
        system="""Responde SOLO una palabra: GASTO, CORREGIR_GASTO, ELIMINAR_GASTO, PLANTA, EDITAR_PLANTA, ELIMINAR_PLANTA, EVENTO, EDITAR_EVENTO, ELIMINAR_EVENTO, RECORDATORIO, CANCELAR_RECORDATORIO, SHOPPING, CORREGIR_SHOPPING, ELIMINAR_SHOPPING, REUNION, EDITAR_REUNION, ELIMINAR_REUNION, SALUD, ACTIVIDAD_FISICA, GEO_REMINDER, CONFIGURAR, RESUMEN_DIARIO, LISTA o CHAT.

GASTO: registrar un pago, compra o ingreso NUEVO. El usuario describe algo que acaba de pagar o comprar ahora. NUNCA cuando usa "corregir", "cambiar", "editar", "actualizar", "la descripcion", "las notas", "el nombre" de algo ya registrado.
DEUDA: registrar algo que el usuario TODAVIA NO PAGO pero debe pagar. "le debo X a Y", "me deben X", "tengo que pagar X". Diferente a GASTO que es un pago ya realizado.
CORREGIR_GASTO: modificar cualquier campo de un gasto ya registrado — monto, categoria, nombre, descripcion, notas. Ejemplos: "el gasto de X era Y", "cambia la categoria de X", "corrige la descripcion de los 3 de anthropic", "en realidad eran extra usage", "la nota estaba mal". Si el usuario habla de algo que YA registró y quiere cambiarlo → CORREGIR_GASTO.
ELIMINAR_GASTO: eliminar o borrar un gasto de Notion.
PLANTA: adquirir o registrar una planta nueva.
EDITAR_PLANTA: modificar datos de una planta existente (estado, riego, ubicacion, notas).
ELIMINAR_PLANTA: eliminar una planta de Notion.
EDITAR_EVENTO: modificar un evento existente en el calendario.
ELIMINAR_EVENTO: eliminar o borrar un evento del calendario.
RECORDATORIO: el usuario quiere que se le recuerde algo en un momento de tiempo futuro. "recordame en X", "avisame en X", "recuerdame que llame a X a las Y". NUNCA si dice "cuando llegue", "al llegar", "cuando pase", "cuando este cerca" — eso es GEO_REMINDER. NUNCA cuando menciona un lugar fisico o comercio.
CANCELAR_RECORDATORIO: cancelar o borrar un recordatorio pendiente.
GEO_REMINDER: recordatorio basado en ubicacion — se dispara cuando el usuario llega o pasa cerca de un lugar. Ejemplos: "avisame cuando llegue al trabajo", "recordame al llegar a casa", "cuando llegue al gym avisame que X", "cuando pase cerca del super". Cualquier recordatorio con "cuando llegue", "al llegar", "cuando pase cerca" → siempre GEO_REMINDER, aunque no especifique la dirección exacta.
EVENTO: crear un evento nuevo -- turno, cumple, cita, viaje.
SHOPPING: gestionar lista de compras o recetas. Incluye preguntas sobre el estado de la lista.
CORREGIR_SHOPPING: editar las notas, cantidad o categoria de un item de la lista de compras.
ELIMINAR_SHOPPING: eliminar o borrar un item de la lista de compras.
REUNION: registrar notas o foto de una reunion/llamada nueva.
EDITAR_REUNION: editar notas o datos de una reunion ya registrada.
ELIMINAR_REUNION: eliminar una reunion de Notion.
SALUD: registrar o consultar informacion medica. Analisis, consultas, diagnosticos, medicaciones. También editar o eliminar registros médicos existentes.
ACTIVIDAD_FISICA: registrar, consultar, editar o eliminar actividad física. "corri 5km", "jugue al futbol", "fui al gym", "cuantos km corri este mes", screenshot de Adidas/Strava/Nike. NUNCA para eventos de calendario relacionados al deporte — esos son EVENTO.
CONFIGURAR: cambiar configuracion de Knot. Solo cuando el usuario quiere CAMBIAR algo. Incluye cambiar el horario del resumen diario: "pasame el resumen a las 8", "dime el resumen a las 9", "manda el buenos dias a las 7.30" — cualquier pedido de resumen que incluya una hora especifica implica CAMBIAR el horario. Nunca cuando pregunta o se queja.
RESUMEN_DIARIO: el usuario pide RECIBIR el resumen ahora, sin especificar un horario nuevo. "manda el resumen", "pasame el resumen diario", "dame el resumen ya", "enviame el buenos dias". NUNCA si incluye una hora especifica ("a las X") — eso es CONFIGURAR. NUNCA si pregunta sobre la configuracion → eso es CHAT.
LISTA: gestionar listas generativas del usuario (peliculas, libros, lugares, ideas, etc., distintas de SHOPPING que es supermercado). Ejemplos: "agrega 3 pelis de Tarantino a mi lista de pelis", "sumame 5 libros de no ficcion a leer", "agrega Inception a mi lista de pelis", "que tengo en mi lista de libros", "borra X de mi lista de Y", "crea una lista de viajes". El usuario menciona "mi lista de X" o pide agregar items que NO son del super.
CHAT: cualquier pregunta, consulta o conversacion. Si tiene "?" o pide informacion -> CHAT.

REGLA: si el mensaje PREGUNTA algo -> siempre CHAT, nunca GASTO.

IMAGENES SIN TEXTO:
- Factura, ticket, recibo -> GASTO
- Invitacion, flyer, screenshot de turno/evento -> EVENTO
- Foto de receta, lista de ingredientes -> SHOPPING
- Pizarron, apuntes de reunion -> REUNION
- Analisis de sangre, resultado de laboratorio, documento medico -> SALUD
- Documento de texto generico -> CHAT""",
        messages=[{"role": "user", "content": content}]
    )
    r = response.content[0].text.strip().upper()
    if "ELIMINAR_EVENTO" in r:         return "ELIMINAR_EVENTO"
    if "EDITAR_EVENTO" in r:           return "EDITAR_EVENTO"
    if "ACTIVIDAD_FISICA" in r:        return "ACTIVIDAD_FISICA"
    if "CANCELAR_RECORDATORIO" in r:   return "CANCELAR_RECORDATORIO"
    if "CORREGIR_SHOPPING" in r:       return "CORREGIR_SHOPPING"
    if "ELIMINAR_SHOPPING" in r:       return "ELIMINAR_SHOPPING"
    if "DEUDA" in r:                    return "DEUDA"
    if "ELIMINAR_GASTO" in r:          return "ELIMINAR_GASTO"
    if "CORREGIR_GASTO" in r:          return "CORREGIR_GASTO"
    if "ELIMINAR_REUNION" in r:        return "ELIMINAR_REUNION"
    if "EDITAR_REUNION" in r:          return "EDITAR_REUNION"
    if "ELIMINAR_PLANTA" in r:         return "ELIMINAR_PLANTA"
    if "EDITAR_PLANTA" in r:           return "EDITAR_PLANTA"
    if "GEO_REMINDER" in r:            return "GEO_REMINDER"
    if "SALUD" in r:                   return "SALUD"
    if "SHOPPING" in r:                return "SHOPPING"
    if "REUNION" in r:                 return "REUNION"
    if "RESUMEN_DIARIO" in r:          return "RESUMEN_DIARIO"
    if "CONFIGURAR" in r:              return "CONFIGURAR"
    if "LISTA" in r:                   return "LISTA"
    if "RECORDATORIO" in r:            return "RECORDATORIO"
    if "PLANTA" in r:                  return "PLANTA"
    if "EVENTO" in r:                  return "EVENTO"
    if "CHAT" in r:                    return "CHAT"
    return "GASTO"

async def query_finances(month: str = None) -> str:
    if not month:
        month = now_argentina().strftime("%Y-%m")
    try:
        data = await _ds.get_financial_summary(month)
    except Exception:
        return None
    if data["entries"] == 0:
        return f"No hay registros para {month}."
    top_cats = sorted(data["by_category"].items(), key=lambda x: x[1], reverse=True)[:5]
    summary = f"*Finanzas {month}*\n\nIngresos: ${data['ingresos']:,.0f}\nEgresos: ${data['egresos']:,.0f}\nBalance: ${data['balance']:,.0f}\n"
    if top_cats:
        summary += "\n*Top categorias:*\n" + "".join(f"- {c}: ${v:,.0f}\n" for c, v in top_cats)
    return summary


def get_activities_context() -> str:
    """Retorna descripcion de las actividades recurrentes del usuario para el context de los agentes."""
    acts = user_prefs.get("activities", {})
    if not acts:
        return ""
    lines = []
    for name, info in acts.items():
        days = ", ".join(info.get("days", []))
        time = info.get("time", "")
        line = f"- {name.capitalize()}: {days}" + (f" a las {time}" if time else "")
        lines.append(line)
    return "Actividades recurrentes del usuario:\n" + "\n".join(lines)


def get_domain_profile(domain: str) -> str:
    return user_prefs.get("domain_profiles", {}).get(domain, "")


async def save_domain_profile_direct(domain: str, text: str):
    """Guarda un campo de perfil de dominio directamente en la config page de Notion."""
    page_id = user_prefs.get("_config_page_id")
    if not page_id:
        return
    field_map = {
        "actividad_fisica": "Profile Actividad Fisica",
        "dieta":            "Profile Dieta",
        "supermercado":     "Profile Supermercado",
        "gastos":           "Profile Gastos",
        "salud":            "Profile Salud",
        "social":           "Profile Social",
        "hogar":            "Profile Hogar",
        "productividad":    "Profile Productividad",
    }
    notion_field = field_map.get(domain)
    if not notion_field:
        return
    await _ds.update_config_fields(page_id, {notion_field: text})


async def save_purchase_counts_direct():
    page_id = user_prefs.get("_config_page_id")
    if not page_id:
        return
    await _ds.update_config_fields(page_id, {
        "Purchase Counts": json.dumps(user_prefs.get("purchase_counts", {}), ensure_ascii=False)
    })


async def update_domain_profile_bg(domain: str, event_description: str):
    """Fire-and-forget: usa Haiku para actualizar el perfil narrativo de un dominio si detecta patron relevante."""
    try:
        current = get_domain_profile(domain)
        resp = await claude_create(
            model="claude-haiku-4-5-20251001", max_tokens=300,
            system="""Sos un analizador de patrones de comportamiento de un usuario.
Se te da el perfil actual en un dominio y un evento reciente.
Si el evento aporta informacion nueva, confirma un patron o muestra un cambio de habito: devuelve el perfil actualizado (texto natural y conciso, max 150 palabras).
Si el evento no agrega nada relevante al perfil: responde exactamente NO_CAMBIO.
Responde SOLO el texto del perfil actualizado, o NO_CAMBIO.""",
            messages=[{"role": "user", "content": f"Dominio: {domain}\nPerfil actual: {current or '(sin datos todavia)'}\nEvento reciente: {event_description}"}]
        )
        new_text = resp.content[0].text.strip()
        if new_text and new_text != "NO_CAMBIO":
            user_prefs.setdefault("domain_profiles", {})[domain] = new_text
            await save_domain_profile_direct(domain, new_text)
    except Exception:
        pass


async def check_and_notify_deviation(phone: str, items: list, supermercado_profile: str):
    """Fire-and-forget: detecta desviaciones del perfil de supermercado y notifica al usuario."""
    try:
        resp = await claude_create(
            model="claude-haiku-4-5-20251001", max_tokens=100,
            system="Detecta si los items representan una desviacion significativa de los patrones del usuario (ej: siempre compra X, ahora pide Y que contradice X). Si hay desviacion clara, describe en 1 oracion en espanol rioplatense informal. Si no hay desviacion, responde exactamente: NO",
            messages=[{"role": "user", "content": f"Perfil: {supermercado_profile}\nItems agregados ahora: {', '.join(str(i) for i in items)}"}]
        )
        result = resp.content[0].text.strip()
        if result and result.upper() != "NO":
            await send_message(phone, f"💡 {result}")
    except Exception:
        pass




async def infer_service_providers() -> dict:
    access_token = await get_gcal_access_token()
    if not access_token:
        return {}
    try:
        async with httpx.AsyncClient(timeout=15) as http:
            headers = {"Authorization": f"Bearer {access_token}"}
            r = await http.get(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages",
                headers=headers,
                params={"q": "newer_than:60d (factura OR comprobante OR boleta OR vencimiento OR suministro OR servicio)", "maxResults": 30}
            )
            if r.status_code != 200:
                return {}
            messages = r.json().get("messages", [])
            if not messages:
                return {}
            mail_summaries = []
            for msg in messages[:15]:
                msg_r = await http.get(
                    f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg['id']}",
                    headers=headers,
                    params={"format": "metadata", "metadataHeaders": ["Subject", "From"]}
                )
                if msg_r.status_code == 200:
                    hdrs = {h["name"]: h["value"] for h in msg_r.json().get("payload", {}).get("headers", [])}
                    snippet = msg_r.json().get("snippet", "")[:150]
                    mail_summaries.append(f"De: {hdrs.get('From','')}\nAsunto: {hdrs.get('Subject','')}\nPreview: {snippet}")
            if not mail_summaries:
                return {}
            resp = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=300,
                system="""Analiza estos mails de facturas/servicios e identifica que empresa provee que servicio.
Responde SOLO JSON con este formato:
{"electricidad": "Nombre empresa", "gas": "Nombre empresa", "internet": "Nombre empresa", "agua": "Nombre empresa", "telefono": "Nombre empresa"}
Solo inclui los servicios que puedas identificar con certeza. Si no hay info suficiente para un servicio, no lo incluyas.""",
                messages=[{"role": "user", "content": "\n---\n".join(mail_summaries)}]
            )
            raw = resp.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.strip("`").lstrip("json").strip()
            return json.loads(raw)
    except Exception:
        return {}


async def buscar_gastos(query: str, mes: str = None) -> str:
    if not mes:
        mes = now_argentina().strftime("%Y-%m")
    try:
        entries = await _ds.search_expenses(query, mes)
        if not entries:
            return f"No encontre gastos que contengan '{query}' en {mes}."
        lines = []
        for e in entries:
            date_val = str(e.date) if e.date else ""
            direction = "INGRESO" if e.in_out == "INGRESO" else "EGRESO"
            lines.append(f"- {date_val} -- {e.name}: ${e.value_ars:,.0f} ({direction})")
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {str(e)[:100]}"



async def search_google_contact(name: str) -> str:
    """Busca un contacto en Google Contacts por nombre."""
    access_token = await get_gcal_access_token()
    if not access_token:
        return "No hay acceso a Google Contacts."
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            r = await http.get(
                "https://people.googleapis.com/v1/people/me/connections",
                headers={"Authorization": f"Bearer {access_token}"},
                params={
                    "personFields": "names,addresses,phoneNumbers,emailAddresses",
                    "pageSize": 100,
                }
            )
            if r.status_code != 200:
                return f"Error consultando Contacts: {r.text[:100]}"
            connections = r.json().get("connections", [])
            if not connections:
                return "No encontré contactos en tu agenda."
            name_lower = name.lower()
            matches = []
            for person in connections:
                names = person.get("names", [])
                display_name = names[0].get("displayName", "") if names else ""
                if name_lower in display_name.lower():
                    addresses = person.get("addresses", [])
                    phones = person.get("phoneNumbers", [])
                    emails = person.get("emailAddresses", [])
                    info = [f"*{display_name}*"]
                    for addr in addresses:
                        label = addr.get("formattedType", "Dirección")
                        val = addr.get("formattedValue", "")
                        if val:
                            info.append(f"📍 {label}: {val}")
                    for ph in phones:
                        label = ph.get("formattedType", "Tel")
                        val = ph.get("value", "")
                        if val:
                            info.append(f"📞 {label}: {val}")
                    for em in emails:
                        val = em.get("value", "")
                        if val:
                            info.append(f"✉️ {val}")
                    matches.append("\n".join(info))
            if not matches:
                return f"No encontré ningún contacto llamado '{name}'."
            return "\n\n".join(matches[:3])
    except Exception as e:
        return f"Error: {str(e)[:100]}"

async def handle_chat(phone: str, text: str) -> str:
    history = get_history(phone)
    add_to_history(phone, "user", text)
    now = now_argentina()


    # Armar contexto del usuario desde su config en Notion
    user_context_parts = []
    providers = user_prefs.get("service_providers", {})
    if providers:
        prov_str = ", ".join(f"{k}: {v}" for k, v in providers.items())
        user_context_parts.append(f"Proveedores de servicios: {prov_str}.")
    if user_prefs.get("greeting_name"):
        user_context_parts.append(f"Nombre del usuario: {user_prefs['greeting_name']}.")
    resumen_h = user_prefs.get("daily_summary_hour")
    resumen_m = user_prefs.get("daily_summary_minute") or 0
    if resumen_h is not None:
        user_context_parts.append(f"Resumen diario configurado a las {int(resumen_h):02d}:{int(resumen_m):02d}.")
    extras = user_prefs.get("resumen_extras", [])
    if extras:
        user_context_parts.append(f"Extras del resumen: {', '.join(extras)}.")
    noc_h = user_prefs.get("resumen_nocturno_hour") or 22
    noc_en = user_prefs.get("resumen_nocturno_enabled", True)
    user_context_parts.append(f"Resumen nocturno: {'activado' if noc_en else 'desactivado'} a las {int(noc_h):02d}:00.")
    _ulat = current_location.get("lat")
    _ulon = current_location.get("lon")
    _uloc = current_location.get("location_name")
    _upd = current_location.get("updated_at")
    _src = current_location.get("source", "unknown")
    if _src == "owntracks" and _ulat is not None:
        _age = int((now - _upd).total_seconds() / 60) if _upd else None
        _age_str = f" (hace {_age} min)" if _age is not None else ""
        _place = is_at_known_place()
        _loc_label = _place["name"] if _place else (_uloc or f"{_ulat:.5f}, {_ulon:.5f}")
        user_context_parts.append(f"Ubicacion GPS (OwnTracks){_age_str}: {_loc_label} ({_ulat:.5f}, {_ulon:.5f}).")
    elif _ulat is not None and _upd:
        _age = int((now - _upd).total_seconds() / 60)
        _loc_label = _uloc or f"{_ulat:.5f}, {_ulon:.5f}"
        user_context_parts.append(f"Ultima ubicacion conocida: {_loc_label} ({_ulat:.5f}, {_ulon:.5f}), hace {_age} minutos — OwnTracks inactivo. Si el usuario pregunta donde esta, informale la ultima ubicacion registrada y sugeríle que abra OwnTracks para actualizar.")
    elif _ulat is not None:
        _loc_label = _uloc or f"{_ulat:.5f}, {_ulon:.5f}"
        user_context_parts.append(f"Ultima ubicacion guardada en Notion: {_loc_label} — OwnTracks sin datos.")
    else:
        user_context_parts.append("Ubicacion desconocida — OwnTracks sin datos y sin coordenadas guardadas.")
    user_context = "\n".join(user_context_parts)

    tools = [
        {
            "name": "consultar_calendario",
            "description": "Consulta eventos del calendario de Google Calendar. Usa cuando el usuario pregunta sobre su agenda, eventos, turnos, que tiene programado, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "dias_adelante": {"type": "integer", "description": "Cuantos dias hacia adelante consultar. Default 2, usar 7 para 'esta semana', 30 para 'este mes'."},
                    "dias_atras": {"type": "integer", "description": "Cuantos dias hacia atras consultar. Default 0."}
                },
                "required": []
            }
        },
        {
            "name": "consultar_finanzas",
            "description": "Consulta gastos e ingresos registrados en Notion. Usa cuando el usuario pregunta sobre plata, gastos, balance, cuanto gasto, finanzas del mes, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "mes": {"type": "string", "description": "Mes a consultar en formato YYYY-MM. Si no se especifica, usar el mes actual."}
                },
                "required": []
            }
        },
        {
            "name": "consultar_clima",
            "description": "Consulta el clima actual y pronostico. Usa cuando el usuario pregunta sobre el tiempo, temperatura, lluvia, si necesita abrigo, paraguas, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "incluir_manana": {"type": "boolean", "description": "True si pregunta por manana o el pronostico."}
                },
                "required": []
            }
        },
        {
            "name": "consultar_gmail",
            "description": "Consulta los mails importantes no leidos de los ultimos 2 dias. Usa cuando el usuario pregunta sobre emails, correos, facturas recibidas, si le escribieron, notificaciones importantes, etc.",
            "input_schema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        },
        {
            "name": "corregir_gasto",
            "description": "Corrige el monto u otros campos de un gasto ya registrado en Notion. Usa cuando el usuario confirma que queres corregir algo, o cuando encontras una diferencia entre una factura y lo registrado y el usuario pide corregirlo.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_term": {"type": "string", "description": "Nombre o parte del nombre del gasto a corregir. Ej: 'luz', 'CALF', 'Movistar'"},
                    "new_value_ars": {"type": "number", "description": "Nuevo monto en ARS"},
                    "mes": {"type": "string", "description": "Mes en formato YYYY-MM. Si no se especifica usa el mes actual."}
                },
                "required": ["search_term", "new_value_ars"]
            }
        },
        {
            "name": "buscar_gastos",
            "description": "Busca entradas individuales de gastos/ingresos en Notion por nombre. Usa cuando el usuario pregunta si pago algo especifico, si hay un gasto de una empresa concreta, si registro tal o cual pago, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Nombre o parte del nombre a buscar. Ej: 'CALF', 'Movistar', 'alquiler'"},
                    "mes": {"type": "string", "description": "Mes a consultar en formato YYYY-MM. Si no se especifica, usa el mes actual."}
                },
                "required": ["query"]
            }
        },
        {
            "type": "web_search_20250305",
            "name": "web_search"
        },
        {
            "name": "buscar_contacto",
            "description": "Busca información de un contacto en Google Contacts: dirección, teléfono, email. Usar cuando el usuario pregunta por datos de alguien, dónde vive, cómo contactarlo, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "nombre": {"type": "string", "description": "Nombre o parte del nombre del contacto a buscar"}
                },
                "required": ["nombre"]
            }
        },
        {
            "name": "guardar_lugar_conocido",
            "description": "Guarda una direccion como lugar conocido del usuario (casa, trabajo, gimnasio, etc). Usar cuando el usuario menciona donde vive, donde trabaja, o cualquier lugar de referencia personal. Si el usuario acaba de compartir su ubicacion (lat/lon disponibles), pasar lat y lon directamente en vez de direccion.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "nombre": {"type": "string", "description": "Nombre del lugar. Ej: 'Casa', 'Trabajo', 'Gimnasio'"},
                    "direccion": {"type": "string", "description": "Direccion completa para geocodificar. Ej: 'Islas Malvinas 809, Neuquen'. Omitir si se pasan lat/lon."},
                    "lat": {"type": "number", "description": "Latitud exacta si se conoce (del GPS o Maps). Usar en vez de direccion cuando este disponible."},
                    "lon": {"type": "number", "description": "Longitud exacta si se conoce (del GPS o Maps). Usar en vez de direccion cuando este disponible."},
                    "radio": {"type": "integer", "description": "Radio en metros para considerar que el usuario esta en ese lugar. Default 100."}
                },
                "required": ["nombre"]
            }
        },
        {
            "name": "marcar_factura_pagada",
            "description": "Marca una factura pendiente como pagada. Usar cuando el usuario confirma que pago un servicio o factura.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "provider": {"type": "string", "description": "Nombre del proveedor. Ej: 'Camuzzi', 'CALF', 'Movistar'"},
                    "paid_amount": {"type": ["number", "null"], "description": "Monto pagado si lo menciona. Null si no."},
                    "payment_method": {"type": ["string", "null"], "description": "Medio de pago si lo menciona. Ej: 'BBVA', 'Mercado Pago'. Null si no."}
                },
                "required": ["provider"]
            }
        },
        {
            "name": "consultar_deudas",
            "description": "Lista facturas y deudas pendientes de pago. Usar cuando el usuario pregunta que debe, que facturas tiene impagas, cuanto le falta pagar.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "provider": {"type": ["string", "null"], "description": "Filtrar por proveedor o persona. Null para listar todo."}
                },
                "required": []
            }
        },
        {
            "name": "historial_pagos",
            "description": "Consulta el historial de pagos a un proveedor o persona. Usar cuando el usuario pregunta cuando pago algo, cuanto pago, por que pago de mas o menos.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "provider": {"type": "string", "description": "Nombre del proveedor o persona a consultar."}
                },
                "required": ["provider"]
            }
        },
        {
            "name": "consultar_lugares_conocidos",
            "description": "Lista los lugares conocidos guardados del usuario (casa, trabajo, gimnasio, etc).",
            "input_schema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        },
        {
            "name": "consultar_geo_reminders",
            "description": "Lista los geo-reminders activos del usuario. Usar cuando pregunta que recordatorios de ubicacion tiene, cuales tiene activos, o quiere desactivar alguno.",
            "input_schema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        },
        {
            "name": "editar_geo_reminder",
            "description": "Edita un geo-reminder existente: cambia el radio, la recurrencia, o lo desactiva. Usar cuando el usuario quiere modificar o eliminar un geo-reminder.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_term": {"type": "string", "description": "Palabra clave para identificar el geo-reminder. Ej: 'ferreteria', 'anonima'"},
                    "new_radius": {"type": ["integer", "null"], "description": "Nuevo radio en metros, o null para no cambiar"},
                    "new_recurrent": {"type": ["boolean", "null"], "description": "True para recurrente, False para una sola vez, null para no cambiar"},
                    "new_name": {"type": ["string", "null"], "description": "Nuevo nombre para el geo-reminder, o null para no cambiar"},
                    "deactivate": {"type": "boolean", "description": "True para desactivar el reminder completamente"}
                },
                "required": ["search_term"]
            }
        },
        {
            "name": "buscar_comercios_cercanos",
            "description": "Busca comercios, negocios o locales cerca de la ubicacion actual del usuario usando Google Places. Usar cuando el usuario pregunta si hay algun negocio cerca, si tiene alguna tienda a mano, donde queda tal comercio, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "nombre": {"type": "string", "description": "Nombre del comercio o tipo de negocio. Ej: 'La Anonima', 'farmacia', 'panaderia', 'Panipunto'"},
                    "radio_metros": {"type": "integer", "description": "Radio de busqueda en metros. Default 1000."}
                },
                "required": ["nombre"]
            }
        },
        {
            "name": "calcular_fecha",
            "description": "Calcula fechas exactas a partir de descripciones como 'el segundo sabado de septiembre', 'el ultimo viernes de octubre', 'dentro de 15 dias'. Usar SIEMPRE antes de crear o editar un evento cuando la fecha viene de una descripcion relativa.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "descripcion": {"type": "string", "description": "La descripcion de la fecha tal como la dijo el usuario. Ej: 'el segundo sabado de septiembre', 'el proximo viernes', 'dentro de 10 dias'"}
                },
                "required": ["descripcion"]
            }
        },
        {
            "name": "configurar_matrics",
            "description": "Cambia configuracion de Knot: horario del resumen diario, extras del resumen, saludo, resumen nocturno. Usa SOLO cuando el usuario quiere CAMBIAR algo de la config, no cuando pregunta.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "hour": {"type": ["integer", "null"], "description": "Nueva hora del resumen diario (0-23)"},
                    "minute": {"type": ["integer", "null"], "description": "Nuevos minutos del resumen (0-59), default 0"},
                    "greeting_name": {"type": ["string", "null"], "description": "Nuevo nombre para el saludo matutino"},
                    "add_extra": {"type": ["string", "null"], "description": "Instruccion extra a agregar al resumen"},
                    "remove_extra": {"type": ["string", "null"], "description": "Extra a remover del resumen"},
                    "nocturno_enabled": {"type": ["boolean", "null"], "description": "Activar/desactivar resumen nocturno"},
                    "nocturno_hour": {"type": ["integer", "null"], "description": "Hora del resumen nocturno (0-23)"}
                },
                "required": []
            }
        },
        {
            "name": "crear_proyecto",
            "description": "Crea un proyecto, idea o nota de reunion en la base de Proyectos de Notion. Usa cuando el usuario dice 'anota proyecto', 'tengo una idea', 'nuevo proyecto', 'anota como proyecto', o describe una idea/proyecto que quiere guardar.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Nombre del proyecto o idea"},
                    "entry_type": {"type": "string", "enum": ["Proyecto", "Idea", "Reunion"], "description": "Tipo de entrada"},
                    "area": {"type": "string", "enum": ["Laboral", "Hobby", "Personal"], "description": "Area a la que pertenece"},
                    "description": {"type": ["string", "null"], "description": "Descripcion detallada si la hay"},
                    "priority": {"type": ["string", "null"], "enum": ["Alta", "Media", "Baja", None], "description": "Prioridad"},
                    "emoji": {"type": "string", "description": "Emoji representativo"}
                },
                "required": ["name", "entry_type", "area", "emoji"]
            }
        },
        {
            "name": "editar_evento",
            "description": "Edita un evento existente en Google Calendar. Usa cuando el usuario quiere cambiar la hora, fecha, titulo o ubicacion de un evento ya creado.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_term": {"type": ["string", "null"], "description": "Keyword para buscar el evento, o null para el ultimo tocado"},
                    "target_date": {"type": ["string", "null"], "description": "YYYY-MM-DD de la instancia especifica a editar. Siempre completar si el usuario menciona un dia ('el de mañana', 'el del jueves', etc.)"},
                    "new_title": {"type": ["string", "null"]},
                    "new_date": {"type": ["string", "null"], "description": "YYYY-MM-DD"},
                    "new_time": {"type": ["string", "null"], "description": "HH:MM"},
                    "new_location": {"type": ["string", "null"]},
                    "new_description": {"type": ["string", "null"]}
                },
                "required": []
            }
        }
    ]

    system = f"""Sos Knot, asistente personal en WhatsApp. Respondes conciso y natural en espanol rioplatense.
Hoy: {hoy_str(now)}.
Calendario de referencia: {semana_str(now)}.
REGLA CRITICA: cuando el usuario menciona un dia de la semana, usa EXACTAMENTE la fecha de la tabla de arriba. NO calcules fechas mentalmente. NUNCA.
REGLA CRITICA 2: para calculos de fechas, dias de la semana, "que dia cae", "dentro de X dias", usa la tabla de referencia o calcular_fecha. No uses web_search para esto.
REGLA CRITICA 3: antes de nombrar un dia de la semana, verificalo en la tabla. Ejemplo: si vas a decir "sabado 12/04", buscá 12/04 en la tabla. Si la tabla dice "domingo 12/04", corregite. NUNCA asumas el nombre del dia sin verificar.
REGLA CRITICA DE FECHAS: antes de crear o editar cualquier evento cuya fecha venga de lenguaje natural ("el proximo viernes", "el segundo sabado de septiembre", "en dos semanas"), SIEMPRE llama primero a calcular_fecha para obtener la fecha exacta. Nunca asumas la fecha directamente.
{user_context}
Si el usuario pregunta algo que ya sabes por su configuracion, responde directamente sin usar herramientas.

Tenes acceso a informacion real del usuario a traves de herramientas:
- Su calendario de Google (eventos, turnos, agenda)
- Sus finanzas en Notion (gastos e ingresos registrados, por categoria o por nombre)
- Su Gmail (mails recibidos, facturas, comprobantes, comunicaciones)
- El clima actual y pronostico
- Busqueda web para informacion externa
- Configuracion de Knot (cambiar horario del resumen, extras, saludo, nocturno)

Antes de responder cualquier pregunta, pensa que fuentes son relevantes y consulta todas las que hagan falta.

RAZONAMIENTO IMPORTANTE para preguntas sobre pagos de servicios:
1. Busca la factura en Gmail para saber el monto exacto que deberia haberse pagado
2. Busca en Notion usando MULTIPLES terminos: el nombre de la empresa (ej: "CALF") Y el tipo de servicio (ej: "luz", "electricidad") Y variantes posibles. SIEMPRE busca en el mes actual Y en el mes anterior — los servicios se pagan frecuentemente el mes siguiente al de la factura.
3. Si encontras un pago en Notion con monto parecido al de la factura, asumi que corresponde al mismo gasto aunque el nombre sea diferente
4. Si el monto registrado difiere del de la factura, mencionalo y ofrece corregirlo
5. Si no encontras ningun pago relacionado, deci que no aparece registrado
6. Si mencionaste facturas pendientes y el usuario dice que ya las pago, busca en Notion para verificar antes de pedir montos
7. CRITICO: las fechas de vencimiento solo mencionarlas si aparecen textualmente en el mail. Nunca inferir ni inventar fechas.
8. CRITICO: si en esta conversacion ya se confirmo que una factura esta pagada, NO la vuelvas a mencionar como pendiente aunque Gmail la muestre.
9. CRITICO: cada vez que el usuario confirme que pago un servicio (ya sea respondiendo a tu pregunta o diciendotelo directamente), SIEMPRE llama a marcar_factura_pagada con el nombre del proveedor. Esto persiste la informacion en Notion para que no vuelva a aparecer como pendiente en futuros resumenes.
10. CRITICO: cuando el usuario dice "ya lo pague", "ambas", "las dos", "todas", "ya esta", o cualquier confirmacion de pago — NUNCA preguntes de nuevo cuales son. Inferilas del contexto inmediato de la conversacion (los mensajes anteriores). Si mencionaste dos facturas y el usuario dice "ambas", llama a marcar_factura_pagada dos veces, una por cada proveedor. Actuar primero, preguntar solo si genuinamente no hay contexto.

Podes usar varias herramientas en el mismo turno. No respondas hasta tener la informacion necesaria.
IMPORTANTE: No inventes datos. Si no encontras info en ninguna fuente, decilo claramente.
CAPACIDADES COMPLETAS DE MATRICS (no niegues ninguna):
- Crear, editar y eliminar eventos en Google Calendar (via otro modulo, no esta en tus tools pero Knot SI lo hace)
- Registrar gastos e ingresos en Notion (via otro modulo)
- Crear y gestionar geo-reminders basados en ubicacion (via otro modulo)
- Acceder a ubicacion GPS via OwnTracks (si esta activo, la info ya esta en tu contexto)
- Consultar y gestionar lista de compras en Notion (via otro modulo)
- Gestionar tasks y proyectos en Notion (tenes la tool crear_proyecto)
- Consultar calendario, finanzas, clima, Gmail (tus tools directas)
- Buscar en la web
- Configurar Knot (horarios, extras, saludo)

Si el usuario dice que hiciste algo o que Knot hizo algo, NO lo niegues. Consulta el calendario o Notion para verificarlo.
Si algo no esta en tus tools directas pero es una capacidad de Matrics, decile que SI puede hacerlo y guialo.
CRITICO: si guardar_lugar_conocido devuelve error o dice "NO fue guardado", informale al usuario que el lugar NO quedo guardado y sugeríle compartir la ubicacion por WhatsApp. NUNCA confirmes que se guardo algo cuando la tool fallo.

SOFT INTRODUCTION DE CAPACIDADES (muy importante):
Cuando el usuario pregunta por algo que NO TENES REGISTRADO (ej: "cuando vence el seguro del auto", "donde es la reunion de manana", "cuanto pague de luz el mes pasado"), seguí este patrón:
1. Verificá en TUS fuentes (calendar, finanzas, gmail, notion) — usá las tools necesarias.
2. Si NO encontras info: respondé naturalmente que no la tenés ("No tengo registrado nada del seguro del auto") + ofrecé UNA herramienta pertinente, no las enumerés todas.
   - Para vencimientos/fechas → "¿Querés que te lo agende cuando me digas la fecha?"
   - Para gastos pasados que no aparecen → "Si tenés el comprobante o el monto, lo registramos."
   - Para ubicaciones de eventos → "¿Querés que la guarde para la próxima?"
   - Para recordatorios contextuales → "¿Querés que te avise cuando estés cerca de X?"
3. La oferta tiene que ser natural, no forzada. Si genuinamente no hay nada útil para ofrecer, simplemente decí que no tenés info y listo. NO inventes ofertas si no hay una herramienta clara que aplique.
La idea es que el usuario descubra capacidades de Knot a medida que las necesita, no que reciba una lista enumerada de features."""

    history_clean = [h for h in history if h.get("content")]
    messages = history_clean + [{"role": "user", "content": text}]

    try:
        response = await claude_create(
            model="claude-sonnet-4-20250514", max_tokens=1000,
            system=system,
            messages=messages,
            tools=tools
        )
    except Exception:
        return "Error procesando tu mensaje. Intenta de nuevo."

    if response.stop_reason == "end_turn":
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()
        add_to_history(phone, "assistant", reply)
        return reply

    # ── Helper para ejecutar tools de chat ────────────────────────────────
    async def _execute_chat_tool(t_name, t_input):
        t_result = ""
        if t_name == "consultar_calendario":
            dias_adelante = t_input.get("dias_adelante", 2)
            dias_atras = t_input.get("dias_atras", 0)
            t_result = await query_calendar(days_ahead=dias_adelante, days_back=dias_atras) or "No hay eventos en ese periodo."
        elif t_name == "consultar_finanzas":
            mes = t_input.get("mes") or now.strftime("%Y-%m")
            t_result = await query_finances(mes) or "No hay registros para " + mes + "."
            # Trigger #8: 3+ consultas finanzas sin filtro en la última semana
            log = finance_query_log.setdefault(phone, [])
            log.append(now)
            cutoff = now - timedelta(days=7)
            log[:] = [d for d in log if d > cutoff]
            if len(log) >= 3:
                emit_hint(phone, suggestion_gate.Hint(
                    trigger_id="finance_query_freq",
                    message="📊 Veo que mirás tus finanzas seguido. ¿Querés que te mande un mini-resumen automático cada lunes? Decime *si* o *no*.",
                    action_intent="enable_weekly_finance",
                    payload={},
                ))
        elif t_name == "corregir_gasto":
            search_term = t_input.get("search_term", "")
            new_value = t_input.get("new_value_ars")
            mes = t_input.get("mes") or now.strftime("%Y-%m")
            year_c, mon_c = map(int, mes.split("-"))
            from calendar import monthrange as mr
            last_day = mr(year_c, mon_c)[1]
            try:
                results = await _ds.search_expenses(search_term, mes)
                if results:
                    entry = results[0]
                    await _ds.update_expense(entry.id, {"value_ars": float(new_value)})
                    t_result = f"Correccion exitosa: '{entry.name}' actualizado de ${entry.value_ars:,.0f} a ${float(new_value):,.0f} ARS."
                else:
                    t_result = f"No encontre ningun gasto llamado '{search_term}' en {mes}."
            except Exception as e:
                t_result = "Error: " + str(e)[:100]
        elif t_name == "buscar_contacto":
            t_result = await search_google_contact(t_input.get("nombre", ""))
        elif t_name == "guardar_lugar_conocido":
            nombre = t_input.get("nombre", "")
            direccion = t_input.get("direccion", "")
            radio = t_input.get("radio", 100)
            place_lat = t_input.get("lat")
            place_lon = t_input.get("lon")
            formatted = direccion or nombre
            try:
                if place_lat is not None and place_lon is not None:
                    # Coordenadas exactas provistas directamente
                    pass
                elif direccion:
                    async with httpx.AsyncClient(timeout=5) as http:
                        api_key = os.environ.get("GOOGLE_PLACES_KEY", "")
                        _attempts = [direccion]
                        if not any(k in direccion.lower() for k in ["argentina", "neuquen", "neuquén"]):
                            _attempts.append(f"{direccion}, Neuquén, Argentina")
                        _geo_result = None
                        for _addr in _attempts:
                            r = await http.get(
                                "https://maps.googleapis.com/maps/api/geocode/json",
                                params={"address": _addr, "key": api_key, "language": "es"}
                            )
                            if r.status_code == 200 and r.json().get("results"):
                                _geo_result = r.json()["results"][0]
                                break
                        if _geo_result:
                            place_lat = _geo_result["geometry"]["location"]["lat"]
                            place_lon = _geo_result["geometry"]["location"]["lng"]
                            formatted = _geo_result.get("formatted_address", direccion)
                        elif current_location.get("lat") is not None:
                            # Fallback: usar la ubicacion actual si el geocoding fallo
                            place_lat = current_location["lat"]
                            place_lon = current_location["lon"]
                            formatted = current_location.get("location_name") or direccion
                else:
                    # Sin direccion ni coords: usar ubicacion actual si existe
                    if current_location.get("lat") is not None:
                        place_lat = current_location["lat"]
                        place_lon = current_location["lon"]
                        formatted = current_location.get("location_name") or nombre

                if place_lat is not None and place_lon is not None:
                    places = user_prefs.get("known_places", [])
                    places = [p for p in places if p["name"].lower() != nombre.lower()]
                    places.append({"name": nombre, "lat": place_lat, "lon": place_lon, "radius": radio})
                    user_prefs["known_places"] = places
                    await save_user_config(MY_NUMBER)
                    t_result = f"Guardado: {nombre} en {formatted} (radio {radio}m)."
                else:
                    t_result = f"No pude ubicar '{direccion or nombre}'. El lugar NO fue guardado. Informale al usuario y sugeríle que comparta la ubicacion directamente desde WhatsApp (adjuntar → ubicacion)."
            except Exception as e:
                t_result = f"Error: {str(e)[:100]}. El lugar NO fue guardado."
        elif t_name == "editar_geo_reminder":
            import unicodedata
            def strip_accents(s):
                return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
            search_term = strip_accents(t_input.get("search_term", "").lower())
            new_radius = t_input.get("new_radius")
            new_recurrent = t_input.get("new_recurrent")
            new_name = t_input.get("new_name")
            deactivate = t_input.get("deactivate", False)
            matched = [r for r in geo_reminders_cache if search_term in strip_accents(r["name"].lower()) or search_term in strip_accents(r.get("shop_name", "").lower())]
            if not matched:
                t_result = f"No encontre ningun geo-reminder relacionado con '{search_term}'."
            else:
                reminder = matched[0]
                page_id = reminder["page_id"]
                props = {}
                if deactivate:
                    props["Active"] = {"checkbox": False}
                if new_radius is not None:
                    props["Radius"] = {"number": new_radius}
                    reminder["radius"] = new_radius
                if new_recurrent is not None:
                    props["Recurrent"] = {"checkbox": new_recurrent}
                    reminder["recurrent"] = new_recurrent
                if new_name:
                    props["Name"] = {"title": [{"text": {"content": new_name}}]}
                    reminder["name"] = new_name
                try:
                    updates = {}
                    if deactivate:
                        updates["active"] = False
                    if new_radius is not None:
                        updates["radius"] = new_radius
                    if new_recurrent is not None:
                        updates["recurrent"] = new_recurrent
                    if new_name:
                        updates["name"] = new_name
                    await _ds.update_geo_reminder(page_id, updates)
                    if deactivate:
                        geo_reminders_cache.remove(reminder)
                        t_result = f"Geo-reminder '{reminder['name']}' desactivado."
                    else:
                        changes = []
                        if new_name:
                            changes.append(f"nombre -> '{new_name}'")
                        if new_radius is not None:
                            changes.append(f"radio -> {new_radius}m")
                        if new_recurrent is not None:
                            changes.append("recurrente" if new_recurrent else "solo una vez")
                        t_result = f"'{reminder['name']}' actualizado: {', '.join(changes)}."
                except Exception as e:
                    t_result = f"Error: {str(e)[:100]}"
        elif t_name == "marcar_factura_pagada":
            provider = t_input.get("provider", "")
            paid_amount = t_input.get("paid_amount")
            payment_method = t_input.get("payment_method")
            tasks = await get_pending_factura_tasks()
            prov_lower = provider.lower()
            matched_tasks = []
            for task in tasks:
                tp = task.get("provider", "").lower()
                if not tp:
                    continue
                if tp in prov_lower or prov_lower in tp:
                    matched_tasks.append(task)
                    continue
                tp_words = set(w for w in tp.split() if len(w) > 3)
                prov_words = set(w for w in prov_lower.split() if len(w) > 3)
                if tp_words & prov_words:
                    matched_tasks.append(task)
            if not matched_tasks:
                # Try matching directly against Impaga entries in Finances
                impagas = await _ds.get_impaga_facturas(provider=provider)
                if impagas:
                    for imp in impagas:
                        inv_amount = imp.value_ars
                        diff_pct = abs((paid_amount or inv_amount) - inv_amount) / max(inv_amount, 1) if inv_amount else 0
                        if paid_amount and diff_pct > 0.10:
                            pending_state[phone] = {
                                "type": "factura_note",
                                "finance_page_id": imp.id,
                                "paid_amount": paid_amount,
                                "payment_method": payment_method,
                                "provider_name": imp.name,
                            }
                            t_result = f"Pagaste ${paid_amount:,.0f} pero la factura era ${inv_amount:,.0f}. ¿Querés agregar una nota antes de marcar como pagada?"
                        else:
                            await _ds.mark_finance_paid(imp.id, paid_amount, payment_method)
                            t_result = f"✅ {imp.name} marcada como pagada en Finanzas."
                else:
                    t_result = f"No encontre ninguna factura pendiente para '{provider}'."
            else:
                marked = []
                for task in matched_tasks:
                    finance_page_id = task.get("finance_page_id")
                    inv_amount = task.get("amount", 0)
                    if finance_page_id and paid_amount and inv_amount:
                        diff_pct = abs(paid_amount - inv_amount) / max(inv_amount, 1)
                        if diff_pct > 0.10:
                            pending_state[phone] = {
                                "type": "factura_note",
                                "finance_page_id": finance_page_id,
                                "task_page_id": task["page_id"],
                                "paid_amount": paid_amount,
                                "payment_method": payment_method,
                                "invoice_amount": inv_amount,
                                "provider_name": task["name"],
                            }
                            t_result = f"Pagaste ${paid_amount:,.0f} pero la factura era ${inv_amount:,.0f}. ¿Querés agregar una nota?"
                            break
                        await _ds.mark_finance_paid(finance_page_id, paid_amount, payment_method)
                    ok = await mark_factura_task_paid(task["page_id"])
                    if ok:
                        marked.append(task["name"])
                if marked:
                    t_result = f"Marcado como pagado: {', '.join(marked)}."
                else:
                    t_result = f"No pude actualizar las tasks en Notion."
        elif t_name == "consultar_deudas":
            provider_filter = t_input.get("provider")
            impagas = await _ds.get_impaga_facturas(provider=provider_filter)
            if impagas:
                lines = []
                for e in impagas:
                    monto = f"${e.value_ars:,.0f}" if e.value_ars else "monto pendiente"
                    fecha = f" ({str(e.date)[:10]})" if e.date else ""
                    lines.append(f"- {e.name}: {monto}{fecha}")
                t_result = "Facturas y deudas pendientes:\n" + "\n".join(lines)
            else:
                t_result = "No hay facturas ni deudas pendientes."
        elif t_name == "historial_pagos":
            provider = t_input.get("provider", "")
            historial = await _ds.get_finance_history_by_provider(provider, limit=5)
            if historial:
                lines = []
                for e in historial:
                    monto = f"${e.value_ars:,.0f}"
                    fecha = str(e.date)[:10] if e.date else "fecha desconocida"
                    metodo = ""
                    nota = f" — {e.notes}" if e.notes else ""
                    lines.append(f"- {fecha}: {monto}{metodo}{nota}")
                t_result = f"Historial de pagos — {provider}:\n" + "\n".join(lines)
            else:
                t_result = f"No encontre historial de pagos para '{provider}'."
        elif t_name == "consultar_lugares_conocidos":
            places = user_prefs.get("known_places", [])
            if places:
                lines = []
                for p in places:
                    lat_s = f"{float(p['lat']):.5f}" if p.get("lat") is not None else "?"
                    lon_s = f"{float(p['lon']):.5f}" if p.get("lon") is not None else "?"
                    lines.append(f"- {p['name']}: {lat_s}, {lon_s} (radio {p.get('radius', 200)}m)")
                t_result = "Lugares conocidos:\n" + "\n".join(lines)
            else:
                t_result = "No hay lugares conocidos guardados todavia."
        elif t_name == "consultar_geo_reminders":
            if geo_reminders_cache:
                lines = []
                for r in geo_reminders_cache:
                    tipo = "🔁 Recurrente" if r.get("recurrent") else "1️⃣ Una vez"
                    if r.get("type") == "shop" and r.get("shop_name"):
                        lugar = f"cerca de {r['shop_name']} (radio {r.get('radius', 300)}m)"
                    elif r.get("lat") and r.get("lon"):
                        lugar = f"en coordenadas {r['lat']:.4f}, {r['lon']:.4f} (radio {r.get('radius', 300)}m)"
                    else:
                        lugar = "ubicacion no especificada"
                    lines.append(f"- {r['name']} — {lugar} — {tipo}")
                t_result = "Geo-reminders activos:\n" + "\n".join(lines)
            else:
                t_result = "No hay geo-reminders activos."
        elif t_name == "buscar_comercios_cercanos":
            lat, lon = get_current_location()
            nombre = t_input.get("nombre", "")
            radio = t_input.get("radio_metros", 1000)
            shops = await search_nearby_shops(lat, lon, radius=radio, name_filter=nombre)
            if shops:
                lines = []
                for s in shops[:5]:
                    line = f"- {s['name']} a {s['distance_m']}m"
                    if s.get("address"):
                        line += f" ({s['address']})"
                    if s.get("opening_hours"):
                        line += f" — {s['opening_hours']}"
                    line += f" — {s['maps_link']}"
                    lines.append(line)
                t_result = "\n".join(lines)
            else:
                t_result = f"No encontre '{nombre}' en un radio de {radio}m."
        elif t_name == "calcular_fecha":
            t_result = calcular_fecha_exacta(t_input.get("descripcion", ""))
        elif t_name == "buscar_gastos":
            query = t_input.get("query", "")
            mes = t_input.get("mes") or now.strftime("%Y-%m")
            t_result = await buscar_gastos(query, mes)
        elif t_name == "consultar_clima":
            w = await get_weather()
            if w:
                incluir_manana = t_input.get("incluir_manana", False)
                t_result = format_weather_chat(w, include_tomorrow=incluir_manana)
            else:
                t_result = "No pude obtener el clima en este momento."
        elif t_name == "consultar_gmail":
            if not user_prefs.get("service_providers"):
                inferred = await infer_service_providers()
                if inferred:
                    resumen = "\n".join("- " + k.capitalize() + ": *" + v + "*" for k, v in inferred.items())
                    pending_state[phone] = {
                        "type": "confirm_service_providers",
                        "proposed": inferred
                    }
                    await send_message(phone, "Encontre tus proveedores de servicios en tus mails:\n\n" + resumen + "\n\nEs correcto?")
                    await send_interactive_buttons(
                        phone,
                        "Confirmo estos proveedores?",
                        [
                            {"id": "providers_ok", "title": "Si, correcto"},
                            {"id": "providers_no", "title": "Quiero corregir"},
                        ]
                    )
                    t_result = "Inferi los proveedores y le pregunte al usuario para confirmar. No hay resultado de mail todavia."
                else:
                    t_result = "No encontre mails suficientes para identificar proveedores de servicios."
            else:
                gmail_data = await get_gmail_summary()
                t_result = gmail_data or "No encontre mails relevantes."
        elif t_name == "web_search":
            t_result = "Busqueda web ejecutada."
        elif t_name == "configurar_matrics":
            changed = []
            if t_input.get("greeting_name"):
                user_prefs["greeting_name"] = t_input["greeting_name"]
                changed.append("Saludo -> " + t_input["greeting_name"])
            if t_input.get("add_extra"):
                ex = user_prefs.get("resumen_extras", [])
                ex.append(t_input["add_extra"])
                user_prefs["resumen_extras"] = ex
                changed.append("Extra agregado: " + t_input["add_extra"])
            if t_input.get("remove_extra"):
                ex = user_prefs.get("resumen_extras", [])
                user_prefs["resumen_extras"] = [e for e in ex if t_input["remove_extra"].lower() not in e.lower()]
                changed.append("Extra removido: " + t_input["remove_extra"])
            if t_input.get("hour") is not None:
                h = int(t_input["hour"])
                m = int(t_input.get("minute", 0) or 0)
                if 0 <= h <= 23:
                    user_prefs["daily_summary_hour"] = h
                    user_prefs["daily_summary_minute"] = m
                    changed.append(f"Horario resumen -> {h:02d}:{m:02d}")
            if t_input.get("nocturno_enabled") is not None:
                user_prefs["resumen_nocturno_enabled"] = t_input["nocturno_enabled"]
                estado = "activado" if t_input["nocturno_enabled"] else "desactivado"
                changed.append("Resumen nocturno -> " + estado)
            if t_input.get("nocturno_hour") is not None:
                user_prefs["resumen_nocturno_hour"] = int(t_input["nocturno_hour"])
                changed.append(f"Hora nocturno -> {int(t_input['nocturno_hour']):02d}:00")
            if changed:
                await save_user_config(MY_NUMBER)
                t_result = "Configuracion actualizada: " + ", ".join(changed) + ". El cambio toma efecto inmediatamente (el cron lo va a respetar en la proxima verificacion, menos de 1 minuto)."
            else:
                t_result = "No se especifico que cambiar."
        elif t_name == "crear_proyecto":
            proj_name = t_input.get("name", "Proyecto")
            entry_type = t_input.get("entry_type", "Proyecto")
            area = t_input.get("area", "Personal")
            description = t_input.get("description", "")
            priority = t_input.get("priority")
            emoji = t_input.get("emoji", "📋")
            try:
                await _ds.create_project({
                    "name": proj_name, "entry_type": entry_type, "area": area,
                    "description": description, "priority": priority, "emoji": emoji,
                })
                t_result = "Proyecto creado: " + emoji + " " + proj_name + " (" + entry_type + ", " + area + "). Guardado en Notion."
            except Exception as e_proj:
                t_result = "Error: " + str(e_proj)[:100]
        elif t_name == "editar_evento":
            search_term = t_input.get("search_term")
            target_event, err = await _find_calendar_event(search_term, phone, target_date=t_input.get("target_date"))
            if not target_event:
                t_result = err or "No encontre el evento."
            else:
                event_id = target_event["id"]
                event_name = target_event.get("summary", "Evento")
                patch_body = {}
                if t_input.get("new_title"):
                    patch_body["summary"] = t_input["new_title"]
                if t_input.get("new_location"):
                    patch_body["location"] = t_input["new_location"]
                if t_input.get("new_description"):
                    patch_body["description"] = t_input["new_description"]
                if t_input.get("new_date") or t_input.get("new_time"):
                    if "dateTime" in target_event.get("start", {}):
                        old_dt = target_event["start"]["dateTime"][:16]
                        new_date = t_input.get("new_date") or old_dt[:10]
                        new_time = t_input.get("new_time") or old_dt[11:16]
                        patch_body["start"] = {"dateTime": f"{new_date}T{new_time}:00", "timeZone": "America/Argentina/Buenos_Aires"}
                        if "dateTime" in target_event.get("end", {}):
                            dur = datetime.strptime(target_event["end"]["dateTime"][:16], "%Y-%m-%dT%H:%M") - datetime.strptime(old_dt, "%Y-%m-%dT%H:%M")
                            new_end = datetime.strptime(f"{new_date}T{new_time}", "%Y-%m-%dT%H:%M") + dur
                            patch_body["end"] = {"dateTime": new_end.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}
                    elif t_input.get("new_date"):
                        patch_body["start"] = {"date": t_input["new_date"]}
                        patch_body["end"] = {"date": t_input["new_date"]}
                if not patch_body:
                    t_result = "No entendi que campo cambiar."
                else:
                    access_token = await get_gcal_access_token()
                    async with httpx.AsyncClient() as http:
                        update_r = await http.patch(
                            f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                            params={"sendUpdates": "none"},
                            json=patch_body
                        )
                    if update_r.status_code == 200:
                        new_summary = patch_body.get("summary", event_name)
                        last_event_touched[phone] = {"event_id": event_id, "summary": new_summary}
                        t_result = "Evento '" + event_name + "' actualizado correctamente."
                    else:
                        t_result = "Error actualizando: " + update_r.text[:100]
        return t_result

    # ── Primera ronda de tools ────────────────────────────────────────────
    tool_results = []
    for block in response.content:
        if block.type != "tool_use":
            continue
        try:
            result = await _execute_chat_tool(block.name, block.input)
        except Exception as e:
            result = "Error ejecutando " + block.name + ": " + str(e)[:100]
        tool_results.append({
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": result
        })

    if not tool_results:
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()
        add_to_history(phone, "assistant", reply)
        return reply

    messages = messages + [
        {"role": "assistant", "content": response.content},
        {"role": "user", "content": tool_results}
    ]

    # ── Loop para rondas adicionales de tools (max 4 rondas extra) ────────
    reply = ""
    for _round in range(4):
        try:
            next_response = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=800,
                system=system,
                messages=messages,
                tools=tools
            )
        except Exception:
            reply = "Error procesando tu mensaje."
            break

        round_text = next((b.text for b in next_response.content if hasattr(b, "text") and b.text), "").strip()
        if round_text:
            reply = round_text

        round_tools = [b for b in next_response.content if b.type == "tool_use"]
        if not round_tools:
            break

        round_results = []
        for block in round_tools:
            try:
                result = await _execute_chat_tool(block.name, block.input)
            except Exception as e:
                result = "Error: " + str(e)[:100]
            round_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result})

        messages = messages + [
            {"role": "assistant", "content": next_response.content},
            {"role": "user", "content": round_results}
        ]

    if not reply:
        reply = "No pude completar la consulta. Intenta de nuevo."

    add_to_history(phone, "assistant", reply)
    return reply
# ── HANDLER EVENTOS (tool calling) ────────────────────────────────────────────
async def handle_evento_agent(phone: str, text: str, image_b64=None, image_type=None) -> str | None:
    now = now_argentina()
    last_ev = last_event_touched.get(phone, {})
    last_ev_ctx = f"\nUltimo evento creado/editado: \"{last_ev['summary']}\"." if last_ev.get("summary") else ""

    tools = [
        {
            "name": "crear_evento",
            "description": "Crea un nuevo evento en Google Calendar. Usa cuando el usuario quiere agendar algo nuevo.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string", "description": "Titulo del evento"},
                    "date": {"type": "string", "description": "YYYY-MM-DD"},
                    "time": {"type": ["string", "null"], "description": "HH:MM o null para todo el dia"},
                    "duration_minutes": {"type": "integer", "description": "Duracion en minutos, default 60"},
                    "location": {"type": ["string", "null"]},
                    "description": {"type": ["string", "null"]},
                    "emoji": {"type": "string", "description": "Emoji representativo"},
                    "recurrence": {"type": ["string", "null"], "description": "RRULE string para eventos recurrentes. Ej: RRULE:FREQ=WEEKLY;BYDAY=MO o RRULE:FREQ=WEEKLY;BYDAY=TU;COUNT=4. Null si no es recurrente."}
                },
                "required": ["summary", "date", "emoji"]
            }
        },
        {
            "name": "editar_evento",
            "description": "Edita un evento existente. Si no se especifica search_term y hay un ultimo evento, se edita ese.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_term": {"type": ["string", "null"], "description": "Keyword para buscar el evento, o null para el ultimo tocado"},
                    "target_date": {"type": ["string", "null"], "description": "YYYY-MM-DD de la instancia especifica a editar. Siempre completar si el usuario menciona un dia ('el de mañana', 'el del jueves', etc.)"},
                    "new_title": {"type": ["string", "null"]},
                    "new_date": {"type": ["string", "null"], "description": "YYYY-MM-DD"},
                    "new_time": {"type": ["string", "null"], "description": "HH:MM"},
                    "new_location": {"type": ["string", "null"]},
                    "new_description": {"type": ["string", "null"]}
                },
                "required": []
            }
        },
        {
            "name": "eliminar_evento",
            "description": "Elimina uno o varios eventos del calendario.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_term": {"type": "string", "description": "Nombre o keyword del evento"},
                    "target_date": {"type": ["string", "null"], "description": "YYYY-MM-DD si menciona fecha"},
                    "delete_all": {"type": "boolean", "description": "True para borrar todos los de esa fecha"}
                },
                "required": ["search_term"]
            }
        },
        {
            "name": "calcular_fecha",
            "description": "Calcula la fecha exacta YYYY-MM-DD a partir de descripciones como 'el proximo viernes', 'el segundo sabado de mayo', 'dentro de 10 dias'. Usar cuando la fecha no esta en la tabla de referencia del sistema.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "descripcion": {"type": "string", "description": "La descripcion de la fecha tal como la dijo el usuario."}
                },
                "required": ["descripcion"]
            }
        },
        {
            "name": "consultar_calendario",
            "description": "Consulta eventos del calendario. Usa 'fecha' para consultar un dia especifico (chequeo de duplicados). Usa dias_adelante para ver una ventana mas amplia.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "dias_adelante": {"type": "integer", "description": "Default 7"},
                    "dias_atras": {"type": "integer", "description": "Default 0"},
                    "fecha": {"type": ["string", "null"], "description": "YYYY-MM-DD para consultar solo ese dia. Preferir esto al chequear si ya existe un evento en una fecha especifica."}
                },
                "required": []
            }
        }
    ]

    activities_ctx = get_activities_context()
    activities_section = f"\n\n{activities_ctx}" if activities_ctx else ""

    system = f"""Sos Knot, asistente personal en WhatsApp. Hablas en espanol rioplatense, natural y conciso.
Hoy: {hoy_str(now)}.
Calendario de referencia: {semana_str(now)}.{last_ev_ctx}{activities_section}
REGLA CRITICA: cuando el usuario menciona un dia de la semana, usa EXACTAMENTE la fecha de la tabla de arriba. NO calcules fechas mentalmente. NUNCA.
REGLA CRITICA 2: para calculos de fechas, dias de la semana, "que dia cae", "dentro de X dias", NO uses web_search. Usa SOLO la tabla de referencia.
REGLA CRITICA 3: antes de nombrar un dia de la semana, verificalo en la tabla. Ejemplo: si vas a decir "sabado 12/04", buscá 12/04 en la tabla. Si la tabla dice "domingo 12/04", corregite. NUNCA asumas el nombre del dia sin verificar.
REGLA CRITICA DE FECHAS: antes de crear o editar cualquier evento cuya fecha venga de lenguaje natural ("el proximo viernes", "el segundo sabado de septiembre", "en dos semanas"), SIEMPRE llama primero a calcular_fecha para obtener la fecha exacta. Nunca asumas la fecha directamente.

Tu tarea: gestionar eventos del calendario del usuario.
- Si el mensaje tiene titulo Y fecha claros -> usa crear_evento.
- Si quiere modificar un evento -> usa editar_evento.
- Si quiere borrar -> usa eliminar_evento. SIEMPRE incluye target_date cuando el usuario menciona un dia especifico ("el lunes", "ya no voy el martes", "al final el jueves no"). Sin target_date vas a borrar el evento equivocado.
- Si falta info esencial -> pregunta de forma natural y breve.
- Podes consultar el calendario primero si necesitas verificar algo.
- Si el usuario manda una imagen (flyer, screenshot de turno, invitacion), extrae la info y crea el evento.
REFERENCIAS RELATIVAS A OTROS EVENTOS ("después de funcional", "antes del trabajo", "después de la reunión"): si el usuario no da una fecha explícita pero hace referencia a otro evento, primero consultá el calendario para encontrar la próxima ocurrencia de ese evento y calculá la fecha/hora en base a eso. NUNCA asumas "mañana" ni ninguna fecha sin verificar.
MADRUGADA (00:00-05:00): si el usuario dice "mañana" o "hoy" en ese horario, preguntale si se refiere al día de hoy después de dormir o al día siguiente del calendario — porque a esa hora la gente suele hablar del día que viene subjetivamente.
IMPORTANTE: No inventes datos. Usa zona horaria Argentina (UTC-3).
El sistema ofrece recordatorios por separado después de crear/editar — vos no preguntes sobre recordatorios.
VERIFICACION OBLIGATORIA: despues de cada crear_evento o editar_evento, llama a consultar_calendario para verificar que el cambio quedo bien. Si no coincide con lo pedido, intentalo de nuevo. NUNCA confirmes un cambio sin verificarlo.
ANTES DE CREAR O EDITAR: llama a consultar_calendario con el parametro "fecha" igual a la fecha exacta mencionada. Si ya existe un evento similar en ESE DIA especifico → editar_evento. Si no existe en ese dia → crear_evento. NUNCA edites un evento de un dia distinto al que menciono el usuario.
CONFLICTOS DE HORARIO: si al verificar detectas que hay dos eventos a la misma hora, mencioná TODOS los eventos de ese día en orden cronológico para dar contexto completo. No sólo el que choca — así el usuario ve el panorama real del día y decide qué ajustar.
MULTIPLES EVENTOS EN UN MENSAJE O IMAGEN: procesa uno a la vez. Para cada fecha: 1) consultar_calendario, 2) si existe evento similar → editar_evento, si no existe → crear_evento, 3) verificar. Luego el siguiente.
VERIFICACION OBLIGATORIA: despues de cada crear_evento o editar_evento, llama a consultar_calendario para verificar que el cambio se refleja correctamente. Si el resultado no coincide con lo que se pidio, intentalo de nuevo. NUNCA confirmes un cambio sin verificarlo primero.
EVENTOS RECURRENTES - instancias especificas: cuando el usuario dice "el de hoy", "el de mañana", "el del jueves", siempre usa target_date con la fecha exacta correspondiente de la tabla de referencia. Sin target_date, la API devuelve la proxima instancia futura que puede ser incorrecta.
MULTIPLES CAMBIOS EN UN MENSAJE: si el usuario pide cambiar dos eventos distintos (ej: "el de hoy a las X y el de mañana a las Y"), hace UNA tool call por evento, en orden, verificando cada una antes de pasar a la siguiente.

EVENTOS RECURRENTES:
- Si el usuario dice "todos los lunes", "cada martes", etc., usa el campo recurrence con un RRULE valido.
- El BYDAY del RRULE DEBE coincidir con el dia de la semana de la fecha de inicio.
- Dias RRULE: MO=lunes, TU=martes, WE=miercoles, TH=jueves, FR=viernes, SA=sabado, SU=domingo.
- Ejemplo: si pide "todos los lunes a las 17:20", date debe ser el PROXIMO lunes, y recurrence "RRULE:FREQ=WEEKLY;BYDAY=MO".
- Si dice "durante este mes", agrega COUNT con las semanas restantes del mes.
- Si no especifica fin, no pongas COUNT ni UNTIL (sera indefinido).
- NUNCA pongas una fecha que caiga en un dia diferente al BYDAY del RRULE.
- IMPORTANTE: usa siempre null en JSON (no None). Los campos opcionales van con null, nunca con la palabra None."""

    content = []
    if image_b64:
        content.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    content.append({"type": "text", "text": text or "(ver imagen adjunta)"})

    messages = get_history(phone) + [{"role": "user", "content": content}]

    try:
        response = await claude_create(
            model="claude-sonnet-4-20250514", max_tokens=1000,
            system=system, messages=messages, tools=tools
        )
    except Exception:
        return "Error procesando tu mensaje. Intenta de nuevo."

    if response.stop_reason == "end_turn":
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()
        add_to_history(phone, "user", text)
        add_to_history(phone, "assistant", reply)
        return reply

    # ── Ejecutar primera ronda de tools ───────────────────────────────────
    evento_creado = None
    eventos_creados_count = 0
    eventos_tocados = []  # todos los creados/editados con hora, para ofrecer recordatorios
    high_impact_pending = None  # accion de alto impacto que requiere confirmacion
    geocode_candidate = None  # 4a: candidato de geocodificacion para confirmar con el usuario

    async def _execute_evento_tool(t_name, t_input):
        nonlocal evento_creado, eventos_creados_count, high_impact_pending, geocode_candidate
        t_result = ""
        if t_name == "crear_evento":
            data = dict(t_input)
            if not data.get("duration_minutes"):
                data["duration_minutes"] = 60
            if data.get("recurrence"):
                data["date"] = fix_recurring_event_date(data["date"], data["recurrence"])
            guardado, event_id = await create_evento_gcal(data)
            if guardado and event_id:
                last_event_touched[phone] = {"event_id": event_id, "summary": data.get("summary", "Evento")}
                evento_creado = {"data": data, "event_id": event_id}
                eventos_creados_count += 1
                if data.get("time"):
                    eventos_tocados.append({"summary": data.get("summary", "Evento"), "date": data["date"], "time": data["time"]})
                # Auto-guardar en perfil si es recurrente
                if data.get("recurrence") and data.get("time"):
                    name_key = data.get("summary", "").lower().strip()
                    if name_key and name_key not in user_prefs.get("activities", {}):
                        from_rrule = data["recurrence"]
                        day_codes = []
                        for part in from_rrule.split(";"):
                            if "BYDAY=" in part:
                                day_codes = part.split("BYDAY=")[1].strip().split(",")
                        rrule_to_dia = {"MO":"lunes","TU":"martes","WE":"miercoles","TH":"jueves","FR":"viernes","SA":"sabado","SU":"domingo"}
                        days_list = [rrule_to_dia.get(d.strip(), d) for d in day_codes]
                        if not user_prefs.get("activities"):
                            user_prefs["activities"] = {}
                        user_prefs["activities"][name_key] = {"days": days_list, "time": data["time"]}
                        await save_user_config(phone)
                event_summary = data.get("summary", "")
                event_desc_bg = f"Evento creado: '{event_summary}', fecha: {data.get('date', '')}, hora: {data.get('time', '')}{', recurrente' if data.get('recurrence') else ''}"
                asyncio.create_task(update_domain_profile_bg("actividad_fisica", event_desc_bg))
                _SALUD_KEYWORDS = {"medico", "médico", "doctor", "doctora", "clinica", "clínica",
                                   "hospital", "turno", "cita", "consulta", "dentista", "odontologo",
                                   "odontólogo", "psicologo", "psicólogo", "psiquiatra", "kinesiolog",
                                   "nutricionista", "oftalmologo", "oftalmólogo", "dermatologo",
                                   "traumatólogo", "traumatologo", "cardiologo", "cardiólogo",
                                   "ginecolog", "urologo", "urólogo", "analisis", "análisis",
                                   "laboratorio", "ecografia", "ecografía", "radiografia"}
                summary_lower = event_summary.lower()
                if any(k in summary_lower for k in _SALUD_KEYWORDS):
                    asyncio.create_task(update_domain_profile_bg(
                        "salud",
                        f"Cita médica en calendario: '{event_summary}', fecha: {data.get('date', '')}"
                    ))
                hora = f" a las {data['time']}" if data.get("time") else ""
                try:
                    fecha = datetime.strptime(data["date"], "%Y-%m-%d").strftime("%d/%m/%Y")
                except Exception:
                    fecha = data["date"]
                t_result = "Evento creado: " + data.get("emoji", "") + " " + data["summary"] + " el " + fecha + hora + "."
                # Trigger #7: primer evento con ubicacion → ofrecer aviso de tiempo de viaje
                if data.get("location"):
                    emit_hint(phone, suggestion_gate.Hint(
                        trigger_id="event_with_location",
                        message=f"🚗 *{data.get('summary','el evento')}* tiene ubicación. Si querés, te puedo avisar antes con tiempo de salida según el tráfico. Decime *si* o *no*.",
                        action_intent="enable_travel_time_alert",
                        payload={"event_id": event_id, "location": data.get("location"), "summary": data.get("summary", "")},
                    ))
                if data.get("location"):
                    t_result += " Ubicacion: " + data["location"] + "."
                # 4a: intentar geocodificar la ubicacion del evento
                if data.get("location") and not geocode_candidate:
                    try:
                        _city = user_prefs.get("city", "Neuquén")
                        async with httpx.AsyncClient(timeout=5) as _hg:
                            _gq = f"{data['location']}, {_city}"
                            _gr = await _hg.get(
                                "https://nominatim.openstreetmap.org/search",
                                params={"q": _gq, "format": "json", "limit": 1, "addressdetails": 1},
                                headers={"User-Agent": "Knot/1.0"}
                            )
                            if _gr.status_code == 200 and _gr.json():
                                _gresult = _gr.json()[0]
                                geocode_candidate = {
                                    "event_id": event_id,
                                    "lat": float(_gresult["lat"]),
                                    "lon": float(_gresult["lon"]),
                                    "place_name": _format_place_name(_gresult, data["location"]),
                                    "raw_location": data["location"],
                                }
                    except Exception:
                        pass
            else:
                t_result = "Error creando el evento en Google Calendar."

        elif t_name == "editar_evento":
            search_term = t_input.get("search_term")
            target_date_param = t_input.get("target_date")
            target_event, err = await _find_calendar_event(search_term, phone, target_date=target_date_param)
            if not target_event:
                t_result = err
            else:
                event_id = target_event["id"]
                event_name = target_event.get("summary", "Evento")
                is_recurring = bool(target_event.get("recurrence") or target_event.get("recurringEventId"))
                patch_body = {}
                if t_input.get("new_title"):
                    patch_body["summary"] = t_input["new_title"]
                if t_input.get("new_location"):
                    patch_body["location"] = t_input["new_location"]
                if t_input.get("new_description"):
                    patch_body["description"] = t_input["new_description"]
                if t_input.get("new_date") or t_input.get("new_time"):
                    if "dateTime" in target_event.get("start", {}):
                        old_dt = target_event["start"]["dateTime"][:16]
                        new_date = t_input.get("new_date") or old_dt[:10]
                        new_time = t_input.get("new_time") or old_dt[11:16]
                        patch_body["start"] = {"dateTime": f"{new_date}T{new_time}:00", "timeZone": "America/Argentina/Buenos_Aires"}
                        if "dateTime" in target_event.get("end", {}):
                            dur = datetime.strptime(target_event["end"]["dateTime"][:16], "%Y-%m-%dT%H:%M") - datetime.strptime(old_dt, "%Y-%m-%dT%H:%M")
                            new_end = datetime.strptime(f"{new_date}T{new_time}", "%Y-%m-%dT%H:%M") + dur
                            patch_body["end"] = {"dateTime": new_end.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"}
                    elif t_input.get("new_date"):
                        patch_body["start"] = {"date": t_input["new_date"]}
                        patch_body["end"] = {"date": t_input["new_date"]}
                if not patch_body:
                    t_result = "No entendi que campo cambiar del evento."
                elif is_recurring and not target_date_param:
                    # Alto impacto: editar evento recurrente sin fecha especifica = afecta TODAS las instancias
                    old_time = target_event.get("start", {}).get("dateTime", "")[:16][11:] if "dateTime" in target_event.get("start", {}) else ""
                    new_time_val = t_input.get("new_time", "")
                    descripcion = f"Cambiar *todos* los _{event_name}_ (evento recurrente)"
                    if new_time_val and old_time:
                        descripcion += f" de {old_time} a {new_time_val}"
                    high_impact_pending = {"action": "edit_recurring", "event_id": event_id, "event_name": event_name, "patch_body": patch_body, "descripcion": descripcion}
                    t_result = f"CONFIRMACION_REQUERIDA: {descripcion}"
                else:
                    access_token = await get_gcal_access_token()
                    async with httpx.AsyncClient() as http:
                        update_r = await http.patch(
                            f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                            params={"sendUpdates": "none"},
                            json=patch_body
                        )
                    if update_r.status_code == 200:
                        new_summary = patch_body.get("summary", event_name)
                        last_event_touched[phone] = {"event_id": event_id, "summary": new_summary}
                        t_result = "Evento '" + event_name + "' actualizado correctamente."
                        new_time = t_input.get("new_time") or (target_event.get("start", {}).get("dateTime", "")[:16][11:] if "dateTime" in target_event.get("start", {}) else None)
                        new_date = t_input.get("new_date") or (target_event.get("start", {}).get("dateTime", "")[:10] if "dateTime" in target_event.get("start", {}) else None)
                        if new_time and new_date:
                            eventos_tocados.append({"summary": new_summary, "date": new_date, "time": new_time})
                    else:
                        t_result = "Error actualizando: " + update_r.text[:100]
        elif t_name == "eliminar_evento":
            search_term = t_input.get("search_term", "")
            target_date = t_input.get("target_date")
            delete_all = t_input.get("delete_all", False)
            access_token = await get_gcal_access_token()
            if not access_token:
                t_result = "Calendar no configurado"
            else:
                now_dt = now_argentina()
                if target_date:
                    t_min = f"{target_date}T00:00:00-03:00"
                    t_max = f"{target_date}T23:59:59-03:00"
                else:
                    t_min = (now_dt - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00-03:00")
                    t_max = (now_dt + timedelta(days=60)).strftime("%Y-%m-%dT23:59:59-03:00")
                async with httpx.AsyncClient() as http:
                    headers = {"Authorization": f"Bearer {access_token}"}
                    r = await http.get(
                        "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                        headers=headers,
                        params={"q": search_term, "timeMin": t_min, "timeMax": t_max,
                                "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"}
                    )
                    if r.status_code != 200 or not r.json().get("items"):
                        t_result = "No encontre eventos con '" + search_term + "'."
                    else:
                        events = [e for e in r.json()["items"] if "[TEMP]" not in (e.get("description") or "")]
                        to_delete = events if delete_all else events[:1]
                        # Chequeo de alto impacto: evento recurrente sin fecha especifica
                        has_recurring = any(e.get("recurrence") or e.get("recurringEventId") for e in to_delete)
                        if has_recurring and not target_date:
                            ev_names = ", ".join(set(e.get("summary","Evento") for e in to_delete))
                            descripcion = f"Eliminar *todas* las instancias de _{ev_names}_ (evento recurrente)"
                            high_impact_pending = {"action": "delete_recurring", "events": [{"id": e["id"], "summary": e.get("summary","Evento")} for e in to_delete], "descripcion": descripcion}
                            t_result = f"CONFIRMACION_REQUERIDA: {descripcion}"
                        else:
                            ev = to_delete[0]
                            ev_name = ev.get("summary", "Evento")
                            expires_at = (now_argentina() + timedelta(minutes=5)).replace(tzinfo=None).isoformat()
                            pending_state[phone] = {
                                "type": "confirm_delete", "action": "event",
                                "page_id": ev["id"], "name": ev_name,
                                "expires_at": expires_at,
                                "extra_events": [{"id": e["id"], "summary": e.get("summary", "Evento")} for e in to_delete[1:]],
                            }
                            await send_interactive_buttons(phone, f"¿Eliminás *{ev_name}*?", [
                                {"id": "confirm_delete_yes", "title": "Sí, eliminalo"},
                                {"id": "confirm_delete_no", "title": "No, cancelar"},
                            ])
                            t_result = f"Pedí confirmación al usuario para eliminar {ev_name}."

        elif t_name == "calcular_fecha":
            t_result = calcular_fecha_exacta(t_input.get("descripcion", ""))

        elif t_name == "consultar_calendario":
            fecha = t_input.get("fecha")
            if fecha:
                t_result = await query_calendar_date(fecha) or "No hay eventos ese dia."
            else:
                dias = t_input.get("dias_adelante", 7)
                dias_atras = t_input.get("dias_atras", 0)
                t_result = await query_calendar(days_ahead=dias, days_back=dias_atras) or "No hay eventos."

        return t_result

    # Primera ronda
    tool_results = []
    for block in response.content:
        if block.type != "tool_use":
            continue
        try:
            result = await _execute_evento_tool(block.name, block.input)
        except Exception as e:
            result = f"Error: {str(e)[:100]}"
        tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result})

    if not tool_results:
        return next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()

    messages = messages + [
        {"role": "assistant", "content": response.content},
        {"role": "user", "content": tool_results}
    ]

    # ── Loop para rondas adicionales de tools (max 8 rondas extra) ────────
    reply = ""
    for _round in range(8):
        try:
            next_response = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=1500,
                system=system, messages=messages, tools=tools
            )
        except Exception:
            reply = "Error procesando tu mensaje."
            break

        round_text = next((b.text for b in next_response.content if hasattr(b, "text") and b.text), "").strip()
        if round_text:
            reply = round_text

        round_tools = [b for b in next_response.content if b.type == "tool_use"]
        if not round_tools:
            break

        round_results = []
        for block in round_tools:
            try:
                result = await _execute_evento_tool(block.name, block.input)
            except Exception as e:
                result = f"Error: {str(e)[:100]}"
            round_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result})

        messages = messages + [
            {"role": "assistant", "content": next_response.content},
            {"role": "user", "content": round_results}
        ]

    if not reply:
        reply = "Listo, revise tu calendario. Necesitas algo mas?"

    # 4b: Ofrecer recurrencia si hay señales y el evento no es ya recurrente
    if evento_creado and not evento_creado["data"].get("recurrence"):
        _ev_data = evento_creado["data"]
        try:
            _ev_date = datetime.strptime(_ev_data["date"], "%Y-%m-%d")
        except Exception:
            _ev_date = None
        if _ev_date:
            _text_lower = text.lower()
            _rec_keywords = ["los lunes", "los martes", "los miercoles", "los miércoles",
                             "los jueves", "los viernes", "los sabados", "los sábados",
                             "los domingos", "cada semana", "todas las semanas", "todos los"]
            _has_rec = any(kw in _text_lower for kw in _rec_keywords)
            if not _has_rec:
                try:
                    _first_word = (_ev_data.get("summary", "").lower().split() or [""])[0]
                    _at_rec = await get_gcal_access_token()
                    if _at_rec and len(_first_word) >= 3:
                        async with httpx.AsyncClient(timeout=5) as _hrec:
                            _past_dt = _ev_date - timedelta(weeks=3)
                            _rr = await _hrec.get(
                                "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                                headers={"Authorization": f"Bearer {_at_rec}"},
                                params={"q": _first_word,
                                        "timeMin": _past_dt.strftime("%Y-%m-%dT00:00:00-03:00"),
                                        "timeMax": _ev_date.strftime("%Y-%m-%dT00:00:00-03:00"),
                                        "singleEvents": "true", "maxResults": "10"}
                            )
                            if _rr.status_code == 200:
                                _prev = [e for e in _rr.json().get("items", [])
                                         if e.get("start", {}).get("dateTime")
                                         and datetime.strptime(e["start"]["dateTime"][:10], "%Y-%m-%d").weekday() == _ev_date.weekday()
                                         and _first_word in e.get("summary", "").lower()]
                                if len(_prev) >= 2:
                                    _has_rec = True
                except Exception:
                    pass
            if _has_rec:
                _rrule_map = {0:"MO",1:"TU",2:"WE",3:"TH",4:"FR",5:"SA",6:"SU"}
                _rrule_day = _rrule_map.get(_ev_date.weekday(), "MO")
                _dia_nombre = DIAS_SEMANA[_ev_date.weekday()]
                pending_state[phone] = {
                    "type": "recurrence_offer",
                    "event_id": evento_creado["event_id"],
                    "summary": _ev_data.get("summary", "Evento"),
                    "rrule_day": _rrule_day,
                    "date": _ev_data["date"],
                    "eventos_tocados": eventos_tocados,
                }
                await send_message(phone, reply)
                await send_interactive_buttons(
                    phone,
                    f"¿Lo agrego como evento recurrente cada {_dia_nombre}?",
                    [
                        {"id": "recurrence_yes", "title": "Sí, hacerlo recurrente"},
                        {"id": "recurrence_no",  "title": "No, solo esta vez"},
                    ]
                )
                add_to_history(phone, "user", text)
                add_to_history(phone, "assistant", reply)
                return None

    # 4a: Preguntar al usuario si el lugar geocodificado es correcto (solo si no hay otro pending flow)
    if geocode_candidate and not pending_state.get(phone):
        gc = geocode_candidate
        short_addr = gc["place_name"].split(",")[0].strip()
        pending_state[phone] = {
            "type": "geocode_confirm",
            "event_id": gc["event_id"],
            "lat": gc["lat"],
            "lon": gc["lon"],
            "place_name": gc["place_name"],
            "raw_location": gc["raw_location"],
        }
        add_to_history(phone, "user", text)
        add_to_history(phone, "assistant", reply)
        await send_interactive_buttons(
            phone,
            f"¿*{gc['raw_location']}* queda en {short_addr}?",
            [
                {"id": "geocode_yes", "title": "Sí, guardá esa ubicación"},
                {"id": "geocode_no",  "title": "No, no guardes"},
            ]
        )
        return reply

    # Interceptar accion de alto impacto — pedir confirmacion antes de ejecutar
    if high_impact_pending:
        descripcion = high_impact_pending["descripcion"]
        pending_state[phone] = {"type": "confirm_high_impact", **high_impact_pending}
        await send_interactive_buttons(
            phone,
            f"⚠️ Esto va a {descripcion.lower()}.\n¿Confirmas?",
            [
                {"id": "high_impact_yes", "title": "Si, hacer"},
                {"id": "high_impact_no",  "title": "No, cancelar"},
            ]
        )
        add_to_history(phone, "user", text)
        add_to_history(phone, "assistant", reply)
        return None

    # Ofrecer recordatorio si hay eventos con hora (creados o editados)
    if eventos_tocados:
        await send_message(phone, reply)
        # Caso evento recurrente unico
        if eventos_creados_count == 1 and evento_creado and evento_creado["data"].get("recurrence"):
            data = evento_creado["data"]
            pending_state[phone] = {
                "type": "recurring_event_reminder",
                "event_id": evento_creado["event_id"],
                "summary": data.get("summary", "Evento"),
            }
            await send_message(phone, "⏰ ¿Querés que te avise antes de cada " + data.get("summary", "sesión") + "? Decime con cuánta anticipación (ej: '30 min', '1 hora', 'la noche anterior'). Podés elegir hasta 2 recordatorios, o mandá 'no' para omitir.")
        else:
            # Construir descripcion de eventos con fecha y hora
            lineas = []
            for ev in eventos_tocados:
                try:
                    fecha_fmt = datetime.strptime(ev["date"], "%Y-%m-%d")
                    dia = DIAS_SEMANA[fecha_fmt.weekday()]
                    fecha_label = f"{dia} {fecha_fmt.strftime('%d/%m')} a las {ev['time']}"
                except Exception:
                    fecha_label = f"{ev['date']} {ev['time']}"
                lineas.append(f"📅 {ev['summary']} — {fecha_label}")
            eventos_str = "\n".join(lineas)
            pending_state[phone] = {
                "type": "event_reminder",
                "events": [{"summary": ev["summary"], "event_datetime": f"{ev['date']}T{ev['time']}"} for ev in eventos_tocados],
            }
            await send_interactive_buttons(
                phone,
                f"⏰ ¿Querés un recordatorio?\n{eventos_str}",
                [
                    {"id": "rem_15", "title": "15 min antes"},
                    {"id": "rem_60", "title": "1 hora antes"},
                    {"id": "rem_no", "title": "No gracias"},
                ]
            )
        add_to_history(phone, "user", text)
        add_to_history(phone, "assistant", reply)
        return None

    if pending_state.get(phone, {}).get("type") == "confirm_delete":
        add_to_history(phone, "user", text)
        return None

    add_to_history(phone, "user", text)
    add_to_history(phone, "assistant", reply)
    return reply

# ── Config persistente en Notion ───────────────────────────────────────────────

# ── MODULO REUNIONES ──────────────────────────────────────────────────────────
async def handle_reunion(text: str, image_b64: str = None, image_type: str = None, phone: str = None) -> str:
    now = now_argentina()
    content_parts = []
    if image_b64:
        content_parts.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    prompt_reunion = (
        f"Hoy: {now.strftime('%Y-%m-%d %H:%M')}\n"
        f"Mensaje: {text or '(ver imagen adjunta)'}\n\n"
        "Extrae info de la reunion. Responde SOLO JSON:\n"
        '{"nombre": "titulo/asunto de la reunion",'
        '"con_quien": "nombre(s) de los participantes o null",'
        '"fecha": "YYYY-MM-DD o null si no se menciona",'
        '"notas": "transcripcion o resumen de las notas"}'
    )
    content_parts.append({"type": "text", "text": prompt_reunion})

    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=600,
        system="Extrae info de notas de reunion. Responde SOLO JSON valido sin markdown.",
        messages=[{"role": "user", "content": content_parts}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()

    try:
        data = json.loads(raw)
    except Exception:
        return "No pude interpretar las notas de la reunion"

    nombre    = data.get("nombre") or "Reunion"
    con_quien = data.get("con_quien") or ""
    fecha     = data.get("fecha") or now.strftime("%Y-%m-%d")
    notas     = data.get("notas") or ""

    cal_link = ""
    access_token = await get_gcal_access_token()
    if access_token and con_quien:
        try:
            async with httpx.AsyncClient() as http:
                r = await http.get(
                    "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                    headers={"Authorization": f"Bearer {access_token}"},
                    params={"q": con_quien, "timeMin": f"{fecha}T00:00:00-03:00",
                            "timeMax": f"{fecha}T23:59:59-03:00",
                            "singleEvents": "true", "maxResults": "3"}
                )
                if r.status_code == 200:
                    events = r.json().get("items", [])
                    if events:
                        cal_link = events[0].get("htmlLink", "")
        except Exception:
            pass

    try:
        meeting = await _ds.create_meeting({
            "name": nombre, "with_whom": con_quien, "date": fecha,
            "notes": notas, "calendar_link": cal_link,
        })
    except Exception as e:
        return error_servicio("notion")

    try:
        fecha_fmt = datetime.strptime(fecha, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        fecha_fmt = fecha
    con_str = f" with {con_quien}" if con_quien else ""
    cal_str = f"\nVinculada al evento de Calendar" if cal_link else ""
    asyncio.create_task(update_domain_profile_bg(
        "social",
        f"Reunión guardada: '{nombre}'{f', con {con_quien}' if con_quien else ''}, fecha: {fecha}"
    ))
    reply = f"*{nombre}* guardada en Meetings{cal_str}\n{fecha_fmt}{con_str}\n\nNotas guardadas en Notion"
    if phone:
        expires_at = (now_argentina() + timedelta(seconds=60)).replace(tzinfo=None).isoformat()
        pending_state[phone] = {
            "type": "undo_window", "action": "meeting",
            "page_id": meeting.id, "name": nombre, "expires_at": expires_at,
        }
        reply += "\n\n_Si algo no quedó bien, avisame._"
    return reply


async def editar_reunion(text: str) -> tuple[bool, str]:
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=250,
        system="Extrae el nombre de la reunión a editar y los campos a actualizar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": (
            f"Mensaje: {text}\n"
            'Responde: {"search_term": "nombre de la reunion", "updates": {"name": "nuevo nombre o null", "notes": "nuevas notas o null", "with_whom": "nueva persona o null"}}'
        )}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        parsed = json.loads(raw)
    except Exception:
        return False, "No entendi qué reunión querés editar"
    search_term = parsed.get("search_term", "")
    updates = {k: v for k, v in parsed.get("updates", {}).items() if v is not None}
    if not search_term:
        return False, "No entendi qué reunión querés editar"
    results = await _ds.search_meetings(search_term)
    if not results:
        return False, f"No encontré ninguna reunión llamada _{search_term}_"
    meeting = results[0]
    if not updates:
        return False, f"No entendi qué querés cambiar de _{meeting.name}_"
    await _ds.update_meeting(meeting.id, updates)
    changes = ", ".join(f"{k}: {v}" for k, v in updates.items())
    return True, f"*{meeting.name}* actualizada: {changes}"


async def eliminar_reunion(text: str, phone: str = None) -> tuple[bool, str]:
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=100,
        system="Extrae el nombre de la reunión a eliminar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f'Mensaje: {text}\nResponde: {{"search_term": "nombre de la reunion"}}'}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    search_term = json.loads(raw).get("search_term", "")
    if not search_term:
        return False, "No entendi qué reunión querés eliminar"
    results = await _ds.search_meetings(search_term)
    if not results:
        return False, f"No encontré ninguna reunión llamada _{search_term}_"
    meeting = results[0]
    if phone:
        expires_at = (now_argentina() + timedelta(minutes=5)).replace(tzinfo=None).isoformat()
        pending_state[phone] = {
            "type": "confirm_delete", "action": "meeting",
            "page_id": meeting.id, "name": meeting.name, "expires_at": expires_at,
        }
        await send_interactive_buttons(phone, f"¿Eliminás *{meeting.name}*?", [
            {"id": "confirm_delete_yes", "title": "Sí, eliminala"},
            {"id": "confirm_delete_no", "title": "No, cancelar"},
        ])
        return True, ""
    ok = await _ds.archive_meeting(meeting.id)
    return (True, f"*{meeting.name}* eliminada de Notion") if ok else (False, "Error eliminando la reunión")


# ── MODULO SALUD ──────────────────────────────────────────────────────────────

async def create_health_record(data: dict) -> tuple[bool, str]:
    return await _ds.create_health_record(data)


async def query_health_records(type_filter: str = None, specialty_filter: str = None, limit: int = 5) -> list[dict]:
    return await _ds.query_health_records(type_filter, specialty_filter, limit)


async def create_medication(data: dict) -> tuple[bool, str]:
    return await _ds.create_medication(data)


async def query_medications(only_active: bool = False) -> list[dict]:
    return await _ds.query_medications(only_active)


async def update_medication(page_id: str, updates: dict) -> bool:
    return await _ds.update_medication(page_id, updates)


async def handle_salud_agent(phone: str, text: str, image_b64: str = None, image_type: str = None) -> str:
    now = now_argentina()
    tools = [
        {
            "name": "guardar_registro_salud",
            "description": "Guarda un registro médico en Notion: análisis de sangre, consulta, diagnóstico, vacuna, etc. Si hay imagen de un documento médico, extraé todos los datos vos mismo.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "name":       {"type": "string", "description": "Nombre descriptivo. Ej: 'Análisis de sangre — abr 2026'"},
                    "type":       {"type": "string", "enum": ["Análisis", "Consulta", "Diagnóstico", "Vacuna", "Cirugía", "Otro"]},
                    "date":       {"type": "string", "description": "YYYY-MM-DD"},
                    "specialty":  {"type": "string", "enum": ["Clínica General", "Odontología", "Oncología", "Psicología", "Cardiología", "Kinesiología", "Nutrición", "Oftalmología", "Traumatología", "Ginecología", "Urología", "Dermatología", "Otra"]},
                    "doctor":     {"type": ["string", "null"]},
                    "summary":    {"type": "string", "description": "Resumen completo del contenido del documento"},
                    "key_values": {"type": ["string", "null"], "description": "JSON con valores numéricos clave. Ej: '{\"colesterol_total\": 185, \"glucosa\": 92, \"unidad\": \"mg/dL\"}'"},
                    "notes":      {"type": ["string", "null"]},
                },
                "required": ["name", "type", "date", "summary"]
            }
        },
        {
            "name": "guardar_medicacion",
            "description": "Guarda un medicamento que el usuario toma o tomó.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "name":          {"type": "string"},
                    "dose":          {"type": ["string", "null"], "description": "Ej: '500mg'"},
                    "frequency":     {"type": ["string", "null"], "description": "Ej: '1 vez por día con las comidas'"},
                    "prescribed_by": {"type": ["string", "null"]},
                    "start_date":    {"type": ["string", "null"], "description": "YYYY-MM-DD"},
                    "end_date":      {"type": ["string", "null"], "description": "YYYY-MM-DD si ya terminó"},
                    "condition":     {"type": ["string", "null"], "description": "Para qué lo toma"},
                    "active":        {"type": "boolean"},
                    "notes":         {"type": ["string", "null"]},
                },
                "required": ["name", "active"]
            }
        },
        {
            "name": "consultar_registros_salud",
            "description": "Consulta registros médicos guardados para responder preguntas del usuario.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "type_filter":      {"type": ["string", "null"]},
                    "specialty_filter": {"type": ["string", "null"]},
                    "limit":            {"type": "integer", "description": "Default 5, max 20"},
                },
                "required": []
            }
        },
        {
            "name": "consultar_medicaciones",
            "description": "Consulta medicamentos registrados.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "solo_activas": {"type": "boolean"},
                },
                "required": []
            }
        },
        {
            "name": "actualizar_medicacion",
            "description": "Actualiza una medicación: marcarla como inactiva, cambiar dosis, etc. Obtené el id de consultar_medicaciones.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "medication_id": {"type": "string"},
                    "active":        {"type": ["boolean", "null"]},
                    "dose":          {"type": ["string", "null"]},
                    "frequency":     {"type": ["string", "null"]},
                    "end_date":      {"type": ["string", "null"]},
                    "notes":         {"type": ["string", "null"]},
                },
                "required": ["medication_id"]
            }
        },
        {
            "name": "editar_registro_salud",
            "description": "Edita un registro médico existente: corrige el resumen, notas, valores clave o médico. Obtené el id de consultar_registros_salud.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "record_id":  {"type": "string"},
                    "summary":    {"type": ["string", "null"]},
                    "key_values": {"type": ["string", "null"], "description": "JSON con valores numéricos clave"},
                    "notes":      {"type": ["string", "null"]},
                    "doctor":     {"type": ["string", "null"]},
                },
                "required": ["record_id"]
            }
        },
        {
            "name": "eliminar_registro_salud",
            "description": "Elimina (archiva) un registro médico. Obtené el id de consultar_registros_salud.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "record_id": {"type": "string"},
                },
                "required": ["record_id"]
            }
        },
    ]

    content = []
    if image_b64:
        content.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    content.append({"type": "text", "text": text or "(ver imagen adjunta)"})

    system = f"""Sos Knot, asistente personal en WhatsApp. Hablas en espanol rioplatense, natural y conciso.
Hoy: {now.strftime('%d/%m/%Y')}.

Tu tarea: ayudar al usuario a organizar su informacion medica personal. Podes guardar analisis, consultas, diagnosticos, medicaciones, y responder preguntas sobre el historial.

REGLAS:
- Nunca diagnosticas ni opinas si algo es grave.
- Nunca recomendas ni modificas tratamientos medicos.
- Si el usuario pregunta si un valor es preocupante o que deberia hacer, decile que esa pregunta es para su medico. Vos solo organizas la info.
- Si hay imagen de un analisis o documento: extraé todos los datos vos mismo, no le pidas al usuario que los dicte.
- Para analisis de sangre u otros examenes: extraé los valores numericos en key_values como JSON.
- Si el usuario MENCIONA que necesita comprar medicamento (no preguntar diagnóstico), NO le digas que vaya al medico ni le rechaces ayuda. En su lugar, ofrecele:
  "¿Querés que te avise cuando pases cerca de una farmacia? O si preferís te puedo recordar a una hora específica para que pases a comprarlo."
  Y dejá que el usuario elija. NO uses ninguna tool en ese caso, solo respondé el mensaje."""

    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=1500,
        system=system,
        messages=get_history(phone) + [{"role": "user", "content": content}],
        tools=tools
    )

    if response.stop_reason == "end_turn":
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()
        add_to_history(phone, "user", text)
        add_to_history(phone, "assistant", reply)
        return reply

    tool_results = []
    for block in response.content:
        if block.type != "tool_use":
            continue
        inp = dict(block.input)
        tr = ""

        if block.name == "guardar_registro_salud":
            ok, pid = await create_health_record(inp)
            if ok:
                tr = f"Guardado. ID: {pid}"
                asyncio.create_task(update_domain_profile_bg(
                    "salud",
                    f"Nuevo registro: {inp.get('name')}, tipo: {inp.get('type')}, especialidad: {inp.get('specialty','')}, fecha: {inp.get('date','')}, resumen: {inp.get('summary','')[:200]}"
                ))
            else:
                tr = f"Error: {pid}"

        elif block.name == "guardar_medicacion":
            ok, pid = await create_medication(inp)
            if ok:
                tr = f"Medicación guardada. ID: {pid}"
                asyncio.create_task(update_domain_profile_bg(
                    "salud",
                    f"Medicación {'activa' if inp.get('active') else 'finalizada'}: {inp.get('name')}, dosis: {inp.get('dose','-')}, frecuencia: {inp.get('frequency','-')}, para: {inp.get('condition','-')}"
                ))
            else:
                tr = f"Error: {pid}"

        elif block.name == "consultar_registros_salud":
            records = await query_health_records(
                type_filter=inp.get("type_filter"),
                specialty_filter=inp.get("specialty_filter"),
                limit=min(inp.get("limit", 5), 20)
            )
            if not records:
                tr = "No hay registros con esos filtros."
            else:
                lines = []
                for rec in records:
                    kv = f"\n  Valores: {rec['key_values']}" if rec.get("key_values") else ""
                    lines.append(f"- {rec['date']} | {rec['type']} | {rec['name']}\n  {rec['summary'][:300]}{kv}")
                tr = "\n".join(lines)

        elif block.name == "consultar_medicaciones":
            meds = await query_medications(only_active=inp.get("solo_activas", False))
            if not meds:
                tr = "No hay medicaciones registradas."
            else:
                lines = []
                for m in meds:
                    estado = "activa" if m["active"] else "inactiva"
                    cond = f", para {m['condition']}" if m.get("condition") else ""
                    lines.append(f"- {m['name']} {m.get('dose','')} — {m.get('frequency','')} ({estado}{cond})")
                tr = "\n".join(lines)

        elif block.name == "actualizar_medicacion":
            mid = inp.pop("medication_id")
            ok = await update_medication(mid, inp)
            tr = "Actualizada." if ok else "Error actualizando."

        elif block.name == "editar_registro_salud":
            rid = inp.pop("record_id")
            ok = await _ds.update_health_record(rid, inp)
            tr = "Registro actualizado." if ok else "Error actualizando el registro."

        elif block.name == "eliminar_registro_salud":
            rid = inp.get("record_id")
            expires_at = (now_argentina() + timedelta(minutes=5)).replace(tzinfo=None).isoformat()
            pending_state[phone] = {
                "type": "confirm_delete", "action": "health_record",
                "page_id": rid, "name": "este registro médico", "expires_at": expires_at,
            }
            await send_interactive_buttons(phone, "¿Eliminás este registro médico?", [
                {"id": "confirm_delete_yes", "title": "Sí, eliminarlo"},
                {"id": "confirm_delete_no", "title": "No, cancelar"},
            ])
            tr = "Pedí confirmación al usuario para eliminar el registro."

        tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": tr})

    if not tool_results:
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()
        add_to_history(phone, "user", text)
        add_to_history(phone, "assistant", reply)
        return reply

    if pending_state.get(phone, {}).get("type") == "confirm_delete":
        add_to_history(phone, "user", text)
        return None

    final = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=600,
        system=system,
        messages=get_history(phone) + [
            {"role": "user", "content": content},
            {"role": "assistant", "content": response.content},
            {"role": "user", "content": tool_results},
        ],
        tools=tools
    )
    reply = next((b.text for b in final.content if hasattr(b, "text") and b.text), "").strip()
    add_to_history(phone, "user", text)
    add_to_history(phone, "assistant", reply)
    return reply

# ── PENDING STATE HANDLER ──────────────────────────────────────────────────────
async def handle_pending_state(phone: str, text: str, state: dict) -> bool:
    state_type = state.get("type")

    if state_type == "confirm_delete":
        expires_at = state.get("expires_at")
        if expires_at:
            try:
                exp = datetime.fromisoformat(expires_at)
                if now_argentina().replace(tzinfo=None) > exp.replace(tzinfo=None):
                    del pending_state[phone]
                    await send_message(phone, f"Tiempo agotado, no se eliminó *{state.get('name', 'el elemento')}*.")
                    return True
            except Exception:
                pass

        if text == "confirm_delete_yes":
            action = state["action"]
            page_id = state["page_id"]
            name = state.get("name", "")
            del pending_state[phone]
            if action == "expense":
                ok = await _ds.archive_expense(page_id)
                msg = f"*{name}* eliminado." if ok else "No pude eliminar."
            elif action == "plant":
                ok = await _ds.archive_plant(page_id)
                msg = f"*{name}* eliminada." if ok else "No pude eliminar."
            elif action == "meeting":
                ok = await _ds.archive_meeting(page_id)
                msg = f"*{name}* eliminada." if ok else "No pude eliminar."
            elif action == "event":
                access_token = await get_gcal_access_token()
                ok = False
                if access_token:
                    async with httpx.AsyncClient() as http:
                        r = await http.delete(
                            f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{page_id}",
                            headers={"Authorization": f"Bearer {access_token}"}
                        )
                        ok = r.status_code == 204
                if ok:
                    msg = f"*{name}* eliminado del calendario."
                    # Después de eliminar, mostrar como queda el día
                    try:
                        now_ar = now_argentina()
                        async with httpx.AsyncClient(timeout=8) as _h:
                            r2 = await _h.get(
                                "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                                headers={"Authorization": f"Bearer {access_token}"},
                                params={
                                    "timeMin": now_ar.replace(hour=0, minute=0, second=0).strftime("%Y-%m-%dT00:00:00-03:00"),
                                    "timeMax": now_ar.replace(hour=23, minute=59, second=59).strftime("%Y-%m-%dT23:59:59-03:00"),
                                    "singleEvents": "true", "orderBy": "startTime", "maxResults": "10"
                                }
                            )
                        if r2.status_code == 200:
                            evs = [e for e in r2.json().get("items", []) if "[TEMP]" not in (e.get("description") or "")]
                            if evs:
                                msg += "\n\nTu día queda así:"
                                for e in evs:
                                    s = e.get("start", {})
                                    loc = f" -- 📍{e.get('location','')}" if e.get("location") else ""
                                    if "dateTime" in s:
                                        msg += f"\n- {s['dateTime'][11:16]} -- {e.get('summary','Evento')}{loc}"
                                    else:
                                        msg += f"\n- {e.get('summary','Evento')} (todo el día){loc}"
                            else:
                                msg += "\n\nNo te queda nada agendado para hoy."
                    except Exception:
                        pass
                else:
                    msg = error_servicio("calendar")
            elif action == "health_record":
                ok = await _ds.archive_health_record(page_id)
                msg = "Registro médico eliminado." if ok else "No pude eliminar."
            elif action == "fitness_entry":
                ok = await _ds.archive_fitness(page_id)
                msg = "Actividad eliminada." if ok else "No pude eliminar."
            else:
                msg = "Acción no reconocida."
            await send_message(phone, msg)
            return True
        elif text == "confirm_delete_no":
            del pending_state[phone]
            await send_message(phone, "Cancelado, no se eliminó nada.")
            return True
        else:
            del pending_state[phone]
            return False

    if state_type == "undo_window":
        expires_at = state.get("expires_at")
        if expires_at:
            try:
                exp = datetime.fromisoformat(expires_at)
                if now_argentina().replace(tzinfo=None) > exp.replace(tzinfo=None):
                    del pending_state[phone]
                    return False
            except Exception:
                pass

        t_lower = text.lower()
        undo_signals = ["no era", "borralo", "está mal", "esta mal", "error", "cancelá",
                        "cancela", "no quería", "no queria", "equivoque", "me equivoque",
                        "no era eso", "borrá", "deshacer", "undo", "no corresponde",
                        "esta mal", "estaba mal", "no es correcto", "incorrecto"]
        is_undo = any(s in t_lower for s in undo_signals)

        if not is_undo:
            del pending_state[phone]
            return False

        action = state["action"]
        page_id = state["page_id"]
        name = state.get("name", "el último ítem")
        del pending_state[phone]

        ok = False
        if action == "expense":
            ok = await _ds.archive_expense(page_id)
        elif action == "plant":
            ok = await _ds.archive_plant(page_id)
        elif action == "meeting":
            ok = await _ds.archive_meeting(page_id)
        elif action == "finance_invoice":
            ok = await _ds.archive_expense(page_id)

        if ok:
            await send_message(phone, f"Deshecho. *{name}* eliminado.")
        else:
            await send_message(phone, "No pude deshacer la acción.")
        return True

    if state_type == "geocode_confirm":
        event_id = state["event_id"]
        if text.strip() == "geocode_yes":
            del pending_state[phone]
            try:
                _at = await get_gcal_access_token()
                async with httpx.AsyncClient() as _http:
                    await _http.patch(
                        f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                        headers={"Authorization": f"Bearer {_at}", "Content-Type": "application/json"},
                        params={"sendUpdates": "none"},
                        json={"extendedProperties": {"private": {
                            "knot_lat": str(state["lat"]),
                            "knot_lon": str(state["lon"]),
                        }}}
                    )
                await send_message(phone, "📍 Ubicacion guardada. La proxima vez que te avise del evento, te cuento si hay algo de camino que puedas resolver.")
            except Exception:
                await send_message(phone, error_servicio("calendar"))
            return True
        elif text.strip() == "geocode_no":
            del pending_state[phone]
            return True
        else:
            # Intento de correccion — regeocoding con el texto dado
            del pending_state[phone]
            try:
                async with httpx.AsyncClient(timeout=5) as _hg:
                    _gr = await _hg.get(
                        "https://nominatim.openstreetmap.org/search",
                        params={"q": text.strip(), "format": "json", "limit": 1},
                        headers={"User-Agent": "Knot/1.0"}
                    )
                    if _gr.status_code == 200 and _gr.json():
                        _gresult = _gr.json()[0]
                        _lat = float(_gresult["lat"])
                        _lon = float(_gresult["lon"])
                        _name = _gresult.get("display_name", text)[:60].split(",")[0].strip()
                        _at = await get_gcal_access_token()
                        async with httpx.AsyncClient() as _http:
                            await _http.patch(
                                f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                                headers={"Authorization": f"Bearer {_at}", "Content-Type": "application/json"},
                                params={"sendUpdates": "none"},
                                json={"extendedProperties": {"private": {
                                    "knot_lat": str(_lat),
                                    "knot_lon": str(_lon),
                                }}}
                            )
                        await send_message(phone, f"📍 Ubicacion guardada: *{_name}*.")
                    else:
                        await send_message(phone, "No pude encontrar esa dirección. Ubicacion no guardada.")
            except Exception:
                pass
            return True

    if state_type == "recurrence_offer":
        event_id = state["event_id"]
        summary = state["summary"]
        rrule_day = state["rrule_day"]
        ev_tocados = state.get("eventos_tocados", [])
        _rrule_to_weekday = {"MO":0,"TU":1,"WE":2,"TH":3,"FR":4,"SA":5,"SU":6}
        _dia_nombre = DIAS_SEMANA[_rrule_to_weekday.get(rrule_day, 0)]

        if text.strip() == "recurrence_yes":
            del pending_state[phone]
            try:
                _at = await get_gcal_access_token()
                async with httpx.AsyncClient() as _http:
                    r = await _http.patch(
                        f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                        headers={"Authorization": f"Bearer {_at}", "Content-Type": "application/json"},
                        params={"sendUpdates": "none"},
                        json={"recurrence": [f"RRULE:FREQ=WEEKLY;BYDAY={rrule_day}"]}
                    )
                    if r.status_code == 200:
                        pending_state[phone] = {"type": "recurring_event_reminder", "event_id": event_id, "summary": summary}
                        await send_message(phone, f"✅ *{summary}* es recurrente cada {_dia_nombre}.\n¿Querés que te avise antes de cada sesión? Decime con cuánta anticipación (ej: '30 min', '1 hora'). O mandá 'no' para omitir.")
                    else:
                        await send_message(phone, error_servicio("calendar"))
            except Exception:
                await send_message(phone, error_servicio("calendar"))
            return True

        elif text.strip() == "recurrence_no":
            del pending_state[phone]
            if ev_tocados:
                lineas = []
                for ev in ev_tocados:
                    try:
                        fecha_fmt = datetime.strptime(ev["date"], "%Y-%m-%d")
                        dia = DIAS_SEMANA[fecha_fmt.weekday()]
                        fecha_label = f"{dia} {fecha_fmt.strftime('%d/%m')} {ev['time']}"
                    except Exception:
                        fecha_label = f"{ev.get('date','')} {ev.get('time','')}"
                    lineas.append(f"_{ev['summary']}_ — {fecha_label}")
                pending_state[phone] = {
                    "type": "event_reminder",
                    "events": [{"summary": ev["summary"], "event_datetime": f"{ev['date']}T{ev['time']}"} for ev in ev_tocados],
                }
                await send_interactive_buttons(
                    phone,
                    f"¿Querés que te avise antes?\n" + "\n".join(lineas),
                    [
                        {"id": "rem_15", "title": "15 min antes"},
                        {"id": "rem_60", "title": "1 hora antes"},
                        {"id": "rem_no", "title": "No gracias"},
                    ]
                )
            return True
        else:
            del pending_state[phone]
            return False

    if state_type == "hint_response":
        trigger_id = state.get("trigger_id", "")
        action_intent = state.get("action_intent", "")
        payload = state.get("payload", {})
        del pending_state[phone]

        t = text.strip().lower()
        affirmative = t in ("si", "sí", "dale", "ok", "yes", "s", "bueno", "obvio", "perfecto", "genial")
        negative    = t in ("no", "nel", "nope", "para nada", "ahora no")

        hints_state = user_prefs.get("feature_hints") or {}

        if affirmative:
            suggestion_gate.record_accepted(hints_state, trigger_id)
            user_prefs["feature_hints"] = hints_state
            await save_user_config(MY_NUMBER)
            # Ejecutar acción según intent
            if action_intent == "enable_weekly_finance":
                extras = user_prefs.get("resumen_extras", [])
                if "Mini-resumen finanzas semanal (lunes)" not in extras:
                    extras.append("Mini-resumen finanzas semanal (lunes)")
                    user_prefs["resumen_extras"] = extras
                    await save_user_config(MY_NUMBER)
                await send_message(phone, "Listo, los lunes te paso un mini-resumen.")
            elif action_intent == "info_only":
                await send_message(phone, "Anotado.")
            elif action_intent == "set_shopping_recurring":
                item_id = payload.get("item_id")
                name = payload.get("name", "ese item")
                if item_id:
                    try:
                        await _ds.update_shopping_item(item_id, {"frequency": "Monthly"})
                        await send_message(phone, f"🔁 *{name}* marcado como recurrente. Siempre va a estar en tu lista.")
                    except Exception:
                        await send_message(phone, "No pude marcarlo como recurrente.")
                else:
                    await send_message(phone, "No tengo el item para actualizar.")
            elif action_intent == "create_recurring_event":
                p_name = payload.get("name", "")
                p_wd = payload.get("weekday", 0)
                p_hr = payload.get("hour", 9)
                weekday_es = ["lunes","martes","miércoles","jueves","viernes","sábado","domingo"][p_wd]
                await send_message(phone, f"Dale, decime cómo lo agendo: *Crear evento {p_name} todos los {weekday_es} a las {p_hr:02d}:00* o ajustá lo que quieras.")
            elif action_intent == "enable_factura_early_warning":
                extras = user_prefs.get("resumen_extras", [])
                hint_msg = "Avisar 5 días antes de cada factura recurrente"
                if hint_msg not in extras:
                    extras.append(hint_msg)
                    user_prefs["resumen_extras"] = extras
                    await save_user_config(MY_NUMBER)
                await send_message(phone, "Listo, te aviso 5 días antes de cada factura recurrente que detecte.")
            elif action_intent == "enable_travel_time_alert":
                ev_id = payload.get("event_id", "")
                summary = payload.get("summary", "el evento")
                await send_message(phone, f"Listo, te voy a calcular el tiempo de viaje para *{summary}*. Si no logro calcularlo, te pregunto antes cuánto demorás.")
            elif action_intent == "enable_time_reminder":
                desc = payload.get("description", "el recordatorio")
                await send_message(phone, f"Decime el horario en formato HH:MM (ej: 20:00) y los días (todos los días, lunes, etc) para que te recuerde *{desc}*.")
            elif action_intent == "enable_plant_reminder":
                plant_name = payload.get("plant_name", "tu planta")
                extras = user_prefs.get("resumen_extras", [])
                hint_msg = f"Recordatorio de riego para {plant_name} (lunes)"
                if hint_msg not in extras:
                    extras.append(hint_msg)
                    user_prefs["resumen_extras"] = extras
                    await save_user_config(MY_NUMBER)
                await send_message(phone, f"🌿 Listo, los lunes te pregunto si regaste *{plant_name}*.")
            elif action_intent == "add_provider":
                provider = payload.get("provider", "")
                if provider:
                    providers = user_prefs.get("service_providers") or {}
                    providers[provider.lower()] = provider
                    user_prefs["service_providers"] = providers
                    await save_user_config(MY_NUMBER)
                await send_message(phone, f"Sumado *{provider}* a tus proveedores. La próxima factura que llegue ya la voy a reconocer.")
            else:
                await send_message(phone, "Listo, lo activo. Si querés cambiarlo despues, decímelo.")
            return True

        if negative:
            suggestion_gate.record_dismissed(hints_state, trigger_id)
            user_prefs["feature_hints"] = hints_state
            await save_user_config(MY_NUMBER)
            await send_message(phone, "Dale, no te molesto con eso.")
            return True

        # Texto que no es ni si ni no → pasar el mensaje normal y olvidar la oferta
        return False

    if state_type == "ask_payment_method":
        page_id = state.get("page_id")
        name = state.get("name", "gasto")
        del pending_state[phone]
        t = text.strip().lower()
        if t in ("no", "no se", "no sé", "ns", "skip", "omitir"):
            await send_message(phone, "Dale, sin método de pago.")
            return True
        # Buscar match en payment_methods_cache
        matched = None
        for pm in payment_methods_cache:
            if pm.last4 and pm.last4 in t:
                matched = pm
                break
            if pm.name and pm.name.lower() in t:
                matched = pm
                break
            if pm.bank and pm.bank.lower() in t:
                matched = pm
                break
            if pm.modality and pm.modality.lower() in t:
                matched = pm
                break
        if matched:
            try:
                await _ds.update_expense(page_id, {"payment_method_id": matched.id})
                asyncio.create_task(_ds.increment_payment_method_uses(matched.id, matched.uses))
                matched.uses += 1
                label = matched.name or f"{matched.bank} {matched.modality}".strip()
                await send_message(phone, f"💳 *{name}* pagado con *{label}*.")
            except Exception:
                await send_message(phone, "No pude guardar el método de pago.")
        else:
            await send_message(phone, f"No reconocí ese método. Probá con: {', '.join((p.name or p.bank or '') for p in payment_methods_cache[:8])}")

        # Si el mensaje contiene MAS que solo el método (ej: corrección de emoji o categoría),
        # procesar el resto via corregir_gasto
        if len(text.strip()) > 30 or any(w in t for w in ("emoji", "emogi", "categoría", "categoria", "nombre", "nota", "notas", "ponle", "cambia", "corrige", "corregí", "no era", "está mal", "esta mal")):
            try:
                last_touched[phone] = {"page_id": page_id, "name": name}
                _, corr_msg = await corregir_gasto(text, phone=phone)
                if corr_msg:
                    await send_message(phone, corr_msg)
            except Exception:
                pass
        return True

    if state_type == "confirm_known_shop":
        shop_name  = state.get("shop_name", "")
        shop_key   = state.get("shop_key", shop_name.lower().strip())
        inferred   = state.get("inferred_type", "")
        inferred_cat = state.get("inferred_category")
        current_cats = state.get("current_category") or []
        page_id    = state.get("page_id")
        del pending_state[phone]

        t_lower = text.strip().lower()
        affirmative = t_lower in ("si", "sí", "dale", "ok", "yes", "s", "correcto", "exacto")
        negative    = t_lower in ("no", "nel", "nope", "para nada")

        if affirmative:
            shops = user_prefs.get("known_shops") or {}
            shops[shop_key] = inferred
            user_prefs["known_shops"] = shops
            await save_user_config(MY_NUMBER)
            if inferred_cat and inferred_cat not in current_cats and page_id:
                new_cats = [inferred_cat] + [c for c in current_cats if c != inferred_cat]
                try:
                    await _ds.update_expense(page_id, {"categories": new_cats})
                    await send_message(phone, f"Guardado. Actualicé la categoría a *{inferred_cat}*.")
                except Exception:
                    await send_message(phone, "Guardado.")
            else:
                await send_message(phone, "Guardado.")
            return True

        if negative:
            pending_state[phone] = {
                "type": "confirm_known_shop_manual",
                "shop_name": shop_name,
                "shop_key": shop_key,
                "page_id": page_id,
                "current_category": current_cats,
            }
            await send_message(phone, f"¿Qué tipo de negocio es _{shop_name}_?")
            return True

        # Texto libre: interpretar como el tipo correcto
        btype = text.strip().capitalize()
        shops = user_prefs.get("known_shops") or {}
        shops[shop_key] = btype
        user_prefs["known_shops"] = shops
        await save_user_config(MY_NUMBER)
        new_cat = _business_type_to_category(btype)
        if new_cat and new_cat not in current_cats and page_id:
            new_cats = [new_cat] + [c for c in current_cats if c != new_cat]
            try:
                await _ds.update_expense(page_id, {"categories": new_cats})
                await send_message(phone, f"Guardado como *{btype}*. Categoría → *{new_cat}*.")
            except Exception:
                await send_message(phone, f"Guardado como *{btype}*.")
        else:
            await send_message(phone, f"Guardado como *{btype}*.")
        return True

    if state_type == "confirm_known_shop_manual":
        shop_name  = state.get("shop_name", "")
        shop_key   = state.get("shop_key", shop_name.lower().strip())
        page_id    = state.get("page_id")
        current_cats = state.get("current_category") or []
        del pending_state[phone]
        btype = text.strip().capitalize()
        shops = user_prefs.get("known_shops") or {}
        shops[shop_key] = btype
        user_prefs["known_shops"] = shops
        await save_user_config(MY_NUMBER)
        new_cat = _business_type_to_category(btype)
        if new_cat and new_cat not in current_cats and page_id:
            new_cats = [new_cat] + [c for c in current_cats if c != new_cat]
            try:
                await _ds.update_expense(page_id, {"categories": new_cats})
                await send_message(phone, f"Guardado como *{btype}*. Categoría → *{new_cat}*.")
            except Exception:
                await send_message(phone, f"Guardado como *{btype}*.")
        else:
            await send_message(phone, f"Guardado como *{btype}*.")
        return True

    if state_type == "litros_followup":
        page_id = state["page_id"]
        name    = state["name"]
        try:
            clean = text.strip().replace(",", ".").split()[0]
            litros = float(clean)
            if litros <= 0 or litros > 9999:
                raise ValueError("Numero fuera de rango")
        except (ValueError, IndexError):
            del pending_state[phone]
            return False

        try:
            await _ds.update_expense(page_id, {"liters": litros})
            del pending_state[phone]
            await send_message(phone, f"*{name}* -- {litros}L registrados")
        except Exception as e:
            del pending_state[phone]
            await send_message(phone, f"No pude actualizar los litros: {str(e)[:80]}")
        return True

    if state_type == "snooze":
        summary = state.get("summary", "Recordatorio")
        del pending_state[phone]

        if text.strip() == "snooze_no":
            await send_message(phone, "Recordatorio descartado")
            return True

        snooze_map = {"snooze_5": 5, "snooze_15": 15, "snooze_30": 30}
        minutes = snooze_map.get(text.strip())
        if minutes:
            fire_at = now_argentina() + timedelta(minutes=minutes)
            event_data = {
                "summary": summary,
                "fire_at": fire_at.strftime("%Y-%m-%dT%H:%M")
            }
            success, _ = await create_recordatorio(event_data)
            if success:
                await send_message(phone, f"Te recuerdo en {minutes} minutos")
            else:
                await send_message(phone, "No pude posponer el recordatorio")
        return True

    if state_type == "event_reminder":
        reminder_map = {
            "rem_15": 15, "rem_30": 30, "rem_60": 60,
            "rem_1d": 1440, "rem_no": None
        }
        t = text.strip().lower()
        minutes = reminder_map.get(text.strip(), "unknown")
        if minutes == "unknown":
            # Parseo de lenguaje natural — solo mensajes cortos y claros
            if t in ["no", "nah", "paso", "no gracias", "sin aviso", "sin recordatorio", "no por favor"]:
                minutes = None
            elif t.startswith("no") and len(t.split()) <= 2:
                minutes = None  # "no gracias", "no igual" pero NO "no tengo el domingo"
            elif "1 hora" in t or "una hora" in t or "60 min" in t:
                minutes = 60
            elif "30 min" in t or "media hora" in t:
                minutes = 30
            elif "15 min" in t or "quince" in t:
                minutes = 15
            elif "dia" in t or "día" in t or "noche" in t or "1440" in t:
                minutes = 1440
            else:
                return False
        del pending_state[phone]
        if minutes is None:
            await send_message(phone, "Sin recordatorio")
            return True
        # Soporte para lista de eventos o evento unico (backward compat)
        events = state.get("events") or [{"summary": state.get("summary", "Evento"), "event_datetime": state.get("event_datetime")}]
        label = "1 dia" if minutes == 1440 else f"{minutes} min"
        resultados = []
        now_naive = now_argentina().replace(tzinfo=None)
        for ev in events:
            ev_summary = ev.get("summary", "Evento")
            ev_dt_str = ev.get("event_datetime")
            if not ev_dt_str:
                continue
            try:
                fire_dt = datetime.strptime(ev_dt_str, "%Y-%m-%dT%H:%M") - timedelta(minutes=minutes)
                if fire_dt > now_naive:
                    success, _ = await create_recordatorio({"summary": f"🔔 {ev_summary}", "fire_at": fire_dt.strftime("%Y-%m-%dT%H:%M")})
                    resultados.append(f"_{ev_summary}_" if success else f"Error en _{ev_summary}_")
                else:
                    resultados.append(f"_{ev_summary}_ ya paso")
            except Exception:
                resultados.append(f"Error en _{ev_summary}_")
        if resultados:
            await send_message(phone, f"Te aviso {label} antes de: " + ", ".join(resultados))
        return True

    if state_type == "recurring_event_reminder":
        event_id = state.get("event_id")
        summary = state.get("summary", "Evento")
        del pending_state[phone]
        if text.strip().lower() in ["no", "no gracias", "nah", "paso"]:
            await send_message(phone, "Dale, sin recordatorio.")
            return True
        try:
            resp = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=100,
                system="Extrae los recordatorios que pide el usuario. Convierte a minutos. Max 2. Responde SOLO JSON sin markdown: {\"minutes\": [30, 60]} o {\"minutes\": [15]}. Si dice 'la noche anterior' usa 720 (12hs). Si dice '1 dia antes' usa 1440. Si dice '2 horas' usa 120.",
                messages=[{"role": "user", "content": text}]
            )
            raw = resp.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.strip("`").lstrip("json").strip()
            parsed = json.loads(raw)
            minutes_list = parsed.get("minutes", [])[:2]
        except Exception:
            minutes_list = [60]
        if not minutes_list:
            await send_message(phone, "No entendi la anticipacion. Te pongo 1 hora antes por defecto.")
            minutes_list = [60]
        access_token = await get_gcal_access_token()
        if access_token and event_id:
            try:
                async with httpx.AsyncClient() as http:
                    r = await http.patch(
                        f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                        json={"reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": m} for m in minutes_list]}}
                    )
                    if r.status_code == 200:
                        labels = []
                        for m in minutes_list:
                            if m >= 1440:
                                labels.append(str(m // 1440) + " dia" + ("s" if m >= 2880 else "") + " antes")
                            elif m >= 60:
                                labels.append(str(m // 60) + " hora" + ("s" if m >= 120 else "") + " antes")
                            else:
                                labels.append(str(m) + " min antes")
                        rems_str = " y ".join(labels)
                        await send_message(phone, "Listo! Te aviso " + rems_str + " de cada " + summary + ".")
                    else:
                        await send_message(phone, "No pude configurar el recordatorio en Calendar.")
            except Exception:
                await send_message(phone, "Error configurando el recordatorio.")
        else:
            await send_message(phone, "No pude acceder a Calendar para el recordatorio.")
        return True
    if state_type == "recipe_ingredients":
        recipe_name = state.get("recipe_name", "Receta")
        ingredients = state.get("ingredients", [])
        del pending_state[phone]
        if text.strip() == "recipe_add_yes":
            results_text = []
            for item in ingredients:
                item_name = item.get("name", "")
                existing = await _ds.search_shopping_item(item_name)
                if existing:
                    await _ds.update_shopping_item(existing[0].id, {"in_stock": False})
                    results_text.append(f"_{item_name}_ ya estaba, aparece como faltante")
                else:
                    try:
                        await _ds.add_shopping_item(item)
                        results_text.append(f"_{item_name}_ agregado")
                    except Exception as e:
                        results_text.append(f"Error: {str(e)[:50]}")
            await send_message(phone, "\n".join(results_text) + "\n\nLista actualizada en Notion")
        else:
            await send_message(phone, f"_{recipe_name.capitalize()}_ guardada. Ingredientes no agregados a la lista de compras.")
        return True

    if state_type == "recipe_review":
        recipe_name = state.get("recipe_name", "Receta")
        ingredients = state.get("ingredients", [])
        recipe_text = state.get("recipe_text", "")

        if text.strip() == "recipe_ok":
            del pending_state[phone]
            pending_state[phone] = {
                "type": "recipe_save_confirm",
                "recipe_name": recipe_name,
                "recipe_text": recipe_text,
                "ingredients": ingredients,
            }
            await send_interactive_buttons(
                phone,
                f"Guardamos *{recipe_name.capitalize()}* en tus Recetas de Notion?",
                [
                    {"id": "recipe_save_yes", "title": "Si, guardar"},
                    {"id": "recipe_save_no",  "title": "No gracias"},
                ]
            )
        elif text.strip() == "recipe_correct":
            del pending_state[phone]
            pending_state[phone] = {
                "type": "recipe_correction_pending",
                "recipe_name": recipe_name,
                "recipe_text": recipe_text,
                "ingredients": ingredients,
            }
            await send_message(phone, "Decime que esta mal -- que falta, que sobra o que cambiar.")
        return True

    if state_type == "recipe_correction_pending":
        recipe_name = state.get("recipe_name", "Receta")
        recipe_text = state.get("recipe_text", "")
        ingredients = state.get("ingredients", [])
        del pending_state[phone]
        try:
            ing_names = [i.get("name", "") for i in ingredients]
            corr_resp = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=600,
                system="Responde SOLO JSON valido sin markdown.",
                messages=[{"role": "user", "content": f"""Receta: "{recipe_name}"
Lista actual de ingredientes: {json.dumps(ing_names, ensure_ascii=False)}
Correccion del usuario: {text}
Aplica la correccion y devolve la lista corregida como array JSON simple:
["ingrediente1", "ingrediente2", ...]"""}]
            )
            raw_corr = corr_resp.content[0].text.strip()
            if raw_corr.startswith("```"):
                raw_corr = raw_corr.strip("`").lstrip("json").strip()
            corrected_names = json.loads(raw_corr)
            enriched_corrected = await enrich_items_with_claude(corrected_names)
        except Exception:
            enriched_corrected = ingredients
        ing_list = "\n".join(f"- {i.get('emoji','🛒')} {i.get('name','')}" for i in enriched_corrected)
        pending_state[phone] = {
            "type": "recipe_review",
            "recipe_name": recipe_name,
            "recipe_text": recipe_text,
            "ingredients": enriched_corrected,
        }
        await send_message(
            phone,
            f"*{recipe_name.capitalize()}* -- version corregida:\n\n*Ingredientes:*\n{ing_list}"
        )
        await send_interactive_buttons(
            phone,
            "Esta todo bien o seguis corrigiendo?",
            [
                {"id": "recipe_ok",      "title": "Esta bien"},
                {"id": "recipe_correct", "title": "Seguir corrigiendo"},
            ]
        )
        return True

    if state_type == "recipe_save_confirm":
        recipe_name = state.get("recipe_name", "Receta")
        recipe_text = state.get("recipe_text", "")
        ingredients = state.get("ingredients", [])
        del pending_state[phone]
        if text.strip() == "recipe_save_yes":
            await send_message(phone, "Guardando receta en Notion...")
            ok, err = await save_recipe_to_notion(recipe_name, source="Knot", ingredient_names=ingredients, recipe_text=recipe_text)
            if not ok:
                await send_message(phone, f"Error guardando la receta: {err}")
                return True
            ing_list = "\n".join(f"- {i.get('emoji','🛒')} {i.get('name','')}" for i in ingredients)
            pending_state[phone] = {
                "type": "recipe_ingredients",
                "recipe_name": recipe_name,
                "ingredients": ingredients,
            }
            await send_message(
                phone,
                f"*{recipe_name.capitalize()}* guardada en Recipes\n\n*Ingredientes:*\n{ing_list}"
            )
            await send_interactive_buttons(
                phone,
                "Los agregas a la lista de compras?",
                [
                    {"id": "recipe_add_yes", "title": "Si, agregar"},
                    {"id": "recipe_add_no",  "title": "No por ahora"},
                ]
            )
        else:
            await send_message(phone, "Receta no guardada.")
        return True

    if state_type == "chat_correction":
        page_id   = state.get("page_id")
        old_value = state.get("old_value")
        new_value = state.get("new_value")
        name      = state.get("name", "gasto")
        del pending_state[phone]
        if text.strip().lower() in ["si", "dale", "ok", "yes", "corregilo", "corrigelo"]:
            if page_id and new_value:
                try:
                    await _ds.update_expense(page_id, {"value_ars": float(new_value)})
                    await send_message(phone, f"*{name}* corregido: ${old_value:,.0f} -> *${new_value:,.0f} ARS*")
                except Exception as e:
                    await send_message(phone, f"No pude corregir: {str(e)[:100]}")
            else:
                await send_message(phone, "No tengo suficiente info para hacer la correccion.")
        else:
            await send_message(phone, "Quedo como estaba.")
        return True

    if state_type == "geo_reminder_radius_confirm":
        page_id = state.get("page_id")
        name = state.get("name", "Recordatorio")
        del pending_state[phone]
        radius_map = {"radius_50": 50, "radius_100": 100, "radius_300": 300}
        new_radius = radius_map.get(text.strip())
        if new_radius and page_id:
            await _ds.update_geo_reminder(page_id, {"radius": new_radius})
            for r in geo_reminders_cache:
                if r.get("page_id") == page_id:
                    r["radius"] = new_radius
            await send_message(phone, f"✅ Radio actualizado a *{new_radius}m*.")
        return True

    if state_type == "lista_create_confirm":
        from notion_datastore import COLLECTION_TEMPLATES
        list_name = state.get("list_name", "")
        template_key = state.get("template_key")
        pending_action = state.get("pending_action") or {}
        del pending_state[phone]
        affirm = text.strip().lower() in ("si", "sí", "yes", "y", "dale", "ok", "crear", "creala", "crea")
        if not affirm:
            await send_message(phone, "Ok, no la creé.")
            return True
        db_id = await _ds.create_generative_list_db(list_name, template_key)
        if not db_id:
            await send_message(phone, "No pude crear la colección en Notion (no encontré una página padre accesible).")
            return True
        lists = user_prefs.setdefault("generative_lists", {})
        lists[list_name] = {"db_id": db_id, "template": template_key}
        await save_user_config(MY_NUMBER)
        tmpl = COLLECTION_TEMPLATES.get(template_key) if template_key else None
        icon = tmpl["icon"] if tmpl else "📋"
        display = tmpl["display"] if tmpl else list_name
        await send_message(phone, f"{icon} Creé tu colección de _{display}_ en Notion.")
        result = await _execute_lista_add(
            list_name, db_id,
            pending_action.get("count"),
            pending_action.get("criteria"),
            pending_action.get("items") or [],
            template_key,
        )
        if result:
            await send_message(phone, result)
        return True

    if state_type == "geo_reminder_fired":
        page_id = state.get("page_id")
        name = state.get("name", "Recordatorio")
        del pending_state[phone]
        if text.strip() == "geo_done":
            await deactivate_geo_reminder(page_id)
            await send_message(phone, f"✅ _{name}_ desactivado.")
        else:
            # Reset cooldown so the next location update re-fires the reminder
            _geo_reminder_cooldowns.pop(page_id, None)
            _geo_reminders_in_range.discard(page_id)
            await send_message(phone, "Ok, te sigo avisando cuando estes cerca.")
        return True

    if state_type == "geo_reminder_awaiting_location":
        description = state.get("description", "Recordatorio")
        recurrent = state.get("recurrent", False)
        kp_name = state.get("place_name")
        del pending_state[phone]
        # Intentar extraer link de Maps o coordenadas del texto
        import re
        _maps_match = re.search(r'https?://(?:maps\.app\.goo\.gl|goo\.gl/maps|maps\.google\.com)\S*', text)
        if _maps_match:
            _maps_coords = await extract_coords_from_maps_url(_maps_match.group(0))
            if _maps_coords:
                _lat, _lon = _maps_coords
                ok, _ = await create_geo_reminder(
                    description=description, rtype="place",
                    lat=_lat, lon=_lon, recurrent=recurrent,
                )
                if ok:
                    await _save_known_place(kp_name, _lat, _lon)
                    freq = "Cada vez que" if recurrent else "La proxima vez que"
                    saved_kp = f"\n💾 *{kp_name.strip().capitalize()}* guardado como lugar conocido." if kp_name else ""
                    await send_message(phone, f"Geo-reminder guardado\n_{description}_\nCoordenadas del link: {_lat:.5f}, {_lon:.5f}\n{freq} estes cerca, te aviso.{saved_kp}")
                else:
                    await send_message(phone, "No pude guardar el geo-reminder.")
                return True
        _coord = re.search(r'(-\d{2,3}\.\d{4,})[,\s]+(-\d{2,3}\.\d{4,})', text)
        if _coord:
            _lat = float(_coord.group(1))
            _lon = float(_coord.group(2))
            ok, _ = await create_geo_reminder(
                description=description, rtype="place",
                lat=_lat, lon=_lon, recurrent=recurrent,
            )
            if ok:
                await _save_known_place(kp_name, _lat, _lon)
                freq = "Cada vez que" if recurrent else "La proxima vez que"
                saved_kp = f"\n💾 *{kp_name.strip().capitalize()}* guardado como lugar conocido." if kp_name else ""
                await send_message(phone, f"Geo-reminder guardado\n_{description}_\nCoordenadas: {_lat:.5f}, {_lon:.5f}\n{freq} estes cerca, te aviso.{saved_kp}")
            else:
                await send_message(phone, "No pude guardar el geo-reminder.")
            return True
        # Intentar geocodificar lo que escribio
        try:
            async with httpx.AsyncClient(timeout=5) as http:
                r = await http.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": text, "format": "json", "limit": 1, "addressdetails": 1},
                    headers={"User-Agent": "Knot/1.0"}
                )
                if r.status_code == 200 and r.json():
                    result = r.json()[0]
                    place_lat = float(result["lat"])
                    place_lon = float(result["lon"])
                    place_name = _format_place_name(result, text)
                    ok, _ = await create_geo_reminder(
                        description=description,
                        rtype="place",
                        lat=place_lat,
                        lon=place_lon,
                        recurrent=recurrent,
                    )
                    if ok:
                        await _save_known_place(kp_name, place_lat, place_lon)
                        freq = "Cada vez que" if recurrent else "La proxima vez que"
                        saved_kp = f"\n💾 *{kp_name.strip().capitalize()}* guardado como lugar conocido." if kp_name else ""
                        await send_message(phone, f"📍 *Geo-reminder guardado*\n_{description}_\nUbicacion: *{place_name}*\n{freq} estes cerca, te aviso.{saved_kp}")
                        return True
        except Exception:
            pass
        await send_message(phone, "No pude encontrar esa direccion. Intentá compartir la ubicacion directamente desde WhatsApp (📎 → Ubicacion).")
        return True

    if state_type == "confirm_high_impact":
        action = state.get("action")
        t = text.strip().lower()
        confirmed = text.strip() == "high_impact_yes" or t in ["si", "dale", "ok", "yes", "confirmo", "hacelo"]
        del pending_state[phone]
        if not confirmed:
            await send_message(phone, "Cancelado, no se hizo nada.")
            return True
        # Ejecutar la accion confirmada
        if action == "edit_recurring":
            event_id = state["event_id"]
            patch_body = state["patch_body"]
            event_name = state.get("event_name", "Evento")
            access_token = await get_gcal_access_token()
            async with httpx.AsyncClient() as http:
                r = await http.patch(
                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                    headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                    params={"sendUpdates": "none"},
                    json=patch_body
                )
            if r.status_code == 200:
                await send_message(phone, f"Listo, actualice todos los _{event_name}_.")
            else:
                await send_message(phone, f"Error actualizando: {r.text[:100]}")
        elif action == "delete_recurring":
            access_token = await get_gcal_access_token()
            deleted = []
            async with httpx.AsyncClient() as http:
                for ev in state.get("events", []):
                    r = await http.delete(
                        f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{ev['id']}",
                        headers={"Authorization": f"Bearer {access_token}"}
                    )
                    if r.status_code == 204:
                        deleted.append(ev.get("summary", "Evento"))
            if deleted:
                await send_message(phone, "Eliminados: " + ", ".join(set(deleted)) + ".")
            else:
                await send_message(phone, "No pude eliminar los eventos.")
        return True

    if state_type == "confirm_factura_paid":
        task_page_id = state.get("task_page_id")
        task_name = state.get("task_name", "la factura")
        del pending_state[phone]
        if text.strip().lower() in ["si", "dale", "ok", "yes", "correcto", "s"]:
            ok = await mark_factura_task_paid(task_page_id)
            if ok:
                await send_message(phone, f"✅ *{task_name}* marcada como pagada en Tasks")
            else:
                await send_message(phone, "No pude actualizar la task en Notion")
        else:
            await send_message(phone, "Ok, la task queda pendiente")
        return True

    if state_type == "factura_mismatch_confirm":
        provider = state.get("provider", "la factura")
        page_id = state.get("page_id")
        invoice_amount = state.get("invoice_amount")
        paid_amount = state.get("paid_amount")
        remaining = state.get("remaining", [])
        del pending_state[phone]
        if text.strip() == "mismatch_yes":
            # Pedir nota explicativa y marcar como pagada
            pending_state[phone] = {
                "type": "factura_mismatch_note",
                "provider": provider, "page_id": page_id,
                "paid_amount": paid_amount, "remaining": remaining,
            }
            await send_message(phone, f"✅ Entendido. ¿Cuál fue la razón de la diferencia? (o «no» para omitir)")
        else:
            await send_message(phone, f"Ok, *{provider}* queda como factura pendiente.")
            # Seguir con el próximo mismatch si hay
            if remaining:
                nxt = remaining[0]
                pending_state[phone] = {
                    "type": "factura_mismatch_confirm",
                    "provider": nxt["provider"], "invoice_amount": nxt["invoice_amount"],
                    "paid_amount": nxt["paid_amount"], "page_id": nxt["page_id"],
                    "remaining": remaining[1:],
                }
                diff = abs(nxt["invoice_amount"] - nxt["paid_amount"])
                await send_interactive_buttons(
                    phone,
                    f"Factura *{nxt['provider']}* por ${nxt['invoice_amount']:,.0f} pero tu último pago fue ${nxt['paid_amount']:,.0f} (diff ${diff:,.0f}). ¿Ya está pagada?",
                    [{"id": "mismatch_yes", "title": "Sí, ya la pagué"}, {"id": "mismatch_no", "title": "No, está pendiente"}]
                )
        return True

    if state_type == "factura_mismatch_note":
        provider = state.get("provider", "la factura")
        page_id = state.get("page_id")
        paid_amount = state.get("paid_amount")
        remaining = state.get("remaining", [])
        del pending_state[phone]
        note = text.strip() if text.strip().lower() not in ["no", "nope", "n", ""] else None
        await _ds.mark_finance_paid(page_id, paid_amount, None, note)
        tasks = await get_pending_factura_tasks()
        for t in tasks:
            if t.get("finance_page_id") == page_id:
                await mark_factura_task_paid(t["page_id"])
                break
        msg = f"✅ *{provider}* marcada como pagada"
        if note:
            msg += f" — nota: _{note}_"
        await send_message(phone, msg)
        # Seguir con el próximo mismatch si hay
        if remaining:
            nxt = remaining[0]
            pending_state[phone] = {
                "type": "factura_mismatch_confirm",
                "provider": nxt["provider"], "invoice_amount": nxt["invoice_amount"],
                "paid_amount": nxt["paid_amount"], "page_id": nxt["page_id"],
                "remaining": remaining[1:],
            }
            diff = abs(nxt["invoice_amount"] - nxt["paid_amount"])
            await send_interactive_buttons(
                phone,
                f"Factura *{nxt['provider']}* por ${nxt['invoice_amount']:,.0f} pero tu último pago fue ${nxt['paid_amount']:,.0f} (diff ${diff:,.0f}). ¿Ya está pagada?",
                [{"id": "mismatch_yes", "title": "Sí, ya la pagué"}, {"id": "mismatch_no", "title": "No, está pendiente"}]
            )
        return True

    if state_type == "factura_confirm":
        situation   = state.get("situation")
        conf_id     = state.get("conf_id")
        paid_amount = state.get("paid_amount")
        payment_method = state.get("payment_method")
        del pending_state[phone]
        t_lower = text.strip().lower()
        affirm = t_lower in ("si", "sí", "yes", "dale", "ok", "sip", "claro", "obvio")
        deny   = t_lower in ("no", "nope", "nel", "nah")

        if situation == "diff_moderate":
            provider_name = state.get("provider_name", "la factura")
            finance_page_id = state.get("finance_page_id")
            if affirm:
                await _auto_mark_invoice_paid(
                    type("_", (), {"id": finance_page_id, "value_ars": paid_amount})(),
                    paid_amount, payment_method
                )
                await _remove_invoice_confirmation(conf_id)
                await send_message(phone, f"✅ *{provider_name}* marcada como pagada.")
            else:
                await _remove_invoice_confirmation(conf_id)
                await send_message(phone, f"Ok, la dejo pendiente.")
            return True

        if situation == "diff_large":
            provider_name = state.get("provider_name", "la factura")
            finance_page_id = state.get("finance_page_id")
            if affirm:
                # Pago parcial confirmado — no marca la factura como pagada
                await _remove_invoice_confirmation(conf_id)
                await send_message(phone, f"Anotado como pago parcial de *{provider_name}*. La factura sigue pendiente hasta que la saldés completa.")
            else:
                await _remove_invoice_confirmation(conf_id)
                await send_message(phone, f"Ok. La factura de *{provider_name}* sigue pendiente.")
            return True

        if situation == "multiple_invoices":
            candidates = state.get("candidates") or []
            # Intentar matchear la respuesta del usuario con algún candidato
            matched = None
            for c in candidates:
                if c["name"].lower() in t_lower or str(int(c["amount"] or 0)) in t_lower:
                    matched = c
                    break
            if not matched and affirm and candidates:
                # "sí" sin especificar → asumir la de monto más cercano
                matched = min(candidates, key=lambda c: abs((c["amount"] or 0) - paid_amount))
            if matched:
                await _auto_mark_invoice_paid(
                    type("_", (), {"id": matched["id"], "value_ars": matched["amount"]})(),
                    paid_amount, payment_method
                )
                await _remove_invoice_confirmation(conf_id)
                otros = [c for c in candidates if c["id"] != matched["id"]]
                msg = f"✅ *{matched['name']}* marcada como pagada."
                if otros:
                    msg += " Todavía tenés pendiente: " + ", ".join(f"*{c['name']}*" for c in otros) + "."
                await send_message(phone, msg)
            else:
                await _remove_invoice_confirmation(conf_id)
                await send_message(phone, "Ok, dejé todas las facturas como pendientes.")
            return True

    if state_type == "unknown_card_register":
        last4 = state.get("last4", "")
        del pending_state[phone]
        skip_words = {"no", "n", "nope", "omitir", "skip", "no gracias"}
        if not text.strip():
            # texto vacío (imagen sola) — re-preguntar sin consumir el estado
            pending_state[phone] = state
            await send_message(phone, f"💳 Tengo una tarjeta terminada en *{last4}* sin registrar. ¿De qué banco es y es débito o crédito?")
            return True
        if text.strip().lower() in skip_words:
            return True
        response_ai = await claude_create(
            model="claude-haiku-4-5-20251001", max_tokens=100,
            system="Extrae banco y tipo de tarjeta. Responde SOLO JSON.",
            messages=[{"role": "user", "content": f'Mensaje: "{text}". Ultimos 4 digitos: {last4}. Responde: {{"bank": "nombre del banco (ej: BBVA)", "type": "Debit o Credit", "last4": "{last4}"}}'}]
        )
        raw_ai = response_ai.content[0].text.strip().strip("`").lstrip("json").strip()
        try:
            card_data = json.loads(raw_ai)
            bank = card_data.get("bank", "").strip()
            ctype = card_data.get("type", "").strip()
            if bank:
                label = f"{bank} {ctype}".strip()
                pending_state[phone] = {"type": "unknown_card_owner", "last4": last4, "bank": bank, "card_type": ctype, "label": label}
                await send_message(phone, f"✅ *{label}* (****{last4}). ¿De quién es? (ej: _\"mía\"_, _\"Sofi\"_) o «no» para omitir.")
        except Exception:
            await send_message(phone, f"No pude interpretar. Podés agregarla con: _\"agregá [banco] [débito/crédito] terminada en {last4}\"_")
        return True

    if state_type == "unknown_card_owner":
        last4 = state.get("last4", "")
        bank = state.get("bank", "")
        ctype = state.get("card_type", "")
        label = state.get("label", "")
        del pending_state[phone]
        skip_words = {"no", "n", "nope", "omitir", "skip"}
        owner = None if text.strip().lower() in skip_words else text.strip()
        name = f"{label} ****{last4}" if last4 else label
        page_id = await _ds.create_payment_method(
            name=name, modality=ctype, bank=bank, last4=last4, owner=owner
        )
        if page_id:
            from notion_datastore import PaymentMethod as _PM
            payment_methods_cache.append(_PM(
                id=page_id, name=name, modality=ctype,
                bank=bank, last4=last4, owner=owner, is_default=False
            ))
        owner_str = f" — de {owner}" if owner else ""
        await send_message(phone, f"✅ Guardé *{label}* (****{last4}){owner_str} en tus medios de pago.")
        return True

    if state_type == "bulk_correction_confirm":
        del pending_state[phone]
        affirmative = text.strip().lower() in {"si", "sí", "dale", "ok", "yes", "confirmo", "s", "adelante"}
        if not affirmative:
            await send_message(phone, "Cancelado, no modifiqué nada.")
            return True
        entries = state.get("entries", [])
        updates = state.get("updates", {})
        errors = []
        updated = 0
        for e in entries:
            try:
                await _ds.update_expense(e["id"], updates)
                updated += 1
            except Exception as ex:
                errors.append(str(ex)[:80])
        if errors and not updated:
            await send_message(phone, f"Error al actualizar: {errors[0]}")
        else:
            changes = []
            if "value_ars" in updates:
                changes.append(f"monto → *${float(updates['value_ars']):,.0f} ARS*")
            if "categories" in updates:
                changes.append(f"categoría → _{', '.join(updates['categories'])}_")
            if "name" in updates:
                changes.append(f"nombre → _{updates['name']}_")
            if "notes" in updates:
                changes.append(f"nota → _{updates['notes']}_")
            await send_message(phone, f"✅ *{updated}* entradas corregidas\n" + "\n".join(changes))
        return True

    if state_type == "select_payment_method":
        page_id = state.get("page_id")
        bank = state.get("bank", "")
        options = state.get("options", [])
        del pending_state[phone]
        # Buscar la opción que matchea lo que dijo el usuario
        t = text.strip().lower()
        chosen = None
        for opt in options:
            mod = opt.get("modality", "").lower()
            l4 = opt.get("last4") or ""
            if mod in t or (l4 and l4 in t):
                chosen = opt
                break
        if not chosen and options:
            chosen = options[0]
        if chosen and page_id:
            pm_str = chosen["name"] if chosen.get("name") else f"{bank} {chosen.get('modality','')}"
            if chosen.get("last4"):
                pm_str += f" ****{chosen['last4']}"
            await _ds.update_expense(page_id, {"payment_method": pm_str})
            await send_message(phone, f"✅ Actualicé el medio de pago a *{pm_str}*.")
        return True

    if state_type == "save_location_confirm":
        lat = state.get("lat")
        lon = state.get("lon")
        loc_name = state.get("loc_name") or f"{lat:.4f}, {lon:.4f}"
        del pending_state[phone]
        affirmative = text.strip().lower() in ["si", "sí", "dale", "ok", "yes", "s", "guardar"]
        if not affirmative:
            await send_message(phone, "Ok, no guardé el lugar.")
            return True
        # Pedir el nombre del lugar
        pending_state[phone] = {
            "type": "save_location_name",
            "lat": lat, "lon": lon, "loc_name": loc_name,
            "expires_at": (now_argentina() + timedelta(minutes=10)).replace(tzinfo=None).isoformat(),
        }
        await send_message(phone, f"📍 _{loc_name}_\n¿Cómo querés que se llame este lugar? (ej: Trabajo, Casa, Gym)")
        return True

    if state_type == "save_location_name":
        lat = state.get("lat")
        lon = state.get("lon")
        loc_name = state.get("loc_name") or ""
        del pending_state[phone]
        nombre = text.strip().capitalize()
        if not nombre:
            await send_message(phone, "No entendí el nombre. No guardé el lugar.")
            return True
        places = user_prefs.get("known_places", [])
        places = [p for p in places if p["name"].lower() != nombre.lower()]
        places.append({"name": nombre, "lat": lat, "lon": lon, "radius": 150})
        user_prefs["known_places"] = places
        await save_user_config(MY_NUMBER)
        await send_message(phone, f"✅ *{nombre}* guardado como lugar conocido.")
        return True

    if state_type == "confirm_service_providers":
        proposed = state.get("proposed", {})
        del pending_state[phone]
        if text.strip() == "providers_ok":
            user_prefs["service_providers"] = proposed
            await save_user_config(phone)
            await send_message(phone, "Listo, ya se quienes son tus proveedores de servicios. La proxima vez que me preguntes sobre facturas voy a buscar directamente.")
        else:
            pending_state[phone] = {"type": "correct_service_providers", "proposed": proposed}
            await send_message(phone, "Dale, decime las correcciones. Por ejemplo: \"gas es Camuzzi, internet es Personal\"")
        return True

    if state_type == "correct_service_providers":
        proposed = state.get("proposed", {})
        del pending_state[phone]
        try:
            resp = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=200,
                system="Aplica las correcciones del usuario al JSON de proveedores. Responde SOLO JSON.",
                messages=[{"role": "user", "content": f"Proveedores actuales: {json.dumps(proposed, ensure_ascii=False)}\nCorrecciones: {text}\nResponde el JSON corregido."}]
            )
            raw = resp.content[0].text.strip().strip("`").lstrip("json").strip()
            corrected = json.loads(raw)
            user_prefs["service_providers"] = corrected
            await save_user_config(phone)
            resumen = ", ".join(f"{k}: {v}" for k, v in corrected.items())
            await send_message(phone, f"Guardado: {resumen}")
        except Exception:
            await send_message(phone, "No pude aplicar las correcciones. Intenta de nuevo.")
        return True

    return False

# ── Webhook ────────────────────────────────────────────────────────────────────
@app.get("/webhook")
async def verify_webhook(request: Request):
    params = dict(request.query_params)
    if params.get("hub.verify_token") == os.environ.get("WHATSAPP_VERIFY_TOKEN", "finanzas_bot_token"):
        return int(params.get("hub.challenge", 0))
    return {"error": "Verification failed"}

async def _delayed_indicator(phone: str, done: list):
    await asyncio.sleep(PROCESSING_INDICATOR_DELAY)
    if not done[0]:
        await send_message(phone, "⏳ Procesando...")


async def _classify_group(items: list) -> list[list]:
    """Decide si una lista de mensajes agrupados son sobre lo mismo o cosas distintas."""
    descriptions = []
    for i, item in enumerate(items):
        if item["image_b64"] and not item["text"]:
            desc = f"{i}: [imagen]"
        elif item["image_b64"]:
            desc = f'{i}: [imagen + "{item["text"]}"]'
        else:
            desc = f'{i}: "{item["text"]}"'
        descriptions.append(desc)

    try:
        resp = await claude_create(
            model="claude-haiku-4-5-20251001",
            max_tokens=100,
            system="Responde SOLO JSON valido sin markdown.",
            messages=[{"role": "user", "content": f"""El usuario mando estos mensajes en rapida sucesion:

{chr(10).join(descriptions)}

Son sobre lo mismo o son cosas distintas? Indices base 0.
- Si claramente relacionados: {{"r":"related"}}
- Si claramente no relacionados: {{"r":"unrelated","groups":[[0,1],[2]]}}
- Si hay duda: {{"r":"related"}}"""}]
        )
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.strip("`").lstrip("json").strip()
        result = json.loads(raw)
        if result["r"] == "unrelated" and "groups" in result:
            groups = []
            for g in result["groups"]:
                group = [items[i] for i in g if i < len(items)]
                if group:
                    groups.append(group)
            return groups if groups else [items]
    except Exception:
        pass
    return [items]


def _merge_items(items: list) -> dict:
    """Combina múltiples mensajes en uno solo para procesar como unidad."""
    texts = [i["text"] for i in items if i["text"]]
    images = [(i["image_b64"], i["image_type"]) for i in items if i["image_b64"]]
    return {
        "text": "\n".join(texts),
        "image_b64": images[0][0] if images else None,
        "image_type": images[0][1] if images else None,
        "extra_images": images[1:] if len(images) > 1 else [],
    }


async def _flush_buffer(phone: str):
    await asyncio.sleep(BUFFER_WINDOW_SECS)
    items = message_buffer.pop(phone, [])
    buffer_timers.pop(phone, None)
    if not items:
        return
    if len(items) == 1:
        await process_single_item(phone, items[0])
        return
    groups = await _classify_group(items)
    for group in groups:
        await process_single_item(phone, _merge_items(group))


async def enqueue_message(message: dict):
    phone = "54298154894334"
    try:
        msg_id = message.get("id", "")
        if msg_id and msg_id in processed_message_ids:
            return
        if msg_id:
            processed_message_ids.add(msg_id)
            if len(processed_message_ids) > MAX_PROCESSED_IDS:
                processed_message_ids.clear()

        msg_type = message["type"]
        text = ""
        image_b64 = image_type = None

        if msg_type != "reaction" and msg_id:
            await send_reaction(phone, msg_id, "✅")

        if msg_type == "text":
            text = message["text"]["body"]
        elif msg_type == "interactive":
            btn = message.get("interactive", {}).get("button_reply", {})
            text = btn.get("id", "")
            if not text:
                return
            # Botones interactivos van directo, sin buffer
            await process_single_item(phone, {"text": text, "image_b64": None, "image_type": None, "extra_images": []})
            return
        elif msg_type == "image":
            media_id = message["image"]["id"]
            text = message["image"].get("caption", "")
            image_b64, image_type = await get_media_base64(media_id)
        elif msg_type == "document":
            media_id = message["document"]["id"]
            text = message["document"].get("caption", "")
            image_b64, image_type = await get_media_base64(media_id)
        elif msg_type == "audio":
            media_id = message["audio"]["id"]
            await send_message(phone, "🎙️ Transcribiendo audio...")
            transcripcion = await transcribe_audio(media_id)
            if transcripcion:
                text = transcripcion
                await send_message(phone, f"_{transcripcion}_")
            else:
                await send_message(phone, "No pude transcribir el audio. Mandalo como texto.")
                return
        elif msg_type == "location":
            loc = message.get("location", {})
            lat = loc.get("latitude")
            lon = loc.get("longitude")
            if lat and lon:
                current_location["lat"] = float(lat)
                current_location["lon"] = float(lon)
                current_location["updated_at"] = now_argentina()
                current_location["source"] = "whatsapp"
                current_location["velocity"] = 0
                loc_name = await reverse_geocode(float(lat), float(lon))
                if loc_name:
                    current_location["location_name"] = loc_name
                if phone in pending_state and pending_state[phone].get("type") == "geo_reminder_awaiting_location":
                    state = pending_state.pop(phone)
                    description = state.get("description", "Recordatorio")
                    recurrent = state.get("recurrent", False)
                    kp_name = state.get("place_name")
                    ok, _ = await create_geo_reminder(
                        description=description,
                        rtype="place",
                        lat=float(lat),
                        lon=float(lon),
                        recurrent=recurrent,
                    )
                    place_label = loc_name or f"{lat:.4f}, {lon:.4f}"
                    if ok:
                        await _save_known_place(kp_name, float(lat), float(lon))
                        freq = "Cada vez que" if recurrent else "La proxima vez que"
                        saved_kp = f"\n💾 *{kp_name.strip().capitalize()}* guardado como lugar conocido." if kp_name else ""
                        await send_message(phone, f"📍 *Geo-reminder guardado*\n_{description}_\nUbicacion: *{place_label}*\n{freq} estes cerca, te aviso.{saved_kp}")
                    else:
                        await send_message(phone, "No pude guardar el geo-reminder.")
                    return
                place = is_at_known_place()
                if place:
                    await send_message(phone, f"📍 Ubicacion actualizada: *{place['name']}*")
                else:
                    pending_state[phone] = {
                        "type": "save_location_confirm",
                        "lat": float(lat),
                        "lon": float(lon),
                        "loc_name": loc_name,
                        "expires_at": (now_argentina() + timedelta(minutes=10)).replace(tzinfo=None).isoformat(),
                    }
                    await send_message(phone, "📍 Ubicacion actualizada. No reconozco este lugar, queres que lo guarde?")
            return
        else:
            return

        if msg_type == "text" and is_bot_message(text):
            return

        if text.strip().lower() in ["/start", "hola", "help", "ayuda"]:
            await send_message(phone,
                "*Hola! Soy Knot*\n\n"
                "*Gastos:* _\"Verduleria 3500\"_\n"
                "*Plantas:* _\"Me compre un potus\"_\n"
                "*Eventos:* _\"Manana a las 10 turno medico\"_\n"
                "*Fotos:* manda cualquier factura\n"
                "*Audios:* habla directo, te entiendo\n\n"
                "Todo se guarda automaticamente"
            )
            return

        item = {"text": text, "image_b64": image_b64, "image_type": image_type, "extra_images": []}

        # pending_state activo: respuesta inmediata sin buffer
        if phone in pending_state:
            await process_single_item(phone, item)
            return

        if phone not in message_buffer:
            message_buffer[phone] = []
        message_buffer[phone].append(item)

        if phone in buffer_timers and not buffer_timers[phone].done():
            buffer_timers[phone].cancel()
        buffer_timers[phone] = asyncio.create_task(_flush_buffer(phone))

    except Exception as e:
        try:
            await send_message("54298154894334", f"Error: {str(e)[:200]}")
        except Exception:
            pass


@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    try:
        messages = body["entry"][0]["changes"][0]["value"].get("messages")
        if messages:
            background_tasks.add_task(enqueue_message, messages[0])
    except Exception:
        pass
    return {"ok": True}

async def handle_geo_reminder(phone: str, text: str) -> str:
    """Crea un geo-reminder a partir de lenguaje natural."""
    now = now_argentina()
    lat, lon = get_current_location()
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system=f"""Extrae info de un recordatorio geolocalizacion. Hoy: {now.strftime("%Y-%m-%d")}.
Responde SOLO JSON valido sin markdown:
{{"description": "que recordar (solo la tarea, ej: 'agarrar los planos', 'comprar leche')",
  "place_name": "nombre del lugar donde disparar (ej: 'trabajo', 'casa', 'gym', 'super'). null si no se menciona lugar.",
  "type": "place" o "shop",
  "shop_name": "nombre del comercio si es tipo shop, null si no",
  "address": "direccion si la menciona, null si no",
  "recurrent": SOLO true si el usuario explicitamente pide que se repita siempre ("cada vez que pase", "siempre que este cerca", "recordame siempre"). En todos los demas casos false,
  "radius": radio en metros si lo menciona (ej: "a menos de 500m" -> 500, "cuando este muy cerca" -> 20, "en la zona" -> 300). Si no menciona distancia usar null,
  "needs_location": true si necesitas que el usuario comparta la ubicacion del lugar}}""",
        messages=[{"role": "user", "content": text}]
    )
    raw = response.content[0].text.strip().strip("`").lstrip("json").strip()
    try:
        data = json.loads(raw)
    except Exception:
        return "No pude interpretar el recordatorio. Intentalo de nuevo."

    description = data.get("description", text)
    place_name = data.get("place_name")
    rtype = data.get("type", "place")
    shop_name = data.get("shop_name")
    recurrent = data.get("recurrent", False)
    needs_location = data.get("needs_location", False)
    address = data.get("address")
    radius_raw = data.get("radius")
    radius = int(radius_raw) if radius_raw is not None else None
    user_specified_radius = radius is not None
    if radius is None:
        radius = 20

    # Detectar link de Google Maps en el texto
    import re
    _maps_match = re.search(r'https?://(?:maps\.app\.goo\.gl|goo\.gl/maps|maps\.google\.com)\S*', text)
    if _maps_match and not (rtype == "shop" and shop_name):
        _maps_coords = await extract_coords_from_maps_url(_maps_match.group(0))
        if _maps_coords:
            _lat, _lon = _maps_coords
            ok, _ = await create_geo_reminder(
                description=description, rtype="place",
                lat=_lat, lon=_lon, radius=radius, recurrent=recurrent,
            )
            if ok:
                await _save_known_place(place_name, _lat, _lon)
                freq = "Cada vez que" if recurrent else "La proxima vez que"
                saved_kp = f"\n💾 *{place_name.strip().capitalize()}* guardado como lugar conocido." if place_name else ""
                return f"Geo-reminder guardado\n_{description}_\nSaque las coordenadas del link: {_lat:.5f}, {_lon:.5f} (radio: {radius}m)\n{freq} estes cerca, te aviso.{saved_kp}"
            return "No pude guardar el geo-reminder."
    # Detectar coordenadas directamente en el texto original
    _coord = re.search(r'(-\d{2,3}\.\d{4,})[,\s]+(-\d{2,3}\.\d{4,})', text)
    if _coord and not (rtype == "shop" and shop_name):
        _lat = float(_coord.group(1))
        _lon = float(_coord.group(2))
        ok, _ = await create_geo_reminder(
            description=description, rtype="place",
            lat=_lat, lon=_lon, radius=radius, recurrent=recurrent,
        )
        if ok:
            await _save_known_place(place_name, _lat, _lon)
            freq = "Cada vez que" if recurrent else "La proxima vez que"
            saved_kp = f"\n💾 *{place_name.strip().capitalize()}* guardado como lugar conocido." if place_name else ""
            return f"Geo-reminder guardado\n_{description}_\nCoordenadas: {_lat:.5f}, {_lon:.5f} (radio: {radius}m)\n{freq} estes cerca, te aviso.{saved_kp}"
        return "No pude guardar el geo-reminder."

    # Si es tipo shop, crear directamente
    if rtype == "shop" and shop_name:
        ok, page_id = await create_geo_reminder(
            description=description,
            rtype="shop",
            shop_name=shop_name,
            radius=radius,
            recurrent=recurrent,
        )
        if ok:
            freq = "Cada vez que" if recurrent else "La proxima vez que"
            confirm_text = f"📍 *Geo-reminder guardado*\n_{description}_\n{freq} estes a menos de {radius}m de *{shop_name}*, te aviso."
            if not user_specified_radius and page_id:
                await send_message(phone, confirm_text)
                pending_state[phone] = {"type": "geo_reminder_radius_confirm", "page_id": page_id, "name": description}
                await send_interactive_buttons(
                    phone,
                    "Radio por defecto: 20m. ¿Querés cambiarlo?",
                    [
                        {"id": "radius_50", "title": "50m"},
                        {"id": "radius_100", "title": "100m"},
                        {"id": "radius_300", "title": "300m"},
                    ]
                )
                return ""
            return confirm_text
        return "No pude guardar el geo-reminder."

    # Si tiene direccion, geocodificar
    if address:
        try:
            async with httpx.AsyncClient(timeout=5) as http:
                r = await http.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": address, "format": "json", "limit": 1, "addressdetails": 1},
                    headers={"User-Agent": "Knot/1.0"}
                )
                if r.status_code == 200 and r.json():
                    result = r.json()[0]
                    place_lat = float(result["lat"])
                    place_lon = float(result["lon"])
                    geo_label = _format_place_name(result, address)
                    ok, _ = await create_geo_reminder(
                        description=description,
                        rtype="place",
                        lat=place_lat,
                        lon=place_lon,
                        radius=radius,
                        recurrent=recurrent,
                    )
                    if ok:
                        await _save_known_place(place_name, place_lat, place_lon)
                        freq = "Cada vez que" if recurrent else "La proxima vez que"
                        saved_kp = f"\n💾 *{place_name.strip().capitalize()}* guardado como lugar conocido." if place_name else ""
                        return f"📍 *Geo-reminder guardado*\n_{description}_\nAsumi que el lugar es *{geo_label}*.\n{freq} estes a menos de {radius}m, te aviso.{saved_kp}\n\n¿Es correcto o queres ajustar la ubicacion?"
        except Exception:
            pass

    # Buscar en known_places por place_name (lugar donde disparar), no por description
    search_place = (place_name or description).lower()
    for _kp in user_prefs.get("known_places", []):
        _kp_name = _kp.get("name", "").lower()
        if _kp_name and _kp_name in search_place:
            _kp_lat = _kp.get("lat")
            _kp_lon = _kp.get("lon")
            if _kp_lat and _kp_lon:
                ok, _ = await create_geo_reminder(
                    description=description, rtype="place",
                    lat=float(_kp_lat), lon=float(_kp_lon),
                    radius=radius, recurrent=recurrent,
                )
                if ok:
                    freq = "Cada vez que" if recurrent else "La proxima vez que"
                    _check_routine_geo_hint(phone, description, _kp['name'])
                    return f"📍 *Geo-reminder guardado*\n_{description}_\nUbicacion: *{_kp['name']}*\n{freq} estes cerca, te aviso."
                return "No pude guardar el geo-reminder."

    # Si necesita ubicacion o no pudo geocodificar
    pending_state[phone] = {
        "type": "geo_reminder_awaiting_location",
        "description": description,
        "place_name": place_name,
        "recurrent": recurrent,
    }
    lugar_str = f" de *{place_name}*" if place_name else ""
    return f"No tengo guardada la ubicacion{lugar_str}. Compartila por WhatsApp (📎 → Ubicacion), mandame la dirección, o pegá un link de Google Maps."

# ── Keywords para detectar carga de combustible ──────────────────────────────
FUEL_KEYWORDS = {"nafta", "combustible", "gnc", "gasoil", "premium", "super nafta",
                 "carga nafta", "cargue nafta", "puse nafta"}

BOT_PREFIXES = (
    "Procesando", "Receta", "Recordatorio", "Recorda",
    "Buenos dias", "Tu lista", "Finanzas", "Te recuerdo",
    "Guardado", "guardada",
)

def is_bot_message(text: str) -> bool:
    clean = text.lstrip(" *_~")
    return clean.startswith(BOT_PREFIXES)

async def process_single_item(phone: str, item: dict):
    text = item.get("text", "")
    image_b64 = item.get("image_b64")
    image_type = item.get("image_type")
    extra_images = item.get("extra_images", [])

    if not text and not image_b64:
        return

    _done = [False]
    indicator_task = asyncio.create_task(_delayed_indicator(phone, _done))

    def _cancel_indicator():
        _done[0] = True
        if not indicator_task.done():
            indicator_task.cancel()

    async def _reply(msg: str):
        _cancel_indicator()
        await send_message(phone, msg)

    try:
        if phone in pending_state:
            # Si llega una imagen sin texto y el pending state es "soft" (no bloqueante),
            # lo descartamos y procesamos la imagen como un mensaje nuevo
            _soft_states = {"unknown_card_register", "unknown_card_owner", "undo_window", "select_payment_method"}
            if image_b64 and not text and pending_state.get(phone, {}).get("type") in _soft_states:
                del pending_state[phone]
            else:
                _done[0] = True
                indicator_task.cancel()
                handled = await handle_pending_state(phone, text, pending_state.get(phone, {}))
                if handled:
                    return

        if user_prefs.get("_config_page_id") is None:
            await load_user_config(phone)

        tipo = await classify(text, image_b64 is not None, image_b64, image_type, history=get_history(phone), extra_images=extra_images)
        exchange_rate = await get_exchange_rate()

        if tipo == "GASTO":
            reply = await handle_gasto_agent(phone, text, image_b64, image_type, exchange_rate, extra_images=extra_images)
            await _reply(reply)

        elif tipo == "DEUDA":
            reply = await handle_deuda_agent(phone, text)
            await _reply(reply)

        elif tipo == "ELIMINAR_SHOPPING":
            success, msg = await eliminar_shopping(text)
            await _reply(msg if success else msg)

        elif tipo == "ELIMINAR_GASTO":
            success, msg = await eliminar_gasto(text, phone)
            if msg:
                await _reply(msg)

        elif tipo == "CORREGIR_GASTO":
            success, msg = await corregir_gasto(text, phone=phone)
            await _reply(msg if success else msg)

        elif tipo == "PLANTA":
            parsed = await parse_planta(text, exchange_rate)
            success, plant_id = await create_planta(parsed)
            if success:
                reply = format_planta(parsed)
                expires_at = (now_argentina() + timedelta(seconds=60)).replace(tzinfo=None).isoformat()
                pending_state[phone] = {
                    "type": "undo_window", "action": "plant",
                    "page_id": plant_id, "name": parsed.get("name", "planta"),
                    "expires_at": expires_at,
                }
                reply += "\n\n_Si algo no quedó bien, avisame._"
                await _reply(reply)
            else:
                await _reply(plant_id)

        elif tipo == "EDITAR_PLANTA":
            success, msg = await editar_planta(text)
            await _reply(msg)

        elif tipo == "ELIMINAR_PLANTA":
            success, msg = await eliminar_planta(text, phone)
            if msg:
                await _reply(msg)

        elif tipo in ("EVENTO", "EDITAR_EVENTO", "ELIMINAR_EVENTO"):
            reply = await handle_evento_agent(phone, text, image_b64, image_type)
            if reply:
                await _reply(reply)

        elif tipo == "GEO_REMINDER":
            respuesta = await handle_geo_reminder(phone, text)
            if respuesta:
                await _reply(respuesta)

        elif tipo == "RECORDATORIO":
            parsed = await parse_recordatorio(text)
            success, error = await create_recordatorio(parsed)
            if success:
                await _reply(format_recordatorio(parsed))
            else:
                await _reply(f"No pude crear el recordatorio: {error[:100]}")

        elif tipo == "CANCELAR_RECORDATORIO":
            success, msg = await cancelar_recordatorio(text)
            await _reply(msg)

        elif tipo == "SHOPPING":
            shopping_text = text
            if not shopping_text.strip() and image_b64:
                try:
                    extr = await claude_create(
                        model="claude-sonnet-4-20250514", max_tokens=1200,
                        system="Transcribi TODO el contenido de la imagen exactamente como esta escrito. Si es una receta: copia el titulo, luego todas las secciones tal como aparecen. No omitas nada.",
                        messages=[{"role": "user", "content": [
                            {"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}},
                            {"type": "text", "text": "Transcribi todo el contenido de esta imagen fielmente."}
                        ]}]
                    )
                    shopping_text = extr.content[0].text.strip()
                except Exception:
                    shopping_text = ""
            respuesta = await handle_shopping(shopping_text, phone=phone)
            if respuesta is not None:
                await _reply(respuesta)
                add_to_history(phone, "user", text)
                add_to_history(phone, "assistant", respuesta)

        elif tipo == "RESUMEN_DIARIO":
            try:
                _cancel_indicator()
                _at = await get_gcal_access_token()
                async with httpx.AsyncClient() as _http:
                    await send_daily_summary(_http, _at, now_argentina())
                add_to_history(phone, "assistant", "[Resumen diario enviado]")
            except Exception as _e:
                await _reply(f"No pude generar el resumen: {str(_e)[:100]}")

        elif tipo == "CONFIGURAR":
            respuesta = await handle_configurar(text)
            await _reply(respuesta)

        elif tipo == "LISTA":
            respuesta = await handle_lista_generativa(phone, text)
            if respuesta:
                await send_message(phone, respuesta)

        elif tipo == "SALUD":
            reply = await handle_salud_agent(phone, text, image_b64, image_type)
            if reply:
                await _reply(reply)

        elif tipo == "ACTIVIDAD_FISICA":
            reply = await handle_fitness_agent(phone, text, image_b64, image_type)
            if reply:
                await _reply(reply)

        elif tipo == "REUNION":
            respuesta = await handle_reunion(text, image_b64, image_type, phone=phone)
            await _reply(respuesta)

        elif tipo == "EDITAR_REUNION":
            success, msg = await editar_reunion(text)
            await _reply(msg)

        elif tipo == "ELIMINAR_REUNION":
            success, msg = await eliminar_reunion(text, phone)
            if msg:
                await _reply(msg)

        elif tipo == "CORREGIR_SHOPPING":
            success, msg = await corregir_shopping(text)
            await _reply(msg)

        elif tipo == "CHAT":
            respuesta = await handle_chat(phone, text)
            if respuesta:
                await _reply(respuesta)
                if "Ingredientes:" in respuesta and "Preparacion:" in respuesta:
                    try:
                        ext_response = await claude_create(
                            model="claude-sonnet-4-20250514", max_tokens=400,
                            system="Responde SOLO JSON valido sin markdown.",
                            messages=[{"role": "user", "content": f"""Del siguiente texto de receta, extrae el nombre y TODOS los ingredientes.
Texto: {respuesta[:2000]}
Responde:
{{"name": "nombre de la receta",
  "ingredients": ["ingrediente1", "ingrediente2", ...]}}"""}]
                        )
                        raw_ext = ext_response.content[0].text.strip()
                        if raw_ext.startswith("```"):
                            raw_ext = raw_ext.strip("`").lstrip("json").strip()
                        extracted = json.loads(raw_ext)
                        recipe_name_chat = extracted.get("name", "Receta")
                        ingredient_list_chat = extracted.get("ingredients", [])
                        enriched_chat = await enrich_items_with_claude(ingredient_list_chat) if ingredient_list_chat else []
                    except Exception:
                        recipe_name_chat = "Receta"
                        enriched_chat = []
                    pending_state[phone] = {
                        "type": "recipe_save_confirm",
                        "recipe_name": recipe_name_chat,
                        "recipe_text": respuesta,
                        "ingredients": enriched_chat,
                    }
                    await send_interactive_buttons(
                        phone,
                        f"Guardamos *{recipe_name_chat.capitalize()}* en tus Recetas de Notion?",
                        [
                            {"id": "recipe_save_yes", "title": "Si, guardar"},
                            {"id": "recipe_save_no",  "title": "No gracias"},
                        ]
                    )

        # Después de la respuesta principal, intentar emitir un hint si aplica
        try:
            await maybe_fire_hint(phone)
        except Exception:
            pass

    except json.JSONDecodeError:
        pass
    except Exception as e:
        try:
            err_msg = f"{type(e).__name__}: {str(e)}"
            await send_message(phone, f"Error: {err_msg[:200]}")
        except Exception:
            pass
    finally:
        _done[0] = True
        indicator_task.cancel()

@app.api_route("/", methods=["GET", "HEAD"])
async def health():
    return {"status": "ok", "bot": "knot"}

# ── MODULO RECORDATORIOS ───────────────────────────────────────────────────────
async def parse_recordatorio(text: str) -> dict:
    now = now_argentina()
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extrae info del recordatorio. Responde SOLO JSON valido sin markdown.",
        messages=[{"role": "user", "content": f"""Ahora son las {now.strftime("%Y-%m-%d %H:%M")} en Argentina.
Mensaje: {text}
Responde:
{{"summary": "descripcion","fire_at": "YYYY-MM-DDTHH:MM","emoji": "emoji"}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

async def create_recordatorio(data: dict) -> tuple[bool, str]:
    access_token = await get_gcal_access_token()
    if not access_token:
        return False, "Calendar no configurado"
    fire_at = data["fire_at"]
    start_dt = datetime.strptime(fire_at, "%Y-%m-%dT%H:%M")
    end_dt = start_dt + timedelta(minutes=1)
    summary_raw = data.get("summary", "Recordatorio")
    if summary_raw.startswith("🔔"):
        summary_final = summary_raw
    else:
        summary_final = f"🔔 {summary_raw}"
    event = {
        "summary": summary_final,
        "description": "[TEMP]",
        "start": {"dateTime": f"{fire_at}:00", "timeZone": "America/Argentina/Buenos_Aires"},
        "end":   {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:00"), "timeZone": "America/Argentina/Buenos_Aires"},
        "source":   {"title": "Knot", "url": "https://web-production-6874a.up.railway.app"},
        "colorId":  "4",
        "extendedProperties": {"private": {"created_by": "matrics", "type": "recordatorio"}},
    }
    async with httpx.AsyncClient() as http:
        r = await http.post(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json=event
        )
        return (True, "") if r.status_code in [200, 201] else (False, r.text)

def format_recordatorio(data: dict) -> str:
    emoji = data.get("emoji", "🔔")
    fire_at = data.get("fire_at", "")
    try:
        dt = datetime.strptime(fire_at, "%Y-%m-%dT%H:%M")
        fecha = dt.strftime("%d/%m") if dt.date() != now_argentina().date() else "hoy"
        tiempo_str = f"{fecha} a las {dt.strftime('%H:%M')}"
    except Exception:
        tiempo_str = fire_at
    return f"{emoji} *{data['summary']}*\nTe aviso {tiempo_str}\n\nRecordatorio configurado"

# ── MODULO FITNESS ────────────────────────────────────────────────────────────

async def handle_fitness_agent(phone: str, text: str, image_b64: str = None, image_type: str = None) -> str:
    now = now_argentina()
    tools = [
        {
            "name": "registrar_actividad",
            "description": "Guarda una actividad física. Si hay imagen de una app deportiva (Adidas Running, Strava, Nike Run Club, etc.), extraé todos los datos vos mismo de la imagen.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "name":       {"type": "string", "description": "Descripción. Ej: 'Salida a correr — Parque Saavedra'"},
                    "activity":   {"type": "string", "enum": ["Correr", "Fútbol", "Ciclismo", "Natación", "Gym", "Caminata", "Yoga", "Tenis", "Padel", "Otro"]},
                    "date":       {"type": "string", "description": "YYYY-MM-DD"},
                    "duration":   {"type": ["number", "null"], "description": "Minutos"},
                    "distance":   {"type": ["number", "null"], "description": "Kilómetros"},
                    "calories":   {"type": ["number", "null"]},
                    "avg_speed":  {"type": ["number", "null"], "description": "km/h"},
                    "elevation":  {"type": ["number", "null"], "description": "Metros de desnivel"},
                    "notes":      {"type": ["string", "null"]},
                    "source":     {"type": "string", "enum": ["Manual", "App"], "description": "Manual si lo dicta el usuario, App si viene de screenshot"},
                    "source_app": {"type": ["string", "null"], "description": "Ej: 'Adidas Running', 'Strava'"},
                },
                "required": ["name", "activity", "date", "source"]
            }
        },
        {
            "name": "consultar_actividades",
            "description": "Consulta el historial de actividades físicas para responder preguntas: km totales, promedio, comparaciones entre meses, etc.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "activity": {"type": ["string", "null"], "description": "Filtrar por tipo. Ej: 'Correr'"},
                    "month":    {"type": ["string", "null"], "description": "YYYY-MM. Null para todos los registros."},
                    "limit":    {"type": "integer", "description": "Default 30, max 100"},
                },
                "required": []
            }
        },
        {
            "name": "editar_actividad",
            "description": "Edita una actividad registrada. Obtené el id de consultar_actividades.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "entry_id":  {"type": "string"},
                    "activity":  {"type": ["string", "null"]},
                    "date":      {"type": ["string", "null"]},
                    "duration":  {"type": ["number", "null"]},
                    "distance":  {"type": ["number", "null"]},
                    "calories":  {"type": ["number", "null"]},
                    "avg_speed": {"type": ["number", "null"]},
                    "elevation": {"type": ["number", "null"]},
                    "notes":     {"type": ["string", "null"]},
                },
                "required": ["entry_id"]
            }
        },
        {
            "name": "eliminar_actividad",
            "description": "Elimina una actividad registrada. Obtené el id de consultar_actividades.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "entry_id": {"type": "string"},
                },
                "required": ["entry_id"]
            }
        },
    ]

    content = []
    if image_b64:
        content.append({"type": "image", "source": {"type": "base64", "media_type": image_type or "image/jpeg", "data": image_b64}})
    content.append({"type": "text", "text": text or "(ver imagen adjunta)"})

    system = f"""Sos Knot, asistente personal en WhatsApp. Hablas en español rioplatense, natural y conciso.
Hoy: {now.strftime('%d/%m/%Y')}.

Tu tarea: registrar y consultar actividad física del usuario.

REGLAS:
- Si hay imagen de una app deportiva: extraé TODOS los datos que muestre (distancia, tiempo, velocidad, calorías, desnivel, etc.) sin pedirle nada al usuario.
- Para consultas con comparaciones entre meses: hacé dos llamadas a consultar_actividades (una por mes) y calculá vos las diferencias.
- Respondé con números concretos, no con listas genéricas."""

    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=1500,
        system=system,
        messages=get_history(phone) + [{"role": "user", "content": content}],
        tools=tools
    )

    if response.stop_reason == "end_turn":
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()
        add_to_history(phone, "user", text or "")
        add_to_history(phone, "assistant", reply)
        return reply

    tool_results = []
    for block in response.content:
        if block.type != "tool_use":
            continue
        inp = dict(block.input)
        tr = ""

        if block.name == "registrar_actividad":
            ok, pid = await _ds.create_fitness(inp)
            if ok:
                tr = f"Actividad guardada. ID: {pid}"
                asyncio.create_task(update_domain_profile_bg(
                    "actividad_fisica",
                    f"Actividad: {inp.get('activity')}, {inp.get('date')}, distancia: {inp.get('distance') or '-'}km, duración: {inp.get('duration') or '-'}min"
                ))
            else:
                tr = f"Error: {pid}"

        elif block.name == "consultar_actividades":
            entries = await _ds.query_fitness(
                activity=inp.get("activity"),
                month=inp.get("month"),
                limit=min(inp.get("limit", 30), 100),
            )
            if not entries:
                tr = "No hay actividades registradas con esos filtros."
            else:
                lines = []
                for e in entries:
                    parts = [f"{e['date']} — {e['activity']}"]
                    if e["distance"]: parts.append(f"{e['distance']}km")
                    if e["duration"]: parts.append(f"{e['duration']}min")
                    if e["calories"]: parts.append(f"{e['calories']}kcal")
                    if e["avg_speed"]: parts.append(f"{e['avg_speed']}km/h")
                    lines.append(f"[{e['id']}] " + " | ".join(parts))
                tr = "\n".join(lines)

        elif block.name == "editar_actividad":
            eid = inp.pop("entry_id")
            ok = await _ds.update_fitness(eid, inp)
            tr = "Actualizado." if ok else "Error actualizando."

        elif block.name == "eliminar_actividad":
            expires_at = (now_argentina() + timedelta(minutes=5)).replace(tzinfo=None).isoformat()
            pending_state[phone] = {
                "type": "confirm_delete", "action": "fitness_entry",
                "page_id": inp["entry_id"], "name": "esta actividad", "expires_at": expires_at,
            }
            await send_interactive_buttons(phone, "¿Eliminás esta actividad?", [
                {"id": "confirm_delete_yes", "title": "Sí, eliminarla"},
                {"id": "confirm_delete_no", "title": "No, cancelar"},
            ])
            tr = "Pedí confirmación al usuario para eliminar la actividad."

        tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": tr})

    if not tool_results:
        reply = next((b.text for b in response.content if hasattr(b, "text") and b.text), "").strip()
        add_to_history(phone, "user", text or "")
        add_to_history(phone, "assistant", reply)
        return reply

    if pending_state.get(phone, {}).get("type") == "confirm_delete":
        add_to_history(phone, "user", text or "")
        return None

    final = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=800,
        system=system,
        messages=get_history(phone) + [
            {"role": "user", "content": content},
            {"role": "assistant", "content": response.content},
            {"role": "user", "content": tool_results},
        ]
    )
    reply = next((b.text for b in final.content if hasattr(b, "text") and b.text), "").strip()
    add_to_history(phone, "user", text or "")
    add_to_history(phone, "assistant", reply)
    return reply


_cron_job_running = False

async def _cron_loop():
    """Background loop que ejecuta cron_job cada 60 segundos sin depender de llamadas externas."""
    await asyncio.sleep(60)  # Espera inicial para que el servidor termine de arrancar
    while True:
        try:
            await cron_job()
        except Exception:
            pass
        await asyncio.sleep(60)

# ── CRON JOB ───────────────────────────────────────────────────────────────────
@app.get("/cron")
async def cron_job():
    global _cron_job_running
    if _cron_job_running:
        return {"ok": False, "reason": "already running"}
    _cron_job_running = True
    try:
        return await _cron_job_inner()
    finally:
        _cron_job_running = False

async def _cron_job_inner():
    await load_user_config(MY_NUMBER)
    now = now_argentina()
    fired = []

    effective_hour   = user_prefs.get("daily_summary_hour")
    effective_minute = user_prefs.get("daily_summary_minute")
    if effective_hour is None:   effective_hour   = DAILY_SUMMARY_HOUR
    if effective_minute is None: effective_minute = 0
    _sched_min = effective_hour * 60 + effective_minute
    _curr_min = now.hour * 60 + now.minute
    _last_daily = _last_summary_sent.get("daily")
    _sent_today = bool(_last_daily and _last_daily.date() == now.date())
    # Also check persisted date from Notion to survive restarts
    if not _sent_today:
        _persisted_date = user_prefs.get("_last_summary_date")
        if _persisted_date == now.date().isoformat():
            _sent_today = True
    if 0 <= (_curr_min - _sched_min) <= 3 and not _sent_today:
        # Mark BEFORE sending to prevent concurrent double-sends
        _last_summary_sent["daily"] = now
        user_prefs["_last_summary_date"] = now.date().isoformat()
        try:
            access_token_summary = await get_gcal_access_token()
            async with httpx.AsyncClient() as http_summary:
                await send_daily_summary(http_summary, access_token_summary, now)
            # Daily-frequency hint checks (encolan a pending_hints_queue)
            try:
                await _check_daily_hints(now)
            except Exception:
                pass
            await save_user_config(MY_NUMBER)
            fired.append("DAILY_SUMMARY")
        except Exception as e:
            fired.append(f"DAILY_SUMMARY_ERROR: {str(e)[:60]}")
            try:
                await send_message(MY_NUMBER, f"Error en resumen diario: {type(e).__name__}: {str(e)[:120]}")
            except Exception:
                pass

    nocturno_enabled = user_prefs.get("resumen_nocturno_enabled", True)
    nocturno_hour    = user_prefs.get("resumen_nocturno_hour", 22)
    semanal_enabled  = user_prefs.get("resumen_semanal_enabled", True)
    semanal_hour     = user_prefs.get("resumen_semanal_hour", 21)
    _is_sunday = now.weekday() == 6
    _nocturno_check_hour = semanal_hour if _is_sunday else nocturno_hour
    _nocturno_check_enabled = semanal_enabled if _is_sunday else nocturno_enabled
    if _nocturno_check_enabled and now.hour == _nocturno_check_hour and now.minute == 0:
        try:
            access_token_noc = await get_gcal_access_token()
            async with httpx.AsyncClient() as http_noc:
                await send_resumen_nocturno(http_noc, access_token_noc, now)
            fired.append("RESUMEN_NOCTURNO")
        except Exception:
            pass

    access_token = await get_gcal_access_token()
    if not access_token:
        return {"ok": True, "fired": fired, "time": now.strftime("%H:%M"), "warning": "no gcal token"}

    async with httpx.AsyncClient() as http:
        headers = {"Authorization": f"Bearer {access_token}"}
        r = await http.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            headers=headers,
            params={
                "timeMin": now.strftime("%Y-%m-%dT%H:%M:00-03:00"),
                "timeMax": (now + timedelta(minutes=61)).strftime("%Y-%m-%dT%H:%M:00-03:00"),
                "singleEvents": "true", "orderBy": "startTime", "maxResults": "20"
            }
        )
        if r.status_code != 200:
            return {"ok": True, "fired": fired, "time": now.strftime("%H:%M")}
        for event in r.json().get("items", []):
            event_id  = event.get("id")
            summary   = event.get("summary", "Evento")
            desc      = event.get("description", "") or ""
            start     = event.get("start", {})
            if "dateTime" not in start:
                continue
            try:
                diff_seconds = int(
                    (datetime.strptime(start["dateTime"][:16], "%Y-%m-%dT%H:%M") - now.replace(tzinfo=None))
                    .total_seconds()
                )
            except Exception:
                continue

            if "[TEMP]" in desc and -30 <= diff_seconds <= 90:
                clean_summary = summary.replace("🔔 ", "").strip()
                await send_message(MY_NUMBER, f"🔔 *Recordatorio*\n{clean_summary}")
                await http.delete(
                    f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}",
                    headers=headers
                )
                pending_state[MY_NUMBER] = {"type": "snooze", "summary": clean_summary}
                await send_interactive_buttons(
                    MY_NUMBER,
                    f"Queres posponer este recordatorio?\n_{clean_summary}_",
                    [
                        {"id": "snooze_5",  "title": "5 min"},
                        {"id": "snooze_15", "title": "15 min"},
                        {"id": "snooze_no", "title": "No posponer"},
                    ]
                )
                fired.append(f"TEMP: {summary}")
            elif "[REM:60]" in desc and 59 <= diff_seconds // 60 <= 61:
                loc_str = f"\n📍 {event.get('location')}" if event.get("location") else ""
                _ext = event.get("extendedProperties", {}).get("private", {})
                _geo_ctx = ""
                if _ext.get("knot_lat") and _ext.get("knot_lon"):
                    _geo_ctx = await build_geo_context(float(_ext["knot_lat"]), float(_ext["knot_lon"]))
                geo_ctx_str = f"\n\n{_geo_ctx}" if _geo_ctx else ""
                await send_message(MY_NUMBER, f"*En 1 hora:* {summary}{loc_str}{geo_ctx_str}")
                fired.append(f"REM60: {summary}")
            elif "[REM:15]" in desc and 14 <= diff_seconds // 60 <= 16:
                loc_str = f"\n📍 {event.get('location')}" if event.get("location") else ""
                _ext = event.get("extendedProperties", {}).get("private", {})
                _geo_ctx = ""
                if _ext.get("knot_lat") and _ext.get("knot_lon"):
                    _geo_ctx = await build_geo_context(float(_ext["knot_lat"]), float(_ext["knot_lon"]))
                geo_ctx_str = f"\n\n{_geo_ctx}" if _geo_ctx else ""
                await send_message(MY_NUMBER, f"*En 15 minutos:* {summary}{loc_str}{geo_ctx_str}")
                fired.append(f"REM15: {summary}")

    return {"ok": True, "fired": fired, "time": now.strftime("%H:%M")}




async def query_servicios_mes(month: str = None) -> str:
    """Devuelve entradas individuales de categoria Recurrente del mes para cruzar con facturas."""
    if not month:
        month = now_argentina().strftime("%Y-%m")
    try:
        entries = await _ds.get_services_summary(month)
        if not entries:
            return f"Sin pagos de Recurrente en {month}."
        lines = [f"Pagos Recurrente {month}:"]
        for e in entries:
            lines.append(f"- {str(e.date) if e.date else ''} -- {e.name}: ${e.value_ars:,.0f}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {str(e)[:80]}"

# ── TAREAS DE FACTURAS ─────────────────────────────────────────────────────────
async def get_pending_factura_tasks() -> list[dict]:
    """Retorna tasks de facturas pendientes (Finanzas + Matrics + no Listo)."""
    try:
        return await _ds.get_pending_factura_tasks()
    except Exception:
        return []

async def create_factura_task(provider: str, amount: float, due_date: str, period: str, finance_page_id: str = None) -> tuple[bool, str]:
    """Crea una task de factura pendiente. Evita duplicados por proveedor + periodo."""
    return await _ds.create_factura_task(provider, amount, due_date, period, finance_page_id=finance_page_id)

async def mark_factura_task_paid(page_id: str) -> bool:
    """Marca una task de factura como Listo."""
    return await _ds.mark_factura_task_paid(page_id)

async def handle_lista_generativa(phone: str, text: str) -> str:
    """Gestiona colecciones del usuario (pelis, libros, lugares, etc.) — auto-crea DB en Notion con schema rico."""
    from notion_datastore import detect_collection_template, COLLECTION_TEMPLATES
    lists = user_prefs.get("generative_lists") or {}
    existing_names = list(lists.keys())
    existing_str = ", ".join(existing_names) if existing_names else "(ninguna todavia)"

    parse = await claude_create(
        model="claude-haiku-4-5-20251001", max_tokens=400,
        system="Extrae intencion sobre colecciones del usuario. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Mensaje: {text}
Colecciones existentes del usuario: {existing_str}

Responde JSON:
{{"action": "add" | "show" | "delete_item" | "create_only",
  "list_name": "nombre canonico de la coleccion (ej: 'pelis', 'libros', 'viajes'). Matchea con existentes si es posible.",
  "count": cuantos items pidio generar (entero), null si menciono items concretos,
  "criteria": "criterio para generar (ej: 'de Tarantino', 'no ficcion'), null si dio items explicitos",
  "items": ["item1", "item2"] si dio items concretos, null si pidio generar,
  "delete_target": "nombre del item a borrar" si action=delete_item, sino null}}"""}]
    )
    raw = parse.content[0].text.strip().strip("`").lstrip("json").strip()
    try:
        intent = json.loads(raw)
    except Exception:
        return "No entendí qué hacer con la lista. Ejemplo: _\"agregá 3 pelis de Tarantino a mi lista de pelis\"_."

    action = intent.get("action") or "add"
    list_name = (intent.get("list_name") or "").strip().lower()
    count = intent.get("count")
    criteria = intent.get("criteria")
    items = intent.get("items") or []

    if not list_name:
        return "¿A qué lista? (ej: _pelis_, _libros_, _viajes_)"

    if action == "show":
        if list_name not in lists:
            return f"No tenés una colección de _{list_name}_ todavía."
        return f"Tu colección de _{list_name}_ está en Notion. Mirala ahí."

    entry = lists.get(list_name)
    list_db_id = entry.get("db_id") if isinstance(entry, dict) else entry  # compat formato viejo

    if not list_db_id:
        template_key = detect_collection_template(list_name)
        tmpl = COLLECTION_TEMPLATES.get(template_key) if template_key else None
        display = tmpl["display"] if tmpl else list_name
        pending_state[phone] = {
            "type": "lista_create_confirm",
            "list_name": list_name,
            "template_key": template_key,
            "pending_action": {"action": action, "count": count, "criteria": criteria, "items": items},
        }
        return f"No tenés una colección de _{display}_ todavía. ¿La creo en Notion? (sí/no)"

    if action == "delete_item":
        return "Borrar items aún no implementado. Hacelo desde Notion por ahora."

    template_key = entry.get("template") if isinstance(entry, dict) else detect_collection_template(list_name)
    return await _execute_lista_add(list_name, list_db_id, count, criteria, items, template_key)


async def _execute_lista_add(list_name: str, db_id: str, count: int | None, criteria: str | None, items: list, template_key: str | None = None) -> str:
    """Inserta items en una colección (los genera con Sonnet si hace falta, con schema del template)."""
    from notion_datastore import COLLECTION_TEMPLATES
    tmpl = COLLECTION_TEMPLATES.get(template_key) if template_key else None
    item_prompt = tmpl["item_prompt"] if tmpl else '[{"name": "...", "notes": "breve nota o null", "tags": ["tag1"]}]'

    if not items and (count or criteria):
        gen = await claude_create(
            model="claude-sonnet-4-20250514", max_tokens=600,
            system=f"Generás items concretos para una colección de '{list_name}'. Responde SOLO JSON array, sin markdown.",
            messages=[{"role": "user", "content": f"Generá {count or 3} items{' ' + criteria if criteria else ''}. Formato: {item_prompt}"}]
        )
        raw = gen.content[0].text.strip().strip("`").lstrip("json").strip()
        try:
            generated = json.loads(raw)
            items = generated if isinstance(generated, list) else []
        except Exception:
            return "No pude generar los items."
    elif items:
        items = [{"name": str(i)} for i in items]

    if not items:
        return "No tengo items para agregar."

    inserted = await _ds.add_items_to_list_db(db_id, items, template_key)
    if not inserted:
        return "No pude agregar nada a la lista."
    bullet = "\n".join(f"• {i.get('name', '')}" for i in items[:inserted])
    return f"✅ Agregué {inserted} a tu colección de _{list_name}_:\n{bullet}"


async def handle_deuda_agent(phone: str, text: str) -> str:
    """Registra una deuda pendiente: crea entrada Impaga en Finanzas + Task."""
    now = now_argentina()
    response = await claude_create(
        model="claude-haiku-4-5-20251001", max_tokens=200,
        system=f"Hoy: {now.strftime('%Y-%m-%d')}. Extrae la deuda del mensaje. Responde SOLO JSON valido: {{\"provider\": \"nombre de la persona o servicio\", \"amount\": monto numerico o null, \"categoria\": \"categoria (ej: Personal, Recurrente, Depto)\", \"notes\": \"detalle si hay\"}}",
        messages=[{"role": "user", "content": text}]
    )
    try:
        data = json.loads(response.content[0].text.strip().strip("`").lstrip("json").strip())
    except Exception:
        return "No entendí la deuda. Decime a quién le debés y cuánto."
    provider = data.get("provider", "")
    amount = float(data.get("amount") or 0)
    categoria = data.get("categoria") or "Personal"
    notes = data.get("notes") or ""
    period = now.strftime("%B %Y")
    if not provider:
        return "No entendí a quién le debés. ¿Podés aclarar?"
    ok, page_id = await _ds.create_finance_invoice(provider, amount, period, category=categoria)
    if ok and notes:
        await _ds._update_page(page_id, {"Notes": {"rich_text": [{"text": {"content": notes}}]}})
    task_ok, _ = await create_factura_task(provider, amount, "", period, finance_page_id=page_id if ok else None)
    if ok or task_ok:
        monto_str = f"${amount:,.0f}" if amount else "monto a confirmar"
        if ok and page_id:
            expires_at = (now_argentina() + timedelta(seconds=60)).replace(tzinfo=None).isoformat()
            pending_state[phone] = {
                "type": "undo_window", "action": "finance_invoice",
                "page_id": page_id, "name": f"Deuda {provider}", "expires_at": expires_at,
            }
        return f"✅ Deuda registrada: *{provider}* — {monto_str}. Te voy a recordar hasta que la marques como pagada.\n\n_Si algo no quedó bien, avisame._"
    return "No pude registrar la deuda. Intenta de nuevo."

async def load_payment_methods():
    """Carga payment methods de Notion a memoria."""
    global payment_methods_cache
    try:
        payment_methods_cache = await _ds.load_payment_methods()
        print(f"[PaymentMethods] Cargados {len(payment_methods_cache)} métodos")
    except Exception as e:
        print(f"[PaymentMethods] Error cargando: {e}")


async def _run_once_migrations():
    """Migraciones puntuales que se ejecutan una vez al arrancar."""
    try:
        n = await _ds.migrate_empty_categories_to_recurrente()
        if n:
            print(f"[Migration] {n} registros sin categoria actualizados a Recurrente")
    except Exception as e:
        print(f"[Migration] Error: {e}")


async def load_geo_reminders():
    """Carga geo-reminders activos de Notion a memoria."""
    global geo_reminders_cache
    try:
        items = await _ds.get_active_geo_reminders()
        geo_reminders_cache = [
            {
                "page_id": r.id, "name": r.name, "type": r.reminder_type,
                "shop_name": r.shop_name or "", "lat": r.lat, "lon": r.lon,
                "radius": r.radius, "recurrent": r.recurrent,
            }
            for r in items
        ]
        print(f"[GeoReminders] Cargados {len(geo_reminders_cache)} reminders activos")
    except Exception as e:
        print(f"[GeoReminders] Error cargando: {e}")

async def create_geo_reminder(description: str, rtype: str, lat: float = None, lon: float = None,
                               shop_name: str = None, radius: int = 20, recurrent: bool = False) -> tuple[bool, str]:
    """Crea un geo-reminder en Notion y lo agrega al cache en memoria."""
    try:
        item = await _ds.create_geo_reminder({
            "name": description, "type": rtype, "lat": lat, "lon": lon,
            "shop_name": shop_name, "radius": radius, "recurrent": recurrent,
        })
        geo_reminders_cache.append({
            "page_id": item.id, "name": description, "type": rtype,
            "shop_name": shop_name or "", "lat": lat, "lon": lon,
            "radius": radius, "recurrent": recurrent,
        })
        return True, item.id
    except Exception as e:
        return False, str(e)[:100]

async def deactivate_geo_reminder(page_id: str):
    """Desactiva un geo-reminder (one-time) despues de dispararse."""
    global geo_reminders_cache
    try:
        await _ds.deactivate_geo_reminder(page_id)
        geo_reminders_cache = [r for r in geo_reminders_cache if r["page_id"] != page_id]
    except Exception:
        pass

async def check_geo_reminders(lat: float, lon: float) -> list[dict]:
    """Devuelve geo-reminders que se dispararon por la ubicacion actual."""
    triggered = []
    now = now_argentina()
    for reminder in geo_reminders_cache:
        if reminder["type"] == "place":
            if reminder.get("lat") and reminder.get("lon"):
                dist_m = haversine_km(lat, lon, reminder["lat"], reminder["lon"]) * 1000
                if dist_m <= reminder.get("radius", 20):
                    triggered.append(reminder)
        elif reminder["type"] == "shop":
            shop_name = reminder.get("shop_name", "")
            if shop_name:
                trigger_radius = reminder.get("radius", 20)
                shops = await search_nearby_shops(lat, lon, radius=500, name_filter=shop_name)
                close_shops = [s for s in shops if s["distance_m"] <= trigger_radius]
                if close_shops:
                    reminder["_matched_shops"] = close_shops[:3]
                    triggered.append(reminder)
    return triggered

# ── ENDPOINT UBICACION (OwnTracks) ────────────────────────────────────────────
_last_proximity_check: dict[str, datetime] = {}
_last_proximity_store: dict[str, str] = {}
_last_location_save: datetime | None = None
_geo_reminder_cooldowns: dict[str, datetime] = {}
GEO_REMINDER_COOLDOWN_SECONDS = 4 * 3600  # 4 horas entre disparos del mismo geo-reminder
_geo_reminders_in_range: set[str] = set()

async def save_location_to_notion(lat: float, lon: float, loc_name: str = None):
    """Persiste la ubicacion en Notion Config para sobrevivir reinicios."""
    page_id = user_prefs.get("_config_page_id")
    await _ds.save_location(page_id, lat, lon, loc_name)

@app.post("/location")
async def receive_location(request: Request):
    """Recibe updates de OwnTracks (o cualquier fuente de ubicacion)."""
    try:
        body = await request.json()
        msg_type = body.get("_type", "location")
        if msg_type != "location":
            return {"ok": True, "ignored": msg_type}

        lat = body.get("lat")
        lon = body.get("lon")
        vel = body.get("vel", 0)
        if lat is None or lon is None:
            return {"ok": False, "error": "missing lat/lon"}

        now = now_argentina()
        current_location["lat"] = float(lat)
        current_location["lon"] = float(lon)
        current_location["velocity"] = float(vel) if vel else 0
        current_location["updated_at"] = now
        current_location["source"] = "owntracks"

        # Reverse geocode para saber nombre del lugar
        loc_name = await reverse_geocode(float(lat), float(lon))
        if loc_name:
            current_location["location_name"] = loc_name

        # Persistir ubicacion en Notion (max cada 5 min)
        global _last_location_save
        if not _last_location_save or (now - _last_location_save).total_seconds() > 300:
            _last_location_save = now
            await save_location_to_notion(float(lat), float(lon), current_location.get("location_name"))

        # Chequear geo-reminders
        if not is_in_transit() and 9 <= now.hour <= 22:
            triggered = await check_geo_reminders(float(lat), float(lon))
            triggered_ids = {r["page_id"] for r in triggered}

            # Detectar cuales salieron del radio y resetearlos
            for r_id in list(_geo_reminders_in_range):
                if r_id not in triggered_ids:
                    _geo_reminders_in_range.discard(r_id)

            for reminder in triggered:
                r_id = reminder["page_id"]
                # No re-disparar si el cooldown no expiró
                last_fired = _geo_reminder_cooldowns.get(r_id)
                if last_fired and (now - last_fired).total_seconds() < GEO_REMINDER_COOLDOWN_SECONDS:
                    _geo_reminders_in_range.add(r_id)
                    continue
                # Solo avisar si acaba de entrar al radio (no estaba antes)
                if r_id in _geo_reminders_in_range:
                    continue
                _geo_reminders_in_range.add(r_id)
                _geo_reminder_cooldowns[r_id] = now
                shops = reminder.get("_matched_shops") or []
                if shops:
                    lines = [f"📍 *{reminder['name']}*", ""]
                    for s in shops:
                        line = f"• *{s['name']}* a {s['distance_m']}m"
                        if s.get("maps_link"):
                            line += f" — {s['maps_link']}"
                        lines.append(line)
                    msg = "\n".join(lines)
                else:
                    msg = f"📍 *{reminder['name']}*"
                if reminder.get("recurrent"):
                    # Recurrente: avisa y listo
                    await send_message(MY_NUMBER, msg)
                    add_to_history(MY_NUMBER, "assistant", msg)
                else:
                    # One-time: avisa + pregunta
                    await send_message(MY_NUMBER, msg)
                    add_to_history(MY_NUMBER, "assistant", msg)
                    pending_state[MY_NUMBER] = {
                        "type": "geo_reminder_fired",
                        "page_id": reminder["page_id"],
                        "name": reminder["name"],
                    }
                    await send_interactive_buttons(
                        MY_NUMBER,
                        "¿Ya lo resolviste?",
                        [
                            {"id": "geo_done", "title": "Ya pasé ✓"},
                            {"id": "geo_keep", "title": "Seguir avisando"},
                        ]
                    )

        # Chequear si hay oportunidad de compra cercana
        phone = MY_NUMBER
        last_check = _last_proximity_check.get(phone)
        should_check = (
            not is_at_known_place()
            and not is_in_transit()
            and (not last_check or (now - last_check).total_seconds() > 600)
            and 9 <= now.hour <= 21
        )

        if should_check:
            _last_proximity_check[phone] = now
            proximity = await check_shopping_proximity()
            if proximity:
                store_type = proximity["store_type"]
                last_store = _last_proximity_store.get(phone)
                today = now.strftime("%Y-%m-%d")
                store_key = f"{today}:{store_type}"
                if last_store != store_key:
                    _last_proximity_store[phone] = store_key
                    items_str = ", ".join(proximity["items"][:5])
                    shop = proximity["shops"][0]
                    shop_detail = f"*{shop['name']}* a {shop['distance_m']}m"
                    if shop.get("address"):
                        shop_detail += f"\n📍 {shop['address']}"
                    if shop.get("opening_hours"):
                        shop_detail += f"\n🕐 {shop['opening_hours']}"
                    shop_detail += f"\n🗺️ {shop['maps_link']}"
                    try:
                        msg_resp = await claude_create(
                            model="claude-sonnet-4-20250514", max_tokens=150,
                            system="Sos Knot. Genera un mensaje breve y natural en espanol rioplatense avisando que el usuario esta cerca de una tienda donde puede comprar cosas que necesita. No seas pesado, se casual y util. Max 2 lineas de texto, sin repetir la info del comercio que ya se muestra aparte.",
                            messages=[{"role": "user", "content": f"El usuario esta cerca de {shop['name']} (a {shop['distance_m']}m). Necesita comprar: {items_str}."}]
                        )
                        msg_text = msg_resp.content[0].text.strip()
                    except Exception:
                        msg_text = f"Estas cerca y te faltan: {items_str}"
                    await send_message(phone, f"{msg_text}\n\n{shop_detail}")

        return {
            "ok": True,
            "lat": lat, "lon": lon,
            "location_name": current_location.get("location_name"),
            "known_place": (is_at_known_place() or {}).get("name"),
            "in_transit": is_in_transit()
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:100]}

@app.post("/location/places")
async def manage_known_places(request: Request):
    """Agregar/listar lugares conocidos. POST con {action: add/list/remove, name, lat, lon, radius}."""
    try:
        body = await request.json()
        action = body.get("action", "list")
        places = user_prefs.get("known_places", [])

        if action == "list":
            return {"ok": True, "places": places}

        elif action == "add":
            name = body.get("name")
            lat = body.get("lat")
            lon = body.get("lon")
            radius = body.get("radius", 200)
            if not name or lat is None or lon is None:
                return {"ok": False, "error": "missing name/lat/lon"}
            places.append({"name": name, "lat": float(lat), "lon": float(lon), "radius": int(radius)})
            user_prefs["known_places"] = places
            await save_user_config(MY_NUMBER)
            return {"ok": True, "added": name, "total": len(places)}

        elif action == "remove":
            name = body.get("name", "").lower()
            user_prefs["known_places"] = [p for p in places if p["name"].lower() != name]
            await save_user_config(MY_NUMBER)
            return {"ok": True, "removed": name}

        return {"ok": False, "error": "unknown action"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:100]}

@app.get("/health")
async def health_check():
    return {"status": "ok", "time": now_argentina().strftime("%H:%M"), "bot": "matrics",
            "location": {"lat": current_location["lat"], "lon": current_location["lon"],
                         "source": current_location["source"],
                         "location_name": current_location.get("location_name"),
                         "known_place": (is_at_known_place() or {}).get("name")}}

# ── MODULO SHOPPING ────────────────────────────────────────────────────────────

SHOPPING_CATEGORIES = ["Frutas y verduras", "Enlatado", "Infusion", "Lacteo", "Especias",
                       "Limpieza", "Panificado", "Herramienta", "Construccion", "Higiene",
                       "Electronica", "Carne", "Galletitas", "Alcohol", "Bebida", "Fiambre",
                       "Grano", "Comida", "Cosmetica"]
SHOPPING_STORES    = ["Super", "Panaderia", "Verduleria", "Dietetica", "Farmacia", "Drogueria", "Ferreteria"]
SHOPPING_FREQUENCY = ["Often", "Monthly", "Annual", "One time"]

async def get_ingredients_and_enrich(recipe_name: str, recipe_text: str = None) -> tuple[list[dict], bool]:
    if recipe_text:
        context = f'Receta: "{recipe_name}"\nTexto completo de la receta:\n{recipe_text[:2000]}\n\nExtrae TODOS los ingredientes que aparecen en el texto de la receta.'
    else:
        context = f'Receta: "{recipe_name}"\n\nInferi los ingredientes tipicos/estandar completos de esta receta.'
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=800,
        system="Responde SOLO JSON valido sin markdown ni texto extra.",
        messages=[{"role": "user", "content": f"""{context}

Responde SOLO este array JSON:
[{{
  "name": "nombre del ingrediente capitalizado SIN cantidad",
  "display": "cantidad + nombre como aparece en la receta",
  "emoji": "emoji especifico del producto",
  "category": una de {SHOPPING_CATEGORIES},
  "store": tienda mas logica,
  "frequency": uno de {SHOPPING_FREQUENCY}
}}]"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        items = json.loads(raw)
        return items, True
    except Exception:
        return [], False

async def enrich_items_with_claude(items: list[str]) -> list[dict]:
    if not items:
        return []
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=1500,
        system="Enriquece una lista de items. Responde SOLO JSON valido sin markdown.",
        messages=[{"role": "user", "content": f"""Items: {json.dumps(items, ensure_ascii=False)}

Para cada item responde un array con:
- "name": nombre capitalizado
- "emoji": emoji especifico (nunca 🛒)
- "category": una de {SHOPPING_CATEGORIES}
- "store": tienda mas logica
- "frequency": uno de {SHOPPING_FREQUENCY}

Responde SOLO el array JSON."""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        return json.loads(raw)
    except Exception:
        return [{"name": i.capitalize(), "emoji": "🛒", "category": "", "store": "", "frequency": "One time"} for i in items]

async def search_recipe_in_notion(recipe_name: str) -> list[str] | None:
    try:
        return await _ds.get_recipe_ingredients(recipe_name)
    except Exception:
        return None

def _parse_bold(text: str) -> list:
    parts = []
    remaining = text
    while "**" in remaining:
        idx = remaining.find("**")
        if idx > 0:
            parts.append({"type": "text", "text": {"content": remaining[:idx]}})
        remaining = remaining[idx+2:]
        end = remaining.find("**")
        if end == -1:
            parts.append({"type": "text", "text": {"content": "**" + remaining}})
            remaining = ""
            break
        parts.append({"type": "text", "text": {"content": remaining[:end]}, "annotations": {"bold": True}})
        remaining = remaining[end+2:]
    if remaining:
        parts.append({"type": "text", "text": {"content": remaining}})
    return parts if parts else [{"type": "text", "text": {"content": text}}]

async def save_recipe_to_notion(recipe_name: str, source: str = "Knot", ingredient_names: list[str] = None, recipe_text: str = None) -> tuple[bool, str]:
    try:
        try:
            props_response = await claude_create(
                model="claude-sonnet-4-20250514", max_tokens=200,
                system="Responde SOLO JSON valido sin markdown.",
                messages=[{"role": "user", "content": f'''Receta: "{recipe_name}"
Texto: {(recipe_text or "")[:500]}
Responde SOLO este JSON:
{{"difficult": "Easy"|"Moderate"|"Hard"|null,
  "type": ["Postre"|"Cena"|"Almuerzo"|"Desayuno"|"Snack"|"Cosmetica"],
  "coccion": "Horno"|"Sarten"|"Pochar"|"Frizzer "|"Varias prep."|null,
  "healthy": "Healthy"|"Fatty"|"ni healthy ni fatty"|null}}'''}]
            )
            raw_meta = props_response.content[0].text.strip().strip("`").lstrip("json").strip()
            meta = json.loads(raw_meta)
        except Exception:
            meta = {}

        relation_ids = []
        if ingredient_names:
            items_list = ingredient_names if (isinstance(ingredient_names, list) and ingredient_names and isinstance(ingredient_names[0], dict)) else [{"name": n} for n in ingredient_names]
            for ing_item in items_list:
                ing_name = ing_item.get("name", "").strip()
                if not ing_name:
                    continue
                results = await _ds.search_shopping_item(ing_name)
                if results:
                    relation_ids.append({"id": results[0].id})
                else:
                    try:
                        new_item = await _ds.add_shopping_item({
                            "name": ing_name,
                            "emoji": ing_item.get("emoji", "🛒"),
                            "category": ing_item.get("category", ""),
                            "store": ing_item.get("store", ""),
                            "frequency": ing_item.get("frequency", "One time"),
                        })
                        relation_ids.append({"id": new_item.id})
                    except Exception as e:
                        return False, f"Error creando ingrediente '{ing_name}': {str(e)[:100]}"

        content_blocks = None
        if recipe_text:
            try:
                fmt_resp = await claude_create(
                    model="claude-sonnet-4-20250514", max_tokens=1500,
                    system="Formatea la siguiente receta para guardarla en Notion. Usa este formato:\n- Titulo de seccion como ## (Ingredientes, Procedimiento, Notas)\n- Listas con - para ingredientes y pasos numerados con 1. 2. 3.\n- **negrita** para cantidades importantes\n- Responde SOLO el texto formateado, sin comentarios adicionales.",
                    messages=[{"role": "user", "content": f"Receta: {recipe_name}\n\nTexto original:\n{recipe_text[:3000]}"}]
                )
                formatted = fmt_resp.content[0].text.strip()
            except Exception:
                formatted = recipe_text

            content_blocks = []
            for line in formatted.split("\n"):
                line_stripped = line.strip()
                if not line_stripped:
                    continue
                if line_stripped.startswith("## "):
                    content_blocks.append({"object": "block", "type": "heading_2",
                        "heading_2": {"rich_text": [{"type": "text", "text": {"content": line_stripped[3:]}}]}})
                elif line_stripped.startswith("# "):
                    content_blocks.append({"object": "block", "type": "heading_1",
                        "heading_1": {"rich_text": [{"type": "text", "text": {"content": line_stripped[2:]}}]}})
                elif line_stripped.startswith("- "):
                    content = line_stripped[2:]
                    rich = _parse_bold(content)
                    content_blocks.append({"object": "block", "type": "bulleted_list_item",
                        "bulleted_list_item": {"rich_text": rich}})
                elif line_stripped[:2] in [f"{i}." for i in range(1, 30)] or (len(line_stripped) > 2 and line_stripped[0].isdigit() and line_stripped[1] == "."):
                    content = line_stripped.split(".", 1)[-1].strip()
                    rich = _parse_bold(content)
                    content_blocks.append({"object": "block", "type": "numbered_list_item",
                        "numbered_list_item": {"rich_text": rich}})
                else:
                    rich = _parse_bold(line_stripped)
                    content_blocks.append({"object": "block", "type": "paragraph",
                        "paragraph": {"rich_text": rich}})
            if not content_blocks:
                content_blocks = None

        await _ds.create_recipe(
            data={
                "name": recipe_name, "source": source,
                "difficulty": meta.get("difficult"),
                "type": meta.get("type"),
                "cooking_method": meta.get("coccion"),
                "healthy": meta.get("healthy"),
            },
            ingredient_relation_ids=[r["id"] for r in relation_ids],
            content_blocks=content_blocks[:100] if content_blocks else None,
        )
        return True, ""
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        return False, f"Excepcion: {str(e) or repr(e)} | {tb[-200:]}"

async def parse_shopping_intent(text: str) -> dict:
    safe_text = text.replace('"', "'").replace('\r', ' ').replace('\n', ' ')[:2000]
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=800,
        system="Analiza mensajes sobre lista de compras. Responde SOLO JSON valido sin markdown.",
        messages=[{"role": "user", "content": f"""Mensaje: {safe_text}

Responde:
{{"action": "out_of_stock" (necesito comprarlo, me falta) | "in_stock" (ya lo compre, ya lo tengo) | "add" (agregar a la lista) | "list" (ver la lista),
  "items": ["item1", "item2"],
  "recipe_name": "nombre de la receta o null",
  "is_recipe_request": true/false,
  "recipe_ingredients": ["ingrediente1", "ingrediente2", ...]}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)

GENERIC_WORDS = {"salsa", "crema", "pasta", "sopa", "caldo", "jugo",
                  "queso", "pan", "leche", "aceite", "harina", "arroz"}


async def handle_shopping(text: str, phone: str = None) -> str:
    try:
        intent = await parse_shopping_intent(text)
    except Exception as e:
        return f"No pude interpretar el mensaje: {str(e)[:100]}"

    action      = intent.get("action")
    items       = intent.get("items", [])
    is_recipe   = intent.get("is_recipe_request", False)
    recipe_name = intent.get("recipe_name")
    recipe_ingredients_raw = intent.get("recipe_ingredients", [])
    recipe_note = ""

    if action == "add" and is_recipe and recipe_name:
        notion_ingredients = await search_recipe_in_notion(recipe_name)
        if notion_ingredients:
            if phone:
                enriched = await enrich_items_with_claude(notion_ingredients)
                ing_list = "\n".join(f"- {i.get('emoji','🛒')} {i.get('name','')}" for i in enriched)
                pending_state[phone] = {
                    "type": "recipe_ingredients",
                    "recipe_name": recipe_name,
                    "ingredients": enriched
                }
                await send_interactive_buttons(
                    phone,
                    f"Receta encontrada en tus recetas.\n\nIngredientes:\n{ing_list}\n\nLos agregas a la lista de compras?",
                    [
                        {"id": "recipe_add_yes", "title": "Si, agregar"},
                        {"id": "recipe_add_no",  "title": "No por ahora"},
                    ]
                )
                return f"*{recipe_name.capitalize()}* encontrada en tus recetas"
            else:
                items = notion_ingredients
                recipe_note = f"*{recipe_name.capitalize()}* (de tus recetas)\n"
        else:
            try:
                if recipe_ingredients_raw:
                    enriched_direct = await enrich_items_with_claude(recipe_ingredients_raw)
                    ok = True
                else:
                    enriched_direct, ok = await get_ingredients_and_enrich(recipe_name, recipe_text=text)
            except Exception:
                enriched_direct, ok = [], False
            if ok and enriched_direct:
                if phone:
                    pending_state[phone] = {
                        "type": "recipe_review",
                        "recipe_name": recipe_name,
                        "recipe_text": text,
                        "ingredients": enriched_direct,
                    }
                    ing_list_display = "\n".join(
                        f"- {i.get('emoji','🛒')} {i.get('display') or i.get('name','')}"
                        for i in enriched_direct
                    )
                    await send_message(
                        phone,
                        f"*{recipe_name.capitalize()}*\n\n*Ingredientes:*\n{ing_list_display}"
                    )
                    if text and len(text) > 100:
                        try:
                            proc_resp = await claude_create(
                                model="claude-sonnet-4-20250514", max_tokens=600,
                                system="Extrae SOLO la seccion de preparacion/procedimiento de la receta. Sin titulo, sin lista de ingredientes. Solo los pasos de preparacion en texto limpio.",
                                messages=[{"role": "user", "content": text[:2000]}]
                            )
                            proc_text = proc_resp.content[0].text.strip()
                        except Exception:
                            proc_text = text[:600]
                        await send_message(phone, f"*Preparacion:*\n{proc_text}")
                    await send_interactive_buttons(
                        phone,
                        "Esta todo bien o queres corregir algo?",
                        [
                            {"id": "recipe_ok",      "title": "Esta bien"},
                            {"id": "recipe_correct", "title": "Quiero corregir"},
                        ]
                    )
                    return None
                else:
                    return f"*{recipe_name.capitalize()}* -- {len(enriched_direct)} ingredientes detectados."
            else:
                items = []
                recipe_note = f"No pude inferir los ingredientes para esa receta\n"

    if action == "list":
        try:
            items_list = await _ds.get_shopping_list(only_missing=True)
        except Exception as e:
            return f"No pude leer la lista: {str(e)[:100]}"
        if not items_list:
            return "No te falta nada! La lista esta vacia."
        lines = ["*Tu lista de compras:*\n"]
        for item in items_list:
            qty_str = f" _({item.notes})_" if item.notes else ""
            lines.append(f"- {item.name}{qty_str}{f' _{item.category}_' if item.category else ''}")
        return "\n".join(lines)

    if not items or (len(items) == 1 and items[0].lower() in ["todo", "all", "todos", "everything"]):
        if action in ("in_stock", "out_of_stock"):
            pending = await _ds.get_shopping_list(only_missing=True)
            in_stock_val = action == "in_stock"
            await asyncio.gather(*[_ds.update_shopping_item(it.id, {"in_stock": in_stock_val}) for it in pending])
            return f"Listo, {len(pending)} items marcados como {'en stock' if in_stock_val else 'faltantes'}."
        return "No entendi que producto queres actualizar."

    if action == "add":
        try:
            enriched = await enrich_items_with_claude(items)
        except Exception:
            enriched = [{"name": i.capitalize(), "emoji": "🛒", "category": "", "store": "", "frequency": "One time"} for i in items]
        results_text = []
        for item in enriched:
            item_name = item.get("name", "")
            existing = await _ds.search_shopping_item(item_name)
            if existing:
                await _ds.update_shopping_item(existing[0].id, {"in_stock": False})
                results_text.append(f"{item.get('emoji','🛒')} _{item_name}_ ya estaba, aparece como faltante")
            else:
                try:
                    await _ds.add_shopping_item(item)
                    results_text.append(f"{item.get('emoji','🛒')} _{item_name}_ agregado")
                except Exception as e:
                    results_text.append(f"Error agregando _{item_name}_: {str(e)[:50]}")
        # Actualizar perfil de supermercado en background
        added_names = [i.get("name", "") for i in enriched if i.get("name")]
        if added_names:
            asyncio.create_task(update_domain_profile_bg(
                "supermercado",
                f"Agregó a la lista de compras: {', '.join(added_names)}"
            ))
            supermercado_profile = get_domain_profile("supermercado")
            if supermercado_profile and phone:
                asyncio.create_task(check_and_notify_deviation(phone, added_names, supermercado_profile))
        return recipe_note + "\n".join(results_text) + "\n\nLista actualizada en Notion"

    results_text = []
    for item_name in items:
        display  = item_name.capitalize()
        in_stock = action == "in_stock"
        existing = await _ds.search_shopping_item(item_name)
        if existing:
            await _ds.update_shopping_item(existing[0].id, {"in_stock": in_stock})
            results_text.append(f"_{display}_ marcado como en stock" if in_stock else f"_{display}_ agregado a la lista")
            # Trackear compras: si in_stock significa que lo compró
            if in_stock:
                item_key = item_name.lower().strip()
                counts = user_prefs.setdefault("purchase_counts", {})
                counts[item_key] = counts.get(item_key, 0) + 1
                asyncio.create_task(save_purchase_counts_direct())
                if counts[item_key] >= 3:
                    asyncio.create_task(update_domain_profile_bg(
                        "supermercado",
                        f"Compra frecuente confirmada: '{item_name}' ({counts[item_key]} veces marcado como comprado)"
                    ))
                # Trigger #3: 5+ compras del mismo item → ofrecer marcar como Monthly
                if counts[item_key] >= 5 and phone:
                    try:
                        existing_freq = getattr(existing[0], "frequency", "") or ""
                        if "monthly" not in existing_freq.lower() and "often" not in existing_freq.lower():
                            emit_hint(phone, suggestion_gate.Hint(
                                trigger_id=f"recurring_shopping_{item_key[:20]}",
                                message=f"🛒 Compraste *{display}* {counts[item_key]} veces. ¿Querés que la marque como recurrente (Monthly) para que siempre quede en tu lista? Decime *si* o *no*.",
                                action_intent="set_shopping_recurring",
                                payload={"item_id": existing[0].id, "name": item_name},
                            ))
                    except Exception:
                        pass
        else:
            if not in_stock:
                try:
                    enriched = await enrich_items_with_claude([item_name])
                    item_data = enriched[0] if enriched else {"name": display, "emoji": "🛒", "category": "", "store": "", "frequency": "One time"}
                except Exception:
                    item_data = {"name": display, "emoji": "🛒", "category": "", "store": "", "frequency": "One time"}
                try:
                    await _ds.add_shopping_item(item_data)
                    results_text.append(f"{item_data.get('emoji','🛒')} _{display}_ agregado como faltante")
                except Exception:
                    results_text.append(f"Error agregando _{display}_")
            else:
                results_text.append(f"_{display}_ no esta en la lista")

    return "\n".join(results_text) + "\n\nLista actualizada en Notion"
