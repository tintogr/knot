import json
from notion_datastore import UserConfig
from state import (
    _ds, user_prefs, current_location, _last_summary_sent,
    MY_NUMBER, DAILY_SUMMARY_HOUR, claude_create,
)


async def load_user_config(wa_number: str):
    try:
        cfg, page_id = await _ds.load_config(wa_number)
        if not page_id:
            return
        if cfg.daily_summary_hour is not None:
            user_prefs["daily_summary_hour"]   = cfg.daily_summary_hour
        if cfg.daily_summary_minute is not None:
            user_prefs["daily_summary_minute"] = cfg.daily_summary_minute
        user_prefs["resumen_nocturno_hour"]    = cfg.resumen_nocturno_hour
        user_prefs["resumen_nocturno_enabled"] = cfg.resumen_nocturno_enabled
        user_prefs["resumen_semanal_enabled"]  = cfg.resumen_semanal_enabled
        user_prefs["resumen_semanal_hour"]     = cfg.resumen_semanal_hour
        if cfg.greeting_name:
            user_prefs["greeting_name"] = cfg.greeting_name
        if cfg.resumen_extras:
            user_prefs["resumen_extras"] = cfg.resumen_extras
        if cfg.news_topics:
            user_prefs["news_topics"] = cfg.news_topics
        if cfg.service_providers:
            user_prefs["service_providers"] = cfg.service_providers
        if cfg.known_places:
            user_prefs["known_places"] = cfg.known_places
        if cfg.activities:
            user_prefs["activities"] = cfg.activities
        if cfg.purchase_counts:
            user_prefs["purchase_counts"] = cfg.purchase_counts
        if cfg.known_shops:
            user_prefs["known_shops"] = cfg.known_shops
        if cfg.feature_hints:
            user_prefs["feature_hints"] = cfg.feature_hints
        if cfg.generative_lists:
            user_prefs["generative_lists"] = cfg.generative_lists
        if cfg.pending_invoice_confirmations:
            user_prefs["pending_invoice_confirmations"] = cfg.pending_invoice_confirmations
        if cfg.domain_profiles:
            user_prefs.setdefault("domain_profiles", {}).update(cfg.domain_profiles)
        user_prefs["_config_page_id"] = page_id
        if cfg.saved_lat is not None and cfg.saved_lon is not None:
            # Siempre guardar en user_prefs como fallback
            user_prefs["saved_lat"] = float(cfg.saved_lat)
            user_prefs["saved_lon"] = float(cfg.saved_lon)
            if current_location.get("source") in ("default", "env", "unknown"):
                current_location["lat"] = float(cfg.saved_lat)
                current_location["lon"] = float(cfg.saved_lon)
                current_location["source"] = "restored"
                if cfg.saved_city:
                    current_location["location_name"] = cfg.saved_city
        if cfg.last_summary_date:
            user_prefs["_last_summary_date"] = cfg.last_summary_date
            from datetime import date
            if not _last_summary_sent.get("daily"):
                try:
                    if cfg.last_summary_date == date.today().isoformat():
                        from datetime import datetime
                        _last_summary_sent["daily"] = datetime.now()
                except Exception:
                    pass
    except Exception:
        pass


async def save_user_config(wa_number: str):
    try:
        if not user_prefs.get("_config_page_id"):
            await load_user_config(wa_number)
        page_id = user_prefs.get("_config_page_id")
        if not page_id:
            return
        cfg = UserConfig(
            phone=wa_number,
            greeting_name=user_prefs.get("greeting_name"),
            daily_summary_hour=user_prefs.get("daily_summary_hour"),
            daily_summary_minute=user_prefs.get("daily_summary_minute"),
            resumen_nocturno_enabled=user_prefs.get("resumen_nocturno_enabled", True),
            resumen_nocturno_hour=user_prefs.get("resumen_nocturno_hour", 22),
            resumen_semanal_enabled=user_prefs.get("resumen_semanal_enabled", True),
            resumen_semanal_hour=user_prefs.get("resumen_semanal_hour", 21),
            resumen_extras=user_prefs.get("resumen_extras", []),
            news_topics=user_prefs.get("news_topics", []),
            service_providers=user_prefs.get("service_providers", {}),
            known_places=user_prefs.get("known_places", []),
            activities=user_prefs.get("activities", {}),
            domain_profiles=user_prefs.get("domain_profiles", {}),
            purchase_counts=user_prefs.get("purchase_counts", {}),
            known_shops=user_prefs.get("known_shops", {}),
            feature_hints=user_prefs.get("feature_hints", {}),
            generative_lists=user_prefs.get("generative_lists", {}),
            pending_invoice_confirmations=user_prefs.get("pending_invoice_confirmations", []),
        )
        await _ds.save_config(page_id, cfg)
    except Exception:
        pass


async def handle_configurar(text: str) -> str:
    response = await claude_create(
        model="claude-sonnet-4-20250514", max_tokens=300,
        system="Extrae que configuracion cambiar. Responde SOLO JSON.",
        messages=[{"role": "user", "content": f"""Mensaje: {text}
Responde:
{{"setting": "daily_summary_hour",
  "hour": hora en formato 24h como entero. null si no hay horario,
  "minute": minutos como entero. si no se mencionan usa 0,
  "greeting_name": nuevo nombre del saludo matutino o null,
  "add_extra": instruccion nueva para agregar al Resumen Diario, o null,
  "remove_extra": texto de instruccion a quitar del Resumen Diario, o null}}"""}]
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        data = json.loads(raw)
    except Exception:
        return "No entendi que configuracion queres cambiar"

    setting = data.get("setting")
    hour    = data.get("hour")
    minute  = data.get("minute", 0) or 0
    greeting_name = data.get("greeting_name")
    add_extra  = data.get("add_extra")
    remove_extra = data.get("remove_extra")

    changed = []

    if greeting_name:
        user_prefs["greeting_name"] = greeting_name
        changed.append(f"Saludo del Resumen Diario -> *{greeting_name}*")

    if add_extra:
        extras = user_prefs.get("resumen_extras", [])
        if add_extra not in extras:
            extras.append(add_extra)
            user_prefs["resumen_extras"] = extras
        changed.append(f"Extra agregado: _{add_extra}_")

    if remove_extra:
        extras = user_prefs.get("resumen_extras", [])
        user_prefs["resumen_extras"] = [e for e in extras if remove_extra.lower() not in e.lower()]
        changed.append(f"Extra removido: _{remove_extra}_")

    if setting == "daily_summary_hour" and hour is not None:
        try:
            hora = int(hour)
            mins = int(minute)
            if not 0 <= hora <= 23:
                return "El horario tiene que estar entre 0 y 23"
            if not 0 <= mins <= 59:
                mins = 0
            user_prefs["daily_summary_hour"]   = hora
            user_prefs["daily_summary_minute"] = mins
            hora_fmt = f"{hora:02d}:{mins:02d}"
            changed.append(f"Horario del resumen -> *{hora_fmt}*")
            # Si el nuevo horario es futuro (todavia no llego hoy), resetear el flag
            # para permitir el envio en este cambio (no esperar al dia siguiente)
            from datetime import datetime as _dt
            now_ar = _dt.now()
            target_min = hora * 60 + mins
            curr_min = now_ar.hour * 60 + now_ar.minute
            if target_min > curr_min:
                user_prefs.pop("_last_summary_date", None)
                _last_summary_sent.pop("daily", None)
                changed.append("_(reseteo: si el nuevo horario es futuro, te llega hoy)_")
        except Exception:
            return "No pude interpretar el horario"

    if changed:
        await save_user_config(MY_NUMBER)
        return "Listo:\n" + "\n".join(changed)

    extras_actuales = user_prefs.get("resumen_extras", [])
    hora_actual = user_prefs.get("daily_summary_hour") or DAILY_SUMMARY_HOUR
    mins_actual = user_prefs.get("daily_summary_minute") or 0
    estado = f"Actualmente el Resumen Diario llega a las *{hora_actual:02d}:{mins_actual:02d}*"
    if extras_actuales:
        estado += f" e incluye: {', '.join(extras_actuales)}"
    else:
        estado += " sin extras configurados"
    return f"Dale! Que queres modificar?\n\n{estado}\n\nPodes cambiar el horario del resumen, el saludo, o agregar/quitar extras. Para tarjetas y métodos de pago usá el comando aparte."
