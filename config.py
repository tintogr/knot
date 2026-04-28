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
        if cfg.cards:
            user_prefs["cards"] = cfg.cards
        if cfg.banks:
            user_prefs["banks"] = cfg.banks
        if cfg.payment_modalities:
            user_prefs["payment_modalities"] = cfg.payment_modalities
        if cfg.domain_profiles:
            user_prefs.setdefault("domain_profiles", {}).update(cfg.domain_profiles)
        user_prefs["_config_page_id"] = page_id
        if cfg.saved_lat is not None and cfg.saved_lon is not None:
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
    if "payment_methods" not in user_prefs:
        user_prefs["payment_methods"] = ["BBVA", "Mercado Pago", "Efectivo", "Transferencia", "Débito", "Crédito", "Contado"]


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
            cards=user_prefs.get("cards", []),
            banks=user_prefs.get("banks", []),
            payment_modalities=user_prefs.get("payment_modalities", []),
            known_shops=user_prefs.get("known_shops", {}),
            feature_hints=user_prefs.get("feature_hints", {}),
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
  "remove_extra": texto de instruccion a quitar del Resumen Diario, o null,
  "add_card": {{"bank": "banco (ej: BBVA)", "type": "Debit o Credit", "last4": "ultimos 4 digitos o null", "owner": "de quien es o null"}} o null,
  "remove_card": "texto parcial del banco/tipo a quitar" o null,
  "add_bank": "nombre del banco a agregar" o null,
  "remove_bank": "nombre del banco a quitar" o null,
  "add_modality": "modalidad de pago a agregar (ej: Debit, Credit, Cash, Transfer)" o null,
  "remove_modality": "modalidad a quitar" o null}}"""}]
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
    add_card = data.get("add_card")
    remove_card = data.get("remove_card")
    add_bank = data.get("add_bank")
    remove_bank = data.get("remove_bank")
    add_modality = data.get("add_modality")
    remove_modality = data.get("remove_modality")

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

    if add_bank:
        banks = user_prefs.get("banks") or []
        if add_bank not in banks:
            banks.append(add_bank)
            user_prefs["banks"] = banks
        changed.append(f"Banco agregado: *{add_bank}*")

    if remove_bank:
        banks = user_prefs.get("banks") or []
        user_prefs["banks"] = [b for b in banks if remove_bank.lower() not in b.lower()]
        changed.append(f"Banco removido: _{remove_bank}_")

    if add_modality:
        modalities = user_prefs.get("payment_modalities") or []
        if add_modality not in modalities:
            modalities.append(add_modality)
            user_prefs["payment_modalities"] = modalities
        changed.append(f"Modalidad agregada: *{add_modality}*")

    if remove_modality:
        modalities = user_prefs.get("payment_modalities") or []
        user_prefs["payment_modalities"] = [m for m in modalities if remove_modality.lower() not in m.lower()]
        changed.append(f"Modalidad removida: _{remove_modality}_")

    if add_card and isinstance(add_card, dict) and (add_card.get("bank") or add_card.get("label")):
        cards = user_prefs.get("cards") or []
        bank = add_card.get("bank", "").strip()
        ctype = add_card.get("type", "").strip()
        last4 = str(add_card.get("last4") or "").strip() or None
        owner = add_card.get("owner") or None
        label = add_card.get("label") or f"{bank} {ctype}".strip()
        existing = next((c for c in cards if c.get("last4") == last4 and last4) or
                        (c for c in cards if c.get("bank", "").lower() == bank.lower() and c.get("type", "").lower() == ctype.lower()), None)
        if existing:
            if bank: existing["bank"] = bank
            if ctype: existing["type"] = ctype
            if last4: existing["last4"] = last4
            if owner: existing["owner"] = owner
        else:
            cards.append({"bank": bank, "type": ctype, "last4": last4, "owner": owner})
            # Also ensure the bank is in the banks list
            banks = user_prefs.get("banks") or []
            if bank and bank not in banks:
                banks.append(bank)
                user_prefs["banks"] = banks
        user_prefs["cards"] = cards
        suffix = f" (****{last4})" if last4 else ""
        owner_str = f" — de {owner}" if owner else ""
        changed.append(f"Tarjeta agregada: *{label}{suffix}*{owner_str}")

    if remove_card:
        cards = user_prefs.get("cards") or []
        user_prefs["cards"] = [c for c in cards if remove_card.lower() not in c.get("label", "").lower()]
        changed.append(f"Tarjeta removida: _{remove_card}_")

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
    cards_actuales = user_prefs.get("cards") or []
    banks_actuales = user_prefs.get("banks") or []
    modalities_actuales = user_prefs.get("payment_modalities") or []
    hora_actual = user_prefs.get("daily_summary_hour") or DAILY_SUMMARY_HOUR
    mins_actual = user_prefs.get("daily_summary_minute") or 0
    estado = f"Actualmente el Resumen Diario llega a las *{hora_actual:02d}:{mins_actual:02d}*"
    if extras_actuales:
        estado += f" e incluye: {', '.join(extras_actuales)}"
    else:
        estado += " sin extras configurados"
    if banks_actuales:
        estado += f"\nBancos: {', '.join(banks_actuales)}"
    if modalities_actuales:
        estado += f"\nModalidades de pago: {', '.join(modalities_actuales)}"
    if cards_actuales:
        def _card_display(c):
            label = c.get("label") or f"{c.get('bank','')} {c.get('type','')}".strip()
            suffix = f" (****{c['last4']})" if c.get("last4") else ""
            owner_str = f" — de {c['owner']}" if c.get("owner") else ""
            return f"{label}{suffix}{owner_str}"
        estado += f"\nTarjetas: {', '.join(_card_display(c) for c in cards_actuales)}"
    return f"Dale! Que queres modificar?\n\n{estado}\n\nPodes agregar bancos (\"agregá BBVA\"), modalidades (\"agregá Debit\"), tarjetas (\"agregá BBVA Debit terminada en 1234 de Martín\"), o cambiar el horario del resumen."
