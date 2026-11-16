"""
Corre el banco de conversaciones contra el modelo REAL.

    python -m banco.correr                 # todos los casos
    python -m banco.correr vianda super    # los que tengan alguna de esas palabras en el nombre
    python -m banco.correr --lista         # solo listar los casos

Necesita ANTHROPIC_API_KEY (la clave de Anthropic). Cada corrida completa cuesta unos
centavos de dólar: por eso no corre en cada push, sino a pedido. Nada de lo que hace toca
Notion, WhatsApp ni el calendario reales: ver banco/mundo.py.

Termina con código 1 si algún caso falla.
"""
import asyncio
import os
import sys
import time

# Credenciales falsas para poder importar a Knot. La única real es la de Anthropic.
os.environ.setdefault("NOTION_TOKEN", "banco")
os.environ.setdefault("NOTION_DATABASE_ID", "0" * 32)
os.environ.setdefault("WHATSAPP_TOKEN", "banco")
os.environ.setdefault("WHATSAPP_PHONE_ID", "1")
os.environ.setdefault("ANTHROPIC_API_KEY", "falta")
os.environ["MY_WA_NUMBER"] = "54299151234567"       # número inventado
for _secreta in ("GCAL_REFRESH_TOKEN", "GCAL_CLIENT_ID", "GCAL_CLIENT_SECRET", "GROQ_API_KEY", "GOOGLE_PLACES_KEY"):
    os.environ.pop(_secreta, None)                  # que nada salga a Google por accidente

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main            # noqa: E402
import areas           # noqa: E402
import config          # noqa: E402
import finanzas        # noqa: E402
import state           # noqa: E402
import summaries       # noqa: E402
from banco.casos import CASOS      # noqa: E402
from banco.mundo import Mundo, base  # noqa: E402

TELEFONO = state.MY_NUMBER


def instalar(mundo: Mundo) -> dict:
    """Reemplaza todo lo que sale de Knot hacia afuera por el mundo falso. Devuelve lo
    que había, para poder restaurarlo."""
    originales = {}

    def poner(modulo, nombre, valor):
        if hasattr(modulo, nombre):
            originales[(modulo, nombre)] = getattr(modulo, nombre)
            setattr(modulo, nombre, valor)

    # La base de datos
    for modulo in (main, areas, finanzas, summaries, config, state):
        poner(modulo, "_ds", mundo.ds)

    # WhatsApp: lo que Knot "manda" queda anotado (y en la conversación, como el real)
    async def enviar(to, text, descartable=False):
        if not text.startswith("⏳"):
            mundo.enviados.append(text)
            state.add_to_history(to, "assistant", text)
        return True

    async def enviar_botones(to, body, buttons, header=None):
        opciones = [b["title"] for b in buttons[:3]]
        mundo.botones.append({"texto": body, "opciones": opciones, "ids": [b["id"] for b in buttons[:3]]})
        state.add_to_history(to, "assistant", f"{body}\n[opciones: {' | '.join(opciones)}]")
        return True

    async def nada(*a, **k):
        return None

    async def falso(*a, **k):
        return False

    for modulo in (main, summaries):
        poner(modulo, "send_message", enviar)
        poner(modulo, "send_interactive_buttons", enviar_botones)
    poner(main, "send_reaction", nada)

    # Configuración: no se lee ni se guarda en ningún lado
    for modulo in (main, config, summaries):
        poner(modulo, "load_user_config", nada)
        poner(modulo, "save_user_config", nada)
    poner(main, "_guardar_conversacion_en_segundo_plano", lambda: None)

    # Calendario
    async def crear_recordatorio(data):
        if not data.get("fire_at"):
            return False, "Sin fecha para el recordatorio"
        mundo.recordatorios.append(dict(data))
        return True, f"rec{len(mundo.recordatorios)}"

    async def crear_evento(data):
        mundo.eventos.append(dict(data))
        return True, f"ev{len(mundo.eventos)}"

    async def agenda(days_ahead=2, days_back=0):
        return mundo.agenda

    async def agenda_del_dia(fecha):
        dia = f"{fecha[8:10]}/{fecha[5:7]}"
        lineas = [x for x in mundo.agenda.splitlines() if dia in x]
        lineas += [f"- {dia} {e.get('time') or ''} -- {e.get('summary')}" for e in mundo.eventos if e.get("date") == fecha]
        return "\n".join(lineas) or "No hay eventos ese dia."

    async def sin_evento(*a, **k):
        return None, "No encontre ningun evento con ese nombre."

    async def sin_token():
        return None

    poner(main, "create_recordatorio", crear_recordatorio)
    poner(main, "create_evento_gcal", crear_evento)
    poner(main, "query_calendar", agenda)
    poner(main, "query_calendar_date", agenda_del_dia)
    poner(main, "_find_calendar_event", sin_evento)
    poner(main, "_borrar_evento", falso)
    for modulo in (main, summaries):
        poner(modulo, "get_gcal_access_token", sin_token)

    # Lo accesorio: sin clima, sin sugerencias, sin perfiles, dólar fijo
    async def dolar():
        return 1400.0

    poner(main, "get_exchange_rate", dolar)
    poner(main, "get_weather", nada)
    poner(main, "maybe_fire_hint", falso)
    poner(main, "update_domain_profile_bg", nada)
    poner(main, "lookup_business_type", nada)
    poner(main, "_check_and_confirm_shop", nada)
    poner(main, "_delayed_indicator", nada)

    # Para saber a qué módulo se mandó cada mensaje
    decidir_real = main._decidir_ruta

    async def decidir(*a, **k):
        tipo, texto, directa = await decidir_real(*a, **k)
        mundo.rutas.append(tipo if tipo else "(contestó solo)")
        return tipo, texto, directa

    poner(main, "_decidir_ruta", decidir)
    return originales


def restaurar(originales: dict):
    for (modulo, nombre), valor in originales.items():
        setattr(modulo, nombre, valor)


def _limpiar_estado(mundo: Mundo, caso: dict):
    state.chat_history.clear()
    dict.clear(state.pending_state)
    state.message_buffer.clear()
    main._recent_creations.clear()
    main.last_touched.clear()
    main._acciones_extra.clear()
    main._resto_de_mensaje.clear()
    main._gasto_juntado_en.clear()
    main._gastos_sin_archivar.clear()
    main.category_overrides.clear()
    state.last_event_touched.clear()
    state.payment_methods_cache[:] = mundo.medios
    state.user_prefs.clear()
    state.user_prefs.update({
        "_config_page_id": "config-falsa", "greeting_name": None, "activities": {}, "resumen_extras": [],
        "service_providers": {}, "known_places": [], "known_shops": {}, "domain_profiles": {},
        "purchase_counts": {}, "personas": mundo.personas, "knot_state": {}, "feature_hints": {},
    })
    state.current_location.update(lat=None, lon=None, updated_at=None, location_name=None, source="default")
    for rol, texto in caso.get("previo") or []:
        state.add_to_history(TELEFONO, rol, texto)
    if caso.get("pendiente"):
        state.pending_state[TELEFONO] = dict(caso["pendiente"])


async def correr_caso(caso: dict) -> dict:
    """Corre un caso y devuelve {"ok", "comprobaciones", "dicho", "error", "segundos"}."""
    hoy = state.now_argentina().date()
    datos = base(hoy)
    if caso.get("datos"):
        datos = caso["datos"](datos, hoy)
    mundo = Mundo(hoy, datos)
    originales = instalar(mundo)
    agente_antes = main.FINANZAS_AGENTE
    main.FINANZAS_AGENTE = bool(caso.get("finanzas_agente"))
    inicio = time.time()
    error = None
    try:
        _limpiar_estado(mundo, caso)
        for mensaje in caso["mensajes"]:
            await main.process_single_item(TELEFONO, {"text": mensaje, "image_b64": None, "image_type": None,
                                                      "extra_images": [], "quoted_text": None})
    except Exception as e:                         # un caso roto no frena a los demás
        error = f"{type(e).__name__}: {e}"
    finally:
        main.FINANZAS_AGENTE = agente_antes
        restaurar(originales)
    comprobaciones = []
    for c in caso["espera"]:
        try:
            ok, detalle = c(mundo)
        except Exception as e:
            ok, detalle = False, f"la comprobación falló: {type(e).__name__}: {e}"
        comprobaciones.append((bool(ok), detalle))
    return {"ok": error is None and all(ok for ok, _ in comprobaciones), "comprobaciones": comprobaciones,
            "dicho": mundo.dicho(), "rutas": list(mundo.rutas), "error": error,
            "no_previstas": sorted(set(mundo.no_previstas)), "segundos": time.time() - inicio}


def _informe(resultados: list) -> str:
    bien = sum(1 for _, r in resultados if r["ok"])
    lineas = [f"# Banco de conversaciones: {bien} de {len(resultados)} bien", "",
              f"Modelos: {state.SONNET_MODEL} / {state.HAIKU_MODEL}", ""]
    for caso, r in resultados:
        lineas.append(f"## {'✅' if r['ok'] else '❌'} {caso['nombre']}")
        lineas.append(f"Martin: {' → '.join(caso['mensajes'])}")
        if caso.get("finanzas_agente"):
            lineas.append("(con KNOT_FINANZAS_AGENTE=1)")
        if r["rutas"]:
            lineas.append(f"Módulo: {', '.join(str(x) for x in r['rutas'])}")
        for ok, detalle in r["comprobaciones"]:
            lineas.append(f"- {'✅' if ok else '❌'} {detalle}")
        if r["error"]:
            lineas.append(f"- ❌ se rompió: {r['error']}")
        if r["no_previstas"]:
            lineas.append(f"- (el mundo falso no implementa: {', '.join(r['no_previstas'])})")
        if not r["ok"]:
            lineas.append("")
            lineas.append("Knot contestó:")
            lineas.append("```")
            lineas.append(r["dicho"][:1500] or "(nada)")
            lineas.append("```")
            lineas.append(f"Por qué existe este caso: {caso.get('origen', '-')}")
        lineas.append("")
    return "\n".join(lineas)


async def correr(filtros: list) -> int:
    elegidos = [c for c in CASOS if not filtros or any(f.lower() in c["nombre"].lower() for f in filtros)]
    if not elegidos:
        print("Ningún caso coincide con ese filtro.")
        return 1
    resultados = []
    for i, caso in enumerate(elegidos, 1):
        r = await correr_caso(caso)
        resultados.append((caso, r))
        print(f"[{i}/{len(elegidos)}] {'✅' if r['ok'] else '❌'} {caso['nombre']} ({r['segundos']:.0f}s)", flush=True)
    informe = _informe(resultados)
    print("\n" + informe)
    destino = os.environ.get("GITHUB_STEP_SUMMARY")
    if destino:
        with open(destino, "a", encoding="utf-8") as f:
            f.write(informe + "\n")
    return 0 if all(r["ok"] for _, r in resultados) else 1


def principal() -> int:
    args = sys.argv[1:]
    if "--lista" in args:
        for c in CASOS:
            print(f"- {c['nombre']}  ::  {' → '.join(c['mensajes'])}")
        print(f"\n{len(CASOS)} casos")
        return 0
    if os.environ.get("ANTHROPIC_API_KEY", "falta") in ("", "falta", "test", "banco"):
        print("Falta ANTHROPIC_API_KEY: el banco corre contra el modelo real y necesita la clave.")
        return 2
    return asyncio.run(correr([a for a in args if not a.startswith("--")]))


if __name__ == "__main__":
    sys.exit(principal())
