"""
areas.py — Lo que une las áreas de Knot (Finanzas, Agenda, Proyectos, Colecciones).

Hasta noviembre 2026 cada área era una isla: el agente de chat (el que contesta las
preguntas) solo podía mirar el calendario, las finanzas y el mail. No podía decir qué
tareas hay pendientes, qué falta comprar, cómo están las plantas ni qué se habló en una
reunión, y la base de Tareas solo se usaba para las facturas.

Acá viven:
- las herramientas de lectura de las demás áreas, para que una sola pregunta pueda
  cruzarlas ("¿qué tengo pendiente esta semana?");
- las tareas generales (anotar, consultar, completar): la lista transversal que el
  diseño de abril pone en el centro de la Agenda;
- la voz común de Knot y la guía de lo que puede hacer.

No importa nada de main.py (evita imports circulares) y no manda mensajes: solo lee y
escribe datos y devuelve texto para que el agente arme la respuesta.
"""

import unicodedata
from datetime import date, datetime

from state import _ds, now_argentina

# ── Voz ───────────────────────────────────────────────────────────────────────

VOZ_KNOT = """CÓMO HABLÁS (igual en todas las respuestas):
- Corto: lo justo para que se entienda. Si alcanza una línea, una línea. Nunca un mensaje largo.
- Directo y cálido, en español rioplatense, de vos. Sin exclamaciones ni frases de relleno.
- No comentás ni opinás sobre lo que te cuentan ("qué rico", "buena elección") salvo que te lo pidan.
- No hablás de tu funcionamiento interno (bases de datos, módulos, herramientas, IDs). Decís qué
  quedó hecho y dónde, en palabras del usuario: "lo anoté en tus tareas", "quedó en los gastos de hoy".
- Confirmás con el dato concreto (nombre, monto, fecha), así se puede corregir si algo quedó mal.
- Si falta un dato o hay dos interpretaciones posibles, preguntás una sola cosa, corta."""

GUIA_CAPACIDADES = """SI TE PREGUNTAN QUÉ PODÉS HACER (o saludan sin pedir nada, o piden ayuda):
Contestá vos, en no más de 8 líneas, con las áreas y UN ejemplo de cada una tal como lo escribiría:
- Plata: gastos, ingresos, facturas y deudas ("vianda 11500", foto de un ticket).
- Agenda: eventos, recordatorios y tareas ("mañana 10 turno médico", "recordame a las 18 llamar a Ana",
  "tengo que renovar el carnet").
- Listas: compras, recetas, plantas, películas o libros ("agregá leche a las compras").
- Salud y actividad física ("corrí 5 km", foto de un análisis).
- Preguntas sobre todo eso, el mail y el clima ("¿cuánto gasté este mes?", "¿qué tengo pendiente?").
Si preguntan por un área puntual ("¿cómo anoto una deuda?"), respondé solo sobre esa, con 2 o 3 ejemplos.
No enumeres todo lo demás."""


# ── Utilidades ────────────────────────────────────────────────────────────────

def _norm(texto: str) -> str:
    t = "".join(c for c in unicodedata.normalize("NFD", (texto or "").lower()) if unicodedata.category(c) != "Mn")
    return " ".join("".join(c if c.isalnum() else " " for c in t).split())


def _fecha(valor) -> date | None:
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    try:
        return date.fromisoformat(str(valor)[:10])
    except (TypeError, ValueError):
        return None


def _es_tarea_de_pago(nombre: str) -> bool:
    """Las tareas "💰 Pagar ..." y "🔁 Pagar ..." las maneja el sistema de facturas: se
    crean y se cierran solas según el estado de la factura."""
    return (nombre or "").lstrip().startswith(("\U0001f4b0", "\U0001f501"))


def _cuando_vence(vence: date | None, hoy: date) -> str:
    if not vence:
        return ""
    dias = (vence - hoy).days
    if dias < 0:
        return f" — venció el {vence.strftime('%d/%m')}"
    if dias == 0:
        return " — para hoy"
    if dias == 1:
        return " — para mañana"
    return f" — para el {vence.strftime('%d/%m')}"


# ── Tareas ────────────────────────────────────────────────────────────────────

async def _tareas_pendientes() -> list:
    return await _ds.get_pending_tasks()


async def _pagos_pendientes(tareas_de_pago: list, hoy: date) -> list:
    """Lo que falta pagar, leído de la fuente de verdad (las facturas Impagas) y no de sus
    tareas espejo. Las "💰 Pagar ..." repiten una factura, así que solo se usan si no se
    pudo leer Finanzas; las "🔁 Pagar ..." (servicios sin factura) se listan siempre."""
    lineas = []
    try:
        impagas = await _ds.get_impaga_facturas(estricto=True)
    except Exception as e:
        print(f"[areas] no pude leer las facturas impagas: {type(e).__name__}: {e}")
        impagas = None
    if impagas is None:
        lineas.append("(No pude leer las facturas sin pagar; esto sale de las tareas de pago.)")
        for t in tareas_de_pago:
            lineas.append(f"- {t.name}{_cuando_vence(_fecha(t.due_date), hoy)}")
        return lineas
    for f in impagas:
        monto = getattr(f, "value_ars", None)
        lineas.append(f"- {f.name}" + (f" — ${monto:,.0f}" if isinstance(monto, (int, float)) and monto else ""))
    for t in tareas_de_pago:
        if (t.name or "").lstrip().startswith("\U0001f501"):
            lineas.append(f"- {t.name}{_cuando_vence(_fecha(t.due_date), hoy)}")
    return lineas


async def consultar_tareas(incluir_pagos: bool = True) -> str:
    """Tareas pendientes y, salvo que se pida que no, lo que falta pagar. Antes los pagos
    venían solo si el modelo los pedía: a "¿qué tengo pendiente?" contestaba sin las facturas."""
    hoy = now_argentina().date()
    tareas = await _tareas_pendientes()
    propias = [t for t in tareas if not _es_tarea_de_pago(t.name)]
    pagos = [t for t in tareas if _es_tarea_de_pago(t.name)]
    propias.sort(key=lambda t: (_fecha(t.due_date) or date.max, t.name or ""))
    lineas = []
    if propias:
        lineas.append("TAREAS PENDIENTES:")
        for t in propias:
            extra = f" [prioridad {t.priority}]" if t.priority in ("Alta",) else ""
            nota = f" ({t.notes[:80]})" if t.notes else ""
            lineas.append(f"- {t.name}{_cuando_vence(_fecha(t.due_date), hoy)}{extra}{nota}")
    else:
        lineas.append("No hay tareas pendientes anotadas.")
    if incluir_pagos:
        por_pagar = await _pagos_pendientes(pagos, hoy)
        if por_pagar:
            lineas.append("PAGOS PENDIENTES (también son pendientes del usuario; se cierran solos al registrar el pago):")
            lineas.extend(por_pagar)
        else:
            lineas.append("No hay pagos pendientes.")
    return "\n".join(lineas)


async def crear_tarea(nombre: str, vence: str = None, prioridad: str = None,
                      origen: str = None, notas: str = None) -> str:
    nombre = (nombre or "").strip()
    if not nombre:
        return "ERROR: falta el nombre de la tarea. Preguntale al usuario qué tiene que hacer."
    if _es_tarea_de_pago(nombre):
        return "ERROR: las tareas de pago las crea el sistema de facturas. No se creó nada."
    vence_d = _fecha(vence) if vence else None
    if vence and not vence_d:
        return f"ERROR: la fecha '{vence}' no es válida (formato AAAA-MM-DD). No se creó nada."
    # No duplicar: si ya hay una pendiente con el mismo nombre, se avisa.
    clave = _norm(nombre)
    for t in await _tareas_pendientes():
        if _norm(t.name) == clave:
            return (f"YA EXISTÍA una tarea pendiente llamada '{t.name}'"
                    f"{_cuando_vence(_fecha(t.due_date), now_argentina().date())}. No se creó otra.")
    datos = {"name": nombre[:1].upper() + nombre[1:], "source": "Knot",
             "category": origen if origen in ("Agenda", "Proyecto", "Finanzas", "Colección") else "Agenda"}
    if vence_d:
        datos["due_date"] = vence_d.isoformat()
    if prioridad in ("Alta", "Media", "Baja"):
        datos["priority"] = prioridad
    if notas:
        datos["notes"] = str(notas)[:1500]
    creada = await _ds.create_task(datos)
    if not getattr(creada, "id", ""):
        return "ERROR: la tarea NO se guardó."
    return (f"TAREA GUARDADA: '{creada.name}'"
            f"{_cuando_vence(vence_d, now_argentina().date())}.")


async def completar_tarea(nombre: str) -> str:
    """Marca como hecha UNA tarea, solo si queda claro cuál. Nunca adivina."""
    buscado = set(_norm(nombre).split())
    if not buscado:
        return "ERROR: decime qué tarea."
    candidatas = []
    for t in await _tareas_pendientes():
        palabras = set(_norm(t.name).split())
        if _norm(t.name) == _norm(nombre):
            candidatas = [t]
            break
        # Coinciden si todas las palabras significativas pedidas están en el nombre.
        significativas = {p for p in buscado if len(p) > 2} or buscado
        if significativas <= palabras:
            candidatas.append(t)
    if not candidatas:
        return f"No encontré ninguna tarea pendiente que coincida con '{nombre}'. No se cambió nada."
    if len(candidatas) > 1:
        lista = "; ".join(f"'{t.name}'" for t in candidatas[:8])
        return f"HAY VARIAS que coinciden: {lista}. No se cambió nada: preguntale al usuario cuál."
    t = candidatas[0]
    if _es_tarea_de_pago(t.name):
        return (f"'{t.name}' es un pago: no se marca a mano. Se cierra sola cuando se registra el pago "
                f"(usá marcar_factura_pagada o pedile al usuario el monto y el medio de pago).")
    actualizada = await _ds.update_task(t.id, {"status": "Listo"})
    if getattr(actualizada, "status", "") != "Listo":
        return f"ERROR: no pude marcar '{t.name}' como hecha."
    return f"TAREA COMPLETADA: '{t.name}'."


async def tareas_para_el_saludo(hoy: date, tope: int = 5) -> list[str]:
    """Líneas para el saludo de la mañana: tareas (no pagos) vencidas o para hoy."""
    tareas = [t for t in await _tareas_pendientes() if not _es_tarea_de_pago(t.name)]
    con_fecha = [(t, _fecha(t.due_date)) for t in tareas]
    urgentes = sorted([(t, d) for t, d in con_fecha if d and d <= hoy], key=lambda x: x[1])
    lineas = [f"- {t.name}{_cuando_vence(d, hoy) if d < hoy else ''}" for t, d in urgentes[:tope]]
    if len(urgentes) > tope:
        lineas.append(f"_y {len(urgentes) - tope} más_")
    return lineas


# ── Lecturas de las otras áreas ───────────────────────────────────────────────

async def consultar_lista_compras() -> str:
    items = await _ds.get_shopping_list(only_missing=True)
    if not items:
        return "La lista de compras está vacía: no falta nada."
    por_cat: dict[str, list[str]] = {}
    for it in items:
        por_cat.setdefault(it.category or "Otros", []).append(it.name + (f" ({it.notes})" if it.notes else ""))
    lineas = [f"FALTA COMPRAR ({len(items)}):"]
    for cat in sorted(por_cat):
        lineas.append(f"- {cat}: {', '.join(por_cat[cat])}")
    return "\n".join(lineas)


async def consultar_plantas() -> str:
    plantas = await _ds.query_plants()
    if not plantas:
        return "No hay plantas registradas."
    hoy = now_argentina().date()
    lineas = [f"PLANTAS ({len(plantas)}):"]
    for p in plantas:
        partes = [p.name]
        if p.location:
            partes.append(f"en {p.location}")
        if p.watering:
            partes.append(f"riego: {p.watering}")
        ultimo = _fecha(p.last_watering)
        if ultimo:
            partes.append(f"último riego {ultimo.strftime('%d/%m')} (hace {(hoy - ultimo).days} días)")
        if p.status:
            partes.append(f"estado: {p.status}")
        lineas.append("- " + ", ".join(partes))
    return "\n".join(lineas)


async def consultar_reuniones(buscar: str = None) -> str:
    reuniones = await (_ds.search_meetings(buscar, limit=8) if buscar else _ds.query_meetings(limit=10))
    if not reuniones:
        return (f"No encontré reuniones sobre '{buscar}'." if buscar else "No hay reuniones registradas.")
    lineas = ["REUNIONES:"]
    for r in reuniones:
        f = _fecha(r.date)
        con = f" con {r.with_whom}" if r.with_whom else ""
        notas = f": {r.notes[:300]}" if r.notes else ""
        lineas.append(f"- {f.strftime('%d/%m/%Y') if f else 's/f'} — {r.name}{con}{notas}")
    return "\n".join(lineas)


async def consultar_proyectos(area: str = None) -> str:
    proyectos = await _ds.query_projects(area if area in ("Laboral", "Hobby", "Personal", "Educación") else None)
    if not proyectos:
        return "No hay proyectos registrados."
    lineas = ["PROYECTOS E IDEAS:"]
    for p in proyectos:
        estado = f", {p.status}" if p.status else ""
        desc = f": {p.description[:150]}" if p.description else ""
        lineas.append(f"- {p.name} ({p.entry_type or 'Proyecto'}, {p.area or 's/área'}{estado}){desc}")
    return "\n".join(lineas)


# ── Definición de herramientas para el agente de chat ─────────────────────────

HERRAMIENTAS = [
    {
        "name": "consultar_tareas",
        "description": ("Lista las tareas pendientes del usuario (cosas por hacer, con o sin fecha). Usala para "
                        "'¿qué tengo pendiente?', '¿qué me falta hacer?', o antes de completar una tarea."),
        "input_schema": {"type": "object", "properties": {
            "incluir_pagos": {"type": "boolean", "description": "false SOLO si el usuario pidió expresamente las tareas sin los pagos. Por defecto true: lo que falta pagar también es un pendiente."}},
            "required": []},
    },
    {
        "name": "crear_tarea",
        "description": ("Anota una tarea o pendiente SIN horario de aviso: 'tengo que llamar al plomero', 'anotame "
                        "renovar el carnet antes del 30'. Si el usuario quiere que se le AVISE a una hora, eso es un "
                        "recordatorio (otro módulo), no una tarea. No inventes la fecha: si no la dijo, dejala en null."),
        "input_schema": {"type": "object", "properties": {
            "nombre": {"type": "string", "description": "Qué hay que hacer, corto y en infinitivo: 'Llamar al plomero'"},
            "vence": {"type": ["string", "null"], "description": "Fecha límite AAAA-MM-DD, solo si el usuario la dijo"},
            "prioridad": {"type": ["string", "null"], "enum": ["Alta", "Media", "Baja", None]},
            "origen": {"type": ["string", "null"], "enum": ["Agenda", "Proyecto", "Finanzas", "Colección", None],
                       "description": "De qué área sale. Agenda si no está claro."},
            "notas": {"type": ["string", "null"]}},
            "required": ["nombre"]},
    },
    {
        "name": "completar_tarea",
        "description": ("Marca una tarea como hecha: 'ya llamé al plomero', 'listo lo del carnet'. Si hay varias que "
                        "coinciden no cambia nada y te las devuelve para que preguntes cuál."),
        "input_schema": {"type": "object", "properties": {
            "nombre": {"type": "string", "description": "Nombre o palabras de la tarea"}},
            "required": ["nombre"]},
    },
    {
        "name": "consultar_lista_compras",
        "description": "Lo que falta comprar según la lista de compras, por categoría.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "consultar_plantas",
        "description": "Las plantas del usuario: ubicación, riego, último riego y estado. Para '¿cuándo regué el potus?'.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "consultar_reuniones",
        "description": "Notas de reuniones registradas (las más recientes, o las que coincidan con una búsqueda).",
        "input_schema": {"type": "object", "properties": {
            "buscar": {"type": ["string", "null"], "description": "Persona o tema; null para las últimas"}},
            "required": []},
    },
    {
        "name": "consultar_proyectos",
        "description": "Proyectos e ideas registrados, con su estado.",
        "input_schema": {"type": "object", "properties": {
            "area": {"type": ["string", "null"], "enum": ["Laboral", "Hobby", "Personal", "Educación", None]}},
            "required": []},
    },
]

_NOMBRES = {h["name"] for h in HERRAMIENTAS}

TEXTO_PARA_EL_PROMPT = """
OTRAS ÁREAS QUE PODÉS CONSULTAR Y CRUZAR (tenés las herramientas, usalas):
- Tareas: consultar_tareas, crear_tarea, completar_tarea. Una tarea es algo por hacer sin aviso a una hora.
- Lista de compras (consultar_lista_compras), plantas (consultar_plantas), reuniones (consultar_reuniones)
  y proyectos (consultar_proyectos).
Ante preguntas abiertas ("¿qué tengo pendiente?", "¿cómo viene mi semana?") cruzá las fuentes que hagan
falta: agenda + tareas + pagos pendientes, y contestá en un solo mensaje corto, ordenado por fecha.
Los pagos pendientes que devuelve consultar_tareas van SIEMPRE en la respuesta a "¿qué tengo pendiente?".
"Tengo que X" sin fecha: anotala YA con crear_tarea sin fecha. No preguntes la fecha antes de anotar; si
querés, ofrecé ponerle una después.
"Anotado", "listo", "agendado" solo se dicen DESPUÉS de que la herramienta contestó que quedó. Si no
llamaste ninguna herramienta, no pasó nada: no lo anuncies.
Cuando una herramienta devuelve "ERROR", "No encontré" o "HAY VARIAS", NO digas que quedó hecho: contá
qué pasó o preguntá lo que falta."""


async def ejecutar(nombre: str, entrada: dict) -> str | None:
    """Ejecuta una herramienta de este módulo. Devuelve None si el nombre no es de acá."""
    if nombre not in _NOMBRES:
        return None
    entrada = entrada or {}
    try:
        if nombre == "consultar_tareas":
            return await consultar_tareas(entrada.get("incluir_pagos") is not False)
        if nombre == "crear_tarea":
            return await crear_tarea(entrada.get("nombre"), entrada.get("vence"), entrada.get("prioridad"),
                                     entrada.get("origen"), entrada.get("notas"))
        if nombre == "completar_tarea":
            return await completar_tarea(entrada.get("nombre") or "")
        if nombre == "consultar_lista_compras":
            return await consultar_lista_compras()
        if nombre == "consultar_plantas":
            return await consultar_plantas()
        if nombre == "consultar_reuniones":
            return await consultar_reuniones(entrada.get("buscar"))
        if nombre == "consultar_proyectos":
            return await consultar_proyectos(entrada.get("area"))
    except Exception as e:
        print(f"[areas] {nombre} falló: {type(e).__name__}: {e}")
        return (f"ERROR: no pude completar '{nombre}' (falla al leer o guardar los datos). "
                f"Decile al usuario que no se pudo y que pruebe de nuevo en un momento.")
    return None
