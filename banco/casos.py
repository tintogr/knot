"""
Los casos del banco de conversaciones.

Cada caso es algo que Martin le escribe a Knot (o que alguna vez salió mal) con lo que
tiene que pasar. Se corren contra el modelo REAL antes de cambiar un prompt o un modelo:

    python -m banco.correr              # todos
    python -m banco.correr vianda       # solo los que tengan "vianda" en el nombre

Un caso:
    nombre       cómo se llama en el informe
    mensajes     lo que escribe el usuario, en orden (se atienden todos; se evalúa al final)
    previo       conversación anterior ya ocurrida: [("user", "..."), ("assistant", "...")]
    pendiente    pregunta que Knot dejó hecha (el estado interno), si el caso arranca ahí
    datos        función que recibe los datos base y los ajusta para este caso
    finanzas_agente   True para atender las correcciones con el agente que conversa
    espera       comprobaciones: cada una recibe el mundo y devuelve (ok, qué miró)
    origen       de dónde sale el caso (bitácora, traspaso), para saber por qué existe

Para agregar un caso: copiar uno parecido y cambiarle el mensaje y lo esperado.
"""
from datetime import timedelta


# ── Comprobaciones ────────────────────────────────────────────────────────────

def _txt(s):
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", (s or "").lower()) if unicodedata.category(c) != "Mn")


def ruta(*modulos):
    def _c(m):
        ultima = m.rutas[-1] if m.rutas else "(ninguna)"
        return ultima in modulos, f"módulo elegido: {ultima} (esperado: {' o '.join(str(x) for x in modulos)})"
    return _c


def creo_movimiento(contiene, monto, tipo="EGRESO"):
    def _c(m):
        hallado = [e for e in m.creados if _txt(contiene) in _txt(e.name) and abs(e.value_ars - monto) < 1
                   and e.in_out == tipo]
        vistos = ", ".join(f"{e.name} ${e.value_ars:,.0f} {e.in_out}" for e in m.creados) or "ninguno"
        return bool(hallado), f"registró {tipo} '{contiene}' por ${monto:,.0f} (registró: {vistos})"
    return _c


def cantidad_creados(n):
    def _c(m):
        return len(m.creados) == n, f"movimientos nuevos: {len(m.creados)} (esperado: {n})"
    return _c


def no_toco_la_plata():
    def _c(m):
        tocados = [i for i, _ in m.cambios if any(e.id == i for e in m.movimientos)]
        ok = not m.creados and not tocados and not (m.archivados & {e.id for e in m.movimientos})
        return ok, f"no registró, cambió ni borró movimientos (nuevos: {len(m.creados)}, cambiados: {tocados})"
    return _c


def quedo(id_mov, monto=None, estado=None):
    def _c(m):
        e = next((x for x in m.movimientos if x.id == id_mov), None)
        if not e:
            return False, f"no existe el movimiento {id_mov}"
        ok = (monto is None or abs(e.value_ars - monto) < 1) and (estado is None or e.estado == estado)
        return ok, (f"'{e.name}' quedó en ${e.value_ars:,.0f}, {e.estado} "
                    f"(esperado: {'$' + format(monto, ',.0f') if monto else ''} {estado or ''})")
    return _c


def nuevo_quedo(contiene, monto):
    """Un movimiento registrado en este mismo caso terminó con ese monto."""
    def _c(m):
        e = next((x for x in m.creados if _txt(contiene) in _txt(x.name)), None)
        if not e:
            return False, f"no se registró nada con '{contiene}'"
        return abs(e.value_ars - monto) < 1, f"'{e.name}' quedó en ${e.value_ars:,.0f} (esperado ${monto:,.0f})"
    return _c


def creo_tarea(contiene, sin_fecha=False):
    def _c(m):
        t = next((x for x in m.tareas_creadas if _txt(contiene) in _txt(x.name)), None)
        vistas = ", ".join(x.name for x in m.tareas_creadas) or "ninguna"
        ok = bool(t) and (not sin_fecha or not t.due_date)
        return ok, (f"anotó la tarea '{contiene}'" + (" sin inventarle fecha" if sin_fecha else "")
                    + f" (anotó: {vistas}{', con fecha ' + str(t.due_date) if t and t.due_date else ''})")
    return _c


def cerro_tarea(id_tarea):
    def _c(m):
        t = next(x for x in m.tareas if x.id == id_tarea)
        return t.status == "Listo", f"'{t.name}' quedó en estado {t.status} (esperado: Listo)"
    return _c


def creo_recordatorio(contiene, dias=None, hora=None):
    def _c(m):
        r = next((x for x in m.recordatorios if _txt(contiene) in _txt(x.get("summary"))), None)
        vistos = ", ".join(f"{x.get('summary')} @ {x.get('fire_at')}" for x in m.recordatorios) or "ninguno"
        ok = bool(r)
        if r and dias is not None:
            ok = ok and str(r.get("fire_at", ""))[:10] == str(m.hoy + timedelta(days=dias))
        if r and hora:
            ok = ok and str(r.get("fire_at", ""))[11:16] == hora
        return ok, f"creó el recordatorio '{contiene}' (creó: {vistos})"
    return _c


def creo_evento(contiene):
    def _c(m):
        vistos = ", ".join(f"{x.get('summary')} {x.get('date')} {x.get('time')}" for x in m.eventos) or "ninguno"
        return any(_txt(contiene) in _txt(x.get("summary")) for x in m.eventos), \
            f"agendó '{contiene}' (agendó: {vistos})"
    return _c


def agrego_compra(contiene):
    def _c(m):
        vistas = ", ".join(x.name for x in m.compras_agregadas) or "ninguna"
        return any(_txt(contiene) in _txt(x.name) for x in m.compras_agregadas), \
            f"agregó '{contiene}' a las compras (agregó: {vistas})"
    return _c


def no_borro(id_cosa):
    def _c(m):
        return id_cosa not in m.archivados, f"no borró {id_cosa} sin confirmación (borrados: {sorted(m.archivados) or 'ninguno'})"
    return _c


def dijo(*alguna):
    def _c(m):
        d = _txt(m.dicho())
        return any(_txt(a) in d for a in alguna), f"mencionó alguna de: {', '.join(alguna)}"
    return _c


def no_dijo(*ninguna):
    def _c(m):
        d = _txt(m.dicho())
        malas = [n for n in ninguna if _txt(n) in d]
        return not malas, f"no dijo: {', '.join(ninguna)}" + (f" (dijo: {', '.join(malas)})" if malas else "")
    return _c


def pregunto():
    def _c(m):
        return "?" in m.dicho(), "hizo una pregunta en vez de adivinar"
    return _c


def contesto():
    def _c(m):
        return bool(m.dicho().strip()), "contestó algo"
    return _c


def corto(caracteres=700):
    def _c(m):
        largo = max((len(x) for x in m.enviados), default=0)
        return largo <= caracteres, f"mensaje más largo: {largo} caracteres (máximo {caracteres})"
    return _c


def alguna(*comprobaciones):
    """Vale si se cumple cualquiera (hay más de una forma correcta de resolverlo)."""
    def _c(m):
        resultados = [c(m) for c in comprobaciones]
        return any(ok for ok, _ in resultados), " O ".join(d for _, d in resultados)
    return _c


# ── Ajustes de datos ──────────────────────────────────────────────────────────

def _con_vianda_de_hoy(d, hoy):
    d["movimientos"].append({"id": "vianda", "name": "Vianda", "value_ars": 11500, "categories": ["Vianda"],
                             "date": str(hoy)})
    return d


def _con_dos_super(d, hoy):
    d["movimientos"].append({"id": "sup2", "name": "Supermercado Coto", "value_ars": 32000,
                             "categories": ["Supermercado"], "date": str(hoy)})
    return d


def _con_deuda_papa(d, hoy):
    d["movimientos"].append({"id": "deuda_papa", "name": "Devolver préstamo a papá (Fabián)", "value_ars": 100000,
                             "categories": ["Préstamo"], "estado": "Impaga", "date": str(hoy - timedelta(days=10))})
    return d


# ── Casos ─────────────────────────────────────────────────────────────────────

CASOS = [
    # Plata: registrar
    {"nombre": "gasto simple: vianda 11500",
     "mensajes": ["vianda 11500"],
     "espera": [ruta("GASTO"), creo_movimiento("vianda", 11500), cantidad_creados(1)],
     "origen": "uso diario"},
    {"nombre": "gasto con medio de pago y monto en palabras",
     "mensajes": ["pagué el super 45 mil con débito"],
     "espera": [creo_movimiento("super", 45000), cantidad_creados(1), no_dijo("con qué pagaste")],
     "origen": "bitácora 10.2: preguntaba el medio aunque ya estaba dicho"},
    {"nombre": "monto argentino: el punto es de miles",
     "mensajes": ["nafta 17.500"],
     "espera": [creo_movimiento("nafta", 17500)],
     "origen": "CLAUDE.md: montos argentinos"},
    {"nombre": "dos gastos en un mensaje son dos registros",
     "mensajes": ["vianda 11500 y café 3200"],
     "espera": [creo_movimiento("vianda", 11500), creo_movimiento("caf", 3200), cantidad_creados(2)],
     "origen": "agente de entrada: varios gastos juntos van a GASTO"},
    {"nombre": "un ingreso no es un gasto",
     "mensajes": ["me entraron 250000 de LBL por el anteproyecto"],
     "espera": [creo_movimiento("", 250000, tipo="INGRESO"), cantidad_creados(1)],
     "origen": "uso diario"},
    {"nombre": "gasto sin monto: pregunta, no inventa",
     "mensajes": ["compré pan"],
     "espera": [cantidad_creados(0), pregunto()],
     "origen": "principio 4: preguntar si falta algo"},

    # Plata: préstamos y facturas
    {"nombre": "préstamo recibido: ingreso + deuda anotada",
     "mensajes": ["le pedí 200 mil a mi viejo"],
     "espera": [creo_movimiento("", 200000, tipo="INGRESO"), creo_movimiento("devolver", 200000)],
     "origen": "CLAUDE.md: préstamos"},
    {"nombre": "devolver un préstamo salda esa deuda",
     "mensajes": ["le devolví los 100 mil a mi viejo"],
     "datos": _con_deuda_papa,
     "espera": [quedo("deuda_papa", estado="Pagada")],
     "origen": "bitácora 10: 'devolución' no encontraba 'Devolver préstamo a papá'"},
    {"nombre": "pago de una factura pendiente la deja pagada",
     "mensajes": ["pagué camuzzi 30000 con débito"],
     "espera": [quedo("camuzzi", estado="Pagada")],
     "origen": "una factura = un registro"},
    {"nombre": "respuesta con matiz a una pregunta de plata",
     "mensajes": ["no fue parcial, pagué con descuento y quedó saldada"],
     "pendiente": {"type": "factura_confirm", "situation": "diff_large", "conf_id": "x1",
                   "finance_page_id": "movistar", "paid_amount": 17791, "payment_method": None,
                   "gasto_page_id": None, "provider_name": "Factura Movistar", "invoice_amount": 29791.8},
     "previo": [("assistant", "⚠️ La factura de *Factura Movistar* era $29,792 pero pagaste $17,791. "
                              "¿Fue un pago parcial? (sí/no)")],
     "espera": [quedo("movistar", estado="Pagada")],
     "origen": "bitácora 11: leía 'pagué, pero fueron 17791' como una negación"},

    # Plata: corregir (con el agente que conversa)
    {"nombre": "corregir lo que se acaba de registrar",
     "mensajes": ["vianda 11500", "perdón, eran 12500"],
     "finanzas_agente": True,
     "espera": [nuevo_quedo("vianda", 12500), cantidad_creados(1)],
     "origen": "traspaso: el hilo de la conversación"},
    {"nombre": "corregir cuando hay dos parecidos: pregunta cuál",
     "mensajes": ["el super en realidad fue 46000"],
     "datos": _con_dos_super, "finanzas_agente": True,
     "espera": [quedo("sup1", monto=45000), quedo("sup2", monto=32000), pregunto()],
     "origen": "regla: nunca elegir 'el primero'"},
    {"nombre": "corregir con el camino viejo (comparación)",
     "mensajes": ["la vianda de hoy fue 12500, no 11500"],
     "datos": _con_vianda_de_hoy,
     "espera": [quedo("vianda", monto=12500)],
     "origen": "base para comparar con el agente"},
    {"nombre": "un reclamo no es una orden de borrar",
     "mensajes": ["me borraste la factura de movistar"],
     "espera": [no_borro("movistar"), no_toco_la_plata()],
     "origen": "bitácora 10: un verbo en pasado es un reclamo"},

    # Preguntas
    {"nombre": "pregunta de plata no registra nada",
     "mensajes": ["¿cuánto gasté este mes?"],
     "espera": [ruta("CHAT"), no_toco_la_plata(), dijo("45")],
     "origen": "regla: si pregunta, nunca GASTO"},
    {"nombre": "qué tengo pendiente cruza tareas y pagos",
     "mensajes": ["¿qué tengo pendiente?"],
     "espera": [no_toco_la_plata(), dijo("plomero"), dijo("camuzzi", "factura", "pago")],
     "origen": "traspaso sección 3"},
    {"nombre": "pregunta que cruza a plantas",
     "mensajes": ["¿cuándo regué el potus?"],
     "espera": [dijo("4 dias", "hace 4", "cuatro dias"), no_toco_la_plata()],
     "origen": "auditoría 2 bis: el agente no podía contestarlo"},

    # Agenda
    {"nombre": "tarea sin fecha: no la inventa",
     "mensajes": ["tengo que llamar al contador"],
     "espera": [creo_tarea("contador", sin_fecha=True), no_toco_la_plata()],
     "origen": "traspaso sección 3"},
    {"nombre": "cerrar una tarea",
     "mensajes": ["ya llamé al plomero"],
     "espera": [cerro_tarea("t_plomero")],
     "origen": "traspaso sección 3"},
    {"nombre": "recordatorio con mayúscula inicial",
     "mensajes": ["Recordame mañana a las 10 llamar a Ana"],
     "espera": [ruta("RECORDATORIO"), creo_recordatorio("ana", dias=1, hora="10:00")],
     "origen": "auditoría: el filtro descartaba 'Recordame...'"},
    {"nombre": "recordatorio sin fecha: no inventa una hora",
     "mensajes": ["recordame comprar el regalo de Juli"],
     "espera": [alguna(pregunto(), creo_tarea("regalo")), no_dijo("hoy a las 12")],
     "origen": "bitácora 10.6: inventaba 'hoy 12:00'"},
    {"nombre": "evento con día y hora",
     "mensajes": ["mañana a las 9 turno con el oculista"],
     "espera": [creo_evento("oculista")],
     "origen": "uso diario"},
    {"nombre": "otro tema después de '¿guardo la ubicación?' no se pierde",
     "mensajes": ["vianda 11500"],
     "pendiente": {"type": "geocode_confirm", "event_id": "ev1", "lat": -38.9, "lon": -68.1,
                   "place_name": "Club Pacífico", "raw_location": "club"},
     "previo": [("assistant", "📍 ¿Guardo la ubicación del turno como *Club Pacífico*?")],
     "espera": [creo_movimiento("vianda", 11500)],
     "origen": "17/11/2026: cualquier texto se buscaba en el mapa como dirección"},
    {"nombre": "otro tema después de '¿con cuánta anticipación?' no inventa un aviso",
     "mensajes": ["pagué el super 20000"],
     "pendiente": {"type": "recurring_event_reminder", "event_id": "ev1", "summary": "Funcional"},
     "previo": [("assistant", "✅ *Funcional* es recurrente cada martes.\n¿Querés que te avise antes de cada "
                              "sesión? Decime con cuánta anticipación.")],
     "espera": [creo_movimiento("super", 20000)],
     "origen": "17/11/2026: ponía 1 hora por defecto con cualquier respuesta"},
    {"nombre": "dos pedidos distintos en un mensaje",
     "mensajes": ["vianda 11500 y recordame mañana a las 10 llamar al contador"],
     "espera": [creo_movimiento("vianda", 11500), creo_recordatorio("contador", dias=1, hora="10:00")],
     "origen": "traspaso sección 3"},

    # Compras
    {"nombre": "agregar a las compras",
     "mensajes": ["agregá café y azúcar a las compras"],
     "espera": [agrego_compra("caf"), agrego_compra("az"), no_toco_la_plata()],
     "origen": "uso diario"},
    {"nombre": "sacar de las compras pide confirmación",
     "mensajes": ["sacá la leche de las compras"],
     "espera": [no_borro("c_leche"), dijo("leche"), pregunto()],
     "origen": "traspaso sección 3"},
    {"nombre": "sacar de las compras: el sí escrito alcanza",
     "mensajes": ["sacá la leche de las compras", "sí"],
     "espera": [alguna(lambda m: ("c_leche" in m.archivados, "sacó la leche después del sí"))],
     "origen": "traspaso sección 3"},

    # Voz y ayuda
    {"nombre": "hola: saludo corto, sin cartel",
     "mensajes": ["hola"],
     "espera": [contesto(), corto(500), no_toco_la_plata(), no_dijo("notion", "base de datos", "módulo")],
     "origen": "voz de Knot"},
    {"nombre": "qué podés hacer: áreas con ejemplos, corto",
     "mensajes": ["¿qué podés hacer?"],
     "espera": [contesto(), corto(900), no_toco_la_plata(), dijo("gasto", "plata"), dijo("recordame", "agenda", "evento")],
     "origen": "traspaso sección 3"},
    {"nombre": "gracias: no hace nada",
     "mensajes": ["vianda 11500", "gracias"],
     "espera": [cantidad_creados(1), corto(400)],
     "origen": "bitácora: re-registraba el gasto anterior"},
    {"nombre": "no afirma lo que no hizo",
     "mensajes": ["¿me agendaste lo del veterinario para el viernes?"],
     "espera": [no_toco_la_plata(), no_dijo("sí, lo agendé", "si, lo agende", "ya lo agendé")],
     "origen": "regla: nunca decir que hizo algo que no hizo"},
]
