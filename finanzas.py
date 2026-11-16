"""
finanzas.py — Herramientas de plata para el agente que conversa.

Hasta noviembre 2026 había DOS caminos para lo mismo: el módulo CORREGIR_GASTO (un
extractor que no ve la conversación) y una herramienta `corregir_gasto` dentro del agente
de preguntas que solo cambiaba el monto, tomaba la primera coincidencia sin mirar si
había otras y decía "corrección exitosa" sin releer nada. Lo mismo con
`marcar_factura_pagada`: anunciaba "✅ marcada" sin mirar si Notion había aceptado.

Acá vive UN solo camino, con las reglas del proyecto adentro:
- nunca se elige "la primera" si hay varias: se devuelven y el agente pregunta cuál;
- nunca se anuncia un cambio sin releer lo que quedó guardado;
- un monto que no coincide con la factura no se resuelve solo: se pregunta.

Igual que areas.py: no importa nada de main.py, no manda mensajes y devuelve texto para
que el agente arme la respuesta. Las palabras ERROR / HAY VARIAS / NO SE ... al principio
le avisan al agente que no tiene que decir que quedó hecho.
"""

from calendar import monthrange
from datetime import date

from state import _ds, QueryFilter, DateRange, now_argentina

CATEGORIAS = ["Supermercado", "Sueldo", "Recurrente", "Servicio", "Transporte", "Vianda", "Salud",
              "Salud Mental", "Salida", "Birra", "Ocio", "Compras", "Depto", "Plantas", "Viajes",
              "Venta", "Préstamo"]


# ── Utilidades ────────────────────────────────────────────────────────────────

def _limpio(page_id: str) -> str:
    return (page_id or "").replace("-", "").strip()


def _fecha_txt(valor) -> str:
    if not valor:
        return "sin fecha"
    s = valor.strftime("%Y-%m-%d") if hasattr(valor, "strftime") else str(valor)[:10]
    try:
        return f"{s[8:10]}/{s[5:7]}/{s[:4]}"
    except Exception:
        return s


def _rango_mes(mes: str):
    anio, m = (int(x) for x in mes.split("-")[:2])
    return DateRange(start=date(anio, m, 1), end=date(anio, m, monthrange(anio, m)[1]))


def _linea(e) -> str:
    """Un movimiento en una línea, con su id para poder señalarlo sin ambigüedad."""
    tipo = "INGRESO" if e.in_out == "INGRESO" else "EGRESO"
    partes = [f"id={e.id}", _fecha_txt(e.date), e.name or "(sin nombre)", f"${(e.value_ars or 0):,.0f}", tipo]
    if e.estado:
        partes.append(e.estado)
    if e.categories:
        partes.append("/".join(e.categories))
    if getattr(e, "method", None):
        partes.append(e.method)
    return " | ".join(partes)


def _asi_quedo(e) -> str:
    partes = [f"'{e.name}'", f"${(e.value_ars or 0):,.0f}", f"fecha {_fecha_txt(e.date)}",
              "ingreso" if e.in_out == "INGRESO" else "egreso"]
    if e.categories:
        partes.append("categoría " + "/".join(e.categories))
    if e.estado:
        partes.append(f"estado {e.estado}")
    if getattr(e, "method", None):
        partes.append(f"medio {e.method}")
    if e.notes:
        partes.append(f"notas: {e.notes[:200]}")
    return ", ".join(partes)


async def _buscar(texto: str = None, mes: str = None, fecha: str = None, limite: int = 8) -> list:
    filtro = QueryFilter(name_contains=(texto or None), limit=limite)
    try:
        if fecha:
            d = date.fromisoformat(fecha[:10])
            filtro.date_range = DateRange(start=d, end=d)
        elif mes:
            filtro.date_range = _rango_mes(mes)
    except (ValueError, TypeError):
        pass
    return await _ds.query_expenses(filtro)


# ── Herramientas ──────────────────────────────────────────────────────────────

async def buscar_gastos(query: str, mes: str = None, fecha: str = None) -> str:
    query = (query or "").strip()
    if not query and not mes and not fecha:
        return "ERROR: decime qué buscar (un nombre, un mes o una fecha)."
    if not mes and not fecha:
        mes = now_argentina().strftime("%Y-%m")
    encontrados = await _buscar(query, mes, fecha, limite=15)
    donde = f"el {_fecha_txt(fecha)}" if fecha else f"en {mes}"
    if not encontrados:
        return (f"No encontré movimientos {('con ' + repr(query) + ' ') if query else ''}{donde}. "
                f"Si puede estar en otro mes, buscá de nuevo indicando ese mes.")
    return (f"MOVIMIENTOS {donde} ({len(encontrados)}):\n" + "\n".join("- " + _linea(e) for e in encontrados)
            + "\n(Los id son para usar en corregir_gasto; no se los muestres al usuario.)")


async def corregir_gasto(id: str = None, buscar: str = None, mes: str = None, fecha: str = None,
                         nuevo_monto: float = None, nuevo_nombre: str = None, nueva_categoria: list = None,
                         nuevas_notas: str = None, nuevo_estado: str = None, nueva_fecha: str = None) -> str:
    """Corrige UN movimiento. Si no queda claro cuál es, no toca nada."""
    cambios = {}
    if nuevo_monto is not None:
        try:
            monto = float(nuevo_monto)
        except (TypeError, ValueError):
            return f"ERROR: el monto '{nuevo_monto}' no es un número. No se cambió nada."
        if monto <= 0:
            return "ERROR: el monto tiene que ser mayor a cero. No se cambió nada."
        cambios["value_ars"] = monto
    if nuevo_nombre and str(nuevo_nombre).strip():
        cambios["name"] = str(nuevo_nombre).strip()
    if nueva_categoria:
        cats = [nueva_categoria] if isinstance(nueva_categoria, str) else list(nueva_categoria)
        invalidas = [c for c in cats if c not in CATEGORIAS]
        if invalidas:
            return (f"ERROR: la categoría {', '.join(repr(c) for c in invalidas)} no existe. "
                    f"Las que hay: {', '.join(CATEGORIAS)}. No se cambió nada.")
        cambios["categories"] = cats
    if nuevas_notas is not None and str(nuevas_notas).strip():
        cambios["notes"] = str(nuevas_notas).strip()[:1900]
    if nuevo_estado:
        if nuevo_estado not in ("Pagada", "Impaga"):
            return "ERROR: el estado solo puede ser Pagada o Impaga. No se cambió nada."
        cambios["estado"] = nuevo_estado
    if nueva_fecha:
        try:
            cambios["date"] = date.fromisoformat(str(nueva_fecha)[:10]).isoformat()
        except ValueError:
            return f"ERROR: la fecha '{nueva_fecha}' no es válida (formato AAAA-MM-DD). No se cambió nada."
    if not cambios:
        return "ERROR: no indicaste qué cambiar. Preguntale al usuario qué dato hay que corregir."

    if id and _limpio(id):
        actual = await _ds.get_expense(_limpio(id))
        if not actual:
            return f"ERROR: no encontré el movimiento con id {id}. Buscalo de nuevo con buscar_gastos."
    else:
        texto = (buscar or "").strip()
        if not texto:
            return ("ERROR: falta decir cuál movimiento. Usá buscar_gastos y pasá su id, o preguntale "
                    "al usuario cuál es.")
        candidatos = await _buscar(texto, mes, fecha, limite=8)
        if not candidatos:
            donde = f" el {_fecha_txt(fecha)}" if fecha else (f" en {mes}" if mes else "")
            return f"No encontré ningún movimiento con '{texto}'{donde}. No se cambió nada."
        if len(candidatos) > 1:
            lista = "\n".join("- " + _linea(e) for e in candidatos)
            return (f"HAY VARIAS que coinciden con '{texto}'. No se cambió nada: preguntale al usuario "
                    f"cuál es (por fecha o monto) y volvé a llamar con su id.\n{lista}")
        actual = candidatos[0]

    antes, nombre_antes = _asi_quedo(actual), actual.name
    try:
        await _ds.update_expense(actual.id, cambios)
    except Exception as e:
        print(f"[finanzas] no pude corregir {actual.id}: {type(e).__name__}: {e}")
        return f"ERROR: no pude guardar la corrección de '{nombre_antes}'. No quedó cambiado."
    releido = await _ds.get_expense(actual.id)
    if not releido:
        return (f"Mandé la corrección de '{nombre_antes}' pero NO pude releerla para confirmar. "
                f"Decile al usuario que la revise.")
    return f"CORREGIDO. Antes: {antes}. Así quedó: {_asi_quedo(releido)}."


async def _impagas_de(proveedor: str) -> list:
    """Impagas que corresponden a ese proveedor: por nombre y por los aliases de la base
    Servicios ('interfast' -> Expensas)."""
    proveedor = (proveedor or "").strip()
    todas = [i for i in await _ds.get_impaga_facturas() if (i.value_ars or 0) > 0]
    if not proveedor:
        return todas
    p = proveedor.lower()
    svc = _ds.service_of(proveedor) or {}
    elegidas = []
    for imp in todas:
        nombre = (imp.name or "").lower()
        palabras_p = {w for w in p.split() if len(w) > 3}
        palabras_n = {w for w in nombre.replace("—", " ").split() if len(w) > 3}
        mismo = p in nombre or bool(palabras_p & palabras_n)
        if not mismo and svc.get("servicio"):
            svc_imp = _ds.service_of(imp.name or "") or {}
            mismo = svc_imp.get("servicio") == svc.get("servicio")
        if mismo:
            elegidas.append(imp)
    return elegidas


async def marcar_factura_pagada(provider: str = None, id: str = None, paid_amount: float = None,
                                payment_method: str = None, monto_confirmado: bool = False,
                                nota: str = None) -> str:
    """Marca UNA factura o deuda pendiente como pagada. No crea un gasto aparte: la
    factura es el registro (una factura = un registro)."""
    if id and _limpio(id):
        candidatas = [i for i in await _ds.get_impaga_facturas() if _limpio(i.id) == _limpio(id)]
        if not candidatas:
            return f"ERROR: no hay ninguna factura pendiente con id {id}. Mirá consultar_deudas."
    else:
        if not (provider or "").strip():
            return "ERROR: falta decir qué factura. Mirá consultar_deudas o preguntale al usuario."
        candidatas = await _impagas_de(provider)
        if not candidatas:
            return (f"No encontré ninguna factura pendiente de '{provider}'. No se marcó nada. "
                    f"Puede que ya esté pagada: fijate con buscar_gastos o historial_pagos.")
        if len(candidatas) > 1:
            lista = "\n".join("- " + _linea(i) for i in candidatas)
            return (f"HAY VARIAS facturas pendientes de '{provider}'. No se marcó nada: preguntale al "
                    f"usuario cuál pagó y volvé a llamar con su id.\n{lista}")
    factura = candidatas[0]
    facturado = factura.value_ars or 0
    try:
        pagado = float(paid_amount) if paid_amount is not None else facturado
    except (TypeError, ValueError):
        return f"ERROR: el monto '{paid_amount}' no es un número. No se marcó nada."
    if facturado and abs(pagado - facturado) / facturado > 0.10 and not monto_confirmado:
        return (f"NO SE MARCÓ: '{factura.name}' figura por ${facturado:,.0f} y el usuario dice que pagó "
                f"${pagado:,.0f}. Preguntale si con eso quedó saldada (descuento, recargo, monto mal "
                f"leído) o si fue un pago parcial. Si quedó saldada, volvé a llamar con "
                f"monto_confirmado=true y la aclaración en nota. Si fue parcial, no la marques.")
    hoy = now_argentina()
    partes_nota = [f"Pagada el {hoy.strftime('%d/%m/%Y')}."]
    if facturado and abs(pagado - facturado) > 0.5:
        partes_nota.append(f"Facturaba ${facturado:,.0f}.")
    if nota and str(nota).strip():
        partes_nota.append(str(nota).strip()[:300])
    ok = await _ds.mark_finance_paid(factura.id, pagado, payment_method, " ".join(partes_nota))
    if not ok:
        return f"ERROR: no pude marcar '{factura.name}' como pagada. Sigue pendiente."
    # La factura resta en el mes en que se pagó, no en el que llegó el mail.
    try:
        await _ds.update_expense(factura.id, {"date": hoy.strftime("%Y-%m-%d")})
    except Exception as e:
        print(f"[finanzas] no pude poner la fecha de pago en {factura.id}: {type(e).__name__}: {e}")
    # Su tarea "💰 Pagar ..." se cierra con ella.
    try:
        for t in await _ds.get_pending_factura_tasks():
            if _limpio(t.get("finance_page_id")) == _limpio(factura.id):
                await _ds.mark_factura_task_paid(t["page_id"])
                break
    except Exception as e:
        print(f"[finanzas] no pude cerrar la tarea de {factura.id}: {type(e).__name__}: {e}")
    releida = await _ds.get_expense(factura.id)
    if not releida or releida.estado != "Pagada":
        return (f"Marqué '{factura.name}' como pagada pero al releerla NO la veo como Pagada. "
                f"Decile al usuario que la revise.")
    return (f"FACTURA PAGADA. Así quedó: {_asi_quedo(releida)}. "
            f"No se creó un gasto aparte: el pago quedó registrado en la factura.")


async def consultar_deudas(provider: str = None) -> str:
    impagas = await _impagas_de(provider) if provider else await _ds.get_impaga_facturas(estricto=True)
    if not impagas:
        return ("No hay facturas ni deudas pendientes" + (f" de '{provider}'." if provider else "."))
    hoy = now_argentina().date()
    lineas = []
    for e in impagas:
        extra = ""
        vence = _ds.vencimiento_de(e.notes or "")
        if vence:
            dias = (vence - hoy).days
            extra = (f" — venció el {vence.strftime('%d/%m')}" if dias < 0 else " — vence hoy" if dias == 0
                     else f" — vence el {vence.strftime('%d/%m')}")
        lineas.append(f"- id={e.id} | {e.name} | ${(e.value_ars or 0):,.0f}{extra}")
    return (f"PENDIENTES DE PAGO ({len(impagas)}):\n" + "\n".join(lineas)
            + "\n(Los id son para marcar_factura_pagada; no se los muestres al usuario.)")


# ── Definición de herramientas para el agente ─────────────────────────────────

HERRAMIENTAS = [
    {
        "name": "buscar_gastos",
        "description": ("Busca movimientos (gastos e ingresos) por nombre, en un mes o en un día. Usala para "
                        "'¿pagué tal cosa?', '¿cuánto fue lo de X?' y SIEMPRE antes de corregir algo si no "
                        "tenés su id. Devuelve cada movimiento con su id."),
        "input_schema": {"type": "object", "properties": {
            "query": {"type": "string", "description": "Nombre o parte del nombre: 'CALF', 'vianda', 'alquiler'"},
            "mes": {"type": ["string", "null"], "description": "AAAA-MM. Si no se indica, el mes actual."},
            "fecha": {"type": ["string", "null"], "description": "AAAA-MM-DD para un día puntual."}},
            "required": ["query"]},
    },
    {
        "name": "corregir_gasto",
        "description": ("Corrige UN gasto o ingreso ya registrado: monto, nombre, categoría, notas, fecha o "
                        "estado (Pagada/Impaga). Pasá el id (de buscar_gastos) o un texto en 'buscar'. Si hay "
                        "varios que coinciden NO cambia nada y te los devuelve para que preguntes cuál. "
                        "Montos argentinos: punto = miles, coma = decimales ('17.500' son diecisiete mil "
                        "quinientos -> 17500)."),
        "input_schema": {"type": "object", "properties": {
            "id": {"type": ["string", "null"], "description": "id del movimiento, si ya lo tenés"},
            "buscar": {"type": ["string", "null"], "description": "Nombre o parte del nombre, si no tenés el id"},
            "mes": {"type": ["string", "null"], "description": "AAAA-MM para acotar la búsqueda"},
            "fecha": {"type": ["string", "null"], "description": "AAAA-MM-DD que tiene HOY el movimiento, para acotar"},
            "nuevo_monto": {"type": ["number", "null"], "description": "Nuevo monto en pesos, número sin separador de miles"},
            "nuevo_nombre": {"type": ["string", "null"]},
            "nueva_categoria": {"type": ["array", "null"], "items": {"type": "string", "enum": CATEGORIAS}},
            "nuevas_notas": {"type": ["string", "null"], "description": "Reemplaza las notas"},
            "nuevo_estado": {"type": ["string", "null"], "enum": ["Pagada", "Impaga", None]},
            "nueva_fecha": {"type": ["string", "null"], "description": "AAAA-MM-DD"}},
            "required": []},
    },
    {
        "name": "marcar_factura_pagada",
        "description": ("Marca UNA factura o deuda pendiente como pagada cuando el usuario dice que la pagó. "
                        "No crea un gasto aparte. Si el monto pagado difiere más de 10% del facturado NO la "
                        "marca y te pide que preguntes si quedó saldada o fue parcial. Si hay varias del mismo "
                        "proveedor te las devuelve para que preguntes cuál."),
        "input_schema": {"type": "object", "properties": {
            "provider": {"type": ["string", "null"], "description": "Proveedor o persona: 'Camuzzi', 'CALF', 'papá'"},
            "id": {"type": ["string", "null"], "description": "id de la factura (de consultar_deudas), si ya lo tenés"},
            "paid_amount": {"type": ["number", "null"], "description": "Monto pagado si lo dijo; null si no"},
            "payment_method": {"type": ["string", "null"], "description": "Medio de pago si lo dijo: 'BBVA', 'Mercado Pago'"},
            "monto_confirmado": {"type": "boolean", "description": "true SOLO si el usuario ya confirmó que con ese monto distinto la factura quedó saldada"},
            "nota": {"type": ["string", "null"], "description": "Aclaración del usuario sobre la diferencia"}},
            "required": []},
    },
    {
        "name": "consultar_deudas",
        "description": ("Lista facturas y deudas pendientes de pago, con vencimiento si se conoce. Para '¿qué "
                        "debo?', '¿qué facturas tengo impagas?'."),
        "input_schema": {"type": "object", "properties": {
            "provider": {"type": ["string", "null"], "description": "Filtrar por proveedor o persona. Null para todas."}},
            "required": []},
    },
]

_NOMBRES = {h["name"] for h in HERRAMIENTAS}

TEXTO_PARA_EL_PROMPT = """
CORRECCIONES Y PAGOS DE PLATA (tenés las herramientas, usalas):
- Para corregir algo ya registrado: corregir_gasto. Si no sabés exactamente cuál es, primero buscar_gastos
  y usá su id. Un gasto que se acaba de registrar en esta conversación es "el último": buscalo por nombre
  y fecha de hoy.
- Cuando dice que pagó una factura o le devolvió plata a alguien: marcar_factura_pagada.
- Si la herramienta contesta "HAY VARIAS", "NO SE MARCÓ", "No encontré" o "ERROR", NO digas que quedó
  hecho: contá qué pasó en una línea y preguntá lo que falta.
- Al confirmar un cambio decí cómo quedó (nombre, monto, fecha), nunca los id."""


async def ejecutar(nombre: str, entrada: dict) -> str | None:
    """Ejecuta una herramienta de este módulo. Devuelve None si el nombre no es de acá."""
    if nombre not in _NOMBRES:
        return None
    e = entrada or {}
    try:
        if nombre == "buscar_gastos":
            return await buscar_gastos(e.get("query") or "", e.get("mes"), e.get("fecha"))
        if nombre == "corregir_gasto":
            return await corregir_gasto(
                id=e.get("id"), buscar=e.get("buscar") or e.get("search_term"), mes=e.get("mes"),
                fecha=e.get("fecha"),
                nuevo_monto=e.get("nuevo_monto") if e.get("nuevo_monto") is not None else e.get("new_value_ars"),
                nuevo_nombre=e.get("nuevo_nombre"), nueva_categoria=e.get("nueva_categoria"),
                nuevas_notas=e.get("nuevas_notas"), nuevo_estado=e.get("nuevo_estado"),
                nueva_fecha=e.get("nueva_fecha"))
        if nombre == "marcar_factura_pagada":
            return await marcar_factura_pagada(
                provider=e.get("provider"), id=e.get("id"), paid_amount=e.get("paid_amount"),
                payment_method=e.get("payment_method"), monto_confirmado=bool(e.get("monto_confirmado")),
                nota=e.get("nota"))
        if nombre == "consultar_deudas":
            return await consultar_deudas(e.get("provider"))
    except Exception as ex:
        print(f"[finanzas] {nombre} falló: {type(ex).__name__}: {ex}")
        return (f"ERROR: no pude completar '{nombre}' (falla al leer o guardar los datos). "
                f"Decile al usuario que no se pudo y que pruebe de nuevo en un momento.")
    return None
