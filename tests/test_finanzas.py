"""Correcciones y pagos de plata por un solo camino (finanzas.py): no se elige "la
primera" si hay varias, no se anuncia un cambio sin releerlo y un monto que no coincide
con la factura se pregunta."""
import asyncio
import types
from datetime import date

import pytest

import finanzas

E = types.SimpleNamespace


def _mov(id, name, value, fecha="2026-11-10", in_out="EGRESO", estado="Pagada", cats=None, notes=None):
    return E(id=id, name=name, value_ars=value, date=fecha, in_out=in_out, estado=estado,
             categories=cats or [], notes=notes, method="")


class _Datos:
    """Base de datos falsa, en memoria."""

    def __init__(self, movimientos):
        self.movs = {m.id: m for m in movimientos}
        self.actualizados = []
        self.marcar_ok = True
        self.falla_lectura = False
        self.tareas = []
        self.tareas_cerradas = []

    async def query_expenses(self, filtro):
        out = []
        for m in self.movs.values():
            if filtro.name_contains and filtro.name_contains.lower() not in m.name.lower():
                continue
            if filtro.date_range:
                d = date.fromisoformat(str(m.date)[:10])
                if not (filtro.date_range.start <= d <= filtro.date_range.end):
                    continue
            out.append(m)
        return out[: filtro.limit]

    async def get_expense(self, entry_id):
        return self.movs.get(entry_id)

    async def update_expense(self, entry_id, updates):
        self.actualizados.append((entry_id, dict(updates)))
        m = self.movs[entry_id]
        for clave, campo in (("value_ars", "value_ars"), ("name", "name"), ("categories", "categories"),
                             ("notes", "notes"), ("estado", "estado"), ("date", "date")):
            if clave in updates:
                setattr(m, campo, updates[clave])
        return m

    async def get_impaga_facturas(self, provider=None, estricto=False):
        if self.falla_lectura:
            if estricto:
                raise RuntimeError("Notion no responde")
            return []
        return [m for m in self.movs.values() if m.estado == "Impaga"]

    async def mark_finance_paid(self, page_id, paid_amount=None, payment_method=None, notes=None):
        if not self.marcar_ok:
            return False
        m = self.movs[page_id]
        m.estado = "Pagada"
        if paid_amount is not None:
            m.value_ars = paid_amount
        if notes:
            m.notes = ((m.notes or "") + " " + notes).strip()
        return True

    async def get_pending_factura_tasks(self):
        return self.tareas

    async def mark_factura_task_paid(self, page_id):
        self.tareas_cerradas.append(page_id)
        return True

    def service_of(self, texto):
        t = (texto or "").lower()
        if "interfast" in t or "expensas" in t:
            return {"servicio": "Expensas"}
        return None

    @staticmethod
    def vencimiento_de(notas):
        return date(2026, 11, 18) if "vence 18/11/2026" in (notas or "") else None


@pytest.fixture
def datos(monkeypatch):
    d = _Datos([
        _mov("g1", "Vianda", 11500),
        _mov("g2", "Supermercado La Anónima", 45000, fecha="2026-11-03"),
        _mov("g3", "Supermercado Coto", 32000, fecha="2026-11-12"),
        _mov("f1", "Factura Camuzzi — Noviembre 2026", 30000, estado="Impaga", notes="vence 18/11/2026"),
        _mov("f2", "Factura Movistar — Octubre 2026", 29791, estado="Impaga"),
        _mov("f3", "Factura Movistar — Noviembre 2026", 31000, estado="Impaga"),
        _mov("f4", "Factura Expensas — Noviembre 2026", 80000, estado="Impaga"),
    ])
    monkeypatch.setattr(finanzas, "_ds", d)
    return d


def _run(coro):
    return asyncio.run(coro)


# ── Corregir ──────────────────────────────────────────────────────────────────

def test_corrige_uno_y_cuenta_como_quedo(datos):
    r = _run(finanzas.corregir_gasto(buscar="vianda", nuevo_monto=12500))
    assert r.startswith("CORREGIDO")
    assert "$11,500" in r and "$12,500" in r          # antes y después, releído
    assert datos.movs["g1"].value_ars == 12500


def test_si_hay_varios_no_toca_ninguno(datos):
    r = _run(finanzas.corregir_gasto(buscar="supermercado", nuevo_monto=1))
    assert r.startswith("HAY VARIAS")
    assert "g2" in r and "g3" in r
    assert datos.actualizados == []


def test_con_la_fecha_se_desambigua(datos):
    r = _run(finanzas.corregir_gasto(buscar="supermercado", fecha="2026-11-12", nuevo_monto=33000))
    assert r.startswith("CORREGIDO")
    assert datos.movs["g3"].value_ars == 33000 and datos.movs["g2"].value_ars == 45000


def test_por_id_va_directo(datos):
    r = _run(finanzas.corregir_gasto(id="g2", nueva_fecha="2026-11-04", nueva_categoria=["Supermercado"]))
    assert r.startswith("CORREGIDO")
    assert datos.movs["g2"].date == "2026-11-04"


def test_sin_nada_que_cambiar_no_inventa(datos):
    assert _run(finanzas.corregir_gasto(buscar="vianda")).startswith("ERROR")
    assert datos.actualizados == []


def test_no_encuentra_y_lo_dice(datos):
    r = _run(finanzas.corregir_gasto(buscar="peluquería", nuevo_monto=5000))
    assert r.startswith("No encontré") and "No se cambió nada" in r


@pytest.mark.parametrize("kw", [{"nuevo_monto": -5}, {"nuevo_monto": "mucho"}, {"nueva_fecha": "31/02"},
                                {"nueva_categoria": ["Inventada"]}, {"nuevo_estado": "Quizás"}])
def test_datos_invalidos_no_se_guardan(datos, kw):
    assert _run(finanzas.corregir_gasto(buscar="vianda", **kw)).startswith("ERROR")
    assert datos.actualizados == []


def test_si_la_base_falla_no_dice_que_quedo(datos, monkeypatch):
    async def _rompe(*a, **k):
        raise RuntimeError("Notion 500")
    monkeypatch.setattr(datos, "update_expense", _rompe)
    r = _run(finanzas.corregir_gasto(buscar="vianda", nuevo_monto=12000))
    assert r.startswith("ERROR") and "No quedó cambiado" in r


# ── Marcar pagada ─────────────────────────────────────────────────────────────

def test_marca_pagada_y_cierra_su_tarea(datos):
    datos.tareas = [{"page_id": "t1", "finance_page_id": "f1"}, {"page_id": "t2", "finance_page_id": "f2"}]
    r = _run(finanzas.marcar_factura_pagada(provider="Camuzzi"))
    assert r.startswith("FACTURA PAGADA")
    assert datos.movs["f1"].estado == "Pagada"
    assert datos.tareas_cerradas == ["t1"]
    assert "No se creó un gasto aparte" in r


def test_si_notion_no_acepta_no_dice_que_la_marco(datos):
    datos.marcar_ok = False
    r = _run(finanzas.marcar_factura_pagada(provider="Camuzzi"))
    assert r.startswith("ERROR") and "Sigue pendiente" in r
    assert datos.movs["f1"].estado == "Impaga"


def test_varias_del_mismo_proveedor_pregunta_cual(datos):
    r = _run(finanzas.marcar_factura_pagada(provider="Movistar"))
    assert r.startswith("HAY VARIAS")
    assert datos.movs["f2"].estado == "Impaga" and datos.movs["f3"].estado == "Impaga"


def test_monto_muy_distinto_no_se_resuelve_solo(datos):
    r = _run(finanzas.marcar_factura_pagada(provider="Camuzzi", paid_amount=17500))
    assert r.startswith("NO SE MARCÓ")
    assert datos.movs["f1"].estado == "Impaga"


def test_monto_distinto_ya_confirmado_por_el_usuario(datos):
    r = _run(finanzas.marcar_factura_pagada(provider="Camuzzi", paid_amount=17500, monto_confirmado=True,
                                            nota="descuento por pago anticipado"))
    assert r.startswith("FACTURA PAGADA")
    assert datos.movs["f1"].value_ars == 17500
    assert "Facturaba $30,000" in datos.movs["f1"].notes and "descuento" in datos.movs["f1"].notes


def test_reconoce_el_proveedor_por_su_alias(datos):
    r = _run(finanzas.marcar_factura_pagada(provider="interfast"))
    assert r.startswith("FACTURA PAGADA")
    assert datos.movs["f4"].estado == "Pagada"


def test_factura_que_no_existe(datos):
    r = _run(finanzas.marcar_factura_pagada(provider="Edenor"))
    assert r.startswith("No encontré") and "No se marcó nada" in r


# ── Consultas ─────────────────────────────────────────────────────────────────

def test_deudas_con_vencimiento(datos):
    r = _run(finanzas.consultar_deudas())
    assert "PENDIENTES DE PAGO (4)" in r and "18/11" in r


def test_si_no_pudo_leer_las_deudas_no_dice_que_no_hay(datos):
    datos.falla_lectura = True
    r = _run(finanzas.ejecutar("consultar_deudas", {}))
    assert r.startswith("ERROR")


def test_buscar_devuelve_los_id(datos):
    r = _run(finanzas.buscar_gastos("supermercado", mes="2026-11"))
    assert "id=g2" in r and "id=g3" in r


def test_ejecutar_ignora_herramientas_ajenas(datos):
    assert _run(finanzas.ejecutar("consultar_clima", {})) is None


def test_acepta_los_nombres_de_campo_viejos(datos):
    # El modelo puede seguir mandando search_term / new_value_ars por la conversación previa.
    r = _run(finanzas.ejecutar("corregir_gasto", {"search_term": "vianda", "new_value_ars": 13000}))
    assert r.startswith("CORREGIDO") and datos.movs["g1"].value_ars == 13000
