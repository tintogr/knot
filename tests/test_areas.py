"""Tareas generales y lecturas de las demás áreas (lo que usa el agente de preguntas)."""
import asyncio
import types
from datetime import date, timedelta

import pytest

import areas
import state

T = types.SimpleNamespace


class _Datos:
    """Base de datos falsa, en memoria."""

    def __init__(self):
        self.tareas = []
        self.creadas = []

    async def get_pending_tasks(self, category=None):
        return [t for t in self.tareas if t.status != "Listo"]

    async def create_task(self, data):
        t = T(id=f"t{len(self.tareas)}", name=data["name"], status="Sin empezar", priority=data.get("priority"),
              due_date=data.get("due_date"), notes=data.get("notes"), category=data.get("category"), source="Knot")
        self.tareas.append(t)
        self.creadas.append(data)
        return t

    async def update_task(self, task_id, updates):
        t = next(x for x in self.tareas if x.id == task_id)
        t.status = updates.get("status", t.status)
        return t

    async def get_shopping_list(self, only_missing=True):
        return [T(name="Leche", category="Lacteo", notes=None), T(name="Yerba", category="Infusion", notes="2 kg"),
                T(name="Queso", category="Lacteo", notes=None)]

    async def query_plants(self):
        hoy = state.now_argentina().date()
        return [T(name="Potus", location="living", watering="semanal", status="Bien",
                  last_watering=(hoy - timedelta(days=4)).isoformat())]

    async def query_meetings(self, limit=10):
        return [T(name="Obra Roca", with_whom="Ana", date="2026-11-10", notes="Definir aberturas")]

    async def search_meetings(self, q, limit=8):
        return []

    async def query_projects(self, area=None):
        return [T(name="Ducato", entry_type="Proyecto", area="Personal", status="En progreso", description=None)]


@pytest.fixture
def datos(monkeypatch):
    d = _Datos()
    monkeypatch.setattr(areas, "_ds", d)
    return d


def _tarea(nombre, vence=None, estado="Sin empezar", prioridad=None):
    return T(id=nombre, name=nombre, status=estado, priority=prioridad, due_date=vence, notes=None,
             category="Agenda", source="Knot")


def test_anotar_una_tarea(datos):
    r = asyncio.run(areas.ejecutar("crear_tarea", {"nombre": "llamar al plomero", "vence": "2026-11-30"}))
    assert "TAREA GUARDADA" in r
    assert datos.creadas[0]["name"] == "Llamar al plomero"
    assert datos.creadas[0]["due_date"] == "2026-11-30"
    assert datos.creadas[0]["category"] == "Agenda"


def test_no_se_inventa_fecha_ni_se_duplica(datos):
    asyncio.run(areas.crear_tarea("Renovar el carnet"))
    assert "due_date" not in datos.creadas[0]                       # sin fecha dicha, sin fecha
    r = asyncio.run(areas.crear_tarea("renovar el carnét"))         # misma tarea, escrita distinto
    assert "YA EXISTÍA" in r and len(datos.creadas) == 1


def test_fecha_invalida_no_guarda(datos):
    r = asyncio.run(areas.crear_tarea("Pagar seña", vence="el viernes"))
    assert r.startswith("ERROR") and datos.creadas == []


def test_completar_una_tarea(datos):
    datos.tareas = [_tarea("Llamar al plomero"), _tarea("Renovar el carnet")]
    r = asyncio.run(areas.ejecutar("completar_tarea", {"nombre": "plomero"}))
    assert "COMPLETADA" in r
    assert datos.tareas[0].status == "Listo" and datos.tareas[1].status == "Sin empezar"


def test_si_hay_varias_parecidas_pregunta_y_no_toca_nada(datos):
    datos.tareas = [_tarea("Llamar al plomero"), _tarea("Llamar al contador")]
    r = asyncio.run(areas.completar_tarea("llamar"))
    assert "HAY VARIAS" in r
    assert all(t.status == "Sin empezar" for t in datos.tareas)


def test_si_no_existe_lo_dice(datos):
    datos.tareas = [_tarea("Llamar al plomero")]
    assert "No encontré" in asyncio.run(areas.completar_tarea("pintar el balcón"))


def test_las_tareas_de_pago_no_se_marcan_a_mano(datos):
    """Las "💰 Pagar ..." las cierra el sistema de facturas cuando se registra el pago."""
    datos.tareas = [_tarea("\U0001f4b0 Pagar Movistar")]
    r = asyncio.run(areas.completar_tarea("pagar movistar"))
    assert "no se marca a mano" in r and datos.tareas[0].status == "Sin empezar"
    assert asyncio.run(areas.crear_tarea("\U0001f4b0 Pagar algo")).startswith("ERROR")


def test_consultar_tareas_separa_las_propias_de_los_pagos(datos):
    hoy = state.now_argentina().date()
    datos.tareas = [_tarea("Renovar el carnet", (hoy + timedelta(days=1)).isoformat()),
                    _tarea("Llamar al plomero", (hoy - timedelta(days=2)).isoformat()),
                    _tarea("\U0001f4b0 Pagar Movistar", hoy.isoformat())]
    r = asyncio.run(areas.consultar_tareas())
    lineas = r.split("\n")
    assert lineas[1].startswith("- Llamar al plomero") and "venció" in lineas[1]     # la más vieja primero
    assert "para mañana" in lineas[2]
    assert "1 pagos pendientes" in r and "Pagar Movistar" not in r
    assert "Pagar Movistar" in asyncio.run(areas.consultar_tareas(incluir_pagos=True))


def test_tareas_del_saludo_solo_vencidas_o_de_hoy(datos):
    hoy = date(2026, 11, 15)
    datos.tareas = [_tarea("Para hoy", "2026-11-15"), _tarea("Vencida", "2026-11-12"),
                    _tarea("Futura", "2026-11-20"), _tarea("Sin fecha"),
                    _tarea("\U0001f4b0 Pagar Luz", "2026-11-15")]
    lineas = asyncio.run(areas.tareas_para_el_saludo(hoy))
    assert lineas == ["- Vencida — venció el 12/11", "- Para hoy"]


def test_lecturas_de_otras_areas(datos):
    compras = asyncio.run(areas.ejecutar("consultar_lista_compras", {}))
    assert "FALTA COMPRAR (3)" in compras and "Lacteo: Leche, Queso" in compras and "Yerba (2 kg)" in compras
    plantas = asyncio.run(areas.ejecutar("consultar_plantas", {}))
    assert "Potus" in plantas and "hace 4 días" in plantas
    assert "Obra Roca con Ana" in asyncio.run(areas.ejecutar("consultar_reuniones", {}))
    assert "No encontré reuniones" in asyncio.run(areas.ejecutar("consultar_reuniones", {"buscar": "xyz"}))
    assert "Ducato" in asyncio.run(areas.ejecutar("consultar_proyectos", {}))


def test_una_falla_de_datos_se_informa_como_error(datos, monkeypatch):
    async def _falla(*a, **k):
        raise RuntimeError("Notion caído")

    monkeypatch.setattr(datos, "get_pending_tasks", _falla)
    r = asyncio.run(areas.ejecutar("consultar_tareas", {}))
    assert r.startswith("ERROR")                      # el agente no puede afirmar que no hay tareas


def test_herramienta_ajena_no_se_atiende_aca(datos):
    assert asyncio.run(areas.ejecutar("consultar_calendario", {})) is None


def test_las_herramientas_estan_bien_definidas():
    nombres = [h["name"] for h in areas.HERRAMIENTAS]
    assert len(nombres) == len(set(nombres))
    for h in areas.HERRAMIENTAS:
        assert h["description"] and h["input_schema"]["type"] == "object"
