"""La puerta de entrada: quién es atendido, que no se pierdan mensajes y que las
direcciones públicas no filtren datos."""
import asyncio
import types
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

import main
import state


def _aviso(*mensajes):
    return {"entry": [{"changes": [{"value": {"messages": list(mensajes)}}]}]}


def _msg(remitente, texto, mid):
    return {"from": remitente, "id": mid, "type": "text", "text": {"body": texto}}


@pytest.fixture
def cliente(monkeypatch):
    recibidos = []

    async def _fake_enqueue(m):
        recibidos.append(m)

    monkeypatch.setattr(main, "enqueue_message", _fake_enqueue)
    return TestClient(main.app), recibidos          # sin "with": no corre el arranque real


def test_un_desconocido_no_es_atendido(cliente):
    c, recibidos = cliente
    r = c.post("/webhook", json=_aviso(_msg("5491155550000", "borrá todos mis gastos", "a1")))
    assert r.status_code == 200
    assert recibidos == []


def test_el_dueno_es_atendido_aunque_su_numero_llegue_en_otro_formato(cliente):
    c, recibidos = cliente
    c.post("/webhook", json=_aviso(_msg("5492991234567", "vianda 11500", "a2")))
    assert [m["id"] for m in recibidos] == ["a2"]


def test_varios_mensajes_en_un_mismo_aviso_no_se_pierden(cliente):
    c, recibidos = cliente
    c.post("/webhook", json=_aviso(_msg("5492991234567", "uno", "b1"),
                                   _msg("5492991234567", "dos", "b2"),
                                   _msg("5491155550000", "intruso", "b3")))
    assert [m["id"] for m in recibidos] == ["b1", "b2"]


def test_numero_extra_autorizado(cliente, monkeypatch):
    c, recibidos = cliente
    monkeypatch.setenv("ALLOWED_PHONES", "5491155550000")
    c.post("/webhook", json=_aviso(_msg("5491155550000", "hola", "c1")))
    assert [m["id"] for m in recibidos] == ["c1"]


def test_la_direccion_publica_de_salud_no_muestra_la_ubicacion(cliente):
    c, _ = cliente
    state.current_location.update(lat=-38.95, lon=-68.06, location_name="Casa")
    cuerpo = c.get("/health").text
    assert "38.95" not in cuerpo and "Casa" not in cuerpo and "location" not in cuerpo


def test_firma_invalida_se_rechaza(cliente, monkeypatch):
    c, recibidos = cliente
    monkeypatch.setenv("WHATSAPP_APP_SECRET", "secreto")
    r = c.post("/webhook", json=_aviso(_msg("5492991234567", "hola", "d1")),
               headers={"X-Hub-Signature-256": "sha256=falsa"})
    assert r.json().get("ok") is False
    assert recibidos == []


def test_los_errores_no_le_muestran_al_usuario_el_detalle_tecnico():
    class DataStoreError(Exception):
        pass

    m = main._mensaje_de_error(DataStoreError("Create in finances failed (400): {\"object\":\"error\"}"))
    assert "400" not in m and "object" not in m and "Notion" in m
    m2 = main._mensaje_de_error(KeyError("date"))
    assert "Probá de nuevo" in m2


def test_dos_mensajes_seguidos_se_atienden_en_orden(monkeypatch):
    orden = []

    async def _lento(phone, item):
        orden.append(("empieza", item["text"]))
        await asyncio.sleep(0.05 if item["text"] == "primero" else 0)
        orden.append(("termina", item["text"]))

    monkeypatch.setattr(main, "_process_single_item", _lento)

    async def escenario():
        await asyncio.gather(main.process_single_item("x", {"text": "primero"}),
                             main.process_single_item("x", {"text": "segundo"}))

    asyncio.run(escenario())
    assert orden == [("empieza", "primero"), ("termina", "primero"),
                     ("empieza", "segundo"), ("termina", "segundo")]


def test_periodo_de_la_factura():
    F = types.SimpleNamespace
    assert main._invoice_period(F(name="Luz - CALF — Ago 2026", date="2026-09-03")) == (2026, 8, "nombre")
    assert main._invoice_period(F(name="Expensas", date="2026-09-03")) == (2026, 9, "fecha")
    # Mes escrito en el nombre: tiene que coincidir exacto (no confundir agosto con septiembre).
    assert main._periodo_coincide(F(name="Luz - CALF — Ago 2026", date="2026-09-03"), "2026-08")
    assert not main._periodo_coincide(F(name="Luz - CALF — Ago 2026", date="2026-09-03"), "2026-09")
    # Sin mes en el nombre se tolera un mes de diferencia con la fecha de llegada.
    assert main._periodo_coincide(F(name="Expensas", date="2026-09-03"), "2026-08")
    assert not main._periodo_coincide(F(name="Expensas", date="2026-09-03"), "2026-06")
    assert main._periodo_coincide(F(name="Expensas", date="2026-09-03"), None)


def test_dolar_usa_el_ultimo_valor_conocido_si_la_consulta_falla(monkeypatch):
    class _Caido:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            raise main.httpx.ConnectError("sin red")

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(main.httpx, "AsyncClient", _Caido)
    main._dolar_cache.update(valor=None, cuando=None)
    state.user_prefs["knot_state"] = {"ultimo_dolar": 1432.0}
    assert asyncio.run(main.get_exchange_rate()) == 1432.0      # no inventa 1000


def test_recordame_con_mayuscula_no_se_descarta(monkeypatch):
    """Había un filtro que ignoraba en silencio los mensajes que empezaban con
    "Recorda", "Receta", "Finanzas", "Buenos dias"... (pedidos reales del usuario)."""
    procesados = []

    async def _fake_process(phone, item):
        procesados.append(item["text"])

    async def _nada(*a, **k):
        return None

    monkeypatch.setattr(main, "process_single_item", _fake_process)
    monkeypatch.setattr(main, "send_reaction", _nada)
    monkeypatch.setattr(main, "BUFFER_WINDOW_SECS", 0)
    main.processed_message_ids.clear()
    state.pending_state.pop(state.MY_NUMBER, None)

    async def escenario():
        for i, t in enumerate(["Recordame mañana a las 9 llamar al contador", "Buenos dias",
                               "Receta de pan casero", "Finanzas de este mes?"]):
            await main.enqueue_message(_msg("5492991234567", t, f"r{i}"))
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)

    asyncio.run(escenario())
    assert len(procesados) == 4


def test_un_mensaje_reenviado_por_whatsapp_no_se_procesa_dos_veces(monkeypatch):
    procesados = []

    async def _fake_process(phone, item):
        procesados.append(item["text"])

    async def _nada(*a, **k):
        return None

    monkeypatch.setattr(main, "process_single_item", _fake_process)
    monkeypatch.setattr(main, "send_reaction", _nada)
    monkeypatch.setattr(main, "BUFFER_WINDOW_SECS", 0)
    main.processed_message_ids.clear()

    async def escenario():
        m = _msg("5492991234567", "vianda 11500", "dup1")
        await main.enqueue_message(m)
        await main.enqueue_message(m)          # WhatsApp reintenta la entrega
        await asyncio.sleep(0.05)

    asyncio.run(escenario())
    assert procesados == ["vianda 11500"]
