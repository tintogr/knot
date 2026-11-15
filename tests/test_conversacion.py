"""Un mensaje con varios pedidos, la conversación que sobrevive a los reinicios y la
confirmación antes de borrar."""
import asyncio
import json
import types
from datetime import timedelta

import pytest

import main
import state

TEL = state.MY_NUMBER


@pytest.fixture(autouse=True)
def _limpio(monkeypatch):
    state.chat_history.clear()
    state.pending_state.pop(TEL, None)
    main._acciones_extra.clear()
    main._conv_guardada["contenido"] = None
    monkeypatch.setitem(state.user_prefs, "_config_page_id", "pag")
    yield
    state.pending_state.pop(TEL, None)


# ── Varios pedidos en un mismo mensaje ────────────────────────────────────────

def test_un_mensaje_con_dos_pedidos_atiende_los_dos(monkeypatch):
    atendidos = []

    async def _fake(phone, item):
        atendidos.append((item.get("_modulo"), item["text"]))
        if not item.get("_modulo"):                 # el primero: el agente de entrada detecta el segundo
            main._acciones_extra[phone] = [{"modulo": "RECORDATORIO", "mensaje": "recordame llamar al contador mañana a las 10"}]

    monkeypatch.setattr(main, "_process_single_item", _fake)
    monkeypatch.setattr(main, "_guardar_conversacion_en_segundo_plano", lambda: None)
    asyncio.run(main.process_single_item(TEL, {"text": "pagué el super 45 mil y recordame llamar al contador mañana a las 10"}))
    assert atendidos == [(None, "pagué el super 45 mil y recordame llamar al contador mañana a las 10"),
                         ("RECORDATORIO", "recordame llamar al contador mañana a las 10")]


def test_el_segundo_pedido_no_pisa_una_pregunta_de_plata(monkeypatch):
    async def _fake(phone, item):
        if not item.get("_modulo"):
            state.pending_state[phone] = {"type": "ask_payment_method", "page_id": "g1"}
            main._acciones_extra[phone] = [{"modulo": "RECORDATORIO", "mensaje": "recordame X"}]
        else:
            state.pending_state[phone] = {"type": "reminder_created"}      # el recordatorio deja su propio estado

    monkeypatch.setattr(main, "_process_single_item", _fake)
    monkeypatch.setattr(main, "_guardar_conversacion_en_segundo_plano", lambda: None)
    asyncio.run(main.process_single_item(TEL, {"text": "super 45000 y recordame X"}))
    assert state.pending_state[TEL]["type"] == "ask_payment_method"       # sigue esperando el medio de pago


def test_el_agente_de_entrada_descarta_modulos_inventados(monkeypatch):
    respuesta = {"modulo": "GASTO", "mensaje": "super 45000", "respuesta": None, "imagen": None, "persona": None,
                 "otras": [{"modulo": "RECORDATORIO", "mensaje": "recordame llamar"},
                           {"modulo": "MODULO_QUE_NO_EXISTE", "mensaje": "algo"},
                           {"modulo": "TAREA", "mensaje": ""}]}

    async def _modelo(**kw):
        return types.SimpleNamespace(content=[types.SimpleNamespace(type="text", text=json.dumps(respuesta))])

    monkeypatch.setattr(main, "claude_create", _modelo)
    r = asyncio.run(main._router_agent(TEL, "super 45000 y recordame llamar", False))
    assert r["modulo"] == "GASTO"
    assert r["otras"] == [{"modulo": "RECORDATORIO", "mensaje": "recordame llamar"}]


def test_tarea_es_un_modulo_valido():
    assert "TAREA" in main._MODULOS_VALIDOS


# ── La conversación sobrevive a un reinicio ───────────────────────────────────

class _Config:
    """Página de configuración falsa: guarda el texto como lo haría Notion."""

    def __init__(self):
        self.texto = ""
        self.guardados = 0

    async def guardar_texto_config(self, page_id, campo, texto):
        self.texto, self.guardados = texto, self.guardados + 1
        return True

    async def leer_texto_config(self, page_id, campo):
        return self.texto


def test_la_conversacion_y_la_pregunta_pendiente_vuelven_tras_un_reinicio(monkeypatch):
    cfg = _Config()
    monkeypatch.setattr(main, "_ds", cfg)
    state.add_to_history(TEL, "user", "vianda 11500")
    state.add_to_history(TEL, "assistant", "✅ Vianda — $11.500. ¿Con qué pagaste?")
    state.pending_state[TEL] = {"type": "ask_payment_method", "page_id": "g1"}
    assert asyncio.run(main.guardar_conversacion()) is True

    # reinicio: se pierde todo lo que estaba en memoria
    state.chat_history.clear()
    state.pending_state.pop(TEL, None)

    assert asyncio.run(main.restaurar_conversacion()) == "historial+pendiente"
    assert [m["content"] for m in state.get_history(TEL)] == ["vianda 11500", "✅ Vianda — $11.500. ¿Con qué pagaste?"]
    assert state.pending_state[TEL] == {"type": "ask_payment_method", "page_id": "g1"}


def test_no_se_guarda_de_nuevo_si_nada_cambio(monkeypatch):
    cfg = _Config()
    monkeypatch.setattr(main, "_ds", cfg)
    state.add_to_history(TEL, "user", "hola")
    asyncio.run(main.guardar_conversacion())
    asyncio.run(main.guardar_conversacion())
    assert cfg.guardados == 1
    state.add_to_history(TEL, "assistant", "¿Qué necesitás?")
    asyncio.run(main.guardar_conversacion())
    assert cfg.guardados == 2


def test_una_pregunta_vieja_no_se_recupera_pero_la_charla_si(monkeypatch):
    cfg = _Config()
    monkeypatch.setattr(main, "_ds", cfg)
    hace_2h = (state.now_argentina() - timedelta(hours=2)).replace(tzinfo=None).isoformat()
    cfg.texto = json.dumps({"cuando": hace_2h, "historial": [{"role": "user", "content": "hola"}],
                            "pendiente": {"type": "confirm_delete", "page_id": "x"}})
    assert asyncio.run(main.restaurar_conversacion()) == "historial"
    assert TEL not in state.pending_state


def test_una_conversacion_de_hace_dias_no_se_recupera(monkeypatch):
    cfg = _Config()
    monkeypatch.setattr(main, "_ds", cfg)
    hace_3d = (state.now_argentina() - timedelta(days=3)).replace(tzinfo=None).isoformat()
    cfg.texto = json.dumps({"cuando": hace_3d, "historial": [{"role": "user", "content": "viejo"}], "pendiente": None})
    assert asyncio.run(main.restaurar_conversacion()) == "nada"
    assert state.get_history(TEL) == []


def test_lo_guardado_ilegible_no_rompe_el_arranque(monkeypatch):
    cfg = _Config()
    cfg.texto = "{esto no es json"
    monkeypatch.setattr(main, "_ds", cfg)
    assert asyncio.run(main.restaurar_conversacion()) == "nada"


# ── Confirmación antes de borrar ──────────────────────────────────────────────

def test_si_y_no_escritos_valen_como_los_botones():
    assert main._si_o_no_escrito("Sí") == "confirm_delete_yes"
    assert main._si_o_no_escrito("dale!") == "confirm_delete_yes"
    assert main._si_o_no_escrito("No.") == "confirm_delete_no"
    assert main._si_o_no_escrito("mejor no") == "confirm_delete_no"
    # Una frase que dice otra cosa NO es una confirmación.
    assert main._si_o_no_escrito("sí, pero el otro") is None
    assert main._si_o_no_escrito("no, borrá el de ayer") is None
    assert main._si_o_no_escrito("vianda 11500") is None


def _preparar_compras(monkeypatch, encontrados):
    archivados, preguntas = [], []

    async def _modelo(**kw):
        return types.SimpleNamespace(content=[types.SimpleNamespace(text='{"search_term": "leche"}')])

    async def _buscar(nombre):
        return encontrados

    async def _archivar(item_id):
        archivados.append(item_id)
        return True

    async def _botones(to, body, buttons, header=None):
        preguntas.append(body)
        return True

    monkeypatch.setattr(main, "claude_create", _modelo)
    monkeypatch.setattr(main, "_ds", types.SimpleNamespace(search_shopping_item=_buscar, archive_shopping_item=_archivar))
    monkeypatch.setattr(main, "send_interactive_buttons", _botones)
    return archivados, preguntas


def test_sacar_algo_de_las_compras_pide_confirmacion(monkeypatch):
    archivados, preguntas = _preparar_compras(monkeypatch, [types.SimpleNamespace(id="i1", name="Leche")])
    ok, msg = asyncio.run(main.eliminar_shopping("sacá la leche", TEL))
    assert ok and msg == ""
    assert archivados == []                                   # todavía no borró nada
    assert "Leche" in preguntas[0]
    assert state.pending_state[TEL]["action"] == "shopping"


def test_si_hay_varios_parecidos_pregunta_cual(monkeypatch):
    archivados, preguntas = _preparar_compras(monkeypatch, [types.SimpleNamespace(id="i1", name="Leche de almendras"),
                                                            types.SimpleNamespace(id="i2", name="Leche en polvo")])
    ok, msg = asyncio.run(main.eliminar_shopping("sacá la leche", TEL))
    assert not ok and "Leche de almendras" in msg and "Leche en polvo" in msg
    assert archivados == [] and preguntas == [] and TEL not in state.pending_state


def test_un_si_escrito_confirma_el_borrado_y_un_no_lo_cancela(monkeypatch):
    archivados, enviados = [], []

    async def _archivar(item_id):
        archivados.append(item_id)
        return True

    async def _send(to, text, descartable=False):
        enviados.append(text)
        return True

    monkeypatch.setattr(main, "_ds", types.SimpleNamespace(archive_shopping_item=_archivar))
    monkeypatch.setattr(main, "send_message", _send)
    vence = (state.now_argentina() + timedelta(minutes=5)).replace(tzinfo=None).isoformat()

    estado = {"type": "confirm_delete", "action": "shopping", "page_id": "i1", "name": "Leche", "expires_at": vence}
    state.pending_state[TEL] = dict(estado)
    assert asyncio.run(main.handle_pending_state(TEL, "no", state.pending_state[TEL])) is True
    assert archivados == [] and TEL not in state.pending_state

    state.pending_state[TEL] = dict(estado)
    assert asyncio.run(main.handle_pending_state(TEL, "Sí, dale", state.pending_state[TEL])) is True
    assert archivados == ["i1"] and "Leche" in enviados[-1]
