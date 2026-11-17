"""Respuestas escritas a las preguntas que deja un evento (¿guardo la ubicación?, ¿se
repite?, ¿te aviso antes?). Antes cada una tenía su lista de palabras: "vianda 11500"
después de "¿guardo la ubicación?" se buscaba en el mapa, y después de "¿con cuánta
anticipación?" se ponía un aviso de 1 hora inventado."""
import asyncio
import json
import types

import pytest

import main
import state

TEL = state.MY_NUMBER


@pytest.fixture(autouse=True)
def _limpio(monkeypatch):
    state.pending_state.pop(TEL, None)
    enviados = []

    async def _send(phone, texto, **kw):
        enviados.append(texto)
        return True

    monkeypatch.setattr(main, "send_message", _send)
    yield enviados
    state.pending_state.pop(TEL, None)


def _modelo_que_dice(monkeypatch, respuesta: dict):
    async def _modelo(**kw):
        return types.SimpleNamespace(content=[types.SimpleNamespace(type="text", text=json.dumps(respuesta))])
    monkeypatch.setattr(main, "claude_create", _modelo)


def test_otro_tema_despues_de_guardar_ubicacion_no_se_busca_en_el_mapa(monkeypatch):
    _modelo_que_dice(monkeypatch, {"tipo": "otro"})
    estado = {"type": "geocode_confirm", "event_id": "e1", "lat": 1, "lon": 2, "place_name": "Club", "raw_location": "club"}
    state.pending_state[TEL] = dict(estado)
    atendido = asyncio.run(main.handle_pending_state(TEL, "vianda 11500", estado))
    assert atendido is False                      # lo atiende el agente como un mensaje nuevo
    assert TEL not in state.pending_state


def test_aviso_de_evento_recurrente_sin_anticipacion_pregunta_y_no_inventa(monkeypatch, _limpio):
    _modelo_que_dice(monkeypatch, {"tipo": "si", "minutos": []})
    estado = {"type": "recurring_event_reminder", "event_id": "e1", "summary": "Funcional"}
    state.pending_state[TEL] = dict(estado)
    atendido = asyncio.run(main.handle_pending_state(TEL, "dale", estado))
    assert atendido is True
    assert "anticipación" in _limpio[-1]
    assert state.pending_state[TEL]["type"] == "recurring_event_reminder"   # sigue esperando


def test_aviso_de_evento_recurrente_otro_tema_suelta_la_pregunta(monkeypatch, _limpio):
    _modelo_que_dice(monkeypatch, {"tipo": "otro"})
    estado = {"type": "recurring_event_reminder", "event_id": "e1", "summary": "Funcional"}
    state.pending_state[TEL] = dict(estado)
    assert asyncio.run(main.handle_pending_state(TEL, "cuánto gasté en vianda?", estado)) is False
    assert TEL not in state.pending_state
    assert _limpio == []                           # no puso ningún aviso


def test_aviso_antes_del_evento_escrito_en_palabras(monkeypatch, _limpio):
    _modelo_que_dice(monkeypatch, {"tipo": "minutos", "minutos": [30]})
    creados = []

    async def _crear(datos):
        creados.append(datos)
        return True, "r1"

    monkeypatch.setattr(main, "create_recordatorio", _crear)
    estado = {"type": "event_reminder", "events": [{"summary": "Dentista", "event_datetime": "2099-01-10T10:00"}]}
    state.pending_state[TEL] = dict(estado)
    assert asyncio.run(main.handle_pending_state(TEL, "avisame media hora antes", estado)) is True
    assert creados and creados[0]["fire_at"] == "2099-01-10T09:30"


def test_si_escrito_a_se_repite_no_se_pierde(monkeypatch):
    _modelo_que_dice(monkeypatch, {"tipo": "si"})
    llamado = {}

    class _Http:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def patch(self, url, headers=None, params=None, json=None):
            llamado["json"] = json
            return types.SimpleNamespace(status_code=200)

    async def _token():
        return "tok"

    monkeypatch.setattr(main, "get_gcal_access_token", _token)
    monkeypatch.setattr(main.httpx, "AsyncClient", lambda *a, **k: _Http())
    estado = {"type": "recurrence_offer", "event_id": "e1", "summary": "Funcional", "rrule_day": "TU"}
    state.pending_state[TEL] = dict(estado)
    assert asyncio.run(main.handle_pending_state(TEL, "sí, todas las semanas", estado)) is True
    assert llamado["json"] == {"recurrence": ["RRULE:FREQ=WEEKLY;BYDAY=TU"]}
