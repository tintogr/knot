"""El cron de recordatorios: un recordatorio atrasado suena igual (antes se perdía),
no suena dos veces y no borra una pregunta de plata pendiente."""
import asyncio
from datetime import timedelta

import pytest

import main
import state


class _Resp:
    def __init__(self, status=200, data=None):
        self.status_code, self._data = status, data or {}

    def json(self):
        return self._data


def _preparar(monkeypatch, eventos, borrado_ok=True):
    """Arma un calendario falso con `eventos` y captura lo que Knot mandaría."""
    enviados, botones, borrados, consultas = [], [], [], []
    ahora = state.now_argentina()

    class _Http:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, headers=None, params=None):
            consultas.append(params)
            return _Resp(200, {"items": eventos})

        async def delete(self, url, headers=None):
            if not borrado_ok:
                raise main.httpx.ConnectError("sin red")
            borrados.append(url.rsplit("/", 1)[-1])
            return _Resp(204)

    async def _nada(*a, **k):
        return None

    async def _token():
        return "token"

    async def _send(to, text, descartable=False):
        enviados.append(text)
        return True

    async def _botones(to, body, buttons, header=None):
        botones.append(body)
        return True

    monkeypatch.setattr(main.httpx, "AsyncClient", _Http)
    monkeypatch.setattr(main, "load_user_config", _nada)
    monkeypatch.setattr(main, "save_user_config", _nada)
    monkeypatch.setattr(main, "get_gcal_access_token", _token)
    monkeypatch.setattr(main, "send_message", _send)
    monkeypatch.setattr(main, "send_interactive_buttons", _botones)
    # Que no se disparen el saludo ni el resumen nocturno durante la prueba.
    state._last_summary_sent["daily"] = ahora
    state._last_summary_sent["nocturno"] = ahora
    main._recordatorios_disparados.clear()
    state.pending_state.pop(state.MY_NUMBER, None)
    return enviados, botones, borrados, consultas


def _evento(eid, titulo, minutos_desde_ahora, desc="[TEMP]"):
    inicio = state.now_argentina() + timedelta(minutes=minutos_desde_ahora)
    return {"id": eid, "summary": f"🔔 {titulo}", "description": desc,
            "start": {"dateTime": inicio.strftime("%Y-%m-%dT%H:%M:00-03:00")}}


def test_recordatorio_en_hora_suena_y_se_borra(monkeypatch):
    enviados, botones, borrados, _ = _preparar(monkeypatch, [_evento("e1", "Tomar la pastilla", 0)])
    asyncio.run(main._cron_job_inner())
    assert len(enviados) == 1 and "Tomar la pastilla" in enviados[0]
    assert "atrasé" not in enviados[0]
    assert borrados == ["e1"]
    assert len(botones) == 1                                   # ofrece posponer
    assert state.pending_state[state.MY_NUMBER]["type"] == "snooze"


def test_recordatorio_atrasado_suena_igual_y_avisa_la_hora(monkeypatch):
    """Antes: si el cron llegaba 2 minutos tarde (un deploy, el saludo leyendo facturas)
    el recordatorio no sonaba nunca."""
    enviados, _, borrados, consultas = _preparar(monkeypatch, [_evento("e2", "Llamar al contador", -25)])
    asyncio.run(main._cron_job_inner())
    assert len(enviados) == 1 and "Llamar al contador" in enviados[0]
    assert "era para las" in enviados[0]
    assert borrados == ["e2"]
    # y la consulta al calendario mira hacia atrás
    desde = consultas[0]["timeMin"]
    assert desde < state.now_argentina().strftime("%Y-%m-%dT%H:%M:00-03:00")


def test_recordatorio_futuro_todavia_no_suena(monkeypatch):
    enviados, _, borrados, _ = _preparar(monkeypatch, [_evento("e3", "Reunión", 30)])
    asyncio.run(main._cron_job_inner())
    assert enviados == [] and borrados == []


def test_un_evento_comun_del_pasado_no_suena(monkeypatch):
    enviados, _, borrados, _ = _preparar(monkeypatch, [_evento("e4", "Turno dentista", -40, desc="")])
    asyncio.run(main._cron_job_inner())
    assert enviados == [] and borrados == []


def test_no_suena_dos_veces_aunque_falle_el_borrado(monkeypatch):
    enviados, _, _, _ = _preparar(monkeypatch, [_evento("e5", "Sacar la basura", -1)], borrado_ok=False)
    asyncio.run(main._cron_job_inner())
    asyncio.run(main._cron_job_inner())       # siguiente minuto: el evento sigue en el calendario
    assert len(enviados) == 1


def test_un_recordatorio_no_borra_una_pregunta_de_plata(monkeypatch):
    enviados, botones, _, _ = _preparar(monkeypatch, [_evento("e6", "Regar", 0)])
    state.pending_state[state.MY_NUMBER] = {"type": "confirm_factura_paid", "factura": "Movistar"}
    asyncio.run(main._cron_job_inner())
    assert len(enviados) == 1                                  # el recordatorio suena
    assert botones == []                                       # pero no pisa la pregunta
    assert state.pending_state[state.MY_NUMBER]["type"] == "confirm_factura_paid"
    state.pending_state.pop(state.MY_NUMBER, None)


def test_si_whatsapp_no_acepta_el_envio_se_reintenta(monkeypatch):
    enviados, _, borrados, _ = _preparar(monkeypatch, [_evento("e7", "Pagar el gas", 0)])

    async def _falla(to, text, descartable=False):
        return False

    monkeypatch.setattr(main, "send_message", _falla)
    asyncio.run(main._cron_job_inner())
    assert borrados == []                                      # sigue en el calendario
    assert "e7" not in main._recordatorios_disparados          # y se va a reintentar
