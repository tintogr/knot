"""Llamadas al modelo: no frenan al servidor, reintentan lo transitorio y usan un
modelo de respaldo si el configurado fue retirado."""
import asyncio
import time
import types

import pytest

import state


class _Err(Exception):
    def __init__(self, status, msg=""):
        super().__init__(msg)
        self.status_code = status


def _poner_cliente(monkeypatch, create):
    monkeypatch.setattr(state, "anthropic", types.SimpleNamespace(messages=types.SimpleNamespace(create=create)))

    async def _sin_espera(_):
        return None
    monkeypatch.setattr(state.asyncio, "sleep", _sin_espera)


def test_reintenta_si_la_api_esta_saturada(monkeypatch):
    llamadas = []

    def create(**kw):
        llamadas.append(kw)
        if len(llamadas) < 3:
            raise _Err(529, "overloaded")
        return "ok"

    _poner_cliente(monkeypatch, create)
    assert asyncio.run(state.claude_create(max_tokens=5, messages=[])) == "ok"
    assert len(llamadas) == 3


def test_no_reintenta_un_pedido_mal_armado(monkeypatch):
    llamadas = []

    def create(**kw):
        llamadas.append(kw)
        raise _Err(400, "invalid request")

    _poner_cliente(monkeypatch, create)
    with pytest.raises(_Err):
        asyncio.run(state.claude_create(max_tokens=5, messages=[]))
    assert len(llamadas) == 1


def test_modelo_retirado_usa_el_de_respaldo(monkeypatch):
    modelos = []

    def create(**kw):
        modelos.append(kw["model"])
        if kw["model"] == state.SONNET_MODEL:
            raise _Err(404, "model: not_found_error")
        return "ok"

    _poner_cliente(monkeypatch, create)
    assert asyncio.run(state.claude_create(max_tokens=5, messages=[])) == "ok"
    assert modelos == [state.SONNET_MODEL, state.MODELOS_DE_RESPALDO[state.SONNET_MODEL]]


def test_el_servidor_sigue_respondiendo_mientras_el_modelo_piensa(monkeypatch):
    def create(**kw):
        time.sleep(0.3)          # el modelo "piensa" (llamada bloqueante)
        return "ok"

    monkeypatch.setattr(state, "anthropic", types.SimpleNamespace(messages=types.SimpleNamespace(create=create)))

    async def escenario():
        latidos = 0

        async def latir():
            nonlocal latidos
            while True:
                await asyncio.sleep(0.02)
                latidos += 1

        t = asyncio.create_task(latir())
        await state.claude_create(max_tokens=5, messages=[])
        t.cancel()
        return latidos

    # Si la llamada congelara el servidor, no habría ningún latido durante la espera.
    assert asyncio.run(escenario()) >= 5
