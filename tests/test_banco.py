"""El banco de conversaciones corre contra el modelo real y por eso no entra en estas
pruebas. Acá se prueba el banco en sí, con un modelo de mentira: que atienda el mensaje
con Knot entero, que anote lo que pasó, que no toque nada real y que deje todo como estaba."""
import asyncio
import json
import types

import pytest

import main
import state
from banco import casos, correr

N = types.SimpleNamespace


def _modelo_que_contesta(respuesta_json: dict):
    async def _falso(**kw):
        return N(content=[N(type="text", text=json.dumps(respuesta_json))], stop_reason="end_turn")
    return _falso


def test_los_casos_estan_bien_armados():
    nombres = [c["nombre"] for c in casos.CASOS]
    assert len(nombres) == len(set(nombres)), "hay dos casos con el mismo nombre"
    assert len(casos.CASOS) >= 25
    for c in casos.CASOS:
        assert c["mensajes"] and all(isinstance(m, str) and m.strip() for m in c["mensajes"])
        assert c["espera"] and all(callable(e) for e in c["espera"])
        assert c.get("origen")


def test_corre_un_caso_con_knot_entero_y_restaura_todo(monkeypatch):
    monkeypatch.setattr(main, "claude_create", _modelo_que_contesta(
        {"modulo": None, "mensaje": "hola", "respuesta": "Hola, ¿qué necesitás?", "otras": []}))
    ds_real, enviar_real = main._ds, main.send_message
    caso = {"nombre": "prueba", "mensajes": ["hola"], "origen": "prueba",
            "espera": [casos.contesto(), casos.ruta("(contestó solo)"), casos.no_toco_la_plata()]}
    r = asyncio.run(correr.correr_caso(caso))
    assert r["ok"], r
    assert "¿qué necesitás?" in r["dicho"]
    assert main._ds is ds_real and main.send_message is enviar_real


def test_un_caso_que_no_cumple_queda_marcado(monkeypatch):
    monkeypatch.setattr(main, "claude_create", _modelo_que_contesta(
        {"modulo": None, "mensaje": "vianda 11500", "respuesta": "Listo.", "otras": []}))
    caso = {"nombre": "prueba", "mensajes": ["vianda 11500"], "origen": "prueba",
            "espera": [casos.creo_movimiento("vianda", 11500)]}
    r = asyncio.run(correr.correr_caso(caso))
    assert not r["ok"]
    assert "registró: ninguno" in r["comprobaciones"][0][1]


def test_un_error_no_frena_el_banco(monkeypatch):
    async def _rompe(**kw):
        raise RuntimeError("sin conexión")
    monkeypatch.setattr(main, "claude_create", _rompe)
    caso = {"nombre": "prueba", "mensajes": ["hola"], "origen": "prueba", "espera": [casos.contesto()]}
    r = asyncio.run(correr.correr_caso(caso))
    # Knot atrapa el error y avisa; el banco no se cae.
    assert isinstance(r["ok"], bool)


def test_arranca_con_la_conversacion_y_la_pregunta_pendiente(monkeypatch):
    visto = {}

    async def _falso(**kw):
        visto["historial"] = list(state.get_history(correr.TELEFONO))
        visto["pendiente"] = state.pending_state.get(correr.TELEFONO)
        return N(content=[N(type="text", text=json.dumps(
            {"modulo": None, "mensaje": "x", "respuesta": "ok", "otras": []}))], stop_reason="end_turn")

    monkeypatch.setattr(main, "claude_create", _falso)
    caso = {"nombre": "prueba", "mensajes": ["dale"], "origen": "prueba", "espera": [casos.contesto()],
            "previo": [("user", "vianda 11500"), ("assistant", "✅ Vianda — $11,500")]}
    asyncio.run(correr.correr_caso(caso))
    assert [m["content"] for m in visto["historial"]][:2] == ["vianda 11500", "✅ Vianda — $11,500"]


def test_sin_clave_no_corre(monkeypatch, capsys):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    monkeypatch.setattr(correr.sys, "argv", ["correr"])
    assert correr.principal() == 2
    assert "ANTHROPIC_API_KEY" in capsys.readouterr().out


def test_el_mundo_falso_no_deja_salir_a_internet():
    import os
    for clave in ("GCAL_REFRESH_TOKEN", "GCAL_CLIENT_ID", "GCAL_CLIENT_SECRET"):
        assert clave not in os.environ
