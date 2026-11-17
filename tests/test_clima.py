"""Clima: open-meteo gratis contesta 429 desde Render. Después de un 429 se deja de
pedirle por unas horas y se usa wttr.in directo."""
import asyncio
import types

import summaries


class _Http:
    def __init__(self, pedidos):
        self.pedidos = pedidos

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, url, params=None):
        self.pedidos.append(url)
        if "open-meteo" in url:
            return types.SimpleNamespace(status_code=429, text="Too many requests")
        return types.SimpleNamespace(status_code=500, text="")


def test_despues_de_un_429_no_vuelve_a_pedirle_a_open_meteo(monkeypatch):
    pedidos = []
    monkeypatch.setattr(summaries.httpx, "AsyncClient", lambda timeout=10: _Http(pedidos))
    monkeypatch.setattr(summaries, "current_location", {"lat": -38.95, "lon": -68.06})
    monkeypatch.setitem(summaries._weather_cache, "data", None)
    monkeypatch.setitem(summaries._openmeteo_pausado_hasta, "t", None)
    monkeypatch.delenv("OPENMETEO_API_KEY", raising=False)

    asyncio.run(summaries.get_weather())
    asyncio.run(summaries.get_weather())
    assert sum("open-meteo" in u for u in pedidos) == 1
    assert sum("wttr.in" in u for u in pedidos) == 2
