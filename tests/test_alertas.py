"""Si WhatsApp no anda, Knot avisa por otro canal (evento con alarma en Calendar y,
si el permiso lo deja, un mail). Como mucho un aviso cada unas horas, nunca por WhatsApp
y sin tirar excepciones."""
import asyncio
import types

import alertas


class _Http:
    def __init__(self, registro, mail_ok=True):
        self.registro, self.mail_ok = registro, mail_ok

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, url, headers=None):
        self.registro.append(("GET", url))
        return types.SimpleNamespace(status_code=200, json=lambda: {"emailAddress": "yo@ejemplo.com"}, text="")

    async def post(self, url, headers=None, json=None):
        self.registro.append(("POST", url, json))
        ok = "calendar" in url or self.mail_ok
        return types.SimpleNamespace(status_code=200 if ok else 403, text="")


def _preparar(monkeypatch, registro, mail_ok=True, token="tok"):
    async def _token():
        return token
    monkeypatch.setattr(alertas, "get_gcal_access_token", _token)
    monkeypatch.setattr(alertas.httpx, "AsyncClient", lambda timeout=15: _Http(registro, mail_ok))
    monkeypatch.setitem(alertas._ultimo_aviso, "cuando", None)


def test_avisa_con_evento_y_mail_una_sola_vez(monkeypatch):
    registro = []
    _preparar(monkeypatch, registro)
    assert asyncio.run(alertas.avisar_whatsapp_caido("token vencido")) is True
    assert asyncio.run(alertas.avisar_whatsapp_caido("token vencido")) is False   # no repite
    posts = [r for r in registro if r[0] == "POST"]
    assert len(posts) == 2
    evento = next(p[2] for p in posts if "calendar" in p[1])
    assert "[TEMP]" not in evento["description"]          # si no, el cron lo mandaría por WhatsApp
    assert evento["reminders"]["overrides"][0]["minutes"] == 0


def test_sin_permiso_de_mail_alcanza_con_el_evento(monkeypatch):
    registro = []
    _preparar(monkeypatch, registro, mail_ok=False)
    assert asyncio.run(alertas.avisar_whatsapp_caido("x")) is True


def test_sin_google_no_rompe(monkeypatch):
    registro = []
    _preparar(monkeypatch, registro, token=None)
    assert asyncio.run(alertas.avisar_whatsapp_caido("x")) is False
    assert registro == []
