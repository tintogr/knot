"""Registro del número de WhatsApp: cuando Meta contesta "Account not registered"
(133010), Knot registra el número solo (si tiene el PIN) y reintenta el mensaje."""
import asyncio
import types

import pytest

import wa_utils


class _Resp:
    def __init__(self, status, cuerpo=None, texto=None):
        self.status_code = status
        self._cuerpo = cuerpo or {}
        self.text = texto if texto is not None else str(self._cuerpo)

    def json(self):
        return self._cuerpo


NO_REGISTRADO = _Resp(400, texto='{"error":{"message":"(#133010) Account not registered","code":133010}}')
FUERA_DE_LISTA = _Resp(400, texto='{"error":{"message":"(#131030) Recipient phone number not in allowed list","code":131030}}')
ENVIADO = _Resp(200, {"messages": [{"id": "wamid.1"}]})


class _Cliente:
    """Cliente HTTP de mentira: anota los pedidos y contesta lo que se le programó."""

    def __init__(self, mensajes, registro=None, estado=None, acepta=None):
        self.acepta = acepta                  # único formato de número que Meta acepta
        self.mensajes = list(mensajes)        # respuestas a /messages, en orden
        self.registro = registro or _Resp(200, {"success": True})
        self.estado = estado
        self.pedidos = []
        self.is_closed = False

    async def post(self, url, headers=None, json=None):
        self.pedidos.append(("POST", url, json))
        if url.endswith("/register"):
            return self.registro
        if url.endswith("/subscribed_apps"):
            return _Resp(200, {"success": True})
        if self.acepta is not None:
            return ENVIADO if (json or {}).get("to") == self.acepta else FUERA_DE_LISTA
        return self.mensajes.pop(0)

    async def get(self, url, headers=None, params=None):
        self.pedidos.append(("GET", url, params))
        return self.estado

    def a(self, sufijo):
        return [p for p in self.pedidos if p[1].endswith(sufijo)]


@pytest.fixture
def poner(monkeypatch):
    monkeypatch.setattr(wa_utils, "_registro", {"intentado": False})
    monkeypatch.setattr(wa_utils, "_destino_que_anda", {})
    monkeypatch.setattr(wa_utils, "add_to_history", lambda *a, **k: None)
    monkeypatch.delenv("WHATSAPP_PIN", raising=False)
    monkeypatch.delenv("WHATSAPP_WABA_ID", raising=False)

    def _poner(cliente):
        monkeypatch.setattr(wa_utils, "_cliente", lambda: cliente)
        return cliente
    return _poner


def test_registra_el_numero_y_reintenta_el_mensaje(poner, monkeypatch):
    monkeypatch.setenv("WHATSAPP_PIN", "123456")
    monkeypatch.setenv("WHATSAPP_WABA_ID", "2051088002261169")
    c = poner(_Cliente([NO_REGISTRADO, ENVIADO]))
    assert asyncio.run(wa_utils.send_message("5491100000000", "hola")) is True
    assert len(c.a("/register")) == 1
    assert c.a("/register")[0][2] == {"messaging_product": "whatsapp", "pin": "123456"}
    assert len(c.a("/2051088002261169/subscribed_apps")) == 1
    assert len(c.a("/messages")) == 2


def test_sin_pin_no_registra_y_avisa_en_el_log(poner, capsys):
    c = poner(_Cliente([NO_REGISTRADO]))
    assert asyncio.run(wa_utils.send_message("5491100000000", "hola")) is False
    assert c.a("/register") == []
    assert "WHATSAPP_PIN" in capsys.readouterr().out


def test_un_pin_mal_escrito_no_se_manda(poner, monkeypatch):
    monkeypatch.setenv("WHATSAPP_PIN", "12ab")
    c = poner(_Cliente([NO_REGISTRADO]))
    assert asyncio.run(wa_utils.send_message("5491100000000", "hola")) is False
    assert c.a("/register") == []


def test_solo_intenta_registrar_una_vez(poner, monkeypatch):
    # Meta permite 10 registros cada 72 horas: si lo rechaza no hay que insistir.
    monkeypatch.setenv("WHATSAPP_PIN", "123456")
    c = poner(_Cliente([NO_REGISTRADO, NO_REGISTRADO], registro=_Resp(400, texto="rechazado")))
    assert asyncio.run(wa_utils.send_message("5491100000000", "uno")) is False
    assert asyncio.run(wa_utils.send_message("5491100000000", "dos")) is False
    assert len(c.a("/register")) == 1


def test_otro_error_no_dispara_el_registro(poner, monkeypatch):
    monkeypatch.setenv("WHATSAPP_PIN", "123456")
    c = poner(_Cliente([_Resp(400, texto='{"error":{"code":100,"message":"does not exist"}}')]))
    assert asyncio.run(wa_utils.send_message("5491100000000", "hola")) is False
    assert c.a("/register") == []


def test_al_arrancar_registra_si_esta_pendiente(poner, monkeypatch):
    monkeypatch.setenv("WHATSAPP_PIN", "123456")
    c = poner(_Cliente([], estado=_Resp(200, {"display_phone_number": "+1 555", "status": "PENDING"})))
    asyncio.run(wa_utils.revisar_numero_al_arrancar())
    assert len(c.a("/register")) == 1


def test_al_arrancar_no_toca_un_numero_conectado(poner, monkeypatch):
    monkeypatch.setenv("WHATSAPP_PIN", "123456")
    c = poner(_Cliente([], estado=_Resp(200, {"display_phone_number": "+1 555", "status": "CONNECTED"})))
    asyncio.run(wa_utils.revisar_numero_al_arrancar())
    assert c.a("/register") == []


def test_al_arrancar_sin_pin_no_consulta_nada(poner):
    c = poner(_Cliente([]))
    asyncio.run(wa_utils.revisar_numero_al_arrancar())
    assert c.pedidos == []


def test_si_no_puede_leer_el_estado_no_registra_a_ciegas(poner, monkeypatch):
    monkeypatch.setenv("WHATSAPP_PIN", "123456")
    c = poner(_Cliente([], estado=_Resp(400, texto="sin permiso")))
    asyncio.run(wa_utils.revisar_numero_al_arrancar())
    assert c.a("/register") == []


# ── Formato del número de destino ─────────────────────────────────────────────

@pytest.mark.parametrize("escrito", ["54299155551234", "5492995551234"])
def test_si_no_esta_en_la_lista_prueba_las_otras_formas_y_la_recuerda(poner, escrito):
    c = poner(_Cliente([], acepta="542995551234"))
    assert asyncio.run(wa_utils.send_message(escrito, "hola")) is True
    antes = len(c.a("/messages"))
    assert asyncio.run(wa_utils.send_message(escrito, "otra")) is True
    assert len(c.a("/messages")) == antes + 1          # la segunda va directo a la que anda
    assert c.a("/messages")[-1][2]["to"] == "542995551234"


def test_si_ninguna_forma_anda_avisa_y_no_insiste_de_mas(poner):
    c = poner(_Cliente([], acepta="000"))
    assert asyncio.run(wa_utils.send_message("54299155551234", "hola")) is False
    assert len(c.a("/messages")) == 3


def test_un_numero_de_otro_pais_no_se_toca(poner):
    c = poner(_Cliente([], acepta="000"))
    assert asyncio.run(wa_utils.send_message("15551234567", "hola")) is False
    assert len(c.a("/messages")) == 1


def test_las_otras_formas_de_un_celular_argentino():
    assert wa_utils._otras_formas("54299155551234") == ["542995551234", "5492995551234"]
    assert wa_utils._otras_formas("5492995551234") == ["542995551234"]
    assert wa_utils._otras_formas("542995551234") == ["5492995551234"]
    assert wa_utils._otras_formas("541115551234") == ["5491115551234"]
