"""La pagina de privacidad es publica: tiene que responder y no mostrar datos del usuario."""
import asyncio
import os

import paginas


def test_privacidad_responde_html():
    resp = asyncio.run(paginas.privacidad())
    assert resp.status_code == 200
    assert "privacidad" in resp.body.decode("utf-8").lower()


def test_privacidad_no_muestra_datos_del_usuario():
    html = paginas.PRIVACIDAD_HTML
    assert os.environ["MY_WA_NUMBER"] not in html
    assert "@" not in html          # ningun mail
    assert "Martin" not in html


def test_rutas_registradas():
    rutas = {r.path for r in paginas.router.routes}
    assert {"/privacidad", "/privacy"} <= rutas
