"""Paginas publicas de Knot (no piden clave y no muestran ningun dato del usuario).

Meta exige un link a una politica de privacidad para publicar la app de WhatsApp;
sin publicar no entrega los mensajes reales al webhook.
"""
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

PRIVACIDAD_HTML = """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Knot - Politica de privacidad</title>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; line-height: 1.6;
         max-width: 40rem; margin: 0 auto; padding: 2rem 1rem; color: #1a1a1a; background: #fff; }
  h1 { font-size: 1.5rem; }
  h2 { font-size: 1.1rem; margin-top: 2rem; }
</style>
</head>
<body>
<h1>Knot - Pol&iacute;tica de privacidad</h1>
<p>Knot es un asistente personal de uso privado que funciona por WhatsApp. Lo usa
&uacute;nicamente su due&ntilde;o: no es un servicio abierto al p&uacute;blico y no atiende
mensajes de otros n&uacute;meros.</p>

<h2>Qu&eacute; datos trata</h2>
<p>Los mensajes que el due&ntilde;o le env&iacute;a por WhatsApp (texto, audios, im&aacute;genes
y documentos) y la informaci&oacute;n que &eacute;l mismo le pide organizar: gastos, agenda,
tareas y notas.</p>

<h2>Para qu&eacute; se usan</h2>
<p>Solo para responderle al due&ntilde;o y para guardar lo que &eacute;l pide en sus propias
cuentas (su espacio de notas y su calendario). Los mensajes se procesan con un modelo de
lenguaje para entender el pedido.</p>

<h2>Con qui&eacute;n se comparten</h2>
<p>Con nadie. Los datos no se venden, no se ceden a terceros y no se usan para publicidad.
Solo pasan por los proveedores t&eacute;cnicos necesarios para que el asistente funcione
(mensajer&iacute;a, alojamiento y procesamiento de lenguaje).</p>

<h2>Conservaci&oacute;n y borrado</h2>
<p>La informaci&oacute;n queda en las cuentas del due&ntilde;o, que puede verla, corregirla o
borrarla cuando quiera. Si otra persona le escribiera a este n&uacute;mero por error, su
mensaje se descarta sin responder y no se guarda.</p>

<h2>Contacto</h2>
<p>Cualquier consulta sobre esta pol&iacute;tica se puede hacer por WhatsApp al mismo
n&uacute;mero del asistente.</p>
</body>
</html>
"""


@router.get("/privacidad", response_class=HTMLResponse)
@router.get("/privacy", response_class=HTMLResponse)
async def privacidad():
    return HTMLResponse(PRIVACIDAD_HTML)
