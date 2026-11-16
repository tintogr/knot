# Traspaso — 16/11/2026 (tarde). Cambio de computadora

Martin vende la Mac donde se hizo todo esto y sigue en otra computadora. Este archivo es para que la
próxima sesión arranque sin la conversación anterior. Leer además `CLAUDE.md`, `docs/BITACORA.md` §13 y
`docs/TRASPASO_2026-11-16.md` (secciones 3 y 4: pruebas en vivo y lista de pendientes).

## 1. Estado: Knot volvió a andar (verificado 16/11 17:20)

Martin mandó "hola" al número nuevo y Knot contestó. Qué había pasado, en orden:

1. **Meta borró el número de prueba** (y su cuenta de WhatsApp) sin avisar. Se pidió otro:
   +1 555 168 8171, Phone number ID `1510256998827654`, cuenta (WABA) `2051088002261169`.
   El usuario de sistema "Matrics token" tiene acceso total a la cuenta nueva: **el token de Render es el
   mismo de siempre**. `WHATSAPP_PHONE_ID` y `WHATSAPP_WABA_ID` actualizados en Render.
2. **La app de Meta estaba sin publicar**: así no entrega mensajes reales. Se publicó (hizo falta la página
   `/privacidad`, `paginas.py`). También se cargó `WHATSAPP_APP_SECRET` (firma de los avisos de Meta).
3. **El número nuevo quedó "Pendiente"** → error `133010 Account not registered`. Knot ahora lo registra
   solo con `WHATSAPP_PIN` (Martin lo cargó en Render; es su clave, no pedirla ni copiarla).
4. **"Recipient not in allowed list" (131030)**: la lista de destinatarios de Meta guardó el celular de
   Martin como `54 298 489 4334` y Knot escribía con 15. Ahora `wa_utils._post` prueba las otras formas
   de un celular argentino y recuerda la que anda (se olvida en cada reinicio; cuesta un intento).

## 2. Lo que sigue frágil

- **Es un número de prueba**: Meta lo puede volver a borrar. Salida firme: chip prepago propio (Martin dijo
  que lo compra cuando se acuerde). Con número propio no hay lista de destinatarios.
- Si cambia el número otra vez: actualizar `WHATSAPP_PHONE_ID` y `WHATSAPP_WABA_ID`, darle la cuenta al
  usuario de sistema, agregar el celular de Martin a la lista de destinatarios. El registro lo hace Knot.
- No hay aviso por otro canal cuando WhatsApp falla (Martin se entera porque Knot no habla).
- Mail de seguridad de Facebook del 14/11 ("¿Acabas de crear una llave de acceso?"): Martin tiene que
  revisar si fue él.

## 3. Hecho en esta sesión además de Meta

- Lectura completa del código. Diagnóstico acordado con Martin: conviven tres generaciones (estados con
  listas de palabras, catálogo de módulos, agentes con herramientas). **Dirección: un solo agente que ve
  la conversación y usa herramientas, área por área.** No partir `main.py` tal cual.
- `banco/`: 30 casos reales contra el modelo real con un mundo falso. **Todavía no corrió nunca**: falta
  el secreto `ANTHROPIC_API_KEY` en GitHub (repo → Settings → Secrets and variables → Actions). Después:
  Actions → "banco de conversaciones" → Run workflow, y arreglar lo que falle.
- `finanzas.py`: buscar, corregir, marcar pagada y deudas por un solo camino. `KNOT_FINANZAS_AGENTE=1`
  (manda las correcciones al agente que conversa) está **apagado** hasta que el banco pase con eso prendido.
- 115 pruebas, todas pasan (GitHub Actions `pruebas`, Python 3.11).

## 4. Próximos pasos, en orden

1. Pruebas en vivo de la sección 3 de `TRASPASO_2026-11-16.md`, con Martin.
2. Cargar el secreto y correr el banco; arreglar; evaluar prender `KNOT_FINANZAS_AGENTE`.
3. Seguir la convergencia al agente único (siguiente área después de plata).
4. Aviso por otro canal si WhatsApp falla; clima (open-meteo devuelve 429 desde Render); los
   `except: pass` que quedan; migrar modelos (con el banco); datos de Martin fuera del código.
5. Ojo: `_remind_pending_invoice_confirmations` registra un pago solo después de dos preguntas sin
   respuesta. Va contra "preguntar antes que actuar"; consultarlo con Martin.

## 5. Para armar la computadora nueva

- `git clone https://github.com/tintogr/knot` y abrir Claude Code en esa carpeta. Todo el código y los
  documentos están en GitHub; no quedó nada sin subir.
- Instalar `gh` y entrar: `gh auth login --hostname github.com --git-protocol https --web`.
- Python 3.11 o más nuevo y `pip install -r requirements.txt -r requirements-dev.txt` → `python -m pytest`.
  (En la Mac vieja había 3.9 y hacía falta un truco; con 3.11 no.)
- Las claves viven en Render, no en la computadora. Render y Meta se usan desde Chrome con la extensión
  de Claude y la sesión de Martin iniciada.
- La memoria de Claude y las skills personales no están en GitHub: Martin se lleva `para-la-otra-compu.zip`
  (instrucciones adentro).
