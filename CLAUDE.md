# Knot — estado del proyecto

> 📓 Para el detalle completo de todo lo que se cambió y por qué, ver [`docs/BITACORA.md`](docs/BITACORA.md).
> 🧭 **Leer antes de diseñar algo nuevo:** [`docs/VISION.md`](docs/VISION.md) — qué quiere Martin que sea Knot,
> sus principios no negociables y el workspace de 5 áreas diseñado en abril (originales en `docs/historia/`).

Bot personal de WhatsApp (Python / FastAPI) desplegado en **Render** (auto-deploy al pushear a `main`).
El dueño es **Martin** (arquitecto, no programador — todo el código lo genera Claude).

> 🚚 **Último traspaso (cambio de computadora, 16/11/2026):** [`docs/TRASPASO_2026-11-16_tarde.md`](docs/TRASPASO_2026-11-16_tarde.md).

> 🔎 **Auditoría de noviembre 2026:** [`docs/AUDITORIA_2026-11.md`](docs/AUDITORIA_2026-11.md) — qué se
> corrigió, qué variables de Render hay que verificar y la lista priorizada de lo que falta.

## Archivos
- `main.py` — webhook, clasificador, handlers de agentes (gastos, eventos, etc.), lógica de negocio.
- `notion_datastore.py` — `NotionDataStore`: acceso a las DBs de Notion (finanzas, servicios, etc.).
- `summaries.py` — resúmenes diario/nocturno, clima, lectura de facturas de Gmail (con visión de PDFs).
- `state.py` — globals compartidos, constantes, `SONNET_MODEL` / `HAIKU_MODEL`, `_ds`.
- `finanzas.py` — herramientas de plata del agente que conversa (buscar, corregir, marcar pagada, deudas). Un solo camino.
- `banco/` — banco de conversaciones: casos reales corridos contra el modelo real, con un mundo falso (ver abajo).
- `paginas.py` — páginas públicas sin clave (`/privacidad`: la exige Meta para publicar la app; sin publicar no entrega mensajes reales).
- `config.py`, `wa_utils.py`, `gcal.py` — config de usuario, WhatsApp, Google Calendar.
- `areas.py` — lo que une las áreas: tareas generales, lecturas de compras/plantas/reuniones/proyectos para
  el agente de chat, la voz común (`VOZ_KNOT`) y la guía de capacidades. **Lo nuevo va en módulos así, no en `main.py`.**

- `tests/` — pruebas automáticas (no tocan nada real). Correr con `python -m pytest` antes de commitear;
  `pip install -r requirements-dev.txt` la primera vez. Todo arreglo nuevo va con su prueba.

## Workflow de git
Somos los únicos dos devs. **Mergear directo a `main` y pushear** (Render deploya solo). No hace falta PR.
Verificar sintaxis antes de commitear: `python -c "import ast; ast.parse(open('main.py',encoding='utf-8').read())"`.
Nunca usar comillas tipográficas en el código. Model IDs centralizados en `state.py` (`SONNET_MODEL`/`HAIKU_MODEL`).
Render corre **Python 3.11** (la sintaxis `str | None` es válida); en la Mac local hay 3.9 → al testear con `exec` agregar `from __future__ import annotations`.
En la Mac de Martin, `gh` está instalado a mano en `~/bin` (sin Homebrew) y conectado a git con `gh auth setup-git`.

## Sistema de Servicios (fuente de verdad)
DB Notion **"Servicios"** (database `922e0822baee4d1bba19e778e0e177d4`, data source `2d3de41d-6d68-4f7f-bbf8-87bb6d5762a0`).
Cada servicio: Servicio, Empresa, **Aliases**, Categoría, Tipo (Hogar/Suscripción/Impuesto), Frecuencia, Pagado hasta, Vence dia, Llega por mail, Activo.
`_ds.load_services()` la carga al startup; `_ds.service_of(text)` mapea texto→servicio vía aliases (ej: "interfast" → Expensas). Se usa en el dedup de facturas, en la canonización del extractor y en el **agente de gastos** (los pagos de servicios se nombran **"Servicio - Empresa"**: "ARCA" -> "Monotributo - ARCA", "Electricidad - Calf Energía" -> "Luz - CALF"; ver `_canonical_service_name`. No aplica a suscripciones ni a medios de cobro como Pronto Pago).
DB Finanzas: data source `2b717b92-440a-4d78-a59a-723c913d6f5c`. Categoría "Suscripciones" para apps.
Campo **Estado** (select): `Impaga` / `Pagada`. Los gastos que reporta Martin nacen `Pagada`; facturas por mail y deudas nacen `Impaga`. Vacío = registros viejos, se tratan como pagados.
Campo **Method** es una **relación** a la DB Métodos de pago (`61930ca6-a8e2-4238-9b2e-bc4a69844624`), NO un select.

## Puerta de entrada (migración a agente, sept 2026)
`_router_agent` reemplaza al clasificador de una palabra: ve la conversación real (12 turnos sin recortar),
elige módulo, puede **contestar solo** (modulo=null) y le pasa al módulo el texto de Martin + contexto entre
corchetes (los módulos no ven la conversación). Catálogo compartido en `_CATALOGO_MODULOS`; los nombres
válidos se derivan de ahí (`_MODULOS_VALIDOS`).
**Apagado de emergencia: `KNOT_ROUTER=0` en Render** → vuelve `classify()`. También cae solo al clasificador
si el agente falla o inventa un módulo.
Las preguntas pendientes de plata (`confirm_factura_paid`, `factura_mismatch_confirm`, `factura_confirm`)
ya resuelven con `_classify_yes_no_answer(pregunta, texto, phone, datos)`: recibe la conversación (8 turnos)
y los montos concretos, y corre en Sonnet. Si devuelve OTRO se suelta el estado y lo atiende el agente.
Pendiente: los otros ~33 estados (botones de eventos, plantas, método de pago) siguen con código y listas
de palabras.

## Modo verificación (período de prueba)
Después de escribir en Notion, Knot **relee la página** (`_ds.get_expense`) y muestra lo que quedó
guardado: nombre, monto, fecha, tipo, categoría, estado, método, notas y link. Nunca se inventa la
confirmación: si no puede releer, muestra el mensaje de siempre. Se apaga con **`KNOT_VERIFICAR=0`**.
Aplicado a: registro de gastos, marcar factura pagada y correcciones.

## Limpieza del calendario (diaria, con el resumen)
`limpiar_eventos_vencidos(rutinas)` borra lo anterior a hoy que no aporta mirado hacia atrás:
los recordatorios `[TEMP]` que creó Knot (ya sonaron) y las clases pasadas de las actividades que
Martin declaró como rutina (`user_prefs["activities"]`). **Nunca** toca turnos, reuniones, eventos
con invitados ni cumpleaños. Google los guarda 30 días en la papelera.

## WhatsApp: política de Meta y ventana de 24 h
- Desde el **15/01/2026** los términos de WhatsApp Business prohíben a los "AI Providers" usar la API para
  ofrecer un asistente de IA cuando esa es la función principal (apunta a ChatGPT, Perplexity, Luzia, etc.).
  Knot es un asistente general pero de **un solo usuario** y no se distribuye: zona gris, riesgo bajo. Si
  Meta lo aplicara, se pierde el número del bot, no el WhatsApp personal de Martin.
- Texto libre solo dentro de las **24 h** desde el último mensaje de Martin. Fuera de eso la API acepta el
  envío pero después avisa **error 131047** por webhook (`statuses`) y el mensaje no llega. Knot lo detecta,
  lo guarda en `mensajes_no_entregados` y lo reenvía apenas Martin escribe (con la hora, solo si tiene
  menos de un día). Los resúmenes se mandan con `send_message(..., descartable=True)` y **no** se reenvían:
  Martin quiere los resúmenes del día en el día. `send_message` devuelve bool.

- **Número nuevo = hay que registrarlo.** Un número recién creado queda "Pendiente" y Meta contesta
  `(#133010) Account not registered`. Knot lo registra solo (`wa_utils.registrar_numero`) si en Render está
  **`WHATSAPP_PIN`** (6 dígitos, queda como PIN de verificación en dos pasos del número): al arrancar si Meta
  dice que no está conectado, o ante el primer 133010. `WHATSAPP_WABA_ID` (ID de la cuenta de WhatsApp, no es
  secreto) hace que además conecte la cuenta con la app para recibir mensajes. Un intento por arranque.
- **Formato del celular**: Meta solo acepta el número escrito como lo tiene anotado (con un número de
  prueba, como figura en la lista de destinatarios). Si contesta `(#131030) Recipient phone number not in
  allowed list`, `wa_utils._post` prueba las otras formas (54…, 549…, sin el 15) y recuerda la que anda.
- La app de Meta tiene que estar **publicada** (modo activo) para que entregue mensajes reales al webhook.

## Facturas por mail (reescrito 07/10/2026)
`get_invoices_from_gmail` lee **cada mail una sola vez**: busca los posteriores a `knot_state.facturas_desde_ms`
(columna Estado Knot), uno por llamada con su PDF, hasta 15 por corrida (corre con el resumen diario), y si
uno falla corta ahí para reintentarlo. Lo anterior al 07/10/2026 16:00 se revisó a mano. No son facturas: comprobantes de pago,
resúmenes de tarjeta, CencoPay, cuotas de Mercado Pago (las compras ya están cargadas).
`create_finance_invoice` da por repetida una factura solo por: mismo **número** (en Notes), mismo
proveedor + **período** escrito en el nombre, o mismo monto (±1%) a **≤20 días**. Nunca por "monto parecido
al último pago" (así se perdían todos los meses Calfibra y EPAS).
**Una factura = un registro:** si un pago que reporta Martin corresponde a una Impaga, el pago pasa a la
factura (monto, método, fecha del pago) y el gasto suelto se archiva (`_juntar_pago_con_factura`).
**Tareas "💰 Pagar ..."**: acompañan a cada Impaga (Martin lo quiere así). `sincronizar_tareas_facturas` corre
cada mañana: crea la tarea si falta y archiva las que están Listo o cuya factura ya no está Impaga.
Solo toca tareas cuyo nombre empieza con "💰 Pagar".
**Débito automático** (Calfibra; decidido por Martin): cuando llega el mail "se te debitará el X" la
factura pasa a **Pagada en el acto** (fecha = la del débito). Si después llega un **rechazo**, vuelve a
Impaga, se anota "débito RECHAZADO" y se avisa en el momento; sigue en el saludo y en Tareas hasta que se
pague. Respaldo: sin aviso, se da por pagada 2 días después del vencimiento/débito si no hubo rechazo.
Las deudas con personas (categoría Préstamo) solo se saldan si el agente de gastos las vincula
explícitamente (`paga_impaga_id`); nunca por coincidencia de palabras. Los préstamos: ingreso con categoría
Préstamo + egreso Impaga "Devolver ..." (balance 0 al pagarlo).

**Servicios que se repiten** (07-08/10/2026): al registrar un pago de un servicio no mensual (EPAS trimestral,
Real-Debrid semestral) `registrar_pago_servicio` corre su **Pagado hasta** un período. Cada mañana
`sincronizar_servicios_recurrentes` crea tareas **"🔁 Pagar ..."** una semana antes de que termine el
"Pagado hasta", y para mensuales sin mail ni débito (Monotributo) desde 5 días antes del vencimiento si no hay
pago ese mes; las archiva cuando quedan pagos. Aparecen en el saludo junto a las facturas.
Medio de pago: Martin **no** quiere preguntas a la noche; se pregunta en el momento o se deduce del texto/comprobante.

## Knot Config (la "base de mis datos" de Martin)
DB `⚙️ Knot Config` (data source `97780908-47dc-41b1-82d7-ff1fe2021465`), una fila por usuario: perfiles por
dominio, lugares, comercios, proveedores, etc. Columnas agregadas el 07/10/2026:
- **Personas** (JSON): quién es quién (`{"Fabian": {"relacion": "papá", "le_dice": [...]}}`). Si está vacía
  queda vacía (ningún dato del usuario en el código, regla de Martin). La ven el agente de entrada y el de gastos (`personas_ctx()`).
  Cuando Martin dice quién es alguien, el agente de entrada devuelve `"persona"` y se guarda (`_guardar_persona`).
- **Estado Knot** (JSON, interno): `facturas_desde_ms` = fecha del último mail de factura leído.
`save_user_config` solo guarda los campos de `UserConfig`: **todo estado que tenga que sobrevivir a un
reinicio de Render tiene que estar ahí** (antes `facturas_mails_vistos` se perdía en cada deploy).
El conector de Notion de Claude Code rechaza escribir texto que empiece con "{" en esas columnas.

## Contexto que Knot usa para clasificar
- **Rafael Lorenzo** = jefe de Martin → sus transferencias por MP = **Sueldo**.
- Facturas de servicios: se leen del PDF adjunto del mail (montos reales) y se deduplican por proveedor (tokens + aliases) + mes + monto.

## Trabajo reciente (feat/fix ya deployados)
- Junio–julio: lectura de PDFs de facturas con visión, dedup robusto, DB Servicios, ~12 bugs de conversación (ver bitácora §1–8).
- 17–26 septiembre (bitácora §11): agente de entrada con conversación e imágenes, historial completo, impagas fuera de los totales (Knot + fórmulas de Notion), Gmail completo, limpieza de calendario, verificación contra Notion.
- Agosto–septiembre (bitácora §10): anti-duplicados persistente, gastos nacen `Pagada`, marcar pagada/impaga y cambiar fecha desde WhatsApp, marcar facturas pagadas **de verdad** (bug de `Method`), match de facturas por período, montos con punto de miles, recordatorios que miran agenda + web y se pueden deshacer, borrado seguro (un reclamo no es una orden), preguntar en vez de inventar ubicaciones.

## Hoja de ruta pendiente
0. **Modelo "una factura = un registro"** (decidido por Martin): la factura es el único registro; Impaga no suma ni resta, Pagada resta; los pagos parciales se aclaran en nombre y notas. **Hecho:** Impaga no suma ni resta, ni en Knot (`get_financial_summary`) ni en Notion (fórmulas `Expenses` y `ARS`, gráfico por categoría). **Hecho también** (07/10): al pagar una factura el pago se junta con ella y no queda un gasto aparte.
1. **Aprendizaje de aliases**: cuando Martin aclara un proveedor nuevo, agregar el alias a la DB Servicios solo.
2. **Recordatorios automáticos** de servicios con `Llega por mail = ☐` (EPAS, Monotributo) usando `Vence dia`.
3. **Comparar facturas mes a mes** y explicar subas (ej: "¿por qué la luz salió cara?") — se apoya en la lectura de PDFs.
4. **Ingesta de resúmenes PDF de Mercado Pago** (parsear movimientos + clasificar + dedup).
6. **Evaluar base de datos propia** en vez de Notion (idea de Martin, sept 2026): servidores, costos, pros y contras. La capa `_ds` ya aísla todo el acceso a Notion.
5. Avisos de salida según tráfico: hoy solo se manda un link de Google Maps; calcularlo de verdad necesita routing (OSRM, gratis) + scheduler.

## Confiabilidad (auditoría 14/11/2026)
- **Solo el dueño** es atendido: `_remitente_autorizado` compara el remitente con `MY_WA_NUMBER`
  (`state.mismo_telefono` reconoce 549… y 54…15…) y con `ALLOWED_PHONES`. Apagado: `KNOT_SOLO_DUENO=0`.
- `claude_create` corre el modelo en un hilo aparte (no congela el servidor), reintenta 429/5xx/cortes y
  cae a `MODELOS_DE_RESPALDO` si el modelo fue retirado (404). Los mensajes de una persona se atienden
  de a uno (`_colas_por_telefono`).
- El cliente de Notion (`_ClienteConReintentos`) reintenta lecturas y updates; las **creaciones** solo
  ante 429 o si la conexión no llegó a abrirse (para no duplicar). `DataStoreError` es un tipo propio.
- `save_config` guarda todo JSON con `_rich_text_largo` (nunca recortar con `[:2000]`: corta el JSON y
  al releer se pierde el dato). "Estado Knot" va primero y aparte.
- Recordatorios `[TEMP]`: suenan hasta `RECORDATORIO_MARGEN_MIN` (180) minutos tarde, con la hora
  original; `_recordatorios_disparados` evita repetirlos; no pisan `_ESTADOS_QUE_NO_SE_PISAN`.
- `pending_state` es un `_PorTelefono`: la clave se normaliza, da igual el formato del número.
- Errores: `_log_error` (traza completa a los logs) + `_mensaje_de_error` (texto para el usuario, sin
  volcar la excepción). **No agregar `except Exception: pass`**: como mínimo, un `print` con el motivo.
- El saludo solo dice "✅ Facturas al día" si pudo leer Notion (`get_impaga_facturas(estricto=True)`) y
  el mail (`summaries.lectura_facturas["ok"]`).
- `/health` es pública: nunca devolver datos del usuario ahí.

## Áreas conectadas (15/11/2026)
- **Tareas generales**: módulo `TAREA` del catálogo → lo atiende `handle_chat` con las herramientas de
  `areas.py` (`crear_tarea`, `consultar_tareas`, `completar_tarea`). Nunca inventa fecha, no duplica, y si
  hay varias parecidas pregunta. Las "💰/🔁 Pagar ..." no se tocan a mano (las cierra el sistema de facturas).
  Las vencidas o de hoy salen en el saludo (`tareas_para_el_saludo`).
- El agente de chat también lee compras, plantas, reuniones y proyectos (`areas.HERRAMIENTAS`).
- **Un mensaje, varios pedidos**: el agente de entrada devuelve `"otras": [{modulo, mensaje}]` (máx. 3);
  `process_single_item` las atiende en orden con `item["_modulo"]` (sin reclasificar) y sin pisar
  preguntas de `_ESTADOS_QUE_NO_SE_PISAN`.
- **La conversación sobrevive a los reinicios**: columna `Conversacion Knot` de la config
  (`guardar_conversacion` tras cada mensaje y cada vuelta del cron si cambió; `restaurar_conversacion` al
  arrancar). Historial hasta 24 h; pregunta pendiente hasta 30 min.
- Todo borrado pide confirmación (ahora también compras) y la confirmación vale escrita (`_si_o_no_escrito`).
- `VOZ_KNOT` y `GUIA_CAPACIDADES` están en los prompts del agente de entrada y del chat: cualquier agente
  nuevo que le hable al usuario tiene que incluir `VOZ_KNOT`.

## Banco de conversaciones y agente único de plata (16/11/2026)
**Banco** (`banco/`): 30 casos de uso real ("vianda 11500", "no, eran 12500", "¿qué tengo pendiente?") que
corren a Knot ENTERO contra el modelo real, con Notion, WhatsApp y Calendar reemplazados por un mundo falso
en memoria (`banco/mundo.py`) que anota lo que Knot hizo. `banco/casos.py` es la lista: cada bug nuevo va
como un caso más. Se corre a pedido (cuesta centavos): `python -m banco.correr [palabras]`, o en GitHub →
Actions → "banco de conversaciones" → Run workflow (necesita el secreto `ANTHROPIC_API_KEY`; ahí también
se puede probar otro modelo antes de migrar). **Correrlo antes de cambiar un prompt o un modelo.**
**Dirección de la arquitectura** (decidida con Martin): converger a UN agente que ve la conversación y usa
herramientas, área por área; los módulos pasan a ser herramientas y las preguntas pendientes con listas de
palabras desaparecen. No partir `main.py` tal cual: ordenar un diseño que se va a reemplazar no rinde.
**Primer paso, plata** (`finanzas.py`): `buscar_gastos`, `corregir_gasto`, `marcar_factura_pagada` y
`consultar_deudas` viven ahí y las usa el agente de preguntas. Reglas adentro: si hay varias coincidencias
no toca nada y devuelve la lista; relee antes de confirmar; un pago >10% distinto de la factura no se marca
solo (`monto_confirmado`). Antes el agente tenía versiones propias que tomaban "la primera" y decían
"✅ marcada" sin mirar.
**`KNOT_FINANZAS_AGENTE=1`** en Render manda las correcciones (CORREGIR_GASTO) al agente que conversa en vez
del extractor. **Prendido en Render desde el 17/11/2026** (banco 30/30). Si falla, `KNOT_FINANZAS_AGENTE=0` vuelve al extractor.

## Reglas críticas
- Timezone: usar `now_argentina()`, nunca `datetime.now()` naive.
- Notion: nunca crear páginas/DBs sin pedir; `except Exception: pass` esconde errores — verificar contenido real.
- WhatsApp: número entrante `549...`, saliente `541...`; límite 1024 chars en interactivos.
- **Nunca anunciar un cambio sin chequear el resultado** (`mark_finance_paid` devuelve bool: "✅ marcada" solo si es True).
- **No obligar al modelo a completar un campo sin datos**: permitir `null` y preguntar (el recordatorio sin fecha inventaba "hoy 12:00").
- **Extractores (corregir/eliminar) reciben contexto** con `_history_context(phone)`; un verbo en pasado es un reclamo, no una orden de borrar.
- **Montos argentinos**: punto = miles, coma = decimales. Especificarlo siempre al pedirle un número al modelo.
- Conector de Notion de Claude Code: **no puede** borrar ni archivar páginas (pasarle a Martin el link) ni **leer** fórmulas (`formulaCode://` no se puede fetchear). **Solución:** al arrancar, Knot copia el texto de las fórmulas de Finanzas a un bloque de código en su página de config (`_ds.publicar_formulas`, página `32f91eb6-2a81-8149-a220-ca06efece020`); leer ese bloque antes de editar una fórmula. **Sí puede** editar esquema y fórmulas (`notion-update-data-source` con `ALTER COLUMN ... SET FORMULA(...)`) y filtros/config de vistas (`notion-update-view`). Verificar antes de afirmar que algo no se puede.
- Martin prefiere que Knot **pregunte cuando no está seguro** antes que inventar o actuar solo.
- Notion devuelve **`null`** (no ausente) en `icon`, `select`, etc. cuando están vacíos: usar `(page.get("icon") or {})`,
  nunca `page.get("icon", {})`. Un registro sin ícono hacía que `get_impaga_facturas` devolviera `[]` en silencio.
- Otros (el asistente del contador, Martin a mano) también escriben en Finanzas: el código tiene que tolerar
  registros incompletos.
- El saludo de la mañana tiene hasta 3 h de margen y guarda `knot_state.ultimo_resumen` (no se manda dos veces).

## Cambios del 17/11/2026
- **Pago sin respuesta** (decisión de Martin): después de dos preguntas sin contestar, `_anotar_sin_confirmar`
  (summaries.py) lo anota igual con **"(sin confirmar)"** en el nombre: factura única con monto parecido →
  pagada; pago muy distinto o varias facturas → el gasto queda marcado y la factura sigue Impaga.
- **Aviso por otro canal** (`alertas.py`): si WhatsApp rechaza un envío o no entrega por algo que no es la
  ventana de 24 h, Knot crea un evento con alarma en Google Calendar (marca `[ALERTA KNOT]`, nunca `[TEMP]`)
  y prueba mandar un mail a la misma cuenta. Un aviso cada 6 h como mucho.
- **Preguntas de eventos** (ubicación, se repite, aviso antes): la respuesta escrita la interpreta
  `_interpretar_respuesta`; si habla de otra cosa se suelta la pregunta y lo atiende el agente.
- Agente de entrada: si devuelve JSON roto, se le pide una vez que lo corrija.
- Clima: después de un 429 de open-meteo se usa wttr.in por 6 h.
- Ningún `except Exception: pass` (una prueba lo impide). Celular solo en Render; ciudad desde la config
  (`state.ciudad_usuario()`).
- Sin crédito de Anthropic, Knot avisa "se terminó el crédito" (pasó este día: el banco no pudo correr).
