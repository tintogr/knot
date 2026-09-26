# Bitácora de Knot — sesiones de desarrollo (junio–septiembre 2026)

> §0–9: junio–julio. §10: agosto–septiembre (al final del documento).

Documento extenso con **todo** lo que hablamos, cambiamos y decidimos, para retomar
sin perder contexto. El `CLAUDE.md` es el resumen corto; esto es el detalle.

---

## 0. Contexto
Knot es el bot personal de WhatsApp de Martin (Python/FastAPI en Render). Esta tanda de
trabajo arrancó cazando bugs del "buenos días" y terminó construyendo un sistema de
servicios completo, arreglando ~12 bugs de conversación y limpiando las finanzas de junio.
Workflow: se mergea a `main` y Render deploya solo. Commits relevantes citados abajo.

---

## 1. Model IDs retirados y centralización
**Problema:** el modelo `claude-sonnet-4-20250514` fue retirado por Anthropic → `404
NotFoundError` en cada llamada (registrar un gasto rompía todo). Estaba hardcodeado en ~45
lugares.
**Fix:** reemplazado por `claude-sonnet-4-6` y **centralizado** en `state.py` como
`SONNET_MODEL` / `HAIKU_MODEL` (overridables por env var). `claude_create()` default-ea a
`SONNET_MODEL`. Commits `1e3bb3d`, `e8db681`.
**Regla a futuro:** nunca hardcodear model IDs; usar las constantes.

## 2. Clima y resúmenes
- `get_weather()` fallaba silencioso → se agregó logging y (en trabajo paralelo) fallback a
  wttr.in si Open-Meteo falla.
- Se crean al startup los campos `Latitude`/`Longitude`/`City` en la Config DB de Notion
  (antes `save_location` fallaba silencioso porque no existían → el clima quedaba sin coords
  tras cada reinicio).
- **Resumen nocturno/dominical:** disparaba con `minute == 0` exacto; si el cron corría 1
  minuto tarde se perdía. Ahora tiene ventana de 3 min con tracking (igual que el diario).

## 3. Bugs de conversación (la familia más grande)
Todos eran "Knot se pierde o cruza los datos":

- **Comprobantes leídos al revés** (`92e1516`): un pago tuyo (ej: expensas, De: Martin →
  Para: Consorcio) se registraba como **ingreso/Sueldo**. Se agregaron reglas de dirección
  De/Para al prompt del agente de gastos: si el remitente es el usuario → EGRESO.

- **"¿Con qué pagaste?" (ask_payment_method) inteligente** (`92e1516` y afinado después):
  si respondías otra cosa, consumía el mensaje y lo perdía. Ahora combina scoring + una
  desambiguación con Haiku (método / SKIP / otra intención) y, si es otra intención, suelta
  el estado y **re-rutea** el mensaje.

- **Responder/citar mensajes viejos de WhatsApp** (`65b9dd7`): el webhook ignoraba
  `message.context` (la cita). Ahora se guarda un mapa `wamid → texto` de entrantes y
  salientes (`state.recent_message_texts` + `record_message_text`, y `wa_utils` registra el
  id de cada mensaje que manda), y cuando citás un mensaje se inyecta como contexto al
  clasificar. Limitación: el mapa vive en memoria, no sobrevive reinicios de Render.

- **Gastos en USD** (`25e8376`): "gasté 250 usd en X" convertía a pesos pero **no
  registraba** — preguntaba el método en texto libre y perdía el contexto. Ahora el prompt
  obliga a convertir USD→ARS y registrar igual (con "(USD X)" en notas), sin preguntar el
  método de palabra (de eso se encarga el flujo ask_payment_method).

- **El agente de gastos ahora tiene historial** (`624f21b`): `handle_gasto_agent` procesaba
  cada mensaje aislado. "supermercado" ... "42000" en mensajes separados nunca se juntaban.
  Ahora recibe `get_history(phone)` (mismo patrón que el agente de eventos) y acumula datos
  multi-mensaje; con salvaguarda de no re-registrar gastos ya confirmados (con ✅).

- **La pregunta de método no se come gastos nuevos** (`cfda87c`, `4cc73d6`): una pregunta
  "¿con qué pagaste?" quedó abierta 2 días; el mensaje "carne y atun 93mil la anonima
  **efectivo**" la respondió sin querer → le puso Efectivo a Camuzzi y **el gasto de $93.000
  se perdió**. Fix: si el mensaje parece un **gasto nuevo completo** (monto + ≥3 palabras y el
  número no es un last4 de tarjeta), NO es respuesta al método → se re-rutea como gasto nuevo,
  aunque diga "efectivo". (Se probó un timeout de 1h pero se quitó: la regla contextual
  alcanza — decisión de Martin.)

- **Día recurrente del calendario** (`111525e`): "no voy más los jueves" hacía que el agente
  llamara a borrar una vez por cada jueves futuro → 3 confirmaciones "¿Eliminás Funcional?" y
  encima no podía apuntar solo a esa serie. Ahora `eliminar_evento` tiene un param `weekday`:
  si dejás de ir un día recurrente, borra **solo esa serie** (matcheando por
  `recurringEventId` de las instancias de ese día) con **una** confirmación. Se distingue
  "el jueves 25 no voy" (target_date) de "no voy más los jueves" (weekday). También se
  arregló que `confirm_delete` borre los `extra_events` (antes se guardaban pero no se
  borraban).

- **Emails de servicios pagados** (`09498fa`): el mail "Abono vencido" del gimnasio seguía
  apareciendo en "Emails importantes" aunque estuviera pagado. Ahora `get_important_emails`
  le pasa al clasificador la lista de servicios conocidos (empresas + aliases de la DB
  Servicios) con la orden de **ignorar** recordatorios de pago/abono/vencimiento de esos
  servicios.

## 4. Facturas — lectura con visión y dedup
El "buenos días" creaba facturas duplicadas y con montos inventados. Evolución del fix:

- **Leer los PDF adjuntos con visión** (`a339356`): antes las facturas que se creaban en
  Notion salían de re-parsear un resumen de texto de 5 líneas con Haiku (perdía montos). Se
  creó `get_invoices_from_gmail(now)` en `summaries.py`: descarga los PDF adjuntos y se los
  pasa a Sonnet como document blocks → lee el "TOTAL A PAGAR" real. Canoniza el proveedor
  contra la DB Servicios.

- **Dedup robusto** (varios commits):
  - `4327eba`: match por **tokens** del proveedor (no substring): "CALF Energía" = "CALF
    (Luz)"; "calf" no colisiona con "calfibra".
  - `785fcbe`: también por el **mes del campo Date** (las entradas reales se llaman
    "Expensas", "Calf Energía" sin el mes en el nombre) y por **monto** (±2%, ≤45 días).
  - `6ea4ff8`: **no crear facturas en $0** (avisos "abono vencido" sin importe).
  - `bae7796`: buscar candidatos por **cada token** del proveedor (así "Interfast Expensas"
    encuentra la entrada "Expensas").
  - `532159a`: dedup **alias-aware** vía `service_of` (Interfast ↔ Expensas).

## 5. Sistema de Servicios (lo más grande — nuevo)
DB Notion **"Servicios"** creada bajo la página Networth. Es la **fuente de verdad** para
identificar servicios aunque el nombre varíe.
- Database id `922e0822baee4d1bba19e778e0e177d4`; data source `2d3de41d-6d68-4f7f-bbf8-87bb6d5762a0`.
- Campos: **Servicio** (título), **Empresa**, **Aliases** (palabras clave separadas por coma),
  **Categoría** (Recurrente/Servicio/Impuestos/Suscripciones), **Tipo** (Hogar/Suscripción/
  Impuesto), **Frecuencia** (Mensual/Bimestral/Trimestral/Anual), **Pagado hasta** (fecha,
  para prepagos/anuales — Knot no lo marca pendiente antes), **Vence dia**, **Llega por mail**
  (checkbox), **Activo** (checkbox).
- **13 servicios cargados:**
  - Hogar: Luz (CALF), Internet (Calfibra), Gas (Camuzzi, bimestral), Expensas (Interfast /
    Consorcio ARIES VI), Agua (EPAS — Llega por mail ☐), Teléfono (Movistar), Gimnasio (Box
    Gym Neuquén).
  - Impuesto: Monotributo (ARCA — Llega por mail ☐).
  - Suscripciones: OneDrive (Microsoft, anual, pagado hasta ~abr 2027), Render (mensual,
    compartida con Tincho — un mes cada uno, USD 38), Photoshop (Adobe, 3 meses gratis por
    retención hasta ~jul 2026, activo), iCloud (Apple), Real-Debrid (trimestral, USD 12 c/3
    meses, pagado hasta ~ago 2026).
- Código: `_ds.load_services()` al startup (`state.SERVICES_DB_ID`, env `NOTION_SERVICES_DB_ID`);
  `_ds.service_of(text)` mapea texto→servicio vía aliases. Usado en el dedup y en el extractor.
- **Categoría "Suscripciones"** agregada a la DB Finanzas (se agregó preservando las 19
  categorías existentes; ojo: agregar opciones a un multi-select en la API **reemplaza** el
  set, hay que listar TODAS las opciones con sus colores exactos).

## 6. Limpieza de datos en Notion (finanzas)
- **Junio reconciliado** contra el resumen PDF de Mercado Pago: se cargaron ~24 gastos
  desglosados + 5 ingresos (sueldos de Rafael $2.5M, préstamo del papá $100k marcado
  pendiente, reembolsos de Mati/Ana). Excluidos: compra de dólar MEP, transferencias entre
  cuentas propias, rendimientos.
- **Duplicados de facturas removidos** (movidos fuera de Finanzas, reversible): variantes de
  CALF, Calfibra junio, Camuzzi en $0, y el **ingreso fantasma** "Transferencia MP - Varios
  +$102.250" (era el pago de expensas mal leído).
- **CALF explicado:** la de junio dio $91.618,57 (real, sin descuento) vs mayo $13.414,52
  (tenía devolución de anticipo). ~La mitad de la factura son impuestos/aportes, no luz.
- **Apple Watch:** corregido de USD 200 → **USD 250** (misma entrada, no duplicado).
- **Camuzzi 2 períodos / carne y atún:** Camuzzi corregido a MP Transferencia; el gasto
  perdido de **$93.000** (carne y atún, La Anónima, efectivo) recuperado y cargado; a la
  Verdulería $2.800 se le limpió el Efectivo mal asignado.
- **EPAS:** deuda acumulada $507.459,01 registrada (pagada 11/06); recordatorio mensual de
  calendario creado (día 10) porque EPAS no manda factura por mail.
- **Monotributo junio** ($42.386,74 pagado 13/06 vía ARCA) registrado; el recordatorio que se
  había creado se borró (ya estaba pago).

## 7. IDs de Notion clave
- Finanzas (data source): `2b717b92-440a-4d78-a59a-723c913d6f5c`
- Servicios (database): `922e0822baee4d1bba19e778e0e177d4` — (data source `2d3de41d-6d68-4f7f-bbf8-87bb6d5762a0`)
- Métodos de pago (data source): `61930ca6-a8e2-4238-9b2e-bc4a69844624`
- Página Networth (padre): `aa49a53f5ebf4cfd90b37843da1001fb`

## 8. Hoja de ruta pendiente
1. **Aprendizaje de aliases**: cuando Martin aclara un proveedor nuevo ("interfast es
   expensas"), agregar el alias a la DB Servicios automáticamente.
2. Usar `service_of` para clasificar **pagos** ("pagué interfast" → Expensas) en el agente
   de gastos (hoy solo se usa en facturas).
3. **Recordatorios automáticos** de servicios con `Llega por mail = ☐` (EPAS, Monotributo)
   usando `Vence dia`.
4. **Comparar facturas mes a mes** y explicar subas (ej: "¿por qué la luz salió cara?") — se
   apoya en la lectura de PDFs ya hecha. (Sugerencia: arrancar por acá.)
5. **Ingesta de resúmenes PDF de Mercado Pago**: que Martin mande el PDF y Knot reconcilie
   solo (parsear movimientos + clasificar con contexto + dedup contra Notion). La versión
   "en serio" de lo que hicimos a mano en junio.

## 9. Cosas para verificar / abiertas
- Verdulería $2.800 y Vianda $11.000 (22/07): quedaron **sin método de pago** confirmado.
- El mapa de mensajes citados no sobrevive reinicios de Render (aceptable para un bot
  personal; persistirlo sería un extra).
- Movistar tiene varias entradas históricas; se puede ordenar como se hizo con CALF si molesta.

---

## 10. Sesión agosto–septiembre 2026

Arrancó con "Knot anda muy mal: no sigue el hilo y dice que no puede hacer cosas que
sí puede". Casi todo resultó ser una misma familia: **Knot afirmaba o hacía cosas sin
tener la información**, y cuando algo fallaba lo tapaba.

### 10.1 Setup
- Martin sigue solo en esta Mac. Se descartó sincronización automática entre PCs.
- `gh` (GitHub CLI 2.97) instalado a mano en `~/bin` (no hay Homebrew), `PATH` en
  `~/.zshrc`, login por device flow y `gh auth setup-git`. GitHub Desktop tiene sus
  propias credenciales, que la terminal no ve.

### 10.2 Pregunta "¿con qué pagaste?" y el hilo de la conversación
- `dd2a0e8`: un gasto nuevo de 2 palabras ("Expensas 110000") no llegaba al umbral
  de ≥3 palabras y se comía como respuesta al método de pago → umbral a 2.
- `264bc47`: las confirmaciones de facturas (`confirm_factura_paid`,
  `factura_mismatch_confirm`, `factura_confirm`) abandonaban la pregunta ante
  cualquier mensaje de ≥4 palabras, aunque fuera la aclaración pedida ("la de camuzzi
  que acabo de pagar es de agosto") → terminaba en `corregir_gasto` con respuestas
  sin sentido. Ahora solo se abandona si el mensaje trae un monto.
- `20d6fad`: al soltar la pregunta y reprocesar el mensaje, **el agente de gastos
  re-ejecutaba el registro anterior** en vez del nuevo (se perdieron "antigüedades" y
  "funcional 77000"). La línea del prompt "no re-registres los ✅" no alcanzaba: ahora
  recibe la lista explícita de lo registrado en los últimos 10 min + "REGLA #1: lo que
  registrás es el ÚLTIMO mensaje".

### 10.3 Duplicados
- `22f2aa3` + `28d5d1d`: guardarraíl anti-duplicado (mismo nombre + monto + fecha en
  10 min). Primero era solo memoria (`_recent_creations`), pero cada deploy de Render
  la borra → ahora también consulta Notion por `created_time`
  (`_ds.find_recent_duplicate`).
- Los 4 duplicados del 26/07 (Movistar, Claude, Verdura, Yogurt) salieron del incidente
  de las 18:08 del 10/08, 6 segundos **antes** de que existiera el guardarraíl. Martin
  los borró a mano.
- `254cdb0`: si el agente falla **después** de guardar, se lista lo que sí quedó
  guardado ("no hace falta reenviarlo") en vez de un `Error:` seco. Eso era lo que
  hacía reenviar y generaba el confuso "ya estaba registrado".

### 10.4 Estado Pagada/Impaga y correcciones
- `22f2aa3`: `create_notion_entry` nunca pasaba `estado` → todos los gastos por
  WhatsApp nacían con Estado vacío. Ahora los EGRESO nacen `Pagada`.
- `a8b6ebc`: "el alquiler está impago" → `corregir_gasto` con `new_estado`;
  `update_expense` soporta `estado`.
- `b8f561d`: lote de 6 gastos con uno sin `date` → se guardaba y después
  `data['date']` tiraba KeyError. Ahora falta date = hoy; la confirmación muestra la
  fecha si no es hoy; `corregir_gasto` tiene `new_date` ("cambiá la fecha de la cena
  al 06/09"). El agente deja de decir "no puedo hacer correcciones".
- `05daa46`: "no, ahora borraste la de agosto!" se clasificó ELIMINAR_GASTO; el
  extractor sacó "agosto" y con `limit=1` ofreció borrar "Sesiones psicóloga - julio y
  agosto". Ahora el extractor puede devolver null (verbo en pasado = reclamo), varias
  coincidencias se listan, la confirmación muestra el monto, y corregir/eliminar
  reciben contexto (`_history_context`).

### 10.5 Facturas
- `49ad66c` (**el más grave**): `mark_finance_paid` escribía `Method` como `select`,
  pero es una **relación**. Notion rechazaba el update entero (400), el except devolvía
  False y nadie lo miraba: Knot decía "✅ marcada como pagada" sin tocar nada, en casi
  todo pago con comprobante. Ahora se resuelve el id del método
  (`_resolve_payment_method_id`), se loguea el fallo y los 4 lugares que anunciaban el
  cambio chequean el bool.
- `7681e26`: el match factura↔pago era solo por proveedor + monto (±10%). Ahora el
  agente extrae `periodo_factura` del comprobante y se compara con el mes de la
  factura (`_invoice_period`: del título "— Ago 2026", o del Date con ±1 mes). Si no
  coincide, pregunta.
- `9cff9ab`: la factura de Camuzzi de abril quedó en $22,96 (era $22.966): el punto
  de miles argentino leído como decimal. Prompt con formato explícito + control contra
  el historial del proveedor (x1000 si cae en rango).
- `254cdb0`: el agente de gastos por fin usa la DB Servicios (era el ítem 2 de la hoja
  de ruta): "ARCA" → "Monotributo" (`_canonical_service_name`, solo si el nombre es
  exactamente una empresa/alias).

### 10.6 Calendario, ubicaciones y recordatorios
- `f5c129a`, `042be0e`, `0c95776`: "¿dónde es?" sobre "en lo de Mati" (location =
  "Allen") mandaba al agente a Google Contacts (falla: el token no tiene scope de
  People API; **decisión: no agregarlo**). Ahora: mira el calendario, reconoce que una
  ciudad o "lo de Mati" no es una dirección, revisa lugares conocidos y pregunta. El
  aviso decía "tiene ubicación" (falso) y el "sí" **no hacía nada** (no hay routing):
  ahora dice "Lo anoté en Allen, ¿te armo la ruta?" y manda un link de Google Maps con
  origen GPS (gratis, muestra el tráfico). `73afdcf`: no se ofrece si la ubicación es
  tu propia ciudad.
- `d770917`: "hacerme acordar del keynote" → recordatorio **hoy 12:00** inventado:
  `parse_recordatorio` no veía la agenda y estaba obligado a dar `fire_at`. Ahora
  recibe 60 días de agenda y puede devolver null.
- `73afdcf`: si no está en agenda ni tiene fecha, **busca en la web**
  (`web_search_20250305`) a qué se refiere ("el próximo keynote" = evento de Apple) y
  propone con confirmación. Tras crear un recordatorio hay 15 min para decir "no es
  hoy": se borra el evento y se rehace. El chat ya no responde sobre otro evento
  cuando no encuentra el pedido (el keynote había terminado en el cumpleaños de Martin).

### 10.7 Limpieza de datos (hecha desde Claude Code con el conector de Notion)
- CALF: la factura de **julio** ($93.115,36, comprobante 20832901) nunca se había
  cargado ni pagado → se creó y se marcó Pagada (Pronto Pago, 02/09). La de agosto
  ($90.246,49) se pagó **dos veces** el 02/09 (BBVA 13:31 y Pronto Pago 15:53); CALF
  tomó el extra a cuenta de septiembre, que quedó en $10.506,22.
- Camuzzi abril: monto corregido a $22.966. Los dos "Gas - Camuzzi" $22.966 (10/04 y
  10/08, creados con 10 min de diferencia) son el mismo pago → falta borrar uno.
- "ARCA" → "Monotributo"; cena del cumpleaños → 06/09; gastos del cumpleaños → 10/09.
- El conector **no puede borrar ni archivar**: se le pasan links a Martin.

### 10.8 Abierto
- **Doble conteo factura + pago** (ver `CLAUDE.md`, hoja de ruta 0): decisión pendiente.
- Borrar a mano: copia de "Supermercado $63.000" del 04/07 (16:18:56), uno de los "Gas -
  Camuzzi" $22.966 y el pago "Electricidad - Calf Energía" $10.506,22 del 16/09 (duplica
  a la factura de septiembre).
- `all_tool_results` en `handle_gasto_agent` se construye y nunca se usa (código muerto).
- El alias "pronto pago" está en Internet (Calfibra), pero Pronto Pago también cobra
  CALF → un gasto llamado solo "Pronto Pago" se renombraría a "Internet".

---

## 11. Migración a agente y confiabilidad (17–26 septiembre 2026)

Pregunta de fondo de Martin: *"¿por qué Knot no entiende como una persona?"*. Respuesta:
no era un chat. Era un clasificador de una palabra del día 2 del proyecto (21/03) con 24
categorías colgadas, 58 llamadas al modelo, 36 estados pendientes resueltos con listas de
palabras, y un historial de 10 mensajes recortados. **Quien decidía no veía la conversación.**

### 11.1 Agente de entrada (`ad00afa`, `7a267d4`, `4dad836`)
- `_router_agent` reemplaza a `classify()`: ve 12 turnos sin recortar, elige módulo, puede
  contestar solo, y le pasa al módulo el texto de Martin + contexto entre corchetes.
  Catálogo compartido `_CATALOGO_MODULOS`. Apagado: `KNOT_ROUTER=0` (y cae solo al
  clasificador si falla o inventa un módulo).
- Las preguntas de facturas resuelven con conversación + montos, en Sonnet.
- El agente **mira las imágenes** (antes solo sabía que había una) y las describe en el
  historial en vez de guardar "(imagen)".

### 11.2 La conversación completa (`ccc0e94`)
- Nada de lo que Knot mandaba solo (recordatorios, avisos) ni las respuestas a preguntas
  pendientes entraba al historial → "no tengo registro de ningún corte constructivo".
  Ahora `send_message` y los botones registran todo (menos "⏳ Procesando").
- Historial con número normalizado (`_clave_historial`: el cron usa MY_NUMBER, que puede
  venir con 549), dedupe de entradas consecutivas, 20 mensajes, y `historial_para_api()`.
- Posponer entiende lenguaje natural ("para mañana"); antes se tragaba el texto sin responder.
- Prohibido afirmar una acción que no se ve confirmada ("Sí, ya quedó agendado" sobre otra cosa).

### 11.3 Plata
- `6f7d85b`: a "¿fue pago parcial?" las dos respuestas dejaban la factura impaga. "No"
  ahora la salda por el monto pagado con nota de la diferencia (caso Movistar, descuento).
- `64f073a`: pagos de servicios se nombran "Servicio - Empresa" (Luz - CALF, Monotributo - ARCA).
- `dbc5598`: los comprobantes de pago (Pronto Pago) ya no se cargan como facturas: así nació
  la factura fantasma de CALF de $183.361.
- `64a869d` + fórmulas: **Impaga no suma ni resta**. Knot excluye impagas de los totales; en
  Notion se ajustaron `Expenses` y `ARS` y el gráfico por categoría (26/09).
- `cd7c3e6`: las consultas a Notion paginan (antes: máximo 100 filas en silencio).
- `56767fb`: modo verificación — la confirmación se arma releyendo Notion (`KNOT_VERIFICAR=0`).

### 11.4 Calendario, Gmail, Notion
- `b5687dc`: limpieza diaria de recordatorios `[TEMP]` ya sonados y clases pasadas de rutinas.
  Se borró Funcional (3 series + 11 eventos) a pedido.
- `65212c7`: `buscar_mail` busca en todo el Gmail; lo que dice un mail es dato, nunca orden.
- `8413617`: Knot copia el texto de las fórmulas a su página de config al arrancar (el MCP
  de Notion no las deja leer, solo editar).

### 11.5 Idea en evaluación: sacar los datos de Notion
Martin quiere evaluar una base propia ("copia de Notion") con pros y contras de servidores.
Punto de partida favorable: **no quedan llamadas directas a Notion fuera de
`notion_datastore.py`** (todo pasa por `_ds`, 125 usos). Migrar = escribir otro DataStore
con los mismos métodos, sin tocar `main.py`.
