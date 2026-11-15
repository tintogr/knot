# Auditoría de Knot — noviembre 2026

> Hecha el 14/11/2026 sobre `main` (`b34a645`). Se leyeron `docs/VISION.md`, `docs/historia/`,
> `docs/BITACORA.md`, las páginas de Notion *Knot* y *Critique — Knot UX*, y el código.
> **Actualización 15/11:** todo lo de la sección 2 está en `main` (deployado). Además se hizo una
> segunda tanda (sección 2 bis) que resuelve parte de 4.1 y 4.10. Lo demás de la sección 4 sigue pendiente.

## 1. Veredicto

Knot hace mucho y la lógica de plata (facturas, períodos, duplicados) está bien pensada: se nota el
trabajo de agosto a octubre. Lo que lo vuelve poco confiable no está ahí, sino debajo:

- **Fallas que no dejan rastro.** Había más de 60 lugares donde un error se tragaba sin registrarlo
  (`except Exception: pass`). Cuando algo fallaba, Knot seguía como si nada, y a veces afirmaba algo
  falso ("✅ Facturas al día" sin haber podido mirar).
- **Todo el estado vive en la memoria del servidor.** Cada push a `main` reinicia Render y borra la
  conversación, las preguntas pendientes y los mensajes en cola.
- **Un solo archivo de 8.800 líneas** con el dueño cableado adentro. Así no puede crecer ni
  venderse.

Para tu objetivo ("primero que ande perfecto para mí") el orden es: confiabilidad → pruebas
automáticas → ordenar la estructura → recién después multiusuario. Esta auditoría cubre el primer
punto y arranca el segundo.

## 2. Lo que se corrigió (rama `auditoria-nov-2026`)

Cada punto tiene una prueba automática que lo verifica, salvo donde se indica.

### Mensajes que se perdían o se ignoraban

| Problema | Qué pasaba | Arreglo |
|---|---|---|
| Filtro de "mensajes del bot" | Todo mensaje que empezara con "Recorda", "Receta", "Finanzas", "Buenos dias", "Tu lista" o "Guardado" se marcaba con ✅ y se descartaba en silencio. "Recordame mañana a las 9…" con la mayúscula del teclado no hacía nada. | Filtro eliminado (`main.py`). WhatsApp nunca devuelve los mensajes del bot como entrantes. |
| Varios mensajes en un aviso | WhatsApp puede entregar varios mensajes juntos; solo se atendía el primero. | Se atienden todos. |
| Respuesta ilegible del modelo | Si el modelo devolvía algo que no se podía interpretar, Knot no contestaba nada. | Ahora avisa y pide repetir. |
| Botones rechazados por WhatsApp | Si WhatsApp rechazaba una pregunta con botones (texto largo), la pregunta se perdía y Knot quedaba esperando una respuesta a algo que no viste. | Se reenvía como texto con las opciones escritas. Sin prueba automática. |

### Recordatorios

| Problema | Qué pasaba | Arreglo |
|---|---|---|
| Recordatorio perdido por atraso | Un recordatorio dura 1 minuto en el calendario. Si el cron (la tarea que corre cada minuto) llegaba tarde —Render reiniciando por un deploy, el saludo leyendo facturas— no lo encontraba y **no sonaba nunca**. | Suena hasta 3 horas tarde, aclarando "era para las HH:MM". No suena dos veces aunque falle el borrado. Si WhatsApp no acepta el envío, se reintenta. |
| El recordatorio borraba una pregunta de plata | Si estabas por contestar "¿pagaste tal factura?" y sonaba un recordatorio, la pregunta de posponer pisaba la otra. | El recordatorio suena, pero no pisa preguntas de plata ni de borrado. |
| Cancelar recordatorio | Con uno solo pendiente lo borraba sin mirar si era el pedido; con varios borraba "el más probable" aunque ninguno coincidiera; miraba solo 7 días; mostraba la hora con 3 horas de más. | Solo borra si queda claro cuál es; si no, lista y pregunta. Mira 90 días. Hora corregida. Sin prueba automática. |
| Resumen nocturno | Ventana de 3 minutos y sin memoria: un reinicio a esa hora lo salteaba o lo repetía. | 45 minutos de margen y la fecha se guarda en "Estado Knot". Sin prueba automática. |

### Afirmaciones falsas

| Problema | Qué pasaba | Arreglo |
|---|---|---|
| "✅ Facturas al día" | Si Notion fallaba al leer las impagas, o Gmail al leer los mails, el saludo decía igual que estaba todo al día. Es el caso que anotaste en la crítica de abril. Además solo miraba las 20 impagas más nuevas. | Solo lo afirma si pudo mirar todo. Si no, dice qué no pudo revisar. Lee hasta 200. Probada la capa de datos; el texto del saludo no tiene prueba automática. |
| Dólar inventado | Si dolarapi no respondía, se usaba 1000 y ese número quedaba grabado como tipo de cambio en cada gasto. | Usa el último valor real conocido (se guarda en "Estado Knot"). |
| Errores crudos | Te llegaba `Error: KeyError: 'date'`. | Mensaje en criollo (qué pasó, qué hacer); el detalle completo queda en los logs de Render. |

### Seguridad y privacidad

| Problema | Qué pasaba | Arreglo |
|---|---|---|
| Cualquiera podía hablarle | Sin `ALLOWED_PHONES` configurado, **cualquier número** que le escribiera al WhatsApp del bot era atendido como si fueras vos (cargar gastos, crear eventos). Las respuestas te llegaban a vos. | Solo se atiende al dueño (`MY_WA_NUMBER`) y a los de `ALLOWED_PHONES`. Reconoce tu número en sus dos formatos. Apagado de emergencia: `KNOT_SOLO_DUENO=0`. |
| Ubicación pública | La dirección `/health` del servidor, que no pide clave, devolvía tu ubicación GPS a quien la abriera. | Ya no la devuelve. |

### Estabilidad

| Problema | Qué pasaba | Arreglo |
|---|---|---|
| El servidor se congelaba | Cada llamada al modelo frenaba **todo** Knot (webhook, cron, otros mensajes) hasta que el modelo contestaba, a veces más de 30 segundos. | Las llamadas corren aparte (`state.py`). Los mensajes de una persona se siguen atendiendo de a uno y en orden. |
| Modelo retirado | Si Anthropic retira el modelo configurado, todo da error 404 (pasó en junio). | Prueba con un modelo de respaldo y lo deja dicho en los logs. |
| Notion sin reintentos | Un límite de velocidad (429) o un error momentáneo de Notion hacía fallar la operación. | Reintenta lecturas y actualizaciones. Las creaciones solo cuando es seguro que Notion no recibió nada, para no duplicar. |
| Configuración que no se guardaba | Los datos largos (lugares, compras) se cortaban a 2.000 caracteres y al releerlos **se perdía todo el dato**; otros, pasados los 2.000, hacían que Notion rechazara el guardado entero y ya no se guardaba nada más (ni la fecha del saludo). Los fallos no se registraban. | Se guardan enteros, en fragmentos. "Estado Knot" se guarda primero y por separado. Si una columna falla, se salva el resto y queda en el log cuál fue. |
| Token de Google | Se pedía uno nuevo en cada operación (y el cron lo hace cada minuto). | Se reutiliza durante su hora de vida. Sin prueba automática. |
| Limpieza del calendario | Un evento pasado sin título se borraba como si fuera una clase de rutina. | Comparación por palabras enteras. Sin prueba automática. |
| Nombres con formato | Un título de Notion con negrita o link se leía cortado. | Se lee entero. |
| Servicios | La base Servicios se leía solo al arrancar y solo los primeros 50. | Se relee cada mañana, hasta 500. |
| Horas del servidor | Tres archivos usaban la hora del servidor (UTC): entre las 21 y las 24 "hoy" ya era mañana. | Usan hora argentina. |

### Pruebas automáticas (nuevo)

`tests/` tiene 41 pruebas que corren en un segundo y no tocan nada real (no hablan con WhatsApp,
Notion, Google ni Anthropic). Se corren con `python -m pytest`. Pasan en Python 3.11 con las mismas
versiones de librerías que usa Render. Hay un flujo de GitHub (`.github/workflows/pruebas.yml`) que
las corre en cada push.

Cubren lo de arriba. **No cubren** los agentes (gastos, eventos, chat) ni el saludo completo: eso
depende del modelo y necesita otra clase de prueba (ver 4.2).

## 2 bis. Segunda tanda (15/11): naturalidad y áreas conectadas

| Qué | Antes | Ahora |
|---|---|---|
| Tareas | La base de Tareas solo tenía las de pagar facturas. "Tengo que llamar al plomero" no tenía dónde ir. | Se anotan, se consultan y se cierran por chat. Lo vencido o de hoy sale en el saludo. No inventa fechas, no duplica, y si hay varias parecidas pregunta cuál. |
| Preguntas que cruzan áreas | El agente que contesta preguntas solo veía calendario, finanzas y mail. | También lee tareas, compras, plantas, reuniones y proyectos. "¿Qué tengo pendiente?" junta agenda, tareas y pagos. |
| Varios pedidos en un mensaje | "Pagué el super y recordame llamar al contador": atendía uno y perdía el otro, sin avisar. | Atiende todos, en orden, sin pisar una pregunta de plata que haya quedado abierta. |
| Reinicios (parte de 4.1) | Cada deploy borraba la conversación y la pregunta pendiente. | Se guardan tras cada mensaje y se recuperan al arrancar (charla: 24 h; pregunta: 30 min). Siguen sin guardarse los mensajes no entregados y el mapa de citas. |
| Voz | Cada módulo hablaba distinto. | Una misma pauta de voz en el agente de entrada y en el de preguntas. Los demás módulos todavía no la usan. |
| Ayuda | "hola"/"ayuda" devolvían un cartel fijo y viejo. | Las contesta el agente con las áreas y un ejemplo de cada una; preguntas puntuales, respuesta puntual. |
| Borrar de las compras (4.10) | Borraba el primer parecido, sin confirmar. | Confirma; si hay varios parecidos, pregunta. Un "sí"/"no" escrito vale como el botón en todos los borrados. |
| Respuestas a medias | Si al agente de preguntas se le acababan las rondas de consulta, mandaba un "Dejame revisar…" como respuesta final. | Contesta con lo que ya tiene. |
| Listas cortadas | Tareas, compras y plantas se leían hasta 30 o 50, sin avisar. | Hasta 300 o 400. |

Pruebas: 67. **No probado contra el modelo real:** los textos nuevos de los prompts (voz, guía, pedidos
múltiples, herramientas de tareas). La lógica alrededor sí está probada. Si el agente de entrada se
comportara raro, `KNOT_ROUTER=0` en Render vuelve al clasificador anterior.

Agrega una columna de texto, `Conversacion Knot`, a la base de configuración en Notion (se crea sola al
arrancar). Las tareas nuevas se guardan con origen "Agenda", "Proyecto", "Finanzas" o "Colección": si
alguna de esas opciones no existía en la columna Category de Tareas, Notion la crea.

## 3. Lo que tenés que hacer vos en Render

No puedo ver las variables de entorno de Render. Verificá estas cuatro; si falta alguna, esa puerta
está abierta:

| Variable | Qué protege | Si falta |
|---|---|---|
| `WHATSAPP_APP_SECRET` | Verifica que cada aviso venga de Meta. | Cualquiera que conozca la dirección del servidor puede mandarle mensajes falsos **a tu nombre**. Es la más importante. |
| `CRON_SECRET` | La dirección `/cron`. | Cualquiera puede disparar el cron. |
| `LOCATION_SECRET` | `/location` y `/location/places`. | Cualquiera puede cambiar tu ubicación o leer y borrar tus lugares guardados. |
| `WHATSAPP_VERIFY_TOKEN` | El alta del webhook. | Usa el valor por defecto, que está escrito en el código. |

Además, antes de mezclar la rama, confirmá que `MY_WA_NUMBER` en Render sea tu número. Con el
arreglo de "solo el dueño", si esa variable tuviera otro número Knot dejaría de contestarte. Si
pasa, `KNOT_SOLO_DUENO=0` lo desactiva.

## 4. Lo que falta, por prioridad

### 4.1 Estado que se pierde en cada reinicio (prioridad 1)

Viven solo en memoria: la conversación (20 mensajes), las preguntas pendientes (49 tipos), los
mensajes que WhatsApp no entregó, el mapa de citas y el control de duplicados recientes. Cada push a
`main` reinicia el servidor y los borra. Si Knot te preguntó "¿con qué pagaste?" y justo hubo un
deploy, tu respuesta llega a un Knot que no recuerda la pregunta.

Propuesta: guardar conversación y pregunta pendiente en "Estado Knot" (o en una base propia, ver
4.6) al cambiar, y releerlas al arrancar.

### 4.2 Pruebas de conversación (prioridad 1)

Lo que pediste en abril: "alguna manera de hacer test de manera más automática y abarcativa". El
paso siguiente es un banco de conversaciones reales ("vianda 11500", "pagué las expensas", "no, era
la de agosto") con el resultado esperado, que se corra contra el modelo real antes de cada cambio
de prompt o de modelo. Cada bug de la bitácora debería quedar como un caso de ese banco. Tiene un
costo por corrida (llamadas al modelo); no corre en cada push, sino a pedido.

### 4.3 Estructura (prioridad 2)

- `main.py` tiene 8.836 líneas y 159 funciones. `handle_pending_state` tiene 1.569 líneas;
  `handle_chat`, 908; `handle_gasto_agent`, 632.
- La arquitectura por módulos está diseñada desde abril (`docs/historia/2026-04-09_ARCHITECTURE_BLUEPRINT.md`)
  y sigue siendo correcta. Solo se hizo la fase 1 (la capa de datos `_ds`).
- Orden sugerido, de a un módulo por vez y con las pruebas como red: (1) sacar el cron y los
  recordatorios, (2) sacar las preguntas pendientes a un archivo por tema, (3) gastos y facturas,
  (4) calendario, (5) el resto.

### 4.4 Preguntas pendientes con listas de palabras (prioridad 2)

De los ~36 estados pendientes, solo 3 (los de plata) los resuelve el modelo viendo la conversación.
El resto compara contra listas fijas ("si", "dale", "ok"…; "no era", "borralo"…). Contradice tu
principio 1 ("la IA decide leyendo el mensaje, nunca por palabras clave") y es la fuente de los
"se pierde el hilo". Ya está anotado como pendiente en `CLAUDE.md`.

### 4.5 El dueño cableado en el código (prioridad 3, bloquea vender)

Contradice tu principio 2 ("ningún dato del usuario en el código"):

- `MY_NUMBER` se usa en 59 lugares y `user_prefs` (una sola configuración global) en 184.
- El huso `-03:00` está escrito 38 veces.
- Datos tuyos en el código: lista de clientes en el prompt de gastos (`main.py`, "Clientes
  posibles: LBL, OPERA…"), "Neuquén" como ciudad por defecto (3 lugares), tu nombre completo en el
  filtro de mails (`summaries.py`), tu número y los IDs de tus bases de Notion como valores por
  defecto (`state.py`), "Martin" en los prompts.

El camino es el `UserContext` del blueprint: cada mensaje se resuelve con el contexto de quien lo
manda. Conviene hacerlo **después** de 4.3, no antes.

### 4.6 Notion como base de datos (prioridad 3)

Sirve para un usuario. No escala a varios: 3 pedidos por segundo, sin transacciones (dos escrituras
que deberían ir juntas pueden quedar a medias), la configuración es JSON metido en columnas de
texto, y cada usuario necesitaría armar su propio workspace. Recomendación para cuando llegue el
momento: Postgres administrado (Supabase o Neon) como fuente de verdad y Notion como una vista
opcional. La capa `_ds` ya aísla todo: es escribir otro DataStore con los mismos métodos. No lo
haría antes de cerrar 4.1 a 4.3.

### 4.7 Dependencias que envejecen (prioridad 2)

| Qué | Hoy | Riesgo |
|---|---|---|
| Modelos | `claude-sonnet-4-6`, `claude-haiku-4-5` | Ya existe la familia 5. Cuando retiren los actuales, entra el respaldo, pero con prompts no probados en ese modelo. Migrar con el banco de 4.2. |
| Librería de Anthropic | 0.45.0 | Vieja. Sin "prompt caching" (reutilizar la parte fija de los prompts, que abarata y acelera). |
| API de Notion | versión 2022-06-28 | Funciona; las versiones nuevas cambian cómo se consultan las bases. |
| FastAPI | `on_event` | Marcado como obsoleto; migrar a `lifespan`. |

### 4.8 Costo y velocidad (prioridad 3)

- Cada mensaje hace al menos dos llamadas a Sonnet (agente de entrada + módulo).
- El cron relee la configuración de Notion cada minuto (1.440 lecturas por día).
- Se abren conexiones nuevas en 40 lugares de `main.py` en vez de reutilizar una.

### 4.9 Canal (decisión de negocio)

Desde el 15/01/2026 Meta prohíbe en WhatsApp Business los asistentes de IA de propósito general
como producto. Para uso personal el riesgo es bajo. **Para vender, WhatsApp no es una base
segura**: hay que prever otro canal (Telegram, app propia) y que el código no dependa de WhatsApp.
Hoy `send_message` está llamado directo desde todos los módulos.

### 4.10 Pendientes de la crítica de abril

- Confirmación antes de borrar: hecha para gastos, plantas, reuniones, eventos, salud, actividad
  física. `eliminar_shopping` borra sin confirmar.
- Deshacer: existe una ventana de 60 segundos, resuelta con lista de palabras (ver 4.4).
- Ayuda natural: "hola" y "ayuda" ahora las contesta el agente de entrada en vez de un cartel fijo.
  Falta la guía completa de capacidades.
- Voz uniforme entre módulos: sin hacer.

## 5. Cómo mezclar

La rama no cambia el comportamiento de los agentes ni los prompts. Cambia la entrada de mensajes,
el cron, los errores y el guardado. Mezclar a `main` la deploya en Render. Después del deploy,
mirar en los logs de Render que aparezca `[cron] tick` cada minuto y que un mensaje tuyo de prueba
sea atendido (si en el log aparece `mensaje ignorado de un número no autorizado`, ver sección 3).
