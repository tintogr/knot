# Knot — estado del proyecto

> 📓 Para el detalle completo de todo lo que se cambió y por qué, ver [`docs/BITACORA.md`](docs/BITACORA.md).

Bot personal de WhatsApp (Python / FastAPI) desplegado en **Render** (auto-deploy al pushear a `main`).
El dueño es **Martin** (arquitecto, no programador — todo el código lo genera Claude).

## Archivos
- `main.py` — webhook, clasificador, handlers de agentes (gastos, eventos, etc.), lógica de negocio.
- `notion_datastore.py` — `NotionDataStore`: acceso a las DBs de Notion (finanzas, servicios, etc.).
- `summaries.py` — resúmenes diario/nocturno, clima, lectura de facturas de Gmail (con visión de PDFs).
- `state.py` — globals compartidos, constantes, `SONNET_MODEL` / `HAIKU_MODEL`, `_ds`.
- `config.py`, `wa_utils.py`, `gcal.py` — config de usuario, WhatsApp, Google Calendar.

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

## Contexto que Knot usa para clasificar
- **Rafael Lorenzo** = jefe de Martin → sus transferencias por MP = **Sueldo**.
- Facturas de servicios: se leen del PDF adjunto del mail (montos reales) y se deduplican por proveedor (tokens + aliases) + mes + monto.

## Trabajo reciente (feat/fix ya deployados)
- Junio–julio: lectura de PDFs de facturas con visión, dedup robusto, DB Servicios, ~12 bugs de conversación (ver bitácora §1–8).
- Agosto–septiembre (bitácora §10): anti-duplicados persistente, gastos nacen `Pagada`, marcar pagada/impaga y cambiar fecha desde WhatsApp, marcar facturas pagadas **de verdad** (bug de `Method`), match de facturas por período, montos con punto de miles, recordatorios que miran agenda + web y se pueden deshacer, borrado seguro (un reclamo no es una orden), preguntar en vez de inventar ubicaciones.

## Hoja de ruta pendiente
0. **Decisión abierta — doble conteo factura + pago**: cuando un pago matchea una factura, quedan dos EGRESO por el mismo dinero (la factura marcada Pagada y el gasto del pago). Pasó con CALF mayo/sep y Calfibra abril. Opción propuesta: al marcar la factura, archivar el gasto del pago (la factura queda como único registro, con el método). Falta que Martin decida.
1. **Aprendizaje de aliases**: cuando Martin aclara un proveedor nuevo, agregar el alias a la DB Servicios solo.
2. **Recordatorios automáticos** de servicios con `Llega por mail = ☐` (EPAS, Monotributo) usando `Vence dia`.
3. **Comparar facturas mes a mes** y explicar subas (ej: "¿por qué la luz salió cara?") — se apoya en la lectura de PDFs.
4. **Ingesta de resúmenes PDF de Mercado Pago** (parsear movimientos + clasificar + dedup).
5. Avisos de salida según tráfico: hoy solo se manda un link de Google Maps; calcularlo de verdad necesita routing (OSRM, gratis) + scheduler.

## Reglas críticas
- Timezone: usar `now_argentina()`, nunca `datetime.now()` naive.
- Notion: nunca crear páginas/DBs sin pedir; `except Exception: pass` esconde errores — verificar contenido real.
- WhatsApp: número entrante `549...`, saliente `541...`; límite 1024 chars en interactivos.
- **Nunca anunciar un cambio sin chequear el resultado** (`mark_finance_paid` devuelve bool: "✅ marcada" solo si es True).
- **No obligar al modelo a completar un campo sin datos**: permitir `null` y preguntar (el recordatorio sin fecha inventaba "hoy 12:00").
- **Extractores (corregir/eliminar) reciben contexto** con `_history_context(phone)`; un verbo en pasado es un reclamo, no una orden de borrar.
- **Montos argentinos**: punto = miles, coma = decimales. Especificarlo siempre al pedirle un número al modelo.
- El conector de Notion de Claude Code **no puede borrar ni archivar** páginas: pasarle a Martin el link directo.
- Martin prefiere que Knot **pregunte cuando no está seguro** antes que inventar o actuar solo.
