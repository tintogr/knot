# Fase 4 — Inteligencia contextual

**Fecha:** 2026-04-20
**Estado:** Aprobado

## Objetivo

Que Knot use información que ya tiene (calendario, shopping, geo-reminders, facturas) para ser útil sin que el usuario tenga que preguntar. No requiere nuevas fuentes de datos — es cruzar las que existen de forma inteligente.

---

## Ítems

### 4a — Ubicaciones de eventos y contexto geográfico

#### Parte 1: Geocodificación al crear un evento

Cuando el usuario crea un evento que menciona un lugar ("turno en Qura odontomedicina", "reunión en el café del centro"), Knot intenta geocodearlo con Nominatim (OpenStreetMap) usando el nombre + ciudad del usuario.

**Flujo:**
1. Se crea el evento en GCal normalmente
2. Si hay un lugar mencionado en el nombre/descripción, Knot hace una búsqueda en Nominatim
3. Si encuentra resultado con buena confianza: pregunta *"¿Qura queda en Italia 376, Neuquén?"*
   - Usuario dice sí → guarda `lat/lon` en `extended_properties` del evento de GCal
   - Usuario corrige ("no, es en Av. Argentina 123") → reintenta geocodificación con la corrección, confirma y guarda
   - Usuario dice que no y nada más → no guarda, responde "Ok, la próxima vez que vayas podés decirme dónde es"
4. Si no encuentra resultado: no pregunta, crea el evento normalmente

**Guardado:** `extendedProperties.private.knot_lat` y `knot_lon` en el evento de GCal.

#### Parte 2: Contexto geográfico en recordatorios

Cuando Knot envía un recordatorio (tanto los anticipados de 15/30/60 min como el de la hora exacta del evento), si el evento tiene `knot_lat/knot_lon` guardados, cruza con:
- Shopping list (items pendientes)
- Geo-reminders activos
- Tasks pendientes con ubicación implícita

Usa Claude para razonar si algún ítem de esas listas puede resolverse de camino al evento, y si hay resultado relevante, lo agrega al mensaje del recordatorio.

**Ejemplo de recordatorio con contexto:**
> *"🔔 En 15 minutos: Turno Qura — Italia 376*
> *De camino, tenés almendras y proteína en tu lista de compras — suele haber dietéticas por esa zona."*

Si el evento NO tiene ubicación guardada, el recordatorio igual puede incluir:
> *"¿Sabés si hay algo de camino que puedas resolver? Decime dónde queda y la próxima vez te aviso."*

Esto aplica tanto al recordatorio anticipado como al mensaje de "ahora tenés X" (cuando el recordatorio es en el momento exacto del evento).

**Límite:** solo se menciona si hay algo concreto de la lista — no se fuerza contexto si no hay nada relevante.

---

### 4b — Detección de recurrencias

Cuando el usuario crea un evento con palabras que sugieren recurrencia semanal ("fútbol los lunes", "gym los martes") O cuando Knot detecta que creó el mismo tipo de evento 2+ semanas seguidas en el mismo día/hora aproximados, ofrece hacerlo recurrente.

**Flujo:**
1. Al crear un evento: el clasificador detecta señales de recurrencia en el texto ("todos los", "cada lunes", "los martes")
   - Si detecta: pregunta *"¿Lo agrego como evento recurrente cada semana?"*
2. Al crear un evento "uno más" igual al anterior (mismo nombre, mismo día de la semana, semana siguiente): Knot lo detecta y pregunta *"La semana pasada también tuviste fútbol los lunes. ¿Lo hago recurrente?"*

**Implementación:** en `handle_evento_agent`, después de crear el evento, verificar si hay eventos con nombre similar en las últimas 2-3 semanas en el mismo día de la semana.

---

### 4c — Facturas pendientes con antigüedad en el resumen diario

En `send_daily_summary`, la sección de facturas Impaga muestra cuántos días llevan pendientes.

**Formato actual:** `- Movistar: $6.500`
**Formato nuevo:** `- Movistar: $6.500 _(14 días pendiente)_`

Si lleva más de 30 días: `- Movistar: $6.500 ⚠️ _(32 días pendiente)_`

**Implementación:** `get_impaga_facturas()` ya devuelve `EntryResult` con `date` (fecha de creación). Calcular diferencia con hoy.

---

### 4d — Resumen semanal dominical

Los domingos a la noche (configurable, default 21:00), Knot envía un resumen diferente al daily:

**Contenido:**
1. **Semana que pasó:** total de gastos vs semana anterior ("Gastaste $85k esta semana, la semana pasada $72k")
2. **Semana que viene:** los eventos de los próximos 7 días agrupados por día
3. **Facturas Impaga activas:** lista completa con días pendientes
4. **Plantas:** las que tienen riego pendiente o próximo (si aplica)

**Diferencia con el daily:** el daily es hoy + facturas. El semanal es retrospectivo + próximos 7 días + overview financiero.

**Implementación:**
- Nuevo campo en `user_prefs`: `resumen_semanal_enabled` (default True), `resumen_semanal_hour` (default 21)
- En el scheduler que ya corre cada hora: si es domingo y la hora coincide y `resumen_semanal_enabled`, llamar `send_weekly_summary()`
- `send_weekly_summary()` consulta GCal próximos 7 días, Finanzas últimos 7 días, Impaga activas, plantas

---

## Google Maps routing (anotado para fase futura)

El usuario quiere poder pedir: *"Mandame la ruta a Altabarda pasando por La Anónima"* y recibir un link de Google Maps con esa ruta. Requiere Google Maps Directions API (no disponible hoy, anotar para cuando se integre).

---

## Compatibilidad con flujos existentes

- La geocodificación es completamente aditiva — si falla o el usuario rechaza, el evento se crea igual
- El contexto en recordatorios solo se agrega si hay algo relevante — si no hay shopping/geo-reminders, el recordatorio es idéntico al actual
- `resumen_semanal_enabled` arranca en True pero el usuario puede desactivarlo via CONFIGURAR
- `get_impaga_facturas()` ya existe — solo se agrega el cálculo de antigüedad

## Componentes a implementar

### main.py
- En `handle_evento_agent`: después de crear evento, intentar geocodificación del lugar y preguntar al usuario
- `pending_state` nuevo tipo `geocode_confirm`: `{ type, event_id, candidate_name, lat, lon }`
- Handler de `geocode_confirm` en `handle_pending_state`
- En `send_daily_summary`: agregar días-pendiente a cada factura Impaga
- En los handlers de recordatorio (REM15, REM60, exacto): agregar contexto geográfico si el evento tiene `knot_lat/knot_lon`
- `build_geo_context(lat, lon)` → consulta shopping list + geo-reminders + Claude razona relevancia
- `send_weekly_summary()` — nueva función
- Scheduler: chequear si es domingo + hora del resumen semanal
- En `load_user_config`: cargar `resumen_semanal_enabled` y `resumen_semanal_hour`
- En `handle_evento_agent`: detección de recurrencia por texto + por historial reciente de GCal
