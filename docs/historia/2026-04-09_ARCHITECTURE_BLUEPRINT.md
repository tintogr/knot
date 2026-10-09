# Matrics — Arquitectura v2 (Multi-usuario)

## Estructura de módulos

```
matrics/
├── main.py                    # FastAPI app, webhook, routing mínimo
├── config.py                  # Env vars, constantes, settings
├── models.py                  # Dataclasses: User, Expense, Event, etc.
│
├── datastore/
│   ├── __init__.py
│   ├── base.py                # Abstract DataStore (interfaz)
│   ├── notion.py              # Implementación Notion
│   └── (futuro: supabase.py)
│
├── services/
│   ├── __init__.py
│   ├── whatsapp.py            # Envío de mensajes, botones, media
│   ├── calendar.py            # Google Calendar CRUD
│   ├── gmail.py               # Lectura de mails, facturas
│   ├── weather.py             # Open-Meteo
│   ├── location.py            # OwnTracks, geocoding, Places
│   ├── ai.py                  # Claude wrapper, clasificador, extracciones JSON
│   └── exchange.py            # Dólar blue
│
├── handlers/
│   ├── __init__.py
│   ├── gastos.py              # handle_gasto_agent + correcciones
│   ├── eventos.py             # handle_evento_agent (crear/editar/eliminar)
│   ├── shopping.py            # Lista de compras + recetas
│   ├── plantas.py             # Registro de plantas
│   ├── chat.py                # Chat general con tools
│   ├── configurar.py          # Cambios de config
│   ├── reunion.py             # Notas de reunión
│   ├── recordatorio.py        # Recordatorios temporales
│   └── geo.py                 # Geo-reminders
│
├── state/
│   ├── __init__.py
│   ├── manager.py             # UserStateManager (per-user state)
│   ├── history.py             # Chat history (per-user, persistido)
│   └── pending.py             # Pending state machine (per-user)
│
├── cron/
│   ├── __init__.py
│   ├── daily_summary.py       # Resumen matutino
│   ├── nocturno.py            # Resumen nocturno
│   └── reminders.py           # Firing de recordatorios
│
├── auth/
│   ├── __init__.py
│   ├── oauth.py               # Google OAuth flow
│   └── onboarding.py          # Registro de usuarios nuevos
│
└── web/
    └── (futuro: dashboard, página de auth)
```

## Principio fundamental: UserContext

Cada request se resuelve con un `UserContext` que contiene todo lo que
los handlers necesitan. No hay más estado global.

```python
@dataclass
class UserContext:
    phone: str
    user: User                    # Datos del usuario (nombre, prefs, etc.)
    datastore: DataStore          # Su backend de datos (Notion, Supabase, etc.)
    calendar: CalendarService     # Su Google Calendar (con su token)
    gmail: GmailService | None    # Su Gmail (opcional)
    location: LocationState       # Su ubicación actual
    state: UserStateManager       # Su pending_state, history, last_touched
```

Cuando llega un mensaje:
1. `main.py` extrae el phone del webhook
2. Carga/crea el `UserContext` para ese phone
3. Pasa el contexto al handler correspondiente
4. El handler opera solo con lo que recibe — nunca toca globales

## Flujo de un mensaje

```
WhatsApp webhook
  → main.py: extraer phone + contenido
  → state/manager.py: cargar UserContext
  → services/ai.py: clasificar(texto, historial)
  → handlers/X.py: procesar(user_context, texto)
    → datastore: leer/escribir datos
    → services/calendar: consultar/crear eventos
    → services/whatsapp: enviar respuesta
  → state/manager.py: persistir cambios de estado
```

## Modelo de datos por usuario

Cada usuario tiene:
- **Config**: nombre, horarios de resumen, extras, known_places, providers
- **Credenciales**: Google OAuth tokens (Calendar + Gmail + Contacts)
- **Notion workspace**: IDs de sus bases (si usa Notion)
- **Estado efímero**: ubicación, pending_state, last_touched, historial

Lo persistido vive en el DataStore. Lo efímero vive en memoria
con TTL y se reconstruye al reiniciar.

## Migración gradual

La refactorización se hace módulo por módulo:

1. **Fase 1**: Crear `datastore/base.py` + `datastore/notion.py`
   - Mover todas las llamadas a Notion API detrás de la interfaz
   - `main.py` sigue funcionando, pero usa `datastore` en vez de httpx directo

2. **Fase 2**: Crear `state/manager.py`
   - Mover `user_prefs`, `pending_state`, `chat_history`, etc.
   - Indexar todo por phone
   - El `main.py` deja de tener estado global

3. **Fase 3**: Extraer servicios (`services/`)
   - WhatsApp, Calendar, Weather, etc. como clases instanciadas por usuario

4. **Fase 4**: Extraer handlers (`handlers/`)
   - Cada handler recibe `UserContext` en vez de leer globales

5. **Fase 5**: Auth + onboarding
   - OAuth flow, registro de usuarios, provisioning de Notion bases

Cada fase deja el bot funcional. No hay "big bang".

## Decisiones de diseño

### ¿Por qué DataStore abstracto?
Porque el backend puede cambiar por usuario (Notion vs Supabase)
o para todos (si Notion no escala). La interfaz garantiza que los
handlers no saben ni les importa dónde viven los datos.

### ¿Por qué no migrar a Sheets?
Sheets no soporta relaciones (Recipes ↔ Shopping), tipos ricos
(multi_select, status), ni queries complejas. La interfaz de Notion
para el usuario final es parte del valor.

### ¿Haiku vs Sonnet?
- **Haiku**: clasificador, extracciones JSON, enriquecimiento de items
- **Sonnet**: handlers con tools, chat general, análisis de imágenes
  Esto reduce costos ~60% sin perder calidad donde importa.

### ¿Redis/DB para estado?
No todavía. Para <50 usuarios, estado en memoria + persistencia
en Notion/DataStore al cambiar es suficiente. Si escala, se agrega
Redis como cache layer sin cambiar la interfaz.
